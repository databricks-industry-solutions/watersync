---
name: watersync-row-diff
description: Compares row counts and MD5 content hashes between JDBC source tables (any database — PostgreSQL, Lakebase, MySQL, SQL Server, Oracle) and their Delta target tables in the watersync ingestion pipeline. Use when the user asks to check row differences, verify data parity, validate a full refresh, or run a hash check between source and target.
---

# WaterSync Row-Diff Validation

Run this validation after an ingestion pipeline run to confirm that the source database tables and the Delta final tables are in sync. Works with **any** source database type supported by watersync: PostgreSQL, Lakebase (Databricks Postgres), MySQL, SQL Server, Oracle, or any source reachable via a Unity Catalog connection.

## Prerequisites

- The ingestion config table must be readable at the FQN stored in the job's `configuration_fqn` parameter.
- For JDBC sources: use `spark.read.format("jdbc")` with the JDBC URL and resolved credentials (no native driver installation needed for validation).
- For UC Connection sources: the connection must be accessible via `spark.read.format("jdbc").option("databricks.connection", connection_name)`.

## What the validation checks

| # | Check | Type | Pass criteria |
|---|-------|------|---------------|
| 1 | **Source = Active count** | **PASS/FAIL** | Source count = Final active (`__END_AT IS NULL`) count |
| 2 | **MD5 hash** | **PASS/FAIL** | Per-table aggregate MD5 over business columns matches between source and final table |
| 3 | **Staging cleanliness** | **WARNING** | Staging row count equals source count. Staging bloat from incremental append-mode is expected and does NOT fail the check — it is reported as a ⚠️ warning with guidance to run `full_refresh=true` to reset |

## How to run

### Step 1 — Read the ingestion config

Query the config table to discover all source/staging/target mappings. Both `jdbc_url` and `connection_name` must be read — one or the other will be populated:

```python
config_fqn = "<catalog>.<schema>.jdbc_ingestion_config"  # from job parameter configuration_fqn
ingestion_group = "<group>"  # from job parameter ingestion_group
HASH_ROW_LIMIT = 1_000_000  # Hash only the latest N rows for tables > 1M records

configs = spark.sql(f"""
    SELECT source_table_name, staging_table_fqn, target_table_fqn,
           key_columns, watermark_column, jdbc_url, jdbc_user,
           uc_secret_name, connection_name
    FROM {config_fqn}
    WHERE ingestion_group = '{ingestion_group}' AND enabled = true
""").collect()
```

**Important:** Always include `watermark_column` in the config query — it is needed for the hash-limiting logic (Step 5).

### Step 1b — Resolve JDBC credentials

The watersync config table supports three credential sources. Check which fields are populated and resolve in priority order:

| Config fields | Method | Resolution |
|--------------|--------|------------|
| `uc_secret_name` is set | **Unity Catalog secret** | `dbutils.secrets.get(catalog=..., schema=..., key=...)` |
| `jdbc_secret_scope` + `jdbc_secret_key` are set | **Databricks secret scope** | `dbutils.secrets.get(scope=..., key=...)` |
| `connection_name` is set (no direct JDBC) | **UC Connection** | No password needed — Spark handles auth via the connection |

**Unity Catalog secrets** (most common for Lakebase): The `uc_secret_name` field uses the format `catalog.schema.secret_name`. Split on `.` and pass as **keyword arguments**:

```python
# uc_secret_name = "main.watersync.db_pass"
parts = uc_secret_name.split(".", 2)
password = dbutils.secrets.get(catalog=parts[0], schema=parts[1], key=parts[2])
```

> **Common mistakes:**
> - `secret('catalog.schema', 'key')` SQL function — this is for Databricks secret scopes, not UC secrets.
> - `dbutils.secrets.get(scope='catalog.schema', key='key')` — wrong; UC secrets require the `catalog=` keyword arg.
> - `w.postgres.generate_database_credential(...)` — the SDK `postgres` service may not be available on all workspace versions.

**Databricks secret scopes** (legacy): When `jdbc_secret_scope` and `jdbc_secret_key` are both set:

```python
password = dbutils.secrets.get(scope=jdbc_secret_scope, key=jdbc_secret_key)
```

The resolved password is then passed to `spark.read.format("jdbc").option("password", password)` or to a native Python driver connection.

### Step 2 — Determine the connection method

Prefer `spark.read.format("jdbc")` for all read-only validation (counts, hashes). It avoids installing native drivers and works uniformly across all database types. Native drivers (psycopg2, etc.) are only needed for DML operations (inserts, updates, deletes).

| Field | Method | How to connect |
|-------|--------|----------------|
| `connection_name` is set | **UC Connection** | `spark.read.format("jdbc").option("databricks.connection", connection_name)` |
| `jdbc_url` is set | **Direct JDBC** | `spark.read.format("jdbc").option("url", jdbc_url).option("user", user).option("password", password)` |

Both methods use the same Spark JDBC reader. The only difference is how authentication is handled:

```python
# UC Connection
def build_jdbc_reader_uc(connection_name, dbtable):
    return (spark.read.format("jdbc")
            .option("databricks.connection", connection_name)
            .option("dbtable", dbtable))

# Direct JDBC (Lakebase, Postgres, MySQL, etc.)
def build_jdbc_reader_direct(jdbc_url, user, password, dbtable):
    return (spark.read.format("jdbc")
            .option("url", jdbc_url)
            .option("user", user)
            .option("password", password)
            .option("dbtable", dbtable))
```

**Lakebase detection:** If the JDBC URL host matches `*.database.*.cloud.databricks.com`, this is a Lakebase (Databricks Postgres) endpoint. The endpoint hostname pattern is `ep-<name>-<id>.database.<region>.cloud.databricks.com`. Authenticate using the credential resolved in **Step 1b** (typically a UC secret).

### Step 3 — Row count comparison

For each config row, count rows in three places:

```python
# Source count via Spark JDBC pushdown
src_count = (spark.read.format("jdbc")
    .option("url", jdbc_url).option("user", user).option("password", password)
    .option("dbtable", f"(SELECT COUNT(*) AS cnt FROM {source_table}) t")
    .load().first()["cnt"])

# Delta staging
staging_count = spark.table(staging_table_fqn).count()

# Delta final (active records only)
active_count = spark.sql(
    f"SELECT count(*) AS cnt FROM {target_table_fqn} WHERE __END_AT IS NULL"
).first()["cnt"]
```

The **primary integrity check** is: `src_count == active_count`. This is pass/fail.

### Step 4 — Staging cleanliness (WARNING only)

Staging count may exceed source count for incremental ingestion because staging uses append mode — each batch’s rows accumulate across runs. This is **expected behavior**, not a data-integrity failure.

**Report staging bloat as a ⚠️ warning, never as a ❌ failure.** Include guidance:

```
⚠️  Staging bloat (not a data-integrity failure): customers, products
   Staging uses append mode for incremental ingestion. Run with full_refresh=true to reset.
```

Staging should only equal source after a clean full refresh.

### Step 5 — MD5 hash comparison (with 1M threshold)

For tables with **≤ 1M rows**: hash ALL rows (full comparison).
For tables with **> 1M rows**: hash only the **latest 1M rows** ordered by `watermark_column` DESC. This keeps the validation fast while still catching drift in recent data.

#### Determine hash scope

```python
HASH_ROW_LIMIT = 1_000_000
use_limit = src_count > HASH_ROW_LIMIT
hash_scope = f"latest {HASH_ROW_LIMIT:,}" if use_limit else "full"
```

#### Source hash — push MD5 to the source DB via JDBC

```python
cols_expr = " || '|' || ".join(
    [f"COALESCE(CAST({col} AS VARCHAR), '')" for col in business_cols]
)
if use_limit and watermark_col:
    hash_query = (
        f"(SELECT MD5({cols_expr}) AS row_hash FROM {source_table} "
        f"ORDER BY {watermark_col} DESC LIMIT {HASH_ROW_LIMIT}) t"
    )
else:
    hash_query = f"(SELECT MD5({cols_expr}) AS row_hash FROM {source_table}) t"

src_hashes = sorted(
    r["row_hash"] for r in
    spark.read.format("jdbc")
    .option("url", jdbc_url).option("user", user).option("password", password)
    .option("dbtable", hash_query)
    .load().collect()
)
src_agg_hash = hashlib.md5("".join(src_hashes).encode()).hexdigest()
```

#### Target hash — Delta active rows, with matching limit

```python
target_df = spark.table(target_table_fqn).filter("__END_AT IS NULL")
if use_limit and watermark_col:
    target_df = target_df.orderBy(F.desc(watermark_col)).limit(HASH_ROW_LIMIT)
target_df = target_df.select(
    F.md5(F.concat_ws("|", *[F.col(c).cast("string") for c in business_cols])).alias("row_hash")
)
tgt_hashes = sorted(r["row_hash"] for r in target_df.collect())
tgt_agg_hash = hashlib.md5("".join(tgt_hashes).encode()).hexdigest()
```

**Important:** Both source and target must use the **same limit and ordering** so the compared row sets match.

### Step 6 — Pre-compute schemas before the loop

To avoid repeated Spark Analyze RPCs (one per iteration), compute target schemas once before the loop:

```python
target_schemas = {}
for c in configs:
    tfqn = c["target_table_fqn"]
    target_schemas[tfqn] = [
        f.name for f in spark.table(tfqn).schema.fields if f.name not in EXCLUDE_COLS
    ]
```

Then inside the loop, use `target_cols = target_schemas[target]` instead of re-reading the schema.

### Step 7 — Report results

Present a summary table with the hash scope column:

```
Table              Source   Active  Src=Act     Hash            Scope  Staging    Clean
----------------------------------------------------------------------------------
customers             201      201    ✅          ✅             full      208       ⚠️
products              102      102    ✅          ✅             full      106       ⚠️
orders              1,202    1,202    ✅          ✅             full    1,202       ✅
big_table       5,400,000 5,400,000  ✅          ✅   latest 1,000,000 5,400,000    ✅
```

**Pass/fail logic:**
- **❌ FAIL** if ANY table has `src_count != active_count` OR hash mismatch
- **✅ PASS** if all source=active counts match AND all hashes match
- **⚠️ WARNING** (does NOT affect pass/fail) if staging_count != source_count

End with a clear **✅ ALL CHECKS PASSED** or **❌ SOME CHECKS FAILED** verdict.

## Column selection for hashing

Exclude these columns from the hash:
- SCD2 metadata: `__START_AT`, `__END_AT`
- Ingestion metadata: `_ingested_at`, `_source_table`, `_ingestion_group`, `_ingestion_type`, `_IS_DELETED`, `_csa_update_dt`
- Timestamp columns that differ between source and target due to timezone or precision differences (e.g., `updated_at`, `last_changed`, `modified_at`)

Include all business columns (IDs, names, amounts, statuses, etc.).

## Troubleshooting

| Symptom | Likely cause |
|---------|--------------|
| Active count > source count | Staging not truncated on full refresh — ghost records from prior runs replayed by CDC |
| Staging count > source count | **Expected for incremental ingestion** — staging uses append mode. Not a failure; report as ⚠️ warning |
| Hash mismatch but counts match | Column value drift (e.g., updated timestamps, precision differences) — check which columns differ |
| Source count > active count | Ingestion missed rows — check watermark window, JDBC partitioning bounds, or fetch size |
| UC Connection error | Verify the connection name exists and the current user has USE permission on it |
| Hash scope says "latest 1,000,000" | Table exceeded 1M rows; only the most recent 1M (by watermark_column) were hashed. This is by design for performance |
