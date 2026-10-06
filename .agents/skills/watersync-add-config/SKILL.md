---
name: watersync-add-config
description: Helps users add, update, or validate new ingestion source configurations in the watersync `jdbc_ingestion_config` table. Use when the user asks to add a new table to ingestion, configure a new source, onboard a new database, set up a new ingestion group, or troubleshoot config validation errors.
---

# WaterSync — Adding Ingestion Configurations

This skill guides the agent through adding new rows to the `jdbc_ingestion_config` Delta table that drives the watersync ingestion pipeline. Each row represents one source table to be ingested.

## Config table location

The config table FQN follows the pattern `<catalog>.<schema>.jdbc_ingestion_config`. The exact FQN is stored in the job's `configuration_fqn` parameter. Query the job or ask the user for the catalog and schema.

## Table schema

| Column | Type | Required | Description |
|--------|------|----------|-------------|
| `ingestion_group` | STRING | ✅ | Logical grouping name (e.g. `epic`, `finance`, `hr`). Tables in the same group run in the same job. |
| `source_table_name` | STRING | ✅ | Fully qualified source table name including schema (e.g. `dbo.customers`, `public.orders`, `HR.EMPLOYEES`). |
| `staging_table_fqn` | STRING | Optional | Three-part Delta staging table (e.g. `catalog.schema.staging_customers`). If NULL, auto-derived from source name: `<config_catalog>.<config_schema>.staging_<sanitized_source_name>`. |
| `target_table_fqn` | STRING | ✅ | Three-part Delta target table (e.g. `catalog.schema.customers`). Must match pattern `X.Y.Z`. |
| `ingestion_type` | STRING | ✅ | `incremental` or `full`. See ingestion type rules below. |
| `key_columns` | STRING | Conditional | Comma-separated primary key column(s) (e.g. `customer_id` or `order_id,line_id`). **Required for `incremental`**. Required for `full` when `auto_cdc_from_snapshot = true`. |
| `watermark_column` | STRING | Conditional | Column used for incremental change detection (e.g. `updated_at`). **Required for `incremental` unless `epic_csa_enabled = true`**. |
| `partition_column` | STRING | Optional | Column for JDBC parallel read partitioning (e.g. `id`). Improves read parallelism. |
| `predicate_column` | STRING | Optional | Column for predicate pushdown filtering. |
| `epic_csa_enabled` | BOOLEAN | ✅ | `true` to use Epic CSA (Change Sequence Auditing) worker instead of timestamp watermark. **Only valid with `incremental` type**. When true, `watermark_column` is not required. |
| `auto_cdc_from_snapshot` | BOOLEAN | ✅ | `true` to derive CDC changes from full snapshots. **Only valid with `full` type**. Requires `key_columns` and `staging_table_fqn`. |
| `jdbc_url` | STRING | Conditional | JDBC connection URL. **Required if `connection_name` is NULL**. See connection section below. |
| `jdbc_user` | STRING | Conditional | JDBC username. **Required if using direct JDBC (no `connection_name`)**. |
| `jdbc_secret_scope` | STRING | Conditional | Databricks legacy secret scope for password. Must be set together with `jdbc_secret_key`. |
| `jdbc_secret_key` | STRING | Conditional | Secret key within the scope. Must be set together with `jdbc_secret_scope`. |
| `uc_secret_name` | STRING | Conditional | UC secret in `catalog.schema.secret_key` format (e.g. `my_catalog.my_schema.db_pass`). Alternative to scope/key secrets. |
| `connection_name` | STRING | Conditional | Unity Catalog connection name (e.g. `my_postgres_connection`). **If set, `jdbc_url`, `jdbc_user`, and secrets are not required** — the connection handles authentication. |
| `watermark_threshold_minutes` | INT | ✅ | Lookback safety window in minutes (default: `5`). |
| `fetch_size` | INT | ✅ | JDBC fetch size (default: `10000`). |
| `num_partitions` | INT | ✅ | Number of parallel JDBC read partitions (default: `8`). |
| `update_dttm` | TIMESTAMP | Auto | Set to `current_timestamp()` on insert/update. |
| `enabled` | BOOLEAN | ✅ | `true` to include in ingestion runs, `false` to skip. |

## Connection methods

Every config row must use **one** of two connection methods:

### Option A — Unity Catalog Connection (preferred)

Set `connection_name` to a UC connection that already exists in the workspace. Leave `jdbc_url`, `jdbc_user`, and all secret fields NULL. The connection object owns the URL, credentials, and driver.

```sql
-- Example: UC Connection row
INSERT INTO catalog.schema.jdbc_ingestion_config VALUES (
  'finance',                                    -- ingestion_group
  'dbo.invoices',                               -- source_table_name
  NULL,                                         -- staging_table_fqn (auto-derived)
  'catalog.schema.invoices',                    -- target_table_fqn
  'incremental',                                -- ingestion_type
  'invoice_id',                                 -- key_columns
  'modified_date',                              -- watermark_column
  'invoice_id',                                 -- partition_column
  NULL,                                         -- predicate_column
  false,                                        -- epic_csa_enabled
  false,                                        -- auto_cdc_from_snapshot
  NULL, NULL, NULL, NULL, NULL,                 -- jdbc_url, jdbc_user, secret_scope, secret_key, uc_secret_name
  'my_sqlserver_connection',                    -- connection_name
  5, 10000, 8,                                  -- watermark_threshold, fetch_size, num_partitions
  current_timestamp(),                          -- update_dttm
  true                                          -- enabled
);
```

### Option B — Direct JDBC

Set `jdbc_url` and `jdbc_user`. Provide credentials via **one** of:
- `uc_secret_name` (format: `catalog.schema.secret_key`) — recommended
- `jdbc_secret_scope` + `jdbc_secret_key` (legacy Databricks secret scope)

Leave `connection_name` NULL.

```sql
-- Example: Direct JDBC row (PostgreSQL with UC secret)
INSERT INTO catalog.schema.jdbc_ingestion_config VALUES (
  'epic',                                       -- ingestion_group
  'dbo.patients',                               -- source_table_name
  NULL,                                         -- staging_table_fqn (auto-derived)
  'catalog.schema.patients',                    -- target_table_fqn
  'incremental',                                -- ingestion_type
  'patient_id',                                 -- key_columns
  NULL,                                         -- watermark_column (not needed: epic_csa_enabled)
  'patient_id',                                 -- partition_column
  NULL,                                         -- predicate_column
  true,                                         -- epic_csa_enabled
  false,                                        -- auto_cdc_from_snapshot
  'jdbc:postgresql://host:5432/db?sslmode=require', -- jdbc_url
  'app_user',                                   -- jdbc_user
  NULL, NULL,                                   -- jdbc_secret_scope, jdbc_secret_key
  'catalog.schema.db_pass',                     -- uc_secret_name
  NULL,                                         -- connection_name
  5, 10000, 8,                                  -- watermark_threshold, fetch_size, num_partitions
  current_timestamp(),                          -- update_dttm
  true                                          -- enabled
);
```

## JDBC URL formats by database type

| Database | JDBC URL pattern |
|----------|------------------|
| PostgreSQL | `jdbc:postgresql://<host>:<port>/<database>?sslmode=require` |
| Lakebase | `jdbc:postgresql://ep-<name>-<id>.database.<region>.cloud.databricks.com/<database>?sslmode=require` |
| MySQL | `jdbc:mysql://<host>:<port>/<database>` |
| SQL Server | `jdbc:sqlserver://<host>:<port>;databaseName=<database>` |
| Oracle | `jdbc:oracle:thin:@<host>:<port>/<service_name>` |

## Ingestion type rules

### `incremental`

Only new/changed rows are read each run. Requires **one** of these change-detection strategies:

| Strategy | Columns required | When to use |
|----------|-----------------|-------------|
| **Timestamp watermark** | `key_columns` + `watermark_column`, `epic_csa_enabled = false` | Source has a reliable `updated_at` / `modified_date` column |
| **Epic CSA** | `key_columns`, `epic_csa_enabled = true` | Source supports Epic Change Sequence Auditing (no watermark column needed) |

### `full`

The entire source table is read each run.

| Variant | Columns required | When to use |
|---------|-----------------|-------------|
| **Simple overwrite** | None beyond required fields, `auto_cdc_from_snapshot = false` | Target is fully replaced each run, no SCD2 history |
| **Snapshot CDC** | `key_columns` + `staging_table_fqn`, `auto_cdc_from_snapshot = true` | Derive inserts/updates/deletes by diffing consecutive snapshots. Requires a staging table for the diff. |

## Validation rules

The ingestion framework validates every config row at runtime. These are the rules enforced by `ingestion.py` and the app's Zod schema. The agent MUST validate these before inserting:

### Connection validation
1. **`jdbc_url` or `connection_name` must be set** — at least one is required.
2. **`jdbc_secret_scope` and `jdbc_secret_key` must be set together** — setting only one raises an error.
3. **Direct JDBC requires a secret** — when `jdbc_url` is set and `connection_name` is NULL, either `uc_secret_name` or both `jdbc_secret_scope`/`jdbc_secret_key` must be provided.

### Ingestion type validation
4. **`incremental` requires `key_columns`** — cannot be NULL or empty.
5. **`incremental` requires `watermark_column` unless `epic_csa_enabled = true`**.
6. **`epic_csa_enabled` requires `ingestion_type = 'incremental'`** — cannot be true for `full`.
7. **`auto_cdc_from_snapshot` requires `ingestion_type = 'full'`** — cannot be true for `incremental`.
8. **`auto_cdc_from_snapshot` requires `staging_table_fqn` and `key_columns`**.

### Format validation
9. **`target_table_fqn` must be three-part** — matches `X.Y.Z` pattern.
10. **`uc_secret_name` must be three-part** — `catalog.schema.secret_key` format.
11. **`jdbc_url` must not contain passwords** — no `password=` or `pwd=` in the URL.

## Gathering information from the user

When the user asks to add a new source, collect the following information. Ask for what's missing:

### Always needed
1. **Ingestion group** — which group should this table belong to? (new or existing)
2. **Source table name** — the fully qualified name at the source (e.g. `dbo.orders`)
3. **Target table name** — the three-part Delta target (e.g. `catalog.schema.orders`)
4. **Ingestion type** — `incremental` or `full`
5. **Key columns** — primary key column(s) at the source

### Connection (one of)
6a. **UC Connection name** — if using Unity Catalog connection  
6b. **JDBC URL + user + secret** — if using direct JDBC

### Conditional
7. **Watermark column** — needed for incremental without Epic CSA
8. **Epic CSA** — is this an Epic system with CSA support?
9. **Partition column** — for parallel reads (recommended for large tables)

### Defaults (can be omitted)
- `staging_table_fqn`: auto-derived if NULL
- `watermark_threshold_minutes`: 5
- `fetch_size`: 10000
- `num_partitions`: 8
- `enabled`: true
- `auto_cdc_from_snapshot`: false

## Copying connection settings from existing rows

When adding a table to an **existing** ingestion group, query the config table for an existing row to copy shared connection settings:

```sql
SELECT jdbc_url, jdbc_user, jdbc_secret_scope, jdbc_secret_key,
       uc_secret_name, connection_name
FROM <config_table>
WHERE ingestion_group = '<group>' AND enabled = true
LIMIT 1
```

Reuse these values for the new row unless the user specifies different credentials.

## INSERT template

Use this parameterised MERGE (upsert) to safely add or update a config row:

```sql
MERGE INTO <config_table> t
USING (SELECT
  '<ingestion_group>' AS ingestion_group,
  '<source_table_name>' AS source_table_name
) s
ON t.ingestion_group = s.ingestion_group AND t.source_table_name = s.source_table_name
WHEN MATCHED THEN UPDATE SET
  staging_table_fqn = <staging_table_fqn>,
  target_table_fqn = '<target_table_fqn>',
  ingestion_type = '<ingestion_type>',
  key_columns = '<key_columns>',
  watermark_column = <watermark_column>,
  partition_column = <partition_column>,
  predicate_column = <predicate_column>,
  epic_csa_enabled = <epic_csa_enabled>,
  auto_cdc_from_snapshot = <auto_cdc_from_snapshot>,
  jdbc_url = <jdbc_url>,
  jdbc_user = <jdbc_user>,
  jdbc_secret_scope = <jdbc_secret_scope>,
  jdbc_secret_key = <jdbc_secret_key>,
  uc_secret_name = <uc_secret_name>,
  connection_name = <connection_name>,
  watermark_threshold_minutes = <watermark_threshold_minutes>,
  fetch_size = <fetch_size>,
  num_partitions = <num_partitions>,
  update_dttm = current_timestamp(),
  enabled = <enabled>
WHEN NOT MATCHED THEN INSERT (
  ingestion_group, source_table_name, staging_table_fqn, target_table_fqn,
  ingestion_type, key_columns, watermark_column, partition_column, predicate_column,
  epic_csa_enabled, auto_cdc_from_snapshot,
  jdbc_url, jdbc_user, jdbc_secret_scope, jdbc_secret_key, uc_secret_name, connection_name,
  watermark_threshold_minutes, fetch_size, num_partitions, update_dttm, enabled
) VALUES (
  s.ingestion_group, s.source_table_name, <staging_table_fqn>, '<target_table_fqn>',
  '<ingestion_type>', '<key_columns>', <watermark_column>, <partition_column>, <predicate_column>,
  <epic_csa_enabled>, <auto_cdc_from_snapshot>,
  <jdbc_url>, <jdbc_user>, <jdbc_secret_scope>, <jdbc_secret_key>, <uc_secret_name>, <connection_name>,
  <watermark_threshold_minutes>, <fetch_size>, <num_partitions>, current_timestamp(), <enabled>
)
```

Replace `<value>` with the actual value or `NULL` for optional fields. Always quote string values.

## Post-insert checklist

After inserting the config row, remind the user to:

1. **Verify the row** — `SELECT * FROM <config_table> WHERE ingestion_group = '...' AND source_table_name = '...'`
2. **Check CDC pipeline scope** — If the new table is `incremental` with `epic_csa_enabled = true` or has `auto_cdc_from_snapshot = true`, verify the CDC pipeline includes it. The pipeline auto-discovers tables from the config at runtime, so no code changes are needed.
3. **Run a test** — Trigger the ingestion job to confirm the new table ingests successfully.
4. **Validate data** — Use the `watersync-row-diff` skill to verify row counts and content match after the first run.

## Troubleshooting

| Error | Cause | Fix |
|-------|-------|----- |
| `requires jdbc_url or connection_name` | Both fields are NULL | Set one of them |
| `must set both jdbc_secret_scope and jdbc_secret_key` | Only one secret field is set | Set both or use `uc_secret_name` instead |
| `requires a secret for JDBC authentication` | Direct JDBC with no secret configured | Add `uc_secret_name` or `jdbc_secret_scope`/`jdbc_secret_key` |
| `requires watermark_column` | Incremental without CSA and no watermark column | Set `watermark_column` or enable `epic_csa_enabled` |
| `EPIC CSA requires incremental ingestion` | `epic_csa_enabled = true` with `ingestion_type = 'full'` | Change type to `incremental` |
| `Snapshot CDC requires full ingestion` | `auto_cdc_from_snapshot = true` with `incremental` | Change type to `full` |
| `Snapshot CDC requires key columns` | `auto_cdc_from_snapshot = true` without keys | Add `key_columns` |
| `Snapshot CDC requires a staging table` | `auto_cdc_from_snapshot = true` without staging FQN | Add `staging_table_fqn` |
| `UC secret name must be in catalog.schema.secret_name format` | `uc_secret_name` is not three-part | Use `catalog.schema.key` format |
| `Do not put passwords in the JDBC URL` | `jdbc_url` contains `password=` | Remove password from URL, use a secret instead |