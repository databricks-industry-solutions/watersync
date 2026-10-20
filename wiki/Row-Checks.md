# Row Checks

Row checks compare each enabled source table with its target table, so you can see whether the two systems hold the same rows. Run them from the **Row checks** page of the [Control Plane App](Control-Plane-App), or from Python or the CLI.

## Basic and detailed checks

Checks escalate in three tiers, so a full table is never read unless it has to be:

| Tier | Check | Runs |
| --- | --- | --- |
| 1 | **Row counts**: `SELECT COUNT(*)` on the source, the target's active rows in Spark | Basic check: on the job schedule (daily by default), or **Run basic check** |
| 2 | **Chunk aggregates**: per chunk, the row count, `SUM` of every numeric column, and sums of exact integer codes for every string, boolean and date column, computed by the source database and by Spark on the target | Detailed check: **Detailed check** on a table's row |
| 3 | **Row comparison** of only the chunks that tier 2 found different | Detailed check, right after tier 2 |

**Target rows** are the target's active rows. Incremental sources and snapshot CDC sources land in an SCD Type 2 table, so only rows with `__END_AT IS NULL` count. Other full loads overwrite the target, so every row counts.

### Tier 2: chunk aggregates

The table is split into chunks by the first column that fits:

1. `partition_column`, if it is an integer: about 100 equal ranges between its minimum and maximum.
2. The key, if `key_columns` is a single integer column: the same ranges.
3. `watermark_column`, if it is a timestamp or date: one chunk per month.
4. Otherwise the whole table is one chunk.

For each chunk the source database returns one row (the aggregation runs on the source, so only about 100 rows cross the network) and Spark computes the same row on the target:

- `COUNT(*)`.
- `SUM` of each integer and decimal column, cast to `DECIMAL(38, scale)` so it cannot overflow. Float columns are left out: their sums depend on the order rows are added in.
- For each string column, the sum of the first 32 bits of the MD5 of its UTF-8 value: `md5` on PostgreSQL and MySQL, `HASHBYTES('MD5')` after a conversion to UTF-8 on SQL Server (2019 or later), `STANDARD_HASH` on Oracle. Spark computes the same value with `conv(substring(md5(col), 1, 8), 16, 10)`. If the source cannot hash a column (an Oracle LOB, an older SQL Server), the check retries with counts and sums only and says so in the details.
- For each boolean column, the sum of 2 for true, 1 for false and 0 for `NULL`, so a single flip always changes it. Oracle before 23ai has no boolean type, so Oracle booleans (usually `NUMBER(1)`, summed as numbers) are covered by the numeric sums instead.
- For each date column, the sum of its days since 1970-01-01 plus 1,000,000, and 0 for `NULL`. The offset keeps any date after year 1 apart from `NULL`.

Every aggregate is exact, so the source database and Spark always compute the same value for equal data. At most 64 columns of each kind are aggregated. **Timestamps and floats are left out on purpose:** a timestamp's numeric value depends on the time zone the source and Spark read it in, and a sum of floats depends on the order the rows are added in, so both would flag chunks whose data is equal. Timestamps, floats, binary columns and any columns past those 64 are compared only in the chunks tier 3 reads. **A change made only in such a column leaves the chunk's count, sums and hashes unchanged, so tier 2 does not flag the chunk and the check reports `MATCH`.** For example, a corrected `updated_at` with no other change goes unnoticed. A change in any other column, or an added or removed row, changes the chunk's aggregates.

### Tier 3: rows of the differing chunks

Source and target are different engines, so no single `EXCEPT` query can see both. Instead the check reads the differing chunks from the source with a `WHERE` on the chunk column, and Spark compares them with the same chunks of the target. It compares the columns that the source and target have in common, matched case-insensitively. It leaves out the SCD Type 2 columns (`__START_AT`, `__END_AT`) and the columns WaterSync adds (`_ingested_at`, `_source_table`, `_ingestion_group`, `_ingestion_type`, `_IS_DELETED`, `_csa_update_dt`).

- **With `key_columns`**, rows are matched by key. A key on only one side is *missing* or *extra*; a key on both sides with different values is *changed*.
- **Without `key_columns`**, only whole rows can be compared (as with `EXCEPT ALL`), so a changed row counts as one missing and one extra row.

When every chunk's aggregates match, tier 3 reads nothing. Otherwise it reads at most 10 chunks, the ones with the largest count difference first. A table that is one chunk (no chunk column) is read in full, or only its [range](#checking-a-range) when one is set. Month chunks are read with a day of margin on each side and then narrowed in Spark, so a time zone difference between the source and Spark cannot hide rows.

### Example

`dbo.orders` has 1.2 million rows: `order_id` from 1 to 400,000 with about 3 lines each, keyed by `order_id, line_id`, with `partition_column` set to `order_id`. Ingestion missed a changed `quantity` on one line.

1. **Tier 2** splits `order_id` into 100 ranges of 4,000 orders, about 12,000 rows each. The source database returns 100 rows of aggregates, and Spark computes the same 100 rows on the target. They are equal everywhere except in the chunk `order_id 148001–152000`, where `sum(quantity)` differs by 2.
2. **Tier 3** reads only that chunk from the source (`WHERE order_id >= 148001 AND order_id < 152001`), about 12,000 rows instead of 1.2 million, and joins it with the same chunk of the target on `order_id, line_id`.
3. **The result** is `MISMATCH` with 0 missing, 0 extra and 1 changed row. The sample key is `order_id=150412, line_id=2`, and the details list the chunk with `sum(quantity)` as the aggregate that differs.

A delete plus an insert in the same table leaves its row count equal, so the basic check reports `MATCH`. The detailed check still flags the chunks the two rows fall in, because their counts or hashes change.

### Status

A table is `MISMATCH` when the counts differ, tier 3 found a row difference, or a differing chunk was left over by the 10-chunk limit. A chunk whose aggregates differ but whose rows all match does not count. This can happen when a string hash differs only in representation, for example the trailing spaces of a `CHAR` column. Otherwise the table is `MATCH`, or `ERROR` when it could not be checked (the message is kept). The job fails when any table is `ERROR`, but not for a `MISMATCH`.

### Checking a range

A check can be limited to a range of rows, so a large table can be checked one slice at a time, for example its last 30 days. In the app, turn on **Check a range of rows only** in the **Detailed check** dialog. From a job run or the CLI, set `range_from` and/or `range_to`.

- The range is `range_from <= column < range_to`; leave a bound empty for no limit on that side.
- The column is `range_column`, or by default the table's chunk column (see tier 2). A table without a chunk column needs `range_column`, otherwise it is reported as `ERROR`.
- Bounds are a number, a date (`2026-09-01`), a timestamp (`2026-09-01 12:00:00`) or a number of days back (`-30d`, from midnight of the job's clock, which is UTC on serverless and on job clusters unless they set another time zone). They are parsed for the column's type and never inserted into SQL as text.
- The range applies to every tier, on both sides: the counts, the chunk aggregates and the rows tier 3 reads. With integer chunks, the 100 ranges are spread over the checked rows only, so a narrow range gets narrow chunks.
- Each result keeps the range in `checked_range`, and the app shows it under the result, so a partial check is never mistaken for the whole table.

A row whose range column changed in the source but not yet in the target (for example a `modified_at` that moved into the range) is on one side of the filter only, so it shows up as missing or extra.

### Reading a mismatch

| Symptom | Likely cause |
| --- | --- |
| Target has fewer rows, the gap is recent | Rows changed within `watermark_threshold_minutes`, or since the last ingestion run, are not ingested yet |
| Target has more rows | Rows deleted in the source: timestamp watermarks cannot see hard deletes (EPIC CSA can) |
| Counts match but rows changed | An update did not move the watermark column forward, so it was never picked up |
| Many rows missing after a reload | The staging or CDC pipeline did not finish; check the ingestion job's last run |

A [full refresh](CDC-Pipeline-and-Full-Refresh#full-refresh) of the table resynchronizes it.

## The Row checks page

The page of the [Control Plane App](Control-Plane-App) shows, from top to bottom:

- **Row check jobs.** Every row check job of the location and of its groups: what it checks (basic or detailed, every group or one), its schedule, its compute and its latest run, with **Edit** and **Delete**. **New row check job** creates one (see [Row check jobs](#row-check-jobs)).
- **Filter bar.** A group, up to 8 picked tables (one per chart color) and a table-name search, applied to every section below. The filtering runs on the SQL warehouse, so the page works the same with thousands of tables.
- **Summary.** One line with the latest basic check of the filtered tables: how many match, differ or failed, the last check and how many row check jobs are scheduled.
- **Status of every table.** One square per enabled table, grouped by ingestion group and coloured by its worst latest result, basic or detailed: error, mismatch, match or not checked. Click a square to show only that table, with any status, and jump to its row; click it again to show every table. The map follows the group and name filters but not picked tables, so every table stays visible while one is picked. Above 300 tables the squares shrink and the map scrolls; it shows at most 5,000 tables.
- **Daily row counts.** For the last 30 days (UTC), from each day's last whole-table basic check: the rows ingested per day (the target's growth since the previous checked day, net of deletes; on the first checked day, since that day's first check) and the target − source difference. Unless tables are picked, the 7 series with the most rows ingested are shown by name and the others add up into "Other".
- **Recent check runs** of every row check job, newest first.
- **Groups or tables**, problems first, 100 at a time with **Show more**, narrowed further by status (needs attention, match, not checked).

What the chart and the list show follows the selection:

| Selection | Chart | List |
| --- | --- | --- |
| Nothing | One series per group, its tables added up | One row per group: table count, results by status, rows, rows out of sync, last check. Click a group to open it |
| A group or a name search | One series per table | Its tables, with an **All groups** link back |
| Picked tables | Those tables | Those tables |

A series or row made of several tables (a group, or "Other") adds up *absolute* differences, so tables off in opposite directions cannot cancel out; the chart calls it **Rows out of sync** at group level.

**Detailed check** on a table's row starts a detailed check of the whole table or of a [range](#checking-a-range); the details button shows the latest results, the differing chunks and sample keys.

## Results table

Each check appends one row per table to `<catalog>.<schema>._watersync_row_checks`. The table is created by the app's migrations, by `UnityCatalogSetup.create_all()`, or by the first check. Like every table whose name starts with `_`, it is managed by WaterSync: read it, but do not modify it.

| Column | Contents |
| --- | --- |
| `check_id`, `job_run_id` | Result id and the job run that produced it |
| `check_mode`, `checked_at` | `basic` or `detailed`, and when the table was checked |
| `ingestion_group`, `source_table_name`, `target_table_fqn` | The config row that was checked |
| `source_count`, `target_count`, `count_diff` | Row counts; `count_diff` is `target_count - source_count` |
| `missing_in_target`, `extra_in_target`, `mismatched_rows` | Detailed checks only, over the chunks tier 3 compared. `mismatched_rows` is NULL without key columns |
| `compared_columns`, `sample_keys` | Detailed checks only. `sample_keys` is JSON with up to 10 keys per difference type |
| `status`, `error`, `duration_ms` | `MATCH`, `MISMATCH` or `ERROR`, the failure message, and the time spent on the table |
| `chunk_column`, `chunk_count` | Detailed checks only. The column the table was chunked by (NULL for one chunk) and the number of chunks |
| `mismatched_chunks`, `drilled_chunks` | Detailed checks only. Chunks whose aggregates differ, and how many of them tier 3 compared row by row |
| `checked_range` | The range checked, for example `modified_at >= 2026-09-01 00:00:00`; NULL for the whole table |
| `chunk_details` | Detailed checks only. JSON with the summed and hashed columns and up to 50 differing chunks: their counts, which aggregates differ and what tier 3 found |

The app shows the latest basic and the latest detailed result of each table. Query the table for the history.

## Row check jobs

Row check jobs are created on the **Row checks** page, with **New row check job**, separately from the [ingestion jobs](Control-Plane-App#jobs-created-by-the-app). Each job runs one check over every enabled table of the location or of one ingestion group, so there is at most one job per scope and check:

| Job | Runs | Default schedule |
| --- | --- | --- |
| `[<catalog>.<schema>] Row Checks` | The basic check of every enabled table of the location | `0 0 6 * * ?` (daily, 06:00 UTC) |
| `[<catalog>.<schema>] Row Checks (detailed)` | The detailed check of every enabled table of the location | `0 0 7 ? * SUN` (weekly, Sunday 07:00 UTC) |
| `[<group>] Row Check (basic)` | The basic check of every enabled table of the group | `0 0 6 * * ?` |
| `[<group>] Row Check (detailed)` | The detailed check of every enabled table of the group | `0 0 7 ? * SUN` |

A job runs `notebooks/Task - Row Check` from your Git repository and installs the package from the same branch, like the ingestion jobs. Its dialog has these tabs:

| Tab | Settings |
| --- | --- |
| **Check** | Which tables (every group, or one group) and which check (basic or detailed). A detailed check can be limited to rows from a [range](#checking-a-range) start such as `-30d`. Both are fixed once the job exists, because they name it |
| **Git source & execution** | GitHub repository and branch, and how many tables are checked at the same time |
| **Job Compute** | **Serverless** (default) with a performance mode, or a **shared autoscaling job cluster** (Databricks Runtime, driver and worker node types, min/max workers) for workspaces without serverless jobs |
| **Schedule** | Quartz cron expression, time zone, active or paused; or manual runs only |
| **Run as** | The job's owner (the app service principal, default), a user, or a service principal |

**Edit** opens the dialog with the job's current settings, read back from the Databricks API. Creating a job for a scope and check that already has one updates that job. **Delete** deletes the job and its run history; the check results stay. Detailed checks aggregate the whole target table in Spark, so size a job cluster like the ingestion one.

| Job parameter | Default | Meaning |
| --- | --- | --- |
| `check_mode` | the job's check | `basic` or `detailed` |
| `ingestion_group` | the job's group | Check one group; empty checks every group |
| `source_table_name` | empty | Check one table; empty checks every table |
| `max_parallel_tables` | `4` | Tables checked at the same time (1–32) |
| `range_column`, `range_from`, `range_to` | empty, except a detailed job's `range_from` | Check only rows with `range_from <= range_column < range_to` (see [Checking a range](#checking-a-range)) |
| `configuration_fqn`, `watermark_fqn`, `row_check_fqn` | the location's tables | Where configs are read and results written |

**Run basic check** and **Detailed check** on the page start a run of an existing job with every parameter set, so any row check job can run them: the location's jobs first, otherwise a job of the table's group. **Run basic check** checks the group the page is filtered to, or every table. Without a location job, checks can only be started for groups that have one. Each job allows 4 concurrent runs, so a check can start while a scheduled one is running. While a run is going the page polls every 15 seconds and reloads the results when it ends.

Whichever identity the job runs as needs the same source access as the ingestion jobs (UC connections, secret scopes or UC secrets) and `SELECT` on the target tables. Running as a user or another service principal has the same requirements for the app as for an ingestion job.

[Export](Import-and-Export) reads the row check jobs back from the Databricks API, and an import with **Create or update jobs** creates or updates them.

## From Python or the CLI

```python
from watersync.models import JdbcRuntimeSettings
from watersync.row_check import RowCheckRunner

runtime = JdbcRuntimeSettings(
    configuration_fqn="main.watersync.jdbc_ingestion_config",
    watermark_fqn="main.watersync.jdbc_ingestion_watermark",
    ingestion_group="sales",            # optional
    source_table_name="public.orders",  # optional
)
results = RowCheckRunner(
    spark, runtime, results_fqn="main.watersync._watersync_row_checks", check_mode="detailed"
).run()
```

```bash
watersync-row-check \
  --configuration-fqn main.watersync.jdbc_ingestion_config \
  --watermark-fqn main.watersync.jdbc_ingestion_watermark \
  --row-check-fqn main.watersync._watersync_row_checks \
  --check-mode detailed \
  --source-table-name public.orders \
  --range-from -30d
```

`RowCheckRunner` takes the same range as `range_column`, `range_from` and `range_to`.
