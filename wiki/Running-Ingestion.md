# Running Ingestion

### From a notebook

```python
from watersync.ingestion import JdbcIngestionOrchestrator
from watersync.models import JdbcRuntimeSettings

runtime = JdbcRuntimeSettings(
    configuration_fqn="main.watersync.jdbc_ingestion_config",
    watermark_fqn="main.watersync.jdbc_ingestion_watermark",
    ingestion_group="epic",
    source_table_name="",   # empty = all enabled tables in the group
    full_refresh=False,
)

orchestrator = JdbcIngestionOrchestrator(spark=spark, runtime=runtime)
results = orchestrator.run_selected_ingestion()
```

Connection settings come from each config row, not from `JdbcRuntimeSettings`. The orchestrator raises after processing every table if any of them failed.

`notebooks/Watersync Notebook Runner` wraps the same calls behind widgets (`plan_configs`, `run_ingestion`, `create_job`, `setup_uc`, `setup_lakebase`).

### Via the CLI

```bash
watersync-run-ingestion \
  --configuration-fqn main.watersync.jdbc_ingestion_config \
  --watermark-fqn main.watersync.jdbc_ingestion_watermark \
  --ingestion-group epic \
  --source-table-name dbo.PAT_ENC   # optional
```

### Fan-out planning

The planner returns one for-each input per enabled table:

```python
from watersync.config_planner import IngestionConfigPlanner

planner = IngestionConfigPlanner(spark=spark, runtime=runtime)
inputs = planner.build_for_each_inputs(ingestion_group="epic")
# [{"ingestion_group": "epic", "source_table_name": "dbo.CLARITY_ADT"}, ...]
```

Or with task-value publishing inside a Lakeflow Job task:

```bash
watersync-plan-configs \
  --configuration-fqn main.watersync.jdbc_ingestion_config \
  --watermark-fqn main.watersync.jdbc_ingestion_watermark \
  --ingestion-group epic \
  --publish-task-value true
```

## Worker Types

### `TimestampWatermarkIngestionWorker`

Default worker, selected when `epic_csa_enabled = false`.

- **Incremental** — runs an existence check, then reads rows where `watermark_column > last_watermark` and `<= now() - watermark_threshold_minutes`, appends them to staging, and stores the max watermark in staging
- **Full** — reads the whole table and overwrites `target_table_fqn` (or staging when `auto_cdc_from_snapshot` is set). No watermark is tracked, so `watermark_column` is not used
- Supports a numeric `partition_column` or string `predicate_column` for parallel reads

### `EpicCsaIngestionWorker`

Selected when `epic_csa_enabled = true`. Uses EPIC Clarity CSA (Change Sync Administration) tables instead of a timestamp column. CSA tables must be enabled per table with your Epic team, and the config row's connection must be able to read the `EPIC_UTIL` schema.

- The CSA table is derived from `source_table_name` as `epic_util.csa_<table>` (e.g. `dbo.PAT_ENC` → `epic_util.csa_pat_enc`)
- The watermark is the CSA `_TIMESTAMP_EXTRACT_KEY` (cast to BIGINT)
- **First run** (no stored watermark) — full read of the source table
- **Later runs** — read `MAX(_TIMESTAMP_EXTRACT_KEY)`; skip if it has not advanced, otherwise read CSA rows in `(last, max]` `LEFT JOIN`ed to the source table on `key_columns`, and append them to staging with `_IS_DELETED` and `_csa_update_dt` (from `_UPDATE_DT`)
- Deleted rows keep their keys from the CSA table, so the CDC pipeline can apply them as deletes
- `key_columns` is required; `watermark_column` is ignored

### `EpicStoredProcedureIngestionWorker`

Selected when `epic_csa_enabled = true` and `epic_change_method = 'stored_procedure'`. Instead of reading the CSA tables, it calls Epic's changed-rows stored procedure, named in `epic_change_procedure`. Epic keeps that procedure's interface stable across upgrades; the CSA tables behind it are not meant to be queried directly. The procedure has been seen as `ESP_CR_ALTERED_ROWS` (2020 header) and `ESP_GET_CHANGED_ROWS`, so check the name in your Clarity database.

- The procedure is called with four positional arguments: the table part of `source_table_name`, the window start, the window end, and a temp table name. That order matches Epic's `@i_TableName`, `@i_StartDate`, `@i_EndDate`, `@i_TempTableName`, and works with mocks that name the parameters differently. The procedure fills the temp table with the key columns and `CR_UPD_DATE` for rows changed in `[start, end)`; the worker keeps the latest change per key
- On SQL Server the call runs through the Spark JDBC `prepareQuery` option, so the procedure and the `SELECT` from its temp table share one connection. The temp table is a `##` global temp table, dropped when that connection closes. `remote_query` can't be used because it doesn't run procedures
- On Postgres (the Lakebase mock) the JDBC driver returns no rows for a `SELECT` that follows a `CALL`, so the worker reads `<epic_change_procedure>_select(...)` instead: a set-returning wrapper that calls the procedure and returns the temp table's rows. See [Lakebase Test Setup](Lakebase-Test-Setup)
- The temp table is `LEFT JOIN`ed to the source table on `key_columns` and appended to staging with `_csa_update_dt` (from `CR_UPD_DATE`). The procedure returns no delete flag, so `_IS_DELETED` is true when a changed key has no row left in the source table
- The watermark is the end of the last window. Each run's window ends at the source server's time minus `watermark_threshold_minutes`, and is skipped when it is empty
- **First run** (no stored watermark, or one left by the CSA worker) and **full refresh** — full read of the source table. The window end is captured before the read, so changes made during the load are read again by the next run
- Reads use one JDBC partition, because each partition would run the procedure again. `partition_column` and `predicate_column` only apply to full reads
- Supported for SQL Server and Postgres (the Lakebase mock) sources; `key_columns` and `epic_change_procedure` are required
