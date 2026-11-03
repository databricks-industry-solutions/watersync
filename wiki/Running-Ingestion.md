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

### Structured logs with ZeroBus

Every `watersync.*` logger can also stream its records to a Delta table through ZeroBus Ingest, so a run can be inspected with SQL after it ends. This is opt-in: the generated jobs and notebooks do not turn it on. Install the extra (`%pip install "watersync[zerobus]"`) and wrap the run:

```python
from watersync.utils import setup_watersync_logging

handler = setup_watersync_logging(
    server_endpoint="https://<id>.zerobus.<region>.cloud.databricks.com",
    workspace_url="https://<workspace>.cloud.databricks.com",
    table_name="main.watersync.watersync_logs",  # created by UnityCatalogSetup.create_all()
    client_id=dbutils.secrets.get(scope="watersync", key="zb_client_id"),
    client_secret=dbutils.secrets.get(scope="watersync", key="zb_client_secret"),
)
try:
    JdbcIngestionOrchestrator(spark=spark, runtime=runtime).run_selected_ingestion()
finally:
    handler.close()  # flushes and closes the stream
```

- **Authentication**, first match wins:
  - `client_id` and `client_secret` of a service principal (OAuth M2M), which works on serverless and classic compute and is recommended;
  - `token_provider`, a function returning a bearer token on each call, which works on classic interactive clusters;
  - neither, which falls back to the runtime's OAuth credentials, for jobs that run as a service principal.
- **Sending is fire-and-forget,** so logging never blocks ingestion. `console=True` (the default) also prints records to the notebook. Records are filtered by `level` (default `INFO`), and `run_id` is generated when not given (read it from `handler.run_id`).
- **Table layout:** each record is one row of `run_id STRING`, `log_timestamp STRING` (UTC, ISO 8601) and `payload VARIANT`. `payload` holds `level`, `logger_name`, `message` (`text`, `module`, `func`, `lineno`, `exc`), and `ingestion_group` and `source_table` when the record has them. New fields need no schema change.

```sql
-- Errors of one run
SELECT log_timestamp,
       payload:source_table::STRING AS source_table,
       payload:message:text::STRING AS message
FROM main.watersync.watersync_logs
WHERE run_id = '<run_id>' AND payload:level::STRING = 'ERROR'
ORDER BY log_timestamp;
```

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
