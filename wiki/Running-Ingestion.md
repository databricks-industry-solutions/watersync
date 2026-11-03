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

Another way to load EPIC tables incrementally: instead of reading the CSA tables, it calls Epic's stored procedure for changed rows (`EPIC_UTIL.ESP_CR_ALTERED_ROWS`) and reads the changed rows from the source table. Selected when `epic_csa_enabled = true` and `epic_change_method = 'stored_procedure'`; `key_columns` is required, and `epic_change_procedure` can name a different procedure. Deleted rows are staged with `_IS_DELETED`, as with the CSA worker, so the same CDC pipeline applies the changes.
