# CDC Pipeline and Full Refresh

## CDC SCD2 Pipeline

`pipeline_bootstrap.py` is the entry point of the Lakeflow Spark Declarative Pipeline, which runs on serverless or, when created from the app with classic SDP compute, on a classic cluster (see [Jobs created by the app](Control-Plane-App.md#jobs-created-by-the-app)). It reads `pipeline.configuration_fqn` and `pipeline.ingestion_group` from the pipeline configuration and, for every enabled row, creates a Silver streaming table at `target_table_fqn` clustered by `key_columns`, fed from the Bronze staging table:

| Source type | Flow |
|---|---|
| Incremental, timestamp | `create_auto_cdc_flow` from staging, sequenced by `watermark_column` |
| Incremental, EPIC CSA | `create_auto_cdc_flow` from staging, sequenced by `_csa_update_dt`, deletes where `_IS_DELETED` |
| Full + `auto_cdc_from_snapshot` | `create_auto_cdc_from_snapshot_flow` over staging table versions |

All flows are stored as SCD Type 2. The `_ingested_at` column (the time WaterSync loaded the record into Databricks) is kept in every Silver table and in plain full-load target tables. It is not tracked as history, so a refresh that changes only `_ingested_at` does not create a new version. The other WaterSync metadata columns (`_source_table`, `_ingestion_group`, `_ingestion_type`) and CSA control columns are excluded from the history.

## Full Refresh

- **Whole job** — run the job with `full_refresh=true`. Every table is read in full and staging is overwritten. Triggering a full refresh from the Control Plane app also fully refreshes the CDC pipeline.
- **Single table** — schedule a full refresh on the app's **Watermarks** page. This sets the table's watermark status to `FULL_REFRESH`. On the next run the worker reloads that table, and the `selective_pipeline_refresh` task fully refreshes only its pipeline target.

Use a full refresh for an EPIC CSA table after missing changes, for example when the job was down longer than the CSA retention window.

Watermark statuses: `SUCCESS`, `SKIPPED`, `FAILED`, `FULL_REFRESH`.
