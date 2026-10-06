# watersync

Python package and Databricks App for metadata-driven JDBC ingestion into Delta, with timestamp-watermark and EPIC Clarity CSA change tracking, SCD Type 2 history via Lakeflow Spark Declarative Pipelines, and Lakeflow Job provisioning.

---

## Architecture

```
jdbc_ingestion_config (Delta)          jdbc_ingestion_watermark (Delta)
        │                                          │
        ▼                                          │
IngestionConfigPlanner                             │
  └─ one for-each input per enabled table          │
        │                                          │
        ▼                                          │
JdbcIngestionOrchestrator                          │
  └─ one worker per config row                     │
        │                                          │
        ├─ TimestampWatermarkIngestionWorker ───────┤ read/write watermark state
        └─ EpicCsaIngestionWorker ─────────────────┘
                │
                ▼
        Bronze: staging Delta tables  (staging_table_fqn, append-only changes)
                │
                ▼
        CDC SCD2 pipeline  (cdc_pipeline.py / pipeline_bootstrap.py)
                │
                ▼
        Silver: SCD Type 2 history tables  (target_table_fqn)
```

Staging tables are the Bronze layer: raw changed rows exactly as read from the source. The `AUTO CDC` targets are the Silver layer: deduplicated SCD Type 2 history per business key. Gold models are built downstream. Full loads without `auto_cdc_from_snapshot` skip staging and the pipeline: they overwrite `target_table_fqn` directly.

A Lakeflow Job runs one ingestion group:

1. **`ingestion_configs`** — `notebooks/Task - Plan Configs` publishes the `table_configs` and `full_refresh_targets` task values
2. **`ingestion_worker`** — for-each task running `notebooks/Task - Run Ingestion` once per table (concurrency set by `foreach_concurrency`)
3. **`selective_pipeline_refresh`** — `src/watersync/utils/selective_pipeline_refresh` fully refreshes only the pipeline tables flagged `FULL_REFRESH`
4. **`cdc_scd2_pipeline`** — pipeline task that applies Bronze staged changes to the Silver SCD Type 2 targets

Steps 3 and 4 are added only when the group has incremental or snapshot-CDC sources. Jobs created by the Python `IngestionJobProvisioner` do not include step 3.

---

## Control Plane App

`app/watersync-control-plane` is a Databricks App (AppKit, React, TypeScript) for running WaterSync without writing SQL:

- **Configuration** — discover source tables over a UC connection or JDBC URL (SQL Server, Oracle, PostgreSQL, MySQL), infer key and watermark columns from primary-key constraints, and save config rows
- **Watermarks** — inspect per-table state and schedule a full refresh for a single table
- **Jobs** — create or update the Lakeflow Job and CDC pipeline for a group (serverless or classic compute), set a schedule, and trigger runs

When you select a catalog/schema, the app creates any missing metadata tables and columns there (`server/migrations`).

Deploy it with the bundle in the repo root:

```bash
databricks bundle deploy -t dev
```

The app uses the SQL warehouse named `Shared Unity Catalog Serverless`. Pass `--var warehouse_id=<sql-warehouse-id>` to use another one. You can also deploy from the Databricks UI: clone the repo into your workspace, open the Asset Bundle editor, and click **Deploy**.

Jobs created by the app install the package from Git (`watersync@git+<git_url>.git@<git_branch>`) and run the notebooks from the same repository. See `app/watersync-control-plane/README.md` for local development.

---

## Installation

Install in editable mode from a notebook:

```python
%pip install -e /Workspace/Users/<user>/watersync
```

Or build a wheel for production jobs:

```bash
pip install build
python -m build --wheel
# upload dist/watersync-0.1.1-py3-none-any.whl to a UC volume
```

Optional extras: `watersync[lakebase]` (Lakebase test setup) and `watersync[zerobus]` (ZeroBus log handler).

---

## One-Time Setup

Create the Unity Catalog schema and the metadata tables:

```python
from watersync.utils import UnityCatalogSetup

setup = UnityCatalogSetup(spark, catalog="main", schema="watersync")
setup.create_all()
# Tables created:
#   main.watersync.jdbc_ingestion_config
#   main.watersync.jdbc_ingestion_watermark
#   main.watersync.watersync_logs
```

Or via the CLI:

```bash
watersync-setup-uc --catalog main --schema watersync
```

Setup is idempotent: existing tables and watermark state are kept, and missing config columns are added. Pass `--truncate-existing` to truncate every table in the schema except `jdbc_ingestion_config`, which resets watermark state.

---

## Config Table

`jdbc_ingestion_config` drives all ingestion. Each row represents one source table.

| Column | Type | Required | Description |
|---|---|---|---|
| `ingestion_group` | STRING | yes | Logical group — all rows with the same group run in one job |
| `source_table_name` | STRING | yes | Source table as `schema.table` (e.g. `dbo.PAT_ENC`) |
| `staging_table_fqn` | STRING | no | Bronze staging table in `catalog.schema.table` form; defaults to `<config_catalog>.<config_schema>.staging_<table>` |
| `target_table_fqn` | STRING | yes | Final destination in `catalog.schema.table` form. Full loads write here directly unless `auto_cdc_from_snapshot` is set; otherwise the CDC pipeline publishes Silver SCD Type 2 history here |
| `ingestion_type` | STRING | no | `incremental` (default) or `full` |
| `key_columns` | STRING | yes* | Comma-separated business keys (*required for CDC and EPIC CSA) |
| `watermark_column` | STRING | yes* | Timestamp column for incremental loads (*not used when `epic_csa_enabled`) |
| `partition_column` | STRING | no | Numeric column for parallel JDBC partitioning |
| `predicate_column` | STRING | no | String column for predicate-based parallel reads (requires `jdbc_url`) |
| `epic_csa_enabled` | BOOLEAN | no | Use the EPIC CSA worker (incremental only) |
| `auto_cdc_from_snapshot` | BOOLEAN | no | For `full` sources, stage each snapshot and maintain SCD Type 2 history in the target |
| `jdbc_url` | STRING | one of* | JDBC URL to the source (*set `jdbc_url` or `connection_name`; at least one is required) |
| `jdbc_user` | STRING | no | JDBC username |
| `jdbc_secret_scope` / `jdbc_secret_key` | STRING | no | Databricks secret holding the JDBC password (set both or neither) |
| `uc_secret_name` | STRING | no | Unity Catalog secret as `catalog.schema.secret_name`, used instead of a scope/key |
| `connection_name` | STRING | one of* | Unity Catalog connection to the source, used instead of `jdbc_url` |
| `watermark_threshold_minutes` | INT | no | Incremental cutoff lag behind `now()` (default 5) |
| `fetch_size` | INT | no | JDBC fetch size (default 10000) |
| `num_partitions` | INT | no | JDBC parallelism (default 8) |
| `update_dttm` | TIMESTAMP | no | Last update of the config row |
| `enabled` | BOOLEAN | yes | Set `false` to skip the row without deleting it |

Every row needs a way to reach the source system: set either `connection_name` (a Unity Catalog connection) or `jdbc_url`. Rows with neither fail validation. A row using `jdbc_url` must also reference a password secret (`jdbc_secret_scope`/`jdbc_secret_key` or `uc_secret_name`). Never store plaintext passwords.

Lakeflow Jobs take four parameters: `configuration_fqn`, `watermark_fqn`, `ingestion_group`, and `full_refresh` (default `false`).

### Adding config rows

Use the Control Plane app's **Configuration** page, the `watersync-add-config` agent skill in `.agents/skills`, or SQL:

```sql
INSERT INTO main.watersync.jdbc_ingestion_config
  (ingestion_group, source_table_name, staging_table_fqn, target_table_fqn,
   ingestion_type, key_columns, watermark_column, epic_csa_enabled,
   auto_cdc_from_snapshot, connection_name, watermark_threshold_minutes,
   fetch_size, num_partitions, update_dttm, enabled)
VALUES
  -- EPIC Clarity table tracked through EPIC_UTIL.CSA_PAT_ENC
  ('epic', 'dbo.PAT_ENC', 'main.bronze_clarity.staging_pat_enc', 'main.silver_clarity.pat_enc',
   'incremental', 'PAT_ENC_CSN_ID', NULL, true,
   false, 'clarity_conn', 5, 10000, 8, current_timestamp(), true),
  -- Timestamp-watermark table
  ('epic', 'dbo.CLARITY_ADT', 'main.bronze_clarity.staging_clarity_adt', 'main.silver_clarity.clarity_adt',
   'incremental', 'EVENT_ID', 'UPDATE_DATE', false,
   false, 'clarity_conn', 5, 10000, 8, current_timestamp(), true);
```

---

## Running Ingestion

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

---

## JDBC Connection Options

Connections are configured per config row, and every row must use one of these two modes:

- **JDBC URL** — set `jdbc_url`, `jdbc_user`, and either `jdbc_secret_scope` + `jdbc_secret_key` or `uc_secret_name`. The SQL dialect is detected from the URL.
- **Unity Catalog connection** — set `connection_name` and leave `jdbc_url` empty. The dialect comes from the connection type.

Supported dialects: SQL Server, Oracle, and PostgreSQL.

---

## Worker Types

### `TimestampWatermarkIngestionWorker`

Default worker, selected when `epic_csa_enabled = false`.

- **Incremental** — runs an existence check, then reads rows where `watermark_column > last_watermark` and `<= now() - watermark_threshold_minutes`, appends them to staging, and stores the max watermark in staging
- **Full** — reads the whole table and overwrites `target_table_fqn` (or staging when `auto_cdc_from_snapshot` is set)
- Supports a numeric `partition_column` or string `predicate_column` for parallel reads

### `EpicCsaIngestionWorker`

Selected when `epic_csa_enabled = true`. Uses EPIC Clarity CSA (Change Sync Administration) tables instead of a timestamp column. CSA tables must be enabled per table with your Epic team, and the config row's connection must be able to read the `EPIC_UTIL` schema.

- The CSA table is derived from `source_table_name` as `epic_util.csa_<table>` (e.g. `dbo.PAT_ENC` → `epic_util.csa_pat_enc`)
- The watermark is the CSA `_TIMESTAMP_EXTRACT_KEY` (cast to BIGINT)
- **First run** (no stored watermark) — full read of the source table
- **Later runs** — read `MAX(_TIMESTAMP_EXTRACT_KEY)`; skip if it has not advanced, otherwise read CSA rows in `(last, max]` `LEFT JOIN`ed to the source table on `key_columns`, and append them to staging with `_IS_DELETED` and `_csa_update_dt` (from `_UPDATE_DT`)
- Deleted rows keep their keys from the CSA table, so the CDC pipeline can apply them as deletes
- `key_columns` is required; `watermark_column` is ignored

---

## CDC SCD2 Pipeline

`pipeline_bootstrap.py` is the entry point of a serverless Lakeflow Spark Declarative Pipeline. It reads `pipeline.configuration_fqn` and `pipeline.ingestion_group` from the pipeline configuration and, for every enabled row, creates a Silver streaming table at `target_table_fqn` clustered by `key_columns`, fed from the Bronze staging table:

| Source type | Flow |
|---|---|
| Incremental, timestamp | `create_auto_cdc_flow` from staging, sequenced by `watermark_column` |
| Incremental, EPIC CSA | `create_auto_cdc_flow` from staging, sequenced by `_csa_update_dt`, deletes where `_IS_DELETED` |
| Full + `auto_cdc_from_snapshot` | `create_auto_cdc_from_snapshot_flow` over staging table versions |

All flows are stored as SCD Type 2. WaterSync metadata columns (`_ingested_at`, `_source_table`, `_ingestion_group`, `_ingestion_type`) and CSA control columns are excluded from the history.

---

## Full Refresh

- **Whole job** — run the job with `full_refresh=true`. Every table is read in full and staging is overwritten. Triggering a full refresh from the Control Plane app also fully refreshes the CDC pipeline.
- **Single table** — schedule a full refresh on the app's **Watermarks** page. This sets the table's watermark status to `FULL_REFRESH`. On the next run the worker reloads that table, and the `selective_pipeline_refresh` task fully refreshes only its pipeline target.

Use a full refresh for an EPIC CSA table after missing changes, for example when the job was down longer than the CSA retention window.

Watermark statuses: `SUCCESS`, `SKIPPED`, `FAILED`, `FULL_REFRESH`.

---

## Job Provisioning

The Control Plane app's **Jobs** page is the recommended way to create jobs. From Python:

```python
from watersync.models import JobProvisioningSettings
from watersync.utils import IngestionJobProvisioner

settings = JobProvisioningSettings(
    ingestion_group="epic",
    configuration_fqn="main.watersync.jdbc_ingestion_config",
    watermark_fqn="main.watersync.jdbc_ingestion_watermark",
    planner_notebook_path="notebooks/Task - Plan Configs",
    worker_notebook_path="notebooks/Task - Run Ingestion",
    wheel_uri="/Volumes/main/watersync/wheels/watersync-0.1.1-py3-none-any.whl",
    foreach_concurrency=4,
    cdc_pipeline_file_path="/Workspace/Users/<user>/watersync/src/watersync/pipeline_bootstrap.py",
    # cdc_pipeline_id="<pipeline-uuid>",   # reuse an existing pipeline instead
    git_url="https://github.com/databricks-industry-solutions/watersync",   # empty = workspace notebook paths
    git_branch="main",
)

result = IngestionJobProvisioner().create_or_update_job(settings)
# {"job_id": ..., "pipeline_id": ..., "job_name": "[epic] Ingestion Pipeline"}
```

Or via the CLI:

```bash
watersync-create-job \
  --configuration-fqn main.watersync.jdbc_ingestion_config \
  --watermark-fqn main.watersync.jdbc_ingestion_watermark \
  --ingestion-group epic \
  --wheel-uri "/Volumes/main/watersync/wheels/watersync-0.1.1-py3-none-any.whl" \
  --planner-notebook-path "/Workspace/Users/<user>/watersync/notebooks/Task - Plan Configs" \
  --worker-notebook-path "/Workspace/Users/<user>/watersync/notebooks/Task - Run Ingestion" \
  --foreach-concurrency 4
```

The CLI always uses workspace notebook paths, and `--cdc-pipeline-file-path` defaults to `./src/watersync/pipeline_bootstrap.py`.

---

## Lakebase Test Setup

Provision a Lakebase Postgres project seeded with `customers`, `products`, and `orders` tables, plus matching `epic_util.csa_*` tables kept up to date by triggers, to test both workers without a real source system:

```python
from watersync.utils import LakebaseTestDatabaseSetup

setup = LakebaseTestDatabaseSetup(
    project_id="watersync-jdbc-test",
    project_display_name="WaterSync JDBC Test DB",
)
setup.ensure_project()
setup.create_standard_tables()
setup.seed_standard_data(customer_count=200, product_count=100, order_count=500)

jdbc_settings = setup.jdbc_settings()      # JDBC URL and credentials for config rows
csa_rows = setup.epic_csa_config_rows()    # suggested EPIC CSA config rows
```

Simulate ongoing changes:

```python
setup.simulate_updates()
setup.simulate_epic_csa_changes(delete_rows=2)
```

Or via the CLI:

```bash
watersync-setup-lakebase \
  --project-id watersync-jdbc-test \
  --customer-count 200 \
  --order-count 500 \
  --simulate-updates
```

---

## CI

`.github/workflows/databricks-ci.yml` validates and deploys the bundle on every pull request. It then runs the `demo_workflow` job (`resources/watersync_demo_workflow.yml`), a serverless smoke test that installs the package from the deployed bundle and runs `tests/` (`tests/run_in_databricks.py`). Finally, it destroys the deployment. Run the same tests locally with `PYTHONPATH=src pytest tests`.

`.github/workflows/publish.yaml` publishes this README and the notebooks under `notebooks/` to GitHub Pages.

---

## Project Layout

```
watersync/
├── databricks.yml                     # bundle: Control Plane app + demo_workflow
├── resources/
│   ├── watersync_app.yml
│   └── watersync_demo_workflow.yml    # CI smoke-test job
├── .github/                           # CI and GitHub Pages workflows
├── app/watersync-control-plane/       # Databricks App (AppKit)
├── docs/slides.html                   # overview deck
├── .agents/skills/                    # agent skills (add config, row diff)
├── notebooks/
│   ├── Task - Plan Configs.py         # job task: planner
│   ├── Task - Run Ingestion.py        # job task: for-each worker
│   └── Watersync Notebook Runner.py   # interactive runner
├── tests/                             # unit tests + run_in_databricks.py (CI smoke test)
└── src/watersync/
    ├── cli.py                         # CLI entry points
    ├── config_planner.py              # fan-out planner
    ├── ingestion.py                   # orchestrator + config repository
    ├── cdc_pipeline.py                # CDC SCD2 pipeline builder
    ├── pipeline_bootstrap.py          # pipeline entry point
    ├── models.py                      # dataclasses
    ├── sql_dialect.py                 # SQL Server / Oracle / PostgreSQL helpers
    ├── common.py                      # shared helpers
    ├── workers/
    │   ├── base.py                    # JdbcIngestionWorker ABC
    │   ├── timestamp_watermark/worker.py
    │   └── epic_csa/worker.py
    └── utils/
        ├── uc_setup.py                # UC schema + table creation
        ├── create_ingestion_job.py    # Lakeflow Job provisioner
        ├── selective_pipeline_refresh.ipynb
        ├── lakebase_test_database_setup.py
        └── zerobus_logger.py
```

---

## License

Released under the Databricks License. See [LICENSE.md](LICENSE.md) and [NOTICE.md](NOTICE.md). To report a vulnerability, see [SECURITY.md](SECURITY.md); to contribute, see [CONTRIBUTING.md](CONTRIBUTING.md).
