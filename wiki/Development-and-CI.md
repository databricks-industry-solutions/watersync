# Development and CI

## CI

`.github/workflows/checks.yml` runs on every pull request and push to `main`, on GitHub-hosted runners with a read-only token and no secrets. It runs `ruff check` and the unit tests in `tests/`, plus typecheck, lint, format and Vitest for the Control Plane app. Python dependencies are installed from hash-pinned lockfiles (`requirements-dev.txt`, `requirements-pyspark.txt`), and npm dependencies with `npm ci`. Run the same tests locally with `PYTHONPATH=src pytest tests`.

`.github/workflows/publish.yaml` publishes `docs/slides.html` to GitHub Pages when it changes on `main`.

The `demo_workflow` job (`resources/watersync_demo_workflow.yml`) is a serverless smoke test that installs the package from the deployed bundle and runs `tests/` (`tests/run_in_databricks.py`, dependencies pinned in `tests/requirements.txt`). Run it manually with `databricks bundle run demo_workflow`.

## Project Layout

```
watersync/
├── databricks.yml                     # bundle: Control Plane app + demo_workflow
├── resources/
│   ├── watersync_app.yml
│   └── watersync_demo_workflow.yml    # serverless smoke-test job
├── .github/                           # CI and GitHub Pages workflows
├── app/watersync-control-plane/       # Databricks App (AppKit)
├── scripts/
│   ├── deploy_databricks_app.py
│   └── watersync_config.py            # export / import a configuration through the app
├── docs/slides.html                   # overview deck
├── wiki/                              # documentation (start at wiki/Home.md)
├── .agents/skills/                    # agent skills (add config, row diff)
├── notebooks/
│   ├── Task - Plan Configs.py         # job task: planner
│   ├── Task - Run Ingestion.py        # job task: for-each worker
│   └── Watersync Notebook Runner.py   # interactive runner
├── tests/                             # unit tests + run_in_databricks.py (smoke test)
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

## Documentation

The documentation lives in `wiki/`, one page per topic, and `wiki/Home.md` is the index. The repository and app READMEs only give an overview and link here.

- Update the wiki page in the same pull request as the behavior change it describes.
- Name pages `Title-With-Dashes.md`, and add new pages to `Home.md` and `_Sidebar.md`.
- Link between pages with relative paths such as `[Import and Export](Import-and-Export#authentication)`, so the links work when browsing the repository.
