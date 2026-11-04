# WaterSync Wiki

WaterSync is a Python package and Databricks App for metadata-driven JDBC ingestion into Delta. It tracks changes with timestamp watermarks or EPIC Clarity CSA, keeps SCD Type 2 history with Lakeflow Spark Declarative Pipelines, and provisions one Lakeflow Job per ingestion group.

## Start here

1. [Architecture](Architecture): how config rows become jobs, staging tables and SCD Type 2 history.
2. [Installation and Setup](Installation-and-Setup): install the package and create the metadata tables.
3. [Control Plane App](Control-Plane-App): deploy the app and manage everything from the UI.
4. [Configuration Table](Configuration-Table): what each config column means and how to connect to sources.

## Guides

| Page | Contents |
| --- | --- |
| [Architecture](Architecture) | Data flow (Bronze staging, Silver SCD Type 2) and the job's task graph |
| [Installation and Setup](Installation-and-Setup) | Package install, wheel build, one-time Unity Catalog setup |
| [Configuration Table](Configuration-Table) | `jdbc_ingestion_config` columns, adding rows, JDBC URL and UC connection options |
| [Running Ingestion](Running-Ingestion) | Running from a notebook or the CLI, fan-out planning, timestamp and EPIC CSA workers |
| [CDC Pipeline and Full Refresh](CDC-Pipeline-and-Full-Refresh) | SCD Type 2 flows per source type; whole-job and single-table full refresh |
| [Control Plane App](Control-Plane-App) | App pages, deployment, and every setting of the jobs it creates (compute, schedule, run as) |
| [Row Checks](Row-Checks) | Tiered comparison of source and target tables: row counts, chunk aggregates, then the rows of differing chunks, and the job that runs it |
| [Import and Export](Import-and-Export) | Moving a configuration between environments from the app or a deployment pipeline; file format |
| [Job Provisioning](Job-Provisioning) | Creating jobs from Python or the CLI instead of the app |
| [Lakebase Test Setup](Lakebase-Test-Setup) | A seeded Lakebase Postgres source for testing both workers |

## Contributing

| Page | Contents |
| --- | --- |
| [App Development](App-Development) | Running the app locally, API routes, migrations, logging, code quality, project structure |
| [Development and CI](Development-and-CI) | CI checks, the Databricks smoke test, repository layout |

See also [CONTRIBUTING.md](https://github.com/databricks-industry-solutions/watersync/blob/main/CONTRIBUTING.md) and [SECURITY.md](https://github.com/databricks-industry-solutions/watersync/blob/main/SECURITY.md).
