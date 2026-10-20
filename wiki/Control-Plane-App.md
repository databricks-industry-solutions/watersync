# Control Plane App

`app/watersync-control-plane` is a Databricks App (AppKit, React, TypeScript) for running WaterSync without writing SQL. It works on one metadata location (`<catalog>.<schema>`) at a time, picked on the first screen and changeable from the header. When you select a location, the app creates the schema and any missing metadata tables and columns there (see [Migrations](App-Development#migrations)).

## Header

The header shows the app version next to its name (hover it for the build time), the selected location (click it to switch catalog and schema) and two actions that apply to the
whole location, available on every page:

- **Export** downloads the location's configuration (source tables and job settings) as JSON.
- **Import** loads such a file into the location, after validating it and showing a preview.

See [Import and Export](Import-and-Export).

### New release notice

When a newer WaterSync release is published on GitHub, a warning triangle appears next to the version. Click it to
read that release's notes and open it on GitHub. To update, redeploy the app from the new release.

The app checks `https://api.github.com/repos/databricks-industry-solutions/watersync/releases/latest` from the
server at most once an hour. If the workspace has no outbound internet access, or there is no newer release, no
triangle is shown. See [App Development](App-Development#release-check) to point the check at another
repository or turn it off.

## Pages

Every page has the same filter bar under its title: an **ingestion group**, up to 8 **picked tables** (a searchable list of the location's tables) and a **table-name search**. Everything on the page follows it, and the filtering runs on the SQL warehouse, so it works the same with thousands of tables. **Clear filters** resets it; each page keeps its own filters.

Lists are paged on the SQL warehouse too: choose **10, 50 or 100 rows per page** (50 by default, remembered in the browser for every list) and move with the first, previous, next and last page buttons. Changing a filter goes back to the first page.

| Page | What it does |
| --- | --- |
| **Overview** | Configured, enabled and failed source counts, and the latest pipeline activity, for the filtered tables of the selected location |
| **Configuration** | Add, edit and delete config rows. Discover source tables over a UC connection or a JDBC URL (SQL Server, Oracle, PostgreSQL, MySQL), optionally reading another database than the connection's (Oracle: service name), and infer key and watermark columns from primary-key constraints |
| **Watermarks** | Inspect per-table state and schedule a full refresh for a single table (see [Full Refresh](CDC-Pipeline-and-Full-Refresh#full-refresh)) |
| **Jobs** | Create or update a group's job and CDC pipeline (see [below](#jobs-created-by-the-app)), run it incrementally or as a full refresh, and edit its schedule. Lists the job of each group with a matching table, a page of groups at a time; groups without a job yet get a button that opens the dialog for them. Only this location's generated jobs are shown, not every job in the workspace. Two views, remembered per browser: **Blocks** (a card per job, the default) and **List** (one table row per job, easier to scan when there are many). Both show each job's status, schedule and last 10 runs |
| **Row checks** | Compare each enabled source table with its target: a summary of the latest basic check (row counts, run daily by the location's row check job), daily rows ingested and differences, and per-group then per-table results, all filtered by group, picked tables or table name. Detailed checks (per-chunk sums and hashes, then the rows of differing chunks) are started per table. See [Row Checks](Row-Checks#the-row-checks-page) |

## Deploying

Deploy the app with the bundle in the repo root:

```bash
databricks bundle deploy -t dev
```

The app uses the SQL warehouse named `Shared Unity Catalog Serverless`. Pass `--var warehouse_id=<sql-warehouse-id>` to use another one. You can also deploy from the Databricks UI: clone the repo into your workspace, open the Asset Bundle editor, and click **Deploy**.

The app runs as its own service principal. That principal needs write access to the metadata schemas, permission to create jobs and pipelines, and access to the UC connections and secret scopes the configs reference.

## Jobs created by the app

The **Jobs** dialog builds one job per ingestion group, named `[<group>] Ingestion Pipeline`. If a job with that name exists, it is updated in place. The dialog has these tabs:

| Tab | Settings |
| --- | --- |
| **Job configuration** | The ingestion group; shows the generated job parameters |
| **Git source & execution** | GitHub repository and branch, parallel tables (`foreach_concurrency`, 1–100), and optionally an existing SDP pipeline ID to reuse |
| **Job Compute** | **Serverless** (default) with a performance mode (`STANDARD` or `PERFORMANCE_OPTIMIZED`), or a **shared autoscaling job cluster** (Databricks Runtime, driver and worker node types, min/max workers) used by the planner and every worker |
| **SDP Compute** | Shown when the group has a CDC pipeline: **Serverless** (default), sharing the job's performance mode, or a **classic autoscaling cluster** (node types, min/max workers, Photon) |
| **Schedule** | Quartz cron expression, time zone, active or paused; or manual runs only |
| **Row checks** | A scheduled **basic** check (row counts; on by default, daily at 06:00 UTC) and a scheduled **detailed** check (off by default; weekly on Sunday), each optional, with its own cron, time zone and active or paused state. The detailed check can be limited to recent rows (for example `-30d`). See [Scheduled checks per group](Row-Checks#scheduled-checks-per-group) |
| **Run as** | The job runs as its owner (the app service principal, default), a user, or a service principal. The SDP pipeline follows the job (default) or uses its own identity |

- Jobs install the package from Git (`watersync@git+<git_url>.git@<git_branch>`) and run the notebooks from the same repository. The branch must not contain whitespace, quotes or backslashes, because it ends up in the pip requirement.
- Node type fields are prefilled with the smallest general-purpose node type with at least 4 cores (`GET /api/node-types`). Other node types are offered as suggestions.
- If a required field is missing on a tab you are not viewing, the dialog switches to that tab and names the field.
- The CDC pipeline is named `[<group>] CDC SCD2 Pipeline` and is created or updated with the job. Serverless pipelines install the package through the pipeline environment. Classic pipelines run on the **Advanced** edition (Auto CDC needs at least Pro) and use a per-group notebook in `/Shared/watersync-generated-pipelines` that installs the package with `%pip`.
- Databricks keeps one serverless performance mode per job, and it covers the SDP pipeline task too, so the job and its pipeline always run with the same mode. The dialog lets you edit it in one place: on **Job Compute** when the job is serverless (**SDP Compute** then shows it read-only, with a button to go there), otherwise on **SDP Compute** when only the pipeline is serverless. It is sent whenever the job compute or the SDP pipeline is serverless.
- Running as a user requires the app service principal to be a workspace admin. Running as another service principal requires it to have the Service Principal User role on that principal. The SDP run-as setting is ignored when you reuse an existing pipeline by ID.

See [App Development](App-Development) for local development, the API routes and migrations.
