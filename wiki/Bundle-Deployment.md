# Bundle Deployment

WaterSync can be deployed without the Control Plane app, with only the Databricks CLI and a configuration file:
`databricks bundle deploy` puts the code in the workspace, and `watersync-import` loads the configuration and
creates the jobs. Jobs then run their notebooks from the deployed files and install the wheel the bundle built, so
they need no Git source, no Git credentials and no wheel uploaded by hand.

## 1. Deploy the code

From the repository root, with a [Databricks CLI](https://docs.databricks.com/aws/en/dev-tools/cli/) profile for
the workspace and [uv](https://docs.astral.sh/uv/) installed (the bundle builds the wheel with `uv build`):

```bash
databricks bundle deploy -t dev        # or -t prod
```

| Target                | Deploys                                                                                                                                       |
| --------------------- | --------------------------------------------------------------------------------------------------------------------------------------------- |
| `dev`, `prod`         | The notebooks and `src/` (under `files/`), the watersync wheel built from the checkout (under `artifacts/.internal/`), and the smoke test job |
| `dev-app`, `prod-app` | The same, plus the [Control Plane app](Control-Plane-App#deploying)                                                                           |

`databricks bundle summary -t dev` shows the deployment's root path, for example
`/Workspace/Users/me@example.com/.bundle/watersync/dev` in development mode. To check the deployed wheel, run the
unit tests against it:

```bash
databricks bundle run demo_workflow -t dev
```

## 2. Import the configuration

`watersync-import` reads a file in the [export format](Import-and-Export#file-format), such as
[`examples/watersync-config.json`](https://github.com/databricks-industry-solutions/watersync/blob/main/examples/watersync-config.json),
and runs on your machine or a CI runner. Install it from the same checkout; it only needs `databricks-sdk`:

```bash
pip install .
watersync-import --file config/watersync.json --catalog main --schema watersync \
  --warehouse-id <sql-warehouse-id> --profile <profile> \
  --bundle-root /Workspace/Users/me@example.com/.bundle/watersync/dev \
  --deploy-jobs
```

It validates every entry first and lists all problems at once. Then it:

1. Creates the metadata schema and tables if they don't exist, and adds any columns added since.
2. Writes the config rows through the SQL warehouse (`--replace-groups` also deletes rows of the file's groups that it no longer lists).
3. With `--deploy-jobs`, creates or updates each group's ingestion job and CDC pipeline, and the row check jobs, the same way the app does. Jobs are matched by name and location, so importing again updates them in place.

`--bundle-root` points every job at that deployment: notebooks from its `files/` folder, and the watersync wheel
it uploaded. It overrides the `notebooks` and `library` of the file's jobs, so one file works for every target.
Without it, jobs use the sources the file sets. Add `--dry-run` to only validate the file.

| Option                            | Meaning                                                                        |
| --------------------------------- | ------------------------------------------------------------------------------ |
| `--file`, `--catalog`, `--schema` | The file, and the location of the metadata tables                              |
| `--warehouse-id`                  | The SQL warehouse that writes the config rows (not needed with `--dry-run`)    |
| `--profile`                       | A Databricks CLI profile; without it, the usual environment variables are used |
| `--bundle-root`                   | A bundle deployment whose files and wheel the jobs use                         |
| `--replace-groups`                | Make each group in the file match it exactly                                   |
| `--deploy-jobs`                   | Create or update the jobs                                                      |
| `--dry-run`                       | Only validate                                                                  |

The jobs run as the identity that runs `watersync-import` unless the file sets `runAs`. That identity needs to
write the metadata schema, use the warehouse, create jobs and pipelines, and read the secret scopes, UC connections
and secrets the configs use.

## 3. Run

```bash
databricks jobs run-now <job-id>
```

`watersync-import` prints each job's ID. To redeploy after a code change, run `databricks bundle deploy` again;
jobs pick up the new files and wheel on their next run. Run `watersync-import` again only when the configuration
changes.

The jobs and pipelines `watersync-import` creates aren't part of the bundle: `databricks bundle destroy` removes the
files and wheel they run, but not the jobs themselves. Delete them first, or deploy the bundle again before they next
run.
