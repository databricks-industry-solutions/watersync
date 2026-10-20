# App Development

Working on the Control Plane app (`app/watersync-control-plane`): a Databricks App built with [AppKit](https://www.databricks.com/devhub/docs/appkit/v0/), React, TypeScript and Tailwind CSS, using the **Analytics** plugin (SQL against a Databricks SQL warehouse) and the **Server** plugin (Express with static files and Vite dev mode).

## Prerequisites

- Node.js v22+ and npm
- Databricks CLI (for deployment)
- Access to a Databricks workspace

## Databricks Authentication

### Local Development

For local development, configure your environment variables by creating a `.env` file:

```bash
cp .env.example .env
```

Edit `.env` and set the environment variables you need:

```env
DATABRICKS_HOST=https://your-workspace.cloud.databricks.com
DATABRICKS_WAREHOUSE_ID=your_sql_warehouse_id   # required: all metadata reads and writes run on it
DATABRICKS_APP_PORT=8000
# ... other environment variables, depending on the plugins you use
```

### CLI Authentication

The Databricks CLI requires authentication to deploy and manage apps. Configure authentication using one of these methods:

#### OAuth U2M

Interactive browser-based authentication with short-lived tokens:

```bash
databricks auth login --host https://your-workspace.cloud.databricks.com
```

This will open your browser to complete authentication. The CLI saves credentials to `~/.databrickscfg`.

#### Configuration Profiles

Use multiple profiles for different workspaces:

```ini
[DEFAULT]
host = https://dev-workspace.cloud.databricks.com

[production]
host = https://prod-workspace.cloud.databricks.com
client_id = prod-client-id
client_secret = prod-client-secret
```

Deploy using a specific profile:

```bash
databricks bundle deploy --profile production
```

**Note:** Personal Access Tokens (PATs) are legacy authentication. OAuth is strongly recommended for better security.

## Getting Started

### Install Dependencies

```bash
npm ci
```

`package-lock.json` must resolve every package from `https://registry.npmjs.org/`, because the Databricks Apps build
and CI install from it. If your npm points at a private mirror (for example a proxy reachable only from a company
laptop), `npm install` writes that mirror's URLs into the lockfile and the app build then fails with `npm error 404`.
`server/lockfile.test.ts` catches this. After adding or upgrading packages, switch the URLs back; the tarballs and
integrity hashes are the same:

```bash
sed -i '' 's#"resolved": "https://<your-mirror-host>/#"resolved": "https://registry.npmjs.org/#' package-lock.json
```

(On Linux, use `sed -i` without `''`.) `npm ci` keeps working on the mirror, because npm swaps the
`registry.npmjs.org` host for your configured registry when it downloads.

### Version

The header shows `version` from `package.json`, with the build time on hover. Both are injected at build time by
`client/vite.config.ts`, so bump `version` when releasing a change.

### Release check

`GET /api/latest-release` (`server/releases.ts`) returns the latest published GitHub release, and the header compares its
tag with the app version (`client/src/lib/version.ts`). If the release is newer, the header shows a warning triangle
that opens the release notes (`client/src/components/ReleaseNotice.tsx`).

- **Tags must be the app version**, as `vX.Y.Z` or `X.Y.Z`, so publish each release with the tag of the
  `package.json` `version` it ships. Drafts and pre-releases are never "latest", so they never trigger the notice.
- **Repository:** set `WATERSYNC_RELEASES_REPO` (in `app.yaml` or `.env`) to another `owner/name`, or to an empty
  value to turn the check off. Default: `databricks-industry-solutions/watersync`.
- **Network:** the server calls the GitHub API without a token, which allows 60 calls an hour per outbound IP.
  Answers are cached for an hour (15 minutes after a failure). Failures are logged as `releases.check_failed` and
  show no notice.
- **Notes rendering:** `client/src/lib/releaseNotes.tsx` renders the Markdown subset GitHub uses (headings, bullet
  lists, bold, inline code, links) as React elements, never as HTML, so release notes cannot inject markup.

### Development

Run the app in development mode with hot reload:

```bash
npm run dev
```

The app will be available at the URL shown in the console output.

### Build

Build both client and server for production:

```bash
npm run build
```

This creates:

- `dist/server.js` - Compiled server bundle
- `client/dist/` - Bundled client assets

### Production

Run the production build:

```bash
npm start
```

## Code Quality

There are a few commands to help you with code quality:

```bash
# Type checking
npm run typecheck

# Linting
npm run lint
npm run lint:fix

# Formatting
npm run format
npm run format:fix

# Unit tests (Vitest, no workspace needed)
npx vitest run
```

## API Routes

| Route                                                                                   | Purpose                                                       |
| --------------------------------------------------------------------------------------- | ------------------------------------------------------------- |
| `POST /api/ensure-schema`                                                               | Create the schema and run pending migrations for a location   |
| `POST /api/config`, `DELETE /api/config`                                                | Save (insert, update or rename) or delete a config row        |
| `PATCH /api/watermark`, `DELETE /api/watermark`                                         | Update or delete a table's watermark state                    |
| `POST /api/source-tables`, `POST /api/source-columns`, `POST /api/source-columns-batch` | Source discovery                                              |
| `GET /api/jobs?catalog=&schema=&group=…`                                                | The location's job of each listed group (at most 100), with its last 10 runs, and the groups without one |
| `GET /api/node-types`                                                                   | Node types for compute settings, with a default               |
| `POST /api/jobs`                                                                        | Create or update a group's job and CDC pipeline               |
| `POST /api/jobs/:jobId/run`, `PATCH /api/jobs/:jobId/schedule`                          | Run a job (optionally as a full refresh), change its schedule |
| `GET /api/export`, `POST /api/import`                                                   | Export or import a location's configuration                   |
| `GET /api/latest-release`                                                               | Latest published GitHub release, for the new release notice   |
| `GET /api/row-checks/jobs`, `POST /api/row-checks/job`, `DELETE /api/row-checks/job`    | List the row check jobs of the location and its groups (each with its last 10 runs), save one, or delete one |
| `POST /api/row-checks/run`                                                              | Start a basic or detailed row check, optionally for one table |

## Query caching

The analytics plugin caches each query result for an hour, keyed by its parameters. Every query in `config/queries` takes a `refresh_token`, which `Layout.tsx` sets to the current time in seconds on each page load and refresh, so both always reach the warehouse. Give new queries the same parameter.

## Migrations

Selecting a location runs the pending migrations in `server/migrations`, once per location and server process.
Applied ids are tracked in `<catalog>.<schema>._watersync_migrations`. Tables whose names start with `_` (`_watersync_migrations`, `_watersync_row_checks`) are managed by WaterSync and not meant to be modified; give new internal tables the same prefix. To add one, create a file and append it to
`migrations` in `server/migrations/index.ts`.

| Id                            | Change                                                 |
| ----------------------------- | ------------------------------------------------------ |
| `001_create_tables`           | `jdbc_ingestion_config` and `jdbc_ingestion_watermark` |
| `002_add_uc_secret_name`      | `uc_secret_name` column                                |
| `003_add_connection_database` | `connection_database` column                           |
| `004_create_row_checks`       | `_watersync_row_checks` (see [Row Checks](Row-Checks)) |

## Logging and Diagnostics

Every failure reaches the application logs, not just the screen. Both the server and the browser emit
one-line JSON records on stdout/stderr, which is what `databricks apps logs` and the app's **Logs** tab show.

```json
{
  "timestamp": "…",
  "level": "error",
  "event": "config_save.failed",
  "requestId": "req_…",
  "status": 500,
  "error": { "message": "…", "stack": "…" }
}
```

- **Server**: `server/logging.ts` owns the logger. Every `/api` request logs one `http.request` line with status
  and duration; a failing route additionally logs `<route>.failed` with the stack, and failing SQL logs
  `sql.statement_failed` with the statement summary. Uncaught exceptions and unhandled rejections are logged
  before the process exits.
- **Browser**: `client/src/lib/logging.ts` batches events to `POST /api/client-logs`, which re-emits them as
  `client.<scope>` records. Component errors, unhandled rejections, window errors, failed API calls, and failed
  analytics queries all go through it, so a user reporting "it showed me an error" leaves a trace.
- **Correlation**: an error response carries the `requestId` it was logged under, the browser attaches that id
  to its own report, and the error screen shows a per-tab session reference. Any of the three finds the rest.
- **Redaction**: fields whose name looks like a credential (`secret`, `password`, `token`, …) are replaced with
  `[redacted]`, and long values are truncated.
- **Verbosity**: set `WATERSYNC_LOG_LEVEL` to `debug`, `info` (default), `warn`, or `error`. `debug` adds
  per-statement SQL timings.

## Export and Import Internals

The **Export** and **Import** buttons in the app header export the selected location to a JSON file and import such a file into another one.
User guide, file format and the deployment-pipeline script: [Import and Export](Import-and-Export).

Where the code lives:

- `server/routes/transfer.ts`: `GET /api/export` and `POST /api/import`.
- `server/transfer.ts`: builds the export, validates a file entry by entry against `configSchema` and
  `jobPayloadSchema` (retargeted at the import location), bulk-upserts configs with a single JSON parameter per
  500 rows, and trims groups for `replaceGroups`.
- `server/ingestionJobs.ts`: `saveJob`, shared by the **Jobs** dialog and the import, and `readJobSettings`, which
  rebuilds a group's job settings from its generated job and CDC pipeline. `ingestionJobs.test.ts` round-trips
  settings through both, so a new job option needs both sides updated.
- `client/src/components/ConfigTransfer.tsx`: the **Export** and **Import** header buttons (placed by `Layout.tsx`) and
  the import dialog.

## Deployment with Databricks Asset Bundles

### 1. Configure Bundle

Update `databricks.yml` with your workspace settings:

```yaml
targets:
  default:
    workspace:
      host: https://your-workspace.cloud.databricks.com
```

Make sure to replace all placeholder values in `databricks.yml` with your actual resource IDs.

### 2. Validate Bundle

```bash
databricks bundle validate
```

### 3. Deploy

Deploy to the default target:

```bash
databricks bundle deploy
```

### 4. Run

Start the deployed app:

```bash
databricks bundle run <APP_NAME> -t dev
```

### Deploy to Production

1. Configure the production target in `databricks.yml`
2. Deploy to production:

```bash
databricks bundle deploy -t prod
```

## Project Structure

```
* client/          # React frontend
  * src/           # Source code
  * public/        # Static assets
* client/src/components/  # Pages: Overview, Configuration (+ ConfigTransfer), Watermarks, Jobs
* server/          # Express backend
  * server.ts      # Server entry point
  * routes/        # Routes (config, discovery, jobs, transfer)
  * migrations/    # Metadata table migrations
  * schemas.ts     # Request validation (zod)
  * ingestionJobs.ts  # Job and CDC pipeline creation; reading job settings back
  * pipelines.ts   # CDC pipeline provisioning
  * transfer.ts    # Configuration export/import
  * discovery.ts   # Source discovery queries
* config/          # Configuration
  * queries/       # Analytics SQL query files
* databricks.yml   # Bundle configuration
* app.yaml         # App configuration
* .env.example     # Environment variables example
```

## Tech Stack

- **Backend**: Node.js, Express
- **Frontend**: React.js, TypeScript, Vite, Tailwind CSS, React Router
- **UI Components**: Radix UI, shadcn/ui
- **Databricks**: AppKit SDK
