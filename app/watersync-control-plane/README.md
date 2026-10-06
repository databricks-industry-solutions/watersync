# watersync-control-plane

A Databricks App powered by [AppKit](https://www.databricks.com/devhub/docs/appkit/v0/), featuring React, TypeScript, and Tailwind CSS.

**Enabled plugins:**

- **Analytics** -- SQL query execution against Databricks SQL Warehouses
- **Server** -- Express HTTP server with static file serving and Vite dev mode

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
npm install
```

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
```

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
* server/          # Express backend
  * server.ts      # Server entry point
  * routes/        # Routes
* shared/          # Shared types
* config/          # Configuration
  * queries/       # SQL query files
* databricks.yml   # Bundle configuration
* app.yaml         # App configuration
* .env.example     # Environment variables example
```

## Tech Stack

- **Backend**: Node.js, Express
- **Frontend**: React.js, TypeScript, Vite, Tailwind CSS, React Router
- **UI Components**: Radix UI, shadcn/ui
- **Databricks**: AppKit SDK
