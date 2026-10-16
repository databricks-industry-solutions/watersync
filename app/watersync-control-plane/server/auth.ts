import { AsyncLocalStorage } from 'node:async_hooks';
import { WorkspaceClient } from '@databricks/sdk-experimental';
import type { RequestHandler } from 'express';
import { logger, requestId } from './logging.js';

/**
 * On-behalf-of-user (OBO) auth. Databricks Apps forwards the signed-in user's OAuth token in
 * `x-forwarded-access-token`; every Databricks call made while handling a request uses a client
 * built from that token, so SQL statements, UC connections, jobs, and pipelines all run with the
 * user's own permissions instead of the app service principal's.
 */
const userClients = new AsyncLocalStorage<WorkspaceClient>();

const isDevelopment = () => process.env.NODE_ENV === 'development';

let devClient: WorkspaceClient | undefined;
let warnedDevFallback = false;

/** Local development has no Apps proxy, so fall back to the developer's own CLI/.env credentials. */
const developmentClient = () => {
  if (!warnedDevFallback) {
    logger.warn('auth.obo_dev_fallback', {
      message: 'No x-forwarded-access-token header; using local Databricks credentials (development only)',
    });
    warnedDevFallback = true;
  }
  devClient ??= new WorkspaceClient({});
  return devClient;
};

const forwardedToken = (header: string | string[] | undefined) =>
  (Array.isArray(header) ? header[0] : header)?.trim() || undefined;

/** Runs the rest of the request inside the caller's user context; rejects requests without a user token. */
export const userContext = (): RequestHandler => (req, res, next) => {
  const token = forwardedToken(req.headers['x-forwarded-access-token']);
  if (!token) {
    if (isDevelopment()) return next();
    logger.warn('auth.missing_user_token', { requestId: requestId(req), path: req.path });
    res.status(401).json({
      error: 'Missing user access token. Enable user authorization (user_api_scopes) for this app.',
      requestId: requestId(req),
    });
    return;
  }
  const host = process.env.DATABRICKS_HOST;
  const client = new WorkspaceClient({ host, token, authType: 'pat' });
  userClients.run(client, next);
};

/**
 * The user_api_scopes the app needs (keep in sync with databricks.yml and resources/watersync_app.yml),
 * with what each one enables and what stops working without it.
 */
export const requiredScopes = [
  {
    scope: 'sql',
    usedFor:
      'Run SQL on the warehouse: dashboards, configuration and watermark reads and writes, schema migrations, and source discovery.',
    impactIfMissing: 'Pages show no data and saving configuration or watermarks fails.',
  },
  {
    scope: 'unity-catalog',
    usedFor: 'Read Unity Catalog secrets (uc_secret_name) to open temporary source discovery connections.',
    impactIfMissing: 'Source table and column discovery fails for configurations that use a UC secret.',
  },
  {
    scope: 'jobs',
    usedFor: 'List, create, update, run, and schedule the ingestion jobs.',
    impactIfMissing: 'The Jobs page is empty and jobs cannot be saved, run, or scheduled.',
  },
  {
    scope: 'pipelines',
    usedFor: 'Create and update the CDC SCD2 pipeline for incremental or auto CDC sources.',
    impactIfMissing: 'Saving a job fails for ingestion groups that need a CDC pipeline.',
  },
  {
    scope: 'clusters',
    usedFor: 'List node types for classic job and pipeline compute.',
    impactIfMissing: 'Node type choices for classic compute cannot be loaded.',
  },
  {
    scope: 'workspace',
    usedFor: 'Write the CDC pipeline bootstrap file under /Shared/watersync-generated-pipelines.',
    impactIfMissing: 'Creating or updating the CDC pipeline fails.',
  },
] as const;

/** Scopes carried by a Databricks OAuth access token (a JWT), or undefined if it cannot be read. */
export const tokenScopes = (token: string) => {
  const payload = token.split('.')[1];
  if (!payload) return undefined;
  try {
    const claims = JSON.parse(Buffer.from(payload, 'base64url').toString('utf8')) as Record<string, unknown>;
    const claim = claims.scope ?? claims.scp;
    if (typeof claim === 'string') return claim.split(/\s+/).filter(Boolean);
    if (Array.isArray(claim)) return claim.filter((value): value is string => typeof value === 'string');
  } catch {
    // Not a JWT; scopes are unknown.
  }
  return undefined;
};

export type ScopeStatus = {
  /** `user`: token inspected; `development`: local run without a token; `missing-token`: user authorization is off. */
  mode: 'user' | 'development' | 'missing-token';
  scopes: Array<(typeof requiredScopes)[number] & { granted: boolean | null }>;
};

/** Compares the forwarded user token's scopes with the scopes the app requires. */
export const scopeStatus = (header: string | string[] | undefined): ScopeStatus => {
  const token = forwardedToken(header);
  if (!token) {
    const mode = isDevelopment() ? 'development' : 'missing-token';
    return {
      mode,
      scopes: requiredScopes.map((item) => ({ ...item, granted: mode === 'development' ? null : false })),
    };
  }
  const granted = tokenScopes(token);
  const allApis = granted?.includes('all-apis') ?? false;
  return {
    mode: 'user',
    scopes: requiredScopes.map((item) => ({
      ...item,
      granted: granted ? allApis || granted.includes(item.scope) : null,
    })),
  };
};

/** The WorkspaceClient for the user making the current request. */
export const workspace = () => {
  const client = userClients.getStore();
  if (client) return client;
  if (isDevelopment()) return developmentClient();
  throw new Error('No user context: Databricks calls must happen while handling an authenticated request');
};
