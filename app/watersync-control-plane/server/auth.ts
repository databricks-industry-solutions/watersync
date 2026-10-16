import { AsyncLocalStorage } from 'node:async_hooks';
import { WorkspaceClient } from '@databricks/sdk-experimental';
import type { RequestHandler } from 'express';
import { logger, requestId } from './logging.js';

/**
 * Hybrid on-behalf-of-user (OBO) auth.
 *
 * Databricks Apps forwards the signed-in user's OAuth token in `x-forwarded-access-token`, but that
 * token can only carry the scopes Apps offers for user authorization (`sql`, `workspace.workspace`,
 * catalog reads, ...). So:
 * - `userWorkspace()` (the user's token) runs all warehouse SQL and workspace file writes, which
 *   keeps reads, writes, and temporary UC connections under the user's own permissions.
 * - `appWorkspace()` (the app service principal) handles the APIs users cannot be given a scope for:
 *   jobs, pipelines, cluster node types, and UC secret values. Callers must check the user's access
 *   first and grant the user access to anything they create.
 */
type UserContext = { client: WorkspaceClient; userName?: string };
const userContexts = new AsyncLocalStorage<UserContext>();

const isDevelopment = () => process.env.NODE_ENV === 'development';

let defaultClient: WorkspaceClient | undefined;
/** Client from the environment: the app service principal when deployed, local CLI/.env credentials in development. */
const environmentClient = () => (defaultClient ??= new WorkspaceClient({}));

let warnedDevFallback = false;
/** Local development has no Apps proxy, so fall back to the developer's own credentials. */
const developmentClient = () => {
  if (!warnedDevFallback) {
    logger.warn('auth.obo_dev_fallback', {
      message: 'No x-forwarded-access-token header; using local Databricks credentials (development only)',
    });
    warnedDevFallback = true;
  }
  return environmentClient();
};

const headerValue = (header: string | string[] | undefined) =>
  (Array.isArray(header) ? header[0] : header)?.trim() || undefined;

/** Runs the rest of the request inside the caller's user context; rejects requests without a user token. */
export const userContext = (): RequestHandler => (req, res, next) => {
  const token = headerValue(req.headers['x-forwarded-access-token']);
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
  userContexts.run({ client, userName: headerValue(req.headers['x-forwarded-email']) }, next);
};

/** The WorkspaceClient for the user making the current request. */
export const userWorkspace = () => {
  const context = userContexts.getStore();
  if (context) return context.client;
  if (isDevelopment()) return developmentClient();
  throw new Error('No user context: Databricks calls must happen while handling an authenticated request');
};

/**
 * The app service principal's WorkspaceClient, for APIs that user tokens cannot be scoped for.
 * Only use it after checking the user may perform the action.
 */
export const appWorkspace = () => environmentClient();

/** User name (email) of the user making the current request, used for permission grants and checks. */
export const currentUserName = async () => {
  const userName = userContexts.getStore()?.userName ?? (await userWorkspace().currentUser.me()).userName;
  if (!userName) throw new Error('Could not determine the signed-in user');
  return userName;
};

/**
 * The user_api_scopes the app needs (keep in sync with databricks.yml and resources/watersync_app.yml),
 * with what each one enables and what stops working without it.
 */
export const requiredScopes = [
  {
    scope: 'sql',
    usedFor:
      'Run SQL on the warehouse as you: dashboards, configuration and watermark reads and writes, schema migrations, temporary source discovery connections, and the access checks done before the app acts on your behalf.',
    impactIfMissing:
      'Pages show no data, saving configuration or watermarks fails, and jobs cannot be saved because your access to the configuration cannot be checked.',
  },
  {
    scope: 'workspace.workspace',
    usedFor: 'Write the CDC pipeline bootstrap file under /Shared/watersync-generated-pipelines as you.',
    impactIfMissing: 'Saving a job fails for ingestion groups that need a CDC pipeline.',
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
  const token = headerValue(header);
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
