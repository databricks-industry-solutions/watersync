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

/** The WorkspaceClient for the user making the current request. */
export const workspace = () => {
  const client = userClients.getStore();
  if (client) return client;
  if (isDevelopment()) return developmentClient();
  throw new Error('No user context: Databricks calls must happen while handling an authenticated request');
};
