import { analytics, createApp, server } from '@databricks/appkit';
import {
  clientLogBatchSchema,
  installProcessLogging,
  logClientEvents,
  logger,
  requestLogging,
  apiRoute,
} from './logging.js';
import { scopeStatus, userContext } from './auth.js';
import { registerConfigRoutes } from './routes/config.js';
import { registerDiscoveryRoutes } from './routes/discovery.js';
import { registerJobRoutes } from './routes/jobs.js';

installProcessLogging();

await createApp({
  plugins: [analytics(), server()],
  onPluginsReady(appkit) {
    appkit.server.extend((app) => {
      app.use(requestLogging());

      app.post(
        '/api/client-logs',
        apiRoute('client_logs', (req, res) => {
          const batch = clientLogBatchSchema.parse(req.body);
          logClientEvents(req, batch);
          res.status(204).end();
        })
      );

      // Registered before userContext so the UI can explain a missing token instead of getting a 401.
      app.get(
        '/api/auth/scopes',
        apiRoute('auth_scopes', (req, res) => {
          const status = scopeStatus(req.headers['x-forwarded-access-token']);
          const missing = status.scopes.filter((item) => item.granted === false).map((item) => item.scope);
          if (missing.length) logger.warn('auth.missing_scopes', { mode: status.mode, missing });
          res.json(status);
        })
      );

      // Every route below talks to Databricks as the signed-in user (on-behalf-of auth).
      app.use('/api', userContext());

      registerConfigRoutes(app);
      registerDiscoveryRoutes(app);
      registerJobRoutes(app);

      logger.info('server.routes_registered', { nodeEnv: process.env.NODE_ENV ?? 'development' });
    });
  },
});
