import { analytics, createApp, server } from '@databricks/appkit';
import {
  clientLogBatchSchema,
  installProcessLogging,
  logClientEvents,
  logger,
  requestLogging,
  apiRoute,
} from './logging.js';
import { userContext } from './auth.js';
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

      // Every route below talks to Databricks as the signed-in user (on-behalf-of auth).
      app.use('/api', userContext());

      registerConfigRoutes(app);
      registerDiscoveryRoutes(app);
      registerJobRoutes(app);

      logger.info('server.routes_registered', { nodeEnv: process.env.NODE_ENV ?? 'development' });
    });
  },
});
