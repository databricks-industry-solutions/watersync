import type { Application } from 'express';
import { importPayloadSchema, locationSchema } from '../schemas.js';
import { exportConfiguration, importConfiguration, importSummary, validateImport } from '../transfer.js';
import { apiRoute } from '../logging.js';

/** Registers the routes that export a location's configuration to a JSON file and import it back. */
export function registerTransferRoutes(app: Application) {
  app.get(
    '/api/export',
    apiRoute('config_export', async (req, res) => {
      const { catalog, schema } = locationSchema.parse(req.query);
      res.json(await exportConfiguration(catalog, schema));
    })
  );

  app.post(
    '/api/import',
    apiRoute('config_import', async (req, res) => {
      const body = importPayloadSchema.parse(req.body);
      const entries = validateImport(body);
      const groups = importSummary(entries.configs, entries.jobs, entries.rowCheckJobs);
      const counts = { jobCount: entries.jobs.length, rowCheckJobCount: entries.rowCheckJobs.length };
      if (body.dryRun) {
        res.json({ dryRun: true, configCount: entries.configs.length, ...counts, groups });
        return;
      }
      const result = await importConfiguration(body.catalog, body.schema, entries, body);
      res.json({ dryRun: false, ...counts, groups, ...result });
    })
  );
}
