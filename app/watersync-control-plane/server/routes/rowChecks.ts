import type { Application } from 'express';
import { z } from 'zod';
import { locationSchema, rowCheckJobPayloadSchema, rowCheckRunPayloadSchema } from '../schemas.js';
import { deleteRowCheckJob, listRowCheckJobs, runRowCheck, saveRowCheckJob } from '../rowChecks.js';
import { apiRoute } from '../logging.js';

const rowCheckJobScopeSchema = locationSchema.extend({
  ingestionGroup: z.string().trim().optional().default(''),
  checkMode: z.enum(['basic', 'detailed']),
});

/** Registers the row check routes: list, save and delete the row check jobs, and start a check. */
export function registerRowCheckRoutes(app: Application) {
  app.get(
    '/api/row-checks/jobs',
    apiRoute('row_checks_jobs_list', async (req, res) => {
      const { catalog, schema } = locationSchema.parse(req.query);
      res.json({ jobs: await listRowCheckJobs(catalog, schema) });
    })
  );

  app.post(
    '/api/row-checks/job',
    apiRoute('row_checks_job_save', async (req, res) => {
      res.json(await saveRowCheckJob(rowCheckJobPayloadSchema.parse(req.body)));
    })
  );

  app.delete(
    '/api/row-checks/job',
    apiRoute('row_checks_job_delete', async (req, res) => {
      const { catalog, schema, ingestionGroup, checkMode } = rowCheckJobScopeSchema.parse(req.query);
      res.json(await deleteRowCheckJob(catalog, schema, ingestionGroup, checkMode));
    })
  );

  app.post(
    '/api/row-checks/run',
    apiRoute('row_checks_run', async (req, res) => {
      res.json(await runRowCheck(rowCheckRunPayloadSchema.parse(req.body)));
    })
  );
}
