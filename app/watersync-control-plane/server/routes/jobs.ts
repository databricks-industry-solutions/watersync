import { z } from 'zod';
import { jobs } from '@databricks/sdk-experimental';
import type { Application } from 'express';
import { jobPayloadSchema, jobSchedulePayloadSchema } from '../schemas.js';
import { workspace, workspaceBaseUrl } from '../sql.js';
import {
  groupNeedsCdcPipeline,
  ensureCdcPipeline,
  plannerNotebookPath,
  workerNotebookPath,
  selectiveRefreshNotebookPath,
  watersyncDependency,
} from '../pipelines.js';
import { apiRoute, logger } from '../logging.js';

/**
 * Registers job management routes (list, create/update, run, schedule).
 * Extracted from the monolithic server.ts for maintainability.
 */
export function registerJobRoutes(app: Application) {
  app.get(
    '/api/jobs',
    apiRoute('jobs_list', async (_req, res) => {
      const visibleJobs = [];
      for await (const job of workspace.jobs.list({ limit: 100, expand_tasks: false })) visibleJobs.push(job);
      const workspaceUrl = workspaceBaseUrl();
      const result = [];
      for (let offset = 0; offset < visibleJobs.length; offset += 8) {
        const batch = visibleJobs.slice(offset, offset + 8);
        const summaries = await Promise.all(
          batch.map(async (job) => {
            const recentRuns = [];
            if (job.job_id) {
              for await (const run of workspace.jobs.listRuns({
                job_id: job.job_id,
                limit: 10,
                expand_tasks: false,
              })) {
                recentRuns.push({
                  run_id: run.run_id,
                  run_url:
                    job.job_id && run.run_id
                      ? `${workspaceBaseUrl()}/jobs/${job.job_id}/runs/${run.run_id}`
                      : undefined,
                  run_name: run.run_name,
                  start_time: run.start_time,
                  end_time: run.end_time,
                  setup_duration: run.setup_duration,
                  execution_duration: run.execution_duration,
                  cleanup_duration: run.cleanup_duration,
                  state: run.state,
                });
              }
            }
            return {
              ...job,
              workspace_url: job.job_id ? `${workspaceUrl}/jobs/${job.job_id}` : workspaceUrl,
              runs: recentRuns,
            };
          })
        );
        result.push(...summaries);
      }
      logger.debug('jobs.listed', { jobCount: result.length });
      res.json({ jobs: result });
    })
  );

  app.post(
    '/api/jobs',
    apiRoute('jobs_save', async (req, res) => {
      const body = jobPayloadSchema.parse(req.body);
      const hasIncrementalSources = await groupNeedsCdcPipeline(body.catalog, body.schema, body.ingestionGroup);
      const cdcPipelineId = hasIncrementalSources
        ? await ensureCdcPipeline({
            pipelineId: body.cdcPipelineId,
            catalog: body.catalog,
            schema: body.schema,
            ingestionGroup: body.ingestionGroup,
            gitUrl: body.gitUrl,
            gitBranch: body.gitBranch,
          })
        : undefined;
      const name = `[${body.ingestionGroup}] Ingestion Pipeline`;
      const configurationFqn = `${body.catalog}.${body.schema}.jdbc_ingestion_config`;
      const watermarkFqn = `${body.catalog}.${body.schema}.jdbc_ingestion_watermark`;
      const environmentKey = 'watersync_environment';
      const jobClusterKey = 'watersync_cluster';
      const dependency = watersyncDependency(body.gitUrl, body.gitBranch);
      const taskCompute =
        body.compute.mode === 'SERVERLESS'
          ? { environment_key: environmentKey }
          : {
              job_cluster_key: jobClusterKey,
              libraries: [{ pypi: { package: dependency } }],
            };
      const tasks: jobs.Task[] = [
        {
          task_key: 'ingestion_configs',
          notebook_task: { notebook_path: plannerNotebookPath, source: 'GIT' },
          ...taskCompute,
        },
        {
          task_key: 'ingestion_worker',
          depends_on: [{ task_key: 'ingestion_configs' }],
          for_each_task: {
            inputs: '{{tasks.ingestion_configs.values.table_configs}}',
            concurrency: body.foreachConcurrency,
            task: {
              task_key: 'ingestion_worker_iteration',
              ...taskCompute,
              notebook_task: {
                notebook_path: workerNotebookPath,
                source: 'GIT',
                base_parameters: {
                  source_table_name: '{{input.source_table_name}}',
                  full_refresh: '{{job.parameters.full_refresh}}',
                },
              },
            },
          },
        },
      ];
      if (cdcPipelineId) {
        tasks.push({
          task_key: 'selective_pipeline_refresh',
          depends_on: [{ task_key: 'ingestion_worker' }],
          ...taskCompute,
          notebook_task: {
            notebook_path: selectiveRefreshNotebookPath,
            source: 'GIT',
            base_parameters: {
              cdc_pipeline_id: cdcPipelineId,
            },
          },
        });
        tasks.push({
          task_key: 'cdc_scd2_pipeline',
          depends_on: [{ task_key: 'selective_pipeline_refresh' }],
          pipeline_task: { pipeline_id: cdcPipelineId, full_refresh: false },
        });
      }
      const settings: jobs.JobSettings = {
        name,
        max_concurrent_runs: 1,
        ...(body.compute.mode === 'SERVERLESS'
          ? {
              performance_target: body.compute.performanceTarget,
              environments: [
                {
                  environment_key: environmentKey,
                  spec: { environment_version: '4', dependencies: [dependency] },
                },
              ],
            }
          : {
              job_clusters: [
                {
                  job_cluster_key: jobClusterKey,
                  new_cluster: {
                    spark_version: body.compute.sparkVersion,
                    driver_node_type_id: body.compute.driverNodeTypeId,
                    node_type_id: body.compute.workerNodeTypeId,
                    autoscale: {
                      min_workers: body.compute.minWorkers,
                      max_workers: body.compute.maxWorkers,
                    },
                  },
                },
              ],
            }),
        parameters: [
          { name: 'configuration_fqn', default: configurationFqn },
          { name: 'watermark_fqn', default: watermarkFqn },
          { name: 'ingestion_group', default: body.ingestionGroup },
          { name: 'full_refresh', default: 'false' },
        ],
        tasks,
        git_source: {
          git_url: body.gitUrl,
          git_provider: 'gitHub',
          git_branch: body.gitBranch,
        },
        ...(body.schedule.enabled
          ? {
              schedule: {
                quartz_cron_expression: body.schedule.quartzCronExpression,
                timezone_id: body.schedule.timezoneId,
                pause_status: body.schedule.pauseStatus,
              },
            }
          : {}),
      };
      const existing = [];
      for await (const job of workspace.jobs.list({ name, limit: 25, expand_tasks: false })) {
        if (job.settings?.name === name) existing.push(job);
      }
      const existingJobId = existing[0]?.job_id;
      let jobId = existingJobId;
      if (existingJobId) {
        await workspace.jobs.reset({ job_id: existingJobId, new_settings: settings });
      } else {
        jobId = (await workspace.jobs.create(settings as jobs.CreateJob)).job_id;
      }
      const action = existingJobId ? 'updated' : 'created';
      logger.info('jobs.saved', {
        action,
        jobId,
        name,
        ingestionGroup: body.ingestionGroup,
        cdcPipelineId,
        computeMode: body.compute.mode,
      });
      res.json({ jobId, pipelineId: cdcPipelineId, action });
    })
  );

  app.post(
    '/api/jobs/:jobId/run',
    apiRoute('jobs_run', async (req, res) => {
      const jobId = z.coerce.number().int().positive().parse(req.params.jobId);
      const fullRefresh = req.body?.fullRefresh === true;

      if (fullRefresh) {
        // runNow cannot combine job_parameters with pipeline_params, so we
        // temporarily flip the pipeline task's full_refresh flag on the job
        // definition, trigger the run, then reset it.  The run captures the
        // setting at creation time so the immediate reset is safe.
        const job = await workspace.jobs.get({ job_id: jobId });
        const cdcTask = job.settings?.tasks?.find((t) => t.pipeline_task);
        if (cdcTask?.pipeline_task) {
          cdcTask.pipeline_task.full_refresh = true;
          await workspace.jobs.update({ job_id: jobId, new_settings: job.settings! });
        }

        const run = await workspace.jobs.runNow({
          job_id: jobId,
          job_parameters: { full_refresh: 'true' },
        });

        // Reset the pipeline task back to incremental
        if (cdcTask?.pipeline_task) {
          cdcTask.pipeline_task.full_refresh = false;
          await workspace.jobs.update({ job_id: jobId, new_settings: job.settings! });
        }

        logger.info('jobs.run_started', { jobId, runId: run.run_id, fullRefresh });
        res.json({ runId: run.run_id, fullRefresh });
      } else {
        const run = await workspace.jobs.runNow({ job_id: jobId });
        logger.info('jobs.run_started', { jobId, runId: run.run_id, fullRefresh });
        res.json({ runId: run.run_id, fullRefresh });
      }
    })
  );

  app.patch(
    '/api/jobs/:jobId/schedule',
    apiRoute('jobs_schedule', async (req, res) => {
      const body = jobSchedulePayloadSchema.parse({
        ...req.body,
        jobId: z.coerce.number().parse(req.params.jobId),
      });
      if (body.enabled) {
        await workspace.jobs.update({
          job_id: body.jobId,
          new_settings: {
            schedule: {
              quartz_cron_expression: body.quartzCronExpression,
              timezone_id: body.timezoneId,
              pause_status: body.pauseStatus,
            },
          },
        });
      } else {
        await workspace.jobs.update({ job_id: body.jobId, fields_to_remove: ['schedule'] });
      }
      logger.info('jobs.schedule_updated', {
        jobId: body.jobId,
        enabled: body.enabled,
        pauseStatus: body.pauseStatus,
      });
      res.json({ ok: true });
    })
  );
}
