import type { jobs } from '@databricks/sdk-experimental';
import { toRunAs, type JobPayload, type JobSettings } from './schemas.js';
import { workspace, workspaceBaseUrl } from './sql.js';
import {
  cdcPipelineName,
  groupNeedsCdcPipeline,
  ensureCdcPipeline,
  plannerNotebookPath,
  workerNotebookPath,
  selectiveRefreshNotebookPath,
  watersyncDependency,
} from './pipelines.js';
import { logger, serializeError } from './logging.js';
import { findLocationJob, jobCompute, readJobCompute, runAsIdentity, runAsTarget } from './jobCompute.js';
import { groupCheckModes, groupRowCheckJobName, readGroupRowChecks, saveGroupRowCheckJobs } from './rowChecks.js';

const jobName = (ingestionGroup: string) => `[${ingestionGroup}] Ingestion Pipeline`;
// configuration_fqn is `<catalog>.<schema>.jdbc_ingestion_config`, with identifier parts that contain no dots.
const catalogOf = (configurationFqn: string) => configurationFqn.split('.')[0];
const schemaOf = (configurationFqn: string) => configurationFqn.split('.')[1];

/** Creates or updates the group's job (and its CDC pipeline when the group needs one). */
export async function saveJob(body: JobPayload) {
  const hasIncrementalSources = await groupNeedsCdcPipeline(body.catalog, body.schema, body.ingestionGroup);
  const cdcPipelineId = hasIncrementalSources
    ? await ensureCdcPipeline({
        pipelineId: body.cdcPipelineId,
        catalog: body.catalog,
        schema: body.schema,
        ingestionGroup: body.ingestionGroup,
        gitUrl: body.gitUrl,
        gitBranch: body.gitBranch,
        compute: body.pipelineCompute,
        runAs: toRunAs(body.runAs.pipeline.mode === 'SAME_AS_JOB' ? body.runAs.job : body.runAs.pipeline),
      })
    : undefined;
  const name = jobName(body.ingestionGroup);
  const jobRunAs = toRunAs(body.runAs.job);
  const configurationFqn = `${body.catalog}.${body.schema}.jdbc_ingestion_config`;
  const watermarkFqn = `${body.catalog}.${body.schema}.jdbc_ingestion_watermark`;
  const compute = jobCompute(body.compute, watersyncDependency(body.gitUrl, body.gitBranch));
  const taskCompute = compute.task;
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
  // performance_target is job-wide and applies to every serverless task, including a serverless pipeline task.
  const serverlessPipelineTask = Boolean(cdcPipelineId) && body.pipelineCompute.mode === 'SERVERLESS';
  const settings: jobs.JobSettings = {
    name,
    max_concurrent_runs: 1,
    ...(body.compute.mode === 'SERVERLESS' || serverlessPipelineTask
      ? { performance_target: body.compute.performanceTarget }
      : {}),
    ...compute.settings,
    parameters: [
      { name: 'configuration_fqn', default: configurationFqn },
      { name: 'watermark_fqn', default: watermarkFqn },
      { name: 'ingestion_group', default: body.ingestionGroup },
      { name: 'full_refresh', default: 'false' },
    ],
    tasks,
    ...(jobRunAs ? { run_as: jobRunAs } : {}),
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
  const action: 'created' | 'updated' = existingJobId ? 'updated' : 'created';
  logger.info('jobs.saved', {
    action,
    jobId,
    name,
    ingestionGroup: body.ingestionGroup,
    cdcPipelineId,
    computeMode: body.compute.mode,
    jobRunAs: body.runAs.job.mode,
    pipelineRunAs: body.runAs.pipeline.mode,
  });
  // The group's scheduled row checks run with the same Git source, compute and identity as its ingestion job.
  const rowCheckJobs = await saveGroupRowCheckJobs(body);
  return { jobId, pipelineId: cdcPipelineId, action, rowCheckJobs };
}

const defaultSchedule = {
  enabled: false,
  quartzCronExpression: '0 0 8 * * ?',
  timezoneId: 'America/New_York',
  pauseStatus: 'UNPAUSED',
} as const;

/**
 * The jobs of a page of ingestion groups at a location, each with its last 10 runs, and the groups that have no
 * job yet. Jobs are found by name, so the cost follows the page size, not the number of jobs in the workspace.
 */
export async function listGroupJobs(catalog: string, schema: string, ingestionGroups: string[]) {
  const configurationFqn = `${catalog}.${schema}.jdbc_ingestion_config`;
  const workspaceUrl = workspaceBaseUrl();
  const jobsFound = [];
  const missing: string[] = [];
  for (let offset = 0; offset < ingestionGroups.length; offset += 8) {
    const batch = ingestionGroups.slice(offset, offset + 8);
    const found = await Promise.all(
      batch.map(async (ingestionGroup) => {
        const job = await findLocationJob(jobName(ingestionGroup), configurationFqn);
        if (!job?.job_id) return { ingestionGroup, job: undefined };
        const runs = [];
        for await (const run of workspace.jobs.listRuns({ job_id: job.job_id, limit: 10, expand_tasks: false })) {
          runs.push({
            run_id: run.run_id,
            run_url: run.run_id ? `${workspaceUrl}/jobs/${job.job_id}/runs/${run.run_id}` : undefined,
            run_name: run.run_name,
            start_time: run.start_time,
            end_time: run.end_time,
            setup_duration: run.setup_duration,
            execution_duration: run.execution_duration,
            cleanup_duration: run.cleanup_duration,
            state: run.state,
          });
          if (runs.length >= 10) break;
        }
        const rowCheckJobs = await Promise.all(
          groupCheckModes.map((mode) => findLocationJob(groupRowCheckJobName(ingestionGroup, mode), configurationFqn))
        );
        const rowChecks = Object.fromEntries(
          groupCheckModes.map((mode, index) => {
            const check = rowCheckJobs[index];
            return [
              mode,
              check?.job_id
                ? {
                    job_id: check.job_id,
                    schedule: check.settings?.schedule ?? null,
                    workspace_url: `${workspaceUrl}/jobs/${check.job_id}`,
                  }
                : null,
            ];
          })
        );
        return {
          ingestionGroup,
          job: {
            job_id: job.job_id,
            row_checks: rowChecks,
            settings: { name: job.settings?.name, schedule: job.settings?.schedule },
            creator_user_name: job.creator_user_name,
            created_time: job.created_time,
            ingestion_group: ingestionGroup,
            workspace_url: `${workspaceUrl}/jobs/${job.job_id}`,
            runs,
          },
        };
      })
    );
    for (const item of found) {
      if (item.job) jobsFound.push(item.job);
      else missing.push(item.ingestionGroup);
    }
  }
  logger.debug('jobs.listed', { catalog, schema, groups: ingestionGroups.length, jobs: jobsFound.length });
  return { jobs: jobsFound, missing };
}

/**
 * Reads back the settings a group's job was generated from, so they can be exported and re-created elsewhere.
 * Only jobs that point at this location's config table count; returns null when the group has none.
 */
async function readGroupJobSettings(ingestionGroup: string, configurationFqn: string): Promise<JobSettings | null> {
  const name = jobName(ingestionGroup);
  for await (const listed of workspace.jobs.list({ name, limit: 25, expand_tasks: false })) {
    if (listed.settings?.name !== name || !listed.job_id) continue;
    const job = await workspace.jobs.get({ job_id: listed.job_id });
    const settings = job.settings;
    const parameters = new Map((settings?.parameters ?? []).map((item) => [item.name, item.default]));
    if (!settings?.git_source?.git_url || parameters.get('configuration_fqn') !== configurationFqn) continue;

    const tasks = settings.tasks ?? [];
    const pipelineId = tasks.find((task) => task.pipeline_task)?.pipeline_task?.pipeline_id;
    const pipeline = pipelineId
      ? await workspace.pipelines.get({ pipeline_id: pipelineId }).catch((error: unknown) => {
          logger.warn('transfer.pipeline_unreadable', { ingestionGroup, pipelineId, error: serializeError(error) });
          return undefined;
        })
      : undefined;
    // A pipeline with the generated name was provisioned by WaterSync and is re-provisioned on import; any other
    // pipeline was supplied by ID and is kept as is.
    const generatedPipeline = pipeline?.name === cdcPipelineName(ingestionGroup);
    const pipelineCluster =
      pipeline?.spec?.clusters?.find((item) => item.label === 'default') ?? pipeline?.spec?.clusters?.[0];
    const jobIdentity = job.run_as_user_name ?? runAsIdentity(settings.run_as) ?? job.creator_user_name;
    const pipelineIdentity =
      pipeline?.run_as_user_name ?? runAsIdentity(pipeline?.run_as) ?? pipeline?.creator_user_name;
    return {
      ingestionGroup,
      gitUrl: settings.git_source.git_url,
      gitBranch: settings.git_source.git_branch ?? 'main',
      foreachConcurrency: tasks.find((task) => task.task_key === 'ingestion_worker')?.for_each_task?.concurrency ?? 1,
      cdcPipelineId: pipelineId && !generatedPipeline ? pipelineId : null,
      schedule: settings.schedule
        ? {
            enabled: true,
            quartzCronExpression: settings.schedule.quartz_cron_expression,
            timezoneId: settings.schedule.timezone_id,
            pauseStatus: settings.schedule.pause_status ?? 'UNPAUSED',
          }
        : defaultSchedule,
      compute: readJobCompute(settings),
      pipelineCompute:
        generatedPipeline && pipeline?.spec?.serverless === false
          ? {
              mode: 'CLASSIC',
              nodeTypeId: pipelineCluster?.node_type_id ?? '',
              driverNodeTypeId: pipelineCluster?.driver_node_type_id ?? pipelineCluster?.node_type_id ?? '',
              minWorkers: pipelineCluster?.autoscale?.min_workers ?? pipelineCluster?.num_workers ?? 1,
              maxWorkers: pipelineCluster?.autoscale?.max_workers ?? Math.max(pipelineCluster?.num_workers ?? 1, 1),
              photon: pipeline.spec.photon ?? false,
            }
          : { mode: 'SERVERLESS', nodeTypeId: '', driverNodeTypeId: '', minWorkers: 1, maxWorkers: 4, photon: false },
      rowChecks: await readGroupRowChecks(catalogOf(configurationFqn), schemaOf(configurationFqn), ingestionGroup),
      runAs: {
        job: runAsTarget(jobIdentity, job.creator_user_name),
        // The run-as of a pipeline supplied by ID is not WaterSync's to set.
        pipeline:
          !generatedPipeline || pipelineIdentity === jobIdentity
            ? { mode: 'SAME_AS_JOB', principal: '' }
            : runAsTarget(pipelineIdentity, pipeline.creator_user_name),
      },
    };
  }
  return null;
}

/** Job settings of every listed group that has a generated job for this location. */
export async function readJobSettings(catalog: string, schema: string, ingestionGroups: string[]) {
  const configurationFqn = `${catalog}.${schema}.jdbc_ingestion_config`;
  const result: JobSettings[] = [];
  for (let offset = 0; offset < ingestionGroups.length; offset += 8) {
    const batch = ingestionGroups.slice(offset, offset + 8);
    const settings = await Promise.all(batch.map((group) => readGroupJobSettings(group, configurationFqn)));
    result.push(...settings.filter((item) => item !== null));
  }
  return result;
}
