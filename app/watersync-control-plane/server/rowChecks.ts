import type { jobs } from '@databricks/sdk-experimental';
import { toRunAs } from './schemas.js';
import type {
  JobCompute,
  RowCheckJobPayload,
  RowCheckJobSettings,
  RowCheckRunPayload,
  RunAsTarget,
  Schedule,
} from './schemas.js';
import { allResponseRows, execute, tableName, workspace, workspaceBaseUrl } from './sql.js';
import { watersyncDependency } from './pipelines.js';
import { logger } from './logging.js';
import { findLocationJob, jobCompute, readJobCompute, runAsIdentity, runAsTarget, upsertJob } from './jobCompute.js';

export const rowCheckNotebookPath = 'notebooks/Task - Row Check';
const taskKey = 'row_check';

export type RowCheckMode = 'basic' | 'detailed';
export const rowCheckModes: RowCheckMode[] = ['basic', 'detailed'];

/**
 * Each row check job runs one check mode over every enabled table of the location (ingestionGroup '') or of one
 * group, so there is at most one job per scope and mode. The location's basic job keeps its original name.
 */
export const rowCheckJobName = (catalog: string, schema: string, ingestionGroup: string, mode: RowCheckMode) =>
  ingestionGroup
    ? `[${ingestionGroup}] Row Check (${mode})`
    : mode === 'basic'
      ? `[${catalog}.${schema}] Row Checks`
      : `[${catalog}.${schema}] Row Checks (detailed)`;
const rowCheckFqn = (catalog: string, schema: string) => `${catalog}.${schema}._watersync_row_checks`;
const configurationFqnOf = (catalog: string, schema: string) => `${catalog}.${schema}.jdbc_ingestion_config`;

const findRowCheckJob = (catalog: string, schema: string, ingestionGroup: string, mode: RowCheckMode) =>
  findLocationJob(rowCheckJobName(catalog, schema, ingestionGroup, mode), configurationFqnOf(catalog, schema));

const parameterValue = (run: jobs.BaseRun, name: string) => {
  const item = run.job_parameters?.find((parameter) => parameter.name === name);
  return item?.value ?? item?.default ?? '';
};

type RowCheckJobOptions = {
  catalog: string;
  schema: string;
  name: string;
  description: string;
  gitUrl: string;
  gitBranch: string;
  compute: JobCompute;
  runAs: RunAsTarget;
  maxParallelTables: number;
  schedule: Schedule;
  // The defaults of a run started by the schedule or with "Run now".
  checkMode: 'basic' | 'detailed';
  ingestionGroup: string;
  rangeFrom: string;
};

/** The settings of a row check job, whatever its scope and mode. */
function rowCheckJobSettings(options: RowCheckJobOptions): jobs.JobSettings {
  const compute = jobCompute(options.compute, watersyncDependency(options.gitUrl, options.gitBranch));
  const runAs = toRunAs(options.runAs);
  return {
    name: options.name,
    description: options.description,
    // Lets a detailed check started from the app run while a scheduled check is still going.
    max_concurrent_runs: 4,
    ...compute.settings,
    ...(options.compute.mode === 'SERVERLESS' ? { performance_target: options.compute.performanceTarget } : {}),
    ...(runAs ? { run_as: runAs } : {}),
    parameters: [
      { name: 'configuration_fqn', default: configurationFqnOf(options.catalog, options.schema) },
      { name: 'watermark_fqn', default: `${options.catalog}.${options.schema}.jdbc_ingestion_watermark` },
      { name: 'row_check_fqn', default: rowCheckFqn(options.catalog, options.schema) },
      { name: 'check_mode', default: options.checkMode },
      { name: 'ingestion_group', default: options.ingestionGroup },
      { name: 'source_table_name', default: '' },
      { name: 'max_parallel_tables', default: String(options.maxParallelTables) },
      { name: 'range_column', default: '' },
      { name: 'range_from', default: options.rangeFrom },
      { name: 'range_to', default: '' },
    ],
    tasks: [
      {
        task_key: taskKey,
        ...compute.task,
        notebook_task: {
          notebook_path: rowCheckNotebookPath,
          source: 'GIT',
          base_parameters: { job_run_id: '{{job.run_id}}' },
        },
      },
    ],
    git_source: { git_url: options.gitUrl, git_provider: 'gitHub', git_branch: options.gitBranch },
    ...(options.schedule.enabled
      ? {
          schedule: {
            quartz_cron_expression: options.schedule.quartzCronExpression,
            timezone_id: options.schedule.timezoneId,
            pause_status: options.schedule.pauseStatus,
          },
        }
      : {}),
  };
}

/** Creates or updates the row check job of the payload's scope (location or group) and mode. */
export async function saveRowCheckJob(body: RowCheckJobPayload) {
  const name = rowCheckJobName(body.catalog, body.schema, body.ingestionGroup, body.checkMode);
  const scope = body.ingestionGroup ? `the ${body.ingestionGroup} group` : `${body.catalog}.${body.schema}`;
  const settings = rowCheckJobSettings({
    ...body,
    name,
    description: `${body.checkMode === 'basic' ? 'Basic' : 'Detailed'} row check of ${scope}. Created by the WaterSync control plane.`,
    rangeFrom: body.checkMode === 'detailed' ? body.rangeFrom : '',
  });
  const existing = await findRowCheckJob(body.catalog, body.schema, body.ingestionGroup, body.checkMode);
  const { jobId, action } = await upsertJob(existing?.job_id, settings);
  logger.info('row_checks.job_saved', {
    action,
    jobId,
    name,
    ingestionGroup: body.ingestionGroup,
    checkMode: body.checkMode,
    scheduled: body.schedule.enabled,
    computeMode: body.compute.mode,
    runAs: body.runAs.mode,
  });
  return { jobId, action, name };
}

/** Deletes the row check job of a scope and mode; its run history goes with it. */
export async function deleteRowCheckJob(catalog: string, schema: string, ingestionGroup: string, mode: RowCheckMode) {
  const existing = await findRowCheckJob(catalog, schema, ingestionGroup, mode);
  if (!existing?.job_id) return { deleted: false };
  await workspace.jobs.delete({ job_id: existing.job_id });
  logger.info('row_checks.job_deleted', { jobId: existing.job_id, ingestionGroup, checkMode: mode });
  return { deleted: true };
}

const parameterDefault = (job: jobs.Job, name: string) =>
  job.settings?.parameters?.find((item) => item.name === name)?.default ?? '';

/** The settings a row check job was generated from, read back from the Databricks API. */
function settingsOf(job: jobs.Job, ingestionGroup: string, checkMode: RowCheckMode): RowCheckJobSettings {
  const settings = job.settings;
  return {
    ingestionGroup,
    checkMode,
    rangeFrom: checkMode === 'detailed' ? parameterDefault(job, 'range_from') : '',
    gitUrl: settings?.git_source?.git_url ?? '',
    gitBranch: settings?.git_source?.git_branch ?? 'main',
    maxParallelTables: Number(parameterDefault(job, 'max_parallel_tables')) || 4,
    schedule: settings?.schedule
      ? {
          enabled: true,
          quartzCronExpression: settings.schedule.quartz_cron_expression,
          timezoneId: settings.schedule.timezone_id,
          pauseStatus: settings.schedule.pause_status ?? 'UNPAUSED',
        }
      : { enabled: false, quartzCronExpression: '0 0 6 * * ?', timezoneId: 'UTC', pauseStatus: 'UNPAUSED' },
    compute: readJobCompute(settings),
    runAs: runAsTarget(
      job.run_as_user_name ?? runAsIdentity(settings?.run_as) ?? job.creator_user_name,
      job.creator_user_name
    ),
  };
}

async function recentRuns(jobId: number) {
  const runs = [];
  for await (const run of workspace.jobs.listRuns({ job_id: jobId, limit: 10, expand_tasks: false })) {
    runs.push({
      runId: run.run_id,
      jobId,
      runUrl: run.run_id ? `${workspaceBaseUrl()}/jobs/${jobId}/runs/${run.run_id}` : undefined,
      startTime: run.start_time,
      endTime: run.end_time,
      lifeCycleState: run.state?.life_cycle_state,
      resultState: run.state?.result_state,
      stateMessage: run.state?.state_message,
      checkMode: parameterValue(run, 'check_mode') || 'basic',
      ingestionGroup: parameterValue(run, 'ingestion_group'),
      sourceTableName: parameterValue(run, 'source_table_name'),
      rangeColumn: parameterValue(run, 'range_column'),
      rangeFrom: parameterValue(run, 'range_from'),
      rangeTo: parameterValue(run, 'range_to'),
    });
    if (runs.length >= 10) break;
  }
  return runs;
}

/** The groups of the location's config table, for finding their row check jobs. */
async function locationGroups(catalog: string, schema: string) {
  const response = await execute(
    `SELECT DISTINCT ingestion_group FROM ${tableName(catalog, schema, 'jdbc_ingestion_config')}
     WHERE ingestion_group IS NOT NULL ORDER BY ingestion_group`
  );
  return (await allResponseRows(response)).map((row) => String(row.ingestion_group));
}

type FoundRowCheckJob = { job: jobs.Job; ingestionGroup: string; checkMode: RowCheckMode };

/**
 * Finds the row check jobs of the location and of its groups (or of the given groups only). Jobs are found by
 * name, so the cost follows the number of groups, not the number of jobs in the workspace.
 */
async function findRowCheckJobs(catalog: string, schema: string, ingestionGroups?: string[]) {
  const groups = ingestionGroups ?? (await locationGroups(catalog, schema));
  const scopes = ['', ...groups].flatMap((ingestionGroup) =>
    rowCheckModes.map((checkMode) => ({ ingestionGroup, checkMode }))
  );
  const found: FoundRowCheckJob[] = [];
  for (let offset = 0; offset < scopes.length; offset += 8) {
    const batch = scopes.slice(offset, offset + 8);
    const jobsFound = await Promise.all(
      batch.map(({ ingestionGroup, checkMode }) => findRowCheckJob(catalog, schema, ingestionGroup, checkMode))
    );
    batch.forEach((scope, index) => {
      const job = jobsFound[index];
      if (job?.job_id) found.push({ job, ...scope });
    });
  }
  return found;
}

/** The location's and its groups' row check jobs as the Databricks API reports them, each with its recent runs. */
export async function listRowCheckJobs(catalog: string, schema: string, ingestionGroups?: string[]) {
  const found = await findRowCheckJobs(catalog, schema, ingestionGroups);
  const result = await Promise.all(
    found.map(async ({ job, ingestionGroup, checkMode }) => {
      const jobId = job.job_id!;
      return {
        jobId,
        name: job.settings?.name ?? '',
        workspaceUrl: `${workspaceBaseUrl()}/jobs/${jobId}`,
        ...settingsOf(job, ingestionGroup, checkMode),
        runs: await recentRuns(jobId),
      };
    })
  );
  logger.debug('row_checks.jobs_listed', { catalog, schema, jobs: result.length });
  return result;
}

/** Row check job settings of the location and its groups, for export. */
export async function readRowCheckJobSettings(catalog: string, schema: string, ingestionGroups: string[]) {
  const found = await findRowCheckJobs(catalog, schema, ingestionGroups);
  return found.map(({ job, ingestionGroup, checkMode }) => settingsOf(job, ingestionGroup, checkMode));
}

/**
 * Starts a check on a row check job; every run parameter is sent, so any job can run any check. The location's
 * jobs come first, then the jobs of the checked group, each preferring the requested mode.
 */
export async function runRowCheck(body: RowCheckRunPayload) {
  const otherMode: RowCheckMode = body.checkMode === 'basic' ? 'detailed' : 'basic';
  const groups = body.ingestionGroup ? ['', body.ingestionGroup] : [''];
  let job: jobs.Job | undefined;
  for (const ingestionGroup of groups) {
    for (const mode of [body.checkMode, otherMode]) {
      job ??= await findRowCheckJob(body.catalog, body.schema, ingestionGroup, mode);
    }
  }
  const jobId = job?.job_id;
  if (!jobId) {
    throw new Error(
      body.ingestionGroup
        ? `Create a row check job for this location or for the ${body.ingestionGroup} group first`
        : 'Create a row check job for this location first'
    );
  }
  const run = await workspace.jobs.runNow({
    job_id: jobId,
    job_parameters: {
      check_mode: body.checkMode,
      ingestion_group: body.ingestionGroup,
      source_table_name: body.sourceTableName,
      // Always sent, so a run never inherits a range or a group from the job defaults.
      range_column: body.rangeColumn,
      range_from: body.rangeFrom,
      range_to: body.rangeTo,
    },
  });
  logger.info('row_checks.run_started', {
    jobId,
    runId: run.run_id,
    checkMode: body.checkMode,
    ingestionGroup: body.ingestionGroup,
    sourceTableName: body.sourceTableName,
    rangeColumn: body.rangeColumn,
    rangeFrom: body.rangeFrom,
    rangeTo: body.rangeTo,
  });
  return { runId: run.run_id, runUrl: `${workspaceBaseUrl()}/jobs/${jobId}/runs/${run.run_id}` };
}
