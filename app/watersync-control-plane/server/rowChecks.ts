import type { jobs } from '@databricks/sdk-experimental';
import { groupRowChecksSchema, toRunAs } from './schemas.js';
import type {
  GroupRowChecks,
  JobCompute,
  JobPayload,
  RowCheckJobPayload,
  RowCheckRunPayload,
  RunAsTarget,
  Schedule,
} from './schemas.js';
import { workspace, workspaceBaseUrl } from './sql.js';
import { watersyncDependency } from './pipelines.js';
import { logger } from './logging.js';
import { findLocationJob, jobCompute, readJobCompute, runAsIdentity, runAsTarget, upsertJob } from './jobCompute.js';

export const rowCheckNotebookPath = 'notebooks/Task - Row Check';
const taskKey = 'row_check';

// One job per metadata location: it checks every enabled table of every group there.
export const rowCheckJobName = (catalog: string, schema: string) => `[${catalog}.${schema}] Row Checks`;
const rowCheckFqn = (catalog: string, schema: string) => `${catalog}.${schema}._watersync_row_checks`;

async function findRowCheckJob(catalog: string, schema: string) {
  const name = rowCheckJobName(catalog, schema);
  for await (const job of workspace.jobs.list({ name, limit: 25, expand_tasks: false })) {
    if (job.settings?.name === name && job.job_id) return job.job_id;
  }
  return undefined;
}

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

/** The settings of a row check job: the location's job and each group's scheduled jobs share this shape. */
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
      { name: 'configuration_fqn', default: `${options.catalog}.${options.schema}.jdbc_ingestion_config` },
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

/** Creates or updates the location's row check job. Its default run is the basic check, on the schedule. */
export async function saveRowCheckJob(body: RowCheckJobPayload) {
  const name = rowCheckJobName(body.catalog, body.schema);
  const settings = rowCheckJobSettings({
    ...body,
    name,
    description: 'Compares WaterSync source tables with their targets. Created by the WaterSync control plane.',
    checkMode: 'basic',
    ingestionGroup: '',
    rangeFrom: '',
  });
  const { jobId, action } = await upsertJob(await findRowCheckJob(body.catalog, body.schema), settings);
  logger.info('row_checks.job_saved', {
    action,
    jobId,
    name,
    scheduled: body.schedule.enabled,
    computeMode: body.compute.mode,
    runAs: body.runAs.mode,
  });
  return { jobId, action };
}

// ── Scheduled checks per group ───────────────────────────────────

export type GroupCheckMode = 'basic' | 'detailed';
export const groupCheckModes: GroupCheckMode[] = ['basic', 'detailed'];
/** The job that runs one group's scheduled basic or detailed check, next to its "[group] Ingestion Pipeline". */
export const groupRowCheckJobName = (ingestionGroup: string, mode: GroupCheckMode) =>
  `[${ingestionGroup}] Row Check (${mode})`;
const configurationFqnOf = (catalog: string, schema: string) => `${catalog}.${schema}.jdbc_ingestion_config`;

/**
 * Creates or updates a group's scheduled row check jobs, with the ingestion job's Git source, compute and
 * identity. A check that is turned off keeps its job without a schedule, so its run history and manual runs
 * stay; a check that was never turned on gets no job.
 */
export async function saveGroupRowCheckJobs(body: JobPayload) {
  const configurationFqn = configurationFqnOf(body.catalog, body.schema);
  const results: Partial<Record<GroupCheckMode, { jobId?: number; action: 'created' | 'updated' | 'unscheduled' }>> =
    {};
  for (const mode of groupCheckModes) {
    const check = body.rowChecks[mode];
    const name = groupRowCheckJobName(body.ingestionGroup, mode);
    const existing = await findLocationJob(name, configurationFqn);
    if (!check.enabled && !existing) continue;
    const settings = rowCheckJobSettings({
      catalog: body.catalog,
      schema: body.schema,
      name,
      description: `Scheduled ${mode} row check of the ${body.ingestionGroup} group. Created by the WaterSync control plane.`,
      gitUrl: body.gitUrl,
      gitBranch: body.gitBranch,
      compute: body.compute,
      runAs: body.runAs.job,
      maxParallelTables: body.rowChecks.maxParallelTables,
      schedule: check,
      checkMode: mode,
      ingestionGroup: body.ingestionGroup,
      rangeFrom: mode === 'detailed' ? body.rowChecks.detailed.rangeFrom : '',
    });
    const { jobId, action } = await upsertJob(existing?.job_id, settings);
    results[mode] = { jobId, action: check.enabled ? action : 'unscheduled' };
  }
  logger.info('row_checks.group_jobs_saved', { ingestionGroup: body.ingestionGroup, results });
  return results;
}

const scheduleOf = (job: jobs.Job | undefined, fallback: Schedule): Schedule =>
  job?.settings?.schedule
    ? {
        enabled: true,
        quartzCronExpression: job.settings.schedule.quartz_cron_expression,
        timezoneId: job.settings.schedule.timezone_id,
        pauseStatus: job.settings.schedule.pause_status ?? 'UNPAUSED',
      }
    : { ...fallback, enabled: false };

/** A group's scheduled row checks, read back from their jobs (for export and the Jobs page). */
export async function readGroupRowChecks(
  catalog: string,
  schema: string,
  ingestionGroup: string
): Promise<GroupRowChecks> {
  const configurationFqn = configurationFqnOf(catalog, schema);
  const [basic, detailed] = await Promise.all(
    groupCheckModes.map((mode) => findLocationJob(groupRowCheckJobName(ingestionGroup, mode), configurationFqn))
  );
  const parameter = (job: jobs.Job | undefined, name: string) =>
    job?.settings?.parameters?.find((item) => item.name === name)?.default ?? '';
  const defaults = groupRowChecksSchema.parse({});
  return {
    maxParallelTables: Number(parameter(basic ?? detailed, 'max_parallel_tables')) || defaults.maxParallelTables,
    basic: scheduleOf(basic, defaults.basic),
    detailed: { ...scheduleOf(detailed, defaults.detailed), rangeFrom: parameter(detailed, 'range_from') },
  };
}

/** The location's row check job as the Databricks API reports it, with its recent runs; null when there is none. */
export async function readRowCheckJob(catalog: string, schema: string) {
  const jobId = await findRowCheckJob(catalog, schema);
  if (!jobId) return null;
  const job = await workspace.jobs.get({ job_id: jobId });
  const settings = job.settings;
  const parameters = new Map((settings?.parameters ?? []).map((item) => [item.name, item.default]));
  const runs = [];
  for await (const run of workspace.jobs.listRuns({ job_id: jobId, limit: 10, expand_tasks: false })) {
    runs.push({
      runId: run.run_id,
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
  return {
    jobId,
    workspaceUrl: `${workspaceBaseUrl()}/jobs/${jobId}`,
    gitUrl: settings?.git_source?.git_url ?? '',
    gitBranch: settings?.git_source?.git_branch ?? 'main',
    maxParallelTables: Number(parameters.get('max_parallel_tables') ?? 4) || 4,
    schedule: settings?.schedule ?? null,
    compute: readJobCompute(settings),
    runAs: runAsTarget(
      job.run_as_user_name ?? runAsIdentity(settings?.run_as) ?? job.creator_user_name,
      job.creator_user_name
    ),
    runs,
  };
}

/** Starts the row check job; the default is a basic check of every table, detailed checks name their table. */
export async function runRowCheck(body: RowCheckRunPayload) {
  const jobId = await findRowCheckJob(body.catalog, body.schema);
  if (!jobId) throw new Error('Set up the row check job for this location first');
  const run = await workspace.jobs.runNow({
    job_id: jobId,
    job_parameters: {
      check_mode: body.checkMode,
      ingestion_group: body.ingestionGroup,
      source_table_name: body.sourceTableName,
      // Always sent, so a run never inherits a range from the job defaults.
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
