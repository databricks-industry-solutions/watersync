import { beforeEach, describe, expect, it, vi } from 'vitest';
import type { jobs } from '@databricks/sdk-experimental';
import { rowCheckJobPayloadSchema, rowCheckRunPayloadSchema } from './schemas.js';

// A minimal in-memory workspace: whatever saveRowCheckJob creates is what listRowCheckJobs reads back.
const state: { jobs: Map<number, jobs.JobSettings>; runs: jobs.BaseRun[]; groups: string[] } = {
  jobs: new Map(),
  runs: [],
  groups: [],
};
let nextJobId = 9;
const appServicePrincipal = '11111111-2222-3333-4444-555555555555';
async function* listed<T>(items: T[]) {
  await Promise.resolve();
  yield* items;
}
const workspace = {
  jobs: {
    list: vi.fn(({ name }: { name: string }) =>
      listed(
        [...state.jobs]
          .filter(([, settings]) => settings.name === name)
          .map(([job_id, settings]) => ({ job_id, settings: { name: settings.name } }))
      )
    ),
    create: vi.fn((settings: jobs.JobSettings) => {
      const job_id = nextJobId++;
      state.jobs.set(job_id, settings);
      return Promise.resolve({ job_id });
    }),
    reset: vi.fn(({ job_id, new_settings }: { job_id: number; new_settings: jobs.JobSettings }) => {
      state.jobs.set(job_id, new_settings);
      return Promise.resolve();
    }),
    delete: vi.fn(({ job_id }: { job_id: number }) => {
      state.jobs.delete(job_id);
      return Promise.resolve();
    }),
    // Databricks adds the creator (the app service principal) and the effective run-as identity.
    get: vi.fn(({ job_id }: { job_id: number }) => {
      const settings = state.jobs.get(job_id);
      return Promise.resolve({
        job_id,
        creator_user_name: appServicePrincipal,
        run_as_user_name:
          settings?.run_as?.user_name ?? settings?.run_as?.service_principal_name ?? appServicePrincipal,
        settings,
      });
    }),
    listRuns: vi.fn(() => listed(state.runs)),
    runNow: vi.fn(() => Promise.resolve({ run_id: 101 })),
  },
};
vi.mock('./sql.js', () => ({
  workspace,
  workspaceBaseUrl: () => 'https://example.cloud.databricks.com',
  // The groups of the config table.
  execute: vi.fn(() => Promise.resolve({})),
  allResponseRows: vi.fn(() => Promise.resolve(state.groups.map((ingestion_group) => ({ ingestion_group })))),
  tableName: vi.fn(),
}));

const { deleteRowCheckJob, listRowCheckJobs, readRowCheckJobSettings, runRowCheck, runRowCheckJob, saveRowCheckJob } =
  await import('./rowChecks.js');

const payload = (overrides: Record<string, unknown> = {}) =>
  rowCheckJobPayloadSchema.parse({
    catalog: 'dev',
    schema: 'watersync',
    gitUrl: 'https://github.com/databricks-industry-solutions/watersync',
    gitBranch: 'main',
    maxParallelTables: 6,
    schedule: { enabled: true, quartzCronExpression: '0 0 6 * * ?', timezoneId: 'UTC', pauseStatus: 'UNPAUSED' },
    ...overrides,
  });
const byName = (name: string) => [...state.jobs.values()].find((settings) => settings.name === name);
const parametersOf = (settings?: jobs.JobSettings) =>
  Object.fromEntries((settings?.parameters ?? []).map((item) => [item.name, item.default]));
const run = (overrides: Record<string, unknown>) =>
  runRowCheck(rowCheckRunPayloadSchema.parse({ catalog: 'dev', schema: 'watersync', ...overrides }));

beforeEach(() => {
  state.jobs.clear();
  state.runs = [];
  state.groups = [];
  nextJobId = 9;
  vi.clearAllMocks();
});

describe('row check jobs', () => {
  it('creates a scheduled basic job for the whole location by default', async () => {
    expect(await saveRowCheckJob(payload())).toEqual({
      jobId: 9,
      action: 'created',
      name: '[dev.watersync] Row Checks',
    });
    const job = byName('[dev.watersync] Row Checks');
    expect(parametersOf(job)).toMatchObject({
      configuration_fqn: 'dev.watersync.jdbc_ingestion_config',
      row_check_fqn: 'dev.watersync._watersync_row_checks',
      check_mode: 'basic',
      ingestion_group: '',
      source_table_name: '',
      max_parallel_tables: '6',
      range_from: '',
    });
    expect(job?.schedule?.quartz_cron_expression).toBe('0 0 6 * * ?');
    expect(job?.tasks?.[0]?.notebook_task?.notebook_path).toBe('notebooks/Task - Row Check');
  });

  it('creates one job per group and mode, with a detailed range start', async () => {
    await saveRowCheckJob(payload({ ingestionGroup: 'sales', checkMode: 'detailed', rangeFrom: '-30d' }));
    await saveRowCheckJob(payload({ ingestionGroup: 'sales', checkMode: 'basic', rangeFrom: '-30d' }));
    expect(parametersOf(byName('[sales] Row Check (detailed)'))).toMatchObject({
      check_mode: 'detailed',
      ingestion_group: 'sales',
      range_from: '-30d',
    });
    // A basic check has no range.
    expect(parametersOf(byName('[sales] Row Check (basic)')).range_from).toBe('');
    expect(state.jobs.size).toBe(2);
  });

  it('updates the existing job in place and drops a disabled schedule', async () => {
    await saveRowCheckJob(payload());
    const schedule = { enabled: false, quartzCronExpression: '0 0 6 * * ?', timezoneId: 'UTC', pauseStatus: 'PAUSED' };
    expect(await saveRowCheckJob(payload({ schedule }))).toMatchObject({ jobId: 9, action: 'updated' });
    expect(state.jobs.get(9)?.schedule).toBeUndefined();
    expect(state.jobs.size).toBe(1);
  });

  it('lists the location and group jobs with their settings and recent runs', async () => {
    state.groups = ['hr', 'sales'];
    await saveRowCheckJob(payload());
    await saveRowCheckJob(payload({ ingestionGroup: 'sales', checkMode: 'detailed', rangeFrom: '-7d' }));
    state.runs = [
      {
        run_id: 5,
        state: { life_cycle_state: 'RUNNING' },
        job_parameters: [
          { name: 'check_mode', value: 'detailed', default: 'basic' },
          { name: 'source_table_name', value: 'public.orders', default: '' },
        ],
      },
    ];
    const listedJobs = await listRowCheckJobs('dev', 'watersync');
    expect(listedJobs.map((job) => [job.ingestionGroup, job.checkMode, job.name])).toEqual([
      ['', 'basic', '[dev.watersync] Row Checks'],
      ['sales', 'detailed', '[sales] Row Check (detailed)'],
    ]);
    expect(listedJobs[1]).toMatchObject({
      jobId: 10,
      gitBranch: 'main',
      maxParallelTables: 6,
      rangeFrom: '-7d',
      schedule: { enabled: true, quartzCronExpression: '0 0 6 * * ?' },
    });
    expect(listedJobs[0].runs[0]).toMatchObject({
      runId: 5,
      jobId: 9,
      checkMode: 'detailed',
      sourceTableName: 'public.orders',
      lifeCycleState: 'RUNNING',
    });
  });

  it('reads back exactly what it saved, for export', async () => {
    const body = payload({
      ingestionGroup: 'sales',
      checkMode: 'detailed',
      rangeFrom: '-30d',
      runAs: { mode: 'USER', principal: 'ops@example.com' },
    });
    await saveRowCheckJob(body);
    const { catalog: _catalog, schema: _schema, ...settings } = body;
    expect(await readRowCheckJobSettings('dev', 'watersync', ['sales'])).toEqual([settings]);
  });

  it('ignores a job of the same name that belongs to another location', async () => {
    await saveRowCheckJob(payload({ catalog: 'prod', ingestionGroup: 'sales' }));
    expect(await listRowCheckJobs('dev', 'watersync', ['sales'])).toEqual([]);
  });

  it('deletes a job', async () => {
    await saveRowCheckJob(payload({ ingestionGroup: 'sales' }));
    expect(await deleteRowCheckJob('dev', 'watersync', 'sales', 'basic')).toEqual({ deleted: true });
    expect(state.jobs.size).toBe(0);
    expect(await deleteRowCheckJob('dev', 'watersync', 'sales', 'basic')).toEqual({ deleted: false });
  });

  it('runs on serverless as the app service principal by default', async () => {
    await saveRowCheckJob(payload());
    const job = state.jobs.get(9);
    expect(job?.environments?.[0]?.spec?.dependencies?.[0]).toMatch(/^watersync@git\+/);
    expect(job?.tasks?.[0]?.environment_key).toBe('watersync_environment');
    expect(job?.performance_target).toBe('STANDARD');
    expect(job?.job_clusters).toBeUndefined();
    expect(job?.run_as).toBeUndefined();
    expect((await listRowCheckJobs('dev', 'watersync'))[0]).toMatchObject({
      compute: { mode: 'SERVERLESS', performanceTarget: 'STANDARD' },
      runAs: { mode: 'DEFAULT' },
    });
  });

  it('runs on a job cluster as another identity, and reads both back', async () => {
    const compute = {
      mode: 'JOB_CLUSTER',
      performanceTarget: 'STANDARD',
      sparkVersion: '17.3.x-scala2.13',
      driverNodeTypeId: 'm5.xlarge',
      workerNodeTypeId: 'm5.2xlarge',
      minWorkers: 2,
      maxWorkers: 6,
    };
    await saveRowCheckJob(payload({ compute, runAs: { mode: 'USER', principal: 'ops@example.com' } }));
    const job = state.jobs.get(9);
    const task = job?.tasks?.[0];
    expect(task?.job_cluster_key).toBe('watersync_cluster');
    expect(task?.environment_key).toBeUndefined();
    expect(task?.libraries?.[0]?.pypi?.package).toMatch(/^watersync@git\+/);
    expect(job?.job_clusters?.[0]?.new_cluster).toMatchObject({
      spark_version: '17.3.x-scala2.13',
      driver_node_type_id: 'm5.xlarge',
      node_type_id: 'm5.2xlarge',
      autoscale: { min_workers: 2, max_workers: 6 },
    });
    // performance_target only applies to serverless.
    expect(job?.performance_target).toBeUndefined();
    expect(job?.environments).toBeUndefined();
    expect(job?.run_as).toEqual({ user_name: 'ops@example.com' });
    expect((await listRowCheckJobs('dev', 'watersync'))[0]).toMatchObject({
      compute,
      runAs: { mode: 'USER', principal: 'ops@example.com' },
    });
  });

  it('rejects a job cluster without node types, and a range start it cannot read', () => {
    const compute = {
      mode: 'JOB_CLUSTER',
      performanceTarget: 'STANDARD',
      sparkVersion: '',
      driverNodeTypeId: '',
      workerNodeTypeId: '',
      minWorkers: 1,
      maxWorkers: 4,
    };
    expect(() => payload({ compute })).toThrow();
    expect(() => payload({ checkMode: 'detailed', rangeFrom: 'last month' })).toThrow();
  });
});

describe('row check runs', () => {
  it('starts a detailed check for one table on the location job', async () => {
    await saveRowCheckJob(payload());
    const started = await run({
      checkMode: 'detailed',
      ingestionGroup: 'sales',
      sourceTableName: 'public.orders',
      rangeFrom: '-30d',
    });
    expect(started.runId).toBe(101);
    expect(workspace.jobs.runNow).toHaveBeenCalledWith({
      job_id: 9,
      job_parameters: {
        check_mode: 'detailed',
        ingestion_group: 'sales',
        source_table_name: 'public.orders',
        range_column: '',
        range_from: '-30d',
        range_to: '',
      },
    });
  });

  it("falls back to the group's job, preferring the requested mode", async () => {
    await saveRowCheckJob(payload({ ingestionGroup: 'sales', checkMode: 'basic' }));
    await saveRowCheckJob(payload({ ingestionGroup: 'sales', checkMode: 'detailed' }));
    await run({ checkMode: 'detailed', ingestionGroup: 'sales', sourceTableName: 'public.orders' });
    expect(workspace.jobs.runNow).toHaveBeenCalledWith(expect.objectContaining({ job_id: 10 }));
  });

  it('runs one job now with its own settings', async () => {
    await saveRowCheckJob(payload());
    await saveRowCheckJob(payload({ ingestionGroup: 'sales', checkMode: 'detailed', rangeFrom: '-5d' }));
    expect(await runRowCheckJob('dev', 'watersync', 'sales', 'detailed')).toEqual({
      runId: 101,
      runUrl: 'https://example.cloud.databricks.com/jobs/10/runs/101',
    });
    // No parameters: the run takes the job's defaults, as a scheduled run does.
    expect(workspace.jobs.runNow).toHaveBeenCalledWith({ job_id: 10 });
    await expect(runRowCheckJob('dev', 'watersync', 'hr', 'basic')).rejects.toThrow('There is no basic row check job');
  });

  it('refuses to run before a job exists', async () => {
    await expect(run({ checkMode: 'basic' })).rejects.toThrow('Create a row check job for this location first');
    // A group's job does not check the whole location.
    await saveRowCheckJob(payload({ ingestionGroup: 'sales' }));
    await expect(run({ checkMode: 'basic' })).rejects.toThrow('Create a row check job');
  });
});
