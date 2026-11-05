import { beforeEach, describe, expect, it, vi } from 'vitest';
import type { jobs, pipelines } from '@databricks/sdk-experimental';
import { jobPayloadSchema } from './schemas.js';

// A minimal in-memory workspace: whatever saveJob creates is what readJobSettings reads back, with the
// identity fields Databricks adds (the creator is the app service principal; run_as_user_name is effective).
const appServicePrincipal = '11111111-2222-3333-4444-555555555555';
// Jobs by id.
const state: { jobs: Map<number, jobs.JobSettings>; pipeline?: pipelines.CreatePipeline } = { jobs: new Map() };
let nextJobId = 7;
const identity = (runAs?: { user_name?: string; service_principal_name?: string }) =>
  runAs?.user_name ?? runAs?.service_principal_name ?? appServicePrincipal;
// The SDK's list methods return async iterables.
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
    get: vi.fn(({ job_id }: { job_id: number }) =>
      Promise.resolve({
        job_id,
        creator_user_name: appServicePrincipal,
        run_as_user_name: identity(state.jobs.get(job_id)?.run_as),
        settings: state.jobs.get(job_id),
      })
    ),
  },
  pipelines: {
    listPipelines: vi.fn(() => listed([])),
    create: vi.fn((settings: pipelines.CreatePipeline) => {
      state.pipeline = settings;
      return Promise.resolve({ pipeline_id: 'generated-pipeline' });
    }),
    get: vi.fn(({ pipeline_id }: { pipeline_id: string }) =>
      Promise.resolve(
        pipeline_id === 'generated-pipeline'
          ? {
              pipeline_id,
              name: state.pipeline?.name,
              creator_user_name: appServicePrincipal,
              run_as_user_name: identity(state.pipeline?.run_as),
              spec: state.pipeline,
            }
          : { pipeline_id, name: 'Hand-made pipeline', creator_user_name: 'someone@example.com' }
      )
    ),
  },
  workspace: { mkdirs: vi.fn(), import: vi.fn() },
};
vi.mock('./sql.js', () => ({
  workspace,
  // groupNeedsCdcPipeline: the group has incremental sources.
  execute: vi.fn(() => Promise.resolve({ result: { data_array: [['true']] } })),
  parameter: vi.fn(),
  tableName: vi.fn(),
}));

const { saveJob, readJobSettings } = await import('./ingestionJobs.js');
const { gitProviderOf } = await import('./gitSource.js');

const payload = (overrides: Record<string, unknown> = {}) =>
  jobPayloadSchema.parse({
    catalog: 'dev',
    schema: 'watersync',
    ingestionGroup: 'sales',
    gitUrl: 'https://github.com/databricks-industry-solutions/watersync',
    gitBranch: 'feature/x',
    foreachConcurrency: 6,
    cdcPipelineId: null,
    schedule: {
      enabled: false,
      quartzCronExpression: '0 0 8 * * ?',
      timezoneId: 'America/New_York',
      pauseStatus: 'UNPAUSED',
    },
    compute: {
      mode: 'SERVERLESS',
      performanceTarget: 'PERFORMANCE_OPTIMIZED',
      sparkVersion: '',
      driverNodeTypeId: '',
      workerNodeTypeId: '',
      minWorkers: 1,
      maxWorkers: 4,
    },
    ...overrides,
  });
const wheel = '/Volumes/main/watersync/libs/watersync-0.4.0-py3-none-any.whl';
const github = {
  source: 'GIT',
  gitUrl: 'https://github.com/databricks-industry-solutions/watersync',
  referenceType: 'BRANCH',
  reference: 'main',
};
const jobCluster = {
  mode: 'JOB_CLUSTER',
  performanceTarget: 'STANDARD',
  sparkVersion: '18.x-scala2.13',
  driverNodeTypeId: 'i3.xlarge',
  workerNodeTypeId: 'i3.xlarge',
  minWorkers: 1,
  maxWorkers: 2,
};
// The job saveJob wrote, when it wrote exactly one.
const onlyJob = () => {
  const saved = [...state.jobs.values()];
  expect(saved).toHaveLength(1);
  return saved[0];
};
// The paths of the payload's validation issues.
const issues = (overrides: Record<string, unknown>) =>
  jobPayloadSchema.safeParse({ ...payload(), ...overrides }).error?.issues.map((issue) => issue.path.join('.'));
const roundTrip = async (body: ReturnType<typeof payload>) => {
  await saveJob(body);
  const { catalog: _catalog, schema: _schema, ...settings } = body;
  expect(await readJobSettings('dev', 'watersync', ['sales'])).toEqual([settings]);
};

describe('readJobSettings', () => {
  beforeEach(() => {
    state.jobs.clear();
    state.pipeline = undefined;
  });

  it('reads back serverless settings run as the owner', async () => {
    await roundTrip(payload());
  });

  it('reads back job cluster, classic pipeline, schedule and explicit identities', async () => {
    await roundTrip(
      payload({
        schedule: { enabled: true, quartzCronExpression: '0 30 2 * * ?', timezoneId: 'UTC', pauseStatus: 'PAUSED' },
        compute: {
          mode: 'JOB_CLUSTER',
          performanceTarget: 'STANDARD',
          sparkVersion: '18.x-scala2.13',
          driverNodeTypeId: 'i3.2xlarge',
          workerNodeTypeId: 'i3.xlarge',
          minWorkers: 2,
          maxWorkers: 8,
        },
        pipelineCompute: {
          mode: 'CLASSIC',
          nodeTypeId: 'm5.xlarge',
          driverNodeTypeId: 'm5.2xlarge',
          minWorkers: 1,
          maxWorkers: 3,
          photon: true,
        },
        runAs: {
          job: { mode: 'USER', principal: 'data.engineer@example.com' },
          pipeline: { mode: 'SERVICE_PRINCIPAL', principal: '99999999-8888-7777-6666-555555555555' },
        },
      })
    );
  });

  it('maps the flat Git fields of older export files to a branch, installed from the repository', async () => {
    const body = payload();
    expect(body.notebooks).toEqual({
      source: 'GIT',
      gitUrl: 'https://github.com/databricks-industry-solutions/watersync',
      gitProvider: null,
      referenceType: 'BRANCH',
      reference: 'feature/x',
    });
    expect(body.library).toEqual({ source: 'GIT' });
    expect(payload({ libraryPath: wheel }).library).toEqual({ source: 'WHEEL', path: wheel });
    await saveJob(body);
    expect(onlyJob().environments?.[0]?.spec?.dependencies).toEqual([
      'watersync@git+https://github.com/databricks-industry-solutions/watersync.git@feature/x',
    ]);
  });

  it('installs and reads back a wheel on serverless and job cluster compute, and in the pipeline', async () => {
    await roundTrip(payload({ library: { source: 'WHEEL', path: wheel } }));
    expect(onlyJob().environments?.[0]?.spec?.dependencies).toEqual([wheel]);
    expect(state.pipeline?.environment?.dependencies).toEqual([wheel]);

    state.jobs.clear();
    const workspaceWheel =
      '/Workspace/Users/me/.bundle/watersync/dev/artifacts/.internal/watersync-0.4.0-py3-none-any.whl';
    await roundTrip(payload({ library: { source: 'WHEEL', path: workspaceWheel }, compute: jobCluster }));
    expect(onlyJob().tasks?.[0]?.libraries).toEqual([{ whl: workspaceWheel }]);
  });

  it('runs notebooks from a workspace folder and installs a pip requirement', async () => {
    const body = payload({
      notebooks: { source: 'WORKSPACE', basePath: '/Workspace/Users/me/watersync/' },
      library: { source: 'PIP', requirement: 'watersync==0.4.0' },
    });
    expect(body.notebooks).toEqual({ source: 'WORKSPACE', basePath: '/Workspace/Users/me/watersync' });
    await roundTrip(body);
    const job = onlyJob();
    expect(job.git_source).toBeUndefined();
    expect(job.tasks?.map((task) => task.notebook_task ?? task.for_each_task?.task.notebook_task)).toMatchObject([
      { notebook_path: '/Workspace/Users/me/watersync/notebooks/Task - Plan Configs', source: 'WORKSPACE' },
      { notebook_path: '/Workspace/Users/me/watersync/notebooks/Task - Run Ingestion', source: 'WORKSPACE' },
      undefined,
      {
        notebook_path: '/Workspace/Users/me/watersync/src/watersync/utils/selective_pipeline_refresh',
        source: 'WORKSPACE',
      },
      undefined,
    ]);
    expect(job.environments?.[0]?.spec?.dependencies).toEqual(['watersync==0.4.0']);

    state.jobs.clear();
    await roundTrip(payload({ ...body, compute: jobCluster }));
    expect(onlyJob().tasks?.[0]?.libraries).toEqual([{ pypi: { package: 'watersync==0.4.0' } }]);
  });

  it('pins the notebooks and the package to a tag or a commit', async () => {
    await roundTrip(payload({ notebooks: { ...github, referenceType: 'TAG', reference: 'v0.4.0' } }));
    expect(onlyJob().git_source).toEqual({ git_url: github.gitUrl, git_provider: 'gitHub', git_tag: 'v0.4.0' });
    expect(onlyJob().environments?.[0]?.spec?.dependencies).toEqual([`watersync@git+${github.gitUrl}.git@v0.4.0`]);

    state.jobs.clear();
    const commit = 'bf24af596167de4e05f65b13d5e6e7a8657f875a';
    await roundTrip(payload({ notebooks: { ...github, referenceType: 'COMMIT', reference: commit } }));
    expect(onlyJob().git_source).toMatchObject({ git_commit: commit });
    expect(issues({ notebooks: { ...github, referenceType: 'COMMIT', reference: 'main' } })).toEqual([
      'notebooks.reference',
    ]);
  });

  it('runs the notebooks from Azure DevOps without the user name in the URL', async () => {
    const body = payload({
      // A clone URL as Azure DevOps shows it: the organization as the user name, spaces encoded in the project.
      notebooks: { ...github, gitUrl: 'https://UKHS@dev.azure.com/UKHS/Data%20Management/_git/watersync' },
    });
    expect(body.notebooks).toMatchObject({ gitUrl: 'https://dev.azure.com/UKHS/Data%20Management/_git/watersync' });
    await roundTrip(body);
    expect(onlyJob().git_source).toEqual({
      git_url: 'https://dev.azure.com/UKHS/Data%20Management/_git/watersync',
      git_provider: 'azureDevOpsServices',
      git_branch: 'main',
    });
    // Azure DevOps URLs take no .git suffix.
    expect(onlyJob().environments?.[0]?.spec?.dependencies).toEqual([
      'watersync@git+https://dev.azure.com/UKHS/Data%20Management/_git/watersync@main',
    ]);
  });

  it('asks for the provider of a self-hosted Git server', async () => {
    const enterprise = { ...github, gitUrl: 'https://git.example.org/data/watersync' };
    expect(issues({ notebooks: enterprise })).toEqual(['notebooks.gitProvider']);
    await roundTrip(payload({ notebooks: { ...enterprise, gitProvider: 'gitHubEnterprise' } }));
    expect(onlyJob().git_source).toMatchObject({ git_provider: 'gitHubEnterprise' });
    expect(onlyJob().environments?.[0]?.spec?.dependencies).toEqual([
      'watersync@git+https://git.example.org/data/watersync.git@main',
    ]);
  });

  it('rejects a library source that does not fit the notebooks, and paths it cannot install', () => {
    expect(
      issues({ notebooks: { source: 'WORKSPACE', basePath: '/Workspace/x' }, library: { source: 'GIT' } })
    ).toEqual(['library.source']);
    for (const path of [
      '/Workspace/Users/me/watersync.tar.gz',
      '/Volumes/main/watersync/watersync.whl',
      '/Volumes/main/watersync/libs/my "wheel".whl',
      'dbfs:/Volumes/main/watersync/libs/watersync.whl',
    ]) {
      expect(issues({ library: { source: 'WHEEL', path } }), path).toEqual(['library.path']);
    }
    expect(issues({ library: { source: 'PIP', requirement: 'watersync"; rm -rf /' } })).toEqual([
      'library.requirement',
    ]);
  });

  it('detects the provider of the Git hosts it knows', () => {
    for (const [gitUrl, provider] of [
      ['https://github.com/org/watersync', 'gitHub'],
      ['https://dev.azure.com/org/project/_git/watersync', 'azureDevOpsServices'],
      ['https://org.visualstudio.com/project/_git/watersync', 'azureDevOpsServices'],
      ['https://gitlab.com/org/watersync', 'gitLab'],
      ['https://bitbucket.org/org/watersync', 'bitbucketCloud'],
      ['https://git-codecommit.us-east-1.amazonaws.com/v1/repos/watersync', 'awsCodeCommit'],
    ]) {
      expect(gitProviderOf(gitUrl), gitUrl).toBe(provider);
    }
    expect(gitProviderOf('https://git.example.org/org/watersync')).toBeUndefined();
  });

  it('keeps a pipeline supplied by ID', async () => {
    await roundTrip(payload({ cdcPipelineId: 'existing-pipeline' }));
  });

  it('ignores a job with the group name that belongs to another location', async () => {
    await saveJob(payload({ catalog: 'prod' }));
    expect(await readJobSettings('dev', 'watersync', ['sales'])).toEqual([]);
  });
});
