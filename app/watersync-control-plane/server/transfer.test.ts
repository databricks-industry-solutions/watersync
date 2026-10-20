import { beforeEach, describe, expect, it, vi } from 'vitest';
import { z } from 'zod';

type Parameter = { name: string; value?: string };
const execute = vi.fn<(statement: string, parameters?: Parameter[]) => Promise<unknown>>();
const saveJob = vi.fn();
vi.mock('./sql.js', () => ({
  execute,
  ensureMigrations: vi.fn(),
  parameter: (name: string, value: string | null | undefined, type = 'STRING') => ({ name, type, value }),
  tableName: (catalog: string, schema: string, table: string) => `\`${catalog}\`.\`${schema}\`.\`${table}\``,
  responseRows: () => [{ num_affected_rows: '2' }],
  allResponseRows: vi.fn(),
}));
vi.mock('./ingestionJobs.js', () => ({ saveJob, readJobSettings: vi.fn() }));

const { validateImport, importSummary, importConfiguration } = await import('./transfer.js');

const config = (overrides: Record<string, unknown> = {}) => ({
  ingestionGroup: 'sales',
  sourceTableName: 'public.orders',
  targetTableFqn: 'main.bronze.orders',
  ingestionType: 'incremental',
  keyColumns: 'id',
  watermarkColumn: 'updated_at',
  epicCsaEnabled: false,
  autoCdcFromSnapshot: false,
  connectionName: null,
  connectionDatabase: 'ignored_without_connection',
  watermarkThresholdMinutes: 5,
  fetchSize: 10000,
  numPartitions: 8,
  enabled: true,
  ...overrides,
});
const job = (overrides: Record<string, unknown> = {}) => ({
  ingestionGroup: 'sales',
  gitUrl: 'https://github.com/databricks-industry-solutions/watersync',
  gitBranch: 'main',
  foreachConcurrency: 4,
  schedule: { enabled: false, quartzCronExpression: '0 0 8 * * ?', timezoneId: 'UTC', pauseStatus: 'PAUSED' },
  compute: {
    mode: 'SERVERLESS',
    performanceTarget: 'STANDARD',
    sparkVersion: '',
    driverNodeTypeId: '',
    workerNodeTypeId: '',
    minWorkers: 1,
    maxWorkers: 4,
  },
  ...overrides,
});
const location = { catalog: 'dev', schema: 'watersync' };

describe('validateImport', () => {
  it('retargets entries at the import location and applies schema defaults', () => {
    const { configs, jobs } = validateImport({
      ...location,
      file: { configs: [{ ...config(), catalog: 'prod', schema: 'other' }], jobs: [job()] },
    });
    expect(configs[0]).toMatchObject({ catalog: 'dev', schema: 'watersync', sourceTableName: 'public.orders' });
    expect(jobs[0]).toMatchObject({
      catalog: 'dev',
      pipelineCompute: { mode: 'SERVERLESS' },
      runAs: { job: { mode: 'DEFAULT' }, pipeline: { mode: 'SAME_AS_JOB' } },
    });
  });

  it('reports every invalid entry and duplicate with its position in the file', () => {
    let error: unknown;
    try {
      validateImport({
        ...location,
        file: {
          configs: [config(), config({ watermarkColumn: null }), config()],
          jobs: [job({ gitUrl: 'https://gitlab.com/x/y' })],
        },
      });
    } catch (caught) {
      error = caught;
    }
    expect(error).toBeInstanceOf(z.ZodError);
    const paths = (error as z.ZodError).issues.map((issue) => issue.path.join('.'));
    expect(paths).toEqual(
      expect.arrayContaining(['file.configs.1.watermarkColumn', 'file.jobs.0.gitUrl', 'file.configs.2'])
    );
  });
});

describe('importSummary', () => {
  it('counts source tables per group and flags groups with job settings', () => {
    const { configs, jobs } = validateImport({
      ...location,
      file: {
        configs: [config(), config({ sourceTableName: 'public.items' }), config({ ingestionGroup: 'hr' })],
        jobs: [job()],
      },
    });
    expect(importSummary(configs, jobs)).toEqual([
      { ingestionGroup: 'hr', configCount: 1, hasJobSettings: false, rowChecks: [] },
      { ingestionGroup: 'sales', configCount: 2, hasJobSettings: true, rowChecks: [] },
    ]);
  });
});

describe('scheduled row checks in a file', () => {
  const on = { enabled: true, quartzCronExpression: '0 0 6 * * ?', timezoneId: 'UTC', pauseStatus: 'UNPAUSED' };

  it('lists the checks a group turns on', () => {
    const { configs, jobs } = validateImport({
      ...location,
      file: { configs: [config()], jobs: [job({ rowChecks: { basic: on, detailed: { ...on, rangeFrom: '-30d' } } })] },
    });
    expect(importSummary(configs, jobs)[0].rowChecks).toEqual(['basic', 'detailed']);
    expect(jobs[0].rowChecks.detailed.rangeFrom).toBe('-30d');
  });

  it('imports job settings exported before row checks existed, with both checks off', () => {
    const { jobs } = validateImport({ ...location, file: { configs: [config()], jobs: [job()] } });
    expect(jobs[0].rowChecks.basic.enabled).toBe(false);
    expect(jobs[0].rowChecks.detailed.enabled).toBe(false);
  });

  it('rejects a range that the check could not parse', () => {
    expect(() =>
      validateImport({
        ...location,
        file: { configs: [config()], jobs: [job({ rowChecks: { detailed: { ...on, rangeFrom: 'last month' } } })] },
      })
    ).toThrow(/days back/);
  });
});

describe('importConfiguration', () => {
  beforeEach(() => {
    execute.mockReset();
    saveJob.mockReset();
  });

  it('upserts normalized rows, then removes unlisted rows, then deploys jobs', async () => {
    const entries = validateImport({
      ...location,
      file: { configs: [config(), config({ sourceTableName: 'public.items', ingestionType: 'full' })], jobs: [job()] },
    });
    saveJob.mockResolvedValue({ jobId: 7, pipelineId: 'p', action: 'created' });

    const result = await importConfiguration('dev', 'watersync', entries, { replaceGroups: true, deployJobs: true });

    const [[merge, mergeParameters], [remove, removeParameters]] = execute.mock.calls;
    expect(merge).toContain('MERGE INTO `dev`.`watersync`.`jdbc_ingestion_config`');
    const rows = JSON.parse(mergeParameters![0].value!) as Record<string, unknown>[];
    expect(rows).toHaveLength(2);
    expect(rows[0]).toMatchObject({ source_table_name: 'public.orders', watermark_column: 'updated_at' });
    // Full loads never keep a watermark column, and the database override needs a UC connection.
    expect(rows[1]).toMatchObject({ watermark_column: null, connection_database: null });
    expect(remove).toContain('DELETE FROM');
    expect(JSON.parse(removeParameters![1].value!)).toEqual([
      ['sales', 'public.orders'],
      ['sales', 'public.items'],
    ]);
    expect(saveJob).toHaveBeenCalledWith(expect.objectContaining({ ingestionGroup: 'sales', catalog: 'dev' }));
    expect(result).toEqual({
      configCount: 2,
      removedCount: 2,
      jobs: [{ ingestionGroup: 'sales', jobId: 7, pipelineId: 'p', action: 'created' }],
    });
  });

  it('keeps going when a job fails and leaves other rows alone without replaceGroups', async () => {
    const entries = validateImport({
      ...location,
      file: { configs: [config()], jobs: [job(), job({ ingestionGroup: 'hr' })] },
    });
    saveJob
      .mockRejectedValueOnce(new Error('PERMISSION_DENIED'))
      .mockResolvedValueOnce({ jobId: 9, action: 'updated' });

    const result = await importConfiguration('dev', 'watersync', entries, { replaceGroups: false, deployJobs: true });

    expect(execute).toHaveBeenCalledTimes(1);
    expect(result.jobs).toEqual([
      { ingestionGroup: 'sales', error: 'PERMISSION_DENIED' },
      { ingestionGroup: 'hr', jobId: 9, action: 'updated' },
    ]);
  });
});
