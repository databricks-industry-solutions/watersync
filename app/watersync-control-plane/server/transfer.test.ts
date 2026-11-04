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
const saveRowCheckJob = vi.fn();
vi.mock('./rowChecks.js', () => ({ saveRowCheckJob, readRowCheckJobSettings: vi.fn() }));

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

  it('accepts files written before the EPIC stored-procedure settings existed', () => {
    const { configs } = validateImport({ ...location, file: { configs: [config()], jobs: [] } });
    expect(configs[0]).toMatchObject({
      epicChangeMethod: 'csa_table',
      epicChangeSkipUpdateDate: false,
    });
  });

  it('rejects EPIC stored-procedure settings that are invalid or need EPIC CSA', () => {
    const csa = { watermarkColumn: null, epicCsaEnabled: true, epicChangeMethod: 'stored_procedure' };
    let error: unknown;
    try {
      validateImport({
        ...location,
        file: {
          configs: [
            config({ epicChangeMethod: 'stored_procedure' }),
            config({ ...csa, sourceTableName: 'a', epicChangeProcedure: 'x; DROP TABLE y' }),
            config({ ...csa, sourceTableName: 'b', epicChangeColumnNames: "A'--" }),
            config({ ...csa, sourceTableName: 'c', epicChangeMethod: 'cdc' }),
          ],
          jobs: [],
        },
      });
    } catch (caught) {
      error = caught;
    }
    const paths = (error as z.ZodError).issues.map((issue) => issue.path.join('.'));
    expect(paths).toEqual([
      'file.configs.0.epicChangeMethod',
      'file.configs.1.epicChangeProcedure',
      'file.configs.2.epicChangeColumnNames',
      'file.configs.3.epicChangeMethod',
    ]);
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

describe('row check jobs in a file', () => {
  const rowCheckJob = (overrides: Record<string, unknown> = {}) => ({
    gitUrl: 'https://github.com/databricks-industry-solutions/watersync',
    gitBranch: 'main',
    maxParallelTables: 4,
    schedule: { enabled: true, quartzCronExpression: '0 0 6 * * ?', timezoneId: 'UTC', pauseStatus: 'UNPAUSED' },
    ...overrides,
  });

  it("lists each group's checks; the location's checks are not a group", () => {
    const { configs, jobs, rowCheckJobs } = validateImport({
      ...location,
      file: {
        configs: [config()],
        jobs: [job()],
        rowCheckJobs: [
          rowCheckJob(),
          rowCheckJob({ ingestionGroup: 'sales' }),
          rowCheckJob({ ingestionGroup: 'sales', checkMode: 'detailed', rangeFrom: '-30d' }),
        ],
      },
    });
    expect(rowCheckJobs[0]).toMatchObject({ catalog: 'dev', ingestionGroup: '', checkMode: 'basic' });
    expect(importSummary(configs, jobs, rowCheckJobs)).toEqual([
      { ingestionGroup: 'sales', configCount: 1, hasJobSettings: true, rowChecks: ['basic', 'detailed'] },
    ]);
  });

  it('rejects a range that the check could not parse, and two jobs for one scope and mode', () => {
    let error: unknown;
    try {
      validateImport({
        ...location,
        file: {
          configs: [config()],
          jobs: [],
          rowCheckJobs: [
            rowCheckJob({ checkMode: 'detailed', rangeFrom: 'last month' }),
            rowCheckJob({ ingestionGroup: 'sales' }),
            rowCheckJob({ ingestionGroup: 'sales' }),
          ],
        },
      });
    } catch (caught) {
      error = caught;
    }
    const issues = (error as z.ZodError).issues;
    expect(issues.map((issue) => issue.path.join('.'))).toEqual([
      'file.rowCheckJobs.0.rangeFrom',
      'file.rowCheckJobs.2',
    ]);
    expect(issues[0].message).toMatch(/days back/);
  });

  it('deploys them with the jobs, and keeps going when one fails', async () => {
    const entries = validateImport({
      ...location,
      file: {
        configs: [config()],
        jobs: [],
        rowCheckJobs: [rowCheckJob(), rowCheckJob({ ingestionGroup: 'sales', checkMode: 'detailed' })],
      },
    });
    saveRowCheckJob
      .mockRejectedValueOnce(new Error('PERMISSION_DENIED'))
      .mockResolvedValueOnce({ jobId: 12, action: 'created', name: '[sales] Row Check (detailed)' });
    const result = await importConfiguration('dev', 'watersync', entries, { replaceGroups: false, deployJobs: true });
    expect(result.rowCheckJobs).toEqual([
      { ingestionGroup: '', checkMode: 'basic', error: 'PERMISSION_DENIED' },
      {
        ingestionGroup: 'sales',
        checkMode: 'detailed',
        jobId: 12,
        action: 'created',
        name: '[sales] Row Check (detailed)',
      },
    ]);
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
      rowCheckJobs: [],
    });
  });

  it('stores EPIC stored-procedure settings only where they apply', async () => {
    const csa = { watermarkColumn: null, epicCsaEnabled: true };
    const procedure = { epicChangeProcedure: 'EPIC_UTIL.ESP_CR_ALTERED_ROWS', epicChangeSkipUpdateDate: true };
    const entries = validateImport({
      ...location,
      file: {
        configs: [
          config({ ...csa, ...procedure, epicChangeMethod: 'stored_procedure', epicChangeColumnNames: 'A, B' }),
          config({ ...csa, ...procedure, sourceTableName: 'public.csa' }),
          config({ ...procedure, sourceTableName: 'public.plain' }),
          config({
            ...csa,
            sourceTableName: 'public.default',
            epicChangeMethod: 'stored_procedure',
            epicChangeProcedure: '',
          }),
        ],
        jobs: [],
      },
    });

    await importConfiguration('dev', 'watersync', entries, { replaceGroups: false, deployJobs: false });

    const rows = JSON.parse(execute.mock.calls[0][1]![0].value!) as Record<string, unknown>[];
    expect(rows[0]).toMatchObject({
      epic_change_method: 'stored_procedure',
      epic_change_procedure: 'EPIC_UTIL.ESP_CR_ALTERED_ROWS',
      epic_change_skip_update_date: true,
      epic_change_column_names: 'A, B',
    });
    // CSA tables and non-EPIC rows keep no procedure settings; an empty procedure means Epic's default.
    expect(rows[1]).toMatchObject({ epic_change_method: 'csa_table', epic_change_procedure: null });
    expect(rows[2]).toMatchObject({ epic_change_method: null, epic_change_skip_update_date: false });
    expect(rows[3]).toMatchObject({ epic_change_method: 'stored_procedure', epic_change_procedure: null });
  });

  it('deploys a job with the volume wheel it was exported with', async () => {
    const wheel = '/Volumes/main/watersync/libs/watersync-0.4.0-py3-none-any.whl';
    const entries = validateImport({
      ...location,
      file: { configs: [config()], jobs: [job({ libraryPath: wheel }), job({ ingestionGroup: 'hr' })] },
    });
    saveJob.mockResolvedValue({ jobId: 7, action: 'created' });

    await importConfiguration('dev', 'watersync', entries, { replaceGroups: false, deployJobs: true });

    expect(saveJob).toHaveBeenCalledWith(expect.objectContaining({ ingestionGroup: 'sales', libraryPath: wheel }));
    // A file written before the setting existed installs from the GitHub branch.
    expect(saveJob).toHaveBeenCalledWith(expect.objectContaining({ ingestionGroup: 'hr', libraryPath: null }));
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
