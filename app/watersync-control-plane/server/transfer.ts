import { z } from 'zod';
import {
  configSchema,
  jobPayloadSchema,
  rowCheckJobPayloadSchema,
  transferFormat,
  transferVersion,
  type ConfigPayload,
  type JobPayload,
  type RowCheckJobPayload,
} from './schemas.js';
import { allResponseRows, ensureMigrations, execute, parameter, responseRows, tableName } from './sql.js';
import { readJobSettings, saveJob } from './ingestionJobs.js';
import { readRowCheckJobSettings, saveRowCheckJob } from './rowChecks.js';
import { logger, serializeError, errorMessage } from './logging.js';

type ColumnKind = 'STRING' | 'BOOLEAN' | 'INT';
// Every jdbc_ingestion_config column carried by an export, with its config payload field.
const configColumns = [
  ['ingestion_group', 'ingestionGroup', 'STRING'],
  ['source_table_name', 'sourceTableName', 'STRING'],
  ['staging_table_fqn', 'stagingTableFqn', 'STRING'],
  ['target_table_fqn', 'targetTableFqn', 'STRING'],
  ['ingestion_type', 'ingestionType', 'STRING'],
  ['key_columns', 'keyColumns', 'STRING'],
  ['watermark_column', 'watermarkColumn', 'STRING'],
  ['partition_column', 'partitionColumn', 'STRING'],
  ['predicate_column', 'predicateColumn', 'STRING'],
  ['epic_csa_enabled', 'epicCsaEnabled', 'BOOLEAN'],
  ['auto_cdc_from_snapshot', 'autoCdcFromSnapshot', 'BOOLEAN'],
  ['jdbc_url', 'jdbcUrl', 'STRING'],
  ['jdbc_user', 'jdbcUser', 'STRING'],
  ['jdbc_secret_scope', 'jdbcSecretScope', 'STRING'],
  ['jdbc_secret_key', 'jdbcSecretKey', 'STRING'],
  ['uc_secret_name', 'ucSecretName', 'STRING'],
  ['connection_name', 'connectionName', 'STRING'],
  ['connection_database', 'connectionDatabase', 'STRING'],
  ['watermark_threshold_minutes', 'watermarkThresholdMinutes', 'INT'],
  ['fetch_size', 'fetchSize', 'INT'],
  ['num_partitions', 'numPartitions', 'INT'],
  ['enabled', 'enabled', 'BOOLEAN'],
] as const satisfies ReadonlyArray<readonly [string, keyof ConfigPayload, ColumnKind]>;

const fromSqlValue = (value: string | null, kind: ColumnKind) =>
  value == null ? null : kind === 'BOOLEAN' ? value.toLowerCase() === 'true' : kind === 'INT' ? Number(value) : value;

/** Builds the export file: every config row of the location plus the job settings stored for its groups. */
export async function exportConfiguration(catalog: string, schema: string) {
  await ensureMigrations(catalog, schema);
  // Same defaults as config_entries.sql, so the file holds what the app shows.
  const response = await execute(
    `SELECT ingestion_group, source_table_name, staging_table_fqn, target_table_fqn,
       lower(coalesce(ingestion_type, 'incremental')) AS ingestion_type, key_columns, watermark_column,
       partition_column, predicate_column, coalesce(epic_csa_enabled, false) AS epic_csa_enabled,
       coalesce(auto_cdc_from_snapshot, false) AS auto_cdc_from_snapshot, jdbc_url, jdbc_user, jdbc_secret_scope,
       jdbc_secret_key, uc_secret_name, connection_name, connection_database,
       coalesce(watermark_threshold_minutes, 5) AS watermark_threshold_minutes,
       coalesce(fetch_size, 10000) AS fetch_size, coalesce(num_partitions, 8) AS num_partitions,
       coalesce(enabled, true) AS enabled
     FROM ${tableName(catalog, schema, 'jdbc_ingestion_config')}
     ORDER BY ingestion_group, source_table_name`
  );
  const configs = (await allResponseRows(response)).map((row) =>
    Object.fromEntries(configColumns.map(([column, field, kind]) => [field, fromSqlValue(row[column], kind)]))
  );
  const groups = [...new Set(configs.map((config) => String(config.ingestionGroup)))];
  const [jobs, rowCheckJobs] = await Promise.all([
    readJobSettings(catalog, schema, groups),
    readRowCheckJobSettings(catalog, schema, groups),
  ]);
  logger.info('transfer.exported', {
    catalog,
    schema,
    configCount: configs.length,
    jobCount: jobs.length,
    rowCheckJobCount: rowCheckJobs.length,
  });
  return {
    format: transferFormat,
    version: transferVersion,
    exportedAt: new Date().toISOString(),
    source: { catalog, schema },
    configs,
    jobs,
    rowCheckJobs,
  };
}

type ImportPayload = {
  catalog: string;
  schema: string;
  file: {
    configs: Record<string, unknown>[];
    jobs: Record<string, unknown>[];
    rowCheckJobs?: Record<string, unknown>[];
  };
};

/**
 * Validates every entry of the file against the same schemas the editors use, retargeted at the import location.
 * All problems are reported at once so a file can be fixed in a single pass.
 */
export function validateImport({ catalog, schema, file }: ImportPayload) {
  const issues: z.core.$ZodIssue[] = [];
  // Keeps each entry's position in the file, so issues point at the entry to fix.
  const parseEach = <T>(entries: Record<string, unknown>[], parser: z.ZodType<T>, section: string) =>
    entries.flatMap((entry, index) => {
      const result = parser.safeParse({ ...entry, catalog, schema });
      if (result.success) return [{ index, data: result.data }];
      issues.push(...result.error.issues.map((issue) => ({ ...issue, path: ['file', section, index, ...issue.path] })));
      return [];
    });
  const rejectDuplicates = <T>(
    entries: { index: number; data: T }[],
    section: string,
    describe: (data: T) => string
  ) => {
    const seen = new Set<string>();
    for (const { index, data } of entries) {
      const label = describe(data);
      if (seen.has(label)) {
        issues.push({
          code: 'custom',
          path: ['file', section, index],
          message: `Duplicate ${label}`,
          input: undefined,
        });
      }
      seen.add(label);
    }
  };
  const configs = parseEach<ConfigPayload>(file.configs, configSchema, 'configs');
  const jobs = parseEach<JobPayload>(file.jobs, jobPayloadSchema, 'jobs');
  const rowCheckJobs = parseEach<RowCheckJobPayload>(file.rowCheckJobs ?? [], rowCheckJobPayloadSchema, 'rowCheckJobs');
  rejectDuplicates(configs, 'configs', (config) => `source table ${config.ingestionGroup} / ${config.sourceTableName}`);
  rejectDuplicates(jobs, 'jobs', (job) => `job settings for group ${job.ingestionGroup}`);
  rejectDuplicates(rowCheckJobs, 'rowCheckJobs', (job) => `${job.checkMode} row check job for ${rowCheckScope(job)}`);
  if (issues.length) throw new z.ZodError(issues);
  return {
    configs: configs.map(({ data }) => data),
    jobs: jobs.map(({ data }) => data),
    rowCheckJobs: rowCheckJobs.map(({ data }) => data),
  };
}

const rowCheckScope = (job: { ingestionGroup: string }) =>
  job.ingestionGroup ? `group ${job.ingestionGroup}` : 'the location';

/**
 * Per group: how many source tables the file brings, whether it carries job settings, and which row check jobs
 * (basic, detailed) it has. Row check jobs of the whole location are not listed per group.
 */
export function importSummary(configs: ConfigPayload[], jobs: JobPayload[], rowCheckJobs: RowCheckJobPayload[] = []) {
  type GroupSummary = { ingestionGroup: string; configCount: number; hasJobSettings: boolean; rowChecks: string[] };
  const groups = new Map<string, GroupSummary>();
  const group = (ingestionGroup: string) =>
    groups.get(ingestionGroup) ??
    groups
      .set(ingestionGroup, { ingestionGroup, configCount: 0, hasJobSettings: false, rowChecks: [] })
      .get(ingestionGroup)!;
  for (const config of configs) group(config.ingestionGroup).configCount += 1;
  for (const job of jobs) {
    const summary = group(job.ingestionGroup);
    summary.hasJobSettings = true;
  }
  for (const job of rowCheckJobs) {
    if (job.ingestionGroup) group(job.ingestionGroup).rowChecks.push(job.checkMode);
  }
  return [...groups.values()].sort((a, b) => a.ingestionGroup.localeCompare(b.ingestionGroup));
}

// Rows go to the warehouse as one JSON parameter per statement; this keeps each well below request limits.
const upsertChunkSize = 500;
const rowStruct = `ARRAY<STRUCT<${configColumns.map(([column, , kind]) => `${column}: ${kind}`).join(', ')}>>`;

// Same normalization as the single-row save in routes/config.ts.
const toConfigRow = (config: ConfigPayload) => ({
  ...Object.fromEntries(configColumns.map(([column, field]) => [column, config[field] ?? null])),
  watermark_column:
    config.ingestionType === 'incremental' && !config.epicCsaEnabled ? (config.watermarkColumn ?? null) : null,
  connection_database: config.connectionName ? (config.connectionDatabase ?? null) : null,
});

async function upsertConfigs(catalog: string, schema: string, configs: ConfigPayload[]) {
  const target = tableName(catalog, schema, 'jdbc_ingestion_config');
  const columns = configColumns.map(([column]) => column);
  const updates = columns
    .filter((column) => column !== 'ingestion_group' && column !== 'source_table_name')
    .map((column) => `${column} = s.${column}`);
  for (let offset = 0; offset < configs.length; offset += upsertChunkSize) {
    const chunk = configs.slice(offset, offset + upsertChunkSize);
    await execute(
      `MERGE INTO ${target} t
       USING (SELECT inline(from_json(:rows, '${rowStruct}'))) s
       ON t.ingestion_group = s.ingestion_group AND t.source_table_name = s.source_table_name
       WHEN MATCHED THEN UPDATE SET ${updates.join(', ')}, update_dttm = current_timestamp()
       WHEN NOT MATCHED THEN INSERT (${columns.join(', ')}, update_dttm)
         VALUES (${columns.map((column) => `s.${column}`).join(', ')}, current_timestamp())`,
      [parameter('rows', JSON.stringify(chunk.map(toConfigRow)))]
    );
  }
}

/** Deletes rows of the imported groups that the file no longer lists. Runs only after the upsert succeeded. */
async function removeUnlistedConfigs(catalog: string, schema: string, configs: ConfigPayload[]) {
  const groups = [...new Set(configs.map((config) => config.ingestionGroup))];
  if (!groups.length) return 0;
  const response = await execute(
    `DELETE FROM ${tableName(catalog, schema, 'jdbc_ingestion_config')}
     WHERE array_contains(from_json(:groups, 'ARRAY<STRING>'), ingestion_group)
       AND NOT array_contains(from_json(:keys, 'ARRAY<ARRAY<STRING>>'), array(ingestion_group, source_table_name))`,
    [
      parameter('groups', JSON.stringify(groups)),
      parameter('keys', JSON.stringify(configs.map((config) => [config.ingestionGroup, config.sourceTableName]))),
    ]
  );
  return Number(responseRows(response)[0]?.num_affected_rows ?? 0);
}

export async function importConfiguration(
  catalog: string,
  schema: string,
  { configs, jobs, rowCheckJobs }: { configs: ConfigPayload[]; jobs: JobPayload[]; rowCheckJobs: RowCheckJobPayload[] },
  options: { replaceGroups: boolean; deployJobs: boolean }
) {
  await ensureMigrations(catalog, schema);
  // Configs first: whether a job gets a CDC pipeline depends on the group's rows in the config table.
  await upsertConfigs(catalog, schema, configs);
  const removedCount = options.replaceGroups ? await removeUnlistedConfigs(catalog, schema, configs) : 0;
  const jobResults = [];
  if (options.deployJobs) {
    // One at a time and independently: a failing group must not stop the others.
    for (const job of jobs) {
      try {
        jobResults.push({ ingestionGroup: job.ingestionGroup, ...(await saveJob(job)) });
      } catch (error) {
        logger.warn('transfer.job_failed', { ingestionGroup: job.ingestionGroup, error: serializeError(error) });
        jobResults.push({ ingestionGroup: job.ingestionGroup, error: errorMessage(error) });
      }
    }
  }
  const rowCheckJobResults = [];
  if (options.deployJobs) {
    for (const job of rowCheckJobs) {
      const scope = { ingestionGroup: job.ingestionGroup, checkMode: job.checkMode };
      try {
        rowCheckJobResults.push({ ...scope, ...(await saveRowCheckJob(job)) });
      } catch (error) {
        logger.warn('transfer.row_check_job_failed', { ...scope, error: serializeError(error) });
        rowCheckJobResults.push({ ...scope, error: errorMessage(error) });
      }
    }
  }
  logger.info('transfer.imported', {
    catalog,
    schema,
    configCount: configs.length,
    removedCount,
    jobCount: jobResults.length,
    failedJobCount: jobResults.filter((result) => 'error' in result).length,
    rowCheckJobCount: rowCheckJobResults.length,
    failedRowCheckJobCount: rowCheckJobResults.filter((result) => 'error' in result).length,
  });
  return { configCount: configs.length, removedCount, jobs: jobResults, rowCheckJobs: rowCheckJobResults };
}
