import { z } from 'zod';

export const identifierPart = z.string().regex(/^[A-Za-z_][A-Za-z0-9_]*$/);
const coercedBoolean = z.preprocess(
  (val) => (typeof val === 'string' ? val === 'true' : val),
  z.boolean()
);
export const locationSchema = z.object({ catalog: identifierPart, schema: identifierPart });
export const configSchema = locationSchema
  .extend({
    originalIngestionGroup: z.string().optional(),
    originalSourceTableName: z.string().optional(),
    ingestionGroup: z.string().min(1),
    sourceTableName: z.string().min(1),
    stagingTableFqn: z.string().nullable().optional(),
    targetTableFqn: z.string().regex(/^[^.]+\.[^.]+\.[^.]+$/),
    ingestionType: z.enum(['incremental', 'full']),
    keyColumns: z.string().nullable().optional(),
    watermarkColumn: z.string().nullable().optional(),
    partitionColumn: z.string().nullable().optional(),
    predicateColumn: z.string().nullable().optional(),
    epicCsaEnabled: coercedBoolean,
    autoCdcFromSnapshot: coercedBoolean,
    jdbcUrl: z.string().nullable().optional(),
    jdbcUser: z.string().nullable().optional(),
    jdbcSecretScope: z.string().nullable().optional(),
    jdbcSecretKey: z.string().nullable().optional(),
    ucSecretName: z.string().nullable().optional(),
    connectionName: z.string().nullable().optional(),
    watermarkThresholdMinutes: z.number().int().nonnegative(),
    fetchSize: z.number().int().positive(),
    numPartitions: z.number().int().positive(),
    enabled: coercedBoolean,
  })
  .superRefine((value, context) => {
    if (value.epicCsaEnabled && value.ingestionType !== 'incremental') {
      context.addIssue({
        code: 'custom',
        path: ['epicCsaEnabled'],
        message: 'EPIC CSA requires incremental ingestion',
      });
    }
    if (value.autoCdcFromSnapshot && value.ingestionType !== 'full') {
      context.addIssue({
        code: 'custom',
        path: ['autoCdcFromSnapshot'],
        message: 'Snapshot CDC requires full ingestion',
      });
    }
    if (value.autoCdcFromSnapshot && !value.stagingTableFqn?.trim()) {
      context.addIssue({ code: 'custom', path: ['stagingTableFqn'], message: 'Snapshot CDC requires a staging table' });
    }
    if (value.autoCdcFromSnapshot && !value.keyColumns?.trim()) {
      context.addIssue({ code: 'custom', path: ['keyColumns'], message: 'Snapshot CDC requires key columns' });
    }
    if (value.ingestionType !== 'incremental') return;
    if (!value.keyColumns?.trim()) {
      context.addIssue({
        code: 'custom',
        path: ['keyColumns'],
        message: 'Incremental ingestion requires a key column',
      });
    }
    if (!value.epicCsaEnabled && !value.watermarkColumn?.trim()) {
      context.addIssue({
        code: 'custom',
        path: ['watermarkColumn'],
        message: 'Incremental ingestion requires a watermark column unless EPIC CSA is enabled',
      });
    }
  });
export const sourceDatabaseType = z.enum(['postgresql', 'mysql', 'sqlserver', 'oracle']);
export const sourceDiscoverySchema = z
  .object({
    connectionName: z.string().trim().optional().default(''),
    jdbcUrl: z.string().trim().optional().default(''),
    jdbcUser: z.string().trim().optional().default(''),
    jdbcSecretScope: z.string().trim().optional().default(''),
    jdbcSecretKey: z.string().trim().optional().default(''),
    ucSecretName: z.string().trim().optional().default(''),
    database: z.string().trim().optional().default(''),
    databaseType: sourceDatabaseType,
    tableFilter: z.string().trim().max(200).optional().default(''),
  })
  .superRefine((value, context) => {
    if (value.connectionName) return;
    for (const field of ['database', 'jdbcUrl', 'jdbcUser'] as const) {
      if (!value[field]) {
        context.addIssue({ code: 'custom', path: [field], message: 'Required for direct JDBC discovery' });
      }
    }
    if (!value.ucSecretName) {
      for (const field of ['jdbcSecretScope', 'jdbcSecretKey'] as const) {
        if (!value[field]) {
          context.addIssue({ code: 'custom', path: [field], message: 'Required for direct JDBC discovery (or provide ucSecretName)' });
        }
      }
    }
    if (/(?:password|pwd)\s*=/i.test(value.jdbcUrl)) {
      context.addIssue({
        code: 'custom',
        path: ['jdbcUrl'],
        message: 'Do not put passwords in the JDBC URL; use a Databricks secret scope and key',
      });
    }
  });
export const sourceColumnsSchema = sourceDiscoverySchema.extend({
  sourceSchema: z.string().trim().min(1),
  table: z.string().trim().min(1),
});
export const sourceColumnsBatchSchema = sourceDiscoverySchema.extend({
  tables: z
    .array(z.object({ sourceSchema: z.string().trim().min(1), table: z.string().trim().min(1) }))
    .min(1)
    .max(10),
});
export const keySchema = locationSchema.extend({ ingestionGroup: z.string().min(1), sourceTableName: z.string().min(1) });
export const watermarkSchema = keySchema.extend({ lastWatermark: z.string().nullable(), status: z.string().min(1) });
export const cronExpression = z
  .string()
  .trim()
  .refine((value) => value.split(/\s+/).length >= 6, 'Use a Quartz cron expression with at least 6 fields');
export const scheduleSchema = z.object({
  enabled: z.boolean(),
  quartzCronExpression: cronExpression,
  timezoneId: z.string().trim().min(1),
  pauseStatus: z.enum(['PAUSED', 'UNPAUSED']),
});
export const computeSchema = z
  .object({
    mode: z.enum(['SERVERLESS', 'JOB_CLUSTER']),
    performanceTarget: z.enum(['STANDARD', 'PERFORMANCE_OPTIMIZED']),
    sparkVersion: z.string().trim(),
    driverNodeTypeId: z.string().trim(),
    workerNodeTypeId: z.string().trim(),
    minWorkers: z.number().int().min(0),
    maxWorkers: z.number().int().min(1),
  })
  .superRefine((value, context) => {
    if (value.mode !== 'JOB_CLUSTER') return;
    for (const [field, fieldValue] of [
      ['sparkVersion', value.sparkVersion],
      ['driverNodeTypeId', value.driverNodeTypeId],
      ['workerNodeTypeId', value.workerNodeTypeId],
    ] as const) {
      if (!fieldValue) context.addIssue({ code: 'custom', path: [field], message: 'Required for a job cluster' });
    }
    if (value.maxWorkers < value.minWorkers) {
      context.addIssue({
        code: 'custom',
        path: ['maxWorkers'],
        message: 'Maximum workers must be at least the minimum',
      });
    }
  });
export const jobPayloadSchema = locationSchema.extend({
  ingestionGroup: z.string().min(1),
  gitUrl: z.url().refine((value) => new URL(value).hostname === 'github.com', 'Repository must be hosted on GitHub'),
  gitBranch: z.string().trim().min(1),
  foreachConcurrency: z.number().int().min(1).max(100),
  cdcPipelineId: z.string().trim().nullable().optional(),
  schedule: scheduleSchema,
  compute: computeSchema,
});
export const jobSchedulePayloadSchema = scheduleSchema.extend({ jobId: z.number().int().positive() });

export type SourceDiscovery = z.infer<typeof sourceDiscoverySchema>;
export type SourceDatabaseType = z.infer<typeof sourceDatabaseType>;
