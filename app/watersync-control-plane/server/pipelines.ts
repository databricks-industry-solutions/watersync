import { createHash } from 'node:crypto';
import { pipelines } from '@databricks/sdk-experimental';
import { workspace } from './auth.js';
import { execute, parameter, tableName } from './sql.js';
import { logger } from './logging.js';
import type { PipelineCompute } from './schemas.js';

export const plannerNotebookPath = 'notebooks/Task - Plan Configs';
export const workerNotebookPath = 'notebooks/Task - Run Ingestion';
export const selectiveRefreshNotebookPath = 'src/watersync/utils/selective_pipeline_refresh';

export const watersyncDependency = (gitUrl: string, gitBranch: string) =>
  `watersync@git+${gitUrl.replace(/\.git$/, '')}.git@${gitBranch}`;

export async function groupNeedsCdcPipeline(catalog: string, schema: string, ingestionGroup: string) {
  const response = await execute(
    `SELECT count_if(coalesce(enabled, true) AND (
       lower(ingestion_type) = 'incremental' OR coalesce(auto_cdc_from_snapshot, false)
     )) > 0
     FROM ${tableName(catalog, schema, 'jdbc_ingestion_config')}
     WHERE ingestion_group = :ingestion_group`,
    [parameter('ingestion_group', ingestionGroup)]
  );
  return response.result?.data_array?.[0]?.[0]?.toLowerCase() === 'true';
}

// Minimal entrypoint for the CDC SCD2 pipeline.  The actual logic lives in the
// watersync package (installed from git via environment.dependencies).  Pipeline
// libraries need an absolute /Workspace path, so we write this tiny stub to
// /Shared/ — it just imports and calls the builder.
const pipelineBootstrap = 'from __future__ import annotations\n\nfrom pyspark import pipelines as dp\nfrom pyspark.sql import SparkSession\n\nfrom watersync.cdc_pipeline import build_pipeline_from_spark_conf\n\nspark = SparkSession.getActiveSession() or SparkSession.builder.getOrCreate()\nbuild_pipeline_from_spark_conf(spark=spark, dp_module=dp)\n';

// Classic pipeline clusters cannot install pip packages and `environment` only applies to serverless,
// so classic pipelines run a notebook that installs watersync with %pip before the bootstrap code.
const classicPipelineBootstrap = (dependency: string) =>
  `# Databricks notebook source\n# MAGIC %pip install "${dependency}"\n\n# COMMAND ----------\n\n${pipelineBootstrap.replace('from __future__ import annotations\n\n', '')}`;

export async function ensureCdcPipeline(opts: {
  pipelineId?: string | null;
  catalog: string;
  schema: string;
  ingestionGroup: string;
  gitUrl: string;
  gitBranch: string;
  compute: PipelineCompute;
  runAs?: pipelines.RunAs;
}) {
  if (opts.pipelineId) return opts.pipelineId;

  const pipelineName = `[${opts.ingestionGroup}] CDC SCD2 Pipeline`;
  const safeNamePrefix = `[${opts.ingestionGroup}] CDC SCD2`.replace(/'/g, "''");
  let existingPipelineId: string | undefined;
  for await (const p of workspace().pipelines.listPipelines({ filter: `name LIKE '${safeNamePrefix}%'` })) {
    if (p.name === pipelineName && p.pipeline_id) {
      existingPipelineId = p.pipeline_id;
      break;
    }
  }

  const genDir = '/Shared/watersync-generated-pipelines';
  const bootstrapPath = `${genDir}/cdc_pipeline_bootstrap.py`;
  await workspace().workspace.mkdirs({ path: genDir });
  const dependency = watersyncDependency(opts.gitUrl, opts.gitBranch);
  let library: pipelines.PipelineLibrary;
  if (opts.compute.mode === 'CLASSIC') {
    // One notebook per group, because the %pip line carries the group's git source.
    const groupSlug = opts.ingestionGroup.replace(/[^A-Za-z0-9_-]/g, '_').slice(0, 60);
    const groupHash = createHash('sha256').update(opts.ingestionGroup).digest('hex').slice(0, 8);
    const notebookPath = `${genDir}/cdc_pipeline_bootstrap_classic_${groupSlug}_${groupHash}`;
    await workspace().workspace.import({
      path: notebookPath,
      format: 'SOURCE',
      language: 'PYTHON',
      overwrite: true,
      content: Buffer.from(classicPipelineBootstrap(dependency)).toString('base64'),
    });
    library = { notebook: { path: `/Workspace${notebookPath}` } };
  } else {
    await workspace().workspace.import({ path: bootstrapPath, format: 'RAW', overwrite: true, content: Buffer.from(pipelineBootstrap).toString('base64') })
    library = { file: { path: `/Workspace${bootstrapPath}` } };
  }

  const configurationFqn = `${opts.catalog}.${opts.schema}.jdbc_ingestion_config`;
  const watermarkFqn = `${opts.catalog}.${opts.schema}.jdbc_ingestion_watermark`;
  const settings: pipelines.CreatePipeline = {
    name: pipelineName,
    catalog: opts.catalog,
    target: opts.schema,
    configuration: {
      'pipeline.configuration_fqn': configurationFqn,
      'pipeline.watermark_fqn': watermarkFqn,
      'pipeline.ingestion_group': opts.ingestionGroup,
    },
    libraries: [library],
    channel: 'CURRENT',
    ...(opts.runAs ? { run_as: opts.runAs } : {}),
    ...(opts.compute.mode === 'CLASSIC'
      ? {
          serverless: false,
          // Auto CDC flows need at least the Pro edition on classic compute.
          edition: 'ADVANCED',
          photon: opts.compute.photon,
          clusters: [
            {
              label: 'default',
              node_type_id: opts.compute.nodeTypeId,
              driver_node_type_id: opts.compute.driverNodeTypeId,
              autoscale: {
                min_workers: opts.compute.minWorkers,
                max_workers: opts.compute.maxWorkers,
                mode: 'ENHANCED',
              },
            },
          ],
        }
      : { serverless: true, environment: { dependencies: [dependency] } }),
  };

  if (existingPipelineId) {
    await workspace().pipelines.update({ pipeline_id: existingPipelineId, ...settings });
    logger.info('pipeline.updated', {
      pipelineId: existingPipelineId,
      pipelineName,
      ingestionGroup: opts.ingestionGroup,
      computeMode: opts.compute.mode,
    });
    return existingPipelineId;
  }
  const created = await workspace().pipelines.create(settings);
  if (!created.pipeline_id) throw new Error('Databricks created the CDC pipeline without returning an ID');
  logger.info('pipeline.created', {
    pipelineId: created.pipeline_id,
    pipelineName,
    ingestionGroup: opts.ingestionGroup,
    computeMode: opts.compute.mode,
  });
  return created.pipeline_id;
}
