import type { Application } from 'express';
import { configSchema, keySchema, watermarkSchema, locationSchema } from '../schemas.js';
import { execute, ensureMigrations, parameter, tableName } from '../sql.js';
import { apiRoute, logger } from '../logging.js';

/**
 * Registers configuration and watermark CRUD routes.
 * Extracted from the monolithic server.ts for maintainability.
 */
export function registerConfigRoutes(app: Application) {
  app.post(
    '/api/ensure-schema',
    apiRoute('ensure_schema', async (req, res) => {
      const { catalog, schema } = locationSchema.parse(req.body);
      try {
        await ensureMigrations(catalog, schema);
        logger.info('migrations.ensure_schema', { catalog, schema });
        res.json({ ok: true });
      } catch (error) {
        const message = error instanceof Error ? error.message : String(error);
        logger.warn('migrations.ensure_schema_failed', { catalog, schema, error: message });
        res.json({ ok: false, error: message });
      }
    })
  );

  app.post(
    '/api/config',
    apiRoute('config_save', async (req, res) => {
      const body = configSchema.parse(req.body);
      const target = tableName(body.catalog, body.schema, 'jdbc_ingestion_config');
      await ensureMigrations(body.catalog, body.schema);
      await execute(
        `MERGE INTO ${target} t USING (SELECT :ingestion_group ingestion_group, :source_table_name source_table_name) s
         ON t.ingestion_group = coalesce(:original_ingestion_group, s.ingestion_group)
         AND t.source_table_name = coalesce(:original_source_table_name, s.source_table_name)
         WHEN MATCHED THEN UPDATE SET ingestion_group=s.ingestion_group, source_table_name=s.source_table_name,
           staging_table_fqn=:staging_table_fqn, target_table_fqn=:target_table_fqn,
           ingestion_type=:ingestion_type, key_columns=:key_columns,
           watermark_column=:watermark_column, partition_column=:partition_column,
           predicate_column=:predicate_column, epic_csa_enabled=:epic_csa_enabled,
           auto_cdc_from_snapshot=:auto_cdc_from_snapshot,
           jdbc_url=:jdbc_url, jdbc_user=:jdbc_user, jdbc_secret_scope=:jdbc_secret_scope,
           jdbc_secret_key=:jdbc_secret_key, uc_secret_name=:uc_secret_name, connection_name=:connection_name,
           watermark_threshold_minutes=:watermark_threshold_minutes, fetch_size=:fetch_size,
           num_partitions=:num_partitions,
           update_dttm=current_timestamp(), enabled=:enabled
         WHEN NOT MATCHED THEN INSERT (ingestion_group, source_table_name, staging_table_fqn, target_table_fqn,
           ingestion_type, key_columns, watermark_column, partition_column, predicate_column, epic_csa_enabled, auto_cdc_from_snapshot,
           jdbc_url, jdbc_user, jdbc_secret_scope, jdbc_secret_key, uc_secret_name, connection_name,
           watermark_threshold_minutes, fetch_size, num_partitions, update_dttm, enabled)
         VALUES (s.ingestion_group, s.source_table_name, :staging_table_fqn, :target_table_fqn,
           :ingestion_type, :key_columns, :watermark_column, :partition_column, :predicate_column, :epic_csa_enabled, :auto_cdc_from_snapshot,
           :jdbc_url, :jdbc_user, :jdbc_secret_scope, :jdbc_secret_key, :uc_secret_name, :connection_name,
           :watermark_threshold_minutes, :fetch_size, :num_partitions, current_timestamp(), :enabled)`,
        [
          parameter('original_ingestion_group', body.originalIngestionGroup),
          parameter('original_source_table_name', body.originalSourceTableName),
          parameter('ingestion_group', body.ingestionGroup),
          parameter('source_table_name', body.sourceTableName),
          parameter('staging_table_fqn', body.stagingTableFqn),
          parameter('target_table_fqn', body.targetTableFqn),
          parameter('ingestion_type', body.ingestionType),
          parameter('key_columns', body.keyColumns),
          parameter('watermark_column', body.epicCsaEnabled ? null : body.watermarkColumn),
          parameter('partition_column', body.partitionColumn),
          parameter('predicate_column', body.predicateColumn),
          parameter('epic_csa_enabled', String(body.epicCsaEnabled), 'BOOLEAN'),
          parameter('auto_cdc_from_snapshot', String(body.autoCdcFromSnapshot), 'BOOLEAN'),
          parameter('jdbc_url', body.jdbcUrl),
          parameter('jdbc_user', body.jdbcUser),
          parameter('jdbc_secret_scope', body.jdbcSecretScope),
          parameter('jdbc_secret_key', body.jdbcSecretKey),
          parameter('uc_secret_name', body.ucSecretName),
          parameter('connection_name', body.connectionName),
          parameter('watermark_threshold_minutes', String(body.watermarkThresholdMinutes), 'INT'),
          parameter('fetch_size', String(body.fetchSize), 'INT'),
          parameter('num_partitions', String(body.numPartitions), 'INT'),
          parameter('enabled', String(body.enabled), 'BOOLEAN'),
        ]
      );
      logger.info('config.saved', {
        catalog: body.catalog,
        schema: body.schema,
        ingestionGroup: body.ingestionGroup,
        sourceTableName: body.sourceTableName,
      });
      res.json({ ok: true });
    })
  );

  app.delete(
    '/api/config',
    apiRoute('config_delete', async (req, res) => {
      const body = keySchema.parse(req.body);
      await execute(
        `DELETE FROM ${tableName(body.catalog, body.schema, 'jdbc_ingestion_config')} WHERE ingestion_group=:ingestion_group AND source_table_name=:source_table_name`,
        [parameter('ingestion_group', body.ingestionGroup), parameter('source_table_name', body.sourceTableName)]
      );
      logger.info('config.deleted', {
        catalog: body.catalog,
        schema: body.schema,
        ingestionGroup: body.ingestionGroup,
        sourceTableName: body.sourceTableName,
      });
      res.json({ ok: true });
    })
  );

  app.patch(
    '/api/watermark',
    apiRoute('watermark_update', async (req, res) => {
      const body = watermarkSchema.parse(req.body);
      await execute(
        `UPDATE ${tableName(body.catalog, body.schema, 'jdbc_ingestion_watermark')} SET last_watermark=:last_watermark, status=:status, last_run_timestamp=current_timestamp(), last_error=NULL WHERE ingestion_group=:ingestion_group AND source_table_name=:source_table_name`,
        [
          parameter('last_watermark', body.lastWatermark),
          parameter('status', body.status),
          parameter('ingestion_group', body.ingestionGroup),
          parameter('source_table_name', body.sourceTableName),
        ]
      );
      logger.info('watermark.updated', {
        catalog: body.catalog,
        schema: body.schema,
        ingestionGroup: body.ingestionGroup,
        sourceTableName: body.sourceTableName,
        status: body.status,
      });
      res.json({ ok: true });
    })
  );

  app.delete(
    '/api/watermark',
    apiRoute('watermark_delete', async (req, res) => {
      const body = keySchema.parse(req.body);
      await execute(
        `DELETE FROM ${tableName(body.catalog, body.schema, 'jdbc_ingestion_watermark')} WHERE ingestion_group=:ingestion_group AND source_table_name=:source_table_name`,
        [parameter('ingestion_group', body.ingestionGroup), parameter('source_table_name', body.sourceTableName)]
      );
      logger.info('watermark.deleted', {
        catalog: body.catalog,
        schema: body.schema,
        ingestionGroup: body.ingestionGroup,
        sourceTableName: body.sourceTableName,
      });
      res.json({ ok: true });
    })
  );
}
