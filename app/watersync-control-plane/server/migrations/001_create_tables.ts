import type { Migration } from './index.js';

/**
 * Creates the two WaterSync control-plane tables — `jdbc_ingestion_config` and
 * `jdbc_ingestion_watermark` — if they do not already exist in the target
 * catalog.schema.
 *
 * For fresh installations the tables are created with the full column set
 * (including `uc_secret_name`).  For existing installations the
 * `CREATE TABLE IF NOT EXISTS` is a no-op and migration 002 adds any missing
 * columns.
 */
export default {
  id: '001_create_tables',
  description: 'Create jdbc_ingestion_config and jdbc_ingestion_watermark tables',
  async run(ctx, catalog, schema) {
    const configTable = ctx.tableName(catalog, schema, 'jdbc_ingestion_config');
    await ctx.execute(
      `CREATE TABLE IF NOT EXISTS ${configTable} (
        ingestion_group STRING NOT NULL,
        source_table_name STRING NOT NULL,
        staging_table_fqn STRING,
        target_table_fqn STRING NOT NULL,
        ingestion_type STRING NOT NULL,
        key_columns STRING,
        watermark_column STRING,
        partition_column STRING,
        predicate_column STRING,
        epic_csa_enabled BOOLEAN NOT NULL,
        auto_cdc_from_snapshot BOOLEAN NOT NULL,
        jdbc_url STRING,
        jdbc_user STRING,
        jdbc_secret_scope STRING,
        jdbc_secret_key STRING,
        uc_secret_name STRING,
        connection_name STRING,
        watermark_threshold_minutes INT NOT NULL,
        fetch_size INT NOT NULL,
        num_partitions INT NOT NULL,
        update_dttm TIMESTAMP,
        enabled BOOLEAN NOT NULL
      )`
    );

    const watermarkTable = ctx.tableName(catalog, schema, 'jdbc_ingestion_watermark');
    await ctx.execute(
      `CREATE TABLE IF NOT EXISTS ${watermarkTable} (
        ingestion_group STRING NOT NULL,
        source_table_name STRING NOT NULL,
        staging_table_fqn STRING,
        ingestion_type STRING,
        last_watermark STRING,
        last_run_timestamp TIMESTAMP,
        status STRING,
        last_error STRING
      )`
    );
  },
} satisfies Migration;