import type { Migration } from './index.js';

/**
 * Adds the `uc_secret_name` column to `jdbc_ingestion_config` so that UC secrets
 * (catalog.schema.secret_name) can be stored alongside the legacy Databricks
 * secret scope / key pair.
 */
export default {
  id: '002_add_uc_secret_name',
  description: 'Add uc_secret_name column to jdbc_ingestion_config (for pre-existing tables)',
  async run(ctx, catalog, schema) {
    const target = ctx.tableName(catalog, schema, 'jdbc_ingestion_config');
    // Check whether the column already exists (Databricks SQL does not support
    // ADD COLUMN IF NOT EXISTS).
    const response = await ctx.execute(
      `SELECT column_name FROM ${catalog}.information_schema.columns
       WHERE table_catalog = '${catalog}' AND table_schema = '${schema}'
         AND table_name = 'jdbc_ingestion_config' AND column_name = 'uc_secret_name'`
    );
    const rows = (response as { result?: { data_array?: string[][] } }).result?.data_array ?? [];
    if (rows.length > 0) return; // column already exists
    await ctx.execute(
      `ALTER TABLE ${target} ADD COLUMNS (uc_secret_name STRING)`
    );
  },
} satisfies Migration;