import type { Migration } from './index.js';

/**
 * Adds the `connection_database` column to `jdbc_ingestion_config` so a config
 * row using a UC connection can read from a database (Oracle: service name)
 * other than the one defined on the connection.
 */
export default {
  id: '003_add_connection_database',
  description: 'Add connection_database column to jdbc_ingestion_config',
  async run(ctx, catalog, schema) {
    const target = ctx.tableName(catalog, schema, 'jdbc_ingestion_config');
    // Databricks SQL does not support ADD COLUMN IF NOT EXISTS.
    const response = await ctx.execute(
      `SELECT column_name FROM ${catalog}.information_schema.columns
       WHERE table_catalog = '${catalog}' AND table_schema = '${schema}'
         AND table_name = 'jdbc_ingestion_config' AND column_name = 'connection_database'`
    );
    const rows = (response as { result?: { data_array?: string[][] } }).result?.data_array ?? [];
    if (rows.length > 0) return; // column already exists
    await ctx.execute(`ALTER TABLE ${target} ADD COLUMNS (connection_database STRING)`);
  },
} satisfies Migration;
