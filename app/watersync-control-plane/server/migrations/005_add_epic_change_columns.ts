import type { Migration } from './index.js';

const columns = [
  ['epic_change_method', 'STRING'],
  ['epic_change_procedure', 'STRING'],
  ['epic_change_skip_update_date', 'BOOLEAN'],
  ['epic_change_column_names', 'STRING'],
] as const;

/**
 * Adds the columns that let an EPIC CSA row load changes through Epic's stored procedure
 * instead of the CSA tables.
 */
export default {
  id: '005_add_epic_change_columns',
  description: 'Add EPIC stored-procedure columns to jdbc_ingestion_config',
  async run(ctx, catalog, schema) {
    const target = ctx.tableName(catalog, schema, 'jdbc_ingestion_config');
    // Databricks SQL does not support ADD COLUMN IF NOT EXISTS; the Python setup may have added some already.
    const response = await ctx.execute(
      `SELECT column_name FROM ${catalog}.information_schema.columns
       WHERE table_catalog = '${catalog}' AND table_schema = '${schema}'
         AND table_name = 'jdbc_ingestion_config'
         AND column_name IN (${columns.map(([name]) => `'${name}'`).join(', ')})`
    );
    const rows = (response as { result?: { data_array?: string[][] } }).result?.data_array ?? [];
    const existing = new Set(rows.map((row) => row[0]));
    const missing = columns.filter(([name]) => !existing.has(name));
    if (!missing.length) return;
    await ctx.execute(
      `ALTER TABLE ${target} ADD COLUMNS (${missing.map(([name, type]) => `${name} ${type}`).join(', ')})`
    );
  },
} satisfies Migration;
