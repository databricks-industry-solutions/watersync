import type { Migration } from './index.js';

/**
 * Creates `_watersync_row_checks`, where the row check job appends one result
 * per table and run (see src/watersync/row_check.py, which creates the same
 * table when it runs first).
 */
export default {
  id: '004_create_row_checks',
  description: 'Create _watersync_row_checks table',
  async run(ctx, catalog, schema) {
    const target = ctx.tableName(catalog, schema, '_watersync_row_checks');
    await ctx.execute(
      `CREATE TABLE IF NOT EXISTS ${target} (
        check_id STRING,
        job_run_id STRING,
        check_mode STRING,
        checked_at TIMESTAMP,
        ingestion_group STRING,
        source_table_name STRING,
        target_table_fqn STRING,
        source_count BIGINT,
        target_count BIGINT,
        count_diff BIGINT,
        missing_in_target BIGINT,
        extra_in_target BIGINT,
        mismatched_rows BIGINT,
        compared_columns STRING,
        sample_keys STRING,
        status STRING,
        error STRING,
        duration_ms BIGINT,
        chunk_column STRING,
        chunk_count BIGINT,
        mismatched_chunks BIGINT,
        drilled_chunks BIGINT,
        chunk_details STRING,
        checked_range STRING
      )
      CLUSTER BY AUTO`
    );
  },
} satisfies Migration;
