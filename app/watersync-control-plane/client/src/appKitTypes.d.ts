import { sql } from '@databricks/appkit-ui/js';

type StringParam = ReturnType<typeof sql.string>;
type IntParam = ReturnType<typeof sql.int>;
type BooleanParam = ReturnType<typeof sql.boolean>;

declare module '@databricks/appkit-ui/react' {
  interface QueryRegistry {
    config_entries: {
      name: 'config_entries';
      parameters: {
        table_name: StringParam;
        ingestion_group: StringParam;
        search: StringParam;
        tables: StringParam;
        page_size: IntParam;
        page_offset: IntParam;
        refresh_token: IntParam;
      };
      result: Array<{
        ingestion_group: string;
        source_table_name: string;
        staging_table_fqn: string | null;
        target_table_fqn: string;
        ingestion_type: string;
        key_columns: string | null;
        watermark_column: string | null;
        partition_column: string | null;
        predicate_column: string | null;
        epic_csa_enabled: boolean;
        auto_cdc_from_snapshot: boolean;
        jdbc_url: string | null;
        jdbc_user: string | null;
        jdbc_secret_scope: string | null;
        jdbc_secret_key: string | null;
        uc_secret_name: string | null;
        connection_name: string | null;
        connection_database: string | null;
        watermark_threshold_minutes: number;
        fetch_size: number;
        num_partitions: number;
        update_dttm: string | null;
        enabled: boolean;
        total_count: number | string;
      }>;
    };
    watermark_entries: {
      name: 'watermark_entries';
      parameters: {
        table_name: StringParam;
        ingestion_group: StringParam;
        search: StringParam;
        tables: StringParam;
        page_size: IntParam;
        page_offset: IntParam;
        refresh_token: IntParam;
      };
      result: Array<{
        ingestion_group: string;
        source_table_name: string;
        staging_table_fqn: string | null;
        ingestion_type: string;
        last_watermark: string | null;
        last_run_timestamp: string | null;
        status: string | null;
        last_error: string | null;
        total_count: number | string;
      }>;
    };
    config_summary: {
      name: 'config_summary';
      parameters: {
        config_table: StringParam;
        watermark_table: StringParam;
        ingestion_group: StringParam;
        search: StringParam;
        tables: StringParam;
        refresh_token: IntParam;
      };
      result: Array<{
        config_count: number;
        enabled_count: number;
        group_count: number;
        failed_count: number;
        last_run_timestamp: string | null;
      }>;
    };
    ingestion_groups: {
      name: 'ingestion_groups';
      parameters: { table_name: StringParam; refresh_token: IntParam };
      result: Array<{
        ingestion_group: string;
        source_count: number;
        enabled_source_count: number;
        enabled_incremental_source_count: number;
      }>;
    };
    row_check_daily: {
      name: 'row_check_daily';
      parameters: {
        config_table: StringParam;
        row_check_table: StringParam;
        days: IntParam;
        ingestion_group: StringParam;
        search: StringParam;
        tables: StringParam;
        top: IntParam;
        level: StringParam;
        refresh_token: IntParam;
      };
      result: Array<{
        day: string;
        ingestion_group: string;
        source_table_name: string;
        is_other: boolean | string;
        entity_count: number | string;
        source_count: number | string | null;
        target_count: number | string | null;
        rows_ingested: number | string | null;
        count_diff: number | string | null;
      }>;
    };
    row_check_status_map: {
      name: 'row_check_status_map';
      parameters: {
        config_table: StringParam;
        row_check_table: StringParam;
        ingestion_group: StringParam;
        search: StringParam;
        refresh_token: IntParam;
      };
      result: Array<{ ingestion_group: string; source_table_name: string; state: string }>;
    };
    table_options: {
      name: 'table_options';
      parameters: {
        config_table: StringParam;
        ingestion_group: StringParam;
        search: StringParam;
        enabled_only: BooleanParam;
        refresh_token: IntParam;
      };
      result: Array<{ ingestion_group: string; source_table_name: string }>;
    };
    job_groups: {
      name: 'job_groups';
      parameters: {
        config_table: StringParam;
        ingestion_group: StringParam;
        search: StringParam;
        tables: StringParam;
        page_size: IntParam;
        page_offset: IntParam;
        refresh_token: IntParam;
      };
      result: Array<{
        ingestion_group: string;
        source_count: number | string;
        enabled_source_count: number | string;
        total_count: number | string;
      }>;
    };
    row_check_results: {
      name: 'row_check_results';
      parameters: {
        config_table: StringParam;
        row_check_table: StringParam;
        ingestion_group: StringParam;
        search: StringParam;
        tables: StringParam;
        status: StringParam;
        page_size: IntParam;
        page_offset: IntParam;
        refresh_token: IntParam;
      };
      result: Array<{
        ingestion_group: string;
        source_table_name: string;
        target_table_fqn: string;
        key_columns: string | null;
        basic_checked_at: string | null;
        basic_source_count: number | null;
        basic_target_count: number | null;
        basic_count_diff: number | null;
        basic_status: string | null;
        basic_error: string | null;
        basic_checked_range: string | null;
        detailed_checked_at: string | null;
        detailed_source_count: number | null;
        detailed_target_count: number | null;
        detailed_missing_in_target: number | null;
        detailed_extra_in_target: number | null;
        detailed_mismatched_rows: number | null;
        detailed_compared_columns: string | null;
        // JSON columns: the analytics plugin returns them parsed (see lib/jsonColumn.ts).
        detailed_sample_keys: unknown;
        detailed_status: string | null;
        detailed_error: string | null;
        detailed_duration_ms: number | null;
        detailed_chunk_column: string | null;
        detailed_chunk_count: number | null;
        detailed_mismatched_chunks: number | null;
        detailed_drilled_chunks: number | null;
        detailed_chunk_details: unknown;
        detailed_checked_range: string | null;
        severity: number | string;
        total_count: number | string;
      }>;
    };
    row_check_summary: {
      name: 'row_check_summary';
      parameters: {
        config_table: StringParam;
        row_check_table: StringParam;
        ingestion_group: StringParam;
        search: StringParam;
        tables: StringParam;
        refresh_token: IntParam;
      };
      result: Array<{
        table_count: number | string;
        group_count: number | string;
        match_count: number | string;
        mismatch_count: number | string;
        error_count: number | string;
        last_checked_at: string | null;
      }>;
    };
    row_check_groups: {
      name: 'row_check_groups';
      parameters: {
        config_table: StringParam;
        row_check_table: StringParam;
        status: StringParam;
        page_size: IntParam;
        page_offset: IntParam;
        refresh_token: IntParam;
      };
      result: Array<{
        ingestion_group: string;
        table_count: number | string;
        match_count: number | string;
        mismatch_count: number | string;
        error_count: number | string;
        unchecked_count: number | string;
        source_count: number | string | null;
        target_count: number | string | null;
        rows_out_of_sync: number | string | null;
        last_checked_at: string | null;
        total_count: number | string;
      }>;
    };
  }
}

export {};
