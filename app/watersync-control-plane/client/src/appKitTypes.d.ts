import { sql } from '@databricks/appkit-ui/js';

type StringParam = ReturnType<typeof sql.string>;
type IntParam = ReturnType<typeof sql.int>;

declare module '@databricks/appkit-ui/react' {
  interface QueryRegistry {
    config_entries: {
      name: 'config_entries';
      parameters: {
        table_name: StringParam;
        search: StringParam;
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
        watermark_threshold_minutes: number;
        fetch_size: number;
        num_partitions: number;
        update_dttm: string | null;
        enabled: boolean;
      }>;
    };
    watermark_entries: {
      name: 'watermark_entries';
      parameters: {
        table_name: StringParam;
        search: StringParam;
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
      }>;
    };
    config_summary: {
      name: 'config_summary';
      parameters: { config_table: StringParam; watermark_table: StringParam; refresh_token: IntParam };
      result: Array<{
        config_count: number;
        enabled_count: number;
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
  }
}

export {};
