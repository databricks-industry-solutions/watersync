export type Location = { catalog: string; schema: string };
export type Context = { location: Location; setLocation: (location: Location) => void; revision: number; refresh: () => void };
export type ConfigRow = {
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
};
export type WatermarkRow = {
  ingestion_group: string;
  source_table_name: string;
  staging_table_fqn: string | null;
  ingestion_type: string;
  last_watermark: string | null;
  last_run_timestamp: string | null;
  status: string | null;
  last_error: string | null;
};
export type JobRun = {
  run_id?: number;
  run_url?: string;
  run_name?: string;
  start_time?: number;
  end_time?: number;
  setup_duration?: number;
  execution_duration?: number;
  cleanup_duration?: number;
  state?: { life_cycle_state?: string; result_state?: string; state_message?: string };
};
export type JobSchedule = {
  quartz_cron_expression: string;
  timezone_id: string;
  pause_status?: 'PAUSED' | 'UNPAUSED';
};
export type JobRow = {
  job_id?: number;
  settings?: {
    name?: string;
    schedule?: JobSchedule;
  };
  creator_user_name?: string;
  created_time?: number;
  workspace_url?: string;
  runs: JobRun[];
};
export type SourceTable = { table_schema: string; table_name: string };
export type SourceColumn = {
  column_name: string;
  data_type: string;
  is_nullable: string;
  ordinal_position: string;
  is_primary_key: string;
};
export type TableDraft = {
  table: SourceTable;
  columns: SourceColumn[];
  keyColumn: string;
  watermarkColumn: string;
  partitionColumn: string;
  predicateColumn: string;
  targetFqn: string;
  stagingFqn: string;
};
export type InferenceProgress = { completed: number; total: number };
