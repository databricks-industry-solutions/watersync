-- @param table_name STRING
-- @param search STRING
-- @param page_size INT
-- @param page_offset INT
-- @param refresh_token INT
SELECT
  ingestion_group,
  source_table_name,
  staging_table_fqn,
  target_table_fqn,
  lower(coalesce(ingestion_type, 'incremental')) AS ingestion_type,
  key_columns,
  watermark_column,
  partition_column,
  predicate_column,
  coalesce(epic_csa_enabled, false) AS epic_csa_enabled,
  coalesce(auto_cdc_from_snapshot, false) AS auto_cdc_from_snapshot,
  jdbc_url,
  jdbc_user,
  jdbc_secret_scope,
  jdbc_secret_key,
  uc_secret_name,
  connection_name,
  coalesce(watermark_threshold_minutes, 5) AS watermark_threshold_minutes,
  coalesce(fetch_size, 10000) AS fetch_size,
  coalesce(num_partitions, 8) AS num_partitions,
  update_dttm,
  coalesce(enabled, true) AS enabled
FROM IDENTIFIER(:table_name)
WHERE :refresh_token >= 0 AND (:search = ''
   OR lower(ingestion_group) LIKE concat('%', lower(:search), '%')
   OR lower(source_table_name) LIKE concat('%', lower(:search), '%'))
ORDER BY ingestion_group, source_table_name
LIMIT :page_size OFFSET :page_offset;
