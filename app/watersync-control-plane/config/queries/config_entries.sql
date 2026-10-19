-- @param table_name STRING
-- @param ingestion_group STRING
-- @param search STRING
-- @param tables STRING
-- @param page_size INT
-- @param page_offset INT
-- @param refresh_token INT
-- Page filters, as on every page: ingestion_group ('' for all), search (a substring of the table name) and
-- tables (a JSON array of {"group", "table"}; empty for all).
-- One page of rows; total_count is the number of matching rows before paging.
WITH picked AS (
  SELECT from_json(nullif(:tables, ''), 'array<struct<group:string,table:string>>') AS tables
)
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
  connection_database,
  coalesce(watermark_threshold_minutes, 5) AS watermark_threshold_minutes,
  coalesce(fetch_size, 10000) AS fetch_size,
  coalesce(num_partitions, 8) AS num_partitions,
  update_dttm,
  coalesce(enabled, true) AS enabled,
  count(*) OVER () AS total_count
FROM IDENTIFIER(:table_name) c
CROSS JOIN picked p
WHERE :refresh_token >= 0
  AND (:ingestion_group = '' OR c.ingestion_group = :ingestion_group)
  AND (:search = '' OR lower(c.source_table_name) LIKE concat('%', lower(:search), '%'))
  AND (p.tables IS NULL OR size(p.tables) = 0
    OR exists(p.tables, x -> x.group = c.ingestion_group AND x.table = c.source_table_name))
ORDER BY c.ingestion_group, c.source_table_name
LIMIT :page_size OFFSET :page_offset;
