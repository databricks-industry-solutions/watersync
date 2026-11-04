-- @param config_table STRING
-- @param watermark_table STRING
-- @param row_check_table STRING
-- @param ingestion_group STRING
-- @param search STRING
-- @param tables STRING
-- @param stale_hours INT
-- @param page_size INT
-- @param page_offset INT
-- @param refresh_token INT
-- One row per ingestion group with tables matching the page filters: its tables, the latest load of its enabled
-- tables (from the watermark table) and their latest basic row check. Groups with problems come first;
-- total_count is the number of groups before paging.
WITH picked AS (
  SELECT from_json(nullif(:tables, ''), 'array<struct<group:string,table:string>>') AS tables
),
loads AS (
  SELECT ingestion_group, source_table_name, max_by(status, last_run_timestamp) AS status,
    max(last_run_timestamp) AS last_run_timestamp
  FROM IDENTIFIER(:watermark_table)
  GROUP BY ingestion_group, source_table_name
),
checks AS (
  SELECT ingestion_group, source_table_name, max_by(status, checked_at) AS status, max(checked_at) AS checked_at
  FROM IDENTIFIER(:row_check_table)
  WHERE check_mode = 'basic'
  GROUP BY ingestion_group, source_table_name
),
tables_state AS (
  SELECT
    c.ingestion_group,
    coalesce(c.enabled, true) AS enabled,
    l.status AS load_status,
    l.last_run_timestamp,
    k.status AS check_status,
    k.checked_at
  FROM IDENTIFIER(:config_table) c
  CROSS JOIN picked p
  LEFT JOIN loads l ON l.ingestion_group = c.ingestion_group AND l.source_table_name = c.source_table_name
  LEFT JOIN checks k ON k.ingestion_group = c.ingestion_group AND k.source_table_name = c.source_table_name
  WHERE :refresh_token >= 0
    AND (:ingestion_group = '' OR c.ingestion_group = :ingestion_group)
    AND (:search = '' OR lower(c.source_table_name) LIKE concat('%', lower(:search), '%'))
    AND (p.tables IS NULL OR size(p.tables) = 0
      OR exists(p.tables, x -> x.group = c.ingestion_group AND x.table = c.source_table_name))
),
groups AS (
  SELECT
    ingestion_group,
    count(*) AS table_count,
    count_if(enabled) AS enabled_count,
    count_if(enabled AND load_status = 'FAILED') AS failed_count,
    count_if(enabled AND last_run_timestamp IS NULL) AS never_loaded_count,
    count_if(enabled AND last_run_timestamp < current_timestamp() - make_dt_interval(0, :stale_hours)) AS stale_count,
    min(CASE WHEN enabled THEN last_run_timestamp END) AS oldest_load,
    max(CASE WHEN enabled THEN last_run_timestamp END) AS latest_load,
    count_if(enabled AND check_status = 'MATCH') AS match_count,
    count_if(enabled AND check_status = 'MISMATCH') AS mismatch_count,
    count_if(enabled AND check_status = 'ERROR') AS check_error_count,
    count_if(enabled AND check_status IS NULL) AS unchecked_count,
    max(CASE WHEN enabled THEN checked_at END) AS last_checked_at
  FROM tables_state
  GROUP BY ingestion_group
)
SELECT *, count(*) OVER () AS total_count
FROM groups
ORDER BY failed_count + mismatch_count + check_error_count DESC, stale_count + never_loaded_count DESC,
  ingestion_group
LIMIT :page_size OFFSET :page_offset;
