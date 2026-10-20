-- @param config_table STRING
-- @param watermark_table STRING
-- @param ingestion_group STRING
-- @param search STRING
-- @param tables STRING
-- @param stale_hours INT
-- @param refresh_token INT
-- Page filters, as on every page: ingestion_group ('' for all), search (a substring of the table name) and
-- tables (a JSON array of {"group", "table"}; empty for all). Load counts cover enabled tables only, from each
-- table's latest run in the watermark table; stale means its last run is older than stale_hours.
WITH picked AS (
  SELECT from_json(nullif(:tables, ''), 'array<struct<group:string,table:string>>') AS tables
),
configs AS (
  SELECT c.* FROM IDENTIFIER(:config_table) c CROSS JOIN picked p
  WHERE :refresh_token >= 0
  AND (:ingestion_group = '' OR c.ingestion_group = :ingestion_group)
  AND (:search = '' OR lower(c.source_table_name) LIKE concat('%', lower(:search), '%'))
  AND (p.tables IS NULL OR size(p.tables) = 0
    OR exists(p.tables, x -> x.group = c.ingestion_group AND x.table = c.source_table_name))
),
latest AS (
  SELECT ingestion_group, source_table_name, max_by(status, last_run_timestamp) AS status,
    max(last_run_timestamp) AS last_run_timestamp
  FROM IDENTIFIER(:watermark_table)
  GROUP BY ingestion_group, source_table_name
),
state AS (
  SELECT c.ingestion_group, coalesce(c.enabled, true) AS enabled, l.status, l.last_run_timestamp
  FROM configs c
  LEFT JOIN latest l ON l.ingestion_group = c.ingestion_group AND l.source_table_name = c.source_table_name
)
SELECT
  count(*) AS config_count,
  count_if(enabled) AS enabled_count,
  count(DISTINCT ingestion_group) AS group_count,
  count_if(enabled AND status = 'FAILED') AS failed_count,
  count_if(enabled AND status = 'SUCCESS') AS success_count,
  count_if(enabled AND status = 'SKIPPED') AS skipped_count,
  count_if(enabled AND status = 'FULL_REFRESH') AS full_refresh_count,
  count_if(enabled AND last_run_timestamp IS NULL) AS never_loaded_count,
  count_if(enabled AND last_run_timestamp < current_timestamp() - make_dt_interval(0, :stale_hours)) AS stale_count,
  max(last_run_timestamp) AS last_run_timestamp
FROM state;
