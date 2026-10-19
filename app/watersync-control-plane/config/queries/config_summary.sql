-- @param config_table STRING
-- @param watermark_table STRING
-- @param ingestion_group STRING
-- @param search STRING
-- @param tables STRING
-- @param refresh_token INT
-- Page filters, as on every page: ingestion_group ('' for all), search (a substring of the table name) and
-- tables (a JSON array of {"group", "table"}; empty for all).
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
watermarks AS (
  SELECT c.* FROM IDENTIFIER(:watermark_table) c CROSS JOIN picked p
  WHERE :refresh_token >= 0
  AND (:ingestion_group = '' OR c.ingestion_group = :ingestion_group)
  AND (:search = '' OR lower(c.source_table_name) LIKE concat('%', lower(:search), '%'))
  AND (p.tables IS NULL OR size(p.tables) = 0
    OR exists(p.tables, x -> x.group = c.ingestion_group AND x.table = c.source_table_name))
)
SELECT
  (SELECT count(*) FROM configs) AS config_count,
  (SELECT count(*) FROM configs WHERE enabled = true) AS enabled_count,
  (SELECT count(DISTINCT ingestion_group) FROM configs) AS group_count,
  (SELECT count(*) FROM watermarks WHERE status = 'FAILED') AS failed_count,
  (SELECT max(last_run_timestamp) FROM watermarks) AS last_run_timestamp;
