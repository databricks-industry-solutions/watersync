-- @param table_name STRING
-- @param ingestion_group STRING
-- @param search STRING
-- @param tables STRING
-- @param page_size INT
-- @param page_offset INT
-- @param refresh_token INT
-- Page filters, as on every page: ingestion_group ('' for all), search (a substring of the table name) and
-- tables (a JSON array of {"group", "table"}; empty for all).
-- One page of rows, most recently run first; total_count is the number of matching rows before paging.
WITH picked AS (
  SELECT from_json(nullif(:tables, ''), 'array<struct<group:string,table:string>>') AS tables
)
SELECT
  ingestion_group,
  source_table_name,
  staging_table_fqn,
  ingestion_type,
  last_watermark,
  last_run_timestamp,
  status,
  last_error,
  count(*) OVER () AS total_count
FROM IDENTIFIER(:table_name) c
CROSS JOIN picked p
WHERE :refresh_token >= 0
  AND (:ingestion_group = '' OR c.ingestion_group = :ingestion_group)
  AND (:search = '' OR lower(c.source_table_name) LIKE concat('%', lower(:search), '%'))
  AND (p.tables IS NULL OR size(p.tables) = 0
    OR exists(p.tables, x -> x.group = c.ingestion_group AND x.table = c.source_table_name))
ORDER BY c.last_run_timestamp DESC NULLS LAST, c.ingestion_group, c.source_table_name
LIMIT :page_size OFFSET :page_offset;
