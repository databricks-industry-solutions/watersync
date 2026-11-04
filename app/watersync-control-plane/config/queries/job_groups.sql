-- @param config_table STRING
-- @param ingestion_group STRING
-- @param search STRING
-- @param tables STRING
-- @param page_size INT
-- @param page_offset INT
-- @param refresh_token INT
-- Page filters, as on every page: ingestion_group ('' for all), search (a substring of the table name) and
-- tables (a JSON array of {"group", "table"}; empty for all).
-- One page of the ingestion groups that have at least one matching table: each has at most one generated job.
-- total_count is the number of matching groups before paging.
WITH picked AS (
  SELECT from_json(nullif(:tables, ''), 'array<struct<group:string,table:string>>') AS tables
)
SELECT
  c.ingestion_group,
  count(*) AS source_count,
  count_if(coalesce(c.enabled, true)) AS enabled_source_count,
  count(*) OVER () AS total_count
FROM IDENTIFIER(:config_table) c
CROSS JOIN picked p
WHERE :refresh_token >= 0
  AND (:ingestion_group = '' OR c.ingestion_group = :ingestion_group)
  AND (:search = '' OR lower(c.source_table_name) LIKE concat('%', lower(:search), '%'))
  AND (p.tables IS NULL OR size(p.tables) = 0
    OR exists(p.tables, x -> x.group = c.ingestion_group AND x.table = c.source_table_name))
GROUP BY c.ingestion_group
ORDER BY c.ingestion_group
LIMIT :page_size OFFSET :page_offset;
