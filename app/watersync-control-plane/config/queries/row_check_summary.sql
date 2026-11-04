-- @param config_table STRING
-- @param row_check_table STRING
-- @param ingestion_group STRING
-- @param search STRING
-- @param tables STRING
-- @param refresh_token INT
-- Latest basic check of every enabled table matching the page filters (see row_check_results.sql), counted by
-- status, so the summary covers all of them and not only the rows the table shows.
WITH picked AS (
  SELECT from_json(nullif(:tables, ''), 'array<struct<group:string,table:string>>') AS tables
),
latest AS (
  SELECT ingestion_group, source_table_name, max_by(status, checked_at) AS status, max(checked_at) AS checked_at
  FROM IDENTIFIER(:row_check_table)
  WHERE check_mode = 'basic'
  GROUP BY ingestion_group, source_table_name
)
SELECT
  count(*) AS table_count,
  count(DISTINCT c.ingestion_group) AS group_count,
  count_if(l.status = 'MATCH') AS match_count,
  count_if(l.status = 'MISMATCH') AS mismatch_count,
  count_if(l.status = 'ERROR') AS error_count,
  max(l.checked_at) AS last_checked_at
FROM IDENTIFIER(:config_table) c
CROSS JOIN picked p
LEFT JOIN latest l ON l.ingestion_group = c.ingestion_group AND l.source_table_name = c.source_table_name
WHERE :refresh_token >= 0
  AND coalesce(c.enabled, true)
  AND (:ingestion_group = '' OR c.ingestion_group = :ingestion_group)
  AND (:search = '' OR lower(c.source_table_name) LIKE concat('%', lower(:search), '%'))
  AND (p.tables IS NULL OR size(p.tables) = 0
    OR exists(p.tables, t -> t.group = c.ingestion_group AND t.table = c.source_table_name));
