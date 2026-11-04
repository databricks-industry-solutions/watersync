-- @param config_table STRING
-- @param watermark_table STRING
-- @param ingestion_group STRING
-- @param search STRING
-- @param tables STRING
-- @param stale_hours INT
-- @param refresh_token INT
-- The enabled tables matching the page filters whose latest load needs a look: failed, never loaded, or last run
-- older than stale_hours. Failures first, then never loaded, then the oldest; total_count counts all of them.
WITH picked AS (
  SELECT from_json(nullif(:tables, ''), 'array<struct<group:string,table:string>>') AS tables
),
latest AS (
  SELECT ingestion_group, source_table_name, max_by(status, last_run_timestamp) AS status,
    max_by(last_error, last_run_timestamp) AS last_error, max(last_run_timestamp) AS last_run_timestamp
  FROM IDENTIFIER(:watermark_table)
  GROUP BY ingestion_group, source_table_name
),
attention AS (
  SELECT
    c.ingestion_group,
    c.source_table_name,
    lower(coalesce(c.ingestion_type, 'incremental')) AS ingestion_type,
    l.status,
    l.last_run_timestamp,
    l.last_error,
    CASE
      WHEN l.status = 'FAILED' THEN 'failed'
      WHEN l.last_run_timestamp IS NULL THEN 'never_loaded'
      ELSE 'stale'
    END AS reason
  FROM IDENTIFIER(:config_table) c
  CROSS JOIN picked p
  LEFT JOIN latest l ON l.ingestion_group = c.ingestion_group AND l.source_table_name = c.source_table_name
  WHERE :refresh_token >= 0
    AND coalesce(c.enabled, true)
    AND (:ingestion_group = '' OR c.ingestion_group = :ingestion_group)
    AND (:search = '' OR lower(c.source_table_name) LIKE concat('%', lower(:search), '%'))
    AND (p.tables IS NULL OR size(p.tables) = 0
      OR exists(p.tables, x -> x.group = c.ingestion_group AND x.table = c.source_table_name))
    AND (l.status = 'FAILED' OR l.last_run_timestamp IS NULL
      OR l.last_run_timestamp < current_timestamp() - make_dt_interval(0, :stale_hours))
)
SELECT *, count(*) OVER () AS total_count
FROM attention
ORDER BY CASE reason WHEN 'failed' THEN 0 WHEN 'never_loaded' THEN 1 ELSE 2 END, last_run_timestamp,
  ingestion_group, source_table_name
LIMIT 8;
