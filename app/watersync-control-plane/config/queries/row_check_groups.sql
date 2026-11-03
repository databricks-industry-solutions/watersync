-- @param config_table STRING
-- @param row_check_table STRING
-- @param status STRING
-- @param page_size INT
-- @param page_offset INT
-- @param refresh_token INT
-- One row per ingestion group with the latest results of its enabled tables added up: the Row checks page shows
-- it when no group or table is selected. status ('' for all, 'attention', 'match', 'unchecked') keeps groups with
-- at least one table in that state. Groups with problems come first; total_count is the number of matching
-- groups before paging.
WITH latest AS (
  SELECT
    ingestion_group,
    source_table_name,
    check_mode,
    max_by(status, checked_at) AS status,
    max_by(source_count, checked_at) AS source_count,
    max_by(target_count, checked_at) AS target_count,
    max_by(count_diff, checked_at) AS count_diff,
    max(checked_at) AS checked_at
  FROM IDENTIFIER(:row_check_table)
  GROUP BY ingestion_group, source_table_name, check_mode
),
tables AS (
  SELECT
    c.ingestion_group,
    b.status AS basic_status,
    d.status AS detailed_status,
    b.source_count,
    b.target_count,
    b.count_diff,
    b.checked_at,
    CASE
      WHEN 'ERROR' IN (b.status, d.status) THEN 'error'
      WHEN 'MISMATCH' IN (b.status, d.status) THEN 'mismatch'
      WHEN coalesce(b.status, d.status) IS NOT NULL THEN 'match'
      ELSE 'unchecked'
    END AS state
  FROM IDENTIFIER(:config_table) c
  LEFT JOIN latest b
    ON b.ingestion_group = c.ingestion_group AND b.source_table_name = c.source_table_name AND b.check_mode = 'basic'
  LEFT JOIN latest d
    ON d.ingestion_group = c.ingestion_group AND d.source_table_name = c.source_table_name AND d.check_mode = 'detailed'
  WHERE :refresh_token >= 0 AND coalesce(c.enabled, true)
),
groups AS (
  SELECT
    ingestion_group,
    count(*) AS table_count,
    count_if(state = 'match') AS match_count,
    count_if(state = 'mismatch') AS mismatch_count,
    count_if(state = 'error') AS error_count,
    count_if(state = 'unchecked') AS unchecked_count,
    sum(source_count) AS source_count,
    sum(target_count) AS target_count,
    -- Absolute, so tables off in opposite directions do not cancel out.
    sum(abs(count_diff)) AS rows_out_of_sync,
    max(checked_at) AS last_checked_at
  FROM tables
  GROUP BY ingestion_group
)
SELECT *, count(*) OVER () AS total_count
FROM groups
WHERE :status = ''
   OR (:status = 'attention' AND mismatch_count + error_count > 0)
   OR (:status = 'match' AND match_count > 0)
   OR (:status = 'unchecked' AND unchecked_count > 0)
ORDER BY error_count DESC, mismatch_count DESC, ingestion_group
LIMIT :page_size OFFSET :page_offset;
