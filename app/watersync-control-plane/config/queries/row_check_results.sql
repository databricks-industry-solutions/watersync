-- @param config_table STRING
-- @param row_check_table STRING
-- @param ingestion_group STRING
-- @param search STRING
-- @param tables STRING
-- @param status STRING
-- @param page_size INT
-- @param page_offset INT
-- @param refresh_token INT
-- The latest basic and detailed result of each enabled table matching the page filters: ingestion_group ('' for
-- all), search (a substring of the table name), tables (a JSON array of {"group", "table"}; empty for all) and
-- status ('' for all, 'attention' for a mismatch or error in either check, 'match', 'unchecked'). Problems come
-- first; total_count is the number of matching tables before paging.
WITH picked AS (
  SELECT from_json(nullif(:tables, ''), 'array<struct<group:string,table:string>>') AS tables
),
latest AS (
  SELECT
    *,
    row_number() OVER (
      PARTITION BY ingestion_group, source_table_name, check_mode
      ORDER BY checked_at DESC
    ) AS recency
  FROM IDENTIFIER(:row_check_table)
),
results AS (
  SELECT
    c.ingestion_group,
    c.source_table_name,
    c.target_table_fqn,
    c.key_columns,
    b.checked_at AS basic_checked_at,
    b.source_count AS basic_source_count,
    b.target_count AS basic_target_count,
    b.count_diff AS basic_count_diff,
    b.status AS basic_status,
    b.error AS basic_error,
    b.checked_range AS basic_checked_range,
    d.checked_at AS detailed_checked_at,
    d.source_count AS detailed_source_count,
    d.target_count AS detailed_target_count,
    d.missing_in_target AS detailed_missing_in_target,
    d.extra_in_target AS detailed_extra_in_target,
    d.mismatched_rows AS detailed_mismatched_rows,
    d.compared_columns AS detailed_compared_columns,
    d.sample_keys AS detailed_sample_keys,
    d.status AS detailed_status,
    d.error AS detailed_error,
    d.duration_ms AS detailed_duration_ms,
    d.chunk_column AS detailed_chunk_column,
    d.chunk_count AS detailed_chunk_count,
    d.mismatched_chunks AS detailed_mismatched_chunks,
    d.drilled_chunks AS detailed_drilled_chunks,
    d.chunk_details AS detailed_chunk_details,
    d.checked_range AS detailed_checked_range,
    -- Problems first: errors, then mismatches, then matches, then tables never checked.
    CASE
      WHEN 'ERROR' IN (b.status, d.status) THEN 0
      WHEN 'MISMATCH' IN (b.status, d.status) THEN 1
      WHEN coalesce(b.status, d.status) IS NOT NULL THEN 2
      ELSE 3
    END AS severity
  FROM IDENTIFIER(:config_table) c
  CROSS JOIN picked p
  LEFT JOIN latest b
    ON b.ingestion_group = c.ingestion_group AND b.source_table_name = c.source_table_name
    AND b.check_mode = 'basic' AND b.recency = 1
  LEFT JOIN latest d
    ON d.ingestion_group = c.ingestion_group AND d.source_table_name = c.source_table_name
    AND d.check_mode = 'detailed' AND d.recency = 1
  WHERE :refresh_token >= 0
    AND coalesce(c.enabled, true)
    AND (:ingestion_group = '' OR c.ingestion_group = :ingestion_group)
    AND (:search = '' OR lower(c.source_table_name) LIKE concat('%', lower(:search), '%'))
    AND (p.tables IS NULL OR size(p.tables) = 0
      OR exists(p.tables, t -> t.group = c.ingestion_group AND t.table = c.source_table_name))
)
SELECT *, count(*) OVER () AS total_count
FROM results
WHERE :status = ''
   OR (:status = 'attention' AND severity <= 1)
   OR (:status = 'match' AND severity = 2)
   OR (:status = 'unchecked' AND severity = 3)
ORDER BY severity, ingestion_group, source_table_name
LIMIT :page_size OFFSET :page_offset;
