-- @param config_table STRING
-- @param row_check_table STRING
-- @param ingestion_group STRING
-- @param search STRING
-- @param refresh_token INT
-- One row per enabled table for the Row checks status map: its worst latest result across the basic and the
-- detailed check (error, mismatch, match, unchecked). Follows the group and name filters, but not picked tables,
-- so the map keeps showing every table while one is picked. At most 5000 tables.
WITH latest AS (
  SELECT ingestion_group, source_table_name, check_mode, max_by(status, checked_at) AS status
  FROM IDENTIFIER(:row_check_table)
  GROUP BY ingestion_group, source_table_name, check_mode
)
SELECT
  c.ingestion_group,
  c.source_table_name,
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
WHERE :refresh_token >= 0
  AND coalesce(c.enabled, true)
  AND (:ingestion_group = '' OR c.ingestion_group = :ingestion_group)
  AND (:search = '' OR lower(c.source_table_name) LIKE concat('%', lower(:search), '%'))
ORDER BY c.ingestion_group, c.source_table_name
LIMIT 5000;
