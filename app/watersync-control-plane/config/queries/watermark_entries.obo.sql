-- @param table_name STRING
-- @param search STRING
-- @param page_size INT
-- @param page_offset INT
-- @param refresh_token INT
SELECT
  ingestion_group,
  source_table_name,
  staging_table_fqn,
  ingestion_type,
  last_watermark,
  last_run_timestamp,
  status,
  last_error
FROM IDENTIFIER(:table_name)
WHERE :refresh_token >= 0 AND (:search = ''
   OR lower(ingestion_group) LIKE concat('%', lower(:search), '%')
   OR lower(source_table_name) LIKE concat('%', lower(:search), '%'))
ORDER BY last_run_timestamp DESC NULLS LAST, ingestion_group, source_table_name
LIMIT :page_size OFFSET :page_offset;
