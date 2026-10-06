-- @param table_name STRING
-- @param refresh_token INT
SELECT
  ingestion_group,
  count(*) AS source_count,
  count_if(coalesce(enabled, true)) AS enabled_source_count,
  count_if(coalesce(enabled, true) AND lower(ingestion_type) = 'incremental') AS enabled_incremental_source_count
FROM IDENTIFIER(:table_name)
WHERE :refresh_token >= 0
GROUP BY ingestion_group
ORDER BY ingestion_group;
