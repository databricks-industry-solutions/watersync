-- @param config_table STRING
-- @param ingestion_group STRING
-- @param search STRING
-- @param enabled_only BOOLEAN
-- @param refresh_token INT
-- Tables offered by the filter bar's table picker: at most 50 per search. Row checks only offer enabled tables.
SELECT ingestion_group, source_table_name
FROM IDENTIFIER(:config_table)
WHERE :refresh_token >= 0
  AND (NOT :enabled_only OR coalesce(enabled, true))
  AND (:ingestion_group = '' OR ingestion_group = :ingestion_group)
  AND (:search = '' OR lower(source_table_name) LIKE concat('%', lower(:search), '%'))
ORDER BY ingestion_group, source_table_name
LIMIT 50;
