-- @param config_table STRING
-- @param row_check_table STRING
-- @param days INT
-- @param ingestion_group STRING
-- @param search STRING
-- @param tables STRING
-- @param top INT
-- @param level STRING
-- @param refresh_token INT
-- Per day (UTC) and enabled table: the day's last whole-table basic check, and the rows the target gained since
-- the previous checked day (net of deletes). One extra day is read so the first day shown has a previous day; a
-- day with no earlier day falls back to the growth between its own first and last check.
--
-- ingestion_group narrows to one group ('' for all) and search to tables whose name contains it. tables is a JSON array of {"group", "table"} to chart.
-- level is 'group' (one series per group, its tables added up) or 'table' (one per table). Unless tables are
-- picked, the :top series with the most rows ingested are kept by name and the others add up into one "Other"
-- series. A series made of several tables (a group, or Other) adds up absolute differences, so opposite ones
-- cannot cancel out. entity_count is the number of groups or tables matching the filters, before that folding.
WITH picked AS (
  SELECT from_json(nullif(:tables, ''), 'array<struct<group:string,table:string>>') AS tables
),
daily AS (
  SELECT
    r.ingestion_group,
    r.source_table_name,
    to_date(r.checked_at) AS day,
    max_by(r.source_count, r.checked_at) AS source_count,
    max_by(r.target_count, r.checked_at) AS target_count,
    max_by(r.count_diff, r.checked_at) AS count_diff,
    CASE WHEN count(*) > 1
      THEN max_by(r.target_count, r.checked_at) - min_by(r.target_count, r.checked_at)
    END AS growth_within_day
  FROM IDENTIFIER(:row_check_table) r
  JOIN IDENTIFIER(:config_table) c
    ON c.ingestion_group = r.ingestion_group AND c.source_table_name = r.source_table_name
  CROSS JOIN picked p
  WHERE :refresh_token >= 0
    AND coalesce(c.enabled, true)
    AND r.check_mode = 'basic'
    AND r.status <> 'ERROR'
    AND r.checked_range IS NULL
    AND r.checked_at >= date_sub(current_date(), :days)
    AND (:ingestion_group = '' OR r.ingestion_group = :ingestion_group)
    AND (:search = '' OR lower(r.source_table_name) LIKE concat('%', lower(:search), '%'))
    AND (p.tables IS NULL OR size(p.tables) = 0
      OR exists(p.tables, t -> t.group = r.ingestion_group AND t.table = r.source_table_name))
  GROUP BY r.ingestion_group, r.source_table_name, to_date(r.checked_at)
),
shown AS (
  SELECT *
  FROM (
    SELECT
      *,
      coalesce(
        target_count - lag(target_count) OVER (PARTITION BY ingestion_group, source_table_name ORDER BY day),
        growth_within_day
      ) AS rows_ingested
    FROM daily
  )
  WHERE day > date_sub(current_date(), :days)
),
-- One row per series and day: a group (its tables added up; source_table_name '') or a table.
entities AS (
  SELECT
    day,
    ingestion_group,
    CASE WHEN :level = 'group' THEN '' ELSE source_table_name END AS source_table_name,
    sum(source_count) AS source_count,
    sum(target_count) AS target_count,
    sum(rows_ingested) AS rows_ingested,
    CASE WHEN :level = 'group' THEN sum(abs(count_diff)) ELSE sum(count_diff) END AS count_diff
  FROM shown
  GROUP BY day, ingestion_group, CASE WHEN :level = 'group' THEN '' ELSE source_table_name END
),
ranked AS (
  SELECT
    ingestion_group,
    source_table_name,
    row_number() OVER (
      ORDER BY sum(abs(coalesce(rows_ingested, 0))) DESC, ingestion_group, source_table_name
    ) AS volume_rank,
    count(*) OVER () AS entity_count
  FROM entities
  GROUP BY ingestion_group, source_table_name
),
folded AS (
  SELECT
    e.*,
    r.entity_count,
    NOT (coalesce(size(p.tables), 0) > 0 OR r.volume_rank <= :top) AS is_other
  FROM entities e
  JOIN ranked r USING (ingestion_group, source_table_name)
  CROSS JOIN picked p
)
SELECT
  date_format(day, 'yyyy-MM-dd') AS day,
  CASE WHEN is_other THEN '' ELSE ingestion_group END AS ingestion_group,
  CASE WHEN is_other THEN '' ELSE source_table_name END AS source_table_name,
  is_other,
  max(entity_count) AS entity_count,
  sum(source_count) AS source_count,
  sum(target_count) AS target_count,
  sum(rows_ingested) AS rows_ingested,
  CASE WHEN is_other OR :level = 'group' THEN sum(abs(count_diff)) ELSE sum(count_diff) END AS count_diff
FROM folded
GROUP BY day, is_other, CASE WHEN is_other THEN '' ELSE ingestion_group END,
  CASE WHEN is_other THEN '' ELSE source_table_name END
ORDER BY day, is_other, ingestion_group, source_table_name;
