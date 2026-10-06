-- @param config_table STRING
-- @param watermark_table STRING
-- @param refresh_token INT
SELECT
  (SELECT count(*) FROM IDENTIFIER(:config_table)) AS config_count,
  (SELECT count(*) FROM IDENTIFIER(:config_table) WHERE enabled = true) AS enabled_count,
  (SELECT count(*) FROM IDENTIFIER(:watermark_table) WHERE status = 'FAILED') AS failed_count,
  (SELECT max(last_run_timestamp) FROM IDENTIFIER(:watermark_table)) AS last_run_timestamp
WHERE :refresh_token >= 0;
