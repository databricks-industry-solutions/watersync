# Configuration Table

`jdbc_ingestion_config` drives all ingestion. Each row represents one source table.

| Column | Type | Required | Description |
|---|---|---|---|
| `ingestion_group` | STRING | yes | Logical group — all rows with the same group run in one job |
| `source_table_name` | STRING | yes | Source table as `schema.table` (e.g. `dbo.PAT_ENC`) |
| `staging_table_fqn` | STRING | no | Bronze staging table in `catalog.schema.table` form; defaults to `<config_catalog>.<config_schema>.staging_<table>` |
| `target_table_fqn` | STRING | yes | Final destination in `catalog.schema.table` form. Full loads write here directly unless `auto_cdc_from_snapshot` is set; otherwise the CDC pipeline publishes Silver SCD Type 2 history here |
| `ingestion_type` | STRING | no | `incremental` (default) or `full` |
| `key_columns` | STRING | yes* | Comma-separated business keys (*required for CDC and EPIC CSA) |
| `watermark_column` | STRING | yes* | Timestamp column for incremental loads (*required for `incremental` unless `epic_csa_enabled`; ignored and saved empty by the app for full loads and EPIC CSA) |
| `partition_column` | STRING | no | Numeric column for parallel JDBC partitioning |
| `predicate_column` | STRING | no | String column for predicate-based parallel reads (requires `jdbc_url`) |
| `epic_csa_enabled` | BOOLEAN | no | Use the EPIC CSA worker (incremental only) |
| `epic_change_method` | STRING | no | With `epic_csa_enabled`: `csa_table` (default, read the CSA tables) or `stored_procedure` (call Epic's changed-rows procedure) |
| `epic_change_procedure` | STRING | yes* | Changed-rows procedure as `[database.]schema.procedure`, e.g. `dbo.ESP_CR_ALTERED_ROWS` (*required for `stored_procedure`) |
| `auto_cdc_from_snapshot` | BOOLEAN | no | For `full` sources, stage each snapshot and maintain SCD Type 2 history in the target |
| `jdbc_url` | STRING | one of* | JDBC URL to the source (*set `jdbc_url` or `connection_name`; at least one is required) |
| `jdbc_user` | STRING | no | JDBC username |
| `jdbc_secret_scope` / `jdbc_secret_key` | STRING | no | Databricks secret holding the JDBC password (set both or neither) |
| `uc_secret_name` | STRING | no | Unity Catalog secret as `catalog.schema.secret_name`, used instead of a scope/key |
| `connection_name` | STRING | one of* | Unity Catalog connection to the source, used instead of `jdbc_url` |
| `connection_database` | STRING | no | With `connection_name`: database to read instead of the connection's default (Oracle: service name). Ignored without a connection |
| `watermark_threshold_minutes` | INT | no | Incremental cutoff lag behind `now()` (default 5) |
| `fetch_size` | INT | no | JDBC fetch size (default 10000) |
| `num_partitions` | INT | no | JDBC parallelism (default 8) |
| `update_dttm` | TIMESTAMP | no | Last update of the config row |
| `enabled` | BOOLEAN | yes | Set `false` to skip the row without deleting it |

Every row needs a way to reach the source system: set either `connection_name` (a Unity Catalog connection) or `jdbc_url`. Rows with neither fail validation. A row using `jdbc_url` must also reference a password secret (`jdbc_secret_scope`/`jdbc_secret_key` or `uc_secret_name`). Never store plaintext passwords.

Lakeflow Jobs take four parameters: `configuration_fqn`, `watermark_fqn`, `ingestion_group`, and `full_refresh` (default `false`).

### Adding config rows

Use the Control Plane app's **Configuration** page, the `watersync-add-config` agent skill in `.agents/skills`, or SQL:

```sql
INSERT INTO main.watersync.jdbc_ingestion_config
  (ingestion_group, source_table_name, staging_table_fqn, target_table_fqn,
   ingestion_type, key_columns, watermark_column, epic_csa_enabled,
   auto_cdc_from_snapshot, connection_name, watermark_threshold_minutes,
   fetch_size, num_partitions, update_dttm, enabled)
VALUES
  -- EPIC Clarity table tracked through EPIC_UTIL.CSA_PAT_ENC
  ('epic', 'dbo.PAT_ENC', 'main.bronze_clarity.staging_pat_enc', 'main.silver_clarity.pat_enc',
   'incremental', 'PAT_ENC_CSN_ID', NULL, true,
   false, 'clarity_conn', 5, 10000, 8, current_timestamp(), true),
  -- Timestamp-watermark table
  ('epic', 'dbo.CLARITY_ADT', 'main.bronze_clarity.staging_clarity_adt', 'main.silver_clarity.clarity_adt',
   'incremental', 'EVENT_ID', 'UPDATE_DATE', false,
   false, 'clarity_conn', 5, 10000, 8, current_timestamp(), true);
```

To read an EPIC table through Epic's changed-rows stored procedure instead of the CSA tables, set the two `epic_change_*` columns on an EPIC CSA row. The Control Plane app doesn't show them yet, so set them with SQL:

```sql
UPDATE main.watersync.jdbc_ingestion_config
SET epic_change_method = 'stored_procedure',
    epic_change_procedure = 'dbo.ESP_CR_ALTERED_ROWS',
    update_dttm = current_timestamp()
WHERE ingestion_group = 'epic' AND source_table_name = 'dbo.PAT_ENC';
```

Run `UnityCatalogSetup.create_config_table()` (or the notebook's `setup_uc` action) first on an existing config table, to add the two columns.

## JDBC Connection Options

Connections are configured per config row, and every row must use one of these two modes:

- **JDBC URL** — set `jdbc_url`, `jdbc_user`, and either `jdbc_secret_scope` + `jdbc_secret_key` or `uc_secret_name`. The SQL dialect is detected from the URL.
- **Unity Catalog connection** — set `connection_name` and leave `jdbc_url` empty. The dialect comes from the connection type. Set `connection_database` to read from another database than the one defined on the connection; it is passed as the `service_name` option for Oracle and as `database` otherwise.

Supported dialects: SQL Server, Oracle, and PostgreSQL.
