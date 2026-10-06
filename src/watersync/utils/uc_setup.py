from __future__ import annotations

from typing import Any


class UnityCatalogSetup:
    def __init__(self, spark: Any, catalog: str, schema: str):
        self.spark = spark
        self.catalog = catalog
        self.schema = schema

    @property
    def schema_fqn(self) -> str:
        return f"{self.catalog}.{self.schema}"

    @property
    def config_table(self) -> str:
        return f"{self.schema_fqn}.jdbc_ingestion_config"

    @property
    def state_table(self) -> str:
        return f"{self.schema_fqn}.jdbc_ingestion_watermark"

    @property
    def log_table(self) -> str:
        return f"{self.schema_fqn}.watersync_logs"

    def ensure_schema(self) -> None:
        self.spark.sql(f"CREATE SCHEMA IF NOT EXISTS IDENTIFIER('{self.schema_fqn}')")

    def create_config_table(self) -> None:
        self.spark.sql(f"""
            CREATE TABLE IF NOT EXISTS IDENTIFIER('{self.config_table}') (
              ingestion_group STRING COMMENT 'Logical group processed by the same Lakeflow Job',
              source_table_name STRING COMMENT 'Fully qualified source table name',
              staging_table_fqn STRING COMMENT 'Staging table in catalog.schema.table format',
              target_table_fqn STRING COMMENT 'Final destination in catalog.schema.table format',
              ingestion_type STRING COMMENT 'full or incremental',
              key_columns STRING COMMENT 'Comma-separated business keys',
              watermark_column STRING COMMENT 'Timestamp or change-sequence column used by ingestion',
              partition_column STRING COMMENT 'Numeric JDBC partition column',
              predicate_column STRING COMMENT 'String JDBC predicate-partition column',
              epic_csa_enabled BOOLEAN COMMENT 'True when EPIC CSA mode is enabled',
              auto_cdc_from_snapshot BOOLEAN COMMENT 'Use snapshot CDC for full-load sources',
              jdbc_url STRING COMMENT 'JDBC URL; leave empty when using a UC connection',
              jdbc_user STRING COMMENT 'JDBC username',
              jdbc_secret_scope STRING COMMENT 'Secret scope containing the JDBC password',
              jdbc_secret_key STRING COMMENT 'Secret key containing the JDBC password',
              uc_secret_name STRING COMMENT 'Unity Catalog secret (catalog.schema.secret_name) containing the JDBC password',
              connection_name STRING COMMENT 'Unity Catalog connection used instead of jdbc_url',
              watermark_threshold_minutes INT COMMENT 'Delay behind current time for incremental cutoffs',
              fetch_size INT COMMENT 'JDBC fetch size',
              num_partitions INT COMMENT 'JDBC parallelism',
              update_dttm TIMESTAMP COMMENT 'Last update timestamp for the config row',
              enabled BOOLEAN COMMENT 'True when the config row is active'
            )
            USING DELTA
            CLUSTER BY AUTO
            COMMENT 'Configuration table for grouped JDBC ingestion jobs'
            TBLPROPERTIES ('delta.enableChangeDataFeed' = 'true')
        """)
        existing = {field.name for field in self.spark.table(self.config_table).schema.fields}
        additions = {
            "target_table_fqn": "STRING COMMENT 'Final destination in catalog.schema.table format'",
            "auto_cdc_from_snapshot": "BOOLEAN COMMENT 'Use snapshot CDC for full-load sources'",
            "jdbc_url": "STRING COMMENT 'JDBC URL; leave empty when using a UC connection'",
            "jdbc_user": "STRING COMMENT 'JDBC username'",
            "jdbc_secret_scope": "STRING COMMENT 'Secret scope containing the JDBC password'",
            "jdbc_secret_key": "STRING COMMENT 'Secret key containing the JDBC password'",
            "uc_secret_name": "STRING COMMENT 'Unity Catalog secret (catalog.schema.secret_name) containing the JDBC password'",
            "connection_name": "STRING COMMENT 'Unity Catalog connection used instead of jdbc_url'",
            "watermark_threshold_minutes": "INT COMMENT 'Delay behind current time for incremental cutoffs'",
            "fetch_size": "INT COMMENT 'JDBC fetch size'",
            "num_partitions": "INT COMMENT 'JDBC parallelism'",
        }
        missing = [f"{name} {ddl}" for name, ddl in additions.items() if name not in existing]
        if missing:
            self.spark.sql(f"ALTER TABLE IDENTIFIER('{self.config_table}') ADD COLUMNS ({', '.join(missing)})")

    def create_log_table(self) -> None:
        self.spark.sql(f"""
            CREATE TABLE IF NOT EXISTS IDENTIFIER('{self.log_table}') (
              run_id        STRING  COMMENT 'UUID shared across one orchestration run — primary filter key',
              log_timestamp STRING  COMMENT 'UTC ISO-8601 timestamp of the log event',
              payload       VARIANT COMMENT 'Full log record as nested JSON: level, logger_name, message{{text, module, func, lineno, exc}}, ingestion_group, source_table'
            )
            USING DELTA
            CLUSTER BY AUTO
            COMMENT 'ZeroBus-backed structured log table for watersync runs'
        """)

    def create_watermark_state_table(self) -> None:
        self.spark.sql(f"""
            CREATE TABLE IF NOT EXISTS IDENTIFIER('{self.state_table}') (
              ingestion_group STRING COMMENT 'Matches ingestion_group in config',
              source_table_name STRING COMMENT 'Matches source_table_name in config',
              staging_table_fqn STRING COMMENT 'Matches staging_table_fqn in config',
              ingestion_type STRING COMMENT 'Matches ingestion_type in config',
              last_watermark STRING COMMENT 'Stored watermark value',
              last_run_timestamp TIMESTAMP COMMENT 'Timestamp of the last run',
              status STRING COMMENT 'SUCCESS, SKIPPED, FAILED, or FULL_REFRESH',
              last_error STRING COMMENT 'Last failure message'
            )
            USING DELTA
            CLUSTER BY AUTO
            COMMENT 'Tracks grouped JDBC ingestion state'
        """)

    def truncate_managed_objects(self) -> None:
        tables = [
            row.table_name
            for row in self.spark.sql(f"""
                SELECT table_name
                FROM {self.catalog}.information_schema.tables
                WHERE table_schema = '{self.schema}'
                  AND table_name <> 'jdbc_ingestion_config'
            """).collect()
        ]
        for table in tables:
            try:
                self.spark.sql(f"TRUNCATE TABLE IDENTIFIER('{self.schema_fqn}.{table}')")
            except Exception:
                continue

    def create_all(self, truncate_existing: bool = False) -> None:
        self.ensure_schema()
        self.create_config_table()
        self.create_watermark_state_table()
        self.create_log_table()
        if truncate_existing:
            self.truncate_managed_objects()
