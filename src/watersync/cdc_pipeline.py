from __future__ import annotations

from typing import Any

from pyspark.sql.functions import col, expr

from watersync.common import quote_sql_string, resolve_staging_table_fqn, row_to_dict

_METADATA_COLUMNS = ["_ingested_at", "_source_table", "_ingestion_group", "_ingestion_type"]
_CSA_CONTROL_COLUMNS = ["_IS_DELETED", "_csa_update_dt"]


class CdcScd2PipelineBuilder:
    def __init__(self, spark: Any, dp_module: Any, configuration_fqn: str, ingestion_group: str = ""):
        self.spark = spark
        self.dp = dp_module
        self.configuration_fqn = configuration_fqn
        self.ingestion_group = ingestion_group

    @property
    def config_table(self) -> str:
        return self.configuration_fqn

    def load_configs(self) -> list[dict[str, Any]]:
        filters = ["enabled = true"]
        if self.ingestion_group:
            filters.append(f"ingestion_group = '{quote_sql_string(self.ingestion_group)}'")
        rows = self.spark.read.table(self.config_table).filter(" AND ".join(filters)).collect()
        configs = [row_to_dict(row) for row in rows]
        if not configs:
            suffix = f" for ingestion_group='{self.ingestion_group}'" if self.ingestion_group else ""
            raise ValueError(f"No enabled config rows found{suffix}")
        return configs

    def make_snapshot_source(self, staging_fqn: str, drop_columns: list[str] | None = None):
        def next_snapshot_and_version(latest_snapshot_version):
            versions = [
                row["version"]
                for row in self.spark.sql(f"DESCRIBE HISTORY {staging_fqn}").select("version").orderBy("version").collect()
            ]
            if not versions:
                return None
            next_version = versions[0] if latest_snapshot_version is None else next((v for v in versions if v > latest_snapshot_version), None)
            if next_version is None:
                return None
            df = self.spark.read.format("delta").option("versionAsOf", next_version).table(staging_fqn)
            if drop_columns:
                existing = set(df.columns)
                to_drop = [c for c in drop_columns if c in existing]
                if to_drop:
                    df = df.drop(*to_drop)
            return df, next_version
        return next_snapshot_and_version

    def register_incremental_standard_flow(self, history_table: str, staging_table: str, staging_fqn: str, key_columns: list[str], watermark_column: str) -> None:
        @self.dp.view(name=f"v_{staging_table}", comment=f"Streaming view on {staging_fqn}")
        def _make_view(_tbl=staging_fqn):
            return self.spark.readStream.table(_tbl)

        self.dp.create_auto_cdc_flow(
            target=history_table,
            source=f"v_{staging_table}",
            keys=key_columns,
            sequence_by=col(watermark_column),
            stored_as_scd_type="2",
            except_column_list=_METADATA_COLUMNS,
        )

    def register_incremental_csa_flow(self, history_table: str, flow_prefix: str, staging_table: str, staging_fqn: str, key_columns: list[str]) -> None:
        @self.dp.view(
            name=f"v_{staging_table}_upserts",
            comment=f"Upsert events from {staging_fqn}",
        )
        def _make_upsert_view(_tbl=staging_fqn):
            return self.spark.readStream.table(_tbl)

        self.dp.create_auto_cdc_flow(
            name=f"{flow_prefix}_upserts",
            target=history_table,
            source=f"v_{staging_table}_upserts",
            keys=key_columns,
            sequence_by=col("_csa_update_dt"),
            apply_as_deletes = expr("_IS_DELETED IS TRUE"),
            stored_as_scd_type="2",
            except_column_list=_METADATA_COLUMNS + _CSA_CONTROL_COLUMNS,
        )

    def register_snapshot_flow(self, history_table: str, staging_fqn: str, key_columns: list[str]) -> None:
        self.dp.create_auto_cdc_from_snapshot_flow(
            target=history_table,
            source=self.make_snapshot_source(staging_fqn, drop_columns=_METADATA_COLUMNS),
            keys=key_columns,
            stored_as_scd_type=2,
        )

    def build(self) -> None:
        config_catalog, config_schema, _ = self.configuration_fqn.split(".", 2)
        for config in self.load_configs():
            source_table = config["source_table_name"]
            staging_fqn = resolve_staging_table_fqn(
                config.get("staging_table_fqn"), source_table, config_catalog, config_schema
            )
            staging_table = staging_fqn.split(".")[-1]
            key_columns = [key.strip() for key in (config["key_columns"] or "").split(",") if key.strip()]
            watermark_column = (config["watermark_column"] or "").strip()
            ingestion_type = (config["ingestion_type"] or "incremental").strip().lower()
            auto_cdc_from_snapshot = bool(config.get("auto_cdc_from_snapshot"))
            if ingestion_type == "full" and not auto_cdc_from_snapshot:
                continue
            epic_csa_enabled = bool(config["epic_csa_enabled"])
            history_table = (config.get("target_table_fqn") or "").strip()
            if len(history_table.split(".")) != 3:
                raise ValueError(f"target_table_fqn must use catalog.schema.table for {source_table}")
            flow_prefix = "_".join(history_table.split("."))

            if not key_columns:
                raise ValueError(f"key_columns is required for CDC on {source_table}")

            self.dp.create_streaming_table(
                name=history_table,
                comment=(
                    f"SCD Type 2 history for {source_table} "
                    f"(group: {config['ingestion_group']}, mode: {ingestion_type}). "
                    f"Keys: {key_columns}"
                ),
                cluster_by=key_columns
            )

            if ingestion_type == "incremental":
                if epic_csa_enabled:
                    self.register_incremental_csa_flow(history_table, flow_prefix, staging_table, staging_fqn, key_columns)
                else:
                    self.register_incremental_standard_flow(history_table, staging_table, staging_fqn, key_columns, watermark_column)
            else:
                self.register_snapshot_flow(history_table, staging_fqn, key_columns)


def build_pipeline_from_spark_conf(spark: Any, dp_module: Any) -> None:
    builder = CdcScd2PipelineBuilder(
        spark=spark,
        dp_module=dp_module,
        configuration_fqn=spark.conf.get("pipeline.configuration_fqn"),
        ingestion_group=spark.conf.get("pipeline.ingestion_group", ""),
    )
    builder.build()
