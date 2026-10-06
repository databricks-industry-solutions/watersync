from __future__ import annotations

import json
import logging
from typing import Any, Callable

from watersync.common import quote_sql_string, row_to_dict
from watersync.models import JdbcRuntimeSettings

logger = logging.getLogger(__name__)


class JdbcIngestionConfigRepository:
    def __init__(self, spark: Any, runtime: JdbcRuntimeSettings):
        self.spark = spark
        self.runtime = runtime

    def load_for_group(self, ingestion_group: str) -> list[dict[str, Any]]:
        if not ingestion_group:
            raise ValueError("ingestion_group is required to plan fanout inputs")

        query = f"""
            SELECT
                ingestion_group,
                source_table_name
            FROM {self.runtime.config_table}
            WHERE enabled = true
              AND ingestion_group = '{quote_sql_string(ingestion_group)}'
            ORDER BY source_table_name
        """
        configs = [row_to_dict(row) for row in self.spark.sql(query).collect()]
        if not configs:
            raise ValueError(f"No enabled config rows found for ingestion_group={ingestion_group}")
        logger.info(
            "[PLAN]   Loaded %d config(s) for ingestion_group=%s",
            len(configs),
            ingestion_group,
        )
        return configs


class IngestionConfigPlanner:
    def __init__(self, spark: Any, runtime: JdbcRuntimeSettings):
        self.spark = spark
        self.runtime = runtime
        self.repository = JdbcIngestionConfigRepository(spark=spark, runtime=runtime)

    def build_for_each_inputs(self, ingestion_group: str) -> list[dict[str, Any]]:
        return self.repository.load_for_group(ingestion_group=ingestion_group)

    def build_for_each_inputs_json(self, ingestion_group: str) -> str:
        return json.dumps(self.build_for_each_inputs(ingestion_group=ingestion_group))

    def detect_full_refresh_targets(self, ingestion_group: str) -> list[str]:
        """Return the ``target_table_fqn`` of every table whose watermark status
        is ``FULL_REFRESH`` for the given *ingestion_group*.

        This is called once in the planner step so the downstream
        selective-refresh task knows which pipeline tables need a full
        refresh without per-worker detection.
        """
        query = f"""
            SELECT DISTINCT c.target_table_fqn
            FROM {self.runtime.config_table} c
            JOIN {self.runtime.state_table} w
              ON c.ingestion_group = w.ingestion_group
             AND c.source_table_name = w.source_table_name
            WHERE c.enabled = true
              AND c.ingestion_group = '{quote_sql_string(ingestion_group)}'
              AND w.status = 'FULL_REFRESH'
        """
        rows = self.spark.sql(query).collect()
        targets = [str(row["target_table_fqn"]) for row in rows if row["target_table_fqn"]]
        if targets:
            logger.info(
                "[PLAN]   Detected %d table(s) with FULL_REFRESH status: %s",
                len(targets),
                targets,
            )
        return targets

    def publish_task_value(
        self,
        ingestion_group: str,
        writer: Callable[[str, str], None] | None = None,
        task_value_key: str = "table_configs",
    ) -> str:
        payload = self.build_for_each_inputs_json(ingestion_group=ingestion_group)
        if writer is not None:
            writer(task_value_key, payload)
        return payload
