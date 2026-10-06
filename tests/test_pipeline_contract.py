from __future__ import annotations

import unittest
import sys
import types
from unittest.mock import patch

delta_tables = types.ModuleType("delta.tables")
delta_tables.DeltaTable = object
sys.modules.setdefault("delta.tables", delta_tables)

from watersync.cdc_pipeline import CdcScd2PipelineBuilder
from watersync.common import validate_table_fqn


class FakeDp:
    def __init__(self) -> None:
        self.tables: list[str] = []
        self.flows: list[dict] = []

    def create_streaming_table(self, name: str, **_kwargs) -> None:
        self.tables.append(name)

    def view(self, **_kwargs):
        return lambda function: function

    def create_auto_cdc_flow(self, **kwargs) -> None:
        self.flows.append(kwargs)

    def create_auto_cdc_from_snapshot_flow(self, **kwargs) -> None:
        self.flows.append(kwargs)


class PipelineContractTest(unittest.TestCase):
    def test_target_fqn_requires_three_parts(self) -> None:
        self.assertEqual(validate_table_fqn("catalog.schema.table"), "catalog.schema.table")
        with self.assertRaises(ValueError):
            validate_table_fqn("schema.table")

    def test_full_config_is_not_registered_in_sdp(self) -> None:
        dp = FakeDp()
        builder = CdcScd2PipelineBuilder(None, dp, "meta.config.jdbc_ingestion_config", "group")
        builder.load_configs = lambda: [
            {
                "ingestion_group": "group",
                "source_table_name": "source.orders",
                "target_table_name": None,
                "target_table_fqn": "gold.sales.orders",
                "ingestion_type": "full",
                "key_columns": "order_id",
                "watermark_column": None,
                "epic_csa_enabled": False,
                "auto_cdc_from_snapshot": False,
            }
        ]

        with patch("watersync.cdc_pipeline.col", side_effect=lambda value: value):
            builder.build()

        self.assertEqual(dp.tables, [])
        self.assertEqual(dp.flows, [])

    def test_incremental_config_uses_exact_target_fqn(self) -> None:
        dp = FakeDp()
        builder = CdcScd2PipelineBuilder(None, dp, "meta.config.jdbc_ingestion_config", "group")
        builder.load_configs = lambda: [
            {
                "ingestion_group": "group",
                "source_table_name": "source.orders",
                "target_table_name": "staging_orders",
                "target_table_fqn": "gold.sales.orders_scd2",
                "ingestion_type": "incremental",
                "key_columns": "order_id",
                "watermark_column": "updated_at",
                "epic_csa_enabled": False,
                "auto_cdc_from_snapshot": False,
            }
        ]

        with patch("watersync.cdc_pipeline.col", side_effect=lambda value: value):
            builder.build()

        self.assertEqual(dp.tables, ["gold.sales.orders_scd2"])
        self.assertEqual(dp.flows[0]["target"], "gold.sales.orders_scd2")

    def test_flagged_full_config_uses_snapshot_cdc(self) -> None:
        dp = FakeDp()
        builder = CdcScd2PipelineBuilder(None, dp, "meta.config.jdbc_ingestion_config", "group")
        builder.load_configs = lambda: [{
            "ingestion_group": "group",
            "source_table_name": "source.orders",
            "staging_table_fqn": "bronze.sales.orders_snapshot",
            "target_table_fqn": "gold.sales.orders_scd2",
            "ingestion_type": "full",
            "key_columns": "order_id",
            "watermark_column": None,
            "epic_csa_enabled": False,
            "auto_cdc_from_snapshot": True,
        }]

        builder.build()

        self.assertEqual(dp.tables, ["gold.sales.orders_scd2"])
        self.assertEqual(dp.flows[0]["target"], "gold.sales.orders_scd2")
        self.assertEqual(dp.flows[0]["keys"], ["order_id"])


if __name__ == "__main__":
    unittest.main()
