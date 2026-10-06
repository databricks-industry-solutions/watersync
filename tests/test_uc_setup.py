from __future__ import annotations

import types
import unittest

from watersync.utils.uc_setup import UnityCatalogSetup


class FakeSpark:
    def __init__(self, existing_config_columns: list[str]) -> None:
        self.statements: list[str] = []
        self.existing_config_columns = existing_config_columns

    def sql(self, statement: str):
        self.statements.append(statement)
        return types.SimpleNamespace(collect=lambda: [])

    def table(self, _name: str):
        fields = [types.SimpleNamespace(name=name) for name in self.existing_config_columns]
        return types.SimpleNamespace(schema=types.SimpleNamespace(fields=fields))


class UnityCatalogSetupTest(unittest.TestCase):
    def test_config_table_includes_uc_secret_name(self) -> None:
        spark = FakeSpark(existing_config_columns=[])
        UnityCatalogSetup(spark, catalog="main", schema="watersync").create_config_table()
        self.assertIn("uc_secret_name STRING", spark.statements[0])

    def test_existing_config_table_gets_uc_secret_name_added(self) -> None:
        spark = FakeSpark(existing_config_columns=["ingestion_group", "source_table_name"])
        UnityCatalogSetup(spark, catalog="main", schema="watersync").create_config_table()
        alter = [statement for statement in spark.statements if statement.startswith("ALTER TABLE")]
        self.assertEqual(len(alter), 1)
        self.assertIn("uc_secret_name STRING", alter[0])

    def test_watermark_table_is_not_replaced(self) -> None:
        spark = FakeSpark(existing_config_columns=[])
        UnityCatalogSetup(spark, catalog="main", schema="watersync").create_watermark_state_table()
        self.assertIn("CREATE TABLE IF NOT EXISTS", spark.statements[0])
        self.assertNotIn("CREATE OR REPLACE", spark.statements[0])


if __name__ == "__main__":
    unittest.main()
