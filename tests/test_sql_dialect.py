from __future__ import annotations

import sys
import types
import unittest

delta_tables = types.ModuleType("delta.tables")
delta_tables.DeltaTable = object
sys.modules.setdefault("delta.tables", delta_tables)

from watersync.sql_dialect import (
    detect_sql_dialect,
    dialect_from_connection_type,
    sql_bigint_cast,
    sql_exists_subquery,
    sql_timestamp_literal,
    watermark_window_predicate,
)


class SqlDialectTest(unittest.TestCase):
    def test_detect_from_jdbc_url(self) -> None:
        self.assertEqual(
            detect_sql_dialect("jdbc:postgresql://host:5432/db"), "postgres"
        )
        self.assertEqual(
            detect_sql_dialect("jdbc:sqlserver://host:1433;databaseName=db"),
            "sqlserver",
        )
        self.assertEqual(
            detect_sql_dialect("jdbc:oracle:thin:@//host:1521/orcl"), "oracle"
        )
        self.assertEqual(detect_sql_dialect(""), "unknown")

    def test_detect_from_uc_connection_type(self) -> None:
        connection_type = types.SimpleNamespace(value="SQLSERVER")
        self.assertEqual(
            dialect_from_connection_type(connection_type), "sqlserver"
        )
        self.assertEqual(dialect_from_connection_type("POSTGRESQL"), "postgres")
        self.assertEqual(dialect_from_connection_type("ORACLE"), "oracle")
        self.assertEqual(dialect_from_connection_type("MYSQL"), "unknown")

    def test_timestamp_literal_per_engine(self) -> None:
        value = "2024-01-02 03:04:05"
        self.assertEqual(
            sql_timestamp_literal(value, "postgres"),
            "CAST('2024-01-02 03:04:05' AS TIMESTAMP)",
        )
        self.assertEqual(
            sql_timestamp_literal(value, "sqlserver"),
            "CAST('2024-01-02 03:04:05' AS DATETIME2)",
        )
        self.assertEqual(
            sql_timestamp_literal(value, "oracle"),
            "TO_TIMESTAMP('2024-01-02 03:04:05', 'YYYY-MM-DD HH24:MI:SS')",
        )

    def test_exists_subquery_per_engine(self) -> None:
        where = "updated_at > CAST('x' AS TIMESTAMP)"
        self.assertIn(
            "LIMIT 1",
            sql_exists_subquery("dbo.t", where, "postgres"),
        )
        self.assertIn(
            "SELECT TOP 1",
            sql_exists_subquery("dbo.t", where, "sqlserver"),
        )
        self.assertIn(
            "ROWNUM = 1",
            sql_exists_subquery("dbo.t", where, "oracle"),
        )
        self.assertNotIn(" AS change_check", sql_exists_subquery("dbo.t", where, "oracle"))

    def test_watermark_window_and_bigint(self) -> None:
        pred = watermark_window_predicate(
            "updated_at", "2024-01-01 00:00:00", "2024-01-02 00:00:00", "oracle"
        )
        self.assertIn("TO_TIMESTAMP", pred)
        self.assertEqual(
            sql_bigint_cast("csa._TIMESTAMP_EXTRACT_KEY", "oracle"),
            "CAST(csa._TIMESTAMP_EXTRACT_KEY AS NUMBER(19))",
        )
        self.assertEqual(
            sql_bigint_cast("csa._TIMESTAMP_EXTRACT_KEY", "sqlserver"),
            "CAST(csa._TIMESTAMP_EXTRACT_KEY AS BIGINT)",
        )


if __name__ == "__main__":
    unittest.main()
