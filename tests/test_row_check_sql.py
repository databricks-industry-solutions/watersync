from __future__ import annotations

import unittest
from datetime import datetime
from decimal import Decimal

from watersync.row_check_sql import (
    DATE_OFFSET_DAYS,
    ChunkPlan,
    CsaChanges,
    RowRange,
    parse_range_bound,
    boolean_code_sql,
    check_dialect,
    chunk_aggregate_query,
    date_code_sql,
    is_time_bound,
    month_chunk_sql,
    quote_identifier,
    string_hash_sql,
)


class RowCheckSqlTest(unittest.TestCase):
    def test_mysql_is_detected_from_the_jdbc_url(self) -> None:
        self.assertEqual(check_dialect("unknown", "jdbc:mysql://host:3306/db"), "mysql")
        self.assertEqual(check_dialect("postgres", "jdbc:postgresql://host/db"), "postgres")
        self.assertEqual(check_dialect("unknown", ""), "unknown")

    def test_identifiers_are_quoted_per_engine(self) -> None:
        self.assertEqual(quote_identifier("OrderId", "postgres"), '"OrderId"')
        self.assertEqual(quote_identifier("ORDER_ID", "oracle"), '"ORDER_ID"')
        self.assertEqual(quote_identifier("a]b", "sqlserver"), "[a]]b]")
        self.assertEqual(quote_identifier("order", "mysql"), "`order`")
        self.assertEqual(quote_identifier("order_id", "unknown"), "order_id")

    def test_string_hash_per_engine(self) -> None:
        self.assertIn("md5", string_hash_sql('"name"', "postgres"))
        self.assertIn("HASHBYTES('MD5'", string_hash_sql("[name]", "sqlserver"))
        self.assertIn("UTF8", string_hash_sql("[name]", "sqlserver"))
        self.assertIn("STANDARD_HASH", string_hash_sql('"NAME"', "oracle"))
        self.assertIn("CONV(SUBSTRING(MD5", string_hash_sql("`name`", "mysql"))
        self.assertIsNone(string_hash_sql("name", "unknown"))
        for dialect in ("postgres", "sqlserver", "oracle", "mysql"):
            self.assertTrue(string_hash_sql("c", dialect).startswith("COALESCE("), dialect)

    def test_month_chunk_per_engine(self) -> None:
        self.assertEqual(month_chunk_sql("[d]", "sqlserver"), "(YEAR([d]) * 100 + MONTH([d]))")
        self.assertIn("EXTRACT(YEAR FROM", month_chunk_sql('"d"', "oracle"))

    def test_numeric_plan_splits_the_range_into_about_100_chunks(self) -> None:
        plan = ChunkPlan.numeric("id", 1, 1000)
        self.assertEqual(plan.width, 10)
        self.assertEqual(plan.label(0), "id 1–10")
        self.assertEqual(plan.label(None), "id IS NULL")
        self.assertEqual(ChunkPlan.numeric("id", 5, 5).width, 1)
        self.assertEqual(ChunkPlan.numeric("id", 1, 1001).width, 11)

    def test_numeric_plan_sql_matches_on_both_sides(self) -> None:
        plan = ChunkPlan.numeric("Id", 1, 1000)
        self.assertEqual(plan.source_sql("postgres"), 'FLOOR((CAST("Id" AS DECIMAL(38, 0)) - 1) / 10)')
        self.assertEqual(plan.spark_sql(), "FLOOR((CAST(`Id` AS DECIMAL(38, 0)) - 1) / 10)")

    def test_numeric_predicate_reads_only_the_chunks(self) -> None:
        plan = ChunkPlan.numeric("id", 1, 1000)
        self.assertEqual(
            plan.source_predicate([2, None], "sqlserver"),
            "[id] IS NULL OR ([id] >= 21 AND [id] < 31)",
        )

    def test_month_predicate_adds_a_day_of_margin(self) -> None:
        plan = ChunkPlan("month", "updated_at")
        self.assertEqual(plan.label(202603), "updated_at 2026-03")
        self.assertEqual(
            plan.source_predicate([202603], "postgres"),
            '("updated_at" >= CAST(\'2026-02-28 00:00:00\' AS TIMESTAMP) '
            'AND "updated_at" < CAST(\'2026-04-02 00:00:00\' AS TIMESTAMP))',
        )
        self.assertIn("< '2027-01-02 00:00:00'", plan.source_predicate([202612], "mysql"))

    def test_single_chunk_plan(self) -> None:
        plan = ChunkPlan("none")
        self.assertEqual(plan.source_sql("oracle"), "0")
        self.assertEqual(plan.source_predicate([0], "oracle"), "1=1")
        self.assertEqual(plan.label(0), "all rows")

    def test_aggregate_query_groups_by_the_chunk_alias(self) -> None:
        query = chunk_aggregate_query(
            "sales.orders",
            "FLOOR((CAST([id] AS DECIMAL(38, 0)) - 1) / 10)",
            [("s0", "[amount]", 2)],
            [("h0", "COALESCE(x, 0)")],
        )
        self.assertIn("SUM(CAST([amount] AS DECIMAL(38, 2))) AS s0", query)
        self.assertIn("SUM(CAST(COALESCE(x, 0) AS DECIMAL(38, 0))) AS h0", query)
        self.assertIn("AS ws_chunk_id, sales.orders.* FROM sales.orders WHERE", query)
        self.assertIn("GROUP BY ws_chunk_id", query)


    def test_aggregate_query_filters_the_checked_range(self) -> None:
        query = chunk_aggregate_query("sales.orders", "0", [], [], where="[id] >= 100")
        self.assertIn("FROM sales.orders WHERE [id] >= 100", query)

    def test_csa_changes_select_the_keys_changed_in_the_range(self) -> None:
        changes = CsaChanges.parse(
            "dbo.ORDERS", "epic_util.csa_orders", ["ORDER_ID", "LINE"], "2026-09-01", "2026-10-01"
        )
        self.assertEqual(
            changes.source_sql("postgres"),
            "EXISTS (SELECT 1 FROM epic_util.csa_orders csa WHERE csa.ORDER_ID = dbo.ORDERS.ORDER_ID "
            "AND csa.LINE = dbo.ORDERS.LINE AND csa._UPDATE_DT >= CAST('2026-09-01 00:00:00' AS TIMESTAMP) "
            "AND csa._UPDATE_DT < CAST('2026-10-01 00:00:00' AS TIMESTAMP))",
        )
        self.assertEqual(
            changes.keys_query("postgres"),
            "(SELECT DISTINCT csa.ORDER_ID AS ORDER_ID, csa.LINE AS LINE FROM epic_util.csa_orders csa "
            "WHERE csa._UPDATE_DT >= CAST('2026-09-01 00:00:00' AS TIMESTAMP) "
            "AND csa._UPDATE_DT < CAST('2026-10-01 00:00:00' AS TIMESTAMP)) csa_changes",
        )
        self.assertEqual(
            changes.describe(datetime(2026, 8, 1)),
            "keys changed since 2026-09-01 00:00:00 and before 2026-10-01 00:00:00 in epic_util.csa_orders",
        )
        self.assertIn("the CSA change table is empty", changes.describe(None))

    def test_csa_changes_need_key_columns_and_a_time(self) -> None:
        with self.assertRaises(ValueError):
            CsaChanges.parse("dbo.ORDERS", "epic_util.csa_orders", [], "-7d", "")
        self.assertTrue(is_time_bound("-30d"))
        self.assertTrue(is_time_bound("2026-09-01"))
        self.assertFalse(is_time_bound("100"))
        self.assertFalse(is_time_bound(""))

    def test_range_bounds_are_typed(self) -> None:
        self.assertEqual(parse_range_bound(" 100 ", "number"), Decimal("100"))
        self.assertIsNone(parse_range_bound("", "number"))
        self.assertEqual(parse_range_bound("2026-09-01", "timestamp"), datetime(2026, 9, 1))
        self.assertEqual(parse_range_bound("2026-09-01 12:30:00", "timestamp"), datetime(2026, 9, 1, 12, 30))
        self.assertEqual(parse_range_bound("O'Neil", "string"), "O'Neil")

    def test_relative_bound_counts_whole_days(self) -> None:
        now = datetime(2026, 10, 17, 15, 45)
        self.assertEqual(parse_range_bound("-30d", "timestamp", now), datetime(2026, 9, 17))

    def test_invalid_bounds_are_rejected(self) -> None:
        for value, kind in (("1; DROP TABLE x", "number"), ("NaN", "number"), ("yesterday", "timestamp"), ("-30", "timestamp")):
            with self.assertRaises(ValueError, msg=value):
                parse_range_bound(value, kind)

    def test_range_sql_per_engine(self) -> None:
        ids = RowRange.parse("OrderId", "number", "100", "200")
        self.assertEqual(ids.source_sql("postgres"), '"OrderId" >= 100 AND "OrderId" < 200')
        self.assertEqual(ids.describe(), "OrderId >= 100 and < 200")
        since = RowRange.parse("modified_at", "timestamp", "2026-09-01", "")
        self.assertEqual(since.source_sql("sqlserver"), "[modified_at] >= CAST('2026-09-01 00:00:00' AS DATETIME2)")
        names = RowRange.parse("last_name", "string", "", "O'N")
        self.assertEqual(names.source_sql("mysql"), "`last_name` < 'O''N'")
        self.assertEqual(RowRange("id", "number").source_sql("oracle"), "1=1")


    def test_boolean_code_per_engine(self) -> None:
        self.assertEqual(boolean_code_sql('"active"', "postgres"), 'CASE WHEN "active" THEN 2 WHEN NOT "active" THEN 1 ELSE 0 END')
        self.assertEqual(boolean_code_sql("[active]", "sqlserver"), "CASE WHEN [active] = 1 THEN 2 WHEN [active] = 0 THEN 1 ELSE 0 END")
        self.assertIn("`active` = 1", boolean_code_sql("`active`", "mysql"))
        # Oracle before 23ai has no BOOLEAN that Spark reads as one.
        self.assertIsNone(boolean_code_sql('"ACTIVE"', "oracle"))
        self.assertIsNone(boolean_code_sql("active", "unknown"))

    def test_date_code_per_engine(self) -> None:
        self.assertEqual(date_code_sql('"d"', "postgres"), f"COALESCE((\"d\" - DATE '1970-01-01') + {DATE_OFFSET_DAYS}, 0)")
        self.assertEqual(date_code_sql("[d]", "sqlserver"), f"COALESCE(DATEDIFF(DAY, '1970-01-01', [d]) + {DATE_OFFSET_DAYS}, 0)")
        self.assertIn("DATEDIFF(`d`, '1970-01-01')", date_code_sql("`d`", "mysql"))
        self.assertIn("TRUNC(\"D\") - DATE '1970-01-01'", date_code_sql('"D"', "oracle"))
        self.assertIsNone(date_code_sql("d", "unknown"))

    def test_date_offset_keeps_old_dates_apart_from_null(self) -> None:
        # 0001-01-01 is 719,162 days before 1970-01-01; with the offset it is still above the NULL code 0.
        self.assertGreater(DATE_OFFSET_DAYS - 719_162, 0)


if __name__ == "__main__":
    unittest.main()
