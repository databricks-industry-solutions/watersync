from __future__ import annotations

import sys
import types
import unittest
from unittest.mock import MagicMock, patch

delta_tables = types.ModuleType("delta.tables")
delta_tables.DeltaTable = object
sys.modules.setdefault("delta.tables", delta_tables)

from watersync.models import IngestionConfig, JdbcRuntimeSettings
from watersync.row_check import RowCheckRunner, result_status, target_has_scd2_history


def make_config(**overrides) -> IngestionConfig:
    values = dict(
        ingestion_group="sales",
        source_table_name="public.orders",
        staging_table_fqn="main.watersync.staging_orders",
        target_table_fqn="main.watersync.orders",
        ingestion_type="incremental",
        key_columns="order_id",
        watermark_column="updated_at",
        partition_column="",
        predicate_column="",
        jdbc_url="jdbc:postgresql://host:5432/db",
    )
    values.update(overrides)
    return IngestionConfig(**values)


class FakeSpark:
    def __init__(self) -> None:
        self.statements: list[str] = []

    def sql(self, statement: str):
        self.statements.append(statement)


def make_runner(check_mode: str = "basic") -> RowCheckRunner:
    runtime = JdbcRuntimeSettings(
        configuration_fqn="main.watersync.jdbc_ingestion_config",
        watermark_fqn="main.watersync.jdbc_ingestion_watermark",
    )
    return RowCheckRunner(
        FakeSpark(),
        runtime,
        results_fqn="main.watersync._watersync_row_checks",
        check_mode=check_mode,
        job_run_id="42",
    )


class RowCheckTest(unittest.TestCase):
    def test_rejects_unknown_check_mode(self) -> None:
        with self.assertRaises(ValueError):
            make_runner("full")

    def test_scd2_history_only_for_incremental_and_snapshot_cdc(self) -> None:
        self.assertTrue(target_has_scd2_history(make_config()))
        self.assertTrue(target_has_scd2_history(make_config(ingestion_type="full", auto_cdc_from_snapshot=True)))
        self.assertFalse(target_has_scd2_history(make_config(ingestion_type="full")))

    def test_equal_counts_match(self) -> None:
        runner = make_runner()
        with (
            patch.object(runner, "active_target_rows"),
            patch.object(runner, "compare_counts", return_value={"source_count": 10, "target_count": 10}),
        ):
            result = runner.check_table(make_config())
        self.assertEqual(result["status"], "MATCH")
        self.assertEqual(result["count_diff"], 0)
        self.assertEqual(result["check_mode"], "basic")
        self.assertEqual(result["job_run_id"], "42")
        self.assertIsNone(result["error"])

    def test_count_difference_is_target_minus_source(self) -> None:
        runner = make_runner()
        with (
            patch.object(runner, "active_target_rows"),
            patch.object(runner, "compare_counts", return_value={"source_count": 10, "target_count": 12}),
        ):
            result = runner.check_table(make_config())
        self.assertEqual(result["status"], "MISMATCH")
        self.assertEqual(result["count_diff"], 2)

    def test_detailed_mismatch_with_equal_counts(self) -> None:
        runner = make_runner("detailed")
        detailed = {
            "source_count": 10,
            "target_count": 10,
            "missing_in_target": 0,
            "extra_in_target": 0,
            "mismatched_rows": 3,
            "mismatched_chunks": 1,
            "drilled_chunks": 1,
        }
        with (
            patch.object(runner, "active_target_rows"),
            patch.object(runner, "compare_tiered", return_value=detailed),
        ):
            result = runner.check_table(make_config())
        self.assertEqual(result["status"], "MISMATCH")
        self.assertEqual(result["mismatched_rows"], 3)

    def test_chunks_cleared_by_row_comparison_match(self) -> None:
        # Aggregates can differ without a row difference (e.g. CHAR padding in a string hash).
        result = dict(
            count_diff=0, missing_in_target=0, extra_in_target=0, mismatched_rows=0, mismatched_chunks=2, drilled_chunks=2
        )
        self.assertEqual(result_status(result), "MATCH")

    def test_chunks_left_unchecked_are_a_mismatch(self) -> None:
        result = dict(
            count_diff=0, missing_in_target=0, extra_in_target=0, mismatched_rows=0, mismatched_chunks=12, drilled_chunks=10
        )
        self.assertEqual(result_status(result), "MISMATCH")

    def test_basic_result_has_no_chunk_fields(self) -> None:
        result = dict(
            count_diff=0, missing_in_target=None, extra_in_target=None, mismatched_rows=None, mismatched_chunks=None, drilled_chunks=None
        )
        self.assertEqual(result_status(result), "MATCH")

    def test_failure_is_reported_as_error(self) -> None:
        runner = make_runner()
        with patch.object(runner, "active_target_rows", side_effect=RuntimeError("Table not found")):
            result = runner.check_table(make_config())
        self.assertEqual(result["status"], "ERROR")
        self.assertIn("Table not found", result["error"])
        self.assertIsNotNone(result["duration_ms"])

    def test_range_defaults_to_the_chunk_column(self) -> None:
        from pyspark.sql.types import IntegerType, StringType, StructField, TimestampType

        fields = {
            "order_id": StructField("order_id", IntegerType()),
            "updated_at": StructField("updated_at", TimestampType()),
            "status": StructField("status", StringType()),
        }
        runner = make_runner()
        runner.range_from = "100"
        self.assertEqual(runner.resolve_range(make_config(key_columns="order_id"), fields).column, "order_id")
        # A time range goes on the timestamp watermark, even when an integer key is the chunk column.
        runner.range_from = "-7d"
        self.assertEqual(runner.resolve_range(make_config(key_columns="a,b"), fields).column, "updated_at")
        self.assertEqual(runner.resolve_range(make_config(key_columns="order_id"), fields).column, "updated_at")
        # Without a timestamp column there is nothing to filter a time range by: every row is checked.
        self.assertIsNone(runner.resolve_range(make_config(key_columns="order_id", watermark_column=""), fields))
        # A number still bounds the chunk column, and cannot bound a timestamp.
        runner.range_from = "100"
        with self.assertRaises(ValueError):
            runner.resolve_range(make_config(key_columns="a,b"), fields)

    def test_time_range_on_a_csa_table_checks_the_keys_changed_in_it(self) -> None:
        from datetime import datetime

        runner = make_runner("detailed")
        runner.range_from, runner.range_now = "-7d", datetime(2026, 10, 20, 9, 30)
        queries: list[str] = []
        changed_keys, oldest = MagicMock(name="changed_keys"), datetime(2026, 10, 15)

        def reader(query: str):
            queries.append(query)
            loaded = MagicMock()
            loaded.toDF.return_value = changed_keys
            loaded.first.return_value = [oldest]
            return MagicMock(load=MagicMock(return_value=loaded))

        worker = MagicMock(build_jdbc_reader=MagicMock(side_effect=reader))
        target = MagicMock(columns=["PAT_ENC_CSN_ID", "CONTACT_DATE"])
        config = make_config(
            source_table_name="dbo.PAT_ENC", key_columns="pat_enc_csn_id", watermark_column="", epic_csa_enabled=True
        )
        where, selected, checked = runner.select_rows(config, worker, "sqlserver", {}, target)
        self.assertEqual(
            where,
            "EXISTS (SELECT 1 FROM epic_util.csa_pat_enc csa WHERE csa.pat_enc_csn_id = dbo.PAT_ENC.pat_enc_csn_id "
            "AND csa._UPDATE_DT >= CAST('2026-10-13 00:00:00' AS DATETIME2))",
        )
        self.assertIn("SELECT DISTINCT csa.pat_enc_csn_id AS pat_enc_csn_id FROM epic_util.csa_pat_enc", queries[0])
        # The target keeps the rows of the changed keys, matched by the target's spelling of the key.
        target.join.assert_called_once_with(changed_keys, on=["PAT_ENC_CSN_ID"], how="left_semi")
        self.assertIs(selected, target.join.return_value)
        # The change table only reaches back 5 of the 7 days asked for.
        self.assertIn("CSA keeps changes only since 2026-10-15 00:00:00", checked)
        self.assertIn("more often than the CSA retention period", checked)

    def test_number_range_on_a_csa_table_stays_on_the_chunk_column(self) -> None:
        from pyspark.sql.types import IntegerType, StructField

        runner = make_runner("detailed")
        runner.range_from = "100"
        config = make_config(key_columns="order_id", watermark_column="", epic_csa_enabled=True)
        fields = {"order_id": StructField("order_id", IntegerType())}
        target = MagicMock(columns=["order_id"])
        with patch.object(runner, "range_filter", return_value="condition"):
            where, selected, checked = runner.select_rows(config, None, "postgres", fields, target)
        target.filter.assert_called_once_with("condition")
        self.assertEqual(where, '"order_id" >= 100')
        self.assertEqual(checked, "order_id >= 100")

    def test_explicit_range_column_must_exist(self) -> None:
        runner = make_runner()
        runner.range_column, runner.range_to = "missing", "5"
        with self.assertRaises(ValueError):
            runner.resolve_range(make_config(), {})

    def test_results_table_is_not_replaced(self) -> None:
        spark = FakeSpark()
        RowCheckRunner.create_results_table(spark, "main.watersync._watersync_row_checks")
        self.assertIn("CREATE TABLE IF NOT EXISTS", spark.statements[0])
        self.assertIn("main.watersync._watersync_row_checks", spark.statements[0])


if __name__ == "__main__":
    unittest.main()
