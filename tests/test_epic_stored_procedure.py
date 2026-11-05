from __future__ import annotations

import sys
import types
import unittest
from dataclasses import replace
from decimal import Decimal

delta_tables = types.ModuleType("delta.tables")
delta_tables.DeltaTable = object
sys.modules.setdefault("delta.tables", delta_tables)

from watersync.ingestion import JdbcIngestionOrchestrator, validate_epic_change_method
from watersync.models import IngestionConfig, JdbcRuntimeSettings
from watersync.workers import (
    EpicCsaIngestionWorker,
    EpicStoredProcedureIngestionWorker,
    TimestampWatermarkIngestionWorker,
)
from watersync.workers.epic_stored_procedure.worker import (
    build_cdc_rows_query,
    build_cdc_stats_query,
    build_changed_keys_query,
    build_key_range_query,
    build_postgres_procedure_run,
    build_procedure_call,
    build_procedure_call_until_now,
    build_temp_table_rows_query,
    build_temp_table_shape,
    cdc_table_name,
    changed_rows_temp_table,
    format_watermark,
    is_integral_key,
    is_integral_type,
    is_valid_column_names,
    is_valid_procedure_name,
    parallel_read_bounds,
    parse_watermark,
    report_schema,
    source_now_query,
)

RUNTIME = JdbcRuntimeSettings(
    configuration_fqn="main.watersync.jdbc_ingestion_config",
    watermark_fqn="main.watersync.jdbc_ingestion_watermark",
)
CONFIG = IngestionConfig(
    ingestion_group="epic",
    source_table_name="dbo.PAT_ENC",
    staging_table_fqn="main.watersync.stg_pat_enc",
    target_table_fqn="main.clarity.pat_enc",
    ingestion_type="incremental",
    key_columns="PAT_ENC_CSN_ID,LINE",
    watermark_column="",
    partition_column="",
    predicate_column="",
    epic_csa_enabled=True,
    epic_change_method="stored_procedure",
    epic_change_procedure="EPIC_UTIL.ESP_CR_ALTERED_ROWS",
    jdbc_url="jdbc:sqlserver://host:1433;databaseName=Clarity",
)


class ProcedureSqlTest(unittest.TestCase):
    def test_sqlserver_call_uses_epics_documented_parameters(self) -> None:
        temp = changed_rows_temp_table("abc")
        self.assertEqual(temp, "##ws_chg_abc")
        self.assertEqual(
            build_procedure_call(
                "EPIC_UTIL.ESP_CR_ALTERED_ROWS", "PAT_ENC", "2026-11-01 00:00:00", "2026-11-02 00:00:00", temp, False
            ),
            "SET NOCOUNT ON; EXEC EPIC_UTIL.ESP_CR_ALTERED_ROWS @i_TableName = N'PAT_ENC', "
            "@i_StartDate = '2026-11-01T00:00:00', @i_EndDate = '2026-11-02T00:00:00', "
            "@i_TempTableName = N'##ws_chg_abc', @i_NoDuplicates = 1;",
        )

    def test_line_keyed_tables_ask_for_line_numbers(self) -> None:
        call = build_procedure_call("EPIC_UTIL.ESP_CR_ALTERED_ROWS", "PAT_ENC_DX", "a", "b", "##t", True)
        self.assertTrue(call.endswith("@i_NoDuplicates = 1, @i_IncludeLineNum = 1;"))

    def test_procedure_call_computes_window_end_and_summary_on_the_server(self) -> None:
        self.assertEqual(
            build_procedure_call_until_now(
                "EPIC_UTIL.ESP_CR_ALTERED_ROWS", "PAT_ENC", "2026-11-03 14:56:44", 5, "##ws_chg_abc", False,
                "PAT_ENC_CSN_ID",
            ),
            "SET NOCOUNT ON; DECLARE @ws_window_end DATETIME2(0) = CONVERT(DATETIME2(0), CONVERT(VARCHAR(19), "
            "DATEADD(MINUTE, -5, SYSDATETIME()), 120)); "
            "IF @ws_window_end < '2026-11-03T14:56:44' SET @ws_window_end = '2026-11-03T14:56:44'; "
            "EXEC EPIC_UTIL.ESP_CR_ALTERED_ROWS @i_TableName = N'PAT_ENC', @i_StartDate = '2026-11-03T14:56:44', "
            "@i_EndDate = @ws_window_end, @i_TempTableName = N'##ws_chg_abc', @i_NoDuplicates = 1; "
            "DECLARE @ws_changed BIGINT, @ws_low BIGINT, @ws_high BIGINT; "
            "SELECT @ws_changed = COUNT_BIG(*), @ws_low = TRY_CAST(MIN(PAT_ENC_CSN_ID) AS BIGINT), "
            "@ws_high = TRY_CAST(MAX(PAT_ENC_CSN_ID) AS BIGINT) FROM ##ws_chg_abc; "
            "EXEC sp_set_session_context @key = N'ws_window_end', @value = @ws_window_end; "
            "EXEC sp_set_session_context @key = N'ws_changed', @value = @ws_changed; "
            "EXEC sp_set_session_context @key = N'ws_low', @value = @ws_low; "
            "EXEC sp_set_session_context @key = N'ws_high', @value = @ws_high;",
        )
        with self.assertRaises(ValueError):
            build_procedure_call_until_now("p; DROP TABLE x", "t", "a", 5, "##t", False, "k")

    def test_changed_keys_query_adds_one_typed_summary_row(self) -> None:
        self.assertEqual(
            build_changed_keys_query("##ws_chg_abc", ["PAT_ENC_CSN_ID", "LINE"], False),
            "(SELECT chg.PAT_ENC_CSN_ID, chg.LINE, chg.CR_UPD_DATE AS _csa_update_dt, "
            "CAST(NULL AS VARCHAR(19)) AS _ws_window_end, CAST(NULL AS BIGINT) AS _ws_changed, "
            "CAST(NULL AS BIGINT) AS _ws_low, CAST(NULL AS BIGINT) AS _ws_high FROM ##ws_chg_abc chg "
            "UNION ALL SELECT chg.PAT_ENC_CSN_ID, chg.LINE, chg.CR_UPD_DATE AS _csa_update_dt, "
            "CONVERT(VARCHAR(19), CAST(SESSION_CONTEXT(N'ws_window_end') AS DATETIME2(0)), 120), "
            "CAST(SESSION_CONTEXT(N'ws_changed') AS BIGINT), CAST(SESSION_CONTEXT(N'ws_low') AS BIGINT), "
            "CAST(SESSION_CONTEXT(N'ws_high') AS BIGINT) "
            "FROM (SELECT 1 AS one) o LEFT JOIN ##ws_chg_abc chg ON 1 = 0) chg_keys",
        )
        skipped = build_changed_keys_query("##ws_chg_abc", ["PAT_ENC_CSN_ID"], True)
        self.assertNotIn("CR_UPD_DATE", skipped)

    def test_temp_table_shape_only_creates_an_empty_table(self) -> None:
        self.assertEqual(
            build_temp_table_shape("##ws_chg_abc", "dbo.PAT_ENC", ["PAT_ENC_CSN_ID", "LINE"], False),
            "SET NOCOUNT ON; IF OBJECT_ID('tempdb..##ws_chg_abc') IS NULL "
            "SELECT TOP 0 PAT_ENC_CSN_ID, LINE, CAST(NULL AS DATETIME) AS CR_UPD_DATE INTO ##ws_chg_abc FROM dbo.PAT_ENC;",
        )
        self.assertNotIn("CR_UPD_DATE", build_temp_table_shape("##t", "dbo.PAT_ENC", ["PAT_ENC_CSN_ID"], True))

    def test_skip_update_date_and_column_names(self) -> None:
        call = build_procedure_call(
            "EPIC_UTIL.ESP_CR_ALTERED_ROWS", "PAT_ENC", "a", "b", "##t", False,
            skip_update_date=True, column_names="HSP_ACCOUNT_ID,INPATIENT_DATA_ID",
        )
        self.assertTrue(call.endswith(
            "@i_NoDuplicates = 1, @i_SkipUpdateDate = 1, @i_ColumnNames = 'HSP_ACCOUNT_ID,INPATIENT_DATA_ID';"
        ))
        self.assertIn(
            "CAST('2026-11-03T15:00:00' AS DATETIME2(0)) AS _csa_update_dt",
            build_temp_table_rows_query("##t", "dbo.PAT_ENC", ["PAT_ENC_CSN_ID"], "2026-11-03 15:00:00"),
        )
        self.assertTrue(is_valid_column_names(""))
        self.assertTrue(is_valid_column_names("HSP_ACCOUNT_ID, INPATIENT_DATA_ID"))
        self.assertFalse(is_valid_column_names("A'; DROP TABLE x --"))
        with self.assertRaises(ValueError):
            build_procedure_call("p", "t", "a", "b", "##t", False, column_names="A'--")

    def test_temp_table_rows_query_joins_composite_key(self) -> None:
        self.assertEqual(
            build_temp_table_rows_query("##ws_chg_abc", "dbo.PAT_ENC", ["PAT_ENC_CSN_ID", "LINE"]),
            "(SELECT chg.CR_UPD_DATE AS _csa_update_dt, "
            "chg.PAT_ENC_CSN_ID AS _chg_key_PAT_ENC_CSN_ID, chg.LINE AS _chg_key_LINE, main.* "
            "FROM ##ws_chg_abc chg "
            "LEFT JOIN dbo.PAT_ENC main ON chg.PAT_ENC_CSN_ID = main.PAT_ENC_CSN_ID "
            "AND chg.LINE = main.LINE) chg_source",
        )

    def test_key_range_query_and_bounds(self) -> None:
        self.assertEqual(
            build_key_range_query("dbo.PAT_ENC", "PAT_ENC_CSN_ID", 10, 99),
            "(SELECT * FROM dbo.PAT_ENC WHERE PAT_ENC_CSN_ID >= 10 AND PAT_ENC_CSN_ID <= 99) src_range",
        )
        self.assertEqual(parallel_read_bounds(5_000_000, 1, 5_000_000, 8), (1, 5_000_000))
        # 1,000 changed keys over a 1M-wide range: too sparse for a range read.
        self.assertIsNone(parallel_read_bounds(1_000, 1, 1_000_000, 8))
        self.assertIsNone(parallel_read_bounds(5_000_000, 1, 5_000_000, 1))
        self.assertIsNone(parallel_read_bounds(0, None, None, 8))

    def test_postgres_mock_runs_through_its_run_function_and_cdc_table(self) -> None:
        self.assertEqual(
            build_postgres_procedure_run("epic_util.esp_cr_altered_rows", "orders", "dbo", "2026-11-03 14:56:44", 0, True),
            "(SELECT epic_util.esp_cr_altered_rows_run('orders', 'dbo', TIMESTAMP '2026-11-03 14:56:44', 0, 1) "
            "AS window_end) procedure_run",
        )
        self.assertEqual(cdc_table_name("epic_util.esp_cr_altered_rows", "orders"), "epic_util.CDC_orders")
        self.assertEqual(report_schema("Clarity.dbo.PAT_ENC"), "dbo")
        self.assertEqual(
            build_cdc_stats_query("epic_util.CDC_orders", "order_id", "2026-11-03 14:56:44", "2026-11-03 15:00:00", "postgres"),
            "(SELECT COUNT(*) AS changed, MIN(order_id) AS low, MAX(order_id) AS high FROM epic_util.CDC_orders "
            "WHERE CR_UPD_DATE >= CAST('2026-11-03 14:56:44' AS TIMESTAMP) "
            "AND CR_UPD_DATE < CAST('2026-11-03 15:00:00' AS TIMESTAMP)) cdc_stats",
        )
        self.assertIn(
            "FROM epic_util.CDC_orders cdc LEFT JOIN dbo.orders main ON cdc.order_id = main.order_id "
            "AND cdc.line_id = main.line_id WHERE cdc.CR_UPD_DATE >= CAST('a' AS TIMESTAMP)",
            build_cdc_rows_query("epic_util.CDC_orders", "dbo.orders", ["order_id", "line_id"], "a", "b", "postgres"),
        )

    def test_call_quotes_table_name(self) -> None:
        self.assertIn("@i_TableName = N'o''brien'", build_procedure_call("p", "o'brien", "a", "b", "##t", False))

    def test_rejects_unsupported_dialect_and_bad_names(self) -> None:
        with self.assertRaises(ValueError):
            build_procedure_call("p; DROP TABLE x", "t", "a", "b", "##t", False)
        with self.assertRaises(ValueError):
            cdc_table_name("p; DROP TABLE x", "t")
        with self.assertRaises(ValueError):
            source_now_query("oracle")

    def test_integral_key_types(self) -> None:
        from pyspark.sql import types as T

        self.assertTrue(is_integral_type(T.IntegerType()))
        self.assertTrue(is_integral_type(T.DecimalType(18, 0)))
        self.assertFalse(is_integral_type(T.DecimalType(18, 2)))
        self.assertFalse(is_integral_type(T.StringType()))

    def test_integral_keys(self) -> None:
        self.assertTrue(is_integral_key(42))
        self.assertTrue(is_integral_key(Decimal("42")))
        self.assertFalse(is_integral_key(Decimal("4.2")))
        self.assertFalse(is_integral_key("Z1234"))
        self.assertFalse(is_integral_key(True))

    def test_procedure_names(self) -> None:
        for name in ("ESP_CR_ALTERED_ROWS", "dbo.ESP_GET_CHANGED_ROWS", "Clarity.dbo.ESP_CR_ALTERED_ROWS"):
            self.assertTrue(is_valid_procedure_name(name), name)
        for name in ("", "dbo.", "a.b.c.d", "dbo.x; DROP TABLE y", "x'--"):
            self.assertFalse(is_valid_procedure_name(name), name)

    def test_csa_extract_key_is_not_a_watermark(self) -> None:
        self.assertIsNone(parse_watermark("1730000000000000"))
        self.assertIsNone(parse_watermark(None))
        self.assertIsNotNone(parse_watermark("2026-11-02 10:00:00.123"))

    def test_watermark_survives_state_table_truncation(self) -> None:
        # update_watermark_state stores str(watermark)[:19]
        stored = format_watermark(parse_watermark("2026-11-02 10:00:00.987"))[:19]
        self.assertEqual(stored, "2026-11-02 10:00:00")
        self.assertEqual(parse_watermark(stored), parse_watermark("2026-11-02 10:00:00"))


class ConfigValidationTest(unittest.TestCase):
    def test_valid_stored_procedure_config(self) -> None:
        validate_epic_change_method(CONFIG)
        validate_epic_change_method(replace(CONFIG, epic_change_procedure=""))
        validate_epic_change_method(replace(CONFIG, epic_change_method="csa_table", epic_change_procedure=""))

    def test_empty_procedure_uses_epics_documented_name(self) -> None:
        worker = EpicStoredProcedureIngestionWorker(None, RUNTIME, replace(CONFIG, epic_change_procedure=""))
        self.assertEqual(worker.procedure, "EPIC_UTIL.ESP_CR_ALTERED_ROWS")
        custom = EpicStoredProcedureIngestionWorker(None, RUNTIME, replace(CONFIG, epic_change_procedure="dbo.MY_PROC"))
        self.assertEqual(custom.procedure, "dbo.MY_PROC")

    def test_invalid_stored_procedure_configs(self) -> None:
        for config in (
            replace(CONFIG, epic_change_method="cdc"),
            replace(CONFIG, epic_csa_enabled=False),
            replace(CONFIG, ingestion_type="full"),
            replace(CONFIG, key_columns=""),
            replace(CONFIG, epic_change_procedure="dbo.x; DROP TABLE y"),
        ):
            with self.assertRaises(ValueError, msg=config):
                validate_epic_change_method(config)

    def test_invalid_column_names_config(self) -> None:
        with self.assertRaises(ValueError):
            validate_epic_change_method(replace(CONFIG, epic_change_column_names="A; DROP TABLE x"))
        validate_epic_change_method(replace(CONFIG, epic_change_column_names="HSP_ACCOUNT_ID,INPATIENT_DATA_ID"))

    def test_table_and_keys_must_be_plain_names(self) -> None:
        # The procedure's batch can run several statements, so these never reach it.
        for config in (
            replace(CONFIG, source_table_name="dbo.PAT_ENC; DROP TABLE x"),
            replace(CONFIG, source_table_name="[dbo].[PAT ENC]"),
            replace(CONFIG, key_columns="PAT_ENC_CSN_ID) AND 1=1; EXEC x; SELECT (PAT_ENC_CSN_ID"),
        ):
            with self.assertRaises(ValueError, msg=config):
                validate_epic_change_method(config)
            with self.assertRaises(ValueError, msg=config):
                EpicStoredProcedureIngestionWorker(None, RUNTIME, config).read_changes("2026-01-01 00:00:00")
        validate_epic_change_method(replace(CONFIG, source_table_name="Clarity.dbo.PAT_ENC", key_columns="PAT_ENC_CSN_ID, LINE"))
        # Other methods keep accepting quoted names.
        validate_epic_change_method(replace(CONFIG, epic_change_method="csa_table", source_table_name="[dbo].[PAT ENC]"))

    def test_build_worker_dispatch(self) -> None:
        orchestrator = JdbcIngestionOrchestrator(spark=None, runtime=RUNTIME)
        self.assertIsInstance(orchestrator.build_worker(CONFIG), EpicStoredProcedureIngestionWorker)
        self.assertIs(
            type(orchestrator.build_worker(replace(CONFIG, epic_change_method="csa_table"))),
            EpicCsaIngestionWorker,
        )
        self.assertIsInstance(
            orchestrator.build_worker(replace(CONFIG, epic_csa_enabled=False, epic_change_method="csa_table")),
            TimestampWatermarkIngestionWorker,
        )


class StubWorker(EpicStoredProcedureIngestionWorker):
    def __init__(self, last_watermark: str | None, window_end: str, full_refresh: bool = False) -> None:
        super().__init__(None, RUNTIME, CONFIG)
        self.__dict__["effective_full_refresh"] = full_refresh
        self._last_watermark = last_watermark
        self._window_end = window_end
        self.calls: list[tuple] = []

    def get_last_watermark(self) -> str | None:
        return self._last_watermark

    def get_window_end(self) -> str:
        self.calls.append(("clock",))
        return self._window_end

    def read_full_source_jdbc(self):
        self.calls.append(("full",))
        return "full_df"

    def read_changes(self, start: str):
        # The real read gets the window end from the procedure's statement.
        self.calls.append(("changes", start))
        return ("changed_df" if self._window_end > start else None), self._window_end


class ReadSourceTest(unittest.TestCase):
    def test_first_run_is_full_load_ending_at_window_end(self) -> None:
        worker = StubWorker(None, "2026-11-02 10:00:00")
        result = worker.read_source()
        self.assertEqual(worker.calls, [("clock",), ("full",)])
        self.assertEqual(result.persisted_watermark, "2026-11-02 10:00:00")

    def test_full_refresh_overrides_watermark(self) -> None:
        worker = StubWorker("2026-11-02 09:00:00", "2026-11-02 10:00:00", full_refresh=True)
        worker.read_source()
        self.assertEqual(worker.calls, [("clock",), ("full",)])

    def test_incremental_reads_changes_without_a_clock_query(self) -> None:
        worker = StubWorker("2026-11-02 09:00:00", "2026-11-02 10:00:00")
        result = worker.read_source()
        self.assertEqual(worker.calls, [("changes", "2026-11-02 09:00:00")])
        self.assertEqual(result.df, "changed_df")
        self.assertEqual(result.persisted_watermark, "2026-11-02 10:00:00")

    def test_empty_window_is_skipped(self) -> None:
        worker = StubWorker("2026-11-02 10:00:00", "2026-11-02 10:00:00")
        self.assertTrue(worker.read_source().skip)
        self.assertEqual(worker.calls, [("changes", "2026-11-02 10:00:00")])


class _ClockCountingWorker(EpicStoredProcedureIngestionWorker):
    """SQL Server with one partition: a clock query, then the procedure joined on the source."""

    sql_dialect = "sqlserver"

    def __init__(self, window_end: str) -> None:
        super().__init__(None, RUNTIME, replace(CONFIG, num_partitions=1))
        self._window_end = window_end
        self.calls: list[tuple] = []

    def get_window_end(self) -> str:
        self.calls.append(("clock",))
        return self._window_end

    def read_changed_rows_on_source(self, start: str, end: str):
        self.calls.append(("on_source", start, end))
        return "changed_df"


class ReadChangesTest(unittest.TestCase):
    def test_single_partition_sqlserver_uses_clock_then_joins_on_source(self) -> None:
        worker = _ClockCountingWorker("2026-11-02 10:00:00")
        self.assertEqual(worker.read_changes("2026-11-02 09:00:00"), ("changed_df", "2026-11-02 10:00:00"))
        self.assertEqual(worker.calls, [("clock",), ("on_source", "2026-11-02 09:00:00", "2026-11-02 10:00:00")])

    def test_single_partition_empty_window(self) -> None:
        worker = _ClockCountingWorker("2026-11-02 09:00:00")
        self.assertEqual(worker.read_changes("2026-11-02 09:00:00"), (None, "2026-11-02 09:00:00"))
        self.assertEqual(worker.calls, [("clock",)])


if __name__ == "__main__":
    unittest.main()
