from __future__ import annotations

import sys
import types
import unittest
from dataclasses import replace

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
    build_changed_rows_query,
    build_postgres_changed_rows_query,
    build_procedure_call,
    changed_rows_temp_table,
    format_watermark,
    is_valid_procedure_name,
    parse_watermark,
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
    epic_change_procedure="dbo.ESP_CR_ALTERED_ROWS",
    jdbc_url="jdbc:sqlserver://host:1433;databaseName=Clarity",
)


class ProcedureSqlTest(unittest.TestCase):
    def test_sqlserver_call_uses_global_temp_table(self) -> None:
        temp = changed_rows_temp_table("abc")
        self.assertEqual(temp, "##ws_chg_abc")
        self.assertEqual(
            build_procedure_call(
                "dbo.ESP_CR_ALTERED_ROWS", "PAT_ENC",
                "2026-11-01 00:00:00.000", "2026-11-02 00:00:00.000", temp,
            ),
            "SET NOCOUNT ON; EXEC dbo.ESP_CR_ALTERED_ROWS N'PAT_ENC', "
            "'2026-11-01T00:00:00.000', '2026-11-02T00:00:00.000', N'##ws_chg_abc';",
        )

    def test_postgres_reads_through_select_wrapper(self) -> None:
        self.assertEqual(
            build_postgres_changed_rows_query(
                "epic_util.esp_cr_altered_rows", "orders", "dbo.orders", ["order_id", "line_id"],
                "2026-11-01 00:00:00", "2026-11-02 00:00:00",
            ),
            "(SELECT (chg.change_row ->> 'cr_upd_date')::timestamp AS _csa_update_dt, "
            "chg_key.order_id AS _chg_key_order_id, chg_key.line_id AS _chg_key_line_id, main.* "
            "FROM epic_util.esp_cr_altered_rows_select('orders', TIMESTAMP '2026-11-01 00:00:00', "
            "TIMESTAMP '2026-11-02 00:00:00', 1) AS chg(change_row) "
            "CROSS JOIN LATERAL jsonb_populate_record(NULL::dbo.orders, chg.change_row) AS chg_key "
            "LEFT JOIN dbo.orders main ON chg_key.order_id = main.order_id "
            "AND chg_key.line_id = main.line_id) chg_source",
        )

    def test_call_quotes_table_name(self) -> None:
        call = build_procedure_call("p", "o'brien", "a", "b", "t")
        self.assertIn("N'o''brien'", call)

    def test_rejects_unsupported_dialect_and_bad_names(self) -> None:
        with self.assertRaises(ValueError):
            build_procedure_call("p; DROP TABLE x", "t", "a", "b", "t")
        with self.assertRaises(ValueError):
            build_postgres_changed_rows_query("p; DROP TABLE x", "t", "dbo.t", ["k"], "a", "b")
        with self.assertRaises(ValueError):
            source_now_query("oracle")

    def test_changed_rows_query_joins_composite_key(self) -> None:
        self.assertEqual(
            build_changed_rows_query("##ws_chg_abc", "dbo.PAT_ENC", ["PAT_ENC_CSN_ID", "LINE"]),
            "(SELECT chg.CR_UPD_DATE AS _csa_update_dt, "
            "chg.PAT_ENC_CSN_ID AS _chg_key_PAT_ENC_CSN_ID, chg.LINE AS _chg_key_LINE, main.* "
            "FROM (SELECT PAT_ENC_CSN_ID, LINE, CR_UPD_DATE, ROW_NUMBER() OVER "
            "(PARTITION BY PAT_ENC_CSN_ID, LINE ORDER BY CR_UPD_DATE DESC) AS _ws_change_rank "
            "FROM ##ws_chg_abc) chg "
            "LEFT JOIN dbo.PAT_ENC main ON chg.PAT_ENC_CSN_ID = main.PAT_ENC_CSN_ID "
            "AND chg.LINE = main.LINE "
            "WHERE chg._ws_change_rank = 1) chg_source",
        )

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
        validate_epic_change_method(replace(CONFIG, epic_change_method="csa_table", epic_change_procedure=""))

    def test_invalid_stored_procedure_configs(self) -> None:
        for config in (
            replace(CONFIG, epic_change_method="cdc"),
            replace(CONFIG, epic_csa_enabled=False),
            replace(CONFIG, ingestion_type="full"),
            replace(CONFIG, key_columns=""),
            replace(CONFIG, epic_change_procedure=""),
            replace(CONFIG, epic_change_procedure="dbo.x; DROP TABLE y"),
        ):
            with self.assertRaises(ValueError, msg=config):
                validate_epic_change_method(config)

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
        return self._window_end

    def read_full_source_jdbc(self):
        self.calls.append(("full",))
        return "full_df"

    def read_changed_rows(self, start: str, end: str):
        self.calls.append(("changed", start, end))
        return "changed_df"


class ReadSourceTest(unittest.TestCase):
    def test_first_run_is_full_load_ending_at_window_end(self) -> None:
        worker = StubWorker(None, "2026-11-02 10:00:00")
        result = worker.read_source()
        self.assertEqual(worker.calls, [("full",)])
        self.assertEqual(result.persisted_watermark, "2026-11-02 10:00:00")

    def test_full_refresh_overrides_watermark(self) -> None:
        worker = StubWorker("2026-11-02 09:00:00", "2026-11-02 10:00:00", full_refresh=True)
        worker.read_source()
        self.assertEqual(worker.calls, [("full",)])

    def test_incremental_window(self) -> None:
        worker = StubWorker("2026-11-02 09:00:00", "2026-11-02 10:00:00")
        result = worker.read_source()
        self.assertEqual(
            worker.calls, [("changed", "2026-11-02 09:00:00", "2026-11-02 10:00:00")]
        )
        self.assertEqual(result.df, "changed_df")
        self.assertEqual(result.persisted_watermark, "2026-11-02 10:00:00")

    def test_empty_window_is_skipped(self) -> None:
        worker = StubWorker("2026-11-02 10:00:00", "2026-11-02 10:00:00")
        self.assertTrue(worker.read_source().skip)
        self.assertEqual(worker.calls, [])


if __name__ == "__main__":
    unittest.main()
