from __future__ import annotations

import logging
import re
import uuid
from datetime import datetime, timedelta

from pyspark.sql import functions as F

from watersync.common import quote_sql_string
from watersync.models import ReadResult
from watersync.sql_dialect import SqlDialect
from watersync.workers.epic_csa.worker import EpicCsaIngestionWorker

logger = logging.getLogger(__name__)

# Up to database.schema.procedure; the name is interpolated into SQL, so nothing else is allowed.
_PROCEDURE_NAME = re.compile(r"^[A-Za-z_][\w$#]*(\.[A-Za-z_][\w$#]*){0,2}$")
_WATERMARK_FORMAT = "%Y-%m-%d %H:%M:%S"


def is_valid_procedure_name(name: str) -> bool:
    return bool(_PROCEDURE_NAME.match(name or ""))


def format_watermark(value: datetime) -> str:
    """Whole seconds, because the state table keeps only the first 19 characters of a watermark.

    Rounding the window end down never skips changes; they fall into the next window.
    """
    return value.strftime(_WATERMARK_FORMAT)


def parse_watermark(value: str | None) -> datetime | None:
    """None for a missing watermark or one written by another method (e.g. a CSA extract key)."""
    for pattern in (_WATERMARK_FORMAT, f"{_WATERMARK_FORMAT}.%f"):
        try:
            return datetime.strptime(value or "", pattern)
        except ValueError:
            continue
    return None


def changed_rows_temp_table(run_id: str) -> str:
    # A global temp table, because Epic's procedure fills it inside dynamic SQL,
    # where a #local table would be dropped on return. It goes away when the connection closes.
    return f"##ws_chg_{run_id}"


def _require_supported_dialect(dialect: SqlDialect) -> None:
    if dialect not in ("sqlserver", "postgres"):
        raise ValueError(f"EPIC stored-procedure ingestion does not support the '{dialect}' dialect")


def _require_valid_procedure(procedure: str) -> None:
    if not is_valid_procedure_name(procedure):
        raise ValueError(f"Invalid EPIC changed-rows procedure name: {procedure!r}")


def _sql_server_datetime(value: str) -> str:
    # The 'T' form is read the same way whatever the session's language or DATEFORMAT.
    return quote_sql_string(value).replace(" ", "T", 1)


def build_procedure_call(procedure: str, table_name: str, start: str, end: str, temp_table: str) -> str:
    """SQL Server statement that fills temp_table with the rows changed in [start, end).

    Sent as the JDBC prepareQuery, so the SELECT from temp_table runs on the same connection.
    Arguments are positional: the order (table, start, end, temp table) is the same in Epic's
    ESP_CR_ALTERED_ROWS (@i_TableName, ...) and in mocks that name them differently.
    """
    _require_valid_procedure(procedure)
    # NOCOUNT keeps row-count messages from coming back ahead of the SELECT's result set.
    return (
        f"SET NOCOUNT ON; EXEC {procedure} N'{quote_sql_string(table_name)}', "
        f"'{_sql_server_datetime(start)}', '{_sql_server_datetime(end)}', "
        f"N'{quote_sql_string(temp_table)}';"
    )


def build_changed_rows_query(temp_table: str, source_table: str, keys: list[str]) -> str:
    """SQL Server: join the procedure's changed keys back to the source; a missing source row means a delete.

    Keeps the latest change per key, since the procedure returns every change in the window.
    """
    key_list = ", ".join(keys)
    join_condition = " AND ".join(f"chg.{key} = main.{key}" for key in keys)
    key_aliases = ", ".join(f"chg.{key} AS _chg_key_{key}" for key in keys)
    return (
        f"(SELECT chg.CR_UPD_DATE AS _csa_update_dt, {key_aliases}, main.* "
        f"FROM (SELECT {key_list}, CR_UPD_DATE, ROW_NUMBER() OVER "
        f"(PARTITION BY {key_list} ORDER BY CR_UPD_DATE DESC) AS _ws_change_rank FROM {temp_table}) chg "
        f"LEFT JOIN {source_table} main ON {join_condition} "
        f"WHERE chg._ws_change_rank = 1) chg_source"
    )


def build_postgres_changed_rows_query(
    procedure: str,
    table_name: str,
    source_table: str,
    keys: list[str],
    start: str,
    end: str,
) -> str:
    """Postgres (the Lakebase mock): one SELECT over the procedure's set-returning wrapper.

    The Postgres JDBC driver returns no result set for "CALL ...; SELECT ...", so prepareQuery
    can't be used. <procedure>_select calls the procedure and returns the temp table's rows as
    jsonb; jsonb_populate_record turns them back into the source table's key types.
    """
    _require_valid_procedure(procedure)
    join_condition = " AND ".join(f"chg_key.{key} = main.{key}" for key in keys)
    key_aliases = ", ".join(f"chg_key.{key} AS _chg_key_{key}" for key in keys)
    return (
        f"(SELECT (chg.change_row ->> 'cr_upd_date')::timestamp AS _csa_update_dt, {key_aliases}, main.* "
        f"FROM {procedure}_select('{quote_sql_string(table_name)}', "
        f"TIMESTAMP '{quote_sql_string(start)}', TIMESTAMP '{quote_sql_string(end)}', 1) AS chg(change_row) "
        f"CROSS JOIN LATERAL jsonb_populate_record(NULL::{source_table}, chg.change_row) AS chg_key "
        f"LEFT JOIN {source_table} main ON {join_condition}) chg_source"
    )


def source_now_query(dialect: SqlDialect) -> str:
    """Source server time as text, so no Spark or Python timezone conversion shifts the window."""
    _require_supported_dialect(dialect)
    if dialect == "sqlserver":
        return "(SELECT CONVERT(VARCHAR(23), SYSDATETIME(), 121) AS now_ts) source_now"
    return "(SELECT to_char(LOCALTIMESTAMP, 'YYYY-MM-DD HH24:MI:SS.MS') AS now_ts) source_now"


class EpicStoredProcedureIngestionWorker(EpicCsaIngestionWorker):
    """Reads EPIC changes through the changed-rows stored procedure (ESP_CR_ALTERED_ROWS).

    Epic keeps the procedure's interface stable across upgrades, unlike the CSA
    tables behind it. The watermark is the end of the last [start, end) window.
    """

    def get_last_watermark(self) -> str | None:
        last = parse_watermark(super().get_last_watermark())
        return format_watermark(last) if last else None

    def get_window_end(self) -> str:
        """Source server time minus watermark_threshold_minutes, so late commits are not skipped."""
        row = self.build_jdbc_reader(source_now_query(self.sql_dialect)).load().first()
        now = parse_watermark(row["now_ts"])
        if now is None:
            raise ValueError(f"Unexpected source timestamp format: {row['now_ts']!r}")
        return format_watermark(now - timedelta(minutes=self.config.watermark_threshold_minutes))

    def read_changed_rows(self, start: str, end: str):
        keys = self.config.key_column_list
        procedure = self.config.epic_change_procedure
        source_table = self.config.source_table_name
        table_name = source_table.split(".")[-1]
        # Single partition: each partition would run the procedure again on its own connection.
        if self.sql_dialect == "sqlserver":
            temp_table = changed_rows_temp_table(uuid.uuid4().hex[:12])
            reader = self.build_jdbc_reader(
                build_changed_rows_query(temp_table, source_table, keys),
                prepare_query=build_procedure_call(procedure, table_name, start, end, temp_table),
            )
        else:
            _require_supported_dialect(self.sql_dialect)
            reader = self.build_jdbc_reader(
                build_postgres_changed_rows_query(procedure, table_name, source_table, keys, start, end)
            )
        df = reader.load()
        df = df.withColumn("_IS_DELETED", F.col(keys[0]).isNull())
        for key in keys:
            df = df.withColumn(key, F.coalesce(F.col(key), F.col(f"_chg_key_{key}")))
        return df.drop(*[f"_chg_key_{key}" for key in keys])

    def read_source(self) -> ReadResult:
        _ctx = {
            "ingestion_group": self.config.ingestion_group,
            "source_table": self.config.source_table_name,
        }
        # Capture the window end before any read, so changes made during a full
        # load are picked up again by the next incremental window.
        window_end = self.get_window_end()
        last_watermark = self.get_last_watermark()

        if self.effective_full_refresh or last_watermark is None:
            logger.info(
                "[READ-ESP] %s — %s, performing full source read  window_end=%s",
                self.config.source_table_name,
                "full_refresh requested" if self.effective_full_refresh else "first run",
                window_end,
                extra=_ctx,
            )
            return ReadResult(df=self.read_full_source_jdbc(), persisted_watermark=window_end)

        if window_end <= last_watermark:
            logger.info(
                "[READ-ESP] %s — window is empty (last=%s, end=%s), skipping",
                self.config.source_table_name,
                last_watermark,
                window_end,
                extra=_ctx,
            )
            return ReadResult(df=None, skip=True)

        logger.info(
            "[READ-ESP] %s — incremental via %s  window=[%s, %s)",
            self.config.source_table_name,
            self.config.epic_change_procedure,
            last_watermark,
            window_end,
            extra=_ctx,
        )
        return ReadResult(
            df=self.read_changed_rows(last_watermark, window_end),
            persisted_watermark=window_end,
        )
