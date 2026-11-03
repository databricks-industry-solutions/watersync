from __future__ import annotations

import logging
import re
import uuid
from datetime import datetime, timedelta
from decimal import Decimal

from pyspark.sql import functions as F
from pyspark.sql import types as T

from watersync.common import quote_sql_string
from watersync.models import ReadResult
from watersync.sql_dialect import SqlDialect, sql_timestamp_literal
from watersync.workers.epic_csa.worker import EpicCsaIngestionWorker

logger = logging.getLogger(__name__)

# Up to database.schema.procedure; the name is interpolated into SQL, so nothing else is allowed.
_PROCEDURE_NAME = re.compile(r"^[A-Za-z_][\w$#]*(\.[A-Za-z_][\w$#]*){0,2}$")
_WATERMARK_FORMAT = "%Y-%m-%d %H:%M:%S"
# A parallel range read is used only when it reads at most this many source rows per changed key,
# assuming roughly dense keys; sparser changes are joined on the source over one connection.
_MAX_SCAN_RATIO = 10
# The changed-rows procedure in Epic's SQL Server documentation, used when epic_change_procedure is empty.
DEFAULT_EPIC_CHANGE_PROCEDURE = "EPIC_UTIL.ESP_CR_ALTERED_ROWS"
_COLUMN_NAMES = re.compile(r"^[A-Za-z_][\w$#]*(\s*,\s*[A-Za-z_][\w$#]*)*$")


def is_valid_procedure_name(name: str) -> bool:
    return bool(_PROCEDURE_NAME.match(name or ""))


def is_valid_column_names(value: str) -> bool:
    """epic_change_column_names: empty, or comma-separated column names (passed to @i_ColumnNames)."""
    return not value or bool(_COLUMN_NAMES.match(value))


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


def _require_supported_dialect(dialect: SqlDialect) -> None:
    if dialect not in ("sqlserver", "postgres"):
        raise ValueError(f"EPIC stored-procedure ingestion does not support the '{dialect}' dialect")


def _require_valid_procedure(procedure: str) -> None:
    if not is_valid_procedure_name(procedure):
        raise ValueError(f"Invalid EPIC changed-rows procedure name: {procedure!r}")


def _sql_server_datetime(value: str) -> str:
    # The 'T' form is read the same way whatever the session's language or DATEFORMAT.
    return quote_sql_string(value).replace(" ", "T", 1)


def is_integral_type(data_type: T.DataType) -> bool:
    """True for an integer key column (INT, BIGINT, NUMERIC(18, 0)), which Spark can split into ranges."""
    if isinstance(data_type, (T.ByteType, T.ShortType, T.IntegerType, T.LongType)):
        return True
    return isinstance(data_type, T.DecimalType) and data_type.scale == 0


def is_integral_key(value) -> bool:
    """True for an integer key value (INT, BIGINT, NUMERIC(18, 0)), which Spark can split into ranges."""
    if isinstance(value, bool):
        return False
    if isinstance(value, int):
        return True
    return isinstance(value, Decimal) and value == value.to_integral_value()


def parallel_read_bounds(changed: int, low, high, num_partitions: int) -> tuple[int, int] | None:
    """[low, high] of the range read, or None when one connection with a join on the source is better."""
    if num_partitions <= 1 or changed <= 0 or low is None or high is None:
        return None
    low, high = int(low), int(high)
    if high - low + 1 > _MAX_SCAN_RATIO * changed:
        return None
    return low, high


def source_now_query(dialect: SqlDialect) -> str:
    """Source server time as text, so no Spark or Python timezone conversion shifts the window."""
    _require_supported_dialect(dialect)
    if dialect == "sqlserver":
        return "(SELECT CONVERT(VARCHAR(23), SYSDATETIME(), 121) AS now_ts) source_now"
    return "(SELECT to_char(LOCALTIMESTAMP, 'YYYY-MM-DD HH24:MI:SS.MS') AS now_ts) source_now"


# --- SQL Server: Epic's procedure fills a temp table named by the caller --------------------------------
#
# The procedure runs in sessionInitStatement, which Spark runs only on the connection that reads the data.
# prepareQuery, which Spark also runs when it first resolves the result's columns, only creates an empty
# temp table with the right columns there; on the reading connection the procedure has already filled it.


def changed_rows_temp_table(run_id: str) -> str:
    # A global temp table, as in Epic's sample (##CHANGES_PAT_ENC): the procedure fills it inside its own
    # scope, where a #local table would be dropped on return. It goes away when the connection closes.
    return f"##ws_chg_{run_id}"


def build_procedure_call(
    procedure: str,
    table_name: str,
    start: str,
    end: str,
    temp_table: str,
    include_line_num: bool,
    skip_update_date: bool = False,
    column_names: str = "",
    end_is_variable: bool = False,
) -> str:
    """EXEC with the parameters Epic documents for SQL Server, filling temp_table with [start, end).

    @i_NoDuplicates = 1 returns only the latest change per row. Line-keyed tables (PAT_ENC_CSN_ID, LINE)
    only get their line columns with @i_IncludeLineNum = 1. @i_SkipUpdateDate = 1 leaves out
    CR_UPD_DATE, which Epic notes is faster; @i_ColumnNames limits the changes to those columns.
    """
    _require_valid_procedure(procedure)
    if not is_valid_column_names(column_names):
        raise ValueError(f"Invalid epic_change_column_names: {column_names!r}")
    end_argument = end if end_is_variable else f"'{_sql_server_datetime(end)}'"
    arguments = [
        f"@i_TableName = N'{quote_sql_string(table_name)}'",
        f"@i_StartDate = '{_sql_server_datetime(start)}'",
        f"@i_EndDate = {end_argument}",
        f"@i_TempTableName = N'{quote_sql_string(temp_table)}'",
        "@i_NoDuplicates = 1",
    ]
    if include_line_num:
        arguments.append("@i_IncludeLineNum = 1")
    if skip_update_date:
        arguments.append("@i_SkipUpdateDate = 1")
    if column_names:
        arguments.append(f"@i_ColumnNames = '{column_names}'")
    # NOCOUNT keeps row-count messages from coming back ahead of the SELECT's result set.
    return f"SET NOCOUNT ON; EXEC {procedure} {', '.join(arguments)};"


def build_procedure_call_until_now(
    procedure: str,
    table_name: str,
    start: str,
    threshold_minutes: int,
    temp_table: str,
    include_line_num: bool,
    first_key: str,
    skip_update_date: bool = False,
    column_names: str = "",
) -> str:
    """sessionInitStatement: the procedure for a window ending at the server's clock minus threshold_minutes.

    @ws_window_end is cut to whole seconds (style 120 truncates) and never before start. After the call,
    one scan of the temp table gives the count and the first key's range (NULL when the key isn't a
    number). All four are kept in the session context, because sessionInitStatement and the query are
    separate batches; build_changed_keys_query returns them, so no clock query or Spark aggregation is needed.
    """
    start_literal = _sql_server_datetime(start)
    call = build_procedure_call(
        procedure, table_name, start, "@ws_window_end", temp_table, include_line_num,
        skip_update_date, column_names, end_is_variable=True,
    )
    return (
        "SET NOCOUNT ON; "
        "DECLARE @ws_window_end DATETIME2(0) = CONVERT(DATETIME2(0), CONVERT(VARCHAR(19), "
        f"DATEADD(MINUTE, -{int(threshold_minutes)}, SYSDATETIME()), 120)); "
        f"IF @ws_window_end < '{start_literal}' SET @ws_window_end = '{start_literal}'; "
        f"{call.removeprefix('SET NOCOUNT ON; ')} "
        "DECLARE @ws_changed BIGINT, @ws_low BIGINT, @ws_high BIGINT; "
        f"SELECT @ws_changed = COUNT_BIG(*), @ws_low = TRY_CAST(MIN({first_key}) AS BIGINT), "
        f"@ws_high = TRY_CAST(MAX({first_key}) AS BIGINT) FROM {temp_table}; "
        "EXEC sp_set_session_context @key = N'ws_window_end', @value = @ws_window_end; "
        "EXEC sp_set_session_context @key = N'ws_changed', @value = @ws_changed; "
        "EXEC sp_set_session_context @key = N'ws_low', @value = @ws_low; "
        "EXEC sp_set_session_context @key = N'ws_high', @value = @ws_high;"
    )


def build_temp_table_shape(temp_table: str, source_table: str, keys: list[str], skip_update_date: bool) -> str:
    """prepareQuery: an empty temp table with the procedure's columns, so Spark can resolve the result.

    Does nothing where the procedure (in sessionInitStatement) has already filled the table.
    """
    update_column = "" if skip_update_date else ", CAST(NULL AS DATETIME) AS CR_UPD_DATE"
    return (
        f"SET NOCOUNT ON; IF OBJECT_ID('tempdb..{temp_table}') IS NULL "
        f"SELECT TOP 0 {', '.join(keys)}{update_column} INTO {temp_table} FROM {source_table};"
    )


def build_changed_keys_query(temp_table: str, keys: list[str], skip_update_date: bool) -> str:
    """SQL Server: changed keys and their change time, plus one row with NULL keys carrying the summary.

    The summary (window end, count, key range) comes from the session context set by
    build_procedure_call_until_now. LEFT JOIN ON 1 = 0 gives that row the temp table's column types,
    so the UNION ALL keeps them, and it comes back even when nothing changed.
    """
    key_columns = ", ".join(f"chg.{key}" for key in keys)
    update_column = "" if skip_update_date else ", chg.CR_UPD_DATE AS _csa_update_dt"
    no_summary = (
        "CAST(NULL AS VARCHAR(19)) AS _ws_window_end, CAST(NULL AS BIGINT) AS _ws_changed, "
        "CAST(NULL AS BIGINT) AS _ws_low, CAST(NULL AS BIGINT) AS _ws_high"
    )
    summary = (
        "CONVERT(VARCHAR(19), CAST(SESSION_CONTEXT(N'ws_window_end') AS DATETIME2(0)), 120), "
        "CAST(SESSION_CONTEXT(N'ws_changed') AS BIGINT), CAST(SESSION_CONTEXT(N'ws_low') AS BIGINT), "
        "CAST(SESSION_CONTEXT(N'ws_high') AS BIGINT)"
    )
    return (
        f"(SELECT {key_columns}{update_column}, {no_summary} FROM {temp_table} chg "
        f"UNION ALL SELECT {key_columns}{update_column}, {summary} "
        f"FROM (SELECT 1 AS one) o LEFT JOIN {temp_table} chg ON 1 = 0) chg_keys"
    )


def build_temp_table_rows_query(
    temp_table: str,
    source_table: str,
    keys: list[str],
    window_end_if_skipped: str | None = None,
) -> str:
    """SQL Server: the temp table's changed keys joined to the source; a missing source row means a delete.

    Without CR_UPD_DATE (@i_SkipUpdateDate = 1), the window end is the change time.
    """
    join_condition = " AND ".join(f"chg.{key} = main.{key}" for key in keys)
    key_aliases = ", ".join(f"chg.{key} AS _chg_key_{key}" for key in keys)
    update = (
        f"CAST('{_sql_server_datetime(window_end_if_skipped)}' AS DATETIME2(0))"
        if window_end_if_skipped
        else "chg.CR_UPD_DATE"
    )
    return (
        f"(SELECT {update} AS _csa_update_dt, {key_aliases}, main.* "
        f"FROM {temp_table} chg LEFT JOIN {source_table} main ON {join_condition}) chg_source"
    )


def build_key_range_query(source_table: str, key: str, low: int, high: int) -> str:
    """Source rows whose key is in [low, high]; Spark splits it into num_partitions ranges."""
    return f"(SELECT * FROM {source_table} WHERE {key} >= {int(low)} AND {key} <= {int(high)}) src_range"


# --- Postgres (the Lakebase mock): results in a CDC_ table, like Epic's Oracle version ------------------


def cdc_table_name(procedure: str, table_name: str) -> str:
    """Where the mock leaves its results: CDC_<table> in the procedure's schema (epic_util.CDC_orders)."""
    _require_valid_procedure(procedure)
    schema = procedure.rsplit(".", 1)[0] + "." if "." in procedure else ""
    return f"{schema}CDC_{table_name}"


def report_schema(source_table: str) -> str:
    """i_ReportDB: the schema that contains the reporting table (CLARITY.PAT_ENC -> CLARITY, dbo.PAT_ENC -> dbo)."""
    parts = source_table.split(".")
    return parts[-2] if len(parts) >= 2 else ""


def build_postgres_procedure_run(
    procedure: str,
    table_name: str,
    report_db: str,
    start: str,
    threshold_minutes: int,
    include_line_num: bool,
) -> str:
    """<procedure>_run calls the mock and returns the window end.

    The Postgres JDBC driver returns no rows for "CALL ...; SELECT ...", so the mock wraps the call
    in a function that a SELECT can run.
    """
    _require_valid_procedure(procedure)
    return (
        f"(SELECT {procedure}_run('{quote_sql_string(table_name)}', '{quote_sql_string(report_db)}', "
        f"TIMESTAMP '{quote_sql_string(start)}', {int(threshold_minutes)}, {int(include_line_num)}) "
        f"AS window_end) procedure_run"
    )


def build_cdc_stats_query(cdc_table: str, first_key: str, start: str, end: str, dialect: SqlDialect) -> str:
    """Count and key range of the changed rows in [start, end)."""
    return (
        f"(SELECT COUNT(*) AS changed, MIN({first_key}) AS low, MAX({first_key}) AS high "
        f"FROM {cdc_table} WHERE {_cdc_window(start, end, dialect, 'CR_UPD_DATE')}) cdc_stats"
    )


def build_cdc_rows_query(
    cdc_table: str,
    source_table: str,
    keys: list[str],
    start: str,
    end: str,
    dialect: SqlDialect,
) -> str:
    """The CDC_ table's changed keys joined to the source; a key with no source row is a delete.

    The window filter keeps the read correct even if the CDC_ table holds rows from other calls.
    """
    join_condition = " AND ".join(f"cdc.{key} = main.{key}" for key in keys)
    key_aliases = ", ".join(f"cdc.{key} AS _chg_key_{key}" for key in keys)
    return (
        f"(SELECT cdc.CR_UPD_DATE AS _csa_update_dt, {key_aliases}, main.* "
        f"FROM {cdc_table} cdc LEFT JOIN {source_table} main ON {join_condition} "
        f"WHERE {_cdc_window(start, end, dialect, 'cdc.CR_UPD_DATE')}) chg_source"
    )


def _cdc_window(start: str, end: str, dialect: SqlDialect, column: str) -> str:
    return (
        f"{column} >= {sql_timestamp_literal(start, dialect)} "
        f"AND {column} < {sql_timestamp_literal(end, dialect)}"
    )


class EpicStoredProcedureIngestionWorker(EpicCsaIngestionWorker):
    """Reads EPIC changes through Epic's EPIC_UTIL.ESP_CR_ALTERED_ROWS, as Epic documents it.

    On SQL Server the procedure fills a temp table named by the caller; the changed keys come back
    over that one connection and the source rows are read in num_partitions key ranges. The
    watermark is the end of the last [start, end) window.
    """

    @property
    def procedure(self) -> str:
        return self.config.epic_change_procedure or DEFAULT_EPIC_CHANGE_PROCEDURE

    @property
    def include_line_num(self) -> bool:
        return len(self.config.key_column_list) > 1

    @property
    def table_name(self) -> str:
        return self.config.source_table_name.split(".")[-1]

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

    def read_changes(self, start: str) -> tuple[object | None, str]:
        """(rows, window_end) for [start, window_end); rows is None when the window is empty."""
        if self.sql_dialect == "sqlserver":
            if self.config.num_partitions > 1:
                return self.read_changed_rows_in_parallel(start)
            window_end = self.get_window_end()
            if window_end <= start:
                return None, window_end
            return self.read_changed_rows_on_source(start, window_end), window_end
        _require_supported_dialect(self.sql_dialect)
        return self.read_cdc_changes(start)

    # --- SQL Server ------------------------------------------------------------------------------------

    def _sqlserver_reader(self, dbtable: str, *partitioning, prepare_query: str, session_init: str):
        """A SQL Server read that runs the procedure once (session_init) and resolves its columns (prepare_query)."""
        reader = self.build_jdbc_reader(dbtable, *partitioning, prepare_query=prepare_query)
        return reader.option("sessionInitStatement", session_init)

    def read_changed_keys(self, start: str):
        """Changed keys, their change time, and the window end, count and key range, from one procedure run."""
        keys = self.config.key_column_list
        skip = self.config.epic_change_skip_update_date
        temp_table = changed_rows_temp_table(uuid.uuid4().hex[:12])
        return (
            self._sqlserver_reader(
                build_changed_keys_query(temp_table, keys, skip),
                prepare_query=build_temp_table_shape(temp_table, self.config.source_table_name, keys, skip),
                session_init=build_procedure_call_until_now(
                    self.procedure, self.table_name, start,
                    self.config.watermark_threshold_minutes, temp_table, self.include_line_num,
                    keys[0], skip, self.config.epic_change_column_names,
                ),
            ).load()
        )

    def read_changed_rows_in_parallel(self, start: str) -> tuple[object | None, str]:
        """Changed keys over one connection, then the source rows in num_partitions key ranges, joined in Spark.

        When the first key column isn't an integer or the changed keys are too sparse, the rows are
        joined on the source over one connection instead.
        """
        _ctx = {
            "ingestion_group": self.config.ingestion_group,
            "source_table": self.config.source_table_name,
        }
        keys = self.config.key_column_list
        first_key = keys[0]
        fixed = self.fix_changed_keys(self.read_changed_keys(start))
        # One row with NULL keys carries the window end, count and key range computed on SQL Server.
        summary = fixed.where(F.col("_ws_window_end").isNotNull()).select(
            "_ws_window_end", "_ws_changed", "_ws_low", "_ws_high"
        ).first()
        window_end, changed = summary["_ws_window_end"], summary["_ws_changed"]
        low, high = summary["_ws_low"], summary["_ws_high"]
        if window_end <= start:
            return None, window_end
        changes = fixed.where(F.col(first_key).isNotNull()).drop("_ws_window_end", "_ws_changed", "_ws_low", "_ws_high")
        if self.config.epic_change_skip_update_date:
            # Without CR_UPD_DATE, the window end orders this window's changes after the previous one's.
            changes = changes.withColumn("_csa_update_dt", F.to_timestamp(F.lit(window_end)))

        if changed == 0:
            source = self.build_jdbc_reader(
                f"(SELECT * FROM {self.config.source_table_name} WHERE 1 = 0) src_range"
            ).load()
            return self._join_changes(changes, source, keys), window_end

        if not is_integral_type(changes.schema[first_key].dataType):
            logger.info(
                "[READ-ESP] %s — key %s is not an integer, joining on the source",
                self.config.source_table_name,
                first_key,
                extra=_ctx,
            )
            return self.read_changed_rows_on_source(start, window_end), window_end

        bounds = parallel_read_bounds(changed, low, high, self.config.num_partitions)
        if bounds is None:
            logger.info(
                "[READ-ESP] %s — %d changed keys spread over %s..%s, joining on the source",
                self.config.source_table_name,
                changed,
                low,
                high,
                extra=_ctx,
            )
            return self.read_changed_rows_on_source(start, window_end), window_end

        low, high = bounds
        logger.info(
            "[READ-ESP] %s — %d changed keys, reading %s in %s..%s with %d partitions",
            self.config.source_table_name,
            changed,
            first_key,
            low,
            high,
            self.config.num_partitions,
            extra=_ctx,
        )
        source = self.build_jdbc_reader(
            build_key_range_query(self.config.source_table_name, first_key, low, high),
            first_key,
            low,
            high,
        ).load()
        return self._join_changes(changes, source, keys), window_end

    def fix_changed_keys(self, changed_keys):
        """A fixed copy of the changed keys, used for both the read range and the join.

        Re-running the procedure for the join could return keys outside the read range,
        which would then be staged as deletes; it would also stream the keys a second time.
        localCheckpoint keeps them on the executors for this run, without a table; if those
        blocks are lost the run fails rather than reading the procedure again. The keys arrive in
        one partition (one connection), so they are spread first; one task storing them all is slow.
        """
        return changed_keys.repartition(max(self.config.num_partitions, 1)).localCheckpoint(eager=True)

    @staticmethod
    def _join_changes(changes, source, keys: list[str]):
        """Same columns as read_changed_rows_on_source; a key with no source row is a delete."""
        key_names = {key.lower() for key in keys}
        joined = changes.alias("chg").join(
            source.alias("main"),
            on=[F.col(f"chg.{key}") == F.col(f"main.{key}") for key in keys],
            how="left",
        )
        columns = [F.col("chg._csa_update_dt").alias("_csa_update_dt")]
        for column in source.columns:
            if column.lower() in key_names:
                columns.append(F.coalesce(F.col(f"main.{column}"), F.col(f"chg.{column}")).alias(column))
            else:
                columns.append(F.col(f"main.{column}").alias(column))
        columns.append(F.col(f"main.{keys[0]}").isNull().alias("_IS_DELETED"))
        return joined.select(*columns)

    def read_changed_rows_on_source(self, start: str, end: str):
        """One connection runs the procedure once and joins its temp table to the source."""
        keys = self.config.key_column_list
        skip = self.config.epic_change_skip_update_date
        temp_table = changed_rows_temp_table(uuid.uuid4().hex[:12])
        # Single partition: each partition would run the procedure again on its own connection.
        df = self._sqlserver_reader(
            build_temp_table_rows_query(temp_table, self.config.source_table_name, keys, end if skip else None),
            prepare_query=build_temp_table_shape(temp_table, self.config.source_table_name, keys, skip),
            session_init=build_procedure_call(
                self.procedure, self.table_name, start, end, temp_table,
                self.include_line_num, skip, self.config.epic_change_column_names,
            ),
        ).load()
        return self._restore_keys(df, keys)

    # --- Postgres (the Lakebase mock) -------------------------------------------------------------------

    @property
    def cdc_table(self) -> str:
        return cdc_table_name(self.procedure, self.table_name)

    def read_cdc_changes(self, start: str) -> tuple[object | None, str]:
        """Run the mock once, then read its CDC_ table joined to the source in num_partitions key ranges."""
        keys = self.config.key_column_list
        window_end = self.build_jdbc_reader(
            build_postgres_procedure_run(
                self.procedure, self.table_name, report_schema(self.config.source_table_name),
                start, self.config.watermark_threshold_minutes, self.include_line_num,
            )
        ).load().first()["window_end"]
        if window_end <= start:
            return None, window_end
        stats = self.build_jdbc_reader(
            build_cdc_stats_query(self.cdc_table, keys[0], start, window_end, self.sql_dialect)
        ).load().first()
        query = build_cdc_rows_query(
            self.cdc_table, self.config.source_table_name, keys, start, window_end, self.sql_dialect
        )
        low, high = stats["low"], stats["high"]
        if self.config.num_partitions > 1 and stats["changed"] > 0 and is_integral_key(low) and low != high:
            reader = self.build_jdbc_reader(query, f"_chg_key_{keys[0]}", int(low), int(high))
        else:
            reader = self.build_jdbc_reader(query)
        return self._restore_keys(reader.load(), keys), window_end

    @staticmethod
    def _restore_keys(df, keys: list[str]):
        """A key with no source row is a delete; its key columns come from the procedure's output."""
        df = df.withColumn("_IS_DELETED", F.col(keys[0]).isNull())
        for key in keys:
            df = df.withColumn(key, F.coalesce(F.col(key), F.col(f"_chg_key_{key}")))
        return df.drop(*[f"_chg_key_{key}" for key in keys])

    def read_source(self) -> ReadResult:
        _ctx = {
            "ingestion_group": self.config.ingestion_group,
            "source_table": self.config.source_table_name,
        }
        last_watermark = self.get_last_watermark()

        if self.effective_full_refresh or last_watermark is None:
            # Capture the window end before the read, so changes made during the full
            # load are picked up again by the next incremental window.
            window_end = self.get_window_end()
            logger.info(
                "[READ-ESP] %s — %s, performing full source read  window_end=%s",
                self.config.source_table_name,
                "full_refresh requested" if self.effective_full_refresh else "first run",
                window_end,
                extra=_ctx,
            )
            return ReadResult(df=self.read_full_source_jdbc(), persisted_watermark=window_end)

        df, window_end = self.read_changes(last_watermark)
        if df is None:
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
            self.procedure,
            last_watermark,
            window_end,
            extra=_ctx,
        )
        return ReadResult(df=df, persisted_watermark=window_end)
