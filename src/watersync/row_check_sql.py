"""Source-side SQL for the chunked row check: chunk ids, aggregates and chunk predicates per database."""

from __future__ import annotations

import calendar
import re
from dataclasses import dataclass
from datetime import datetime, timedelta
from decimal import Decimal, InvalidOperation
from typing import Literal

from watersync.common import quote_sql_string
from watersync.sql_dialect import SqlDialect, sql_timestamp_literal

# MySQL is not a SqlDialect because ingestion needs nothing MySQL-specific; chunk hashing does.
CheckDialect = Literal["postgres", "sqlserver", "oracle", "mysql", "unknown"]
ChunkKind = Literal["numeric", "month", "none"]

TARGET_CHUNK_COUNT = 100


def check_dialect(sql_dialect: SqlDialect, jdbc_url: str = "") -> CheckDialect:
    if sql_dialect == "unknown" and "mysql" in jdbc_url.lower():
        return "mysql"
    return sql_dialect


def quote_identifier(name: str, dialect: CheckDialect) -> str:
    """Quotes a column name exactly as the source reported it, so mixed-case and reserved names work."""
    if dialect == "sqlserver":
        return "[" + name.replace("]", "]]") + "]"
    if dialect == "mysql":
        return "`" + name.replace("`", "``") + "`"
    if dialect in ("postgres", "oracle"):
        return '"' + name.replace('"', '""') + '"'
    return name


def decimal_sum_sql(expr: str, scale: int) -> str:
    # DECIMAL(38) keeps SUM from overflowing BIGINT (SQL Server) and makes every engine return the same type.
    return f"SUM(CAST({expr} AS DECIMAL(38, {scale})))"


def string_hash_sql(column: str, dialect: CheckDialect) -> str | None:
    """First 32 bits of the MD5 of a string column's UTF-8 bytes as an integer, 0 for NULL.

    Summing it per chunk gives an order-independent fingerprint that Spark reproduces on the target
    with ``conv(substring(md5(col), 1, 8), 16, 10)``. Returns None when the database has no MD5.
    """
    if dialect == "postgres":
        hashed = f"('x' || substr(md5({column}::text), 1, 8))::bit(32)::bigint"
    elif dialect == "sqlserver":
        # HASHBYTES hashes the stored bytes; convert to UTF-8 first (SQL Server 2019+) to match Spark.
        utf8 = f"CONVERT(VARCHAR(MAX), CAST({column} AS NVARCHAR(MAX)) COLLATE Latin1_General_100_BIN2_UTF8)"
        hashed = f"CAST(SUBSTRING(HASHBYTES('MD5', {utf8}), 1, 4) AS BIGINT)"
    elif dialect == "oracle":
        hashed = f"TO_NUMBER(SUBSTR(RAWTOHEX(STANDARD_HASH(TO_CHAR({column}), 'MD5')), 1, 8), 'XXXXXXXX')"
    elif dialect == "mysql":
        hashed = f"CAST(CONV(SUBSTRING(MD5({column}), 1, 8), 16, 10) AS UNSIGNED)"
    else:
        return None
    return f"COALESCE({hashed}, 0)"


# Booleans and dates are compared as exact integers, so the source and Spark always agree on them. Timestamps
# (time zones) and floats (order-dependent sums) are left out of the chunk aggregates.
DATE_OFFSET_DAYS = 1_000_000


def boolean_code_sql(column: str, dialect: CheckDialect) -> str | None:
    """2 for true, 1 for false, 0 for NULL, so summing it per chunk catches any single flip.

    Spark computes the same with ``when(col, 2).when(~col, 1).otherwise(0)``. Returns None where the source
    has no boolean type that Spark reads as one (Oracle before 23ai).
    """
    if dialect == "postgres":
        return f"CASE WHEN {column} THEN 2 WHEN NOT {column} THEN 1 ELSE 0 END"
    if dialect in ("sqlserver", "mysql"):
        # BIT and TINYINT(1) columns.
        return f"CASE WHEN {column} = 1 THEN 2 WHEN {column} = 0 THEN 1 ELSE 0 END"
    return None


def date_code_sql(column: str, dialect: CheckDialect) -> str | None:
    """Days since 1970-01-01 plus DATE_OFFSET_DAYS, 0 for NULL; the offset keeps old dates apart from NULL.

    Spark computes the same with ``coalesce(datediff(col, '1970-01-01') + DATE_OFFSET_DAYS, 0)``.
    """
    if dialect == "postgres":
        days = f"({column} - DATE '1970-01-01')"
    elif dialect == "sqlserver":
        days = f"DATEDIFF(DAY, '1970-01-01', {column})"
    elif dialect == "mysql":
        days = f"DATEDIFF({column}, '1970-01-01')"
    elif dialect == "oracle":
        days = f"(TRUNC({column}) - DATE '1970-01-01')"
    else:
        return None
    return f"COALESCE({days} + {DATE_OFFSET_DAYS}, 0)"


def month_chunk_sql(column: str, dialect: CheckDialect) -> str:
    if dialect == "sqlserver":
        return f"(YEAR({column}) * 100 + MONTH({column}))"
    return f"(EXTRACT(YEAR FROM {column}) * 100 + EXTRACT(MONTH FROM {column}))"


def timestamp_literal(value: datetime, dialect: CheckDialect) -> str:
    text = value.strftime("%Y-%m-%d %H:%M:%S")
    if dialect in ("postgres", "sqlserver", "oracle"):
        return sql_timestamp_literal(text, dialect)
    # MySQL compares DATETIME columns with plain string literals.
    return f"'{text}'"


@dataclass(frozen=True)
class ChunkPlan:
    """How a table is split into chunks: ranges of an integer column, calendar months, or one chunk."""

    kind: ChunkKind
    column: str = ""
    minimum: int = 0
    width: int = 1

    @classmethod
    def numeric(cls, column: str, minimum: int, maximum: int, chunk_count: int = TARGET_CHUNK_COUNT) -> ChunkPlan:
        span = maximum - minimum + 1
        return cls("numeric", column, minimum, max(1, -(-span // chunk_count)))

    def source_sql(self, dialect: CheckDialect) -> str:
        column = quote_identifier(self.column, dialect)
        if self.kind == "numeric":
            # Decimal division, so rows below the planned minimum still floor like Spark does.
            return f"FLOOR((CAST({column} AS DECIMAL(38, 0)) - {self.minimum}) / {self.width})"
        if self.kind == "month":
            return month_chunk_sql(column, dialect)
        return "0"

    def spark_sql(self) -> str:
        column = "`" + self.column.replace("`", "``") + "`"
        if self.kind == "numeric":
            return f"FLOOR((CAST({column} AS DECIMAL(38, 0)) - {self.minimum}) / {self.width})"
        if self.kind == "month":
            return f"(YEAR({column}) * 100 + MONTH({column}))"
        return "0"

    def label(self, chunk_id: int | None) -> str:
        if self.kind == "none":
            return "all rows"
        if chunk_id is None:
            return f"{self.column} IS NULL"
        if self.kind == "numeric":
            low = self.minimum + chunk_id * self.width
            return f"{self.column} {low}–{low + self.width - 1}"
        return f"{self.column} {chunk_id // 100:04d}-{chunk_id % 100:02d}"

    def source_predicate(self, chunk_ids: list[int | None], dialect: CheckDialect) -> str:
        """WHERE clause that reads at least the rows of the given chunks from the source."""
        if self.kind == "none":
            return "1=1"
        column = quote_identifier(self.column, dialect)
        parts = [f"{column} IS NULL" for chunk_id in chunk_ids if chunk_id is None]
        for chunk_id in sorted(c for c in chunk_ids if c is not None):
            if self.kind == "numeric":
                low = self.minimum + chunk_id * self.width
                parts.append(f"({column} >= {low} AND {column} < {low + self.width})")
            else:
                year, month = divmod(chunk_id, 100)
                start = datetime(year, month, 1)
                end = start + timedelta(days=calendar.monthrange(year, month)[1])
                # A day of margin covers time zone differences between the source and Spark; the
                # rows are then narrowed to the chunk in Spark, the same way on both sides.
                parts.append(
                    f"({column} >= {timestamp_literal(start - timedelta(days=1), dialect)} "
                    f"AND {column} < {timestamp_literal(end + timedelta(days=1), dialect)})"
                )
        return " OR ".join(parts) or "1=0"


def chunk_aggregate_query(
    table: str,
    chunk_sql: str,
    sums: list[tuple[str, str, int]],
    codes: list[tuple[str, str]],
    where: str = "1=1",
) -> str:
    """One row per chunk: ``ws_chunk_id``, ``row_count``, then an alias per sum (alias, column, scale) and per
    integer code (alias, expression): string hashes, boolean codes and date codes."""
    aggregates = ["COUNT(*) AS row_count"]
    aggregates += [f"{decimal_sum_sql(column, scale)} AS {alias}" for alias, column, scale in sums]
    aggregates += [f"{decimal_sum_sql(expr, 0)} AS {alias}" for alias, expr in codes]
    # The derived table lets GROUP BY use the alias, which SQL Server and Oracle do not allow directly. The table
    # keeps its name, unaliased, so a WHERE clause can refer to it (CsaChanges.source_sql).
    return (
        f"(SELECT ws_chunk_id, {', '.join(aggregates)} FROM ("
        f"SELECT {chunk_sql} AS ws_chunk_id, {table}.* FROM {table} WHERE {where}"
        f") chunked GROUP BY ws_chunk_id) chunk_aggregates"
    )


RangeKind = Literal["number", "timestamp", "string"]
_RELATIVE_DAYS = re.compile(r"^-(\d{1,5})d$")


@dataclass(frozen=True)
class RowRange:
    """Rows with ``start <= column < end``; either bound may be missing. Values are typed, never raw SQL."""

    column: str
    kind: RangeKind
    start: Decimal | datetime | str | None = None
    end: Decimal | datetime | str | None = None

    @classmethod
    def parse(cls, column: str, kind: RangeKind, start: str, end: str, now: datetime | None = None) -> RowRange:
        return cls(column, kind, parse_range_bound(start, kind, now), parse_range_bound(end, kind, now))

    def source_sql(self, dialect: CheckDialect) -> str:
        quoted = quote_identifier(self.column, dialect)
        parts = []
        if self.start is not None:
            parts.append(f"{quoted} >= {range_literal(self.start, dialect)}")
        if self.end is not None:
            parts.append(f"{quoted} < {range_literal(self.end, dialect)}")
        return " AND ".join(parts) or "1=1"

    def describe(self) -> str:
        parts = []
        if self.start is not None:
            parts.append(f">= {_display(self.start)}")
        if self.end is not None:
            parts.append(f"< {_display(self.end)}")
        return f"{self.column} {' and '.join(parts)}"


def parse_range_bound(value: str, kind: RangeKind, now: datetime | None = None) -> Decimal | datetime | str | None:
    """Parses a user-supplied bound for a column of the given kind; '' means no bound."""
    text = (value or "").strip()
    if not text:
        return None
    if kind == "number":
        try:
            number = Decimal(text)
        except InvalidOperation:
            raise ValueError(f"Range bound '{text}' is not a number") from None
        if not number.is_finite():
            raise ValueError(f"Range bound '{text}' is not a number")
        return number
    if kind == "timestamp":
        relative = _RELATIVE_DAYS.match(text)
        if relative:
            # Relative bounds start at midnight, so every table of a run gets the same whole days.
            today = (now or datetime.now()).replace(hour=0, minute=0, second=0, microsecond=0)
            return today - timedelta(days=int(relative.group(1)))
        try:
            return datetime.fromisoformat(text)
        except ValueError:
            raise ValueError(
                f"Range bound '{text}' is not a date or timestamp; use YYYY-MM-DD, 'YYYY-MM-DD HH:MM:SS' or -<days>d"
            ) from None
    return text


def is_time_bound(value: str) -> bool:
    """Whether a bound reads as a time (a date, a timestamp or -<days>d) rather than a number."""
    text = (value or "").strip()
    if not text:
        return False
    try:
        return not Decimal(text).is_finite()
    except InvalidOperation:
        return True


@dataclass(frozen=True)
class CsaChanges:
    """
    Rows of an EPIC CSA table whose key the CSA change table lists as changed with ``start <= _UPDATE_DT < end``.

    The change table keeps changes only for its retention period, so a start older than that checks fewer days.
    """

    table: str
    csa_table: str
    keys: tuple[str, ...]
    start: datetime | None = None
    end: datetime | None = None

    @classmethod
    def parse(
        cls, table: str, csa_table: str, keys: list[str], start: str, end: str, now: datetime | None = None
    ) -> CsaChanges:
        if not keys:
            raise ValueError(f"EPIC CSA table {table} needs key_columns to check the rows changed in a range")
        bounds = [parse_range_bound(bound, "timestamp", now) for bound in (start, end)]
        return cls(table, csa_table, tuple(keys), *bounds)  # type: ignore[arg-type]

    def _bounds_sql(self, dialect: CheckDialect) -> list[str]:
        parts = []
        if self.start is not None:
            parts.append(f"csa._UPDATE_DT >= {timestamp_literal(self.start, dialect)}")
        if self.end is not None:
            parts.append(f"csa._UPDATE_DT < {timestamp_literal(self.end, dialect)}")
        return parts

    def source_sql(self, dialect: CheckDialect) -> str:
        """WHERE clause on the source table: its key has a change in the range."""
        joins = [f"csa.{key} = {self.table}.{key}" for key in self.keys]
        return f"EXISTS (SELECT 1 FROM {self.csa_table} csa WHERE {' AND '.join(joins + self._bounds_sql(dialect))})"

    def keys_query(self, dialect: CheckDialect) -> str:
        """The changed keys, to pick the same rows of the target."""
        columns = ", ".join(f"csa.{key} AS {key}" for key in self.keys)
        where = " AND ".join(self._bounds_sql(dialect)) or "1=1"
        return f"(SELECT DISTINCT {columns} FROM {self.csa_table} csa WHERE {where}) csa_changes"

    def oldest_query(self) -> str:
        return f"(SELECT MIN(csa._UPDATE_DT) AS oldest FROM {self.csa_table} csa) csa_oldest"

    def describe(self, oldest: datetime | None) -> str:
        """What was checked; says so when the change table no longer reaches back to the start."""
        parts = []
        if self.start is not None:
            parts.append(f"since {_display(self.start)}")
        if self.end is not None:
            parts.append(f"before {_display(self.end)}")
        text = f"keys changed {' and '.join(parts)} in {self.csa_table}"
        if oldest is None:
            return f"{text}; the CSA change table is empty"
        if self.start is not None and oldest > self.start:
            return (
                f"{text}; CSA keeps changes only since {_display(oldest)}, so earlier changes were not checked: "
                "schedule detailed checks more often than the CSA retention period"
            )
        return text


def range_literal(value: Decimal | datetime | str, dialect: CheckDialect) -> str:
    if isinstance(value, Decimal):
        return str(value)
    if isinstance(value, datetime):
        return timestamp_literal(value, dialect)
    return "'" + quote_sql_string(value) + "'"


def _display(value: Decimal | datetime | str) -> str:
    return value.strftime("%Y-%m-%d %H:%M:%S") if isinstance(value, datetime) else str(value)
