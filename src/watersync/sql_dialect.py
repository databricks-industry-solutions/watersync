from __future__ import annotations

from typing import Literal

from watersync.common import quote_sql_string

SqlDialect = Literal["postgres", "sqlserver", "oracle", "unknown"]


def dialect_from_connection_type(connection_type: object) -> SqlDialect:
    """Map a Unity Catalog ConnectionType enum or string to a SQL dialect."""
    value = getattr(connection_type, "value", connection_type)
    normalized = str(value or "").upper()
    return {
        "POSTGRESQL": "postgres",
        "SQLSERVER": "sqlserver",
        "ORACLE": "oracle",
    }.get(normalized, "unknown")


def detect_sql_dialect(jdbc_url: str, extra: str = "") -> SqlDialect:
    blob = f"{jdbc_url} {extra}".lower()
    if any(token in blob for token in ("sqlserver", "jtds", "microsoft.sqlserver")):
        return "sqlserver"
    if "oracle" in blob:
        return "oracle"
    if "postgres" in blob:
        return "postgres"
    return "unknown"


def sql_timestamp_literal(value: str, dialect: SqlDialect) -> str:
    quoted = quote_sql_string(value)
    if dialect == "sqlserver":
        return f"CAST('{quoted}' AS DATETIME2)"
    if dialect == "oracle":
        return f"TO_TIMESTAMP('{quoted}', 'YYYY-MM-DD HH24:MI:SS')"
    return f"CAST('{quoted}' AS TIMESTAMP)"


def sql_bigint_cast(expr: str, dialect: SqlDialect) -> str:
    if dialect == "oracle":
        return f"CAST({expr} AS NUMBER(19))"
    return f"CAST({expr} AS BIGINT)"


def watermark_window_predicate(
    column: str,
    last_watermark: str,
    cutoff: str | None,
    dialect: SqlDialect,
) -> str:
    predicates = [f"{column} > {sql_timestamp_literal(last_watermark, dialect)}"]
    if cutoff:
        predicates.append(f"{column} <= {sql_timestamp_literal(cutoff, dialect)}")
    return " AND ".join(predicates)


def sql_exists_subquery(table: str, where_predicate: str, dialect: SqlDialect) -> str:
    """Single-row JDBC subquery: 1 if any matching row exists."""
    if dialect == "sqlserver":
        inner = (
            f"SELECT TOP 1 1 AS has_rows FROM {table} WHERE {where_predicate}"
        )
    elif dialect == "oracle":
        inner = (
            f"SELECT 1 AS has_rows FROM {table} "
            f"WHERE {where_predicate} AND ROWNUM = 1"
        )
    else:
        inner = (
            f"SELECT 1 AS has_rows FROM {table} WHERE {where_predicate} LIMIT 1"
        )
    return f"({inner}) change_check"
