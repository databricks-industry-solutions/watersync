from __future__ import annotations

from typing import Any


def normalize_text(value: str | None) -> str:
    return (value or "").strip()


def quote_sql_string(value: str) -> str:
    return value.replace("'", "''")


def normalize_ingestion_type(value: str | None) -> str:
    normalized = normalize_text(value or "incremental").lower() or "incremental"
    allowed = {"full", "incremental"}
    if normalized not in allowed:
        raise ValueError(f"Unsupported ingestion_type '{value}'. Expected one of {sorted(allowed)}")
    return normalized


def default_staging_table_fqn(source_table_name: str, catalog: str, schema: str) -> str:
    short_name = f"staging_{normalize_text(source_table_name).split('.')[-1]}"
    return f"{catalog}.{schema}.{short_name}"


def resolve_staging_table_fqn(
    raw_staging_table_fqn: str | None,
    source_table_name: str,
    catalog: str,
    schema: str,
) -> str:
    configured = normalize_text(raw_staging_table_fqn)
    return configured or default_staging_table_fqn(source_table_name, catalog, schema)


def validate_table_fqn(value: str, field_name: str = "target_table_fqn") -> str:
    normalized = normalize_text(value)
    parts = normalized.split(".")
    if len(parts) != 3 or any(not part.strip() for part in parts):
        raise ValueError(f"{field_name} must use catalog.schema.table format; got '{value}'")
    return normalized


def row_to_dict(row: Any) -> dict[str, Any]:
    if hasattr(row, "asDict"):
        return row.asDict(recursive=True)
    return dict(row)
