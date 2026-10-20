from __future__ import annotations

import json
import logging
import time
import uuid
from concurrent.futures import ThreadPoolExecutor
from dataclasses import replace
from datetime import datetime
from decimal import Decimal
from typing import Any

from pyspark.sql import Window
from pyspark.sql import functions as F
from pyspark.sql.types import (
    BooleanType,
    ByteType,
    DateType,
    DecimalType,
    IntegerType,
    LongType,
    ShortType,
    StringType,
    StructField,
    TimestampNTZType,
    TimestampType,
)

from watersync.common import epic_csa_table_name
from watersync.ingestion import JdbcIngestionConfigRepository
from watersync.models import IngestionConfig, JdbcRuntimeSettings
from watersync.row_check_sql import (
    DATE_OFFSET_DAYS,
    CheckDialect,
    ChunkPlan,
    CsaChanges,
    RowRange,
    boolean_code_sql,
    check_dialect,
    chunk_aggregate_query,
    date_code_sql,
    is_time_bound,
    quote_identifier,
    string_hash_sql,
)
from watersync.workers import TimestampWatermarkIngestionWorker

logger = logging.getLogger(__name__)

CHECK_MODES = ("basic", "detailed")
SAMPLE_KEY_LIMIT = 10
# Tier 2 aggregates at most this many numeric and this many string columns.
MAX_AGGREGATED_COLUMNS = 64
# Tier 3 compares the rows of at most this many mismatched chunks, the largest count differences first.
MAX_DRILLED_CHUNKS = 10
# Mismatched chunks kept in chunk_details.
MAX_REPORTED_CHUNKS = 50

# Columns the framework adds to staging or target tables; they have no source counterpart.
EXCLUDED_COLUMNS = {
    "__start_at",
    "__end_at",
    "_ingested_at",
    "_source_table",
    "_ingestion_group",
    "_ingestion_type",
    "_is_deleted",
    "_csa_update_dt",
}
INTEGRAL_TYPES = (ByteType, ShortType, IntegerType, LongType)

RESULT_SCHEMA = (
    "check_id STRING, job_run_id STRING, check_mode STRING, checked_at TIMESTAMP, "
    "ingestion_group STRING, source_table_name STRING, target_table_fqn STRING, "
    "source_count BIGINT, target_count BIGINT, count_diff BIGINT, "
    "missing_in_target BIGINT, extra_in_target BIGINT, mismatched_rows BIGINT, "
    "compared_columns STRING, sample_keys STRING, status STRING, error STRING, duration_ms BIGINT, "
    "chunk_column STRING, chunk_count BIGINT, mismatched_chunks BIGINT, drilled_chunks BIGINT, chunk_details STRING, "
    "checked_range STRING"
)


def target_has_scd2_history(config: IngestionConfig) -> bool:
    """Incremental and snapshot-CDC sources land in an SCD2 table; other full loads overwrite the target."""
    return config.ingestion_type == "incremental" or config.auto_cdc_from_snapshot


def is_integral(field: StructField) -> bool:
    data_type = field.dataType
    return isinstance(data_type, INTEGRAL_TYPES) or (isinstance(data_type, DecimalType) and data_type.scale == 0)


def sum_scale(field: StructField) -> int | None:
    """Scale to SUM a column at, or None when it is not summed. Floats are left out: their sums depend on order."""
    if isinstance(field.dataType, INTEGRAL_TYPES):
        return 0
    if isinstance(field.dataType, DecimalType):
        return field.dataType.scale
    return None


def range_kind(field: StructField) -> str:
    if isinstance(field.dataType, (*INTEGRAL_TYPES, DecimalType)):
        return "number"
    if isinstance(field.dataType, (TimestampType, TimestampNTZType, DateType)):
        return "timestamp"
    if isinstance(field.dataType, StringType):
        return "string"
    raise ValueError(f"Cannot filter a range of {field.name} ({field.dataType.simpleString()})")


def normalize(value: Any) -> Decimal | None:
    """Makes aggregates from the source driver and from Spark comparable (Decimal('1.50') == Decimal('1.5'))."""
    return None if value is None else Decimal(str(value))


def chunk_key(value: Any) -> int | None:
    return None if value is None else int(Decimal(str(value)))


def column(name: str):
    # Backticks keep names with dots, spaces or other special characters intact.
    return F.col("`" + name.replace("`", "``") + "`")


def row_hash(columns: list[str]):
    # concat_ws skips NULLs, so map them to a sentinel to tell NULL apart from an empty string.
    return F.md5(
        F.concat_ws("\u0001", *[F.coalesce(column(c).cast("string"), F.lit("\u0000")) for c in columns])
    )


def spark_string_hash(name: str):
    """Spark side of row_check_sql.string_hash_sql."""
    return F.coalesce(F.conv(F.substring(F.md5(column(name)), 1, 8), 16, 10).cast("decimal(38,0)"), F.lit(0))


def spark_boolean_code(name: str):
    """Spark side of row_check_sql.boolean_code_sql: 2 for true, 1 for false, 0 for NULL."""
    return F.when(column(name), F.lit(2)).when(~column(name), F.lit(1)).otherwise(F.lit(0))


def spark_date_code(name: str):
    """Spark side of row_check_sql.date_code_sql: days since 1970-01-01 plus DATE_OFFSET_DAYS, 0 for NULL."""
    return F.coalesce(F.datediff(column(name), F.lit("1970-01-01").cast("date")) + F.lit(DATE_OFFSET_DAYS), F.lit(0))


def chunk_field(config: IngestionConfig, fields: dict[str, StructField]) -> StructField | None:
    """The integer partition column, else a single integer key, else a timestamp or date watermark column."""
    candidates = [config.partition_column] + (config.key_column_list if len(config.key_column_list) == 1 else [])
    for name in candidates:
        field = fields.get(name.lower()) if name else None
        if field is not None and is_integral(field):
            return field
    watermark = fields.get(config.watermark_column.lower()) if config.watermark_column else None
    if watermark is not None and isinstance(watermark.dataType, (TimestampType, TimestampNTZType, DateType)):
        return watermark
    return None


def result_status(result: dict[str, Any]) -> str:
    # Mismatched chunks that tier 3 did not reach stay unexplained; ones it cleared do not count.
    undrilled = (result["mismatched_chunks"] or 0) - (result["drilled_chunks"] or 0)
    differences = [
        result["count_diff"],
        result["missing_in_target"],
        result["extra_in_target"],
        result["mismatched_rows"],
        undrilled,
    ]
    return "MISMATCH" if any(differences) else "MATCH"


class RowCheckRunner:
    """Compares each enabled source table with its target table and appends one result row per table.

    ``basic`` is tier 1: a COUNT(*) pushed down to the source and a count of the target's active rows.
    ``detailed`` runs tiers 2 and 3. Tier 2 splits the table into chunks (ranges of an integer
    column, or months of the watermark column) and compares per-chunk counts, numeric sums, and sums of
    integer codes for strings (hashes), booleans and dates, computed by the source database and by Spark
    on the target. Timestamps and floats are not aggregated. Tier 3 reads only
    the chunks that differ and compares them row by row, by key when the config has key columns.
    """

    def __init__(
        self,
        spark: Any,
        runtime: JdbcRuntimeSettings,
        results_fqn: str,
        check_mode: str = "basic",
        job_run_id: str = "",
        max_parallel_tables: int = 4,
        range_column: str = "",
        range_from: str = "",
        range_to: str = "",
    ):
        if check_mode not in CHECK_MODES:
            raise ValueError(f"Unsupported check_mode '{check_mode}'. Expected one of {list(CHECK_MODES)}")
        self.spark = spark
        self.runtime = runtime
        self.results_fqn = results_fqn
        self.check_mode = check_mode
        self.job_run_id = job_run_id
        self.max_parallel_tables = max(1, max_parallel_tables)
        # Checks only rows with range_from <= column < range_to; without a column, see resolve_range and select_rows.
        self.range_column = range_column.strip()
        self.range_from = range_from.strip()
        self.range_to = range_to.strip()
        self.range_now = datetime.now()
        self.repository = JdbcIngestionConfigRepository(spark=spark, runtime=runtime)

    @staticmethod
    def create_results_table(spark: Any, results_fqn: str) -> None:
        spark.sql(f"""
            CREATE TABLE IF NOT EXISTS IDENTIFIER('{results_fqn}') (
              check_id STRING COMMENT 'Unique id of this result row',
              job_run_id STRING COMMENT 'Databricks job run that produced the result',
              check_mode STRING COMMENT 'basic (row counts) or detailed (chunk aggregates, then rows of differing chunks)',
              checked_at TIMESTAMP COMMENT 'When the table was checked',
              ingestion_group STRING COMMENT 'Matches ingestion_group in config',
              source_table_name STRING COMMENT 'Matches source_table_name in config',
              target_table_fqn STRING COMMENT 'Target table that was compared',
              source_count BIGINT COMMENT 'Rows in the source table',
              target_count BIGINT COMMENT 'Active rows in the target table (__END_AT IS NULL for SCD2 targets)',
              count_diff BIGINT COMMENT 'target_count - source_count',
              missing_in_target BIGINT COMMENT 'Detailed only: source rows with no matching target row, in the compared chunks',
              extra_in_target BIGINT COMMENT 'Detailed only: target rows with no matching source row, in the compared chunks',
              mismatched_rows BIGINT COMMENT 'Detailed only: keys on both sides with different values, in the compared chunks',
              compared_columns STRING COMMENT 'Detailed only: comma-separated columns compared row by row',
              sample_keys STRING COMMENT 'Detailed only: JSON with up to 10 keys per difference type',
              status STRING COMMENT 'MATCH, MISMATCH or ERROR',
              error STRING COMMENT 'Failure message when status is ERROR',
              duration_ms BIGINT COMMENT 'Time spent checking the table',
              chunk_column STRING COMMENT 'Detailed only: column the table was chunked by; NULL for a single chunk',
              chunk_count BIGINT COMMENT 'Detailed only: chunks compared by aggregates',
              mismatched_chunks BIGINT COMMENT 'Detailed only: chunks whose aggregates differ',
              drilled_chunks BIGINT COMMENT 'Detailed only: mismatched chunks compared row by row',
              chunk_details STRING COMMENT 'Detailed only: JSON with the aggregated columns and the mismatched chunks',
              checked_range STRING COMMENT 'Range of rows checked, e.g. modified_at >= 2026-09-01 00:00:00; NULL for the whole table'
            )
            USING DELTA
            CLUSTER BY AUTO
            COMMENT 'Row checks between WaterSync source tables and their targets'
        """)

    def run(self) -> list[dict[str, Any]]:
        configs = self.repository.load_selected_configs(
            ingestion_group=self.runtime.ingestion_group,
            source_table_name=self.runtime.source_table_name,
        )
        logger.info(
            "[CHECK]  Starting %s row check  group=%s  tables=%d",
            self.check_mode,
            self.runtime.ingestion_group or "(all)",
            len(configs),
        )
        self.create_results_table(self.spark, self.results_fqn)
        with ThreadPoolExecutor(max_workers=self.max_parallel_tables) as pool:
            results = list(pool.map(self.check_table, configs))
        if results:
            # mergeSchema adds the chunk columns to a results table created by an earlier version.
            self.spark.createDataFrame(results, schema=RESULT_SCHEMA).write.format("delta").mode("append").option(
                "mergeSchema", "true"
            ).saveAsTable(self.results_fqn)

        counts = {status: sum(1 for r in results if r["status"] == status) for status in ("MATCH", "MISMATCH", "ERROR")}
        logger.info(
            "[CHECK]  Run complete  total=%d  match=%d  mismatch=%d  error=%d",
            len(results),
            counts["MATCH"],
            counts["MISMATCH"],
            counts["ERROR"],
        )
        if counts["ERROR"]:
            raise RuntimeError(f"{counts['ERROR']} table(s) could not be checked")
        return results

    def check_table(self, config: IngestionConfig) -> dict[str, Any]:
        started = time.monotonic()
        result: dict[str, Any] = {
            "check_id": str(uuid.uuid4()),
            "job_run_id": self.job_run_id or None,
            "check_mode": self.check_mode,
            "checked_at": datetime.now(),
            "ingestion_group": config.ingestion_group,
            "source_table_name": config.source_table_name,
            "target_table_fqn": config.target_table_fqn,
            "source_count": None,
            "target_count": None,
            "count_diff": None,
            "missing_in_target": None,
            "extra_in_target": None,
            "mismatched_rows": None,
            "compared_columns": None,
            "sample_keys": None,
            "status": "ERROR",
            "error": None,
            "duration_ms": None,
            "chunk_column": None,
            "chunk_count": None,
            "mismatched_chunks": None,
            "drilled_chunks": None,
            "chunk_details": None,
            "checked_range": None,
        }
        _ctx = {"ingestion_group": config.ingestion_group, "source_table": config.source_table_name}
        try:
            # The full-load variant of the config reads the whole source with the table's JDBC partitioning.
            worker = TimestampWatermarkIngestionWorker(self.spark, self.runtime, replace(config, ingestion_type="full"))
            target_df = self.active_target_rows(config)
            if self.check_mode == "basic":
                result.update(self.compare_counts(config, worker, target_df))
            else:
                result.update(self.compare_tiered(config, worker, target_df))
            result["count_diff"] = result["target_count"] - result["source_count"]
            result["status"] = result_status(result)
            logger.info(
                "[CHECK]  %s — %s  source=%s  target=%s  mismatched_chunks=%s",
                config.source_table_name,
                result["status"],
                result["source_count"],
                result["target_count"],
                result["mismatched_chunks"],
                extra=_ctx,
            )
        except Exception as exc:
            result["error"] = str(exc)[:4000]
            logger.error(
                "[CHECK]  %s — ERROR: %s", config.source_table_name, str(exc)[:500], exc_info=True, extra=_ctx
            )
        result["duration_ms"] = int((time.monotonic() - started) * 1000)
        return result

    def active_target_rows(self, config: IngestionConfig):
        df = self.spark.table(config.target_table_fqn)
        if target_has_scd2_history(config) and "__END_AT" in df.columns:
            df = df.filter(F.col("__END_AT").isNull())
        return df

    # ── Tier 1: row counts ──────────────────────────────────────────

    def compare_counts(
        self, config: IngestionConfig, worker: TimestampWatermarkIngestionWorker, target_df
    ) -> dict[str, Any]:
        result: dict[str, Any] = {}
        where = "1=1"
        if self.has_range:
            dialect = check_dialect(worker.sql_dialect, config.jdbc_url)
            where, target_df, result["checked_range"] = self.select_rows(
                config, worker, dialect, self.source_fields(worker), target_df
            )
        count_query = f"(SELECT COUNT(*) AS row_count FROM {config.source_table_name} WHERE {where}) row_count"
        # Positional access: Oracle returns the alias in upper case.
        result["source_count"] = int(worker.build_jdbc_reader(count_query).load().first()[0])
        result["target_count"] = target_df.count()
        return result

    # ── Ranges ──────────────────────────────────────────────────────

    @property
    def has_range(self) -> bool:
        return bool(self.range_from or self.range_to)

    def source_fields(self, worker: TimestampWatermarkIngestionWorker) -> dict[str, StructField]:
        """Source columns by lower-case name. Resolving the schema only runs Spark's WHERE 1=0 probe, not a read."""
        schema = worker.build_jdbc_reader(f"(SELECT * FROM {worker.config.source_table_name}) source_schema").load().schema
        return {f.name.lower(): f for f in schema.fields if f.name.lower() not in EXCLUDED_COLUMNS}

    @property
    def range_is_time(self) -> bool:
        """Whether the set bounds are dates, timestamps or days back (-30d), not numbers."""
        bounds = [bound for bound in (self.range_from, self.range_to) if bound]
        return bool(bounds) and all(is_time_bound(bound) for bound in bounds)

    def resolve_range(self, config: IngestionConfig, fields: dict[str, StructField]) -> RowRange | None:
        """
        The range to filter by. Without a range column, a time range goes on the timestamp watermark column, or
        the chunk column when that is a timestamp; None means the table has no such column, so every row is checked.
        """
        if self.range_column:
            field = fields.get(self.range_column.lower())
            if field is None:
                raise ValueError(f"Range column '{self.range_column}' is not a column of the source table")
        elif self.range_is_time:
            watermark = fields.get(config.watermark_column.lower()) if config.watermark_column else None
            chunk = chunk_field(config, fields)
            field = next(
                (
                    candidate
                    for candidate in (watermark, chunk)
                    if candidate is not None and range_kind(candidate) == "timestamp"
                ),
                None,
            )
            if field is None:
                return None
        else:
            field = chunk_field(config, fields)
            if field is None:
                raise ValueError(
                    "The table has no chunk column (integer partition column or key, or timestamp watermark column) "
                    "to filter a range by; set the range column"
                )
        return RowRange.parse(field.name, range_kind(field), self.range_from, self.range_to, self.range_now)

    def select_rows(
        self,
        config: IngestionConfig,
        worker: TimestampWatermarkIngestionWorker,
        dialect: CheckDialect,
        fields: dict[str, StructField],
        target_df,
    ) -> tuple[str, Any, str]:
        """
        The rows a ranged check compares: the source WHERE clause, the target rows, and what was checked.
        A time range on an EPIC CSA table checks the keys its CSA change table lists as changed in the range.
        """
        if config.epic_csa_enabled and not self.range_column and self.range_is_time:
            changes = CsaChanges.parse(
                config.source_table_name,
                epic_csa_table_name(config.source_table_name),
                config.key_column_list,
                self.range_from,
                self.range_to,
                self.range_now,
            )
            target_keys = []
            for key in config.key_column_list:
                name = next((c for c in target_df.columns if c.lower() == key.lower()), None)
                if name is None:
                    raise ValueError(f"Key column '{key}' is not a column of the target table")
                target_keys.append(name)
            changed = worker.build_jdbc_reader(changes.keys_query(dialect)).load().toDF(*target_keys)
            target = target_df.join(changed, on=target_keys, how="left_semi")
            oldest = worker.build_jdbc_reader(changes.oldest_query()).load().first()[0]
            return changes.source_sql(dialect), target, changes.describe(oldest)
        row_range = self.resolve_range(config, fields)
        if row_range is None:
            return "1=1", target_df, "every row: the table has no timestamp column to filter the range by"
        return (
            row_range.source_sql(dialect),
            target_df.filter(self.range_filter(row_range, target_df.columns)),
            row_range.describe(),
        )

    @staticmethod
    def range_filter(row_range: RowRange, columns: list[str]):
        name = next((c for c in columns if c.lower() == row_range.column.lower()), None)
        if name is None:
            raise ValueError(f"Range column '{row_range.column}' is not a column of the target table")
        condition = F.lit(True)
        if row_range.start is not None:
            condition = condition & (column(name) >= F.lit(row_range.start))
        if row_range.end is not None:
            condition = condition & (column(name) < F.lit(row_range.end))
        return condition

    # ── Tiers 2 and 3: chunk aggregates, then rows of differing chunks ──

    def compare_tiered(
        self, config: IngestionConfig, worker: TimestampWatermarkIngestionWorker, target_df
    ) -> dict[str, Any]:
        table = config.source_table_name
        dialect = check_dialect(worker.sql_dialect, config.jdbc_url)
        # Source databases may return upper-case names (Oracle), so match columns case-insensitively and
        # use the source's spelling on both sides.
        source_fields = self.source_fields(worker)
        target_columns = {c.lower(): c for c in target_df.columns if c.lower() not in EXCLUDED_COLUMNS}
        fields = [field for name, field in source_fields.items() if name in target_columns]
        if not fields:
            raise ValueError("Source and target tables have no columns in common")
        target = target_df.select([column(target_columns[f.name.lower()]).alias(f.name) for f in fields])
        by_name = {f.name.lower(): f for f in fields}
        missing_keys = [key for key in config.key_column_list if key.lower() not in by_name]
        if missing_keys:
            raise ValueError(f"Key column(s) {missing_keys} are not present in both source and target")
        keys = [by_name[key.lower()].name for key in config.key_column_list]

        where = "1=1"
        checked_range = None
        if self.has_range:
            where, target, checked_range = self.select_rows(config, worker, dialect, by_name, target)

        plan = self.plan_chunks(config, worker, dialect, by_name, where)
        chunks, details = self.compare_chunks(worker, dialect, plan, fields, target, where)
        mismatched = [chunk for chunk in chunks if chunk["differs"]]
        drilled = sorted(mismatched, key=lambda c: -abs(c["target_rows"] - c["source_rows"]))[:MAX_DRILLED_CHUNKS]
        result: dict[str, Any] = {
            "source_count": sum(chunk["source_rows"] for chunk in chunks),
            "target_count": sum(chunk["target_rows"] for chunk in chunks),
            "chunk_column": plan.column or None,
            "chunk_count": len(chunks),
            "mismatched_chunks": len(mismatched),
            "drilled_chunks": len(drilled),
            "compared_columns": ",".join(f.name for f in fields),
            "checked_range": checked_range,
            "missing_in_target": 0,
            "extra_in_target": 0,
            # Without keys a changed row shows up as one missing and one extra row.
            "mismatched_rows": 0 if keys or not drilled else None,
        }
        if drilled:
            chunk_ids = [chunk["chunk"] for chunk in drilled]
            if plan.kind == "none" and where == "1=1":
                source_rows = worker.read_source_jdbc_standard(None)
            else:
                predicate = f"({plan.source_predicate(chunk_ids, dialect)}) AND {where}"
                source_rows = worker.build_jdbc_reader(f"(SELECT * FROM {table} WHERE {predicate}) drill").load()
            source = source_rows.select([column(f.name) for f in fields])
            per_chunk, samples = self.compare_rows(plan, chunk_ids, source, target, [f.name for f in fields], keys)
            for chunk in drilled:
                found = per_chunk.get(chunk["chunk"], {})
                chunk["drilled"] = True
                chunk["missing_in_target"] = found.get("missing_in_target", 0)
                chunk["extra_in_target"] = found.get("extra_in_target", 0)
                if keys:
                    chunk["mismatched"] = found.get("mismatched", 0)
            result["missing_in_target"] = sum(chunk["missing_in_target"] for chunk in drilled)
            result["extra_in_target"] = sum(chunk["extra_in_target"] for chunk in drilled)
            if keys:
                result["mismatched_rows"] = sum(chunk["mismatched"] for chunk in drilled)
            result["sample_keys"] = json.dumps(samples) if samples else None
        details["chunks"] = [{**chunk, "label": plan.label(chunk["chunk"])} for chunk in mismatched[:MAX_REPORTED_CHUNKS]]
        result["chunk_details"] = json.dumps(details)
        return result

    def plan_chunks(
        self,
        config: IngestionConfig,
        worker: TimestampWatermarkIngestionWorker,
        dialect: CheckDialect,
        fields: dict[str, StructField],
        where: str = "1=1",
    ) -> ChunkPlan:
        """Ranges of the chunk column within the checked rows, months of it, or a single chunk."""
        field = chunk_field(config, fields)
        if field is None:
            return ChunkPlan("none")
        if not is_integral(field):
            return ChunkPlan("month", field.name)
        quoted = quote_identifier(field.name, dialect)
        bounds = (
            worker.build_jdbc_reader(
                f"(SELECT MIN({quoted}) AS low, MAX({quoted}) AS high FROM {config.source_table_name} WHERE {where}) bounds"
            )
            .load()
            .first()
        )
        if bounds is None or bounds[0] is None:
            return ChunkPlan("none")
        return ChunkPlan.numeric(field.name, int(bounds[0]), int(bounds[1]))

    def compare_chunks(
        self,
        worker: TimestampWatermarkIngestionWorker,
        dialect: CheckDialect,
        plan: ChunkPlan,
        fields: list[StructField],
        target,
        where: str = "1=1",
    ) -> tuple[list[dict[str, Any]], dict[str, Any]]:
        """Tier 2: per-chunk COUNT, numeric SUMs, and sums of integer codes for strings (hashes), booleans and
        dates, from the source database and from Spark."""
        summed = [(f, sum_scale(f)) for f in fields if sum_scale(f) is not None][:MAX_AGGREGATED_COLUMNS]
        # (label, field, source SQL, Spark expression); each kind is skipped where the source cannot compute it.
        kinds = [
            ("hash", StringType, string_hash_sql, spark_string_hash),
            ("bool", BooleanType, boolean_code_sql, spark_boolean_code),
            ("date", DateType, date_code_sql, spark_date_code),
        ]
        codes = []
        for label, data_type, source_sql, spark_expr in kinds:
            if source_sql("c", dialect) is None:
                continue
            matching = [f for f in fields if isinstance(f.dataType, data_type)][:MAX_AGGREGATED_COLUMNS]
            codes += [(label, f, source_sql, spark_expr) for f in matching]
        details: dict[str, Any] = {
            "chunking": plan.kind,
            "summed_columns": [f.name for f, _ in summed],
            "hashed_columns": [f.name for label, f, _, _ in codes if label == "hash"],
            "boolean_columns": [f.name for label, f, _, _ in codes if label == "bool"],
            "date_columns": [f.name for label, f, _, _ in codes if label == "date"],
        }

        def source_query(code_columns) -> str:
            return chunk_aggregate_query(
                worker.config.source_table_name,
                plan.source_sql(dialect),
                [(f"s{i}", quote_identifier(f.name, dialect), scale) for i, (f, scale) in enumerate(summed)],
                [
                    (f"c{i}", source_sql(quote_identifier(f.name, dialect), dialect))
                    for i, (_, f, source_sql, _) in enumerate(code_columns)
                ],
                where,
            )

        try:
            source_rows = worker.build_jdbc_reader(source_query(codes)).load().collect()
        except Exception as exc:
            if not codes:
                raise
            # Some sources cannot hash every string column (Oracle LOBs, SQL Server before 2019).
            logger.warning(
                "[CHECK]  %s — hashing or encoding columns failed, retrying with counts and sums only: %s",
                worker.config.source_table_name,
                str(exc)[:300],
            )
            details["note"] = (
                "The source could not hash text columns or encode boolean and date columns, so only counts and "
                f"sums were compared: {str(exc)[:300]}"
            )
            details.update(hashed_columns=[], boolean_columns=[], date_columns=[])
            codes = []
            source_rows = worker.build_jdbc_reader(source_query([])).load().collect()

        metrics = ["row_count"] + [f"sum({f.name})" for f, _ in summed] + [f"{label}({f.name})" for label, f, _, _ in codes]
        target_rows = (
            target.groupBy(F.expr(plan.spark_sql()).alias("ws_chunk_id"))
            .agg(
                F.count(F.lit(1)),
                *[F.sum(column(f.name).cast(f"decimal(38,{scale})")) for f, scale in summed],
                *[F.sum(spark_expr(f.name)).cast("decimal(38,0)") for _, f, _, spark_expr in codes],
            )
            .collect()
        )
        source_by_chunk = {chunk_key(row[0]): [normalize(v) for v in row[1:]] for row in source_rows}
        target_by_chunk = {chunk_key(row[0]): [normalize(v) for v in row[1:]] for row in target_rows}

        chunks = []
        # A chunk on one side only: zero rows and no sums on the other.
        empty = [Decimal(0)] + [None] * (len(metrics) - 1)
        for chunk_id in sorted(set(source_by_chunk) | set(target_by_chunk), key=lambda c: (c is None, c or 0)):
            source_values = source_by_chunk.get(chunk_id, empty)
            target_values = target_by_chunk.get(chunk_id, empty)
            chunks.append(
                {
                    "chunk": chunk_id,
                    "source_rows": int(source_values[0]),
                    "target_rows": int(target_values[0]),
                    "differs": [m for m, s, t in zip(metrics, source_values, target_values) if s != t],
                    "drilled": False,
                }
            )
        return chunks, details

    def compare_rows(
        self,
        plan: ChunkPlan,
        chunk_ids: list[int | None],
        source,
        target,
        columns: list[str],
        keys: list[str],
    ) -> tuple[dict[int | None, dict[str, int]], dict[str, list[dict[str, str]]]]:
        """Tier 3: compares the rows of the given chunks; returns per-chunk difference counts and sample keys.

        Both sides are narrowed to the chunks with the same Spark expression, so the extra rows a
        source predicate reads (see ChunkPlan.source_predicate) do not show up as differences.
        """
        in_chunks = F.col("ws_chunk_id").isin([c for c in chunk_ids if c is not None])
        if None in chunk_ids:
            in_chunks = in_chunks | F.col("ws_chunk_id").isNull()
        source = source.withColumn("ws_chunk_id", F.expr(plan.spark_sql())).filter(in_chunks)
        target = target.withColumn("ws_chunk_id", F.expr(plan.spark_sql())).filter(in_chunks)

        per_chunk: dict[int | None, dict[str, int]] = {}
        if not keys:
            # Without keys only whole-row differences can be found: compare the two multisets of row hashes.
            source_hashes = source.groupBy("ws_chunk_id", row_hash(columns).alias("row_hash")).agg(
                F.count("*").alias("source_n")
            )
            target_hashes = target.groupBy("ws_chunk_id", row_hash(columns).alias("row_hash")).agg(
                F.count("*").alias("target_n")
            )
            summary = (
                source_hashes.join(target_hashes, on=["ws_chunk_id", "row_hash"], how="full_outer")
                .fillna(0, subset=["source_n", "target_n"])
                .groupBy("ws_chunk_id")
                .agg(
                    F.sum(F.greatest(F.col("source_n") - F.col("target_n"), F.lit(0))).alias("missing_in_target"),
                    F.sum(F.greatest(F.col("target_n") - F.col("source_n"), F.lit(0))).alias("extra_in_target"),
                )
                .collect()
            )
            for row in summary:
                per_chunk[chunk_key(row["ws_chunk_id"])] = {
                    "missing_in_target": int(row["missing_in_target"] or 0),
                    "extra_in_target": int(row["extra_in_target"] or 0),
                }
            return per_chunk, {}

        value_columns = [name for name in columns if name not in keys]
        hashed_value = row_hash(value_columns) if value_columns else F.lit("")
        source_side = source.select(*[column(key) for key in keys], "ws_chunk_id", hashed_value.alias("source_hash"))
        target_side = target.select(*[column(key) for key in keys], "ws_chunk_id", hashed_value.alias("target_hash"))
        joined = (
            source_side.alias("s")
            .join(
                target_side.alias("t"),
                on=[source_side[key].eqNullSafe(target_side[key]) for key in keys],
                how="full_outer",
            )
            .select(
                *[F.coalesce(source_side[key], target_side[key]).alias(key) for key in keys],
                # A row whose chunk column changed counts in its source chunk.
                F.coalesce(source_side["ws_chunk_id"], target_side["ws_chunk_id"]).alias("ws_chunk_id"),
                F.when(target_side["target_hash"].isNull(), F.lit("missing_in_target"))
                .when(source_side["source_hash"].isNull(), F.lit("extra_in_target"))
                .when(source_side["source_hash"] != target_side["target_hash"], F.lit("mismatched"))
                .alias("difference"),
            )
            .filter(F.col("difference").isNotNull())
        )
        for row in joined.groupBy("ws_chunk_id", "difference").count().collect():
            per_chunk.setdefault(chunk_key(row["ws_chunk_id"]), {})[row["difference"]] = int(row["count"])

        samples: dict[str, list[dict[str, str]]] = {}
        if per_chunk:
            # Serverless compute cannot cache, so the sample is a second pass, made only when there is a difference.
            ranked = joined.withColumn(
                "rank", F.row_number().over(Window.partitionBy("difference").orderBy(*[column(key) for key in keys]))
            )
            for row in ranked.filter(F.col("rank") <= SAMPLE_KEY_LIMIT).collect():
                samples.setdefault(row["difference"], []).append({key: str(row[key]) for key in keys})
        return per_chunk, samples
