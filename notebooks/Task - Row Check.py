# Databricks notebook source
# DBTITLE 1,Compare source and target row counts
import json
from pyspark.sql import SparkSession
from watersync.models import JdbcRuntimeSettings
from watersync.row_check import RowCheckRunner

spark = SparkSession.getActiveSession()


def optional_widget(name: str) -> str:
    # Range parameters were added later; a job saved before them has no such widget.
    try:
        return dbutils.widgets.get(name).strip()
    except Exception:
        return ""


runtime = JdbcRuntimeSettings(
    configuration_fqn=dbutils.widgets.get("configuration_fqn"),
    watermark_fqn=dbutils.widgets.get("watermark_fqn"),
    ingestion_group=dbutils.widgets.get("ingestion_group"),
    source_table_name=dbutils.widgets.get("source_table_name"),
)

runner = RowCheckRunner(
    spark=spark,
    runtime=runtime,
    results_fqn=dbutils.widgets.get("row_check_fqn"),
    check_mode=dbutils.widgets.get("check_mode").strip().lower() or "basic",
    job_run_id=dbutils.widgets.get("job_run_id"),
    max_parallel_tables=int(dbutils.widgets.get("max_parallel_tables") or "4"),
    range_column=optional_widget("range_column"),
    range_from=optional_widget("range_from"),
    range_to=optional_widget("range_to"),
)
result = runner.run()
print(json.dumps(result, default=str))

# COMMAND ----------
