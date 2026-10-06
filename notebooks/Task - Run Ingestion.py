# Databricks notebook source
# DBTITLE 1,Run ingestion for one table
import json
from pyspark.sql import SparkSession
from watersync.ingestion import JdbcIngestionOrchestrator
from watersync.models import JdbcRuntimeSettings

spark = SparkSession.getActiveSession()

runtime = JdbcRuntimeSettings(
    configuration_fqn=dbutils.widgets.get("configuration_fqn"),
    watermark_fqn=dbutils.widgets.get("watermark_fqn"),
    ingestion_group=dbutils.widgets.get("ingestion_group"),
    source_table_name=dbutils.widgets.get("source_table_name"),
    full_refresh=dbutils.widgets.get("full_refresh").strip().lower() == "true",
)

orchestrator = JdbcIngestionOrchestrator(spark=spark, runtime=runtime)
result = orchestrator.run_selected_ingestion()
print(json.dumps(result, default=str))

# COMMAND ----------

