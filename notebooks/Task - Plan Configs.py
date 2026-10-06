# Databricks notebook source
# DBTITLE 1,Plan configs and publish task value
from pyspark.sql import SparkSession
from watersync.config_planner import IngestionConfigPlanner
from watersync.models import JdbcRuntimeSettings

spark = SparkSession.getActiveSession()

runtime = JdbcRuntimeSettings(
    configuration_fqn=dbutils.widgets.get("configuration_fqn"),
    watermark_fqn=dbutils.widgets.get("watermark_fqn"),
    ingestion_group=dbutils.widgets.get("ingestion_group"),
)

planner = IngestionConfigPlanner(spark=spark, runtime=runtime)
payload = planner.build_for_each_inputs_json(runtime.ingestion_group)

# Detect tables whose watermark status is FULL_REFRESH (set from the control-plane UI)
full_refresh_targets = planner.detect_full_refresh_targets(runtime.ingestion_group)

# Publish task values for downstream tasks
dbutils.jobs.taskValues.set(key="table_configs", value=payload)
dbutils.jobs.taskValues.set(key="full_refresh_targets", value=full_refresh_targets)
print(payload)
if full_refresh_targets:
    print(f"\nFull refresh targets: {full_refresh_targets}")

# COMMAND ----------

