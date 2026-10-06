from __future__ import annotations

from pyspark import pipelines as dp
from pyspark.sql import SparkSession

from watersync.cdc_pipeline import build_pipeline_from_spark_conf

spark = SparkSession.getActiveSession() or SparkSession.builder.getOrCreate()
build_pipeline_from_spark_conf(spark=spark, dp_module=dp)
