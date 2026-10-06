# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
# DBTITLE 1,Overview
# MAGIC %md
# MAGIC # Watersync Notebook Runner
# MAGIC
# MAGIC Following your preferences, this notebook lives under the project-level `notebooks` folder and lets you execute `watersync` package code directly from a Databricks workspace notebook.
# MAGIC
# MAGIC Usage:
# MAGIC * Choose an `action`
# MAGIC * Set the relevant widgets for that action
# MAGIC * Run the bootstrap cell once
# MAGIC * Run the execution cell
# MAGIC
# MAGIC Supported actions:
# MAGIC * `plan_configs`
# MAGIC * `run_ingestion`
# MAGIC * `create_job`
# MAGIC * `setup_uc`
# MAGIC * `setup_lakebase`
# MAGIC

# COMMAND ----------

# MAGIC %pip install -e ../.
# MAGIC dbutils.library.restartPython()

# COMMAND ----------

# DBTITLE 1,Bootstrap watersync imports
from pathlib import Path
import json
import sys

context = dbutils.notebook.entry_point.getDbutils().notebook().getContext()
notebook_path = context.notebookPath().get()
workspace_notebook_path = (
    Path(notebook_path)
    if notebook_path.startswith("/Workspace/")
    else Path("/Workspace") / notebook_path.lstrip("/")
)
notebook_dir = workspace_notebook_path.parent
project_root = notebook_dir.parent
src_root = project_root / "src"

if str(src_root) not in sys.path:
    sys.path.insert(0, str(src_root))

from watersync.config_planner import IngestionConfigPlanner
from watersync.ingestion import JdbcIngestionOrchestrator
from watersync.models import JdbcRuntimeSettings, JobProvisioningSettings
from watersync.utils.create_ingestion_job import IngestionJobProvisioner
from watersync.utils.lakebase_test_database_setup import LakebaseTestDatabaseSetup
from watersync.utils.uc_setup import UnityCatalogSetup

print(f"Notebook path : {notebook_path}")
print(f"Project root  : {project_root}")
print(f"Source root   : {src_root}")
print("watersync imports loaded successfully")


# COMMAND ----------

# DBTITLE 1,Parameters
dbutils.widgets.dropdown(
    "action",
    "plan_configs",
    ["plan_configs", "run_ingestion", "create_job", "setup_uc", "setup_lakebase"],
    "Watersync action",
)

dbutils.widgets.text("configuration_fqn", "main.watersync.jdbc_ingestion_config", "Configuration table FQN")
dbutils.widgets.text("watermark_fqn", "main.watersync.jdbc_ingestion_watermark", "Watermark table FQN")
dbutils.widgets.text("ingestion_group", "", "Ingestion group")
dbutils.widgets.text("source_table_name", "", "Source table name")
dbutils.widgets.text("jdbc_url", "", "JDBC URL")
dbutils.widgets.text("jdbc_user", "", "JDBC user")
dbutils.widgets.text("jdbc_password", "", "JDBC password")
dbutils.widgets.text("jdbc_secret_scope", "", "JDBC secret scope")
dbutils.widgets.text("jdbc_secret_key", "", "JDBC secret key")
dbutils.widgets.text("uc_secret_name", "", "UC secret name (catalog.schema.secret_name)")
dbutils.widgets.text("watermark_threshold_minutes", "5", "Watermark threshold minutes")
dbutils.widgets.text("fetch_size", "10000", "JDBC fetch size")
dbutils.widgets.text("num_partitions", "8", "JDBC num partitions")

dbutils.widgets.text("wheel_uri", "", "Wheel URI for create_job")
dbutils.widgets.text("cdc_pipeline_id", "", "Existing CDC pipeline id")
dbutils.widgets.text(
    "cdc_pipeline_file_path",
    str(project_root / "src" / "watersync" / "pipeline_bootstrap.py"),
    "CDC pipeline bootstrap file path",
)
dbutils.widgets.text("foreach_concurrency", "4", "ForEach concurrency")
dbutils.widgets.text("git_url", "https://github.com/databricks-industry-solutions/watersync", "Git repo URL for job tasks")
dbutils.widgets.text("git_branch", "main", "Git branch for job tasks")

dbutils.widgets.text("project_id", "jdbc-test", "Lakebase project id")
dbutils.widgets.text("project_display_name", "JDBC Test DB", "Lakebase project display name")
dbutils.widgets.text("customer_count", "200", "Lakebase customer seed count")
dbutils.widgets.text("product_count", "100", "Lakebase product seed count")
dbutils.widgets.text("order_count", "500", "Lakebase order seed count")
dbutils.widgets.dropdown("simulate_updates", "false", ["false", "true"], "Simulate Lakebase updates")
dbutils.widgets.dropdown("truncate_existing", "false", ["false", "true"], "Truncate existing UC tables")
dbutils.widgets.dropdown("full_refresh", "false", ["false", "true"], "Force full refresh (overwrite staging, bypass watermark)")


# COMMAND ----------

# DBTITLE 1,Run watersync action
def _widget(name: str) -> str:
    return dbutils.widgets.get(name).strip()


def _as_bool(name: str) -> bool:
    return _widget(name).lower() == "true"


action = _widget("action")

runtime = JdbcRuntimeSettings(
    configuration_fqn=_widget("configuration_fqn"),
    watermark_fqn=_widget("watermark_fqn"),
    ingestion_group=_widget("ingestion_group"),
    source_table_name=_widget("source_table_name"),
    full_refresh=_as_bool("full_refresh"),
)

if action == "plan_configs":
    if not runtime.ingestion_group:
        raise ValueError("ingestion_group is required for plan_configs")
    result = IngestionConfigPlanner(spark=spark, runtime=runtime).build_for_each_inputs(
        ingestion_group=runtime.ingestion_group
    )
    display(spark.createDataFrame(result))
elif action == "run_ingestion":
    result = JdbcIngestionOrchestrator(spark=spark, runtime=runtime).run_selected_ingestion()
    display(spark.createDataFrame(result))
elif action == "setup_uc":
    catalog, schema, _ = _widget("configuration_fqn").split(".")
    setup = UnityCatalogSetup(spark=spark, catalog=catalog, schema=schema)
    setup.create_all(truncate_existing=_as_bool("truncate_existing"))
    result = {
        "schema": catalog + "." + schema,
        "config_table": setup.config_table,
        "state_table": setup.state_table,
        "truncate_existing": _as_bool("truncate_existing"),
    }
    print(json.dumps(result, indent=2))
elif action == "setup_lakebase":
    setup = LakebaseTestDatabaseSetup(
        project_id=_widget("project_id"),
        project_display_name=_widget("project_display_name"),
    )
    setup.ensure_project()
    setup.create_standard_tables()
    setup.seed_standard_data(
        customer_count=int(_widget("customer_count") or "200"),
        product_count=int(_widget("product_count") or "100"),
        order_count=int(_widget("order_count") or "500"),
    )
    if _as_bool("simulate_updates"):
        setup.simulate_updates()
    result = setup.jdbc_settings()
    print(json.dumps(result, indent=2))
elif action == "create_job":
    wheel_uri = _widget("wheel_uri")
    if not wheel_uri:
        raise ValueError("wheel_uri is required for create_job")
    if not runtime.ingestion_group:
        raise ValueError("ingestion_group is required for create_job")

    git_url = _widget("git_url") or "https://github.com/databricks-industry-solutions/watersync"
    git_branch = _widget("git_branch") or "main"

    job_settings = JobProvisioningSettings(
        ingestion_group=runtime.ingestion_group,
        configuration_fqn=runtime.configuration_fqn,
        watermark_fqn=runtime.watermark_fqn,
        planner_notebook_path="notebooks/Task - Plan Configs",
        worker_notebook_path="notebooks/Task - Run Ingestion",
        wheel_uri=wheel_uri,
        cdc_pipeline_id=_widget("cdc_pipeline_id"),
        cdc_pipeline_file_path=_widget("cdc_pipeline_file_path"),
        foreach_concurrency=int(_widget("foreach_concurrency") or "4"),
        git_url=git_url,
        git_branch=git_branch,
    )
    result = IngestionJobProvisioner().create_or_update_job(job_settings)
    print(json.dumps(result, indent=2))
else:
    raise ValueError(f"Unsupported action: {action}")


# COMMAND ----------

# DBTITLE 1,Upsert ingestion config rows
# ── Upsert ingestion config rows ─────────────────────────────────────────────
# Edit table_configs below and run this cell to add or update rows.
# Merge key: (ingestion_group, source_table_name)

table_configs = [
    dict(
        ingestion_group   = "epic",
        source_table_name = "epic.patients",
        staging_table_fqn = None,           # None -> auto: catalog.schema.staging_<source_table>
        target_table_fqn  = "main.watersync.patients",
        ingestion_type    = "incremental",  # "incremental" | "full"
        key_columns       = "patient_id",
        watermark_column  = "updated_at",
        partition_column  = "patient_id",
        predicate_column  = None,
        epic_csa_enabled  = True,
        auto_cdc_from_snapshot = False,
        #jdbc_url          = "jdbc:postgresql://<host>:5432/<database>?sslmode=require",
        #jdbc_user         = "users",
        #jdbc_secret_scope = "watersync",
        #jdbc_secret_key   = "jdbc_pass",
        connection_name   = "clarity_conn",
        watermark_threshold_minutes = 5,
        fetch_size        = 10000,
        num_partitions    = 8,
        enabled           = True,
    ),
    dict(
        ingestion_group   = "epic",
        source_table_name = "epic.encounters",
        staging_table_fqn = None,           # None -> auto: catalog.schema.staging_<source_table>
        target_table_fqn  = "main.watersync.encounters",
        ingestion_type    = "incremental",  # "incremental" | "full"
        key_columns       = "encounter_id",
        watermark_column  = "modified_at",
        partition_column  = "encounter_id",
        predicate_column  = None,
        epic_csa_enabled  = True,
        auto_cdc_from_snapshot = False,
        #jdbc_url          = "jdbc:postgresql://<host>:5432/<database>?sslmode=require",
        #jdbc_user         = "users",
        #jdbc_secret_scope = "watersync",
        #jdbc_secret_key   = "jdbc_pass",
        connection_name   = "clarity_conn",
        watermark_threshold_minutes = 5,
        fetch_size        = 10000,
        num_partitions    = 8,
        enabled           = True,
    ),
    # Add more tables here...
]

# ─────────────────────────────────────────────────────────────────────────────

from pyspark.sql import functions as F
from pyspark.sql.types import BooleanType, IntegerType, StringType, StructField, StructType

_cfg_schema = StructType([
    StructField("ingestion_group",   StringType(),  False),
    StructField("source_table_name", StringType(),  False),
    StructField("staging_table_fqn", StringType(),  True),
    StructField("target_table_fqn",  StringType(),  True),
    StructField("ingestion_type",    StringType(),  True),
    StructField("key_columns",       StringType(),  True),
    StructField("watermark_column",  StringType(),  True),
    StructField("partition_column",  StringType(),  True),
    StructField("predicate_column",  StringType(),  True),
    StructField("epic_csa_enabled",  BooleanType(), True),
    StructField("auto_cdc_from_snapshot", BooleanType(), True),
    StructField("jdbc_url",          StringType(),  True),
    StructField("jdbc_user",         StringType(),  True),
    StructField("jdbc_secret_scope", StringType(),  True),
    StructField("jdbc_secret_key",   StringType(),  True),
    StructField("uc_secret_name",   StringType(),  True),
    StructField("connection_name",   StringType(),  True),
    StructField("watermark_threshold_minutes", IntegerType(), True),
    StructField("fetch_size",        IntegerType(), True),
    StructField("num_partitions",    IntegerType(), True),
    StructField("enabled",           BooleanType(), False),
])

_rows = [
    (
        r["ingestion_group"],
        r["source_table_name"],
        r.get("staging_table_fqn"),
        r.get("target_table_fqn"),
        r.get("ingestion_type", "incremental"),
        r.get("key_columns"),
        r.get("watermark_column"),
        r.get("partition_column"),
        r.get("predicate_column"),
        r.get("epic_csa_enabled", False),
        r.get("auto_cdc_from_snapshot", False),
        r.get("jdbc_url"),
        r.get("jdbc_user"),
        r.get("jdbc_secret_scope"),
        r.get("jdbc_secret_key"),
        r.get("uc_secret_name"),
        r.get("connection_name"),
        r.get("watermark_threshold_minutes"),
        r.get("fetch_size"),
        r.get("num_partitions"),
        r.get("enabled", True),
    )
    for r in table_configs
]

_config_table  = _widget("configuration_fqn")

(
    spark.createDataFrame(_rows, schema=_cfg_schema)
         .withColumn("update_dttm", F.current_timestamp())
         .createOrReplaceTempView("_upsert_configs")
)

spark.sql(f"""
    MERGE INTO {_config_table} AS t
    USING _upsert_configs AS s
    ON  t.ingestion_group   = s.ingestion_group
    AND t.source_table_name = s.source_table_name
    WHEN MATCHED THEN UPDATE SET *
    WHEN NOT MATCHED THEN INSERT *
""")

print(f"Upserted {len(table_configs)} row(s) into {_config_table}")
display(
    spark.table(_config_table)
         .where(F.col("ingestion_group").isin([r["ingestion_group"] for r in table_configs]))
         .orderBy("ingestion_group", "source_table_name")
)

# COMMAND ----------

# MAGIC %pip install build
# MAGIC
# MAGIC !python -m build --wheel ../.

# COMMAND ----------

# MAGIC %sh
# MAGIC cp ../dist/watersync-0.1.1-py3-none-any.whl /Volumes/<catalog>/<schema>/<volume>/watersync-0.1.1-py3-none-any.whl