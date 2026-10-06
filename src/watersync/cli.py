from __future__ import annotations

import argparse
import json
from pathlib import Path

from pyspark.sql import SparkSession

from watersync.config_planner import IngestionConfigPlanner
from watersync.ingestion import JdbcIngestionOrchestrator
from watersync.models import JdbcRuntimeSettings, JobProvisioningSettings
from watersync.utils import IngestionJobProvisioner, LakebaseTestDatabaseSetup, UnityCatalogSetup


def active_spark() -> SparkSession:
    return SparkSession.getActiveSession() or SparkSession.builder.getOrCreate()


def _namespace_or_kwargs(kwargs: dict) -> argparse.Namespace:
    return argparse.Namespace(**kwargs)


def build_runtime_settings(args: argparse.Namespace) -> JdbcRuntimeSettings:
    return JdbcRuntimeSettings(
        configuration_fqn=args.configuration_fqn,
        watermark_fqn=args.watermark_fqn,
        ingestion_group=args.ingestion_group,
        source_table_name=getattr(args, "source_table_name", ""),
    )


def plan_configs_main(**kwargs) -> None:
    if kwargs:
        args = _namespace_or_kwargs(kwargs)
    else:
        parser = argparse.ArgumentParser(description="Build table fanout inputs from jdbc_ingestion_config")
        parser.add_argument("--configuration-fqn", dest="configuration_fqn", required=True)
        parser.add_argument("--watermark-fqn", dest="watermark_fqn", required=True)
        parser.add_argument("--ingestion-group", dest="ingestion_group", required=True)
        parser.add_argument("--publish-task-value", default="false")
        args = parser.parse_args()

    spark = active_spark()
    planner = IngestionConfigPlanner(spark=spark, runtime=build_runtime_settings(args))
    payload = planner.build_for_each_inputs_json(args.ingestion_group)
    if str(getattr(args, "publish_task_value", "false")).lower() in ("true", "1", "yes"):
        try:
            from pyspark.dbutils import DBUtils

            DBUtils(spark).jobs.taskValues.set(key="table_configs", value=payload)
        except Exception:
            pass
    print(payload)


def run_ingestion_main(**kwargs) -> None:
    if kwargs:
        args = _namespace_or_kwargs(kwargs)
    else:
        parser = argparse.ArgumentParser(description="Run JDBC ingestion for one group or one table")
        parser.add_argument("--configuration-fqn", dest="configuration_fqn", required=True)
        parser.add_argument("--watermark-fqn", dest="watermark_fqn", required=True)
        parser.add_argument("--ingestion-group", dest="ingestion_group", required=True)
        parser.add_argument("--source-table-name", default="")
        args = parser.parse_args()

    spark = active_spark()
    runtime = build_runtime_settings(args)
    orchestrator = JdbcIngestionOrchestrator(spark=spark, runtime=runtime)
    print(json.dumps(orchestrator.run_selected_ingestion(), default=str))


def create_job_main(**kwargs) -> None:
    if kwargs:
        args = _namespace_or_kwargs(kwargs)
    else:
        parser = argparse.ArgumentParser(description="Create or update the ingestion job and CDC pipeline")
        parser.add_argument("--configuration-fqn", dest="configuration_fqn", required=True)
        parser.add_argument("--watermark-fqn", dest="watermark_fqn", required=True)
        parser.add_argument("--ingestion-group", dest="ingestion_group", required=True)
        parser.add_argument("--wheel-uri", required=True)
        parser.add_argument("--planner-notebook-path", required=True)
        parser.add_argument("--worker-notebook-path", required=True)
        parser.add_argument("--cdc-pipeline-id", default="")
        parser.add_argument("--cdc-pipeline-file-path", default=str(Path.cwd() / "src" / "watersync" / "pipeline_bootstrap.py"))
        parser.add_argument("--foreach-concurrency", default=4, type=int)
        args = parser.parse_args()

    provisioner = IngestionJobProvisioner()
    settings = JobProvisioningSettings(
        ingestion_group=args.ingestion_group,
        configuration_fqn=args.configuration_fqn,
        watermark_fqn=args.watermark_fqn,
        wheel_uri=args.wheel_uri,
        planner_notebook_path=args.planner_notebook_path,
        worker_notebook_path=args.worker_notebook_path,
        cdc_pipeline_id=getattr(args, "cdc_pipeline_id", ""),
        cdc_pipeline_file_path=getattr(args, "cdc_pipeline_file_path", str(Path.cwd() / "src" / "watersync" / "pipeline_bootstrap.py")),
        foreach_concurrency=int(getattr(args, "foreach_concurrency", 4)),
    )
    print(json.dumps(provisioner.create_or_update_job(settings), default=str))


def setup_lakebase_main(**kwargs) -> None:
    if kwargs:
        args = _namespace_or_kwargs(kwargs)
    else:
        parser = argparse.ArgumentParser(description="Create a Lakebase test project and seed JDBC source tables")
        parser.add_argument("--project-id", default="watersync-jdbc-test")
        parser.add_argument("--project-display-name", default="WaterSync JDBC Test DB")
        parser.add_argument("--customer-count", type=int, default=200)
        parser.add_argument("--product-count", type=int, default=100)
        parser.add_argument("--order-count", type=int, default=500)
        parser.add_argument("--simulate-updates", action="store_true")
        args = parser.parse_args()

    setup = LakebaseTestDatabaseSetup(
        project_id=getattr(args, "project_id", "watersync-jdbc-test"),
        project_display_name=getattr(args, "project_display_name", "WaterSync JDBC Test DB"),
    )
    setup.ensure_project()
    setup.create_standard_tables()
    setup.seed_standard_data(int(getattr(args, "customer_count", 200)), int(getattr(args, "product_count", 100)), int(getattr(args, "order_count", 500)))
    if getattr(args, "simulate_updates", False):
        setup.simulate_updates()
    print(json.dumps(setup.jdbc_settings(), default=str))


def setup_uc_main(**kwargs) -> None:
    if kwargs:
        args = _namespace_or_kwargs(kwargs)
    else:
        parser = argparse.ArgumentParser(description="Create the UC schema and ingestion metadata tables")
        parser.add_argument("--catalog", required=True)
        parser.add_argument("--schema", required=True)
        parser.add_argument("--truncate-existing", action="store_true")
        args = parser.parse_args()

    setup = UnityCatalogSetup(active_spark(), catalog=args.catalog, schema=args.schema)
    setup.create_all(truncate_existing=getattr(args, "truncate_existing", False))
    print(json.dumps({"schema": f"{args.catalog}.{args.schema}", "config_table": setup.config_table, "state_table": setup.state_table}))
