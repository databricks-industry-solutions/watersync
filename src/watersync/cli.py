from __future__ import annotations

import argparse
import json
from pathlib import Path

from typing import Any

from watersync.models import JdbcRuntimeSettings, JobProvisioningSettings

# Each command imports what it needs when it runs: the ones that only call the Databricks API, such as
# watersync-create-job, then work outside Databricks without PySpark or Delta Lake installed.


def active_spark() -> Any:
    from pyspark.sql import SparkSession

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

    from watersync.config_planner import IngestionConfigPlanner

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

    from watersync.ingestion import JdbcIngestionOrchestrator

    spark = active_spark()
    runtime = build_runtime_settings(args)
    orchestrator = JdbcIngestionOrchestrator(spark=spark, runtime=runtime)
    print(json.dumps(orchestrator.run_selected_ingestion(), default=str))


def row_check_main(**kwargs) -> None:
    if kwargs:
        args = _namespace_or_kwargs(kwargs)
    else:
        parser = argparse.ArgumentParser(description="Compare source and target tables (basic: counts, detailed: rows)")
        parser.add_argument("--configuration-fqn", dest="configuration_fqn", required=True)
        parser.add_argument("--watermark-fqn", dest="watermark_fqn", required=True)
        parser.add_argument("--row-check-fqn", dest="row_check_fqn", required=True)
        parser.add_argument("--ingestion-group", dest="ingestion_group", default="")
        parser.add_argument("--source-table-name", default="")
        parser.add_argument("--check-mode", dest="check_mode", choices=["basic", "detailed"], default="basic")
        parser.add_argument("--max-parallel-tables", dest="max_parallel_tables", default=4, type=int)
        parser.add_argument("--range-column", dest="range_column", default="")
        parser.add_argument("--range-from", dest="range_from", default="")
        parser.add_argument("--range-to", dest="range_to", default="")
        args = parser.parse_args()

    from watersync.row_check import RowCheckRunner

    runner = RowCheckRunner(
        spark=active_spark(),
        runtime=build_runtime_settings(args),
        results_fqn=args.row_check_fqn,
        check_mode=getattr(args, "check_mode", "basic"),
        max_parallel_tables=int(getattr(args, "max_parallel_tables", 4)),
        range_column=getattr(args, "range_column", ""),
        range_from=getattr(args, "range_from", ""),
        range_to=getattr(args, "range_to", ""),
    )
    print(json.dumps(runner.run(), default=str))


def create_job_main(**kwargs) -> None:
    if kwargs:
        args = _namespace_or_kwargs(kwargs)
    else:
        parser = argparse.ArgumentParser(description="Create or update the ingestion job and CDC pipeline")
        parser.add_argument("--configuration-fqn", dest="configuration_fqn", required=True)
        parser.add_argument("--watermark-fqn", dest="watermark_fqn", required=True)
        parser.add_argument("--ingestion-group", dest="ingestion_group", required=True)
        parser.add_argument(
            "--wheel-uri",
            required=True,
            help="Wheel in a volume or the workspace, or any pip requirement (e.g. watersync==0.4.0)",
        )
        parser.add_argument("--planner-notebook-path", required=True)
        parser.add_argument("--worker-notebook-path", required=True)
        parser.add_argument("--cdc-pipeline-id", default="")
        parser.add_argument("--cdc-pipeline-file-path", default=str(Path.cwd() / "src" / "watersync" / "pipeline_bootstrap.py"))
        parser.add_argument("--foreach-concurrency", default=4, type=int)
        parser.add_argument("--git-url", default="", help="Repository the notebooks run from; empty for workspace paths")
        parser.add_argument("--git-branch", default="main")
        parser.add_argument("--git-tag", default="", help="Pin the notebooks to a tag instead of the branch")
        parser.add_argument("--git-commit", default="", help="Pin the notebooks to a commit instead of the branch")
        parser.add_argument(
            "--git-provider", default="", help="For a self-hosted server, e.g. gitHubEnterprise; detected otherwise"
        )
        args = parser.parse_args()

    from watersync.utils.create_ingestion_job import IngestionJobProvisioner

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
        git_url=getattr(args, "git_url", ""),
        git_branch=getattr(args, "git_branch", "main"),
        git_tag=getattr(args, "git_tag", ""),
        git_commit=getattr(args, "git_commit", ""),
        git_provider=getattr(args, "git_provider", ""),
    )
    print(json.dumps(provisioner.create_or_update_job(settings), default=str))


def import_config_main(argv: list[str] | None = None) -> int:
    """Imports a WaterSync configuration file without the Control Plane app.

    Validates the file, writes the config rows through a SQL warehouse, and (with --deploy-jobs) creates or
    updates the jobs, CDC pipelines and row check jobs. With --bundle-root, every job runs its notebooks from that
    bundle deployment's files and installs the wheel it built, whatever the file says.
    """
    from watersync.config_import import ImportValidationError, bundle_wheel, import_configuration, use_bundle, validate_file

    parser = argparse.ArgumentParser(prog="watersync-import", description=import_config_main.__doc__.splitlines()[0])
    parser.add_argument("--file", required=True, type=Path, help="Export file (format watersync-config)")
    parser.add_argument("--catalog", required=True)
    parser.add_argument("--schema", required=True)
    parser.add_argument("--warehouse-id", help="SQL warehouse that writes the config rows (not needed with --dry-run)")
    parser.add_argument("--profile", help="Databricks CLI profile (default: unified auth from the environment)")
    parser.add_argument(
        "--bundle-root",
        help="A bundle deployment's root, e.g. /Workspace/Users/me/.bundle/watersync/dev: jobs run its files and wheel",
    )
    parser.add_argument("--replace-groups", action="store_true", help="Delete rows of the file's groups it doesn't list")
    parser.add_argument("--deploy-jobs", action="store_true", help="Create or update the jobs in the file")
    parser.add_argument("--dry-run", action="store_true", help="Only validate the file")
    args = parser.parse_args(argv)

    try:
        validated = validate_file(json.loads(args.file.read_text()))
    except ImportValidationError as error:
        print(f"{args.file} is not valid:\n{error}")
        return 1
    groups = sorted({config["ingestionGroup"] for config in validated.configs})
    summary = (
        f"{len(validated.configs)} source tables in {len(groups)} groups, {len(validated.jobs)} jobs, "
        f"{len(validated.row_check_jobs)} row check jobs"
    )
    if args.dry_run and not args.bundle_root:
        print(f"Valid: {summary}")
        return 0

    from databricks.sdk import WorkspaceClient

    workspace = WorkspaceClient(profile=args.profile) if args.profile else WorkspaceClient()
    if args.bundle_root:
        wheel = bundle_wheel(workspace, args.bundle_root)
        use_bundle(validated, f"{args.bundle_root.rstrip('/')}/files", wheel)
        print(f"Jobs use the bundle at {args.bundle_root}: notebooks from files/, {wheel.rsplit('/', 1)[-1]}")
    if args.dry_run:
        print(f"Valid: {summary}")
        return 0
    if not args.warehouse_id:
        parser.error("--warehouse-id is required unless --dry-run")
    succeeded = import_configuration(
        workspace,
        args.warehouse_id,
        args.catalog,
        args.schema,
        validated,
        replace_groups=args.replace_groups,
        deploy_jobs=args.deploy_jobs,
    )
    return 0 if succeeded else 1


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

    from watersync.utils.lakebase_test_database_setup import LakebaseTestDatabaseSetup

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

    from watersync.utils.uc_setup import UnityCatalogSetup

    setup = UnityCatalogSetup(active_spark(), catalog=args.catalog, schema=args.schema)
    setup.create_all(truncate_existing=getattr(args, "truncate_existing", False))
    print(json.dumps({"schema": f"{args.catalog}.{args.schema}", "config_table": setup.config_table, "state_table": setup.state_table}))
