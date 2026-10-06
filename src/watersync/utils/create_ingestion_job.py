from __future__ import annotations

from databricks.sdk import WorkspaceClient
from databricks.sdk.service import pipelines as pl
from databricks.sdk.service.compute import AutoScale, ClusterSpec, DataSecurityMode, Environment, Library
from databricks.sdk.service.jobs import (
    ForEachTask,
    GitProvider,
    GitSource,
    JobCluster,
    JobEnvironment,
    JobParameterDefinition,
    JobSettings,
    NotebookTask,
    PipelineTask,
    Source,
    Task,
    TaskDependency,
)

from watersync.models import JobProvisioningSettings


class IngestionJobProvisioner:
    def __init__(self, workspace_client: WorkspaceClient | None = None):
        self.w = workspace_client or WorkspaceClient()

    def ensure_pipeline(self, settings: JobProvisioningSettings) -> str:
        if settings.cdc_pipeline_id:
            self._sync_pipeline_env(settings.cdc_pipeline_id, settings)
            return settings.cdc_pipeline_id
        if not settings.cdc_pipeline_file_path:
            raise ValueError("cdc_pipeline_file_path is required when cdc_pipeline_id is not provided")

        pipeline_name = f"[{settings.ingestion_group}] CDC SCD2 Pipeline"
        safe_prefix = f"[{settings.ingestion_group}] CDC SCD2".replace("'", "''")
        existing = [
            pipeline
            for pipeline in self.w.pipelines.list_pipelines(filter=f"name LIKE '{safe_prefix}%'")
            if pipeline.name == pipeline_name
        ]
        if existing:
            self._sync_pipeline_env(existing[0].pipeline_id, settings)
            return existing[0].pipeline_id

        config_catalog, config_schema, _ = settings.configuration_fqn.split(".", 2)
        created = self.w.pipelines.create(
            name=pipeline_name,
            catalog=config_catalog,
            target=config_schema,
            configuration={
                "pipeline.configuration_fqn": settings.configuration_fqn,
                "pipeline.watermark_fqn": settings.watermark_fqn,
                "pipeline.ingestion_group": settings.ingestion_group,
            },
            libraries=[pl.PipelineLibrary(file=pl.FileLibrary(path=settings.cdc_pipeline_file_path))],
            environment=pl.PipelinesEnvironment(dependencies=[settings.wheel_uri]),
            serverless=True,
            channel="CURRENT",
        )
        return created.pipeline_id

    def _sync_pipeline_env(self, pipeline_id: str, settings: JobProvisioningSettings) -> None:
        """Reconcile pipeline config so it uses the Volume-hosted wheel."""
        pipe = self.w.pipelines.get(pipeline_id=pipeline_id)
        spec = pipe.spec
        # Mirror the same create() arguments but via update() on the existing pipeline
        self.w.pipelines.update(
            pipeline_id=pipeline_id,
            name=pipe.name,
            catalog=spec.catalog,
            target=spec.target,
            configuration=spec.configuration,
            libraries=[pl.PipelineLibrary(file=pl.FileLibrary(path=settings.cdc_pipeline_file_path))],
            environment=pl.PipelinesEnvironment(dependencies=[settings.wheel_uri]),
            serverless=spec.serverless,
            channel=spec.channel,
        )

    def build_job_settings(self, settings: JobProvisioningSettings, pipeline_id: str) -> JobSettings:
        # Determine source type based on whether a Git URL is configured
        use_git = bool(settings.git_url)
        source = Source.GIT if use_git else Source.WORKSPACE

        git_source = (
            GitSource(
                git_url=settings.git_url,
                git_provider=GitProvider.GIT_HUB,
                git_branch=settings.git_branch,
            )
            if use_git
            else None
        )

        env_key = "Default"
        environments = [
            JobEnvironment(
                environment_key=env_key,
                spec=Environment(client="1", dependencies=[settings.wheel_uri]),
            )
        ]

        planner_task = Task(
            task_key="ingestion_configs",
            description="Build one For each item per enabled source table.",
            notebook_task=NotebookTask(
                notebook_path=settings.planner_notebook_path,
                source=source,
            ),
            environment_key=env_key,
        )

        worker_iteration = Task(
            task_key="ingestion_worker_iteration",
            notebook_task=NotebookTask(
                notebook_path=settings.worker_notebook_path,
                base_parameters={
                    "source_table_name": "{{input.source_table_name}}",
                    "full_refresh": "{{job.parameters.full_refresh}}",
                },
                source=source,
            ),
            environment_key=env_key,
        )

        ingestion_worker = Task(
            task_key="ingestion_worker",
            description="Fan-out JDBC ingestion worker.",
            depends_on=[TaskDependency(task_key="ingestion_configs")],
            for_each_task=ForEachTask(
                inputs="{{tasks.ingestion_configs.values.table_configs}}",
                concurrency=settings.foreach_concurrency,
                task=worker_iteration,
            ),
        )

        cdc_task = Task(
            task_key="cdc_scd2_pipeline",
            description="Run the Lakeflow Spark Declarative Pipeline SCD2 step.",
            depends_on=[TaskDependency(task_key="ingestion_worker")],
            pipeline_task=PipelineTask(pipeline_id=pipeline_id, full_refresh=False),
        )

        return JobSettings(
            name=f"[{settings.ingestion_group}] Ingestion Pipeline",
            parameters=[
                JobParameterDefinition(name="ingestion_group", default=settings.ingestion_group),
                JobParameterDefinition(name="configuration_fqn", default=settings.configuration_fqn),
                JobParameterDefinition(name="watermark_fqn", default=settings.watermark_fqn),
                JobParameterDefinition(name="full_refresh", default="false"),
            ],
            tasks=[planner_task, ingestion_worker, cdc_task],
            max_concurrent_runs=1,
            environments=environments,
            git_source=git_source,
        )

    def create_or_update_job(self, settings: JobProvisioningSettings) -> dict[str, str | int]:
        pipeline_id = self.ensure_pipeline(settings)
        job_settings = self.build_job_settings(settings, pipeline_id)
        existing_jobs = [job for job in self.w.jobs.list(name=job_settings.name) if job.settings and job.settings.name == job_settings.name]
        if existing_jobs:
            job_id = existing_jobs[0].job_id
            self.w.jobs.reset(job_id=job_id, new_settings=job_settings)
        else:
            result = self.w.jobs.create(
                name=job_settings.name,
                parameters=job_settings.parameters,
                tasks=job_settings.tasks,
                max_concurrent_runs=job_settings.max_concurrent_runs,
                environments=job_settings.environments,
                git_source=job_settings.git_source,
            )
            job_id = result.job_id
        return {
            "job_id": job_id,
            "pipeline_id": pipeline_id,
            "job_name": job_settings.name,
        }
