# Job Provisioning

The Control Plane app's **Jobs** page is the recommended way to create jobs: it also configures job and SDP compute, the schedule and run-as identities (see [Jobs created by the app](Control-Plane-App#jobs-created-by-the-app)), and its settings can be exported and imported with the rest of the configuration. From Python:

```python
from watersync.models import JobProvisioningSettings
from watersync.utils import IngestionJobProvisioner

settings = JobProvisioningSettings(
    ingestion_group="epic",
    configuration_fqn="main.watersync.jdbc_ingestion_config",
    watermark_fqn="main.watersync.jdbc_ingestion_watermark",
    planner_notebook_path="notebooks/Task - Plan Configs",
    worker_notebook_path="notebooks/Task - Run Ingestion",
    wheel_uri="/Volumes/main/watersync/wheels/watersync-0.3.1-py3-none-any.whl",
    foreach_concurrency=4,
    cdc_pipeline_file_path="/Workspace/Users/<user>/watersync/src/watersync/pipeline_bootstrap.py",
    # cdc_pipeline_id="<pipeline-uuid>",   # reuse an existing pipeline instead
    git_url="https://github.com/databricks-industry-solutions/watersync",   # empty = workspace notebook paths
    git_branch="main",
)

result = IngestionJobProvisioner().create_or_update_job(settings)
# {"job_id": ..., "pipeline_id": ..., "job_name": "[epic] Ingestion Pipeline"}
```

Or via the CLI:

```bash
watersync-create-job \
  --configuration-fqn main.watersync.jdbc_ingestion_config \
  --watermark-fqn main.watersync.jdbc_ingestion_watermark \
  --ingestion-group epic \
  --wheel-uri "/Volumes/main/watersync/wheels/watersync-0.3.1-py3-none-any.whl" \
  --planner-notebook-path "/Workspace/Users/<user>/watersync/notebooks/Task - Plan Configs" \
  --worker-notebook-path "/Workspace/Users/<user>/watersync/notebooks/Task - Run Ingestion" \
  --foreach-concurrency 4
```

The CLI always uses workspace notebook paths, and `--cdc-pipeline-file-path` defaults to `./src/watersync/pipeline_bootstrap.py`.
