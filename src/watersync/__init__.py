"""WaterSync: metadata-driven JDBC ingestion into Delta.

Names are imported on first use, so the parts that only talk to the Databricks API (such as the
``watersync-create-job`` CLI) work outside Databricks, without PySpark or Delta Lake installed.
"""

from __future__ import annotations

from importlib import import_module
from typing import Any

_EXPORTS = {
    "CdcScd2PipelineBuilder": "watersync.cdc_pipeline",
    "build_pipeline_from_spark_conf": "watersync.cdc_pipeline",
    "IngestionConfigPlanner": "watersync.config_planner",
    "PlannerConfigRepository": ("watersync.config_planner", "JdbcIngestionConfigRepository"),
    "EpicCsaIngestionWorker": "watersync.ingestion",
    "EpicStoredProcedureIngestionWorker": "watersync.ingestion",
    "JdbcIngestionOrchestrator": "watersync.ingestion",
    "JdbcIngestionWorker": "watersync.ingestion",
    "TimestampWatermarkIngestionWorker": "watersync.ingestion",
    "IngestionConfig": "watersync.models",
    "JdbcRuntimeSettings": "watersync.models",
    "JobProvisioningSettings": "watersync.models",
    "ReadResult": "watersync.models",
    "IngestionJobProvisioner": "watersync.utils.create_ingestion_job",
    "LakebaseTestDatabaseSetup": "watersync.utils.lakebase_test_database_setup",
    "UnityCatalogSetup": "watersync.utils.uc_setup",
}

__all__ = sorted(_EXPORTS)


def __getattr__(name: str) -> Any:
    target = _EXPORTS.get(name)
    if target is None:
        raise AttributeError(f"module 'watersync' has no attribute {name!r}")
    module, attribute = target if isinstance(target, tuple) else (target, name)
    value = getattr(import_module(module), attribute)
    globals()[name] = value
    return value
