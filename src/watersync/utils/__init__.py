"""Setup and provisioning helpers, imported on first use (see watersync/__init__.py)."""

from __future__ import annotations

from importlib import import_module
from typing import Any

_EXPORTS = {
    "IngestionJobProvisioner": "watersync.utils.create_ingestion_job",
    "LakebaseTestDatabaseSetup": "watersync.utils.lakebase_test_database_setup",
    "UnityCatalogSetup": "watersync.utils.uc_setup",
    "ZerobusLogHandler": "watersync.utils.zerobus_logger",
    "setup_watersync_logging": "watersync.utils.zerobus_logger",
}

__all__ = sorted(_EXPORTS)


def __getattr__(name: str) -> Any:
    module = _EXPORTS.get(name)
    if module is None:
        raise AttributeError(f"module 'watersync.utils' has no attribute {name!r}")
    value = getattr(import_module(module), name)
    globals()[name] = value
    return value
