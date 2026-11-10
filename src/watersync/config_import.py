"""Import a WaterSync configuration file without the Control Plane app.

Reads the file the app exports (``format: watersync-config``), validates every entry, writes the config rows
through a SQL warehouse, and creates or updates each group's ingestion job and CDC pipeline and the row check
jobs through the Databricks API. The jobs are built the same way as the app's (server/ingestionJobs.ts,
server/pipelines.ts, server/rowChecks.ts, server/jobSource.ts), so a file imported here or through the app gives
the same jobs.

Only ``databricks-sdk`` is needed: it runs on a laptop or a CI runner, without PySpark.
"""

from __future__ import annotations

import base64
import hashlib
import json
import re
import time
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Callable
from urllib.parse import urlparse

FILE_FORMAT = "watersync-config"
FILE_VERSION = 1

PLANNER_NOTEBOOK = "notebooks/Task - Plan Configs"
WORKER_NOTEBOOK = "notebooks/Task - Run Ingestion"
SELECTIVE_REFRESH_NOTEBOOK = "src/watersync/utils/selective_pipeline_refresh"
ROW_CHECK_NOTEBOOK = "notebooks/Task - Row Check"
ENVIRONMENT_KEY = "watersync_environment"
JOB_CLUSTER_KEY = "watersync_cluster"
GENERATED_PIPELINES_DIR = "/Shared/watersync-generated-pipelines"

# The pipeline entry point: the logic lives in the watersync package the pipeline installs.
PIPELINE_BOOTSTRAP = (
    "from __future__ import annotations\n\nfrom pyspark import pipelines as dp\nfrom pyspark.sql import SparkSession\n\n"
    "from watersync.cdc_pipeline import build_pipeline_from_spark_conf\n\n"
    "spark = SparkSession.getActiveSession() or SparkSession.builder.getOrCreate()\n"
    "build_pipeline_from_spark_conf(spark=spark, dp_module=dp)\n"
)

GIT_PROVIDERS = (
    "gitHub",
    "gitHubEnterprise",
    "azureDevOpsServices",
    "gitLab",
    "gitLabEnterpriseEdition",
    "bitbucketCloud",
    "bitbucketServer",
    "awsCodeCommit",
)
_DETECTED_PROVIDERS = [
    (re.compile(r"^github\.com$"), "gitHub"),
    (re.compile(r"^(dev\.azure\.com|.+\.visualstudio\.com)$"), "azureDevOpsServices"),
    (re.compile(r"^gitlab\.com$"), "gitLab"),
    (re.compile(r"^bitbucket\.org$"), "bitbucketCloud"),
    (re.compile(r"^git-codecommit\.[a-z0-9-]+\.amazonaws\.com$"), "awsCodeCommit"),
]

# Values that end up in a pip requirement or a %pip line, or in SQL: the same rules as the app.
_PIP_TOKEN = re.compile(r"^[^\s\"'`\\$]+$")
# One requirement and no pip options: a value starting with - would be read as an option line.
_PIP_REQUIREMENT = re.compile(r"^[^\s\"'`\\$-][^\s\"'`\\$]*$")
_GIT_URL = re.compile(r"^https?://[^\s\"'`\\$]+$", re.IGNORECASE)
_IDENTIFIER = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")
_WHEEL_PATH = re.compile(
    r"^/(Volumes/[^/\s\"'`\\$]+/[^/\s\"'`\\$]+/[^/\s\"'`\\$]+|Workspace)/[^\s\"'`\\$]*[^/\s\"'`\\$]\.whl$"
)
_PROCEDURE_NAME = re.compile(r"^[A-Za-z_][\w$#]*(\.[A-Za-z_][\w$#]*){0,2}$")
_COLUMN_NAMES = re.compile(r"^[A-Za-z_][\w$#]*(\s*,\s*[A-Za-z_][\w$#]*)*$")
_RANGE_BOUND = re.compile(r"^$|^-?\d+(\.\d+)?$|^\d{4}-\d{2}-\d{2}( \d{2}:\d{2}:\d{2})?$|^-\d{1,5}d$")
_SERVICE_PRINCIPAL = re.compile(r"^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$", re.IGNORECASE)

# jdbc_ingestion_config columns, with the file field and SQL type of each (server/transfer.ts configColumns).
CONFIG_COLUMNS: list[tuple[str, str, str]] = [
    ("ingestion_group", "ingestionGroup", "STRING"),
    ("source_table_name", "sourceTableName", "STRING"),
    ("staging_table_fqn", "stagingTableFqn", "STRING"),
    ("target_table_fqn", "targetTableFqn", "STRING"),
    ("ingestion_type", "ingestionType", "STRING"),
    ("key_columns", "keyColumns", "STRING"),
    ("watermark_column", "watermarkColumn", "STRING"),
    ("partition_column", "partitionColumn", "STRING"),
    ("predicate_column", "predicateColumn", "STRING"),
    ("epic_csa_enabled", "epicCsaEnabled", "BOOLEAN"),
    ("epic_change_method", "epicChangeMethod", "STRING"),
    ("epic_change_procedure", "epicChangeProcedure", "STRING"),
    ("epic_change_skip_update_date", "epicChangeSkipUpdateDate", "BOOLEAN"),
    ("epic_change_column_names", "epicChangeColumnNames", "STRING"),
    ("auto_cdc_from_snapshot", "autoCdcFromSnapshot", "BOOLEAN"),
    ("jdbc_url", "jdbcUrl", "STRING"),
    ("jdbc_user", "jdbcUser", "STRING"),
    ("jdbc_secret_scope", "jdbcSecretScope", "STRING"),
    ("jdbc_secret_key", "jdbcSecretKey", "STRING"),
    ("uc_secret_name", "ucSecretName", "STRING"),
    ("connection_name", "connectionName", "STRING"),
    ("connection_database", "connectionDatabase", "STRING"),
    ("watermark_threshold_minutes", "watermarkThresholdMinutes", "INT"),
    ("fetch_size", "fetchSize", "INT"),
    ("num_partitions", "numPartitions", "INT"),
    ("enabled", "enabled", "BOOLEAN"),
]


class ImportValidationError(ValueError):
    """Every problem of a file at once, each with its path in the file (``jobs.0.notebooks.gitUrl``)."""

    def __init__(self, issues: list[tuple[str, str]]):
        self.issues = issues
        super().__init__("\n".join(f"{path}: {message}" for path, message in issues))


@dataclass
class ValidatedFile:
    configs: list[dict[str, Any]] = field(default_factory=list)
    jobs: list[dict[str, Any]] = field(default_factory=list)
    row_check_jobs: list[dict[str, Any]] = field(default_factory=list)


# --- Validation ----------------------------------------------------------------------------------------------


def git_provider_of(url: str) -> str | None:
    host = (urlparse(url).hostname or "").lower()
    return next((provider for pattern, provider in _DETECTED_PROVIDERS if pattern.match(host)), None)


def without_user_info(url: str) -> str:
    return re.sub(r"^(https?://)[^/@]+@", r"\1", url.strip(), flags=re.IGNORECASE)


def _text(value: Any) -> str:
    return value.strip() if isinstance(value, str) else ""


def _optional_text(value: Any) -> str | None:
    text = _text(value)
    return text or None


def _bool(value: Any, default: bool = False) -> bool:
    if value is None:
        return default
    if isinstance(value, str):
        return value.strip().lower() == "true"
    return bool(value)


def _int(value: Any, default: int) -> Any:
    return default if value is None else value


class _Issues:
    def __init__(self) -> None:
        self.items: list[tuple[str, str]] = []

    def add(self, path: str, message: str) -> None:
        self.items.append((path, message))


def _validate_config(entry: dict[str, Any], path: str, issues: _Issues) -> dict[str, Any]:
    """One config row, normalized like the app's toConfigRow, as {file field: value}."""
    config = {name: entry.get(name) for _, name, _ in CONFIG_COLUMNS}
    for name in ("ingestionGroup", "sourceTableName"):
        config[name] = _text(entry.get(name))
        if not config[name]:
            issues.add(f"{path}.{name}", "Required")
    config["ingestionType"] = _text(entry.get("ingestionType")).lower() or "incremental"
    if config["ingestionType"] not in ("incremental", "full"):
        issues.add(f"{path}.ingestionType", "Use incremental or full")
    config["targetTableFqn"] = _text(entry.get("targetTableFqn"))
    if not re.fullmatch(r"[^.]+\.[^.]+\.[^.]+", config["targetTableFqn"]):
        issues.add(f"{path}.targetTableFqn", "Use catalog.schema.table")
    for name in ("epicCsaEnabled", "autoCdcFromSnapshot", "epicChangeSkipUpdateDate"):
        config[name] = _bool(entry.get(name))
    config["enabled"] = _bool(entry.get("enabled"), default=True)
    for name in (
        "stagingTableFqn", "keyColumns", "watermarkColumn", "partitionColumn", "predicateColumn", "jdbcUrl",
        "jdbcUser", "jdbcSecretScope", "jdbcSecretKey", "ucSecretName", "connectionName", "connectionDatabase",
        "epicChangeProcedure", "epicChangeColumnNames",
    ):
        config[name] = _optional_text(entry.get(name))
    for name, default, minimum in (("watermarkThresholdMinutes", 5, 0), ("fetchSize", 10000, 1), ("numPartitions", 8, 1)):
        value = _int(entry.get(name), default)
        if not isinstance(value, int) or isinstance(value, bool) or value < minimum:
            issues.add(f"{path}.{name}", f"Use a whole number of at least {minimum}")
        config[name] = value

    method = _text(entry.get("epicChangeMethod")) or "csa_table"
    if method not in ("csa_table", "stored_procedure"):
        issues.add(f"{path}.epicChangeMethod", "Use csa_table or stored_procedure")
    incremental = config["ingestionType"] == "incremental"
    if config["epicCsaEnabled"] and not incremental:
        issues.add(f"{path}.epicCsaEnabled", "EPIC CSA requires incremental ingestion")
    if method == "stored_procedure" and not config["epicCsaEnabled"]:
        issues.add(f"{path}.epicChangeMethod", "The EPIC stored procedure requires EPIC CSA mode")
    if config["epicChangeProcedure"] and not _PROCEDURE_NAME.match(config["epicChangeProcedure"]):
        issues.add(f"{path}.epicChangeProcedure", "Use a procedure name such as EPIC_UTIL.ESP_CR_ALTERED_ROWS")
    if config["epicChangeColumnNames"] and not _COLUMN_NAMES.match(config["epicChangeColumnNames"]):
        issues.add(f"{path}.epicChangeColumnNames", "Use comma-separated column names")
    # The procedure's batch can run several statements: the same rules as the worker.
    if method == "stored_procedure" and config["epicCsaEnabled"]:
        if not _PROCEDURE_NAME.match(config["sourceTableName"]):
            issues.add(f"{path}.sourceTableName", "Use a plain table name, such as dbo.PAT_ENC, with the stored procedure")
        if config["keyColumns"] and not _COLUMN_NAMES.match(config["keyColumns"]):
            issues.add(f"{path}.keyColumns", "Use comma-separated column names with the stored procedure")
    if config["autoCdcFromSnapshot"]:
        if incremental:
            issues.add(f"{path}.autoCdcFromSnapshot", "Snapshot CDC requires full ingestion")
        if not config["stagingTableFqn"]:
            issues.add(f"{path}.stagingTableFqn", "Snapshot CDC requires a staging table")
        if not config["keyColumns"]:
            issues.add(f"{path}.keyColumns", "Snapshot CDC requires key columns")
    if incremental and not config["keyColumns"]:
        issues.add(f"{path}.keyColumns", "Incremental ingestion requires a key column")
    if incremental and not config["epicCsaEnabled"] and not config["watermarkColumn"]:
        issues.add(f"{path}.watermarkColumn", "Incremental ingestion requires a watermark column unless EPIC CSA is enabled")
    # The rules the ingestion job applies when it loads the row (watersync/ingestion.py).
    if not config["jdbcUrl"] and not config["connectionName"]:
        issues.add(f"{path}.connectionName", "Set connectionName or jdbcUrl")
    if bool(config["jdbcSecretScope"]) != bool(config["jdbcSecretKey"]):
        issues.add(f"{path}.jdbcSecretKey", "Set jdbcSecretScope and jdbcSecretKey together")
    if config["jdbcUrl"] and not config["connectionName"] and not (config["jdbcSecretScope"] or config["ucSecretName"]):
        issues.add(f"{path}.jdbcSecretScope", "A JDBC URL needs a password secret (jdbcSecretScope/jdbcSecretKey or ucSecretName)")

    # Stored like the app stores it: no watermark where none is read, procedure settings only where they apply.
    if not incremental or config["epicCsaEnabled"]:
        config["watermarkColumn"] = None
    if not config["connectionName"]:
        config["connectionDatabase"] = None
    stored_procedure = config["epicCsaEnabled"] and method == "stored_procedure"
    config["epicChangeMethod"] = method if config["epicCsaEnabled"] else None
    if not stored_procedure:
        config["epicChangeProcedure"] = None
        config["epicChangeSkipUpdateDate"] = False
        config["epicChangeColumnNames"] = None
    return config


def _with_legacy_source(entry: dict[str, Any]) -> dict[str, Any]:
    """Older files have flat gitUrl/gitBranch/libraryPath fields instead of notebooks and library."""
    entry = dict(entry)
    git_url, git_branch, library_path = entry.pop("gitUrl", None), entry.pop("gitBranch", None), entry.pop("libraryPath", None)
    if entry.get("notebooks") is None and git_url is not None:
        entry["notebooks"] = {"source": "GIT", "gitUrl": git_url, "referenceType": "BRANCH", "reference": git_branch}
    if entry.get("library") is None:
        wheel = _text(library_path)
        entry["library"] = {"source": "WHEEL", "path": wheel} if wheel else {"source": "GIT"}
    return entry


def _validate_source(entry: dict[str, Any], path: str, issues: _Issues) -> tuple[dict[str, Any], dict[str, Any]]:
    notebooks_in = entry.get("notebooks") or {}
    library_in = entry.get("library") or {"source": "GIT"}
    source = notebooks_in.get("source")
    if source == "WORKSPACE":
        base_path = _text(notebooks_in.get("basePath")).rstrip("/")
        if not re.fullmatch(r"/[^\"'`\\$\n]+", base_path):
            issues.add(f"{path}.notebooks.basePath", "Use an absolute workspace path, e.g. /Workspace/Users/me/watersync")
        notebooks: dict[str, Any] = {"source": "WORKSPACE", "basePath": base_path}
    elif source == "GIT":
        git_url = without_user_info(_text(notebooks_in.get("gitUrl")))
        if not _GIT_URL.match(git_url) or not urlparse(git_url).hostname:
            issues.add(f"{path}.notebooks.gitUrl", "Use the repository's https URL")
        provider = notebooks_in.get("gitProvider") or None
        if provider is not None and provider not in GIT_PROVIDERS:
            issues.add(f"{path}.notebooks.gitProvider", f"Use one of {', '.join(GIT_PROVIDERS)}")
        if not provider and not git_provider_of(git_url):
            issues.add(
                f"{path}.notebooks.gitProvider",
                "Choose the Git provider: it is only detected for github.com, dev.azure.com, gitlab.com, "
                "bitbucket.org and AWS CodeCommit",
            )
        reference_type = notebooks_in.get("referenceType") or "BRANCH"
        if reference_type not in ("BRANCH", "TAG", "COMMIT"):
            issues.add(f"{path}.notebooks.referenceType", "Use BRANCH, TAG or COMMIT")
        reference = _text(notebooks_in.get("reference"))
        if not _PIP_TOKEN.match(reference):
            issues.add(f"{path}.notebooks.reference", "Required; no whitespace, quotes or backslashes")
        elif reference_type == "COMMIT" and not re.fullmatch(r"[0-9a-fA-F]{7,40}", reference):
            issues.add(f"{path}.notebooks.reference", "Use a commit hash")
        notebooks = {
            "source": "GIT",
            "gitUrl": git_url,
            "gitProvider": provider,
            "referenceType": reference_type,
            "reference": reference,
        }
    else:
        issues.add(f"{path}.notebooks.source", "Use GIT or WORKSPACE")
        notebooks = {"source": source}

    library_source = library_in.get("source")
    if library_source == "WHEEL":
        library: dict[str, Any] = {"source": "WHEEL", "path": _text(library_in.get("path"))}
        if not _WHEEL_PATH.match(library["path"]):
            issues.add(f"{path}.library.path", "Use a wheel in a volume (/Volumes/<catalog>/<schema>/<volume>/<file>.whl) or the workspace")
    elif library_source == "PIP":
        library = {"source": "PIP", "requirement": _text(library_in.get("requirement"))}
        if not _PIP_REQUIREMENT.match(library["requirement"]):
            issues.add(f"{path}.library.requirement", "One requirement, such as watersync==0.4.0; no spaces, quotes, backslashes, $ or leading -")
    elif library_source == "GIT":
        library = {"source": "GIT"}
        if notebooks.get("source") != "GIT":
            issues.add(f"{path}.library.source", "Installing from the Git repository needs notebooks from a Git repository")
    else:
        issues.add(f"{path}.library.source", "Use GIT, WHEEL or PIP")
        library = {"source": library_source}
    return notebooks, library


def _validate_schedule(value: Any, path: str, issues: _Issues, default_cron: str) -> dict[str, Any]:
    schedule = value or {}
    cron = _text(schedule.get("quartzCronExpression")) or default_cron
    if len(cron.split()) < 6:
        issues.add(f"{path}.schedule.quartzCronExpression", "Use a Quartz cron expression with at least 6 fields")
    pause = schedule.get("pauseStatus") or "UNPAUSED"
    if pause not in ("PAUSED", "UNPAUSED"):
        issues.add(f"{path}.schedule.pauseStatus", "Use PAUSED or UNPAUSED")
    return {
        "enabled": _bool(schedule.get("enabled")),
        "quartzCronExpression": cron,
        "timezoneId": _text(schedule.get("timezoneId")) or "UTC",
        "pauseStatus": pause,
    }


def _validate_compute(value: Any, path: str, issues: _Issues) -> dict[str, Any]:
    compute = {
        "mode": "SERVERLESS",
        "performanceTarget": "PERFORMANCE_OPTIMIZED",
        "sparkVersion": "",
        "driverNodeTypeId": "",
        "workerNodeTypeId": "",
        "minWorkers": 1,
        "maxWorkers": 4,
        **(value or {}),
    }
    if compute["mode"] not in ("SERVERLESS", "JOB_CLUSTER"):
        issues.add(f"{path}.compute.mode", "Use SERVERLESS or JOB_CLUSTER")
    if compute["performanceTarget"] not in ("STANDARD", "PERFORMANCE_OPTIMIZED"):
        issues.add(f"{path}.compute.performanceTarget", "Use STANDARD or PERFORMANCE_OPTIMIZED")
    if compute["mode"] == "JOB_CLUSTER":
        for name in ("sparkVersion", "driverNodeTypeId", "workerNodeTypeId"):
            if not _text(compute[name]):
                issues.add(f"{path}.compute.{name}", "Required for a job cluster")
        if compute["maxWorkers"] < compute["minWorkers"]:
            issues.add(f"{path}.compute.maxWorkers", "Maximum workers must be at least the minimum")
    return compute


def _validate_run_as(value: Any, path: str, issues: _Issues, modes: tuple[str, ...], default: str) -> dict[str, str]:
    run_as = {"mode": default, "principal": "", **(value or {})}
    run_as["principal"] = _text(run_as["principal"])
    if run_as["mode"] not in modes:
        issues.add(f"{path}.mode", f"Use one of {', '.join(modes)}")
    elif run_as["mode"] == "USER" and not re.fullmatch(r"[^\s@]+@[^\s@]+", run_as["principal"]):
        issues.add(f"{path}.principal", "Enter the user email")
    elif run_as["mode"] == "SERVICE_PRINCIPAL" and not _SERVICE_PRINCIPAL.match(run_as["principal"]):
        issues.add(f"{path}.principal", "Enter the service principal application ID")
    return run_as


def _validate_job(entry: dict[str, Any], path: str, issues: _Issues) -> dict[str, Any]:
    entry = _with_legacy_source(entry)
    job: dict[str, Any] = {"ingestionGroup": _text(entry.get("ingestionGroup"))}
    if not job["ingestionGroup"]:
        issues.add(f"{path}.ingestionGroup", "Required")
    job["notebooks"], job["library"] = _validate_source(entry, path, issues)
    concurrency = entry.get("foreachConcurrency", 4)
    if not isinstance(concurrency, int) or not 1 <= concurrency <= 100:
        issues.add(f"{path}.foreachConcurrency", "Use a whole number from 1 to 100")
    job["foreachConcurrency"] = concurrency
    job["cdcPipelineId"] = _optional_text(entry.get("cdcPipelineId"))
    job["schedule"] = _validate_schedule(entry.get("schedule"), path, issues, "0 0 8 * * ?")
    job["compute"] = _validate_compute(entry.get("compute"), path, issues)
    pipeline = {"mode": "SERVERLESS", "nodeTypeId": "", "driverNodeTypeId": "", "minWorkers": 1, "maxWorkers": 4,
                "photon": False, **(entry.get("pipelineCompute") or {})}
    if pipeline["mode"] not in ("SERVERLESS", "CLASSIC"):
        issues.add(f"{path}.pipelineCompute.mode", "Use SERVERLESS or CLASSIC")
    if pipeline["mode"] == "CLASSIC":
        for name in ("nodeTypeId", "driverNodeTypeId"):
            if not _text(pipeline[name]):
                issues.add(f"{path}.pipelineCompute.{name}", "Required for classic pipeline compute")
        if pipeline["maxWorkers"] < pipeline["minWorkers"]:
            issues.add(f"{path}.pipelineCompute.maxWorkers", "Maximum workers must be at least the minimum")
    job["pipelineCompute"] = pipeline
    run_as = entry.get("runAs") or {}
    job["runAs"] = {
        "job": _validate_run_as(run_as.get("job"), f"{path}.runAs.job", issues, ("DEFAULT", "USER", "SERVICE_PRINCIPAL"), "DEFAULT"),
        "pipeline": _validate_run_as(
            run_as.get("pipeline"), f"{path}.runAs.pipeline", issues,
            ("SAME_AS_JOB", "DEFAULT", "USER", "SERVICE_PRINCIPAL"), "SAME_AS_JOB",
        ),
    }
    return job


def _validate_row_check_job(entry: dict[str, Any], path: str, issues: _Issues) -> dict[str, Any]:
    entry = _with_legacy_source(entry)
    job: dict[str, Any] = {"ingestionGroup": _text(entry.get("ingestionGroup"))}
    job["notebooks"], job["library"] = _validate_source(entry, path, issues)
    job["checkMode"] = entry.get("checkMode") or "basic"
    if job["checkMode"] not in ("basic", "detailed"):
        issues.add(f"{path}.checkMode", "Use basic or detailed")
    job["rangeFrom"] = _text(entry.get("rangeFrom")) if job["checkMode"] == "detailed" else ""
    if not _RANGE_BOUND.match(job["rangeFrom"]):
        issues.add(f"{path}.rangeFrom", "Use a number, a date (2026-09-01), a timestamp or days back (-30d)")
    parallel = entry.get("maxParallelTables", 4)
    if not isinstance(parallel, int) or not 1 <= parallel <= 32:
        issues.add(f"{path}.maxParallelTables", "Use a whole number from 1 to 32")
    job["maxParallelTables"] = parallel
    job["schedule"] = _validate_schedule(entry.get("schedule"), path, issues, "0 0 6 * * ?")
    job["compute"] = _validate_compute(entry.get("compute"), path, issues)
    job["runAs"] = _validate_run_as(entry.get("runAs"), f"{path}.runAs", issues, ("DEFAULT", "USER", "SERVICE_PRINCIPAL"), "DEFAULT")
    return job


def validate_file(file: dict[str, Any]) -> ValidatedFile:
    """Every entry of the file, checked with the app's rules; raises ImportValidationError listing all problems."""
    issues = _Issues()
    if file.get("format") != FILE_FORMAT:
        issues.add("format", "Not a WaterSync configuration export")
    if file.get("version") != FILE_VERSION:
        issues.add("version", f"Unsupported export version (expected {FILE_VERSION})")
    result = ValidatedFile(
        configs=[_validate_config(entry, f"configs.{i}", issues) for i, entry in enumerate(file.get("configs") or [])],
        jobs=[_validate_job(entry, f"jobs.{i}", issues) for i, entry in enumerate(file.get("jobs") or [])],
        row_check_jobs=[
            _validate_row_check_job(entry, f"rowCheckJobs.{i}", issues)
            for i, entry in enumerate(file.get("rowCheckJobs") or [])
        ],
    )
    for section, entries, label in (
        ("configs", result.configs, lambda e: f"source table {e['ingestionGroup']} / {e['sourceTableName']}"),
        ("jobs", result.jobs, lambda e: f"job settings for group {e['ingestionGroup']}"),
        ("rowCheckJobs", result.row_check_jobs,
         lambda e: f"{e['checkMode']} row check job for {e['ingestionGroup'] or 'the location'}"),
    ):
        seen: set[str] = set()
        for i, entry in enumerate(entries):
            name = label(entry)
            if name in seen:
                issues.add(f"{section}.{i}", f"Duplicate {name}")
            seen.add(name)
    if issues.items:
        raise ImportValidationError(issues.items)
    return result


def use_bundle(validated: ValidatedFile, base_path: str, wheel_path: str) -> None:
    """Points every job at a bundle deployment: notebooks from its files folder, the wheel it built."""
    for job in [*validated.jobs, *validated.row_check_jobs]:
        job["notebooks"] = {"source": "WORKSPACE", "basePath": base_path.rstrip("/")}
        job["library"] = {"source": "WHEEL", "path": wheel_path}


# --- Job settings (the same as the app's) --------------------------------------------------------------------


def _git_provider(notebooks: dict[str, Any]) -> str:
    return notebooks.get("gitProvider") or git_provider_of(notebooks["gitUrl"]) or "gitHub"


def git_source(notebooks: dict[str, Any]) -> dict[str, Any] | None:
    if notebooks["source"] != "GIT":
        return None
    reference_field = {"TAG": "git_tag", "COMMIT": "git_commit"}.get(notebooks["referenceType"], "git_branch")
    return {"git_url": notebooks["gitUrl"], "git_provider": _git_provider(notebooks), reference_field: notebooks["reference"]}


def notebook_task(notebooks: dict[str, Any], relative_path: str) -> dict[str, Any]:
    if notebooks["source"] == "GIT":
        return {"notebook_path": relative_path, "source": "GIT"}
    return {"notebook_path": f"{notebooks['basePath']}/{relative_path}", "source": "WORKSPACE"}


def watersync_requirement(notebooks: dict[str, Any], library: dict[str, Any]) -> str:
    if library["source"] == "WHEEL":
        return library["path"]
    if library["source"] == "PIP":
        return library["requirement"]
    url = notebooks["gitUrl"]
    if _git_provider(notebooks) in ("gitHub", "gitHubEnterprise"):
        url = re.sub(r"\.git$", "", url) + ".git"
    return f"watersync@git+{url}@{notebooks['reference']}"


def is_wheel_file(requirement: str) -> bool:
    return requirement.startswith(("/Volumes/", "/Workspace/")) and requirement.endswith(".whl")


def job_compute(compute: dict[str, Any], dependency: str) -> tuple[dict[str, Any], dict[str, Any]]:
    """(fields for every task, fields for the job): an environment on serverless, a job cluster otherwise."""
    if compute["mode"] == "SERVERLESS":
        return {"environment_key": ENVIRONMENT_KEY}, {
            "environments": [
                {"environment_key": ENVIRONMENT_KEY, "spec": {"environment_version": "4", "dependencies": [dependency]}}
            ]
        }
    library = {"whl": dependency} if is_wheel_file(dependency) else {"pypi": {"package": dependency}}
    return {"job_cluster_key": JOB_CLUSTER_KEY, "libraries": [library]}, {
        "job_clusters": [
            {
                "job_cluster_key": JOB_CLUSTER_KEY,
                "new_cluster": {
                    "spark_version": compute["sparkVersion"],
                    "driver_node_type_id": compute["driverNodeTypeId"],
                    "node_type_id": compute["workerNodeTypeId"],
                    "autoscale": {"min_workers": compute["minWorkers"], "max_workers": compute["maxWorkers"]},
                },
            }
        ]
    }


def run_as(target: dict[str, str]) -> dict[str, str] | None:
    if target["mode"] == "USER":
        return {"user_name": target["principal"]}
    if target["mode"] == "SERVICE_PRINCIPAL":
        return {"service_principal_name": target["principal"]}
    return None


def _schedule(schedule: dict[str, Any]) -> dict[str, Any]:
    if not schedule["enabled"]:
        return {}
    return {
        "schedule": {
            "quartz_cron_expression": schedule["quartzCronExpression"],
            "timezone_id": schedule["timezoneId"],
            "pause_status": schedule["pauseStatus"],
        }
    }


def ingestion_job_name(ingestion_group: str) -> str:
    return f"[{ingestion_group}] Ingestion Pipeline"


def cdc_pipeline_name(ingestion_group: str) -> str:
    return f"[{ingestion_group}] CDC SCD2 Pipeline"


def ingestion_job_settings(catalog: str, schema: str, job: dict[str, Any], cdc_pipeline_id: str | None) -> dict[str, Any]:
    notebooks = job["notebooks"]
    task_compute, job_compute_settings = job_compute(job["compute"], watersync_requirement(notebooks, job["library"]))
    tasks: list[dict[str, Any]] = [
        {"task_key": "ingestion_configs", "notebook_task": notebook_task(notebooks, PLANNER_NOTEBOOK), **task_compute},
        {
            "task_key": "ingestion_worker",
            "depends_on": [{"task_key": "ingestion_configs"}],
            "for_each_task": {
                "inputs": "{{tasks.ingestion_configs.values.table_configs}}",
                "concurrency": job["foreachConcurrency"],
                "task": {
                    "task_key": "ingestion_worker_iteration",
                    **task_compute,
                    "notebook_task": {
                        **notebook_task(notebooks, WORKER_NOTEBOOK),
                        "base_parameters": {
                            "source_table_name": "{{input.source_table_name}}",
                            "full_refresh": "{{job.parameters.full_refresh}}",
                        },
                    },
                },
            },
        },
    ]
    if cdc_pipeline_id:
        # Starting a notebook takes a while: run the refresh only when the planner flagged tables for it.
        tasks.append(
            {
                "task_key": "has_full_refresh_targets",
                "depends_on": [{"task_key": "ingestion_worker"}],
                "condition_task": {
                    "op": "GREATER_THAN",
                    "left": "{{tasks.ingestion_configs.values.full_refresh_count}}",
                    "right": "0",
                },
            }
        )
        tasks.append(
            {
                "task_key": "selective_pipeline_refresh",
                "depends_on": [{"task_key": "has_full_refresh_targets", "outcome": "true"}],
                **task_compute,
                "notebook_task": {
                    **notebook_task(notebooks, SELECTIVE_REFRESH_NOTEBOOK),
                    "base_parameters": {"cdc_pipeline_id": cdc_pipeline_id},
                },
            }
        )
        tasks.append(
            {
                "task_key": "cdc_scd2_pipeline",
                # Runs after the refresh, or right away when the refresh is skipped.
                "depends_on": [
                    {"task_key": "has_full_refresh_targets", "outcome": "false"},
                    {"task_key": "selective_pipeline_refresh"},
                ],
                "run_if": "NONE_FAILED",
                "pipeline_task": {"pipeline_id": cdc_pipeline_id, "full_refresh": False},
            }
        )
    # performance_target is job-wide and also applies to a serverless pipeline task.
    serverless = job["compute"]["mode"] == "SERVERLESS" or (
        bool(cdc_pipeline_id) and job["pipelineCompute"]["mode"] == "SERVERLESS"
    )
    job_run_as = run_as(job["runAs"]["job"])
    source = git_source(notebooks)
    return {
        "name": ingestion_job_name(job["ingestionGroup"]),
        "max_concurrent_runs": 1,
        **({"performance_target": job["compute"]["performanceTarget"]} if serverless else {}),
        **job_compute_settings,
        "parameters": [
            {"name": "configuration_fqn", "default": f"{catalog}.{schema}.jdbc_ingestion_config"},
            {"name": "watermark_fqn", "default": f"{catalog}.{schema}.jdbc_ingestion_watermark"},
            {"name": "ingestion_group", "default": job["ingestionGroup"]},
            {"name": "full_refresh", "default": "false"},
        ],
        "tasks": tasks,
        **({"run_as": job_run_as} if job_run_as else {}),
        **({"git_source": source} if source else {}),
        **_schedule(job["schedule"]),
    }


def cdc_pipeline_settings(
    catalog: str, schema: str, job: dict[str, Any], library: dict[str, Any]
) -> dict[str, Any]:
    """The group's CDC pipeline; `library` is where its entry point lives (see ensure_cdc_pipeline)."""
    dependency = watersync_requirement(job["notebooks"], job["library"])
    pipeline_compute = job["pipelineCompute"]
    target = job["runAs"]["job"] if job["runAs"]["pipeline"]["mode"] == "SAME_AS_JOB" else job["runAs"]["pipeline"]
    pipeline_run_as = run_as(target)
    compute: dict[str, Any] = (
        {
            "serverless": False,
            # Auto CDC flows need at least the Pro edition on classic compute.
            "edition": "ADVANCED",
            "photon": pipeline_compute["photon"],
            "clusters": [
                {
                    "label": "default",
                    "node_type_id": pipeline_compute["nodeTypeId"],
                    "driver_node_type_id": pipeline_compute["driverNodeTypeId"],
                    "autoscale": {
                        "min_workers": pipeline_compute["minWorkers"],
                        "max_workers": pipeline_compute["maxWorkers"],
                        "mode": "ENHANCED",
                    },
                }
            ],
        }
        if pipeline_compute["mode"] == "CLASSIC"
        else {"serverless": True, "environment": {"dependencies": [dependency]}}
    )
    return {
        "name": cdc_pipeline_name(job["ingestionGroup"]),
        "catalog": catalog,
        "target": schema,
        "configuration": {
            "pipeline.configuration_fqn": f"{catalog}.{schema}.jdbc_ingestion_config",
            "pipeline.watermark_fqn": f"{catalog}.{schema}.jdbc_ingestion_watermark",
            "pipeline.ingestion_group": job["ingestionGroup"],
        },
        "libraries": [library],
        "channel": "CURRENT",
        **({"run_as": pipeline_run_as} if pipeline_run_as else {}),
        **compute,
    }


def row_check_job_name(catalog: str, schema: str, ingestion_group: str, mode: str) -> str:
    if ingestion_group:
        return f"[{ingestion_group}] Row Check ({mode})"
    return f"[{catalog}.{schema}] Row Checks" if mode == "basic" else f"[{catalog}.{schema}] Row Checks (detailed)"


def row_check_job_settings(catalog: str, schema: str, job: dict[str, Any]) -> dict[str, Any]:
    notebooks = job["notebooks"]
    task_compute, job_compute_settings = job_compute(job["compute"], watersync_requirement(notebooks, job["library"]))
    scope = f"the {job['ingestionGroup']} group" if job["ingestionGroup"] else f"{catalog}.{schema}"
    job_run_as = run_as(job["runAs"])
    source = git_source(notebooks)
    return {
        "name": row_check_job_name(catalog, schema, job["ingestionGroup"], job["checkMode"]),
        "description": f"{'Basic' if job['checkMode'] == 'basic' else 'Detailed'} row check of {scope}. Created by watersync-import.",
        "max_concurrent_runs": 4,
        **job_compute_settings,
        **({"performance_target": job["compute"]["performanceTarget"]} if job["compute"]["mode"] == "SERVERLESS" else {}),
        **({"run_as": job_run_as} if job_run_as else {}),
        "parameters": [
            {"name": "configuration_fqn", "default": f"{catalog}.{schema}.jdbc_ingestion_config"},
            {"name": "watermark_fqn", "default": f"{catalog}.{schema}.jdbc_ingestion_watermark"},
            {"name": "row_check_fqn", "default": f"{catalog}.{schema}._watersync_row_checks"},
            {"name": "check_mode", "default": job["checkMode"]},
            {"name": "ingestion_group", "default": job["ingestionGroup"]},
            {"name": "source_table_name", "default": ""},
            {"name": "max_parallel_tables", "default": str(job["maxParallelTables"])},
            {"name": "range_column", "default": ""},
            {"name": "range_from", "default": job["rangeFrom"]},
            {"name": "range_to", "default": ""},
        ],
        "tasks": [
            {
                "task_key": "row_check",
                **task_compute,
                "notebook_task": {
                    **notebook_task(notebooks, ROW_CHECK_NOTEBOOK),
                    "base_parameters": {"job_run_id": "{{job.run_id}}"},
                },
            }
        ],
        **({"git_source": source} if source else {}),
        **_schedule(job["schedule"]),
    }


# --- Writing to the workspace --------------------------------------------------------------------------------


class Warehouse:
    """Runs SQL on a SQL warehouse through the Statement Execution API."""

    def __init__(self, workspace: Any, warehouse_id: str):
        self.workspace = workspace
        self.warehouse_id = warehouse_id

    def execute(self, statement: str, parameters: dict[str, str] | None = None) -> list[list[Any]]:
        from databricks.sdk.service.sql import StatementParameterListItem, StatementState

        response = self.workspace.statement_execution.execute_statement(
            statement=statement,
            warehouse_id=self.warehouse_id,
            parameters=[StatementParameterListItem(name=name, value=value) for name, value in (parameters or {}).items()],
            wait_timeout="50s",
        )
        while response.status.state in (StatementState.PENDING, StatementState.RUNNING):
            time.sleep(2)
            response = self.workspace.statement_execution.get_statement(response.statement_id)
        if response.status.state != StatementState.SUCCEEDED:
            error = response.status.error
            raise RuntimeError(f"SQL failed ({response.status.state.value}): {error.message if error else statement}")
        return (response.result.data_array if response.result else None) or []

    # The two calls UnityCatalogSetup makes on a SparkSession.
    def sql(self, statement: str) -> Any:
        rows = self.execute(statement)
        return type("Result", (), {"collect": staticmethod(lambda: rows)})()

    def table(self, name: str) -> Any:
        catalog, schema, table = name.split(".")
        rows = self.execute(
            f"SELECT column_name FROM {_quoted(catalog)}.information_schema.columns "
            "WHERE table_schema = :schema AND table_name = :table",
            {"schema": schema, "table": table},
        )
        fields = [type("Field", (), {"name": row[0]})() for row in rows]
        return type("Table", (), {"schema": type("Schema", (), {"fields": fields})()})()


def ensure_tables(warehouse: Warehouse, catalog: str, schema: str) -> None:
    """The metadata schema and tables, with any columns added since they were created."""
    from watersync.utils.uc_setup import UnityCatalogSetup

    setup = UnityCatalogSetup(warehouse, catalog=catalog, schema=schema)
    setup.ensure_schema()
    setup.create_config_table()
    setup.create_watermark_state_table()


def _quoted(name: str) -> str:
    return "`" + name.replace("`", "``") + "`"


def _config_table(catalog: str, schema: str) -> str:
    return f"{_quoted(catalog)}.{_quoted(schema)}.`jdbc_ingestion_config`"


def upsert_configs(warehouse: Warehouse, catalog: str, schema: str, configs: list[dict[str, Any]]) -> None:
    columns = [column for column, _, _ in CONFIG_COLUMNS]
    row_struct = f"ARRAY<STRUCT<{', '.join(f'{column}: {kind}' for column, _, kind in CONFIG_COLUMNS)}>>"
    updates = ", ".join(f"{column} = s.{column}" for column in columns if column not in ("ingestion_group", "source_table_name"))
    for offset in range(0, len(configs), 500):
        rows = [{column: config[name] for column, name, _ in CONFIG_COLUMNS} for config in configs[offset : offset + 500]]
        warehouse.execute(
            f"""MERGE INTO {_config_table(catalog, schema)} t
            USING (SELECT inline(from_json(:rows, '{row_struct}'))) s
            ON t.ingestion_group = s.ingestion_group AND t.source_table_name = s.source_table_name
            WHEN MATCHED THEN UPDATE SET {updates}, update_dttm = current_timestamp()
            WHEN NOT MATCHED THEN INSERT ({', '.join(columns)}, update_dttm)
              VALUES ({', '.join(f's.{column}' for column in columns)}, current_timestamp())""",
            {"rows": json.dumps(rows)},
        )


def remove_unlisted_configs(warehouse: Warehouse, catalog: str, schema: str, configs: list[dict[str, Any]]) -> int:
    """Deletes rows of the imported groups that the file no longer lists."""
    groups = sorted({config["ingestionGroup"] for config in configs})
    if not groups:
        return 0
    rows = warehouse.execute(
        f"""DELETE FROM {_config_table(catalog, schema)}
        WHERE array_contains(from_json(:groups, 'ARRAY<STRING>'), ingestion_group)
          AND NOT array_contains(from_json(:keys, 'ARRAY<ARRAY<STRING>>'), array(ingestion_group, source_table_name))""",
        {
            "groups": json.dumps(groups),
            "keys": json.dumps([[config["ingestionGroup"], config["sourceTableName"]] for config in configs]),
        },
    )
    return int(rows[0][0]) if rows and rows[0] else 0


def group_needs_cdc_pipeline(warehouse: Warehouse, catalog: str, schema: str, ingestion_group: str) -> bool:
    rows = warehouse.execute(
        f"""SELECT count_if(coalesce(enabled, true) AND (
              lower(ingestion_type) = 'incremental' OR coalesce(auto_cdc_from_snapshot, false))) > 0
            FROM {_config_table(catalog, schema)} WHERE ingestion_group = :group""",
        {"group": ingestion_group},
    )
    return bool(rows) and str(rows[0][0]).lower() == "true"


class JobsApi:
    """The job and pipeline calls the import makes, on the REST API with plain dictionaries."""

    def __init__(self, workspace: Any):
        self.workspace = workspace

    def _do(self, method: str, path: str, **kwargs: Any) -> dict[str, Any]:
        return self.workspace.api_client.do(method, path, **kwargs) or {}

    def find_job(self, name: str, configuration_fqn: str) -> int | None:
        """A job WaterSync generated for this location: same name and configuration_fqn parameter."""
        listed = self._do("GET", "/api/2.2/jobs/list", query={"name": name, "limit": 25}).get("jobs", [])
        for item in listed:
            settings = self._do("GET", "/api/2.2/jobs/get", query={"job_id": item["job_id"]}).get("settings", {})
            parameters = {p.get("name"): p.get("default") for p in settings.get("parameters", [])}
            if settings.get("name") == name and parameters.get("configuration_fqn") == configuration_fqn:
                return item["job_id"]
        return None

    def save_job(self, job_id: int | None, settings: dict[str, Any]) -> tuple[int, str]:
        if job_id:
            self._do("POST", "/api/2.2/jobs/reset", body={"job_id": job_id, "new_settings": settings})
            return job_id, "updated"
        return self._do("POST", "/api/2.2/jobs/create", body=settings)["job_id"], "created"

    def find_pipeline(self, name: str) -> str | None:
        prefix = name.replace("'", "''")
        listed = self._do("GET", "/api/2.0/pipelines", query={"filter": f"name LIKE '{prefix}'", "max_results": 100})
        return next((p["pipeline_id"] for p in listed.get("statuses", []) if p.get("name") == name), None)

    def save_pipeline(self, pipeline_id: str | None, settings: dict[str, Any]) -> str:
        if pipeline_id:
            self._do("PUT", f"/api/2.0/pipelines/{pipeline_id}", body={**settings, "pipeline_id": pipeline_id})
            return pipeline_id
        return self._do("POST", "/api/2.0/pipelines", body=settings)["pipeline_id"]

    def write_workspace_file(self, path: str, content: str, notebook: bool) -> None:
        self._do("POST", "/api/2.0/workspace/mkdirs", body={"path": GENERATED_PIPELINES_DIR})
        self._do(
            "POST",
            "/api/2.0/workspace/import",
            body={
                "path": path,
                "format": "SOURCE" if notebook else "RAW",
                **({"language": "PYTHON"} if notebook else {}),
                "overwrite": True,
                "content": base64.b64encode(content.encode()).decode(),
            },
        )


def ensure_cdc_pipeline(api: JobsApi, catalog: str, schema: str, job: dict[str, Any]) -> str:
    """The group's CDC pipeline, created or updated in place; a pipeline ID in the file is used as it is."""
    if job["cdcPipelineId"]:
        return job["cdcPipelineId"]
    if job["notebooks"]["source"] == "WORKSPACE" and job["pipelineCompute"]["mode"] == "SERVERLESS":
        # The deployed files already hold the entry point.
        library = {"file": {"path": f"{job['notebooks']['basePath']}/src/watersync/pipeline_bootstrap.py"}}
    elif job["pipelineCompute"]["mode"] == "CLASSIC":
        # Classic clusters can't take environment dependencies: a notebook per group installs the package first.
        group = job["ingestionGroup"]
        slug = re.sub(r"[^A-Za-z0-9_-]", "_", group)[:60]
        digest = hashlib.sha256(group.encode()).hexdigest()[:8]
        path = f"{GENERATED_PIPELINES_DIR}/cdc_pipeline_bootstrap_classic_{slug}_{digest}"
        dependency = watersync_requirement(job["notebooks"], job["library"])
        # Validation already rejects these; the requirement must never end the %pip line or its quotes.
        if not _PIP_TOKEN.match(dependency):
            raise ValueError(f"Refusing to write a %pip line for {dependency!r}")
        body = PIPELINE_BOOTSTRAP.replace("from __future__ import annotations\n\n", "")
        api.write_workspace_file(path, f'# Databricks notebook source\n# MAGIC %pip install "{dependency}"\n\n# COMMAND ----------\n\n{body}', notebook=True)
        library = {"notebook": {"path": f"/Workspace{path}"}}
    else:
        path = f"{GENERATED_PIPELINES_DIR}/cdc_pipeline_bootstrap.py"
        api.write_workspace_file(path, PIPELINE_BOOTSTRAP, notebook=False)
        library = {"file": {"path": f"/Workspace{path}"}}
    settings = cdc_pipeline_settings(catalog, schema, job, library)
    return api.save_pipeline(api.find_pipeline(settings["name"]), settings)


def import_configuration(
    workspace: Any,
    warehouse_id: str,
    catalog: str,
    schema: str,
    validated: ValidatedFile,
    *,
    replace_groups: bool = False,
    deploy_jobs: bool = False,
    log: Callable[[str], None] = print,
) -> bool:
    """Writes the configs, then creates or updates the jobs; returns whether every job succeeded."""
    # The same rule as the app: the names are also put into SQL that UnityCatalogSetup builds.
    for flag, name in (("--catalog", catalog), ("--schema", schema)):
        if not _IDENTIFIER.match(name):
            raise ValueError(f"{flag} must be a plain name (letters, digits and _), got {name!r}")
    warehouse = Warehouse(workspace, warehouse_id)
    ensure_tables(warehouse, catalog, schema)
    upsert_configs(warehouse, catalog, schema, validated.configs)
    log(f"Imported {len(validated.configs)} source tables into {catalog}.{schema}")
    if replace_groups:
        removed = remove_unlisted_configs(warehouse, catalog, schema, validated.configs)
        if removed:
            log(f"Removed {removed} source tables no longer in the file")
    if not deploy_jobs:
        return True

    api = JobsApi(workspace)
    configuration_fqn = f"{catalog}.{schema}.jdbc_ingestion_config"
    succeeded = True
    # One at a time and independently: a failing job doesn't stop the others.
    for job in validated.jobs:
        group = job["ingestionGroup"]
        try:
            pipeline_id = (
                ensure_cdc_pipeline(api, catalog, schema, job)
                if group_needs_cdc_pipeline(warehouse, catalog, schema, group)
                else None
            )
            settings = ingestion_job_settings(catalog, schema, job, pipeline_id)
            job_id, action = api.save_job(api.find_job(settings["name"], configuration_fqn), settings)
            pipeline = f", CDC pipeline {pipeline_id}" if pipeline_id else ""
            log(f"Job for {group}: {action} job {job_id}{pipeline}")
        except Exception as error:  # noqa: BLE001 - reported per job, the import goes on
            succeeded = False
            log(f"Job for {group} failed: {error}")
    for job in validated.row_check_jobs:
        label = f"{job['checkMode']} row check job for {job['ingestionGroup'] or 'the location'}"
        try:
            settings = row_check_job_settings(catalog, schema, job)
            job_id, action = api.save_job(api.find_job(settings["name"], configuration_fqn), settings)
            log(f"{label[0].upper()}{label[1:]}: {action} job {job_id} ({settings['name']})")
        except Exception as error:  # noqa: BLE001
            succeeded = False
            log(f"{label[0].upper()}{label[1:]} failed: {error}")
    return succeeded


def bundle_wheel(workspace: Any, bundle_root: str) -> str:
    """The watersync wheel a bundle deployment uploaded to <root>/artifacts/.internal."""
    folder = f"{bundle_root.rstrip('/')}/artifacts/.internal"
    listed = workspace.api_client.do("GET", "/api/2.0/workspace/list", query={"path": folder}).get("objects", [])
    wheels = sorted(
        (item for item in listed if Path(item["path"]).name.startswith("watersync-") and item["path"].endswith(".whl")),
        key=lambda item: item.get("modified_at", 0),
    )
    if not wheels:
        raise ValueError(f"No watersync wheel in {folder}: run databricks bundle deploy first")
    return wheels[-1]["path"]
