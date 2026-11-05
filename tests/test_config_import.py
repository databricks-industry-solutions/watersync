from __future__ import annotations

import copy
import json
import types
import unittest
from pathlib import Path

from watersync.config_import import (
    ImportValidationError,
    bundle_wheel,
    import_configuration,
    ingestion_job_settings,
    row_check_job_settings,
    use_bundle,
    validate_file,
)

SAMPLE = json.loads((Path(__file__).resolve().parents[1] / "examples" / "watersync-config.json").read_text())
BUNDLE = "/Workspace/Users/me/.bundle/watersync/dev"
WHEEL = f"{BUNDLE}/artifacts/.internal/watersync-0.4.0-py3-none-any.whl"


def issues_of(file: dict) -> list[str]:
    try:
        validate_file(file)
    except ImportValidationError as error:
        return [path for path, _ in error.issues]
    return []


class FakeWorkspace:
    """Records the SQL and REST calls of an import; the config table has incremental rows for every group."""

    def __init__(self) -> None:
        self.statements: list[tuple[str, dict]] = []
        self.calls: list[tuple[str, str, dict]] = []
        self.jobs: dict[int, dict] = {}
        self.pipelines: dict[str, dict] = {}
        workspace = self

        class StatementExecution:
            def execute_statement(self, statement, warehouse_id, parameters, wait_timeout):
                from databricks.sdk.service.sql import StatementState

                workspace.statements.append((statement, {p.name: p.value for p in parameters}))
                rows = [["true"]] if "count_if" in statement else [["2"]] if "DELETE" in statement else []
                return types.SimpleNamespace(
                    status=types.SimpleNamespace(state=StatementState.SUCCEEDED, error=None),
                    result=types.SimpleNamespace(data_array=rows),
                    statement_id="s",
                )

        self.statement_execution = StatementExecution()
        self.api_client = types.SimpleNamespace(do=self.do)

    def do(self, method, path, query=None, body=None):
        self.calls.append((method, path, body or query or {}))
        if path == "/api/2.2/jobs/list":
            return {"jobs": [{"job_id": i} for i, s in self.jobs.items() if s["name"] == query["name"]]}
        if path == "/api/2.2/jobs/get":
            return {"settings": self.jobs[query["job_id"]]}
        if path == "/api/2.2/jobs/create":
            job_id = 100 + len(self.jobs)
            self.jobs[job_id] = body
            return {"job_id": job_id}
        if path == "/api/2.2/jobs/reset":
            self.jobs[body["job_id"]] = body["new_settings"]
            return {}
        if path == "/api/2.0/pipelines" and method == "GET":
            return {"statuses": [{"pipeline_id": i, "name": s["name"]} for i, s in self.pipelines.items()]}
        if path == "/api/2.0/pipelines" and method == "POST":
            pipeline_id = f"p{len(self.pipelines)}"
            self.pipelines[pipeline_id] = body
            return {"pipeline_id": pipeline_id}
        if path.startswith("/api/2.0/pipelines/"):
            self.pipelines[path.rsplit("/", 1)[-1]] = body
            return {}
        if path == "/api/2.0/workspace/list":
            return {"objects": [{"path": WHEEL, "modified_at": 2}, {"path": f"{BUNDLE}/artifacts/.internal/other.whl"}]}
        return {}


class ValidationTest(unittest.TestCase):
    def test_sample_file_is_valid(self) -> None:
        validated = validate_file(SAMPLE)
        self.assertEqual((len(validated.configs), len(validated.jobs), len(validated.row_check_jobs)), (10, 4, 3))

    def test_older_flat_git_fields_are_a_branch(self) -> None:
        file = copy.deepcopy(SAMPLE)
        job = file["jobs"][0]
        del job["notebooks"], job["library"]
        job.update(gitUrl="https://github.com/org/watersync", gitBranch="dev", libraryPath=None)
        validated = validate_file(file)
        self.assertEqual(validated.jobs[0]["notebooks"]["reference"], "dev")
        self.assertEqual(validated.jobs[0]["library"], {"source": "GIT"})

    def test_reports_every_problem_with_its_path(self) -> None:
        file = copy.deepcopy(SAMPLE)
        file["configs"][0]["ingestionType"] = "full"  # EPIC CSA needs incremental
        file["configs"].append(copy.deepcopy(file["configs"][1]))  # duplicate table
        file["jobs"][0]["notebooks"]["gitUrl"] = "https://git.example.org/x/y"  # unknown host, no provider
        file["jobs"][1]["library"] = {"source": "WHEEL", "path": "/tmp/watersync.whl"}
        file["rowCheckJobs"][0]["runAs"] = {"mode": "USER", "principal": "not-an-email"}
        self.assertEqual(
            issues_of(file),
            [
                "configs.0.epicCsaEnabled",
                "jobs.0.notebooks.gitProvider",
                "jobs.1.library.path",
                "rowCheckJobs.0.runAs.principal",
                "configs.10",
            ],
        )

    def test_jobs_without_compute_run_performance_optimized(self) -> None:
        file = copy.deepcopy(SAMPLE)
        del file["jobs"][0]["compute"], file["rowCheckJobs"][0]["compute"]
        validated = validate_file(file)
        self.assertEqual(validated.jobs[0]["compute"]["performanceTarget"], "PERFORMANCE_OPTIMIZED")
        self.assertEqual(validated.row_check_jobs[0]["compute"]["performanceTarget"], "PERFORMANCE_OPTIMIZED")

    def test_rejects_values_that_could_inject_code_or_sql(self) -> None:
        file = copy.deepcopy(SAMPLE)
        jobs = file["jobs"]
        jobs[0]["notebooks"]["gitUrl"] = "https://github.com/org/watersync\nimport os; os.system('id')\n#"
        jobs[1]["notebooks"] = {**jobs[1]["notebooks"], "source": "GIT"}
        jobs[1]["notebooks"]["gitUrl"] = 'https://github.com/org/watersync"; import os #'
        jobs[2]["library"] = {"source": "PIP", "requirement": "watersync --extra-index-url https://example.org/simple"}
        jobs[3]["library"] = {"source": "PIP", "requirement": "-r https://example.org/requirements.txt"}
        sp = next(i for i, c in enumerate(file["configs"]) if c.get("epicChangeMethod") == "stored_procedure")
        file["configs"][sp]["sourceTableName"] = "dbo.PAT_ENC; DROP TABLE x"
        found = issues_of(file)
        for path in ("jobs.0.notebooks.gitUrl", "jobs.1.notebooks.gitUrl", "jobs.2.library.requirement",
                     "jobs.3.library.requirement", f"configs.{sp}.sourceTableName"):
            self.assertIn(path, found)

    def test_catalog_and_schema_must_be_plain_names(self) -> None:
        validated = validate_file(SAMPLE)
        with self.assertRaises(ValueError):
            import_configuration(FakeWorkspace(), "wh", "dev`; DROP TABLE x; --", "watersync", validated, log=lambda _: None)

    def test_git_install_needs_git_notebooks(self) -> None:
        file = copy.deepcopy(SAMPLE)
        file["jobs"][0]["notebooks"] = {"source": "WORKSPACE", "basePath": "/Workspace/x"}
        self.assertEqual(issues_of(file), ["jobs.0.library.source"])


class JobSettingsTest(unittest.TestCase):
    def test_bundle_jobs_run_its_files_and_install_its_wheel(self) -> None:
        validated = validate_file(SAMPLE)
        use_bundle(validated, f"{BUNDLE}/files/", WHEEL)
        settings = ingestion_job_settings("dev", "watersync", validated.jobs[0], "pipeline-1")
        self.assertNotIn("git_source", settings)
        self.assertEqual(settings["environments"][0]["spec"]["dependencies"], [WHEEL])
        paths = [
            (task.get("notebook_task") or task.get("for_each_task", {}).get("task", {}).get("notebook_task") or {})
            for task in settings["tasks"]
        ]
        self.assertEqual(
            [(p.get("notebook_path"), p.get("source")) for p in paths],
            [
                (f"{BUNDLE}/files/notebooks/Task - Plan Configs", "WORKSPACE"),
                (f"{BUNDLE}/files/notebooks/Task - Run Ingestion", "WORKSPACE"),
                (None, None),
                (f"{BUNDLE}/files/src/watersync/utils/selective_pipeline_refresh", "WORKSPACE"),
                (None, None),
            ],
        )
        gate, refresh, pipeline = settings["tasks"][2:]
        self.assertEqual(gate["condition_task"]["left"], "{{tasks.ingestion_configs.values.full_refresh_count}}")
        self.assertEqual(refresh["depends_on"], [{"task_key": "has_full_refresh_targets", "outcome": "true"}])
        self.assertEqual((pipeline["run_if"], len(pipeline["depends_on"])), ("NONE_FAILED", 2))
        self.assertEqual(pipeline["pipeline_task"], {"pipeline_id": "pipeline-1", "full_refresh": False})

    def test_git_sources_and_job_clusters(self) -> None:
        validated = validate_file(SAMPLE)
        by_group = {job["ingestionGroup"]: job for job in validated.jobs}
        azure = ingestion_job_settings("dev", "watersync", by_group["clarity_sp"], None)
        self.assertEqual(
            azure["git_source"],
            {"git_url": "https://dev.azure.com/example-org/clarity/_git/watersync", "git_provider": "azureDevOpsServices",
             "git_branch": "main"},
        )
        self.assertEqual([task["task_key"] for task in azure["tasks"]], ["ingestion_configs", "ingestion_worker"])
        tagged = ingestion_job_settings("dev", "watersync", by_group["finance"], None)
        self.assertEqual(tagged["git_source"]["git_tag"], "v0.4.0")
        self.assertEqual(tagged["environments"][0]["spec"]["dependencies"], ["watersync==0.4.0"])
        self.assertEqual(tagged["run_as"], {"user_name": "data.engineer@example.org"})
        github = ingestion_job_settings("dev", "watersync", by_group["clarity_csa"], None)
        self.assertEqual(
            github["environments"][0]["spec"]["dependencies"],
            ["watersync@git+https://github.com/databricks-industry-solutions/watersync.git@main"],
        )
        reference = ingestion_job_settings("dev", "watersync", by_group["clarity_reference"], None)
        self.assertEqual(reference["tasks"][0]["job_cluster_key"], "watersync_cluster")
        self.assertEqual(list(reference["tasks"][0]["libraries"][0]), ["whl"])
        self.assertNotIn("performance_target", reference)

    def test_row_check_job(self) -> None:
        validated = validate_file(SAMPLE)
        location = row_check_job_settings("dev", "watersync", validated.row_check_jobs[0])
        self.assertEqual(location["name"], "[dev.watersync] Row Checks")
        detailed = row_check_job_settings("dev", "watersync", validated.row_check_jobs[1])
        self.assertEqual(detailed["name"], "[clarity_csa] Row Check (detailed)")
        self.assertIn({"name": "range_from", "default": "-7d"}, detailed["parameters"])
        self.assertEqual(detailed["tasks"][0]["notebook_task"]["notebook_path"], "notebooks/Task - Row Check")


class ImportTest(unittest.TestCase):
    def test_writes_configs_then_creates_and_updates_jobs(self) -> None:
        workspace = FakeWorkspace()
        validated = validate_file(SAMPLE)
        use_bundle(validated, f"{BUNDLE}/files", bundle_wheel(workspace, BUNDLE))
        logs: list[str] = []
        self.assertTrue(
            import_configuration(
                workspace, "wh", "dev", "watersync", validated, replace_groups=True, deploy_jobs=True, log=logs.append
            )
        )
        merge, parameters = next(s for s in workspace.statements if s[0].startswith("MERGE"))
        self.assertIn("MERGE INTO `dev`.`watersync`.`jdbc_ingestion_config`", merge)
        rows = json.loads(parameters["rows"])
        self.assertEqual(len(rows), 10)
        # Stored like the app stores them: no watermark column where none is read.
        self.assertIsNone(rows[0]["watermark_column"])
        self.assertIn("Removed 2 source tables no longer in the file", logs)
        self.assertEqual(len(workspace.jobs), 7)
        self.assertEqual(len(workspace.pipelines), 4)
        pipeline = next(iter(workspace.pipelines.values()))
        self.assertEqual(pipeline["environment"]["dependencies"], [WHEEL])
        self.assertEqual(pipeline["libraries"], [{"file": {"path": f"{BUNDLE}/files/src/watersync/pipeline_bootstrap.py"}}])

        # A second import updates every job and pipeline in place.
        import_configuration(workspace, "wh", "dev", "watersync", validated, deploy_jobs=True, log=logs.append)
        self.assertEqual((len(workspace.jobs), len(workspace.pipelines)), (7, 4))
        self.assertTrue(any("updated job" in line for line in logs))


if __name__ == "__main__":
    unittest.main()
