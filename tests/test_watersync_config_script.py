from __future__ import annotations

import importlib.util
import types
import unittest
from pathlib import Path

# The script lives outside the package so deployment pipelines can run it without pyspark.
_spec = importlib.util.spec_from_file_location(
    "watersync_config", Path(__file__).resolve().parents[1] / "scripts" / "watersync_config.py"
)
watersync_config = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(watersync_config)


class FakeResponse:
    def __init__(self, status_code: int, payload=None) -> None:
        self.status_code = status_code
        self.ok = 200 <= status_code < 300
        self._payload = payload

    def json(self):
        if self._payload is None:
            raise ValueError("no JSON")
        return self._payload


class FakeSession:
    def __init__(self, responses: dict[str, list[FakeResponse]]) -> None:
        self.responses = responses
        self.calls: list[tuple[str, str, object]] = []

    def request(self, method, url, *, params, json, headers, timeout, allow_redirects):
        path = url.removeprefix("https://app.example.com")
        self.calls.append((method, path, json))
        return self.responses[path].pop(0)


def fake_workspace(auth_type: str = "oauth-m2m"):
    config = types.SimpleNamespace(auth_type=auth_type, authenticate=lambda: {"Authorization": "Bearer t"})
    return types.SimpleNamespace(config=config)


IMPORTED = {"configCount": 2, "jobCount": 2, "removedCount": 0, "groups": [
    {"ingestionGroup": "hr", "configCount": 1, "hasJobSettings": True},
    {"ingestionGroup": "sales", "configCount": 1, "hasJobSettings": True},
]}
FILE = {"format": "watersync-config", "version": 1, "configs": [{}, {}], "jobs": [
    {"ingestionGroup": "hr", "gitUrl": "https://github.com/x/y"},
    {"ingestionGroup": "sales", "gitUrl": "https://github.com/x/y"},
]}


class WaterSyncConfigScriptTest(unittest.TestCase):
    def app(self, session: FakeSession, auth_type: str = "oauth-m2m"):
        return watersync_config.WaterSyncApp("https://app.example.com/", fake_workspace(auth_type), session)

    def test_import_writes_configs_then_deploys_each_job_into_the_target_location(self) -> None:
        session = FakeSession({
            "/api/import": [FakeResponse(200, IMPORTED)],
            "/api/jobs": [
                FakeResponse(200, {"jobId": 1, "pipelineId": "p", "action": "created"}),
                FakeResponse(200, {"jobId": 2, "action": "updated"}),
            ],
            "/api/row-checks/job": [
                FakeResponse(200, {"jobId": 3, "action": "created", "name": "[sales] Row Check (basic)"}),
            ],
        })
        logs: list[str] = []
        file = {**FILE, "rowCheckJobs": [{"ingestionGroup": "sales", "checkMode": "basic"}]}
        succeeded = watersync_config.import_config(
            self.app(session), "prod", "ws", file, replace_groups=True, deploy_jobs=True, log=logs.append
        )
        self.assertTrue(succeeded)
        _, _, import_body = session.calls[0]
        self.assertEqual(import_body["catalog"], "prod")
        self.assertTrue(import_body["replaceGroups"])
        # Jobs are deployed by separate requests, not inside the import.
        self.assertFalse(import_body["deployJobs"])
        job_bodies = [body for _, path, body in session.calls if path == "/api/jobs"]
        self.assertEqual([body["ingestionGroup"] for body in job_bodies], ["hr", "sales"])
        self.assertTrue(all(body["catalog"] == "prod" and body["schema"] == "ws" for body in job_bodies))
        self.assertIn("Job for hr: created job 1, CDC pipeline p", logs)
        self.assertIn("Job for sales: updated job 2", logs)
        check_bodies = [body for _, path, body in session.calls if path == "/api/row-checks/job"]
        self.assertEqual(check_bodies, [{"ingestionGroup": "sales", "checkMode": "basic", "catalog": "prod", "schema": "ws"}])
        self.assertIn("Basic row check job for sales: created job 3 ([sales] Row Check (basic))", logs)

    def test_a_failing_job_does_not_stop_the_others_but_fails_the_import(self) -> None:
        session = FakeSession({
            "/api/import": [FakeResponse(200, IMPORTED)],
            "/api/jobs": [
                FakeResponse(500, {"error": "PERMISSION_DENIED", "requestId": "req_1"}),
                FakeResponse(200, {"jobId": 2, "action": "updated"}),
            ],
        })
        logs: list[str] = []
        succeeded = watersync_config.import_config(
            self.app(session), "prod", "ws", FILE, deploy_jobs=True, log=logs.append
        )
        self.assertFalse(succeeded)
        self.assertIn("Job for hr failed: PERMISSION_DENIED [request req_1]", logs)
        self.assertIn("Job for sales: updated job 2", logs)

    def test_dry_run_only_validates(self) -> None:
        session = FakeSession({"/api/import": [FakeResponse(200, {**IMPORTED, "dryRun": True})]})
        self.assertTrue(
            watersync_config.import_config(self.app(session), "prod", "ws", FILE, dry_run=True, deploy_jobs=True, log=lambda _: None)
        )
        self.assertEqual(len(session.calls), 1)
        self.assertTrue(session.calls[0][2]["dryRun"])

    def test_validation_errors_are_raised_with_the_server_message(self) -> None:
        session = FakeSession({"/api/import": [FakeResponse(400, {"error": "file.configs.0.keyColumns: required"})]})
        with self.assertRaisesRegex(watersync_config.RequestFailed, "file.configs.0.keyColumns"):
            watersync_config.import_config(self.app(session), "prod", "ws", FILE, log=lambda _: None)

    def test_login_redirect_is_reported_as_missing_access(self) -> None:
        session = FakeSession({"/api/export": [FakeResponse(302)]})
        with self.assertRaisesRegex(watersync_config.RequestFailed, "CAN USE"):
            watersync_config.export_config(self.app(session), "dev", "ws")

    def test_personal_access_tokens_are_rejected_up_front(self) -> None:
        with self.assertRaisesRegex(watersync_config.RequestFailed, "personal access tokens"):
            self.app(FakeSession({}), auth_type="pat")


if __name__ == "__main__":
    unittest.main()
