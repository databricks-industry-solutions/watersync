#!/usr/bin/env python3
"""Export or import a WaterSync configuration file through the WaterSync control-plane app.

Meant for deployment pipelines, e.g. export from dev, commit the file, import it into prod:

    python scripts/watersync_config.py export --catalog dev --schema watersync --output watersync.json
    python scripts/watersync_config.py import --catalog prod --schema watersync --file watersync.json --deploy-jobs

The app does the work (validation, config upsert, job and CDC pipeline creation) exactly as from its UI, so the
script only needs `databricks-sdk`. Authentication uses the standard Databricks unified auth (`--profile`, or
DATABRICKS_HOST with DATABRICKS_CLIENT_ID / DATABRICKS_CLIENT_SECRET for a service principal). Databricks Apps
only accept OAuth tokens, not personal access tokens, and the caller needs CAN USE on the app.

Exit codes: 0 on success, 1 when the request fails or any job could not be deployed.
"""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path
from typing import Any

import requests
from databricks.sdk import WorkspaceClient

# Deploying a job can provision a CDC pipeline, which takes a while.
REQUEST_TIMEOUT_SECONDS = 300


class RequestFailed(Exception):
    pass


class WaterSyncApp:
    def __init__(self, base_url: str, workspace: WorkspaceClient, session: requests.Session | None = None) -> None:
        if workspace.config.auth_type == "pat":
            raise RequestFailed(
                "Databricks Apps do not accept personal access tokens; use OAuth "
                "(`databricks auth login` or a service principal client ID and secret)"
            )
        self.base_url = base_url.rstrip("/")
        self.workspace = workspace
        self.session = session or requests.Session()

    def request(self, method: str, path: str, *, params: dict | None = None, body: Any = None) -> Any:
        response = self.session.request(
            method,
            f"{self.base_url}{path}",
            params=params,
            json=body,
            # Tokens are short-lived, so take fresh headers per call.
            headers=self.workspace.config.authenticate(),
            timeout=REQUEST_TIMEOUT_SECONDS,
            # An unauthenticated call is redirected to the login page instead of failing.
            allow_redirects=False,
        )
        if 300 <= response.status_code < 400 or response.status_code in (401, 403):
            raise RequestFailed(
                f"The app refused the request (HTTP {response.status_code}); check that the caller has CAN USE on it"
            )
        try:
            payload = response.json()
        except ValueError:
            raise RequestFailed(f"Unexpected non-JSON response from {path} (HTTP {response.status_code})") from None
        if not response.ok:
            request_id = f" [request {payload['requestId']}]" if payload.get("requestId") else ""
            raise RequestFailed(f"{payload.get('error') or f'HTTP {response.status_code}'}{request_id}")
        return payload


def resolve_app_url(workspace: WorkspaceClient, app_url: str | None, app_name: str) -> str:
    if app_url:
        return app_url
    url = workspace.apps.get(app_name).url
    if not url:
        raise RequestFailed(f"App {app_name} has no URL yet; is it deployed and running?")
    return url


def export_config(app: WaterSyncApp, catalog: str, schema: str) -> dict:
    return app.request("GET", "/api/export", params={"catalog": catalog, "schema": schema})


def import_config(
    app: WaterSyncApp,
    catalog: str,
    schema: str,
    file: dict,
    *,
    replace_groups: bool = False,
    deploy_jobs: bool = False,
    dry_run: bool = False,
    log=print,
) -> bool:
    """Imports the file and returns whether everything succeeded.

    The app validates the whole file (configs and jobs) before writing anything. Jobs are then deployed one
    request per group, so a large file does not run into request timeouts and progress is visible.
    """
    result = app.request(
        "POST",
        "/api/import",
        body={
            "catalog": catalog,
            "schema": schema,
            "file": file,
            "replaceGroups": replace_groups,
            "deployJobs": False,
            "dryRun": dry_run,
        },
    )
    groups = ", ".join(f"{group['ingestionGroup']} ({group['configCount']})" for group in result["groups"])
    if dry_run:
        log(f"Valid: {result['configCount']} source tables, job settings for {result['jobCount']} groups: {groups}")
        return True
    log(f"Imported {result['configCount']} source tables into {catalog}.{schema}: {groups}")
    if result.get("removedCount"):
        log(f"Removed {result['removedCount']} source tables no longer in the file")
    if not deploy_jobs:
        return True
    succeeded = True
    for job in file.get("jobs", []):
        group = job.get("ingestionGroup")
        try:
            saved = app.request("POST", "/api/jobs", body={**job, "catalog": catalog, "schema": schema})
            pipeline = f", CDC pipeline {saved['pipelineId']}" if saved.get("pipelineId") else ""
            checks = "".join(
                f", {mode} row check job {check.get('jobId')} {check.get('action')}"
                for mode, check in (saved.get("rowCheckJobs") or {}).items()
            )
            log(f"Job for {group}: {saved['action']} job {saved['jobId']}{pipeline}{checks}")
        except RequestFailed as error:
            succeeded = False
            log(f"Job for {group} failed: {error}")
    return succeeded


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Export or import a WaterSync configuration file")
    parser.add_argument("--profile", help="Databricks CLI profile (default: unified auth from the environment)")
    target = parser.add_mutually_exclusive_group()
    target.add_argument("--app-name", default="watersync-control-plane", help="Name of the WaterSync app")
    target.add_argument("--app-url", help="URL of the WaterSync app, instead of looking it up by name")
    commands = parser.add_subparsers(dest="command", required=True)

    export_parser = commands.add_parser("export", help="Write a location's configuration to a JSON file")
    import_parser = commands.add_parser("import", help="Load a JSON file into a location")
    for command in (export_parser, import_parser):
        command.add_argument("--catalog", required=True)
        command.add_argument("--schema", required=True)
    export_parser.add_argument("--output", help="File to write (default: stdout)")
    import_parser.add_argument("--file", required=True, type=Path)
    import_parser.add_argument(
        "--replace-groups",
        action="store_true",
        help="Delete source tables of the imported groups that are not in the file",
    )
    import_parser.add_argument(
        "--deploy-jobs",
        action="store_true",
        help="Create or update the job (and CDC pipeline) of every group with job settings in the file",
    )
    import_parser.add_argument("--dry-run", action="store_true", help="Only validate the file")
    args = parser.parse_args(argv)

    try:
        workspace = WorkspaceClient(profile=args.profile)
        app = WaterSyncApp(resolve_app_url(workspace, args.app_url, args.app_name), workspace)
        if args.command == "export":
            content = json.dumps(export_config(app, args.catalog, args.schema), indent=2) + "\n"
            if args.output:
                Path(args.output).write_text(content)
            else:
                sys.stdout.write(content)
            return 0
        succeeded = import_config(
            app,
            args.catalog,
            args.schema,
            json.loads(args.file.read_text()),
            replace_groups=args.replace_groups,
            deploy_jobs=args.deploy_jobs,
            dry_run=args.dry_run,
        )
        return 0 if succeeded else 1
    except (RequestFailed, requests.RequestException, json.JSONDecodeError) as error:
        print(f"error: {error}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    sys.exit(main())
