#!/usr/bin/env python3

import argparse
import subprocess


def main() -> None:
    parser = argparse.ArgumentParser(description="Deploy the WaterSync Databricks App")
    parser.add_argument("--profile", required=True, help="Databricks CLI profile")
    parser.add_argument("--app-name", default="watersync-control-plane")
    parser.add_argument(
        "--source-code-path",
        required=True,
        help="Workspace path of app/watersync-control-plane, e.g. /Workspace/Users/<user>/watersync/app/watersync-control-plane",
    )
    args = parser.parse_args()

    subprocess.run(
        [
            "databricks",
            "apps",
            "deploy",
            args.app_name,
            "--source-code-path",
            args.source_code_path,
            "--mode",
            "SNAPSHOT",
            "--profile",
            args.profile,
            "--auto-approve",
        ],
        check=True,
    )

    subprocess.run(
        [
            "databricks",
            "apps",
            "get",
            args.app_name,
            "--profile",
            args.profile,
            "--output",
            "json",
        ],
        check=True,
    )


if __name__ == "__main__":
    main()
