from __future__ import annotations

import sys
import types
import unittest

delta_tables = types.ModuleType("delta.tables")
delta_tables.DeltaTable = object
sys.modules.setdefault("delta.tables", delta_tables)

from databricks.sdk.service.jobs import GitProvider

from watersync.models import JobProvisioningSettings
from watersync.utils.create_ingestion_job import IngestionJobProvisioner, git_provider_for, without_user_info


class GitSourceTest(unittest.TestCase):
    def test_provider_comes_from_the_repository_host(self) -> None:
        for url, provider in [
            ("https://github.com/databricks-industry-solutions/watersync", GitProvider.GIT_HUB),
            ("https://dev.azure.com/org/project/_git/watersync", GitProvider.AZURE_DEV_OPS_SERVICES),
            ("https://org.visualstudio.com/project/_git/watersync", GitProvider.AZURE_DEV_OPS_SERVICES),
            ("https://gitlab.com/org/watersync", GitProvider.GIT_LAB),
            ("https://bitbucket.org/org/watersync", GitProvider.BITBUCKET_CLOUD),
            ("https://git-codecommit.us-east-1.amazonaws.com/v1/repos/watersync", GitProvider.AWS_CODE_COMMIT),
        ]:
            self.assertEqual(git_provider_for(url), provider, url)
        with self.assertRaises(ValueError):
            git_provider_for("https://git.example.org/org/watersync")

    def test_user_name_is_dropped_from_azure_devops_clone_urls(self) -> None:
        self.assertEqual(
            without_user_info("https://UKHS@dev.azure.com/UKHS/Data%20Management/_git/watersync"),
            "https://dev.azure.com/UKHS/Data%20Management/_git/watersync",
        )
        self.assertEqual(
            without_user_info("https://github.com/org/watersync"), "https://github.com/org/watersync"
        )

    def test_job_runs_its_notebooks_from_an_azure_devops_repository(self) -> None:
        settings = JobProvisioningSettings(
            ingestion_group="epic",
            configuration_fqn="main.watersync.jdbc_ingestion_config",
            watermark_fqn="main.watersync.jdbc_ingestion_watermark",
            planner_notebook_path="notebooks/Task - Plan Configs",
            worker_notebook_path="notebooks/Task - Run Ingestion",
            wheel_uri="/Volumes/main/watersync/libs/watersync-0.4.0-py3-none-any.whl",
            git_url="https://UKHS@dev.azure.com/UKHS/Data%20Management/_git/watersync",
        )
        job = IngestionJobProvisioner(workspace_client=object()).build_job_settings(settings, "pipeline-id")
        self.assertEqual(job.git_source.git_url, "https://dev.azure.com/UKHS/Data%20Management/_git/watersync")
        self.assertEqual(job.git_source.git_provider, GitProvider.AZURE_DEV_OPS_SERVICES)


    def _settings(self, **overrides) -> JobProvisioningSettings:
        return JobProvisioningSettings(**{
            "ingestion_group": "epic",
            "configuration_fqn": "main.watersync.jdbc_ingestion_config",
            "watermark_fqn": "main.watersync.jdbc_ingestion_watermark",
            "planner_notebook_path": "notebooks/Task - Plan Configs",
            "worker_notebook_path": "notebooks/Task - Run Ingestion",
            "wheel_uri": "watersync==0.4.0",
            "git_url": "https://github.com/databricks-industry-solutions/watersync",
            **overrides,
        })

    def test_tag_or_commit_pins_the_notebooks(self) -> None:
        provisioner = IngestionJobProvisioner(workspace_client=object())
        tagged = provisioner.build_job_settings(self._settings(git_tag="v0.4.0"), "p").git_source
        self.assertEqual((tagged.git_tag, tagged.git_branch, tagged.git_commit), ("v0.4.0", None, None))
        commit = provisioner.build_job_settings(self._settings(git_commit="bf24af5"), "p").git_source
        self.assertEqual((commit.git_commit, commit.git_branch), ("bf24af5", None))
        with self.assertRaises(ValueError):
            provisioner.build_job_settings(self._settings(git_tag="v1", git_commit="abc1234"), "p")

    def test_self_hosted_server_takes_its_provider(self) -> None:
        settings = self._settings(git_url="https://git.example.org/data/watersync", git_provider="gitHubEnterprise")
        source = IngestionJobProvisioner(workspace_client=object()).build_job_settings(settings, "p").git_source
        self.assertEqual(source.git_provider, GitProvider.GIT_HUB_ENTERPRISE)
        with self.assertRaises(ValueError):
            git_provider_for("https://git.example.org/data/watersync")


if __name__ == "__main__":
    unittest.main()
