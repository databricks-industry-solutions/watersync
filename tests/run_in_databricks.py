# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
# MAGIC %md
# MAGIC # WaterSync smoke test
# MAGIC
# MAGIC Runs the unit tests in `tests/` against the `watersync` wheel the bundle built, which the job's environment installs.
# MAGIC Deployed as the `demo_workflow` job and run by the repository CI.

# COMMAND ----------

# MAGIC %pip install --quiet --require-hashes --no-deps -r requirements.txt

# COMMAND ----------

dbutils.library.restartPython()

# COMMAND ----------

import os
import sys

import pytest

sys.dont_write_bytecode = True
tests_dir = os.path.abspath(".")
exit_code = pytest.main([tests_dir, "-q", "-p", "no:cacheprovider"])
if exit_code != 0:
    raise RuntimeError(f"watersync unit tests failed (pytest exit code {exit_code})")
