# Databricks notebook source
# MAGIC %md
# MAGIC # WaterSync smoke test
# MAGIC
# MAGIC Installs the `watersync` package from this bundle and runs the unit tests in `tests/`.
# MAGIC Deployed as the `demo_workflow` job and run by the repository CI.

# COMMAND ----------

# MAGIC %pip install --quiet .. pytest

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
