# Databricks notebook source
# MAGIC %md
# MAGIC # jetstream-eptura-connector — Run
# MAGIC
# MAGIC Single entrypoint. Every entity is a full snapshot, so each run reads
# MAGIC all five Eptura collections and MERGEs them into
# MAGIC `<destination_catalog>.<destination_schema>.<entity>`.
# MAGIC
# MAGIC On first run this notebook self-registers the connector type, manifest,
# MAGIC and instance in `metadata_<env>.<metadata_schema>`.
# MAGIC
# MAGIC Per-instance parameters (see [connectors/eptura/parameters.yaml](../connectors/eptura/parameters.yaml)):
# MAGIC - `environment`, `instance_name`, `destination_catalog`, `destination_schema`
# MAGIC - `metadata_catalog`, `metadata_schema`
# MAGIC - `secret_scope` (keys: eptura_host, eptura_username, eptura_password, eptura_client_id)
# MAGIC - `api_base_path`, `redirect_uri`, `oauth_scope`, `page_size`
# MAGIC - `sla_target_minutes`, `owner_team`, `rate_limit_qps`, `timeout_seconds`,
# MAGIC   `retry_max_attempts`, `tombstone_guard_pct`
# MAGIC - `enabled_objects`
# MAGIC
# MAGIC Per-run parameters:
# MAGIC - `objects` — comma-separated subset to re-sync on this run only (empty = full enabled plan)
# MAGIC
# MAGIC Runtime context (job_id, run_id, trigger_type, triggered_by) is auto-detected
# MAGIC from the Databricks notebook context tags — no task parameters needed.

# COMMAND ----------

# MAGIC %pip install -q -r ../requirements.txt

# COMMAND ----------

import os
import sys

# Databricks runs a notebook with its own folder as the working directory, so
# the repo root is one level up. Resolved explicitly because notebooks have no
# __file__.
sys.path.insert(0, os.path.abspath(os.path.join(os.getcwd(), "..")))

# COMMAND ----------

dbutils.widgets.dropdown("environment", "dev", ["dev", "prod"], "environment")
dbutils.widgets.text("instance_name", "", "instance_name")
dbutils.widgets.text("destination_catalog", "", "destination_catalog")
dbutils.widgets.text("destination_schema", "eptura", "destination_schema")
dbutils.widgets.text("metadata_catalog", "", "metadata_catalog (dev sandbox: raw_dev)")
dbutils.widgets.text("metadata_schema", "jetstream", "metadata_schema (dev sandbox: jetstream_eptura)")
dbutils.widgets.text("secret_scope", "", "secret_scope")
dbutils.widgets.text("api_base_path", "/external/api/rest/v2", "api_base_path")
dbutils.widgets.text("redirect_uri", "", "redirect_uri")
dbutils.widgets.text("oauth_scope", "email", "oauth_scope")
dbutils.widgets.text("page_size", "1000", "page_size")
dbutils.widgets.text("sla_target_minutes", "1440", "sla_target_minutes")
dbutils.widgets.text("owner_team", "", "owner_team")
dbutils.widgets.text("rate_limit_qps", "10", "rate_limit_qps")
dbutils.widgets.text("timeout_seconds", "60", "timeout_seconds")
dbutils.widgets.text("retry_max_attempts", "5", "retry_max_attempts")
dbutils.widgets.text("tombstone_guard_pct", "20", "tombstone_guard_pct")
dbutils.widgets.text("enabled_objects", "", "enabled_objects (CSV, empty = all)")

# Per-run: narrow the sync to a subset of the enabled plan.
dbutils.widgets.text("objects", "", "objects (CSV, this run only)")
# trigger_type is auto-detected from the Databricks job context tags
# (PERIODIC→scheduled, ONE_TIME→api, MANUAL→manual). No widget needed.

# COMMAND ----------

objects = [o.strip() for o in dbutils.widgets.get("objects").split(",") if o.strip()] or None

# COMMAND ----------

from src.pipeline_orchestrator import run

result = run(
    spark=spark,
    dbutils=dbutils,
    objects=objects,
)
print(result)
