# Databricks notebook source
# MAGIC %md
# MAGIC # jetstream-juniper-mist-connector — Run
# MAGIC
# MAGIC Single entrypoint. All per-instance configuration is set through this
# MAGIC job's parameters (widgets below) — no YAML file inside the repo is read
# MAGIC at runtime. On first run this notebook self-registers the connector
# MAGIC type, manifest, and instance in `metadata_<env>.jetstream`.
# MAGIC
# MAGIC Per-instance parameters (see [connectors/juniper_mist/parameters.yaml](../connectors/juniper_mist/parameters.yaml)):
# MAGIC - `environment` — dev | prod (drives metadata catalog)
# MAGIC - `instance_name`, `destination_catalog`, `destination_schema`
# MAGIC - `secret_scope` — scope holding `juniper_mist_token`
# MAGIC - `juniper_mist_org_id` — organization UUID (not a secret)
# MAGIC - `juniper_mist_base_url` — API base URL (for non-US regions)
# MAGIC - `sla_target_minutes`, `owner_team`
# MAGIC - `rate_limit_qps`, `timeout_seconds`, `retry_max_attempts`
# MAGIC - `lookback_days`, `fan_out_max_workers`
# MAGIC - `site_filter` — comma-separated site name substrings for history fan-out
# MAGIC - `enabled_objects` — CSV of objects to sync (empty = all 25 primary + optional)
# MAGIC - `full_load_interval_hours` — skip full-load objects that succeeded more recently than this (`0` = always run)
# MAGIC
# MAGIC Per-run parameters (set at trigger time):
# MAGIC - `mode` — `run` (default) | `full_resync` | `discover`
# MAGIC - `objects` — comma-separated object names (for `full_resync` only)
# MAGIC - `trigger_type` — auto-detected from Databricks job context (PERIODIC→scheduled, MANUAL→manual, ONE_TIME→api)

# COMMAND ----------

# MAGIC %pip install -q -r ../requirements.txt

# COMMAND ----------

import os
import sys

_HERE = os.path.dirname(os.path.abspath("__file__"))
sys.path.insert(0, os.path.abspath(os.path.join(_HERE, "..")))

# COMMAND ----------

# Per-instance widgets
dbutils.widgets.dropdown("environment", "dev", ["dev", "prod"], "environment")
dbutils.widgets.text("instance_name", "juniper_mist", "instance_name")
dbutils.widgets.text("destination_catalog", "", "destination_catalog")
dbutils.widgets.text("destination_schema", "juniper_mist", "destination_schema")
dbutils.widgets.text("metadata_catalog", "", "metadata_catalog (dev sandbox: raw_dev)")
dbutils.widgets.text("metadata_schema", "jetstream", "metadata_schema (dev sandbox: jetstream_juniper_mist)")
dbutils.widgets.text("secret_scope", "", "secret_scope")
dbutils.widgets.text("juniper_mist_org_id", "", "juniper_mist_org_id")
dbutils.widgets.text("juniper_mist_base_url", "https://api.mist.com/api/v1", "juniper_mist_base_url")
dbutils.widgets.text("sla_target_minutes", "1440", "sla_target_minutes")
dbutils.widgets.text("owner_team", "", "owner_team")
dbutils.widgets.text("rate_limit_qps", "5", "rate_limit_qps")
dbutils.widgets.text("timeout_seconds", "60", "timeout_seconds")
dbutils.widgets.text("retry_max_attempts", "5", "retry_max_attempts")
dbutils.widgets.text("lookback_days", "7", "lookback_days")
dbutils.widgets.text("fan_out_max_workers", "8", "fan_out_max_workers")
dbutils.widgets.text("site_filter", "", "site_filter (CSV substrings, empty = all sites)")
dbutils.widgets.text("enabled_objects", "", "enabled_objects (CSV, empty = all 25 objects)")
dbutils.widgets.text("full_load_interval_hours", "0", "full_load_interval_hours (0 = always run full loads)")

# Per-run widgets
dbutils.widgets.dropdown("mode", "run", ["run", "full_resync", "discover"], "mode")
dbutils.widgets.text("objects", "", "objects (CSV, full_resync only)")
# trigger_type is auto-detected from the Databricks job context tag (jobTriggerType)

# COMMAND ----------

mode = dbutils.widgets.get("mode")
objects_arg = dbutils.widgets.get("objects").strip()
objects = [o.strip() for o in objects_arg.split(",") if o.strip()] or None

# COMMAND ----------

from src.pipeline_orchestrator import run

result = run(
    spark=spark,
    dbutils=dbutils,
    mode=mode,
    objects=objects,
)
print(result)
