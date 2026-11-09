# Databricks notebook source
# MAGIC %md
# MAGIC # jetstream-zoom-connector — Run
# MAGIC
# MAGIC Single entrypoint. All per-instance configuration is set through this
# MAGIC job's parameters (widgets below) — no YAML file inside the repo is read
# MAGIC at runtime. On first run this notebook self-registers the connector
# MAGIC type, manifest, and instance in `metadata_<env>.jetstream`.
# MAGIC
# MAGIC Per-instance parameters (see [connectors/zoom/parameters.yaml](../connectors/zoom/parameters.yaml)):
# MAGIC - `environment` — dev | prod (drives metadata catalog)
# MAGIC - `instance_name`, `destination_catalog`, `destination_schema`
# MAGIC - `secret_scope`
# MAGIC - `sla_target_minutes`, `owner_team`
# MAGIC - `zoom_base_url`, `zoom_token_url`, `rate_limit_qps`, `timeout_seconds`, `retry_max_attempts`
# MAGIC - `enabled_objects`, `optional_objects_enabled`
# MAGIC
# MAGIC Per-run parameters (set at trigger time):
# MAGIC - `mode` — `run` (default) or `full_resync`
# MAGIC - `objects` — comma-separated names (only for `full_resync`)
# MAGIC - `trigger_type` — `scheduled` | `manual` | `api` | `backfill`

# COMMAND ----------

# MAGIC %pip install -q -r ../requirements.txt

# COMMAND ----------

import os
import sys

_HERE = os.path.dirname(os.path.abspath("__file__"))
sys.path.insert(0, os.path.abspath(os.path.join(_HERE, "..")))

# COMMAND ----------

# Per-instance widgets (source_config)
dbutils.widgets.dropdown("environment", "dev", ["dev", "prod"], "environment")
dbutils.widgets.text("instance_name", "", "instance_name")
dbutils.widgets.text("destination_catalog", "", "destination_catalog")
dbutils.widgets.text("destination_schema", "zoom", "destination_schema")
dbutils.widgets.text("metadata_catalog", "", "metadata_catalog (dev sandbox: raw_dev)")
dbutils.widgets.text("metadata_schema", "jetstream", "metadata_schema (dev sandbox: jetstream_zoom)")
dbutils.widgets.text("secret_scope", "", "secret_scope")
dbutils.widgets.text("zoom_base_url", "https://api.zoom.us/v2", "zoom_base_url")
dbutils.widgets.text("zoom_token_url", "https://zoom.us/oauth/token", "zoom_token_url")
dbutils.widgets.text("sla_target_minutes", "1440", "sla_target_minutes")
dbutils.widgets.text("owner_team", "", "owner_team")
dbutils.widgets.text("rate_limit_qps", "10", "rate_limit_qps")
dbutils.widgets.text("timeout_seconds", "60", "timeout_seconds")
dbutils.widgets.text("retry_max_attempts", "5", "retry_max_attempts")
dbutils.widgets.text("enabled_objects", "", "enabled_objects (CSV)")
dbutils.widgets.text("optional_objects_enabled", "", "optional_objects_enabled (CSV)")

# Per-run widgets
dbutils.widgets.dropdown("mode", "run", ["run", "full_resync", "discover"], "mode")
dbutils.widgets.text("objects", "", "objects (CSV, full_resync only)")
dbutils.widgets.dropdown("trigger_type", "scheduled",
                          ["scheduled", "manual", "api", "backfill"], "trigger_type")

# COMMAND ----------

mode = dbutils.widgets.get("mode")
trigger_type = dbutils.widgets.get("trigger_type")
objects_arg = dbutils.widgets.get("objects").strip()
objects = [o.strip() for o in objects_arg.split(",") if o.strip()] or None

# COMMAND ----------

from src.pipeline import run

result = run(
    spark=spark,
    dbutils=dbutils,
    mode=mode,
    objects=objects,
    trigger_type=trigger_type,
)
print(result)
