# Databricks notebook source
# MAGIC %md
# MAGIC # ServiceNow connector — Run
# MAGIC
# MAGIC One task extracts one ServiceNow table and one `sync_target`
# MAGIC (`base`, `attachments`, or `journals`). All configuration comes from
# MAGIC this job's parameters. On first run the notebook registers the connector
# MAGIC type, manifest, and instance in `metadata_<env>.jetstream`.
# MAGIC
# MAGIC See [connectors/servicenow/parameters.yaml](../connectors/servicenow/parameters.yaml).

# COMMAND ----------

# MAGIC %pip install -q -r ../requirements.txt

# COMMAND ----------

import os
import sys


def _connector_root() -> str:
    try:
        here = os.path.dirname(os.path.abspath(__file__))
        root = os.path.abspath(os.path.join(here, ".."))
        if os.path.isdir(os.path.join(root, "src")):
            return root
    except NameError:
        pass
    cwd = os.getcwd()
    for candidate in (cwd, os.path.abspath(os.path.join(cwd, ".."))):
        if os.path.isdir(os.path.join(candidate, "src")):
            return candidate
    try:
        nb_path = dbutils.notebook.entry_point.getDbutils().notebook().getContext().notebookPath().get()
        parts = [p for p in nb_path.split("/") if p]
        if "notebook" in parts:
            idx = parts.index("notebook")
            rel = "/" + "/".join(parts[:idx])
            for prefix in ("/Workspace", ""):
                cand = prefix + rel
                if os.path.isdir(os.path.join(cand, "src")):
                    return cand
    except Exception:
        pass
    raise RuntimeError("Cannot locate connector root (expected a src/ directory above this notebook).")


sys.path.insert(0, _connector_root())

# COMMAND ----------

dbutils.widgets.dropdown("environment", "dev", ["dev", "prod"], "environment")
dbutils.widgets.text("instance_name", "", "instance_name")
dbutils.widgets.text("destination_catalog", "", "destination_catalog")
dbutils.widgets.text("destination_schema", "servicenow", "destination_schema")
dbutils.widgets.text("metadata_catalog", "", "metadata_catalog (dev sandbox: raw_dev)")
dbutils.widgets.text("metadata_schema", "jetstream", "metadata_schema (dev sandbox: jetstream_servicenow)")
dbutils.widgets.text("secret_scope", "", "secret_scope")
dbutils.widgets.text("sla_target_minutes", "1440", "sla_target_minutes")
dbutils.widgets.text("owner_team", "", "owner_team")
dbutils.widgets.text("attachment_volume_base", "", "attachment_volume_base")

dbutils.widgets.text("enabled_objects", "incident", "enabled_objects")
dbutils.widgets.dropdown("sync_target", "base", ["base", "attachments", "journals"], "sync_target")
dbutils.widgets.text("primary_key", "sys_id", "primary_key")
dbutils.widgets.text("watermark_column", "sys_updated_on", "watermark_column")
dbutils.widgets.dropdown("download_attachments", "true", ["true", "false"], "download_attachments")
dbutils.widgets.text("attachment_meta_table", "", "attachment_meta_table")
dbutils.widgets.text("journals_meta_table", "", "journals_meta_table")
dbutils.widgets.text("page_size", "10000", "page_size")
dbutils.widgets.text("lookback_minutes", "15", "lookback_minutes")
dbutils.widgets.text("from_datetime", "", "from_datetime")
dbutils.widgets.text("to_datetime", "", "to_datetime")
dbutils.widgets.text("retry_max_attempts", "5", "retry_max_attempts")
dbutils.widgets.dropdown(
    "mode",
    "run",
    ["run", "full_resync", "retry_failed_downloads", "bootstrap_only"],
    "mode",
)
dbutils.widgets.dropdown("skip_bootstrap", "false", ["true", "false"], "skip_bootstrap")
dbutils.widgets.dropdown(
    "trigger_type",
    "scheduled",
    ["scheduled", "manual", "api", "backfill"],
    "trigger_type",
)

# COMMAND ----------

mode = dbutils.widgets.get("mode")
trigger_type = dbutils.widgets.get("trigger_type")

# COMMAND ----------

from src.pipeline import run

result = run(
    spark=spark,
    dbutils=dbutils,
    mode=mode,
    objects=None,
    trigger_type=trigger_type,
)
print(result)
