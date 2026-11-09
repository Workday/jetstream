# Databricks notebook source
# MAGIC %md
# MAGIC # WSS connector — Run
# MAGIC
# MAGIC Syncs objects listed in `enabled_objects` from the Strategic Sourcing API
# MAGIC into Unity Catalog. On first run the notebook registers the connector
# MAGIC type, manifest, and instance.
# MAGIC
# MAGIC See [connectors/wss/parameters.yaml](../connectors/wss/parameters.yaml).

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
dbutils.widgets.text("destination_schema", "wss", "destination_schema")
dbutils.widgets.text("metadata_catalog", "", "metadata_catalog (dev sandbox: raw_dev)")
dbutils.widgets.text("metadata_schema", "jetstream", "metadata_schema (dev sandbox: jetstream_wss)")
dbutils.widgets.text("secret_scope", "", "secret_scope")
dbutils.widgets.text("sla_target_minutes", "1440", "sla_target_minutes")
dbutils.widgets.text("owner_team", "", "owner_team")
dbutils.widgets.dropdown("is_sandbox", "false", ["true", "false"], "is_sandbox")
dbutils.widgets.text("attachment_volume_base", "", "attachment_volume_base")
dbutils.widgets.text("raw_json_volume_base", "", "raw_json_volume_base")
dbutils.widgets.dropdown("download_attachments", "true", ["true", "false"], "download_attachments")
dbutils.widgets.text("enabled_objects", "contracts", "enabled_objects")
dbutils.widgets.text("primary_key", "id", "primary_key")
dbutils.widgets.text("watermark_column", "updated_at", "watermark_column")
dbutils.widgets.dropdown("attachment_enabled", "false", ["true", "false"], "attachment_enabled")
dbutils.widgets.text("attachment_meta_table", "attachments", "attachment_meta_table")
dbutils.widgets.text("page_size", "100", "page_size")
dbutils.widgets.text("lookback_minutes", "30", "lookback_minutes")
dbutils.widgets.text("from_datetime", "", "from_datetime")
dbutils.widgets.text("to_datetime", "", "to_datetime")
dbutils.widgets.text("retry_max_attempts", "5", "retry_max_attempts")
dbutils.widgets.dropdown(
    "mode",
    "run",
    ["run", "full_resync", "retry_failed_downloads", "bootstrap_only"],
    "mode",
)
dbutils.widgets.dropdown(
    "trigger_type",
    "scheduled",
    ["scheduled", "manual", "api", "backfill"],
    "trigger_type",
)

# COMMAND ----------

from src.pipeline import run
from src.utils import get_widget_safely

result = run(
    spark=spark,
    dbutils=dbutils,
    mode=get_widget_safely(dbutils, "mode", "run"),
    trigger_type=get_widget_safely(dbutils, "trigger_type", "scheduled"),
)
print(result)
