# Databricks notebook source
# DBTITLE 1,Jetstream Google Drive Connector — Entry Point
# MAGIC %md
# MAGIC # Jetstream Google Drive Connector
# MAGIC
# MAGIC This notebook is the **Databricks Job entry point** for the Google Drive connector.
# MAGIC
# MAGIC It calls `src.pipeline.run()` which orchestrates:
# MAGIC 1. Self-bootstrap (registry, manifest, instance)
# MAGIC 2. Source config validation
# MAGIC 3. Incremental metadata extraction → Iceberg table
# MAGIC 4. File download → UC Volume or S3
# MAGIC 5. Control-plane tracking (runs, cursors, logs)
# MAGIC
# MAGIC **Configuration** is passed via Databricks Job parameters (widgets) — see `connectors/gdrive/parameters.yaml` for the full schema.

# COMMAND ----------

# DBTITLE 1,Setup: add project root to sys.path
import sys
import os

# Resolve project root — works for both workspace (/Users/...) and Git repos (/Repos/...).
# All workspace-backed paths are accessible under /Workspace on the filesystem.
_nb_path = dbutils.notebook.entry_point.getDbutils().notebook().getContext().notebookPath().get()
_project_root = f"/Workspace{os.path.dirname(os.path.dirname(_nb_path))}"

# Validate: the resolved root must contain a src/ directory.
if not os.path.isdir(os.path.join(_project_root, "src")):
    raise RuntimeError(
        f"Cannot locate project root. Expected 'src/' directory at: {_project_root}. "
        f"Notebook path was: {_nb_path}"
    )

if _project_root not in sys.path:
    sys.path.insert(0, _project_root)

print(f"Project root: {_project_root}")

# COMMAND ----------

# DBTITLE 1,Run the connector pipeline
from src.pipeline import run

# Read mode from job parameter (default: 'run' = incremental)
_mode = dbutils.widgets.get("mode") if "mode" in dbutils.widgets.getAll() else "run"

result = run(
    spark=spark,
    dbutils=dbutils,
    mode=_mode,
    trigger_type="scheduled",
)

print(f"\nRun completed: {result}")
