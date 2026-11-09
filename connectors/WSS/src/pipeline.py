"""Pipeline orchestration engine delegating object synchronization to native src.sync_object."""
from __future__ import annotations

import concurrent.futures
import json
import logging
import os
import uuid
from threading import Lock
from typing import Any, Dict, List, Optional

from pyspark.sql import SparkSession

from src import state as state_mod
from src.auth import WSSSession
from src.config import resolve_run_config
from src.sync_object import sync_object
from src.utils import get_widget_safely

log = logging.getLogger(__name__)

try:
    from src.jetstream.jetstream_id_generator import uuid7
except ImportError:
    try:
        from src.jetstream.ctx import uuid7
    except ImportError:
        def uuid7() -> str:
            return str(uuid.uuid4())

OFFICIAL_MANIFEST_SCHEMA = {
    "$schema": "http://json-schema.org/draft-07/schema#",
    "type": "object",
    "properties": {
        "environment": {"type": "string", "description": "Jetstream metadata catalog environment.", "enum": ["dev", "prod"]},
        "instance_name": {"type": "string", "description": "Deployment name. One connector_instance per Databricks Job."},
        "destination_catalog": {"type": "string", "description": "Unity Catalog for raw WSS tables."},
        "destination_schema": {"type": "string", "description": "Schema under destination_catalog.", "default": "wss"},
        "metadata_catalog": {"type": "string", "default": ""},
        "metadata_schema": {"type": "string", "default": "jetstream"},
        "sla_target_minutes": {"type": "integer", "default": 1440},
        "owner_team": {"type": "string", "default": ""},
        "secret_scope": {"type": "string", "description": "Secret scope with WSS API credentials."},
        "attachment_volume_base": {"type": "string", "default": ""},
        "download_attachments": {"type": "boolean", "default": True},
        "enabled_objects": {"type": "string", "default": "contracts"},
        "primary_key": {"type": "string", "default": "id"},
        "page_size": {"type": "integer", "default": 100},
        "retry_max_attempts": {"type": "integer", "default": 5},
        "mode": {"type": "string", "enum": ["run", "full_resync", "retry_failed_downloads", "bootstrap_only"], "default": "run"},
        "trigger_type": {"type": "string", "enum": ["scheduled", "manual", "api", "backfill"], "default": "scheduled"}
    },
    "additionalProperties": True,
    "required": ["environment", "instance_name", "destination_catalog", "destination_schema", "secret_scope"]
}

class DummyTotals:
    """Accrues run execution summary totals across objects in a thread-safe manner."""
    def __init__(self):
        self._lock = Lock()
        self._rows_ingested_total = 0
        self._rows_inserted = 0
        self._bytes_processed = 0
        self._tables_synced = 0

    @property
    def rows_ingested_total(self) -> int:
        with self._lock:
            return self._rows_ingested_total

    @rows_ingested_total.setter
    def rows_ingested_total(self, val: int) -> None:
        with self._lock:
            self._rows_ingested_total = val

    @property
    def rows_inserted(self) -> int:
        with self._lock:
            return self._rows_inserted

    @rows_inserted.setter
    def rows_inserted(self, val: int) -> None:
        with self._lock:
            self._rows_inserted = val

    @property
    def bytes_processed(self) -> int:
        with self._lock:
            return self._bytes_processed

    @bytes_processed.setter
    def bytes_processed(self, val: int) -> None:
        with self._lock:
            self._bytes_processed = val

    @property
    def tables_synced(self) -> int:
        with self._lock:
            return self._tables_synced

    @tables_synced.setter
    def tables_synced(self, val: int) -> None:
        with self._lock:
            self._tables_synced = val

def _extract_databricks_context(dbutils: Any) -> Any:
    """Extracts JVM Databricks runtime context tags for job_id, workspace_id, run_id, and user UPN."""
    try:
        from src.jetstream.ctx import DatabricksContext
    except ImportError:
        from src.jetstream.jetstream_context import DatabricksContext

    ws_id, j_id, jr_id, trig = "", "", "", ""
    try:
        raw_ctx = dbutils.notebook.entry_point.getDbutils().notebook().getContext()
        tags = raw_ctx.tags()
        ws_id = str(raw_ctx.workspaceId().getOrElse(None) or tags.get("orgId").getOrElse(None) or "")
        j_id = str(tags.get("jobId").getOrElse(None) or tags.get("job_id").getOrElse(None) or "")
        jr_id = str(tags.get("multitaskParentRunId").getOrElse(None) or tags.get("id").getOrElse(None) or raw_ctx.currentRunId().getOrElse(None) or "")
        trig = str(tags.get("user").getOrElse(None) or tags.get("creator").getOrElse(None) or raw_ctx.userName().getOrElse(None) or "")
    except Exception:
        ws_id = get_widget_safely(dbutils, "workspace_id", "")
        j_id = get_widget_safely(dbutils, "job_id", "")
        jr_id = get_widget_safely(dbutils, "job_run_id", "")
        trig = get_widget_safely(dbutils, "triggered_by", "")

    return DatabricksContext(workspace_id=ws_id, job_id=j_id, job_run_id=jr_id, triggered_by=trig)

def run(
    spark: SparkSession,
    dbutils: Any,
    mode: Optional[str] = None,
    trigger_type: Optional[str] = None,
    start_time: Optional[str] = None,
    end_time: Optional[str] = None,
    catalog: Optional[str] = None,
    schema: Optional[str] = None,
    config_objects: Optional[List[Any]] = None,
) -> Dict[str, Any]:
    """Main pipeline orchestration entry point with structured execution summary reporting."""
    exec_mode = mode or get_widget_safely(dbutils, "mode", "run")
    env_param = get_widget_safely(dbutils, "environment", "dev") or "dev"
    target_catalog = catalog or get_widget_safely(dbutils, "destination_catalog", "") or f"raw_{env_param}"
    target_schema = schema or get_widget_safely(dbutils, "destination_schema", "") or "wss"

    req_start_time = start_time or get_widget_safely(dbutils, "from_datetime") or get_widget_safely(dbutils, "start_time")
    req_end_time = end_time or get_widget_safely(dbutils, "to_datetime") or get_widget_safely(dbutils, "end_time")

    meta_catalog = get_widget_safely(dbutils, "metadata_catalog", "") or (
        "metadata_prod" if env_param == "prod" else "metadata_dev"
    )
    meta_schema = get_widget_safely(dbutils, "metadata_schema", "") or "jetstream"

    raw_enabled_objs = get_widget_safely(dbutils, "enabled_objects", "")

    widget_params = {
        "environment": env_param,
        "destination_catalog": target_catalog,
        "destination_schema": target_schema,
        "metadata_catalog": meta_catalog,
        "metadata_schema": meta_schema,
        "raw_json_volume_base": get_widget_safely(dbutils, "raw_json_volume_base", f"/Volumes/{target_catalog}/{target_schema}/api_responses"),
        "attachment_volume_base": get_widget_safely(dbutils, "attachment_volume_base", f"/Volumes/{target_catalog}/{target_schema}/attachments"),
        "secret_scope": get_widget_safely(dbutils, "secret_scope", "") or "jetstream_wss",
        "enabled_objects": raw_enabled_objs,
        "from_datetime": req_start_time,
        "to_datetime": req_end_time,
        "is_sandbox": get_widget_safely(dbutils, "is_sandbox", "false").lower() == "true",
    }

    cfg = resolve_run_config(widget_params, dbutils=dbutils)
    db_ctx = _extract_databricks_context(dbutils)

    resolved_objs = config_objects or cfg.objects
    if resolved_objs:
        obj_list = [getattr(o, "object_name", getattr(o, "name", str(o))) for o in resolved_objs]
        widget_params["enabled_objects"] = ",".join(obj_list)

    try:
        from src.jetstream.ctx import MetaCtx
    except ImportError:
        from src.jetstream.jetstream_context import MetaCtx

    meta_ctx = MetaCtx(catalog=cfg.routing.metadata_catalog, schema=cfg.routing.metadata_schema)

    # -------------------------------------------------------------------------
    # BOOTSTRAP TASK EXECUTION
    # -------------------------------------------------------------------------
    if exec_mode == "bootstrap_only":
        log.info(" [BOOTSTRAP-ONLY MODE ACTIVATED]")
        state_mod._spark_sql(spark, f"CREATE SCHEMA IF NOT EXISTS `{cfg.routing.metadata_catalog}`.`{cfg.routing.metadata_schema}`")
        state_mod._spark_sql(spark, f"CREATE SCHEMA IF NOT EXISTS `{target_catalog}`.`{target_schema}`")

        try:
            from src.jetstream.jetstream_bootstrap import ensure_registered_and_get_instance
        except ImportError:
            from src.jetstream.jetstream_bootstrapper import ensure_registered_and_get_instance

        # PERMANENT FIX: Assign explicit non-null values for all NOT NULL manifest columns
        new_manifest_id = uuid7()
        schema_json_clean = json.dumps(OFFICIAL_MANIFEST_SCHEMA).replace("'", "''")

        manifest_sql = f"""
            MERGE INTO `{cfg.routing.metadata_catalog}`.`{cfg.routing.metadata_schema}`.`connector_manifest` t
            USING (
                SELECT 
                    '{new_manifest_id}' AS manifest_id, 
                    'wss' AS connector_type_id, 
                    '1.0.0' AS manifest_version,
                    '{schema_json_clean}' AS config_schema,
                    TRUE AS is_current
            ) s
            ON t.connector_type_id = s.connector_type_id AND t.manifest_version = s.manifest_version
            WHEN MATCHED THEN UPDATE SET 
                t.config_schema = s.config_schema,
                t.is_current = s.is_current
            WHEN NOT MATCHED THEN INSERT (
                manifest_id, 
                connector_type_id, 
                manifest_version, 
                config_schema, 
                is_current, 
                created_at
            )
            VALUES (
                s.manifest_id, 
                s.connector_type_id, 
                s.manifest_version, 
                s.config_schema, 
                s.is_current, 
                current_timestamp()
            )
        """
        state_mod._spark_sql(spark, manifest_sql)

        instance_id, manifest_ver, _ = ensure_registered_and_get_instance(
            spark=spark,
            meta=meta_ctx,
            connector_type_id="wss",
            connector_type_name="WSS",
            category="application_api",
            parameters_yaml_path="parameters: []",
            connector_version="1.0.0",
            maintainer="Jetstream",
            repo_path="connectors/wss",
            source_config=widget_params,
            destination_catalog=target_catalog,
            destination_schema=target_schema,
            instance_name=get_widget_safely(dbutils, "instance_name", "") or "wss",
            environment=env_param,
            sla_target_minutes=1440,
            owner_team=get_widget_safely(dbutils, "owner_team", ""),
            db_ctx=db_ctx,
        )

        log.info(" [BOOTSTRAP SUCCESS] Registered connector instance ID '%s' (Manifest: %s).", instance_id, manifest_ver)
        return {
            "status": "success",
            "mode": exec_mode,
            "message": f"Bootstrap completed successfully. Registered instance '{instance_id}'.",
            "totals": {"tables_synced": 0, "rows_ingested_total": 0, "rows_inserted": 0, "bytes_processed": 0},
            "success": ["bootstrap_initialization"],
            "failed": []
        }

    # -------------------------------------------------------------------------
    # OBJECT EXTRACTION & SYNC EXECUTION
    # -------------------------------------------------------------------------
    try:
        from src.jetstream.jetstream_bootstrap import lookup_instance_id
    except ImportError:
        from src.jetstream.jetstream_bootstrapper import lookup_instance_id

    instance_id = lookup_instance_id(spark, meta_ctx, db_ctx=db_ctx) or cfg.connector_instance_id

    session = WSSSession(credentials=cfg.credentials, is_sandbox=cfg.is_sandbox)
    totals = DummyTotals()
    summary: Dict[str, List[Any]] = {"success": [], "failed": []}
    objects_to_sync = resolved_objs

    if not objects_to_sync:
        return {
            "status": "success",
            "totals": {"tables_synced": 0, "rows_ingested_total": 0, "rows_inserted": 0, "bytes_processed": 0},
            "success": [],
            "failed": []
        }

    max_workers = min(5, len(objects_to_sync))
    log.info(" Starting parallel synchronization for %d object(s) using %d worker threads...", len(objects_to_sync), max_workers)

    def _sync_worker(obj: Any) -> Dict[str, Any]:
        obj_name = getattr(obj, "object_name", getattr(obj, "name", "unknown"))
        obj.destination_catalog = target_catalog
        obj.destination_schema = target_schema
        obj.raw_json_volume_base = widget_params["raw_json_volume_base"]
        obj.attachment_volume_path = widget_params["attachment_volume_base"]

        if req_start_time:
            obj.from_datetime = req_start_time
            obj.start_time = req_start_time
        if req_end_time:
            obj.to_datetime = req_end_time
            obj.end_time = req_end_time

        job_run_identifier = db_ctx.job_run_id if db_ctx.job_run_id else instance_id
        obj_idempotency_key = f"{job_run_identifier}_{obj_name}_{exec_mode}"

        obj_run_id, _ = state_mod.open_run(
            spark, meta_ctx,
            connector_instance_id=instance_id,
            idempotency_key=obj_idempotency_key,
            run_type=exec_mode,
            trigger_type=trigger_type or "scheduled",
            db_ctx=db_ctx,
        )

        try:
            res = sync_object(
                spark=spark,
                meta=meta_ctx,
                session=session,
                instance_id=instance_id,
                run_id=obj_run_id,
                obj=obj,
                totals=totals,
                mode=exec_mode,
            )

            state_mod.close_run(
                spark, meta_ctx, run_id=obj_run_id, status="success",
                totals={
                    "rows_ingested_total": res.get("rows_read", 0),
                    "rows_inserted": res.get("rows_inserted", 0),
                    "bytes_processed": res.get("bytes_written", 0),
                    "tables_synced": 1,
                }
            )
            return res
        except Exception as exc:
            state_mod.close_run(
                spark, meta_ctx, run_id=obj_run_id, status="failed",
                totals={"rows_ingested_total": 0, "rows_inserted": 0, "bytes_processed": 0, "tables_synced": 0},
                error_code="OBJECT_SYNC_FAILED", error_message=str(exc)
            )
            raise

    summary_lock = Lock()
    with concurrent.futures.ThreadPoolExecutor(max_workers=max_workers) as executor:
        future_to_obj = {executor.submit(_sync_worker, obj): obj for obj in objects_to_sync}

        for future in concurrent.futures.as_completed(future_to_obj):
            obj = future_to_obj[future]
            obj_name = getattr(obj, "object_name", getattr(obj, "name", "unknown"))
            try:
                res = future.result()
                with summary_lock:
                    summary["success"].append(res)
            except Exception as e:
                log.error("Failed sync for object '%s': %s", obj_name, str(e), exc_info=True)
                with summary_lock:
                    summary["failed"].append({"object_name": obj_name, "error": str(e)})

    status = "success" if not summary["failed"] else ("partial_success" if summary["success"] else "failed")

    final_totals = {
        "rows_ingested_total": totals.rows_ingested_total,
        "rows_inserted": totals.rows_inserted,
        "bytes_processed": totals.bytes_processed,
        "tables_synced": totals.tables_synced,
    }

    result_payload = {
        "status": status,
        "totals": final_totals,
        "success": summary["success"],
        "failed": summary["failed"],
    }

    print(f"[SUCCESS] Execution summary: {result_payload}", flush=True)

    if summary["failed"]:
        failed_names = [f["object_name"] for f in summary["failed"]]
        raise RuntimeError(f"Pipeline task completed with failures in objects: {failed_names}")

    return result_payload
