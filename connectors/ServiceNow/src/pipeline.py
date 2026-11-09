"""Top-level orchestration against the shared Jetstream control plane."""

from __future__ import annotations

import logging
import os
import time
import traceback
from dataclasses import dataclass, field, replace
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional, Sequence

from src import config as config_mod
from src import state as state_mod
from src import sync_object as sync_mod
from src.auth import ServiceNowSession
from src.jetstream import bootstrap as bootstrap_mod
from src.jetstream import params as params_mod
from src.jetstream import validate as validate_mod
from src.jetstream.ctx import read_databricks_context, resolve_metadata_catalog
from src.secrets import load_servicenow_credentials

log = logging.getLogger(__name__)


class _AllObjectsFailedError(RuntimeError):
    """Raised when every object in a run fails."""

CONNECTOR_TYPE_ID = "servicenow"
CONNECTOR_TYPE_NAME = "ServiceNow"
CONNECTOR_CATEGORY = "application_api"
CONNECTOR_VERSION = "1.0.0"
CONNECTOR_MAINTAINER = "Jetstream"
CONNECTOR_REPO_PATH = "connectors/servicenow"
PARAMETERS_YAML_PATH = os.path.join(
    os.path.dirname(os.path.dirname(os.path.abspath(__file__))),
    "connectors", "servicenow", "parameters.yaml",
)

VALID_MODES = ("run", "full_resync", "retry_failed_downloads", "bootstrap_only")


def _console(msg: str) -> None:
    print(msg, flush=True)


@dataclass
class _RunTotals:
    rows_ingested_total: int = 0
    rows_inserted: int = 0
    rows_updated: int = 0
    rows_deleted: int = 0
    bytes_processed: int = 0
    tables_synced: int = 0
    objects_succeeded: int = 0
    objects_failed: int = 0
    failed_objects: List[str] = field(default_factory=list)
    first_error_code: Optional[str] = None
    first_error_message: Optional[str] = None

    def as_dict(self) -> Dict[str, int]:
        return {
            "rows_ingested_total": self.rows_ingested_total,
            "rows_inserted": self.rows_inserted,
            "rows_updated": self.rows_updated,
            "rows_deleted": self.rows_deleted,
            "bytes_processed": self.bytes_processed,
            "tables_synced": self.tables_synced,
        }


def _task_identifier(db_ctx: Any, widget_params: Dict[str, Any]) -> str:
    """Unique CP task id. Databricks task_keys are already unique; do not
    double-suffix when the key is already ``{object}_attachments`` / ``_journals``.
    """
    base = (
        (getattr(db_ctx, "task_key", None) or "").strip()
        or str(widget_params.get("instance_name") or "").strip()
        or str(widget_params.get("enabled_objects") or "").strip()
        or "default_task"
    )
    target = str(widget_params.get("sync_target", "base")).strip() or "base"
    if target == "base":
        return base
    suffix = f"_{target}"
    if base.endswith(suffix):
        return base
    return f"{base}{suffix}"


def _idempotency_key(db_ctx: Any, widget_params: Dict[str, Any], mode: str) -> str:
    job_run_id = getattr(db_ctx, "job_run_id", None) or "unknown"
    task_id = _task_identifier(db_ctx, widget_params)
    key = f"{job_run_id}_{task_id}"
    if mode == "retry_failed_downloads":
        key = f"{key}:retry_downloads"
    return key


def _resolve_run_type(mode: str, cfg: config_mod.RunConfig, force_full: bool) -> str:
    if mode == "retry_failed_downloads":
        return "adhoc_retry"
    if cfg.is_adhoc_window and not force_full:
        return "adhoc"
    if mode == "full_resync" or force_full:
        return "full_resync"
    return "incremental"


def run(
    *,
    spark: Any,
    dbutils: Any,
    mode: str = "run",
    objects: Optional[Sequence[str]] = None,
    trigger_type: str = "scheduled",
) -> Dict[str, Any]:
    mode = (mode or "run").strip().lower()
    if mode not in VALID_MODES:
        raise ValueError(f"unknown mode {mode!r}; expected one of {VALID_MODES}")

    db_ctx = read_databricks_context(dbutils)
    widget_params = _read_widgets(dbutils)
    compiled_schema = params_mod.compile_parameters_yaml(PARAMETERS_YAML_PATH)
    widget_params = params_mod.coerce_widget_values(widget_params, compiled_schema)

    routing = config_mod.resolve_platform_routing(widget_params)
    meta = resolve_metadata_catalog(
        routing.environment, routing.metadata_schema, routing.metadata_catalog or None
    )

    if mode == "bootstrap_only":
        instance_id, manifest_version, _ = bootstrap_mod.ensure_registered_and_get_instance(
            spark, meta, connector_type_id=CONNECTOR_TYPE_ID, connector_type_name=CONNECTOR_TYPE_NAME,
            category=CONNECTOR_CATEGORY, parameters_yaml_path=PARAMETERS_YAML_PATH,
            connector_version=CONNECTOR_VERSION, maintainer=CONNECTOR_MAINTAINER,
            repo_path=CONNECTOR_REPO_PATH, source_config=config_mod.source_config_for_manifest(widget_params),
            destination_catalog=routing.destination_catalog, destination_schema=routing.destination_schema,
            instance_name=routing.instance_name, environment=routing.environment,
            sla_target_minutes=routing.sla_target_minutes, owner_team=routing.owner_team, db_ctx=db_ctx,
        )
        _console(f"Bootstrap OK: connector_instance_id={instance_id} manifest_version={manifest_version}")
        return {"mode": mode, "connector_instance_id": instance_id, "manifest_version": manifest_version}

    skip_bootstrap = str(widget_params.get("skip_bootstrap") or "").strip().lower() in ("1", "true", "yes", "y")
    if skip_bootstrap:
        instance_id = bootstrap_mod.lookup_instance_id(spark, meta, db_ctx=db_ctx)
        if not instance_id:
            raise RuntimeError("skip_bootstrap=true but no connector_instance found.")
    else:
        instance_id, _, _ = bootstrap_mod.ensure_registered_and_get_instance(
            spark, meta, connector_type_id=CONNECTOR_TYPE_ID, connector_type_name=CONNECTOR_TYPE_NAME,
            category=CONNECTOR_CATEGORY, parameters_yaml_path=PARAMETERS_YAML_PATH,
            connector_version=CONNECTOR_VERSION, maintainer=CONNECTOR_MAINTAINER,
            repo_path=CONNECTOR_REPO_PATH, source_config=config_mod.source_config_for_manifest(widget_params),
            destination_catalog=routing.destination_catalog, destination_schema=routing.destination_schema,
            instance_name=routing.instance_name, environment=routing.environment,
            sla_target_minutes=routing.sla_target_minutes, owner_team=routing.owner_team, db_ctx=db_ctx,
        )

    cfg_preview = config_mod.resolve_run_config(widget_params, dbutils=dbutils)
    if objects:
        from src.objects import resolve_object
        cfg_preview = replace(
            cfg_preview,
            objects=[
                resolve_object(
                    n,
                    attachment_meta_table=cfg_preview.attachment_meta_table,
                    journals_meta_table=cfg_preview.journals_meta_table,
                    primary_key=cfg_preview.primary_key,
                    watermark_column=cfg_preview.watermark_column,
                )
                for n in objects
            ],
        )

    force_full = mode == "full_resync"
    idempotency_key = _idempotency_key(db_ctx, widget_params, mode)
    run_type = _resolve_run_type(mode, cfg_preview, force_full)

    run_id, is_new_run = state_mod.open_run(
        spark, meta,
        connector_instance_id=instance_id,
        idempotency_key=idempotency_key,
        run_type=run_type,
        trigger_type=trigger_type,
        db_ctx=db_ctx,
    )

    task_id = _task_identifier(db_ctx, widget_params)
    _console("=" * 80)
    _console(f"Jetstream ServiceNow — run_id={run_id}")
    _console(f"  instance:     {routing.instance_name} ({routing.environment})")
    _console(f"  target scope: {cfg_preview.sync_target.upper()}")
    _console(f"  mode:         {mode} ({run_type}) / trigger={trigger_type}")
    _console(f"  task:         {task_id}")
    _console(f"  idempotency:  {idempotency_key}")
    _console("=" * 80)

    run_start_ts = time.monotonic()
    totals = _RunTotals()
    results: Dict[str, Any] = {}
    run_closed = False

    try:
        try:
            validate_mod.validate_source_config(widget_params, compiled_schema)
        except validate_mod.SourceConfigInvalid as exc:
            state_mod.log_event(
                spark, meta, connector_instance_id=instance_id, run_id=run_id,
                event_type="config_validation_failed", severity="SEVERE", message=str(exc),
            )
            state_mod.close_run(spark, meta, run_id=run_id, status="failed", totals=totals.as_dict(), error_code="config_validation_failed", error_message=str(exc))
            run_closed = True
            raise

        creds = load_servicenow_credentials(cfg_preview.secret_scope, dbutils=dbutils)
        cfg = replace(cfg_preview, credentials=creds)
        session = ServiceNowSession(credentials=cfg.credentials)
        retry_only = mode == "retry_failed_downloads"

        for obj_cfg in cfg.objects:
            obj_force = force_full
            obj_run = config_mod.build_object_run_config(obj_cfg, cfg, force_full=obj_force)
            _console(f"-- object={obj_run.object_name} scope={obj_run.sync_target}")
            obj_start = datetime.now(timezone.utc).isoformat()
            try:
                results[obj_run.object_name] = sync_mod.sync_object(
                    spark, meta, session, instance_id, run_id, obj_run, totals,
                    retry_failed_downloads_only=retry_only,
                )
                totals.objects_succeeded += 1
            except Exception as obj_exc:
                totals.objects_failed += 1
                totals.failed_objects.append(obj_run.object_name)
                if totals.first_error_code is None:
                    totals.first_error_code = type(obj_exc).__name__
                    totals.first_error_message = str(obj_exc)
                err = f"{type(obj_exc).__name__}: {obj_exc}"
                _console(f"FAILED object={obj_run.object_name}: {err}")
                results[obj_run.object_name] = {"status": "failed", "error_type": type(obj_exc).__name__, "error": str(obj_exc)}
                state_mod.log_event(
                    spark, meta, connector_instance_id=instance_id, run_id=run_id,
                    event_type="object_sync_failed", severity="SEVERE", message=f"{mode} failed: {obj_exc}",
                )
                state_mod.write_object_run(
                    spark, meta, connector_instance_id=instance_id, run_id=run_id,
                    destination_table=obj_run.destination_table, source_object=obj_run.source_object,
                    status="failed", counts={"rows_read": 0, "rows_inserted": 0, "rows_updated": 0, "rows_deleted": 0, "rows_unchanged": 0, "bytes_written": 0},
                    start_time_iso=obj_start, end_time_iso=datetime.now(timezone.utc).isoformat(), error_message=err[:4000],
                )

        if totals.objects_failed and totals.objects_succeeded == 0:
            status = "failed"
        elif totals.objects_failed:
            status = "failed_with_error"
        else:
            status = "success"

        state_mod.close_run(
            spark, meta, run_id=run_id, status=status, totals=totals.as_dict(),
            error_code=totals.first_error_code if status != "success" else None,
            error_message=totals.first_error_message if status != "success" else None,
        )
        run_closed = True

        elapsed = time.monotonic() - run_start_ts
        _console(f"DONE status={status} elapsed={elapsed:.1f}s objects_ok={totals.objects_succeeded} failed={totals.objects_failed}")

        if status == "failed":
            raise _AllObjectsFailedError(f"All {totals.objects_failed} object(s) failed.")

        return {
            "run_id": run_id, "connector_instance_id": instance_id, "mode": mode,
            "run_type": run_type, "instance_name": routing.instance_name, "task_identifier": task_id,
            "idempotency_key": idempotency_key, "run_status": status,
            "objects_succeeded": totals.objects_succeeded, "objects_failed": totals.objects_failed,
            **totals.as_dict(), "objects": results,
        }

    except Exception as exc:
        if isinstance(exc, _AllObjectsFailedError):
            raise
        if not run_closed:
            state_mod.log_event(spark, meta, connector_instance_id=instance_id, run_id=run_id, event_type="run_failed", severity="SEVERE", message=str(exc))
            state_mod.close_run(spark, meta, run_id=run_id, status="failed", totals=totals.as_dict(), error_code=type(exc).__name__, error_message=str(exc))
        raise

def _read_widgets(dbutils: Any) -> Dict[str, Any]:
    try:
        return dict(dbutils.widgets.getAll())
    except Exception:
        return {}
