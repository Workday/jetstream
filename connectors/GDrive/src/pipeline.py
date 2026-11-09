"""Top-level orchestration for the Google Drive connector.

Flow for `run(spark, dbutils, mode='run', objects=None)`:
  1. Read Databricks Jobs runtime context.
  2. Read job widget parameters via dbutils.widgets.getAll().
  3. Bootstrap connector_registry + connector_manifest + connector_instance
     (self-registration on first run, refresh on subsequent runs).
  4. Validate source_config against the manifest's JSON Schema.
  5. Open connector_run (idempotent on (instance_id, idempotency_key)).
  6. Authenticate to Google Drive.
  7. For each selected object (files_metadata, then files):
       - read cursor from connector_state
       - list metadata / download files incrementally
       - write to Iceberg table (metadata) or UC Volume/S3 (file bytes)
       - upsert connector_state, insert connector_object_run,
         upsert connector_table_registry
  8. Close connector_run with aggregate totals and status.

Every ERROR / WARNING is also appended to connector_log.
"""
from __future__ import annotations

import logging
import os
import time
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional, Sequence

from src import config as config_mod
from src import extract as extract_mod
from src import load as load_mod
from src import objects as objects_mod
from src import state as state_mod
from src.auth import GoogleDriveTokens
from src.config import ObjectCfg, RunConfig
from src.extract import DriveClient, FileMetadata
from src.jetstream import bootstrap as bootstrap_mod
from src.jetstream import params as params_mod
from src.jetstream import validate as validate_mod
from src.jetstream.ctx import (
    DatabricksContext,
    MetaCtx,
    read_databricks_context,
    resolve_metadata_catalog,
)


log = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Connector identity constants
# ---------------------------------------------------------------------------

CONNECTOR_TYPE_ID = "gdrive"
CONNECTOR_TYPE_NAME = "Google Drive"
CONNECTOR_CATEGORY = "file_object_storage"
CONNECTOR_VERSION = "1.0.0"
CONNECTOR_MAINTAINER = "BT Jetstream Team"
CONNECTOR_REPO_PATH = "connectors/gdrive"
PARAMETERS_YAML_PATH = os.path.join(
    os.path.dirname(os.path.dirname(os.path.abspath(__file__))),
    "connectors", "gdrive", "parameters.yaml",
)
CONNECTOR_DESCRIPTION = (
    "Ingests Google Drive file metadata and file bytes via the Drive REST API v3. "
    f"Objects: {', '.join(o.name for o in objects_mod.CATALOG)}."
)


# ---------------------------------------------------------------------------
# Console helpers
# ---------------------------------------------------------------------------

def _console(msg: str) -> None:
    """Print a user-facing progress line to stdout."""
    print(msg, flush=True)


def _fmt_bytes(n: int) -> str:
    v = float(n)
    for unit in ("B", "KB", "MB", "GB", "TB"):
        if abs(v) < 1024:
            return f"{v:.1f} {unit}"
        v /= 1024
    return f"{v:.1f} PB"


def _fmt_duration(secs: float) -> str:
    secs = int(secs)
    if secs < 60:
        return f"{secs}s"
    if secs < 3600:
        return f"{secs // 60}m {secs % 60}s"
    return f"{secs // 3600}h {(secs % 3600) // 60}m"


# ---------------------------------------------------------------------------
# Run-scoped mutable totals
# ---------------------------------------------------------------------------

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
    objects_skipped: int = 0
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


# ---------------------------------------------------------------------------
# Widget reader
# ---------------------------------------------------------------------------

def _read_widgets(dbutils: Any) -> Dict[str, Any]:
    """Read all Databricks Job parameters."""
    try:
        return dict(dbutils.widgets.getAll())
    except Exception:
        # Fallback for unit tests or local dry-runs.
        return {}


# ---------------------------------------------------------------------------
# Public entry point
# ---------------------------------------------------------------------------

def run(
    *,
    spark: Any,
    dbutils: Any,
    mode: str = "run",
    objects: Optional[Sequence[str]] = None,
    trigger_type: str = "scheduled",
) -> Dict[str, Any]:
    """Execute a Google Drive connector run.

    Args:
        spark: SparkSession.
        dbutils: Databricks dbutils.
        mode: 'run' (incremental) | 'full_resync' (reset cursor, re-extract).
        objects: Subset of object names to sync (None = all enabled).
        trigger_type: 'scheduled' | 'manual' | 'retry'.

    Returns:
        Dict with run_id, instance_id, and aggregate totals.
    """
    if mode not in ("run", "full_resync"):
        raise ValueError(f"unknown mode {mode!r}; expected 'run' or 'full_resync'")
    # full_resync without explicit objects list = resync ALL enabled objects.

    # Step 1 — Databricks context + widget parameters.
    widget_params = _read_widgets(dbutils)
    # Pass widget_params so Strategy 4 (serverless) can read job context
    # from the Databricks dynamic value references ({{job_id}}, {{run_id}}).
    db_ctx = read_databricks_context(dbutils, spark=spark, widget_params=widget_params)

    # Coerce widget string values to declared types.
    compiled_schema = params_mod.compile_parameters_yaml(PARAMETERS_YAML_PATH)
    widget_params = params_mod.coerce_widget_values(widget_params, compiled_schema)

    # Step 2 — Non-secret routing values.
    routing = config_mod.resolve_platform_routing(widget_params)
    meta = resolve_metadata_catalog(
        routing.environment, routing.metadata_schema, routing.metadata_catalog
    )

    # Step 3 — Self-bootstrap registry/manifest/instance.
    instance_id, _, _ = bootstrap_mod.ensure_registered_and_get_instance(
        spark, meta,
        connector_type_id=CONNECTOR_TYPE_ID,
        connector_type_name=CONNECTOR_TYPE_NAME,
        category=CONNECTOR_CATEGORY,
        description=CONNECTOR_DESCRIPTION,
        parameters_yaml_path=PARAMETERS_YAML_PATH,
        connector_version=CONNECTOR_VERSION,
        maintainer=CONNECTOR_MAINTAINER,
        repo_path=CONNECTOR_REPO_PATH,
        source_config=widget_params,
        destination_catalog=routing.destination_catalog,
        destination_schema=routing.destination_schema,
        instance_name=routing.instance_name,
        environment=routing.environment,
        sla_target_minutes=routing.sla_target_minutes,
        owner_team=routing.owner_team,
        tags=routing.tags,
        db_ctx=db_ctx,
    )

    # Step 4 — Open connector_run.
    # Idempotency key = {job_run_id}_{instance_name} so retries for the same
    # job run + instance don't create duplicate rows, and different instances
    # (e.g. gdrive_team_a vs gdrive_team_b) sharing a multi-task job are isolated.
    if db_ctx.job_run_id and db_ctx.job_run_id != "unknown":
        idempotency_key = f"{db_ctx.job_run_id}_{routing.instance_name}"
    else:
        import uuid
        idempotency_key = f"fallback-{uuid.uuid4().hex[:12]}_{routing.instance_name}"
    run_type = "full_resync" if mode == "full_resync" else "incremental"
    run_id, is_new_run = state_mod.open_run(
        spark, meta,
        connector_instance_id=instance_id,
        idempotency_key=idempotency_key,
        run_type=run_type,
        trigger_type=trigger_type,
        db_ctx=db_ctx,
    )
    log.info("run_id=%s instance=%s new=%s", run_id, instance_id, is_new_run)

    _console("=" * 80)
    _console(f"Jetstream Google Drive — run_id={run_id}")
    _console(f"  instance:     {routing.instance_name} ({routing.environment})")
    _console(f"  destination:  {routing.destination_catalog}.{routing.destination_schema}")
    _console(f"  metadata:     {meta.catalog}.{meta.schema}")
    _console(f"  mode:         {mode} ({run_type}) / trigger={trigger_type}")
    _console("=" * 80)

    run_start_ts = time.monotonic()
    totals = _RunTotals()

    # Log run start.
    state_mod.log_event(
        spark, meta,
        connector_instance_id=instance_id, run_id=run_id,
        event_type="run_started", severity="INFO",
        message=f"Run started: mode={mode}, run_type={run_type}, trigger={trigger_type}",
    )

    try:
        # Step 5 — Runtime source_config validation.
        try:
            validate_mod.validate_source_config(widget_params, compiled_schema)
        except validate_mod.SourceConfigInvalid as exc:
            state_mod.log_event(
                spark, meta,
                connector_instance_id=instance_id, run_id=run_id,
                event_type="config_validation_failed", severity="SEVERE",
                message=str(exc),
                event_data={"path": exc.path, "rule": exc.rule},
            )
            state_mod.close_run(
                spark, meta,
                run_id=run_id, status="failed",
                totals=totals.as_dict(),
                error_code="config_validation_failed",
                error_message=str(exc),
            )
            raise

        # Step 6 — Resolve secrets and build RunConfig.
        cfg = config_mod.resolve_run_config(widget_params, dbutils=dbutils)

        # Step 7 — Execute the sync body.
        _run_body(
            spark=spark, meta=meta, cfg=cfg,
            instance_id=instance_id, run_id=run_id,
            mode=mode, objects=objects, totals=totals,
        )

        status = "success" if totals.objects_failed == 0 else "failed_with_error"
        state_mod.close_run(
            spark, meta, run_id=run_id, status=status, totals=totals.as_dict(),
            error_code=totals.first_error_code,
            error_message=totals.first_error_message,
        )

    except validate_mod.SourceConfigInvalid:
        raise  # already logged + closed above
    except Exception as exc:
        state_mod.log_event(
            spark, meta,
            connector_instance_id=instance_id, run_id=run_id,
            event_type="run_failed", severity="SEVERE",
            message=f"{type(exc).__name__}: {exc}",
        )
        state_mod.close_run(
            spark, meta, run_id=run_id, status="failed", totals=totals.as_dict(),
            error_code=type(exc).__name__, error_message=str(exc),
        )
        _console("")
        _console("=" * 80)
        _console(
            f"Jetstream Google Drive — run_id={run_id} FAILED "
            f"after {_fmt_duration(time.monotonic() - run_start_ts)}"
        )
        _console(f"  error: {type(exc).__name__}: {exc}")
        _console("=" * 80)
        raise

    _console("")
    _console("=" * 80)
    _console(
        f"Jetstream Google Drive — run_id={run_id} {status.upper()} "
        f"in {_fmt_duration(time.monotonic() - run_start_ts)}"
    )
    _console(
        f"  objects: succeeded={totals.objects_succeeded} "
        f"failed={totals.objects_failed} skipped={totals.objects_skipped}"
    )
    _console(
        f"  rows:    read={totals.rows_ingested_total} "
        f"inserted={totals.rows_inserted} "
        f"updated={totals.rows_updated}"
    )
    _console(f"  bytes:   {_fmt_bytes(totals.bytes_processed)}")
    _console("=" * 80)

    # Log run completion.
    state_mod.log_event(
        spark, meta,
        connector_instance_id=instance_id, run_id=run_id,
        event_type="run_completed", severity="INFO",
        message=(
            f"Run {status}: {totals.objects_succeeded} succeeded, "
            f"{totals.objects_failed} failed, {totals.objects_skipped} skipped. "
            f"Rows: {totals.rows_inserted} inserted, {totals.rows_updated} updated. "
            f"Duration: {_fmt_duration(time.monotonic() - run_start_ts)}"
        ),
    )

    if totals.objects_failed > 0:
        raise RuntimeError(
            f"{totals.objects_failed} object(s) failed "
            f"(succeeded={totals.objects_succeeded}, skipped={totals.objects_skipped}); "
            f"see {meta.qual('connector_log')} run_id={run_id} for details"
        )

    return {
        "run_id": run_id,
        "connector_instance_id": instance_id,
        **totals.as_dict(),
        "objects_succeeded": totals.objects_succeeded,
        "objects_failed": totals.objects_failed,
        "objects_skipped": totals.objects_skipped,
    }


# ---------------------------------------------------------------------------
# Sync body
# ---------------------------------------------------------------------------

def _run_body(
    *,
    spark: Any,
    meta: MetaCtx,
    cfg: RunConfig,
    instance_id: str,
    run_id: str,
    mode: str,
    objects: Optional[Sequence[str]],
    totals: _RunTotals,
) -> None:
    """Core sync logic: iterate over selected objects and extract/load."""
    # Authenticate to Google Drive.
    tokens = GoogleDriveTokens(cfg.source)
    client = DriveClient(cfg.source, tokens)

    # Select objects to process.
    enabled_csv = cfg.enabled_objects_csv if not objects else ",".join(objects)
    selected = objects_mod.select_objects(enabled_csv)

    # Parse folder IDs and file IDs from config.
    folder_ids = [
        fid.strip()
        for fid in cfg.folder_ids_csv.split(",")
        if fid.strip()
    ]
    file_ids = [
        fid.strip()
        for fid in cfg.file_ids_csv.split(",")
        if fid.strip()
    ]

    _console(f"  folder_ids:   {folder_ids or '(all accessible)'}")
    if file_ids:
        _console(f"  file_ids:     {file_ids}")
    _console(f"  objects:      {[o.name for o in selected]}")
    _console("")

    for obj in selected:
        obj_start = time.monotonic()
        _console(f"  [{obj.name}] Starting...")

        try:
            if obj.name == objects_mod.FILES_METADATA:
                _sync_files_metadata(
                    spark=spark, meta=meta, cfg=cfg, client=client,
                    instance_id=instance_id, run_id=run_id,
                    folder_ids=folder_ids, obj=obj, mode=mode, totals=totals,
                )
            elif obj.name == objects_mod.FILES:
                _sync_files(
                    spark=spark, meta=meta, cfg=cfg, client=client,
                    instance_id=instance_id, run_id=run_id,
                    folder_ids=folder_ids, file_ids=file_ids,
                    obj=obj, mode=mode, totals=totals,
                )
            else:
                raise ValueError(f"Unknown object: {obj.name}")

            totals.objects_succeeded += 1
            elapsed = _fmt_duration(time.monotonic() - obj_start)
            _console(f"  [{obj.name}] Completed in {elapsed}")
            state_mod.log_event(
                spark, meta,
                connector_instance_id=instance_id, run_id=run_id,
                event_type="file_download_completed", severity="INFO",
                message=f"{obj.name}: completed in {elapsed}",
            )

        except Exception as exc:
            elapsed = _fmt_duration(time.monotonic() - obj_start)
            _console(f"  [{obj.name}] FAILED after {elapsed}: {exc}")

            if obj.optional:
                totals.objects_skipped += 1
                state_mod.log_event(
                    spark, meta,
                    connector_instance_id=instance_id, run_id=run_id,
                    event_type="object_failed_optional", severity="WARNING",
                    message=f"{obj.name}: {type(exc).__name__}: {exc}",
                )
            else:
                totals.objects_failed += 1
                if totals.first_error_code is None:
                    totals.first_error_code = type(exc).__name__
                    totals.first_error_message = str(exc)[:1000]
                state_mod.log_event(
                    spark, meta,
                    connector_instance_id=instance_id, run_id=run_id,
                    event_type="object_failed", severity="SEVERE",
                    message=f"{obj.name}: {type(exc).__name__}: {exc}",
                )

    # -----------------------------------------------------------------------
    # Stage 2: Parse structured files from Volume → Iceberg tables
    # -----------------------------------------------------------------------
    if cfg.table_mappings:
        _console("")
        _console(f"  [stage2_iceberg] Starting structured file parsing...")
        _console(f"  [stage2_iceberg] Mappings: {len(cfg.table_mappings)}")

        from datetime import date as date_mod
        run_date = date_mod.today().isoformat()

        load_results = load_mod.load_structured_tables(
            spark=spark,
            config=cfg,
            run_date=run_date,
        )

        for lr in load_results:
            obj_start_iso = datetime.now(timezone.utc).isoformat()
            destination_table = (
                f"{cfg.destination_catalog}.{cfg.destination_schema}.{lr.target_table}"
            )

            if lr.success:
                totals.rows_inserted += lr.rows_written
                totals.rows_ingested_total += lr.rows_written
                totals.tables_synced += 1
                totals.objects_succeeded += 1

                state_mod.write_object_run(
                    spark, meta,
                    connector_instance_id=instance_id,
                    run_id=run_id,
                    destination_table=destination_table,
                    source_object=f"stage2:{lr.target_table}",
                    status="success",
                    counts={
                        "rows_read": lr.files_processed,
                        "rows_inserted": lr.rows_written,
                    },
                    start_time_iso=obj_start_iso,
                    extended_metrics={
                        "files_processed": lr.files_processed,
                        "files_skipped": lr.files_skipped,
                        "processed_files": lr.processed_files,
                        "skipped_files": lr.skipped_files,
                    },
                )

                # Register in table registry.
                table_id = state_mod.upsert_table_registry(
                    spark, meta,
                    connector_instance_id=instance_id,
                    destination_catalog=cfg.destination_catalog,
                    destination_schema=cfg.destination_schema,
                    destination_table=lr.target_table,
                    source_object=f"stage2:{lr.target_table}",
                    primary_key_columns=[],
                    row_count_estimate=lr.rows_written,
                    table_format="iceberg",
                )

                # Record schema changes (create_table, add/drop column, type change).
                for sc in lr.schema_changes:
                    state_mod.record_schema_change(
                        spark, meta,
                        table_id=table_id,
                        connector_instance_id=instance_id,
                        change_type=sc["change_type"],
                        column_name=sc.get("column_name"),
                        old_type=sc.get("old_type"),
                        new_type=sc.get("new_type"),
                    )
                if lr.schema_changes:
                    _console(
                        f"  [stage2_iceberg] {lr.target_table}: "
                        f"{len(lr.schema_changes)} schema change(s) recorded"
                    )

                if lr.rows_written == 0 and lr.files_processed == 0:
                    _console(
                        f"  [stage2_iceberg] {lr.target_table}: "
                        f"SKIPPED (no new files in volume path)"
                    )
                    state_mod.log_event(
                        spark, meta,
                        connector_instance_id=instance_id, run_id=run_id,
                        event_type="object_load_skipped", severity="INFO",
                        message=f"{lr.target_table}: skipped (no new files)",
                    )
                else:
                    _console(
                        f"  [stage2_iceberg] {lr.target_table}: "
                        f"{lr.rows_written} rows from {lr.files_processed} files"
                    )
                    state_mod.log_event(
                        spark, meta,
                        connector_instance_id=instance_id, run_id=run_id,
                        event_type="object_load_completed", severity="INFO",
                        message=(
                            f"{lr.target_table}: {lr.rows_written} rows "
                            f"from {lr.files_processed} files"
                        ),
                    )
            else:
                totals.objects_failed += 1
                if totals.first_error_code is None:
                    totals.first_error_code = "Stage2LoadFailed"
                    totals.first_error_message = lr.error_message

                state_mod.write_object_run(
                    spark, meta,
                    connector_instance_id=instance_id,
                    run_id=run_id,
                    destination_table=destination_table,
                    source_object=f"stage2:{lr.target_table}",
                    status="failed",
                    counts={"rows_read": 0},
                    start_time_iso=obj_start_iso,
                )
                state_mod.log_event(
                    spark, meta,
                    connector_instance_id=instance_id, run_id=run_id,
                    event_type="stage2_load_failed", severity="SEVERE",
                    message=f"{lr.target_table}: {lr.error_message}",
                )
                _console(
                    f"  [stage2_iceberg] {lr.target_table}: FAILED — {lr.error_message}"
                )


# ---------------------------------------------------------------------------
# Object: files_metadata
# ---------------------------------------------------------------------------

def _sync_files_metadata(
    *,
    spark: Any,
    meta: MetaCtx,
    cfg: RunConfig,
    client: DriveClient,
    instance_id: str,
    run_id: str,
    folder_ids: List[str],
    obj: ObjectCfg,
    mode: str,
    totals: _RunTotals,
) -> None:
    """Extract Google Drive file/folder metadata into an Iceberg table.

    Incremental on modifiedTime via connector_state cursor.
    """
    obj_start_iso = datetime.now(timezone.utc).isoformat()

    # Read cursor (reset on full_resync).
    cursor_key = "modified_time_cursor"
    if mode == "full_resync":
        state_mod.reset_state(
            spark, meta,
            connector_instance_id=instance_id,
            object_name=obj.name,
            state_key=cursor_key,
        )
        cursor_value = None
    else:
        cursor_value = state_mod.read_state(
            spark, meta,
            connector_instance_id=instance_id,
            object_name=obj.name,
            state_key=cursor_key,
        )

    _console(f"  [{obj.name}] Cursor: {cursor_value or '(full extraction)'}")

    # List metadata from Drive API.
    file_list = extract_mod.list_drive_metadata(
        client, folder_ids, cursor_modified_time=cursor_value
    )
    _console(f"  [{obj.name}] Fetched {len(file_list)} file(s) from Drive API")

    if not file_list:
        # No new data — still record a successful object run.
        state_mod.write_object_run(
            spark, meta,
            connector_instance_id=instance_id,
            run_id=run_id,
            destination_table=f"{cfg.destination_catalog}.{cfg.destination_schema}.{obj.name}",
            source_object=obj.name,
            status="success",
            counts={"rows_read": 0},
            start_time_iso=obj_start_iso,
        )
        return

    # Build rows for Iceberg MERGE.
    destination_table = f"{cfg.destination_catalog}.{cfg.destination_schema}.{obj.name}"
    rows = [
        {
            "file_id": f.file_id,
            "name": f.name,
            "mime_type": f.mime_type,
            "size_bytes": f.size_bytes,
            "created_time": f.created_time,
            "modified_time": f.modified_time,
            "parent_id": f.parent_id,
            "owners": f.owners,
        }
        for f in file_list
    ]

    # Write to Iceberg via MERGE on primary key (file_id).
    rows_inserted, rows_updated = _merge_to_iceberg(
        spark, destination_table, rows, primary_key=["file_id"]
    )

    # Advance cursor to the max modified_time we just processed.
    new_cursor = max(f.modified_time for f in file_list)
    state_mod.upsert_state(
        spark, meta,
        connector_instance_id=instance_id,
        object_name=obj.name,
        state_key=cursor_key,
        state_value=new_cursor,
    )

    # Record object run.
    counts = {
        "rows_read": len(file_list),
        "rows_inserted": rows_inserted,
        "rows_updated": rows_updated,
    }
    state_mod.write_object_run(
        spark, meta,
        connector_instance_id=instance_id,
        run_id=run_id,
        destination_table=destination_table,
        source_object=obj.name,
        status="success",
        counts=counts,
        start_time_iso=obj_start_iso,
    )

    # Update table registry.
    state_mod.upsert_table_registry(
        spark, meta,
        connector_instance_id=instance_id,
        destination_catalog=cfg.destination_catalog,
        destination_schema=cfg.destination_schema,
        destination_table=obj.name,
        source_object=obj.name,
        primary_key_columns=list(obj.primary_key),
        row_count_estimate=None,
        table_format=obj.table_format,
    )

    # Update totals.
    totals.rows_ingested_total += len(file_list)
    totals.rows_inserted += rows_inserted
    totals.rows_updated += rows_updated
    totals.tables_synced += 1

    _console(
        f"  [{obj.name}] Wrote {rows_inserted} new + {rows_updated} updated "
        f"rows to {destination_table}"
    )


# ---------------------------------------------------------------------------
# Object: files (file_copy)
# ---------------------------------------------------------------------------

def _sync_files(
    *,
    spark: Any,
    meta: MetaCtx,
    cfg: RunConfig,
    client: DriveClient,
    instance_id: str,
    run_id: str,
    folder_ids: List[str],
    file_ids: List[str] = None,
    obj: ObjectCfg,
    mode: str,
    totals: _RunTotals,
) -> None:
    """Download whole file bytes and write to UC Volume or S3.

    Incremental on modifiedTime via connector_state cursor.
    The destination is determined by cfg.file_destination (volume or s3).
    """
    obj_start_iso = datetime.now(timezone.utc).isoformat()

    # Validate file destination.
    cfg.file_destination.validate()

    # Read cursor.
    cursor_key = "modified_time_cursor"
    if mode == "full_resync":
        state_mod.reset_state(
            spark, meta,
            connector_instance_id=instance_id,
            object_name=obj.name,
            state_key=cursor_key,
        )
        cursor_value = None
    else:
        cursor_value = state_mod.read_state(
            spark, meta,
            connector_instance_id=instance_id,
            object_name=obj.name,
            state_key=cursor_key,
        )

    _console(f"  [{obj.name}] Cursor: {cursor_value or '(full extraction)'}")

    # List files with paths (for folder hierarchy preservation).
    if folder_ids or file_ids:
        file_list = extract_mod.list_drive_metadata_with_paths(
            client, folder_ids,
            file_ids=file_ids or [],
            cursor_modified_time=cursor_value
        )
    else:
        file_list = extract_mod.list_drive_metadata(
            client, [], cursor_modified_time=cursor_value
        )

    _console(f"  [{obj.name}] Found {len(file_list)} file(s) to download")

    if not file_list:
        # Even with 0 files to download, check if some IDs failed to resolve.
        listing_errors = extract_mod.get_listing_errors()
        if listing_errors:
            msg = (
                f"{len(listing_errors)} Drive ID(s) failed to resolve:\n"
                + "\n".join(f"  \u2022 {e}" for e in listing_errors)
            )
            _console(f"  [{obj.name}] WARNING: {msg}")
            raise extract_mod.DriveListingError(msg)

        state_mod.write_object_run(
            spark, meta,
            connector_instance_id=instance_id,
            run_id=run_id,
            destination_table=cfg.file_destination.destination_label,
            source_object=obj.name,
            status="success",
            counts={"rows_read": 0},
            start_time_iso=obj_start_iso,
        )
        return

    # Prepare S3 client if needed.
    s3_client = None
    if cfg.file_destination.kind == "s3":
        s3_client = extract_mod.create_s3_client(cfg.file_destination)

    success_count = 0
    fail_count = 0
    total_bytes = 0

    for file_meta in file_list:
        payload, result = extract_mod.download_file_bytes(
            client, file_meta,
            file_export_format=cfg.file_export_format,
            max_file_bytes=cfg.max_file_bytes,
        )

        if not result.success or payload is None:
            fail_count += 1
            log.warning(
                "[%s] Failed to download '%s': %s",
                obj.name, file_meta.name, result.error_message,
            )
            continue

        # Write to destination.
        try:
            if cfg.file_destination.kind == "volume":
                # Insert date partition if configured:
                # {volume_path}/{YYYY-MM-DD}/{relative_folder_path}/{file_name}
                base_path = cfg.file_destination.volume_path
                if cfg.file_destination.date_partitioned:
                    base_path = os.path.join(
                        base_path,
                        datetime.now(timezone.utc).strftime("%Y-%m-%d"),
                    )
                extract_mod.write_to_volume(
                    payload,
                    base_path,
                    result.final_file_name or file_meta.name,
                    relative_folder_path=file_meta.relative_folder_path,
                )
            elif cfg.file_destination.kind == "s3":
                extract_mod.write_to_s3(
                    payload,
                    result.final_file_name or file_meta.name,
                    s3_client=s3_client,
                    bucket=cfg.file_destination.s3_bucket,
                    prefix=cfg.file_destination.s3_prefix,
                )
            success_count += 1
            total_bytes += result.bytes_written
        except Exception as exc:
            fail_count += 1
            log.error(
                "[%s] Write failed for '%s': %s",
                obj.name, file_meta.name, exc,
            )

    # Advance cursor.
    if file_list:
        new_cursor = max(f.modified_time for f in file_list)
        state_mod.upsert_state(
            spark, meta,
            connector_instance_id=instance_id,
            object_name=obj.name,
            state_key=cursor_key,
            state_value=new_cursor,
        )

    # Record object run.
    obj_status = "success" if fail_count == 0 else "partial_success"
    counts = {
        "rows_read": len(file_list),
        "rows_inserted": success_count,
        "bytes_written": total_bytes,
    }
    extended_metrics = {
        "files_downloaded": success_count,
        "files_failed": fail_count,
        "destination_type": cfg.file_destination.kind,
        "destination_label": cfg.file_destination.destination_label,
    }
    state_mod.write_object_run(
        spark, meta,
        connector_instance_id=instance_id,
        run_id=run_id,
        destination_table=cfg.file_destination.destination_label,
        source_object=obj.name,
        status=obj_status,
        counts=counts,
        start_time_iso=obj_start_iso,
        extended_metrics=extended_metrics,
    )

    # Update table registry for the file-copy object.
    state_mod.upsert_table_registry(
        spark, meta,
        connector_instance_id=instance_id,
        destination_catalog=cfg.destination_catalog,
        destination_schema=cfg.destination_schema,
        destination_table=cfg.file_destination.destination_label,
        source_object=obj.name,
        primary_key_columns=[],
        row_count_estimate=None,
        table_format=cfg.file_destination.table_format,
    )

    # Update totals.
    totals.rows_ingested_total += len(file_list)
    totals.rows_inserted += success_count
    totals.bytes_processed += total_bytes
    totals.tables_synced += 1

    _console(
        f"  [{obj.name}] Downloaded {success_count}/{len(file_list)} files "
        f"({_fmt_bytes(total_bytes)}) to {cfg.file_destination.destination_label}"
    )

    # Raise if ALL files failed (treat as object failure).
    if success_count == 0 and len(file_list) > 0:
        raise RuntimeError(
            f"All {len(file_list)} file downloads failed for object '{obj.name}'"
        )

    # Check for listing errors (unresolvable IDs). Downloads still happened
    # for valid IDs, but the run must still be marked as failed.
    listing_errors = extract_mod.get_listing_errors()
    if listing_errors:
        msg = (
            f"{len(listing_errors)} Drive ID(s) failed to resolve:\n"
            + "\n".join(f"  \u2022 {e}" for e in listing_errors)
        )
        _console(f"  [{obj.name}] WARNING: {msg}")
        raise extract_mod.DriveListingError(msg)


# ---------------------------------------------------------------------------
# Iceberg MERGE helper
# ---------------------------------------------------------------------------

def _merge_to_iceberg(
    spark: Any,
    table: str,
    rows: List[Dict[str, Any]],
    primary_key: List[str],
) -> tuple:
    """MERGE rows into an Iceberg table. Returns (rows_inserted, rows_updated).

    Creates the table if it doesn't exist (first run).
    """
    if not rows:
        return 0, 0

    from pyspark.sql import Row

    # Build DataFrame from rows.
    df = spark.createDataFrame([Row(**r) for r in rows])
    df.createOrReplaceTempView("__gdrive_staging")

    # Ensure destination table exists.
    _ensure_table_exists(spark, table, df)

    # Count pre-existing matches (for inserted vs updated counts).
    pk_condition = " AND ".join(f"t.{k} = s.{k}" for k in primary_key)
    update_cols = [c for c in df.columns if c not in primary_key]
    update_set = ", ".join(f"t.{c} = s.{c}" for c in update_cols)
    insert_cols = ", ".join(df.columns)
    insert_vals = ", ".join(f"s.{c}" for c in df.columns)

    merge_sql = f"""
    MERGE INTO {table} t
    USING __gdrive_staging s
    ON {pk_condition}
    WHEN MATCHED THEN UPDATE SET {update_set}
    WHEN NOT MATCHED THEN INSERT ({insert_cols}) VALUES ({insert_vals})
    """

    # Get pre-merge row count for matched keys.
    pre_count_sql = f"""
    SELECT COUNT(*) as cnt FROM {table} t
    INNER JOIN __gdrive_staging s ON {pk_condition}
    """
    try:
        pre_count = spark.sql(pre_count_sql).collect()[0]["cnt"]
    except Exception:
        # Table may not exist yet on first run.
        pre_count = 0

    spark.sql(merge_sql)

    rows_updated = pre_count
    rows_inserted = len(rows) - pre_count

    spark.catalog.dropTempView("__gdrive_staging")
    return max(rows_inserted, 0), max(rows_updated, 0)


def _ensure_table_exists(spark: Any, table: str, df: Any) -> None:
    """Create the destination Iceberg table if it doesn't already exist."""
    try:
        spark.sql(f"DESCRIBE TABLE {table}")
    except Exception:
        # Table doesn't exist — create it from the DataFrame schema.
        _console(f"  Creating table {table} (first run)")
        df.limit(0).write.format("delta").mode("overwrite").saveAsTable(table)
