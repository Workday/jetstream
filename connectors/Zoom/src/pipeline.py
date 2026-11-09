"""Top-level orchestration against the shared Jetstream control plane.

Flow for `run(spark, dbutils, mode='run', objects=None)`:
  1. Read Databricks Jobs runtime context.
  2. Read job widget parameters via dbutils.widgets.getAll().
  3. Bootstrap connector_registry + connector_manifest + connector_instance
     (self-registration on first run, refresh on subsequent runs).
  4. Validate source_config against the manifest's JSON Schema.
  5. Open connector_run (idempotent on (instance_id, idempotency_key)).
  6. Authenticate to Zoom, verify granted scopes for MANDATORY objects.
  7. For each selected object (in dependency-safe order):
       - read cursor from connector_state
       - stream pages, write each to Iceberg (real row counts)
       - upsert connector_state, insert connector_object_run,
         upsert connector_table_registry, record schema drift if any
       - optional-object failure: log to connector_log, continue
  8. Close connector_run with aggregate totals and status.

Every ERROR / WARNING / notable event is also appended to connector_log.
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
from src import objects as objects_mod
from src import state as state_mod
from src import write as write_mod
from src.auth import ZoomTokens
from src.config import ObjectCfg, RunConfig
from src.http import HttpClient
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

CONNECTOR_TYPE_ID = "zoom"
CONNECTOR_TYPE_NAME = "Zoom"
CONNECTOR_CATEGORY = "application_api"
CONNECTOR_VERSION = "1.0.0"
CONNECTOR_MAINTAINER = "Jetstream"
CONNECTOR_REPO_PATH = "connectors/zoom"
PARAMETERS_YAML_PATH = os.path.join(
    os.path.dirname(os.path.dirname(os.path.abspath(__file__))),
    "connectors", "zoom", "parameters.yaml",
)
# Human-readable description written to connector_registry.description.
# Object lists are derived from the code catalog so adding a new object to
# src/objects.py automatically shows up here on the next run.
CONNECTOR_DESCRIPTION = (
    "Ingests Zoom accounts data via Zoom OAuth v2 API. "
    f"Primary objects: {', '.join(o.name for o in objects_mod.PRIMARY)}. "
    f"Optional objects: {', '.join(o.name for o in objects_mod.OPTIONAL)}."
)


# ---------------------------------------------------------------------------
# Console helpers — human-readable progress on stdout (visible in the
# Databricks notebook cell output / driver logs). Structured events still
# go to connector_log via state.log_event.
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


# Progress heartbeat during long-running objects. We emit a stdout log
# and a connector_log 'object_progress' event once per
# HEARTBEAT_INTERVAL_SECS wall-clock seconds — fires regardless of page
# count so small-object runs are also visible.
_HEARTBEAT_INTERVAL_SECS = 60

# Number of records buffered before flushing to MERGE. Higher = fewer Spark
# round-trips (each MERGE burns ~5 queries: pre-count, match-count, MERGE,
# post-count, plus schema-diff). Lower = less driver memory per object.
# For 24k users at ~1KB per record → ~5MB buffered. Trivial for the driver.
_MERGE_BATCH_SIZE = 5000


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
    # First mandatory-object failure in this run (if any). Surfaced onto
    # connector_run.error_code / error_message when the run finishes with
    # status='failed_with_error' so ops queries and dashboards see the
    # cause without having to join to connector_object_run.
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
# public entry point
# ---------------------------------------------------------------------------

def run(
    *,
    spark: Any,
    dbutils: Any,
    mode: str = "run",
    objects: Optional[Sequence[str]] = None,
    trigger_type: str = "scheduled",
) -> Dict[str, Any]:
    if mode not in ("run", "full_resync", "discover"):
        raise ValueError(f"unknown mode {mode!r}")
    if mode == "full_resync" and not objects:
        raise ValueError("full_resync requires a non-empty `objects` list")

    # Step 1 — Databricks context + widget parameters
    db_ctx = read_databricks_context(dbutils)
    widget_params = _read_widgets(dbutils)

    # Databricks widgets always return strings; coerce numeric / boolean
    # values to their declared types so downstream validation and the
    # manifest source_config JSON get properly typed values.
    compiled_schema = params_mod.compile_parameters_yaml(PARAMETERS_YAML_PATH)
    widget_params = params_mod.coerce_widget_values(widget_params, compiled_schema)

    # Step 2 — Non-secret routing values (env, catalogs, schemas, instance
    # name, sla, owner). Everything needed by bootstrap and MetaCtx without
    # touching Databricks secrets, so a bad `secret_scope` widget doesn't
    # crash the run before we have a connector_run row to log against.
    routing = config_mod.resolve_platform_routing(widget_params)

    # discover mode short-circuits: it reads Zoom scopes to build the
    # capability report. Needs the full RunConfig (with resolved secrets)
    # but writes NOTHING to the control plane. Safe against any environment.
    if mode == "discover":
        cfg = config_mod.resolve_run_config(widget_params, dbutils=dbutils)
        return _run_discover(cfg)

    meta = resolve_metadata_catalog(routing.environment, routing.metadata_schema, routing.metadata_catalog)

    # Step 3 — Self-bootstrap registry/manifest/instance.
    # Uses widget_params + routing only — no secrets required.
    # (bootstrap re-compiles the schema internally; we already have
    # `compiled_schema` from the top of run() and use it for validation below.)
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
        source_config=config_mod.source_config_for_manifest(widget_params),
        destination_catalog=routing.destination_catalog,
        destination_schema=routing.destination_schema,
        instance_name=routing.instance_name,
        environment=routing.environment,
        sla_target_minutes=routing.sla_target_minutes,
        owner_team=routing.owner_team,
        db_ctx=db_ctx,
    )

    # Step 4 — Open connector_run (idempotent on (instance_id, idempotency_key))
    idempotency_key = db_ctx.job_run_id
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
    _console(f"Jetstream Zoom — run_id={run_id}")
    _console(f"  instance:     {routing.instance_name} ({routing.environment})")
    _console(f"  destination:  {routing.destination_catalog}.{routing.destination_schema}")
    _console(f"  metadata:     {meta.catalog}.{meta.schema}")
    _console(f"  mode:         {mode} ({run_type}) / trigger={trigger_type}")
    _console("=" * 80)

    run_start_ts = time.monotonic()
    # From here every error path lands in connector_log + close_run.
    # This includes source_config validation, secret resolution
    # (bad secret_scope, missing keys), and the actual sync body.
    totals = _RunTotals()
    try:
        # Step 5 — Runtime source_config validation (spec §7 "Runtime Validation")
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
                totals=totals.as_dict(), error_code="config_validation_failed",
                error_message=str(exc),
            )
            raise

        # Step 6 — Resolve secrets NOW (bad secret_scope surfaces here inside
        # the outer try/except so it's captured in connector_log).
        cfg = config_mod.resolve_run_config(widget_params, dbutils=dbutils)

        # Step 7 — Actual sync body.
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
    # Catch-all is intentional: converts any failure into connector_log +
    # connector_run.error_* audit rows so ops can see what broke.
    except Exception as exc:  # NOSONAR
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
        _console(f"Jetstream Zoom — run_id={run_id} FAILED after {_fmt_duration(time.monotonic() - run_start_ts)}")
        _console(f"  error: {type(exc).__name__}: {exc}")
        _console("=" * 80)
        raise

    # Surface mandatory-object failures to the Databricks Job scheduler.
    # `connector_run.status='failed_with_error'` alone isn't visible to
    # ops paging — we need the notebook cell to raise so the Job's
    # own status turns red and any configured alerts fire.
    # Optional-object failures don't count here (spec §11 — best-effort
    # by design, tracked in objects_skipped instead).
    _console("")
    _console("=" * 80)
    _console(
        f"Jetstream Zoom — run_id={run_id} {status.upper()} "
        f"in {_fmt_duration(time.monotonic() - run_start_ts)}"
    )
    _console(
        f"  objects: succeeded={totals.objects_succeeded} "
        f"failed={totals.objects_failed} skipped={totals.objects_skipped}"
    )
    _console(
        f"  rows:    read={totals.rows_ingested_total} "
        f"inserted={totals.rows_inserted} "
        f"updated={totals.rows_updated} "
        f"deleted={totals.rows_deleted}"
    )
    _console(f"  bytes:   {_fmt_bytes(totals.bytes_processed)}")
    _console("=" * 80)

    if totals.objects_failed > 0:
        raise RuntimeError(
            f"{totals.objects_failed} mandatory object(s) failed "
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
# body
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
    tokens = ZoomTokens(cfg.source)
    http = HttpClient(cfg.source, token_provider=tokens)

    granted = tokens.granted_scopes()

    # Start-of-run capability report — also emitted to connector_log so the
    # "what will this run touch" answer is queryable per run.
    report = objects_mod.build_capability_report(
        granted_scopes=granted,
        enabled_primary_csv=cfg.enabled_objects_csv,
        enabled_optional_csv=cfg.optional_objects_enabled_csv,
    )
    log.info("\n%s", objects_mod.format_capability_report(report))
    state_mod.log_event(
        spark, meta,
        connector_instance_id=instance_id, run_id=run_id,
        event_type="run_plan", severity="INFO",
        message=(
            f"will_sync={report['will_sync']} "
            f"granted={len(report['granted_scopes'])} "
            f"catalog_size={report['code_catalog_size']}"
        ),
        event_data=report,
    )

    plan = objects_mod.select_objects(
        cfg.enabled_objects_csv, cfg.optional_objects_enabled_csv,
    )
    if mode == "full_resync":
        wanted = set(objects or ())
        unknown = wanted - {o.name for o in plan}
        if unknown:
            raise ValueError(
                f"full_resync objects {sorted(unknown)} not in this instance's enabled plan"
            )
        # Fan-out children need their ancestor parents to run in the same
        # execution so parent_id_cache is populated. Auto-include the
        # transitive parent chain (rooms for room_devices; users -> meetings
        # for past_meeting_participants). Ancestors run without cursor reset;
        # cursors are reset only for the explicitly-listed `objects`.
        by_name = {o.name: o for o in plan}
        expanded = set(wanted)
        stack = list(wanted)
        while stack:
            name = stack.pop()
            obj = by_name.get(name)
            if obj and obj.parent and obj.parent not in expanded:
                expanded.add(obj.parent)
                stack.append(obj.parent)
        plan = [o for o in plan if o.name in expanded]
        for name in wanted:
            state_mod.reset_state(
                spark, meta,
                connector_instance_id=instance_id,
                object_name=name,
                state_key="cursor",
            )

    # Mandatory-scope gate (fail-fast on non-optional missing scopes).
    missing = extract_mod.check_required_scopes(plan, granted)
    if missing:
        raise RuntimeError(
            f"missing OAuth scopes for mandatory objects {missing}. "
            f"Granted: {sorted(granted)}"
        )

    parent_id_cache: Dict[str, List[str]] = {}
    _console(f"Plan ({len(plan)} objects): {', '.join(o.name for o in plan)}")
    _console("-" * 80)
    for i, obj in enumerate(plan, start=1):
        _run_one_object(
            spark=spark, meta=meta, cfg=cfg,
            instance_id=instance_id, run_id=run_id,
            obj=obj, http=http,
            parent_id_cache=parent_id_cache,
            totals=totals,
            granted_scopes=granted,
            obj_index=i,
            obj_total=len(plan),
        )


# ---------------------------------------------------------------------------
# per-object loop
# ---------------------------------------------------------------------------

def _run_one_object(
    *,
    spark: Any,
    meta: MetaCtx,
    cfg: RunConfig,
    instance_id: str,
    run_id: str,
    obj: ObjectCfg,
    http: HttpClient,
    parent_id_cache: Dict[str, List[str]],
    totals: _RunTotals,
    granted_scopes: set,
    obj_index: int = 1,
    obj_total: int = 1,
) -> None:
    start_iso = _utcnow_iso()
    obj_start_ts = time.monotonic()
    dest_table = write_mod.destination_table_name(obj)

    # Optional-object scope gate — skip with a WARN log (not a run failure).
    if obj.optional and obj.required_scope and obj.required_scope not in granted_scopes:
        totals.objects_skipped += 1
        _console(
            f"\n⊘ [{obj_index}/{obj_total}] {obj.name} SKIPPED — "
            f"required_scope not granted: {obj.required_scope}"
        )
        state_mod.log_event(
            spark, meta,
            connector_instance_id=instance_id, run_id=run_id,
            event_type="optional_object_skipped", severity="WARNING",
            message=f"required_scope not granted for optional object {obj.name}",
            event_data={"object": obj.name, "required_scope": obj.required_scope},
        )
        state_mod.write_object_run(
            spark, meta,
            connector_instance_id=instance_id, run_id=run_id,
            destination_table=dest_table, source_object=obj.name,
            status="skipped", counts={"rows_read": 0},
            start_time_iso=start_iso,
            error_message=f"scope not granted: {obj.required_scope}",
        )
        return

    last_wm = state_mod.read_state(
        spark, meta,
        connector_instance_id=instance_id, object_name=obj.name, state_key="cursor",
    )
    parent_ids = parent_id_cache.get(obj.parent) if obj.parent else None
    max_cursor_seen = last_wm

    tier = "optional" if obj.optional else "primary"
    fanout_suffix = (
        f", fan-out over {len(parent_ids)} {obj.parent}"
        if obj.parent and parent_ids is not None else ""
    )
    _console(
        f"\n▶ [{obj_index}/{obj_total}] {obj.name} "
        f"({tier}, {obj.load_strategy}{fanout_suffix})"
    )
    counts = {"rows_read": 0, "rows_inserted": 0, "rows_updated": 0,
              "rows_deleted": 0, "rows_unchanged": 0, "bytes_written": 0}
    all_records_for_finalise: List[dict] = []
    saw_first_page = False
    table_id: Optional[str] = None
    pages_seen = 0
    parents_total = len(parent_ids) if parent_ids is not None else None
    heartbeat_last_ts = time.monotonic()

    # Buffer records across pages and MERGE only when it fills up. Cuts the
    # per-page Spark overhead (5 queries per MERGE) by up to ~1000x on
    # objects with many tiny pages (e.g. room_devices fan-out).
    batch_buffer: List[dict] = []

    def _flush_batch() -> None:
        """MERGE whatever's in batch_buffer into the destination and reset."""
        nonlocal saw_first_page, table_id
        if not batch_buffer:
            return
        wr = write_mod.write_page(
            spark,
            destination_catalog=cfg.destination_catalog,
            destination_schema=cfg.destination_schema,
            obj=obj,
            records=batch_buffer,
            source_object=obj.name,
        )
        saw_first_page = True
        counts["rows_inserted"] += wr.rows_inserted
        counts["rows_updated"] += wr.rows_updated
        counts["rows_deleted"] += wr.rows_deleted
        counts["rows_unchanged"] += wr.rows_unchanged

        # Register / refresh the destination table row on first flush.
        if table_id is None:
            table_id = state_mod.upsert_table_registry(
                spark, meta,
                connector_instance_id=instance_id,
                destination_catalog=cfg.destination_catalog,
                destination_schema=cfg.destination_schema,
                destination_table=dest_table,
                source_object=obj.name,
                primary_key_columns=obj.primary_key,
                row_count_estimate=None,
            )
        for change in wr.schema_changes:
            state_mod.record_schema_change(
                spark, meta,
                table_id=table_id,
                connector_instance_id=instance_id,
                change_type=change.change_type,
                column_name=change.column_name,
                new_type=change.new_type,
            )
            state_mod.log_event(
                spark, meta,
                connector_instance_id=instance_id, run_id=run_id,
                event_type="schema_change_detected", severity="WARNING",
                message=f"{change.change_type} on {obj.name}: {change.column_name}",
                event_data={"object": obj.name, "column": change.column_name, "new_type": change.new_type},
            )
        batch_buffer.clear()

    try:
        for page in extract_mod.extract(
            obj, cfg.source, http,
            last_watermark=last_wm, parent_ids=parent_ids,
        ):
            records = page["records"]
            counts["rows_read"] += len(records)
            counts["bytes_written"] += int(page.get("bytes", 0) or 0)
            pages_seen += 1

            batch_buffer.extend(records)
            if len(batch_buffer) >= _MERGE_BATCH_SIZE:
                _flush_batch()

            if page.get("max_cursor"):
                max_cursor_seen = _max_str(max_cursor_seen, page["max_cursor"])

            # Cache primary-key values for any downstream fan-out.
            if any(c.parent == obj.name for c in objects_mod.PRIMARY + objects_mod.OPTIONAL):
                spec = extract_mod.ENDPOINTS.get(obj.name) or {}
                pid_field = spec.get("parent_id_field") or "id"
                parent_id_cache.setdefault(obj.name, []).extend(
                    str(r[pid_field]) for r in records if r.get(pid_field) is not None
                )
            if obj.load_strategy == "full":
                all_records_for_finalise.extend(records)

            # Time-based heartbeat: fires every HEARTBEAT_INTERVAL_SECS of
            # wall clock, regardless of page count. Small objects that never
            # cross a page threshold still get at least one progress ping
            # if they take longer than the interval.
            now_ts = time.monotonic()
            if now_ts - heartbeat_last_ts >= _HEARTBEAT_INTERVAL_SECS:
                progress_label = (
                    f"{pages_seen}/{parents_total} parents"
                    if parents_total is not None
                    else f"{pages_seen} pages"
                )
                _console(
                    f"  … {obj.name}: {progress_label}, "
                    f"read={counts['rows_read']}, "
                    f"inserted={counts['rows_inserted']}, "
                    f"updated={counts['rows_updated']}, "
                    f"bytes={_fmt_bytes(counts['bytes_written'])}"
                )
                log.info(
                    "progress: object=%s %s rows_read=%d rows_inserted=%d "
                    "rows_updated=%d bytes=%d",
                    obj.name, progress_label,
                    counts["rows_read"], counts["rows_inserted"],
                    counts["rows_updated"], counts["bytes_written"],
                )
                state_mod.log_event(
                    spark, meta,
                    connector_instance_id=instance_id, run_id=run_id,
                    event_type="object_progress", severity="INFO",
                    message=(
                        f"{obj.name}: {progress_label}, "
                        f"rows_read={counts['rows_read']}, "
                        f"rows_inserted={counts['rows_inserted']}, "
                        f"rows_updated={counts['rows_updated']}"
                    ),
                    event_data={
                        "object": obj.name,
                        "pages_seen": pages_seen,
                        "parents_total": parents_total,
                        **counts,
                    },
                )
                heartbeat_last_ts = now_ts

        # Flush any trailing records that didn't fill the last batch.
        _flush_batch()

        if obj.load_strategy == "full":
            tombstoned = write_mod.finalise_full(
                spark,
                destination_catalog=cfg.destination_catalog,
                destination_schema=cfg.destination_schema,
                obj=obj,
                all_records=all_records_for_finalise,
            )
            counts["rows_deleted"] += tombstoned

        state_mod.upsert_state(
            spark, meta,
            connector_instance_id=instance_id,
            object_name=obj.name, state_key="cursor",
            state_value=max_cursor_seen,
        )
        # Refresh row_count_estimate now that we've written the object.
        if table_id is not None:
            row_count = spark.sql(
                f"SELECT COUNT(*) AS c FROM `{cfg.destination_catalog}`.`{cfg.destination_schema}`.`{dest_table}`"
            ).collect()[0]["c"]
            state_mod.upsert_table_registry(
                spark, meta,
                connector_instance_id=instance_id,
                destination_catalog=cfg.destination_catalog,
                destination_schema=cfg.destination_schema,
                destination_table=dest_table,
                source_object=obj.name,
                primary_key_columns=obj.primary_key,
                row_count_estimate=int(row_count),
            )
        totals.objects_succeeded += 1
        totals.tables_synced += 1
        _accumulate(totals, counts)
        state_mod.write_object_run(
            spark, meta,
            connector_instance_id=instance_id, run_id=run_id,
            destination_table=dest_table, source_object=obj.name,
            status="success", counts=counts,
            start_time_iso=start_iso, end_time_iso=_utcnow_iso(),
        )
        _console(
            f"✓ [{obj_index}/{obj_total}] {obj.name} success "
            f"in {_fmt_duration(time.monotonic() - obj_start_ts)} | "
            f"read={counts['rows_read']} "
            f"inserted={counts['rows_inserted']} "
            f"updated={counts['rows_updated']} "
            f"deleted={counts['rows_deleted']} "
            f"bytes={_fmt_bytes(counts['bytes_written'])}"
        )

    # Per-object catch-all is intentional: lets us classify optional vs
    # mandatory failure, keep the run going for optionals, and record the
    # exception on connector_object_run + connector_log.
    except Exception as exc:  # NOSONAR
        error = f"{type(exc).__name__}: {exc}"
        state_mod.log_event(
            spark, meta,
            connector_instance_id=instance_id, run_id=run_id,
            event_type=("optional_object_failed" if obj.optional else "object_failed"),
            severity=("WARNING" if obj.optional else "SEVERE"),
            message=f"{obj.name}: {error}",
            event_data={"object": obj.name},
        )
        state_mod.write_object_run(
            spark, meta,
            connector_instance_id=instance_id, run_id=run_id,
            destination_table=dest_table, source_object=obj.name,
            status="failed", counts=counts,
            start_time_iso=start_iso, end_time_iso=_utcnow_iso(),
            error_message=error,
        )
        _accumulate(totals, counts)
        marker = "⊘" if obj.optional else "✗"
        outcome = "skipped (optional)" if obj.optional else "FAILED"
        _console(
            f"{marker} [{obj_index}/{obj_total}] {obj.name} {outcome} "
            f"in {_fmt_duration(time.monotonic() - obj_start_ts)}: {error}"
        )
        if obj.optional:
            totals.objects_skipped += 1
            log.warning("optional object %s failed — continuing: %s", obj.name, error)
        else:
            totals.objects_failed += 1
            # Capture only the FIRST mandatory failure so a cascading run
            # doesn't overwrite the true root cause with a downstream symptom.
            if totals.first_error_code is None:
                totals.first_error_code = type(exc).__name__
                totals.first_error_message = f"{obj.name}: {exc}"
            log.exception("object %s failed", obj.name)


# ---------------------------------------------------------------------------
# helpers
# ---------------------------------------------------------------------------

def _read_widgets(dbutils: Any) -> Dict[str, Any]:
    if dbutils is None:
        return {}
    try:
        return dict(dbutils.widgets.getAll())
    # Older DBR versions expose only .get(); silent fallback is intentional.
    except Exception:  # NOSONAR
        # Older DBR versions expose only .get(); fall back to no bulk read.
        return {}


def _accumulate(totals: _RunTotals, counts: Dict[str, int]) -> None:
    totals.rows_ingested_total += counts.get("rows_read", 0)
    totals.rows_inserted += counts.get("rows_inserted", 0)
    totals.rows_updated += counts.get("rows_updated", 0)
    totals.rows_deleted += counts.get("rows_deleted", 0)
    totals.bytes_processed += counts.get("bytes_written", 0)


def _max_str(a: Optional[str], b: Optional[str]) -> Optional[str]:
    if a is None:
        return b
    if b is None:
        return a
    return max(a, b)


def _utcnow_iso() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


# ---------------------------------------------------------------------------
# discover mode — read-only capability probe, no control-plane / destination writes
# ---------------------------------------------------------------------------

def _run_discover(cfg: RunConfig) -> Dict[str, Any]:
    """Fetch Zoom's granted scopes, cross-reference with the code catalog,
    print a human-readable report, and return the structured report.

    Does NOT read or write any control-plane table. Does NOT touch destination.
    Safe to run against any environment, including prod job params, without
    leaving a footprint.
    """
    tokens = ZoomTokens(cfg.source)
    granted = tokens.granted_scopes()
    report = objects_mod.build_capability_report(
        granted_scopes=granted,
        enabled_primary_csv=cfg.enabled_objects_csv,
        enabled_optional_csv=cfg.optional_objects_enabled_csv,
    )
    print(objects_mod.format_capability_report(report))
    return {"mode": "discover", **report}
