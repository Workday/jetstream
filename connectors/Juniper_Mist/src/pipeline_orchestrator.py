"""Top-level orchestration against the shared Jetstream control plane.

Flow for `run(spark, dbutils, mode='run', objects=None)`:
  1. Read Databricks Jobs runtime context.
  2. Read job widget parameters via dbutils.widgets.getAll().
  3. Bootstrap connector_registry + connector_manifest + connector_instance.
  4. Validate source_config against the manifest's JSON Schema.
  5. Open connector_run (idempotent on (instance_id, idempotency_key)).
  6. Build plan, populate parent_id_cache starting with `sites`.
  7. For each selected object (dependency-safe order):
       - read cursor from connector_state
       - stream pages, write each batch to Iceberg (real row counts)
       - upsert connector_state, insert connector_object_run,
         upsert connector_table_registry, record schema drift if any
       - optional-object failure: WARNING log, continue
  8. Close connector_run with aggregate totals and status.

Every ERROR / WARNING / notable event is appended to connector_log.
"""
from __future__ import annotations

import logging
import os
import time
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional, Sequence

from src import api_objects as objects_mod
from src import api_extractor as extract_mod
from src import connector_config as config_mod
from src import control_plane_writer as cp_mod
from src import destination_writer as dest_mod
from src.api_auth import MistTokenProvider
from src.connector_config import ObjectCfg, RunConfig
from src.http_client import HttpClient
from src.jetstream import platform_bootstrap as bootstrap_mod
from src.jetstream import parameter_compiler as params_mod
from src.jetstream import config_validator as validate_mod
from src.jetstream.databricks_context import (
    DatabricksContext,
    MetaCtx,
    read_databricks_context,
    resolve_metadata_catalog,
)


log = logging.getLogger(__name__)

CONNECTOR_TYPE_ID = "juniper_mist"
CONNECTOR_TYPE_NAME = "Juniper Mist"
CONNECTOR_CATEGORY = "application_api"
CONNECTOR_VERSION = "1.0.0"
CONNECTOR_MAINTAINER = "Jetstream"
CONNECTOR_REPO_PATH = "connectors/juniper_mist"
PARAMETERS_YAML_PATH = os.path.join(
    os.path.dirname(os.path.dirname(os.path.abspath(__file__))),
    "connectors", "juniper_mist", "parameters.yaml",
)
CONNECTOR_DESCRIPTION = (
    "Ingests Juniper Mist building-occupancy data via the Mist REST API. "
    f"Primary objects: {', '.join(o.name for o in objects_mod.PRIMARY)}. "
    f"Optional objects: {', '.join(o.name for o in objects_mod.OPTIONAL)}."
)

# Records buffered before flushing to a MERGE. Higher = fewer Spark round-trips.
_MERGE_BATCH_SIZE = 5000
# Emit a heartbeat log every N seconds regardless of page count.
_HEARTBEAT_INTERVAL_SECS = 60


# ---------------------------------------------------------------------------
# Console helpers
# ---------------------------------------------------------------------------

def _console(msg: str) -> None:
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
# Public entry point
# ---------------------------------------------------------------------------

def run(
    *,
    spark: Any,
    dbutils: Any,
    mode: str = "run",
    objects: Optional[Sequence[str]] = None,
) -> Dict[str, Any]:
    if mode not in ("run", "full_resync", "discover"):
        raise ValueError(f"unknown mode {mode!r}")
    if mode == "full_resync" and not objects:
        raise ValueError("full_resync requires a non-empty `objects` list")

    db_ctx = read_databricks_context(dbutils)
    trigger_type = db_ctx.trigger_type
    widget_params = _read_widgets(dbutils)

    compiled_schema = params_mod.compile_parameters_yaml(PARAMETERS_YAML_PATH)
    widget_params = params_mod.coerce_widget_values(widget_params, compiled_schema)

    routing = config_mod.resolve_platform_routing(widget_params)

    if mode == "discover":
        cfg = config_mod.resolve_run_config(widget_params, dbutils=dbutils)
        return _run_discover(cfg)

    meta = resolve_metadata_catalog(
        routing.environment, routing.metadata_schema, routing.metadata_catalog
    )

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

    idempotency_key = db_ctx.job_run_id
    run_type = "full_resync" if mode == "full_resync" else "incremental"
    run_id, is_new_run = cp_mod.open_run(
        spark, meta,
        connector_instance_id=instance_id,
        idempotency_key=idempotency_key,
        run_type=run_type,
        trigger_type=trigger_type,
        db_ctx=db_ctx,
    )
    log.info("run_id=%s instance=%s new=%s", run_id, instance_id, is_new_run)

    _console("=" * 80)
    _console(f"Jetstream Juniper Mist — run_id={run_id}")
    _console(f"  instance:     {routing.instance_name} ({routing.environment})")
    _console(f"  destination:  {routing.destination_catalog}.{routing.destination_schema}")
    _console(f"  metadata:     {meta.catalog}.{meta.schema}")
    _console(f"  mode:         {mode} ({run_type}) / trigger={trigger_type}")
    _console("=" * 80)

    run_start_ts = time.monotonic()
    totals = _RunTotals()
    status = "success"
    try:
        try:
            validate_mod.validate_source_config(widget_params, compiled_schema)
        except validate_mod.SourceConfigInvalid as exc:
            cp_mod.log_event(
                spark, meta,
                connector_instance_id=instance_id, run_id=run_id,
                event_type="config_validation_failed", severity="SEVERE",
                message=str(exc),
                event_data={"path": exc.path, "rule": exc.rule},
            )
            cp_mod.close_run(
                spark, meta, run_id=run_id, status="failed",
                totals=totals.as_dict(), error_code="config_validation_failed",
                error_message=str(exc),
            )
            raise

        cfg = config_mod.resolve_run_config(widget_params, dbutils=dbutils)

        _run_body(
            spark=spark, meta=meta, cfg=cfg,
            instance_id=instance_id, run_id=run_id,
            mode=mode, objects=objects, totals=totals,
        )
        status = "success" if totals.objects_failed == 0 else "failed_with_error"
        cp_mod.close_run(
            spark, meta, run_id=run_id, status=status, totals=totals.as_dict(),
            error_code=totals.first_error_code,
            error_message=totals.first_error_message,
        )
    except validate_mod.SourceConfigInvalid:
        raise
    except Exception as exc:  # NOSONAR
        cp_mod.log_event(
            spark, meta,
            connector_instance_id=instance_id, run_id=run_id,
            event_type="run_failed", severity="SEVERE",
            message=f"{type(exc).__name__}: {exc}",
        )
        cp_mod.close_run(
            spark, meta, run_id=run_id, status="failed", totals=totals.as_dict(),
            error_code=type(exc).__name__, error_message=str(exc),
        )
        _console(f"\nJetstream Juniper Mist — run_id={run_id} FAILED: {exc}")
        raise

    _console("")
    _console("=" * 80)
    _console(
        f"Jetstream Juniper Mist — run_id={run_id} {status.upper()} "
        f"in {_fmt_duration(time.monotonic() - run_start_ts)}"
    )
    _console(
        f"  objects: succeeded={totals.objects_succeeded} "
        f"failed={totals.objects_failed} skipped={totals.objects_skipped}"
    )
    _console(
        f"  rows:    read={totals.rows_ingested_total} "
        f"inserted={totals.rows_inserted} updated={totals.rows_updated} "
        f"deleted={totals.rows_deleted}"
    )
    _console(f"  bytes:   {_fmt_bytes(totals.bytes_processed)}")
    _console("=" * 80)

    if totals.objects_failed > 0:
        raise RuntimeError(
            f"{totals.objects_failed} mandatory object(s) failed "
            f"(succeeded={totals.objects_succeeded}, skipped={totals.objects_skipped}); "
            f"see {meta.qual('connector_log')} run_id={run_id}"
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
# Body
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
    token_provider = MistTokenProvider(cfg.source.api_token)
    http = HttpClient(cfg.source, token_provider=token_provider)

    report = objects_mod.build_capability_report(cfg.enabled_objects_csv)
    log.info("\n%s", objects_mod.format_capability_report(report))
    cp_mod.log_event(
        spark, meta,
        connector_instance_id=instance_id, run_id=run_id,
        event_type="run_plan", severity="INFO",
        message=(
            f"will_sync={report['will_sync']} "
            f"catalog_size={report['code_catalog_size']}"
        ),
        event_data=report,
    )

    plan = objects_mod.select_objects(cfg.enabled_objects_csv)

    if mode == "full_resync":
        wanted = set(objects or ())
        unknown = wanted - {o.name for o in plan}
        if unknown:
            raise ValueError(
                f"full_resync objects {sorted(unknown)} not in this instance's enabled plan"
            )
        # Auto-include transitive parents so fan-out children can populate their caches.
        by_name = {o.name: o for o in plan}
        expanded = set(wanted)
        stack = list(wanted)
        while stack:
            name = stack.pop()
            obj = by_name.get(name)
            if obj and obj.parent and obj.parent not in expanded:
                # Resolve synthetic parents (sites_filtered → sites).
                real_parent = _resolve_real_parent(obj.parent)
                if real_parent and real_parent not in expanded:
                    expanded.add(real_parent)
                    stack.append(real_parent)
        plan = [o for o in plan if o.name in expanded]
        for name in wanted:
            cp_mod.reset_state(
                spark, meta,
                connector_instance_id=instance_id,
                object_name=name, state_key="cursor",
            )

    # Mist is token-auth; no OAuth scope gate needed.

    parent_id_cache: Dict[str, List[Any]] = {}
    _console(f"Plan ({len(plan)} objects): {', '.join(o.name for o in plan)}")
    _console("-" * 80)

    for i, obj in enumerate(plan, start=1):
        _run_one_object(
            spark=spark, meta=meta, cfg=cfg,
            instance_id=instance_id, run_id=run_id,
            obj=obj, http=http,
            parent_id_cache=parent_id_cache,
            totals=totals,
            obj_index=i,
            obj_total=len(plan),
        )

        # After syncing `sites`, build both the full and filtered parent caches.
        if obj.name == "sites":
            _build_site_caches(spark, cfg, parent_id_cache)


def _restore_parent_cache_from_table(
    spark: Any,
    cfg: RunConfig,
    obj: ObjectCfg,
    parent_id_cache: Dict,
) -> None:
    """Repopulate parent_id_cache for a full-load object that was interval-skipped.

    When an object is skipped the API is not called, so _update_parent_cache
    never fires.  If the object is used as a parent by other objects its cache
    entry would remain empty — causing all fan-out children to see 0 parent IDs
    and produce no rows.

    This function reads the last-synced (non-tombstoned) rows from the
    destination Iceberg table and calls parent_cache_entries() exactly as the
    live extraction loop would, so downstream children behave identically
    whether the parent ran this trigger or was skipped.
    """
    # Determine whether any other object treats this one as a parent.
    is_a_parent = any(
        c.parent == obj.name
        for c in (*objects_mod.PRIMARY, *objects_mod.OPTIONAL)
        if c.parent is not None
    )
    # 'sites_filtered' is a synthetic parent whose real source is 'sites'.
    is_sites = obj.name == "sites"

    if not is_a_parent and not is_sites:
        return  # not a parent — nothing to restore

    cat = cfg.destination_catalog
    schema = cfg.destination_schema
    full_name = f"{cat}.{schema}.{obj.name}"

    if not spark.catalog.tableExists(full_name):
        log.warning(
            "interval-skip parent restore: table %s not found; "
            "fan-out children will have no parent IDs this run",
            obj.name,
        )
        return

    try:
        rows = spark.sql(
            f"SELECT * FROM `{cat}`.`{schema}`.`{obj.name}` "  # NOSONAR
            f"WHERE _jetstream_deleted = FALSE"
        ).collect()
        records = [r.asDict() for r in rows]
        entries = extract_mod.parent_cache_entries(obj.name, records)
        parent_id_cache.setdefault(obj.name, []).extend(entries)
        log.info(
            "interval-skip parent restore: %s — loaded %d entries from destination table",
            obj.name, len(entries),
        )
    except Exception as exc:  # NOSONAR
        log.warning(
            "interval-skip parent restore for %s failed (%s); "
            "fan-out children will have no parent IDs this run",
            obj.name, exc,
        )


def _resolve_real_parent(parent_key: Optional[str]) -> Optional[str]:
    """Map synthetic parent keys to their real object names.

    `sites_filtered` is a synthetic key: its real source is the `sites` object.
    """
    _synthetic = {"sites_filtered": "sites"}
    return _synthetic.get(parent_key, parent_key)


def _build_site_caches(spark: Any, cfg: RunConfig, parent_id_cache: Dict) -> None:
    """After the `sites` object has been synced, populate parent_id_cache['sites_filtered'].

    Reads site id + name from the destination table and applies site_filter.
    Falls back to all sites if the filter matches nothing.
    """
    all_site_ids = parent_id_cache.get("sites", [])
    if not cfg.source.site_filter_substrings:
        parent_id_cache["sites_filtered"] = all_site_ids
        return

    try:
        cat = cfg.destination_catalog
        schema = cfg.destination_schema
        rows = spark.sql(
            f"SELECT id, name FROM `{cat}`.`{schema}`.`sites` WHERE _jetstream_deleted = false"
        ).collect()
        site_name_by_id = {str(r["id"]): (r["name"] or "").lower() for r in rows}
        filtered = [
            sid for sid in all_site_ids
            if any(f in site_name_by_id.get(sid, "") for f in cfg.source.site_filter_substrings)
        ]
        parent_id_cache["sites_filtered"] = filtered or all_site_ids
        log.info(
            "site_filter applied: %d/%d sites selected for history fan-out",
            len(parent_id_cache["sites_filtered"]), len(all_site_ids),
        )
    except Exception as exc:  # NOSONAR
        log.warning("site_filter lookup failed (%s); using all sites", exc)
        parent_id_cache["sites_filtered"] = all_site_ids


# ---------------------------------------------------------------------------
# Per-object loop
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
    parent_id_cache: Dict[str, List[Any]],
    totals: _RunTotals,
    obj_index: int = 1,
    obj_total: int = 1,
) -> None:
    # ------------------------------------------------------------------
    # Interval skip: full-load objects are expensive to re-run hourly.
    # If full_load_interval_hours > 0 and this object ran successfully
    # less than that many hours ago, skip it for this trigger.
    # snapshot and incremental_cursor objects are never subject to this.
    # ------------------------------------------------------------------
    if obj.load_strategy == "full" and cfg.source.full_load_interval_hours > 0:
        last_success_dt = cp_mod.read_last_object_success_time(
            spark, meta,
            connector_instance_id=instance_id,
            object_name=obj.name,
        )
        if last_success_dt is not None:
            try:
                # Spark returns either a datetime or a string depending on the
                # Iceberg table's timestamp representation; normalise to UTC datetime.
                if hasattr(last_success_dt, "tzinfo"):
                    from datetime import timezone as _tz
                    if last_success_dt.tzinfo is None:
                        last_success_dt = last_success_dt.replace(tzinfo=_tz.utc)
                    last_success_utc = last_success_dt
                else:
                    last_success_utc = datetime.strptime(
                        str(last_success_dt)[:19], "%Y-%m-%d %H:%M:%S"
                    ).replace(tzinfo=timezone.utc)
                hours_since = (
                    datetime.now(timezone.utc) - last_success_utc
                ).total_seconds() / 3600
                if hours_since < cfg.source.full_load_interval_hours:
                    _console(
                        f"⏭ [{obj_index}/{obj_total}] {obj.name} "
                        f"skipped — ran {hours_since:.1f}h ago "
                        f"(interval={cfg.source.full_load_interval_hours}h)"
                    )
                    log.info(
                        "interval skip: %s last success %.1fh ago < %dh interval",
                        obj.name, hours_since, cfg.source.full_load_interval_hours,
                    )
                    cp_mod.log_event(
                        spark, meta,
                        connector_instance_id=instance_id, run_id=run_id,
                        event_type="object_interval_skipped", severity="INFO",
                        message=(
                            f"{obj.name}: skipped, last success {hours_since:.1f}h ago "
                            f"(interval={cfg.source.full_load_interval_hours}h)"
                        ),
                        event_data={
                            "object": obj.name,
                            "hours_since_last_success": round(hours_since, 2),
                            "interval_hours": cfg.source.full_load_interval_hours,
                        },
                    )
                    totals.objects_skipped += 1
                    # Restore parent_id_cache from the destination table so that
                    # fan-out children (zone_visits, clients, map_clients, etc.)
                    # still get their parent IDs even when the parent object is
                    # skipped this trigger.
                    _restore_parent_cache_from_table(
                        spark, cfg, obj, parent_id_cache
                    )
                    if obj.name == "sites":
                        _build_site_caches(spark, cfg, parent_id_cache)
                    return
            except Exception as _skip_exc:  # NOSONAR
                log.warning(
                    "interval skip check failed for %s (%s); proceeding with run",
                    obj.name, _skip_exc,
                )

    start_iso = _utcnow_iso()
    obj_start_ts = time.monotonic()
    dest_table = dest_mod.destination_table_name(obj)

    last_wm = cp_mod.read_state(
        spark, meta,
        connector_instance_id=instance_id, object_name=obj.name, state_key="cursor",
    )

    # Resolve parent IDs from cache (handles both regular and synthetic parents).
    parent_ids = None
    if obj.parent:
        parent_ids = parent_id_cache.get(obj.parent)

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

    counts = {
        "rows_read": 0, "rows_inserted": 0, "rows_updated": 0,
        "rows_deleted": 0, "rows_unchanged": 0, "bytes_written": 0,
    }
    all_records_for_finalise: List[dict] = []
    table_id: Optional[str] = None
    pages_seen = 0
    heartbeat_last_ts = time.monotonic()
    batch_buffer: List[dict] = []

    def _flush_batch() -> None:
        nonlocal table_id
        if not batch_buffer:
            return
        if obj.load_strategy == "snapshot":
            # Append-only: each batch is a point-in-time slice; no MERGE / dedup.
            wr = dest_mod.write_snapshot(
                spark,
                destination_catalog=cfg.destination_catalog,
                destination_schema=cfg.destination_schema,
                obj=obj,
                records=batch_buffer,
                source_object=obj.name,
            )
        else:
            wr = dest_mod.write_page(
                spark,
                destination_catalog=cfg.destination_catalog,
                destination_schema=cfg.destination_schema,
                obj=obj,
                records=batch_buffer,
                source_object=obj.name,
            )
        counts["rows_inserted"] += wr.rows_inserted
        counts["rows_updated"] += wr.rows_updated
        counts["rows_deleted"] += wr.rows_deleted
        counts["rows_unchanged"] += wr.rows_unchanged

        if table_id is None:
            table_id, _ = cp_mod.upsert_table_registry(
                spark, meta,
                connector_instance_id=instance_id,
                destination_catalog=cfg.destination_catalog,
                destination_schema=cfg.destination_schema,
                destination_table=dest_table,
                source_object=obj.name,
                primary_key_columns=obj.primary_key,
                row_count_estimate=None,
            )
            # Guarantee a create_table event exists in connector_schema_change_event.
            # write_page only appends one when it physically creates the Iceberg table;
            # for pre-existing tables (e.g. after a metadata-catalog migration) we call
            # ensure_create_table_event which does a SELECT-then-INSERT-if-missing so it
            # is safe to call on every run without producing duplicates.
            _has_create_event = any(
                c.change_type == "create_table" for c in wr.schema_changes
            )
            if not _has_create_event:
                cp_mod.ensure_create_table_event(
                    spark, meta,
                    table_id=table_id,
                    connector_instance_id=instance_id,
                )
        for change in wr.schema_changes:
            cp_mod.record_schema_change(
                spark, meta,
                table_id=table_id, connector_instance_id=instance_id,
                change_type=change.change_type, column_name=change.column_name,
                old_type=change.old_type, new_type=change.new_type,
            )
            if change.change_type != "create_table":
                cp_mod.log_event(
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
                mc = page["max_cursor"]
                max_cursor_seen = mc if max_cursor_seen is None else max(max_cursor_seen, mc)

            # Cache this object's IDs for downstream fan-out children.
            _update_parent_cache(obj.name, records, parent_id_cache)

            if obj.load_strategy == "full":
                all_records_for_finalise.extend(records)

            now_ts = time.monotonic()
            if now_ts - heartbeat_last_ts >= _HEARTBEAT_INTERVAL_SECS:
                _console(
                    f"  … {obj.name}: {pages_seen} pages, "
                    f"read={counts['rows_read']}, inserted={counts['rows_inserted']}, "
                    f"bytes={_fmt_bytes(counts['bytes_written'])}"
                )
                cp_mod.log_event(
                    spark, meta,
                    connector_instance_id=instance_id, run_id=run_id,
                    event_type="object_progress", severity="INFO",
                    message=f"{obj.name}: {pages_seen} pages, rows_read={counts['rows_read']}",
                    event_data={"object": obj.name, "pages_seen": pages_seen, **counts},
                )
                heartbeat_last_ts = now_ts

        _flush_batch()

        if obj.load_strategy == "full":
            tombstoned = dest_mod.finalise_full(
                spark,
                destination_catalog=cfg.destination_catalog,
                destination_schema=cfg.destination_schema,
                obj=obj,
                all_records=all_records_for_finalise,
            )
            counts["rows_deleted"] += tombstoned

        cp_mod.upsert_state(
            spark, meta,
            connector_instance_id=instance_id,
            object_name=obj.name, state_key="cursor",
            state_value=max_cursor_seen,
        )
        if table_id is not None:
            try:
                row_count: Optional[int] = spark.sql(
                    f"SELECT COUNT(*) AS c FROM `{cfg.destination_catalog}`"
                    f".`{cfg.destination_schema}`.`{dest_table}`"
                ).collect()[0]["c"]  # NOSONAR
            except Exception as _rc_exc:  # NOSONAR
                # Table was written but Unity Catalog rejected the registration
                # (e.g. reserved-word column name). Log and skip the count update;
                # the object still counts as succeeded so the run continues.
                log.warning(
                    "row-count query for %s failed (%s); skipping table registry update",
                    dest_table, _rc_exc,
                )
                row_count = None
            if row_count is not None:
                cp_mod.upsert_table_registry(  # row-count update; ignore is_new return
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
        cp_mod.write_object_run(
            spark, meta,
            connector_instance_id=instance_id, run_id=run_id,
            destination_table=dest_table, source_object=obj.name,
            status="success", counts=counts,
            start_time_iso=start_iso, end_time_iso=_utcnow_iso(),
        )
        _console(
            f"✓ [{obj_index}/{obj_total}] {obj.name} "
            f"in {_fmt_duration(time.monotonic() - obj_start_ts)} | "
            f"read={counts['rows_read']} inserted={counts['rows_inserted']} "
            f"updated={counts['rows_updated']} deleted={counts['rows_deleted']} "
            f"bytes={_fmt_bytes(counts['bytes_written'])}"
        )

    except Exception as exc:  # NOSONAR
        error = f"{type(exc).__name__}: {exc}"
        cp_mod.log_event(
            spark, meta,
            connector_instance_id=instance_id, run_id=run_id,
            event_type=("optional_object_failed" if obj.optional else "object_failed"),
            severity=("WARNING" if obj.optional else "SEVERE"),
            message=f"{obj.name}: {error}",
            event_data={"object": obj.name},
        )
        cp_mod.write_object_run(
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
            if totals.first_error_code is None:
                totals.first_error_code = type(exc).__name__
                totals.first_error_message = f"{obj.name}: {exc}"
            log.exception("object %s failed", obj.name)


def _update_parent_cache(
    obj_name: str, records: List[dict], parent_id_cache: Dict
) -> None:
    """Populate parent_id_cache for objects that are parents of fan-out children."""
    from src.api_objects import PRIMARY, OPTIONAL
    from src.api_extractor import parent_cache_entries

    any_has_parent = any(
        getattr(c, "parent", None) in (obj_name, None)
        for c in (*PRIMARY, *OPTIONAL)
        if getattr(c, "parent", None) is not None
    )
    if not any_has_parent:
        return

    # Check if any child uses this object as a parent (handles both direct and synthetic).
    children_use_this = [
        c for c in (*PRIMARY, *OPTIONAL)
        if getattr(c, "parent", None) == obj_name
    ]
    if not children_use_this:
        return

    entries = parent_cache_entries(obj_name, records)
    parent_id_cache.setdefault(obj_name, []).extend(entries)


# ---------------------------------------------------------------------------
# Discover mode
# ---------------------------------------------------------------------------

def _run_discover(cfg: RunConfig) -> Dict[str, Any]:
    """Call GET /self, log the privilege report, exit without writing anything."""
    import requests

    token_provider = MistTokenProvider(cfg.source.api_token)
    url = cfg.source.base_url.rstrip("/") + "/../self"  # base_url includes /api/v1
    # Resolve the URL cleanly.
    from urllib.parse import urljoin
    base = cfg.source.base_url.rstrip("/")
    # /api/v1 → /api/v1/../self = /api/self → navigate up one path segment
    parts = base.rsplit("/", 1)
    self_url = parts[0] + "/../self" if len(parts) > 1 else base + "/self"
    # Normalize: just use the known pattern
    api_root = base.replace("/api/v1", "").replace("//", "/")
    self_url = f"{api_root}/api/v1/self"

    try:
        resp = requests.get(
            self_url,
            headers={"Authorization": token_provider.auth_header()},
            timeout=30,
        )
        if resp.status_code == 200:
            me = resp.json() or {}
        else:
            me = {"error": f"HTTP {resp.status_code}"}
    except Exception as exc:
        me = {"error": str(exc)}

    report = objects_mod.build_capability_report(cfg.enabled_objects_csv)
    org_id = cfg.source.org_id
    privileges = me.get("privileges", [])
    org_priv = next(
        (p for p in privileges if isinstance(p, dict) and p.get("org_id") == org_id),
        privileges[0] if privileges else {},
    )

    _console("=" * 80)
    _console("Jetstream Juniper Mist — Discover mode")
    _console("=" * 80)
    _console(f"Token principal : {me.get('name', '(unknown)')}")
    _console(f"Org ID          : {org_id}")
    _console(f"Privilege       : scope={org_priv.get('scope','?')}, role={org_priv.get('role','?')}")
    _console("")
    _console(objects_mod.format_capability_report(report))
    _console("")
    _console("Note: Juniper Mist uses API token auth — no OAuth scopes to check.")
    _console("=" * 80)

    return {
        "mode": "discover",
        "token_name": me.get("name"),
        "org_id": org_id,
        "privilege": org_priv,
        **report,
    }


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _read_widgets(dbutils: Any) -> Dict[str, Any]:
    if dbutils is None:
        return {}
    try:
        return dict(dbutils.widgets.getAll())
    except Exception:  # NOSONAR
        return {}


def _accumulate(totals: _RunTotals, counts: Dict[str, int]) -> None:
    totals.rows_ingested_total += counts.get("rows_read", 0)
    totals.rows_inserted += counts.get("rows_inserted", 0)
    totals.rows_updated += counts.get("rows_updated", 0)
    totals.rows_deleted += counts.get("rows_deleted", 0)
    totals.bytes_processed += counts.get("bytes_written", 0)


def _utcnow_iso() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
