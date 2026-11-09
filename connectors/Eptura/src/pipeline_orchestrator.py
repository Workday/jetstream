"""Run orchestration and control-plane reporting.

One run does this:

  1. Read the Databricks Jobs context and the job's widget parameters.
  2. Self-register in the control plane (connector_registry,
     connector_manifest, connector_instance) — first run inserts, later runs
     refresh.
  3. Validate the parameters against the manifest's compiled JSON Schema.
  4. Open a connector_run row, idempotent on the Databricks job run id.
  5. Authenticate to Eptura, then for each selected entity: page through the
     API, write the snapshot, and record a connector_object_run row plus any
     schema changes and table-registry updates.
  6. Close connector_run with the aggregate totals.

Every entity is read as a full snapshot, so there are no cursors to carry
between runs and no dependency ordering between entities. A failure on one
entity is recorded and the run continues, but the job still exits non-zero
so the Databricks scheduler surfaces it.
"""
from __future__ import annotations

import logging
import os
import time
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional, Sequence

from src import connector_config as config_mod
from src import api_extractor as extract_mod
from src import api_objects as objects_mod
from src import control_plane_writer as state_mod
from src import destination_writer as write_mod
from src.api_auth import EpturaTokens
from src.connector_config import ObjectCfg, RunConfig
from src.http_client import HttpClient
from src.jetstream import platform_bootstrap as bootstrap_mod
from src.jetstream import parameter_compiler as params_mod
from src.jetstream import config_validator as validate_mod
from src.jetstream.databricks_context import MetaCtx, read_databricks_context, resolve_metadata_catalog


log = logging.getLogger(__name__)

CONNECTOR_TYPE_ID = "eptura"
CONNECTOR_TYPE_NAME = "Eptura"
CONNECTOR_CATEGORY = "application_api"
CONNECTOR_VERSION = "1.0.0"
CONNECTOR_MAINTAINER = "Jetstream"
CONNECTOR_REPO_PATH = "connectors/eptura"
PARAMETERS_YAML_PATH = os.path.join(
    os.path.dirname(os.path.dirname(os.path.abspath(__file__))),
    "connectors", "eptura", "parameters.yaml",
)

# This connector always reads complete collections, so there is no second
# mode to distinguish (see src/api_objects.py for why incremental isn't used).
# Must be one of the control plane's run_type values — historical,
# incremental, full_resync, table_resync — or platform reporting that filters
# on them silently skips every Eptura run.
RUN_TYPE = "full_resync"

# Pages between progress lines while fetching a large entity.
_PROGRESS_EVERY_PAGES = 5


@dataclass
class _Totals:
    rows_ingested_total: int = 0
    rows_inserted: int = 0
    rows_updated: int = 0
    rows_deleted: int = 0
    bytes_processed: int = 0
    tables_synced: int = 0
    objects_succeeded: int = 0
    objects_failed: int = 0
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


def run(
    *,
    spark: Any,
    dbutils: Any,
    objects: Optional[Sequence[str]] = None,
) -> Dict[str, Any]:
    """Sync Eptura into the raw layer. `objects` narrows this run to a subset
    of the instance's enabled entities."""
    widgets = _read_widgets(dbutils)

    # Widgets always arrive as strings; the manifest declares real types.
    compiled_schema = params_mod.compile_parameters_yaml(PARAMETERS_YAML_PATH)
    widgets = params_mod.coerce_widget_values(widgets, compiled_schema)

    # Job/run identifiers and trigger type come from the Databricks notebook
    # context tags — same approach as the Juniper Mist connector.
    db_ctx = read_databricks_context(dbutils)

    # Routing first, without touching secrets, so a bad secret_scope still
    # fails inside a run we can log against.
    routing = config_mod.resolve_platform_routing(widgets)
    meta = resolve_metadata_catalog(
        routing.environment, routing.metadata_schema, routing.metadata_catalog,
    )

    instance_id, _, _ = bootstrap_mod.ensure_registered_and_get_instance(
        spark, meta,
        connector_type_id=CONNECTOR_TYPE_ID,
        connector_type_name=CONNECTOR_TYPE_NAME,
        category=CONNECTOR_CATEGORY,
        parameters_yaml_path=PARAMETERS_YAML_PATH,
        connector_version=CONNECTOR_VERSION,
        maintainer=CONNECTOR_MAINTAINER,
        repo_path=CONNECTOR_REPO_PATH,
        source_config=config_mod.source_config_for_manifest(widgets, compiled_schema),
        destination_catalog=routing.destination_catalog,
        destination_schema=routing.destination_schema,
        instance_name=routing.instance_name,
        environment=routing.environment,
        sla_target_minutes=routing.sla_target_minutes,
        owner_team=routing.owner_team,
        db_ctx=db_ctx,
    )

    run_id, is_new = state_mod.open_run(
        spark, meta,
        connector_instance_id=instance_id,
        idempotency_key=db_ctx.job_run_id,
        run_type=RUN_TYPE,
        trigger_type=db_ctx.trigger_type,
        db_ctx=db_ctx,
    )
    log.info("run_id=%s instance=%s new=%s", run_id, instance_id, is_new)

    _banner(run_id, routing, meta, db_ctx.trigger_type)
    started = time.monotonic()
    totals = _Totals()

    try:
        try:
            validate_mod.validate_source_config(widgets, compiled_schema)
        except validate_mod.SourceConfigInvalid as exc:
            state_mod.log_event(
                spark, meta,
                connector_instance_id=instance_id, run_id=run_id,
                event_type="config_validation_failed", severity="SEVERE",
                message=str(exc), event_data={"path": exc.path, "rule": exc.rule},
            )
            state_mod.close_run(
                spark, meta, run_id=run_id, status="failed",
                totals=totals.as_dict(),
                error_code="config_validation_failed", error_message=str(exc),
            )
            raise

        # Secrets resolve here, inside the try, so a bad scope lands in
        # connector_log like any other failure.
        cfg = config_mod.resolve_run_config(widgets, dbutils=dbutils)
        _sync_all(
            spark=spark, meta=meta, cfg=cfg,
            instance_id=instance_id, run_id=run_id,
            requested_objects=objects, totals=totals,
        )

        status = "success" if totals.objects_failed == 0 else "failed_with_error"
        state_mod.close_run(
            spark, meta, run_id=run_id, status=status, totals=totals.as_dict(),
            error_code=totals.first_error_code,
            error_message=totals.first_error_message,
        )
    except validate_mod.SourceConfigInvalid:
        raise  # already logged and closed
    # Catch-all on purpose: any failure becomes a connector_log +
    # connector_run audit trail before it propagates.
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
        _print(f"\nRun {run_id} FAILED after {_duration(time.monotonic() - started)}: "
               f"{type(exc).__name__}: {exc}")
        raise
    except BaseException as exc:  # KeyboardInterrupt, SystemExit, Databricks cancellation
        # BaseException is not caught by `except Exception`, so a Databricks job
        # cancel (KeyboardInterrupt at the py4j layer) would leave the run row
        # stuck in `running` with no end_time. Attempt to close it before re-raising.
        # NOTE: when Databricks cancels a job it tears down Python and the JVM
        # simultaneously, so the Spark write inside close_run may itself raise a
        # second KeyboardInterrupt. The inner BaseException guard swallows that
        # silently — a stuck `running` row is unavoidable in that case; use the
        # operator cleanup query in docs/VERIFICATION.md to find and close them.
        try:
            state_mod.close_run(
                spark, meta, run_id=run_id, status="failed", totals=totals.as_dict(),
                error_code=type(exc).__name__, error_message="Job cancelled or interrupted",
            )
        except BaseException:  # noqa: BLE001 — best-effort; don't mask the original signal
            pass
        raise

    _summary(run_id, status, totals, time.monotonic() - started)

    # connector_run.status alone isn't visible to the Databricks scheduler —
    # raise so the job goes red and any configured alerts fire.
    if totals.objects_failed:
        raise RuntimeError(
            f"{totals.objects_failed} entity/entities failed; see "
            f"{meta.qual('connector_log')} for run_id={run_id}"
        )

    return {
        "run_id": run_id,
        "connector_instance_id": instance_id,
        **totals.as_dict(),
        "objects_succeeded": totals.objects_succeeded,
        "objects_failed": totals.objects_failed,
    }


# ---------------------------------------------------------------------------
# sync
# ---------------------------------------------------------------------------

def _sync_all(
    *,
    spark: Any,
    meta: MetaCtx,
    cfg: RunConfig,
    instance_id: str,
    run_id: str,
    requested_objects: Optional[Sequence[str]],
    totals: _Totals,
) -> None:
    plan = objects_mod.select_objects(cfg.enabled_objects_csv)

    if requested_objects:
        wanted = {o.strip() for o in requested_objects if o.strip()}
        unknown = wanted - {o.name for o in plan}
        if unknown:
            raise ValueError(
                f"requested entities {sorted(unknown)} are not enabled on this "
                f"instance (enabled: {[o.name for o in plan]})"
            )
        plan = [o for o in plan if o.name in wanted]

    write_mod.configure_session(spark)

    tokens = EpturaTokens(cfg.source)
    http = HttpClient(cfg.source, tokens)

    names = [o.name for o in plan]
    _print(f"Plan ({len(plan)}): {', '.join(names)}")
    state_mod.log_event(
        spark, meta,
        connector_instance_id=instance_id, run_id=run_id,
        event_type="run_plan", severity="INFO",
        message=f"will_sync={names}", event_data={"objects": names},
    )

    for i, obj in enumerate(plan, start=1):
        _sync_one(
            spark=spark, meta=meta, cfg=cfg,
            instance_id=instance_id, run_id=run_id,
            obj=obj, http=http, totals=totals,
            index=i, total=len(plan),
        )


def _sync_one(
    *,
    spark: Any,
    meta: MetaCtx,
    cfg: RunConfig,
    instance_id: str,
    run_id: str,
    obj: ObjectCfg,
    http: HttpClient,
    totals: _Totals,
    index: int,
    total: int,
) -> None:
    start_iso = _utcnow_iso()
    started = time.monotonic()
    dest_table = write_mod.destination_table_name(obj)
    counts = {"rows_read": 0, "rows_inserted": 0, "rows_updated": 0,
              "rows_deleted": 0, "rows_unchanged": 0, "bytes_written": 0}

    _print(f"\n[{index}/{total}] {obj.name}")

    try:
        records: List[dict] = []
        for page_num, (page, page_bytes) in enumerate(
            extract_mod.fetch_pages(obj, cfg.source, http), start=1
        ):
            records.extend(page)
            counts["bytes_written"] += page_bytes
            if page_num % _PROGRESS_EVERY_PAGES == 0:
                _print(f"    fetched {len(records)} records ({page_num} pages)")
        counts["rows_read"] = len(records)
        _print(f"    fetched {len(records)} records, writing to {dest_table}")
        if obj.fetch_detail:
            _print(f"    detail: {extract_mod.describe_floor_coverage(records)}")

        result = write_mod.sync_object(
            spark,
            destination_catalog=cfg.destination_catalog,
            destination_schema=cfg.destination_schema,
            obj=obj,
            records=records,
            tombstone_guard_pct=cfg.tombstone_guard_pct,
        )
        counts["rows_inserted"] = result.rows_inserted
        counts["rows_updated"] = result.rows_updated
        counts["rows_deleted"] = result.rows_deleted

        table_id = state_mod.upsert_table_registry(
            spark, meta,
            connector_instance_id=instance_id,
            destination_catalog=cfg.destination_catalog,
            destination_schema=cfg.destination_schema,
            destination_table=dest_table,
            source_object=obj.name,
            primary_key_columns=obj.primary_key,
            row_count_estimate=_row_count(spark, cfg, dest_table),
        )
        for change in result.schema_changes:
            state_mod.record_schema_change(
                spark, meta,
                table_id=table_id,
                connector_instance_id=instance_id,
                change_type=change.change_type,
                column_name=change.column_name or None,
                new_type=change.new_type or None,
            )
            state_mod.log_event(
                spark, meta,
                connector_instance_id=instance_id, run_id=run_id,
                event_type="schema_change_detected", severity="WARNING",
                message=f"{change.change_type} on {obj.name}: {change.column_name}",
                event_data={"object": obj.name, "column": change.column_name,
                            "new_type": change.new_type},
            )

        # Full snapshots carry no cursor between runs, so this slot records
        # when the snapshot last completed — per-entity freshness for ops.
        state_mod.upsert_state(
            spark, meta,
            connector_instance_id=instance_id,
            object_name=obj.name, state_key="last_full_sync",
            state_value=_utcnow_iso(),
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
        _print(
            f"    done in {_duration(time.monotonic() - started)} | "
            f"read={counts['rows_read']} inserted={counts['rows_inserted']} "
            f"updated={counts['rows_updated']} deleted={counts['rows_deleted']} "
            f"({_bytes(counts['bytes_written'])})"
        )

    # Per-entity catch-all: record the failure, keep going, and let the run
    # summary decide the job's exit status.
    except Exception as exc:  # NOSONAR
        error = f"{type(exc).__name__}: {exc}"
        totals.objects_failed += 1
        if totals.first_error_code is None:
            totals.first_error_code = type(exc).__name__
            totals.first_error_message = f"{obj.name}: {exc}"
        _accumulate(totals, counts)
        state_mod.log_event(
            spark, meta,
            connector_instance_id=instance_id, run_id=run_id,
            event_type="object_failed", severity="SEVERE",
            message=f"{obj.name}: {error}", event_data={"object": obj.name},
        )
        state_mod.write_object_run(
            spark, meta,
            connector_instance_id=instance_id, run_id=run_id,
            destination_table=dest_table, source_object=obj.name,
            status="failed", counts=counts,
            start_time_iso=start_iso, end_time_iso=_utcnow_iso(),
            error_message=error,
        )
        _print(f"    FAILED after {_duration(time.monotonic() - started)}: {error}")
        log.exception("entity %s failed", obj.name)


# ---------------------------------------------------------------------------
# helpers
# ---------------------------------------------------------------------------

def _read_widgets(dbutils: Any) -> Dict[str, Any]:
    if dbutils is None:
        return {}
    return dict(dbutils.widgets.getAll())


def _row_count(spark: Any, cfg: RunConfig, table: str) -> Optional[int]:
    full = f"`{cfg.destination_catalog}`.`{cfg.destination_schema}`.`{table}`"
    return int(spark.sql(f"SELECT COUNT(*) AS c FROM {full}").collect()[0]["c"])


def _accumulate(totals: _Totals, counts: Dict[str, int]) -> None:
    totals.rows_ingested_total += counts["rows_read"]
    totals.rows_inserted += counts["rows_inserted"]
    totals.rows_updated += counts["rows_updated"]
    totals.rows_deleted += counts["rows_deleted"]
    totals.bytes_processed += counts["bytes_written"]


def _utcnow_iso() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def _print(msg: str) -> None:
    """Progress for the notebook cell output / driver log. Structured events
    still go to connector_log."""
    print(msg, flush=True)


def _banner(run_id: str, routing, meta: MetaCtx, trigger_type: str) -> None:
    _print("=" * 78)
    _print(f"Jetstream Eptura — run_id={run_id}")
    _print(f"  instance:    {routing.instance_name} ({routing.environment})")
    _print(f"  destination: {routing.destination_catalog}.{routing.destination_schema}")
    _print(f"  metadata:    {meta.catalog}.{meta.schema}")
    _print(f"  run type:    {RUN_TYPE} / trigger={trigger_type}")
    _print("=" * 78)


def _summary(run_id: str, status: str, totals: _Totals, elapsed: float) -> None:
    _print("")
    _print("=" * 78)
    _print(f"Jetstream Eptura — run_id={run_id} {status.upper()} in {_duration(elapsed)}")
    _print(f"  entities: succeeded={totals.objects_succeeded} failed={totals.objects_failed}")
    _print(f"  rows:     read={totals.rows_ingested_total} "
           f"inserted={totals.rows_inserted} updated={totals.rows_updated} "
           f"deleted={totals.rows_deleted}")
    _print(f"  bytes:    {_bytes(totals.bytes_processed)}")
    _print("=" * 78)


def _bytes(n: int) -> str:
    v = float(n)
    for unit in ("B", "KB", "MB", "GB"):
        if abs(v) < 1024:
            return f"{v:.1f} {unit}"
        v /= 1024
    return f"{v:.1f} TB"


def _duration(secs: float) -> str:
    secs = int(secs)
    if secs < 60:
        return f"{secs}s"
    if secs < 3600:
        return f"{secs // 60}m {secs % 60}s"
    return f"{secs // 3600}h {(secs % 3600) // 60}m"
