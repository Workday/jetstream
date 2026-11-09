"""Control-plane reads/writes against the shared jetstream schema.

Talks to `metadata_<env>.jetstream.*` (nine tables), located
via `MetaCtx`. Every write uses parameterised SparkSQL (`spark.sql(query,
args={...})`) so no string-interpolated user data touches the query text.

Public API (called from src/pipeline_orchestrator.py and src/jetstream/bootstrap.py):
    open_run, close_run
    write_object_run
    read_state, upsert_state, reset_state
    upsert_table_registry, record_schema_change
    log_event

None of these functions creates DDL. The nine tables are owned by the
platform team and must exist before any connector runs (spec §6).
"""
from __future__ import annotations

import json
from typing import Any, Mapping, Optional, Sequence, Tuple

from src.jetstream.databricks_context import DatabricksContext, MetaCtx
from src.jetstream.uuid_generator import uuid7


# ---------------------------------------------------------------------------
# connector_run
# ---------------------------------------------------------------------------

def find_existing_run(
    spark: Any,
    meta: MetaCtx,
    connector_instance_id: str,
    idempotency_key: str,
) -> Optional[dict]:
    """Return the existing connector_run row for this (instance, idempotency_key),
    or None. Used for retry-safe run opens."""
    rows = spark.sql(
        f"""
        SELECT run_id, status, start_time, end_time
        FROM {meta.qual('connector_run')}
        WHERE connector_instance_id = :instance_id
          AND idempotency_key       = :idempotency_key
        LIMIT 1
        """,
        args={"instance_id": connector_instance_id, "idempotency_key": idempotency_key},
    ).collect()
    if not rows:
        return None
    r = rows[0]
    return {
        "run_id": r["run_id"],
        "status": r["status"],
        "start_time": r["start_time"],
        "end_time": r["end_time"],
    }


def open_run(
    spark: Any,
    meta: MetaCtx,
    *,
    connector_instance_id: str,
    idempotency_key: str,
    run_type: str,
    trigger_type: str,
    db_ctx: DatabricksContext,
) -> Tuple[str, bool]:
    """Insert a new connector_run or return an in-flight one. Returns (run_id, is_new).

    Retry semantics: if a row with the same (instance, idempotency_key)
    already exists and is terminal (success/failed/cancelled), we raise —
    the orchestrator should have caught the terminal state upstream.
    If it exists and is still `running`, we resume it (no insert).
    """
    existing = find_existing_run(spark, meta, connector_instance_id, idempotency_key)
    if existing:
        if existing["status"] == "running":
            return existing["run_id"], False
        raise RuntimeError(
            f"connector_run already exists with status={existing['status']} for "
            f"(instance={connector_instance_id}, idempotency_key={idempotency_key})"
        )

    run_id = uuid7()
    spark.sql(
        f"""
        INSERT INTO {meta.qual('connector_run')} (
            run_id, connector_instance_id, idempotency_key,
            run_type, trigger_type, status,
            start_time, end_time,
            duration_seconds,
            rows_ingested_total, rows_inserted, rows_updated, rows_deleted,
            bytes_processed, tables_synced,
            databricks_workspace_id, databricks_job_id, databricks_job_run_id,
            error_code, error_message,
            retry_count, triggered_by,
            run_date
        ) VALUES (
            :run_id, :instance_id, :idempotency_key,
            :run_type, :trigger_type, 'running',
            current_timestamp(), NULL,
            NULL,
            0, 0, 0, 0,
            NULL, 0,
            :ws, :job, :job_run,
            NULL, NULL,
            0, :triggered_by,
            current_date()
        )
        """,
        args={
            "run_id": run_id,
            "instance_id": connector_instance_id,
            "idempotency_key": idempotency_key,
            "run_type": run_type,
            "trigger_type": trigger_type,
            "ws": db_ctx.workspace_id,
            "job": db_ctx.job_id,
            "job_run": db_ctx.job_run_id,
            "triggered_by": db_ctx.triggered_by,
        },
    )
    return run_id, True


def close_run(
    spark: Any,
    meta: MetaCtx,
    *,
    run_id: str,
    status: str,
    totals: Mapping[str, int],
    error_code: Optional[str] = None,
    error_message: Optional[str] = None,
) -> None:
    """Mark a run finished. `totals` keys: rows_ingested_total, rows_inserted,
    rows_updated, rows_deleted, bytes_processed, tables_synced."""
    spark.sql(
        f"""
        UPDATE {meta.qual('connector_run')}
        SET status              = :status,
            end_time            = current_timestamp(),
            duration_seconds    = BIGINT(unix_timestamp(current_timestamp()) - unix_timestamp(start_time)),
            rows_ingested_total = :rows_ingested_total,
            rows_inserted       = :rows_inserted,
            rows_updated        = :rows_updated,
            rows_deleted        = :rows_deleted,
            bytes_processed     = :bytes_processed,
            tables_synced       = :tables_synced,
            error_code          = :error_code,
            error_message       = :error_message
        WHERE run_id = :run_id
        """,
        args={
            "run_id": run_id,
            "status": status,
            "rows_ingested_total": int(totals.get("rows_ingested_total", 0)),
            "rows_inserted": int(totals.get("rows_inserted", 0)),
            "rows_updated": int(totals.get("rows_updated", 0)),
            "rows_deleted": int(totals.get("rows_deleted", 0)),
            "bytes_processed": int(totals.get("bytes_processed", 0)),
            "tables_synced": int(totals.get("tables_synced", 0)),
            "error_code": error_code,
            "error_message": error_message,
        },
    )


# ---------------------------------------------------------------------------
# connector_object_run
# ---------------------------------------------------------------------------

def write_object_run(
    spark: Any,
    meta: MetaCtx,
    *,
    connector_instance_id: str,
    run_id: str,
    destination_table: str,
    source_object: str,
    status: str,
    counts: Mapping[str, int],
    start_time_iso: str,
    end_time_iso: Optional[str] = None,
    error_message: Optional[str] = None,
) -> str:
    """Insert one connector_object_run row. `counts` keys: rows_read,
    rows_inserted, rows_updated, rows_deleted, rows_unchanged, bytes_written."""
    object_run_id = uuid7()
    spark.sql(
        f"""
        INSERT INTO {meta.qual('connector_object_run')} (
            object_run_id, run_id, connector_instance_id,
            destination_table, source_object, status,
            rows_read, rows_inserted, rows_updated, rows_deleted, rows_unchanged,
            bytes_written,
            start_time, end_time,
            error_message,
            run_date
        ) VALUES (
            :object_run_id, :run_id, :instance_id,
            :destination_table, :source_object, :status,
            :rows_read, :rows_inserted, :rows_updated, :rows_deleted, :rows_unchanged,
            :bytes_written,
            timestamp(:start_time), CASE WHEN :end_time IS NULL THEN current_timestamp() ELSE timestamp(:end_time) END,
            :error_message,
            current_date()
        )
        """,
        args={
            "object_run_id": object_run_id,
            "run_id": run_id,
            "instance_id": connector_instance_id,
            "destination_table": destination_table,
            "source_object": source_object,
            "status": status,
            "rows_read": int(counts.get("rows_read", 0)),
            "rows_inserted": int(counts.get("rows_inserted", 0)),
            "rows_updated": int(counts.get("rows_updated", 0)),
            "rows_deleted": int(counts.get("rows_deleted", 0)),
            "rows_unchanged": int(counts.get("rows_unchanged", 0)),
            "bytes_written": int(counts.get("bytes_written", 0)),
            "start_time": start_time_iso,
            "end_time": end_time_iso,
            "error_message": error_message,
        },
    )
    return object_run_id


# ---------------------------------------------------------------------------
# connector_state
# ---------------------------------------------------------------------------

def read_state(
    spark: Any,
    meta: MetaCtx,
    *,
    connector_instance_id: str,
    object_name: str,
    state_key: str,
) -> Optional[str]:
    rows = spark.sql(
        f"""
        SELECT state_value
        FROM {meta.qual('connector_state')}
        WHERE connector_instance_id = :instance_id
          AND object_name           = :object_name
          AND state_key             = :state_key
        LIMIT 1
        """,
        args={
            "instance_id": connector_instance_id,
            "object_name": object_name,
            "state_key": state_key,
        },
    ).collect()
    return rows[0]["state_value"] if rows else None


def upsert_state(
    spark: Any,
    meta: MetaCtx,
    *,
    connector_instance_id: str,
    object_name: str,
    state_key: str,
    state_value: Optional[str],
) -> None:
    spark.sql(
        f"""
        MERGE INTO {meta.qual('connector_state')} t
        USING (SELECT
                 :instance_id  AS connector_instance_id,
                 :object_name  AS object_name,
                 :state_key    AS state_key,
                 :state_value  AS state_value
              ) s
        ON  t.connector_instance_id = s.connector_instance_id
        AND t.object_name           = s.object_name
        AND t.state_key             = s.state_key
        WHEN MATCHED THEN UPDATE SET
            state_value = s.state_value,
            updated_at  = current_timestamp()
        WHEN NOT MATCHED THEN INSERT (
            state_id, connector_instance_id, object_name, state_key, state_value, updated_at
        ) VALUES (
            :new_state_id, s.connector_instance_id, s.object_name, s.state_key, s.state_value, current_timestamp()
        )
        """,
        args={
            "instance_id": connector_instance_id,
            "object_name": object_name,
            "state_key": state_key,
            "state_value": state_value,
            "new_state_id": uuid7(),
        },
    )


def reset_state(
    spark: Any,
    meta: MetaCtx,
    *,
    connector_instance_id: str,
    object_name: str,
    state_key: str,
) -> None:
    upsert_state(
        spark, meta,
        connector_instance_id=connector_instance_id,
        object_name=object_name,
        state_key=state_key,
        state_value=None,
    )


# ---------------------------------------------------------------------------
# connector_table_registry
# ---------------------------------------------------------------------------

def upsert_table_registry(
    spark: Any,
    meta: MetaCtx,
    *,
    connector_instance_id: str,
    destination_catalog: str,
    destination_schema: str,
    destination_table: str,
    source_object: str,
    primary_key_columns: Sequence[str],
    row_count_estimate: Optional[int],
    table_format: str = "iceberg_v3",
) -> str:
    """Upsert a row in connector_table_registry. Returns the table_id.

    Because the natural key is (connector_instance_id, destination_catalog,
    destination_schema, destination_table), we generate a stable table_id
    only on first insert; subsequent updates keep the existing table_id.
    """
    # Look up an existing row first — small, no-lock read.
    rows = spark.sql(
        f"""
        SELECT table_id, first_synced_at
        FROM {meta.qual('connector_table_registry')}
        WHERE connector_instance_id = :instance_id
          AND destination_catalog   = :catalog
          AND destination_schema    = :schema
          AND destination_table     = :table
        LIMIT 1
        """,
        args={
            "instance_id": connector_instance_id,
            "catalog": destination_catalog,
            "schema": destination_schema,
            "table": destination_table,
        },
    ).collect()

    if rows:
        table_id = rows[0]["table_id"]
        # `primary_key_columns` is refreshed from the current object
        # definition on every upsert, so a PK change in objects.py reaches
        # the registry on the next run instead of leaving stale metadata for
        # downstream consumers.
        spark.sql(
            f"""
            UPDATE {meta.qual('connector_table_registry')}
            SET last_synced_at      = current_timestamp(),
                row_count_estimate  = :row_count,
                is_active           = TRUE,
                primary_key_columns = :pk_cols,
                updated_at          = current_timestamp()
            WHERE table_id = :table_id
            """,
            args={
                "table_id": table_id,
                "row_count": None if row_count_estimate is None else int(row_count_estimate),
                "pk_cols": list(primary_key_columns),
            },
        )
        return table_id

    table_id = uuid7()
    spark.sql(
        f"""
        INSERT INTO {meta.qual('connector_table_registry')} (
            table_id, connector_instance_id,
            destination_catalog, destination_schema, destination_table,
            source_object, primary_key_columns, table_format,
            is_active, first_synced_at, last_synced_at, row_count_estimate,
            created_at, updated_at
        ) VALUES (
            :table_id, :instance_id,
            :catalog, :schema, :table,
            :source_object, :pk_cols, :table_format,
            TRUE, current_timestamp(), current_timestamp(), :row_count,
            current_timestamp(), current_timestamp()
        )
        """,
        args={
            "table_id": table_id,
            "instance_id": connector_instance_id,
            "catalog": destination_catalog,
            "schema": destination_schema,
            "table": destination_table,
            "source_object": source_object,
            "pk_cols": list(primary_key_columns),
            "table_format": table_format,
            "row_count": None if row_count_estimate is None else int(row_count_estimate),
        },
    )
    return table_id


# ---------------------------------------------------------------------------
# connector_schema_change_event
# ---------------------------------------------------------------------------

def record_schema_change(
    spark: Any,
    meta: MetaCtx,
    *,
    table_id: str,
    connector_instance_id: str,
    change_type: str,               # 'create_table' | 'add_column' | 'drop_column' | 'alter_type' | 'drop_table'
    column_name: Optional[str] = None,
    old_type: Optional[str] = None,
    new_type: Optional[str] = None,
) -> None:
    spark.sql(
        f"""
        INSERT INTO {meta.qual('connector_schema_change_event')} (
            change_event_id, table_id, connector_instance_id,
            change_type, column_name, old_type, new_type,
            detected_at, applied_at
        ) VALUES (
            :event_id, :table_id, :instance_id,
            :change_type, :column_name, :old_type, :new_type,
            current_timestamp(), current_timestamp()
        )
        """,
        args={
            "event_id": uuid7(),
            "table_id": table_id,
            "instance_id": connector_instance_id,
            "change_type": change_type,
            "column_name": column_name,
            "old_type": old_type,
            "new_type": new_type,
        },
    )


# ---------------------------------------------------------------------------
# connector_log
# ---------------------------------------------------------------------------

def log_event(
    spark: Any,
    meta: MetaCtx,
    *,
    connector_instance_id: str,
    run_id: Optional[str],
    event_type: str,
    severity: str,                  # 'INFO' | 'WARNING' | 'SEVERE'
    message: str,
    event_data: Optional[Mapping[str, Any]] = None,
) -> None:
    payload = json.dumps(dict(event_data), default=str) if event_data else None
    spark.sql(
        f"""
        INSERT INTO {meta.qual('connector_log')} (
            log_id, connector_instance_id, run_id,
            event_type, severity, message, event_data,
            created_at, log_date
        ) VALUES (
            :log_id, :instance_id, :run_id,
            :event_type, :severity, :message, :event_data,
            current_timestamp(), current_date()
        )
        """,
        args={
            "log_id": uuid7(),
            "instance_id": connector_instance_id,
            "run_id": run_id,
            "event_type": event_type,
            "severity": severity,
            "message": message,
            "event_data": payload,
        },
    )
