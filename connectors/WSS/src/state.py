"""Relational control-plane state client for Jetstream metadata tables."""
from __future__ import annotations

import json
import logging
import random
import time
from threading import Lock
from typing import Any, Mapping, Optional, Sequence, Tuple

from src.jetstream.jetstream_context import DatabricksContext, MetaCtx
from src.jetstream.jetstream_id_generator import uuid7
from src.retry import exec_with_concurrent_retry

log = logging.getLogger(__name__)

# Process-level thread lock to serialize control-plane SQL writes across parallel worker threads
_control_plane_lock = Lock()


def _spark_sql(spark: Any, query: str, args: Optional[Mapping[str, Any]] = None) -> Any:
    """Thread-safe Spark SQL execution with continuous retry until success for transient collisions."""

    def _do() -> Any:
        with _control_plane_lock:
            if args is not None:
                return spark.sql(query, args=args)
            return spark.sql(query)

    attempt = 1
    while True:
        try:
            return exec_with_concurrent_retry(_do)
        except Exception as e:
            err_str = str(e)
            is_transient = any(
                k in err_str
                for k in (
                    "RuntimeIOException",
                    "Forbidden",
                    "Lock contention",
                    "403",
                    "ConcurrentAppendException",
                    "DELTA_CONCURRENT_APPEND",
                    "ROW_LEVEL_CHANGES",
                    "WITHOUT_HINT",
                    "ConcurrentTransactionException",
                    "DeltaConcurrentModificationException",
                )
            )
            if is_transient:
                sleep_sec = min(30.0, (1.5 ** attempt) + random.uniform(0.5, 2.0))
                log.warning(
                    "[CONTROL-PLANE RETRY] Transient S3/UniForm lock collision detected (Attempt %d). Retrying in %.2fs...",
                    attempt,
                    sleep_sec,
                )
                time.sleep(sleep_sec)
                attempt += 1
                continue
            # Fail immediately on permanent SQL or logical errors
            raise e


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
    rows = _spark_sql(
        spark,
        f"SELECT run_id, status FROM {meta.qual('connector_run')} WHERE connector_instance_id = :id AND idempotency_key = :ik ORDER BY start_time DESC LIMIT 1",
        args={"id": connector_instance_id, "ik": idempotency_key},
    ).collect()

    # ONLY adopt existing run_id if an active thread is currently running it
    if rows and rows[0]["status"] == "running":
        return rows[0]["run_id"], False

    # For new runs AND retries of failed/completed runs, ALWAYS generate a new run_id and INSERT
    run_id = uuid7()
    _spark_sql(
        spark,
        f"""
        INSERT INTO {meta.qual('connector_run')} (
            run_id, connector_instance_id, idempotency_key, run_type, trigger_type, status,
            start_time, end_time, duration_seconds, rows_ingested_total, rows_inserted, rows_updated, rows_deleted,
            bytes_processed, tables_synced, databricks_workspace_id, databricks_job_id, databricks_job_run_id,
            error_code, error_message, retry_count, triggered_by, run_date
        ) VALUES (
            :rid, :cid, :ik, :rt, :tt, 'running', current_timestamp(), NULL, NULL,
            0, 0, 0, 0, NULL, 0, :ws, :job, :jrun, NULL, NULL, 0, :trig, current_date()
        )
        """,
        args={
            "rid": run_id,
            "cid": connector_instance_id,
            "ik": idempotency_key,
            "rt": run_type,
            "tt": trigger_type,
            "ws": str(db_ctx.workspace_id or ""),
            "job": str(db_ctx.job_id or ""),
            "jrun": str(db_ctx.job_run_id or ""),
            "trig": str(db_ctx.triggered_by or ""),
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
    _spark_sql(
        spark,
        f"""
        UPDATE {meta.qual('connector_run')}
        SET status = :status, end_time = current_timestamp(),
            duration_seconds = BIGINT(unix_timestamp(current_timestamp()) - unix_timestamp(start_time)),
            rows_ingested_total = :rows_read, rows_inserted = :rows_inserted,
            rows_updated = :rows_updated, rows_deleted = :rows_deleted,
            bytes_processed = :bytes, tables_synced = :tables,
            error_code = :err_code, error_message = :err_msg
        WHERE run_id = :rid
        """,
        args={
            "rid": run_id,
            "status": status,
            "rows_read": totals.get("rows_ingested_total", 0),
            "rows_inserted": totals.get("rows_inserted", 0),
            "rows_updated": totals.get("rows_updated", 0),
            "rows_deleted": totals.get("rows_deleted", 0),
            "bytes": totals.get("bytes_processed", 0),
            "tables": totals.get("tables_synced", 0),
            "err_code": error_code,
            "err_msg": error_message,
        },
    )


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
    metrics_facet: Optional[str] = None,
    extended_metrics: Optional[Mapping[str, Any]] = None,
    error_message: Optional[str] = None,
) -> str:
    obj_run_id = uuid7()
    ext_json = json.dumps(dict(extended_metrics), default=str) if extended_metrics else None

    _spark_sql(
        spark,
        f"""
        INSERT INTO {meta.qual('connector_object_run')} (
            object_run_id, run_id, connector_instance_id, destination_table, source_object, status,
            rows_read, rows_inserted, rows_updated, rows_deleted, rows_unchanged, bytes_written,
            extended_metrics, start_time, end_time, error_message, run_date
        ) VALUES (
            :orid, :rid, :cid, :dt, :so, :st, :rr, :ri, :ru, :rd, :ruC, :bw,
            :em, current_timestamp(), current_timestamp(), :err, current_date()
        )
        """,
        args={
            "orid": obj_run_id,
            "rid": run_id,
            "cid": connector_instance_id,
            "dt": destination_table,
            "so": source_object,
            "st": status,
            "rr": counts.get("rows_read", 0),
            "ri": counts.get("rows_inserted", 0),
            "ru": counts.get("rows_updated", 0),
            "rd": counts.get("rows_deleted", 0),
            "ruC": counts.get("rows_unchanged", 0),
            "bw": counts.get("bytes_written", 0),
            "em": ext_json,
            "err": error_message,
        },
    )
    return obj_run_id


def read_state(
    spark: Any,
    meta: MetaCtx,
    *,
    connector_instance_id: str,
    object_name: str,
    state_key: str = "last_modified_watermark",
) -> Optional[str]:
    rows = _spark_sql(
        spark,
        f"SELECT state_value FROM {meta.qual('connector_state')} WHERE connector_instance_id = :cid AND object_name = :obj AND state_key = :sk LIMIT 1",
        args={"cid": connector_instance_id, "obj": object_name, "sk": state_key},
    ).collect()
    return rows[0]["state_value"] if rows else None


def upsert_state(
    spark: Any,
    meta: MetaCtx,
    *,
    connector_instance_id: str,
    object_name: str,
    state_key: str = "last_modified_watermark",
    state_value: Optional[str] = None,
) -> None:
    new_state_id = uuid7()
    clean_cid = str(connector_instance_id).replace("'", "''")
    clean_obj = str(object_name).replace("'", "''")
    clean_sk = str(state_key).replace("'", "''")
    clean_sv = str(state_value if state_value is not None else "").replace("'", "''")

    sql = f"""
        MERGE INTO {meta.qual('connector_state')} t
        USING (
            SELECT 
                '{clean_cid}' AS connector_instance_id, 
                '{clean_obj}' AS object_name, 
                '{clean_sk}' AS state_key, 
                '{clean_sv}' AS state_value
        ) s
        ON t.connector_instance_id = s.connector_instance_id 
       AND t.object_name = s.object_name 
       AND t.state_key = s.state_key
        WHEN MATCHED THEN UPDATE SET 
            t.state_value = s.state_value, 
            t.updated_at = current_timestamp()
        WHEN NOT MATCHED THEN INSERT (
            state_id, 
            connector_instance_id, 
            object_name, 
            state_key, 
            state_value, 
            updated_at
        )
        VALUES (
            '{new_state_id}', 
            s.connector_instance_id, 
            s.object_name, 
            s.state_key, 
            s.state_value, 
            current_timestamp()
        )
    """
    _spark_sql(spark, sql)


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
    row_count_estimate: int = 0,
) -> str:
    rows = _spark_sql(
        spark,
        f"SELECT table_id FROM {meta.qual('connector_table_registry')} WHERE connector_instance_id = :cid AND destination_catalog = :cat AND destination_schema = :sch AND destination_table = :tbl LIMIT 1",
        args={
            "cid": connector_instance_id,
            "cat": destination_catalog,
            "sch": destination_schema,
            "tbl": destination_table,
        },
    ).collect()

    if rows:
        table_id = rows[0]["table_id"]
        _spark_sql(
            spark,
            f"UPDATE {meta.qual('connector_table_registry')} SET table_format = 'iceberg_v3', source_object = :so, last_synced_at = current_timestamp(), row_count_estimate = :rce, updated_at = current_timestamp() WHERE table_id = :tid",
            args={"tid": table_id, "so": source_object, "rce": row_count_estimate},
        )
        return table_id

    table_id = uuid7()
    _spark_sql(
        spark,
        f"""
        INSERT INTO {meta.qual('connector_table_registry')} (
            table_id, connector_instance_id, destination_catalog, destination_schema, destination_table,
            source_object, primary_key_columns, table_format, is_active, first_synced_at, last_synced_at, row_count_estimate, created_at, updated_at
        ) VALUES (
            :tid, :cid, :cat, :sch, :tbl, :so, :pk, 'iceberg_v3', TRUE, current_timestamp(), current_timestamp(), :rce, current_timestamp(), current_timestamp()
        )
        """,
        args={
            "tid": table_id,
            "cid": connector_instance_id,
            "cat": destination_catalog,
            "sch": destination_schema,
            "tbl": destination_table,
            "so": source_object,
            "pk": list(primary_key_columns),
            "rce": row_count_estimate,
        },
    )
    return table_id


def record_schema_change(
    spark: Any,
    meta: MetaCtx,
    *,
    table_id: str,
    connector_instance_id: str,
    change_type: str,
    column_name: Optional[str] = None,
    new_type: Optional[str] = None,
) -> None:
    # Ensure column_name and new_type are strictly NULL when recording a table creation event
    effective_col = None if change_type == "create_table" else column_name
    effective_type = None if change_type == "create_table" else new_type

    _spark_sql(
        spark,
        f"INSERT INTO {meta.qual('connector_schema_change_event')} (change_event_id, table_id, connector_instance_id, change_type, column_name, old_type, new_type, detected_at, applied_at) VALUES (:eid, :tid, :cid, :ct, :col, NULL, :nt, current_timestamp(), current_timestamp())",
        args={
            "eid": uuid7(),
            "tid": table_id,
            "cid": connector_instance_id,
            "ct": change_type,
            "col": effective_col,
            "nt": effective_type,
        },
    )


def log_event(
    spark: Any,
    meta: MetaCtx,
    *,
    connector_instance_id: str,
    run_id: Optional[str],
    event_type: str,
    severity: str,
    message: Any,
    event_data: Optional[Mapping[str, Any]] = None,
) -> None:
    msg_str = (
        json.dumps(message, default=str)
        if isinstance(message, (dict, list))
        else str(message)
    )
    payload = json.dumps(dict(event_data), default=str) if event_data else None
    _spark_sql(
        spark,
        f"INSERT INTO {meta.qual('connector_log')} VALUES (:lid, :cid, :rid, :et, :sev, :message, :data, current_timestamp(), current_date())",
        args={
            "lid": uuid7(),
            "cid": connector_instance_id,
            "rid": run_id,
            "et": event_type,
            "sev": severity,
            "message": msg_str,
            "data": payload,
        },
    )
