"""Per-object sync: ServiceNow rows, attachment binaries, and journal comments."""

from __future__ import annotations

import json
from datetime import datetime, timezone
from typing import Any, Callable, Dict, List, Optional, Tuple

from src import attachments as attachments_mod
from src import state as state_mod
from src import transform, writer
from src.auth import ServiceNowSession
from src.config import ObjectRunConfig
from src.servicenow_client import ServiceNowClient, build_encoded_query

STATE_CURSOR = "cursor"
STATE_CURSOR_LEGACY = "watermark"
STATE_ATT_CURSOR = "attachment_cursor"
STATE_ATT_CURSOR_LEGACY = "attachment_watermark"
STATE_JRN_CURSOR = "journal_cursor"


def sync_object(
    spark: Any,
    meta: Any,
    session: ServiceNowSession,
    instance_id: str,
    run_id: str,
    obj: ObjectRunConfig,
    totals: Any,
    *,
    retry_failed_downloads_only: bool = False,
) -> Dict[str, Any]:
    if retry_failed_downloads_only:
        if obj.sync_target == "attachments":
            dl = attachments_mod.download_attachments_graceful(spark, obj, session, retry_failed_only=True)
            totals.bytes_processed += int(dl.get("bytes_written", 0) or 0)
            _write_volume_object_run(spark, meta, instance_id, run_id, obj, dl, start_iso=datetime.now(timezone.utc).isoformat(), rows_read=0)
            return {"downloads": dl}
        skip_note = f"retry_failed_downloads skipped for sync_target={obj.sync_target}"
        state_mod.write_object_run(
            spark, meta, connector_instance_id=instance_id, run_id=run_id,
            destination_table=obj.destination_table, source_object=obj.source_object, status="success",
            counts={"rows_read": 0, "rows_inserted": 0, "rows_updated": 0, "rows_deleted": 0, "rows_unchanged": 0, "bytes_written": 0},
            start_time_iso=datetime.now(timezone.utc).isoformat(),
            end_time_iso=datetime.now(timezone.utc).isoformat(),
        )
        state_mod.log_event(
            spark, meta, connector_instance_id=instance_id, run_id=run_id,
            event_type="raw_sync_complete", severity="INFO", message=skip_note,
        )
        return {"note": "skipped (not an attachments target)"}

    start_iso = datetime.now(timezone.utc).isoformat()
    client = ServiceNowClient(session=session, page_size=obj.page_size, retry=obj.retry)
    adhoc = obj.is_adhoc_window

    results_payload = {
        "status": "success",
        "adhoc_window": adhoc,
        "sync_target": obj.sync_target
    }

    # =========================================================================
    # TARGET: BASE OBJECT
    # =========================================================================
    if obj.sync_target == "base":
        parent_cursor = None
        if obj.use_cursors:
            parent_cursor = _read_cursor(spark, meta, instance_id, obj.object_name, primary=STATE_CURSOR, legacy=STATE_CURSOR_LEGACY)

        encoded_query = build_encoded_query(
            watermark_columns=obj.watermark_columns,
            watermark_values=[parent_cursor] if parent_cursor else None,
            lookback_minutes=obj.lookback_minutes if parent_cursor else 0,
            from_datetime=obj.from_datetime if adhoc else "",
            to_datetime=obj.to_datetime if adhoc else "",
        )
        fields = [] if obj.fetch_all_fields else obj.fields
        wm_col = obj.watermark_columns[0] if obj.watermark_columns else "sys_updated_on"

        parent_ingest = _append_sn_pages(
            spark, client, obj, sn_table=obj.sn_table, fields=fields, encoded_query=encoded_query,
            order_by=f"ORDERBY{wm_col}" if obj.watermark_columns else None, dest_fqn=obj.target_table,
            wm_field=wm_col, to_df=lambda page: transform.build_dataframe(spark, page, obj, run_id=run_id), label=obj.object_name,
        )
        
        if parent_ingest["incoming_rows"]:
            totals.rows_ingested_total += parent_ingest["incoming_rows"]
            totals.rows_inserted += parent_ingest["incoming_rows"]
            totals.tables_synced += 1

            table_id = _upsert_registry_estimated(
                spark, meta, instance_id, obj, destination_table=obj.destination_table, source_object=obj.source_object,
                pk_columns=list(obj.primary_key_columns) or [obj.primary_key], dest_fqn=obj.target_table, incoming=parent_ingest["incoming_rows"],
            )
            _record_schema_changes(spark, meta, instance_id, table_id, parent_ingest["schema_changes"])
            
        new_cursor = parent_ingest["batch_max"]
        if new_cursor:
            prior = parent_cursor if not adhoc else _read_cursor(spark, meta, instance_id, obj.object_name, primary=STATE_CURSOR, legacy=STATE_CURSOR_LEGACY)
            advanced = _later_timestamp(prior, new_cursor)
            if advanced:
                state_mod.upsert_state(spark, meta, connector_instance_id=instance_id, object_name=obj.object_name, state_key=STATE_CURSOR, state_value=advanced)
        elif not parent_cursor and not adhoc:
            state_mod.upsert_state(
                spark, meta, connector_instance_id=instance_id, object_name=obj.object_name,
                state_key=STATE_CURSOR, state_value=_sn_now(),
            )

        state_mod.write_object_run(
            spark, meta, connector_instance_id=instance_id, run_id=run_id,
            destination_table=obj.destination_table, source_object=obj.source_object, status="success",
            counts={"rows_read": parent_ingest["incoming_rows"], "rows_inserted": parent_ingest["incoming_rows"], "rows_updated": 0, "rows_deleted": 0, "rows_unchanged": 0, "bytes_written": 0},
            start_time_iso=start_iso, end_time_iso=datetime.now(timezone.utc).isoformat(),
        )

        state_mod.log_event(
            spark, meta, connector_instance_id=instance_id, run_id=run_id,
            event_type="raw_sync_complete", severity="INFO",
            message=f"synced {obj.object_name} (base): records={parent_ingest['incoming_rows']}",
            event_data={"records": parent_ingest['incoming_rows'], "parent_cursor_was": parent_cursor, "extract_kind": obj.extract_kind}
        )
        results_payload["records"] = parent_ingest["incoming_rows"]


    # =========================================================================
    # TARGET: ATTACHMENTS
    # =========================================================================
    elif obj.sync_target == "attachments":
        att_cursor = None
        if not adhoc and not obj.force_full_attachments:
            att_cursor = _read_cursor(spark, meta, instance_id, obj.object_name, primary=STATE_ATT_CURSOR, legacy=STATE_ATT_CURSOR_LEGACY)
            
        att_ingest = _append_sn_pages(
            spark, client, obj, sn_table="sys_attachment", fields=[], encoded_query=None, order_by=None,
            dest_fqn=obj.attachment_meta_fqn, wm_field="sys_updated_on",
            to_df=lambda page: (transform.build_attachment_meta_dataframe(spark, page, obj, run_id=run_id), []),
            label=f"{obj.object_name}_attachments",
            pages_iter=client.iter_attachments_by_cursor(
                obj.sn_table, cursor=att_cursor, lookback_minutes=obj.lookback_minutes if att_cursor else 0,
                from_datetime=obj.from_datetime if adhoc else "", to_datetime=obj.to_datetime if adhoc else "",
            ),
        )
        
        att_found = att_ingest["incoming_rows"]

        if att_found:
            totals.rows_ingested_total += att_found
            totals.rows_inserted += att_found
            totals.tables_synced += 1

            att_table_id = _upsert_registry_estimated(
                spark, meta, instance_id, obj, destination_table=obj.attachment_meta_table, source_object="/api/now/table/sys_attachment",
                pk_columns=["sys_id"], dest_fqn=obj.attachment_meta_fqn, incoming=att_found,
            )
            _record_schema_changes(spark, meta, instance_id, att_table_id, att_ingest["schema_changes"])
            
        new_att_cursor = att_ingest["batch_max"]
        if new_att_cursor:
            prior_att = att_cursor if not adhoc else _read_cursor(spark, meta, instance_id, obj.object_name, primary=STATE_ATT_CURSOR, legacy=STATE_ATT_CURSOR_LEGACY)
            advanced_att = _later_timestamp(prior_att, new_att_cursor)
            if advanced_att:
                state_mod.upsert_state(spark, meta, connector_instance_id=instance_id, object_name=obj.object_name, state_key=STATE_ATT_CURSOR, state_value=advanced_att)
        elif not att_cursor and not adhoc:
            state_mod.upsert_state(
                spark, meta, connector_instance_id=instance_id, object_name=obj.object_name,
                state_key=STATE_ATT_CURSOR, state_value=_sn_now(),
            )

        state_mod.write_object_run(
            spark, meta, connector_instance_id=instance_id, run_id=run_id,
            destination_table=obj.attachment_meta_table, source_object="/api/now/table/sys_attachment", status="success",
            counts={"rows_read": att_found, "rows_inserted": att_found, "rows_updated": 0, "rows_deleted": 0, "rows_unchanged": 0, "bytes_written": 0},
            start_time_iso=start_iso, end_time_iso=datetime.now(timezone.utc).isoformat(),
        )

        download_stats: Dict[str, Any] = {}
        if obj.download_attachments:
            download_stats = attachments_mod.download_attachments_graceful(spark, obj, session, retry_failed_only=False)
            totals.bytes_processed += int(download_stats.get("bytes_written", 0) or 0)
            _write_volume_object_run(spark, meta, instance_id, run_id, obj, download_stats, start_iso=start_iso, rows_read=att_found)

        state_mod.log_event(
            spark, meta, connector_instance_id=instance_id, run_id=run_id,
            event_type="raw_sync_complete", severity="INFO",
            message=f"synced {obj.object_name} (attachments): meta={att_found} downloaded={download_stats.get('downloaded', 0)}",
            event_data={"attachment_meta": att_found, "downloads": download_stats, "parent_cursor_was": att_cursor}
        )
        results_payload["attachments_found"] = att_found
        results_payload["downloads"] = download_stats


    # =========================================================================
    # TARGET: JOURNALS
    # =========================================================================
    elif obj.sync_target == "journals":
        jrn_cursor = None
        if not adhoc and not obj.force_full:
            jrn_cursor = _read_cursor(spark, meta, instance_id, obj.object_name, primary=STATE_JRN_CURSOR, legacy="journal_watermark")
            
        jrn_ingest = _append_sn_pages(
            spark, client, obj, sn_table="sys_journal_field", fields=[], encoded_query=None, order_by=None,
            dest_fqn=obj.journals_meta_fqn, wm_field="sys_created_on",
            to_df=lambda page: (transform.build_journal_dataframe(spark, page, obj, run_id=run_id), []),
            label=f"{obj.object_name}_journals",
            pages_iter=client.iter_journals_by_cursor(
                obj.sn_table, cursor=jrn_cursor, lookback_minutes=obj.lookback_minutes if jrn_cursor else 0,
                from_datetime=obj.from_datetime if adhoc else "", to_datetime=obj.to_datetime if adhoc else "",
            ),
        )
        
        jrn_found = jrn_ingest["incoming_rows"]

        if jrn_found:
            totals.rows_ingested_total += jrn_found
            totals.rows_inserted += jrn_found
            totals.tables_synced += 1

            jrn_table_id = _upsert_registry_estimated(
                spark, meta, instance_id, obj, destination_table=obj.journals_meta_table, source_object="/api/now/table/sys_journal_field",
                pk_columns=["sys_id"], dest_fqn=obj.journals_meta_fqn, incoming=jrn_found,
            )
            _record_schema_changes(spark, meta, instance_id, jrn_table_id, jrn_ingest["schema_changes"])
            
        new_jrn_cursor = jrn_ingest["batch_max"]
        if new_jrn_cursor:
            prior_jrn = jrn_cursor if not adhoc else _read_cursor(spark, meta, instance_id, obj.object_name, primary=STATE_JRN_CURSOR, legacy="journal_watermark")
            advanced_jrn = _later_timestamp(prior_jrn, new_jrn_cursor)
            if advanced_jrn:
                state_mod.upsert_state(spark, meta, connector_instance_id=instance_id, object_name=obj.object_name, state_key=STATE_JRN_CURSOR, state_value=advanced_jrn)
        elif not jrn_cursor and not adhoc:
            state_mod.upsert_state(
                spark, meta, connector_instance_id=instance_id, object_name=obj.object_name,
                state_key=STATE_JRN_CURSOR, state_value=_sn_now(),
            )

        state_mod.write_object_run(
            spark, meta, connector_instance_id=instance_id, run_id=run_id,
            destination_table=obj.journals_meta_table, source_object="/api/now/table/sys_journal_field", status="success",
            counts={"rows_read": jrn_found, "rows_inserted": jrn_found, "rows_updated": 0, "rows_deleted": 0, "rows_unchanged": 0, "bytes_written": 0},
            start_time_iso=start_iso, end_time_iso=datetime.now(timezone.utc).isoformat(),
        )

        state_mod.log_event(
            spark, meta, connector_instance_id=instance_id, run_id=run_id,
            event_type="raw_sync_complete", severity="INFO",
            message=f"synced {obj.object_name} (journals): records={jrn_found}",
            event_data={"journals": jrn_found, "parent_cursor_was": jrn_cursor}
        )
        results_payload["journals_found"] = jrn_found

    else:
        raise ValueError(
            f"unknown sync_target={obj.sync_target!r}; expected base|attachments|journals"
        )

    return results_payload


def _append_sn_pages(
    spark: Any, client: ServiceNowClient, obj: ObjectRunConfig, *,
    sn_table: str, fields: List[str], encoded_query: Optional[str], order_by: Optional[str],
    dest_fqn: str, wm_field: str, to_df: Callable[[List[dict]], Tuple[Any, List[str]]],
    label: str, pages_iter: Optional[Any] = None,
) -> Dict[str, Any]:
    incoming = pages = 0
    batch_max: Optional[str] = None
    schema_changes: List[Any] = []
    unexpected: List[str] = []
    iterator = pages_iter if pages_iter is not None else client.query_pages(sn_table, fields, encoded_query, order_by)
    
    for page in iterator:
        pages += 1
        page_len = len(page)
        df, extra = to_df(page)
        if extra:
            unexpected = sorted(set(unexpected) | set(extra))
        page_max = _max_field(page, wm_field)
        batch_max = _later_timestamp(batch_max, page_max)
        stats = writer.append_table(spark, df, dest_fqn, table_format=obj.table_format or "iceberg", incoming_rows=page_len)
        incoming += int(stats.get("incoming_rows", 0) or 0)
        schema_changes.extend(stats.get("schema_changes") or [])
        print(f"   {label} page={pages} rows={page_len} cumulative={incoming}", flush=True)
        del page, df
    return {"incoming_rows": incoming, "pages": pages, "batch_max": batch_max, "schema_changes": schema_changes, "unexpected": unexpected}


def _upsert_registry_estimated(
    spark: Any, meta: Any, instance_id: str, obj: ObjectRunConfig, *,
    destination_table: str, source_object: str, pk_columns: List[str], dest_fqn: str, incoming: int,
) -> str:
    meta_est = writer.estimate_table_rows(spark, dest_fqn) if writer.table_exists(spark, dest_fqn) else None
    return state_mod.upsert_table_registry(
        spark, meta, connector_instance_id=instance_id, destination_catalog=obj.destination_catalog,
        destination_schema=obj.destination_schema, destination_table=destination_table, source_object=source_object,
        primary_key_columns=pk_columns, row_count_estimate=meta_est, table_format="iceberg_v3",
        row_count_delta=None if meta_est is not None else (incoming or None),
    )


def _read_cursor(
    spark: Any, meta: Any, instance_id: str, object_name: str, *, primary: str, legacy: str,
) -> Optional[str]:
    val = state_mod.read_state(spark, meta, connector_instance_id=instance_id, object_name=object_name, state_key=primary)
    if val: return val
    legacy_val = state_mod.read_state(spark, meta, connector_instance_id=instance_id, object_name=object_name, state_key=legacy)
    if legacy_val:
        state_mod.upsert_state(spark, meta, connector_instance_id=instance_id, object_name=object_name, state_key=primary, state_value=legacy_val.split(",")[0])
        return legacy_val.split(",")[0]
    return None


def _max_field(records: List[dict], field: str) -> Optional[str]:
    values = [str(v.get("value") if isinstance(v, dict) else v) for r in records if (v := r.get(field)) not in (None, "")]
    return max(values) if values else None


def _sn_now() -> str:
    """ServiceNow Table API watermark format (not ISO-8601)."""
    return datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")


def _later_timestamp(a: Optional[str], b: Optional[str]) -> Optional[str]:
    if not b: return a
    if not a: return b
    try:
        from src.servicenow_client import _parse_sn_datetime
        return b if _parse_sn_datetime(b) >= _parse_sn_datetime(a) else a
    except Exception:
        return b if b >= a else a


def _write_volume_object_run(
    spark: Any, meta: Any, instance_id: str, run_id: str, obj: ObjectRunConfig,
    download_stats: Dict[str, Any], *, start_iso: str, rows_read: int,
) -> None:
    downloaded = int(download_stats.get("downloaded", 0) or 0)
    failed = int(download_stats.get("failed", 0) or 0)
    skipped = int(download_stats.get("skipped", 0) or 0)
    bytes_written = int(download_stats.get("bytes_written", 0) or 0)
    vol_dest = obj.attachment_volume_path or f"volume:{obj.object_name}"
    
    extended = {"files_inserted": downloaded, "total_bytes": bytes_written, "files_skipped": skipped, "files_failed": failed, "volume_base": download_stats.get("volume_base") or obj.attachment_volume_path}
    
    state_mod.upsert_table_registry(spark, meta, connector_instance_id=instance_id, destination_catalog=obj.destination_catalog, destination_schema=obj.destination_schema, destination_table=vol_dest, source_object="/api/now/attachment", primary_key_columns=[], row_count_estimate=downloaded, table_format="volume")
    state_mod.write_object_run(spark, meta, connector_instance_id=instance_id, run_id=run_id, destination_table=vol_dest, source_object="/api/now/attachment", status="success" if failed == 0 else "failed_with_error", counts={"rows_read": rows_read, "rows_inserted": downloaded, "rows_updated": 0, "rows_deleted": 0, "rows_unchanged": 0, "bytes_written": bytes_written}, start_time_iso=start_iso, end_time_iso=datetime.now(timezone.utc).isoformat(), metrics_facet="file_metrics", extended_metrics=json.dumps(extended))


def _record_schema_changes(spark: Any, meta: Any, instance_id: str, table_id: str, changes: List[Any]) -> None:
    for ch in changes:
        change_type = getattr(ch, "change_type", None) or (ch.get("change_type") if isinstance(ch, dict) else None)
        if not change_type: continue
        state_mod.record_schema_change(spark, meta, table_id=table_id, connector_instance_id=instance_id, change_type=change_type, column_name=(getattr(ch, "column_name", None) if not isinstance(ch, dict) else ch.get("column_name")), new_type=(getattr(ch, "new_type", None) if not isinstance(ch, dict) else ch.get("new_type")))