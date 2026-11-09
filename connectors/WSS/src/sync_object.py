"""Per-object sync into raw tables, with an optional date window."""
from __future__ import annotations

import gc
import json
import os
import re
import concurrent.futures
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, List, Optional

from src import attachments as attachments_mod
from src import state as state_mod
from src import transform, writer
from src.auth import WSSSession
from src.config import ObjectRunConfig
from src.wss_client import WSSClient

FLUSH_THRESHOLD = 10000 

def _log(tag: str, msg: str) -> None:
    print(f"    [{tag:<18}] {msg}", flush=True)

def _clean_iso(dt: datetime) -> str:
    """Format UTC timestamps strictly as YYYY-MM-DDTHH:MM:SZ without microseconds."""
    return dt.astimezone(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")

class RawJsonStreamWriter:
    """Streams raw API payloads directly to a UC Volume disk file with ~0 RAM overhead."""
    def __init__(self, volume_base_dir: str, object_name: str, run_id: str):
        self.volume_base_dir = volume_base_dir
        self.object_name = object_name
        self.run_id = run_id
        self.filepath = ""
        self.file_handle = None
        self.total_records = 0
        self.is_first_record = True

    def write_records(self, records: List[Dict[str, Any]]) -> None:
        if not self.volume_base_dir or not records:
            return

        if self.file_handle is None:
            now_utc = datetime.now(timezone.utc)
            date_folder = now_utc.strftime("%Y-%m-%d")
            ts_str = now_utc.strftime("%H%M%S")
            run_prefix = (self.run_id or "adhoc")[:8]

            target_dir = os.path.join(self.volume_base_dir, self.object_name, date_folder)
            os.makedirs(target_dir, exist_ok=True)

            filename = f"response_{self.object_name}_{ts_str}_run_{run_prefix}.json"
            self.filepath = os.path.join(target_dir, filename)
            self.file_handle = open(self.filepath, "w", encoding="utf-8")

            header = f'{{\n  "metadata": {{\n    "object_name": "{self.object_name}",\n    "ingested_at": "{now_utc.isoformat()}",\n    "run_id": "{self.run_id}"\n  }},\n  "records": [\n'
            self.file_handle.write(header)

        for rec in records:
            if not self.is_first_record:
                self.file_handle.write(",\n")
            else:
                self.is_first_record = False
            self.file_handle.write("    " + json.dumps(rec, default=str))
            self.total_records += 1

        self.file_handle.flush()

    def close(self) -> str:
        if self.file_handle is None:
            return ""

        self.file_handle.write("\n  ]\n}\n")
        self.file_handle.close()
        self.file_handle = None
        return self.filepath

def _flush_batch_to_iceberg(spark: Any, meta: Any, instance_id: str, run_id: str, obj: ObjectRunConfig, batch_records: List[Dict[str, Any]], counts: Dict[str, int]) -> None:
    if not batch_records:
        return

    _log("BATCH-WRITE", f"Merging macro-batch of {len(batch_records)} records into Spark/Iceberg...")
    endpoint_val = getattr(obj, "endpoint", obj.source_object)

    df = transform.process_and_flatten_raw_batch(
        spark, batch_records, obj, instance_id=instance_id, run_id=run_id
    )
    w_res = writer.append_table(spark, df, obj.target_table, primary_keys=obj.primary_key_columns, meta=meta, instance_id=instance_id)
    counts["rows_inserted"] += w_res.get("incoming_rows", 0)

    rel_df = transform.extract_child_relationships(spark, batch_records, obj.object_name)
    if rel_df is not None and rel_df.count() > 0:
        rel_tbl_name = f"{obj.object_name}_relationship"
        rel_fqn = f"{obj.destination_catalog}.{obj.destination_schema}.{rel_tbl_name}"
        rel_w = writer.append_table(spark, rel_df, rel_fqn, primary_keys=["parent_id", "relationship_name", "related_entity_id"], meta=meta, instance_id=instance_id)
        rel_inserted = rel_w.get("incoming_rows", 0)
        counts["rel_rows"] += rel_inserted

        state_mod.upsert_table_registry(
            spark, meta, connector_instance_id=instance_id, 
            destination_catalog=obj.destination_catalog, destination_schema=obj.destination_schema, 
            destination_table=rel_tbl_name, source_object=f"{endpoint_val}/relationships", 
            primary_key_columns=["parent_id", "relationship_name", "related_entity_id"], row_count_estimate=rel_inserted
        )
        
        state_mod.write_object_run(
            spark, meta, connector_instance_id=instance_id, run_id=run_id, 
            destination_table=rel_tbl_name, source_object=f"{endpoint_val}/relationships", status="success", 
            counts={"rows_read": rel_inserted, "rows_inserted": rel_inserted, "rows_updated": 0, "rows_deleted": 0, "rows_unchanged": 0, "bytes_written": 0},
            metrics_facet="child_relationship",
            extended_metrics={"parent_object": obj.object_name}
        )

    gc.collect()

def sync_object(
    spark: Any,
    meta: Any,
    session: WSSSession,
    instance_id: str,
    run_id: str,
    obj: ObjectRunConfig,
    totals: Any,
    mode: str = "run",
) -> Dict[str, Any]:
    endpoint_val = getattr(obj, "endpoint", obj.source_object)

    state_mod.log_event(
        spark, meta, connector_instance_id=instance_id, run_id=run_id, 
        event_type="object_sync_started", severity="INFO", 
        message={"records": 0, "parent_cursor_was": None, "extract_kind": mode, "object_name": obj.object_name}
    )

    _log("OBJECT-INIT", f"Target: '{obj.object_name}' | Strategy: {obj.load_strategy.upper()} | Pattern: {obj.auth_pattern}")
    client = WSSClient(session, page_size=obj.page_size, retry=obj.retry)
    counts = {"rows_read": 0, "rows_inserted": 0, "rel_rows": 0, "json_files_saved": 0}

    stream_writer = RawJsonStreamWriter(obj.raw_json_volume_base, obj.object_name, run_id)
    batch_buffer: List[Dict[str, Any]] = []

    # =========================================================================
    # GLOBAL VARIABLE SCOPE INITIALIZATION
    # =========================================================================
    start_time = None
    end_time = None
    is_incremental_mode = False

    try:
        # =========================================================================
        # 1. MULTI-PARAMETER DEPENDENT CHILD EXECUTION FLOW
        # =========================================================================
        is_dependent = (
            obj.api_type in ("INCR_GET_ID", "GET_ID")
            or (bool(obj.dependency_object) and "{" in getattr(obj, "endpoint", ""))
        )

        if is_dependent and obj.dependency_object:
            dep_objs = [d.strip() for d in str(obj.dependency_object).split(",") if d.strip()]
            leaf_dep_obj = dep_objs[-1]
            parent_tbl_name = f"{leaf_dep_obj}"
            parent_fqn = f"`{obj.destination_catalog}`.`{obj.destination_schema}`.`{parent_tbl_name}`"

            if not spark.catalog.tableExists(f"{obj.destination_catalog}.{obj.destination_schema}.{parent_tbl_name}"):
                _log("DEPENDENCY-WARN", f"Parent table '{parent_tbl_name}' missing. Skipping dependent child sync.")
                return {"object_name": obj.object_name, "strategy": obj.load_strategy, "rows_read": 0, "rows_inserted": 0, "rel_rows": 0, "attachments_downloaded": 0, "bytes_written": 0, "json_files_saved": 0}

            placeholders = re.findall(r"\{([^{}]+)\}", obj.endpoint)
            dep_cols = spark.table(parent_fqn).columns

            select_exprs = []
            for ph in placeholders:
                matching_col = next((c for c in dep_cols if c.lower() == ph.lower()), None)
                if matching_col:
                    select_exprs.append(f"`{matching_col}` AS `{ph}`")
                elif "id" in [c.lower() for c in dep_cols]:
                    id_col = next(c for c in dep_cols if c.lower() == "id")
                    select_exprs.append(f"`{id_col}` AS `{ph}`")

            if not select_exprs:
                select_exprs = ["`id`"]

            where_clause_parts = [f"{expr.split(' AS ')[0]} IS NOT NULL" for expr in select_exprs]

            is_incremental_mode = (mode != "full_resync") and (
                obj.api_type in ("INCR_GET_ID", "GET_ID")
                or "INCR" in str(getattr(obj, "load_strategy", "")).upper()
            )

            last_wm = None
            if is_incremental_mode:
                last_wm = (
                    state_mod.read_state(spark, meta, connector_instance_id=instance_id, object_name=obj.object_name, state_key="last_synced_at")
                    or state_mod.read_state(spark, meta, connector_instance_id=instance_id, object_name=obj.object_name, state_key="last_modified_watermark")
                )
                start_time = last_wm  # Assign to top-level start_time for accurate final logging
                synced_col = next((c for c in dep_cols if c.lower() == "_jetstream_synced"), None)

                if last_wm and synced_col:
                    where_clause_parts.append(f"`{synced_col}` >= '{last_wm}'")
                    _log("LOAD-SUMMARY", f"==> OBJECT: '{obj.object_name}' (Child) | MODE: INCREMENTAL | TIMEFRAME: [{last_wm} ---> NOW]")
                elif not last_wm:
                    _log("LOAD-SUMMARY", f"==> OBJECT: '{obj.object_name}' (Child) | MODE: INCREMENTAL (Initial / No Watermark) | TIMEFRAME: Full Historical Baseline Scan")
                elif not synced_col:
                    _log("LOAD-SUMMARY", f"==> OBJECT: '{obj.object_name}' (Child) | MODE: INCREMENTAL (Missing '_jetstream_synced') | TIMEFRAME: Full Parent Table Scan")
            else:
                _log("LOAD-SUMMARY", f"==> OBJECT: '{obj.object_name}' (Child) | MODE: FULL RESYNC | TIMEFRAME: Full Parent Table Scan")

            obj_name_lower = obj.object_name.lower().strip()
            parent_name_lower = leaf_dep_obj.lower().strip()

            if obj_name_lower == "event_bids" and parent_name_lower == "events":
                req_col = "request_type"
                if req_col in dep_cols:
                    where_clause_parts.append(f"UPPER(TRIM(CAST(`{req_col}` AS STRING))) = 'RFP'")
                    _log("DEPENDENCY-FILTER", f"Applying RFP filter on parent '{parent_tbl_name}': `{req_col}` = 'RFP'")

            where_clause = " AND ".join(where_clause_parts)
            select_sql = f"SELECT DISTINCT {', '.join(select_exprs)} FROM {parent_fqn} WHERE {where_clause}"
            parent_rows = spark.sql(select_sql).collect()
            total_parents = len(parent_rows)

            _log("DEPENDENCY-LOAD", f"Extracted {total_parents} parent parameter set(s) from '{parent_tbl_name}'. Executing parallel child extractions...")

            def _fetch_child(p_row: Any) -> List[Dict[str, Any]]:
                row_dict = p_row.asDict()
                params_dict = {ph: str(row_dict[ph]) for ph in placeholders if ph in row_dict}
                child_recs = []
                for page in client.extract_pages(obj, parent_params=params_dict):
                    for rec in page["records"]:
                        if isinstance(rec, dict):
                            for k, v in params_dict.items():
                                if k not in rec:
                                    rec[k] = v
                        child_recs.append(rec)
                return child_recs

            processed_count = 0
            with concurrent.futures.ThreadPoolExecutor(max_workers=5) as executor:
                future_to_pid = {executor.submit(_fetch_child, row): row for row in parent_rows}
                for future in concurrent.futures.as_completed(future_to_pid):
                    processed_count += 1
                    try:
                        records = future.result()
                        if records:
                            counts["rows_read"] += len(records)
                            stream_writer.write_records(records)
                            batch_buffer.extend(records)

                            if len(batch_buffer) >= FLUSH_THRESHOLD:
                                _flush_batch_to_iceberg(spark, meta, instance_id, run_id, obj, batch_buffer, counts)
                                batch_buffer.clear()

                    except Exception as exc:
                        _log("API-ERROR", f"Failed fetching child record: {exc}")

                    if processed_count % 50 == 0 or processed_count == total_parents:
                        _log("DEPENDENCY-PROG", f"Processed {processed_count}/{total_parents} parent parameter sets...")

            if batch_buffer:
                _flush_batch_to_iceberg(spark, meta, instance_id, run_id, obj, batch_buffer, counts)
                batch_buffer.clear()

            now_iso = _clean_iso(datetime.now(timezone.utc))
            state_mod.upsert_state(spark, meta, connector_instance_id=instance_id, object_name=obj.object_name, state_key="last_synced_at", state_value=now_iso)
            state_mod.upsert_state(spark, meta, connector_instance_id=instance_id, object_name=obj.object_name, state_key="last_modified_watermark", state_value=now_iso)

            state_mod.log_event(
                spark, meta, connector_instance_id=instance_id, run_id=run_id, 
                event_type="object_sync_completed", severity="INFO", 
                message={"records": counts["rows_read"], "parent_cursor_was": last_wm, "extract_kind": "incremental" if is_incremental_mode else "full"}
            )

        # =========================================================================
        # 2. STANDARD INCR / FULL REFRESH EXECUTION FLOW
        # =========================================================================
        else:
            is_incremental_mode = (mode != "full_resync") and (obj.api_type in ("INCR", "INCR_GET_ID"))

            if is_incremental_mode:
                manual_start = getattr(obj, "from_datetime", None) or getattr(obj, "start_time", None)
                manual_end = getattr(obj, "to_datetime", None) or getattr(obj, "end_time", None)

                if manual_start:
                    start_time = manual_start
                    end_time = manual_end or _clean_iso(datetime.now(timezone.utc))
                    _log("LOAD-SUMMARY", f"==> OBJECT: '{obj.object_name}' | MODE: INCREMENTAL (Manual Override) | TIMEFRAME: [{start_time} ---> {end_time}]")
                elif getattr(obj, "use_cursors", True):
                    last_wm = state_mod.read_state(spark, meta, connector_instance_id=instance_id, object_name=obj.object_name, state_key="last_modified_watermark")
                    if last_wm:
                        dt = datetime.fromisoformat(last_wm.replace("Z", "+00:00"))
                        lookback = getattr(obj, "lookback_minutes", 30)
                        start_time = _clean_iso(dt - timedelta(minutes=lookback))
                        end_time = _clean_iso(datetime.now(timezone.utc))
                        _log("LOAD-SUMMARY", f"==> OBJECT: '{obj.object_name}' | MODE: INCREMENTAL (Watermark Cursor) | TIMEFRAME: [{start_time} ---> {end_time}] (Lookback Buffer: {lookback}m)")
                    else:
                        _log("LOAD-SUMMARY", f"==> OBJECT: '{obj.object_name}' | MODE: INCREMENTAL (Initial Run / No Watermark) | TIMEFRAME: Unbounded Baseline Pull")
                else:
                    _log("LOAD-SUMMARY", f"==> OBJECT: '{obj.object_name}' | MODE: INCREMENTAL (Cursors Bypassed) | TIMEFRAME: Unbounded Pull")
            else:
                _log("LOAD-SUMMARY", f"==> OBJECT: '{obj.object_name}' | MODE: FULL RESYNC (Mode='{mode}') | TIMEFRAME: Unbounded Full Refresh")

            max_cursor = start_time
            page_num = 1

            for page in client.extract_pages(obj, start_time=start_time, end_time=end_time):
                records = page["records"]
                if records:
                    counts["rows_read"] += len(records)
                    stream_writer.write_records(records)
                    batch_buffer.extend(records)

                if page.get("max_cursor"):
                    max_cursor = max(max_cursor or "", page["max_cursor"])

                if len(batch_buffer) >= FLUSH_THRESHOLD:
                    _flush_batch_to_iceberg(spark, meta, instance_id, run_id, obj, batch_buffer, counts)
                    batch_buffer.clear()

                page_num += 1

            if batch_buffer:
                _flush_batch_to_iceberg(spark, meta, instance_id, run_id, obj, batch_buffer, counts)
                batch_buffer.clear()

            now_iso = _clean_iso(datetime.now(timezone.utc))
            if obj.api_type == "INCR":
                if max_cursor:
                    state_mod.upsert_state(spark, meta, connector_instance_id=instance_id, object_name=obj.object_name, state_key="last_modified_watermark", state_value=max_cursor)
                    _log("WATERMARK-UPDATE", f"Advanced watermark for '{obj.object_name}' to -> '{max_cursor}'")
                elif counts["rows_read"] == 0 and end_time:
                    state_mod.upsert_state(spark, meta, connector_instance_id=instance_id, object_name=obj.object_name, state_key="last_modified_watermark", state_value=end_time)
                    _log("WATERMARK-UPDATE", f"No new rows. Advanced watermark for '{obj.object_name}' to window end -> '{end_time}'")
            else:
                state_mod.upsert_state(spark, meta, connector_instance_id=instance_id, object_name=obj.object_name, state_key="last_synced_at", state_value=now_iso)
                _log("WATERMARK-UPDATE", f"Updated last_synced_at timestamp for '{obj.object_name}' to -> '{now_iso}'")

            state_mod.log_event(
                spark, meta, connector_instance_id=instance_id, run_id=run_id, 
                event_type="object_sync_completed", severity="INFO", 
                message={"records": counts["rows_read"], "parent_cursor_was": start_time, "extract_kind": "incremental" if is_incremental_mode else "full"}
            )

    finally:
        saved_json_path = stream_writer.close()
        if saved_json_path:
            counts["json_files_saved"] = 1
            _log("RAW-BACKUP", f"Saved streaming raw backup -> '{saved_json_path}'")

    # =========================================================================
    # 3. ATTACHMENT RECONCILIATION
    # =========================================================================
    att_res = {"downloaded": 0, "failed": 0, "skipped": 0, "bytes_written": 0}
    has_att = getattr(obj, "has_attachments", False)
    dl_att = getattr(obj, "download_attachments", True)

    if has_att and dl_att:
        _log("ATTACHMENT-SCAN", f"Scanning missing attachment binary files associated with '{obj.object_name}'...")
        att_res = attachments_mod.process_and_sync_attachments_raw(
            spark, obj.destination_catalog, obj.destination_schema, obj.object_name,
            client, obj.attachment_volume_path, meta, instance_id
        ) or {"downloaded": 0, "failed": 0, "skipped": 0, "bytes_written": 0}

        att_tbl_name = "attachments"
        att_fqn = f"{obj.destination_catalog}.{obj.destination_schema}.{att_tbl_name}"
        att_total = spark.table(att_fqn).count() if spark.catalog.tableExists(att_fqn) else att_res.get("downloaded", 0)

        state_mod.upsert_table_registry(
            spark, meta, connector_instance_id=instance_id, 
            destination_catalog=obj.destination_catalog, destination_schema=obj.destination_schema, 
            destination_table=att_tbl_name, source_object=f"{endpoint_val}/attachments", 
            primary_key_columns=["parent_id", "attachment_id"], row_count_estimate=att_total
        )
        
        state_mod.write_object_run(
            spark, meta, connector_instance_id=instance_id, run_id=run_id, 
            destination_table=f"{obj.destination_catalog}.{obj.destination_schema}.{att_tbl_name}", 
            source_object=f"{endpoint_val}/attachments", status="success", 
            counts={"rows_read": att_res.get("downloaded", 0) + att_res.get("failed", 0), "rows_inserted": att_res.get("downloaded", 0), "rows_updated": 0, "rows_deleted": 0, "rows_unchanged": 0, "bytes_written": att_res.get("bytes_written", 0)},
            metrics_facet="file_storage",
            extended_metrics={
                "files_inserted": att_res.get("downloaded", 0),
                "total_bytes": att_res.get("bytes_written", 0),
                "files_failed": att_res.get("failed", 0)
            }
        )

        state_mod.log_event(
            spark, meta, connector_instance_id=instance_id, run_id=run_id,
            event_type="attachment_sync_completed", severity="INFO",
            message={
                "attachment_meta": att_total,
                "downloads": {
                    "downloaded": att_res.get("downloaded", 0),
                    "skipped": att_res.get("skipped", 0),
                    "failed": att_res.get("failed", 0),
                    "bytes_written": att_res.get("bytes_written", 0),
                    "volume_base": obj.attachment_volume_path
                },
                "parent_cursor_was": _clean_iso(datetime.now(timezone.utc))
            }
        )

    # =========================================================================
    # 4. CONTROL PLANE REGISTRY COMMIT & COMPLETION
    # =========================================================================
    actual_rows = spark.table(obj.target_table.replace("`", "")).count() if spark.catalog.tableExists(obj.target_table.replace("`", "")) else counts["rows_inserted"]

    state_mod.upsert_table_registry(
        spark, meta, connector_instance_id=instance_id, 
        destination_catalog=obj.destination_catalog, destination_schema=obj.destination_schema, 
        destination_table=obj.object_name, source_object=endpoint_val, 
        primary_key_columns=obj.primary_key_columns, row_count_estimate=actual_rows
    )
    
    state_mod.write_object_run(
        spark, meta, connector_instance_id=instance_id, run_id=run_id, 
        destination_table=obj.target_table.replace("`", ""), source_object=endpoint_val, status="success", 
        counts=counts,
        metrics_facet="relational_tabular",
        extended_metrics={
            "rel_rows": counts.get("rel_rows", 0),
            "json_files_saved": counts.get("json_files_saved", 0)
        }
    )

    totals.rows_ingested_total += counts["rows_read"]
    totals.rows_inserted += counts["rows_inserted"]
    totals.bytes_processed += att_res.get("bytes_written", 0)
    totals.tables_synced += 1

    timeframe_str = f"[{start_time or 'START'} ---> {end_time or 'NOW'}]" if is_incremental_mode else "FULL_TABLE_SCAN"
    _log("OBJECT-COMPLETE", f"Finished '{obj.object_name}' | Mode: {'INCREMENTAL' if is_incremental_mode else 'FULL_RESYNC'} | Window: {timeframe_str} | Read: {counts['rows_read']} | Merged: {counts['rows_inserted']} | Rel Rows: {counts['rel_rows']} | Att Files: {att_res.get('downloaded', 0)}")

    return {
        "object_name": obj.object_name,
        "strategy": obj.load_strategy,
        "rows_read": counts["rows_read"],
        "rows_inserted": counts["rows_inserted"],
        "rel_rows": counts["rel_rows"],
        "attachments_downloaded": att_res.get("downloaded", 0),
        "bytes_written": att_res.get("bytes_written", 0),
        "json_files_saved": counts["json_files_saved"],
    }