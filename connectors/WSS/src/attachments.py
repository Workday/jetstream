"""Strictly Rate-limited (5 QPS, 5 parallel workers) attachment stream downloader with thread-safe table provisioning."""
from __future__ import annotations

import concurrent.futures
import logging
import os
import time
from datetime import datetime, timezone
from threading import Lock
from typing import Any, Dict

from pyspark.sql import functions as F
from pyspark.sql.types import LongType, StringType, StructField, StructType, TimestampType

from src import state as state_mod
from src import writer
from src.attachment_downloader import download_file_stream, sanitize_filename

log = logging.getLogger(__name__)

# Process-level thread lock to serialize attachments table provisioning across parallel worker threads
_attachments_lock = Lock()


def _log(tag: str, msg: str) -> None:
    print(f"    [{tag:<18}] {msg}", flush=True)


ATT_UPDATE_SCHEMA = StructType([
    StructField("parent_id", StringType(), True),
    StructField("parent_type", StringType(), True),
    StructField("attachment_id", StringType(), True),
    StructField("file_name", StringType(), True),
    StructField("file_size", LongType(), True),
    StructField("uploaded_at", StringType(), True),
    StructField("download_status", StringType(), True),
    StructField("download_error", StringType(), True),
    StructField("downloaded_at", StringType(), True),
    StructField("volume_path", StringType(), True),
])


class RateLimiter:
    """
    Enforces a strict global API limit.
    At 5.0 QPS, it strictly forces exactly 0.2 seconds between every API call.
    The Lock() ensures no two threads can bypass this interval.
    """
    def __init__(self, qps: float = 5.0):
        self.interval = 1.0 / qps
        self.lock = Lock()
        self.last_call = 0.0

    def wait(self):
        with self.lock:
            now = time.time()
            elapsed = now - self.last_call
            if elapsed < self.interval:
                time.sleep(self.interval - elapsed)
            self.last_call = time.time()


def ensure_attachments_raw_table(
    spark: Any,
    raw_catalog: str,
    raw_schema: str,
    meta: Any = None,
    instance_id: str = "",
) -> str:
    """Create the attachments Iceberg table, waiting briefly if the catalog has not caught up."""
    clean_cat = raw_catalog.replace("`", "")
    clean_sch = raw_schema.replace("`", "")
    target_table = f"{clean_cat}.{clean_sch}.attachments"
    target_fqn = f"`{clean_cat}`.`{clean_sch}`.`attachments`"

    try:
        spark.sql(f"CREATE SCHEMA IF NOT EXISTS `{clean_cat}`.`{clean_sch}`")
    except Exception:
        pass

    max_retries = 3
    for attempt in range(1, max_retries + 1):
        # 1. Fast check if table already exists in catalog
        if spark.catalog.tableExists(target_table):
            return target_table

        with _attachments_lock:
            # 2. Double-check inside lock in case another thread created it while waiting
            if spark.catalog.tableExists(target_table):
                return target_table

            _log("ATTACHMENT-INIT", f"Provisioning Iceberg table '{target_fqn}' (Attempt {attempt})...")
            schema = StructType([
                StructField("parent_id", StringType(), True),
                StructField("parent_type", StringType(), True),
                StructField("attachment_id", StringType(), True),
                StructField("file_name", StringType(), True),
                StructField("file_size", LongType(), True),
                StructField("uploaded_at", StringType(), True),
                StructField("download_status", StringType(), True),
                StructField("download_error", StringType(), True),
                StructField("downloaded_at", StringType(), True),
                StructField("volume_path", StringType(), True),
                StructField("_jetstream_synced", TimestampType(), True),
                StructField("_jetstream_deleted", StringType(), True),
                StructField("_jetstream_source_object", StringType(), True),
            ])
            empty_df = spark.createDataFrame([], schema=schema)

            try:
                empty_df.writeTo(target_fqn).using("iceberg").create()

                if meta and instance_id:
                    try:
                        tbl_id = state_mod.upsert_table_registry(
                            spark,
                            meta,
                            connector_instance_id=instance_id,
                            destination_catalog=clean_cat,
                            destination_schema=clean_sch,
                            destination_table="attachments",
                            source_object="attachments",
                            primary_key_columns=["parent_id", "attachment_id"],
                            row_count_estimate=0,
                        )
                        state_mod.record_schema_change(
                            spark,
                            meta,
                            table_id=tbl_id,
                            connector_instance_id=instance_id,
                            change_type="create_table",
                            column_name=None,
                            new_type="iceberg_v3",
                        )
                    except Exception as meta_err:
                        log.warning("[ATTACHMENT-INIT] Metadata recording warning: %s", meta_err)

                return target_table

            except Exception as err:
                err_str = str(err)
                is_already_exists = any(
                    k in err_str for k in ("ALREADY_EXISTS", "already exists", "42P07")
                )
                if is_already_exists:
                    _log(
                        "ATTACHMENT-INIT",
                        f"Table '{target_table}' is being created by another thread. Waiting 10s for metadata propagation...",
                    )
                    time.sleep(10.0)

                    # Re-check catalog after 10s propagation pause
                    if spark.catalog.tableExists(target_table):
                        _log("ATTACHMENT-INIT", f"Catalog propagation complete for '{target_table}'. Resuming pipeline.")
                        return target_table
                    continue

                raise err

    return target_table


def process_and_sync_attachments_raw(
    spark: Any,
    raw_catalog: str,
    raw_schema: str,
    parent_obj_name: str,
    client: Any,
    volume_base_dir: str,
    meta: Any = None,
    instance_id: str = "",
) -> Dict[str, Any]:
    clean_cat = raw_catalog.replace("`", "")
    clean_sch = raw_schema.replace("`", "")
    rel_table = f"`{clean_cat}`.`{clean_sch}`.`{parent_obj_name}_relationship`"
    raw_att_table = ensure_attachments_raw_table(spark, clean_cat, clean_sch, meta=meta, instance_id=instance_id)

    if not spark.catalog.tableExists(f"{clean_cat}.{clean_sch}.{parent_obj_name}_relationship"):
        _log("ATTACHMENT-SCAN", "No relationship table found. Skipping scan.")
        return {"downloaded": 0, "failed": 0, "bytes_written": 0}

    reconciliation_sql = f"""
        SELECT DISTINCT r.parent_id, '{parent_obj_name}' AS parent_type, r.related_entity_id AS attachment_id
        FROM {rel_table} r
        LEFT JOIN `{clean_cat}`.`{clean_sch}`.`attachments` a
          ON r.parent_id = a.parent_id AND r.related_entity_id = a.attachment_id
        WHERE LOWER(TRIM(r.relationship_name)) = 'attachments'
          AND r.related_entity_id IS NOT NULL
          AND (a.attachment_id IS NULL OR a.download_status != 'success')
    """

    try:
        missing_records = spark.sql(reconciliation_sql).collect()
    except Exception as sql_err:
        _log("ATTACHMENT-ERROR", f"Reconciliation query failed: {sql_err}")
        return {"downloaded": 0, "failed": 0, "bytes_written": 0}

    total_pending = len(missing_records)
    if total_pending == 0:
        _log("ATTACHMENT-SCAN", "Attachment check complete: 0 pending downloads.")
        return {"downloaded": 0, "failed": 0, "bytes_written": 0}

    _log("ATTACHMENT-START", f"Identified {total_pending} pending files. Starting safe worker pool (5 workers @ strictly 5 QPS)...")

    # Strictly enforced 5 requests per second
    rate_limiter = RateLimiter(qps=5.0)

    def _worker(row):
        rate_limiter.wait()

        att_id = str(row["attachment_id"])
        parent_id = str(row["parent_id"])
        parent_type = str(row["parent_type"])

        meta_url = f"{client.session.base_url.rstrip('/')}/services/attachments/v1/attachments/{att_id}"
        try:
            resp = client.session.request(
                "GET",
                meta_url,
                headers={
                    "X-Api-Key": client.session.credentials.api_key,
                    "X-User-Token": client.session.credentials.user_token,
                    "X-User-Email": client.session.credentials.user_email,
                },
            )
            raw_json = resp if isinstance(resp, dict) else {}
        except Exception as api_err:
            return {
                "parent_id": parent_id,
                "parent_type": parent_type,
                "attachment_id": att_id,
                "file_name": f"{att_id}.bin",
                "file_size": 0,
                "uploaded_at": "",
                "download_status": "failed",
                "download_error": str(api_err)[:500],
                "downloaded_at": "",
                "volume_path": "",
            }

        data_obj = raw_json.get("data", {}) if isinstance(raw_json, dict) else {}
        attr = data_obj.get("attributes", {}) if isinstance(data_obj, dict) else {}
        download_url = attr.get("download_url")
        file_name = sanitize_filename(str(attr.get("file_name") or f"{att_id}.bin"))
        file_size = int(attr.get("file_size") or 0)
        uploaded_at = str(attr.get("uploaded_at") or "")

        if not download_url:
            return {
                "parent_id": parent_id,
                "parent_type": parent_type,
                "attachment_id": att_id,
                "file_name": file_name,
                "file_size": file_size,
                "uploaded_at": uploaded_at,
                "download_status": "failed",
                "download_error": "No download_url",
                "downloaded_at": "",
                "volume_path": "",
            }

        target_dir = os.path.join(volume_base_dir, datetime.now(timezone.utc).strftime("%Y-%m-%d"), parent_type)
        os.makedirs(target_dir, exist_ok=True)
        dest_path = os.path.join(target_dir, f"{att_id}_{file_name}")

        dl_res = download_file_stream(download_url, dest_path)

        if dl_res.success:
            return {
                "parent_id": parent_id,
                "parent_type": parent_type,
                "attachment_id": att_id,
                "file_name": file_name,
                "file_size": dl_res.bytes_written,
                "uploaded_at": uploaded_at,
                "download_status": "success",
                "download_error": "",
                "downloaded_at": datetime.now(timezone.utc).isoformat(),
                "volume_path": str(dest_path),
            }
        else:
            return {
                "parent_id": parent_id,
                "parent_type": parent_type,
                "attachment_id": att_id,
                "file_name": file_name,
                "file_size": file_size,
                "uploaded_at": uploaded_at,
                "download_status": "failed",
                "download_error": str(dl_res.error)[:500],
                "downloaded_at": "",
                "volume_path": "",
            }

    ok_count = failed_count = total_bytes = 0
    updates = []

    # Safe cap at 5 concurrent network threads
    with concurrent.futures.ThreadPoolExecutor(max_workers=min(5, total_pending)) as executor:
        futures = [executor.submit(_worker, row) for row in missing_records]
        for idx, f in enumerate(concurrent.futures.as_completed(futures), start=1):
            res = f.result()
            if res:
                if res["download_status"] == "success":
                    ok_count += 1
                    total_bytes += res["file_size"]
                else:
                    failed_count += 1
                updates.append(res)
                if idx % 10 == 0 or idx == total_pending:
                    _log("ATTACHMENT-STEP", f"Downloaded {idx}/{total_pending} files ({ok_count} succeeded, {failed_count} failed)...")

    if updates:
        _log("ATTACHMENT-COMMIT", f"Updating 'attachments' table with {len(updates)} metadata record(s)...")
        att_df = spark.createDataFrame(updates, schema=ATT_UPDATE_SCHEMA)
        att_df = att_df.withColumn("_jetstream_synced", F.current_timestamp())
        att_df = att_df.withColumn("_jetstream_deleted", F.lit("false"))
        att_df = att_df.withColumn("_jetstream_source_object", F.lit("attachments"))

        writer.append_table(spark, att_df, raw_att_table, primary_keys=["parent_id", "attachment_id"], meta=meta, instance_id=instance_id)

    mb_size = round(total_bytes / (1024 * 1024), 2)
    _log("ATTACHMENT-DONE", f"Completed: {ok_count} downloaded ({mb_size} MB), {failed_count} failed.")
    return {"downloaded": ok_count, "failed": failed_count, "bytes_written": total_bytes}