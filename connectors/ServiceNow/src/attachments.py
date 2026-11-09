"""High-level orchestration for ServiceNow attachment downloads."""

from __future__ import annotations

import concurrent.futures
from typing import Any, Dict, List

from pyspark.sql import functions as F

from src.attachment_downloader import process_attachment_download
from src.auth import ServiceNowSession
from src.config import ObjectRunConfig
from src.writer import table_exists


def download_attachments_graceful(
    spark: Any,
    obj: ObjectRunConfig,
    session: ServiceNowSession,
    *,
    retry_failed_only: bool = False,
    concurrency: int = 4,
) -> Dict[str, Any]:
    vol_base = obj.attachment_volume_path
    if not vol_base:
        return {"note": "skipped (no volume path)", "bytes_written": 0}
    meta_fqn = obj.attachment_meta_fqn
    if not table_exists(spark, meta_fqn):
        return {"note": "skipped (no meta table)", "bytes_written": 0}

    df = spark.table(meta_fqn)
    if retry_failed_only:
        df = df.filter(F.col("download_status").isin("failed", "failed_with_error", "pending"))
    else:
        df = df.filter(F.col("download_status") == "pending")

    has_link = "download_link" in df.columns
    cols = ["sys_id", "table_sys_id", "file_name", "size_bytes"]
    if has_link:
        cols.append("download_link")

    df = df.select(*cols).distinct()
    try:
        rows = df.collect()
    except Exception as exc:
        return {"failed": 1, "error": f"read failed: {exc}", "bytes_written": 0}

    if not rows:
        return {"downloaded": 0, "skipped": 0, "failed": 0, "bytes_written": 0, "volume_base": vol_base}

    targets: List[Dict[str, Any]] = []
    for r in rows:
        # Convert PySpark Row to dict so .get() works cleanly
        r_dict = r.asDict()
        targets.append({
            "sys_id": str(r_dict.get("sys_id") or ""),
            "table_sys_id": str(r_dict.get("table_sys_id") or ""),
            "url": str(r_dict.get("download_link")) if (has_link and r_dict.get("download_link")) else None,
            "name": str(r_dict.get("file_name") or ""),
            "size": int(r_dict.get("size_bytes")) if r_dict.get("size_bytes") is not None else 0,
        })

    ok = skip = fail = bytes_out = 0
    updates: List[Dict[str, Any]] = []

    with concurrent.futures.ThreadPoolExecutor(max_workers=concurrency) as pool:
        futures = {
            pool.submit(
                process_attachment_download,
                spark=spark,
                session=session,
                att_sys_id=tgt["sys_id"],
                table_sys_id=tgt["table_sys_id"],
                download_link=tgt["url"],
                file_name=tgt["name"],
                size_bytes=tgt["size"],
                volume_base_path=vol_base,
            ): tgt["sys_id"]
            for tgt in targets
        }
        for fut in concurrent.futures.as_completed(futures):
            res = fut.result()
            updates.append(res)
            st = res.get("status")
            if st == "success":
                ok += 1
                bytes_out += int(res.get("bytes_written", 0) or 0)
            elif st == "skipped":
                skip += 1
            else:
                fail += 1

    if updates:
        _apply_status_updates(spark, meta_fqn, updates, obj.table_format)

    return {
        "downloaded": ok,
        "skipped": skip,
        "failed": fail,
        "bytes_written": bytes_out,
        "volume_base": vol_base,
    }


def _apply_status_updates(spark: Any, dest_fqn: str, updates: List[Dict[str, Any]], format_name: str) -> None:
    from pyspark.sql.types import StringType, StructField, StructType, TimestampType
    schema = StructType([
        StructField("sys_id", StringType(), True),
        StructField("download_status", StringType(), True),
        StructField("volume_path", StringType(), True),
        StructField("download_error", StringType(), True),
        StructField("downloaded_at", TimestampType(), True),
    ])
    df_up = spark.createDataFrame(updates, schema=schema)
    df_up.createOrReplaceTempView("att_updates_tmp")

    merge_sql = f"""
    MERGE INTO {dest_fqn} t
    USING att_updates_tmp s
    ON t.sys_id = s.sys_id
    WHEN MATCHED THEN UPDATE SET
        t.download_status = s.download_status,
        t.volume_path = s.volume_path,
        t.download_error = s.download_error,
        t.downloaded_at = s.downloaded_at
    """
    spark.sql(merge_sql)