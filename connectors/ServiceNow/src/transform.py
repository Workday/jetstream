"""Transform ServiceNow JSON into Spark DataFrames with Jetstream system columns."""

from __future__ import annotations

import json
import re
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional, Set, Tuple

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    BooleanType,
    DoubleType,
    LongType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

from src.config import ObjectRunConfig

JETSTREAM_RESERVED = (
    "_jetstream_synced",
    "_jetstream_deleted",
    "_jetstream_source_object",
)

_ISO_TS = re.compile(
    r"^\d{4}-\d{2}-\d{2}([ T]\d{2}:\d{2}:\d{2}(\.\d+)?(Z|[+-]\d{2}:?\d{2})?)?$"
)


def build_dataframe(
    spark: SparkSession,
    records: List[dict[str, Any]],
    cfg: ObjectRunConfig,
    *,
    run_id: str = "",
) -> Tuple[DataFrame, List[str]]:
    _ = run_id
    if not records:
        empty = spark.createDataFrame([], schema="sys_id STRING")
        return empty, []

    contract = None if cfg.fetch_all_fields else list(cfg.fields)
    unexpected: Set[str] = set()
    rows: List[dict[str, Any]] = []

    for r in records:
        flat = _jsonify(_flatten_record(r))
        for col in JETSTREAM_RESERVED:
            flat.pop(col, None)

        if contract is not None:
            extras = set(flat.keys()) - set(contract)
            unexpected |= extras
            row = {c: _coerce_sn_value(flat.get(c)) for c in contract}
            rows.append(row)
        else:
            rows.append({k: _coerce_sn_value(v) for k, v in flat.items()})

    schema = _infer_batch_schema(rows)
    df = spark.createDataFrame(rows, schema=schema)
    df = (
        df.withColumn("_jetstream_synced", F.current_timestamp())
        .withColumn("_jetstream_deleted", F.lit(False))
        .withColumn("_jetstream_source_object", F.lit(cfg.source_object))
    )
    return df, sorted(unexpected)


def build_attachment_meta_dataframe(
    spark: SparkSession,
    attachments: List[dict[str, Any]],
    cfg: ObjectRunConfig,
    *,
    run_id: str = "",
) -> DataFrame:
    _ = run_id
    rows: List[dict[str, Any]] = []
    for a in attachments:
        flat = _flatten_record(a)
        table_sys_id = str(flat.get("table_sys_id") or "")
        size_raw = flat.get("size_bytes")
        try:
            size_bytes = int(size_raw) if size_raw not in (None, "") else 0
        except (TypeError, ValueError):
            size_bytes = 0
        rows.append({
            "sys_id": _as_str(flat.get("sys_id")),
            "table_name": _as_str(flat.get("table_name") or cfg.sn_table),
            "table_sys_id": table_sys_id,
            "file_name": _as_str(flat.get("file_name")),
            "content_type": _as_str(flat.get("content_type")),
            "size_bytes": size_bytes,
            "sys_created_on": _as_str(flat.get("sys_created_on")),
            "sys_updated_on": _as_str(flat.get("sys_updated_on")),
            "download_link": _as_str(flat.get("download_link")),
            "download_status": "pending",
            "volume_path": "",
            "download_error": "",
            "downloaded_at": None,
        })

    if not rows:
        empty = spark.createDataFrame([], schema="sys_id STRING")
        return empty

    att_schema = StructType([
        StructField("sys_id", StringType(), True),
        StructField("table_name", StringType(), True),
        StructField("table_sys_id", StringType(), True),
        StructField("file_name", StringType(), True),
        StructField("content_type", StringType(), True),
        StructField("size_bytes", LongType(), True),
        StructField("sys_created_on", StringType(), True),
        StructField("sys_updated_on", StringType(), True),
        StructField("download_link", StringType(), True),
        StructField("download_status", StringType(), True),
        StructField("volume_path", StringType(), True),
        StructField("download_error", StringType(), True),
        StructField("downloaded_at", TimestampType(), True),
    ])
    df = spark.createDataFrame(rows, schema=att_schema)
    return (
        df.withColumn("_jetstream_synced", F.current_timestamp())
        .withColumn("_jetstream_deleted", F.lit(False))
        .withColumn("_jetstream_source_object", F.lit("/api/now/table/sys_attachment"))
    )


def build_journal_dataframe(
    spark: SparkSession,
    journals: List[dict[str, Any]],
    cfg: ObjectRunConfig,
    *,
    run_id: str = "",
) -> DataFrame:
    _ = run_id
    rows: List[dict[str, Any]] = []
    for j in journals:
        flat = _flatten_record(j)
        rows.append({
            "sys_id": _as_str(flat.get("sys_id")),
            "name": _as_str(flat.get("name") or cfg.sn_table),
            "element": _as_str(flat.get("element")),
            "element_id": _as_str(flat.get("element_id")),
            "value": _as_str(flat.get("value")),
            "sys_created_on": _as_str(flat.get("sys_created_on")),
            "sys_created_by": _as_str(flat.get("sys_created_by")),
        })

    if not rows:
        return spark.createDataFrame([], schema="sys_id STRING")

    j_schema = StructType([
        StructField("sys_id", StringType(), True),
        StructField("name", StringType(), True),
        StructField("element", StringType(), True),
        StructField("element_id", StringType(), True),
        StructField("value", StringType(), True),
        StructField("sys_created_on", StringType(), True),
        StructField("sys_created_by", StringType(), True),
    ])
    df = spark.createDataFrame(rows, schema=j_schema)
    return (
        df.withColumn("_jetstream_synced", F.current_timestamp())
        .withColumn("_jetstream_deleted", F.lit(False))
        .withColumn("_jetstream_source_object", F.lit("/api/now/table/sys_journal_field"))
    )


def _flatten_record(record: dict[str, Any]) -> dict[str, Any]:
    flat: dict[str, Any] = {}
    for key, value in record.items():
        if isinstance(value, dict) and "value" in value:
            flat[key] = value.get("value")
        else:
            flat[key] = value
    return flat


def _jsonify(row: dict[str, Any]) -> dict[str, Any]:
    out: dict[str, Any] = {}
    for k, v in row.items():
        if isinstance(v, (dict, list)):
            out[k] = json.dumps(v, default=str)
        else:
            out[k] = v
    return out


def _as_str(v: Any) -> Optional[str]:
    if v is None:
        return None
    return str(v)


def _coerce_sn_value(v: Any) -> Any:
    if v is None:
        return None
    if isinstance(v, (bool, int, float, datetime)):
        return v
    if isinstance(v, (dict, list)):
        return json.dumps(v, default=str)
    if not isinstance(v, str):
        return str(v)

    s = v.strip()
    if s == "":
        return None
    low = s.lower()
    if low in ("true", "false"):
        return low == "true"
    if re.fullmatch(r"-?\d+", s):
        try:
            return int(s)
        except ValueError:
            return s
    if re.fullmatch(r"-?\d+\.\d+", s):
        try:
            return float(s)
        except ValueError:
            return s
    if _ISO_TS.match(s):
        try:
            if re.fullmatch(r"\d{4}-\d{2}-\d{2}", s):
                return datetime.fromisoformat(s).replace(tzinfo=timezone.utc)
            normalized = s.replace("Z", "+00:00")
            if " " in normalized and "T" not in normalized:
                normalized = normalized.replace(" ", "T", 1)
            return datetime.fromisoformat(normalized)
        except ValueError:
            return s
    return s


def _spark_type_of(v: Any) -> Any:
    if isinstance(v, bool):
        return BooleanType()
    if isinstance(v, int):
        return LongType()
    if isinstance(v, float):
        return DoubleType()
    if isinstance(v, datetime):
        return TimestampType()
    return StringType()


def _widen_spark_type(current: Any, incoming: Any) -> Any:
    if current is None:
        return incoming
    if isinstance(current, StringType) or isinstance(incoming, StringType):
        return StringType()
    if type(current) is type(incoming):
        return current
    numeric = (BooleanType, LongType, DoubleType)
    if isinstance(current, numeric) and isinstance(incoming, numeric):
        if isinstance(current, DoubleType) or isinstance(incoming, DoubleType):
            return DoubleType()
        if isinstance(current, LongType) or isinstance(incoming, LongType):
            return LongType()
        return BooleanType()
    return StringType()


def _infer_batch_schema(rows: List[dict[str, Any]]) -> StructType:
    seen_order: Dict[str, None] = {}
    for r in rows:
        for k in r.keys():
            seen_order.setdefault(k, None)

    fields: List[StructField] = []
    for k in seen_order:
        inferred: Any = None
        for r in rows:
            v = r.get(k)
            if v is None:
                continue
            inferred = _widen_spark_type(inferred, _spark_type_of(v))
            if isinstance(inferred, StringType):
                break
        fields.append(StructField(k, inferred if inferred is not None else StringType(), True))
    return StructType(fields)