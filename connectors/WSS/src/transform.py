"""Flatten nested WSS JSON payloads into Iceberg DataFrames."""
from __future__ import annotations

import json
import logging
from typing import Any, Dict, List, Optional
import pyspark.sql.functions as F
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.types import StringType, BooleanType

log = logging.getLogger(__name__)


def process_and_flatten_raw_batch(
    spark: SparkSession,
    batch_records: List[Dict[str, Any]],
    obj_cfg: Any,
    **kwargs: Any,
) -> DataFrame:
    """Flattens raw JSON records, purges corrupt columns, preserves original field names, and appends mandatory Jetstream metadata."""
    if not batch_records:
        return spark.createDataFrame([], schema=StringType())

    # 1. Filter out non-dict elements while retaining sparse/null-payload dicts
    valid_records = [r for r in batch_records if isinstance(r, dict)]
    if not valid_records:
        return spark.createDataFrame([], schema=StringType())

    # 2. Serialize dicts to strict double-quoted JSON strings for Spark parsing
    json_str_list = [json.dumps(r, default=str) for r in valid_records]
    rdd = spark.sparkContext.parallelize(json_str_list)
    df = spark.read.json(rdd)

    # 3. Hard purge of Spark internal corrupt record column
    if "_corrupt_record" in df.columns:
        df = df.drop("_corrupt_record")

    # 4. Flatten top-level 'attributes' struct using getField() to preserve exact original column headers safely
    if "attributes" in df.columns:
        attr_schema = df.schema["attributes"].dataType
        if hasattr(attr_schema, "fields"):
            attr_fields = [f.name for f in attr_schema.fields]
            non_attr_cols = [F.col(f"`{c}`") for c in df.columns if c != "attributes"]
            attr_cols = [F.col("attributes").getField(f_name).alias(f_name) for f_name in attr_fields]
            df = df.select(*non_attr_cols, *attr_cols)

    # 5. Flatten top-level 'links' or 'meta' if present
    for struct_col in ["links", "meta"]:
        if struct_col in df.columns:
            struct_schema = df.schema[struct_col].dataType
            if hasattr(struct_schema, "fields"):
                s_fields = [f.name for f in struct_schema.fields]
                other_cols = [F.col(f"`{c}`") for c in df.columns if c != struct_col]
                s_cols = [
                    F.col(struct_col).getField(f_name).alias(f"{struct_col}_{f_name}")
                    for f_name in s_fields
                    if f"{struct_col}_{f_name}" not in df.columns
                ]
                df = df.select(*other_cols, *s_cols)

    # 6. Ensure primary key columns exist
    pk_cols = getattr(obj_cfg, "primary_key_columns", ["id"])
    for pk in pk_cols:
        if pk not in df.columns:
            df = df.withColumn(pk, F.lit(None).cast(StringType()))

    # 7. Attach strictly requested Jetstream audit metadata columns
    source_obj_name = getattr(obj_cfg, "source_object", getattr(obj_cfg, "object_name", ""))
    now_ts = F.current_timestamp()

    df = (
        df.withColumn("_jetstream_synced", now_ts)
          .withColumn("_jetstream_deleted", F.lit(False).cast(BooleanType()))
          .withColumn("_jetstream_source_object", F.lit(str(source_obj_name)))
    )

    return df


def extract_child_relationships(
    spark: SparkSession,
    batch_records: List[Dict[str, Any]],
    object_name: str,
    **kwargs: Any,
) -> DataFrame:
    """Extracts nested child object relationships into a normalized relationship DataFrame."""
    rel_rows = []
    for rec in batch_records:
        if not isinstance(rec, dict):
            continue
        parent_id = str(rec.get("id") or "")
        if not parent_id:
            continue

        relationships = rec.get("relationships", {})
        if isinstance(relationships, dict):
            for rel_name, rel_data in relationships.items():
                if isinstance(rel_data, dict):
                    data_obj = rel_data.get("data")
                    if isinstance(data_obj, list):
                        for item in data_obj:
                            if isinstance(item, dict) and "id" in item:
                                rel_rows.append({
                                    "parent_id": parent_id,
                                    "relationship_name": rel_name,
                                    "related_entity_id": str(item["id"]),
                                    "related_entity_type": str(item.get("type", "")),
                                })
                    elif isinstance(data_obj, dict) and "id" in data_obj:
                        rel_rows.append({
                            "parent_id": parent_id,
                            "relationship_name": rel_name,
                            "related_entity_id": str(data_obj["id"]),
                            "related_entity_type": str(data_obj.get("type", "")),
                        })

    if not rel_rows:
        return None

    json_str_list = [json.dumps(r, default=str) for r in rel_rows]
    rdd = spark.sparkContext.parallelize(json_str_list)
    df = spark.read.json(rdd)

    if "_corrupt_record" in df.columns:
        df = df.drop("_corrupt_record")

    now_ts = F.current_timestamp()
    df = (
        df.withColumn("_jetstream_synced", now_ts)
          .withColumn("_jetstream_deleted", F.lit(False).cast(BooleanType()))
          .withColumn("_jetstream_source_object", F.lit(f"{object_name}/relationships"))
    )

    return df
