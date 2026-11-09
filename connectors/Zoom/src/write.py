"""Spark → Iceberg writer with spec-compliant _jetstream_* metadata columns.

Row-level system columns (spec §10, §11.3):
    _jetstream_synced         TIMESTAMP        every table
    _jetstream_deleted        BOOLEAN NOT NULL every table
    _jetstream_source_object  STRING           optional; added on fan-out
                                              tables where rows in one target
                                              come from multiple source paths

Note: `_jetstream_id` / `_jetstream_index` are NOT added because every zoom
object has a natural PK; those columns are only for keyless tables (spec §10).

The writer returns a WriteResult with true row counts (rows_inserted,
rows_updated, rows_deleted) so `connector_object_run` / `connector_run` /
`connector_table_registry` can report real numbers.

Schema-drift detection: on the FIRST write per (run, object) after the target
table already exists, compare the incoming DataFrame columns to
`spark.catalog.listColumns(...)` and return the added columns as
`SchemaChange` records so `pipeline.run()` can log them.
"""
from __future__ import annotations

import json
import logging
import time
from dataclasses import dataclass, field
from typing import Any, Iterable, List, Mapping, Optional, Sequence, Tuple

from src.config import ObjectCfg


log = logging.getLogger(__name__)

# Delta / Iceberg on Databricks can raise concurrent-modification exceptions
# when auto-OPTIMIZE or another writer touches the same table mid-MERGE.
# Delta docs recommend retrying the failed transaction. See
# https://docs.databricks.com/delta/concurrency-control.html
_MERGE_MAX_RETRY_ATTEMPTS = 5
_MERGE_RETRY_BACKOFF_SECS = 1.0
_MERGE_RETRY_BACKOFF_MULTIPLIER = 2.0


def _spark_sql_with_delta_retry(spark: Any, sql: str) -> Any:
    """Run a spark.sql with retry on Delta concurrent-modification conflicts.

    Retries on any exception whose class name contains 'Concurrent' or
    'MetadataChanged' (Delta's family of transient conflict exceptions).
    Other exceptions bubble up unchanged.
    """
    backoff = _MERGE_RETRY_BACKOFF_SECS
    for attempt in range(1, _MERGE_MAX_RETRY_ATTEMPTS + 1):
        try:
            return spark.sql(sql)
        # Delta's transient conflicts are a family of exception classes
        # matched by class-name substring (Concurrent*, MetadataChanged*);
        # narrowing here would drop the retry path.
        except Exception as exc:  # NOSONAR
            name = type(exc).__name__
            transient = "Concurrent" in name or "MetadataChanged" in name
            if not transient or attempt >= _MERGE_MAX_RETRY_ATTEMPTS:
                raise
            log.warning(
                "delta concurrency conflict on attempt %d/%d, retrying in %.1fs: %s",
                attempt, _MERGE_MAX_RETRY_ATTEMPTS, backoff, exc,
            )
            time.sleep(backoff)
            backoff *= _MERGE_RETRY_BACKOFF_MULTIPLIER


METADATA_COLUMNS = (
    "_jetstream_synced",
    "_jetstream_deleted",
    "_jetstream_source_object",
)


@dataclass
class SchemaChange:
    change_type: str            # 'create_table' | 'add_column'
    column_name: Optional[str] = None
    new_type: Optional[str] = None


@dataclass
class WriteResult:
    rows_read: int = 0
    rows_inserted: int = 0
    rows_updated: int = 0
    rows_deleted: int = 0
    rows_unchanged: int = 0
    schema_changes: List[SchemaChange] = field(default_factory=list)


def target_qual(destination_catalog: str, destination_schema: str, obj: ObjectCfg) -> str:
    return f"`{destination_catalog}`.`{destination_schema}`.`{obj.name}`"


def destination_table_name(obj: ObjectCfg) -> str:
    return obj.name


def write_page(
    spark: Any,
    *,
    destination_catalog: str,
    destination_schema: str,
    obj: ObjectCfg,
    records: Sequence[Mapping[str, Any]],
    source_object: str,
) -> WriteResult:
    """MERGE one batch of records into the object's raw table. Returns a
    WriteResult with true row-count breakdown and any detected schema changes.

    `source_object` is stamped into every row's `_jetstream_source_object`
    column, and also drives fan-out lineage (the specific parent id used).
    """
    result = WriteResult(rows_read=len(records))
    if not records:
        return result

    # Deduplicate by primary key before MERGE. Zoom endpoints can return the
    # same record more than once within an accumulated batch (co-hosted
    # meetings appearing under multiple users, pagination overlap, etc.).
    # Delta/Iceberg MERGE fails with DELTA_MULTIPLE_SOURCE_ROW_MATCHING_TARGET_ROW_IN_MERGE
    # if the source has duplicate PKs. Keep the last occurrence per PK
    # (Fivetran-style "later wins").
    before_dedup = len(records)
    records = _dedupe_by_pk(records, obj.primary_key)
    dropped = before_dedup - len(records)
    if dropped > 0:
        log.info(
            "dedup: %s dropped %d duplicate PK row(s) from batch of %d",
            obj.name, dropped, before_dedup,
        )
    if not records:
        return result

    df = _build_df(spark, records, source_object)
    target = target_qual(destination_catalog, destination_schema, obj)

    # First write ever: create the table from the DF schema.
    if not _table_exists(spark, destination_catalog, destination_schema, obj):
        df.writeTo(target).using("iceberg").create()
        n = df.count()
        result.rows_inserted = n
        result.schema_changes.append(SchemaChange(change_type="create_table"))
        return result

    # --------------------------------------------------------------
    # Schema reconciliation.
    #
    # Two independent mismatches can happen between the incoming batch
    # and the existing target table:
    #
    #   (a) Incoming has NEW columns that don't exist on target.
    #       We evolve the target via `ALTER TABLE ADD COLUMN` so the
    #       subsequent MERGE can write them. Recorded as `add_column`
    #       schema-change events by the caller.
    #
    #   (b) Target has columns that the incoming batch doesn't. Common
    #       for Zoom endpoints where sparse fields (e.g. activation_code
    #       on rooms) only appear on some pages. We simply omit those
    #       columns from the MERGE — target rows keep their existing
    #       values on UPDATE and get NULL on INSERT for those columns.
    # --------------------------------------------------------------
    full_name = f"{destination_catalog}.{destination_schema}.{obj.name}"
    target_cols = {c.name for c in spark.catalog.listColumns(full_name)}
    incoming_cols_ordered = list(df.columns)
    dtypes = dict(df.dtypes)

    # (a) apply new columns to the target
    for col_name in incoming_cols_ordered:
        if col_name in target_cols:
            continue
        col_type = dtypes.get(col_name, "string")
        # target/col_name/col_type are code-derived (ObjectCfg + Spark catalog),
        # never user input; no SQL injection surface.
        spark.sql(f"ALTER TABLE {target} ADD COLUMN `{col_name}` {col_type}")  # NOSONAR
        result.schema_changes.append(
            SchemaChange(change_type="add_column", column_name=col_name, new_type=col_type)
        )
        target_cols.add(col_name)

    # (b) build MERGE with explicit column list — only what's in the batch
    df.createOrReplaceTempView("_jetstream_incoming")
    on_clause = " AND ".join(f"t.`{pk}` = s.`{pk}`" for pk in obj.primary_key)
    set_clause = ", ".join(
        f"t.`{c}` = s.`{c}`" for c in incoming_cols_ordered
    )
    insert_cols_list = ", ".join(f"`{c}`" for c in incoming_cols_ordered)
    insert_vals_list = ", ".join(f"s.`{c}`" for c in incoming_cols_ordered)

    # `target` is a fully-qualified table name derived from ObjectCfg (code),
    # not user input — safe to interpolate.
    pre_count = spark.sql(f"SELECT COUNT(*) AS c FROM {target}").collect()[0]["c"]  # NOSONAR
    match_count = spark.sql(
        f"""
        SELECT COUNT(*) AS c
        FROM {target} t JOIN _jetstream_incoming s ON {on_clause}
        """
    ).collect()[0]["c"]

    _spark_sql_with_delta_retry(
        spark,
        f"""
        MERGE INTO {target} t
        USING _jetstream_incoming s
        ON {on_clause}
        WHEN MATCHED THEN UPDATE SET {set_clause}
        WHEN NOT MATCHED THEN INSERT ({insert_cols_list}) VALUES ({insert_vals_list})
        """,
    )
    post_count = spark.sql(f"SELECT COUNT(*) AS c FROM {target}").collect()[0]["c"]  # NOSONAR
    spark.catalog.dropTempView("_jetstream_incoming")

    result.rows_inserted = int(post_count - pre_count)
    result.rows_updated = int(match_count)
    result.rows_unchanged = 0
    return result


def finalise_full(
    spark: Any,
    *,
    destination_catalog: str,
    destination_schema: str,
    obj: ObjectCfg,
    all_records: Iterable[Mapping[str, Any]],
) -> int:
    """Tombstone rows present in target but not in `all_records` (full-load only).

    Returns the number of rows tombstoned. Only meaningful for
    `load_strategy: full` objects — the caller is responsible for not
    calling this on incremental_cursor objects.
    """
    if not _table_exists(spark, destination_catalog, destination_schema, obj):
        return 0
    target = target_qual(destination_catalog, destination_schema, obj)
    pk_cols = list(obj.primary_key)
    rows = [
        {k: r[k] for k in pk_cols if k in r}
        for r in all_records
        if all(k in r for k in pk_cols)
    ]
    if not rows:
        return 0

    spark.createDataFrame(rows).createOrReplaceTempView("_jetstream_live_pks")
    on_clause = " AND ".join(f"t.`{pk}` = l.`{pk}`" for pk in pk_cols)

    pre_tomb = spark.sql(
        f"SELECT COUNT(*) AS c FROM {target} WHERE _jetstream_deleted = TRUE"
    ).collect()[0]["c"]
    spark.sql(
        f"""
        UPDATE {target} t
        SET _jetstream_deleted = TRUE,
            _jetstream_synced  = current_timestamp()
        WHERE NOT t._jetstream_deleted
          AND NOT EXISTS (
              SELECT 1 FROM _jetstream_live_pks l WHERE {on_clause}
          )
        """
    )
    post_tomb = spark.sql(
        f"SELECT COUNT(*) AS c FROM {target} WHERE _jetstream_deleted = TRUE"
    ).collect()[0]["c"]
    spark.catalog.dropTempView("_jetstream_live_pks")
    return int(post_tomb - pre_tomb)


# ---------------------------------------------------------------------------
# internals
# ---------------------------------------------------------------------------

def _build_df(spark: Any, records, source_object: str):
    """Wrap records as a Spark DataFrame with the _jetstream_* columns appended.

    Uses an explicitly-computed schema (never `createDataFrame` with schema
    inference alone), because Zoom returns pages where some fields are None
    for every record — e.g. `last_login_time` on users who never logged in.
    Spark's inference fails on those all-None columns with
    "CANNOT_DETERMINE_TYPE". Our schema builder falls back to StringType
    for all-None columns so the batch is always writable.
    """
    from datetime import datetime, timezone

    now = datetime.now(timezone.utc)
    enriched: List[dict] = []
    for r in records:
        row = dict(r)
        # Reserved-name handling (spec §11.3): if the source ever supplies a
        # `_jetstream_*` field, drop the source value and use ours instead.
        for reserved in METADATA_COLUMNS:
            row.pop(reserved, None)
        row = _jsonify(row)
        row["_jetstream_synced"] = now
        row["_jetstream_deleted"] = False
        row["_jetstream_source_object"] = source_object
        enriched.append(row)

    schema = _infer_batch_schema(enriched)
    return spark.createDataFrame(enriched, schema=schema)


def _infer_batch_schema(rows: List[dict]):
    """Build a StructType from a batch by walking values column-by-column.

    Behaviour:
      * Field order: first-seen (matches Zoom API response order).
      * Type per field: the type of the first non-None value seen.
      * All-None columns default to StringType (safe / nullable) so a
        subsequent page with actual values can widen via schema evolution.
      * Booleans are checked BEFORE ints (Python bool is a subclass of int).
    """
    from datetime import datetime
    from pyspark.sql.types import (
        BooleanType, DoubleType, LongType, StringType, StructField,
        StructType, TimestampType,
    )

    # Preserve insertion order of first-seen keys across the batch.
    seen_order: Dict[str, None] = {}
    for r in rows:
        for k in r.keys():
            seen_order.setdefault(k, None)

    fields: List = []
    for k in seen_order:
        inferred = StringType()
        for r in rows:
            v = r.get(k)
            if v is None:
                continue
            if isinstance(v, bool):
                inferred = BooleanType()
            elif isinstance(v, int):
                inferred = LongType()
            elif isinstance(v, float):
                inferred = DoubleType()
            elif isinstance(v, datetime):
                inferred = TimestampType()
            else:
                inferred = StringType()
            break
        fields.append(StructField(k, inferred, True))
    return StructType(fields)


def _jsonify(row: dict) -> dict:
    """Serialise nested dicts/lists to JSON strings so Spark infers a clean schema."""
    out = {}
    for k, v in row.items():
        if isinstance(v, (dict, list)):
            out[k] = json.dumps(v, default=str)
        else:
            out[k] = v
    return out


def _table_exists(spark: Any, destination_catalog: str, destination_schema: str, obj: ObjectCfg) -> bool:
    full = f"{destination_catalog}.{destination_schema}.{obj.name}"
    return spark.catalog.tableExists(full)


def _dedupe_by_pk(
    records: Sequence[Mapping[str, Any]],
    pk_cols: Sequence[str],
) -> List[Mapping[str, Any]]:
    """Return records deduplicated by the primary-key tuple, keeping the
    last occurrence of each key.

    Records missing any PK column are dropped (they'd fail the MERGE anyway).
    Preserves insertion order of the surviving keys.
    """
    if not pk_cols:
        return list(records)
    seen: dict = {}
    for r in records:
        try:
            key = tuple(r[c] for c in pk_cols)
        except KeyError:
            continue
        seen[key] = r
    return list(seen.values())
