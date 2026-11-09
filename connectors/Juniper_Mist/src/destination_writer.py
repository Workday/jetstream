"""Spark → Iceberg writer with spec-compliant _jetstream_* metadata columns.

Row-level system columns (spec §10, §11.3):
    _jetstream_synced         TIMESTAMP        set on every MERGE row
    _jetstream_deleted        BOOLEAN NOT NULL tombstone flag
    _jetstream_source_object  STRING           logical API object name

Schema-drift detection: on first write per (run, object) after the target
table exists, compare incoming DataFrame columns to catalog columns and
return SchemaChange records so pipeline_orchestrator can log them.

MERGE pattern:
  - Deduplicate by PK before MERGE (last-wins) to prevent
    DELTA_MULTIPLE_SOURCE_ROW_MATCHING_TARGET_ROW_IN_MERGE.
  - Add new columns via ALTER TABLE ADD COLUMN before MERGE.
  - Target columns absent from the incoming batch are omitted from SET/INSERT.
  - Tables are created lazily using df.writeTo(...).using("iceberg").create().
"""
from __future__ import annotations

import json
import logging
import time
from dataclasses import dataclass, field
from typing import Any, Iterable, List, Mapping, Optional, Sequence

from src.connector_config import ObjectCfg


log = logging.getLogger(__name__)

_MERGE_MAX_RETRY_ATTEMPTS = 5
_MERGE_RETRY_BACKOFF_SECS = 1.0
_MERGE_RETRY_BACKOFF_MULTIPLIER = 2.0

METADATA_COLUMNS = (
    "_jetstream_synced",
    "_jetstream_deleted",
    "_jetstream_source_object",
)


def _spark_sql_with_retry(spark: Any, sql: str) -> Any:
    """Run spark.sql with retry on Delta/Iceberg concurrent-modification conflicts."""
    backoff = _MERGE_RETRY_BACKOFF_SECS
    for attempt in range(1, _MERGE_MAX_RETRY_ATTEMPTS + 1):
        try:
            return spark.sql(sql)
        except Exception as exc:  # NOSONAR
            name = type(exc).__name__
            transient = "Concurrent" in name or "MetadataChanged" in name
            if not transient or attempt >= _MERGE_MAX_RETRY_ATTEMPTS:
                raise
            log.warning(
                "concurrency conflict on attempt %d/%d, retrying in %.1fs: %s",
                attempt, _MERGE_MAX_RETRY_ATTEMPTS, backoff, exc,
            )
            time.sleep(backoff)
            backoff *= _MERGE_RETRY_BACKOFF_MULTIPLIER


@dataclass
class SchemaChange:
    change_type: str            # 'create_table' | 'add_column' | 'type_widened'
    column_name: Optional[str] = None
    old_type: Optional[str] = None
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
    """MERGE one batch of records into the object's Iceberg table."""
    result = WriteResult(rows_read=len(records))
    if not records:
        return result

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

    if not _table_exists(spark, destination_catalog, destination_schema, obj):
        df.writeTo(target).using("iceberg").create()
        result.rows_inserted = df.count()
        result.schema_changes.append(SchemaChange(change_type="create_table"))
        return result

    # Schema reconciliation: add new columns and widen types where needed.
    full_name = f"{destination_catalog}.{destination_schema}.{obj.name}"
    df = _evolve_schema(spark, df, target, obj, full_name, result)

    # Re-read column list from df after _evolve_schema may have re-cast columns.
    incoming_cols_ordered = list(df.columns)
    df.createOrReplaceTempView("_jetstream_incoming")
    on_clause = " AND ".join(f"t.`{pk}` = s.`{pk}`" for pk in obj.primary_key)
    set_clause = ", ".join(f"t.`{c}` = s.`{c}`" for c in incoming_cols_ordered)
    insert_cols_list = ", ".join(f"`{c}`" for c in incoming_cols_ordered)
    insert_vals_list = ", ".join(f"s.`{c}`" for c in incoming_cols_ordered)

    # Guard against phantom catalog entries: a previous run may have appeared to create
    # the table (no Python exception) but Unity Catalog failed to register it — leaving
    # spark.catalog.tableExists() returning True while the table is not actually queryable.
    try:
        pre_count = spark.sql(f"SELECT COUNT(*) AS c FROM {target}").collect()[0]["c"]  # NOSONAR
    except Exception as _phantom_exc:  # NOSONAR
        log.warning(
            "Table %s is in Spark catalog but not queryable (%s); "
            "recreating from current batch.",
            target, _phantom_exc,
        )
        df.writeTo(target).using("iceberg").createOrReplace()
        result.rows_inserted = df.count()
        result.schema_changes.append(SchemaChange(change_type="create_table"))
        return result
    match_count = spark.sql(
        f"SELECT COUNT(*) AS c FROM {target} t JOIN _jetstream_incoming s ON {on_clause}"
    ).collect()[0]["c"]  # NOSONAR

    _spark_sql_with_retry(
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
    return result


def write_snapshot(
    spark: Any,
    *,
    destination_catalog: str,
    destination_schema: str,
    obj: ObjectCfg,
    records: Sequence[Mapping[str, Any]],
    source_object: str,
) -> WriteResult:
    """Append-only write for live-snapshot objects (device_stats, clients, etc.).

    Each call inserts ALL records as a new snapshot batch — no MERGE, no
    tombstoning, no PK deduplication.  The `_jetstream_synced` timestamp
    differentiates rows that belong to different hourly runs, enabling
    full client-movement history to be reconstructed across snapshots.

    Schema evolution (add_column / type_widened) is applied identically to
    write_page so the table always reflects the widest observed schema.
    """
    result = WriteResult(rows_read=len(records))
    if not records:
        return result

    df = _build_df(spark, records, source_object)
    target = target_qual(destination_catalog, destination_schema, obj)

    if not _table_exists(spark, destination_catalog, destination_schema, obj):
        df.writeTo(target).using("iceberg").create()
        result.rows_inserted = df.count()
        result.schema_changes.append(SchemaChange(change_type="create_table"))
        return result

    # Schema reconciliation (same rules as write_page).
    full_name = f"{destination_catalog}.{destination_schema}.{obj.name}"
    df = _evolve_schema(spark, df, target, obj, full_name, result)

    # Append — no MERGE, no dedup.  Every row is a distinct point-in-time record.
    df.writeTo(target).using("iceberg").append()
    result.rows_inserted = df.count()
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

    Returns the number of rows tombstoned.
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
    ).collect()[0]["c"]  # NOSONAR
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
    )  # NOSONAR
    post_tomb = spark.sql(
        f"SELECT COUNT(*) AS c FROM {target} WHERE _jetstream_deleted = TRUE"
    ).collect()[0]["c"]  # NOSONAR
    spark.catalog.dropTempView("_jetstream_live_pks")
    return int(post_tomb - pre_tomb)


# ---------------------------------------------------------------------------
# internals
# ---------------------------------------------------------------------------

def _evolve_schema(
    spark: Any,
    df: Any,
    target: str,
    obj: ObjectCfg,
    full_name: str,
    result: WriteResult,
) -> Any:
    """Apply schema evolution in-place: numeric→STRING widening and add_column.

    Returns (possibly re-cast) DataFrame.  Appends SchemaChange records to
    `result`.  Shared by write_page (MERGE) and write_snapshot (append).
    """
    target_col_info = {c.name: c.dataType for c in spark.catalog.listColumns(full_name)}
    target_cols = set(target_col_info.keys())
    incoming_cols_ordered = list(df.columns)
    dtypes = dict(df.dtypes)

    # Numeric → STRING type widening: Mist sometimes returns template variable
    # strings (e.g. '{{GUEST_WIFI_USER_VLAN}}') in fields that were previously
    # all-integer. We promote the column to STRING in both the incoming DataFrame
    # and the destination table so the write succeeds.
    _NUMERIC_SPARK_TYPES = {"bigint", "int", "long", "double", "float"}
    for col_name in incoming_cols_ordered:
        if col_name not in target_col_info:
            continue
        incoming_type = dtypes.get(col_name, "string").lower()
        target_type = (target_col_info[col_name] or "string").lower()
        if incoming_type == "string" and target_type in _NUMERIC_SPARK_TYPES:
            try:
                spark.sql(  # NOSONAR
                    f"ALTER TABLE {target} ALTER COLUMN `{col_name}` TYPE STRING"
                )
            except Exception as exc:  # NOSONAR
                log.warning(
                    "could not widen %s.%s type to STRING (%s); "
                    "nullifying non-castable values instead",
                    obj.name, col_name, exc,
                )
                df = df.withColumn(col_name, df[col_name].cast("string"))
            else:
                result.schema_changes.append(
                    SchemaChange(
                        change_type="type_widened",
                        column_name=col_name,
                        old_type=target_type,
                        new_type="string",
                    )
                )
                log.warning(
                    "type widened: %s.%s %s → string (mixed numeric+string values from API)",
                    obj.name, col_name, target_type,
                )

    for col_name in incoming_cols_ordered:
        if col_name in target_cols:
            continue
        col_type = dtypes.get(col_name, "string")
        spark.sql(f"ALTER TABLE {target} ADD COLUMN `{col_name}` {col_type}")  # NOSONAR
        result.schema_changes.append(
            SchemaChange(change_type="add_column", column_name=col_name, new_type=col_type)
        )
        target_cols.add(col_name)

    return df


def _build_df(spark: Any, records, source_object: str):
    """Wrap records as a Spark DataFrame with _jetstream_* columns appended."""
    from datetime import datetime, timezone

    now = datetime.now(timezone.utc)
    enriched: List[dict] = []
    for r in records:
        row = dict(r)
        for reserved in METADATA_COLUMNS:
            row.pop(reserved, None)
        row["_jetstream_synced"] = now
        row["_jetstream_deleted"] = False
        row["_jetstream_source_object"] = source_object
        enriched.append(row)

    schema, col_types = _infer_batch_schema(enriched)
    enriched = _coerce_rows(enriched, col_types)
    return spark.createDataFrame(enriched, schema=schema)


def _infer_batch_schema(rows: List[dict]):
    """Build a StructType by scanning ALL values per column across the batch.

    Mist API fields like `timestamp`, `enter`, `exit`, `_ttl` are returned as
    either float or int depending on whether sub-second precision is present.
    Stopping at the first non-None value causes DoubleType to be inferred when
    the first record has a float, then PySpark rejects later int values.

    Rules (widest-type wins across the whole batch):
      - Any datetime value          → TimestampType
      - Any float (or float + int)  → DoubleType   (int values coerced to float)
      - All-int values              → LongType
      - Any bool (no numeric)       → BooleanType
      - All-None or string/mixed    → StringType
    """
    from datetime import datetime
    from pyspark.sql.types import (
        BooleanType, DoubleType, LongType, StringType, StructField,
        StructType, TimestampType,
    )

    seen_order: dict = {}
    for r in rows:
        for k in r.keys():
            seen_order.setdefault(k, None)

    fields: list = []
    col_types: dict = {}
    for k in seen_order:
        has_dt = has_float = has_int = has_bool = has_str = False
        for r in rows:
            v = r.get(k)
            if v is None:
                continue
            if isinstance(v, bool):
                has_bool = True
            elif isinstance(v, float):
                has_float = True
            elif isinstance(v, int):
                has_int = True
            elif isinstance(v, datetime):
                has_dt = True
            else:
                has_str = True

        if has_dt and not (has_float or has_int or has_bool or has_str):
            t = TimestampType()
        elif has_str and (has_int or has_float or has_bool):
            # Mixed: API returned both strings and numerics for this column
            # (e.g. vlan_id can be 100 or '{{GUEST_WIFI_USER_VLAN}}').
            # StringType is the only safe choice; numerics coerced to str below.
            t = StringType()
        elif has_float:
            # Mixed float+int → widen to DoubleType; ints will be coerced below.
            t = DoubleType()
        elif has_int:
            t = LongType()
        elif has_bool:
            t = BooleanType()
        else:
            t = StringType()

        col_types[k] = t
        fields.append(StructField(k, t, True))

    return StructType(fields), col_types


def _coerce_rows(rows: List[dict], col_types: dict) -> List[dict]:
    """Cast individual values to match the inferred column type.

    Handles three cases that PySpark's strict verifier rejects:
      - DoubleType column with int value  → cast int to float
      - LongType column with float value  → cast float to int (truncate)
      - StringType column with numeric    → cast to str (mixed API fields like vlan_id)
    """
    from pyspark.sql.types import DoubleType, LongType, StringType

    double_cols = {k for k, t in col_types.items() if isinstance(t, DoubleType)}
    long_cols = {k for k, t in col_types.items() if isinstance(t, LongType)}
    str_cols = {k for k, t in col_types.items() if isinstance(t, StringType)}

    if not double_cols and not long_cols and not str_cols:
        return rows

    coerced = []
    for r in rows:
        row = dict(r)
        for k in double_cols:
            v = row.get(k)
            if isinstance(v, int) and not isinstance(v, bool):
                row[k] = float(v)
        for k in long_cols:
            v = row.get(k)
            if isinstance(v, float):
                row[k] = int(v)
        for k in str_cols:
            v = row.get(k)
            if v is not None and not isinstance(v, str):
                row[k] = str(v)
        coerced.append(row)
    return coerced


def _table_exists(
    spark: Any, destination_catalog: str, destination_schema: str, obj: ObjectCfg
) -> bool:
    """Check whether the destination Iceberg table exists in Unity Catalog.

    Uses a zero-row SQL probe rather than spark.catalog.tableExists() because
    the Spark catalog cache can return True for tables that Unity Catalog never
    successfully registered (phantom entries).  A SQL probe forces a round-trip
    to Unity Catalog and always reflects the true state.
    """
    try:
        spark.sql(
            f"SELECT 1 FROM `{destination_catalog}`.`{destination_schema}`"
            f".`{obj.name}` LIMIT 0"  # NOSONAR
        )
        return True
    except Exception:  # NOSONAR
        return False


def _dedupe_by_pk(
    records: Sequence[Mapping[str, Any]],
    pk_cols: Sequence[str],
) -> List[Mapping[str, Any]]:
    """Deduplicate by PK tuple, keeping the last occurrence. Drops records
    missing any PK column (they'd fail the MERGE ON clause)."""
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
