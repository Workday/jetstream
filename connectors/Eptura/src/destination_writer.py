"""Spark writer for the Eptura raw layer.

Each entity is a full snapshot, so one run produces exactly one write per
entity: build a DataFrame from the fetched records, MERGE it into the
destination table on the primary key, then tombstone whatever the API no
longer returns.

Nested payloads stay nested. Eptura returns deep structures — `rooms.floor`
carries the whole building and address tree, `buildings.costCenters` is an
array of structs — and downstream consumers navigate them with dotted paths
like `floor.building.address.state.name`. Records are therefore serialised to
JSON lines and handed to Spark's JSON reader, which infers real
StructType / ArrayType columns from the whole batch. Passing dicts to
`createDataFrame` instead would flatten them to strings and break that access.

Row-level system columns (spec §10, §11.3):
    _jetstream_synced   TIMESTAMP  when this row was last seen in the source
    _jetstream_deleted  BOOLEAN    TRUE once the API stops returning the row

`_jetstream_id` / `_jetstream_index` are not used: every Eptura entity has a
natural primary key, and those columns are only for keyless tables.
"""
from __future__ import annotations

import json
import logging
import time
from dataclasses import dataclass, field
from typing import Any, Dict, List, Mapping, Sequence

from src.connector_config import ObjectCfg


log = logging.getLogger(__name__)

METADATA_COLUMNS = ("_jetstream_synced", "_jetstream_deleted")

_INCOMING_VIEW = "_jetstream_incoming"

# Databricks can raise concurrent-modification errors when auto-OPTIMIZE
# touches a table mid-MERGE. The documented remedy is to retry the
# transaction: https://docs.databricks.com/delta/concurrency-control.html
_MERGE_MAX_ATTEMPTS = 5
_MERGE_BACKOFF_SECONDS = 1.0
_MERGE_BACKOFF_MULTIPLIER = 2.0


@dataclass
class SchemaChange:
    change_type: str                 # 'create_table' | 'add_column'
    column_name: str = ""
    new_type: str = ""


@dataclass
class WriteResult:
    rows_read: int = 0
    rows_inserted: int = 0
    rows_updated: int = 0
    rows_deleted: int = 0
    schema_changes: List[SchemaChange] = field(default_factory=list)


def destination_table_name(obj: ObjectCfg) -> str:
    return obj.name


def configure_session(spark: Any) -> None:
    """Ask Spark to evolve nested struct fields during MERGE.

    Top-level columns are added explicitly by `_add_missing_columns` so they
    can be reported as schema-change events; this setting covers a new field
    appearing *inside* an existing struct, which Eptura's deep payloads make
    likely.

    Serverless and policy-restricted clusters refuse some conf writes. Losing
    nested evolution degrades to a clear MERGE error on the day Eptura adds a
    field, which is far better than failing every run up front.
    """
    try:
        spark.conf.set("spark.databricks.delta.schema.autoMerge.enabled", "true")
    except Exception as exc:  # NOSONAR
        log.warning(
            "could not enable nested schema evolution (%s); a new field inside "
            "an existing struct will fail its MERGE until the column is added "
            "manually", exc,
        )


def sync_object(
    spark: Any,
    *,
    destination_catalog: str,
    destination_schema: str,
    obj: ObjectCfg,
    records: Sequence[Mapping[str, Any]],
    tombstone_guard_pct: float = 20.0,
) -> WriteResult:
    """Write one entity's full snapshot and return true row counts."""
    result = WriteResult(rows_read=len(records))
    if not records:
        # A full snapshot that came back empty is far more likely to be an
        # API problem than a genuinely empty entity. Skip rather than
        # tombstone the whole table.
        log.warning("%s returned no records — skipping write", obj.name)
        return result

    deduped = _dedupe_by_pk(records, obj.primary_key)
    if len(deduped) < len(records):
        log.info("%s: dropped %d duplicate primary key(s)",
                 obj.name, len(records) - len(deduped))

    df = _build_df(spark, deduped)
    full_name = f"{destination_catalog}.{destination_schema}.{obj.name}"
    target = f"`{destination_catalog}`.`{destination_schema}`.`{obj.name}`"

    if not spark.catalog.tableExists(full_name):
        df.writeTo(target).using("iceberg").create()
        result.rows_inserted = len(deduped)
        result.schema_changes.append(SchemaChange(change_type="create_table"))
        return result

    result.schema_changes.extend(_add_missing_columns(spark, target, full_name, df))

    df.createOrReplaceTempView(_INCOMING_VIEW)
    try:
        on_clause = " AND ".join(f"t.`{pk}` = s.`{pk}`" for pk in obj.primary_key)
        rows_before = _scalar(spark, f"SELECT COUNT(*) AS c FROM {target}")
        result.rows_updated = _scalar(
            spark,
            f"SELECT COUNT(*) AS c FROM {target} t JOIN {_INCOMING_VIEW} s ON {on_clause}",
        )
        _merge(spark, target, on_clause, list(df.columns))
        result.rows_inserted = _scalar(spark, f"SELECT COUNT(*) AS c FROM {target}") - rows_before
        result.rows_deleted = _tombstone_missing(
            spark, target, on_clause, obj.name, tombstone_guard_pct,
        )
    finally:
        spark.catalog.dropTempView(_INCOMING_VIEW)

    return result


# ---------------------------------------------------------------------------
# DataFrame construction
# ---------------------------------------------------------------------------

def _build_df(spark: Any, records: Sequence[Mapping[str, Any]]):
    """Turn records into a DataFrame with nested structure preserved.

    Spark's JSON reader scans the whole batch, so fields that are absent or
    null on the first few records still get a column, and all-null fields
    fall back to a nullable string rather than failing type inference.
    """
    from pyspark.sql.functions import current_timestamp, lit

    lines = [json.dumps(_strip_reserved(r), default=str) for r in records]
    raw = spark.createDataFrame([(line,) for line in lines], "value STRING")
    return (
        spark.read.json(raw)
        .withColumn("_jetstream_synced", current_timestamp())
        .withColumn("_jetstream_deleted", lit(False))
    )


def _strip_reserved(record: Mapping[str, Any]) -> Dict[str, Any]:
    """Drop any source field colliding with our system columns (spec §11.3)."""
    return {k: v for k, v in record.items() if k not in METADATA_COLUMNS}


def _dedupe_by_pk(
    records: Sequence[Mapping[str, Any]],
    pk_cols: Sequence[str],
) -> List[Mapping[str, Any]]:
    """Keep the last record per primary key.

    MERGE fails outright if two source rows match the same target row, and
    offset pagination can repeat a record when the collection shifts under
    us mid-run. Records missing a key column are dropped — they could never
    be merged anyway.
    """
    seen: Dict[tuple, Mapping[str, Any]] = {}
    for r in records:
        try:
            seen[tuple(r[c] for c in pk_cols)] = r
        except KeyError:
            continue
    return list(seen.values())


# ---------------------------------------------------------------------------
# schema evolution
# ---------------------------------------------------------------------------

def _add_missing_columns(spark: Any, target: str, full_name: str, df) -> List[SchemaChange]:
    """Add top-level columns the target doesn't have yet.

    Columns the target has but this batch lacks are simply left out of the
    MERGE: existing rows keep their values, new rows get NULL.
    """
    existing = {c.name for c in spark.catalog.listColumns(full_name)}
    dtypes = dict(df.dtypes)
    changes: List[SchemaChange] = []
    for name in df.columns:
        if name in existing:
            continue
        col_type = dtypes.get(name, "string")
        # Names and types come from the Spark catalog and our own schema
        # inference, never from user input.
        spark.sql(f"ALTER TABLE {target} ADD COLUMN `{name}` {col_type}")  # NOSONAR
        changes.append(
            SchemaChange(change_type="add_column", column_name=name, new_type=col_type)
        )
    return changes


# ---------------------------------------------------------------------------
# merge + tombstone
# ---------------------------------------------------------------------------

def _merge(spark: Any, target: str, on_clause: str, columns: List[str]) -> None:
    set_clause = ", ".join(f"t.`{c}` = s.`{c}`" for c in columns)
    insert_cols = ", ".join(f"`{c}`" for c in columns)
    insert_vals = ", ".join(f"s.`{c}`" for c in columns)
    _sql_with_retry(
        spark,
        f"""
        MERGE INTO {target} t
        USING {_INCOMING_VIEW} s
        ON {on_clause}
        WHEN MATCHED THEN UPDATE SET {set_clause}
        WHEN NOT MATCHED THEN INSERT ({insert_cols}) VALUES ({insert_vals})
        """,
    )


def _tombstone_missing(
    spark: Any,
    target: str,
    on_clause: str,
    object_name: str,
    guard_pct: float,
) -> int:
    """Mark live rows the API no longer returns as deleted.

    Guarded because a partial API response looks identical to a genuine bulk
    deletion: if the snapshot is missing more than `guard_pct` of the live
    rows we refuse to write and fail the entity instead.
    """
    live = _scalar(spark, f"SELECT COUNT(*) AS c FROM {target} WHERE NOT _jetstream_deleted")
    if live == 0:
        return 0

    missing = _scalar(
        spark,
        f"""
        SELECT COUNT(*) AS c FROM {target} t
        WHERE NOT t._jetstream_deleted
          AND NOT EXISTS (SELECT 1 FROM {_INCOMING_VIEW} s WHERE {on_clause})
        """,
    )
    if missing == 0:
        return 0

    pct = missing / live * 100
    if pct > guard_pct:
        raise RuntimeError(
            f"{object_name}: snapshot is missing {missing} of {live} live rows "
            f"({pct:.1f}%), above the {guard_pct:.1f}% tombstone guard. "
            f"This usually means a partial API response, not a real deletion. "
            f"Investigate, or raise tombstone_guard_pct if the drop is expected."
        )

    _sql_with_retry(
        spark,
        f"""
        UPDATE {target} t
        SET _jetstream_deleted = TRUE,
            _jetstream_synced  = current_timestamp()
        WHERE NOT t._jetstream_deleted
          AND NOT EXISTS (SELECT 1 FROM {_INCOMING_VIEW} s WHERE {on_clause})
        """,
    )
    return missing


# ---------------------------------------------------------------------------
# helpers
# ---------------------------------------------------------------------------

def _scalar(spark: Any, sql: str) -> int:
    return int(spark.sql(sql).collect()[0]["c"])


def _sql_with_retry(spark: Any, sql: str) -> Any:
    """Run a statement, retrying Delta's transient conflict exceptions.

    Those are a family of classes (Concurrent*, MetadataChanged*) matched by
    name, since importing them directly would tie this module to a runtime
    that isn't present locally.
    """
    backoff = _MERGE_BACKOFF_SECONDS
    for attempt in range(1, _MERGE_MAX_ATTEMPTS + 1):
        try:
            return spark.sql(sql)
        except Exception as exc:  # NOSONAR
            name = type(exc).__name__
            if attempt >= _MERGE_MAX_ATTEMPTS or not (
                "Concurrent" in name or "MetadataChanged" in name
            ):
                raise
            log.warning("delta conflict on attempt %d/%d, retrying in %.1fs: %s",
                        attempt, _MERGE_MAX_ATTEMPTS, backoff, exc)
            time.sleep(backoff)
            backoff *= _MERGE_BACKOFF_MULTIPLIER
