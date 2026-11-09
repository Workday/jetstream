"""Iceberg writer. Schema evolution is ADD COLUMN only.

Creates / appends Unity Catalog Iceberg tables. New columns are applied via
``ALTER TABLE … ADD COLUMN`` and returned as ``SchemaChange`` records for
``connector_schema_change_event``. Existing column types are never altered.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass, field
from typing import Any, List, Optional, Sequence

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from src.config import ObjectRunConfig

log = logging.getLogger(__name__)


@dataclass
class SchemaChange:
    change_type: str  # create_table | add_column
    column_name: Optional[str] = None
    new_type: Optional[str] = None


@dataclass
class WriteResult:
    incoming_rows: int = 0
    table_rows_after: int = 0
    write_mode: str = "append"
    table: str = ""
    schema_changes: List[SchemaChange] = field(default_factory=list)

    def as_legacy_dict(self) -> dict:
        return {
            "incoming_rows": self.incoming_rows,
            "table_rows_after": self.table_rows_after,
            "write_mode": self.write_mode,
            "table": self.table,
            "schema_changes": self.schema_changes,
        }


def ensure_schema(spark: SparkSession, catalog: str, schema: str) -> None:
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS `{catalog}`.`{schema}`")


def table_exists(spark: SparkSession, fqn: str) -> bool:
    return spark.catalog.tableExists(fqn)


def get_max_column(spark: SparkSession, fqn: str, column: str) -> Optional[str]:
    if not table_exists(spark, fqn):
        return None
    row = spark.table(fqn).agg(F.max(F.col(column)).alias("m")).collect()
    if not row or row[0]["m"] is None:
        return None
    return str(row[0]["m"])


def _qualified(fqn: str) -> str:
    parts = [p.replace("`", "") for p in fqn.split(".") if p.strip()]
    if not parts:
        raise ValueError("empty table FQN")
    return ".".join(f"`{p}`" for p in parts)


def _leading_columns(df: DataFrame, preferred: Sequence[str]) -> DataFrame:
    lead = [c for c in preferred if c in df.columns]
    rest = [c for c in df.columns if c not in lead]
    if not lead:
        return df
    return df.select(*lead, *rest)


def _coerce_to_existing_types(spark: SparkSession, df: DataFrame, fqn: str) -> DataFrame:
    """Cast incoming columns to existing Iceberg types; do not widen types."""
    if not table_exists(spark, fqn):
        return df
    target_types = {f.name: f.dataType for f in spark.table(fqn).schema.fields}
    out = df
    for col_name in df.columns:
        if col_name not in target_types:
            continue
        src = out.schema[col_name].dataType
        tgt = target_types[col_name]
        if src != tgt:
            log.warning(
                "type conflict on %s.%s: incoming=%s existing=%s — casting to existing",
                fqn, col_name, src.simpleString(), tgt.simpleString(),
            )
            out = out.withColumn(col_name, F.col(col_name).cast(tgt))
    return out


def estimate_table_rows(spark: SparkSession, fqn: str) -> Optional[int]:
    """Metadata-only row estimate. Never runs ``SELECT COUNT(*)`` / ``df.count()``.

    Tries ``DESCRIBE DETAIL`` (``numRows``) then Iceberg snapshot
    ``summary['total-records']``. Returns None if neither is available so
    callers can fall back to ``prior_estimate + incoming``.
    """
    if not table_exists(spark, fqn):
        return None
    q = _qualified(fqn)
    try:
        detail = spark.sql(f"DESCRIBE DETAIL {q}").collect()
        if detail:
            d = detail[0].asDict()
            for key in ("numRows", "num_rows"):
                val = d.get(key)
                if val is not None:
                    return int(val)
    except Exception as exc:  # noqa: BLE001
        log.debug("DESCRIBE DETAIL estimate failed for %s: %s", fqn, exc)

    try:
        snap = spark.sql(
            f"SELECT summary['total-records'] AS n FROM {q}.snapshots "
            "ORDER BY committed_at DESC LIMIT 1"
        ).collect()
        if snap and snap[0]["n"] is not None:
            return int(snap[0]["n"])
    except Exception as exc:  # noqa: BLE001
        log.debug("Iceberg snapshot estimate failed for %s: %s", fqn, exc)
    return None


def append_table(
    spark: SparkSession,
    df: DataFrame,
    fqn: str,
    *,
    table_format: str = "iceberg",
    cluster_cols: Optional[List[str]] = None,
    partition_cols: Optional[List[str]] = None,
    incoming_rows: Optional[int] = None,
) -> dict:
    """Append-only Iceberg write with ADD COLUMN schema evolution.

    Pass ``incoming_rows`` (e.g. ServiceNow page length) to skip ``df.count()``.
    ``table_rows_after`` is set only on create (equals incoming). After append
    it is left ``None`` — callers use ``estimate_table_rows`` once per object,
    never a full Iceberg scan.
    """
    _ = cluster_cols, partition_cols, table_format
    parts = fqn.split(".")
    if len(parts) == 3:
        ensure_schema(spark, parts[0], parts[1])

    preferred = [
        "sys_id",
        "sys_updated_on",
        "_jetstream_synced",
        "_jetstream_deleted",
        "_jetstream_source_object",
    ]
    df = _leading_columns(df, preferred)
    incoming = int(incoming_rows) if incoming_rows is not None else df.count()
    target = _qualified(fqn)
    result = WriteResult(incoming_rows=incoming, write_mode="append", table=fqn)

    if not table_exists(spark, fqn):
        df.writeTo(target).using("iceberg").create()
        result.table_rows_after = incoming
        result.schema_changes.append(SchemaChange(change_type="create_table"))
        return result.as_legacy_dict()

    # Evolve target: ADD COLUMN for new names only (Zoom parity).
    target_cols = {c.name for c in spark.catalog.listColumns(fqn)}
    dtypes = dict(df.dtypes)
    for col_name in df.columns:
        if col_name in target_cols:
            continue
        col_type = dtypes.get(col_name, "string")
        spark.sql(f"ALTER TABLE {target} ADD COLUMN `{col_name}` {col_type}")  # NOSONAR
        result.schema_changes.append(
            SchemaChange(change_type="add_column", column_name=col_name, new_type=col_type)
        )
        target_cols.add(col_name)

    df = _coerce_to_existing_types(spark, df, fqn)
    # Align to target column set (missing → null via select of overlapping + new)
    target_order = [c.name for c in spark.table(fqn).schema.fields]
    for c in target_order:
        if c not in df.columns:
            df = df.withColumn(c, F.lit(None))
    extra = [c for c in df.columns if c not in target_order]
    df = df.select(*(target_order + extra))

    df.writeTo(target).using("iceberg").append()
    result.table_rows_after = 0  # unknown without a scan; caller estimates once
    return result.as_legacy_dict()


def scd1_merge(
    spark: SparkSession,
    df: DataFrame,
    fqn: str,
    primary_key: str,
    *,
    table_format: str = "iceberg",
    incoming_rows: Optional[int] = None,
) -> dict:
    """SCD1 merge helper. The sync path appends raw tables and does not call this."""
    _ = table_format
    parts = fqn.split(".")
    if len(parts) == 3:
        ensure_schema(spark, parts[0], parts[1])

    incoming = incoming_rows if incoming_rows is not None else df.count()
    target = _qualified(fqn)
    if not table_exists(spark, fqn):
        df.writeTo(target).using("iceberg").create()
        return {"incoming_rows": incoming, "table_rows_after": incoming, "write_mode": "scd1_create", "table": fqn}

    view = f"_scd1_src_{primary_key}"
    df.createOrReplaceTempView(view)
    spark.sql(f"""
        MERGE INTO {target} AS t
        USING {view} AS s
        ON t.`{primary_key}` = s.`{primary_key}`
        WHEN MATCHED THEN UPDATE SET *
        WHEN NOT MATCHED THEN INSERT *
    """)
    return {
        "incoming_rows": incoming,
        "table_rows_after": estimate_table_rows(spark, fqn) or incoming,
        "write_mode": "scd1_merge",
        "table": fqn,
    }


def ensure_namespace(spark: SparkSession, cfg: ObjectRunConfig) -> None:
    ensure_schema(spark, cfg.destination_catalog, cfg.destination_schema)


def table_exists_cfg(spark: SparkSession, cfg: ObjectRunConfig) -> bool:
    return table_exists(spark, cfg.target_table)


def write(spark: SparkSession, df: DataFrame, cfg: ObjectRunConfig) -> dict:
    mode = cfg.write_mode.lower()
    if mode == "append":
        return append_table(spark, df, cfg.target_table, table_format=cfg.table_format)
    if mode in ("merge", "scd1"):
        return scd1_merge(spark, df, cfg.target_table, cfg.primary_key, table_format=cfg.table_format)
    if mode == "overwrite":
        ensure_namespace(spark, cfg)
        target = _qualified(cfg.target_table)
        df.writeTo(target).using("iceberg").createOrReplace()
        return {"incoming_rows": df.count(), "write_mode": "overwrite", "table": cfg.target_table}
    raise ValueError(f"Unsupported write_mode '{cfg.write_mode}'")
