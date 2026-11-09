"""Iceberg v3 table writer with batch deduplication, schema evolution, thread locking, infinite retry, and raw history append mode."""
from __future__ import annotations

import logging
import random
import time
from threading import Lock
from typing import Any, Dict, List, Optional, Sequence

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import ArrayType, MapType, StructType
from src import state as state_mod

log = logging.getLogger(__name__)

# Process-level thread lock to serialize concurrent table writes across worker threads
_writer_lock = Lock()


def _exec_write_with_retry(spark: SparkSession, sql_stmt: str) -> None:
    """Executes an INSERT/MERGE SQL statement with thread-locking and unbounded retry until success for transient concurrency conflicts."""
    attempt = 1
    with _writer_lock:
        while True:
            try:
                spark.sql(sql_stmt)
                return
            except Exception as e:
                err_msg = str(e)
                is_concurrent = any(
                    k in err_msg
                    for k in (
                        "ConcurrentAppendException",
                        "DELTA_CONCURRENT_APPEND",
                        "ConcurrentTransactionException",
                        "DeltaConcurrentModificationException",
                        "ROW_LEVEL_CHANGES",
                        "WITHOUT_HINT",
                        "Lock contention",
                        "RuntimeIOException",
                        "403",
                        "Forbidden",
                    )
                )
                if is_concurrent:
                    sleep_sec = min(30.0, (1.5 ** attempt) + random.uniform(0.5, 2.0))
                    log.warning(
                        "[DELTA CONCURRENCY RETRY] Commit conflict detected on target table (Attempt %d). Retrying in %.2fs...",
                        attempt,
                        sleep_sec,
                    )
                    time.sleep(sleep_sec)
                    attempt += 1
                    continue
                # Fail immediately if the error is non-transient
                raise e


def table_exists(spark: SparkSession, fqn: str) -> bool:
    clean_fqn = fqn.replace("`", "")
    return spark.catalog.tableExists(clean_fqn)


def _is_complex_type(data_type: Any) -> bool:
    return isinstance(data_type, (StructType, ArrayType, MapType))


def _reconcile_and_evolve(spark: SparkSession, df: DataFrame, target_table: str, meta: Any = None, instance_id: str = "") -> DataFrame:
    clean_table = target_table.replace("`", "")
    if not spark.catalog.tableExists(clean_table):
        return df

    target_schema = spark.table(clean_table).schema
    target_fields = {f.name: f.dataType for f in target_schema.fields}
    reconciled_df = df
    incoming_schema = {f.name: f.dataType for f in reconciled_df.schema.fields}

    for target_col, target_type in target_fields.items():
        # Escape column name with backticks to prevent PySpark dot-path parsing
        target_col_ref = F.col(f"`{target_col}`")

        if target_col in reconciled_df.columns:
            source_type = incoming_schema[target_col]
            if source_type != target_type:
                if _is_complex_type(target_type) or _is_complex_type(source_type):
                    try:
                        reconciled_df = reconciled_df.withColumn(
                            target_col,
                            F.from_json(F.to_json(target_col_ref), target_type)
                        )
                    except Exception as complex_err:
                        log.warning("Complex type reconciliation fallback for column '%s': %s", target_col, complex_err)
                        reconciled_df = reconciled_df.withColumn(target_col, target_col_ref.cast(target_type))
                else:
                    reconciled_df = reconciled_df.withColumn(target_col, target_col_ref.cast(target_type))
        else:
            reconciled_df = reconciled_df.withColumn(target_col, F.lit(None).cast(target_type))

    parts = clean_table.split(".")
    target_fqn = f"`{parts[0]}`.`{parts[1]}`.`{parts[2]}`" if len(parts) == 3 else f"`{clean_table}`"

    for field in reconciled_df.schema.fields:
        if field.name not in target_fields:
            try:
                state_mod._spark_sql(spark, f"ALTER TABLE {target_fqn} ADD COLUMN `{field.name}` {field.dataType.simpleString()}")
                if meta and instance_id and len(parts) == 3:
                    tbl_name = parts[-1]
                    tbl_id = state_mod.upsert_table_registry(
                        spark, meta, connector_instance_id=instance_id,
                        destination_catalog=parts[0], destination_schema=parts[1],
                        destination_table=tbl_name, source_object=tbl_name,
                        primary_key_columns=["id"]
                    )
                    state_mod.record_schema_change(
                        spark, meta, table_id=tbl_id, connector_instance_id=instance_id,
                        change_type="add_column", column_name=field.name, new_type=field.dataType.simpleString()
                    )
            except Exception as err:
                log.warning("Schema evolution warning: %s", err)

    target_cols = [f.name for f in spark.table(clean_table).schema.fields]
    extra_cols = [c for c in reconciled_df.columns if c not in target_cols]

    select_exprs = [F.col(f"`{c}`") for c in (target_cols + extra_cols)]
    return reconciled_df.select(*select_exprs)


def append_table(spark: SparkSession, df: DataFrame, fqn: str, primary_keys: List[str], meta: Any = None, instance_id: str = "") -> dict:
    clean_fqn = fqn.replace("`", "")
    parts = clean_fqn.split(".")
    if len(parts) == 3:
        state_mod._spark_sql(spark, f"CREATE SCHEMA IF NOT EXISTS `{parts[0]}`.`{parts[1]}`")

    # ENFORCE IN-BATCH DEDUPLICATION (prevents intra-batch duplication while preserving cross-batch history)
    pk_cols = [pk for pk in primary_keys if pk in df.columns]
    if pk_cols:
        df = df.dropDuplicates(pk_cols)

    incoming = df.count()
    if incoming == 0:
        return {"incoming_rows": 0, "table_rows_after": 0, "write_mode": "append", "table": clean_fqn}

    reconciled_df = _reconcile_and_evolve(spark, df, clean_fqn, meta, instance_id)
    target_fqn = f"`{parts[0]}`.`{parts[1]}`.`{parts[2]}`" if len(parts) == 3 else f"`{clean_fqn}`"

    # Create target table on initial load if missing
    if not spark.catalog.tableExists(clean_fqn):
        with _writer_lock:
            reconciled_df.writeTo(target_fqn).using("iceberg").create()
        if meta and instance_id and len(parts) == 3:
            tbl_id = state_mod.upsert_table_registry(
                spark, meta, connector_instance_id=instance_id,
                destination_catalog=parts[0], destination_schema=parts[1],
                destination_table=parts[2], source_object=parts[2],
                primary_key_columns=primary_keys, row_count_estimate=incoming
            )
            state_mod.record_schema_change(
                spark, meta, table_id=tbl_id, connector_instance_id=instance_id,
                change_type="create_table", column_name=None, new_type="iceberg_v3"
            )
        return {"incoming_rows": incoming, "table_rows_after": incoming, "write_mode": "append", "table": clean_fqn}

    # Append-only raw load. Each poll adds the rows returned by the API.
    view_name = f"stg_write_{parts[-1]}_{int(time.time() * 1000)}"
    reconciled_df.createOrReplaceTempView(view_name)
    
    cols_str = ", ".join([f"`{col}`" for col in reconciled_df.columns])
    
    append_sql = f"""
        INSERT INTO {target_fqn} ({cols_str})
        SELECT {cols_str} FROM {view_name}
    """
    
    try:
        _exec_write_with_retry(spark, append_sql)
    finally:
        spark.catalog.dropTempView(view_name)

    total_after = spark.table(clean_fqn).count() if spark.catalog.tableExists(clean_fqn) else incoming
    return {"incoming_rows": incoming, "table_rows_after": total_after, "write_mode": "append", "table": clean_fqn}