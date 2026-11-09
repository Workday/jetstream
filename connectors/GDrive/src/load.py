"""Stage 2: Parse structured files from Volume and load into Iceberg tables.

After Stage 1 (extract.py) lands raw files from Google Drive into a UC Volume
preserving folder hierarchy, this module scans the Volume subfolders, filters
for structured file formats (.csv, .xlsx, Google Sheets exported as .xlsx),
and writes the parsed tabular data into the target Iceberg tables.

Public API (called from src/pipeline.py):
    load_structured_tables — process all table_mappings for the current run
    load_single_mapping    — process one TableMapping entry

Design:
    - Stateless: receives a SparkSession + RunConfig, returns LoadResult metrics.
    - Format detection is extension-based (Stage 1 guarantees exported Sheets
      are .xlsx, binary files keep their original extension).
    - Non-structured files (.docx, .pdf, .pptx, etc.) are silently skipped.
    - Each mapping writes to: {destination_catalog}.{destination_schema}.{target_table}
"""
from __future__ import annotations

import logging
import os
from dataclasses import dataclass, field
from typing import List, Optional, Sequence

from pyspark.sql import DataFrame, SparkSession

from src.config import (
    STRUCTURED_FORMATS,
    FileDestination,
    RunConfig,
    TableMapping,
)

log = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

# File extensions that map to structured data readers.
_EXTENSION_TO_FORMAT = {
    ".csv": "csv",
    ".xlsx": "excel",
    ".xls": "excel",
    ".tsv": "csv",  # TSV treated as CSV with tab delimiter
}

# Extensions to skip (non-structured files that may be present in the Volume).
_SKIP_EXTENSIONS = frozenset({
    ".docx", ".doc", ".pdf", ".pptx", ".ppt",
    ".png", ".jpg", ".jpeg", ".gif", ".svg",
    ".zip", ".json", ".txt", ".mp4", ".mp3",
})


# ---------------------------------------------------------------------------
# Result dataclass
# ---------------------------------------------------------------------------

@dataclass
class LoadResult:
    """Outcome of loading one TableMapping into Iceberg."""
    target_table: str
    success: bool
    files_processed: int = 0
    files_skipped: int = 0
    rows_written: int = 0
    error_message: Optional[str] = None
    processed_files: List[str] = field(default_factory=list)
    skipped_files: List[str] = field(default_factory=list)
    schema_changes: List[dict] = field(default_factory=list)


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------

def load_structured_tables(
    spark: SparkSession,
    config: RunConfig,
    run_date: str,
) -> List[LoadResult]:
    """Process all table_mappings for the current run.

    Args:
        spark: Active SparkSession.
        config: Resolved RunConfig with table_mappings and file_destination.
        run_date: ISO date string (YYYY-MM-DD) for the current run partition.

    Returns:
        List of LoadResult, one per TableMapping entry.
    """
    if not config.table_mappings:
        log.info("No table_mappings configured; skipping Stage 2.")
        return []

    results: List[LoadResult] = []
    for mapping in config.table_mappings:
        result = load_single_mapping(
            spark=spark,
            mapping=mapping,
            file_destination=config.file_destination,
            destination_catalog=config.destination_catalog,
            destination_schema=config.destination_schema,
            run_date=run_date,
        )
        results.append(result)
        if result.success:
            log.info(
                "Loaded %d rows into %s.%s.%s from %d files (%d skipped)",
                result.rows_written,
                config.destination_catalog,
                config.destination_schema,
                mapping.target_table,
                result.files_processed,
                result.files_skipped,
            )
        else:
            log.error(
                "Failed to load %s: %s",
                mapping.target_table,
                result.error_message,
            )

    return results


def load_single_mapping(
    spark: SparkSession,
    mapping: TableMapping,
    file_destination: FileDestination,
    destination_catalog: str,
    destination_schema: str,
    run_date: str,
) -> LoadResult:
    """Parse structured files for one TableMapping and write to Iceberg.

    Args:
        spark: Active SparkSession.
        mapping: The TableMapping entry to process.
        file_destination: FileDestination config (volume_path, hierarchy settings).
        destination_catalog: Target catalog (e.g. raw_dev).
        destination_schema: Target schema (e.g. gdrive).
        run_date: ISO date string (YYYY-MM-DD) for date-partitioned volume paths.

    Returns:
        LoadResult with metrics.
    """
    target_table = mapping.target_table
    full_table_name = f"{destination_catalog}.{destination_schema}.{target_table}"

    try:
        # 1. Resolve the Volume path to scan.
        scan_path = _resolve_scan_path(
            file_destination=file_destination,
            mapping=mapping,
            run_date=run_date,
        )

        # For source_type="file", scan_path points directly to a file.
        # For source_type="folder", scan_path is a directory to scan.
        if mapping.source_type == "file":
            if os.path.isfile(scan_path):
                structured_files = [scan_path]
                skipped_files: List[str] = []
            else:
                # File doesn't exist — not an error in incremental mode
                # (no new version downloaded this run).
                log.info(
                    "Source file not found for '%s': %s (skipping)",
                    target_table, scan_path,
                )
                return LoadResult(
                    target_table=target_table,
                    success=True,
                    error_message=None,
                )
        else:
            if not os.path.isdir(scan_path):
                # Volume path doesn't exist — not an error in incremental
                # mode (no files downloaded for this subfolder this run).
                log.info(
                    "Volume path not found for '%s': %s (skipping)",
                    target_table, scan_path,
                )
                return LoadResult(
                    target_table=target_table,
                    success=True,
                    error_message=None,
                )

            # 2. Discover structured files.
            structured_files, skipped_files = _discover_files(
                scan_path=scan_path,
                supported_formats=mapping.supported_formats,
                recursive=mapping.recursive,
            )

        if not structured_files:
            log.warning(
                "No structured files found in %s for mapping '%s'",
                scan_path, target_table,
            )
            return LoadResult(
                target_table=target_table,
                success=True,
                files_skipped=len(skipped_files),
                skipped_files=skipped_files,
            )

        # 3. Read all structured files into a unified DataFrame.
        df = _read_structured_files(spark, structured_files)
        if df is None:
            return LoadResult(
                target_table=target_table,
                success=False,
                error_message="All files failed to parse.",
                files_skipped=len(skipped_files),
                skipped_files=skipped_files,
            )

        # 3a. Enrich with Jetstream metadata columns.
        from pyspark.sql import functions as F

        df = (
            df.withColumn("_jetstream_synced", F.current_timestamp())
              .withColumn("_jetstream_deleted", F.lit(False).cast("boolean"))
              .withColumn("_jetstream_source_object", F.lit("gdrive"))
        )

        # 3b. Detect schema changes (before write overwrites the table).
        schema_changes = _detect_schema_changes(spark, df, full_table_name)

        # 4. Write to Iceberg table.
        row_count = _write_to_iceberg(
            spark, df, full_table_name,
            write_mode=getattr(mapping, "write_mode", "overwrite"),
        )

        return LoadResult(
            target_table=target_table,
            success=True,
            files_processed=len(structured_files),
            files_skipped=len(skipped_files),
            rows_written=row_count,
            processed_files=[os.path.basename(f) for f in structured_files],
            skipped_files=skipped_files,
            schema_changes=schema_changes,
        )

    except Exception as exc:
        log.exception("Unexpected error loading mapping '%s'", target_table)
        return LoadResult(
            target_table=target_table,
            success=False,
            error_message=f"{type(exc).__name__}: {exc}",
        )


# ---------------------------------------------------------------------------
# Path resolution
# ---------------------------------------------------------------------------

def _resolve_scan_path(
    file_destination: FileDestination,
    mapping: TableMapping,
    run_date: str,
) -> str:
    """Build the full Volume path to scan for a given mapping.

    Layout: {volume_path}/{run_date}/{source_subfolder}
      - run_date is inserted only if file_destination.date_partitioned is True.
      - source_subfolder comes from the mapping; if empty, uses the volume root.
    """
    base = file_destination.volume_path.rstrip("/")

    if file_destination.date_partitioned:
        base = os.path.join(base, run_date)

    if mapping.source_subfolder:
        return os.path.join(base, mapping.source_subfolder)

    return base


# ---------------------------------------------------------------------------
# File discovery
# ---------------------------------------------------------------------------

def _discover_files(
    scan_path: str,
    supported_formats: Sequence[str],
    recursive: bool,
) -> tuple:
    """Walk the scan_path and classify files as structured or skipped.

    Returns:
        (structured_file_paths, skipped_file_names)
    """
    structured: List[str] = []
    skipped: List[str] = []

    # Normalize supported_formats to a set of extensions.
    accepted_extensions = _formats_to_extensions(supported_formats)

    if recursive:
        for dirpath, _, filenames in os.walk(scan_path):
            for fname in filenames:
                full_path = os.path.join(dirpath, fname)
                ext = os.path.splitext(fname)[1].lower()
                if ext in accepted_extensions:
                    structured.append(full_path)
                else:
                    skipped.append(fname)
    else:
        for fname in os.listdir(scan_path):
            full_path = os.path.join(scan_path, fname)
            if not os.path.isfile(full_path):
                continue
            ext = os.path.splitext(fname)[1].lower()
            if ext in accepted_extensions:
                structured.append(full_path)
            else:
                skipped.append(fname)

    return structured, skipped


def _formats_to_extensions(supported_formats: Sequence[str]) -> frozenset:
    """Map format names (csv, xlsx, google_sheets) to file extensions."""
    ext_set = set()
    for fmt in supported_formats:
        fmt_lower = fmt.lower().strip()
        if fmt_lower == "csv":
            ext_set.update({".csv", ".tsv"})
        elif fmt_lower in ("xlsx", "xls"):
            ext_set.add(f".{fmt_lower}")
        elif fmt_lower == "google_sheets":
            # Google Sheets are exported as .xlsx in Stage 1
            ext_set.add(".xlsx")
        else:
            # Allow explicit extension (e.g. user passes ".parquet")
            ext_set.add(f".{fmt_lower}" if not fmt_lower.startswith(".") else fmt_lower)
    return frozenset(ext_set)


# ---------------------------------------------------------------------------
# File reading
# ---------------------------------------------------------------------------

def _read_structured_files(
    spark: SparkSession,
    file_paths: List[str],
) -> Optional[DataFrame]:
    """Read a list of structured files into a single unified DataFrame.

    Groups files by format (csv vs excel), reads each group, and unions them.
    Uses schema merging to handle slight column variations across files.
    """
    csv_files: List[str] = []
    excel_files: List[str] = []

    for fpath in file_paths:
        ext = os.path.splitext(fpath)[1].lower()
        reader_format = _EXTENSION_TO_FORMAT.get(ext)
        if reader_format == "csv":
            csv_files.append(fpath)
        elif reader_format == "excel":
            excel_files.append(fpath)
        else:
            log.warning("No reader for extension '%s', skipping: %s", ext, fpath)

    frames: List[DataFrame] = []

    # Read CSVs (Spark can read multiple CSV paths natively).
    if csv_files:
        try:
            df_csv = spark.read.format("csv").options(
                header="true",
                inferSchema="false",
                multiLine="true",
                escape='"',
                mode="PERMISSIVE",
            ).load(csv_files)
            frames.append(df_csv)
        except Exception as exc:
            log.error("Failed to read CSV files: %s", exc)

    # Read Excel files individually (each may have different schemas).
    for xlsx_path in excel_files:
        try:
            df_xl = spark.read.format("excel").options(
                headerRows="1",
                inferSchema="false",
            ).load(xlsx_path)
            frames.append(df_xl)
        except Exception as exc:
            log.error("Failed to read Excel file %s: %s", xlsx_path, exc)

    if not frames:
        return None

    # Normalize column names to lowercase and cast all to string for safe union.
    from pyspark.sql import functions as F

    normalized: List[DataFrame] = []
    for df in frames:
        df_norm = df.select(
            [F.col(f"`{c}`").cast("string").alias(c.lower().strip()) for c in df.columns]
        )
        normalized.append(df_norm)

    # Union all frames with schema merging (unionByName allows mismatched cols).
    result = normalized[0]
    for df in normalized[1:]:
        result = result.unionByName(df, allowMissingColumns=True)

    return result


# ---------------------------------------------------------------------------
# Schema change detection
# ---------------------------------------------------------------------------

def _detect_schema_changes(
    spark: SparkSession,
    incoming_df: DataFrame,
    full_table_name: str,
) -> List[dict]:
    """Compare incoming DataFrame schema against the existing Iceberg table.

    Returns a list of change dicts ready for `record_schema_change`:
        [{"change_type": "create_table" | "add_column" | "drop_column" | "alter_type",
          "column_name": str | None, "old_type": str | None, "new_type": str | None}]

    If the table doesn't exist yet, returns a single ``create_table`` entry.
    If schemas are identical, returns an empty list.
    """
    changes: List[dict] = []

    try:
        table_exists = spark.catalog.tableExists(full_table_name)
    except Exception:
        table_exists = False

    if not table_exists:
        changes.append({"change_type": "create_table", "column_name": None,
                        "old_type": None, "new_type": None})
        return changes

    # Build {lowercase_col_name: data_type_string} for both sides.
    existing_fields = {
        f.name.lower(): f.dataType.simpleString()
        for f in spark.table(full_table_name).schema.fields
    }
    incoming_fields = {
        f.name.lower(): f.dataType.simpleString()
        for f in incoming_df.schema.fields
    }

    # Columns added (in incoming but not existing).
    for col in sorted(incoming_fields.keys() - existing_fields.keys()):
        changes.append({"change_type": "add_column", "column_name": col,
                        "old_type": None, "new_type": incoming_fields[col]})

    # Columns dropped (in existing but not incoming).
    for col in sorted(existing_fields.keys() - incoming_fields.keys()):
        changes.append({"change_type": "drop_column", "column_name": col,
                        "old_type": existing_fields[col], "new_type": None})

    # Type changes (present in both but different type).
    for col in sorted(existing_fields.keys() & incoming_fields.keys()):
        if existing_fields[col] != incoming_fields[col]:
            changes.append({"change_type": "alter_type", "column_name": col,
                            "old_type": existing_fields[col],
                            "new_type": incoming_fields[col]})

    return changes


# ---------------------------------------------------------------------------
# Iceberg writer
# ---------------------------------------------------------------------------

def _evolve_schema_if_needed(
    spark: SparkSession,
    df: DataFrame,
    full_table_name: str,
) -> None:
    """Add any new DataFrame columns to an existing Iceberg table.

    Iceberg's `.append()` rejects extra columns in the incoming DataFrame.
    This helper issues ALTER TABLE ADD COLUMNS for any columns present in
    the DataFrame but missing from the target table, so incremental loads
    work seamlessly when new metadata columns (e.g. _jetstream_*) are
    introduced.
    """
    import re

    _SAFE_NAME_RE = re.compile(r"^[\w.]+$")

    try:
        if not spark.catalog.tableExists(full_table_name):
            return
    except Exception:
        return

    existing_cols = {
        f.name.lower() for f in spark.table(full_table_name).schema.fields
    }
    incoming_fields = {
        f.name.lower(): f.dataType.simpleString()
        for f in df.schema.fields
    }

    new_cols = [
        (name, dtype)
        for name, dtype in incoming_fields.items()
        if name not in existing_cols
    ]

    if not new_cols:
        return

    for col_name, col_type in new_cols:
        if not _SAFE_NAME_RE.match(col_name):
            raise ValueError(f"Unsafe column name for ALTER TABLE: {col_name!r}")
        if not _SAFE_NAME_RE.match(col_type):
            raise ValueError(f"Unsafe column type for ALTER TABLE: {col_type!r}")

    col_defs = ", ".join(
        f"`{name}` {dtype}" for name, dtype in new_cols
    )
    alter_sql = f"ALTER TABLE {full_table_name} ADD COLUMNS ({col_defs})"
    log.info("Evolving schema: %s", alter_sql)
    spark.sql(alter_sql)


def _write_to_iceberg(
    spark: SparkSession,
    df: DataFrame,
    full_table_name: str,
    write_mode: str = "overwrite",
) -> int:
    """Write a DataFrame to an Iceberg table.

    Args:
        spark: Active SparkSession.
        df: DataFrame to write.
        full_table_name: Fully qualified table name (catalog.schema.table).
        write_mode: How to write data:
            - "overwrite" (default): Truncate + replace (createOrReplace).
            - "append": Append rows to existing table (create if not exists,
              append if exists).

    Returns the number of rows written.
    """
    row_count = df.count()

    if write_mode == "append":
        # Evolve schema first so new columns (e.g. _jetstream_*) don't
        # cause an Iceberg append rejection on existing tables.
        _evolve_schema_if_needed(spark, df, full_table_name)

        # Append: create table on first run, append on subsequent.
        try:
            df.writeTo(full_table_name).using("iceberg").tableProperty(
                "format-version", "3"
            ).append()
        except Exception:
            # Table doesn't exist yet — create it.
            df.writeTo(full_table_name).using("iceberg").tableProperty(
                "format-version", "3"
            ).create()
    else:
        # Overwrite: full truncate + replace.
        (
            df.writeTo(full_table_name)
            .using("iceberg")
            .tableProperty("format-version", "3")
            .createOrReplace()
        )

    log.info(
        "Wrote %d rows (mode=%s) to Iceberg table %s",
        row_count, write_mode, full_table_name,
    )
    return row_count
