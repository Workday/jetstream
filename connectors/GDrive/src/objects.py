"""Google Drive object catalog.

The catalog lives in code (not in a per-instance YAML) because these are
source-side facts about the Google Drive API. Different `connector_instance`
rows for the same connector type can only differ in *which* of these objects
are enabled and where the file-copy object lands, not in the objects'
definitions themselves.

Google Drive is a mixed-destination connector (spec §6.10). One instance can,
in a single run, write both:
  * files_metadata — parsed file/folder metadata into an Iceberg table, and
  * files          — whole file bytes into a UC Volume or S3 prefix.

This mirrors the ServiceNow "mixed destination" walkthrough in the standards
doc: one connector_instance, two destination objects with different
table_format values.
"""
from __future__ import annotations

from src.config import ObjectCfg


# Logical object names.
FILES_METADATA = "files_metadata"
FILES = "files"


# ---------------------------------------------------------------------------
# Catalog. Order matters: metadata is listed first so its Iceberg table exists
# before (and independently of) the file-copy object in the same run.
# ---------------------------------------------------------------------------

CATALOG: list[ObjectCfg] = [
    # Structured — Drive file/folder metadata into an Iceberg table.
    # Natural key = file_id, so no surrogate _jetstream_id/_index is added
    # (spec §10). Incremental on the source `modified_time`.
    ObjectCfg(
        name=FILES_METADATA,
        kind="metadata",
        primary_key=["file_id"],
        load_strategy="incremental_cursor",
        cursor_field="modified_time",
        table_format="iceberg",
    ),
    # Unstructured — whole file bytes into a UC Volume or S3 prefix.
    # No destination table schema, no natural key, no row-level _jetstream_*
    # columns (spec §6.10, §11.5). table_format is resolved at run time from
    # the instance's file_destination (volume | s3_prefix).
    ObjectCfg(
        name=FILES,
        kind="file_copy",
        primary_key=[],
        load_strategy="incremental_cursor",
        cursor_field="modified_time",
        table_format="volume",
    ),
]


ALL_BY_NAME: dict[str, ObjectCfg] = {o.name: o for o in CATALOG}


def select_objects(enabled_objects_csv: str = "") -> list[ObjectCfg]:
    """Return the ObjectCfg list for this run, in catalog order.

    Empty `enabled_objects_csv` means all objects. Otherwise the CSV must be a
    subset of the known catalog; metadata is always kept ahead of the
    file-copy object.
    """
    wanted = {s.strip() for s in enabled_objects_csv.split(",") if s.strip()}

    unknown = wanted - set(ALL_BY_NAME)
    if unknown:
        raise ValueError(
            f"unknown Google Drive object(s) requested: {sorted(unknown)}; "
            f"known: {sorted(ALL_BY_NAME)}"
        )

    return [o for o in CATALOG if not wanted or o.name in wanted]
