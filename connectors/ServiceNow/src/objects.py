"""ServiceNow object resolution (dynamic from Job widgets)."""

from __future__ import annotations

from dataclasses import dataclass, field, replace
from typing import Dict, List, Sequence


@dataclass(frozen=True)
class ObjectCfg:
    name: str
    sn_table: str
    primary_key: str = "sys_id"
    primary_key_columns: Sequence[str] = ("sys_id",)
    watermark_columns: Sequence[str] = ("sys_updated_on",)
    lookback_minutes: int = 15
    write_mode: str = "append"
    table_format: str = "iceberg"
    attachment_meta_table: str = ""
    journals_meta_table: str = ""
    optional: bool = False
    default_fields: Sequence[str] = field(default_factory=lambda: ("*",))


# Rare per-table overrides. Empty by default — prefer Job widgets.
OVERRIDES: Dict[str, ObjectCfg] = {}


def parse_primary_key_csv(primary_key_csv: str = "") -> List[str]:
    parts = [x.strip() for x in (primary_key_csv or "").split(",") if x.strip()]
    return parts or ["sys_id"]


def parse_watermark_column_csv(watermark_column_csv: str = "") -> List[str]:
    parts = [x.strip() for x in (watermark_column_csv or "").split(",") if x.strip()]
    return parts or ["sys_updated_on"]


def resolve_object(
    name: str,
    *,
    attachment_meta_table: str = "",
    journals_meta_table: str = "",
    primary_key: str = "",
    watermark_column: str = "",
) -> ObjectCfg:
    clean = (name or "").strip()
    if not clean:
        raise ValueError("object name must be non-empty")

    base = OVERRIDES.get(clean)
    if base is None:
        base = ObjectCfg(name=clean, sn_table=clean)

    meta = (attachment_meta_table or "").strip() or f"{clean}_attachments"
    j_meta = (journals_meta_table or "").strip() or f"{clean}_journals"

    pk_csv = (primary_key or "").strip()
    if pk_csv:
        pk_cols = parse_primary_key_csv(pk_csv)
    elif base.primary_key_columns:
        pk_cols = list(base.primary_key_columns)
    else:
        pk_cols = parse_primary_key_csv(base.primary_key)

    wm_csv = (watermark_column or "").strip()
    if wm_csv:
        wm_cols = parse_watermark_column_csv(wm_csv)
    elif base.watermark_columns:
        wm_cols = list(base.watermark_columns)
    else:
        wm_cols = parse_watermark_column_csv("")

    return replace(
        base,
        name=clean,
        sn_table=base.sn_table or clean,
        primary_key=pk_cols[0],
        primary_key_columns=tuple(pk_cols),
        watermark_columns=tuple(wm_cols),
        attachment_meta_table=meta,
        journals_meta_table=j_meta,
    )


def get_object(name: str) -> ObjectCfg:
    return resolve_object(name)


def list_object_names() -> List[str]:
    return sorted(OVERRIDES.keys())


def select_objects(
    enabled_csv: str,
    optional_enabled_csv: str = "",
    *,
    attachment_meta_table: str = "",
    journals_meta_table: str = "",
    primary_key: str = "",
    watermark_column: str = "",
) -> List[ObjectCfg]:
    enabled = [x.strip() for x in (enabled_csv or "").split(",") if x.strip()]
    if not enabled:
        enabled = ["incident"]
    _ = optional_enabled_csv
    return [
        resolve_object(
            name,
            attachment_meta_table=attachment_meta_table,
            journals_meta_table=journals_meta_table,
            primary_key=primary_key,
            watermark_column=watermark_column,
        )
        for name in enabled
    ]
