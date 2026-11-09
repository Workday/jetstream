"""Declarative object configuration reader for Jetstream API catalog."""
from __future__ import annotations

import os
from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional, Sequence
import yaml

@dataclass
class ObjectCfg:
    name: str
    endpoint: str
    api_type: str = "INCR"
    dependency_object: Optional[str] = None
    parent_param_key: Optional[str] = None
    auth_pattern: str = "pattern_1"
    primary_key: str = "id"
    primary_key_columns: Sequence[str] = ("id",)
    watermark_columns: Sequence[str] = ("updated_at",)
    lookback_minutes: int = 30
    parent: Optional[str] = None
    has_attachments: bool = False
    attachment_enabled: bool = False
    attachment_meta_table: str = "attachments"
    page_size: int = 100
    retry: Dict[str, Any] = field(default_factory=dict)
    destination_catalog: str = ""
    destination_schema: str = ""
    raw_json_volume_base: str = ""
    attachment_volume_path: str = ""
    download_attachments: bool = True
    use_cursors: bool = True
    from_datetime: Optional[str] = None
    to_datetime: Optional[str] = None

    @property
    def object_name(self) -> str:
        return self.name

    @property
    def source_object(self) -> str:
        return self.name

    @property
    def target_table(self) -> str:
        if self.destination_catalog and self.destination_schema:
            return f"`{self.destination_catalog}`.`{self.destination_schema}`.`{self.name}`"
        return f"`{self.name}`"

    @property
    def load_strategy(self) -> str:
        if self.api_type == "FULL":
            return "full_refresh"
        if self.api_type == "INCR_GET_ID":
            return "dependent_child"
        return "incremental_cursor"


def load_api_catalog() -> Dict[str, Dict[str, Any]]:
    """Loads API catalog mapping dynamically across root or connector relative paths."""
    here = os.path.dirname(os.path.abspath(__file__))
    repo_root = os.path.dirname(here)
    
    possible_paths = [
        os.path.join(repo_root, "connectors", "wss", "api_catalog.yaml"),
        os.path.join(here, "..", "connectors", "wss", "api_catalog.yaml"),
        os.path.join(repo_root, "api_catalog.yaml"),
    ]
    
    catalog_path = None
    for p in possible_paths:
        if os.path.exists(p):
            catalog_path = p
            break

    if not catalog_path:
        raise FileNotFoundError(f"Catalog specification missing in expected paths: {possible_paths}")

    with open(catalog_path, "r", encoding="utf-8") as f:
        data = yaml.safe_load(f) or {}

    if "objects" in data and isinstance(data["objects"], dict):
        return data["objects"]
    elif "apis" in data and isinstance(data["apis"], dict):
        return data["apis"]
    
    return data if isinstance(data, dict) else {}


def resolve_object(
    name: str,
    *,
    attachment_enabled: bool = False,
    attachment_meta_table: str = "",
    primary_key: str = "",
    watermark_column: str = "",
) -> ObjectCfg:
    clean_name = (name or "").strip()
    catalog = load_api_catalog()
    entry = catalog.get(clean_name)

    if not entry:
        available_keys = list(catalog.keys())
        raise ValueError(
            f"Object '{clean_name}' is not defined in api_catalog.yaml. "
            f"Available objects in catalog: {available_keys}"
        )

    api_type = str(entry.get("api_type", "INCR")).upper()
    dep_obj = entry.get("dependency_object")
    parent_param = entry.get("parent_param_key")
    auth_pat = entry.get("auth_pattern", "pattern_1")
    endpoint = entry.get("endpoint", "")
    pk = primary_key if primary_key else entry.get("primary_key", "id")
    has_att = bool(entry.get("has_attachments", False))

    wm_cols = (
        ()
        if api_type in ("FULL", "INCR_GET_ID")
        or watermark_column.upper() in ("NONE", "FALSE", "")
        else ("updated_at",)
    )

    return ObjectCfg(
        name=clean_name,
        endpoint=endpoint,
        api_type=api_type,
        dependency_object=dep_obj,
        parent_param_key=parent_param,
        auth_pattern=auth_pat,
        primary_key=pk,
        primary_key_columns=(pk,),
        watermark_columns=wm_cols,
        parent=dep_obj,
        has_attachments=has_att or attachment_enabled,
        attachment_enabled=bool(attachment_enabled) and has_att,
        attachment_meta_table=attachment_meta_table or "attachments",
    )


def select_objects(
    enabled_csv: str,
    *,
    attachment_enabled: bool = False,
    attachment_meta_table: str = "",
    primary_key: str = "",
    watermark_column: str = "",
) -> List[ObjectCfg]:
    enabled = [x.strip() for x in (enabled_csv or "").split(",") if x.strip()]
    if not enabled:
        return []
    return [
        resolve_object(
            name,
            attachment_enabled=attachment_enabled,
            attachment_meta_table=attachment_meta_table,
            primary_key=primary_key,
            watermark_column=watermark_column,
        )
        for name in enabled
    ]