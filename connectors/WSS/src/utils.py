"""Utility module for raw JSON volume backups and Databricks widget helpers."""
from __future__ import annotations

import json
import logging
import os
from typing import Any, Dict, List

log = logging.getLogger(__name__)


def get_widget_safely(dbutils: Any, name: str, default: str = "") -> str:
    """Safely fetches a Databricks widget value without throwing Py4J InputWidgetNotDefined exceptions."""
    if not dbutils:
        return default
    try:
        val = dbutils.widgets.get(name)
        return str(val).strip() if val is not None else default
    except Exception:
        return default


def save_raw_api_backup(
    records: List[Dict[str, Any]],
    target_catalog: str,
    target_schema: str,
    object_name: str,
    execution_timestamp: str,
) -> str:
    """
    Saves all extracted API records for an object into a single response.json file in UC Volume.
    Fully parameterized with zero hardcoded paths or defaults.
    """
    if not target_catalog or not target_schema:
        log.error("[JSON BACKUP ERROR] 'target_catalog' and 'target_schema' must be explicitly provided.")
        return ""

    if not object_name or not execution_timestamp:
        log.error("[JSON BACKUP ERROR] 'object_name' and 'execution_timestamp' must be explicitly provided.")
        return ""

    target_dir = (
        f"/Volumes/{target_catalog}/{target_schema}/api_responses/"
        f"{object_name}/{execution_timestamp}"
    )

    try:
        os.makedirs(target_dir, exist_ok=True)
        file_path = os.path.join(target_dir, "response.json")

        with open(file_path, "w", encoding="utf-8") as f:
            json.dump(records, f, indent=2, ensure_ascii=False)

        log.info(
            "[JSON BACKUP SUCCESS] Saved %d records for '%s' to %s",
            len(records),
            object_name,
            file_path,
        )
        return file_path

    except Exception as e:
        log.error(
            "[JSON BACKUP ERROR] Failed to save raw JSON backup for '%s': %s",
            object_name,
            str(e),
        )
        return ""