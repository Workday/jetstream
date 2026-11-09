"""Databricks runtime context + metadata-catalog resolution."""
from __future__ import annotations

from dataclasses import dataclass
from typing import Any

METADATA_SCHEMA = "jetstream"
_ENV_TO_CATALOG = {
    "dev": "metadata_dev",
    "prod": "metadata_prod",
}
_VALID_METADATA_SCHEMA_PREFIX = "jetstream"

@dataclass(frozen=True)
class DatabricksContext:
    workspace_id: str
    job_id: str
    job_run_id: str
    triggered_by: str = ""
    task_key: str = ""

@dataclass(frozen=True)
class MetaCtx:
    catalog: str
    schema: str = METADATA_SCHEMA

    def qual(self, table: str) -> str:
        return f"`{self.catalog}`.`{self.schema}`.`{table}`"

def resolve_metadata_catalog(
    environment: str,
    schema: str = METADATA_SCHEMA,
    catalog: str = "",
) -> MetaCtx:
    env = (environment or "").strip().lower()
    if env not in _ENV_TO_CATALOG:
        raise ValueError(f"environment must be one of {sorted(_ENV_TO_CATALOG)}, got {environment!r}")

    default_catalog = _ENV_TO_CATALOG[env]
    resolved_catalog = (catalog or "").strip() or default_catalog
    if env == "prod" and resolved_catalog != default_catalog:
        raise ValueError(
            f"metadata_catalog override to {resolved_catalog!r} is only permitted "
            f"in dev; prod instances must use {default_catalog!r}"
        )

    schema_normalized = (schema or METADATA_SCHEMA).strip() or METADATA_SCHEMA
    if not schema_normalized.startswith(_VALID_METADATA_SCHEMA_PREFIX):
        raise ValueError(
            f"metadata_schema must start with {_VALID_METADATA_SCHEMA_PREFIX!r} "
            f"(e.g. 'jetstream' or 'jetstream_wss'), got {schema!r}"
        )
    if env == "prod" and schema_normalized != METADATA_SCHEMA:
        raise ValueError(
            f"metadata_schema override to {schema_normalized!r} is only permitted "
            f"in dev; prod instances must use the shared {METADATA_SCHEMA!r} schema"
        )
    return MetaCtx(catalog=resolved_catalog, schema=schema_normalized)

def read_databricks_context(dbutils: Any) -> DatabricksContext:
    if dbutils is None:
        return DatabricksContext(
            workspace_id="local",
            job_id="local",
            job_run_id="local",
            triggered_by="local",
            task_key="",
        )
    try:
        ctx = dbutils.notebook.entry_point.getDbutils().notebook().getContext()
        tags = ctx.tags()
        def _t(k: str) -> str:
            v = tags.get(k)
            return v.get() if v is not None and v.isDefined() else ""
        return DatabricksContext(
            workspace_id=_t("orgId") or _t("browserHostName") or "unknown",
            job_id=_t("jobId") or "unknown",
            job_run_id=_t("multitaskParentRunId") or _t("jobRunId") or _t("runId") or "unknown",
            triggered_by=_t("user") or _t("userName") or "",
            task_key=_t("taskKey") or _t("task_key") or "",
        )
    except Exception as exc:
        raise RuntimeError("Failed to read Databricks Jobs runtime context.") from exc