"""Databricks runtime context + metadata-catalog resolution.

The Jetstream platform spec (§7 Instance Bootstrapping) requires every run
to identify itself by its Databricks Jobs run context. We read that context
via dbutils; for local unit tests we accept a hand-built DatabricksContext.

The metadata catalog is selected by an explicit `environment` job parameter
(`dev` -> `metadata_dev`, `prod` -> `metadata_prod`).
Explicit is deliberately preferred over inferring from workspace id or
cluster tags, which fail silently when the mapping/tag is missing.
"""
from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Mapping


METADATA_SCHEMA = "jetstream"

_ENV_TO_CATALOG = {
    "dev": "metadata_dev",
    "prod": "metadata_prod",
}

# Spec §3.2: developers building or testing a new connector type must point
# their sandbox instance at `jetstream_<connector_type>` in the dev catalog,
# never at the shared `jetstream` schema. Overriding the schema is the
# mechanism that makes that isolation possible.
_VALID_METADATA_SCHEMA_PREFIX = "jetstream"


@dataclass(frozen=True)
class DatabricksContext:
    """Minimal Databricks Jobs runtime context needed by connector_run."""
    workspace_id: str
    job_id: str
    job_run_id: str
    triggered_by: str = ""
    task_key: str = ""


@dataclass(frozen=True)
class MetaCtx:
    """Fully-qualified target for the shared jetstream control-plane schema."""
    catalog: str
    schema: str = METADATA_SCHEMA

    def qual(self, table: str) -> str:
        return f"`{self.catalog}`.`{self.schema}`.`{table}`"


def resolve_metadata_catalog(
    environment: str,
    schema: str = METADATA_SCHEMA,
    catalog: str = "",
) -> MetaCtx:
    """Map an `environment` job param to a MetaCtx.

    Defaults per env:
        dev  -> metadata_dev.jetstream
        prod -> metadata_prod.jetstream

    Both `schema` and `catalog` are overridable **only when environment=dev**,
    to support the sandbox phase (spec §3.2). Prod always uses the shared
    catalog+schema; any override is rejected.

    Schema overrides must still start with `jetstream` so the sandbox naming
    convention is enforced.
    """
    env = (environment or "").strip().lower()
    if env not in _ENV_TO_CATALOG:
        raise ValueError(
            f"environment must be one of {sorted(_ENV_TO_CATALOG)}, got {environment!r}"
        )

    default_catalog = _ENV_TO_CATALOG[env]
    resolved_catalog = (catalog or "").strip() or default_catalog
    if env == "prod" and resolved_catalog != default_catalog:
        raise ValueError(
            f"metadata_catalog override to {resolved_catalog!r} is only permitted "
            f"in dev; prod instances must use {default_catalog!r} (spec §3.2)"
        )

    schema_normalized = (schema or METADATA_SCHEMA).strip() or METADATA_SCHEMA
    if not schema_normalized.startswith(_VALID_METADATA_SCHEMA_PREFIX):
        raise ValueError(
            f"metadata_schema must start with {_VALID_METADATA_SCHEMA_PREFIX!r} "
            f"(e.g. 'jetstream' or 'jetstream_servicenow'), got {schema!r}"
        )
    if env == "prod" and schema_normalized != METADATA_SCHEMA:
        raise ValueError(
            f"metadata_schema override to {schema_normalized!r} is only permitted "
            f"in dev; prod instances must use the shared {METADATA_SCHEMA!r} schema (spec §3.2)"
        )
    return MetaCtx(catalog=resolved_catalog, schema=schema_normalized)


def read_databricks_context(dbutils: Any) -> DatabricksContext:
    """Pull workspace/job/run identifiers from the Databricks Jobs runtime.

    `dbutils=None` returns a placeholder context (for local dry-runs).
    """
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
        # `tags()` returns a Scala Map; each key exists as an Option[String].
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
    except Exception as exc:  # pragma: no cover - depends on Databricks runtime
        raise RuntimeError(
            "failed to read Databricks Jobs runtime context; are you running "
            "inside a Databricks Job task?"
        ) from exc
