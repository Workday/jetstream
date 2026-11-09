"""Databricks runtime context + metadata-catalog resolution.

The Jetstream platform spec (§7 Instance Bootstrapping) requires every run
to identify itself by its Databricks Jobs run context. We read that context
via dbutils; for local unit tests we accept a hand-built DatabricksContext.

The metadata catalog is selected by an explicit `environment` job parameter
(`dev` -> `jetstream_metadata_dev`, `prod` -> `jetstream_metadata_prod`).
Explicit is deliberately preferred over inferring from workspace id or
cluster tags, which fail silently when the mapping/tag is missing.
"""
from __future__ import annotations

from dataclasses import dataclass
from typing import Any


METADATA_SCHEMA = "jetstream"

_ENV_TO_CATALOG = {
    "dev": "jetstream_metadata_dev",
    "prod": "jetstream_metadata_prod",
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
        dev  -> jetstream_metadata_dev.jetstream
        prod -> jetstream_metadata_prod.jetstream

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
            f"(e.g. 'jetstream' or 'jetstream_gdrive'), got {schema!r}"
        )
    if env == "prod" and schema_normalized != METADATA_SCHEMA:
        raise ValueError(
            f"metadata_schema override to {schema_normalized!r} is only permitted "
            f"in dev; prod instances must use the shared {METADATA_SCHEMA!r} schema (spec §3.2)"
        )
    return MetaCtx(catalog=resolved_catalog, schema=schema_normalized)


def read_databricks_context(
    dbutils: Any, spark: Any = None, widget_params: dict | None = None,
) -> DatabricksContext:
    """Pull workspace/job/run identifiers from the Databricks Jobs runtime.

    `dbutils=None` returns a placeholder context (for local dry-runs).

    Strategy chain (first one that produces a valid job_id wins):
      1. Classic getContext().tags() — works on single-user clusters.
      2. Spark conf tags — works on USER_ISOLATION / shared clusters.
      3. getContext().toJson() parse — broader compatibility.
      4. Widget params (job parameters) — works on serverless where
         strategies 1-3 are blocked.  Requires the job to set
         `databricks_job_id`, `databricks_job_run_id`, and optionally
         `databricks_workspace_id` as base_parameters.  Use the
         Databricks dynamic value references ``{{job_id}}`` and
         ``{{run_id}}`` so they resolve automatically at runtime.
    """
    if dbutils is None:
        return DatabricksContext(
            workspace_id="local", job_id="local", job_run_id="local", triggered_by="local",
        )

    # Strategy 1: Classic getContext().tags() — works on single-user mode.
    try:
        ctx = dbutils.notebook.entry_point.getDbutils().notebook().getContext()
        tags = ctx.tags()

        def _t(k: str) -> str:
            v = tags.get(k)
            return v.get() if v is not None and v.isDefined() else ""

        result = DatabricksContext(
            workspace_id=_t("orgId") or _t("browserHostName") or "unknown",
            job_id=_t("jobId") or "unknown",
            job_run_id=_t("multitaskParentRunId") or _t("jobRunId") or _t("runId") or "unknown",
            triggered_by=_t("user") or _t("userName") or "",
        )
        if result.job_id != "unknown":
            return result
    except Exception:
        pass

    # Strategy 2: Spark conf (works on USER_ISOLATION / shared clusters).
    if spark is not None:
        try:
            def _conf(k: str, default: str = "unknown") -> str:
                try:
                    return spark.conf.get(k) or default
                except Exception:
                    return default

            result = DatabricksContext(
                workspace_id=_conf("spark.databricks.clusterUsageTags.clusterOwnerOrgId"),
                job_id=_conf("spark.databricks.job.id"),
                job_run_id=_conf("spark.databricks.job.runId"),
                triggered_by=_conf("spark.databricks.job.triggeredBy", ""),
            )
            if result.job_id != "unknown":
                return result
        except Exception:
            pass

    # Strategy 3: Parse getContext() JSON (fallback for edge cases).
    try:
        import json
        ctx_json = dbutils.notebook.entry_point.getDbutils().notebook().getContext().toJson()
        ctx_dict = json.loads(ctx_json)
        tags = ctx_dict.get("tags", {})
        extra = ctx_dict.get("extraContext", {})
        result = DatabricksContext(
            workspace_id=tags.get("orgId", extra.get("orgId", "unknown")),
            job_id=tags.get("jobId", extra.get("jobId", "unknown")),
            job_run_id=tags.get("multitaskParentRunId", tags.get("jobRunId", "unknown")),
            triggered_by=tags.get("user", extra.get("user", "")),
        )
        if result.job_id != "unknown":
            return result
    except Exception:
        pass

    # Strategy 4: Widget params — works on serverless where strategies 1-3
    # are blocked.  The job must set databricks_job_id, databricks_job_run_id
    # (use Databricks dynamic value references {{job_id}} / {{run_id}}).
    wp = widget_params or {}
    w_job_id = str(wp.get("databricks_job_id", "")).strip()
    w_run_id = str(wp.get("databricks_job_run_id", "")).strip()
    w_ws_id = str(wp.get("databricks_workspace_id", "")).strip()

    # Try apiUrl() for workspace_id as a last resort.
    if not w_ws_id:
        try:
            api_url = (
                dbutils.notebook.entry_point.getDbutils()
                .notebook().getContext().apiUrl().get()
            )
            # Extract the o= parameter or use the URL itself.
            if api_url:
                w_ws_id = api_url
        except Exception:
            pass

    if w_job_id:
        return DatabricksContext(
            workspace_id=w_ws_id or "unknown",
            job_id=w_job_id,
            job_run_id=w_run_id or "unknown",
            triggered_by="",
        )

    # Nothing worked — return best-effort "unknown" context so the run
    # is not blocked.
    return DatabricksContext(
        workspace_id=w_ws_id or "unknown",
        job_id="unknown",
        job_run_id="unknown",
        triggered_by="",
    )
