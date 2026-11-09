"""Typed views over a validated `source_config` dict.

Under the Jetstream platform model, per-instance configuration lives in
Databricks Job parameters (read via `dbutils.widgets.getAll()`), not in a
YAML file inside this repo. The compiled JSON Schema for those parameters
lives in `connectors/zoom/parameters.yaml`.

This module keeps the small `SourceCfg` / `ObjectCfg` / `RetryCfg` dataclasses
so `src/http.py`, `src/auth.py`, and `src/extract.py` stay simple and testable
without needing the whole platform stack. All they see is a frozen dataclass.

The resolver hydrates those dataclasses from the widget dict + the Databricks
secret scope, once per run, in `resolve_run_config()`.
"""
from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Mapping, Optional, Sequence


# ---------------------------------------------------------------------------
# Small typed views (unchanged shape — tests use these directly)
# ---------------------------------------------------------------------------

@dataclass(frozen=True)
class RetryCfg:
    max_attempts: int = 5
    backoff_seconds: float = 1.0
    backoff_multiplier: float = 2.0
    retry_on_status: Sequence[int] = (429, 500, 502, 503, 504)


@dataclass(frozen=True)
class SourceCfg:
    base_url: str
    token_url: str
    scope: str
    account_id: str            # resolved from Databricks secret
    client_id: str             # resolved
    client_secret: str         # resolved
    rate_limit_qps: float = 10
    retry: RetryCfg = field(default_factory=RetryCfg)
    timeout_seconds: int = 60


@dataclass(frozen=True)
class ObjectCfg:
    name: str
    primary_key: Sequence[str]
    load_strategy: str = "full"          # 'full' | 'incremental_cursor'
    cursor_field: Optional[str] = None
    lookback_minutes: int = 0
    parent: Optional[str] = None
    # When this object is a fan-out child (parent is set), Zoom typically
    # passes the parent id in the URL but does NOT echo it in each record.
    # `parent_id_column` names the column we stamp with the fan-out parent id
    # so it can appear in primary_key. E.g. for room_devices this is 'room_id',
    # for past_meeting_participants it's 'meeting_uuid'.
    parent_id_column: Optional[str] = None
    required_scope: Optional[str] = None
    query_params: dict = field(default_factory=dict)
    cursor_param_from: Optional[str] = None
    cursor_param_to: Optional[str] = None
    cursor_format: str = "datetime"
    optional: bool = False
    # Per-object HTTP tuning — override the framework defaults for objects
    # that hit tighter Zoom-side throttles (typically Heavy-tier Dashboard
    # endpoints). `max_workers` caps ThreadPoolExecutor concurrency for this
    # object's fan-out; `max_retry_attempts` overrides retry.max_attempts
    # for HTTP calls to this object's endpoints. Leave None to use defaults
    # (_HTTP_MAX_WORKERS=8 workers, retry.max_attempts=5).
    max_workers: Optional[int] = None
    max_retry_attempts: Optional[int] = None


# ---------------------------------------------------------------------------
# Runtime resolver
# ---------------------------------------------------------------------------

# Convention: every zoom instance stores its OAuth material under one secret
# scope, with these three fixed keys.
_SECRET_KEYS = {
    "account_id": "zoom_account_id",
    "client_id": "zoom_client_id",
    "client_secret": "zoom_client_secret",
}


@dataclass(frozen=True)
class PlatformRouting:
    """Non-secret routing values needed before the run row is opened.

    Everything here can be read directly from widget values without any
    Databricks secret lookups. Used by pipeline.run() for the metadata
    catalog + bootstrap + connector_instance columns, so a bad secret
    scope doesn't crash the pipeline before it has a `connector_run`
    row to log the failure against.
    """
    environment: str
    instance_name: str
    destination_catalog: str
    destination_schema: str
    metadata_catalog: str
    metadata_schema: str
    sla_target_minutes: int
    owner_team: str


def resolve_platform_routing(widgets: Mapping[str, Any]) -> PlatformRouting:
    """Extract non-secret routing values from a coerced widget dict."""
    def _str(k: str, default: str = "") -> str:
        v = widgets.get(k, default)
        return default if v is None else str(v).strip()

    def _int(k: str, default: int) -> int:
        v = widgets.get(k)
        return default if v in (None, "") else int(v)

    return PlatformRouting(
        environment=_str("environment"),
        instance_name=_str("instance_name"),
        destination_catalog=_str("destination_catalog"),
        destination_schema=_str("destination_schema"),
        metadata_catalog=_str("metadata_catalog"),
        metadata_schema=_str("metadata_schema", "jetstream") or "jetstream",
        sla_target_minutes=_int("sla_target_minutes", 1440),
        owner_team=_str("owner_team"),
    )


@dataclass(frozen=True)
class RunConfig:
    """Everything the pipeline needs for one run, after widget + secret resolution."""
    # From platform routing widgets
    environment: str
    instance_name: str
    destination_catalog: str
    destination_schema: str
    metadata_catalog: str
    metadata_schema: str
    sla_target_minutes: int
    owner_team: str
    # Zoom source auth + tuning
    source: SourceCfg
    # Object selection (raw csv, resolved by src.objects.select_objects)
    enabled_objects_csv: str
    optional_objects_enabled_csv: str


def resolve_run_config(widgets: Mapping[str, Any], dbutils: Any = None) -> RunConfig:
    """Build a RunConfig from a validated widget dict.

    Callers are expected to have already run
    `jetstream.validate.validate_source_config(widgets, config_schema)` so
    required fields are present and typed. Defaults from `parameters.yaml`
    are re-applied here for the optional fields, since `getAll()` returns
    string values only.
    """
    def _str(k: str, default: str = "") -> str:
        v = widgets.get(k, default)
        return default if v is None else str(v).strip()

    def _int(k: str, default: int) -> int:
        v = widgets.get(k)
        return default if v in (None, "") else int(v)

    def _float(k: str, default: float) -> float:
        v = widgets.get(k)
        return default if v in (None, "") else float(v)

    secret_scope = _str("secret_scope")
    resolve = _secret_resolver(secret_scope, dbutils)

    retry = RetryCfg(
        max_attempts=_int("retry_max_attempts", 5),
        backoff_seconds=1.0,
        backoff_multiplier=2.0,
        retry_on_status=(429, 500, 502, 503, 504),
    )
    source = SourceCfg(
        base_url=_str("zoom_base_url", "https://api.zoom.us/v2").rstrip("/"),
        token_url=_str("zoom_token_url", "https://zoom.us/oauth/token"),
        scope="",
        account_id=resolve(_SECRET_KEYS["account_id"]),
        client_id=resolve(_SECRET_KEYS["client_id"]),
        client_secret=resolve(_SECRET_KEYS["client_secret"]),
        rate_limit_qps=_float("rate_limit_qps", 10),
        retry=retry,
        timeout_seconds=_int("timeout_seconds", 60),
    )
    return RunConfig(
        environment=_str("environment"),
        instance_name=_str("instance_name"),
        destination_catalog=_str("destination_catalog"),
        destination_schema=_str("destination_schema"),
        metadata_catalog=_str("metadata_catalog"),
        metadata_schema=_str("metadata_schema", "jetstream") or "jetstream",
        sla_target_minutes=_int("sla_target_minutes", 1440),
        owner_team=_str("owner_team"),
        source=source,
        enabled_objects_csv=_str("enabled_objects"),
        optional_objects_enabled_csv=_str("optional_objects_enabled"),
    )


def source_config_for_manifest(widgets: Mapping[str, Any]) -> dict:
    """Return the subset of widget values that goes into
    connector_instance.source_config (JSON). Excludes destination_catalog
    and destination_schema, which are separate top-level columns."""
    exclude = {"destination_catalog", "destination_schema"}
    return {k: v for k, v in widgets.items() if k not in exclude}


def _secret_resolver(scope: str, dbutils: Any):
    if not scope:
        return lambda key: f"<no-scope:{key}>"
    if dbutils is None:
        return lambda key: f"<{scope}:{key}>"
    return lambda key: dbutils.secrets.get(scope=scope, key=key)
