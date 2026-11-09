"""Typed views over validated source_config widget values.

Under the Jetstream platform model, per-instance configuration lives in
Databricks Job parameters (read via `dbutils.widgets.getAll()`). This module
converts those widget strings into typed dataclasses so the rest of the
connector sees clean Python types, not raw strings.

Authentication: Juniper Mist uses a simple API token
(Authorization: Token <value>). There is no OAuth flow. The token is
stored in a Databricks secret scope; only the scope name appears here.
"""
from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Mapping, Optional, Sequence, Tuple


# ---------------------------------------------------------------------------
# Small typed building blocks
# ---------------------------------------------------------------------------

@dataclass(frozen=True)
class RetryCfg:
    max_attempts: int = 5
    backoff_seconds: float = 1.0
    backoff_multiplier: float = 2.0
    retry_on_status: Sequence[int] = (429, 500, 502, 503, 504)


@dataclass(frozen=True)
class SourceCfg:
    """Everything needed to call the Juniper Mist API."""
    base_url: str
    org_id: str                         # from widget (not a secret)
    api_token: str                      # from Databricks secret scope
    rate_limit_qps: float = 5.0
    retry: RetryCfg = field(default_factory=RetryCfg)
    timeout_seconds: int = 60
    lookback_days: int = 7
    fan_out_max_workers: int = 8
    # Case-insensitive substrings; empty tuple = no filter (all sites)
    site_filter_substrings: Tuple[str, ...] = ()
    # Minimum hours between full-load runs. 0 = disabled (always run).
    # snapshot and incremental_cursor objects are never subject to this check.
    full_load_interval_hours: int = 0


@dataclass(frozen=True)
class ObjectCfg:
    """Per-object sync specification (defined in api_objects.py, not YAML)."""
    name: str
    primary_key: Sequence[str]
    load_strategy: str = "full"             # 'full' | 'incremental_cursor' | 'snapshot'
    cursor_field: Optional[str] = None
    lookback_minutes: int = 0
    parent: Optional[str] = None            # parent key in parent_id_cache
    parent_id_column: Optional[str] = None  # column stamped with fan-out parent id
    required_scope: Optional[str] = None    # always None for Mist (no OAuth scopes)
    query_params: dict = field(default_factory=dict)
    cursor_param_from: Optional[str] = None  # e.g. 'start' (epoch seconds)
    cursor_param_to: Optional[str] = None    # e.g. 'end'
    optional: bool = False
    max_workers: Optional[int] = None
    max_retry_attempts: Optional[int] = None


# ---------------------------------------------------------------------------
# Platform routing (resolved from widgets, no secrets needed)
# ---------------------------------------------------------------------------

@dataclass(frozen=True)
class PlatformRouting:
    environment: str
    instance_name: str
    destination_catalog: str
    destination_schema: str
    metadata_catalog: str
    metadata_schema: str
    sla_target_minutes: int
    owner_team: str


def resolve_platform_routing(widgets: Mapping[str, Any]) -> PlatformRouting:
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


# ---------------------------------------------------------------------------
# Full RunConfig (requires secret resolution)
# ---------------------------------------------------------------------------

@dataclass(frozen=True)
class RunConfig:
    environment: str
    instance_name: str
    destination_catalog: str
    destination_schema: str
    metadata_catalog: str
    metadata_schema: str
    sla_target_minutes: int
    owner_team: str
    source: SourceCfg
    enabled_objects_csv: str


def resolve_run_config(widgets: Mapping[str, Any], dbutils: Any = None) -> RunConfig:
    """Build a RunConfig from a validated widget dict, resolving secrets."""

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

    def _str_with_secret_fallback(widget_key: str, secret_key: str, default: str = "") -> str:
        """Return widget value if set; fall back to the secret scope key if empty."""
        v = _str(widget_key, "")
        if v:
            return v
        if secret_scope and dbutils is not None:
            try:
                sv = dbutils.secrets.get(scope=secret_scope, key=secret_key)
                if sv:
                    return sv.strip()
            except Exception:
                pass
        return default

    site_filter_raw = _str("site_filter", "")
    site_filter_substrings = tuple(
        s.strip().lower() for s in site_filter_raw.split(",") if s.strip()
    )

    retry = RetryCfg(
        max_attempts=_int("retry_max_attempts", 5),
        backoff_seconds=1.0,
        backoff_multiplier=2.0,
        retry_on_status=(429, 500, 502, 503, 504),
    )
    source = SourceCfg(
        base_url=_str_with_secret_fallback(
            "juniper_mist_base_url", "juniper_mist_base_url",
            "https://api.mist.com/api/v1",
        ).rstrip("/"),
        org_id=_str_with_secret_fallback("juniper_mist_org_id", "juniper_mist_org_id"),
        api_token=resolve("juniper_mist_token"),
        rate_limit_qps=_float("rate_limit_qps", 5.0),
        retry=retry,
        timeout_seconds=_int("timeout_seconds", 60),
        lookback_days=_int("lookback_days", 7),
        fan_out_max_workers=_int("fan_out_max_workers", 8),
        site_filter_substrings=site_filter_substrings,
        full_load_interval_hours=_int("full_load_interval_hours", 0),
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
    )


def source_config_for_manifest(widgets: Mapping[str, Any]) -> dict:
    """Subset of widget values written to connector_instance.source_config.

    Excludes destination routing columns (they live as top-level columns on
    connector_instance, not inside source_config JSON).
    """
    exclude = {"destination_catalog", "destination_schema"}
    return {k: v for k, v in widgets.items() if k not in exclude}


def _secret_resolver(scope: str, dbutils: Any):
    if not scope:
        return lambda key: f"<no-scope:{key}>"
    if dbutils is None:
        return lambda key: f"<{scope}:{key}>"
    return lambda key: dbutils.secrets.get(scope=scope, key=key)
