"""Typed views over the Databricks Job widget values.

Everything this connector needs to run comes from two places: non-secret Job
parameters (catalogs, schemas, page size, timeouts) and a Databricks secret
scope (host + OAuth credentials). This module turns both into frozen
dataclasses so the rest of the code never touches raw widget strings.
"""
from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Mapping, Sequence

from src.jetstream import parameter_compiler as params_mod


@dataclass(frozen=True)
class RetryCfg:
    max_attempts: int = 5
    backoff_seconds: float = 1.0
    backoff_multiplier: float = 2.0
    retry_on_status: Sequence[int] = (429, 500, 502, 503, 504)


@dataclass(frozen=True)
class SourceCfg:
    """Eptura iOffice API connection settings (secrets resolved at runtime)."""
    site_address: str            # e.g. https://your-tenant.ioffice.com
    api_base_path: str           # e.g. /external/api/rest/v2
    username: str
    password: str
    client_id: str
    redirect_uri: str = ""
    oauth_scope: str = "email"
    page_size: int = 1000
    rate_limit_qps: float = 10
    retry: RetryCfg = field(default_factory=RetryCfg)
    timeout_seconds: int = 60

    @property
    def base_url(self) -> str:
        return f"{self.site_address.rstrip('/')}/{self.api_base_path.strip('/')}"


@dataclass(frozen=True)
class ObjectCfg:
    """One Eptura entity. The name doubles as the API path segment and the
    destination table name."""
    name: str
    primary_key: Sequence[str] = ("id",)
    # List endpoints sometimes omit nested relations. When True, each listed
    # id is re-fetched as GET /{name}/{id} so those fields (e.g. floor on
    # neighborhoods) are present in the snapshot.
    fetch_detail: bool = False


_SECRET_KEYS = {
    "site_address": "eptura_host",
    "username": "eptura_username",
    "password": "eptura_password",
    "client_id": "eptura_client_id",
}


@dataclass(frozen=True)
class PlatformRouting:
    """Where this run reports to. Resolved before secrets so a bad
    `secret_scope` can still be logged against a real connector_run row."""
    environment: str
    instance_name: str
    destination_catalog: str
    destination_schema: str
    metadata_catalog: str
    metadata_schema: str
    sla_target_minutes: int
    owner_team: str


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
    tombstone_guard_pct: float


def resolve_platform_routing(widgets: Mapping[str, Any]) -> PlatformRouting:
    return PlatformRouting(
        environment=_str(widgets, "environment"),
        instance_name=_str(widgets, "instance_name"),
        destination_catalog=_str(widgets, "destination_catalog"),
        destination_schema=_str(widgets, "destination_schema"),
        metadata_catalog=_str(widgets, "metadata_catalog"),
        metadata_schema=_str(widgets, "metadata_schema", "jetstream") or "jetstream",
        sla_target_minutes=_int(widgets, "sla_target_minutes", 1440),
        owner_team=_str(widgets, "owner_team"),
    )


def resolve_run_config(widgets: Mapping[str, Any], dbutils: Any = None) -> RunConfig:
    resolve = _secret_resolver(_str(widgets, "secret_scope"), dbutils)

    source = SourceCfg(
        site_address=resolve(_SECRET_KEYS["site_address"]).rstrip("/"),
        api_base_path=_str(widgets, "api_base_path", "/external/api/rest/v2"),
        username=resolve(_SECRET_KEYS["username"]),
        password=resolve(_SECRET_KEYS["password"]),
        client_id=resolve(_SECRET_KEYS["client_id"]),
        redirect_uri=_str(widgets, "redirect_uri"),
        oauth_scope=_str(widgets, "oauth_scope", "email"),
        page_size=_int(widgets, "page_size", 1000),
        rate_limit_qps=_float(widgets, "rate_limit_qps", 10),
        retry=RetryCfg(max_attempts=_int(widgets, "retry_max_attempts", 5)),
        timeout_seconds=_int(widgets, "timeout_seconds", 60),
    )
    routing = resolve_platform_routing(widgets)
    return RunConfig(
        environment=routing.environment,
        instance_name=routing.instance_name,
        destination_catalog=routing.destination_catalog,
        destination_schema=routing.destination_schema,
        metadata_catalog=routing.metadata_catalog,
        metadata_schema=routing.metadata_schema,
        sla_target_minutes=routing.sla_target_minutes,
        owner_team=routing.owner_team,
        source=source,
        enabled_objects_csv=_str(widgets, "enabled_objects"),
        tombstone_guard_pct=_float(widgets, "tombstone_guard_pct", 20.0),
    )


def source_config_for_manifest(
    widgets: Mapping[str, Any],
    config_schema: Mapping[str, Any],
) -> dict:
    """The subset of the parameters that describes this instance.

    Two kinds of parameter are left out: the destination catalog and schema,
    which have dedicated columns on `connector_instance`, and anything the
    manifest marks `scope: run`, which belongs to one execution rather than
    to the instance's standing configuration.
    """
    exclude = {"destination_catalog", "destination_schema"}
    exclude |= params_mod.run_scoped_names(config_schema)
    return {k: v for k, v in widgets.items() if k not in exclude}


# ---------------------------------------------------------------------------
# widget readers
# ---------------------------------------------------------------------------

def _str(widgets: Mapping[str, Any], key: str, default: str = "") -> str:
    v = widgets.get(key, default)
    return default if v is None else str(v).strip()


def _int(widgets: Mapping[str, Any], key: str, default: int) -> int:
    v = widgets.get(key)
    return default if v in (None, "") else int(v)


def _float(widgets: Mapping[str, Any], key: str, default: float) -> float:
    v = widgets.get(key)
    return default if v in (None, "") else float(v)


def _secret_resolver(scope: str, dbutils: Any):
    """Return a `key -> secret` lookup. Without a scope or dbutils (local
    dry-runs, unit tests) it returns readable placeholders instead of failing."""
    if not scope:
        return lambda key: f"<no-scope:{key}>"
    if dbutils is None:
        return lambda key: f"<{scope}:{key}>"
    return lambda key: dbutils.secrets.get(scope=scope, key=key)
