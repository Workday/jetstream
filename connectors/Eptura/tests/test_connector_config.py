"""Tests for the widget-based config resolver."""
from __future__ import annotations

from pathlib import Path

from src.connector_config import (
    resolve_platform_routing,
    resolve_run_config,
    source_config_for_manifest,
)
from src.jetstream.parameter_compiler import compile_parameters_yaml

_EPTURA_SCHEMA = compile_parameters_yaml(
    Path(__file__).resolve().parent.parent / "connectors" / "eptura" / "parameters.yaml"
)


def test_defaults_are_applied_for_optional_fields():
    widgets = {
        "environment": "dev",
        "instance_name": "eptura_dev",
        "destination_catalog": "raw_dev",
        "destination_schema": "eptura",
        "secret_scope": "example_scope",
    }
    cfg = resolve_run_config(widgets, dbutils=None)
    assert cfg.environment == "dev"
    assert cfg.instance_name == "eptura_dev"
    assert cfg.destination_catalog == "raw_dev"
    assert cfg.destination_schema == "eptura"
    assert cfg.sla_target_minutes == 1440
    assert cfg.source.site_address.startswith("<example_scope:")
    assert cfg.source.api_base_path == "/external/api/rest/v2"
    assert cfg.source.page_size == 1000
    assert cfg.source.rate_limit_qps == 10
    assert cfg.source.timeout_seconds == 60
    assert cfg.source.base_url.endswith("/external/api/rest/v2")
    assert cfg.tombstone_guard_pct == 20.0
    assert cfg.enabled_objects_csv == ""


def test_numeric_overrides_are_carried_through():
    cfg = resolve_run_config({
        "environment": "dev",
        "instance_name": "e", "destination_catalog": "c", "destination_schema": "s",
        "secret_scope": "sc",
        "page_size": 500,
        "rate_limit_qps": 2.5,
        "retry_max_attempts": 8,
        "tombstone_guard_pct": 50,
    }, dbutils=None)
    assert cfg.source.page_size == 500
    assert cfg.source.rate_limit_qps == 2.5
    assert cfg.source.retry.max_attempts == 8
    assert cfg.tombstone_guard_pct == 50.0


def test_trailing_slash_is_stripped_from_the_site_address():
    class FakeDbutils:
        class secrets:
            @staticmethod
            def get(scope, key):
                return "https://your-tenant.ioffice.com/"

    cfg = resolve_run_config({
        "environment": "dev",
        "instance_name": "e", "destination_catalog": "c", "destination_schema": "s",
        "secret_scope": "sc",
    }, dbutils=FakeDbutils())
    assert cfg.source.site_address == "https://your-tenant.ioffice.com"
    assert cfg.source.base_url == "https://your-tenant.ioffice.com/external/api/rest/v2"


def test_dbutils_secrets_are_resolved_when_provided():
    class FakeSecrets:
        def get(self, scope, key):
            return f"resolved-{key}"

    class FakeDbutils:
        secrets = FakeSecrets()

    widgets = {
        "environment": "prod",
        "instance_name": "eptura", "destination_catalog": "c", "destination_schema": "s",
        "secret_scope": "sc",
    }
    cfg = resolve_run_config(widgets, dbutils=FakeDbutils())
    assert cfg.source.site_address == "resolved-eptura_host"
    assert cfg.source.username == "resolved-eptura_username"
    assert cfg.source.client_id == "resolved-eptura_client_id"


def test_source_config_for_manifest_excludes_destination_fields():
    widgets = {
        "environment": "dev",
        "instance_name": "x",
        "destination_catalog": "cat",
        "destination_schema": "sch",
        "secret_scope": "sc",
    }
    payload = source_config_for_manifest(widgets, _EPTURA_SCHEMA)
    assert "destination_catalog" not in payload
    assert "destination_schema" not in payload
    assert payload["environment"] == "dev"
    assert payload["instance_name"] == "x"
    assert payload["secret_scope"] == "sc"


def test_source_config_for_manifest_excludes_per_run_overrides():
    """A one-off `objects=users` re-sync must not leave the instance looking
    permanently scoped to users."""
    widgets = {
        "environment": "dev",
        "instance_name": "x",
        "destination_catalog": "cat",
        "destination_schema": "sch",
        "secret_scope": "sc",
        "enabled_objects": "rooms,users",
        "objects": "users",
    }
    payload = source_config_for_manifest(widgets, _EPTURA_SCHEMA)
    assert "objects" not in payload
    assert payload["enabled_objects"] == "rooms,users"


def test_metadata_schema_defaults_to_shared_jetstream_when_absent():
    widgets = {
        "environment": "prod",
        "instance_name": "e", "destination_catalog": "c", "destination_schema": "s",
        "secret_scope": "sc",
    }
    cfg = resolve_run_config(widgets, dbutils=None)
    assert cfg.metadata_schema == "jetstream"


def test_metadata_schema_dev_sandbox_override_is_carried_through():
    widgets = {
        "environment": "dev",
        "instance_name": "e", "destination_catalog": "c", "destination_schema": "s",
        "secret_scope": "sc",
        "metadata_schema": "jetstream_eptura",
    }
    cfg = resolve_run_config(widgets, dbutils=None)
    assert cfg.metadata_schema == "jetstream_eptura"


def test_platform_routing_does_not_touch_secrets():
    routing = resolve_platform_routing({
        "environment": "dev",
        "instance_name": "eptura_sandbox",
        "destination_catalog": "raw_dev",
        "destination_schema": "eptura",
        "metadata_catalog": "raw_dev",
        "metadata_schema": "jetstream_eptura",
        "secret_scope": "nonexistent",
        "sla_target_minutes": 1440,
        "owner_team": "data-platform",
    })
    assert routing.environment == "dev"
    assert routing.instance_name == "eptura_sandbox"
    assert routing.metadata_schema == "jetstream_eptura"


def test_platform_routing_defaults_metadata_schema_to_jetstream():
    routing = resolve_platform_routing({
        "environment": "prod",
        "instance_name": "e",
        "destination_catalog": "c",
        "destination_schema": "s",
        "secret_scope": "sc",
    })
    assert routing.metadata_schema == "jetstream"
    assert routing.metadata_catalog == ""
