"""Tests for the widget-based config resolver."""
from __future__ import annotations

from src.config import (
    resolve_platform_routing,
    resolve_run_config,
    source_config_for_manifest,
)


def test_defaults_are_applied_for_optional_fields():
    widgets = {
        "environment": "dev",
        "instance_name": "zoom_dev",
        "destination_catalog": "raw_dev",
        "destination_schema": "zoom",
        "secret_scope": "example_scope",
    }
    cfg = resolve_run_config(widgets, dbutils=None)
    assert cfg.environment == "dev"
    assert cfg.instance_name == "zoom_dev"
    assert cfg.destination_catalog == "raw_dev"
    assert cfg.destination_schema == "zoom"
    assert cfg.sla_target_minutes == 1440
    assert cfg.source.base_url == "https://api.zoom.us/v2"
    assert cfg.source.token_url == "https://zoom.us/oauth/token"
    assert cfg.source.rate_limit_qps == 10
    assert cfg.source.timeout_seconds == 60
    # dbutils=None -> placeholder secret string, never a real value
    assert cfg.source.account_id.startswith("<example_scope:")


def test_dbutils_secrets_are_resolved_when_provided():
    class FakeSecrets:
        def get(self, scope, key):
            return f"resolved-{key}"

    class FakeDbutils:
        secrets = FakeSecrets()

    widgets = {
        "environment": "prod",
        "instance_name": "z", "destination_catalog": "c", "destination_schema": "s",
        "secret_scope": "sc",
    }
    cfg = resolve_run_config(widgets, dbutils=FakeDbutils())
    assert cfg.source.account_id == "resolved-zoom_account_id"
    assert cfg.source.client_secret == "resolved-zoom_client_secret"


def test_source_config_for_manifest_excludes_destination_fields():
    widgets = {
        "environment": "dev",
        "instance_name": "x",
        "destination_catalog": "cat",
        "destination_schema": "sch",
        "secret_scope": "sc",
    }
    payload = source_config_for_manifest(widgets)
    assert "destination_catalog" not in payload
    assert "destination_schema" not in payload
    assert payload["environment"] == "dev"
    assert payload["instance_name"] == "x"
    assert payload["secret_scope"] == "sc"


def test_metadata_schema_defaults_to_shared_jetstream_when_absent():
    widgets = {
        "environment": "prod",
        "instance_name": "z", "destination_catalog": "c", "destination_schema": "s",
        "secret_scope": "sc",
    }
    cfg = resolve_run_config(widgets, dbutils=None)
    assert cfg.metadata_schema == "jetstream"


def test_metadata_schema_dev_sandbox_override_is_carried_through():
    widgets = {
        "environment": "dev",
        "instance_name": "z", "destination_catalog": "c", "destination_schema": "s",
        "secret_scope": "sc",
        "metadata_schema": "jetstream_zoom",
    }
    cfg = resolve_run_config(widgets, dbutils=None)
    assert cfg.metadata_schema == "jetstream_zoom"


# ---------------------------------------------------------------------------
# resolve_platform_routing — secret-free routing extract for pre-open path
# ---------------------------------------------------------------------------

def test_platform_routing_does_not_touch_secrets():
    """Bad secret_scope must NOT crash platform routing extraction.

    pipeline.run() relies on this so bootstrap and MetaCtx can be built
    before open_run, even when secret_scope is wrong. The failure then
    surfaces after open_run so it lands in connector_log.
    """
    routing = resolve_platform_routing({
        "environment": "dev",
        "instance_name": "zoom_sandbox_x",
        "destination_catalog": "raw_dev",
        "destination_schema": "zoom",
        "metadata_catalog": "raw_dev",
        "metadata_schema": "jetstream_zoom",
        "secret_scope": "nonexistent",   # would fail if we tried to resolve
        "sla_target_minutes": 1440,
        "owner_team": "data-platform",
    })
    assert routing.environment == "dev"
    assert routing.instance_name == "zoom_sandbox_x"
    assert routing.destination_catalog == "raw_dev"
    assert routing.destination_schema == "zoom"
    assert routing.metadata_catalog == "raw_dev"
    assert routing.metadata_schema == "jetstream_zoom"
    assert routing.sla_target_minutes == 1440
    assert routing.owner_team == "data-platform"


def test_platform_routing_defaults_metadata_schema_to_jetstream():
    routing = resolve_platform_routing({
        "environment": "prod",
        "instance_name": "z",
        "destination_catalog": "c",
        "destination_schema": "s",
        "secret_scope": "sc",
    })
    assert routing.metadata_schema == "jetstream"
    assert routing.metadata_catalog == ""  # empty -> ctx.resolve_metadata_catalog picks default
