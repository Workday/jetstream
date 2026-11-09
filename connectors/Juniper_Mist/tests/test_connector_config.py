"""Tests for src/connector_config.py."""
from __future__ import annotations

import pytest


def test_resolve_run_config_basic():
    from src.connector_config import resolve_run_config

    widgets = {
        "environment": "dev",
        "instance_name": "juniper_mist_test",
        "destination_catalog": "raw_dev",
        "destination_schema": "juniper_mist",
        "metadata_catalog": "",
        "metadata_schema": "jetstream",
        "sla_target_minutes": "1440",
        "owner_team": "data-eng",
        "secret_scope": "",
        "juniper_mist_org_id": "44a1aa85-fff5-498f-91fa-6cd89632176c",
        "juniper_mist_base_url": "https://api.mist.com/api/v1",
        "rate_limit_qps": "5",
        "timeout_seconds": "60",
        "retry_max_attempts": "5",
        "lookback_days": "7",
        "fan_out_max_workers": "8",
        "site_filter": "",
        "enabled_objects": "",
    }
    cfg = resolve_run_config(widgets, dbutils=None)

    assert cfg.environment == "dev"
    assert cfg.instance_name == "juniper_mist_test"
    assert cfg.destination_catalog == "raw_dev"
    assert cfg.destination_schema == "juniper_mist"
    assert cfg.source.org_id == "44a1aa85-fff5-498f-91fa-6cd89632176c"
    assert cfg.source.base_url == "https://api.mist.com/api/v1"
    assert cfg.source.rate_limit_qps == 5.0
    assert cfg.source.lookback_days == 7
    assert cfg.source.fan_out_max_workers == 8
    assert cfg.source.site_filter_substrings == ()
    assert cfg.enabled_objects_csv == ""


def test_site_filter_parsed_as_tuple():
    from src.connector_config import resolve_run_config

    widgets = {
        "environment": "dev",
        "instance_name": "x",
        "destination_catalog": "c",
        "destination_schema": "s",
        "metadata_catalog": "",
        "metadata_schema": "jetstream",
        "sla_target_minutes": "1440",
        "owner_team": "",
        "secret_scope": "",
        "juniper_mist_org_id": "some-org",
        "juniper_mist_base_url": "https://api.mist.com/api/v1",
        "rate_limit_qps": "5",
        "timeout_seconds": "30",
        "retry_max_attempts": "3",
        "lookback_days": "7",
        "fan_out_max_workers": "4",
        "site_filter": "Pleasanton,Santa Clara",
        "enabled_objects": "",
    }
    cfg = resolve_run_config(widgets, dbutils=None)

    assert cfg.source.site_filter_substrings == ("pleasanton", "santa clara")


def test_base_url_trailing_slash_stripped():
    from src.connector_config import resolve_run_config

    widgets = {
        "environment": "dev",
        "instance_name": "x",
        "destination_catalog": "c",
        "destination_schema": "s",
        "metadata_catalog": "",
        "metadata_schema": "jetstream",
        "sla_target_minutes": "1440",
        "owner_team": "",
        "secret_scope": "",
        "juniper_mist_org_id": "some-org",
        "juniper_mist_base_url": "https://api.mist.com/api/v1/",
        "rate_limit_qps": "5",
        "timeout_seconds": "30",
        "retry_max_attempts": "3",
        "lookback_days": "7",
        "fan_out_max_workers": "4",
        "site_filter": "",
        "enabled_objects": "",
    }
    cfg = resolve_run_config(widgets, dbutils=None)
    assert not cfg.source.base_url.endswith("/")


def test_source_config_for_manifest_excludes_destination():
    from src.connector_config import source_config_for_manifest

    widgets = {
        "destination_catalog": "should-be-excluded",
        "destination_schema": "also-excluded",
        "juniper_mist_org_id": "kept",
        "enabled_objects": "",
    }
    result = source_config_for_manifest(widgets)
    assert "destination_catalog" not in result
    assert "destination_schema" not in result
    assert result["juniper_mist_org_id"] == "kept"


def test_resolve_platform_routing():
    from src.connector_config import resolve_platform_routing

    widgets = {
        "environment": "prod",
        "instance_name": "prod_mist",
        "destination_catalog": "raw_prod",
        "destination_schema": "juniper_mist",
        "metadata_catalog": "",
        "metadata_schema": "jetstream",
        "sla_target_minutes": "720",
        "owner_team": "platform",
    }
    rt = resolve_platform_routing(widgets)
    assert rt.environment == "prod"
    assert rt.sla_target_minutes == 720
    assert rt.owner_team == "platform"
    assert rt.metadata_schema == "jetstream"
