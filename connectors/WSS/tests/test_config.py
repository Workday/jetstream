"""Generic catalog, volume, and object defaults."""
from __future__ import annotations

from src.config import resolve_platform_routing, resolve_run_config


def test_dev_defaults_use_generic_names():
    routing = resolve_platform_routing({"environment": "dev"})
    assert routing.destination_catalog == "raw_dev"
    assert routing.destination_schema == "wss"
    assert routing.metadata_catalog == "metadata_dev"
    assert routing.metadata_schema == "jetstream"
    assert routing.attachment_volume_path == "/Volumes/raw_dev/wss/attachments"
    assert routing.raw_json_volume_base == "/Volumes/raw_dev/wss/api_responses"
    assert routing.instance_name == "wss"
    assert routing.owner_team == ""


def test_prod_metadata_catalog():
    routing = resolve_platform_routing({"environment": "prod"})
    assert routing.destination_catalog == "raw_prod"
    assert routing.metadata_catalog == "metadata_prod"


def test_all_selects_catalog_objects():
    cfg = resolve_run_config({"environment": "dev", "enabled_objects": "all", "secret_scope": "jetstream_wss"})
    names = [obj.object_name for obj in cfg.objects]
    assert "contracts" in names
    assert "projects" in names
    assert "all" not in names


def test_named_object_keeps_endpoint():
    cfg = resolve_run_config({
        "environment": "dev",
        "destination_catalog": "raw_dev",
        "enabled_objects": "contracts",
        "secret_scope": "jetstream_wss",
    })
    assert len(cfg.objects) == 1
    obj = cfg.objects[0]
    assert obj.object_name == "contracts"
    assert obj.endpoint == "/services/contracts/v1/contracts"
    assert obj.destination_catalog == "raw_dev"
