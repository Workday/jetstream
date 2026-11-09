"""Defaults for catalogs, volumes, and table selection."""
from __future__ import annotations

from src.config import resolve_platform_routing, resolve_run_config


def test_dev_defaults_use_generic_names():
    routing = resolve_platform_routing({"environment": "dev"})
    assert routing.destination_catalog == "raw_dev"
    assert routing.destination_schema == "servicenow"
    assert routing.attachment_volume_base == "/Volumes/raw_dev/servicenow/attachments"
    assert routing.instance_name == "servicenow_dev"
    assert routing.metadata_schema == "jetstream"


def test_prod_catalog_fallback():
    routing = resolve_platform_routing({"environment": "prod", "destination_schema": "servicenow"})
    assert routing.destination_catalog == "raw_prod"
    assert routing.attachment_volume_base == "/Volumes/raw_prod/servicenow/attachments"


def test_explicit_volume_and_catalog_win():
    routing = resolve_platform_routing({
        "environment": "dev",
        "destination_catalog": "raw_dev",
        "destination_schema": "servicenow",
        "attachment_volume_base": "/Volumes/raw_dev/servicenow/attachments/",
        "instance_name": "servicenow",
    })
    assert routing.destination_catalog == "raw_dev"
    assert routing.attachment_volume_base == "/Volumes/raw_dev/servicenow/attachments"
    assert routing.instance_name == "servicenow"


def test_blank_enabled_objects_defaults_to_incident():
    cfg = resolve_run_config({
        "environment": "dev",
        "destination_catalog": "raw_dev",
        "secret_scope": "jetstream_servicenow",
        "enabled_objects": "",
    })
    assert [obj.name for obj in cfg.objects] == ["incident"]
    assert cfg.objects[0].sn_table == "incident"
    assert cfg.objects[0].attachment_meta_table == "incident_attachments"
    assert cfg.objects[0].journals_meta_table == "incident_journals"


def test_change_request_keeps_widget_keys():
    cfg = resolve_run_config({
        "environment": "dev",
        "destination_catalog": "raw_dev",
        "enabled_objects": "change_request",
        "primary_key": "sys_id,number",
        "watermark_column": "sys_created_on",
        "sync_target": "journals",
    })
    obj = cfg.objects[0]
    assert obj.name == "change_request"
    assert obj.primary_key_columns == ("sys_id", "number")
    assert obj.watermark_columns == ("sys_created_on",)
    assert cfg.sync_target == "journals"
