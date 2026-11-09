"""Tests for src/api_objects.py."""
from __future__ import annotations

import pytest

from src.api_objects import (
    ALL_BY_NAME,
    OPTIONAL,
    PRIMARY,
    build_capability_report,
    format_capability_report,
    select_objects,
)


def test_all_by_name_covers_every_object():
    all_objects = {*[o.name for o in PRIMARY], *[o.name for o in OPTIONAL]}
    assert set(ALL_BY_NAME.keys()) == all_objects


def test_sitegroups_is_primary():
    """sitegroups was moved from OPTIONAL to PRIMARY per user decision."""
    primary_names = [o.name for o in PRIMARY]
    assert "sitegroups" in primary_names


def test_sites_in_primary():
    assert any(o.name == "sites" for o in PRIMARY)


def test_map_clients_is_optional():
    optional_names = [o.name for o in OPTIONAL]
    assert "map_clients" in optional_names


def test_unconnected_clients_is_optional():
    optional_names = [o.name for o in OPTIONAL]
    assert "unconnected_clients" in optional_names


def test_zone_visits_pk_has_enter_epoch():
    obj = ALL_BY_NAME["zone_visits"]
    assert "enter_epoch" in obj.primary_key
    assert "site_id" in obj.primary_key
    assert "mac" in obj.primary_key


def test_zone_visits_by_zone_pk_has_scope_id():
    obj = ALL_BY_NAME["zone_visits_by_zone"]
    assert "scope_id" in obj.primary_key


def test_sites_filtered_parent_for_zone_visits():
    obj = ALL_BY_NAME["zone_visits"]
    assert obj.parent == "sites_filtered"


def test_maps_has_sites_parent():
    obj = ALL_BY_NAME["maps"]
    assert obj.parent == "sites"


def test_map_clients_has_maps_parent():
    obj = ALL_BY_NAME["map_clients"]
    assert obj.parent == "maps"


def test_site_settings_pk_is_site_id():
    obj = ALL_BY_NAME["site_settings"]
    assert obj.primary_key == ["site_id"]


def test_device_stats_pk_is_id():
    obj = ALL_BY_NAME["device_stats"]
    assert "id" in obj.primary_key


def test_select_objects_empty_returns_all_objects():
    """Empty enabled_objects returns all primary + optional objects."""
    plan = select_objects("")
    plan_names = {o.name for o in plan}
    all_names = {o.name for o in (*PRIMARY, *OPTIONAL)}
    assert plan_names == all_names


def test_select_objects_subset():
    plan = select_objects("org,sites,inventory")
    names = {o.name for o in plan}
    assert names == {"org", "sites", "inventory"}


def test_select_objects_can_include_optional():
    plan = select_objects("org,map_clients")
    names = {o.name for o in plan}
    assert "map_clients" in names


def test_select_objects_preserves_dependency_order():
    plan = select_objects("maps,sites")
    names = [o.name for o in plan]
    assert names.index("sites") < names.index("maps")


def test_select_objects_unknown_raises():
    with pytest.raises(ValueError, match="unknown Juniper Mist object"):
        select_objects("org,nonexistent_object")


def test_select_objects_whitespace_stripped():
    plan = select_objects(" org , sites ")
    names = {o.name for o in plan}
    assert names == {"org", "sites"}


def test_build_capability_report_empty_csv():
    """Empty csv → all objects (primary + optional) are synced by default."""
    report = build_capability_report("")
    will_sync = set(report["will_sync"])
    all_names = {o.name for o in (*PRIMARY, *OPTIONAL)}
    assert will_sync == all_names


def test_build_capability_report_custom():
    report = build_capability_report("org,sites")
    assert set(report["will_sync"]) == {"org", "sites"}


def test_format_capability_report_contains_headers():
    report = build_capability_report("")
    rendered = format_capability_report(report)
    assert "object" in rendered.lower()
    assert "tier" in rendered.lower()
    assert "will_sync" in rendered.lower() or "YES" in rendered


def test_primary_objects_not_optional():
    for obj in PRIMARY:
        assert not obj.optional, f"{obj.name} is in PRIMARY but has optional=True"


def test_optional_objects_are_optional():
    for obj in OPTIONAL:
        assert obj.optional, f"{obj.name} is in OPTIONAL but has optional=False"


# ---------------------------------------------------------------------------
# Snapshot strategy assertions
# ---------------------------------------------------------------------------

_SNAPSHOT_OBJECTS = {"device_stats", "clients", "map_clients", "unconnected_clients"}


def test_snapshot_objects_have_correct_strategy():
    """The four live-snapshot objects must use load_strategy='snapshot'."""
    for name in _SNAPSHOT_OBJECTS:
        obj = ALL_BY_NAME[name]
        assert obj.load_strategy == "snapshot", (
            f"{name} expected load_strategy='snapshot', got {obj.load_strategy!r}"
        )


def test_snapshot_objects_have_no_cursor_fields():
    """Snapshot objects must not declare cursor fields (they carry no watermark)."""
    for name in _SNAPSHOT_OBJECTS:
        obj = ALL_BY_NAME[name]
        assert obj.cursor_field is None, f"{name}.cursor_field should be None"
        assert obj.cursor_param_from is None, f"{name}.cursor_param_from should be None"
        assert obj.cursor_param_to is None, f"{name}.cursor_param_to should be None"


def test_full_load_objects_are_not_snapshot():
    """Configuration/reference objects must remain on full-load (upsert + tombstone)."""
    _FULL_LOAD_OBJECTS = {"org", "sites", "inventory", "maps", "zones", "site_settings"}
    for name in _FULL_LOAD_OBJECTS:
        obj = ALL_BY_NAME[name]
        assert obj.load_strategy == "full", (
            f"{name} expected load_strategy='full', got {obj.load_strategy!r}"
        )
