"""Tests for src/objects.py — the code-level Zoom object catalog."""
from __future__ import annotations

import pytest

from src import objects as m


def test_default_selection_returns_all_primary_only():
    plan = m.select_objects("", "")
    names = [o.name for o in plan]
    assert names == [o.name for o in m.PRIMARY]
    assert all(not o.optional for o in plan)


def test_primary_subset_is_honored():
    plan = m.select_objects("users,rooms", "")
    assert [o.name for o in plan] == ["users", "rooms"]


def test_optional_objects_are_included_when_requested():
    plan = m.select_objects("users", "dashboard_meetings")
    names = [o.name for o in plan]
    # primary first, then optional
    assert names == ["users", "dashboard_meetings"]


def test_unknown_object_raises():
    with pytest.raises(ValueError, match="unknown Zoom object"):
        m.select_objects("not_a_thing", "")


def test_parents_precede_children_in_default_order():
    plan = m.select_objects("", "")
    order = {o.name: i for i, o in enumerate(plan)}
    for o in plan:
        if o.parent:
            assert order[o.parent] < order[o.name], (
                f"{o.parent} must come before {o.name}"
            )


# ---------------------------------------------------------------------------
# build_capability_report
# ---------------------------------------------------------------------------

def test_capability_report_marks_granted_and_wanted_correctly():
    granted = {
        "user:read:list_users:admin",
        "meeting:read:list_meetings:admin",
    }
    report = m.build_capability_report(
        granted_scopes=granted,
        enabled_primary_csv="users,meetings",
        enabled_optional_csv="",
    )
    by = {r["object"]: r for r in report["objects"]}
    assert by["users"]["granted"] is True
    assert by["users"]["wanted"] is True
    assert by["users"]["will_sync"] is True
    assert by["meetings"]["will_sync"] is True
    # rooms: not wanted (not in enabled_objects), scope not granted either
    assert by["rooms"]["wanted"] is False
    assert by["rooms"]["granted"] is False
    assert by["rooms"]["will_sync"] is False


def test_capability_report_flags_wanted_but_missing_scope():
    granted = {"user:read:list_users:admin"}
    report = m.build_capability_report(
        granted_scopes=granted,
        enabled_primary_csv="users,meetings",  # meetings wanted but no scope
        enabled_optional_csv="",
    )
    assert "meetings" in report["wanted_but_missing_scope"]
    assert "users" not in report["wanted_but_missing_scope"]
    assert report["will_sync"] == ["users"]


def test_capability_report_empty_enabled_means_all_primary():
    granted = {r.required_scope for r in m.PRIMARY}
    report = m.build_capability_report(
        granted_scopes=granted,
        enabled_primary_csv="",
        enabled_optional_csv="",
    )
    # Every primary object is wanted; every one has scope; all will sync.
    primary_names = {o.name for o in m.PRIMARY}
    will = set(report["will_sync"])
    assert primary_names == will
    # No optional objects included when csv is empty.
    for r in report["objects"]:
        if r["tier"] == "optional":
            assert r["wanted"] is False
            assert r["will_sync"] is False


def test_format_capability_report_renders_table():
    granted = {"user:read:list_users:admin"}
    report = m.build_capability_report(granted, "users", "")
    text = m.format_capability_report(report)
    assert "Jetstream Zoom" in text
    assert "users" in text
    assert "will_sync" in text.lower() or "will sync" in text.lower()
