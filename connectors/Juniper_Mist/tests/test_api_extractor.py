"""Tests for src/api_extractor.py.

These tests focus on:
  - page_limit pagination (stop when < limit)
  - search_after pagination (stop on missing `next`)
  - single_object pagination
  - count pagination
  - field normalization (_id→id remap, derived fields, _jsonify)
  - watermark params injection
  - fan-out for single and double parents
  - parent_cache_entries for maps (double fan-out keys)
"""
from __future__ import annotations

import json
from typing import Any

import pytest

from tests.conftest import StubResponse, StubSession


def _build_http_client(stub_session, source_cfg):
    from src.http_client import HttpClient
    return HttpClient(source_cfg, token_provider=lambda: "tok", session=stub_session)


ORG_ID = "44a1aa85-fff5-498f-91fa-6cd89632176c"
SITE_ID = "site-001"


# ---------------------------------------------------------------------------
# page_limit
# ---------------------------------------------------------------------------

def test_page_limit_single_page(stub_session, source_cfg):
    from src.api_extractor import ENDPOINTS, extract
    from src.connector_config import ObjectCfg

    stub_session.add(
        lambda m, url, p: "/sites" in url and p.get("page") == 1,
        StubResponse(200, json_body=[{"id": "s1"}, {"id": "s2"}]),
    )
    obj = ObjectCfg(name="sites", primary_key=["id"], load_strategy="full")
    http = _build_http_client(stub_session, source_cfg)
    pages = list(extract(obj, source_cfg, http, last_watermark=None))
    assert len(pages) == 1
    assert len(pages[0]["records"]) == 2
    assert pages[0]["records"][0]["id"] == "s1"


def test_page_limit_multiple_pages(stub_session, source_cfg):
    from src.api_extractor import extract
    from src.connector_config import ObjectCfg

    stub_session.add(
        lambda m, url, p: "/sites" in url and p.get("page") == 1,
        StubResponse(200, json_body=[{"id": f"s{i}"} for i in range(1000)]),
    )
    stub_session.add(
        lambda m, url, p: "/sites" in url and p.get("page") == 2,
        StubResponse(200, json_body=[{"id": "s1000"}, {"id": "s1001"}]),
    )
    obj = ObjectCfg(name="sites", primary_key=["id"], load_strategy="full")
    http = _build_http_client(stub_session, source_cfg)
    pages = list(extract(obj, source_cfg, http, last_watermark=None))
    assert len(pages) == 2
    assert len(pages[0]["records"]) == 1000
    assert len(pages[1]["records"]) == 2


def test_page_limit_empty_response(stub_session, source_cfg):
    from src.api_extractor import extract
    from src.connector_config import ObjectCfg

    stub_session.add(lambda m, url, p: True, StubResponse(200, json_body=[]))
    obj = ObjectCfg(name="sites", primary_key=["id"], load_strategy="full")
    http = _build_http_client(stub_session, source_cfg)
    pages = list(extract(obj, source_cfg, http, last_watermark=None))
    assert len(pages) == 1
    assert pages[0]["records"] == []


# ---------------------------------------------------------------------------
# single_object
# ---------------------------------------------------------------------------

def test_single_object_returns_one_page(stub_session, source_cfg):
    from src.api_extractor import extract
    from src.connector_config import ObjectCfg

    stub_session.add(
        lambda m, url, p: "/orgs/" in url and not url.endswith("/sites"),
        StubResponse(200, json_body={"id": ORG_ID, "name": "My Org"}),
    )
    obj = ObjectCfg(name="org", primary_key=["id"], load_strategy="full")
    http = _build_http_client(stub_session, source_cfg)
    pages = list(extract(obj, source_cfg, http, last_watermark=None))
    assert len(pages) == 1
    assert pages[0]["records"][0]["name"] == "My Org"


# ---------------------------------------------------------------------------
# search_after
# ---------------------------------------------------------------------------

def test_search_after_stops_on_empty_next(stub_session, source_cfg):
    from src.api_extractor import extract
    from src.connector_config import ObjectCfg

    stub_session.add(
        lambda m, url, p: "visits/search" in url and p.get("page") is None and p.get("search_after") is None,
        StubResponse(200, json_body={
            "results": [{"timestamp": 1.0, "mac": "aa", "enter": 1.0, "site_id": SITE_ID}],
            "next": "?search_after=[100]&start=0&end=999",
        }),
    )
    stub_session.add(
        lambda m, url, p: "visits/search" in url and p.get("search_after") is not None,
        StubResponse(200, json_body={"results": [], "next": ""}),
    )

    obj = ObjectCfg(
        name="zone_visits",
        primary_key=["site_id", "mac", "enter_epoch"],
        load_strategy="incremental_cursor",
        cursor_field="timestamp",
        parent="sites_filtered",
        parent_id_column="site_id",
        cursor_param_from="start",
        cursor_param_to="end",
    )
    http = _build_http_client(stub_session, source_cfg)
    # Use user_type=client to match the first user_type in the loop
    pages = list(extract(obj, source_cfg, http, last_watermark=None, parent_ids=[SITE_ID]))
    # 3 user_types × 2 pages each, but second page has no records — stops after first page per type
    # total: 3 user_types × 2 iterations = up to 6 pages; second page of each is empty so stops
    # First user_type: page1 (records), page2 (empty=stop), then next user_type...
    all_records = [r for p in pages for r in p["records"]]
    # 3 user_types × 1 record each (first pages with results)
    assert len(all_records) == 3


def test_search_after_derives_enter_epoch(stub_session, source_cfg):
    from src.api_extractor import extract
    from src.connector_config import ObjectCfg

    stub_session.add(
        lambda m, url, p: "visits/search" in url,
        StubResponse(200, json_body={
            "results": [{"timestamp": 1000.5, "mac": "bb", "enter": 1000.5, "site_id": SITE_ID}],
            "next": None,
        }),
    )
    obj = ObjectCfg(
        name="zone_visits",
        primary_key=["site_id", "mac", "enter_epoch"],
        load_strategy="incremental_cursor",
        cursor_field="timestamp",
        parent="sites_filtered",
        parent_id_column="site_id",
        cursor_param_from="start",
        cursor_param_to="end",
    )
    http = _build_http_client(stub_session, source_cfg)
    pages = list(extract(obj, source_cfg, http, last_watermark=None, parent_ids=[SITE_ID]))
    records_with_enter_epoch = [r for p in pages for r in p["records"] if r.get("enter_epoch")]
    assert records_with_enter_epoch, "No records with enter_epoch found"
    # enter_epoch = str(int(1000.5 * 1000)) = "1000500"
    assert records_with_enter_epoch[0]["enter_epoch"] == "1000500"


# ---------------------------------------------------------------------------
# Field normalization
# ---------------------------------------------------------------------------

def test_id_remap_applied_for_device_stats(stub_session, source_cfg):
    from src.api_extractor import extract
    from src.connector_config import ObjectCfg

    stub_session.add(
        lambda m, url, p: "stats/devices" in url,
        StubResponse(200, json_body=[{"_id": "device-1", "mac": "aabbcc"}]),
    )
    obj = ObjectCfg(name="device_stats", primary_key=["id"], load_strategy="full")
    http = _build_http_client(stub_session, source_cfg)
    pages = list(extract(obj, source_cfg, http, last_watermark=None))
    assert len(pages[0]["records"]) == 1
    rec = pages[0]["records"][0]
    assert "id" in rec
    assert rec["id"] == "device-1"
    assert "_id" not in rec


def test_jsonify_serializes_nested_dicts(stub_session, source_cfg):
    from src.api_extractor import extract
    from src.connector_config import ObjectCfg

    stub_session.add(
        lambda m, url, p: "/setting" in url,
        StubResponse(200, json_body={"site_id": SITE_ID, "occupancy": {"num_clients_min": 5}, "nested_list": [1, 2]}),
    )
    obj = ObjectCfg(
        name="site_settings",
        primary_key=["site_id"],
        load_strategy="full",
        parent="sites",
        parent_id_column="site_id",
    )
    http = _build_http_client(stub_session, source_cfg)
    pages = list(extract(obj, source_cfg, http, last_watermark=None, parent_ids=[SITE_ID]))
    rec = pages[0]["records"][0]
    assert isinstance(rec["occupancy"], str), "nested dict should be serialised to JSON string"
    assert isinstance(rec["nested_list"], str), "nested list should be serialised to JSON string"
    # Confirm the JSON string is valid JSON
    parsed = json.loads(rec["occupancy"])
    assert parsed == {"num_clients_min": 5}


# ---------------------------------------------------------------------------
# Fan-out: no parent_ids warns and returns empty
# ---------------------------------------------------------------------------

def test_fanout_no_parent_ids_returns_empty(stub_session, source_cfg):
    from src.api_extractor import extract
    from src.connector_config import ObjectCfg

    obj = ObjectCfg(
        name="maps",
        primary_key=["id"],
        load_strategy="full",
        parent="sites",
        parent_id_column="site_id",
    )
    http = _build_http_client(stub_session, source_cfg)
    pages = list(extract(obj, source_cfg, http, last_watermark=None, parent_ids=[]))
    assert pages == []


# ---------------------------------------------------------------------------
# parent_cache_entries
# ---------------------------------------------------------------------------

def test_parent_cache_entries_maps_returns_dicts():
    from src.api_extractor import parent_cache_entries

    records = [
        {"id": "map-1", "site_id": "site-1"},
        {"id": "map-2", "site_id": "site-1"},
    ]
    entries = parent_cache_entries("maps", records)
    assert len(entries) == 2
    assert entries[0] == {"site_id": "site-1", "map_id": "map-1"}
    assert entries[1] == {"site_id": "site-1", "map_id": "map-2"}


def test_parent_cache_entries_sites_returns_strings():
    from src.api_extractor import parent_cache_entries

    records = [{"id": "site-1"}, {"id": "site-2"}]
    entries = parent_cache_entries("sites", records)
    assert entries == ["site-1", "site-2"]


def test_parent_cache_entries_skips_missing_keys():
    from src.api_extractor import parent_cache_entries

    records = [
        {"id": "map-1", "site_id": "site-1"},
        {"id": "map-2"},  # missing site_id
    ]
    entries = parent_cache_entries("maps", records)
    assert len(entries) == 1
    assert entries[0]["map_id"] == "map-1"


# ---------------------------------------------------------------------------
# Double fan-out: map_clients
# ---------------------------------------------------------------------------

def test_double_fanout_map_clients(stub_session, source_cfg):
    from src.api_extractor import extract
    from src.connector_config import ObjectCfg

    stub_session.add(
        lambda m, url, p: "/stats/maps/" in url and "/clients" in url,
        StubResponse(200, json_body=[{"mac": "aabbcc", "x": 10, "y": 20}]),
    )
    obj = ObjectCfg(
        name="map_clients",
        primary_key=["site_id", "map_id", "mac"],
        load_strategy="full",
        parent="maps",
        parent_id_column="map_id",
        optional=True,
    )
    parent_ids = [{"site_id": "site-1", "map_id": "map-1"}]
    http = _build_http_client(stub_session, source_cfg)
    pages = list(extract(obj, source_cfg, http, last_watermark=None, parent_ids=parent_ids))
    assert len(pages) == 1
    rec = pages[0]["records"][0]
    assert rec.get("site_id") == "site-1"
    assert rec.get("map_id") == "map-1"
    assert rec.get("mac") == "aabbcc"


# ---------------------------------------------------------------------------
# Watermark params
# ---------------------------------------------------------------------------

def test_watermark_params_with_prior_cursor():
    from src.api_extractor import _watermark_params
    from src.connector_config import ObjectCfg, SourceCfg, RetryCfg

    obj = ObjectCfg(
        name="zone_visits",
        primary_key=["site_id", "mac", "enter_epoch"],
        load_strategy="incremental_cursor",
        cursor_field="timestamp",
        lookback_minutes=30,
        parent="sites_filtered",
        parent_id_column="site_id",
        cursor_param_from="start",
        cursor_param_to="end",
    )
    src = SourceCfg(
        base_url="https://api.mist.com/api/v1",
        org_id="org-1",
        api_token="tok",
        rate_limit_qps=100000,
        retry=RetryCfg(),
        lookback_days=7,
    )
    cursor = "2026-08-01T12:00:00Z"
    params = _watermark_params(obj, cursor, src)
    assert "start" in params
    assert "end" in params
    # start should be less than end
    assert params["start"] < params["end"]


def test_watermark_params_no_cursor_uses_lookback_days():
    from src.api_extractor import _watermark_params
    from src.connector_config import ObjectCfg, SourceCfg, RetryCfg
    import time

    obj = ObjectCfg(
        name="clients_search",
        primary_key=["site_id", "mac", "timestamp"],
        load_strategy="incremental_cursor",
        cursor_field="timestamp",
        lookback_minutes=60,
        parent="sites_filtered",
        parent_id_column="site_id",
        cursor_param_from="start",
        cursor_param_to="end",
    )
    src = SourceCfg(
        base_url="https://api.mist.com/api/v1",
        org_id="org-1",
        api_token="tok",
        rate_limit_qps=100000,
        retry=RetryCfg(),
        lookback_days=7,
    )
    now = int(time.time())
    params = _watermark_params(obj, None, src)
    expected_start = now - 7 * 86400
    assert abs(params["start"] - expected_start) < 5  # within 5 seconds
    assert params["end"] >= params["start"]


def test_watermark_params_full_object_returns_empty():
    from src.api_extractor import _watermark_params
    from src.connector_config import ObjectCfg, SourceCfg, RetryCfg

    obj = ObjectCfg(name="org", primary_key=["id"], load_strategy="full")
    src = SourceCfg(
        base_url="https://api.mist.com/api/v1",
        org_id="org-1",
        api_token="tok",
        rate_limit_qps=100000,
        retry=RetryCfg(),
        lookback_days=7,
    )
    assert _watermark_params(obj, None, src) == {}


# ---------------------------------------------------------------------------
# Unknown object raises KeyError
# ---------------------------------------------------------------------------

def test_extract_unknown_object_raises(stub_session, source_cfg):
    from src.api_extractor import extract
    from src.connector_config import ObjectCfg

    obj = ObjectCfg(name="nonexistent_object_xyz", primary_key=["id"], load_strategy="full")
    http = _build_http_client(stub_session, source_cfg)
    with pytest.raises(KeyError, match="nonexistent_object_xyz"):
        list(extract(obj, source_cfg, http, last_watermark=None))
