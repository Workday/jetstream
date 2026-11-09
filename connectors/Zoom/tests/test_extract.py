"""Extract-layer tests: pagination, fan-out, watermark, scope-check."""
from __future__ import annotations

from typing import Any, Dict, List
from unittest.mock import MagicMock

import pytest

from src import extract as ex


# ---------------------------------------------------------------------------
# helpers — a tiny scripted "HttpClient" that just returns canned bodies.
# ---------------------------------------------------------------------------

class ScriptedHttp:
    def __init__(self, responses_by_url):
        self.responses_by_url = {k: list(v) for k, v in responses_by_url.items()}
        self.calls: List[Dict[str, Any]] = []

    def get(self, url, *, params=None, max_attempts=None):
        self.calls.append({"url": url, "params": dict(params or {}), "max_attempts": max_attempts})
        # Use the URL as the key; serve responses in order until exhausted.
        bucket = self.responses_by_url.get(url, [])
        if not bucket:
            raise AssertionError(f"no scripted response for {url}; calls so far: {self.calls}")
        return bucket.pop(0)


# ---------------------------------------------------------------------------
# token-pagination
# ---------------------------------------------------------------------------

def test_token_pagination_stops_on_empty_token(source_cfg, object_cfg_users):
    http = ScriptedHttp({
        "https://api.zoom.us/v2/users": [
            {"users": [{"id": "a"}, {"id": "b"}], "next_page_token": "x"},
            {"users": [{"id": "c"}], "next_page_token": ""},
        ],
    })
    pages = list(ex.extract(object_cfg_users, source_cfg, http, last_watermark=None))
    assert len(pages) == 2
    assert [r["id"] for r in pages[0]["records"]] == ["a", "b"]
    assert [r["id"] for r in pages[1]["records"]] == ["c"]


def test_token_pagination_forwards_next_page_token(source_cfg, object_cfg_users):
    http = ScriptedHttp({
        "https://api.zoom.us/v2/users": [
            {"users": [{"id": 1}], "next_page_token": "T2"},
            {"users": [{"id": 2}], "next_page_token": None},
        ],
    })
    list(ex.extract(object_cfg_users, source_cfg, http, last_watermark=None))
    assert "next_page_token" not in http.calls[0]["params"]
    assert http.calls[1]["params"]["next_page_token"] == "T2"


# ---------------------------------------------------------------------------
# numbered-pagination
# ---------------------------------------------------------------------------

def test_numbered_pagination_iterates_to_page_count(source_cfg):
    from src.config import ObjectCfg
    obj = ObjectCfg(
        name="meeting_participants",
        primary_key=["meeting_id", "participant_uuid"],
        load_strategy="full",
        parent="meetings",
        required_scope="report:read:admin",
    )
    http = ScriptedHttp({
        "https://api.zoom.us/v2/report/meetings/m1/participants": [
            {"participants": [{"meeting_id": "m1", "participant_uuid": "u1"}],
             "page_count": 2, "page_number": 1},
            {"participants": [{"meeting_id": "m1", "participant_uuid": "u2"}],
             "page_count": 2, "page_number": 2},
        ],
    })
    pages = list(ex.extract(obj, source_cfg, http, last_watermark=None, parent_ids=["m1"]))
    assert len(pages) == 2
    assert http.calls[0]["params"]["page_number"] == 1
    assert http.calls[1]["params"]["page_number"] == 2


# ---------------------------------------------------------------------------
# fan-out
# ---------------------------------------------------------------------------

def test_fanout_iterates_each_parent_id(source_cfg, object_cfg_meetings):
    http = ScriptedHttp({
        "https://api.zoom.us/v2/users/u1/meetings": [
            {"meetings": [{"id": "m1"}], "next_page_token": ""},
        ],
        "https://api.zoom.us/v2/users/u2/meetings": [
            {"meetings": [{"id": "m2"}], "next_page_token": ""},
        ],
    })
    pages = list(ex.extract(object_cfg_meetings, source_cfg, http,
                            last_watermark=None, parent_ids=["u1", "u2"]))
    assert len(pages) == 2
    assert pages[0]["records"][0]["id"] == "m1"
    assert pages[1]["records"][0]["id"] == "m2"


def test_fanout_with_no_parent_ids_yields_nothing(source_cfg, object_cfg_meetings, caplog):
    http = ScriptedHttp({})
    pages = list(ex.extract(object_cfg_meetings, source_cfg, http,
                            last_watermark=None, parent_ids=[]))
    assert pages == []


# ---------------------------------------------------------------------------
# max_cursor tracking
# ---------------------------------------------------------------------------

def test_max_cursor_tracks_largest_value_in_page(source_cfg, object_cfg_meetings):
    http = ScriptedHttp({
        "https://api.zoom.us/v2/users/u1/meetings": [
            {"meetings": [
                {"id": "m1", "start_time": "2026-06-29T09:00:00Z"},
                {"id": "m2", "start_time": "2026-06-29T11:00:00Z"},
                {"id": "m3", "start_time": "2026-06-29T10:00:00Z"},
            ], "next_page_token": ""},
        ],
    })
    pages = list(ex.extract(object_cfg_meetings, source_cfg, http,
                            last_watermark=None, parent_ids=["u1"]))
    assert pages[0]["max_cursor"] == "2026-06-29T11:00:00Z"


# ---------------------------------------------------------------------------
# unknown object
# ---------------------------------------------------------------------------

def test_unknown_object_raises_with_helpful_message(source_cfg):
    from src.config import ObjectCfg
    bogus = ObjectCfg(name="not_a_real_endpoint", primary_key=["id"])
    http = ScriptedHttp({})
    with pytest.raises(KeyError) as exc:
        list(ex.extract(bogus, source_cfg, http, last_watermark=None))
    assert "not_a_real_endpoint" in str(exc.value)


# ---------------------------------------------------------------------------
# check_required_scopes
# ---------------------------------------------------------------------------

def test_scope_check_passes_when_all_required_are_granted(object_cfg_users, object_cfg_meetings):
    granted = {"user:read:admin", "meeting:read:admin", "report:read:admin"}
    assert ex.check_required_scopes([object_cfg_users, object_cfg_meetings], granted) == []


def test_scope_check_returns_missing_object_names(object_cfg_users, object_cfg_meetings):
    granted = {"user:read:admin"}  # missing meeting:read:admin
    missing = ex.check_required_scopes([object_cfg_users, object_cfg_meetings], granted)
    assert missing == ["meetings"]


def test_scope_check_ignores_objects_without_required_scope():
    from src.config import ObjectCfg
    o = ObjectCfg(name="anything", primary_key=["id"])  # no required_scope
    assert ex.check_required_scopes([o], set()) == []


def test_scope_check_ignores_optional_objects_missing_scopes():
    from src.config import ObjectCfg
    mandatory = ObjectCfg(name="users", primary_key=["id"],
                          required_scope="user:read:admin")
    optional = ObjectCfg(name="dashboard_meetings", primary_key=["uuid"],
                        required_scope="dashboard:read:list_meetings:admin",
                        optional=True)
    granted = {"user:read:admin"}
    # Optional object's missing scope must NOT appear — pipeline handles it.
    assert ex.check_required_scopes([mandatory, optional], granted) == []


# ---------------------------------------------------------------------------
# _watermark_params — server-side date-window filtering
# ---------------------------------------------------------------------------

def test_watermark_params_empty_when_endpoint_does_not_declare_them():
    from src.config import ObjectCfg
    o = ObjectCfg(name="x", primary_key=["id"])
    assert ex._watermark_params(o, "2026-06-29") == {}


def test_watermark_params_emits_date_range_when_declared():
    from src.config import ObjectCfg
    o = ObjectCfg(
        name="dashboard_meetings", primary_key=["uuid"],
        cursor_param_from="from", cursor_param_to="to", cursor_format="date",
        lookback_minutes=60,
    )
    out = ex._watermark_params(o, "2026-06-29")
    assert set(out.keys()) == {"from", "to"}
    # Both values are YYYY-MM-DD (10 chars).
    assert len(out["from"]) == 10
    assert len(out["to"]) == 10
    # `to` is today or later than `from`.
    assert out["to"] >= out["from"]


def test_watermark_params_defaults_to_yesterday_when_no_prior_watermark():
    from src.config import ObjectCfg
    o = ObjectCfg(
        name="dashboard_meetings", primary_key=["uuid"],
        cursor_param_from="from", cursor_param_to="to", cursor_format="date",
    )
    out = ex._watermark_params(o, None)
    assert "from" in out and "to" in out
    assert out["to"] >= out["from"]


# ---------------------------------------------------------------------------
# parallel fan-out
# ---------------------------------------------------------------------------

class _ThreadSafeScriptedHttp:
    """Like ScriptedHttp but safe for concurrent .get() from worker threads."""

    def __init__(self, responses_by_url):
        import threading
        self.responses_by_url = {k: list(v) for k, v in responses_by_url.items()}
        self.calls: List[Dict[str, Any]] = []
        self._lock = threading.Lock()

    def get(self, url, *, params=None, max_attempts=None):
        with self._lock:
            self.calls.append({"url": url, "params": dict(params or {}), "max_attempts": max_attempts})
            bucket = self.responses_by_url.get(url, [])
            if not bucket:
                raise AssertionError(f"no scripted response for {url}")
            return bucket.pop(0)


def test_parallel_fanout_yields_all_pages_regardless_of_completion_order(source_cfg):
    """With 8 parents in flight, every parent's records should appear in the
    yielded pages exactly once — order-agnostic."""
    from src.config import ObjectCfg
    obj = ObjectCfg(
        name="room_devices",
        primary_key=["room_id", "id"],
        load_strategy="full",
        parent="rooms",
        parent_id_column="room_id",
    )
    parent_ids = [f"pid_{i}" for i in range(20)]  # forces multi-worker path
    http = _ThreadSafeScriptedHttp({
        f"https://api.zoom.us/v2/rooms/pid_{i}/devices": [
            {"devices": [{"id": f"d{i}_a"}, {"id": f"d{i}_b"}], "next_page_token": ""},
        ]
        for i in range(20)
    })
    pages = list(ex.extract(obj, source_cfg, http, last_watermark=None, parent_ids=parent_ids))
    # Each parent produced 1 page x 2 records = 40 records total
    all_records = [r for p in pages for r in p["records"]]
    assert len(all_records) == 40
    # Each device carries the parent id (stamped by parent_id_column)
    stamped_parents = {r["room_id"] for r in all_records}
    assert stamped_parents == set(parent_ids)
    # Every scripted URL got called exactly once
    called_urls = {c["url"] for c in http.calls}
    assert called_urls == {f"https://api.zoom.us/v2/rooms/pid_{i}/devices" for i in range(20)}


def test_parallel_fanout_propagates_worker_errors(source_cfg):
    """If any worker raises (e.g. HttpError after retries exhausted), extract
    should re-raise to the caller so the object gets marked failed."""
    from src.config import ObjectCfg
    obj = ObjectCfg(
        name="room_devices",
        primary_key=["room_id", "id"],
        load_strategy="full",
        parent="rooms",
        parent_id_column="room_id",
    )
    parent_ids = [f"pid_{i}" for i in range(20)]
    # No scripted responses — every worker's .get() will raise AssertionError.
    http = _ThreadSafeScriptedHttp({})
    with pytest.raises(AssertionError):
        list(ex.extract(obj, source_cfg, http, last_watermark=None, parent_ids=parent_ids))


def test_sequential_fanout_kept_for_single_parent(source_cfg, object_cfg_meetings):
    """Single-parent fan-out shouldn't spin up a thread pool (no benefit,
    less predictable for tests). Existing sequential ScriptedHttp still works."""
    http = ScriptedHttp({
        "https://api.zoom.us/v2/users/u1/meetings": [
            {"meetings": [{"id": "m1"}], "next_page_token": ""},
        ],
    })
    pages = list(ex.extract(object_cfg_meetings, source_cfg, http,
                            last_watermark=None, parent_ids=["u1"]))
    assert len(pages) == 1
    assert pages[0]["records"][0]["id"] == "m1"


# ---------------------------------------------------------------------------
# per-object HTTP overrides (max_workers, max_retry_attempts)
# ---------------------------------------------------------------------------

def test_object_max_workers_caps_fanout_concurrency(source_cfg):
    """obj.max_workers=1 must force sequential fan-out even with many parents."""
    from src.config import ObjectCfg
    obj = ObjectCfg(
        name="room_devices",
        primary_key=["room_id", "id"],
        load_strategy="full",
        parent="rooms",
        parent_id_column="room_id",
        max_workers=1,  # override _HTTP_MAX_WORKERS=8 down to 1
    )
    parent_ids = [f"pid_{i}" for i in range(5)]
    http = ScriptedHttp({
        f"https://api.zoom.us/v2/rooms/pid_{i}/devices": [
            {"devices": [{"id": f"d{i}"}], "next_page_token": ""},
        ]
        for i in range(5)
    })
    # ScriptedHttp is not thread-safe; if this test hangs or errors, it means
    # the fan-out spawned a ThreadPoolExecutor (bug).
    pages = list(ex.extract(obj, source_cfg, http, last_watermark=None, parent_ids=parent_ids))
    assert len(pages) == 5


def test_object_max_retry_attempts_passed_to_http(source_cfg):
    """obj.max_retry_attempts must be forwarded to http.get() as `max_attempts`."""
    from src.config import ObjectCfg
    obj = ObjectCfg(
        name="dashboard_meetings",  # non-fan-out endpoint, /metrics/meetings
        primary_key=["uuid"],
        max_retry_attempts=10,
    )

    captured: List[Dict[str, Any]] = []

    class _RecordingHttp:
        def get(self, url, *, params=None, max_attempts=None):
            captured.append({"url": url, "params": dict(params or {}), "max_attempts": max_attempts})
            return {"meetings": [], "next_page_token": ""}

    list(ex.extract(obj, source_cfg, _RecordingHttp(), last_watermark=None))
    assert captured, "expected at least one http.get call"
    assert captured[0]["max_attempts"] == 10


def test_object_without_max_retry_attempts_passes_none(source_cfg, object_cfg_users):
    """Objects that don't override retry send max_attempts=None so http.get
    falls back to source.retry.max_attempts."""
    captured: List[Dict[str, Any]] = []

    class _RecordingHttp:
        def get(self, url, *, params=None, max_attempts=None):
            captured.append({"max_attempts": max_attempts})
            return {"users": [], "next_page_token": ""}

    list(ex.extract(object_cfg_users, source_cfg, _RecordingHttp(), last_watermark=None))
    assert captured[0]["max_attempts"] is None


# ---------------------------------------------------------------------------
# Zoom parent-id URL encoding (UUID fan-out)
# ---------------------------------------------------------------------------

def test_encode_zoom_parent_id_numeric_id_is_untouched():
    """Numeric ids (users.id, rooms.id) have no reserved chars → no change."""
    assert ex._encode_zoom_parent_id("93778635816") == "93778635816"


def test_encode_zoom_parent_id_simple_uuid_single_encoded():
    """Base64 UUIDs with '+' or '=' get single URL-encoded."""
    assert ex._encode_zoom_parent_id("abc+def==") == "abc%2Bdef%3D%3D"


def test_encode_zoom_parent_id_leading_slash_double_encoded():
    """Zoom rule: UUIDs starting with '/' MUST be double-URL-encoded."""
    encoded = ex._encode_zoom_parent_id("/abcDEF==")
    # first encode: %2FabcDEF%3D%3D  → second encode: %252FabcDEF%253D%253D
    assert encoded == "%252FabcDEF%253D%253D"


def test_encode_zoom_parent_id_double_slash_double_encoded():
    """Zoom rule: UUIDs containing '//' MUST be double-URL-encoded."""
    encoded = ex._encode_zoom_parent_id("abc//def==")
    assert encoded == "abc%252F%252Fdef%253D%253D"


def test_fanout_url_double_encodes_leading_slash_uuid(source_cfg):
    """End-to-end: a UUID starting with '/' arrives at http.get() with the
    URL path segment double-encoded, matching Zoom's documented requirement."""
    from src.config import ObjectCfg
    obj = ObjectCfg(
        name="dashboard_meeting_participants",
        primary_key=["meeting_uuid", "participant_uuid"],
        load_strategy="incremental_cursor",
        parent="dashboard_meetings",
        parent_id_column="meeting_uuid",
        max_workers=1,  # sequential path for deterministic assertion
    )
    parent_uuid = "/abcDEF+GHI=="
    expected_url = (
        "https://api.zoom.us/v2/metrics/meetings/%252FabcDEF%252BGHI%253D%253D/participants"
    )
    http = ScriptedHttp({
        expected_url: [
            {"participants": [{"participant_uuid": "p1"}], "next_page_token": ""},
        ],
    })
    pages = list(ex.extract(obj, source_cfg, http,
                            last_watermark=None, parent_ids=[parent_uuid]))
    # If encoding was wrong, ScriptedHttp would AssertionError on the missing URL.
    assert len(pages) == 1
    assert pages[0]["records"][0]["meeting_uuid"] == parent_uuid


def test_fanout_stamps_parent_id_column_with_raw_uuid_not_encoded(source_cfg):
    """The URL uses the encoded form, but the stamped column value must be
    the RAW parent id so it joins cleanly to dashboard_meetings.uuid."""
    from src.config import ObjectCfg
    obj = ObjectCfg(
        name="dashboard_meeting_participants",
        primary_key=["meeting_uuid", "participant_uuid"],
        load_strategy="incremental_cursor",
        parent="dashboard_meetings",
        parent_id_column="meeting_uuid",
        max_workers=1,
    )
    raw_uuid = "abc+def=="
    encoded_url = "https://api.zoom.us/v2/metrics/meetings/abc%2Bdef%3D%3D/participants"
    http = ScriptedHttp({
        encoded_url: [
            {"participants": [{"participant_uuid": "p1"}, {"participant_uuid": "p2"}],
             "next_page_token": ""},
        ],
    })
    pages = list(ex.extract(obj, source_cfg, http,
                            last_watermark=None, parent_ids=[raw_uuid]))
    for record in pages[0]["records"]:
        assert record["meeting_uuid"] == raw_uuid  # raw, not URL-encoded


def test_dashboard_meetings_endpoint_caches_uuid_as_parent_id_field():
    """Regression guard: /metrics/meetings must cache `uuid` (not numeric `id`)
    as parent_ids for downstream dashboard_meeting_participants fan-out.
    Using `id` collapses recurring-meeting occurrences to the most recent."""
    assert ex.ENDPOINTS["dashboard_meetings"]["parent_id_field"] == "uuid"


def test_meetings_endpoint_caches_uuid_as_parent_id_field():
    """Same regression guard for the primary-tier /users/{id}/meetings path,
    which parents past_meeting_participants fan-out."""
    assert ex.ENDPOINTS["meetings"]["parent_id_field"] == "uuid"
