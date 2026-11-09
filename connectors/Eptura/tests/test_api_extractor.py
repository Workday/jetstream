"""Pagination against the Eptura limit/startAt convention."""
from __future__ import annotations

import pytest

from src.api_extractor import describe_floor_coverage, fetch_pages, unwrap_payload


class FakeHttp:
    """Serves pre-canned pages and records the params it was called with."""

    def __init__(self, pages):
        self._pages = list(pages)
        self.calls = []
        self.last_response_bytes = 10

    def get_json(self, url, *, params=None):
        self.calls.append({"url": url, **(dict(params) if params else {})})
        return self._pages.pop(0) if self._pages else {"response": []}


def test_single_page_stops_immediately(source_cfg, rooms):
    http = FakeHttp([{"response": [{"id": 1}, {"id": 2}], "total": 2}])
    pages = list(fetch_pages(rooms, source_cfg, http))
    assert [r for page, _ in pages for r in page] == [{"id": 1}, {"id": 2}]
    assert len(http.calls) == 1


def test_startat_advances_by_records_returned(source_cfg, rooms):
    # page_size is 2 in the fixture.
    http = FakeHttp([
        {"response": [{"id": 1}, {"id": 2}], "total": 3},
        {"response": [{"id": 3}], "total": 3},
    ])
    pages = list(fetch_pages(rooms, source_cfg, http))
    assert sum(len(page) for page, _ in pages) == 3
    assert [c["startAt"] for c in http.calls] == [0, 2]
    assert all(c["limit"] == 2 for c in http.calls)


def test_stops_on_partial_page_without_total(source_cfg, rooms):
    http = FakeHttp([{"response": [{"id": 1}]}])
    pages = list(fetch_pages(rooms, source_cfg, http))
    assert len(pages) == 1
    assert len(http.calls) == 1


def test_stops_when_total_reached(source_cfg, rooms):
    http = FakeHttp([
        {"response": [{"id": 1}, {"id": 2}], "total": 4},
        {"response": [{"id": 3}, {"id": 4}], "total": 4},
    ])
    pages = list(fetch_pages(rooms, source_cfg, http))
    assert sum(len(page) for page, _ in pages) == 4
    assert len(http.calls) == 2


def test_empty_first_page_yields_nothing(source_cfg, rooms):
    http = FakeHttp([{"response": [], "total": 0}])
    assert list(fetch_pages(rooms, source_cfg, http)) == []


def test_request_url_is_the_entity_name(source_cfg, rooms):
    seen = {}

    class UrlCapturingHttp(FakeHttp):
        def get_json(self, url, *, params=None):
            seen["url"] = url
            return super().get_json(url, params=params)

    list(fetch_pages(rooms, source_cfg, UrlCapturingHttp([{"response": [{"id": 1}]}])))
    assert seen["url"] == "https://example.ioffice.com/external/api/rest/v2/rooms"


def test_fetch_detail_replaces_list_rows_with_item_payload(source_cfg):
    from src.connector_config import ObjectCfg

    obj = ObjectCfg(name="neighborhoods", fetch_detail=True)
    http = FakeHttp([
        {"response": [{"id": 7, "name": "N7"}], "total": 1},
        {"id": 7, "name": "N7", "floor": {"id": 42, "name": "3"}},
    ])
    pages = list(fetch_pages(obj, source_cfg, http))
    assert pages[0][0] == [{"id": 7, "name": "N7", "floor": {"id": 42, "name": "3"}}]
    assert http.calls[0]["limit"] == 2
    # second call is the unparameterised detail GET — FakeHttp still records params
    assert len(http.calls) == 2


def test_fetch_detail_keeps_list_row_when_item_call_fails(source_cfg):
    from src.connector_config import ObjectCfg
    from src.http_client import HttpError

    obj = ObjectCfg(name="neighborhoods", fetch_detail=True)

    class FailingDetail(FakeHttp):
        def get_json(self, url, *, params=None):
            self.calls.append(dict(params) if params else {"url": url})
            if "/neighborhoods/7" in url:
                raise HttpError(404, "missing", url)
            return self._pages.pop(0)

    http = FailingDetail([{"response": [{"id": 7, "name": "N7"}]}])
    pages = list(fetch_pages(obj, source_cfg, http))
    assert pages[0][0] == [{"id": 7, "name": "N7"}]


def test_fetch_detail_unwraps_response_envelope(source_cfg):
    from src.connector_config import ObjectCfg

    obj = ObjectCfg(name="neighborhoods", fetch_detail=True)
    http = FakeHttp([
        {"response": [{"id": 1}]},
        {"response": {"id": 1, "floor": {"id": 9}}},
    ])
    pages = list(fetch_pages(obj, source_cfg, http))
    assert pages[0][0][0]["floor"]["id"] == 9


def test_fetch_detail_unwraps_list_inside_response_envelope(source_cfg):
    """Item GET uses the same {response: [...]} wrapper as the collection."""
    from src.connector_config import ObjectCfg

    obj = ObjectCfg(name="neighborhoods", fetch_detail=True)
    http = FakeHttp([
        {"response": [{"id": 338, "name": "NYC 350"}]},
        {"response": [{"id": 338, "name": "NYC 350", "floor": {"id": 12}}]},
    ])
    pages = list(fetch_pages(obj, source_cfg, http))
    assert pages[0][0][0]["floor"]["id"] == 12


def test_page_bytes_are_reported(source_cfg, rooms):
    http = FakeHttp([{"response": [{"id": 1}]}])
    pages = list(fetch_pages(rooms, source_cfg, http))
    assert pages[0][1] == 10


def test_fetch_detail_bytes_include_item_calls(source_cfg):
    from src.connector_config import ObjectCfg

    obj = ObjectCfg(name="neighborhoods", fetch_detail=True)
    http = FakeHttp([
        {"response": [{"id": 1}, {"id": 2}]},
        {"response": [{"id": 1, "floor": {"id": 9}}]},
        {"response": [{"id": 2, "floor": {"id": 9}}]},
    ])
    pages = list(fetch_pages(obj, source_cfg, http))
    # list page (10) + two item GETs (10 each)
    assert pages[0][1] == 30


def test_describe_floor_coverage_includes_a_sample():
    assert describe_floor_coverage([
        {"id": 338, "floor": {"id": 12}},
        {"id": 339},
    ]) == "1/2 rows have floor.id (sample id=338 floor.id=12)"


def test_fetch_detail_keeps_list_row_when_unwrap_is_empty(source_cfg):
    from src.connector_config import ObjectCfg

    obj = ObjectCfg(name="neighborhoods", fetch_detail=True)
    http = FakeHttp([
        {"response": [{"id": 7, "name": "N7"}]},
        {"response": []},
    ])
    pages = list(fetch_pages(obj, source_cfg, http))
    assert pages[0][0] == [{"id": 7, "name": "N7"}]


@pytest.mark.parametrize("payload,expected", [
    ([{"id": 1}], ([{"id": 1}], None)),
    ({"response": [{"id": 1}], "total": 5}, ([{"id": 1}], 5)),
    ({"response": []}, ([], None)),
    (None, ([], None)),
])
def test_unwrap_handles_both_response_shapes(payload, expected):
    assert unwrap_payload(payload) == expected
