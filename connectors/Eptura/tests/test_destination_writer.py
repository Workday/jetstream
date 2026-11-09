"""Writer helpers that don't need a Spark session.

The MERGE / tombstone paths are only exercisable inside Databricks; what is
testable here is the record preparation that feeds them.
"""
from __future__ import annotations

import json

from src.destination_writer import METADATA_COLUMNS, _dedupe_by_pk, _strip_reserved


def test_dedupe_keeps_the_last_record_per_key():
    records = [{"id": 1, "v": "old"}, {"id": 2, "v": "x"}, {"id": 1, "v": "new"}]
    result = _dedupe_by_pk(records, ["id"])
    assert result == [{"id": 1, "v": "new"}, {"id": 2, "v": "x"}]


def test_dedupe_preserves_first_seen_order():
    records = [{"id": 3}, {"id": 1}, {"id": 2}, {"id": 1}]
    assert [r["id"] for r in _dedupe_by_pk(records, ["id"])] == [3, 1, 2]


def test_dedupe_drops_records_missing_the_key():
    records = [{"id": 1}, {"name": "no id"}, {"id": 2}]
    assert _dedupe_by_pk(records, ["id"]) == [{"id": 1}, {"id": 2}]


def test_reserved_system_columns_are_stripped_from_source_records():
    record = {"id": 1, "_jetstream_deleted": True, "_jetstream_synced": "2020-01-01"}
    assert _strip_reserved(record) == {"id": 1}
    assert set(METADATA_COLUMNS) == {"_jetstream_synced", "_jetstream_deleted"}


def test_nested_values_survive_json_serialisation():
    """The writer hands JSON lines to Spark precisely so structures like
    rooms.floor.building.address stay navigable downstream."""
    record = {
        "id": 1,
        "floor": {"id": 7, "building": {"name": "Toronto", "address": {"city": "Toronto"}}},
        "costCenters": [{"name": "Direct Lease", "category": {"name": "Office Type"}}],
    }
    round_tripped = json.loads(json.dumps(_strip_reserved(record), default=str))
    assert round_tripped["floor"]["building"]["address"]["city"] == "Toronto"
    assert round_tripped["costCenters"][0]["category"]["name"] == "Office Type"
