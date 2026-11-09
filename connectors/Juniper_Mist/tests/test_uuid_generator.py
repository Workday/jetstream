"""Tests for src/jetstream/uuid_generator.py."""
from __future__ import annotations

import re
import uuid

from src.jetstream.uuid_generator import uuid7

_UUID_RE = re.compile(
    r"^[0-9a-f]{8}-[0-9a-f]{4}-7[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$"
)


def test_uuid7_format():
    result = uuid7()
    assert isinstance(result, str), "uuid7() must return a string"
    assert _UUID_RE.match(result), f"uuid7() output {result!r} is not canonical UUID format"


def test_uuid7_version_7():
    result = uuid7()
    u = uuid.UUID(result)
    assert u.version == 7


def test_uuid7_timestamp_non_decreasing():
    """The 48-bit millisecond timestamp embedded in UUIDv7 must be non-decreasing.

    We can't guarantee strict ordering of the full UUID string within the same
    millisecond (the random suffix may sort out of generation order), but the
    timestamp prefix must never go backwards.
    """
    import time
    ids = []
    for _ in range(50):
        ids.append(uuid7())
        time.sleep(0.001)  # ensure different milliseconds
    # Extract the 48-bit timestamp from each UUID (first 12 hex chars = 48 bits)
    timestamps = [int(uid.replace("-", "")[:12], 16) for uid in ids]
    for i in range(1, len(timestamps)):
        assert timestamps[i] >= timestamps[i - 1], (
            f"timestamp went backwards at index {i}: {timestamps[i-1]} -> {timestamps[i]}"
        )


def test_uuid7_unique():
    ids = {uuid7() for _ in range(200)}
    assert len(ids) == 200, "uuid7() must produce unique values"
