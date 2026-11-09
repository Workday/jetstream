"""Tests for UUIDv7 helper."""
from __future__ import annotations

import uuid

from src.jetstream.uuid_generator import uuid7


def test_uuid7_returns_valid_uuid():
    val = uuid7()
    parsed = uuid.UUID(val)
    assert parsed.version == 7


def test_uuid7_values_are_monotonic_ish():
    # UUIDv7 ids generated close together should sort together via the ts prefix.
    a = uuid7()
    b = uuid7()
    # First 48 bits are unix_ts_ms — compare hex prefixes.
    assert a[:8] <= b[:8]
