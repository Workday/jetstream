"""UUIDv7 ids used on control-plane rows."""
from __future__ import annotations

import uuid

from src.jetstream.jetstream_id_generator import uuid7


def test_uuid7_reports_version_7():
    value = uuid.UUID(uuid7())
    assert value.version == 7
