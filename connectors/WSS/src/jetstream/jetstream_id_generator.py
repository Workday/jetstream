"""UUIDv7 generator (spec §3.4)."""
from __future__ import annotations

import os
import time
import uuid

def _uuid7_fallback() -> uuid.UUID:
    ts_ms = int(time.time() * 1000) & ((1 << 48) - 1)
    rand_a = int.from_bytes(os.urandom(2), "big") & 0x0FFF
    rand_b = int.from_bytes(os.urandom(8), "big") & ((1 << 62) - 1)
    val = ((ts_ms << 80) | (0x7 << 76) | (rand_a << 64) | (0b10 << 62) | rand_b)
    return uuid.UUID(int=val)

_native_uuid7 = getattr(uuid, "uuid7", None)

def uuid7() -> str:
    if _native_uuid7 is not None:
        return str(_native_uuid7())
    return str(_uuid7_fallback())