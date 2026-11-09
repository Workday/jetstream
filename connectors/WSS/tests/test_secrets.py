"""Secret-scope contract. Values are never stored in this package."""
from __future__ import annotations

from src.secrets import DEFAULT_SECRET_SCOPE, REQUIRED_KEYS


def test_scope_and_key_names():
    assert DEFAULT_SECRET_SCOPE == "jetstream_wss"
    assert REQUIRED_KEYS == ("wss_api_key", "wss_user_token", "wss_user_email")
