"""Tests for src/api_auth.py."""
from __future__ import annotations

import pytest

from src.api_auth import MistAuthError, MistTokenProvider


def test_call_returns_token():
    provider = MistTokenProvider("my-secret-token")
    assert provider() == "my-secret-token"


def test_auth_header_format():
    provider = MistTokenProvider("my-secret-token")
    assert provider.auth_header() == "Token my-secret-token"


def test_granted_scopes_always_empty():
    provider = MistTokenProvider("any-token")
    assert provider.granted_scopes() == set()


def test_multiple_calls_reuse_same_token():
    provider = MistTokenProvider("stable-token")
    assert provider() == provider() == "stable-token"


def test_empty_token_raises():
    with pytest.raises(MistAuthError, match="juniper_mist_token is missing"):
        MistTokenProvider("")


def test_unresolved_placeholder_raises():
    with pytest.raises(MistAuthError):
        MistTokenProvider("<no-scope:juniper_mist_token>")


def test_normal_token_no_raise():
    provider = MistTokenProvider("tok_abc123")
    assert provider() == "tok_abc123"
