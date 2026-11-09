"""Zoom S2S OAuth tests."""
from __future__ import annotations

import time

import pytest

from src.auth import ZoomOAuthError, ZoomTokens
from tests.conftest import StubResponse


# ---------------------------------------------------------------------------
# happy paths
# ---------------------------------------------------------------------------

def test_token_fetched_on_first_call(source_cfg, stub_session):
    stub_session.add(
        lambda m, u, p: m == "POST" and "oauth/token" in u,
        StubResponse(200, {"access_token": "abc", "expires_in": 3600,
                           "scope": "user:read:admin meeting:read:admin"}),
    )
    t = ZoomTokens(source_cfg, session=stub_session)
    assert t() == "abc"
    assert {"user:read:admin", "meeting:read:admin"}.issubset(t.granted_scopes())
    assert len(stub_session.calls) == 1


def test_token_is_cached_until_near_expiry(source_cfg, stub_session):
    stub_session.add(
        lambda m, u, p: True,
        StubResponse(200, {"access_token": "tok1", "expires_in": 3600}),
    )
    t = ZoomTokens(source_cfg, session=stub_session)
    assert t() == "tok1"
    assert t() == "tok1"
    assert len(stub_session.calls) == 1, "second call should use cached token"


def test_basic_auth_header_uses_client_id_and_secret(source_cfg, stub_session):
    from base64 import b64encode

    stub_session.add(
        lambda m, u, p: True,
        StubResponse(200, {"access_token": "abc", "expires_in": 3600}),
    )
    t = ZoomTokens(source_cfg, session=stub_session)
    t()
    call = stub_session.calls[0]
    auth = call["headers"]["Authorization"]
    assert auth.startswith("Basic ")
    expected = "Basic " + b64encode(
        f"{source_cfg.client_id}:{source_cfg.client_secret}".encode("utf-8")
    ).decode("ascii")
    assert auth == expected


def test_account_credentials_grant_with_account_id(source_cfg, stub_session):
    captured = {}

    def grab(method, url, params):
        captured["params"] = params
        return True

    stub_session.add(grab, StubResponse(200, {"access_token": "abc", "expires_in": 3600}))
    ZoomTokens(source_cfg, session=stub_session)()
    assert captured["params"]["grant_type"] == "account_credentials"
    assert captured["params"]["account_id"] == "acct"
    assert captured["params"]["scope"] == "user:read:admin meeting:read:admin"


# ---------------------------------------------------------------------------
# error paths
# ---------------------------------------------------------------------------

def test_non_200_raises_oauth_error(source_cfg, stub_session):
    stub_session.add(
        lambda m, u, p: True,
        StubResponse(401, json_body=None, text='{"reason":"Invalid client"}'),
    )
    t = ZoomTokens(source_cfg, session=stub_session)
    with pytest.raises(ZoomOAuthError) as exc:
        t()
    assert "401" in str(exc.value)


def test_response_missing_access_token_raises(source_cfg, stub_session):
    stub_session.add(
        lambda m, u, p: True,
        StubResponse(200, {"expires_in": 3600, "scope": "x"}),
    )
    t = ZoomTokens(source_cfg, session=stub_session)
    with pytest.raises(ZoomOAuthError) as exc:
        t()
    assert "access_token" in str(exc.value)


# ---------------------------------------------------------------------------
# refresh-on-expiry
# ---------------------------------------------------------------------------

def test_expired_token_triggers_refresh(source_cfg, stub_session, monkeypatch):
    stub_session.add(
        lambda m, u, p: True,
        StubResponse(200, {"access_token": "tok-a", "expires_in": 1}),
    )
    t = ZoomTokens(source_cfg, session=stub_session)
    assert t() == "tok-a"

    # Replace the cached token's expires_at with a moment in the past, then
    # swap the route so a refresh returns a different token.
    stub_session.routes.clear()
    stub_session.add(
        lambda m, u, p: True,
        StubResponse(200, {"access_token": "tok-b", "expires_in": 3600}),
    )
    monkeypatch.setattr(time, "time", lambda: 10**12)
    assert t() == "tok-b"
