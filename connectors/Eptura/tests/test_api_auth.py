"""Eptura two-step OAuth flow."""
from __future__ import annotations

import pytest

from src.api_auth import EpturaOAuthError, EpturaTokens
from tests.conftest import StubResponse


def _auth_route(method, url, params):
    return method == "POST" and "/oauth2/auth" in url


def _token_route(method, url, params):
    return method == "POST" and "/oauth2/token" in url


def _happy_path(stub_session, token="abc", auth_status=201):
    # 201 is what the live auth endpoint actually answers.
    stub_session.add(_auth_route, StubResponse(auth_status, {"code": "auth-code-123"}))
    stub_session.add(_token_route, StubResponse(200, {"access_token": token, "expires_in": 3600}))


@pytest.mark.parametrize("auth_status", [200, 201, 202])
def test_any_2xx_from_the_auth_step_is_accepted(source_cfg, stub_session, auth_status):
    """Eptura answers the auth step with 201, so requiring exactly 200 rejects
    a valid authorization code and fails every run."""
    _happy_path(stub_session, auth_status=auth_status)
    assert EpturaTokens(source_cfg, session=stub_session)() == "abc"


def test_token_fetched_on_first_call(source_cfg, stub_session):
    _happy_path(stub_session)
    assert EpturaTokens(source_cfg, session=stub_session)() == "abc"
    assert len(stub_session.calls) == 2


def test_token_is_cached(source_cfg, stub_session):
    _happy_path(stub_session, token="tok1")
    tokens = EpturaTokens(source_cfg, session=stub_session)
    assert tokens() == "tok1"
    assert tokens() == "tok1"
    assert len(stub_session.calls) == 2


def test_invalidate_forces_reauth(source_cfg, stub_session):
    _happy_path(stub_session)
    tokens = EpturaTokens(source_cfg, session=stub_session)
    tokens()
    tokens.invalidate()
    tokens()
    assert len(stub_session.calls) == 4


def test_auth_step_sends_credential_headers(source_cfg, stub_session):
    _happy_path(stub_session)
    EpturaTokens(source_cfg, session=stub_session)()
    headers = stub_session.calls[0]["headers"]
    assert headers["x-response-type"] == "code"
    assert headers["x-auth-username"] == "user"
    assert headers["x-auth-password"] == "pass"
    assert headers["x-client-id"] == "client-id"
    assert headers["x-scope"] == "email"


def test_token_step_exchanges_the_auth_code(source_cfg, stub_session):
    _happy_path(stub_session)
    EpturaTokens(source_cfg, session=stub_session)()
    headers = stub_session.calls[1]["headers"]
    assert headers["x-grant-type"] == "authorization_code"
    assert headers["x-auth-code"] == "auth-code-123"


def test_expiry_uses_declared_ttl_minus_skew(source_cfg, stub_session):
    stub_session.add(_auth_route, StubResponse(200, {"code": "c"}))
    stub_session.add(_token_route, StubResponse(200, {"access_token": "t", "expires_in": 600}))
    tokens = EpturaTokens(source_cfg, session=stub_session)
    tokens()
    remaining = tokens._cached.expires_at - __import__("time").time()
    assert 530 < remaining < 545


def test_missing_auth_code_raises(source_cfg, stub_session):
    stub_session.add(_auth_route, StubResponse(200, {}))
    with pytest.raises(EpturaOAuthError, match="Authorization code"):
        EpturaTokens(source_cfg, session=stub_session)()


def test_missing_access_token_raises(source_cfg, stub_session):
    stub_session.add(_auth_route, StubResponse(200, {"code": "c"}))
    stub_session.add(_token_route, StubResponse(200, {}))
    with pytest.raises(EpturaOAuthError, match="access_token"):
        EpturaTokens(source_cfg, session=stub_session)()


def test_non_200_auth_raises(source_cfg, stub_session):
    stub_session.add(_auth_route, StubResponse(401, text="unauthorized"))
    with pytest.raises(EpturaOAuthError, match="401"):
        EpturaTokens(source_cfg, session=stub_session)()
