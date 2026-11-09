"""HTTP client: rate limiting, retries, and 401 re-authentication."""
from __future__ import annotations

import pytest

from src.http_client import HttpClient, HttpError
from tests.conftest import StubResponse


URL = "https://example.ioffice.com/external/api/rest/v2/rooms"


def _any_get(method, url, params):
    return method == "GET"


def _client(source_cfg, stub_session, stub_tokens):
    return HttpClient(source_cfg, stub_tokens, session=stub_session)


def test_returns_parsed_json(source_cfg, stub_session, stub_tokens):
    stub_session.add(_any_get, StubResponse(200, {"response": [{"id": 1}], "total": 1}))
    body = _client(source_cfg, stub_session, stub_tokens).get_json(URL)
    assert body["total"] == 1


def test_bearer_token_is_attached(source_cfg, stub_session, stub_tokens):
    stub_session.add(_any_get, StubResponse(200, {"response": []}))
    _client(source_cfg, stub_session, stub_tokens).get_json(URL)
    assert stub_session.calls[0]["headers"]["Authorization"] == "Bearer test-token"


def test_retries_on_429_then_succeeds(source_cfg, stub_session, stub_tokens):
    stub_session.add(
        _any_get,
        StubResponse(429, headers={"Retry-After": "0"}),
        StubResponse(200, {"response": [{"id": 1}]}),
    )
    body = _client(source_cfg, stub_session, stub_tokens).get_json(URL)
    assert body["response"] == [{"id": 1}]
    assert len(stub_session.calls) == 2


def test_gives_up_after_max_attempts(source_cfg, stub_session, stub_tokens):
    stub_session.add(_any_get, StubResponse(503, text="unavailable"))
    with pytest.raises(HttpError) as exc:
        _client(source_cfg, stub_session, stub_tokens).get_json(URL)
    assert exc.value.status == 503
    assert len(stub_session.calls) == source_cfg.retry.max_attempts


def test_401_triggers_reauth_and_one_retry(source_cfg, stub_session, stub_tokens):
    stub_session.add(
        _any_get,
        StubResponse(401, text="expired"),
        StubResponse(200, {"response": [{"id": 7}]}),
    )
    body = _client(source_cfg, stub_session, stub_tokens).get_json(URL)
    assert body["response"] == [{"id": 7}]
    assert stub_tokens.invalidations == 1


def test_persistent_401_raises_without_looping(source_cfg, stub_session, stub_tokens):
    stub_session.add(_any_get, StubResponse(401, text="nope"))
    with pytest.raises(HttpError) as exc:
        _client(source_cfg, stub_session, stub_tokens).get_json(URL)
    assert exc.value.status == 401
    # One re-auth only: the original call plus a single retry.
    assert stub_tokens.invalidations == 1
    assert len(stub_session.calls) == 2


def test_404_is_not_retried(source_cfg, stub_session, stub_tokens):
    stub_session.add(_any_get, StubResponse(404, text="no such entity"))
    with pytest.raises(HttpError):
        _client(source_cfg, stub_session, stub_tokens).get_json(URL)
    assert len(stub_session.calls) == 1


def test_records_response_size(source_cfg, stub_session, stub_tokens):
    stub_session.add(_any_get, StubResponse(200, {"response": []}))
    client = _client(source_cfg, stub_session, stub_tokens)
    client.get_json(URL)
    assert client.last_response_bytes == 1
