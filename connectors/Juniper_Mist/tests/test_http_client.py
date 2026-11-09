"""Tests for src/http_client.py."""
from __future__ import annotations

import pytest

from tests.conftest import StubResponse, StubSession


def _make_client(session, source_cfg):
    from src.http_client import HttpClient
    return HttpClient(source_cfg, token_provider=lambda: "test-token", session=session)


def test_successful_get_returns_json(stub_session, source_cfg):
    stub_session.add(
        lambda m, url, p: url.endswith("/test"),
        StubResponse(200, json_body={"result": "ok"}),
    )
    client = _make_client(stub_session, source_cfg)
    body = client.get("https://api.mist.com/api/v1/test")
    assert body == {"result": "ok"}


def test_authorization_header_injected(stub_session, source_cfg):
    stub_session.add(lambda m, u, p: True, StubResponse(200, json_body={}))
    client = _make_client(stub_session, source_cfg)
    client.get("https://api.mist.com/api/v1/any")
    call = stub_session.calls[-1]
    assert call["headers"]["Authorization"] == "Token test-token"


def test_404_raises_http_error(stub_session, source_cfg):
    from src.http_client import HttpError
    stub_session.add(lambda m, u, p: True, StubResponse(404, text="not found"))
    client = _make_client(stub_session, source_cfg)
    with pytest.raises(HttpError) as exc_info:
        client.get("https://api.mist.com/api/v1/missing")
    assert exc_info.value.status == 404


def test_empty_response_returns_none(stub_session, source_cfg):
    stub_session.add(lambda m, u, p: True, StubResponse(200, json_body=None, text=""))
    from tests.conftest import StubResponse as SR

    # Simulate content=b"" → no JSON
    resp = StubResponse(200, json_body=None)
    resp.content = b""
    stub_session.routes.clear()
    stub_session.add(lambda m, u, p: True, resp)

    from src.http_client import HttpClient
    client = HttpClient(source_cfg, token_provider=lambda: "t", session=stub_session)
    body = client.get("https://api.mist.com/api/v1/empty")
    assert body is None


def test_retry_on_500(stub_session, source_cfg):
    from src.connector_config import RetryCfg, SourceCfg

    fast_cfg = SourceCfg(
        base_url=source_cfg.base_url,
        org_id=source_cfg.org_id,
        api_token=source_cfg.api_token,
        rate_limit_qps=100000,
        retry=RetryCfg(max_attempts=3, backoff_seconds=0.001, backoff_multiplier=1.0),
        timeout_seconds=5,
    )

    call_count = 0

    class CountingSession:
        def get(self, url, *, params=None, headers=None, timeout=None):
            nonlocal call_count
            call_count += 1
            if call_count < 3:
                return StubResponse(500, text="server error")
            return StubResponse(200, json_body={"ok": True})

    from src.http_client import HttpClient
    client = HttpClient(fast_cfg, token_provider=lambda: "t", session=CountingSession())
    result = client.get("https://api.mist.com/api/v1/flaky")
    assert result == {"ok": True}
    assert call_count == 3


def test_retry_exhausted_raises(stub_session, source_cfg):
    from src.connector_config import RetryCfg, SourceCfg
    from src.http_client import HttpClient, HttpError

    fast_cfg = SourceCfg(
        base_url=source_cfg.base_url,
        org_id=source_cfg.org_id,
        api_token=source_cfg.api_token,
        rate_limit_qps=100000,
        retry=RetryCfg(max_attempts=2, backoff_seconds=0.001, backoff_multiplier=1.0),
        timeout_seconds=5,
    )

    class AlwaysFailing:
        def get(self, url, *, params=None, headers=None, timeout=None):
            return StubResponse(503, text="unavailable")

    client = HttpClient(fast_cfg, token_provider=lambda: "t", session=AlwaysFailing())
    with pytest.raises(HttpError) as exc_info:
        client.get("https://api.mist.com/api/v1/down")
    assert exc_info.value.status == 503


def test_last_response_bytes_tracked(stub_session, source_cfg):
    data = b'{"x": 1}'
    resp = StubResponse(200, json_body={"x": 1})
    resp.content = data
    stub_session.add(lambda m, u, p: True, resp)
    from src.http_client import HttpClient
    client = HttpClient(source_cfg, token_provider=lambda: "t", session=stub_session)
    client.get("https://api.mist.com/api/v1/x")
    assert client.last_response_bytes == len(data)
