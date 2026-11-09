"""HTTP client tests: retry on 429/5xx, rate limit, errors propagate."""
from __future__ import annotations

import time

import pytest

from src.http import HttpClient, HttpError
from tests.conftest import StubResponse


def _stub_token():
    return "TOK"


def test_get_returns_json_body(source_cfg, stub_session):
    stub_session.add(
        lambda m, u, p: m == "GET",
        StubResponse(200, {"users": [{"id": 1}]}),
    )
    client = HttpClient(source_cfg, token_provider=_stub_token, session=stub_session)
    body = client.get("https://api.zoom.us/v2/users")
    assert body == {"users": [{"id": 1}]}


def test_bearer_token_is_attached_per_request(source_cfg, stub_session):
    stub_session.add(lambda m, u, p: True, StubResponse(200, {}))
    HttpClient(source_cfg, token_provider=_stub_token, session=stub_session).get("https://api.zoom.us/v2/x")
    assert stub_session.calls[0]["headers"]["Authorization"] == "Bearer TOK"


def test_4xx_raises_http_error_without_retry(source_cfg, stub_session):
    stub_session.add(lambda m, u, p: True, StubResponse(403, json_body={"code": 403}, text="forbidden"))
    client = HttpClient(source_cfg, token_provider=_stub_token, session=stub_session)
    with pytest.raises(HttpError) as exc:
        client.get("https://api.zoom.us/v2/x")
    assert exc.value.status == 403
    assert len(stub_session.calls) == 1


def test_429_is_retried_then_succeeds(source_cfg, stub_session, monkeypatch):
    monkeypatch.setattr(time, "sleep", lambda s: None)  # no real sleeping in tests
    sequence = iter([
        StubResponse(429, json_body=None, text="rate limited", headers={"Retry-After": "0"}),
        StubResponse(200, json_body={"users": [{"id": 1}]}),
    ])

    def dispatch(url, *, params=None, headers=None, timeout=None):
        stub_session.calls.append({"method": "GET", "url": url, "params": params, "headers": headers})
        return next(sequence)

    stub_session.get = dispatch  # type: ignore
    client = HttpClient(source_cfg, token_provider=_stub_token, session=stub_session)
    body = client.get("https://api.zoom.us/v2/users")
    assert body == {"users": [{"id": 1}]}
    assert len(stub_session.calls) == 2


def test_retries_exhausted_raises(source_cfg, stub_session, monkeypatch):
    monkeypatch.setattr(time, "sleep", lambda s: None)

    def dispatch(url, *, params=None, headers=None, timeout=None):
        stub_session.calls.append({"method": "GET", "url": url, "params": params, "headers": headers})
        return StubResponse(503, json_body=None, text="bad")

    stub_session.get = dispatch  # type: ignore
    client = HttpClient(source_cfg, token_provider=_stub_token, session=stub_session)
    with pytest.raises(HttpError) as exc:
        client.get("https://api.zoom.us/v2/users")
    assert exc.value.status == 503
    assert len(stub_session.calls) == source_cfg.retry.max_attempts


def test_5xx_in_non_retry_set_raises_immediately(source_cfg, stub_session, monkeypatch):
    # Build a SourceCfg variant with an empty retry_on_status set.
    from src.config import RetryCfg, SourceCfg
    s = SourceCfg(
        base_url=source_cfg.base_url, token_url=source_cfg.token_url, scope=source_cfg.scope,
        account_id=source_cfg.account_id, client_id=source_cfg.client_id, client_secret=source_cfg.client_secret,
        rate_limit_qps=source_cfg.rate_limit_qps,
        retry=RetryCfg(max_attempts=3, backoff_seconds=0.001, backoff_multiplier=1.0, retry_on_status=()),
        timeout_seconds=source_cfg.timeout_seconds,
    )
    stub_session.add(lambda m, u, p: True, StubResponse(500, json_body=None, text="boom"))
    with pytest.raises(HttpError) as exc:
        HttpClient(s, token_provider=_stub_token, session=stub_session).get("https://x/y")
    assert exc.value.status == 500
    assert len(stub_session.calls) == 1


# ---------------------------------------------------------------------------
# Zoom-wrapped upstream errors (HTTP 400 with body {"code": -1, ...})
# ---------------------------------------------------------------------------

def test_wrapped_upstream_error_is_retried(source_cfg, stub_session, monkeypatch):
    """Zoom sometimes returns HTTP 400 with body {'code':-1, 'message':'502 ...'}
    when their origin hits an upstream 5xx. Retry as if it were a real 5xx."""
    monkeypatch.setattr(time, "sleep", lambda s: None)
    sequence = iter([
        StubResponse(400, json_body={"code": -1, "message": "502 Bad Gateway: cloudflare"}),
        StubResponse(400, json_body={"code": -1, "message": "502 Bad Gateway: cloudflare"}),
        StubResponse(200, json_body={"users": [{"id": 1}]}),
    ])

    def dispatch(url, *, params=None, headers=None, timeout=None):
        stub_session.calls.append({"method": "GET", "url": url, "params": params, "headers": headers})
        return next(sequence)

    stub_session.get = dispatch  # type: ignore
    client = HttpClient(source_cfg, token_provider=_stub_token, session=stub_session)
    body = client.get("https://api.zoom.us/v2/rooms/xyz/devices")
    assert body == {"users": [{"id": 1}]}
    assert len(stub_session.calls) == 3


def test_real_zoom_4xx_error_does_NOT_retry(source_cfg, stub_session, monkeypatch):
    """Real Zoom API errors (e.g. code=300 'Meeting not found') should raise
    immediately without retry. Only code=-1 (wrapped upstream) retries."""
    monkeypatch.setattr(time, "sleep", lambda s: None)
    stub_session.add(
        lambda m, u, p: True,
        StubResponse(400, json_body={"code": 300, "message": "Meeting does not exist"},
                     text='{"code":300,"message":"Meeting does not exist"}'),
    )
    with pytest.raises(HttpError) as exc:
        HttpClient(source_cfg, token_provider=_stub_token, session=stub_session).get("https://x/y")
    assert exc.value.status == 400
    assert len(stub_session.calls) == 1  # no retry — code!=-1
    assert "300" in exc.value.body


def test_wrapped_upstream_error_gives_up_after_max_attempts(source_cfg, stub_session, monkeypatch):
    """If the wrapped 400 keeps coming back, we still eventually raise so the
    caller (extract._paginate_fanout worker) gets a real failure signal."""
    monkeypatch.setattr(time, "sleep", lambda s: None)
    stub_session.add(
        lambda m, u, p: True,
        StubResponse(400, json_body={"code": -1, "message": "502 Bad Gateway"}),
    )
    with pytest.raises(HttpError) as exc:
        HttpClient(source_cfg, token_provider=_stub_token, session=stub_session).get("https://x/y")
    assert exc.value.status == 400
    assert len(stub_session.calls) == source_cfg.retry.max_attempts
