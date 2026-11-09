"""Shared pytest fixtures.

Tests cover everything except the Spark writer, which needs a Databricks
runtime. The HTTP layer is exercised through StubSession so no network
traffic is ever produced.
"""
from __future__ import annotations

import os
import sys
from typing import Dict, List

import pytest

_REPO_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
if _REPO_ROOT not in sys.path:
    sys.path.insert(0, _REPO_ROOT)


class StubResponse:
    def __init__(self, status_code: int = 200, json_body=None, headers=None, text: str = ""):
        self.status_code = status_code
        self._json = json_body
        self.headers = headers or {}
        self.text = text
        self.content = b"x" if json_body is not None or text else b""

    def json(self):
        return self._json


class StubSession:
    """Routes requests to canned responses.

    Each route is `(predicate, responses)`. A route with several responses
    returns them in order and repeats the last one, which is how retry and
    re-auth behaviour is tested.
    """

    def __init__(self):
        self.routes: List = []
        self.calls: List[Dict] = []

    def add(self, predicate, *responses: StubResponse) -> None:
        self.routes.append([predicate, list(responses), 0])

    def get(self, url, *, params=None, headers=None, timeout=None):
        return self._dispatch("GET", url, params, headers)

    def post(self, url, *, data=None, headers=None, timeout=None):
        return self._dispatch("POST", url, data, headers)

    def _dispatch(self, method, url, params, headers):
        self.calls.append({"method": method, "url": url, "params": params, "headers": headers})
        for route in self.routes:
            predicate, responses, seen = route
            if predicate(method, url, params):
                route[2] = seen + 1
                return responses[min(seen, len(responses) - 1)]
        return StubResponse(404, json_body={"error": "no route", "url": url}, text="no route")


@pytest.fixture
def stub_session() -> StubSession:
    return StubSession()


@pytest.fixture
def source_cfg():
    from src.connector_config import RetryCfg, SourceCfg
    return SourceCfg(
        site_address="https://example.ioffice.com",
        api_base_path="/external/api/rest/v2",
        username="user",
        password="pass",
        client_id="client-id",
        redirect_uri="https://your-tenant.iofficeconnect.com/",
        oauth_scope="email",
        page_size=2,
        rate_limit_qps=1000,
        retry=RetryCfg(max_attempts=3, backoff_seconds=0.001, backoff_multiplier=1.0),
        timeout_seconds=5,
    )


@pytest.fixture
def rooms():
    from src.connector_config import ObjectCfg
    return ObjectCfg(name="rooms")


class StubTokens:
    """Token provider that records how often it was invalidated."""

    def __init__(self, value: str = "test-token"):
        self.value = value
        self.invalidations = 0

    def __call__(self) -> str:
        return self.value

    def invalidate(self) -> None:
        self.invalidations += 1


@pytest.fixture
def stub_tokens() -> StubTokens:
    return StubTokens()
