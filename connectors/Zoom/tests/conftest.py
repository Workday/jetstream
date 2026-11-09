"""Shared pytest fixtures."""
from __future__ import annotations

import os
import sys
from typing import Any, Dict, List, Optional

import pytest

# Put the repo root on sys.path so tests can `from src.* import ...`.
_REPO_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
if _REPO_ROOT not in sys.path:
    sys.path.insert(0, _REPO_ROOT)


# ---------------------------------------------------------------------------
# StubResponse + StubSession — mimic enough of `requests` for our HTTP layer.
# ---------------------------------------------------------------------------

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
    """A minimal stand-in for requests.Session that returns scripted responses.

    `routes` is a list of (predicate, response) pairs. The first predicate
    that matches the (method, url, params) triple gets fired.
    """

    def __init__(self):
        self.routes: List = []
        self.calls: List[Dict] = []

    def add(self, predicate, response: StubResponse) -> None:
        self.routes.append((predicate, response))

    def get(self, url, *, params=None, headers=None, timeout=None):
        return self._dispatch("GET", url, params, headers)

    def post(self, url, *, data=None, headers=None, timeout=None):
        return self._dispatch("POST", url, data, headers)

    def request(self, method, url, **kwargs):
        return self._dispatch(method.upper(), url, kwargs.get("params") or kwargs.get("data"), kwargs.get("headers"))

    def _dispatch(self, method, url, params, headers):
        self.calls.append({"method": method, "url": url, "params": params, "headers": headers})
        for predicate, response in self.routes:
            if predicate(method, url, params):
                return response
        return StubResponse(404, json_body={"error": "no route", "url": url}, text="no route")


@pytest.fixture
def stub_session() -> StubSession:
    return StubSession()


# ---------------------------------------------------------------------------
# Config helpers
# ---------------------------------------------------------------------------

@pytest.fixture
def source_cfg():
    from src.config import RetryCfg, SourceCfg
    return SourceCfg(
        base_url="https://api.zoom.us/v2",
        token_url="https://zoom.us/oauth/token",
        scope="user:read:admin meeting:read:admin",
        account_id="acct",
        client_id="cid",
        client_secret="csec",
        rate_limit_qps=1000,            # effectively disable rate-limit in tests
        retry=RetryCfg(max_attempts=3, backoff_seconds=0.001, backoff_multiplier=1.0),
        timeout_seconds=5,
    )


@pytest.fixture
def object_cfg_users():
    from src.config import ObjectCfg
    return ObjectCfg(
        name="users", primary_key=["id"], load_strategy="full",
        required_scope="user:read:admin",
    )


@pytest.fixture
def object_cfg_meetings():
    from src.config import ObjectCfg
    return ObjectCfg(
        name="meetings", primary_key=["id"], load_strategy="incremental_cursor",
        cursor_field="start_time", lookback_minutes=60,
        parent="users", required_scope="meeting:read:admin",
    )
