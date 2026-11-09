"""Shared pytest fixtures for the Jetstream Juniper Mist connector."""
from __future__ import annotations

import os
import sys
from typing import Any, Dict, List, Optional

import pytest

_REPO_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
if _REPO_ROOT not in sys.path:
    sys.path.insert(0, _REPO_ROOT)


# ---------------------------------------------------------------------------
# StubResponse + StubSession — mimic enough of `requests` for our HTTP layer
# ---------------------------------------------------------------------------

class StubResponse:
    def __init__(
        self,
        status_code: int = 200,
        json_body=None,
        headers=None,
        text: str = "",
    ):
        self.status_code = status_code
        self._json = json_body
        self.headers = headers or {}
        self.text = text or (str(json_body) if json_body is not None else "")
        self.content = b"x" if json_body is not None or text else b""

    def json(self):
        return self._json


class StubSession:
    """Minimal stand-in for requests.Session that returns scripted responses.

    `add(predicate, response)` registers a route. The first matching
    predicate for a (method, url, params) triple fires its response.
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
        return self._dispatch(
            method.upper(),
            url,
            kwargs.get("params") or kwargs.get("data"),
            kwargs.get("headers"),
        )

    def _dispatch(self, method, url, params, headers):
        self.calls.append({"method": method, "url": url, "params": params, "headers": headers})
        for predicate, response in self.routes:
            if predicate(method, url, params):
                return response
        return StubResponse(404, json_body={"error": "no route", "url": url})


@pytest.fixture
def stub_session() -> StubSession:
    return StubSession()


# ---------------------------------------------------------------------------
# Config fixtures
# ---------------------------------------------------------------------------

@pytest.fixture
def source_cfg():
    from src.connector_config import RetryCfg, SourceCfg
    return SourceCfg(
        base_url="https://api.mist.com/api/v1",
        org_id="44a1aa85-fff5-498f-91fa-6cd89632176c",
        api_token="test-token-abc123",
        rate_limit_qps=10000,       # effectively disable rate-limit in tests
        retry=RetryCfg(max_attempts=2, backoff_seconds=0.001, backoff_multiplier=1.0),
        timeout_seconds=5,
        lookback_days=7,
        fan_out_max_workers=2,
        site_filter_substrings=(),
    )


@pytest.fixture
def object_cfg_sites():
    from src.connector_config import ObjectCfg
    return ObjectCfg(name="sites", primary_key=["id"], load_strategy="full")


@pytest.fixture
def object_cfg_zone_visits():
    from src.connector_config import ObjectCfg
    return ObjectCfg(
        name="zone_visits",
        primary_key=["site_id", "mac", "enter_epoch"],
        load_strategy="incremental_cursor",
        cursor_field="timestamp",
        lookback_minutes=30,
        parent="sites_filtered",
        parent_id_column="site_id",
        cursor_param_from="start",
        cursor_param_to="end",
    )


@pytest.fixture
def object_cfg_maps():
    from src.connector_config import ObjectCfg
    return ObjectCfg(
        name="maps",
        primary_key=["id"],
        load_strategy="full",
        parent="sites",
        parent_id_column="site_id",
    )
