"""Google OAuth token provider (refresh-token grant).

Thread-safe. Caches the access token until `expires_in - 60s`. Google Drive
uses standard OAuth 2.0: a long-lived `refresh_token` (stored in the Databricks
secret scope) is exchanged for a short-lived `access_token` at the token
endpoint, and the access token is sent as a Bearer token on every Drive API
call.
"""
from __future__ import annotations

import threading
import time
from dataclasses import dataclass
from typing import Optional

import requests

from src.config import SourceCfg


class GoogleOAuthError(RuntimeError):
    pass


@dataclass
class _Token:
    value: str
    expires_at: float


class GoogleDriveTokens:
    """Thread-safe token provider. Use as `tokens()` to get the access token."""

    def __init__(self, source: SourceCfg, *, session: Optional[requests.Session] = None):
        self._source = source
        self._session = session or requests.Session()
        self._lock = threading.Lock()
        self._cached: Optional[_Token] = None

    def __call__(self) -> str:
        return self._get().value

    def _get(self) -> _Token:
        with self._lock:
            if self._cached is None or time.time() >= self._cached.expires_at:
                self._cached = self._refresh()
            return self._cached

    def _refresh(self) -> _Token:
        s = self._source
        data = {
            "grant_type": "refresh_token",
            "client_id": s.client_id,
            "client_secret": s.client_secret,
            "refresh_token": s.refresh_token,
        }
        resp = self._session.post(
            s.token_uri,
            data=data,
            headers={"Content-Type": "application/x-www-form-urlencoded"},
            timeout=30,
        )
        if resp.status_code != 200:
            raise GoogleOAuthError(
                f"Google token endpoint returned {resp.status_code}: {resp.text[:300]}"
            )
        body = resp.json()
        token = body.get("access_token")
        if not token:
            raise GoogleOAuthError(f"Google token response missing access_token: {body}")
        ttl = int(body.get("expires_in", 3600))
        # 60-second grace period to avoid using a token that expires mid-call.
        return _Token(value=token, expires_at=time.time() + ttl - 60)
