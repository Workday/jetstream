"""Zoom Server-to-Server OAuth token provider.

Thread-safe. Caches the access token until `expires_in - 60s`. Also
captures the `scope` field from the token response so the pipeline can
verify required scopes are granted before issuing any API call.
"""
from __future__ import annotations

import threading
import time
from base64 import b64encode
from dataclasses import dataclass, field
from typing import Optional, Set

import requests

from src.config import SourceCfg


class ZoomOAuthError(RuntimeError):
    pass


@dataclass
class _Token:
    value: str
    expires_at: float
    scopes: Set[str] = field(default_factory=set)


class ZoomTokens:
    """Thread-safe token provider. Use as `tokens()` to get the access token."""

    def __init__(self, source: SourceCfg, *, session: Optional[requests.Session] = None):
        self._source = source
        self._session = session or requests.Session()
        self._lock = threading.Lock()
        self._cached: Optional[_Token] = None

    def __call__(self) -> str:
        return self._get().value

    def granted_scopes(self) -> Set[str]:
        return set(self._get().scopes)

    def _get(self) -> _Token:
        with self._lock:
            if self._cached is None or time.time() >= self._cached.expires_at:
                self._cached = self._refresh()
            return self._cached

    def _refresh(self) -> _Token:
        s = self._source
        basic = b64encode(f"{s.client_id}:{s.client_secret}".encode("utf-8")).decode("ascii")
        headers = {
            "Authorization": f"Basic {basic}",
            "Content-Type": "application/x-www-form-urlencoded",
        }
        data = {"grant_type": "account_credentials", "account_id": s.account_id}
        if s.scope:
            data["scope"] = s.scope

        resp = self._session.post(s.token_url, headers=headers, data=data, timeout=30)
        if resp.status_code != 200:
            raise ZoomOAuthError(
                f"Zoom token endpoint returned {resp.status_code}: {resp.text[:300]}"
            )
        body = resp.json()
        token = body.get("access_token")
        if not token:
            raise ZoomOAuthError(f"Zoom token response missing access_token: {body}")
        ttl = int(body.get("expires_in", 3600))
        scopes = set((body.get("scope") or "").split())
        # 60-second grace period to avoid using a token that expires mid-call.
        return _Token(value=token, expires_at=time.time() + ttl - 60, scopes=scopes)
