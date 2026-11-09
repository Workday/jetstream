"""Eptura iOffice OAuth 2.0 token provider.

Two-step flow, exactly as documented at
https://knowledge.eptura.com/Workplace/Integrations/REST_API/Using_OAuth_2.0_to_Access_the_API

  1. POST credentials as headers to /external/api/oauth2/auth -> authorization code
  2. POST that code to /external/api/oauth2/token -> bearer access token

The token is cached until shortly before its declared expiry. Eptura does not
always return `expires_in`, so `invalidate()` lets the HTTP layer force a
re-auth when the API rejects a token we still believed was good.
"""
from __future__ import annotations

import threading
import time
from dataclasses import dataclass
from typing import Optional

import requests

from src.connector_config import SourceCfg


# Refresh this many seconds before the declared expiry so an in-flight
# request never races the cutover.
_EXPIRY_SKEW_SECONDS = 60
_DEFAULT_TTL_SECONDS = 3600


class EpturaOAuthError(RuntimeError):
    pass


def _succeeded(response) -> bool:
    """Accept any 2xx.

    The auth endpoint answers 201, not 200, so an equality check on 200
    rejects a perfectly good authorization code.
    """
    return 200 <= response.status_code < 300


@dataclass
class _Token:
    value: str
    expires_at: float


class EpturaTokens:
    """Callable token provider: `tokens()` returns a valid bearer token."""

    def __init__(self, source: SourceCfg, *, session: Optional[requests.Session] = None):
        self._source = source
        self._session = session or requests.Session()
        self._lock = threading.Lock()
        self._cached: Optional[_Token] = None

    def __call__(self) -> str:
        with self._lock:
            if self._cached is None or time.time() >= self._cached.expires_at:
                self._cached = self._refresh()
            return self._cached.value

    def invalidate(self) -> None:
        """Drop the cached token so the next call re-authenticates."""
        with self._lock:
            self._cached = None

    def _refresh(self) -> _Token:
        s = self._source
        site = s.site_address.rstrip("/")

        auth_resp = self._session.post(
            f"{site}/external/api/oauth2/auth",
            headers={
                "Content-Type": "application/json",
                "x-response-type": "code",
                "x-auth-username": s.username,
                "x-auth-password": s.password,
                "x-client-id": s.client_id,
                "x-redirect-uri": s.redirect_uri,
                "x-scope": s.oauth_scope,
            },
            timeout=s.timeout_seconds,
        )
        if not _succeeded(auth_resp):
            raise EpturaOAuthError(
                f"Eptura auth endpoint returned {auth_resp.status_code}: "
                f"{auth_resp.text[:300]}"
            )
        auth_code = auth_resp.json().get("code")
        if not auth_code:
            raise EpturaOAuthError(
                f"Authorization code missing from Eptura auth response: {auth_resp.text[:300]}"
            )

        token_resp = self._session.post(
            f"{site}/external/api/oauth2/token",
            headers={
                "Content-Type": "application/json",
                "x-grant-type": "authorization_code",
                "x-redirect-uri": s.redirect_uri,
                "x-client-id": s.client_id,
                "x-scope": s.oauth_scope,
                "x-auth-code": auth_code,
            },
            timeout=s.timeout_seconds,
        )
        if not _succeeded(token_resp):
            raise EpturaOAuthError(
                f"Eptura token endpoint returned {token_resp.status_code}: "
                f"{token_resp.text[:300]}"
            )
        body = token_resp.json()
        token = body.get("access_token")
        if not token:
            raise EpturaOAuthError(f"Eptura token response missing access_token: {body}")

        ttl = int(body.get("expires_in", _DEFAULT_TTL_SECONDS))
        return _Token(value=token, expires_at=time.time() + ttl - _EXPIRY_SKEW_SECONDS)
