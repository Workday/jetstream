"""ServiceNow OAuth 2.0 access-token management (refresh-token grant)."""

from __future__ import annotations

import time
from dataclasses import dataclass, field
from typing import Any

import requests

from src.secrets import ServiceNowCredentials

_TOKEN_SKEW_SECONDS = 60


@dataclass
class ServiceNowSession:
    """Authenticated ServiceNow session with automatic token refresh."""

    credentials: ServiceNowCredentials
    access_token: str = ""
    expires_at: float = 0.0
    _http: requests.Session = field(default_factory=requests.Session, repr=False)

    def __post_init__(self) -> None:
        self.refresh()

    @property
    def base_url(self) -> str:
        return self.credentials.instance_url

    @property
    def api_base(self) -> str:
        return f"{self.base_url}/api/now"

    def is_expired(self) -> bool:
        return time.time() >= (self.expires_at - _TOKEN_SKEW_SECONDS)

    def refresh(self) -> None:
        payload = {
            "grant_type": "refresh_token",
            "client_id": self.credentials.client_id,
            "client_secret": self.credentials.client_secret,
            "refresh_token": self.credentials.refresh_token,
        }
        resp = self._http.post(
            self.credentials.token_url,
            data=payload,
            headers={"Content-Type": "application/x-www-form-urlencoded"},
            timeout=30,
        )
        if resp.status_code != 200:
            raise RuntimeError(
                f"ServiceNow token refresh failed ({resp.status_code}): {resp.text[:500]}"
            )
        body: dict[str, Any] = resp.json()
        self.access_token = body["access_token"]
        expires_in = int(body.get("expires_in", 1800))
        self.expires_at = time.time() + expires_in

    def auth_headers(self, extra: dict[str, str] | None = None) -> dict[str, str]:
        if self.is_expired():
            self.refresh()
        headers = {
            "Authorization": f"Bearer {self.access_token}",
            "Accept": "application/json",
        }
        if extra:
            headers.update(extra)
        return headers
