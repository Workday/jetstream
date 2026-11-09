"""Authentication session manager enforcing literal URL execution and 429 backoff handling."""
from __future__ import annotations

import base64
import logging
import time
from dataclasses import dataclass
from typing import Any, Dict, Optional
from urllib.parse import quote, urlencode
import requests

log = logging.getLogger(__name__)

@dataclass(frozen=True)
class WSSCredentials:
    api_key: str
    user_token: str
    user_email: str

def load_wss_credentials(secret_scope: str, dbutils: Any = None) -> WSSCredentials:
    if dbutils is not None:
        return WSSCredentials(
            api_key=dbutils.secrets.get(scope=secret_scope, key="api_key"),
            user_token=dbutils.secrets.get(scope=secret_scope, key="user_token"),
            user_email=dbutils.secrets.get(scope=secret_scope, key="user_email"),
        )
    import os
    return WSSCredentials(
        api_key=os.environ.get("WSS_API_KEY", ""),
        user_token=os.environ.get("WSS_USER_TOKEN", ""),
        user_email=os.environ.get("WSS_USER_EMAIL", ""),
    )

def get_auth_headers(pattern: str, creds: WSSCredentials) -> Dict[str, str]:
    pattern_clean = (pattern or "pattern_3").lower().strip()

    if pattern_clean == "pattern_1":
        return {
            "X-Api-Key": creds.api_key,
            "X-User-Token": creds.user_token,
            "X-User-Email": creds.user_email,
            "Content-Type": "application/vnd.api+json",
            "Accept": "application/vnd.api+json, application/vnd.api+json; com.workdayspend.api.version=2019-01-01",
        }
    elif pattern_clean == "pattern_2":
        token_bytes = f"{creds.user_email}:{creds.user_token}".encode("utf-8")
        basic_b64 = base64.b64encode(token_bytes).decode("utf-8")
        return {
            "X-Api-Key": creds.api_key,
            "Accept": "application/scim+json",
            "Authorization": f"Basic {basic_b64}",
        }
    elif pattern_clean == "pattern_3":
        return {
            "X-Api-Key": creds.api_key,
            "X-User-Token": creds.user_token,
            "X-User-Email": creds.user_email,
            "Content-Type": "application/vnd.api+json",
            "Accept": "application/vnd.api+json",
        }
    else:
        raise ValueError(f"Unsupported authentication pattern: {pattern}")

class WSSSession:
    def __init__(self, credentials: WSSCredentials, is_sandbox: bool = False):
        self.credentials = credentials
        self.base_url = "https://api.us.workdayspend.com"
        self.session = requests.Session()

    def request(
        self,
        method: str,
        url: str,
        headers: Optional[Dict[str, str]] = None,
        params: Optional[Dict[str, Any]] = None,
        timeout: int = 60,
        max_retries: int = 10,
        base_backoff_sec: float = 10.0,
    ) -> Any:
        """Executes HTTP requests, preserving literal URLs when params is None."""
        req_kwargs: Dict[str, Any] = {"headers": headers, "timeout": timeout}

        if params:
            req_kwargs["params"] = urlencode(params, quote_via=quote)

        attempt = 1
        while attempt <= max_retries:
            try:
                resp = self.session.request(method, url, **req_kwargs)

                if resp.status_code in (429, 500, 502, 503, 504):
                    retry_after = resp.headers.get("Retry-After")
                    sleep_time = float(retry_after) if (retry_after and retry_after.isdigit()) else (
                        base_backoff_sec if attempt == 1 else base_backoff_sec * (1.2 ** (attempt - 1))
                    )
                    time.sleep(round(sleep_time, 2))
                    attempt += 1
                    continue

                resp.raise_for_status()
                return resp.json() if resp.text else {}

            except requests.exceptions.RequestException as req_err:
                if attempt < max_retries:
                    time.sleep(base_backoff_sec)
                    attempt += 1
                else:
                    raise req_err

        final_resp = self.session.request(method, url, **req_kwargs)
        final_resp.raise_for_status()
        return final_resp.json() if final_resp.text else {}