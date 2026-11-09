"""GET-only HTTP client for the Eptura REST API.

The only place in this connector that talks to `requests`. Responsibilities,
in order: stay under the configured request rate, attach a fresh bearer
token, retry transient failures, and hand back parsed JSON.

Retry policy:
  * 429 and 5xx        -> back off and retry (honouring `Retry-After`)
  * transport errors   -> back off and retry
  * 401                -> re-authenticate once, then retry immediately
  * any other 4xx      -> raise, since retrying won't help
"""
from __future__ import annotations

import logging
import time
from typing import Any, Mapping, Optional

import requests

from src.connector_config import SourceCfg


log = logging.getLogger(__name__)


class HttpError(RuntimeError):
    def __init__(self, status: int, body: str, url: str):
        super().__init__(f"HTTP {status} from {url}: {body[:500]}")
        self.status = status
        self.body = body
        self.url = url


class _RateLimiter:
    """Token bucket that sleeps just enough to hold the configured qps."""

    def __init__(self, qps: float):
        self._qps = max(qps, 0.1)
        self._tokens = self._qps
        self._last = time.monotonic()

    def take(self) -> None:
        now = time.monotonic()
        self._tokens = min(self._qps, self._tokens + (now - self._last) * self._qps)
        self._last = now
        if self._tokens < 1:
            time.sleep((1 - self._tokens) / self._qps)
            self._tokens = 0
        else:
            self._tokens -= 1


class HttpClient:
    def __init__(self, source: SourceCfg, tokens, *, session: Optional[requests.Session] = None):
        self._source = source
        self._tokens = tokens
        self._session = session or requests.Session()
        self._limiter = _RateLimiter(source.rate_limit_qps)
        self.last_response_bytes = 0

    def get_json(self, url: str, *, params: Optional[Mapping[str, Any]] = None) -> Any:
        retry = self._source.retry
        backoff = retry.backoff_seconds
        reauthed = False
        attempt = 0

        while True:
            attempt += 1
            self._limiter.take()
            try:
                resp = self._session.get(
                    url,
                    params=params,
                    headers={
                        "Authorization": f"Bearer {self._tokens()}",
                        "Content-Type": "application/json",
                    },
                    timeout=self._source.timeout_seconds,
                )
            except requests.RequestException as exc:
                if attempt >= retry.max_attempts:
                    raise HttpError(0, str(exc), url) from exc
                log.warning("transport error %s (%d/%d), retrying in %.1fs",
                            exc, attempt, retry.max_attempts, backoff)
                time.sleep(backoff)
                backoff *= retry.backoff_multiplier
                continue

            # A token we thought was valid was rejected — Eptura's declared
            # TTL can be optimistic on long runs. Re-auth once, then give up.
            if resp.status_code == 401 and not reauthed:
                reauthed = True
                log.warning("401 from %s — re-authenticating and retrying once", url)
                self._tokens.invalidate()
                continue

            if resp.status_code in retry.retry_on_status and attempt < retry.max_attempts:
                wait = _retry_after(resp) or backoff
                log.warning("http %d from %s (%d/%d), retrying in %.1fs",
                            resp.status_code, url, attempt, retry.max_attempts, wait)
                time.sleep(wait)
                backoff *= retry.backoff_multiplier
                continue

            if resp.status_code >= 400:
                raise HttpError(resp.status_code, resp.text, url)

            self.last_response_bytes = len(resp.content or b"")
            return resp.json() if resp.content else None


def _retry_after(resp: Any) -> Optional[float]:
    value = resp.headers.get("Retry-After") if resp.headers else None
    if value and str(value).isdigit():
        return float(value)
    return None
