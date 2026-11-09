"""HTTP client: retry + token-bucket rate limit + Mist token injection.

This is the only place that talks to `requests`. Responsibilities:
  - Inject `Authorization: Token <value>` on every GET
  - Enforce a token-bucket rate limit (rate_limit_qps from config)
  - Retry on 429 (Retry-After header honoured) and 5xx with exponential backoff
  - Raise HttpError on non-retryable 4xx
  - Track last_response_bytes per thread (used for bytes_written in object_run)
  - Be thread-safe for parallel fan-out workers
"""
from __future__ import annotations

import logging
import threading
import time
from typing import Any, Callable, Mapping, Optional

import requests

from src.connector_config import SourceCfg


log = logging.getLogger(__name__)


class HttpError(RuntimeError):
    def __init__(self, status: int, body: str, url: str):
        super().__init__(f"HTTP {status} from {url}: {body[:500]}")
        self.status = status
        self.body = body
        self.url = url


class _Bucket:
    """Thread-safe token bucket — sleeps as needed to stay under qps.

    Multiple worker threads share one bucket; the lock protects the token
    counter and last-refill timestamp. The actual sleep happens OUTSIDE
    the lock so waiting threads don't block others' state updates.
    """

    def __init__(self, qps: float):
        self._qps = max(qps, 0.001)
        self._tokens = self._qps
        self._last = time.monotonic()
        self._lock = threading.Lock()

    def take(self) -> None:
        with self._lock:
            now = time.monotonic()
            self._tokens = min(self._qps, self._tokens + (now - self._last) * self._qps)
            self._last = now
            if self._tokens < 1:
                wait = (1 - self._tokens) / self._qps
                self._tokens = 0
            else:
                self._tokens -= 1
                wait = 0.0
        if wait > 0:
            time.sleep(wait)


class HttpClient:
    """GET-only HTTP client for the Juniper Mist API."""

    def __init__(
        self,
        source: SourceCfg,
        token_provider: Callable[[], str],
        *,
        session: Optional[requests.Session] = None,
    ):
        self._source = source
        self._token_provider = token_provider
        self._session = session or requests.Session()
        self._bucket = _Bucket(source.rate_limit_qps)
        # Per-thread byte counter so parallel fan-out workers don't interfere.
        self._local = threading.local()

    @property
    def last_response_bytes(self) -> int:
        return getattr(self._local, "bytes", 0)

    def get(
        self,
        url: str,
        *,
        params: Optional[Mapping[str, Any]] = None,
        max_attempts: Optional[int] = None,
    ) -> Any:
        """GET a URL with rate-limit enforcement and retry.

        Returns the parsed JSON body, or None if the response has no content.
        Raises HttpError on a non-retryable error response.

        `max_attempts` overrides `source.retry.max_attempts` for objects that
        need a longer retry runway (e.g. endpoints behind tighter throttles).
        """
        retry = self._source.retry
        max_att = int(max_attempts) if max_attempts is not None else retry.max_attempts
        backoff = retry.backoff_seconds
        attempt = 0

        while True:
            attempt += 1
            self._bucket.take()
            # Mist uses "Token <value>", not "Bearer <value>"
            headers = {"Authorization": f"Token {self._token_provider()}"}
            try:
                resp = self._session.get(
                    url,
                    params=params,
                    headers=headers,
                    timeout=self._source.timeout_seconds,
                )
            except requests.RequestException as exc:
                if attempt >= max_att:
                    raise HttpError(0, str(exc), url) from exc
                log.warning(
                    "transport error %s (%d/%d), retrying in %.1fs", exc, attempt, max_att, backoff
                )
                time.sleep(backoff)
                backoff *= retry.backoff_multiplier
                continue

            if resp.status_code in retry.retry_on_status and attempt < max_att:
                ra = resp.headers.get("Retry-After")
                wait = float(ra) if ra and ra.isdigit() else backoff
                log.warning(
                    "http %d from %s (%d/%d), retrying in %.1fs",
                    resp.status_code, url, attempt, max_att, wait,
                )
                time.sleep(wait)
                backoff *= retry.backoff_multiplier
                continue

            if resp.status_code >= 400:
                raise HttpError(resp.status_code, resp.text, url)

            self._local.bytes = len(resp.content or b"")
            return resp.json() if resp.content else None
