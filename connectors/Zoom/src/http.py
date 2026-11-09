"""HTTP client: retry + simple rate-limit + bearer-token injection.

This is the only place that talks to `requests`. It's intentionally small
— roughly: get a token bucket, get a bearer token, send the request,
retry on 429/5xx with exponential backoff, raise on other 4xx.
"""
from __future__ import annotations

import logging
import threading
import time
from typing import Any, Callable, Mapping, Optional

import requests

from src.config import SourceCfg


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
        self._qps = qps
        self._tokens = qps
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
    """Get-only HTTP client tailored to Zoom (no POST/PUT/DELETE needed for read)."""

    def __init__(
        self,
        source: SourceCfg,
        token_provider: Callable[[], str],
        *,
        session: Optional[requests.Session] = None,
    ):
        self._source = source
        self._tokens = token_provider
        self._session = session or requests.Session()
        self._bucket = _Bucket(source.rate_limit_qps)
        # Byte length of the most recent successful response body.
        # Stored per-thread via threading.local so parallel fan-out workers
        # don't overwrite each other. Read by extract to populate
        # connector_object_run.bytes_written and connector_run.bytes_processed.
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
        """GET a URL with rate-limit and retry.

        `max_attempts` optionally overrides `source.retry.max_attempts` for
        this specific call (e.g. Heavy-tier endpoints need a longer retry
        runway to ride out Zoom's per-app throttle windows).
        """
        retry = self._source.retry
        max_att = int(max_attempts) if max_attempts is not None else retry.max_attempts
        backoff = retry.backoff_seconds
        attempt = 0
        while True:
            attempt += 1
            self._bucket.take()
            headers = {"Authorization": f"Bearer {self._tokens()}"}
            try:
                resp = self._session.get(
                    url, params=params, headers=headers,
                    timeout=self._source.timeout_seconds,
                )
            except requests.RequestException as exc:
                if attempt >= max_att:
                    raise HttpError(0, str(exc), url) from exc
                log.warning("transport error %s (%d/%d), backing off %.1fs",
                            exc, attempt, max_att, backoff)
                time.sleep(backoff)
                backoff *= retry.backoff_multiplier
                continue

            if resp.status_code in retry.retry_on_status and attempt < max_att:
                ra = resp.headers.get("Retry-After")
                wait = float(ra) if ra and ra.isdigit() else backoff
                log.warning("http %d from %s (%d/%d), backing off %.1fs",
                            resp.status_code, url, attempt, max_att, wait)
                time.sleep(wait)
                backoff *= retry.backoff_multiplier
                continue

            if resp.status_code >= 400:
                # Zoom sometimes returns HTTP 400 with body
                # {"code": -1, "message": "502 Bad Gateway: ..."} when
                # their origin hits an upstream / Cloudflare 5xx. The
                # request itself is fine — retry as if it were a real
                # 5xx. Real Zoom-side 4xx errors have numeric codes
                # (300, 1001, 1010, ...) and are NOT retried.
                if attempt < max_att and _is_wrapped_upstream_error(resp):
                    log.warning(
                        "wrapped upstream error (http %d) from %s (%d/%d), backing off %.1fs",
                        resp.status_code, url, attempt, max_att, backoff,
                    )
                    time.sleep(backoff)
                    backoff *= retry.backoff_multiplier
                    continue
                raise HttpError(resp.status_code, resp.text, url)
            self._local.bytes = len(resp.content or b"")
            return resp.json() if resp.content else None


def _is_wrapped_upstream_error(resp: Any) -> bool:
    """True if `resp` is a Zoom-wrapped upstream infrastructure error.

    Zoom sometimes returns HTTP 4xx (typically 400) with a JSON body of
    the form {"code": -1, "message": "502 Bad Gateway: ..."} when their
    origin hits a Cloudflare / upstream 5xx. The underlying request is
    fine — retry is safe.

    Any other 4xx (real Zoom API errors with numeric codes like 300,
    1001, 1012) is NOT wrapped and should not retry.
    """
    try:
        body = resp.json()
    # Response may not be JSON at all; broad catch is the intended fallback
    # for the Zoom-wrapped-error detection heuristic.
    except Exception:  # NOSONAR
        return False
    return isinstance(body, dict) and body.get("code") == -1
