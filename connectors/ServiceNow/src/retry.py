"""Shared retry helpers for ServiceNow HTTP and control-plane writes."""

from __future__ import annotations

import logging
import random
import time
from typing import Any, Callable, TypeVar

from src.config import RetryCfg

log = logging.getLogger(__name__)

T = TypeVar("T")

_DEFAULT_RETRY = RetryCfg()


def sleep_before_retry(
    attempt: int,
    *,
    retry: RetryCfg = _DEFAULT_RETRY,
    response: Any = None,
) -> None:
    """Sleep using Retry-After when present, else exponential backoff."""
    wait = retry.backoff_seconds * (retry.backoff_multiplier ** max(attempt - 1, 0))
    if response is not None:
        headers = getattr(response, "headers", None) or {}
        ra = headers.get("Retry-After") if hasattr(headers, "get") else None
        if ra:
            try:
                wait = float(ra)
            except ValueError:
                pass
    time.sleep(max(wait, 0.0))


def should_retry_status(status_code: int, retry: RetryCfg = _DEFAULT_RETRY) -> bool:
    return status_code in set(retry.retry_on_status)


def exec_with_concurrent_retry(
    func: Callable[..., T],
    *,
    max_attempts: int = 8,
    base_backoff_seconds: float = 1.0,
    max_backoff_seconds: float = 10.0,
    **kwargs: Any,
) -> T:
    """Retry control-plane writes on Delta concurrent-append conflicts.

    Full-jitter exponential backoff (``random.uniform(0, cap)``), not a fixed
    schedule: N parallel Job tasks that collide on attempt 1 at the same
    instant would, with a fixed delay, all sleep the same amount and collide
    again in lockstep on attempt 2. Randomizing the wait spreads retries
    apart so contention actually drains as attempts progress, instead of the
    whole herd re-colliding every round.
    """
    for attempt in range(max_attempts):
        try:
            return func(**kwargs) if kwargs else func()
        except Exception as err:
            err_str = str(err)
            concurrent = (
                "ConcurrentAppendException" in err_str
                or "DELTA_CONCURRENT" in err_str
                or "Concurrent" in err_str
            )
            if concurrent and attempt < max_attempts - 1:
                cap = min(max_backoff_seconds, base_backoff_seconds * (2 ** attempt))
                sleep_sec = random.uniform(0.0, cap)
                log.warning(
                    "Metadata concurrent write; retrying in %.1fs (attempt %s/%s)",
                    sleep_sec, attempt + 1, max_attempts,
                )
                print(
                    f"   [RETRY] Metadata table lock detected. Retrying in {sleep_sec:.1f}s...",
                    flush=True,
                )
                time.sleep(sleep_sec)
                continue
            raise
    raise RuntimeError("exec_with_concurrent_retry exhausted attempts")  # pragma: no cover
