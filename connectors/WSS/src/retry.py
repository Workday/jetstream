"""Shared retry helpers for WSS HTTP calls and control-plane writes."""
from __future__ import annotations

import logging
import random
import time
from typing import Any, Callable, TypeVar
from src.config import RetryCfg

log = logging.getLogger(__name__)
T = TypeVar("T")
_DEFAULT_RETRY = RetryCfg()

def sleep_before_retry(attempt: int, *, retry: RetryCfg = _DEFAULT_RETRY, response: Any = None) -> None:
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
    max_attempts: int = 10,
    base_backoff_seconds: float = 15.0,
    max_backoff_seconds: float = 20.0,
    **kwargs: Any,
) -> T:
    """Full-jitter retry on Delta/Iceberg concurrent append locks (15-20s sleep)."""
    for attempt in range(max_attempts):
        try:
            return func(**kwargs) if kwargs else func()
        except Exception as err:
            err_str = str(err)
            concurrent = ("ConcurrentAppendException" in err_str or "DELTA_CONCURRENT" in err_str or "Concurrent" in err_str or "AnalysisException" in err_str)
            if concurrent and attempt < max_attempts - 1:
                sleep_sec = random.uniform(base_backoff_seconds, max_backoff_seconds)
                print(f"   [RETRY {attempt+1}/{max_attempts}] Lock contention detected. Retrying in {sleep_sec:.1f}s...")
                time.sleep(sleep_sec)
            else:
                raise
    raise RuntimeError("exec_with_concurrent_retry exhausted max attempts")