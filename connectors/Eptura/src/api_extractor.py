"""Eptura iOffice pagination.

Every entity is a flat collection at `{base_url}/{entity}` that pages with
`limit` + `startAt` offsets, per https://ioffice.github.io/api/#header-pagination

Two things about this API drive the code below:

  * `page` / `pageSize` are silently ignored and the server then caps the
    response at 50 records, so `limit` / `startAt` must always be sent.
  * Endpoints return either a bare JSON list or an envelope shaped
    `{"response": [...], "total": N}`. Both are handled.
"""
from __future__ import annotations

import logging
from dataclasses import dataclass, field
from typing import Any, Dict, Iterator, List, Mapping, Optional, Sequence, Tuple

from src.connector_config import ObjectCfg, SourceCfg
from src.http_client import HttpClient, HttpError


log = logging.getLogger(__name__)


@dataclass
class _HydrateStats:
    overlaid: int = 0
    http_failed: int = 0
    unwrap_empty: int = 0
    missing_id: int = 0
    with_floor: int = 0
    sample: Optional[Tuple[Any, Any]] = field(default=None)


def fetch_pages(
    obj: ObjectCfg,
    source: SourceCfg,
    http: HttpClient,
) -> Iterator[Tuple[List[Dict[str, Any]], int]]:
    """Yield `(records, response_bytes)` for each page of one entity.

    `response_bytes` is the list page plus every item GET when
    `obj.fetch_detail` is set. (Counting only `http.last_response_bytes`
    after hydration would report the last item body, ~hundreds of bytes.)
    """
    url = f"{source.base_url}/{obj.name}"
    page_size = source.page_size
    start_at = 0

    while True:
        payload = http.get_json(url, params={"limit": page_size, "startAt": start_at})
        records, total = unwrap_payload(payload)
        if not records:
            return

        page_bytes = http.last_response_bytes
        if obj.fetch_detail:
            records, detail_bytes, stats = _hydrate_records(http, url, records)
            page_bytes += detail_bytes
            log.info(
                "%s detail hydrate: overlaid=%d http_failed=%d unwrap_empty=%d "
                "missing_id=%d with_floor=%d sample=%s (%d list bytes + %d detail bytes)",
                obj.name, stats.overlaid, stats.http_failed, stats.unwrap_empty,
                stats.missing_id, stats.with_floor, stats.sample,
                page_bytes - detail_bytes, detail_bytes,
            )

        yield records, page_bytes

        start_at += len(records)
        if total is not None and start_at >= total:
            return
        # A short page means we've reached the end of the collection.
        if len(records) < page_size:
            return


def describe_floor_coverage(records: Sequence[Mapping[str, Any]]) -> str:
    """One notebook line: how many hydrated rows carry `floor.id`."""
    n = len(records)
    with_floor = 0
    sample: Optional[Tuple[Any, Any]] = None
    for row in records:
        floor = row.get("floor")
        if isinstance(floor, dict) and floor.get("id") is not None:
            with_floor += 1
            if sample is None:
                sample = (row.get("id"), floor.get("id"))
    line = f"{with_floor}/{n} rows have floor.id"
    if sample:
        line += f" (sample id={sample[0]} floor.id={sample[1]})"
    return line


def _hydrate_records(
    http: HttpClient,
    collection_url: str,
    records: List[Dict[str, Any]],
) -> Tuple[List[Dict[str, Any]], int, _HydrateStats]:
    stats = _HydrateStats()
    detail_bytes = 0
    hydrated: List[Dict[str, Any]] = []
    for record in records:
        row, bytes_read, outcome = _hydrate_detail(http, collection_url, record)
        detail_bytes += bytes_read
        if outcome == "overlaid":
            stats.overlaid += 1
        elif outcome == "http_failed":
            stats.http_failed += 1
        elif outcome == "unwrap_empty":
            stats.unwrap_empty += 1
        else:
            stats.missing_id += 1
        floor = row.get("floor")
        if isinstance(floor, dict) and floor.get("id") is not None:
            stats.with_floor += 1
            if stats.sample is None:
                stats.sample = (row.get("id"), floor.get("id"))
        hydrated.append(row)
    return hydrated, detail_bytes, stats


def _hydrate_detail(
    http: HttpClient,
    collection_url: str,
    record: Dict[str, Any],
) -> Tuple[Dict[str, Any], int, str]:
    """Overlay GET /{collection}/{id} onto a list row when that call succeeds.

    The neighborhoods collection omits `floor`; the item endpoint returns it
    and that nested `floor.id` is the FK used to join neighborhoods to rooms.
    A failed detail fetch keeps the list row so one 404 cannot drop the page.

    Returns `(record, response_bytes, outcome)` where outcome is
    `overlaid` | `http_failed` | `unwrap_empty` | `missing_id`.
    """
    record_id = record.get("id")
    if record_id is None:
        return record, 0, "missing_id"
    try:
        payload = http.get_json(f"{collection_url}/{record_id}")
    except HttpError as exc:
        log.warning("detail fetch %s/%s failed (%s); keeping list payload",
                    collection_url, record_id, exc)
        return record, 0, "http_failed"
    bytes_read = http.last_response_bytes
    detail = _unwrap_item(payload)
    if not detail:
        log.warning(
            "detail unwrap empty for %s/%s; payload_type=%s keys=%s; keeping list payload",
            collection_url, record_id, type(payload).__name__,
            list(payload)[:12] if isinstance(payload, dict) else None,
        )
        return record, bytes_read, "unwrap_empty"
    # List rows are a subset; overlay so a nested `floor` from the item
    # call is added without dropping fields the collection already had.
    return {**record, **detail}, bytes_read, "overlaid"


def _unwrap_item(payload: Any) -> Optional[Dict[str, Any]]:
    """Accept a bare object or the collection envelope around one object.

    GET /neighborhoods/{id} on this API often returns
    `{"response": [ {...} ]}` — the same wrapper as the list call. Treating
    a list `response` as a failed unwrap kept the slim list row and dropped
    `floor`.
    """
    if isinstance(payload, dict) and "response" in payload:
        inner = payload["response"]
        if isinstance(inner, dict):
            return inner
        if isinstance(inner, list) and inner and isinstance(inner[0], dict):
            return inner[0]
        return None
    return payload if isinstance(payload, dict) else None


def unwrap_payload(payload: Any) -> Tuple[List[Dict[str, Any]], Optional[int]]:
    """Split an Eptura response into `(records, total)`, accepting either the
    bare-list or `{"response": [...], "total": N}` shape."""
    if isinstance(payload, list):
        return payload, None
    if isinstance(payload, dict):
        total = payload.get("total")
        return payload.get("response", []), None if total is None else int(total)
    return [], None
