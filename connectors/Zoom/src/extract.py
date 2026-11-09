"""Per-object Zoom extract logic.

All Zoom-specific knowledge lives in this single module: the endpoint
table, the two pagination styles, parent-id fan-out, and watermark
injection for endpoints that support server-side date filtering.

There is no plugin protocol. The function `extract(...)` is called once
per object by `pipeline.run()` and yields page-shaped dicts that the
writer turns into Spark rows.
"""
from __future__ import annotations

import logging
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, Iterator, List, Mapping, Optional
from urllib.parse import quote

from src.config import ObjectCfg, SourceCfg
from src.http import HttpClient


log = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Endpoint table — one entry per logical object name.
# ---------------------------------------------------------------------------
#
# Fields:
#   path            HTTP path under base_url; may contain a {parent_id}
#   records_path    JSON key holding the list of records in the response
#   pagination      'token' (next_page_token) | 'numbered' (page_number)
#   parent_id_field which field on the parent yields the value for {parent_id}
#   cursor_param    if set, this is the `from`/`to` param name pair on the API
#   cursor_format   'date' (YYYY-MM-DD) | 'datetime' (ISO-8601 UTC)
#
ENDPOINTS: Dict[str, Dict[str, Any]] = {
    # ---- Space dimension (Medium tier) ----
    "users": {
        "path": "/users",
        "records_path": "users",
        "pagination": "token",
    },
    "groups": {
        "path": "/groups",
        "records_path": "groups",
        "pagination": "token",
    },
    "rooms": {
        "path": "/rooms",
        "records_path": "rooms",
        "pagination": "token",
    },
    "room_locations": {
        "path": "/rooms/locations",
        "records_path": "locations",
        "pagination": "token",
    },
    "room_devices": {
        # Zoom has no account-wide bulk devices endpoint on the
        # zoom_rooms:* scope family (the /devices Device Management API
        # needs device:* scopes). So we fan out one API call per room.
        "path": "/rooms/{parent_id}/devices",
        "records_path": "devices",
        "pagination": "token",
        "parent_id_field": "id",
    },
    # ---- Booking facts — PRIMARY path (Light + Medium tier) ----
    # Per-user meetings list. YAML must set query_params.type=previous_meetings
    # to fetch past meetings for capacity analysis (or 'scheduled' / 'upcoming'
    # for forecasting).
    #
    # `parent_id_field` here controls which field is CACHED as parent_ids for
    # downstream fan-outs (past_meeting_participants). It must be `uuid`, not
    # numeric `id`: Zoom's /past_meetings/{id}/participants endpoint returns
    # participants from the MOST RECENT occurrence only when called with the
    # numeric id, so recurring-meeting historical occurrences would be lost.
    # Using `uuid` fetches per-occurrence participants correctly.
    "meetings": {
        "path": "/users/{parent_id}/meetings",
        "records_path": "meetings",
        "pagination": "token",
        "parent_id_field": "uuid",
    },
    # Report-API participants (numbered pagination, quota-sensitive) — kept
    # for backward compat / callers on classic scopes. Prefer
    # `past_meeting_participants` below on granular scopes.
    "meeting_participants": {
        "path": "/report/meetings/{parent_id}/participants",
        "records_path": "participants",
        "pagination": "numbered",
        "parent_id_field": "id",
    },
    # Preferred participant path — Light tier, token pagination, works with
    # `meeting:read:list_past_participants:admin`. Parent is `meetings`,
    # fan-out uses numeric `id` (accepts UUID too but numeric avoids encoding).
    "past_meeting_participants": {
        "path": "/past_meetings/{parent_id}/participants",
        "records_path": "participants",
        "pagination": "token",
        "parent_id_field": "id",
    },
    # ---- Dashboard enrichment — OPTIONAL (Heavy tier) ----
    # Account-wide list of past meetings with rich telemetry fields
    # (mmr_address, cipher, in_room_participants, has_*).
    # YAML must set: cursor_param_from=from, cursor_param_to=to,
    # cursor_format=date, query_params.type=past, lookback_minutes>0.
    "dashboard_meetings": {
        "path": "/metrics/meetings",
        "records_path": "meetings",
        "pagination": "token",
        # Cache `uuid` (not numeric `id`) for downstream fan-out. See the
        # note on `meetings` above: /metrics/meetings/{id}/participants
        # with numeric id collapses recurring-meeting occurrences into the
        # most recent one. Use uuid to fetch per-occurrence participants.
        "parent_id_field": "uuid",
    },
    # Dashboard participants — includes `device` field, so callers can derive
    # physical (device == "Zoom Rooms") vs virtual attendees.
    "dashboard_meeting_participants": {
        "path": "/metrics/meetings/{parent_id}/participants",
        "records_path": "participants",
        "pagination": "token",
        "parent_id_field": "id",
    },
}


PAGE_SIZE = 300


# ---------------------------------------------------------------------------
# extract — the only public function
# ---------------------------------------------------------------------------

def extract(
    obj: ObjectCfg,
    source: SourceCfg,
    http: HttpClient,
    *,
    last_watermark: Optional[str],
    parent_ids: Optional[List[str]] = None,
) -> Iterator[Dict[str, Any]]:
    """Yield page dicts for one object.

    Each yielded page is:
        {
          "records":   [<dict>, ...],
          "page":      int,
          "max_cursor": Optional[str],   # max cursor_field value seen, ISO-8601
        }

    For fan-out objects (`parent` set in YAML), `parent_ids` must be the
    list of parent ids harvested in the same run. The caller is
    responsible for running `users` before `meetings`, etc.
    """
    spec = ENDPOINTS.get(obj.name)
    if spec is None:
        raise KeyError(f"unknown Zoom object '{obj.name}' (known: {sorted(ENDPOINTS)})")

    is_fanout = "{parent_id}" in spec["path"]
    if is_fanout and not parent_ids:
        log.warning("object=%s requires parent ids but none cached; yielding nothing", obj.name)
        return

    base_params = dict(obj.query_params or {})
    base_params.setdefault("page_size", PAGE_SIZE)
    base_params.update(_watermark_params(obj, last_watermark))

    if is_fanout:
        yield from _paginate_fanout(http, obj, spec, base_params, source, parent_ids)
    else:
        url = source.base_url + spec["path"]
        yield from _paginate(http, url, base_params, spec, obj)


# ---------------------------------------------------------------------------
# parallel fan-out
# ---------------------------------------------------------------------------

# Concurrent HTTP workers per fan-out object. The shared _Bucket in HttpClient
# enforces the QPS ceiling across workers, so raising this above rate_limit_qps
# gives no extra throughput but does help absorb 502-backoff dead time.
# Effective speedup on our workloads: ~5-8x on large fan-outs (meetings,
# past_meeting_participants, dashboard_meeting_participants).
_HTTP_MAX_WORKERS = 8


def _encode_zoom_parent_id(pid: str) -> str:
    """URL-encode a parent id for insertion into a Zoom API path segment.

    Numeric ids (users.id, rooms.id, meetings.id) are unchanged. UUIDs and
    other strings containing reserved URL characters are single-URL-encoded.

    Special case documented by Zoom: if a meeting UUID starts with '/' or
    contains '//', the UUID must be **double** URL-encoded before being sent
    as a path parameter, otherwise the request 404s or hits the wrong route.
    Ref: developers.zoom.us — 'Using Zoom APIs' → 'Meeting ID and UUID'.
    """
    if pid.startswith("/") or "//" in pid:
        return quote(quote(pid, safe=""), safe="")
    return quote(pid, safe="")


def _paginate_fanout(http, obj, spec, base_params, source, parent_ids):
    """Fan out over parent_ids, paginating each in a worker thread.

    Each worker paginates one parent's URL end-to-end (list of pages),
    stamps `parent_id_column` on every record, and returns the pages.
    We yield pages as workers complete — order is not deterministic but
    downstream MERGE / cursor / parent_id_cache are all order-agnostic.
    """
    def _one_parent(pid):
        url = source.base_url + spec["path"].replace("{parent_id}", _encode_zoom_parent_id(str(pid)))
        pages = list(_paginate(http, url, base_params, spec, obj))
        # Zoom's fan-out endpoints (/rooms/{id}/devices,
        # /past_meetings/{id}/participants, /metrics/meetings/{id}/participants)
        # put the parent id in the URL but do not include it in each returned
        # record. Stamp it in so downstream MERGE-on-PK works when the child's
        # primary_key references the parent id column.
        if obj.parent_id_column:
            for page in pages:
                for record in page["records"]:
                    record.setdefault(obj.parent_id_column, str(pid))
        return pages

    n_workers_cap = obj.max_workers if obj.max_workers is not None else _HTTP_MAX_WORKERS
    n_workers = min(n_workers_cap, max(1, len(parent_ids)))
    if n_workers <= 1:
        # Single-parent or single-worker: keep sequential path so tests
        # that observe call order stay deterministic.
        for pid in parent_ids:
            yield from _one_parent(pid)
        return

    # Deferred import so tests that don't exercise fan-out don't pay for it.
    import concurrent.futures

    with concurrent.futures.ThreadPoolExecutor(
        max_workers=n_workers, thread_name_prefix=f"zoom-{obj.name}"
    ) as pool:
        futures = [pool.submit(_one_parent, pid) for pid in parent_ids]
        for fut in concurrent.futures.as_completed(futures):
            # Any HttpError etc. from a worker re-raises here and aborts
            # the object (caller in pipeline._run_one_object catches it).
            pages = fut.result()
            yield from pages


# ---------------------------------------------------------------------------
# pagination
# ---------------------------------------------------------------------------

def _paginate(
    http: HttpClient,
    url: str,
    base_params: Mapping[str, Any],
    spec: Mapping[str, Any],
    obj: ObjectCfg,
) -> Iterator[Dict[str, Any]]:
    if spec["pagination"] == "token":
        yield from _paginate_token(http, url, base_params, spec, obj)
    elif spec["pagination"] == "numbered":
        yield from _paginate_numbered(http, url, base_params, spec, obj)
    else:
        raise ValueError(f"unknown pagination style: {spec['pagination']}")


def _paginate_token(http, url, base_params, spec, obj):
    token: Optional[str] = None
    page = 0
    while True:
        page += 1
        params = dict(base_params)
        if token:
            params["next_page_token"] = token
        body = http.get(url, params=params, max_attempts=obj.max_retry_attempts) or {}
        records = list(body.get(spec["records_path"]) or [])
        yield {
            "records": records,
            "page": page,
            "max_cursor": _max_cursor(records, obj.cursor_field),
            "bytes": getattr(http, "last_response_bytes", 0),
        }
        # Zoom returns '' (not absent) on the last page — treat as falsy.
        token = body.get("next_page_token") or None
        if not token:
            return


def _paginate_numbered(http, url, base_params, spec, obj):
    page_number = 1
    page_count = 1
    while page_number <= page_count:
        params = dict(base_params)
        params["page_number"] = page_number
        body = http.get(url, params=params, max_attempts=obj.max_retry_attempts) or {}
        page_count = int(body.get("page_count") or 1)
        records = list(body.get(spec["records_path"]) or [])
        yield {
            "records": records,
            "page": page_number,
            "max_cursor": _max_cursor(records, obj.cursor_field),
            "bytes": getattr(http, "last_response_bytes", 0),
        }
        page_number += 1


# ---------------------------------------------------------------------------
# helpers
# ---------------------------------------------------------------------------

def _watermark_params(obj: ObjectCfg, last_watermark: Optional[str]) -> Dict[str, Any]:
    """Build server-side from/to params for endpoints that accept them.

    Emitted only when the ObjectCfg declares BOTH `cursor_param_from` and
    `cursor_param_to`. The `from` value is `last_watermark - lookback_minutes`
    (falling back to `now - 1 day` when no prior watermark exists); the `to`
    value is `now`. Format is controlled by `obj.cursor_format`:
      'date'      -> YYYY-MM-DD              (Dashboard, Report)
      'datetime'  -> ISO-8601 UTC with 'Z'   (call-log style APIs)

    For endpoints without cursor_param_from/to declared, returns {}.
    """
    if not obj.cursor_param_from or not obj.cursor_param_to:
        return {}

    now = datetime.now(timezone.utc)
    fmt_date = "%Y-%m-%d"
    fmt_dt = "%Y-%m-%dT%H:%M:%SZ"

    lookback = max(obj.lookback_minutes or 0, 0)

    from_dt: Optional[datetime] = None
    if last_watermark:
        # Watermark may be date-only or ISO-8601. Best-effort parse.
        try:
            if len(last_watermark) == 10:
                from_dt = datetime.strptime(last_watermark, fmt_date).replace(tzinfo=timezone.utc)
            else:
                from_dt = datetime.strptime(
                    last_watermark.replace("Z", "+00:00"), "%Y-%m-%dT%H:%M:%S%z"
                ).astimezone(timezone.utc)
            from_dt -= timedelta(minutes=lookback)
        except ValueError:
            from_dt = None

    if from_dt is None:
        # No prior watermark — safe default: yesterday.
        from_dt = now - timedelta(days=1)

    fmt = fmt_date if obj.cursor_format == "date" else fmt_dt
    return {
        obj.cursor_param_from: from_dt.strftime(fmt),
        obj.cursor_param_to: now.strftime(fmt),
    }


def _max_cursor(records, field: Optional[str]) -> Optional[str]:
    if not field or not records:
        return None
    values = [str(r[field]) for r in records if r.get(field) is not None]
    return max(values) if values else None


def parent_ids_from_pages(pages: Iterator[Dict[str, Any]], parent_id_field: str) -> List[str]:
    """Drain `pages` and return the list of parent-id field values seen.

    Used by `pipeline.run()` to harvest e.g. `users.id` for the meetings
    fan-out, while ALSO forwarding the same pages to the writer.
    Implemented as a simple consumer because the caller has already
    iterated and persisted the records.
    """
    ids: List[str] = []
    for page in pages:
        for r in page["records"]:
            v = r.get(parent_id_field)
            if v is not None:
                ids.append(str(v))
    return ids


def check_required_scopes(objects, granted_scopes) -> List[str]:
    """Return the list of MANDATORY (non-optional) object names whose
    required_scope is NOT granted. Optional objects are excluded — the
    pipeline handles their missing-scope case by skipping with a warning.
    """
    missing = []
    for o in objects:
        if getattr(o, "optional", False):
            continue
        if o.required_scope and o.required_scope not in granted_scopes:
            missing.append(o.name)
    return missing


def _now_utc_iso() -> str:
    """Return current UTC time as ISO-8601 with Z suffix (test-friendly hook)."""
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
