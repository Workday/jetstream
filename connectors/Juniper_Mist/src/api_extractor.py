"""Per-object Juniper Mist extraction logic.

All Mist-specific knowledge lives here: the endpoint table, the four
pagination styles, fan-out mechanics, watermark injection, and record
normalization (field remapping, derived columns, JSON serialisation of
nested structures).

The only public function is `extract(obj, source_cfg, http_client, ...)`,
which yields `PageDict` items that `pipeline_orchestrator` accumulates and
passes to `destination_writer`.

Pagination styles:
  page_limit    — GET ?page=N&limit=1000; stop when len(records) < limit
  search_after  — GET ?search_after=...; stop when `next` absent or empty
  single_object — one GET; wrap the response dict in a one-element list
  count         — loop over `distinct_values`; read `results` list per call
"""
from __future__ import annotations

import concurrent.futures
import json
import logging
import time
from datetime import datetime, timezone
from typing import Any, Dict, Iterator, List, Mapping, Optional, Union
from urllib.parse import parse_qs, urlparse

from src.connector_config import ObjectCfg, SourceCfg
from src.http_client import HttpClient


log = logging.getLogger(__name__)

PAGE_SIZE = 1000

# ---------------------------------------------------------------------------
# Endpoint table
# ---------------------------------------------------------------------------
#
# Keys:
#   path               URL path template; {org_id} or {parent_id} are substituted
#   pagination         'page_limit' | 'search_after' | 'single_object'
#   records_path       key in the response dict holding the list (None = top-level array)
#   parent_id_field    field on the parent record whose value goes in {parent_id}
#   parent_cache_keys  when set, cache parent for double-fanout as a dict of field mappings:
#                        {cache_key: record_field, ...}
#                        e.g. {"site_id": "site_id", "map_id": "id"}
#   id_remap           {old_key: new_key} — rename fields before writing
#   derived_fields     {col_name: callable(record) -> value} — synthesise columns
#   user_types         for zone_visits endpoints: loop over these values for ?user_type param
#   distinct_values    for count endpoints: loop over these values for ?distinct param

ENDPOINTS: Dict[str, Dict[str, Any]] = {
    # --- Org-level, single object ---
    "org": {
        "path": "/orgs/{org_id}",
        "pagination": "single_object",
    },
    # --- Org-level, page/limit ---
    "sitegroups": {
        "path": "/orgs/{org_id}/sitegroups",
        "pagination": "page_limit",
    },
    "inventory": {
        "path": "/orgs/{org_id}/inventory",
        "pagination": "page_limit",
    },
    "device_stats": {
        "path": "/orgs/{org_id}/stats/devices",
        "pagination": "page_limit",
        # Mist stats responses use `_id` instead of `id`; remap so the PK column is `id`.
        "id_remap": {"_id": "id"},
    },
    "org_wlans": {
        "path": "/orgs/{org_id}/wlans",
        "pagination": "page_limit",
    },
    "sites": {
        "path": "/orgs/{org_id}/sites",
        "pagination": "page_limit",
    },
    # --- Per-site fan-out, page/limit ---
    "maps": {
        "path": "/sites/{parent_id}/maps",
        "pagination": "page_limit",
        "parent_id_field": "id",
        # Cache as {site_id, map_id} dicts for the double-fanout children.
        "parent_cache_keys": {"site_id": "site_id", "map_id": "id"},
    },
    "zones": {
        "path": "/sites/{parent_id}/zones",
        "pagination": "page_limit",
        "parent_id_field": "id",
    },
    "zone_stats": {
        "path": "/sites/{parent_id}/stats/zones",
        "pagination": "page_limit",
        "parent_id_field": "id",
    },
    "site_wlans": {
        "path": "/sites/{parent_id}/wlans",
        "pagination": "page_limit",
        "parent_id_field": "id",
    },
    "mapstacks": {
        "path": "/sites/{parent_id}/mapstacks",
        "pagination": "page_limit",
        "parent_id_field": "id",
    },
    "rssizones": {
        "path": "/sites/{parent_id}/rssizones",
        "pagination": "page_limit",
        "parent_id_field": "id",
    },
    "rssizone_stats": {
        "path": "/sites/{parent_id}/stats/rssizones",
        "pagination": "page_limit",
        "parent_id_field": "id",
    },
    "guests": {
        "path": "/sites/{parent_id}/guests",
        "pagination": "page_limit",
        "parent_id_field": "id",
    },
    # --- Per-site fan-out, single object (one record per site) ---
    "site_settings": {
        "path": "/sites/{parent_id}/setting",
        "pagination": "single_object",
        "parent_id_field": "id",
    },
    # --- Per-site fan-out, live snapshots (filtered sites) ---
    "clients": {
        "path": "/sites/{parent_id}/stats/clients",
        "pagination": "page_limit",
        "parent_id_field": "id",
    },
    "assets": {
        "path": "/sites/{parent_id}/stats/assets",
        "pagination": "page_limit",
        "parent_id_field": "id",
    },
    "sdkclients": {
        "path": "/sites/{parent_id}/stats/sdkclients",
        "pagination": "page_limit",
        "parent_id_field": "id",
    },
    # --- Per-site fan-out, search_after (incremental history) ---
    "zone_visits": {
        "path": "/sites/{parent_id}/zones/visits/search",
        "pagination": "search_after",
        "records_path": "results",
        "parent_id_field": "id",
        # Loop over user_type values; each is a separate search call.
        "user_types": ["client", "sdkclient", "asset"],
        # Fixed query param for site-scoped visits.
        "fixed_params": {"scope": "site"},
        # Derived columns: enter_epoch for PK stability (int milliseconds).
        "derived_fields": {
            "enter_epoch": lambda r: str(int(float(r.get("enter", 0)) * 1000)),
        },
    },
    "zone_visits_by_zone": {
        "path": "/sites/{parent_id}/zones/visits/search",
        "pagination": "search_after",
        "records_path": "results",
        "parent_id_field": "id",
        "user_types": ["client", "sdkclient", "asset"],
        "fixed_params": {"scope": "zone"},
        "derived_fields": {
            "enter_epoch": lambda r: str(int(float(r.get("enter", 0)) * 1000)),
            # zone_id is an alias for scope_id on zone-scoped visits.
            "zone_id": lambda r: r.get("scope_id"),
        },
    },
    "clients_search": {
        "path": "/sites/{parent_id}/clients/search",
        "pagination": "search_after",
        "records_path": "results",
        "parent_id_field": "id",
    },
    "guests_search": {
        "path": "/sites/{parent_id}/guests/search",
        "pagination": "search_after",
        "records_path": "results",
        "parent_id_field": "id",
    },
    # --- Double fan-out: per map within a site ---
    "map_clients": {
        # {site_id} and {map_id} come from the dict parents cached for `maps`.
        "path": "/sites/{site_id}/stats/maps/{map_id}/clients",
        "pagination": "page_limit",
    },
    "unconnected_clients": {
        "path": "/sites/{site_id}/stats/maps/{map_id}/unconnected_clients",
        "pagination": "page_limit",
    },
}


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------

PageDict = Dict[str, Any]


def extract(
    obj: ObjectCfg,
    source_cfg: SourceCfg,
    http_client: HttpClient,
    *,
    last_watermark: Optional[str],
    parent_ids: Optional[List[Any]] = None,
) -> Iterator[PageDict]:
    """Yield page dicts for one object.

    Each yielded page:
        {
          "records":    [dict, ...],
          "bytes":      int,
          "max_cursor": Optional[str],   # max cursor_field value as ISO-8601 UTC
          "page":       int,
        }

    For fan-out objects, `parent_ids` must be the list of parent IDs (or
    dicts for double fan-out) harvested in the same run. The caller (pipeline)
    is responsible for running parent objects before children.
    """
    spec = ENDPOINTS.get(obj.name)
    if spec is None:
        raise KeyError(
            f"unknown Juniper Mist object '{obj.name}' (known: {sorted(ENDPOINTS)})"
        )

    base_params: Dict[str, Any] = {}
    # Apply fixed params from the endpoint spec.
    base_params.update(spec.get("fixed_params", {}))
    # Apply per-object query params from ObjectCfg.
    base_params.update(obj.query_params or {})

    is_org_level = "{org_id}" in spec["path"]

    if is_org_level:
        url = source_cfg.base_url + spec["path"].replace("{org_id}", source_cfg.org_id)
        if spec["pagination"] == "single_object":
            yield from _paginate_single_object(http_client, url, base_params, spec, obj)
        else:
            base_params.setdefault("limit", PAGE_SIZE)
            yield from _paginate_page_limit(http_client, url, base_params, spec, obj)
        return

    # Fan-out object.
    if not parent_ids:
        log.warning(
            "object=%s requires parent_ids but none available; skipping", obj.name
        )
        return

    base_params.setdefault("limit", PAGE_SIZE)
    yield from _paginate_fanout(
        http_client, source_cfg, base_params, spec, obj,
        parent_ids=parent_ids,
        last_watermark=last_watermark,
    )


# ---------------------------------------------------------------------------
# Fan-out dispatcher
# ---------------------------------------------------------------------------

def _paginate_fanout(
    http_client: HttpClient,
    source_cfg: SourceCfg,
    base_params: Dict[str, Any],
    spec: Dict[str, Any],
    obj: ObjectCfg,
    parent_ids: List[Any],
    last_watermark: Optional[str],
) -> Iterator[PageDict]:
    """Fan out over `parent_ids`, paginating each end-to-end in a worker thread."""

    def _one_parent(parent_id: Any) -> List[PageDict]:
        url = _build_url(spec, source_cfg, parent_id)
        params = dict(base_params)
        # Inject watermark params for incremental objects.
        params.update(_watermark_params(obj, last_watermark, source_cfg))

        if spec["pagination"] == "single_object":
            pages = list(_paginate_single_object(http_client, url, params, spec, obj))
        elif spec["pagination"] == "search_after":
            pages = list(_paginate_search_after(http_client, url, params, spec, obj))
        else:
            pages = list(_paginate_page_limit(http_client, url, params, spec, obj))

        # Stamp parent_id_column on every record so downstream MERGE has the PK.
        if obj.parent_id_column:
            pid_val = _parent_id_value(parent_id, obj.parent_id_column)
            for page in pages:
                for record in page["records"]:
                    record.setdefault(obj.parent_id_column, pid_val)

        # For double fan-out (map_clients), also stamp site_id from the dict.
        if isinstance(parent_id, dict) and "site_id" in parent_id:
            for page in pages:
                for record in page["records"]:
                    record.setdefault("site_id", str(parent_id["site_id"]))

        return pages

    max_workers_cap = obj.max_workers if obj.max_workers is not None else source_cfg.fan_out_max_workers
    n_workers = min(max_workers_cap, max(1, len(parent_ids)))

    if n_workers <= 1:
        for pid in parent_ids:
            yield from _one_parent(pid)
        return

    with concurrent.futures.ThreadPoolExecutor(
        max_workers=n_workers, thread_name_prefix=f"mist-{obj.name}"
    ) as pool:
        futures = [pool.submit(_one_parent, pid) for pid in parent_ids]
        for fut in concurrent.futures.as_completed(futures):
            yield from fut.result()


def _build_url(spec: Dict[str, Any], source_cfg: SourceCfg, parent_id: Any) -> str:
    """Build a full URL by substituting path placeholders from parent_id."""
    path = spec["path"]
    if isinstance(parent_id, dict):
        # Double fan-out: parent_id is a dict with named keys matching placeholders.
        url = source_cfg.base_url + path
        for key, val in parent_id.items():
            url = url.replace(f"{{{key}}}", str(val))
        return url
    return source_cfg.base_url + path.replace("{parent_id}", str(parent_id))


def _parent_id_value(parent_id: Any, column: str) -> str:
    """Extract the stamping value for parent_id_column."""
    if isinstance(parent_id, dict):
        # For double fan-out, stamp the column that matches (e.g. map_id).
        return str(parent_id.get(column, parent_id.get("map_id", "")))
    return str(parent_id)


# ---------------------------------------------------------------------------
# Pagination styles
# ---------------------------------------------------------------------------

def _paginate_page_limit(
    http_client: HttpClient,
    url: str,
    base_params: Mapping[str, Any],
    spec: Dict[str, Any],
    obj: ObjectCfg,
) -> Iterator[PageDict]:
    page_num = 1
    while True:
        params = dict(base_params)
        params["page"] = page_num
        body = http_client.get(url, params=params, max_attempts=obj.max_retry_attempts)
        records = _extract_records(body, spec)
        records = [_normalize_record(r, spec) for r in records]
        yield {
            "records": records,
            "page": page_num,
            "max_cursor": _max_cursor(records, obj.cursor_field),
            "bytes": getattr(http_client, "last_response_bytes", 0),
        }
        if len(records) < base_params.get("limit", PAGE_SIZE):
            return
        page_num += 1


def _paginate_search_after(
    http_client: HttpClient,
    url: str,
    base_params: Mapping[str, Any],
    spec: Dict[str, Any],
    obj: ObjectCfg,
) -> Iterator[PageDict]:
    """Search-after pagination with optional user_type looping.

    For zone_visits endpoints, the spec declares `user_types`; we loop over
    each and yield pages across all user types as a flat stream.
    """
    user_types = spec.get("user_types")
    if user_types:
        page_num = 0
        for user_type in user_types:
            for page in _paginate_search_after_single(
                http_client, url, base_params, spec, obj,
                extra_params={"user_type": user_type},
                page_offset=page_num,
            ):
                page_num += 1
                yield page
    else:
        yield from _paginate_search_after_single(http_client, url, base_params, spec, obj)


def _paginate_search_after_single(
    http_client: HttpClient,
    url: str,
    base_params: Mapping[str, Any],
    spec: Dict[str, Any],
    obj: ObjectCfg,
    extra_params: Optional[Dict[str, Any]] = None,
    page_offset: int = 0,
) -> Iterator[PageDict]:
    search_after: Any = None
    page_num = page_offset

    while True:
        params = dict(base_params)
        if extra_params:
            params.update(extra_params)
        if search_after is not None:
            params["search_after"] = search_after

        body = http_client.get(url, params=params, max_attempts=obj.max_retry_attempts)
        if body is None:
            return

        records_raw = _extract_records(body, spec)
        records = [_normalize_record(r, spec) for r in records_raw]
        # Stamp the user_type so it appears on the record if it isn't already.
        if extra_params and "user_type" in extra_params:
            ut = extra_params["user_type"]
            for r in records:
                r.setdefault("user_type", ut)

        page_num += 1
        yield {
            "records": records,
            "page": page_num,
            "max_cursor": _max_cursor_epoch(records, obj.cursor_field),
            "bytes": getattr(http_client, "last_response_bytes", 0),
        }

        if not records:
            return
        nxt = body.get("next") if isinstance(body, dict) else None
        if not nxt:
            return
        search_after = _parse_search_after(nxt)
        if search_after is None:
            return


def _paginate_single_object(
    http_client: HttpClient,
    url: str,
    base_params: Mapping[str, Any],
    spec: Dict[str, Any],
    obj: ObjectCfg,
) -> Iterator[PageDict]:
    body = http_client.get(url, params=base_params or None, max_attempts=obj.max_retry_attempts)
    if body is None:
        return
    records = body if isinstance(body, list) else [body]
    records = [_normalize_record(r, spec) for r in records]
    yield {
        "records": records,
        "page": 1,
        "max_cursor": None,
        "bytes": getattr(http_client, "last_response_bytes", 0),
    }


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _extract_records(body: Any, spec: Dict[str, Any]) -> List[dict]:
    """Extract the record list from an API response body."""
    if body is None:
        return []
    records_path = spec.get("records_path")
    if records_path and isinstance(body, dict):
        raw = body.get(records_path)
    elif isinstance(body, list):
        raw = body
    elif isinstance(body, dict):
        # Fallback: try common wrapper keys then treat the whole dict as one record.
        raw = body.get("results") or body.get("data") or body.get("items")
        if raw is None:
            raw = [body]
    else:
        raw = []
    return list(raw) if raw else []


def _normalize_record(record: Any, spec: Dict[str, Any]) -> dict:
    """Apply field remapping and derived-field synthesis to a single record.

    Universal _jsonify is applied here so nested dicts/lists become JSON
    strings regardless of object type — no object-specific flattening logic.
    """
    if not isinstance(record, dict):
        return {"value": record}
    r = _jsonify(dict(record))
    # Remap field names (e.g. _id → id for device_stats).
    for old_key, new_key in spec.get("id_remap", {}).items():
        if old_key in r and new_key not in r:
            r[new_key] = r.pop(old_key)
    # Synthesise derived columns (e.g. enter_epoch, zone_id).
    for col_name, func in spec.get("derived_fields", {}).items():
        try:
            r[col_name] = func(r)
        except Exception:  # NOSONAR — derived field failure should not break the record
            r[col_name] = None
    return r


def _jsonify(row: dict) -> dict:
    """Serialise nested dicts/lists to JSON strings (universal, connector-agnostic).

    Spark's schema inference fails on nested types from heterogeneous pages.
    All nested values become strings so inferred schemas are always flat.
    """
    out = {}
    for k, v in row.items():
        if isinstance(v, (dict, list)):
            out[k] = json.dumps(v, default=str)
        else:
            out[k] = v
    return out


def _max_cursor(records: List[dict], field: Optional[str]) -> Optional[str]:
    """Return the max string value of cursor_field across records."""
    if not field or not records:
        return None
    values = [str(r[field]) for r in records if r.get(field) is not None]
    return max(values) if values else None


def _max_cursor_epoch(records: List[dict], field: Optional[str]) -> Optional[str]:
    """Return the max cursor value as an ISO-8601 UTC string.

    For Mist history endpoints the cursor field is a Unix epoch float.
    We store it as ISO-8601 so the Jetstream state table holds human-readable
    timestamps.
    """
    if not field or not records:
        return None
    values = []
    for r in records:
        v = r.get(field)
        if v is not None:
            try:
                values.append(float(v))
            except (TypeError, ValueError):
                pass
    if not values:
        return None
    max_epoch = max(values)
    return datetime.fromtimestamp(max_epoch, tz=timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def _watermark_params(
    obj: ObjectCfg,
    last_watermark: Optional[str],
    source_cfg: SourceCfg,
) -> Dict[str, Any]:
    """Build start/end epoch query params for incremental search endpoints.

    Returns an empty dict for full-load objects (no cursor_param_from declared).
    """
    if not obj.cursor_param_from or not obj.cursor_param_to:
        return {}

    now_epoch = int(time.time())
    lookback_sec = (obj.lookback_minutes or 0) * 60

    if last_watermark:
        try:
            dt = datetime.strptime(last_watermark, "%Y-%m-%dT%H:%M:%SZ").replace(tzinfo=timezone.utc)
            start_epoch = int(dt.timestamp()) - lookback_sec
        except ValueError:
            start_epoch = now_epoch - (source_cfg.lookback_days * 86400)
    else:
        start_epoch = now_epoch - (source_cfg.lookback_days * 86400)

    return {
        obj.cursor_param_from: start_epoch,
        obj.cursor_param_to: now_epoch,
    }


def _parse_search_after(nxt: Any) -> Any:
    """Extract the search_after value from a Mist `next` field.

    Mist returns `next` as a relative URL query string like:
      `?search_after=[...]&start=...&end=...`
    or as a bare JSON list.
    """
    if isinstance(nxt, list):
        return json.dumps(nxt)
    if not isinstance(nxt, str):
        return nxt
    if "search_after=" in nxt:
        query = urlparse(nxt).query if "://" in nxt else nxt.lstrip("?")
        values = parse_qs(query).get("search_after")
        return values[0] if values else nxt
    return nxt


# ---------------------------------------------------------------------------
# Parent-ID cache population helpers (called by pipeline_orchestrator)
# ---------------------------------------------------------------------------

def parent_cache_entries(
    obj_name: str,
    records: List[dict],
) -> List[Any]:
    """Return the values to append to parent_id_cache[obj_name].

    For most objects this is the list of `id` values.
    For `maps`, it returns dicts with {site_id, map_id} for double fan-out.
    """
    spec = ENDPOINTS.get(obj_name, {})
    cache_keys = spec.get("parent_cache_keys")
    if cache_keys:
        entries = []
        for r in records:
            entry = {}
            ok = True
            for cache_key, record_field in cache_keys.items():
                val = r.get(record_field)
                if not val:
                    ok = False
                    break
                entry[cache_key] = str(val)
            if ok:
                entries.append(entry)
        return entries
    pid_field = spec.get("parent_id_field", "id")
    return [str(r[pid_field]) for r in records if r.get(pid_field) is not None]
