"""Universal WSS API client with adaptive date-chunking, pagination, and retry logic."""
from __future__ import annotations

from datetime import datetime, timedelta, timezone
import logging
import time
from typing import Any, Dict, Generator, Optional
from urllib.parse import parse_qs, urlparse
import requests
from src.auth import WSSSession, get_auth_headers

log = logging.getLogger(__name__)


def _parse_iso(dt_str: str) -> datetime:
    """Parses ISO timestamp strings into UTC datetime objects."""
    clean_str = dt_str.replace("Z", "+00:00")
    dt = datetime.fromisoformat(clean_str)
    return dt if dt.tzinfo else dt.replace(tzinfo=timezone.utc)


def _fmt_iso(dt: datetime) -> str:
    """Formats UTC datetime objects into standard ISO strings."""
    return dt.astimezone(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def _split_url_and_params(raw_url: str, base_url: str = "https://api.us.workdayspend.com") -> tuple[str, Dict[str, Any]]:
    """Splits full URL into clean base URL and params dict, preserving all filters across cursor pages."""
    parsed = urlparse(raw_url)

    if not parsed.scheme or not parsed.netloc:
        base_parsed = urlparse(base_url)
        clean_url = f"{base_parsed.scheme}://{base_parsed.netloc}{parsed.path}"
    else:
        clean_url = f"{parsed.scheme}://{parsed.netloc}{parsed.path}"

    raw_params = parse_qs(parsed.query, keep_blank_values=True)

    flat_params: Dict[str, Any] = {}
    for k, v in raw_params.items():
        flat_params[k] = v[0] if isinstance(v, list) and len(v) == 1 else v

    return clean_url, flat_params


class WSSClient:
    def __init__(
        self,
        session: WSSSession,
        page_size: int = 100,
        max_retries: int = 10,
        retry_delay: int = 5,
        retry: Any = None,
    ):
        self.session = session
        self.page_size = page_size
        self.max_retries = max_retries
        self.retry_delay = retry_delay
        self.retry = retry

    def _request_with_retry(
        self,
        method: str,
        url: str,
        headers: Optional[Dict[str, str]] = None,
        params: Optional[Dict[str, Any]] = None,
    ) -> Any:
        """Executes API requests with exponential backoff on 429/5xx errors while failing fast on 4xx errors."""
        for attempt in range(1, self.max_retries + 1):
            try:
                resp = self.session.request(method, url, headers=headers, params=params)

                if isinstance(resp, dict) and "errors" in resp:
                    err_msg = str(resp["errors"])
                    log.error("[WSS API error payload] %s", err_msg)
                    raise requests.exceptions.HTTPError(f"API returned errors: {err_msg}")

                return resp
            except Exception as e:
                response_obj = getattr(e, "response", None)
                status_code = getattr(response_obj, "status_code", None)

                if status_code in (401, 403, 404, 422):
                    log.error("[HTTP %s Client Error] Unretryable failure on %s. Failing immediately.", status_code, url)
                    raise e

                if attempt < self.max_retries:
                    wait_time = self.retry_delay * attempt
                    if status_code == 429:
                        log.warning(
                            "[HTTP 429 Throttled] Rate limit hit on %s. Attempt %d/%d. Sleeping %ds...",
                            url, attempt, self.max_retries, wait_time
                        )
                    elif status_code and status_code >= 500:
                        log.warning(
                            "[HTTP %s Server Error] Failure on %s. Attempt %d/%d. Sleeping %ds...",
                            status_code, url, attempt, self.max_retries, wait_time
                        )
                    else:
                        log.warning(
                            "[Request Failure] %s on %s. Attempt %d/%d. Sleeping %ds...",
                            str(e), url, attempt, self.max_retries, wait_time
                        )
                    time.sleep(wait_time)
                else:
                    log.error("Maximum retries (%d) exceeded for URL: %s", self.max_retries, url)
                    raise e

    def extract_pages(
        self,
        obj_cfg: Any,
        start_time: Optional[str] = None,
        end_time: Optional[str] = None,
        parent_params: Optional[Dict[str, Any]] = None,
    ) -> Generator[Dict[str, Any], None, None]:
        """Paginates across standard REST, SCIM, and adaptive date-chunked endpoints."""
        endpoint = obj_cfg.endpoint

        if parent_params:
            for k, v in parent_params.items():
                endpoint = endpoint.replace(f"{{{k}}}", str(v))

        headers = get_auth_headers(obj_cfg.auth_pattern, self.session.credentials)
        is_scim = obj_cfg.auth_pattern == "pattern_2" or "/scim/" in endpoint
        object_name = getattr(obj_cfg, "object_name", getattr(obj_cfg, "name", "")).lower()
        api_type = getattr(obj_cfg, "api_type", "FULL")

        # ---------------------------------------------------------------------
        # 1. ALWAYS USE ADAPTIVE 3-DAY DATE-CHUNKING FOR 'events' (ISOLATED)
        # ---------------------------------------------------------------------
        is_events_object = (object_name == "events" or endpoint.rstrip("/").endswith("/events"))

        if is_events_object and not parent_params and api_type in ("INCR", "INCR_GET_ID"):
            effective_start = start_time or "2018-01-01T00:00:00Z"
            log.info("[EVENTS ADAPTIVE CHUNKER] Extracting 'events' in 3-day windows from %s...", effective_start)
            
            cur_start = _parse_iso(effective_start)
            max_end = _parse_iso(end_time) if end_time else datetime.now(timezone.utc)
            step = timedelta(days=3)
            page_num = 1
            base_url_str = f"{self.session.base_url.rstrip('/')}{endpoint}"

            while cur_start < max_end:
                cur_to = min(cur_start + step, max_end)
                params = {
                    "page[size]": self.page_size,
                    "filter[updated_at_from]": _fmt_iso(cur_start),
                    "filter[updated_at_to]": _fmt_iso(cur_to),
                }

                resp = self._request_with_retry("GET", base_url_str, headers=headers, params=params)
                body = resp if isinstance(resp, dict) else {}
                
                data = (
                    body.get("data")
                    or body.get("entries")
                    or body.get("Resources")
                    or body.get("resources")
                    or body.get(object_name)
                )
                if data is None:
                    raw_data = [v for k, v in body.items() if k != "links" and isinstance(v, list)]
                    records = raw_data[0] if raw_data else []
                else:
                    records = data if isinstance(data, list) else ([data] if data else [])

                links = body.get("links", {}) if isinstance(body, dict) else {}
                next_link = links.get("next") if isinstance(links, dict) else None

                if len(records) >= 100 or next_link:
                    if step > timedelta(hours=1):
                        step = step / 2
                        log.warning(
                            "[ADAPTIVE CHUNK REDUCE] %d records in window [%s - %s]. Shrinking step to %s.",
                            len(records), _fmt_iso(cur_start), _fmt_iso(cur_to), step
                        )
                        continue
                    else:
                        log.warning("[ADAPTIVE CHUNK MIN] Window step reached 1h threshold. Processing batch.")

                max_cursor = None
                for r in records:
                    if isinstance(r, dict):
                        attr = r.get("attributes", {}) if "attributes" in r else r
                        updated = attr.get("updated_at") or attr.get("created_at") or attr.get("scc_updated_at")
                        if updated and (max_cursor is None or str(updated) > str(max_cursor)):
                            max_cursor = str(updated)

                yield {"records": records, "max_cursor": max_cursor or _fmt_iso(cur_to), "page": page_num}
                page_num += 1

                cur_start = cur_to
                step = min(step * 2, timedelta(days=3))

            return

        # ---------------------------------------------------------------------
        # 2. SCIM v2 PROTOCOL
        # ---------------------------------------------------------------------
        full_url: Optional[str] = f"{self.session.base_url.rstrip('/')}{endpoint}"

        if is_scim:
            start_index = 1
            page_num = 1

            while full_url:
                params: Dict[str, Any] = {"startIndex": start_index, "count": self.page_size}
                if not parent_params and api_type in ("INCR", "INCR_GET_ID"):
                    filter_parts = []
                    if start_time:
                        filter_parts.append(f'meta.lastModified gt "{start_time}"')
                        if end_time:
                            filter_parts.append(f'meta.lastModified lt "{end_time}"')
                    if filter_parts:
                        params["filter"] = " and ".join(filter_parts)

                resp = self._request_with_retry("GET", full_url, headers=headers, params=params)
                body = resp if isinstance(resp, dict) else {}
                records = body.get("Resources", []) if isinstance(body, dict) else []

                total_results = int(body.get("totalResults") or len(records)) if isinstance(body, dict) else len(records)
                items_per_page = int(body.get("itemsPerPage") or len(records)) if isinstance(body, dict) else len(records)

                max_cursor = None
                for r in records:
                    if isinstance(r, dict):
                        meta_info = r.get("meta", {}) if "meta" in r else {}
                        updated = meta_info.get("lastModified") or meta_info.get("created")
                        if updated and (max_cursor is None or str(updated) > str(max_cursor)):
                            max_cursor = str(updated)

                yield {"records": records, "max_cursor": max_cursor, "page": page_num}

                if len(records) > 0 and (start_index + items_per_page <= total_results):
                    start_index += items_per_page
                    page_num += 1
                else:
                    full_url = None

        # ---------------------------------------------------------------------
        # 3. STANDARD REST ENDPOINTS (contracts, suppliers, projects, etc.)
        # ---------------------------------------------------------------------
        else:
            page_num = 1
            seen_urls = set()

            params: Optional[Dict[str, Any]] = {"page[size]": self.page_size}
            
            if not parent_params and api_type in ("INCR", "INCR_GET_ID"):
                if start_time:
                    params["filter[updated_at_from]"] = start_time
                if end_time:
                    params["filter[updated_at_to]"] = end_time

            while full_url:
                if full_url in seen_urls:
                    log.warning("Loop detected on URL: %s. Terminating pagination.", full_url)
                    break
                seen_urls.add(full_url)

                if "?" in full_url:
                    clean_url, parsed_params = _split_url_and_params(full_url, base_url=self.session.base_url)
                    resp = self._request_with_retry("GET", clean_url, headers=headers, params=parsed_params)
                else:
                    resp = self._request_with_retry("GET", full_url, headers=headers, params=params)

                if isinstance(resp, list):
                    records = resp
                    body = {}
                elif isinstance(resp, dict):
                    body = resp
                    data = (
                        body.get("data")
                        or body.get("entries")
                        or body.get("Resources")
                        or body.get("resources")
                        or body.get(object_name)
                    )
                    if data is None:
                        raw_data = [v for k, v in body.items() if k != "links" and isinstance(v, list)]
                        records = raw_data[0] if raw_data else []
                    else:
                        records = data if isinstance(data, list) else ([data] if data else [])
                else:
                    records = []
                    body = {}

                max_cursor = None
                for r in records:
                    if isinstance(r, dict):
                        attr = r.get("attributes", {}) if "attributes" in r else r
                        updated = attr.get("updated_at") or attr.get("created_at") or attr.get("scc_updated_at")
                        if updated and (max_cursor is None or str(updated) > str(max_cursor)):
                            max_cursor = str(updated)

                yield {"records": records, "max_cursor": max_cursor, "page": page_num}

                links = body.get("links", {}) if isinstance(body, dict) else {}
                next_link = links.get("next") if isinstance(links, dict) else None

                if next_link and len(records) > 0:
                    full_url = str(next_link)
                    params = None
                    page_num += 1
                else:
                    full_url = None