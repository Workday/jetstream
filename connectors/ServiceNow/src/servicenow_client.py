"""ServiceNow Table API client with pagination and encoded-query support."""

from __future__ import annotations

from datetime import datetime, timezone
from typing import Any, Iterator, List, Optional
from urllib.parse import urlencode

import requests

from src.auth import ServiceNowSession
from src.config import RetryCfg
from src.retry import should_retry_status, sleep_before_retry


class ServiceNowClient:
    """Thin wrapper around the ServiceNow Table API."""

    def __init__(
        self,
        session: ServiceNowSession,
        page_size: int = 10000,
        retry: Optional[RetryCfg] = None,
    ) -> None:
        self.session = session
        self.page_size = min(page_size, 10000)
        self._http = session._http
        self.retry = retry or RetryCfg()

    def query_all(
        self,
        table: str,
        fields: List[str],
        encoded_query: Optional[str] = None,
        order_by: Optional[str] = None,
    ) -> List[dict[str, Any]]:
        return [row for page in self.query_pages(table, fields, encoded_query, order_by) for row in page]

    def query_iter(
        self,
        table: str,
        fields: List[str],
        encoded_query: Optional[str] = None,
        order_by: Optional[str] = None,
    ) -> Iterator[dict[str, Any]]:
        for page in self.query_pages(table, fields, encoded_query, order_by):
            yield from page

    def query_pages(
        self,
        table: str,
        fields: List[str],
        encoded_query: Optional[str] = None,
        order_by: Optional[str] = None,
    ) -> Iterator[List[dict[str, Any]]]:
        offset = 0
        while True:
            page = self._fetch_page(table, fields, encoded_query, order_by, offset)
            results = page.get("result") or []
            if not results:
                break
            yield results
            if len(results) < self.page_size:
                break
            offset += self.page_size

    def iter_attachments_by_cursor(
        self,
        table_name: str,
        *,
        cursor: Optional[str] = None,
        lookback_minutes: int = 0,
        from_datetime: str = "",
        to_datetime: str = "",
    ) -> Iterator[List[dict[str, Any]]]:
        parts = [f"table_name={table_name}"]
        clause = build_encoded_query(
            watermark_columns=["sys_updated_on"],
            watermark_values=[cursor] if cursor and not (from_datetime or "").strip() else None,
            lookback_minutes=lookback_minutes if cursor and not (from_datetime or "").strip() else 0,
            from_datetime=from_datetime,
            to_datetime=to_datetime,
        )
        if clause:
            parts.append(clause)
        query = "^".join(parts)
        yield from self.query_pages(
            table="sys_attachment",
            fields=[
                "sys_id", "table_name", "table_sys_id", "file_name", "content_type",
                "size_bytes", "sys_created_on", "sys_updated_on", "download_link",
            ],
            encoded_query=query,
            order_by="ORDERBYsys_updated_on",
        )

    def iter_journals_by_cursor(
        self,
        table_name: str,
        *,
        cursor: Optional[str] = None,
        lookback_minutes: int = 0,
        from_datetime: str = "",
        to_datetime: str = "",
    ) -> Iterator[List[dict[str, Any]]]:
        """Page sys_journal_field for one parent table. Uses sys_created_on since entries are insert-only."""
        parts = [f"name={table_name}"]
        clause = build_encoded_query(
            watermark_columns=["sys_created_on"],
            watermark_values=[cursor] if cursor and not (from_datetime or "").strip() else None,
            lookback_minutes=lookback_minutes if cursor and not (from_datetime or "").strip() else 0,
            from_datetime=from_datetime,
            to_datetime=to_datetime,
        )
        if clause:
            parts.append(clause)
        query = "^".join(parts)
        yield from self.query_pages(
            table="sys_journal_field",
            fields=["sys_id", "name", "element", "element_id", "value", "sys_created_on", "sys_created_by"],
            encoded_query=query,
            order_by="ORDERBYsys_created_on",
        )

    def _fetch_page(
        self,
        table: str,
        fields: List[str],
        encoded_query: Optional[str],
        order_by: Optional[str],
        offset: int,
    ) -> dict[str, Any]:
        params: dict[str, str | int] = {
            "sysparm_limit": self.page_size,
            "sysparm_offset": offset,
            "sysparm_display_value": "false",
            "sysparm_exclude_reference_link": "true",
        }
        if fields and fields != ["*"] and fields != ["ALL"]:
            params["sysparm_fields"] = ",".join(fields)
        if encoded_query:
            params["sysparm_query"] = encoded_query
        if order_by:
            params["sysparm_query"] = (
                f"{params['sysparm_query']}^{order_by}"
                if params.get("sysparm_query")
                else order_by
            )

        url = f"{self.session.api_base}/table/{table}?{urlencode(params)}"
        return self._request_json("GET", url)

    def list_table_field_names(self, table: str) -> List[str]:
        url = f"{self.session.api_base}/ui/meta/{table}"
        body = self._request_json("GET", url)
        columns = (body.get("result") or {}).get("columns") or body.get("columns") or {}
        if isinstance(columns, dict):
            return sorted(str(k) for k in columns.keys())
        if isinstance(columns, list):
            names: List[str] = []
            for col in columns:
                if isinstance(col, dict):
                    name = col.get("name") or col.get("element") or col.get("column_name")
                    if name:
                        names.append(str(name))
            return sorted(names)
        return []

    def _request_json(self, method: str, url: str) -> dict[str, Any]:
        retry = self.retry
        last_error = ""
        for attempt in range(1, retry.max_attempts + 1):
            try:
                resp = self._http.request(
                    method,
                    url,
                    headers=self.session.auth_headers(),
                    timeout=120,
                )
            except requests.RequestException as exc:
                last_error = str(exc)
                if attempt < retry.max_attempts:
                    sleep_before_retry(attempt, retry=retry)
                    continue
                raise RuntimeError(f"ServiceNow API request failed: {url}: {exc}") from exc

            if resp.status_code == 200:
                return resp.json()
            last_error = f"HTTP {resp.status_code}: {resp.text[:500]}"
            if should_retry_status(resp.status_code, retry) and attempt < retry.max_attempts:
                sleep_before_retry(attempt, retry=retry, response=resp)
                continue
            raise RuntimeError(f"ServiceNow API error ({resp.status_code}) for {url}: {resp.text[:500]}")
        raise RuntimeError(f"ServiceNow API failed: {url}: {last_error}")


def build_encoded_query(
    watermark_columns: List[str],
    watermark_values: Optional[List[str]],
    lookback_minutes: int = 0,
    full_refresh_filter: str = "",
    *,
    from_datetime: str = "",
    to_datetime: str = "",
) -> Optional[str]:
    parts: list[str] = []
    if full_refresh_filter.strip():
        parts.append(full_refresh_filter.strip())

    from_dt = (from_datetime or "").strip()
    to_dt = (to_datetime or "").strip()
    if from_dt:
        col = watermark_columns[0] if watermark_columns else "sys_updated_on"
        parts.append(f"{col}>{_format_sn_datetime(from_dt)}")
        upper = to_dt or datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")
        parts.append(f"{col}<{_format_sn_datetime(upper)}")
    elif watermark_values and watermark_columns:
        wm_clause = _watermark_clause(watermark_columns, watermark_values, lookback_minutes)
        if wm_clause:
            parts.append(wm_clause)

    if not parts:
        return None
    return "^".join(parts)


def _format_sn_datetime(value: str) -> str:
    return _parse_sn_datetime(value).strftime("%Y-%m-%d %H:%M:%S")


def _watermark_clause(
    columns: List[str],
    values: List[str],
    lookback_minutes: int,
) -> str:
    from datetime import timedelta

    if not columns or not values:
        return ""

    if len(columns) == 1 or len(values) < 2:
        col = columns[0]
        val = values[0]
        if lookback_minutes > 0:
            dt = _parse_sn_datetime(val) - timedelta(minutes=lookback_minutes)
            val = dt.strftime("%Y-%m-%d %H:%M:%S")
        else:
            val = _format_sn_datetime(val)
        return f"{col}>{val}"

    adjusted = [
        _normalize_watermark_value(v, lookback_minutes if i == 0 else 0)
        for i, v in enumerate(values)
    ]
    n = min(len(columns), len(adjusted))
    groups: List[str] = []
    for i in range(n):
        col = columns[i]
        prefix = "^".join(f"{columns[j]}={adjusted[j]}" for j in range(i))
        tail = f"{col}>{adjusted[i]}"
        groups.append(f"{prefix}^{tail}" if prefix else tail)
    return "^NQ".join(groups)


def _normalize_watermark_value(value: str, lookback_minutes: int) -> str:
    from datetime import timedelta
    try:
        dt = _parse_sn_datetime(value)
    except ValueError:
        return value
    if lookback_minutes > 0:
        dt = dt - timedelta(minutes=lookback_minutes)
    return dt.strftime("%Y-%m-%d %H:%M:%S")


def _parse_sn_datetime(value: str) -> datetime:
    v = (value or "").strip()
    if not v:
        raise ValueError(f"cannot parse empty ServiceNow/ISO timestamp: {value!r}")
    v = v.replace("Z", "+00:00")
    if "T" in v:
        try:
            dt = datetime.fromisoformat(v)
            if dt.tzinfo is None:
                dt = dt.replace(tzinfo=timezone.utc)
            return dt.astimezone(timezone.utc).replace(tzinfo=timezone.utc)
        except ValueError:
            pass
    v2 = value.strip().replace("T", " ")
    for fmt in ("%Y-%m-%d %H:%M:%S", "%Y-%m-%d %H:%M:%S.%f", "%Y-%m-%d"):
        try:
            raw = v2.split(".")[0] if "%f" not in fmt else v2
            if fmt == "%Y-%m-%d":
                dt = datetime.strptime(raw[:10], fmt)
            else:
                dt = datetime.strptime(raw[:19], "%Y-%m-%d %H:%M:%S")
            return dt.replace(tzinfo=timezone.utc)
        except ValueError:
            continue
    raise ValueError(f"cannot parse ServiceNow/ISO timestamp: {value!r}")