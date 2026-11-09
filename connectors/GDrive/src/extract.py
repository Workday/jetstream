"""Google Drive extraction logic.

Ports the Drive API interaction from the legacy ingestion script into the Jetstream
framework, using src/auth.GoogleDriveTokens for token management instead of
oauth2client.

Public API (called from src/pipeline.py):
    list_drive_metadata   — fetch file/folder metadata recursively
    download_file         — download a single file (native export or binary)
    write_to_destination  — write file bytes to UC Volume or S3

The extraction layer is stateless: cursors and run tracking live in
src/state.py; this module only talks to the Google Drive REST API and the
destination file system.
"""
from __future__ import annotations

import io
import logging
import os
import time
from dataclasses import dataclass
from typing import Any, Dict, List, Optional, Sequence, Tuple

import boto3
import requests

from src.auth import GoogleDriveTokens
from src.config import FileDestination, RunConfig, SourceCfg

log = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Constants (ported from the legacy ingestion script)
# ---------------------------------------------------------------------------

MIME_TYPE_FOLDER = "application/vnd.google-apps.folder"
MIME_TYPE_SHORTCUT = "application/vnd.google-apps.shortcut"

# Metadata fields requested from the Drive API files.list / files.get calls.
LIST_FIELDS = (
    "nextPageToken, files(id, name, mimeType, size, createdTime, "
    "modifiedTime, parents, owners)"
)
GET_FIELDS = "id, name, mimeType, size, createdTime, modifiedTime, parents, owners"

# Google-native MIME types and their available export formats.
GOOGLE_NATIVE_EXPORT_TYPES: Dict[str, Dict[str, str]] = {
    "application/vnd.google-apps.spreadsheet": {
        "xlsx": "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        "csv": "text/csv",
        "pdf": "application/pdf",
    },
    "application/vnd.google-apps.document": {
        "docx": "application/vnd.openxmlformats-officedocument.wordprocessingml.document",
        "pdf": "application/pdf",
        "txt": "text/plain",
    },
    "application/vnd.google-apps.presentation": {
        "pptx": "application/vnd.openxmlformats-officedocument.presentationml.presentation",
        "pdf": "application/pdf",
    },
    "application/vnd.google-apps.drawing": {
        "png": "image/png",
        "svg": "image/svg+xml",
        "pdf": "application/pdf",
    },
    "application/vnd.google-apps.form": {
        "zip": "application/zip",
    },
    "application/vnd.google-apps.script": {
        "json": "application/vnd.google-apps.script+json",
    },
    "application/vnd.google-apps.jam": {
        "pdf": "application/pdf",
    },
    "application/vnd.google-apps.site": {
        "txt": "text/plain",
    },
}

# Default export format per Google-native type.
DEFAULT_EXPORT_FORMAT: Dict[str, str] = {
    "application/vnd.google-apps.spreadsheet": "xlsx",
    "application/vnd.google-apps.document": "docx",
    "application/vnd.google-apps.presentation": "pptx",
    "application/vnd.google-apps.drawing": "png",
    "application/vnd.google-apps.form": "zip",
    "application/vnd.google-apps.script": "json",
    "application/vnd.google-apps.jam": "pdf",
    "application/vnd.google-apps.site": "txt",
}

# LRO polling defaults (overridable via env vars for testing).
_LRO_POLL_TIMEOUT = int(os.environ.get("GDRIVE_LRO_POLL_TIMEOUT", "600"))
_LRO_INITIAL_BACKOFF = int(os.environ.get("GDRIVE_LRO_INITIAL_BACKOFF", "5"))
_LRO_MAX_BACKOFF = int(os.environ.get("GDRIVE_LRO_MAX_BACKOFF", "30"))
_LRO_DOWNLOAD_TIMEOUT = int(os.environ.get("GDRIVE_LRO_DOWNLOAD_TIMEOUT", "120"))


# ---------------------------------------------------------------------------
# Data classes for extraction results
# ---------------------------------------------------------------------------

@dataclass
class FileMetadata:
    """Parsed metadata for a single Drive file."""
    file_id: str
    name: str
    mime_type: str
    size_bytes: Optional[int]
    created_time: str
    modified_time: str
    parent_id: Optional[str]
    owners: str
    relative_folder_path: str = ""


@dataclass
class DownloadResult:
    """Outcome of a single file download attempt."""
    success: bool
    file_id: str
    source_file_name: str
    final_file_name: Optional[str] = None
    chosen_file_format: Optional[str] = None
    bytes_written: int = 0
    error_code: Optional[str] = None
    error_message: Optional[str] = None


# ---------------------------------------------------------------------------
# HTTP helper (Drive API requests with auth + rate-limiting + retry)
# ---------------------------------------------------------------------------

class DriveClient:
    """Thin HTTP wrapper around Google Drive REST API v3.

    Uses GoogleDriveTokens for Bearer auth and respects SourceCfg rate-limit
    and retry settings.
    """

    def __init__(self, source: SourceCfg, tokens: GoogleDriveTokens):
        self._source = source
        self._tokens = tokens
        self._session = requests.Session()
        self._last_request_time: float = 0.0

    @property
    def _base(self) -> str:
        return self._source.drive_api_base.rstrip("/")

    def _auth_headers(self) -> Dict[str, str]:
        return {"Authorization": f"Bearer {self._tokens()}"}

    def _throttle(self) -> None:
        """Client-side rate limiting per rate_limit_qps."""
        if self._source.rate_limit_qps <= 0:
            return
        min_interval = 1.0 / self._source.rate_limit_qps
        elapsed = time.time() - self._last_request_time
        if elapsed < min_interval:
            time.sleep(min_interval - elapsed)
        self._last_request_time = time.time()

    def _request_with_retry(
        self,
        method: str,
        url: str,
        *,
        params: Optional[Dict] = None,
        headers: Optional[Dict] = None,
        data: Optional[Any] = None,
        stream: bool = False,
        timeout: Optional[int] = None,
    ) -> requests.Response:
        """Execute an HTTP request with exponential-backoff retry on transient errors."""
        retry = self._source.retry
        timeout = timeout or self._source.timeout_seconds
        hdrs = {**self._auth_headers(), **(headers or {})}

        for attempt in range(retry.max_attempts):
            self._throttle()
            try:
                resp = self._session.request(
                    method, url,
                    params=params, headers=hdrs, data=data,
                    stream=stream, timeout=timeout,
                )
                if resp.status_code not in retry.retry_on_status:
                    return resp
                log.warning(
                    "Drive API %s %s returned %d (attempt %d/%d)",
                    method, url, resp.status_code, attempt + 1, retry.max_attempts,
                )
            except (requests.ConnectionError, requests.Timeout) as exc:
                log.warning(
                    "Drive API %s %s transient error (attempt %d/%d): %s",
                    method, url, attempt + 1, retry.max_attempts, exc,
                )
                if attempt == retry.max_attempts - 1:
                    raise

            backoff = retry.backoff_seconds * (retry.backoff_multiplier ** attempt)
            time.sleep(backoff)

        return resp  # type: ignore[possibly-undefined]

    # ------------------------------------------------------------------
    # Drive API operations
    # ------------------------------------------------------------------

    def get_file_metadata(self, file_id: str) -> Dict[str, Any]:
        """Fetch metadata for a single file."""
        url = f"{self._base}/files/{file_id}"
        resp = self._request_with_retry(
            "GET", url,
            params={"fields": GET_FIELDS, "supportsAllDrives": "true"},
        )
        resp.raise_for_status()
        return resp.json()

    def list_files(
        self,
        query: str,
        page_token: Optional[str] = None,
        page_size: int = 100,
    ) -> Dict[str, Any]:
        """Execute a files.list call with pagination support."""
        url = f"{self._base}/files"
        params = {
            "q": query,
            "pageSize": str(page_size),
            "fields": LIST_FIELDS,
            "supportsAllDrives": "true",
            "includeItemsFromAllDrives": "true",
            "corpora": "allDrives",
        }
        if page_token:
            params["pageToken"] = page_token
        resp = self._request_with_retry("GET", url, params=params)
        resp.raise_for_status()
        return resp.json()

    def download_binary(self, file_id: str) -> bytes:
        """Download a non-Google-native file as raw bytes."""
        url = f"{self._base}/files/{file_id}"
        resp = self._request_with_retry(
            "GET", url,
            params={"alt": "media", "supportsAllDrives": "true"},
            stream=True,
        )
        resp.raise_for_status()
        return resp.content

    def export_file(self, file_id: str, export_mime: str) -> bytes:
        """Export a Google-native file to the given MIME type."""
        url = f"{self._base}/files/{file_id}/export"
        resp = self._request_with_retry(
            "GET", url,
            params={"mimeType": export_mime},
        )
        resp.raise_for_status()
        return resp.content

    def download_via_lro(
        self, file_id: str, export_mime: str
    ) -> Optional[bytes]:
        """Export a large Google-native file via the files.download LRO.

        Used as a fallback when the standard export endpoint hits the 10 MB
        size cap. Returns bytes on success or None on failure.
        """
        headers = self._auth_headers()
        start_ts = time.monotonic()

        # 1. Start the long-running operation.
        resp = self._request_with_retry(
            "POST", f"{self._base}/files/{file_id}/download",
            headers={**headers, "Content-Length": "0"},
            params={"mimeType": export_mime} if export_mime else None,
            timeout=60,
        )
        if resp.status_code != 200:
            log.error("LRO start returned %d: %s", resp.status_code, resp.text[:300])
            return None

        operation = resp.json()

        # 2. Poll until done.
        backoff = _LRO_INITIAL_BACKOFF
        while not operation.get("done"):
            if time.monotonic() - start_ts > _LRO_POLL_TIMEOUT:
                log.error("LRO polling timed out after %ds", _LRO_POLL_TIMEOUT)
                return None

            op_name = operation.get("name")
            if not op_name:
                log.error("LRO response missing 'name'; cannot poll.")
                return None

            time.sleep(backoff)
            backoff = min(backoff * 2, _LRO_MAX_BACKOFF)

            poll = self._request_with_retry(
                "GET", f"{self._base}/operations/{op_name}",
                timeout=60,
            )
            if poll.status_code != 200:
                log.error("LRO poll returned %d: %s", poll.status_code, poll.text[:300])
                return None
            operation = poll.json()

        if operation.get("error"):
            log.error("LRO export failed: %s", operation["error"])
            return None

        download_uri = (operation.get("response") or {}).get("downloadUri")
        if not download_uri:
            log.error("LRO completed but returned no downloadUri.")
            return None

        # 3. Fetch bytes (follow redirects manually for signed URLs).
        try:
            dl_resp = self._session.get(
                download_uri, headers=headers,
                stream=True, timeout=_LRO_DOWNLOAD_TIMEOUT,
            )
            dl_resp.raise_for_status()
            return dl_resp.content
        except Exception as exc:
            log.error("LRO downloadUri fetch failed: %s", exc)
            return None

    def resolve_shortcut(self, file_id: str) -> Optional[Dict[str, Any]]:
        """Resolve a Drive shortcut to its target file metadata."""
        url = f"{self._base}/files/{file_id}"
        resp = self._request_with_retry(
            "GET", url,
            params={
                "fields": "id, name, shortcutDetails(targetId, targetMimeType)",
                "supportsAllDrives": "true",
            },
        )
        if resp.status_code != 200:
            return None
        details = resp.json().get("shortcutDetails", {})
        target_id = details.get("targetId")
        if not target_id:
            return None
        return self.get_file_metadata(target_id)


# ---------------------------------------------------------------------------
# Metadata listing
# ---------------------------------------------------------------------------

def list_drive_metadata(
    client: DriveClient,
    folder_ids: Sequence[str],
    *,
    cursor_modified_time: Optional[str] = None,
) -> List[FileMetadata]:
    """Recursively list file metadata from the given folder IDs.

    If folder_ids is empty, lists all files accessible to the OAuth identity.
    When cursor_modified_time is provided (ISO 8601), only returns files with
    modifiedTime > cursor (incremental extraction).
    """
    if folder_ids:
        all_files: List[Dict] = []
        for fid in folder_ids:
            all_files.extend(_list_recursive(client, fid))
    else:
        # No folder scope — list everything accessible.
        all_files = _list_all(client)

    # Apply incremental cursor filter.
    if cursor_modified_time:
        all_files = [
            f for f in all_files
            if f.get("modifiedTime", "") > cursor_modified_time
        ]

    return [_to_file_metadata(f) for f in all_files]


class DriveListingError(RuntimeError):
    """Raised when a Drive folder or file ID cannot be resolved."""
    pass


# Module-level storage for listing errors. Populated by
# list_drive_metadata_with_paths so the caller can process valid files
# first, then check for failures and mark the run as failed.
_last_listing_errors: List[str] = []


def get_listing_errors() -> List[str]:
    """Return any errors from the last call to list_drive_metadata_with_paths."""
    return list(_last_listing_errors)


def list_drive_metadata_with_paths(
    client: DriveClient,
    folder_ids: Sequence[str],
    *,
    file_ids: Sequence[str] = (),
    cursor_modified_time: Optional[str] = None,
) -> List[FileMetadata]:
    """List file metadata preserving folder hierarchy for download.

    Args:
        folder_ids: Google Drive folder IDs to recurse into.
        file_ids: Individual Google Drive file IDs (Sheets, Slides, etc.)
            to include directly (downloaded to volume root with date partition).
        cursor_modified_time: ISO 8601 timestamp for incremental filtering.

    Returns:
        List of FileMetadata for successfully resolved IDs. If some IDs
        failed, the resolved files are still returned (so downloads proceed)
        but errors are stored in `_last_listing_errors` for the caller to
        check and mark the run as failed after processing.
    """
    _last_listing_errors.clear()
    all_files: List[Dict] = []
    errors: List[str] = []

    # --- Process folder_ids (must be folders) ---
    for fid in folder_ids:
        try:
            item_meta = client.get_file_metadata(fid)
        except Exception as exc:
            errors.append(
                f"folder_id={fid}: Failed to fetch metadata — {type(exc).__name__}: {exc}"
            )
            continue

        mime = item_meta.get("mimeType", "")
        name = item_meta.get("name", fid)

        if mime != MIME_TYPE_FOLDER:
            errors.append(
                f"folder_id={fid}: Expected a folder but got '{name}' "
                f"(mime={mime}). Move this ID to 'file_ids' parameter instead."
            )
            continue

        log.info("folder_id=%s is folder '%s'; listing recursively.", fid, name)
        all_files.extend(_list_recursive_with_paths(client, fid, name))

    # --- Process file_ids (must be files, not folders) ---
    for fid in file_ids:
        try:
            item_meta = client.get_file_metadata(fid)
        except Exception as exc:
            errors.append(
                f"file_id={fid}: Failed to fetch metadata — {type(exc).__name__}: {exc}"
            )
            continue

        mime = item_meta.get("mimeType", "")
        name = item_meta.get("name", fid)

        if mime == MIME_TYPE_FOLDER:
            errors.append(
                f"file_id={fid}: Expected a file but got folder '{name}'. "
                f"Move this ID to 'folder_ids' parameter instead."
            )
            continue

        log.info("file_id=%s is file '%s' (mime=%s); including directly.", fid, name, mime)
        item_meta["relative_folder_path"] = ""
        all_files.append(item_meta)

    # --- Log errors loudly but still return successfully resolved files ---
    if errors:
        msg = (
            f"{len(errors)} Drive ID(s) failed to resolve:\n"
            + "\n".join(f"  • {e}" for e in errors)
        )
        log.error(msg)
        # Store errors for the caller to handle after processing valid files.
        # The caller should still mark the run as failed, but valid downloads
        # should not be blocked by unresolvable IDs.
        _last_listing_errors.clear()
        _last_listing_errors.extend(errors)

    if cursor_modified_time:
        all_files = [
            f for f in all_files
            if f.get("modifiedTime", "") > cursor_modified_time
        ]

    return [_to_file_metadata(f) for f in all_files]


def _list_all(client: DriveClient) -> List[Dict]:
    """List all non-trashed files accessible to the identity."""
    results: List[Dict] = []
    page_token: Optional[str] = None
    while True:
        data = client.list_files("trashed=false", page_token=page_token)
        results.extend(data.get("files", []))
        page_token = data.get("nextPageToken")
        if not page_token:
            break
    return results


def _list_recursive(client: DriveClient, folder_id: str) -> List[Dict]:
    """BFS recursive listing of all non-folder files under folder_id."""
    all_files: List[Dict] = []
    visited: set = set()
    queue = [folder_id]

    while queue:
        current = queue.pop(0)
        if current in visited:
            continue
        visited.add(current)

        page_token: Optional[str] = None
        while True:
            data = client.list_files(
                f"'{current}' in parents and trashed=false",
                page_token=page_token,
            )
            for item in data.get("files", []):
                if item.get("mimeType") == MIME_TYPE_FOLDER:
                    queue.append(item["id"])
                else:
                    all_files.append(item)
            page_token = data.get("nextPageToken")
            if not page_token:
                break

    return all_files


def _list_recursive_with_paths(
    client: DriveClient, folder_id: str, current_path: str
) -> List[Dict]:
    """DFS recursive listing preserving relative folder paths."""
    all_files: List[Dict] = []
    page_token: Optional[str] = None

    while True:
        data = client.list_files(
            f"'{folder_id}' in parents and trashed=false",
            page_token=page_token,
        )
        for item in data.get("files", []):
            if item.get("mimeType") == MIME_TYPE_FOLDER:
                child_path = (
                    os.path.join(current_path, item["name"])
                    if current_path
                    else item["name"]
                )
                all_files.extend(
                    _list_recursive_with_paths(client, item["id"], child_path)
                )
            else:
                item["relative_folder_path"] = current_path
                all_files.append(item)
        page_token = data.get("nextPageToken")
        if not page_token:
            break

    return all_files


def _to_file_metadata(raw: Dict) -> FileMetadata:
    owners = ", ".join(
        o.get("emailAddress", "") for o in raw.get("owners", [])
    ) if raw.get("owners") else ""
    size_str = raw.get("size")
    return FileMetadata(
        file_id=raw.get("id", ""),
        name=raw.get("name", ""),
        mime_type=raw.get("mimeType", ""),
        size_bytes=int(size_str) if size_str else None,
        created_time=raw.get("createdTime", ""),
        modified_time=raw.get("modifiedTime", ""),
        parent_id=raw["parents"][0] if raw.get("parents") else None,
        owners=owners,
        relative_folder_path=raw.get("relative_folder_path", ""),
    )


# ---------------------------------------------------------------------------
# File download
# ---------------------------------------------------------------------------

def download_file(
    client: DriveClient,
    file_meta: FileMetadata,
    *,
    file_export_format: str = "",
    max_file_bytes: int = 0,
) -> DownloadResult:
    """Download a single file from Drive.

    Handles:
      - Shortcut resolution
      - Google-native exports (with LRO fallback for >10 MB)
      - Binary downloads
      - max_file_bytes size guard
    """
    file_id = file_meta.file_id
    file_name = file_meta.name
    mime_type = file_meta.mime_type

    # Resolve shortcuts.
    if mime_type == MIME_TYPE_SHORTCUT:
        target = client.resolve_shortcut(file_id)
        if not target:
            return DownloadResult(
                success=False, file_id=file_id, source_file_name=file_name,
                error_message=f"Unable to resolve shortcut '{file_name}'",
            )
        file_id = target["id"]
        file_name = target.get("name", file_name)
        mime_type = target.get("mimeType", "")

    # Size guard.
    if max_file_bytes > 0 and file_meta.size_bytes and file_meta.size_bytes > max_file_bytes:
        return DownloadResult(
            success=False, file_id=file_id, source_file_name=file_name,
            error_message=(
                f"Skipped: {file_meta.size_bytes} bytes exceeds "
                f"max_file_bytes={max_file_bytes}"
            ),
        )

    safe_name = _sanitize_filename(file_name)

    try:
        if mime_type.startswith("application/vnd.google-apps."):
            payload, final_name, fmt = _download_google_native(
                client, file_id, safe_name, mime_type, file_export_format
            )
        else:
            payload = client.download_binary(file_id)
            final_name = safe_name
            fmt = os.path.splitext(safe_name)[1].lstrip(".") or None
    except Exception as exc:
        return DownloadResult(
            success=False, file_id=file_id, source_file_name=file_name,
            error_message=f"{type(exc).__name__}: {exc}",
        )

    if payload is None:
        return DownloadResult(
            success=False, file_id=file_id, source_file_name=file_name,
            error_message="Download returned empty payload (possible export size limit)",
        )

    return DownloadResult(
        success=True,
        file_id=file_id,
        source_file_name=file_name,
        final_file_name=final_name,
        chosen_file_format=fmt,
        bytes_written=len(payload),
    )


def download_file_bytes(
    client: DriveClient,
    file_meta: FileMetadata,
    *,
    file_export_format: str = "",
    max_file_bytes: int = 0,
) -> Tuple[Optional[bytes], DownloadResult]:
    """Download a file and return both the raw bytes and the result metadata.

    Used by the pipeline when it needs the bytes to write to a destination.
    """
    file_id = file_meta.file_id
    file_name = file_meta.name
    mime_type = file_meta.mime_type

    # Resolve shortcuts.
    if mime_type == MIME_TYPE_SHORTCUT:
        target = client.resolve_shortcut(file_id)
        if not target:
            return None, DownloadResult(
                success=False, file_id=file_id, source_file_name=file_name,
                error_message=f"Unable to resolve shortcut '{file_name}'",
            )
        file_id = target["id"]
        file_name = target.get("name", file_name)
        mime_type = target.get("mimeType", "")

    # Size guard.
    if max_file_bytes > 0 and file_meta.size_bytes and file_meta.size_bytes > max_file_bytes:
        return None, DownloadResult(
            success=False, file_id=file_id, source_file_name=file_name,
            error_message=(
                f"Skipped: {file_meta.size_bytes} bytes exceeds "
                f"max_file_bytes={max_file_bytes}"
            ),
        )

    safe_name = _sanitize_filename(file_name)

    try:
        if mime_type.startswith("application/vnd.google-apps."):
            payload, final_name, fmt = _download_google_native(
                client, file_id, safe_name, mime_type, file_export_format
            )
        else:
            payload = client.download_binary(file_id)
            final_name = safe_name
            fmt = os.path.splitext(safe_name)[1].lstrip(".") or None
    except Exception as exc:
        return None, DownloadResult(
            success=False, file_id=file_id, source_file_name=file_name,
            error_message=f"{type(exc).__name__}: {exc}",
        )

    if payload is None:
        return None, DownloadResult(
            success=False, file_id=file_id, source_file_name=file_name,
            error_message="Download returned empty payload (possible export size limit)",
        )

    result = DownloadResult(
        success=True,
        file_id=file_id,
        source_file_name=file_name,
        final_file_name=final_name,
        chosen_file_format=fmt,
        bytes_written=len(payload),
    )
    return payload, result


# ---------------------------------------------------------------------------
# Destination writers
# ---------------------------------------------------------------------------

def write_to_volume(
    payload: bytes,
    volume_path: str,
    file_name: str,
    relative_folder_path: str = "",
) -> str:
    """Write file bytes to a UC Volume path. Returns the full output path."""
    target_dir = (
        os.path.join(volume_path, relative_folder_path)
        if relative_folder_path
        else volume_path
    )
    os.makedirs(target_dir, exist_ok=True)
    output_path = os.path.join(target_dir, file_name)
    with open(output_path, "wb") as f:
        f.write(payload)
    log.info("Written %d bytes to %s", len(payload), output_path)
    return output_path


def write_to_s3(
    payload: bytes,
    file_name: str,
    *,
    s3_client: Any,
    bucket: str,
    prefix: str,
) -> str:
    """Write file bytes to S3. Returns the full S3 key."""
    s3_key = f"{prefix.rstrip('/')}/{file_name}"
    s3_client.put_object(Bucket=bucket, Key=s3_key, Body=payload)
    log.info("Streamed %d bytes to s3://%s/%s", len(payload), bucket, s3_key)
    return s3_key


def create_s3_client(file_dest: FileDestination) -> Any:
    """Create an S3 client via STS assume-role from FileDestination config."""
    if not file_dest.s3_role_arn:
        raise ValueError(
            "S3 role ARN is required. Static access keys are not supported."
        )
    sts = boto3.client("sts")
    assumed = sts.assume_role(
        RoleArn=file_dest.s3_role_arn,
        RoleSessionName=file_dest.s3_role_session_name,
        DurationSeconds=3600,
    )
    creds = assumed["Credentials"]
    session = boto3.Session(
        aws_access_key_id=creds["AccessKeyId"],
        aws_secret_access_key=creds["SecretAccessKey"],
        aws_session_token=creds["SessionToken"],
    )
    return session.client("s3")


# ---------------------------------------------------------------------------
# Internal helpers
# ---------------------------------------------------------------------------

def _sanitize_filename(file_name: str) -> str:
    """Return a filesystem-safe file name."""
    safe = (file_name or "").strip()
    safe = safe.replace("/", "_").replace("\\", "_")
    invalid_chars = '<>:"|?*'
    safe = "".join("_" if ch in invalid_chars else ch for ch in safe)
    safe = safe.strip().strip(".")
    return safe or "unnamed_file"


def _download_google_native(
    client: DriveClient,
    file_id: str,
    safe_base_name: str,
    mime_type: str,
    preferred_format: str,
) -> Tuple[Optional[bytes], str, Optional[str]]:
    """Export a Google-native file. Returns (bytes, final_name, format).

    Falls back to LRO if the standard export hits the 10 MB size cap.
    """
    export_formats = GOOGLE_NATIVE_EXPORT_TYPES.get(mime_type)
    if not export_formats:
        raise ValueError(
            f"No export format available for mime_type='{mime_type}'"
        )

    chosen = (
        preferred_format
        if preferred_format and preferred_format in export_formats
        else DEFAULT_EXPORT_FORMAT.get(mime_type, next(iter(export_formats)))
    )
    export_mime = export_formats[chosen]
    final_name = f"{safe_base_name}.{chosen}"

    try:
        payload = client.export_file(file_id, export_mime)
        return payload, final_name, chosen
    except requests.HTTPError as exc:
        if "exportSizeLimitExceeded" not in str(exc):
            raise

    # Fallback: LRO for large files.
    log.warning(
        "Export size limit hit for '%s'; retrying via LRO.", safe_base_name
    )
    payload = client.download_via_lro(file_id, export_mime)
    if payload is not None:
        return payload, final_name, chosen

    # Try PDF as last resort if not already chosen.
    if chosen != "pdf" and "pdf" in export_formats:
        log.warning("LRO failed for .%s; trying PDF fallback.", chosen)
        pdf_mime = export_formats["pdf"]
        payload = client.download_via_lro(file_id, pdf_mime)
        if payload is not None:
            return payload, f"{safe_base_name}.pdf", "pdf"

    return None, final_name, chosen
