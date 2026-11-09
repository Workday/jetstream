"""Attachment downloader module supporting S3 streaming and WSS metadata lookups."""
from __future__ import annotations

import logging
import os
import re
from dataclasses import dataclass
from typing import Any, Dict, Optional
import requests

from src.auth import get_auth_headers

log = logging.getLogger(__name__)


@dataclass
class DownloadResult:
    """Standardized download response object expected by src.attachment."""
    success: bool
    bytes_written: int = 0
    error: str = ""


def sanitize_filename(filename: str) -> str:
    """Sanitizes a filename by removing path slashes, invalid characters, and whitespace."""
    if not filename:
        return "unnamed_file"
    
    clean_name = os.path.basename(str(filename))
    clean_name = re.sub(r'[\\/*?:"<>|]', '_', clean_name)
    clean_name = clean_name.strip(" .")
    
    return clean_name if clean_name else "unnamed_file"


def download_file_stream(*args, **kwargs) -> DownloadResult:
    """
    Downloads binary streams directly from S3 pre-signed URLs or WSS attachment ids.
    Supports signatures:
      1. download_file_stream(download_url, dest_path) -> Used by src.attachment.py
      2. download_file_stream(wss_client, attachment_id, target_dir)
    """
    # Signature 1: Direct URL download (invoked by src.attachment.py)
    if len(args) == 2 and isinstance(args[0], str) and args[0].startswith("http"):
        download_url, dest_path = args[0], args[1]
        try:
            os.makedirs(os.path.dirname(dest_path), exist_ok=True)
            bytes_written = 0
            with requests.get(download_url, headers={}, stream=True, timeout=60) as file_resp:
                if file_resp.status_code in (400, 403):
                    return DownloadResult(success=False, error=f"S3 HTTP {file_resp.status_code} link expired")
                file_resp.raise_for_status()

                with open(dest_path, "wb") as f:
                    for chunk in file_resp.iter_content(chunk_size=8192):
                        f.write(chunk)
                        bytes_written += len(chunk)

            return DownloadResult(success=True, bytes_written=bytes_written)
        except Exception as e:
            return DownloadResult(success=False, error=str(e))

    # Signature 2: WSS API metadata lookup + binary fetch
    wss_client = kwargs.get("wss_client") or (args[0] if len(args) > 0 else None)
    attachment_id = kwargs.get("attachment_id") or (args[1] if len(args) > 1 else None)
    target_dir = kwargs.get("target_dir") or (args[2] if len(args) > 2 else None)
    auth_pattern = kwargs.get("auth_pattern", "pattern_1")

    if not attachment_id or not wss_client or not target_dir:
        return DownloadResult(success=False, error="Missing required downloader parameters")

    base_url = wss_client.session.base_url.rstrip("/")
    metadata_url = f"{base_url}/services/attachments/v1/attachments/{attachment_id}"
    headers = get_auth_headers(auth_pattern, wss_client.session.credentials)

    try:
        resp = wss_client._request_with_retry("GET", metadata_url, headers=headers)
        body = resp if isinstance(resp, dict) else {}
        data = body.get("data", {})
        attributes = data.get("attributes", {}) if isinstance(data, dict) else {}

        download_url = attributes.get("download_url")
        raw_filename = attributes.get("filename") or f"attachment_{attachment_id}.bin"

        if not download_url:
            return DownloadResult(success=False, error="No download_url returned")

        safe_filename = sanitize_filename(raw_filename)
        dest_path = os.path.join(target_dir, f"{attachment_id}_{safe_filename}")

        return download_file_stream(download_url, dest_path)

    except Exception as e:
        return DownloadResult(success=False, error=str(e))