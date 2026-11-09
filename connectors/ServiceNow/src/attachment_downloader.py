"""Binary streaming for ServiceNow attachments to Unity Catalog Volumes."""

from __future__ import annotations

import os
import re
from datetime import datetime, timezone
from typing import Any, Dict, Optional

import requests

from src.auth import ServiceNowSession

# Cap sizes to prevent out-of-memory or massive runaway downloads (e.g., 100MB)
MAX_FILE_SIZE_BYTES = 100 * 1024 * 1024  


def process_attachment_download(
    spark: Any,
    session: ServiceNowSession,
    att_sys_id: str,
    table_sys_id: str,
    download_link: Optional[str],
    file_name: str,
    size_bytes: int,
    volume_base_path: str,
) -> Dict[str, Any]:
    """Streams a single attachment to a UC volume in a nested folder structure and returns status."""
    now = datetime.now(timezone.utc)
    
    # Fallback to standard Table API file endpoint if download_link is missing
    if not download_link:
        download_link = f"{session.api_base}/attachment/{att_sys_id}/file"

    # Ensure the filename is safe for the filesystem
    safe_name = re.sub(r'[^a-zA-Z0-9_\-\.]', '_', file_name)
    if not safe_name:
        safe_name = f"file_{att_sys_id}"
    
    clean_parent_id = (table_sys_id or "unknown_parent").strip()
    
    # /Volumes/<catalog>/<schema>/attachments/<object>/<table_sys_id>/<att_sys_id>/<file_name>
    dest_dir = f"{volume_base_path.rstrip('/')}/{clean_parent_id}/{att_sys_id}"
    dest_path = f"{dest_dir}/{safe_name}"

    result = {
        "sys_id": att_sys_id,
        "download_status": "failed",
        "volume_path": "",
        "download_error": "",
        "downloaded_at": now,
        "bytes_written": 0,
        "status": "failed"  # Internal router status key
    }

    # Empty files: record skipped so retry_failed_downloads does not loop on them.
    if size_bytes == 0:
        result["download_status"] = "skipped"
        result["status"] = "skipped"
        result["download_error"] = "Skipped: File size is 0 bytes"
        return result

    # Oversized files: skipped (not failed) so retry does not re-attempt forever.
    if size_bytes > MAX_FILE_SIZE_BYTES:
        result["download_status"] = "skipped"
        result["status"] = "skipped"
        result["download_error"] = f"Skipped: file too large ({size_bytes} > {MAX_FILE_SIZE_BYTES})"
        return result

    try:
        # Create nested directories natively in the UC Volume if they don't exist
        os.makedirs(os.path.dirname(dest_path), exist_ok=True)
        
        # Stream the binary payload in chunks to avoid blowing out node memory
        with session._http.request(
            "GET", 
            download_link, 
            headers=session.auth_headers(), 
            stream=True, 
            timeout=300
        ) as response:
            response.raise_for_status()
            
            written = 0
            with open(dest_path, "wb") as f:
                for chunk in response.iter_content(chunk_size=8192):
                    if chunk:
                        f.write(chunk)
                        written += len(chunk)
            
            result["download_status"] = "success"
            result["status"] = "success"
            result["volume_path"] = dest_path
            result["bytes_written"] = written

    except requests.RequestException as e:
        result["download_status"] = "failed_with_error"
        result["status"] = "failed"
        result["download_error"] = f"HTTP Error: {str(e)[:500]}"
    except Exception as e:
        result["download_status"] = "failed_with_error"
        result["status"] = "failed"
        result["download_error"] = f"File I/O Error: {str(e)[:500]}"

    return result