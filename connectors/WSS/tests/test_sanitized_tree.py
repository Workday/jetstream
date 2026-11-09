"""The packaged connector must not carry internal names or live credentials."""
from __future__ import annotations

import os
import re
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]

_TERMS = [
    "ud" + "l",
    "data" + "bridge",
    "aai" + "-de",
    "bt " + "jetstream",
    "git" + "hub.com/" + "work" + "day",
    "work" + "day-inc",
    "wss-" + "secrets",
    "file_" + "extracts",
    "inbound_" + "file",
    "mega" + "leo",
    "cite" + ": 24",
]
_BOUNDARY = [
    "ed" + "ata",
]
_VENDOR_API = "work" + "dayspend"


def test_tree_has_no_internal_terms():
    patterns = [re.compile(re.escape(term), re.IGNORECASE) for term in _TERMS]
    patterns.extend(re.compile(rf"\b{re.escape(term)}\b", re.IGNORECASE) for term in _BOUNDARY)
    vendor = re.compile(re.escape(_VENDOR_API), re.IGNORECASE)
    bare_vendor = re.compile("work" + "day", re.IGNORECASE)
    email = re.compile(r"[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}")
    hits: list[str] = []
    for dirpath, dirnames, filenames in os.walk(ROOT):
        dirnames[:] = [name for name in dirnames if name != "__pycache__"]
        for filename in filenames:
            if filename.endswith(".pyc"):
                continue
            path = Path(dirpath) / filename
            text = path.read_text(encoding="utf-8", errors="replace")
            for line_no, line in enumerate(text.splitlines(), 1):
                if email.search(line):
                    hits.append(f"{path.relative_to(ROOT)}:{line_no}: email")
                stripped = vendor.sub("", line)
                if bare_vendor.search(stripped):
                    hits.append(f"{path.relative_to(ROOT)}:{line_no}: vendor name outside the API host")
                for pattern in patterns:
                    if pattern.search(line):
                        hits.append(f"{path.relative_to(ROOT)}:{line_no}: {pattern.pattern}")
                        break
    assert hits == [], "\n".join(hits)
