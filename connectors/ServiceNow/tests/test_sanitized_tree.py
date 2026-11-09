"""The packaged connector must not carry internal names or live credentials."""
from __future__ import annotations

import os
import re
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]

# Terms are split so this file can be scanned with the same rules.
_TERMS = [
    "ud" + "l",
    "work" + "day",
    "data" + "bridge",
    "aai" + "-de",
    "aai" + "_de",
    "u_" + "tax",
    "u_" + "number",
    "file_" + "extracts",
    "inbound_" + "file",
    "bt-" + "ud" + "l",
    "git" + "hub.com/" + "work" + "day",
    "mega" + "leo",
    "open" + "lineage",
    "jetstream-" + "servicenow-connector-cursor",
]
_BOUNDARY = [
    "w" + "ss",
    "Fr" + "ont",
    "Sales" + "force",
    "ed" + "ata",
]


def test_tree_has_no_internal_terms():
    patterns = [re.compile(re.escape(term), re.IGNORECASE) for term in _TERMS]
    patterns.extend(
        re.compile(rf"\b{re.escape(term)}\b", re.IGNORECASE) for term in _BOUNDARY
    )
    email = re.compile(r"[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}")
    hits: list[str] = []
    for dirpath, dirnames, filenames in os.walk(ROOT):
        dirnames[:] = [name for name in dirnames if name != "__pycache__"]
        for filename in filenames:
            if filename.endswith((".pyc",)):
                continue
            path = Path(dirpath) / filename
            text = path.read_text(encoding="utf-8", errors="replace")
            for line_no, line in enumerate(text.splitlines(), 1):
                if email.search(line):
                    hits.append(f"{path.relative_to(ROOT)}:{line_no}: email")
                for pattern in patterns:
                    if pattern.search(line):
                        hits.append(f"{path.relative_to(ROOT)}:{line_no}: {pattern.pattern}")
                        break
    assert hits == [], "\n".join(hits)
