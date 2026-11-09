"""Compile parameters.yaml or string into a JSON Schema document (spec §7)."""
from __future__ import annotations

import os
from pathlib import Path
from typing import Any, Dict, List, Mapping
import yaml

_SUPPORTED_TYPES = {"string", "integer", "number", "boolean"}

def compile_parameters_yaml(path_or_yaml: str | Path) -> Dict[str, Any]:
    content = str(path_or_yaml)
    if os.path.isfile(content):
        raw = yaml.safe_load(Path(content).read_text())
    else:
        raw = yaml.safe_load(content)

    if not isinstance(raw, Mapping) or "parameters" not in raw:
        raise ValueError("Expected top-level 'parameters:' list in YAML configuration")
    return compile_parameters(raw["parameters"])

def compile_parameters(parameters: List[Mapping[str, Any]]) -> Dict[str, Any]:
    properties: Dict[str, Any] = {}
    unconditionally_required: List[str] = []

    seen: set = set()
    for p in parameters:
        name = p["name"]
        if name in seen:
            raise ValueError(f"duplicate parameter name: {name!r}")
        seen.add(name)

        ptype = p.get("type", "string")
        if ptype not in _SUPPORTED_TYPES:
            raise ValueError(f"parameter {name!r}: unsupported type {ptype!r}")

        prop: Dict[str, Any] = {"type": ptype}
        if "description" in p: prop["description"] = p["description"]
        if "enum" in p: prop["enum"] = list(p["enum"])
        if "default" in p: prop["default"] = p["default"]
        properties[name] = prop

        if p.get("required"):
            unconditionally_required.append(name)

    schema: Dict[str, Any] = {
        "$schema": "http://json-schema.org/draft-07/schema#",
        "type": "object",
        "properties": properties,
        "additionalProperties": True,
    }
    if unconditionally_required:
        schema["required"] = unconditionally_required
    return schema

def coerce_widget_values(
    widgets: Mapping[str, Any],
    config_schema: Mapping[str, Any],
) -> Dict[str, Any]:
    props = config_schema.get("properties", {})
    out: Dict[str, Any] = {}
    for k, v in widgets.items():
        prop = props.get(k)
        if not isinstance(v, str) or not prop:
            out[k] = v
            continue
        t = prop.get("type")
        if v == "" and t in ("integer", "number", "boolean"):
            continue
        try:
            if t == "integer":
                out[k] = int(v)
            elif t == "number":
                out[k] = float(v)
            elif t == "boolean":
                out[k] = v.strip().lower() in ("true", "1", "yes", "y")
            else:
                out[k] = v
        except (ValueError, TypeError):
            out[k] = v
    return out