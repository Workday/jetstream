"""Compile a connector's parameters.yaml into a JSON Schema document.

Spec §7 "Parameter Specification Convention": every connector type has
exactly one parameters.yaml at connectors/<connector_type_id>/parameters.yaml,
authored in a small, opinionated schema. Do NOT hand-write JSON Schema.

Supported per-parameter fields:
    name:        str, required
    type:        one of "string" | "integer" | "number" | "boolean"
    required:    bool, default False
    default:     any (echoed into the schema; used at read-time)
    enum:        list of allowed values (for scalar params)
    description: str
    depends_on:  {other_param: expected_value} — that parameter is only
                 required / valid when the referenced parameter matches.

This module is intentionally the shortest compiler that faithfully
represents those fields as a JSON Schema draft-07 document, so
`validate_source_config` can run against it with any off-the-shelf
JSON Schema validator.
"""
from __future__ import annotations

from pathlib import Path
from typing import Any, Dict, List, Mapping

import yaml


_SUPPORTED_TYPES = {"string", "integer", "number", "boolean", "map", "array"}

# Types that are semantically complex but passed as JSON strings from widgets.
# They compile to "string" in the JSON Schema since dbutils.widgets always
# returns strings; the connector code (config.py) parses the JSON at runtime.
_JSON_STRING_TYPES = {"map", "array"}


def compile_parameters_yaml(path: str | Path) -> Dict[str, Any]:
    """Read a parameters.yaml file and return the compiled JSON Schema."""
    raw = yaml.safe_load(Path(path).read_text())
    if not isinstance(raw, Mapping) or "parameters" not in raw:
        raise ValueError(f"{path}: expected top-level 'parameters:' list")
    return compile_parameters(raw["parameters"])


def compile_parameters(parameters: List[Mapping[str, Any]]) -> Dict[str, Any]:
    """Compile the in-memory parameters list into a JSON Schema document."""
    properties: Dict[str, Any] = {}
    unconditionally_required: List[str] = []
    conditional: List[Dict[str, Any]] = []

    seen: set = set()
    for p in parameters:
        name = p["name"]
        if name in seen:
            raise ValueError(f"duplicate parameter name: {name!r}")
        seen.add(name)

        ptype = p.get("type", "string")
        if ptype not in _SUPPORTED_TYPES:
            raise ValueError(f"parameter {name!r}: unsupported type {ptype!r}")

        # map/array are passed as JSON strings from widgets; compile to "string".
        json_schema_type = "string" if ptype in _JSON_STRING_TYPES else ptype
        prop: Dict[str, Any] = {"type": json_schema_type}
        if "description" in p:
            prop["description"] = p["description"]
        if "enum" in p:
            prop["enum"] = list(p["enum"])
        if "default" in p:
            prop["default"] = p["default"]
        properties[name] = prop

        depends_on = p.get("depends_on")
        if depends_on:
            # {other: value} -> if other == value then this param is required (and allowed)
            if not isinstance(depends_on, Mapping) or len(depends_on) != 1:
                raise ValueError(
                    f"parameter {name!r}: depends_on must be a single-entry mapping"
                )
            (other, expected), = depends_on.items()
            branch: Dict[str, Any] = {
                "if": {"properties": {other: {"const": expected}}, "required": [other]},
                "then": {"required": [name]} if p.get("required") else {},
                "else": {"not": {"required": [name]}},
            }
            conditional.append(branch)
        elif p.get("required"):
            unconditionally_required.append(name)

    schema: Dict[str, Any] = {
        "$schema": "http://json-schema.org/draft-07/schema#",
        "type": "object",
        "properties": properties,
        "additionalProperties": True,
    }
    if unconditionally_required:
        schema["required"] = unconditionally_required
    if conditional:
        schema["allOf"] = conditional
    return schema


# ---------------------------------------------------------------------------
# Widget value coercion
# ---------------------------------------------------------------------------

def coerce_widget_values(
    widgets: Mapping[str, Any],
    config_schema: Mapping[str, Any],
) -> Dict[str, Any]:
    """Coerce Databricks widget string values to the types declared in
    `config_schema`.

    Databricks widgets always return strings ("10", "true", "1440"), but our
    manifest schema declares numeric / boolean fields with proper types.
    Without this bridge, `validate_source_config` rejects every numeric
    widget as "not of type number".

    Rules:
      * Values already typed (int, float, bool) pass through unchanged.
      * String values are coerced to the property's declared type:
          "integer" -> int, "number" -> float, "boolean" -> bool.
      * An empty string on a typed non-string field is dropped from the
        result (treated as "not supplied") so the JSON Schema default kicks
        in downstream via each caller's own fallback.
      * If coercion fails (e.g. "abc" for a number), the value passes
        through unchanged so the validator surfaces a clear
        "not of type number" error, not a Python TypeError.
    """
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
                out[k] = _to_bool(v)
            else:
                out[k] = v
        except (ValueError, TypeError):
            out[k] = v
    return out


def _to_bool(v: str) -> bool:
    v_low = v.strip().lower()
    if v_low in ("true", "1", "yes", "y"):
        return True
    if v_low in ("false", "0", "no", "n"):
        return False
    raise ValueError(f"cannot coerce {v!r} to boolean")
