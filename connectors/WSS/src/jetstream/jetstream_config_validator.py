"""Runtime validation of a connector instance's source_config (spec §7)."""
from __future__ import annotations

from typing import Any, Mapping

class SourceConfigInvalid(ValueError):
    def __init__(self, message: str, path: str = "", rule: str = ""):
        super().__init__(message)
        self.path = path
        self.rule = rule

def validate_source_config(source_config: Mapping[str, Any], config_schema: Mapping[str, Any]) -> None:
    try:
        import jsonschema
    except ImportError as exc:
        raise RuntimeError("jsonschema is required for source_config validation.") from exc

    validator = jsonschema.Draft7Validator(config_schema)
    errors = sorted(validator.iter_errors(source_config), key=lambda e: e.absolute_path)
    if not errors:
        return
    first = errors[0]
    path = ".".join(str(p) for p in first.absolute_path) or "<root>"
    raise SourceConfigInvalid(
        f"source_config validation failed at {path}: {first.message}",
        path=path,
        rule=first.validator or "",
    )