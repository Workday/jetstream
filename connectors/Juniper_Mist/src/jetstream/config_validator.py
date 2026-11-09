"""Runtime validation of a connector instance's source_config.

Spec §7 "Runtime Validation": every connector job validates its own
source_config against the compiled config_schema before touching the source
system. This is a thin wrapper around jsonschema so every connector uses
the same validator with the same error shape.
"""
from __future__ import annotations

from typing import Any, Mapping


class SourceConfigInvalid(ValueError):
    """Raised when source_config does not match its connector_manifest schema."""

    def __init__(self, message: str, path: str = "", rule: str = ""):
        super().__init__(message)
        self.path = path
        self.rule = rule


def validate_source_config(
    source_config: Mapping[str, Any],
    config_schema: Mapping[str, Any],
) -> None:
    """Validate source_config against config_schema; raise SourceConfigInvalid on failure."""
    try:
        import jsonschema
    except ImportError as exc:  # pragma: no cover
        raise RuntimeError(
            "jsonschema is required for source_config validation; add it to requirements.txt"
        ) from exc

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
