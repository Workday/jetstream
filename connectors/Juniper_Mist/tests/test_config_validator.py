"""Tests for src/jetstream/config_validator.py."""
from __future__ import annotations

import pytest

from src.jetstream.config_validator import SourceConfigInvalid, validate_source_config


SCHEMA = {
    "$schema": "http://json-schema.org/draft-07/schema#",
    "type": "object",
    "properties": {
        "environment": {"type": "string", "enum": ["dev", "prod"]},
        "instance_name": {"type": "string"},
        "rate_limit_qps": {"type": "number"},
        "enabled_objects": {"type": "string"},
    },
    "required": ["environment", "instance_name"],
}


def test_valid_config_passes():
    validate_source_config({"environment": "dev", "instance_name": "test"}, SCHEMA)


def test_missing_required_raises():
    with pytest.raises(SourceConfigInvalid, match="validation failed"):
        validate_source_config({"environment": "dev"}, SCHEMA)


def test_wrong_enum_raises():
    with pytest.raises(SourceConfigInvalid, match="environment"):
        validate_source_config({"environment": "staging", "instance_name": "test"}, SCHEMA)


def test_source_config_invalid_has_path_and_rule():
    try:
        validate_source_config({"environment": "staging", "instance_name": "test"}, SCHEMA)
    except SourceConfigInvalid as exc:
        assert exc.path != ""
        assert exc.rule != ""


def test_additional_properties_allowed():
    validate_source_config(
        {"environment": "dev", "instance_name": "test", "extra_field": "allowed"},
        SCHEMA,
    )
