"""Tests for the runtime source_config validator."""
from __future__ import annotations

import pytest

from src.jetstream.parameter_compiler import compile_parameters
from src.jetstream.config_validator import SourceConfigInvalid, validate_source_config


@pytest.fixture
def schema():
    return compile_parameters([
        {"name": "environment", "type": "string", "required": True, "enum": ["dev", "prod"]},
        {"name": "instance_name", "type": "string", "required": True},
        {"name": "sla_target_minutes", "type": "integer", "default": 1440},
    ])


def test_valid_config_passes(schema):
    validate_source_config(
        {"environment": "dev", "instance_name": "x", "sla_target_minutes": 60},
        schema,
    )


def test_missing_required_fails(schema):
    with pytest.raises(SourceConfigInvalid) as exc:
        validate_source_config({"environment": "dev"}, schema)
    assert "instance_name" in str(exc.value)


def test_enum_violation_fails(schema):
    with pytest.raises(SourceConfigInvalid) as exc:
        validate_source_config(
            {"environment": "staging", "instance_name": "x"}, schema,
        )
    assert "staging" in str(exc.value) or "enum" in exc.value.rule
