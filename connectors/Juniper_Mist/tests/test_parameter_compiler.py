"""Tests for src/jetstream/parameter_compiler.py."""
from __future__ import annotations

import pytest


def test_compile_parameters_yaml_roundtrip():
    from pathlib import Path
    from src.jetstream.parameter_compiler import compile_parameters_yaml

    yaml_path = Path(__file__).parent.parent / "connectors" / "juniper_mist" / "parameters.yaml"
    schema = compile_parameters_yaml(yaml_path)
    assert schema.get("type") == "object"
    required = schema.get("required", [])
    assert "environment" in required
    assert "instance_name" in required
    assert "secret_scope" in required
    assert "juniper_mist_org_id" in required


def test_compile_parameters_basic():
    from src.jetstream.parameter_compiler import compile_parameters

    params = [
        {"name": "foo", "type": "string", "required": True, "description": "A foo"},
        {"name": "bar", "type": "integer", "required": False, "default": 42},
    ]
    schema = compile_parameters(params)
    assert schema["properties"]["foo"]["type"] == "string"
    assert schema["properties"]["bar"]["type"] == "integer"
    assert schema["properties"]["bar"]["default"] == 42
    assert "foo" in schema["required"]
    assert "bar" not in schema.get("required", [])


def test_compile_duplicate_raises():
    from src.jetstream.parameter_compiler import compile_parameters

    with pytest.raises(ValueError, match="duplicate"):
        compile_parameters([
            {"name": "foo", "type": "string"},
            {"name": "foo", "type": "string"},
        ])


def test_compile_unsupported_type_raises():
    from src.jetstream.parameter_compiler import compile_parameters

    with pytest.raises(ValueError, match="unsupported type"):
        compile_parameters([{"name": "x", "type": "array"}])


def test_coerce_widget_values():
    from src.jetstream.parameter_compiler import coerce_widget_values

    schema = {
        "properties": {
            "rate_limit_qps": {"type": "number"},
            "lookback_days": {"type": "integer"},
            "enabled": {"type": "boolean"},
            "name": {"type": "string"},
        }
    }
    result = coerce_widget_values(
        {"rate_limit_qps": "5.5", "lookback_days": "7", "enabled": "true", "name": "abc"},
        schema,
    )
    assert result["rate_limit_qps"] == 5.5
    assert result["lookback_days"] == 7
    assert result["enabled"] is True
    assert result["name"] == "abc"


def test_coerce_empty_numeric_dropped():
    from src.jetstream.parameter_compiler import coerce_widget_values

    schema = {"properties": {"n": {"type": "integer"}}}
    result = coerce_widget_values({"n": ""}, schema)
    assert "n" not in result
