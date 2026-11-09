"""parameters.yaml compiles to the public catalog names."""
from __future__ import annotations

from pathlib import Path

from src.jetstream.params import compile_parameters_yaml

_YAML = Path(__file__).resolve().parents[1] / "connectors" / "servicenow" / "parameters.yaml"


def test_schema_uses_generic_catalogs():
    schema = compile_parameters_yaml(_YAML)
    environment = schema["properties"]["environment"]
    assert environment["enum"] == ["dev", "prod"]
    assert "metadata_dev" in environment["description"]
    assert "metadata_prod" in environment["description"]
    destination = schema["properties"]["destination_catalog"]["description"]
    assert "raw_dev" in destination
    assert schema["properties"]["enabled_objects"]["default"] == "incident"
    assert "secret_scope" in schema["required"]
    assert "instance_name" in schema["required"]
    assert "destination_catalog" in schema["required"]
