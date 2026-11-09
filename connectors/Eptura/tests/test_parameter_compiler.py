"""Tests for parameters.yaml → JSON Schema compilation."""
from __future__ import annotations

import textwrap
from pathlib import Path

import pytest

from src.jetstream.parameter_compiler import (
    SCOPE_KEY,
    compile_parameters,
    compile_parameters_yaml,
    run_scoped_names,
)


def test_compiles_basic_types_and_defaults():
    schema = compile_parameters([
        {"name": "a", "type": "string", "required": True},
        {"name": "b", "type": "integer", "default": 10},
        {"name": "c", "type": "boolean"},
    ])
    assert schema["type"] == "object"
    assert schema["properties"]["a"] == {"type": "string", SCOPE_KEY: "instance"}
    assert schema["properties"]["b"] == {
        "type": "integer", "default": 10, SCOPE_KEY: "instance",
    }
    assert schema["required"] == ["a"]


def test_parameters_are_instance_scoped_by_default():
    schema = compile_parameters([{"name": "a", "type": "string"}])
    assert run_scoped_names(schema) == set()


def test_run_scope_is_annotated_and_discoverable():
    schema = compile_parameters([
        {"name": "page_size", "type": "integer"},
        {"name": "objects", "type": "string", "scope": "run"},
    ])
    assert schema["properties"]["objects"][SCOPE_KEY] == "run"
    assert run_scoped_names(schema) == {"objects"}


def test_run_scoped_parameters_are_still_validated():
    """Stripping them from source_config must not stop the schema from
    catching a bad value."""
    import jsonschema
    schema = compile_parameters([
        {"name": "trigger_type", "type": "string", "scope": "run",
         "enum": ["scheduled", "manual"]},
    ])
    with pytest.raises(jsonschema.ValidationError):
        jsonschema.Draft7Validator(schema).validate({"trigger_type": "nonsense"})


def test_rejects_unknown_scope():
    with pytest.raises(ValueError, match="unsupported scope"):
        compile_parameters([{"name": "x", "type": "string", "scope": "global"}])


def test_rejects_required_run_scoped_parameter():
    with pytest.raises(ValueError, match="cannot be required"):
        compile_parameters([
            {"name": "x", "type": "string", "scope": "run", "required": True},
        ])


def test_enum_and_description_pass_through():
    schema = compile_parameters([
        {"name": "env", "type": "string", "required": True,
         "enum": ["dev", "prod"], "description": "which one"},
    ])
    p = schema["properties"]["env"]
    assert p["enum"] == ["dev", "prod"]
    assert p["description"] == "which one"


def test_depends_on_produces_conditional_required():
    schema = compile_parameters([
        {"name": "auth_method", "type": "string", "required": True, "enum": ["iam", "key"]},
        {"name": "role_arn", "type": "string", "required": True,
         "depends_on": {"auth_method": "iam"}},
    ])
    assert "allOf" in schema
    branch = schema["allOf"][0]
    assert branch["if"]["properties"]["auth_method"]["const"] == "iam"
    assert branch["then"]["required"] == ["role_arn"]
    # role_arn is not in the top-level unconditionally-required list
    assert "role_arn" not in schema.get("required", [])


def test_rejects_unknown_type():
    with pytest.raises(ValueError, match="unsupported type"):
        compile_parameters([{"name": "x", "type": "date"}])


def test_rejects_duplicate_name():
    with pytest.raises(ValueError, match="duplicate parameter"):
        compile_parameters([
            {"name": "x", "type": "string"},
            {"name": "x", "type": "string"},
        ])


def test_compile_from_file(tmp_path: Path):
    p = tmp_path / "params.yaml"
    p.write_text(textwrap.dedent("""
        parameters:
          - name: bucket
            type: string
            required: true
          - name: prefix
            type: string
            default: ""
    """).strip())
    schema = compile_parameters_yaml(p)
    assert schema["required"] == ["bucket"]
    assert schema["properties"]["prefix"]["default"] == ""


def test_repo_eptura_parameters_yaml_compiles():
    """The connector's own parameters.yaml must be well-formed."""
    repo_root = Path(__file__).resolve().parent.parent
    schema = compile_parameters_yaml(repo_root / "connectors" / "eptura" / "parameters.yaml")
    assert "environment" in schema["properties"]
    assert schema["properties"]["environment"]["enum"] == ["dev", "prod"]
    for req in ["environment", "instance_name", "destination_catalog",
                "destination_schema", "secret_scope", "redirect_uri"]:
        assert req in schema["required"], f"expected {req} to be required"


# ---------------------------------------------------------------------------
# coerce_widget_values
# ---------------------------------------------------------------------------

from src.jetstream.parameter_compiler import coerce_widget_values


def test_coerce_converts_string_ints_and_floats():
    schema = compile_parameters([
        {"name": "sla_target_minutes", "type": "integer"},
        {"name": "rate_limit_qps", "type": "number"},
        {"name": "name", "type": "string"},
    ])
    out = coerce_widget_values(
        {"sla_target_minutes": "1440", "rate_limit_qps": "10", "name": "abc"},
        schema,
    )
    assert out == {"sla_target_minutes": 1440, "rate_limit_qps": 10.0, "name": "abc"}


def test_coerce_bool_variants():
    schema = compile_parameters([{"name": "flag", "type": "boolean"}])
    for v in ("true", "True", "1", "yes", "Y"):
        assert coerce_widget_values({"flag": v}, schema)["flag"] is True
    for v in ("false", "0", "no", "N"):
        assert coerce_widget_values({"flag": v}, schema)["flag"] is False


def test_empty_string_on_typed_field_is_dropped():
    """Empty widget value on optional numeric field falls back to schema default."""
    schema = compile_parameters([{"name": "sla", "type": "integer", "default": 1440}])
    out = coerce_widget_values({"sla": ""}, schema)
    assert "sla" not in out


def test_uncoercable_value_passes_through_for_validator_to_flag():
    schema = compile_parameters([{"name": "x", "type": "number"}])
    out = coerce_widget_values({"x": "abc"}, schema)
    assert out == {"x": "abc"}  # validator will surface a clean error


def test_string_fields_pass_through_unchanged():
    schema = compile_parameters([{"name": "env", "type": "string"}])
    out = coerce_widget_values({"env": "dev"}, schema)
    assert out == {"env": "dev"}


def test_widget_keys_not_in_schema_are_kept():
    """Widget dict may include per-run params (mode, trigger_type) that aren't
    part of the manifest schema — they must pass through untouched."""
    schema = compile_parameters([{"name": "env", "type": "string"}])
    out = coerce_widget_values({"env": "dev", "mode": "run", "objects": ""}, schema)
    assert out == {"env": "dev", "mode": "run", "objects": ""}


def test_coerce_full_eptura_widget_set_validates():
    """End-to-end: coerce real widget-like values, then validate against the
    live eptura parameters.yaml schema."""
    import jsonschema
    repo_root = Path(__file__).resolve().parent.parent
    schema = compile_parameters_yaml(repo_root / "connectors" / "eptura" / "parameters.yaml")
    widgets = {
        "environment": "dev",
        "instance_name": "eptura_sandbox",
        "metadata_catalog": "raw_dev",
        "metadata_schema": "jetstream_eptura",
        "destination_catalog": "raw_dev",
        "destination_schema": "eptura",
        "secret_scope": "example_scope",
        "api_base_path": "/external/api/rest/v2",
        "redirect_uri": "https://your-tenant.iofficeconnect.com/",
        "oauth_scope": "email",
        "page_size": "1000",
        "sla_target_minutes": "1440",
        "rate_limit_qps": "10",
        "timeout_seconds": "60",
        "retry_max_attempts": "5",
        "tombstone_guard_pct": "20",
        "enabled_objects": "users,rooms",
        "owner_team": "data-platform",
        "objects": "",
        "trigger_type": "scheduled",
    }
    coerced = coerce_widget_values(widgets, schema)
    # Should not raise:
    jsonschema.Draft7Validator(schema).validate(coerced)


def test_eptura_schema_marks_only_per_run_parameters_as_run_scoped():
    repo_root = Path(__file__).resolve().parent.parent
    schema = compile_parameters_yaml(repo_root / "connectors" / "eptura" / "parameters.yaml")
    # After aligning with Juniper Mist: trigger_type, job_id, run_id, workspace_id
    # are all auto-detected from the Databricks context — no longer parameters.
    assert run_scoped_names(schema) == {"objects"}


def test_instance_parameters_stay_instance_scoped():
    """Guards against a new parameter accidentally being marked run-scoped
    and silently dropping out of connector_instance.source_config."""
    repo_root = Path(__file__).resolve().parent.parent
    schema = compile_parameters_yaml(repo_root / "connectors" / "eptura" / "parameters.yaml")
    instance_scoped = set(schema["properties"]) - run_scoped_names(schema)
    assert {"environment", "instance_name", "secret_scope", "enabled_objects",
            "destination_catalog", "tombstone_guard_pct"} <= instance_scoped
