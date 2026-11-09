"""Tests for src/jetstream/databricks_context.py."""
from __future__ import annotations

import pytest

from src.jetstream.databricks_context import (
    MetaCtx,
    resolve_metadata_catalog,
)


def test_dev_default_catalog():
    meta = resolve_metadata_catalog("dev")
    assert meta.catalog == "metadata_dev"
    assert meta.schema == "jetstream"


def test_prod_default_catalog():
    meta = resolve_metadata_catalog("prod")
    assert meta.catalog == "metadata_prod"
    assert meta.schema == "jetstream"


def test_dev_sandbox_override_catalog():
    meta = resolve_metadata_catalog("dev", catalog="raw_dev")
    assert meta.catalog == "raw_dev"


def test_dev_sandbox_override_schema():
    meta = resolve_metadata_catalog("dev", schema="jetstream_juniper_mist")
    assert meta.schema == "jetstream_juniper_mist"


def test_prod_catalog_override_rejected():
    with pytest.raises(ValueError, match="only permitted in dev"):
        resolve_metadata_catalog("prod", catalog="other_catalog")


def test_prod_schema_override_rejected():
    with pytest.raises(ValueError, match="only permitted in dev"):
        resolve_metadata_catalog("prod", schema="jetstream_juniper_mist")


def test_invalid_environment_raises():
    with pytest.raises(ValueError, match="environment must be one of"):
        resolve_metadata_catalog("staging")


def test_schema_must_start_with_jetstream():
    with pytest.raises(ValueError, match="must start with"):
        resolve_metadata_catalog("dev", schema="custom_schema")


def test_meta_ctx_qual():
    meta = MetaCtx(catalog="metadata_dev", schema="jetstream")
    assert meta.qual("connector_run") == "`metadata_dev`.`jetstream`.`connector_run`"


def test_read_databricks_context_none_returns_local():
    from src.jetstream.databricks_context import read_databricks_context
    ctx = read_databricks_context(None)
    assert ctx.workspace_id == "local"
    assert ctx.job_id == "local"
    assert ctx.trigger_type == "manual"


@pytest.mark.parametrize("raw,expected", [
    ("PERIODIC", "scheduled"),
    ("CRON", "scheduled"),
    ("ONE_TIME", "api"),
    ("MANUAL", "manual"),
    ("", "manual"),
    ("UNKNOWN_VALUE", "manual"),
])
def test_trigger_type_mapping(raw, expected):
    """Databricks jobTriggerType tag values map to our label correctly."""
    import types
    from src.jetstream.databricks_context import read_databricks_context

    def _make_opt(v):
        opt = types.SimpleNamespace()
        opt.isDefined = lambda: bool(v)
        opt.get = lambda: v
        return opt

    tags_dict = {"jobTriggerType": _make_opt(raw), "orgId": _make_opt("ws1"),
                 "jobId": _make_opt("j1"), "jobRunId": _make_opt("r1")}

    tag_map = types.SimpleNamespace()
    tag_map.get = lambda k: tags_dict.get(k)

    ctx_obj = types.SimpleNamespace()
    ctx_obj.tags = lambda: tag_map

    entry = types.SimpleNamespace()
    entry.getDbutils = lambda: types.SimpleNamespace(
        notebook=lambda: types.SimpleNamespace(getContext=lambda: ctx_obj)
    )

    notebook_ns = types.SimpleNamespace(
        entry_point=entry,
        getContext=lambda: ctx_obj,
    )

    class FakeDbutils:
        notebook = types.SimpleNamespace(
            entry_point=types.SimpleNamespace(
                getDbutils=lambda: types.SimpleNamespace(
                    notebook=lambda: types.SimpleNamespace(
                        getContext=lambda: ctx_obj
                    )
                )
            )
        )

    ctx = read_databricks_context(FakeDbutils())
    assert ctx.trigger_type == expected
