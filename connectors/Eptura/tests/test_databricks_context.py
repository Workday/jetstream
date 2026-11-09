"""Tests for the metadata catalog / schema resolver and Databricks context.

Covers spec §3.2 dev-sandbox rule: developers iterating on a new connector
type point their instance at `jetstream_<connector_type>` in the dev catalog;
prod instances must always land in the shared `jetstream` schema.
"""
from __future__ import annotations

import pytest

from src.jetstream.databricks_context import read_databricks_context, resolve_metadata_catalog


def test_default_schema_is_shared_jetstream():
    m = resolve_metadata_catalog("dev")
    assert m.catalog == "metadata_dev"
    assert m.schema == "jetstream"


def test_prod_maps_to_prod_catalog():
    m = resolve_metadata_catalog("prod")
    assert m.catalog == "metadata_prod"
    assert m.schema == "jetstream"


def test_dev_sandbox_schema_is_accepted():
    m = resolve_metadata_catalog("dev", "jetstream_eptura")
    assert m.catalog == "metadata_dev"
    assert m.schema == "jetstream_eptura"


def test_prod_rejects_schema_override():
    with pytest.raises(ValueError, match="only permitted in dev"):
        resolve_metadata_catalog("prod", "jetstream_eptura")


def test_dev_sandbox_catalog_override_is_accepted():
    m = resolve_metadata_catalog("dev", "jetstream_eptura", "raw_dev")
    assert m.catalog == "raw_dev"
    assert m.schema == "jetstream_eptura"


def test_prod_rejects_catalog_override():
    with pytest.raises(ValueError, match="metadata_catalog override"):
        resolve_metadata_catalog("prod", catalog="raw_prod")


def test_empty_catalog_string_falls_back_to_default():
    m = resolve_metadata_catalog("dev", catalog="")
    assert m.catalog == "metadata_dev"


def test_schema_must_start_with_jetstream():
    with pytest.raises(ValueError, match="must start with"):
        resolve_metadata_catalog("dev", "random_schema")


def test_unknown_environment_rejected():
    with pytest.raises(ValueError, match="environment must be one of"):
        resolve_metadata_catalog("staging")


def test_qual_builds_backticked_three_part_name():
    m = resolve_metadata_catalog("dev", "jetstream_eptura")
    assert m.qual("connector_run") == "`metadata_dev`.`jetstream_eptura`.`connector_run`"


# ---------------------------------------------------------------------------
# run identification
# ---------------------------------------------------------------------------

def test_no_dbutils_gives_placeholders():
    ctx = read_databricks_context(None)
    assert ctx.workspace_id == "local"
    assert ctx.job_id == "local"
    assert ctx.job_run_id == "local"
    assert ctx.triggered_by == "local"
    assert ctx.trigger_type == "manual"


class _MockDbutils:
    """Simulates the Databricks notebook context tags API.

    The Scala chain is:
        dbutils.notebook.entry_point.getDbutils().notebook().getContext().tags()
    """

    def __init__(self, tags: dict):
        self._tags = tags
        self.notebook = _NB(tags)


class _NB:
    def __init__(self, tags):
        self.entry_point = _EntryPoint(tags)


class _EntryPoint:
    def __init__(self, tags):
        self._tags = tags

    def getDbutils(self):
        return _InnerDbutils(self._tags)


class _InnerDbutils:
    def __init__(self, tags):
        self._tags = tags

    def notebook(self):
        return _InnerNB(self._tags)


class _InnerNB:
    def __init__(self, tags):
        self._tags = tags

    def getContext(self):
        return _Context(self._tags)


class _Context:
    def __init__(self, tags):
        self._tags = tags

    def tags(self):
        return _ScalaMap(self._tags)


class _ScalaMap:
    """Minimal stand-in for a Scala Map[String, String]."""

    def __init__(self, data: dict):
        self._data = data

    def get(self, key: str):
        return _ScalaOption(self._data.get(key))


class _ScalaOption:
    def __init__(self, value):
        self._value = value

    def isDefined(self):
        return self._value is not None

    def get(self):
        return self._value


def test_context_reads_job_and_run_ids_from_tags():
    tags = {
        "orgId": "111",
        "jobId": "222",
        "jobRunId": "333",
        "userName": "svc-eptura",
        "jobTriggerType": "PERIODIC",
    }
    ctx = read_databricks_context(_MockDbutils(tags))
    assert ctx.workspace_id == "111"
    assert ctx.job_id == "222"
    assert ctx.job_run_id == "333"
    assert ctx.triggered_by == "svc-eptura"
    assert ctx.trigger_type == "scheduled"


def test_trigger_type_periodic_maps_to_scheduled():
    ctx = read_databricks_context(_MockDbutils({
        "orgId": "1", "jobId": "2", "jobRunId": "3",
        "jobTriggerType": "PERIODIC",
    }))
    assert ctx.trigger_type == "scheduled"


def test_trigger_type_one_time_maps_to_api():
    ctx = read_databricks_context(_MockDbutils({
        "orgId": "1", "jobId": "2", "jobRunId": "3",
        "jobTriggerType": "ONE_TIME",
    }))
    assert ctx.trigger_type == "api"


def test_trigger_type_manual_maps_to_manual():
    ctx = read_databricks_context(_MockDbutils({
        "orgId": "1", "jobId": "2", "jobRunId": "3",
        "jobTriggerType": "MANUAL",
    }))
    assert ctx.trigger_type == "manual"


def test_unknown_trigger_type_defaults_to_manual():
    ctx = read_databricks_context(_MockDbutils({
        "orgId": "1", "jobId": "2", "jobRunId": "3",
        "jobTriggerType": "SOMETHING_NEW",
    }))
    assert ctx.trigger_type == "manual"


class _ExplodingDbutils:
    """Stands in for a broken/missing notebook context."""

    @property
    def notebook(self):
        class _NB:
            @property
            def entry_point(self):
                raise AttributeError("no notebook context")
        return _NB()


def test_missing_context_raises_with_actionable_message():
    with pytest.raises(RuntimeError, match="failed to read Databricks Jobs runtime context"):
        read_databricks_context(_ExplodingDbutils())
