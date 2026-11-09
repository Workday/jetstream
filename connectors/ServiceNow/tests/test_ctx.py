"""Tests for the metadata catalog / schema resolver."""
from __future__ import annotations

import pytest

from src.jetstream.ctx import resolve_metadata_catalog


def test_default_schema_is_shared_jetstream():
    meta = resolve_metadata_catalog("dev")
    assert meta.catalog == "metadata_dev"
    assert meta.schema == "jetstream"


def test_prod_maps_to_prod_catalog():
    meta = resolve_metadata_catalog("prod")
    assert meta.catalog == "metadata_prod"
    assert meta.schema == "jetstream"


def test_dev_sandbox_schema_is_accepted():
    meta = resolve_metadata_catalog("dev", "jetstream_servicenow")
    assert meta.catalog == "metadata_dev"
    assert meta.schema == "jetstream_servicenow"


def test_prod_rejects_schema_override():
    with pytest.raises(ValueError, match="only permitted in dev"):
        resolve_metadata_catalog("prod", "jetstream_servicenow")


def test_dev_sandbox_catalog_override_is_accepted():
    meta = resolve_metadata_catalog("dev", "jetstream_servicenow", "raw_dev")
    assert meta.catalog == "raw_dev"
    assert meta.schema == "jetstream_servicenow"


def test_prod_rejects_catalog_override():
    with pytest.raises(ValueError, match="metadata_catalog override"):
        resolve_metadata_catalog("prod", catalog="raw_prod")


def test_empty_catalog_string_falls_back_to_default():
    meta = resolve_metadata_catalog("dev", catalog="")
    assert meta.catalog == "metadata_dev"


def test_schema_must_start_with_jetstream():
    with pytest.raises(ValueError, match="must start with"):
        resolve_metadata_catalog("dev", "random_schema")


def test_unknown_environment_rejected():
    with pytest.raises(ValueError, match="environment must be one of"):
        resolve_metadata_catalog("staging")


def test_qual_builds_backticked_three_part_name():
    meta = resolve_metadata_catalog("dev", "jetstream_servicenow")
    assert meta.qual("connector_run") == "`metadata_dev`.`jetstream_servicenow`.`connector_run`"
