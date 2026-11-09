"""Tests for the metadata catalog / schema resolver.

Covers spec §3.2 dev-sandbox rule: developers iterating on a new connector
type point their instance at `jetstream_<connector_type>` in the dev catalog;
prod instances must always land in the shared `jetstream` schema.
"""
from __future__ import annotations

import pytest

from src.jetstream.ctx import resolve_metadata_catalog


def test_default_schema_is_shared_jetstream():
    m = resolve_metadata_catalog("dev")
    assert m.catalog == "metadata_dev"
    assert m.schema == "jetstream"


def test_prod_maps_to_prod_catalog():
    m = resolve_metadata_catalog("prod")
    assert m.catalog == "metadata_prod"
    assert m.schema == "jetstream"


def test_dev_sandbox_schema_is_accepted():
    m = resolve_metadata_catalog("dev", "jetstream_zoom")
    assert m.catalog == "metadata_dev"
    assert m.schema == "jetstream_zoom"


def test_prod_rejects_schema_override():
    with pytest.raises(ValueError, match="only permitted in dev"):
        resolve_metadata_catalog("prod", "jetstream_zoom")


def test_dev_sandbox_catalog_override_is_accepted():
    m = resolve_metadata_catalog("dev", "jetstream_zoom", "raw_dev")
    assert m.catalog == "raw_dev"
    assert m.schema == "jetstream_zoom"


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
    m = resolve_metadata_catalog("dev", "jetstream_zoom")
    assert m.qual("connector_run") == "`metadata_dev`.`jetstream_zoom`.`connector_run`"
