"""Metadata catalog resolution."""
from __future__ import annotations

import pytest

from src.jetstream.jetstream_context import resolve_metadata_catalog


def test_dev_defaults():
    meta = resolve_metadata_catalog("dev")
    assert meta.catalog == "metadata_dev"
    assert meta.schema == "jetstream"


def test_prod_defaults():
    meta = resolve_metadata_catalog("prod")
    assert meta.catalog == "metadata_prod"
    assert meta.schema == "jetstream"


def test_dev_sandbox_is_accepted():
    meta = resolve_metadata_catalog("dev", "jetstream_wss", "raw_dev")
    assert meta.catalog == "raw_dev"
    assert meta.schema == "jetstream_wss"
    assert meta.qual("connector_run") == "`raw_dev`.`jetstream_wss`.`connector_run`"


def test_prod_rejects_catalog_override():
    with pytest.raises(ValueError, match="only permitted in dev"):
        resolve_metadata_catalog("prod", catalog="raw_prod")


def test_prod_rejects_schema_override():
    with pytest.raises(ValueError, match="only permitted in dev"):
        resolve_metadata_catalog("prod", "jetstream_wss")


def test_schema_must_start_with_jetstream():
    with pytest.raises(ValueError, match="must start with"):
        resolve_metadata_catalog("dev", "random_schema")
