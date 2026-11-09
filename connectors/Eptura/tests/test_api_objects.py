"""The Eptura entity catalog."""
from __future__ import annotations

import pytest

from src import api_objects as m


def test_catalog_covers_the_five_eptura_entities():
    assert {o.name for o in m.ENTITIES} == {
        "rooms", "buildings", "neighborhoods", "floors", "users",
    }


def test_every_entity_is_keyed_on_id():
    assert all(list(o.primary_key) == ["id"] for o in m.ENTITIES)


def test_only_neighborhoods_hydrate_the_item_endpoint():
    """List /neighborhoods omits floor; GET /neighborhoods/{id} returns it."""
    flags = {o.name: o.fetch_detail for o in m.ENTITIES}
    assert flags == {
        "rooms": False,
        "buildings": False,
        "neighborhoods": True,
        "floors": False,
        "users": False,
    }


def test_empty_selection_means_all():
    assert m.select_objects("") == m.ENTITIES


def test_subset_keeps_catalog_order():
    assert [o.name for o in m.select_objects("users,rooms")] == ["rooms", "users"]


def test_whitespace_and_blanks_are_tolerated():
    assert [o.name for o in m.select_objects(" users , , rooms ")] == ["rooms", "users"]


def test_unknown_entity_raises():
    with pytest.raises(ValueError, match="unknown Eptura entity"):
        m.select_objects("not_a_thing")
