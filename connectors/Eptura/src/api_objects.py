"""Eptura iOffice entity catalog.

Five REST resources, each reachable at `{api_base_path}/{name}` and keyed on
`id`. Every run reads the whole collection and MERGEs it into the destination
table.

Eptura does support incremental reads — most collections accept a
`modifiedOrCreatedAfter` filter — but a cursor can't surface deletions, and
tombstoning is part of what this raw layer promises. Full snapshots keep that
correct at a volume (~45k records) where the cost is not worth optimising. If
that changes, the shape to adopt is incremental daily plus a periodic full
reconcile, not incremental alone.

There is no `spaces` collection, despite the term being everywhere in the
Eptura Workplace UI. `spaces`, `space`, `workspaces`, `seats`, and `desks` all
404. What the product calls a space is a row in `rooms`, whose `type` spans
Workstation, Phone Room, Huddle Room, and the conference rooms — desks
included. Sync `rooms` and you have the spaces.

To add an entity: append an ObjectCfg here and a test case in
tests/test_api_objects.py. Note that the name doubles as the URL path segment, so
nested collections such as `maintenance/requests` would first need a separate
path field on ObjectCfg. Set `fetch_detail=True` when the list endpoint omits
a nested relation that the item endpoint returns (neighborhoods / `floor`).
`notebook/99_connectivity_probe.py` reports what this tenant can actually read.
"""
from __future__ import annotations

from typing import List

from src.connector_config import ObjectCfg


ENTITIES: List[ObjectCfg] = [
    ObjectCfg(name="rooms"),
    ObjectCfg(name="buildings"),
    ObjectCfg(name="neighborhoods", fetch_detail=True),
    ObjectCfg(name="floors"),
    ObjectCfg(name="users"),
]

ALL_BY_NAME = {o.name: o for o in ENTITIES}


def select_objects(enabled_csv: str = "") -> List[ObjectCfg]:
    """Resolve the `enabled_objects` parameter into a plan. Empty means all."""
    wanted = {s.strip() for s in enabled_csv.split(",") if s.strip()}
    unknown = wanted - set(ALL_BY_NAME)
    if unknown:
        raise ValueError(
            f"unknown Eptura entity: {sorted(unknown)}; known: {sorted(ALL_BY_NAME)}"
        )
    return [o for o in ENTITIES if not wanted or o.name in wanted]
