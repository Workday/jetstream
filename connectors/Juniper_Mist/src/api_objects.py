"""Juniper Mist object catalog.

The catalog lives in code (not per-instance YAML) because these are source-side
facts about the Mist API: primary keys, load strategy, cursor fields, fan-out
parents, etc. Different connector_instance rows for the same connector type
can only differ in *which* objects are enabled, not in the objects' definitions.

Adding a new Mist object:
  1. Add an ObjectCfg entry to PRIMARY or OPTIONAL below.
  2. Add an ENDPOINTS entry in src/api_extractor.py.
  That's it — no config file edit needed.

Fan-out parent chain (objects must appear in this order for parent_id_cache
to be populated before dependent fan-out objects run):

  Phase A — Org-level (no dependencies):
    org, sitegroups, inventory, device_stats, org_wlans

  Phase B — Parent of all per-site fan-outs:
    sites

  Phase C — Per-site fan-out (all sites):
    maps, zones, zone_stats, site_wlans, site_settings,
    mapstacks (opt), rssizones (opt), rssizone_stats (opt)

  Phase D — Per-site fan-out, history (filtered sites only):
    zone_visits, zone_visits_by_zone, clients_search,
    clients (opt), client_counts (opt), assets (opt),
    sdkclients (opt), guests (opt), guests_search (opt)

  Phase E — Per-map double fan-out:
    map_clients (opt), unconnected_clients (opt)

`sites_filtered` is a synthetic parent key populated by pipeline_orchestrator
after syncing `sites` by applying the `site_filter` widget.
"""
from __future__ import annotations

from src.connector_config import ObjectCfg


# ---------------------------------------------------------------------------
# PRIMARY — mandatory objects; missing any → run fails
# ---------------------------------------------------------------------------

PRIMARY: list[ObjectCfg] = [
    # Phase A — org-level
    ObjectCfg(
        name="org",
        primary_key=["id"],
        load_strategy="full",
    ),
    ObjectCfg(
        name="sitegroups",
        primary_key=["id"],
        load_strategy="full",
    ),
    ObjectCfg(
        name="inventory",
        primary_key=["id"],
        load_strategy="full",
    ),
    ObjectCfg(
        name="device_stats",
        # snapshot: AP hardware metrics captured at each run; append-only so
        # AP utilisation trends are queryable across hourly runs.
        # PK identifies the AP within a snapshot; rows are NOT unique across runs.
        primary_key=["id"],
        load_strategy="snapshot",
    ),
    ObjectCfg(
        name="org_wlans",
        primary_key=["id"],
        load_strategy="full",
    ),
    # Phase B — parent of all per-site fan-outs
    ObjectCfg(
        name="sites",
        primary_key=["id"],
        load_strategy="full",
    ),
    # Phase C — per-site space model (all sites, no filter)
    ObjectCfg(
        name="maps",
        primary_key=["id"],
        load_strategy="full",
        parent="sites",
        parent_id_column="site_id",
    ),
    ObjectCfg(
        name="zones",
        primary_key=["id"],
        load_strategy="full",
        parent="sites",
        parent_id_column="site_id",
    ),
    ObjectCfg(
        name="zone_stats",
        primary_key=["id"],
        load_strategy="full",
        parent="sites",
        parent_id_column="site_id",
    ),
    ObjectCfg(
        name="site_wlans",
        primary_key=["id"],
        load_strategy="full",
        parent="sites",
        parent_id_column="site_id",
    ),
    ObjectCfg(
        name="site_settings",
        primary_key=["site_id"],
        load_strategy="full",
        parent="sites",
        parent_id_column="site_id",
    ),
    # Phase D — per-site history (filtered sites via sites_filtered)
    ObjectCfg(
        name="zone_visits",
        # Composite PK: site + device + millisecond-precision enter timestamp
        primary_key=["site_id", "mac", "enter_epoch"],
        load_strategy="incremental_cursor",
        cursor_field="timestamp",
        lookback_minutes=30,
        parent="sites_filtered",
        parent_id_column="site_id",
        cursor_param_from="start",
        cursor_param_to="end",
    ),
    ObjectCfg(
        name="zone_visits_by_zone",
        primary_key=["site_id", "scope_id", "mac", "enter_epoch"],
        load_strategy="incremental_cursor",
        cursor_field="timestamp",
        lookback_minutes=30,
        parent="sites_filtered",
        parent_id_column="site_id",
        cursor_param_from="start",
        cursor_param_to="end",
    ),
    ObjectCfg(
        name="clients_search",
        primary_key=["site_id", "mac", "timestamp"],
        load_strategy="incremental_cursor",
        cursor_field="timestamp",
        lookback_minutes=60,
        parent="sites_filtered",
        parent_id_column="site_id",
        cursor_param_from="start",
        cursor_param_to="end",
    ),
]


# ---------------------------------------------------------------------------
# OPTIONAL — best-effort; failure or empty response → WARNING, run continues
# ---------------------------------------------------------------------------

OPTIONAL: list[ObjectCfg] = [
    # Phase C — per-site space model (all sites, no filter)
    ObjectCfg(
        name="mapstacks",
        primary_key=["id"],
        load_strategy="full",
        parent="sites",
        parent_id_column="site_id",
        optional=True,
    ),
    ObjectCfg(
        name="rssizones",
        primary_key=["id"],
        load_strategy="full",
        parent="sites",
        parent_id_column="site_id",
        optional=True,
    ),
    ObjectCfg(
        name="rssizone_stats",
        primary_key=["id"],
        load_strategy="full",
        parent="sites",
        parent_id_column="site_id",
        optional=True,
    ),
    # Phase D — per-site live snapshot and history (filtered sites)
    ObjectCfg(
        name="clients",
        # snapshot: live client roster captured at each run; append-only so
        # hourly snapshots build a movement history throughout the day.
        # PK = (site_id, mac) within one snapshot; NOT unique across runs.
        primary_key=["site_id", "mac"],
        load_strategy="snapshot",
        parent="sites_filtered",
        parent_id_column="site_id",
        optional=True,
    ),
    ObjectCfg(
        name="assets",
        primary_key=["site_id", "mac"],
        load_strategy="full",
        parent="sites_filtered",
        parent_id_column="site_id",
        optional=True,
    ),
    ObjectCfg(
        name="sdkclients",
        primary_key=["site_id", "id"],
        load_strategy="full",
        parent="sites_filtered",
        parent_id_column="site_id",
        optional=True,
    ),
    ObjectCfg(
        name="guests",
        primary_key=["site_id", "mac"],
        load_strategy="full",
        parent="sites",
        parent_id_column="site_id",
        optional=True,
    ),
    ObjectCfg(
        name="guests_search",
        primary_key=["site_id", "mac", "timestamp"],
        load_strategy="incremental_cursor",
        cursor_field="timestamp",
        lookback_minutes=60,
        parent="sites_filtered",
        parent_id_column="site_id",
        cursor_param_from="start",
        cursor_param_to="end",
        optional=True,
    ),
    # Phase E — double fan-out: per map within each site
    ObjectCfg(
        name="map_clients",
        # snapshot: positioned clients on a floor-plan at each run; append-only
        # so hourly snapshots track client movement across zones/maps.
        # PK = (site_id, map_id, mac) within one snapshot; NOT unique across runs.
        primary_key=["site_id", "map_id", "mac"],
        load_strategy="snapshot",
        parent="maps",           # parent_id_cache["maps"] stores (site_id, map_id) dicts
        parent_id_column="map_id",
        optional=True,
    ),
    ObjectCfg(
        name="unconnected_clients",
        # snapshot: unassociated clients visible on a floor-plan at each run;
        # append-only to track presence patterns over time.
        # PK = (site_id, map_id, mac) within one snapshot; NOT unique across runs.
        primary_key=["site_id", "map_id", "mac"],
        load_strategy="snapshot",
        parent="maps",
        parent_id_column="map_id",
        optional=True,
    ),
]


ALL_BY_NAME: dict[str, ObjectCfg] = {o.name: o for o in (*PRIMARY, *OPTIONAL)}


def select_objects(enabled_objects_csv: str = "") -> list[ObjectCfg]:
    """Return the ObjectCfg list for this run, in dependency-safe order.

    If `enabled_objects_csv` is empty, ALL objects (primary + optional) are
    returned. Optional objects that return empty or error from the API are
    handled gracefully (warning logged, run continues).
    If non-empty, only the named objects are returned (primary and optional
    are both valid).
    Order is preserved as authored (Phase A → B → C → D → E) so parents
    always precede their fan-out children.
    """
    csv = (enabled_objects_csv or "").strip()
    if not csv:
        return list([*PRIMARY, *OPTIONAL])

    wanted = {s.strip() for s in csv.split(",") if s.strip()}
    unknown = wanted - set(ALL_BY_NAME)
    if unknown:
        raise ValueError(
            f"unknown Juniper Mist object(s) requested: {sorted(unknown)}; "
            f"known: {sorted(ALL_BY_NAME)}"
        )
    return [o for o in (*PRIMARY, *OPTIONAL) if o.name in wanted]


# ---------------------------------------------------------------------------
# Capability report — "what will I sync" (used by discover mode + run_plan log)
# ---------------------------------------------------------------------------

def build_capability_report(enabled_objects_csv: str = "") -> dict:
    """Return a structured report of which objects will run and why."""
    plan = select_objects(enabled_objects_csv)
    plan_names = {o.name for o in plan}

    rows = []
    for obj in (*PRIMARY, *OPTIONAL):
        tier = "optional" if obj.optional else "primary"
        will_sync = obj.name in plan_names
        rows.append({
            "object": obj.name,
            "tier": tier,
            "load_strategy": obj.load_strategy,
            "parent": obj.parent or "",
            "will_sync": will_sync,
        })
    return {
        "objects": rows,
        "code_catalog_size": len(ALL_BY_NAME),
        "will_sync": [r["object"] for r in rows if r["will_sync"]],
    }


def format_capability_report(report: dict) -> str:
    """Render a capability report as a human-readable table."""
    lines = ["=" * 88, "Jetstream Juniper Mist — capability report", "=" * 88]
    lines.append(f"{'object':<28} {'tier':<9} {'strategy':<22} {'parent':<20} {'will_sync'}")
    lines.append("-" * 88)
    for r in report["objects"]:
        lines.append(
            f"{r['object']:<28} {r['tier']:<9} {r['load_strategy']:<22} "
            f"{r['parent']:<20} {'YES' if r['will_sync'] else 'no'}"
        )
    lines.append("-" * 88)
    lines.append(f"Will sync this run: {report['will_sync'] or '(none)'}")
    lines.append("=" * 88)
    return "\n".join(lines)
