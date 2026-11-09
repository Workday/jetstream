"""Zoom object catalog.

The catalog lives in code (not in a per-instance YAML) because these are
source-side facts about the Zoom API: primary keys, load strategy, cursor
fields, parent fan-outs, required scopes, etc. Different `connector_instance`
rows for the same connector type can only differ in *which* of these
objects are enabled, not in the objects' definitions themselves.

Adding a new Zoom object (recordings, webinars, etc.) means:
  1. Add an entry to CATALOG below.
  2. Add an ENDPOINTS entry in src/extract.py.
  3. That's it — no config file edit, no per-instance change.
"""
from __future__ import annotations

from src.config import ObjectCfg


# ---------------------------------------------------------------------------
# Primary tier — mandatory objects. Missing scope on any of these fails the
# run fast (see src/extract.check_required_scopes).
# ---------------------------------------------------------------------------

PRIMARY: list[ObjectCfg] = [
    # Host enrichment
    ObjectCfg(
        name="users",
        primary_key=["id"],
        load_strategy="full",
        required_scope="user:read:list_users:admin",
    ),
    ObjectCfg(
        name="groups",
        primary_key=["id"],
        load_strategy="full",
        required_scope="group:read:list_groups:admin",
    ),
    # Space dimension
    ObjectCfg(
        name="rooms",
        primary_key=["id"],
        load_strategy="full",
        required_scope="zoom_rooms:read:list_rooms:admin",
    ),
    ObjectCfg(
        name="room_locations",
        primary_key=["id"],
        load_strategy="full",
        required_scope="zoom_rooms:read:list_locations:admin",
    ),
    ObjectCfg(
        name="room_devices",
        primary_key=["room_id", "id"],
        load_strategy="full",
        parent="rooms",
        parent_id_column="room_id",
        required_scope="zoom_rooms:read:list_devices:admin",
    ),
    # Booking facts
    ObjectCfg(
        name="meetings",
        primary_key=["uuid"],
        load_strategy="incremental_cursor",
        cursor_field="start_time",
        lookback_minutes=60,
        parent="users",
        required_scope="meeting:read:list_meetings:admin",
        query_params={"type": "previous_meetings"},
    ),
    ObjectCfg(
        name="past_meeting_participants",
        # Composite PK: (meeting_uuid, participant_id). We use uuid (not
        # numeric meeting id) because /past_meetings/{id}/participants
        # returns participants from the most recent occurrence only when
        # called with the numeric id; using uuid gives per-occurrence data
        # and enables correct joins to meetings.uuid.
        primary_key=["meeting_uuid", "id"],
        load_strategy="full",
        parent="meetings",
        parent_id_column="meeting_uuid",
        required_scope="meeting:read:list_past_participants:admin",
    ),
]


# ---------------------------------------------------------------------------
# Optional tier — Heavy-tier Dashboard endpoints. Missing scope or a
# per-object failure is logged but does NOT fail the run.
# ---------------------------------------------------------------------------

OPTIONAL: list[ObjectCfg] = [
    ObjectCfg(
        name="dashboard_meetings",
        primary_key=["uuid"],
        load_strategy="incremental_cursor",
        cursor_field="end_time",
        lookback_minutes=1440,
        optional=True,
        required_scope="dashboard:read:list_meetings:admin",
        query_params={"type": "past"},
        cursor_param_from="from",
        cursor_param_to="to",
        cursor_format="date",
    ),
    ObjectCfg(
        name="dashboard_meeting_participants",
        # Composite PK: (meeting_uuid, participant_uuid). Same rationale as
        # past_meeting_participants: /metrics/meetings/{id}/participants
        # called with numeric id collapses recurring-meeting occurrences to
        # the most recent. Fan out on uuid to fetch per-occurrence rows.
        primary_key=["meeting_uuid", "participant_uuid"],
        # NOT 'full' — the parent (dashboard_meetings) is date-windowed, so
        # each run's pull only covers meetings from the last N minutes. A
        # `full` load would tombstone every participant of every meeting
        # outside that window on every run. Use incremental_cursor semantics
        # (upsert, no tombstoning); the "cursor" is effectively driven by
        # the parent object's cursor.
        load_strategy="incremental_cursor",
        parent="dashboard_meetings",
        parent_id_column="meeting_uuid",
        optional=True,
        required_scope="dashboard:read:list_meeting_participants:admin",
        query_params={"type": "past"},
        # Zoom Heavy-tier per-app throttle is much tighter than Light-tier.
        # 8 parallel workers at 10 QPS overwhelmed the endpoint—every request
        # 429ed for minutes at a time. Cap concurrency to 2 workers and give
        # retries a longer runway (10 attempts → up to ~8.5 min of backoff
        # per request) so we ride out Zoom's rate-limit windows.
        max_workers=2,
        max_retry_attempts=10,
    ),
]


ALL_BY_NAME: dict[str, ObjectCfg] = {o.name: o for o in (*PRIMARY, *OPTIONAL)}


def select_objects(
    enabled_primary_csv: str = "",
    enabled_optional_csv: str = "",
) -> list[ObjectCfg]:
    """Return the ObjectCfg list for this run, in dependency-safe order.

    Ordering rule: parents must precede children (users before meetings,
    meetings before past_meeting_participants, etc.). Python dicts preserve
    insertion order, so we simply keep PRIMARY then OPTIONAL as authored.
    """
    def _parse(csv: str) -> set[str]:
        return {s.strip() for s in csv.split(",") if s.strip()}

    primary_wanted = _parse(enabled_primary_csv)
    optional_wanted = _parse(enabled_optional_csv)

    unknown = (primary_wanted | optional_wanted) - set(ALL_BY_NAME)
    if unknown:
        raise ValueError(
            f"unknown Zoom object(s) requested: {sorted(unknown)}; "
            f"known: {sorted(ALL_BY_NAME)}"
        )

    primary = [o for o in PRIMARY if not primary_wanted or o.name in primary_wanted]
    optional = [o for o in OPTIONAL if o.name in optional_wanted]
    return [*primary, *optional]


# ---------------------------------------------------------------------------
# Capability report — "what CAN I sync" = code catalog ∩ granted scopes
# ---------------------------------------------------------------------------

def build_capability_report(
    granted_scopes: set[str],
    enabled_primary_csv: str = "",
    enabled_optional_csv: str = "",
) -> dict:
    """Return a structured report cross-referencing:
      * the code catalog (src/objects.py)
      * the scopes Zoom granted to this app (from the token response)
      * the operator's per-job selection (enabled_objects / optional_objects_enabled)

    Callers use this both for the standalone `mode=discover` probe and for a
    start-of-run summary logged into connector_log on every real run.
    """
    def _parse(csv: str) -> set[str]:
        return {s.strip() for s in csv.split(",") if s.strip()}

    primary_wanted = _parse(enabled_primary_csv)
    optional_wanted = _parse(enabled_optional_csv)

    rows = []
    for obj in (*PRIMARY, *OPTIONAL):
        tier = "optional" if obj.optional else "primary"
        has_scope = (obj.required_scope in granted_scopes) if obj.required_scope else True
        if obj.optional:
            wanted = obj.name in optional_wanted
        else:
            wanted = (obj.name in primary_wanted) if primary_wanted else True
        will_sync = has_scope and wanted
        rows.append({
            "object": obj.name,
            "tier": tier,
            "required_scope": obj.required_scope or "",
            "granted": has_scope,
            "wanted": wanted,
            "will_sync": will_sync,
        })

    return {
        "objects": rows,
        "granted_scopes": sorted(granted_scopes),
        "code_catalog_size": len(ALL_BY_NAME),
        "will_sync": [r["object"] for r in rows if r["will_sync"]],
        "wanted_but_missing_scope": [
            r["object"] for r in rows if r["wanted"] and not r["granted"]
        ],
    }


def format_capability_report(report: dict) -> str:
    """Render a build_capability_report() dict as a human-readable table."""
    lines = []
    lines.append("=" * 88)
    lines.append("Jetstream Zoom — capability report")
    lines.append("=" * 88)
    lines.append(f"Granted scopes ({len(report['granted_scopes'])}):")
    for s in report["granted_scopes"]:
        lines.append(f"  - {s}")
    lines.append("")
    lines.append(f"{'object':<32} {'tier':<9} {'granted':<8} {'wanted':<7} {'will_sync'}")
    lines.append("-" * 88)
    for r in report["objects"]:
        lines.append(
            f"{r['object']:<32} {r['tier']:<9} "
            f"{'YES' if r['granted'] else 'no ':<8} "
            f"{'YES' if r['wanted'] else 'no ':<7} "
            f"{'YES' if r['will_sync'] else 'no'}"
        )
    lines.append("-" * 88)
    lines.append(f"Will sync this run: {report['will_sync'] or '(none)'}")
    if report["wanted_but_missing_scope"]:
        lines.append(
            f"WANTED BUT MISSING SCOPE (would block a real run for primary "
            f"objects): {report['wanted_but_missing_scope']}"
        )
    lines.append("=" * 88)
    return "\n".join(lines)
