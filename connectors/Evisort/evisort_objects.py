# Databricks notebook source
# evisort_objects.py
#
# The Evisort object catalog: one EvisortObject registration per syncable
# Evisort API object (documents, groups, tickets today, more as they're
# added). Split out from evisort_connector.py because this is the one part
# of the connector that actually grows every time a new Evisort object gets
# registered (its own field map, schema, flatten function, and pagination
# pair) while config, the API client, the ingest engine, validation, and
# orchestration in evisort_connector.py stay the same size regardless of
# how many objects are registered. %run'd into evisort_connector.py's
# namespace, same mechanism already used for control_plane.py.
#
# This connector does not hardcode which Evisort fields or clauses exist.
# document.fields[] is an array of {name, values: [{value}, ...]} entries
# and document.provisions[] is an array of {name, content: [{text}, ...]}
# entries: both are effectively open-ended EAV lists, and Evisort tenants
# routinely have dozens of custom fields beyond any fixed list this code
# could enumerate in advance (a real sample response had 97 fields on one
# document). So instead of a curated alias catalog, config.columns/
# config.clauses name the exact fields/clauses an instance wants, verbatim,
# and extract_field_values()/extract_clause_text() look each one up by that
# exact name (each in its own list, with no fallback between them) with
# no restriction to any predetermined catalog. Anything
# Evisort actually has, an operator can ask for by typing its exact label.

# COMMAND ----------

from __future__ import annotations

import json
import re
from dataclasses import dataclass, replace
from typing import Callable

CLAUSE_TEXT_JOIN_SEPARATOR = " | "
CLAUSE_TEXT_MAX_LENGTH = 8000

# evisort_id is the sole primary key: Evisort documents have a real natural
# key, so (unlike a connector with no natural key) this table needs no
# generated surrogate key at all.
PRIMARY_KEY: tuple[str, ...] = ("evisort_id",)

# Columns that identify a document and are always present regardless of
# which fields/clauses a given instance selects via config.columns/
# config.clauses: these aren't something an operator picks; a documents
# table with selected fields but no evisort_id would be useless.
# Trimmed deliberately to these three: evisort_id (primary key),
# date_modified (the incremental cursor field, see cursor_field on
# DOCUMENTS_OBJECT below), and file_name (document["name"] -- the
# file name, confirmed present on every document in a real sample; see
# the field-availability audit). doc_id, status, xray_status,
# document_path, upload_date, and upload_date_time are also real
# top-level attributes Evisort returns, but were never requested and are
# intentionally excluded here.
DOCUMENTS_IDENTITY_COLUMNS: tuple[tuple[str, str], ...] = (
    ("evisort_id", "bigint"),
    ("date_modified", "timestamp"),
    ("file_name", "string"),
)

DOCUMENTS_SURROGATE_COLUMNS: tuple[tuple[str, str], ...] = (
    ("_jetstream_source_object", "string"),
    ("_jetstream_synced", "timestamp"),
    ("_jetstream_deleted", "boolean"),
)


def sanitize_column_name(source_name: str) -> str:
    """
    Convert a raw Evisort field/clause label (whatever an operator typed
    into config.columns/config.clauses, verbatim) into a safe destination
    column name: lowercase, every run of non-alphanumeric characters
    collapsed to a single underscore, leading/trailing underscores
    stripped. E.g. "Total Amount" -> "total_amount", "# of Renewals" ->
    "of_renewals".

    Returns:
        The sanitized name. Can be "" if source_name has no alphanumeric
        characters at all; callers must check for that and for
        collisions between two different source names that sanitize to
        the same result (both are validated in validate_config()).
    """
    lowered = source_name.strip().lower()
    underscored = re.sub(r"[^a-z0-9]+", "_", lowered)
    return underscored.strip("_")


# Suffix applied to a clause's destination column name when its raw name
# exactly matches a name also selected in config.columns -- e.g. an
# operator wants both the "Governing Law" field value and the "Governing
# Law" clause text in the same table. Without this, both would sanitize
# to the identical destination name "governing_law" and collide. This
# only fires on an EXACT raw-name match between the two lists; any other
# sanitized-name collision (different raw names landing on the same
# destination, or a duplicate raw name within one list) is still a
# genuine, unresolved collision and is left for validate_config() to
# reject, not silently renamed.
CLAUSE_FIELD_COLLISION_SUFFIX = "_clause_text"


def resolve_column_destinations(
    selected_fields: tuple[str, ...], selected_clauses: tuple[str, ...]
) -> tuple[dict[str, str], dict[str, str]]:
    """
    Compute the destination column name for every selected field and
    every selected clause, deliberately allowing a field and a clause
    with the identical exact name to coexist in the same table.

    Every field's destination is always sanitize_column_name(name):
    fields are never renamed. A clause's destination is
    sanitize_column_name(name) too, UNLESS its raw name exactly matches
    an entry also present in selected_fields, in which case
    CLAUSE_FIELD_COLLISION_SUFFIX is appended so the two don't collide
    ("Governing Law" -> field column "governing_law", clause column
    "governing_law_clause_text").

    This function does not itself validate anything; it only decides
    names. Duplicate raw names within one list, or two different raw
    names that happen to sanitize to the same destination, still produce
    colliding destination names here on purpose, so validate_config()
    can catch and report them.

    Args:
        selected_fields: Exact field names from config.columns.
        selected_clauses: Exact provision names from config.clauses.

    Returns:
        (field_destinations, clause_destinations), each a dict mapping
        every raw name in that list to its resolved destination column
        name.
    """
    field_name_set = set(selected_fields)
    field_destinations = {name: sanitize_column_name(name) for name in selected_fields}
    clause_destinations = {}
    for name in selected_clauses:
        base = sanitize_column_name(name)
        clause_destinations[name] = base + CLAUSE_FIELD_COLLISION_SUFFIX if name in field_name_set else base
    return field_destinations, clause_destinations


def extract_field_values(document: dict, source_name: str) -> list[str] | None:
    """
    Look up one exact field name in document.fields[]. There is no alias
    matching; source_name must be the literal label as it appears in
    this Evisort tenant's data, exactly as the operator typed it into
    config.columns.

    Returns the raw list of values, not joined into a string: a field's
    values[] is genuinely a list (even a "single-value" field is a
    length-1 list), so the destination column is ARRAY<STRING>, matching
    the source shape rather than collapsing it. Joining with a delimiter
    was the earlier design and was lossy: if any real value itself
    contained the delimiter, there was no way to tell where one value
    ended and the next began. An array element has no such ambiguity.

    Args:
        document: Raw Evisort document dict (fields[] inline).
        source_name: Exact field name to look up.

    Returns:
        List of non-null values as strings, in source order (not
        deduplicated, not sorted). None if source_name matches no field,
        or matches a field with no non-null values; this is how "not
        present on this document" is represented, not an empty list.
    """
    for field in document.get("fields", []):
        if field.get("name") != source_name:
            continue
        values = [str(v["value"]) for v in (field.get("values") or []) if v.get("value") is not None]
        return values if values else None
    return None


def extract_clause_text(document: dict, source_name: str) -> str | None:
    """
    Look up one exact clause/provision name in document.provisions[].
    There is no alias matching; source_name must be the literal
    provision name as Evisort's clause library defines it, exactly as
    the operator typed it into config.clauses.

    Args:
        document: Raw Evisort document dict (provisions[] inline).
        source_name: Exact provision name to look up.

    Returns:
        Text snippets joined with " | ", truncated to 8,000 characters.
        None if source_name matches no provision, or matches one with no
        text content; this is how "clause not present on this document"
        is represented, not a separate boolean column.
    """
    for provision in document.get("provisions", []):
        if provision.get("name") != source_name:
            continue
        snippets = [c["text"] for c in provision.get("content", []) if c.get("text")]
        if snippets:
            return CLAUSE_TEXT_JOIN_SEPARATOR.join(snippets)[:CLAUSE_TEXT_MAX_LENGTH]
        return None
    return None


def build_documents_flatten_fn(
    selected_fields: tuple[str, ...], selected_clauses: tuple[str, ...]
) -> Callable[[dict], dict]:
    """
    Build a flatten function for one documents instance's exact field and
    clause selection. Each entry is an exact Evisort field or provision
    label; the returned function looks each one up verbatim, fields via
    extract_field_values() (destination type ARRAY<STRING>), clauses via
    extract_clause_text() (destination type STRING), keyed by its
    resolved destination name from resolve_column_destinations(). No
    fixed catalog, no aliasing, except the automatic clause suffix
    resolve_column_destinations() applies when a clause's raw name
    exactly matches a selected field's raw name (e.g. "Governing Law" as
    both), so the two don't collide.

    Args:
        selected_fields: Exact field names from config.columns.
        selected_clauses: Exact provision names from config.clauses.

    Returns:
        A function taking one raw document dict and returning a flat row
        dict: the 3 DOCUMENTS_IDENTITY_COLUMNS keys, one key per
        selected_fields entry (list or None), and one key per
        selected_clauses entry (string or None).

    Raises:
        KeyError: If a document passed to the returned function is
            missing the mandatory 'evisortId' field.
    """
    field_destinations, clause_destinations = resolve_column_destinations(selected_fields, selected_clauses)

    def flatten_fn(document: dict) -> dict:
        if "evisortId" not in document:
            raise KeyError("Document is missing required field 'evisortId'.")
        row: dict = {
            "evisort_id": int(document["evisortId"]),
            "date_modified": document.get("dateModified"),
            "file_name": document.get("name"),
        }
        for source_name in selected_fields:
            row[field_destinations[source_name]] = extract_field_values(document, source_name)
        for source_name in selected_clauses:
            row[clause_destinations[source_name]] = extract_clause_text(document, source_name)
        return row

    return flatten_fn


# ---------------------------------------------------------------------------
# Evisort object registry
# ---------------------------------------------------------------------------
#
# This connector is generic across Evisort *objects* (documents, groups,
# tickets, nodes, departments, workflows, activities), not across arbitrary
# APIs: auth and the general paginated-REST shape are Evisort's, shared by
# every object; what varies per object is the endpoint, its own pagination
# quirks, its field mapping, its schema, and its natural key. Real Evisort
# APIs paginate at least six different ways:
#   - documents: GET, next page is a full URL at top-level `next`
#   - groups / nodes (Managed Entities): GET with page[limit]/page[offset],
#     next page is a full URL nested at `links.next`
#   - tickets (Workflow API): GET with page/pageSize, no `next` URL at
#     all; the caller must self-terminate by counting rows against `count`
#   - departments (Admin API): GET with page[number]/page[size], next page
#     nested at `links.next`
#   - workflows / activities (Workflow API): no pagination at all; one
#     request returns everything
# So each EvisortObject owns its own build_request()/get_next_marker()
# pair rather than the connector assuming one pagination shape. Adding a
# new Evisort object means adding its field map, schema, flatten
# function, and these two small functions here; the control-plane wiring
# and write path in IngestEngine (evisort_connector.py) are already
# shared, not rebuilt per object.

@dataclass(frozen=True)
class EvisortObject:
    """
    Declares one syncable Evisort API object.

    Attributes:
        object_name: Short key, e.g. "documents". Used as
            connector_state.object_name (so each object gets its own
            incremental cursor) and as the sync_objects config value that
            selects it.
        endpoint_path: API path, e.g. "/documents". Used for the first
            request (inside build_request) and to build
            _jetstream_source_object; not necessarily the literal URL of
            every subsequent page, since some objects paginate via an
            opaque `next` URL/token the API hands back.
        list_key: JSON key in the paginated response holding the record
            array, e.g. "documents".
        id_field: Source field holding the record's natural key, e.g.
            "evisortId". None when no natural key is confirmed for this
            object, in that case IngestEngine computes _jetstream_id (a
            content hash) and _jetstream_index instead, and primary_key
            must be ("_jetstream_id",).
        cursor_field: Source field used to compute the next incremental
            cursor, e.g. "dateModified". None if this object's API has no
            documented modified-since-style filter; every run then does
            a full fetch (still safe: the write is an idempotent MERGE).
        default_table_name: Destination table name for this object.
        schema_columns: (column_name, spark_type) tuples in destination
            column order: this object's own schema() declaration.
        primary_key: Destination column name(s) forming the MERGE key.
        flatten_fn: Raw record dict -> destination row dict.
        build_request: (api_base_url, marker, page_size, cursor_value,
            extra_params) -> (http_method, url, query_params, json_body)
            for one page request. marker is None for the first page and
            is whatever get_next_marker() returned for every page after.
        get_next_marker: (response_json, request_params, request_body) ->
            the marker to pass into build_request() for the next page, or
            None if this was the last page. What a "marker" actually is
            (a full URL, an integer page number, an opaque token) is
            entirely up to the object; iter_object_pages() only ever
            passes it back to build_request() untouched.
        api_base_url_override: Full base URL to use instead of
            config.api_base_url, for objects on a different host/path
            than the shared Evisort API base (e.g. Managed Entities lives
            at api.evisort.com/mes/public, not .../v1). None uses
            config.api_base_url.
    """

    object_name: str
    endpoint_path: str
    list_key: str
    id_field: str | None
    cursor_field: str | None
    default_table_name: str
    schema_columns: tuple[tuple[str, str], ...]
    primary_key: tuple[str, ...]
    flatten_fn: Callable[[dict], dict]
    build_request: Callable
    get_next_marker: Callable
    api_base_url_override: str | None = None


# ---------------------------------------------------------------------------
# documents (Documents API)
#   - modifiedSince is REQUIRED on GET /documents, not optional -- omitting
#     it 400s against the real API. EPOCH_MODIFIED_SINCE is the fallback
#     used whenever there's no real cursor yet.
#   - There is no `include` query parameter at all. The only two extra
#     GET /documents params are includeMapping and returnUniqueFieldValues
#     (both booleans); this connector doesn't need either (no PDF
#     coordinates/html tokens needed, and returnUniqueFieldValues=false is
#     fine since extract_field_values() already returns each field's raw
#     values list as-is, duplicates included), so both are hardcoded false
#     rather than exposed as config for a need that doesn't exist yet.
# ---------------------------------------------------------------------------

EPOCH_MODIFIED_SINCE = "1970-01-01T00:00:00Z"


def _documents_build_request(api_base_url, marker, page_size, cursor_value, extra_params):
    if marker is not None:
        return "GET", marker, None, None
    params = {
        "pageSize": page_size,
        "includeMapping": "false",
        "returnUniqueFieldValues": "false",
        "modifiedSince": cursor_value or EPOCH_MODIFIED_SINCE,
    }
    return "GET", f"{api_base_url.rstrip('/')}/documents", params, None


def _documents_get_next_marker(response_json, request_params, request_body):
    return response_json.get("next")


DOCUMENTS_OBJECT = EvisortObject(
    object_name="documents",
    endpoint_path="/documents",
    list_key="documents",
    id_field="evisortId",
    cursor_field="dateModified",
    # Unused for documents: table_name has no default and is required
    # whenever "documents" is in sync_objects, see resolve_target_table()
    # in evisort_connector.py, which branches on object_name=="documents"
    # to use config.table_name instead of this field. Kept non-empty only
    # because default_table_name is a required dataclass field.
    default_table_name="evisort_contracts",
    # Placeholder only; every real run replaces both schema_columns and
    # flatten_fn via resolve_documents_object(config), since both depend
    # entirely on config.columns/config.clauses (no default field
    # selection exists to fall back to). This registry entry exists so
    # "documents" is a valid EVISORT_OBJECTS/sync_objects key even before
    # config is available.
    schema_columns=DOCUMENTS_IDENTITY_COLUMNS + DOCUMENTS_SURROGATE_COLUMNS,
    primary_key=PRIMARY_KEY,
    flatten_fn=build_documents_flatten_fn((), ()),
    build_request=_documents_build_request,
    get_next_marker=_documents_get_next_marker,
)


def resolve_documents_object(config: "JetstreamConfig") -> EvisortObject:
    """
    Build the documents EvisortObject for this run: schema_columns and
    flatten_fn are both derived entirely from config.columns (field names,
    destination type ARRAY<STRING>) and config.clauses (provision names,
    destination type STRING), the exact names an operator listed,
    verbatim, with no fixed catalog and no "select everything" default.
    validate_config() already rejects any genuinely unresolved collision
    (a duplicate raw name within one list, or two different raw names
    landing on the same destination) before this is ever called. A field
    and a clause sharing the identical exact raw name (e.g. "Governing
    Law" in both columns and clauses) is not a collision, see
    resolve_column_destinations(), so both are guaranteed collision-free
    here, though either individually may be empty.

    Args:
        config: The JetstreamConfig for this job run.

    Returns:
        An EvisortObject identical to DOCUMENTS_OBJECT except for
        schema_columns and flatten_fn.
    """
    selected_fields = tuple(config.get_columns_list())
    selected_clauses = tuple(config.get_clauses_list())
    field_destinations, clause_destinations = resolve_column_destinations(selected_fields, selected_clauses)
    field_columns = tuple((field_destinations[name], "array<string>") for name in selected_fields)
    clause_columns = tuple((clause_destinations[name], "string") for name in selected_clauses)
    schema_columns = DOCUMENTS_IDENTITY_COLUMNS + field_columns + clause_columns + DOCUMENTS_SURROGATE_COLUMNS
    return replace(
        DOCUMENTS_OBJECT,
        schema_columns=schema_columns,
        flatten_fn=build_documents_flatten_fn(selected_fields, selected_clauses),
    )


# ---------------------------------------------------------------------------
# groups (Managed Entities API): Prod-Managed-Entities.json.
# Lives at a different host path (api.evisort.com/mes/public) than the
# shared Documents/Workflow/AuditLogs base (.../v1), hence
# api_base_url_override. Paginates via page[limit]/page[offset] with the
# next page as a full URL nested at links.next, a different shape from
# documents' top-level `next`, proving build_request/get_next_marker
# actually generalizes rather than just relocating the documents-specific
# assumption. No documented modified-since-style filter exists for this
# endpoint, so cursor_field is None: every run does a full fetch; the
# write is still a safe, idempotent MERGE on group_id regardless.
# `nodes` (the other Managed Entities resource) is intentionally not
# registered: GET /api/v1/nodes requires filter[group_id]; you cannot
# list all nodes without already knowing every group_id, a
# dependent-object pattern ("list this object once per record of another
# object") this connector's model doesn't support yet.
# ---------------------------------------------------------------------------

MANAGED_ENTITIES_BASE_URL = "https://api.evisort.com/mes/public"


def flatten_group(record: dict) -> dict:
    """
    Flatten one raw Group resource (JSON:API shape: {type, id, attributes,
    relationships}) into a destination row.

    Raises:
        KeyError: If record is missing required field 'id'.
    """
    if "id" not in record:
        raise KeyError("Group record is missing required field 'id'.")
    attributes = record.get("attributes") or {}
    root_node_rel = (record.get("relationships") or {}).get("root_node")
    root_node_data = (root_node_rel or {}).get("data") if root_node_rel else None
    return {
        "group_id": record["id"],
        "client_id": attributes.get("client_id"),
        "name": attributes.get("name"),
        "note": attributes.get("note"),
        "nodes_count": attributes.get("nodes_count"),
        "created_at": attributes.get("created_at"),
        "created_by": attributes.get("created_by"),
        "updated_at": attributes.get("updated_at"),
        "root_node_id": root_node_data.get("id") if root_node_data else None,
    }


GROUPS_SCHEMA_COLUMNS: tuple[tuple[str, str], ...] = (
    ("group_id", "string"),
    ("client_id", "bigint"),
    ("name", "string"),
    ("note", "string"),
    ("nodes_count", "bigint"),
    ("created_at", "timestamp"),
    ("created_by", "bigint"),
    ("updated_at", "timestamp"),
    ("root_node_id", "string"),
    ("_jetstream_source_object", "string"),
    ("_jetstream_synced", "timestamp"),
    ("_jetstream_deleted", "boolean"),
)


def _groups_build_request(api_base_url, marker, page_size, cursor_value, extra_params):
    if marker is not None:
        return "GET", marker, None, None
    params = {"page[limit]": page_size, "page[offset]": 0}
    return "GET", f"{api_base_url.rstrip('/')}/api/v1/groups", params, None


def _groups_get_next_marker(response_json, request_params, request_body):
    return (response_json.get("links") or {}).get("next")


GROUPS_OBJECT = EvisortObject(
    object_name="groups",
    endpoint_path="/api/v1/groups",
    list_key="data",
    id_field="id",
    cursor_field=None,
    default_table_name="evisort_groups",
    schema_columns=GROUPS_SCHEMA_COLUMNS,
    primary_key=("group_id",),
    flatten_fn=flatten_group,
    build_request=_groups_build_request,
    get_next_marker=_groups_get_next_marker,
    api_base_url_override=MANAGED_ENTITIES_BASE_URL,
)


# ---------------------------------------------------------------------------
# nodes (Managed Entities API) -- schema is real, not guessed. Same JSON:API
# envelope and pagination as groups (same host, page[limit]/page[offset],
# links.next).
#
# The one real architectural difference from every other registered object:
# GET /api/v1/nodes requires filter[group_id] -- you cannot list all nodes
# without already knowing every group_id first. Resolved via the extra_params
# hook (see EvisortObject.build_request's docstring) plus a dedicated
# sync_nodes_object() in evisort_connector.py that internally discovers every
# group_id via its own lightweight pass over GET /api/v1/groups (reusing
# GROUPS_OBJECT's own pagination), then drives one paginated nodes fetch per
# group. Deliberately does NOT require "groups" to be selected in
# sync_objects, and does NOT read the evisort_groups destination table --
# nodes is fully self-sufficient, needing only read access to the Managed
# Entities groups endpoint internally, not a groups sync having ever run.
#
# No documented modified-since-style filter exists for this endpoint either
# (same as groups), so cursor_field is None -- every run does a full fetch
# per group; the write is still a safe, idempotent MERGE on node_id.
# ---------------------------------------------------------------------------


def flatten_node(record: dict) -> dict:
    """
    Flatten one raw Node resource (JSON:API shape: {type, id, attributes,
    relationships}) into a destination row. Schema confirmed against 8 real
    node records across 3 real groups in prod -- every one had exactly
    these attributes, no variation.

    Raises:
        KeyError: If record is missing required field 'id'.
    """
    if "id" not in record:
        raise KeyError("Node record is missing required field 'id'.")
    attributes = record.get("attributes") or {}
    return {
        "node_id": record["id"],
        "resource_type": attributes.get("resource_type"),
        "resource_id": attributes.get("resource_id"),
        "group_id": attributes.get("group_id"),
        "parent_node_id": attributes.get("parent_node_id"),
        "is_parent": attributes.get("is_parent"),
        "created_at": attributes.get("created_at"),
        "updated_at": attributes.get("updated_at"),
    }


NODES_SCHEMA_COLUMNS: tuple[tuple[str, str], ...] = (
    ("node_id", "string"),
    ("resource_type", "string"),
    ("resource_id", "string"),
    ("group_id", "string"),
    ("parent_node_id", "string"),
    ("is_parent", "boolean"),
    ("created_at", "timestamp"),
    ("updated_at", "timestamp"),
    ("_jetstream_source_object", "string"),
    ("_jetstream_synced", "timestamp"),
    ("_jetstream_deleted", "boolean"),
)


def _nodes_build_request(api_base_url, marker, page_size, cursor_value, extra_params):
    """
    Requires extra_params["group_id"] -- raises clearly rather than silently
    hitting the real API without it (which would itself 400, but with a far
    less useful error than this one). Set by sync_nodes_object(), which
    drives one call to iter_object_pages() per discovered group_id.
    """
    if marker is not None:
        return "GET", marker, None, None
    group_id = extra_params.get("group_id")
    if not group_id:
        raise ValueError(
            "nodes requires extra_params['group_id'] on every request -- GET /api/v1/nodes "
            "has no 'list all nodes' mode. This should always be set by sync_nodes_object(); "
            "seeing this error means nodes was invoked through the generic per-object sync "
            "path instead of its dedicated one."
        )
    params = {"filter[group_id]": group_id, "page[limit]": page_size, "page[offset]": 0}
    return "GET", f"{api_base_url.rstrip('/')}/api/v1/nodes", params, None


def _nodes_get_next_marker(response_json, request_params, request_body):
    return (response_json.get("links") or {}).get("next")


NODES_OBJECT = EvisortObject(
    object_name="nodes",
    endpoint_path="/api/v1/nodes",
    list_key="data",
    id_field="id",
    cursor_field=None,
    default_table_name="evisort_nodes",
    schema_columns=NODES_SCHEMA_COLUMNS,
    primary_key=("node_id",),
    flatten_fn=flatten_node,
    build_request=_nodes_build_request,
    get_next_marker=_nodes_get_next_marker,
    api_base_url_override=MANAGED_ENTITIES_BASE_URL,
)


# ---------------------------------------------------------------------------
# tickets (Workflow API): paginates via page/pageSize with NO `next` URL
# in the response at all; self-terminated by comparing rows returned this
# page against pageSize and the response's total `count`.
# No documented modified-since-style filter exists (only a `sort` param),
# so cursor_field is None, same reasoning as groups.
#
# The spec's own path for this endpoint is literally "/v1/contracts/tickets"
# while the server base URL is already "https://api.evisort.com/v1" and
# every other Workflow API path omits the "/v1" prefix (e.g.
# "/contracts/workflows/available"); that looks like a duplication bug in
# Evisort's own spec, not a deliberate second /v1. Implemented exactly as
# documented (server + this path, i.e. an effective double "/v1/v1/");
# flag for verification against the real API before relying on it, and
# adjust build_request here if the real endpoint turns out to be
# "/contracts/tickets" instead.
# ---------------------------------------------------------------------------

def flatten_ticket(record: dict) -> dict:
    """
    Flatten one raw TicketSummary record into a destination row.

    Raises:
        KeyError: If record is missing required field 'id'.
    """
    if "id" not in record:
        raise KeyError("Ticket record is missing required field 'id'.")
    assignees = record.get("assignees") or []
    participants = record.get("participants") or []
    return {
        "ticket_id": record["id"],
        "name": record.get("name"),
        "status": record.get("status"),
        "current_stage_name": record.get("currentStageName"),
        "created_date": record.get("createdDate"),
        "last_activity_date": record.get("lastActivityDate"),
        "assignee_ids": ", ".join(str(a) for a in assignees) if assignees else None,
        "participant_ids": ", ".join(str(p) for p in participants) if participants else None,
    }


TICKETS_SCHEMA_COLUMNS: tuple[tuple[str, str], ...] = (
    ("ticket_id", "string"),
    ("name", "string"),
    ("status", "string"),
    ("current_stage_name", "string"),
    ("created_date", "timestamp"),
    ("last_activity_date", "timestamp"),
    ("assignee_ids", "string"),
    ("participant_ids", "string"),
    ("_jetstream_source_object", "string"),
    ("_jetstream_synced", "timestamp"),
    ("_jetstream_deleted", "boolean"),
)


def _tickets_build_request(api_base_url, marker, page_size, cursor_value, extra_params):
    page = marker if marker is not None else 0
    params = {"page": page, "pageSize": page_size}
    return "GET", f"{api_base_url.rstrip('/')}/v1/contracts/tickets", params, None


def _tickets_get_next_marker(response_json, request_params, request_body):
    data = response_json.get("data", [])
    count = response_json.get("count") or 0
    page = request_params["page"]
    page_size = request_params["pageSize"]
    fetched_so_far = (page + 1) * page_size
    if len(data) < page_size or fetched_so_far >= count:
        return None
    return page + 1


TICKETS_OBJECT = EvisortObject(
    object_name="tickets",
    endpoint_path="/v1/contracts/tickets",
    list_key="data",
    id_field="id",
    cursor_field=None,
    default_table_name="evisort_tickets",
    schema_columns=TICKETS_SCHEMA_COLUMNS,
    primary_key=("ticket_id",),
    flatten_fn=flatten_ticket,
    build_request=_tickets_build_request,
    get_next_marker=_tickets_get_next_marker,
)


# ---------------------------------------------------------------------------
# workflows (Workflow API, GET /contracts/workflows/available) -- response
# is a flat list (not a single object -- the spec's schema was ambiguous on
# this point, hence list_key=None below rather than the usual "data" key),
# 28 distinct top-level keys.
#
# A sixth distinct pagination shape: none at all. No page/pageSize/page[...]
# parameter exists anywhere in the spec for this endpoint (only the optional
# boolean `completed` filter) -- get_next_marker always returns None, one
# request returns everything.
#
# Several fields are deeply nested, variable-shaped structures with no
# useful flat column representation (conditions, fieldLinks, fieldMappings,
# fields, implicitFields, signers, stages, versions, prefilledFormData) --
# stored as raw JSON strings rather than guessing a fixed sub-schema for
# each, consistent with how this connector already treats similarly
# open-ended nested data elsewhere. Flat scalar fields get real typed
# columns.
#
# No documented modified-since-style filter, so cursor_field is None; the
# write is still a safe, idempotent MERGE on workflow_id.
# ---------------------------------------------------------------------------


def flatten_workflow(record: dict) -> dict:
    """
    Flatten one raw Workflow resource into a destination row. Schema
    confirmed against 2 real workflow records in prod -- both had the exact
    same 28 top-level keys.

    Raises:
        KeyError: If record is missing required field 'id'.
    """
    if "id" not in record:
        raise KeyError("Workflow record is missing required field 'id'.")
    return {
        "workflow_id": record["id"],
        "name": record.get("name"),
        "description": record.get("description"),
        "status": record.get("status"),
        "is_draft": record.get("isDraft"),
        "published": record.get("published"),
        "publishable": record.get("publishable"),
        "version_number": record.get("versionNumber"),
        "parent_id": record.get("parentId"),
        "permanent_link_id": record.get("permanentLinkId"),
        "contract_type_id": record.get("contractTypeId"),
        "contract_type_name": record.get("contractTypeName"),
        "builder_id": record.get("builderId"),
        "created_by": record.get("createdBy"),
        "last_modified_by_id": record.get("lastModifiedById"),
        "modified_date": record.get("modifiedDate"),
        "default_playbook_id": record.get("defaultPlaybookId"),
        "has_company_paper": record.get("hasCompanyPaper"),
        "has_counterparty_paper": record.get("hasCounterpartyPaper"),
        "accepted_file_types": record.get("acceptedFileTypes"),
        # Deeply nested / variable-shaped -- raw JSON, not a guessed sub-schema.
        "conditions_json": json.dumps(record.get("conditions")) if record.get("conditions") is not None else None,
        "field_links_json": json.dumps(record.get("fieldLinks")) if record.get("fieldLinks") is not None else None,
        "field_mappings_json": json.dumps(record.get("fieldMappings")) if record.get("fieldMappings") is not None else None,
        "fields_json": json.dumps(record.get("fields")) if record.get("fields") is not None else None,
        "implicit_fields_json": json.dumps(record.get("implicitFields")) if record.get("implicitFields") is not None else None,
        "signers_json": json.dumps(record.get("signers")) if record.get("signers") is not None else None,
        "stages_json": json.dumps(record.get("stages")) if record.get("stages") is not None else None,
        "versions_json": json.dumps(record.get("versions")) if record.get("versions") is not None else None,
        "prefilled_form_data_json": json.dumps(record.get("prefilledFormData")) if record.get("prefilledFormData") is not None else None,
    }


WORKFLOWS_SCHEMA_COLUMNS: tuple[tuple[str, str], ...] = (
    ("workflow_id", "string"),
    ("name", "string"),
    ("description", "string"),
    ("status", "string"),
    ("is_draft", "boolean"),
    ("published", "boolean"),
    ("publishable", "boolean"),
    ("version_number", "bigint"),
    ("parent_id", "string"),
    ("permanent_link_id", "string"),
    ("contract_type_id", "string"),
    ("contract_type_name", "string"),
    ("builder_id", "bigint"),
    ("created_by", "bigint"),
    ("last_modified_by_id", "bigint"),
    ("modified_date", "timestamp"),
    ("default_playbook_id", "string"),
    ("has_company_paper", "boolean"),
    ("has_counterparty_paper", "boolean"),
    ("accepted_file_types", "array<string>"),
    ("conditions_json", "string"),
    ("field_links_json", "string"),
    ("field_mappings_json", "string"),
    ("fields_json", "string"),
    ("implicit_fields_json", "string"),
    ("signers_json", "string"),
    ("stages_json", "string"),
    ("versions_json", "string"),
    ("prefilled_form_data_json", "string"),
    ("_jetstream_source_object", "string"),
    ("_jetstream_synced", "timestamp"),
    ("_jetstream_deleted", "boolean"),
)


def _workflows_build_request(api_base_url, marker, page_size, cursor_value, extra_params):
    params = {}
    if extra_params.get("completed"):
        params["completed"] = extra_params["completed"]
    return "GET", f"{api_base_url.rstrip('/')}/contracts/workflows/available", (params or None), None


def _workflows_get_next_marker(response_json, request_params, request_body):
    # No pagination exists for this endpoint at all -- one request, always done.
    return None


WORKFLOWS_OBJECT = EvisortObject(
    object_name="workflows",
    endpoint_path="/contracts/workflows/available",
    list_key=None,  # response is the list itself, not {"data": [...]} -- see resolve below
    id_field="id",
    cursor_field=None,
    default_table_name="evisort_workflows",
    schema_columns=WORKFLOWS_SCHEMA_COLUMNS,
    primary_key=("workflow_id",),
    flatten_fn=flatten_workflow,
    build_request=_workflows_build_request,
    get_next_marker=_workflows_get_next_marker,
)


# ---------------------------------------------------------------------------
# activities (Workflow API, GET /contracts/activities) -- complete, typed
# schema directly from the official spec: id (uuid), action, type,
# modifiedDate, user {id, name, email}, data (generic/untyped per-event
# payload). Every example value in the spec is an empty placeholder though
# -- unlike nodes/departments/workflows, this hasn't been confirmed against
# an actual non-empty real record yet. Verify with one real run before
# fully trusting it.
#
# entityId + entityType (enum: risk, ticket, ticket judgment) are BOTH
# required on every call -- there is no "list all activities" mode. Of the
# three entity types, only "ticket" IDs are enumerable via anything this
# connector already tracks (this connector's own tickets sync); "risk" and
# "ticket judgment" have no discoverable bulk-list endpoint in any of the
# specs reviewed. Scoped to ticket-activities only, not "all activities" --
# a real, deliberate scope narrowing, not an oversight.
#
# The response never echoes back which entity (ticket) an activity belongs
# to -- no entityId/ticketId field anywhere in the schema. Since many
# tickets' activities all land in one destination table, sync_activities_
# object() (evisort_connector.py) injects the driving ticket_id into each
# raw record as "_entity_id" before flattening, since the API gives no way
# to derive it from the record alone.
#
# No pagination exists for this endpoint either (same as workflows) -- one
# request per ticket returns everything for that ticket. No documented
# modified-since-style filter, so cursor_field is None; the write is still
# a safe, idempotent MERGE on activity_id.
# ---------------------------------------------------------------------------


def flatten_activity(record: dict) -> dict:
    """
    Flatten one raw Activity resource into a destination row.

    Raises:
        KeyError: If record is missing required field 'id'.
    """
    if "id" not in record:
        raise KeyError("Activity record is missing required field 'id'.")
    user = record.get("user") or {}
    return {
        "activity_id": record["id"],
        # Injected by sync_activities_object() before this is called -- the
        # real API response never includes which entity an activity
        # belongs to, so this can't be derived from the record alone.
        "ticket_id": record.get("_entity_id"),
        "action": record.get("action"),
        "type": record.get("type"),
        "modified_date": record.get("modifiedDate"),
        "user_id": user.get("id"),
        "user_name": user.get("name"),
        "user_email": user.get("email"),
        "data_json": json.dumps(record.get("data")) if record.get("data") is not None else None,
    }


ACTIVITIES_SCHEMA_COLUMNS: tuple[tuple[str, str], ...] = (
    ("activity_id", "string"),
    ("ticket_id", "string"),
    ("action", "string"),
    ("type", "string"),
    ("modified_date", "timestamp"),
    ("user_id", "bigint"),
    ("user_name", "string"),
    ("user_email", "string"),
    ("data_json", "string"),
    ("_jetstream_source_object", "string"),
    ("_jetstream_synced", "timestamp"),
    ("_jetstream_deleted", "boolean"),
)


def _activities_build_request(api_base_url, marker, page_size, cursor_value, extra_params):
    entity_id = extra_params.get("entity_id")
    if not entity_id:
        raise ValueError(
            "activities requires extra_params['entity_id'] on every request -- GET "
            "/contracts/activities has no 'list all activities' mode. This should always "
            "be set by sync_activities_object(); seeing this error means activities was "
            "invoked through the generic per-object sync path instead of its dedicated one."
        )
    params = {"entityId": entity_id, "entityType": "ticket"}
    return "GET", f"{api_base_url.rstrip('/')}/contracts/activities", params, None


def _activities_get_next_marker(response_json, request_params, request_body):
    # No pagination exists for this endpoint at all -- one request, always done.
    return None


ACTIVITIES_OBJECT = EvisortObject(
    object_name="activities",
    endpoint_path="/contracts/activities",
    list_key="data",
    id_field="id",
    cursor_field=None,
    default_table_name="evisort_activities",
    schema_columns=ACTIVITIES_SCHEMA_COLUMNS,
    primary_key=("activity_id",),
    flatten_fn=flatten_activity,
    build_request=_activities_build_request,
    get_next_marker=_activities_get_next_marker,
)


# ---------------------------------------------------------------------------
# departments (Admin API) -- confirmed schema (not guessed): {type:
# "department", id, attributes: {name, parentDepartment, path, depth,
# memberCount}}, with a complete, non-empty real example in the official
# docs.
#
# Pagination: page[number]/page[size], links.next as a full URL, plus
# meta.totalCount -- distinct from every other object's shape (see the
# pagination list at the top of this file).
#
# No filter[id] set on the initial request (that parameter narrows to one
# anchor's children, not what a full sync wants) and filter[depth]=100 (the
# documented max) to pull every level of the hierarchy in one paginated
# pass, rather than only direct top-level departments.
#
# A real call against this tenant returns HTTP 404, not the 403 (missing
# admin JWT / LaunchDarkly flag) the docs' own stated requirements would
# suggest -- this "v2/admin" surface may not be live for this tenant at
# all. Not fixable in code; the schema/pagination logic above is otherwise
# ready.
#
# No documented modified-since-style filter, so cursor_field is None, same
# reasoning as groups/nodes -- every run does a full fetch; the write is
# still a safe, idempotent MERGE on department_id.
# ---------------------------------------------------------------------------


def flatten_department(record: dict) -> dict:
    """
    Flatten one raw Department resource (JSON:API shape: {type, id,
    attributes}) into a destination row. Schema confirmed from a complete,
    non-empty real example in the official Admin API docs.

    Raises:
        KeyError: If record is missing required field 'id'.
    """
    if "id" not in record:
        raise KeyError("Department record is missing required field 'id'.")
    attributes = record.get("attributes") or {}
    return {
        "department_id": record["id"],
        "name": attributes.get("name"),
        "parent_department_id": attributes.get("parentDepartment"),
        "path": attributes.get("path"),
        "depth": attributes.get("depth"),
        "member_count": attributes.get("memberCount"),
    }


DEPARTMENTS_SCHEMA_COLUMNS: tuple[tuple[str, str], ...] = (
    ("department_id", "string"),
    ("name", "string"),
    ("parent_department_id", "string"),
    ("path", "string"),
    ("depth", "bigint"),
    ("member_count", "bigint"),
    ("_jetstream_source_object", "string"),
    ("_jetstream_synced", "timestamp"),
    ("_jetstream_deleted", "boolean"),
)


def _departments_build_request(api_base_url, marker, page_size, cursor_value, extra_params):
    if marker is not None:
        return "GET", marker, None, None
    params = {"filter[depth]": 100, "page[number]": 1, "page[size]": page_size}
    return "GET", f"{api_base_url.rstrip('/')}/v2/admin/departments", params, None


def _departments_get_next_marker(response_json, request_params, request_body):
    return (response_json.get("links") or {}).get("next")


DEPARTMENTS_OBJECT = EvisortObject(
    object_name="departments",
    endpoint_path="/v2/admin/departments",
    list_key="data",
    id_field="id",
    cursor_field=None,
    default_table_name="evisort_departments",
    schema_columns=DEPARTMENTS_SCHEMA_COLUMNS,
    primary_key=("department_id",),
    flatten_fn=flatten_department,
    build_request=_departments_build_request,
    get_next_marker=_departments_get_next_marker,
)


# ---------------------------------------------------------------------------
# Registry
# ---------------------------------------------------------------------------
#
# Objects deliberately NOT registered yet, with the specific reason each
# needs resolving before it safely could be (rather than a guess):
#   - auditlog (AuditLogs API): real base URL confirmed from the official
#     spec's "servers" field (https://earlyaccess.api.evisort.com/v1) and
#     real path/required-field enums also confirmed -- but a real call to
#     that host's own /auth/token returns HTTP 404. This tenant's API key
#     may not be enrolled in AuditLogs early access, or the documented
#     spec may not be fully live yet -- not something fixable in code;
#     needs checking with Evisort directly.
#   - users (Admin API): GET /users/export confirmed against both dev and
#     prod to return a real XLSX file (ZIP magic bytes, correct
#     Content-Type) -- but the official docs' own example response shows a
#     flat "response=Key=Value ..." string instead, a genuine contradiction
#     not yet resolved. Needs the real file parsed (openpyxl) to settle it.
#   - departments (Admin API, GET /v2/admin/departments): a real call
#     returns HTTP 404, not the 403 (missing admin JWT / LaunchDarkly flag)
#     anticipated from the docs' own stated requirements -- suggesting this
#     "v2/admin" surface may not be live for this tenant at all, similar to
#     auditlog's early-access gate. Also not fixable in code; needs
#     checking with Evisort directly. DEPARTMENTS_OBJECT itself is already
#     built and registered below, based on a complete real example in the
#     docs -- ready the moment this access question resolves.
EVISORT_OBJECTS: dict[str, EvisortObject] = {
    "documents": DOCUMENTS_OBJECT,
    "groups": GROUPS_OBJECT,
    "tickets": TICKETS_OBJECT,
    "nodes": NODES_OBJECT,
    "departments": DEPARTMENTS_OBJECT,
    "workflows": WORKFLOWS_OBJECT,
    "activities": ACTIVITIES_OBJECT,
}

