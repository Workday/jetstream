# Databricks notebook source
# evisort_connector.py
#
# Jetstream Evisort Connector: the only notebook configured as a
# Databricks Job task. Authenticates to the Evisort API, streams contract
# metadata from GET /documents page by page, and MERGE-writes each page
# into a single Iceberg table keyed on evisort_id, checkpointing the
# modifiedSince cursor after every page.
#
# Three files, not a chain of many: an API connector this size doesn't
# need the file-per-concern split a file/object-storage connector does
# (no file discovery, no multiple write modes, no parsing quirks). Two
# pieces ARE split out, because each has its own independent reason to
# change separately from everything here (config, API client, ingest
# engine, validation, orchestration):
#   - control_plane.py: identical across every Jetstream connector type,
#     not Evisort-specific at all.
#   - evisort_objects.py: the Evisort object catalog (EvisortObject
#     registrations: field maps, schema, flatten functions, pagination).
#     This is the one part that actually grows every time a new Evisort
#     object gets registered; everything in this file stays the same
#     size regardless of how many objects exist.
# See README.md for the full design rationale.

# COMMAND ----------
# MAGIC %run ./control_plane

# COMMAND ----------
# MAGIC %run ./evisort_objects

# COMMAND ----------

from __future__ import annotations

import datetime as _dt
import hashlib
import json
import time
import traceback
import uuid
from dataclasses import asdict, dataclass
from typing import Callable

import requests

# pyspark imports are only used when running inside a real Spark session.
# Guarded so this file can still be syntax-checked and partially exercised
# standalone without pyspark installed.
try:
    from pyspark.sql import DataFrame, SparkSession
    from pyspark.sql.types import ArrayType, BooleanType, DateType, LongType, StringType, StructField, StructType, TimestampType
except ImportError:
    DataFrame = SparkSession = None  # type: ignore[assignment,misc]
    ArrayType = BooleanType = DateType = LongType = StringType = StructField = StructType = TimestampType = None  # type: ignore[assignment,misc]

# %run (above) is a Databricks notebook-only magic, inert as a plain
# comment when this file runs standalone via `python3 evisort_connector.py`.
# Outside Databricks, fall back to real imports of the same two files so
# every name %run would have injected (ControlPlaneManager,
# EVISORT_OBJECTS, DOCUMENTS_OBJECT, extract_field_values(), etc.) is
# still available below, including in the __main__ test block. dbutils is
# only ever present as a global inside an actual Databricks notebook, so
# this branch never runs there.
try:
    dbutils  # noqa: F821
except NameError:
    import sys as _sys
    from pathlib import Path as _Path
    _sys.path.insert(0, str(_Path(__file__).resolve().parent))
    from control_plane import *  # noqa: F401,F403
    # Explicit names, not `import *`: evisort_objects.py's per-object
    # build_request/get_next_marker pairs are intentionally underscore-
    # prefixed (private to the object registry), which `import *` skips,
    # but the __main__ block below exercises them directly by name.
    from evisort_objects import (  # noqa: F401
        CLAUSE_TEXT_JOIN_SEPARATOR, CLAUSE_TEXT_MAX_LENGTH, PRIMARY_KEY,
        DOCUMENTS_IDENTITY_COLUMNS, DOCUMENTS_SURROGATE_COLUMNS,
        sanitize_column_name, extract_field_values, extract_clause_text,
        resolve_column_destinations, CLAUSE_FIELD_COLLISION_SUFFIX,
        build_documents_flatten_fn, EvisortObject,
        EPOCH_MODIFIED_SINCE, _documents_build_request, _documents_get_next_marker,
        DOCUMENTS_OBJECT, resolve_documents_object,
        MANAGED_ENTITIES_BASE_URL, flatten_group, GROUPS_SCHEMA_COLUMNS,
        _groups_build_request, _groups_get_next_marker, GROUPS_OBJECT,
        flatten_node, NODES_SCHEMA_COLUMNS,
        _nodes_build_request, _nodes_get_next_marker, NODES_OBJECT,
        flatten_ticket, TICKETS_SCHEMA_COLUMNS,
        _tickets_build_request, _tickets_get_next_marker, TICKETS_OBJECT,
        flatten_workflow, WORKFLOWS_SCHEMA_COLUMNS,
        _workflows_build_request, _workflows_get_next_marker, WORKFLOWS_OBJECT,
        flatten_activity, ACTIVITIES_SCHEMA_COLUMNS,
        _activities_build_request, _activities_get_next_marker, ACTIVITIES_OBJECT,
        flatten_department, DEPARTMENTS_SCHEMA_COLUMNS,
        _departments_build_request, _departments_get_next_marker, DEPARTMENTS_OBJECT,
        EVISORT_OBJECTS,
    )


# =============================================================================
# SECTION 1: Shared ingest helpers (surrogate key fallback, cursor helper)
# =============================================================================
#
# Generic across every EvisortObject, not specific to one; unlike the
# per-object field maps/schemas/pagination logic in evisort_objects.py,
# these don't grow when a new object is registered.

JETSTREAM_ID_COLUMN = "_jetstream_id"
JETSTREAM_INDEX_COLUMN = "_jetstream_index"
_SURROGATE_COLUMNS = frozenset({
    "_jetstream_source_object", "_jetstream_synced", "_jetstream_deleted",
    JETSTREAM_ID_COLUMN, JETSTREAM_INDEX_COLUMN,
})


def compute_jetstream_id(row: dict, obj: "EvisortObject") -> str:
    """
    SHA-256 hash of a flattened row's non-surrogate column values, for
    objects with no confirmed natural key (EvisortObject.id_field is None).
    Matches the Jetstream standard's definition of _jetstream_id: a
    deterministic hash of a row's non-system column values, used for
    tables without a defined natural key. Deterministic on content, not
    identity: two syncs of an unchanged record hash identically and
    MERGE as a no-op update; a record whose data actually changed between
    syncs hashes differently and lands as an additional row rather than
    updating the old one in place. That is a known, accepted limitation
    of content-hash keys generally, not something specific to this
    connector.

    Args:
        row: The flattened row dict, before _jetstream_id/_jetstream_index
            are added to it.
        obj: The EvisortObject the row belongs to, whose schema_columns
            defines which columns are "data" versus surrogate.

    Returns:
        Hex-encoded SHA-256 digest.
    """
    data_columns = [name for name, _ in obj.schema_columns if name not in _SURROGATE_COLUMNS]
    concatenated = "".join(str(row.get(c, "")) for c in data_columns)
    return hashlib.sha256(concatenated.encode("utf-8")).hexdigest()


def get_max_cursor_value(records: list[dict], cursor_field: str) -> str | None:
    """
    Max value of cursor_field across a page's raw records, for the next
    checkpoint. Evisort's dateModified is ISO 8601 with a fixed-width UTC
    offset, which sorts correctly as a plain string; no parsing needed.
    """
    values = [r[cursor_field] for r in records if r.get(cursor_field)]
    return max(values) if values else None


# COMMAND ----------

# =============================================================================
# SECTION 2: Configuration
# =============================================================================

VALID_ENVIRONMENTS = ("dev", "staging", "prod")
VALID_ON_ERROR = ("fail", "skip")


@dataclass
class JetstreamConfig:
    """Typed configuration for a single connector run, populated from Databricks widgets."""

    target_catalog: str
    target_schema: str
    sync_objects: str = "documents"
    table_name: str = ""
    columns: str = ""
    clauses: str = ""
    table_group_name: str = ""
    api_base_url: str = "https://api.evisort.com/v1"
    secret_scope: str = ""
    secret_key_name: str = "evisort-api-key"
    page_size: str = "100"
    modified_since_override: str = ""
    enable_schema_evolution: str = "true"
    on_error: str = "fail"
    control_plane_catalog: str = "udl_dataflow_metadata_prod"
    control_plane_schema_override: str = ""
    databricks_job_id: str = ""
    databricks_job_run_id: str = ""
    databricks_workspace_id: str = ""
    environment: str = "prod"
    owner_team: str = ""
    sla_target_minutes: str = "1440"
    dry_run: str = "false"
    force_full_refresh: str = "false"
    run_connection_test: str = "true"
    preview_documents_count: str = "5"
    batch_size: str = ""

    def as_bool(self, field_name: str) -> bool:
        return str(getattr(self, field_name)).strip().lower() == "true"

    def as_int(self, field_name: str) -> int:
        raw_value = getattr(self, field_name)
        try:
            return int(raw_value)
        except ValueError as exc:
            raise ValueError(f"Config field '{field_name}' with value '{raw_value}' could not be parsed as an integer.") from exc

    def get_sync_objects_list(self) -> list[str]:
        """Parse sync_objects into a list of EVISORT_OBJECTS keys to sync this run."""
        return [v.strip() for v in self.sync_objects.split(",") if v.strip()]

    def get_columns_list(self) -> list[str]:
        """Parse columns into the list of exact field names this instance's documents table includes (destination type ARRAY<STRING>)."""
        return [v.strip() for v in self.columns.split(",") if v.strip()]

    def get_clauses_list(self) -> list[str]:
        """Parse clauses into the list of exact provision names this instance's documents table includes (destination type STRING)."""
        return [v.strip() for v in self.clauses.split(",") if v.strip()]

    def get_control_plane_schema(self) -> str:
        """
        FQN of the shared jetstream control plane schema for this run, or a
        jetstream_<connector_type>-style schema when
        control_plane_schema_override is set, for example to keep test runs
        separate from the shared schema. Enforced in validate_config(),
        not here: this just builds the FQN.
        """
        schema_name = self.control_plane_schema_override.strip() or "jetstream"
        return f"{self.control_plane_catalog}.{schema_name}"

    def get_table_fqn(self, table_name: str) -> str:
        return f"{self.target_catalog}.{self.target_schema}.{table_name}"


# Maps JetstreamConfig field name -> (default value as string, is required).
_WIDGET_SPEC: dict[str, tuple[str, bool]] = {
    "target_catalog":          ("", True),
    "target_schema":           ("", True),
    "sync_objects":             ("documents", False),
    "table_name":               ("", False),
    "columns":                  ("", False),
    "clauses":                  ("", False),
    "table_group_name":         ("", False),
    "api_base_url":             ("https://api.evisort.com/v1", False),
    "secret_scope":             ("", True),
    "secret_key_name":          ("evisort-api-key", False),
    "page_size":                ("100", False),
    "modified_since_override":  ("", False),
    "enable_schema_evolution":  ("true", False),
    "on_error":                 ("fail", False),
    "control_plane_catalog":    ("udl_dataflow_metadata_prod", False),
    "control_plane_schema_override": ("", False),
    "databricks_job_id":        ("", False),
    "databricks_job_run_id":    ("", False),
    "databricks_workspace_id":  ("", False),
    "environment":              ("prod", False),
    "owner_team":               ("", False),
    "sla_target_minutes":       ("1440", False),
    "dry_run":                  ("false", False),
    "force_full_refresh":       ("false", False),
    "run_connection_test":      ("true", False),
    "preview_documents_count":  ("5", False),
    "batch_size":               ("", False),
}


def register_widgets(dbutils) -> None:
    """Declare all Databricks widgets with their default values."""
    for field_name, (default_value, _is_required) in _WIDGET_SPEC.items():
        dbutils.widgets.text(field_name, default_value)


def load_config(dbutils) -> JetstreamConfig:
    """
    Read all Databricks widgets and build a JetstreamConfig. Job task
    parameters override these defaults by widget name (case-sensitive); a
    typo in a job parameter key is silently ignored by Databricks rather
    than erroring, so the override summary below is the primary way to
    catch that in job run logs.
    """
    print("Registering Jetstream Evisort Connector widgets...")
    register_widgets(dbutils)

    field_values = {name: dbutils.widgets.get(name) for name in _WIDGET_SPEC}
    overridden = [n for n, v in field_values.items() if v != _WIDGET_SPEC[n][0]]
    defaulted = [n for n in field_values if n not in overridden]
    print(f"Widgets overridden by job parameters ({len(overridden)}):")
    for n in overridden:
        print(f"  {n} = {field_values[n]}")
    print(f"Widgets using default value ({len(defaulted)}): {', '.join(defaulted) or '(none)'}")

    return JetstreamConfig(**field_values)


def validate_config(config: JetstreamConfig) -> None:
    """
    Validate required fields, enum values, and numeric fields.

    Raises:
        ValueError: If any validation rule fails.
    """
    for field_name, (_default, is_required) in _WIDGET_SPEC.items():
        if is_required and not getattr(config, field_name).strip():
            raise ValueError(f"Required config field '{field_name}' is missing or empty.")

    for field_name, value, allowed in (
        ("environment", config.environment, VALID_ENVIRONMENTS),
        ("on_error", config.on_error, VALID_ON_ERROR),
    ):
        if value not in allowed:
            raise ValueError(f"Config field '{field_name}' has invalid value '{value}'. Allowed: {', '.join(allowed)}.")

    # control_plane_schema_override, when set, must be named
    # jetstream_<connector_type>, for example jetstream_evisort.
    # Rejecting anything else here prevents a typo from silently
    # redirecting control plane
    # writes to an unrelated schema, and rejecting the literal default
    # "jetstream" specifically catches a redundant override that should
    # just be left blank instead.
    schema_override = config.control_plane_schema_override.strip()
    if schema_override:
        if schema_override == "jetstream":
            raise ValueError(
                "Config field 'control_plane_schema_override' is set to 'jetstream', which is "
                "already the default — leave it blank instead of setting it explicitly."
            )
        if not schema_override.startswith("jetstream_"):
            raise ValueError(
                f"Config field 'control_plane_schema_override' must start with 'jetstream_' "
                f"(e.g. 'jetstream_evisort'), per the platform doc's dev/test schema naming "
                f"convention. Got: '{schema_override}'."
            )

    sync_objects = config.get_sync_objects_list()
    if not sync_objects:
        raise ValueError("Config field 'sync_objects' must list at least one object to sync.")
    unknown_objects = [name for name in sync_objects if name not in EVISORT_OBJECTS]
    if unknown_objects:
        raise ValueError(
            f"Config field 'sync_objects' references unregistered object(s): "
            f"{', '.join(unknown_objects)}. Registered objects: {', '.join(EVISORT_OBJECTS)}."
        )

    # table_name is required whenever "documents" is being synced, and at
    # least one of columns/clauses must be set; neither is expressible via
    # the generic _WIDGET_SPEC required-fields loop above, since "required"
    # here means "required when sync_objects contains documents," not
    # unconditionally.
    if "documents" in sync_objects:
        if not config.table_name.strip():
            raise ValueError(
                "Config field 'table_name' is required when 'documents' is included in sync_objects."
            )
        selected_fields = config.get_columns_list()
        selected_clauses = config.get_clauses_list()
        if not selected_fields and not selected_clauses:
            raise ValueError(
                "At least one of config fields 'columns' or 'clauses' is required when 'documents' "
                "is included in sync_objects — list the exact Evisort field names (columns) and/or "
                "provision names (clauses) this instance's documents table should include. There is "
                "no fixed catalog and no 'select everything' default; any label Evisort actually "
                "returns for this tenant is valid."
            )
        # No catalog to validate names against: any exact label Evisort returns
        # is fair game (that's the point). What IS validated: every selected name,
        # field or clause, must sanitize (see sanitize_column_name()) to a
        # non-empty destination column name, and every RESOLVED destination name
        # (see resolve_column_destinations()) must be distinct across both lists.
        # A field and a clause sharing the identical exact raw name (e.g.
        # "Governing Law" in both columns and clauses) is deliberately NOT a
        # collision: resolve_column_destinations() gives the clause a distinct
        # suffixed name so both can coexist. Any other clash (a duplicate raw
        # name within one list, or two different raw names that happen to land
        # on the same destination) is still rejected here.
        tagged_entries = [("columns", name) for name in selected_fields] + [("clauses", name) for name in selected_clauses]
        sanitized_entries = [(source_param, raw, sanitize_column_name(raw)) for source_param, raw in tagged_entries]
        empty_after_sanitize = [raw for _, raw, sanitized in sanitized_entries if not sanitized]
        if empty_after_sanitize:
            raise ValueError(
                f"Config fields 'columns'/'clauses' have entries with no alphanumeric characters, "
                f"so they sanitize to an empty column name: {', '.join(empty_after_sanitize)!r}."
            )
        field_destinations, clause_destinations = resolve_column_destinations(
            tuple(selected_fields), tuple(selected_clauses)
        )
        destination_entries = (
            [("columns", raw, field_destinations[raw]) for raw in selected_fields]
            + [("clauses", raw, clause_destinations[raw]) for raw in selected_clauses]
        )
        name_occurrences: dict[str, list[str]] = {}
        for source_param, raw, destination in destination_entries:
            name_occurrences.setdefault(destination, []).append(f"{raw!r} (from {source_param})")
        collisions = [
            f"{destination!r} <- {' and '.join(sources)}"
            for destination, sources in name_occurrences.items()
            if len(sources) > 1
        ]
        if collisions:
            raise ValueError(
                f"Config fields 'columns'/'clauses' have entries that collide on the same "
                f"destination column name: {'; '.join(collisions)}. Choose distinct names or drop "
                f"the duplicate. Note: a field and a clause with the IDENTICAL exact name (e.g. "
                f"'Governing Law' in both columns and clauses) is allowed and automatically "
                f"disambiguated with the '{CLAUSE_FIELD_COLLISION_SUFFIX}' suffix on the clause "
                f"side — this error means something else collided."
            )

    for field_name in ("page_size", "preview_documents_count", "sla_target_minutes"):
        try:
            parsed = int(getattr(config, field_name))
        except ValueError as exc:
            raise ValueError(f"Config field '{field_name}' must be an integer.") from exc
        if parsed < 0:
            raise ValueError(f"Config field '{field_name}' must be non-negative, got {parsed}.")

    # batch_size is the one exception to the loop above: blank is valid and
    # deliberately means "no cap" (see UNLIMITED_BATCH_SIZE in sync_object())
    # rather than silently falling back to some hardcoded number nobody
    # chose. Only validated as an integer, and only required to be positive,
    # when an operator actually sets it.
    if config.batch_size.strip():
        try:
            parsed_batch_size = int(config.batch_size)
        except ValueError as exc:
            raise ValueError("Config field 'batch_size' must be an integer, or blank for no limit.") from exc
        if parsed_batch_size <= 0:
            raise ValueError("Config field 'batch_size' must be greater than zero, or blank for no limit.")

    if int(config.page_size) == 0:
        raise ValueError("Config field 'page_size' must be greater than zero.")

    print("Config validation passed.")


def print_config_summary(config: JetstreamConfig) -> None:
    """
    secret_scope/secret_key_name are references to where the Evisort API
    key lives, not the credential value itself, so printing them is safe
    per the platform's credential-handling standard. The API key and
    bearer token are never logged anywhere in this connector.
    """
    print("=" * 70)
    print("Jetstream Evisort Connector configuration summary")
    print("=" * 70)
    for k, v in asdict(config).items():
        print(f"  {k}: {v}")
    print("=" * 70)


def build_source_config(config: JetstreamConfig) -> dict:
    """
    Every current parameter value except target_catalog/target_schema --
    those are destination fields, stored as their own connector_instance
    columns, not part of source_config's JSON at all (matching the
    Jetstream standard's Instance Bootstrapping section: "Split that dictionary
    by convention into source_config and destination_catalog/
    destination_schema"). Built from _WIDGET_SPEC's own key list rather
    than a hand-maintained field list, so a future parameter is
    automatically included here too instead of silently missing from
    what gets persisted and what Runtime Validation can check for drift.
    """
    return {
        name: getattr(config, name)
        for name in _WIDGET_SPEC
        if name not in ("target_catalog", "target_schema")
    }


def build_config_schema() -> str:
    """
    JSON Schema compiled from parameters.yaml, stored in
    connector_manifest.config_schema -- describes source_config's shape
    specifically, so target_catalog/target_schema are deliberately absent
    here too (see build_source_config()'s docstring for why).
    """
    return json.dumps({
        "type": "object",
        "required": ["secret_scope"],
        "properties": {
            "sync_objects": {"type": "string"},
            "table_name": {
                "type": "string",
                # sync_objects is a comma-separated multi-value field (e.g.
                # "documents,groups,tickets"), so this needs "contains"
                # (list membership) rather than the Jetstream standard's literal
                # "value" (exact equality) -- see validate_source_config()'s
                # docstring for why "value" alone would be actively wrong
                # here, not just less precise.
                "depends_on": {"parameter": "sync_objects", "contains": "documents"},
            },
            "columns": {"type": "string"},
            "clauses": {"type": "string"},
            "table_group_name": {"type": "string"},
            "api_base_url": {"type": "string"},
            "secret_scope": {"type": "string"},
            "secret_key_name": {"type": "string"},
            "page_size": {"type": "integer", "minimum": 1},
            "modified_since_override": {"type": "string"},
            "enable_schema_evolution": {"type": "boolean"},
            "on_error": {"type": "string", "enum": ["fail", "skip"]},
            "control_plane_catalog": {"type": "string"},
            "control_plane_schema_override": {"type": "string"},
            "databricks_job_id": {"type": "string"},
            "databricks_job_run_id": {"type": "string"},
            "databricks_workspace_id": {"type": "string"},
            "environment": {"type": "string", "enum": ["dev", "staging", "prod"]},
            "owner_team": {"type": "string"},
            "sla_target_minutes": {"type": "integer", "minimum": 0},
            "dry_run": {"type": "boolean"},
            "force_full_refresh": {"type": "boolean"},
            "run_connection_test": {"type": "boolean"},
            "preview_documents_count": {"type": "integer", "minimum": 1},
            # batch_size is optional, no default, and blank is valid (means
            # "no limit" -- see UNLIMITED_BATCH_SIZE in sync_object()), so
            # this schema entry allows either an integer or an empty string
            # rather than requiring "integer" unconditionally.
            "batch_size": {"type": ["integer", "string"], "minimum": 1},
        },
    })


# COMMAND ----------

# =============================================================================
# SECTION 3: Evisort API client (auth, backoff, pagination)
# =============================================================================

MAX_RETRIES = 5
BACKOFF_BASE_SECONDS = 2

# Sentinel used in place of a real batch_size when force_full_refresh=true
# overrides it. A large plain int, not float('inf') -- iter_object_pages()
# slices page_records[:remaining_capacity] when truncating, and Python
# list slicing raises TypeError on a float index. Kept strictly int
# throughout so no arithmetic in that loop needs a special case for this
# sentinel beyond the print statements recognizing it for a friendlier log.
UNLIMITED_BATCH_SIZE = 10**9
RETRYABLE_STATUS_CODES = (429, 500, 502, 503, 504)
TOKEN_RESPONSE_KEYS = ("token", "access_token", "jwt", "bearer_token")


@dataclass
class ObjectPage:
    """One page of a paginated Evisort object response, yielded by iter_object_pages()."""

    records: list[dict]
    page_number: int
    total_records_found: int
    is_last_page: bool


def _request_with_backoff(method: str, url: str, allow_401: bool = False, **kwargs) -> requests.Response:
    """
    Issue a single HTTP request, retrying on rate limiting (429) and
    transient server errors (5xx) with exponential backoff, honoring
    Retry-After when present. API connectors, unlike file connectors,
    routinely hit rate limits mid-sync and must recover without failing
    the whole run.

    allow_401: when True, a 401 response is returned to the caller as-is
    instead of raised immediately -- used by fetch_object_page(), which
    needs to see the 401 itself to trigger a token refresh and retry,
    rather than having this function turn it into an exception first.
    """
    last_exc: Exception | None = None
    for attempt in range(1, MAX_RETRIES + 1):
        try:
            response = requests.request(method, url, timeout=kwargs.pop("timeout", 60), **kwargs)
        except requests.exceptions.RequestException as exc:
            last_exc = exc
            wait_seconds = BACKOFF_BASE_SECONDS ** attempt
            print(f"WARNING: request to '{url}' failed ({exc}); retrying in {wait_seconds}s (attempt {attempt}/{MAX_RETRIES}).")
            time.sleep(wait_seconds)
            continue

        if allow_401 and response.status_code == 401:
            return response

        if response.status_code not in RETRYABLE_STATUS_CODES:
            response.raise_for_status()
            return response

        retry_after = response.headers.get("Retry-After")
        wait_seconds = int(retry_after) if retry_after and retry_after.isdigit() else BACKOFF_BASE_SECONDS ** attempt
        print(f"WARNING: '{url}' returned HTTP {response.status_code}; backing off {wait_seconds}s (attempt {attempt}/{MAX_RETRIES}).")
        time.sleep(wait_seconds)
        last_exc = RuntimeError(f"HTTP {response.status_code} from '{url}'")

    raise RuntimeError(f"Request to '{url}' failed after {MAX_RETRIES} attempts: {last_exc}") from last_exc


class BearerTokenHolder:
    """
    Mutable holder for the current bearer token.

    A bearer token passed around as a plain string can't be updated in
    place -- once handed to a caller, that caller's copy never changes. A
    reactive refresh triggered deep in fetch_object_page() (on an HTTP 401)
    needs the NEW token to be immediately visible to every other in-flight
    or subsequent call in the run, which requires a shared mutable object
    instead.

    Reactive (refresh-on-401), not proactive (refresh-every-N-pages),
    deliberately: neither Evisort's public API documentation
    (developers.evisort.com) nor any third-party integration guide checked
    states a JWT expiry duration or a page/time-based refresh requirement.
    Sizing a proactive refresh interval would mean guessing that number --
    too conservative wastes an auth call every few pages, too loose still
    fails. Refreshing exactly when a 401 actually occurs self-adapts to
    whatever the real expiry is (and to it changing later) without needing
    to know it up front.
    """

    def __init__(self, api_base_url: str, api_key: str):
        self.api_base_url = api_base_url
        self.api_key = api_key
        self.token = get_bearer_token(api_base_url, api_key)
        self.refresh_count = 0

    def refresh(self) -> str:
        self.refresh_count += 1
        print(f"Bearer token appears to have expired (HTTP 401) -- re-authenticating "
              f"(refresh #{self.refresh_count} this run).")
        self.token = get_bearer_token(self.api_base_url, self.api_key)
        return self.token


def get_bearer_token(api_base_url: str, api_key: str) -> str:
    """Exchange the Evisort API key for a bearer token via POST /auth/token."""
    url = f"{api_base_url.rstrip('/')}/auth/token"
    response = _request_with_backoff("POST", url, headers={"evisort-api-key": api_key})
    try:
        payload = response.json()
    except ValueError as exc:
        raise RuntimeError(f"Evisort token response from '{url}' was not valid JSON: {exc}") from exc
    for key in TOKEN_RESPONSE_KEYS:
        if key in payload and payload[key]:
            return payload[key]
    raise RuntimeError(
        f"Evisort token response from '{url}' did not contain any of the expected "
        f"token fields {TOKEN_RESPONSE_KEYS}. Response keys: {list(payload.keys())}."
    )


def fetch_object_page(
    url: str, token_holder: "BearerTokenHolder", method: str = "GET",
    params: dict | None = None, json_body: dict | None = None
) -> dict:
    """
    Fetch a single page of any paginated Evisort object endpoint, GET or
    POST. On an HTTP 401 (token expired), refreshes token_holder in place
    and retries this exact request once with the new token before giving
    up -- see BearerTokenHolder's docstring for why this is reactive
    rather than a proactive refresh-every-N-pages scheme.
    """
    def _do_request() -> requests.Response:
        headers = {"authorization": f"Bearer {token_holder.token}", "content-type": "application/json"}
        return _request_with_backoff(
            method, url, headers=headers, params=params, json=json_body, allow_401=True
        )

    response = _do_request()
    if response.status_code == 401:
        token_holder.refresh()
        response = _do_request()
        response.raise_for_status()  # a second 401 here is a real failure, not token expiry -- surface it

    try:
        return response.json()
    except ValueError as exc:
        raise RuntimeError(f"Evisort response from '{url}' was not valid JSON: {exc}") from exc


def iter_object_pages(
    api_base_url: str,
    token_holder: "BearerTokenHolder",
    obj: EvisortObject,
    cursor_value: str | None,
    page_size: int,
    batch_size: int,
    extra_params: dict | None = None,
):
    """
    Stream one Evisort object's records one page at a time, until the API
    is exhausted or batch_size records have been yielded. A generator, not
    collect-then-return, so the caller can write and checkpoint each page
    immediately (checkpoint after each batch), so if the job dies mid-run,
    the next run resumes from the last checkpointed page instead of redoing the whole window.

    Pagination mechanics are entirely delegated to obj.build_request() and
    obj.get_next_marker(); this loop only threads the opaque `marker`
    between them, so it works identically whether a "next page" means a
    full URL (documents, groups), an incrementing page number with no
    `next` field at all (tickets), or (for a future object) a token
    embedded in a POST body. extra_params is a hook for object-specific
    request parameters beyond page_size/cursor_value (e.g. a future
    AuditLogs object's required operation/entity/start_time) unused by
    every object registered today.

    Yields:
        ObjectPage, one per page fetched, in request order.
    """
    batch_size_display = "unlimited" if batch_size == UNLIMITED_BATCH_SIZE else str(batch_size)
    print(
        f"[{obj.object_name}] Starting discovery: cursor={cursor_value or '(none)'}, "
        f"page_size={page_size}, batch_size={batch_size_display}"
    )

    marker = None
    page_number = 0
    records_yielded = 0
    total_records_found = 0

    while records_yielded < batch_size:
        method, url, params, body = obj.build_request(api_base_url, marker, page_size, cursor_value, extra_params or {})
        response = fetch_object_page(url, token_holder, method=method, params=params, json_body=body)
        page_number += 1

        # workflows' response is a bare JSON array, not {"data": [...], ...}
        # like every other registered object -- list_key=None signals this.
        # A plain list has no .get() method and no room for a "count"/"meta"
        # envelope, so both need a distinct code path rather than assuming
        # every response is a dict.
        if obj.list_key is None:
            if not isinstance(response, list):
                raise RuntimeError(
                    f"[{obj.object_name}] expected a bare JSON array response (list_key=None) "
                    f"but got {type(response).__name__} instead."
                )
            page_records = response
            if page_number == 1:
                total_records_found = len(response)
        else:
            if page_number == 1:
                total_records_found = response.get("count") or (response.get("meta") or {}).get("total") or 0
                if total_records_found:
                    print(f"[{obj.object_name}]   {total_records_found} total record(s) available from the source API "
                          f"(matching the current filter/cursor) -- this run processes at most batch_size={batch_size_display}.")
            page_records = response.get(obj.list_key, [])

        remaining_capacity = batch_size - records_yielded
        truncated = len(page_records) > remaining_capacity
        if truncated:
            page_records = page_records[:remaining_capacity]
        records_yielded += len(page_records)

        next_marker = None if truncated else obj.get_next_marker(response, params, body)
        is_last_page = next_marker is None

        print(f"[{obj.object_name}]   Page {page_number}: {len(page_records)} record(s), running total {records_yielded}")
        if truncated:
            remaining_estimate = max(total_records_found - records_yielded, 0)
            print(f"[{obj.object_name}]   batch_size={batch_size} reached; approximately {remaining_estimate} "
                  f"record(s) remain and are deferred to the next run.")

        yield ObjectPage(page_records, page_number, total_records_found, is_last_page)
        if is_last_page:
            break
        marker = next_marker


# COMMAND ----------

# =============================================================================
# SECTION 4: Ingest (flatten + MERGE write)
# =============================================================================

_SPARK_TYPE_MAP = {}
if StructType is not None:
    _SPARK_TYPE_MAP = {
        "bigint": LongType(), "string": StringType(), "date": DateType(),
        "timestamp": TimestampType(), "boolean": BooleanType(),
        "array<string>": ArrayType(StringType()),
    }


def _coerce_for_spark_type(value, type_name: str, column_name: str = ""):
    """
    Evisort returns date/timestamp values as ISO strings (e.g.
    "2025-04-21", "2026-05-07T01:00:25+00:00"); flatten functions pass
    them through unchanged. createDataFrame() with an explicit schema is
    strict about Python types; unlike Spark SQL casting at query time,
    DateType/TimestampType columns reject a plain str outright, so this
    converts to the concrete type Spark actually requires, based on the
    column's declared schema type. Applied generically in
    _build_dataframe() so every object's date/timestamp columns are
    covered, not just documents'.

    Falls back to None (logged, not raised) for a value that can't be
    parsed into the target type; real Evisort data has turned out to
    include date/timestamp values that don't cleanly round-trip through
    isoformat, and one bad value shouldn't take down an entire page's
    MERGE over a column no one is even relying on being non-null.
    """
    if value is None:
        return None
    if type_name == "date":
        if isinstance(value, _dt.datetime):
            return value.date()
        if isinstance(value, _dt.date):
            return value
        try:
            return _dt.date.fromisoformat(str(value)[:10])
        except ValueError:
            print(f"WARNING: could not parse {column_name!r} value {value!r} as a date; storing NULL instead.")
            return None
    if type_name == "timestamp":
        if isinstance(value, _dt.datetime):
            return value
        try:
            return _dt.datetime.fromisoformat(str(value).replace("Z", "+00:00"))
        except ValueError:
            print(f"WARNING: could not parse {column_name!r} value {value!r} as a timestamp; storing NULL instead.")
            return None
    return value


@dataclass
class PageWriteResult:
    rows_read: int
    rows_written: int    # rows_inserted + rows_updated -- total rows this MERGE processed
    rows_skipped: int
    schema_evolved: bool
    rows_inserted: int = 0   # real count, from MERGE's own operation metrics where available
    rows_updated: int = 0    # real count of matched rows with a genuine content change
    rows_unchanged: int = 0  # real count of matched rows where content was byte-for-byte identical


class IngestEngine:
    """
    Flattens raw Evisort records and MERGEs them into a target Iceberg
    table. Object-agnostic: every method takes the EvisortObject being
    synced explicitly, rather than reading a single module-level schema;
    the same engine instance handles every object in one run, tracking
    which target tables it has already created this run so a second
    object's table creation doesn't collide with the first's.
    """

    def __init__(self, spark, config: JetstreamConfig) -> None:
        self.spark = spark
        self.config = config
        self._created_tables: set[str] = set()

    def get_real_row_count(self, target_table: str) -> int | None:
        """
        The destination table's real total row count -- for
        connector_table_registry.row_count_estimate, which the Jetstream standard
        defines as "Latest known row count" of the table, not "how many
        rows this run happened to process." A run that only touches 1 row
        (a small incremental sync) must not overwrite a registry showing
        3,015 rows down to 1.

        Returns None (caller should skip updating the field rather than
        write a wrong value) if the table doesn't exist yet or the count
        query fails for any reason.
        """
        try:
            if not self.spark.catalog.tableExists(target_table):
                return None
            return self.spark.sql(f"SELECT COUNT(*) AS n FROM {target_table}").collect()[0]["n"]
        except Exception as exc:
            print(f"WARNING: could not read real row count for '{target_table}' "
                  f"({type(exc).__name__}: {exc}); row_count_estimate will not be updated this run.")
            return None

    def write_page(self, obj: EvisortObject, records: list[dict], target_table: str) -> PageWriteResult:
        """
        Flatten one page of one object's records and MERGE it into target_table.

        Raises:
            ValueError: If flattening a record fails and on_error='fail'.
            RuntimeError: If the write itself fails.
        """
        rows: list[dict] = []
        rows_skipped = 0
        now = _dt.datetime.now(_dt.timezone.utc)

        for index, record in enumerate(records):
            try:
                row = obj.flatten_fn(record)
            except Exception as exc:
                if self.config.on_error == "fail":
                    raise ValueError(f"Failed to flatten '{obj.object_name}' record: {exc}") from exc
                print(f"WARNING: skipping '{obj.object_name}' record due to flattening error ({exc}).")
                rows_skipped += 1
                continue

            if obj.id_field is None:
                # No confirmed natural key for this object: fall back to
                # a content-hash surrogate key, per the Jetstream standard's
                # own definition of _jetstream_id/_jetstream_index for
                # "tables without a defined natural key." _jetstream_index
                # is this write batch's own 0-based position, not a
                # global sequence across pages/runs, sufficient to
                # disambiguate ordering within one page write.
                row[JETSTREAM_ID_COLUMN] = compute_jetstream_id(row, obj)
                row[JETSTREAM_INDEX_COLUMN] = index
                row_id = row[JETSTREAM_ID_COLUMN]
            else:
                row_id = row[obj.primary_key[0]]

            row["_jetstream_source_object"] = f"{obj.endpoint_path}/{row_id}"
            row["_jetstream_synced"] = now
            row["_jetstream_deleted"] = False
            rows.append(row)

        if not rows:
            return PageWriteResult(len(records), 0, rows_skipped, False)

        df = self._build_dataframe(obj, rows)
        table_exists = self.spark.catalog.tableExists(target_table)
        schema_evolved = False

        if not table_exists and target_table not in self._created_tables:
            self._create_target_table(df, target_table)
            self._created_tables.add(target_table)
        elif table_exists and self.config.as_bool("enable_schema_evolution"):
            schema_evolved = self._evolve_schema_if_needed(df, target_table)

        rows_inserted, rows_updated, rows_unchanged = self._merge_into_target(df, target_table, obj.primary_key)
        return PageWriteResult(
            len(records), len(rows), rows_skipped, schema_evolved, rows_inserted, rows_updated, rows_unchanged,
        )

    def _spark_schema(self, obj: EvisortObject) -> "StructType":
        return StructType([
            StructField(name, _SPARK_TYPE_MAP[t], nullable=(name not in obj.primary_key))
            for name, t in obj.schema_columns
        ])

    def _build_dataframe(self, obj: EvisortObject, rows: list[dict]) -> "DataFrame":
        ordered_rows = [
            tuple(_coerce_for_spark_type(row.get(name), t, name) for name, t in obj.schema_columns)
            for row in rows
        ]
        try:
            return self.spark.createDataFrame(ordered_rows, schema=self._spark_schema(obj))
        except Exception as exc:
            raise RuntimeError(f"Failed to build DataFrame for '{obj.object_name}' from flattened rows: {exc}") from exc

    def _create_target_table(self, df: "DataFrame", target_table: str) -> None:
        """
        Create target_table as an empty Iceberg table with df's schema,
        via a plain SQL CREATE TABLE (schema only, no data) rather than a
        DataFrameWriterV2 CTAS (df.writeTo(...).create()).

        Both a DataFrameWriterV2 CTAS and a SQL CREATE TABLE ... AS
        SELECT were confirmed, empirically, to fail with
        CommitCoordinatorGetCommitsFailedException against this
        workspace's Unity Catalog coordinated-commits setup, while a bare
        schema-only CREATE TABLE succeeds reliably every time. The
        failure is specific to creating a table WITH data atomically
        (CTAS, whichever API triggers it) -- not to Iceberg tables, not
        to this schema, and not to SQL vs. DataFrame API choice.

        Creating empty here and letting the caller's own MERGE INTO
        (write_page() always calls _merge_into_target() right after this,
        regardless of which branch created or evolved the table) populate
        the first batch avoids the failing code path entirely, since a
        MERGE INTO an already-existing (even if empty) table is a
        completely different Delta/Iceberg operation than CTAS.
        """
        print(f"Target table '{target_table}' does not exist. Creating it (schema only, no data).")
        column_defs = ", ".join(f"{f.name} {f.dataType.simpleString()}" for f in df.schema.fields)
        try:
            self.spark.sql(
                f"CREATE TABLE {target_table} ({column_defs}) USING ICEBERG TBLPROPERTIES ('format-version' = '3')"
            )
        except Exception as exc:
            raise RuntimeError(f"Failed to create target table '{target_table}': {exc}") from exc

    def _evolve_schema_if_needed(self, df: "DataFrame", target_table: str) -> bool:
        existing = {f.name for f in self.spark.table(target_table).schema.fields}
        new_fields = [f for f in df.schema.fields if f.name not in existing]
        if not new_fields:
            return False
        column_defs = ", ".join(f"{f.name} {f.dataType.simpleString()}" for f in new_fields)
        print(f"Schema evolution: adding columns to '{target_table}': {column_defs}")
        try:
            self.spark.sql(f"ALTER TABLE {target_table} ADD COLUMNS ({column_defs})")
        except Exception as exc:
            raise RuntimeError(f"Failed to evolve schema on '{target_table}': {exc}") from exc
        return True

    def _merge_into_target(self, df: "DataFrame", target_table: str, primary_key: tuple[str, ...]) -> tuple[int, int, int]:
        """
        MERGE INTO keyed on the object's primary_key. A record re-synced
        because it was modified again overwrites its row; a new record is
        inserted. No delete path; none of Evisort's endpoints expose a
        delete signal, so _jetstream_deleted is always FALSE for every object.

        The UPDATE branch is gated on at least one real business column
        actually differing (IS DISTINCT FROM -- null-safe, unlike a plain
        != which mishandles NULL <-> NULL), not "the row matched on
        primary key" alone. Without this, a row re-synced with byte-for-
        byte identical data (e.g. a force_full_refresh re-sync of
        unchanged records) would be indistinguishable from a row whose
        data genuinely changed -- both would fire the same UPDATE branch,
        and rows_unchanged could never be derived at all, since nothing
        would ever skip the branch to begin with.

        _jetstream_synced is intentionally EXCLUDED from the change
        comparison (along with every other _jetstream_* surrogate column)
        -- it's set to "now" on every write, so including it would make
        every matched row look "changed" regardless of real content,
        defeating the whole comparison. Consequence worth knowing: a
        genuinely unchanged row's _jetstream_synced no longer refreshes on
        each run -- it now reflects "last time this row's real content
        changed," not "last time we confirmed/re-synced it."

        Returns (rows_inserted, rows_updated, rows_unchanged) -- all real,
        not guessed: rows_inserted/rows_updated straight from Iceberg's own
        MERGE operation metrics (DESCRIBE HISTORY ... operationMetrics),
        now meaningful because the UPDATE branch is conditional;
        rows_unchanged as the simple remainder (total rows this page minus
        those two) -- no separate comparison query needed once the MERGE
        itself correctly distinguishes changed from unchanged.
        """
        source_view = "_jetstream_evisort_merge_source"
        df.createOrReplaceTempView(source_view)
        join_condition = " AND ".join(f"target.{k} = source.{k}" for k in primary_key)
        non_key_columns = [c for c in df.columns if c not in primary_key]
        update_assignments = ", ".join(f"{c} = source.{c}" for c in non_key_columns)

        # Business-content columns only -- excludes every _jetstream_*
        # surrogate column, not just _jetstream_synced, so a future
        # surrogate column doesn't silently break this the same way.
        content_columns = [c for c in non_key_columns if not c.startswith("_jetstream_")]
        if content_columns:
            change_condition = " OR ".join(f"target.{c} IS DISTINCT FROM source.{c}" for c in content_columns)
            when_matched_clause = f"WHEN MATCHED AND ({change_condition}) THEN UPDATE SET {update_assignments}"
        else:
            # No business columns at all (every column is a surrogate) --
            # nothing to compare, so nothing is ever "changed" by definition.
            when_matched_clause = f"WHEN MATCHED AND (FALSE) THEN UPDATE SET {update_assignments}"

        merge_sql = f"""
        MERGE INTO {target_table} AS target
        USING {source_view} AS source
        ON {join_condition}
        {when_matched_clause}
        WHEN NOT MATCHED THEN INSERT *
        """
        try:
            self.spark.sql(merge_sql)
        except Exception as exc:
            raise RuntimeError(f"Failed to MERGE into target table '{target_table}': {exc}") from exc

        total_rows_this_batch = df.count()
        try:
            history_row = self.spark.sql(f"DESCRIBE HISTORY {target_table} LIMIT 1").collect()[0]
            metrics = history_row["operationMetrics"] or {}
            rows_inserted = int(metrics.get("numTargetRowsInserted", 0))
            rows_updated = int(metrics.get("numTargetRowsUpdated", 0))
            rows_unchanged = max(total_rows_this_batch - rows_inserted - rows_updated, 0)
            return rows_inserted, rows_updated, rows_unchanged
        except Exception as exc:
            # DESCRIBE HISTORY's operationMetrics is officially documented for
            # Delta; for Iceberg it has been observed to work but isn't
            # guaranteed across every Databricks runtime. Falling back to the
            # old behavior (every row in this batch counted as an insert) is
            # safer than failing the whole write over a metrics shortfall --
            # but it does mean rows_inserted may overcount real inserts
            # whenever this fallback fires, since it can no longer tell
            # inserts, updates, and unchanged rows apart. Printed, not
            # silent, so it's visible in job logs when the real counts
            # weren't available.
            print(f"WARNING: could not read real insert/update/unchanged counts from DESCRIBE HISTORY for "
                  f"'{target_table}' ({type(exc).__name__}: {exc}); falling back to counting every "
                  f"written row as an insert. rows_inserted may overcount real inserts this page.")
            return total_rows_this_batch, 0, 0


# COMMAND ----------

# =============================================================================
# SECTION 5: Post-run validation
# =============================================================================

def get_object_run_row(spark, config: JetstreamConfig, object_run_id: str):
    """
    Query one object_run row by its own id. A run can touch more than one
    Evisort object now, so validation runs once per object_run_id rather
    than once per run_id; see validate_object_run().
    """
    cp_schema = config.get_control_plane_schema()
    query = f"""
    SELECT status, rows_read, (COALESCE(rows_inserted, 0) + COALESCE(rows_updated, 0)) AS rows_written,
        COALESCE(rows_unchanged, 0) AS rows_unchanged,
        CAST((unix_timestamp(end_time) - unix_timestamp(start_time)) AS DOUBLE) AS duration_seconds
    FROM {cp_schema}.connector_object_run
    WHERE object_run_id = '{object_run_id}'
    """
    try:
        rows = spark.sql(query).collect()
    except Exception as exc:
        raise RuntimeError(f"Failed to query connector_object_run for object_run_id '{object_run_id}': {exc}") from exc
    return rows[0] if rows else None


def verify_not_stuck_running(row) -> None:
    """
    Raises:
        ValueError: If the object_run row never reached a terminal status.
    """
    if row["status"] == OBJECT_STATUS_RUNNING:
        raise ValueError("connector_object_run is still 'running' after the run completed, indicating an unrecorded failure.")


def verify_rows_written_present(row) -> None:
    """
    Raises:
        ValueError: If a successful object sync read records but genuinely
            accounted for none of them -- neither wrote anything nor
            recognized any as legitimately unchanged.

    rows_written alone (rows_inserted + rows_updated) can correctly be 0
    on a fully successful run: a force_full_refresh re-sync of data that
    hasn't changed at all since the last sync writes nothing, because the
    MERGE correctly skips every row whose content already matches --
    confirmed against a real run (rows_read=3015, rows_inserted=0,
    rows_updated=0, rows_unchanged=3015), not just a hypothetical case.
    Data loss is rows_read records existing with NONE of them landing in
    any of the three real buckets -- inserted, updated, or unchanged --
    not simply rows_written alone being 0.
    """
    if row["status"] != OBJECT_STATUS_SUCCESS:
        return
    rows_read = row["rows_read"] or 0
    rows_written = row["rows_written"] or 0
    rows_unchanged = row["rows_unchanged"] or 0
    if rows_read > 0 and rows_written == 0 and rows_unchanged == 0:
        raise ValueError(
            f"Data loss detected: {rows_read} record(s) were read but zero rows "
            f"were written or recognized as unchanged. Check on_error handling "
            f"and flattening warnings in the job log."
        )


def print_final_summary(object_name: str, row) -> None:
    print("=" * 70)
    print(f"Jetstream Evisort Connector final execution summary — object '{object_name}'")
    print("=" * 70)
    print(f"  Status: {row['status']}")
    print(f"  Records read: {row['rows_read'] or 0}")
    print(f"  Rows written: {row['rows_written'] or 0}")
    print(f"  Rows unchanged (matched, byte-identical, MERGE skipped): {row['rows_unchanged'] or 0}")
    print(f"  Duration: {(row['duration_seconds'] or 0.0):.2f}s")
    print("=" * 70)


def validate_object_run(spark, config: JetstreamConfig, object_name: str, object_run_id: str) -> None:
    """
    Raises:
        ValueError: If a critical data loss or stuck-run scenario is detected.
        RuntimeError: If the object_run row cannot be found or queried.
    """
    print(f"Running post-run validation for object '{object_name}' (object_run_id='{object_run_id}')...")
    row = get_object_run_row(spark, config, object_run_id)
    if row is None:
        raise RuntimeError(f"No connector_object_run row found for object_run_id '{object_run_id}'.")
    verify_not_stuck_running(row)
    verify_rows_written_present(row)
    print_final_summary(object_name, row)
    print(f"Post-run validation passed for '{object_name}'.")


# COMMAND ----------

# =============================================================================
# SECTION 6: Orchestration
# =============================================================================

STAGE_TIMINGS: dict[str, float] = {}
_RUN_START_PERF_COUNTER = time.perf_counter()


def run_stage(stage_name: str, stage_fn, *args, **kwargs):
    print(f"\n>>> Stage '{stage_name}' starting...")
    start = time.perf_counter()
    try:
        result = stage_fn(*args, **kwargs)
    except Exception as exc:
        STAGE_TIMINGS[stage_name] = time.perf_counter() - start
        print(f"!!! Stage '{stage_name}' FAILED after {STAGE_TIMINGS[stage_name]:.2f}s: {exc}")
        print(traceback.format_exc())
        raise
    STAGE_TIMINGS[stage_name] = time.perf_counter() - start
    print(f"<<< Stage '{stage_name}' completed in {STAGE_TIMINGS[stage_name]:.2f}s.")
    return result


def stage_load_and_validate_config(dbutils) -> JetstreamConfig:
    config = load_config(dbutils)
    validate_config(config)
    print_config_summary(config)
    return config


def stage_authenticate(config: JetstreamConfig, dbutils) -> "BearerTokenHolder":
    try:
        api_key = dbutils.secrets.get(scope=config.secret_scope, key=config.secret_key_name)
    except Exception as exc:
        raise RuntimeError(
            f"Failed to read Evisort API key from secret scope '{config.secret_scope}' "
            f"key '{config.secret_key_name}': {exc}"
        ) from exc
    return BearerTokenHolder(config.api_base_url, api_key)


def resolve_api_base_url(config: JetstreamConfig, obj: EvisortObject) -> str:
    """An object's own base URL override (e.g. Managed Entities' /mes/public host) takes precedence over config.api_base_url."""
    return obj.api_base_url_override or config.api_base_url


def stage_connection_test(config: JetstreamConfig, token_holder: "BearerTokenHolder", objects: list[EvisortObject]) -> None:
    if not config.as_bool("run_connection_test"):
        print("run_connection_test=false: skipping connection test.")
        return
    for obj in objects:
        base_url = resolve_api_base_url(config, obj)
        next(iter_object_pages(base_url, token_holder, obj, None, page_size=1, batch_size=1), None)
        print(f"Connection test passed: able to reach {obj.endpoint_path}.")


def _nudge_past_inclusive_boundary(cursor_value: str) -> str:
    """
    Add the smallest representable increment (1 microsecond) to a stored
    cursor value before using it as the next run's modifiedSince filter.

    Confirmed real bug, not theoretical: a full sync followed immediately
    by an incremental sync re-fetched the exact same 1 document, forever,
    on every subsequent incremental run. The stored cursor was the exact
    dateModified of that document (correctly -- it's genuinely the max
    seen); Evisort's modifiedSince filter appears to be inclusive
    (dateModified >= modifiedSince), so requesting modifiedSince=<that
    document's own exact timestamp> matches that document again, forever
    -- and since its timestamp never changes, checkpoint_cursor()'s
    only-advance-forward check (candidate <= current) correctly refuses to
    move the cursor past it either, since it's never strictly newer than
    itself. Without this nudge, the stored cursor and the boundary
    document are permanently locked together.

    Only applied to a genuinely stored connector_state checkpoint -- not
    to modified_since_override, which is an operator-supplied value given
    in good faith and should be used exactly as given, and not to the
    epoch fallback (1970-01-01), which isn't close to any real document's
    timestamp and has no boundary-collision risk.

    Falls back to the original value unchanged if it doesn't parse as a
    real datetime (e.g. a future object's cursor field that isn't a
    timestamp at all), rather than raising and blocking the whole run
    over what would then just be a missed micro-optimization.
    """
    try:
        return (_dt.datetime.fromisoformat(cursor_value) + _dt.timedelta(microseconds=1)).isoformat()
    except (ValueError, TypeError):
        return cursor_value


def resolve_cursor(config: JetstreamConfig, control_plane: ControlPlaneManager, obj: EvisortObject) -> str | None:
    """
    Determine one object's incremental cursor, in priority order:
    force_full_refresh (None) > modified_since_override > stored cursor.
    Objects with no documented modified-since-style filter
    (obj.cursor_field is None, e.g. groups, tickets) always return None:
    every run does a full fetch for those; the write is still a safe,
    idempotent MERGE regardless. modified_since_override applies uniformly
    to every cursor-capable configured object; there's no per-object
    override yet since only documents has a concrete need for it today.
    """
    if obj.cursor_field is None:
        return None
    if config.as_bool("force_full_refresh"):
        print(f"[{obj.object_name}] force_full_refresh=true: ignoring stored cursor, fetching all records.")
        return None
    if config.modified_since_override.strip():
        print(f"[{obj.object_name}] Using modified_since_override='{config.modified_since_override}'.")
        return config.modified_since_override.strip()
    cursor = control_plane.get_cursor(obj.object_name, obj.cursor_field)
    if cursor is None:
        print(f"[{obj.object_name}] Using stored connector_state cursor: (none — brand new instance)")
        return None
    nudged = _nudge_past_inclusive_boundary(cursor)
    print(f"[{obj.object_name}] Using stored connector_state cursor: {cursor} "
          f"(requesting modifiedSince={nudged}, 1 microsecond past the stored checkpoint, "
          f"so the document that set it isn't re-fetched on an inclusive modifiedSince filter)")
    return nudged


def resolve_target_table(config: JetstreamConfig, obj: EvisortObject) -> str:
    """
    table_name overrides the "documents" object's table name specifically
    (its existing, tested default). Any other registered object uses its
    own default_table_name; per-instance overrides for those aren't
    exposed as config yet since there's no concrete need until a second
    object actually exists.
    """
    table_name = config.table_name if obj.object_name == "documents" else obj.default_table_name
    return config.get_table_fqn(table_name)


def ensure_target_schema_exists(spark, config: JetstreamConfig) -> None:
    """
    Create the destination schema if it doesn't already exist, so a brand
    new connector instance doesn't need a manual pre-provisioning step
    beyond target_catalog itself existing. Idempotent: CREATE SCHEMA IF
    NOT EXISTS is a no-op on every subsequent run.

    Scoped to the destination catalog/schema only (e.g. <target_catalog>.<schema>).
    This never touches the jetstream control plane schema; this connector
    must never create, alter, or drop that one, per the Jetstream standard;
    control_plane.ensure_tables_exist() only ever validates it exists.

    Raises:
        RuntimeError: If schema creation fails, typically because the
            Databricks job's service principal lacks CREATE SCHEMA
            privilege on target_catalog.
    """
    target_schema_fqn = f"{config.target_catalog}.{config.target_schema}"
    try:
        spark.sql(f"CREATE SCHEMA IF NOT EXISTS {target_schema_fqn}")
    except Exception as exc:
        raise RuntimeError(
            f"Failed to ensure destination schema '{target_schema_fqn}' exists: {exc}. "
            f"The job's service principal needs CREATE SCHEMA privilege on "
            f"catalog '{config.target_catalog}'."
        ) from exc
    print(f"Destination schema '{target_schema_fqn}' ready.")


def stage_dry_run_preview(
    config: JetstreamConfig, token_holder: "BearerTokenHolder", control_plane: ControlPlaneManager, objects: list[EvisortObject]
) -> None:
    preview_count = config.as_int("preview_documents_count")
    for obj in objects:
        cursor = resolve_cursor(config, control_plane, obj)
        base_url = resolve_api_base_url(config, obj)
        print(f"\ndry_run=true: previewing up to {preview_count} '{obj.object_name}' record(s). No data or state will be written.")
        pages = iter_object_pages(
            base_url, token_holder, obj, cursor, config.as_int("page_size"), batch_size=preview_count,
        )
        previewed = 0
        for page in pages:
            for record in page.records:
                flattened = obj.flatten_fn(record)
                id_value = flattened[obj.primary_key[0]]
                print(f"\n  {obj.object_name} {obj.primary_key[0]}={id_value}:")
                for column, value in flattened.items():
                    if value is not None:
                        print(f"    {column}: {value}")
                previewed += 1
            if page.is_last_page:
                break
        if previewed == 0:
            print(f"No '{obj.object_name}' records matched — nothing to preview.")


@dataclass
class ObjectSyncResult:
    """Per-object outcome of sync_object(), aggregated by main() across every configured object."""

    object_name: str
    object_run_id: str
    rows_read: int
    rows_written: int
    rows_skipped: int
    rows_inserted: int = 0
    rows_updated: int = 0
    rows_unchanged: int = 0


def sync_object(
    config: JetstreamConfig,
    run_id: str,
    token_holder: "BearerTokenHolder",
    control_plane: ControlPlaneManager,
    ingest_engine: IngestEngine,
    obj: EvisortObject,
) -> ObjectSyncResult:
    """
    Sync one Evisort object end to end: resolve its cursor, stream pages,
    write and checkpoint each as it arrives, and finalize its own
    connector_object_run row. A page-fetch failure always stops the run
    (config.on_error only governs per-record flattening errors within an
    already-fetched page, see IngestEngine.write_page()); it does not
    let other configured objects continue past this one's failure in V1.

    Raises:
        Whatever iter_object_pages()/write_page() raise, after recording
        this object's run as failed and finalizing its table registry row.
    """
    target_table = resolve_target_table(config, obj)
    cursor = resolve_cursor(config, control_plane, obj)
    base_url = resolve_api_base_url(config, obj)

    # Registered before the page loop, not only after, as an earlier
    # version of this connector did, so table_id exists for any
    # record_schema_change() call that happens mid-loop. Updated again in
    # the finally block below with the final row count either way.
    table_id = control_plane.update_table_registry(target_table, obj.endpoint_path, obj.primary_key)
    object_run_id = control_plane.start_object_run(run_id, obj.object_name, obj.endpoint_path, target_table)

    total_rows_read = total_rows_written = total_rows_skipped = api_calls = 0
    total_rows_inserted = total_rows_updated = total_rows_unchanged = 0
    try:
        # batch_size is optional with no default -- blank deliberately means
        # "no cap" rather than silently falling back to some hardcoded
        # number nobody chose. force_full_refresh=true also means unlimited,
        # regardless of whether batch_size happens to be set, matching its
        # own documented promise to "refetch everything." Safe to leave
        # unbounded in either case now that BearerTokenHolder handles token
        # expiry reactively -- an arbitrarily long run no longer risks
        # failing partway through on an expired token.
        batch_size_is_set = config.batch_size.strip() != ""
        force_full_refresh = config.as_bool("force_full_refresh")

        if not batch_size_is_set or force_full_refresh:
            effective_batch_size = UNLIMITED_BATCH_SIZE
            if not batch_size_is_set:
                print(f"[{obj.object_name}] batch_size not set -- no limit for this run.")
            else:
                print(f"[{obj.object_name}] force_full_refresh=true: batch_size={config.batch_size} "
                      f"is ignored for this run -- fetching every record.")
        else:
            effective_batch_size = config.as_int("batch_size")

        pages = iter_object_pages(
            base_url, token_holder, obj, cursor, config.as_int("page_size"), effective_batch_size,
        )
        for page in pages:
            api_calls = page.page_number
            result = ingest_engine.write_page(obj, page.records, target_table)

            total_rows_read += result.rows_read
            total_rows_written += result.rows_written
            total_rows_skipped += result.rows_skipped
            total_rows_inserted += result.rows_inserted
            total_rows_updated += result.rows_updated
            total_rows_unchanged += result.rows_unchanged

            if result.schema_evolved:
                control_plane.log_event(
                    run_id, "schema_change_detected", "INFO",
                    f"Schema evolved while writing '{obj.object_name}' page {page.page_number}",
                    {"object": obj.object_name, "page": page.page_number, "table": target_table},
                )
                control_plane.record_schema_change(table_id, "add_column")

            control_plane.record_page_progress(object_run_id, api_calls, total_rows_read, total_rows_written)

            if obj.cursor_field is not None:
                max_cursor = get_max_cursor_value(page.records, obj.cursor_field)
                if max_cursor:
                    control_plane.checkpoint_cursor(obj.object_name, obj.cursor_field, max_cursor)

            if page.is_last_page:
                break
    except Exception as exc:
        control_plane.end_object_run(
            object_run_id, OBJECT_STATUS_FAILED, rows_inserted=total_rows_inserted,
            rows_updated=total_rows_updated, rows_unchanged=total_rows_unchanged, error_message=str(exc),
        )
        control_plane.log_event(
            run_id, "object_sync_failed", "SEVERE",
            f"Sync failed for object '{obj.object_name}' after {api_calls} page(s): {exc}",
            {"object": obj.object_name, "api_calls": api_calls, "rows_written": total_rows_written},
        )
        raise
    else:
        control_plane.end_object_run(
            object_run_id, OBJECT_STATUS_SUCCESS, rows_inserted=total_rows_inserted,
            rows_updated=total_rows_updated, rows_unchanged=total_rows_unchanged,
        )
    finally:
        control_plane.update_table_registry(
            target_table, obj.endpoint_path, obj.primary_key, ingest_engine.get_real_row_count(target_table),
        )

    print(f"[{obj.object_name}] Sync complete: api_calls={api_calls}, rows_read={total_rows_read}, "
          f"rows_written={total_rows_written} (rows_inserted={total_rows_inserted}, "
          f"rows_updated={total_rows_updated}, rows_unchanged={total_rows_unchanged}), "
          f"rows_skipped={total_rows_skipped}")

    return ObjectSyncResult(
        obj.object_name, object_run_id, total_rows_read, total_rows_written, total_rows_skipped,
        total_rows_inserted, total_rows_updated, total_rows_unchanged,
    )


def fetch_all_group_ids(config: JetstreamConfig, token_holder: "BearerTokenHolder") -> list[str]:
    """
    Harvest every real group_id from GET /api/v1/groups, reusing
    GROUPS_OBJECT's own pagination (page[limit]/page[offset], links.next) --
    read-only, no destination writes, no connector_object_run row of its
    own (this is plumbing for nodes, not a synced object itself). Used by
    sync_nodes_object() to drive one paginated nodes fetch per group, since
    GET /api/v1/nodes has no "list all nodes" mode -- filter[group_id] is
    required on every call.

    Deliberately does NOT depend on "groups" being selected in this run's
    sync_objects, and does NOT read the evisort_groups destination table --
    nodes only needs read access to the groups endpoint itself, not a
    groups sync having ever run. Always fetches every group (no batch_size
    cap here; that only applies to nodes' own record count, not this
    internal discovery step) -- there is no evidence a tenant would ever
    have enough groups for this to be a real cost, unlike documents.
    """
    base_url = resolve_api_base_url(config, GROUPS_OBJECT)
    group_ids: list[str] = []
    pages = iter_object_pages(
        base_url, token_holder, GROUPS_OBJECT, None, config.as_int("page_size"), UNLIMITED_BATCH_SIZE,
    )
    for page in pages:
        for record in page.records:
            group_id = record.get("id")
            if group_id:
                group_ids.append(group_id)
        if page.is_last_page:
            break
    print(f"[nodes] Discovered {len(group_ids)} real group(s) to drive nodes fetches for.")
    return group_ids


def sync_nodes_object(
    config: JetstreamConfig,
    run_id: str,
    token_holder: "BearerTokenHolder",
    control_plane: ControlPlaneManager,
    ingest_engine: IngestEngine,
    obj: EvisortObject,
) -> ObjectSyncResult:
    """
    Sync the "nodes" object end to end. Structurally like sync_object(),
    except nodes has no single "list all nodes" endpoint -- GET
    /api/v1/nodes requires filter[group_id] on every call, so this
    internally discovers every group_id (fetch_all_group_ids(), above) and
    drives one paginated nodes fetch per group, accumulating into ONE
    connector_object_run row for "nodes" overall (matching every other
    object's one-row-per-object-per-run granularity, not one row per
    group).

    batch_size (if set) is a TOTAL cap across every group combined, matching
    its documented "per object per run" contract -- not reset per group.
    Once the running total reaches the cap, remaining groups are skipped
    entirely rather than partially fetched.

    Raises:
        Whatever iter_object_pages()/write_page() raise, after recording
        this object's run as failed and finalizing its table registry row.
    """
    target_table = resolve_target_table(config, obj)
    base_url = resolve_api_base_url(config, obj)

    table_id = control_plane.update_table_registry(target_table, obj.endpoint_path, obj.primary_key)
    object_run_id = control_plane.start_object_run(run_id, obj.object_name, obj.endpoint_path, target_table)

    total_rows_read = total_rows_written = total_rows_skipped = api_calls = 0
    total_rows_inserted = total_rows_updated = total_rows_unchanged = 0
    try:
        batch_size_is_set = config.batch_size.strip() != ""
        force_full_refresh = config.as_bool("force_full_refresh")
        if not batch_size_is_set or force_full_refresh:
            overall_batch_size = UNLIMITED_BATCH_SIZE
        else:
            overall_batch_size = config.as_int("batch_size")

        group_ids = fetch_all_group_ids(config, token_holder)

        for group_id in group_ids:
            remaining_capacity = overall_batch_size - total_rows_written
            if remaining_capacity <= 0:
                print(f"[nodes] batch_size={config.batch_size} reached; skipping remaining group(s).")
                break

            pages = iter_object_pages(
                base_url, token_holder, obj, None, config.as_int("page_size"), remaining_capacity,
                extra_params={"group_id": group_id},
            )
            for page in pages:
                api_calls += 1
                result = ingest_engine.write_page(obj, page.records, target_table)

                total_rows_read += result.rows_read
                total_rows_written += result.rows_written
                total_rows_skipped += result.rows_skipped
                total_rows_inserted += result.rows_inserted
                total_rows_updated += result.rows_updated
                total_rows_unchanged += result.rows_unchanged

                if result.schema_evolved:
                    control_plane.log_event(
                        run_id, "schema_change_detected", "INFO",
                        f"Schema evolved while writing 'nodes' page {api_calls} (group_id={group_id})",
                        {"object": "nodes", "page": api_calls, "table": target_table, "group_id": group_id},
                    )
                    control_plane.record_schema_change(table_id, "add_column")

                control_plane.record_page_progress(object_run_id, api_calls, total_rows_read, total_rows_written)

                if page.is_last_page:
                    break
    except Exception as exc:
        control_plane.end_object_run(
            object_run_id, OBJECT_STATUS_FAILED, rows_inserted=total_rows_inserted,
            rows_updated=total_rows_updated, rows_unchanged=total_rows_unchanged, error_message=str(exc),
        )
        control_plane.log_event(
            run_id, "object_sync_failed", "SEVERE",
            f"Sync failed for object 'nodes' after {api_calls} page(s): {exc}",
            {"object": "nodes", "api_calls": api_calls, "rows_written": total_rows_written},
        )
        raise
    else:
        control_plane.end_object_run(
            object_run_id, OBJECT_STATUS_SUCCESS, rows_inserted=total_rows_inserted,
            rows_updated=total_rows_updated, rows_unchanged=total_rows_unchanged,
        )
    finally:
        control_plane.update_table_registry(
            target_table, obj.endpoint_path, obj.primary_key, ingest_engine.get_real_row_count(target_table),
        )

    print(f"[nodes] Sync complete: api_calls={api_calls}, rows_read={total_rows_read}, "
          f"rows_written={total_rows_written} (rows_inserted={total_rows_inserted}, "
          f"rows_updated={total_rows_updated}, rows_unchanged={total_rows_unchanged}), "
          f"rows_skipped={total_rows_skipped}")

    return ObjectSyncResult(
        "nodes", object_run_id, total_rows_read, total_rows_written, total_rows_skipped,
        total_rows_inserted, total_rows_updated, total_rows_unchanged,
    )


def fetch_all_ticket_ids(config: JetstreamConfig, token_holder: "BearerTokenHolder") -> list[str]:
    """
    Harvest every real ticket id via tickets' own pagination -- read-only,
    no destination writes, no connector_object_run row of its own (this is
    plumbing for activities, not a synced object itself). Used by
    sync_activities_object() to drive one activities fetch per ticket,
    since GET /contracts/activities has no "list all activities" mode --
    entityId+entityType are required on every call.

    Deliberately does NOT depend on "tickets" being selected in this run's
    sync_objects, and does NOT read the evisort_tickets destination table --
    activities only needs read access to the tickets endpoint itself, not a
    tickets sync having ever run, same reasoning as fetch_all_group_ids().
    Always fetches every ticket (no batch_size cap here; that only applies
    to activities' own record count, not this internal discovery step).
    """
    base_url = resolve_api_base_url(config, TICKETS_OBJECT)
    ticket_ids: list[str] = []
    pages = iter_object_pages(
        base_url, token_holder, TICKETS_OBJECT, None, config.as_int("page_size"), UNLIMITED_BATCH_SIZE,
    )
    for page in pages:
        for record in page.records:
            ticket_id = record.get("id")
            if ticket_id:
                ticket_ids.append(ticket_id)
        if page.is_last_page:
            break
    print(f"[activities] Discovered {len(ticket_ids)} real ticket(s) to drive activities fetches for.")
    return ticket_ids


def sync_activities_object(
    config: JetstreamConfig,
    run_id: str,
    token_holder: "BearerTokenHolder",
    control_plane: ControlPlaneManager,
    ingest_engine: IngestEngine,
    obj: EvisortObject,
) -> ObjectSyncResult:
    """
    Sync the "activities" object end to end, scoped to ticket-activities
    only -- see the long comment above ACTIVITIES_OBJECT in
    evisort_objects.py for why. Structurally like sync_nodes_object():
    internally discovers every real ticket id (fetch_all_ticket_ids(),
    above), drives one activities fetch per ticket (no pagination exists
    for this endpoint at all -- one request per ticket returns everything
    for that ticket), and accumulates into ONE connector_object_run row for
    "activities" overall (matching every other object's one-row-per-
    object-per-run granularity, not one row per ticket).

    Injects each page's driving ticket_id into every raw record as
    "_entity_id" before flatten_activity() runs, since the real API
    response never echoes back which entity an activity belongs to.

    batch_size (if set) is a TOTAL cap across every ticket combined,
    matching its documented "per object per run" contract -- not reset per
    ticket, same as sync_nodes_object().

    Raises:
        Whatever iter_object_pages()/write_page() raise, after recording
        this object's run as failed and finalizing its table registry row.
    """
    target_table = resolve_target_table(config, obj)
    base_url = resolve_api_base_url(config, obj)

    table_id = control_plane.update_table_registry(target_table, obj.endpoint_path, obj.primary_key)
    object_run_id = control_plane.start_object_run(run_id, obj.object_name, obj.endpoint_path, target_table)

    total_rows_read = total_rows_written = total_rows_skipped = api_calls = 0
    total_rows_inserted = total_rows_updated = total_rows_unchanged = 0
    try:
        batch_size_is_set = config.batch_size.strip() != ""
        force_full_refresh = config.as_bool("force_full_refresh")
        if not batch_size_is_set or force_full_refresh:
            overall_batch_size = UNLIMITED_BATCH_SIZE
        else:
            overall_batch_size = config.as_int("batch_size")

        ticket_ids = fetch_all_ticket_ids(config, token_holder)

        for ticket_id in ticket_ids:
            remaining_capacity = overall_batch_size - total_rows_written
            if remaining_capacity <= 0:
                print(f"[activities] batch_size={config.batch_size} reached; skipping remaining ticket(s).")
                break

            pages = iter_object_pages(
                base_url, token_holder, obj, None, config.as_int("page_size"), remaining_capacity,
                extra_params={"entity_id": ticket_id},
            )
            for page in pages:
                api_calls += 1
                # The real API response never includes which entity an
                # activity belongs to -- inject it here so flatten_activity()
                # has something to read.
                for record in page.records:
                    record["_entity_id"] = ticket_id

                result = ingest_engine.write_page(obj, page.records, target_table)

                total_rows_read += result.rows_read
                total_rows_written += result.rows_written
                total_rows_skipped += result.rows_skipped
                total_rows_inserted += result.rows_inserted
                total_rows_updated += result.rows_updated
                total_rows_unchanged += result.rows_unchanged

                if result.schema_evolved:
                    control_plane.log_event(
                        run_id, "schema_change_detected", "INFO",
                        f"Schema evolved while writing 'activities' page {api_calls} (ticket_id={ticket_id})",
                        {"object": "activities", "page": api_calls, "table": target_table, "ticket_id": ticket_id},
                    )
                    control_plane.record_schema_change(table_id, "add_column")

                control_plane.record_page_progress(object_run_id, api_calls, total_rows_read, total_rows_written)

                if page.is_last_page:
                    break
    except Exception as exc:
        control_plane.end_object_run(
            object_run_id, OBJECT_STATUS_FAILED, rows_inserted=total_rows_inserted,
            rows_updated=total_rows_updated, rows_unchanged=total_rows_unchanged, error_message=str(exc),
        )
        control_plane.log_event(
            run_id, "object_sync_failed", "SEVERE",
            f"Sync failed for object 'activities' after {api_calls} page(s): {exc}",
            {"object": "activities", "api_calls": api_calls, "rows_written": total_rows_written},
        )
        raise
    else:
        control_plane.end_object_run(
            object_run_id, OBJECT_STATUS_SUCCESS, rows_inserted=total_rows_inserted,
            rows_updated=total_rows_updated, rows_unchanged=total_rows_unchanged,
        )
    finally:
        control_plane.update_table_registry(
            target_table, obj.endpoint_path, obj.primary_key, ingest_engine.get_real_row_count(target_table),
        )

    print(f"[activities] Sync complete: api_calls={api_calls}, rows_read={total_rows_read}, "
          f"rows_written={total_rows_written} (rows_inserted={total_rows_inserted}, "
          f"rows_updated={total_rows_updated}, rows_unchanged={total_rows_unchanged}), "
          f"rows_skipped={total_rows_skipped}")

    return ObjectSyncResult(
        "activities", object_run_id, total_rows_read, total_rows_written, total_rows_skipped,
        total_rows_inserted, total_rows_updated, total_rows_unchanged,
    )


def main(spark, dbutils) -> None:
    """Drive the full Jetstream Evisort Connector pipeline for one job execution, across every configured object."""
    run_id = str(uuid.uuid4())
    run_start_time = _dt.datetime.now(_dt.timezone.utc)
    run_succeeded = False
    control_plane = None
    already_handled_failure = False

    try:
        config = run_stage("load_and_validate_config", stage_load_and_validate_config, dbutils)
        # "documents" is resolved per-run from config.columns (no
        # "select everything" default); every other object uses its fixed
        # registry definition as-is.
        objects = [
            resolve_documents_object(config) if name == "documents" else EVISORT_OBJECTS[name]
            for name in config.get_sync_objects_list()
        ]

        print("=" * 70)
        print("Jetstream Evisort Connector run starting")
        print("=" * 70)
        print(f"  run_id: {run_id}")
        print(f"  run_start_time (UTC): {run_start_time.isoformat()}")
        print(f"  sync_objects: {', '.join(o.object_name for o in objects)}")
        print(f"  dry_run: {config.dry_run}  force_full_refresh: {config.force_full_refresh}")
        print("=" * 70)

        control_plane = ControlPlaneManager(spark, config, dbutils)
        run_stage("ensure_control_plane_tables", control_plane.ensure_tables_exist)

        _, job_id, _, job_name = resolve_databricks_context(dbutils, config)
        run_stage(
            "register_instance", control_plane.ensure_registered,
            job_name, job_id, build_config_schema(), build_source_config(config),
        )

        if config.as_bool("dry_run"):
            # authenticate/connection_test happen inside this branch, not
            # shared with the real-run path below -- Runtime Validation
            # (real-run path) must run before ANY connection to Evisort,
            # per the Jetstream standard, but dry_run needs a token to preview
            # real records and never creates a connector_run row at all,
            # so that ordering constraint doesn't apply to it the same way.
            token_holder = run_stage("authenticate", stage_authenticate, config, dbutils)
            run_stage("connection_test", stage_connection_test, config, token_holder, objects)
            run_stage("dry_run_preview", stage_dry_run_preview, config, token_holder, control_plane, objects)
            print("dry_run=true: skipping sync and validation. No data or state was written.")
            run_succeeded = True
            return

        run_type = "full_resync" if config.as_bool("force_full_refresh") else "incremental"
        run_id = run_stage("start_run", control_plane.start_run, run_id, run_start_time, run_type, "scheduled")

        # Runtime Validation, per the Jetstream standard's section of the same
        # name -- the first step after connector_run is inserted, before
        # connecting to the source system. Re-checks the REAL persisted
        # source_config against the REAL stored config_schema (not the
        # in-memory config object, which would trivially always pass and
        # defeat the point of catching drift between admission-time
        # validation and what's actually stored now).
        source_config, config_schema = run_stage(
            "get_stored_config_and_schema", control_plane.get_stored_config_and_schema,
        )
        violation = run_stage("runtime_validation", validate_source_config, source_config, config_schema)
        if violation is not None:
            param, rule = violation
            error_message = f"Runtime Validation failed for '{param}': {rule}"
            control_plane.end_run(run_id, RUN_STATUS_FAILED, {"error_message": error_message})
            control_plane.log_event(
                run_id, "config_validation_failed", "SEVERE", error_message,
                {"parameter": param, "rule": rule},
            )
            # Still raising, deliberately -- so the actual Databricks Job
            # Run shows FAILED in the Jobs UI (a plain `return` here would
            # leave it showing SUCCESS despite the sync never running,
            # which is worse for real alerting/monitoring than the
            # problem below). already_handled_failure tells the generic
            # `except Exception` handler this specific failure was already
            # recorded correctly, so it doesn't call end_run()/log_event()
            # a second time and overwrite this specific error_message with
            # its own generic "Run failed" text pointing to job logs, or create
            # a second, redundant connector_log row for one failure.
            already_handled_failure = True
            raise ValueError(error_message)

        token_holder = run_stage("authenticate", stage_authenticate, config, dbutils)
        run_stage("connection_test", stage_connection_test, config, token_holder, objects)
        run_stage("ensure_target_schema", ensure_target_schema_exists, spark, config)

        ingest_engine = IngestEngine(spark, config)
        DEDICATED_SYNC_FUNCTIONS = {"nodes": sync_nodes_object, "activities": sync_activities_object}
        object_results = [
            run_stage(
                f"sync_{obj.object_name}",
                DEDICATED_SYNC_FUNCTIONS.get(obj.object_name, sync_object),
                config, run_id, token_holder, control_plane, ingest_engine, obj,
            )
            for obj in objects
        ]

        aggregate_metrics = {
            "rows_ingested_total": sum(r.rows_read for r in object_results),
            "rows_inserted": sum(r.rows_inserted for r in object_results),
            "rows_updated": sum(r.rows_updated for r in object_results),
            "rows_unchanged": sum(r.rows_unchanged for r in object_results),
            "rows_deleted": 0,
            "tables_synced": sum(1 for r in object_results if r.rows_written > 0),
        }
        run_stage("end_run", control_plane.end_run, run_id, RUN_STATUS_SUCCESS, aggregate_metrics)

        for r in object_results:
            run_stage(f"validate_{r.object_name}", validate_object_run, spark, config, r.object_name, r.object_run_id)

        run_succeeded = True
    except Exception as exc:
        if control_plane and control_plane.connector_instance_id and not already_handled_failure:
            try:
                control_plane.end_run(run_id, RUN_STATUS_FAILED, {"error_message": "Run failed — see job logs."})
                control_plane.log_event(
                    run_id, "run_failed", "SEVERE",
                    f"Run failed: {exc}",
                    {"run_id": run_id},
                )
            except Exception:
                pass
        raise
    finally:
        total_duration = time.perf_counter() - _RUN_START_PERF_COUNTER
        print("\n" + "=" * 70)
        print("Jetstream Evisort Connector run summary")
        print("=" * 70)
        print(f"  run_id: {run_id}")
        print(f"  overall result: {'SUCCESS' if run_succeeded else 'FAILED'}")
        for stage_name, duration in STAGE_TIMINGS.items():
            print(f"    {stage_name}: {duration:.2f}s")
        print(f"  total run duration: {total_duration:.2f}s")
        print("=" * 70)


# COMMAND ----------

# Entry point. spark/dbutils are injected as globals by the Databricks
# notebook runtime whenever this file runs as a Databricks Job task;
# checking for their presence (rather than __name__, which Databricks also
# sets to "__main__") is what actually distinguishes that from running this
# file standalone with `python3 evisort_connector.py` for the logic checks
# in the __main__ block below.
try:
    spark  # noqa: F821
    dbutils  # noqa: F821
except NameError:
    _running_as_databricks_job = False
else:
    _running_as_databricks_job = True
    main(spark, dbutils)  # noqa: F821


# ---------------------------------------------------------------------------
# Standalone debug entry point
# ---------------------------------------------------------------------------
#
# Gated on "not _running_as_databricks_job" in addition to __name__, not
# __name__ alone -- Databricks sets __name__ == "__main__" for a real Job
# task too (see the comment above), so an __name__-only check would run
# this entire suite on every real production run, printing all of the
# below into real job logs on every incremental sync.

if __name__ == "__main__" and not _running_as_databricks_job:
    # _coerce_for_spark_type(): well-formed values convert to the real
    # date/timestamp Spark requires; malformed real-world values (seen
    # from actual Evisort documents) fall back to None instead of raising
    # and taking down the whole page.
    assert _coerce_for_spark_type("2025-04-21", "date") == _dt.date(2025, 4, 21)
    assert _coerce_for_spark_type("2026-05-07T01:00:25+00:00", "timestamp") == _dt.datetime(
        2026, 5, 7, 1, 0, 25, tzinfo=_dt.timezone.utc
    )
    assert _coerce_for_spark_type("2026-05-07T01:00:25Z", "timestamp") == _dt.datetime(
        2026, 5, 7, 1, 0, 25, tzinfo=_dt.timezone.utc
    )
    assert _coerce_for_spark_type(None, "date") is None
    assert _coerce_for_spark_type(None, "timestamp") is None
    assert _coerce_for_spark_type("not-a-date", "date") is None
    assert _coerce_for_spark_type("not-a-timestamp", "timestamp") is None
    assert _coerce_for_spark_type("some text", "string") == "some text"
    already_a_datetime = _dt.datetime.now(_dt.timezone.utc)
    assert _coerce_for_spark_type(already_a_datetime, "timestamp") is already_a_datetime
    print("_coerce_for_spark_type() verified: well-formed ISO strings convert to real date/datetime "
          "objects, malformed values fall back to None instead of crashing, and non-date/timestamp "
          "types and already-correct-type values pass through unchanged.")

    # Verifies generic field/clause lookup, the EvisortObject registry,
    # and the schema/mapping consistency guarantee without pyspark,
    # dbutils, or a live API connection.
    print(f"EVISORT_OBJECTS registered: {list(EVISORT_OBJECTS)}")
    assert EVISORT_OBJECTS["documents"] is DOCUMENTS_OBJECT
    assert DOCUMENTS_OBJECT.primary_key is PRIMARY_KEY
    assert set(name for name, _ in DOCUMENTS_OBJECT.schema_columns) == (
        {n for n, _ in DOCUMENTS_IDENTITY_COLUMNS} | {n for n, _ in DOCUMENTS_SURROGATE_COLUMNS}
    ), "the un-configured registry placeholder should have no selected columns at all"

    # A field ("Title", multi-value "Counterparties"), a clause ("Force Majeure",
    # present only in provisions, never in fields), and a field with a label
    # that was never part of any predetermined catalog: "SWAG/Promotional
    # Sub-Category" is a real field name from an actual Evisort response
    # sample, included here specifically to prove this connector can pull
    # any field a tenant has, not just a curated subset.
    sample_document = {
        "evisortId": 39509935, "docId": "163512", "status": "READY",
        "customModelStatus": {"xrayStatus": "DONE"},
        "name": "sample.pdf", "path": "Documents/Supplier Contracts",
        "uploadDate": "2026-05-06", "uploadDateTime": "2026-05-06T13:00:50+00:00",
        "dateModified": "2026-05-07T01:00:25+00:00",
        "fields": [
            {"name": "Title", "values": [{"value": "Member Service Agreement"}]},
            {"name": "Counterparties", "values": [{"value": "Acme Co"}, {"value": "Beta LLC"}]},
            {"name": "SWAG/Promotional Sub-Category", "values": [{"value": "Branded Apparel"}]},
            {"name": "Empty Custom Field", "values": []},
        ],
        "provisions": [
            {"name": "Force Majeure", "content": [{"text": "Snippet one.", "score": 1.0}, {"text": "Snippet two.", "score": 0.9}]},
        ],
    }

    # extract_field_values(): returns the RAW LIST, not joined; this is the
    # array behavior. A single-value field is still a length-1 list.
    assert extract_field_values(sample_document, "Title") == ["Member Service Agreement"]
    assert extract_field_values(sample_document, "Counterparties") == ["Acme Co", "Beta LLC"]
    assert extract_field_values(sample_document, "SWAG/Promotional Sub-Category") == ["Branded Apparel"]
    assert extract_field_values(sample_document, "Empty Custom Field") is None
    assert extract_field_values(sample_document, "Force Majeure") is None  # it's a clause, not a field
    assert extract_field_values(sample_document, "Nonexistent Field") is None
    print("extract_field_values() verified: single-value and multi-value fields both return raw lists "
          "(never joined into a string), a field outside any fixed catalog resolves the "
          "same way as any other, and absent/empty/clause-only names correctly return None.")

    # extract_clause_text(): unchanged behavior: joined and truncated text, STRING not array.
    assert extract_clause_text(sample_document, "Force Majeure") == "Snippet one. | Snippet two."
    assert extract_clause_text(sample_document, "Title") is None  # it's a field, not a clause
    assert extract_clause_text(sample_document, "Nonexistent Clause") is None
    print("extract_clause_text() verified: clause text is still joined/truncated STRING, "
          "and a field name looked up as a clause correctly finds nothing.")

    # sanitize_column_name(): the mapping from an arbitrary source label to a safe destination column.
    assert sanitize_column_name("Title") == "title"
    assert sanitize_column_name("SWAG/Promotional Sub-Category") == "swag_promotional_sub_category"
    assert sanitize_column_name("# of Renewals") == "of_renewals"
    assert sanitize_column_name("!!!") == ""  # no alphanumeric characters at all

    # build_documents_flatten_fn(): the full per-instance flatten path, with an
    # operator-chosen field set (including the non-catalog custom field) and
    # clause set.
    selected_fields = ("Title", "Counterparties", "SWAG/Promotional Sub-Category")
    selected_clauses = ("Force Majeure",)
    flatten_fn = build_documents_flatten_fn(selected_fields, selected_clauses)
    flattened = flatten_fn(sample_document)
    assert DOCUMENTS_OBJECT.flatten_fn(sample_document) == {
        "evisort_id": 39509935, "date_modified": "2026-05-07T01:00:25+00:00", "file_name": "sample.pdf",
    }, "the un-configured registry placeholder should extract nothing beyond the 3 identity columns"
    assert flattened["evisort_id"] == 39509935
    assert flattened["title"] == ["Member Service Agreement"]
    assert flattened["counterparties"] == ["Acme Co", "Beta LLC"]
    assert flattened["swag_promotional_sub_category"] == ["Branded Apparel"]
    assert flattened["force_majeure"] == "Snippet one. | Snippet two."
    assert isinstance(flattened["counterparties"], list) and isinstance(flattened["force_majeure"], str)

    schema_column_names = {n for n, _ in (
        DOCUMENTS_IDENTITY_COLUMNS
        + tuple((sanitize_column_name(s), "array<string>") for s in selected_fields)
        + tuple((sanitize_column_name(s), "string") for s in selected_clauses)
        + DOCUMENTS_SURROGATE_COLUMNS
    )}
    row_keys = set(flattened) | {"_jetstream_source_object", "_jetstream_synced", "_jetstream_deleted"}
    assert row_keys == schema_column_names, f"Schema/mapping drift: {row_keys ^ schema_column_names}"
    print("\nAll assertions passed — build_documents_flatten_fn() output matches the schema built from "
          "the same field/clause selection exactly: fields as lists, clauses as strings, including a "
          "field that was never part of any fixed catalog.")

    max_cursor = get_max_cursor_value([sample_document], DOCUMENTS_OBJECT.cursor_field)
    print(f"\nmax '{DOCUMENTS_OBJECT.cursor_field}' across sample:", max_cursor)
    assert max_cursor == sample_document["dateModified"]

    print(f"\nresolve_target_table (documents, table_name override): "
          f"{resolve_target_table(JetstreamConfig(target_catalog='c', target_schema='s', table_name='custom_name'), DOCUMENTS_OBJECT)}")

    # --- columns/clauses selection end to end via resolve_documents_object(): no
    # "select everything" default, arbitrary names (including the non-catalog
    # custom field) resolve correctly with the right destination types. ---
    narrow_config = JetstreamConfig(
        target_catalog="c", target_schema="s", table_name="narrow_table",
        columns="Title,SWAG/Promotional Sub-Category", clauses="Force Majeure",
    )
    narrow_object = resolve_documents_object(narrow_config)
    narrow_schema = dict(narrow_object.schema_columns)
    assert set(narrow_schema) == (
        {name for name, _ in DOCUMENTS_IDENTITY_COLUMNS}
        | {"title", "swag_promotional_sub_category", "force_majeure"}
        | {name for name, _ in DOCUMENTS_SURROGATE_COLUMNS}
    ), f"resolve_documents_object() should include only the selected columns plus identity/surrogate: {set(narrow_schema)}"
    assert narrow_schema["title"] == "array<string>"
    assert narrow_schema["swag_promotional_sub_category"] == "array<string>"
    assert narrow_schema["force_majeure"] == "string"
    assert len(narrow_object.schema_columns) == len(DOCUMENTS_IDENTITY_COLUMNS) + 2 + 1 + len(DOCUMENTS_SURROGATE_COLUMNS)
    narrow_flattened = narrow_object.flatten_fn(sample_document)
    assert narrow_flattened["swag_promotional_sub_category"] == ["Branded Apparel"]
    assert narrow_flattened["force_majeure"] == "Snippet one. | Snippet two."
    print(f"\ncolumns/clauses selection verified end to end: resolve_documents_object() with "
          f"columns='{narrow_config.columns}' clauses='{narrow_config.clauses}' (including a field "
          f"outside any fixed catalog) produces exactly {len(narrow_object.schema_columns)} columns, "
          f"fields typed array<string> and the clause typed string, with real extracted values for all.")

    # --- groups and tickets: flatten + schema consistency, same guarantee as documents ---
    sample_group = {
        "type": "groups", "id": "8f14e45f-ceea-467e-bd5c-77d15b8f5e6d",
        "attributes": {
            "client_id": 42, "name": "Vendor Contracts", "note": "Imported from legacy system",
            "nodes_count": 12, "created_at": "2026-01-01T00:00:00Z", "created_by": 7,
            "updated_at": "2026-02-01T00:00:00Z",
        },
        "relationships": {"root_node": {"data": {"type": "nodes", "id": "1a2b3c4d-0000-0000-0000-000000000000"}}},
    }
    flattened_group = flatten_group(sample_group)
    assert flattened_group["group_id"] == sample_group["id"]
    assert flattened_group["name"] == "Vendor Contracts"
    assert flattened_group["root_node_id"] == "1a2b3c4d-0000-0000-0000-000000000000"
    group_columns = {name for name, _ in GROUPS_SCHEMA_COLUMNS}
    group_row_keys = set(flattened_group) | {"_jetstream_source_object", "_jetstream_synced", "_jetstream_deleted"}
    assert group_row_keys == group_columns, f"groups schema/mapping drift: {group_row_keys ^ group_columns}"

    sample_ticket = {
        "id": "b2c3d4e5-0000-0000-0000-000000000001", "name": "MSA Renewal - Acme Co",
        "status": "in_progress", "currentStageName": "Review", "createdDate": "2026-03-01T00:00:00Z",
        "lastActivityDate": "2026-03-05T00:00:00Z", "assignees": [101, 102], "participants": [201],
    }
    flattened_ticket = flatten_ticket(sample_ticket)
    assert flattened_ticket["ticket_id"] == sample_ticket["id"]
    assert flattened_ticket["assignee_ids"] == "101, 102"
    ticket_columns = {name for name, _ in TICKETS_SCHEMA_COLUMNS}
    ticket_row_keys = set(flattened_ticket) | {"_jetstream_source_object", "_jetstream_synced", "_jetstream_deleted"}
    assert ticket_row_keys == ticket_columns, f"tickets schema/mapping drift: {ticket_row_keys ^ ticket_columns}"

    print("\ngroups and tickets: flatten output matches their own SCHEMA_COLUMNS exactly, same as documents.")

    # --- pagination generalization: three distinct real shapes, one shared loop ---
    # documents: top-level `next` URL; modifiedSince is required, so a fresh/full-refresh
    # cursor_value=None must still produce a real value (the epoch fallback), not an omitted param.
    _, doc_first_url, doc_first_params, _ = _documents_build_request("https://api.evisort.com/v1", None, 100, None, {})
    assert doc_first_url == "https://api.evisort.com/v1/documents"
    assert doc_first_params["modifiedSince"] == EPOCH_MODIFIED_SINCE
    doc_marker = _documents_get_next_marker({"next": "https://api.evisort.com/v1/documents?page=2"}, None, None)
    assert doc_marker == "https://api.evisort.com/v1/documents?page=2"
    assert _documents_get_next_marker({"next": None}, None, None) is None

    # groups: nested links.next URL
    groups_marker = _groups_get_next_marker({"links": {"next": "https://api.evisort.com/mes/public/api/v1/groups?page[offset]=20"}}, None, None)
    assert groups_marker == "https://api.evisort.com/mes/public/api/v1/groups?page[offset]=20"
    assert _groups_get_next_marker({"links": {"next": None}}, None, None) is None

    # tickets: no `next` field at all; self-terminate by counting against `count`
    tickets_marker = _tickets_get_next_marker({"data": [{}] * 100, "count": 250}, {"page": 0, "pageSize": 100}, None)
    assert tickets_marker == 1, "tickets should request page 1 next when page 0 returned a full page and more remain"
    assert _tickets_get_next_marker({"data": [{}] * 40, "count": 240}, {"page": 1, "pageSize": 100}, None) is None, (
        "tickets should stop once a short page is returned, even if count implies more (API inconsistency guard)"
    )

    print("Pagination generalization verified: documents (top-level next), groups (nested links.next), "
          "and tickets (no next — self-terminated by count) all resolve correctly through their own "
          "build_request/get_next_marker pair.")

    # --- _jetstream_id/_jetstream_index fallback for objects with no confirmed natural key ---
    _KeylessTestObject = EvisortObject(
        object_name="test_keyless", endpoint_path="/test", list_key="items",
        id_field=None, cursor_field=None, default_table_name="test_keyless",
        schema_columns=(("value", "string"), ("_jetstream_source_object", "string"),
                         ("_jetstream_synced", "timestamp"), ("_jetstream_deleted", "boolean"),
                         (JETSTREAM_ID_COLUMN, "string"), (JETSTREAM_INDEX_COLUMN, "bigint")),
        primary_key=(JETSTREAM_ID_COLUMN,),
        flatten_fn=lambda r: {"value": r["value"]},
        build_request=lambda *a, **k: ("GET", "https://example.invalid/test", None, None),
        get_next_marker=lambda *a, **k: None,
    )
    row_a = {"value": "same content"}
    row_b = {"value": "same content"}
    row_c = {"value": "different content"}
    hash_a = compute_jetstream_id(row_a, _KeylessTestObject)
    hash_b = compute_jetstream_id(row_b, _KeylessTestObject)
    hash_c = compute_jetstream_id(row_c, _KeylessTestObject)
    assert hash_a == hash_b, "identical row content must hash identically (idempotent MERGE)"
    assert hash_a != hash_c, "different row content must hash differently"
    assert len(hash_a) == 64, "SHA-256 hex digest is 64 characters"
    print(f"\n_jetstream_id fallback verified: identical content -> identical hash ({hash_a[:12]}...), "
          f"different content -> different hash ({hash_c[:12]}...).")

    # Config load/validate smoke test with a stub dbutils.
    class _StubWidgets:
        def __init__(self):
            self._v = {}
        def text(self, name, default):
            self._v.setdefault(name, default)
        def get(self, name):
            return self._v[name]

    class _StubDbutils:
        def __init__(self):
            self.widgets = _StubWidgets()

    stub = _StubDbutils()
    stub.widgets.text("batch_size", "500")
    stub.widgets.text("target_catalog", "demo_catalog")
    stub.widgets.text("target_schema", "demo_schema")
    stub.widgets.text("secret_scope", "jetstream/evisort/demo")
    stub.widgets.text("table_name", "evisort_contracts")
    stub.widgets.text("columns", "Title,Contract Type,Counterparties")
    cfg = load_config(stub)
    validate_config(cfg)
    print_config_summary(cfg)
    assert cfg.get_sync_objects_list() == ["documents"]

    # sync_objects validation: an unregistered object name must fail loudly.
    bad_stub = _StubDbutils()
    bad_stub.widgets.text("batch_size", "500")
    bad_stub.widgets.text("target_catalog", "demo_catalog")
    bad_stub.widgets.text("target_schema", "demo_schema")
    bad_stub.widgets.text("secret_scope", "jetstream/evisort/demo")
    bad_stub.widgets.text("sync_objects", "documents,not_a_real_object")
    bad_cfg = load_config(bad_stub)
    try:
        validate_config(bad_cfg)
        print("ERROR: expected a ValueError for an unregistered sync_objects entry, but none was raised.")
    except ValueError as exc:
        print(f"\nCorrectly rejected unregistered sync_objects entry: {exc}")

    # table_name has no default; omitting it while syncing documents must fail loudly, not
    # silently fall back to some hardcoded table name.
    no_table_stub = _StubDbutils()
    no_table_stub.widgets.text("batch_size", "500")
    no_table_stub.widgets.text("target_catalog", "demo_catalog")
    no_table_stub.widgets.text("target_schema", "demo_schema")
    no_table_stub.widgets.text("secret_scope", "jetstream/evisort/demo")
    no_table_stub.widgets.text("columns", "title")
    no_table_cfg = load_config(no_table_stub)
    try:
        validate_config(no_table_cfg)
        print("ERROR: expected a ValueError for a missing table_name, but none was raised.")
    except ValueError as exc:
        print(f"Correctly rejected missing table_name: {exc}")

    # Neither columns nor clauses has a "select everything" default; omitting
    # BOTH while syncing documents must fail loudly, not silently fall back to
    # any hardcoded list.
    no_columns_stub = _StubDbutils()
    no_columns_stub.widgets.text("batch_size", "500")
    no_columns_stub.widgets.text("target_catalog", "demo_catalog")
    no_columns_stub.widgets.text("target_schema", "demo_schema")
    no_columns_stub.widgets.text("secret_scope", "jetstream/evisort/demo")
    no_columns_stub.widgets.text("table_name", "evisort_contracts")
    no_columns_cfg = load_config(no_columns_stub)
    try:
        validate_config(no_columns_cfg)
        print("ERROR: expected a ValueError for missing columns and clauses, but none was raised.")
    except ValueError as exc:
        print(f"Correctly rejected missing columns and clauses: {exc}")

    # clauses alone (no columns) is sufficient: the requirement is "at least one".
    clauses_only_stub = _StubDbutils()
    clauses_only_stub.widgets.text("batch_size", "500")
    clauses_only_stub.widgets.text("target_catalog", "demo_catalog")
    clauses_only_stub.widgets.text("target_schema", "demo_schema")
    clauses_only_stub.widgets.text("secret_scope", "jetstream/evisort/demo")
    clauses_only_stub.widgets.text("table_name", "evisort_contracts")
    clauses_only_stub.widgets.text("clauses", "Force Majeure")
    clauses_only_cfg = load_config(clauses_only_stub)
    validate_config(clauses_only_cfg)
    print("Correctly allowed clauses alone with no columns set — only one of the two is required.")

    # No fixed catalog to validate names against: any exact Evisort label is
    # accepted. What IS rejected: two entries that sanitize to the same
    # destination column name, whether both are in 'columns', both in
    # 'clauses', or split across the two (a silent-overwrite / type-conflict risk).
    colliding_columns_stub = _StubDbutils()
    colliding_columns_stub.widgets.text("batch_size", "500")
    colliding_columns_stub.widgets.text("target_catalog", "demo_catalog")
    colliding_columns_stub.widgets.text("target_schema", "demo_schema")
    colliding_columns_stub.widgets.text("secret_scope", "jetstream/evisort/demo")
    colliding_columns_stub.widgets.text("table_name", "evisort_contracts")
    colliding_columns_stub.widgets.text("columns", "Effective Date,Effective-Date")
    colliding_columns_cfg = load_config(colliding_columns_stub)
    try:
        validate_config(colliding_columns_cfg)
        print("ERROR: expected a ValueError for colliding sanitized column names, but none was raised.")
    except ValueError as exc:
        print(f"Correctly rejected colliding columns: {exc}")

    # A field and a clause with the IDENTICAL exact name is deliberately
    # allowed now: resolve_column_destinations() disambiguates the clause
    # side with CLAUSE_FIELD_COLLISION_SUFFIX, so both can coexist in the
    # same table instead of the config being rejected.
    same_name_stub = _StubDbutils()
    same_name_stub.widgets.text("batch_size", "500")
    same_name_stub.widgets.text("target_catalog", "demo_catalog")
    same_name_stub.widgets.text("target_schema", "demo_schema")
    same_name_stub.widgets.text("secret_scope", "jetstream/evisort/demo")
    same_name_stub.widgets.text("table_name", "evisort_contracts")
    same_name_stub.widgets.text("columns", "Governing Law")
    same_name_stub.widgets.text("clauses", "Governing Law")
    same_name_cfg = load_config(same_name_stub)
    try:
        validate_config(same_name_cfg)
        print("Correctly allowed 'Governing Law' as both a field and a clause (identical exact name).")
    except ValueError as exc:
        print(f"ERROR: expected 'Governing Law' in both columns and clauses to be allowed, but got: {exc}")
    same_name_obj = resolve_documents_object(same_name_cfg)
    same_name_column_names = [name for name, _ in same_name_obj.schema_columns]
    if "governing_law" in same_name_column_names and "governing_law_clause_text" in same_name_column_names:
        print(f"Correctly produced two distinct columns: 'governing_law' and 'governing_law_clause_text'.")
    else:
        print(f"ERROR: expected both 'governing_law' and 'governing_law_clause_text' in schema_columns, "
              f"got: {same_name_column_names}")
    same_name_row = same_name_obj.flatten_fn({
        "evisortId": 1,
        "fields": [{"name": "Governing Law", "values": [{"value": "India"}]}],
        "provisions": [{"name": "Governing Law", "content": [{"text": "Governed by the laws of India."}]}],
    })
    if same_name_row.get("governing_law") == ["India"] and same_name_row.get("governing_law_clause_text") == "Governed by the laws of India.":
        print("Correctly flattened both the field value and the clause text into their distinct columns.")
    else:
        print(f"ERROR: unexpected flattened row for the field+clause same-name case: {same_name_row}")

    # A genuine cross-list collision (two DIFFERENT raw names that happen to
    # sanitize to the same destination) must still fail. This is distinct
    # from the identical-name case above, which is now allowed.
    cross_collision_stub = _StubDbutils()
    cross_collision_stub.widgets.text("batch_size", "500")
    cross_collision_stub.widgets.text("target_catalog", "demo_catalog")
    cross_collision_stub.widgets.text("target_schema", "demo_schema")
    cross_collision_stub.widgets.text("secret_scope", "jetstream/evisort/demo")
    cross_collision_stub.widgets.text("table_name", "evisort_contracts")
    cross_collision_stub.widgets.text("columns", "Force Majeure")
    cross_collision_stub.widgets.text("clauses", "Force-Majeure")
    cross_collision_cfg = load_config(cross_collision_stub)
    try:
        validate_config(cross_collision_cfg)
        print("ERROR: expected a ValueError for a genuine columns/clauses cross-collision, but none was raised.")
    except ValueError as exc:
        print(f"Correctly rejected genuine columns/clauses cross-collision: {exc}")

    # A column entry with no alphanumeric characters at all sanitizes to an
    # empty destination name and must also fail loudly.
    empty_sanitize_stub = _StubDbutils()
    empty_sanitize_stub.widgets.text("batch_size", "500")
    empty_sanitize_stub.widgets.text("target_catalog", "demo_catalog")
    empty_sanitize_stub.widgets.text("target_schema", "demo_schema")
    empty_sanitize_stub.widgets.text("secret_scope", "jetstream/evisort/demo")
    empty_sanitize_stub.widgets.text("table_name", "evisort_contracts")
    empty_sanitize_stub.widgets.text("columns", "Title,!!!")
    empty_sanitize_cfg = load_config(empty_sanitize_stub)
    try:
        validate_config(empty_sanitize_cfg)
        print("ERROR: expected a ValueError for an empty-after-sanitize column, but none was raised.")
    except ValueError as exc:
        print(f"Correctly rejected empty-after-sanitize column: {exc}")

    # sync_objects=groups only (no documents) needs neither table_name nor columns/clauses.
    groups_only_stub = _StubDbutils()
    groups_only_stub.widgets.text("batch_size", "500")
    groups_only_stub.widgets.text("target_catalog", "demo_catalog")
    groups_only_stub.widgets.text("target_schema", "demo_schema")
    groups_only_stub.widgets.text("secret_scope", "jetstream/evisort/demo")
    groups_only_stub.widgets.text("sync_objects", "groups")
    groups_only_cfg = load_config(groups_only_stub)
    validate_config(groups_only_cfg)
    print("Correctly allowed sync_objects='groups' with no table_name/columns set — those only apply to documents.")

    # control_plane_schema_override: default (unset) behavior unchanged.
    default_schema_stub = _StubDbutils()
    default_schema_stub.widgets.text("batch_size", "500")
    default_schema_stub.widgets.text("target_catalog", "demo_catalog")
    default_schema_stub.widgets.text("target_schema", "demo_schema")
    default_schema_stub.widgets.text("secret_scope", "jetstream/evisort/demo")
    default_schema_stub.widgets.text("sync_objects", "groups")
    default_schema_stub.widgets.text("control_plane_catalog", "udl_dataflow_metadata_dev")
    default_schema_cfg = load_config(default_schema_stub)
    validate_config(default_schema_cfg)
    assert default_schema_cfg.get_control_plane_schema() == "udl_dataflow_metadata_dev.jetstream"
    print("Correctly defaulted to the shared 'jetstream' schema when control_plane_schema_override is unset.")

    # control_plane_schema_override: a valid jetstream_<type> override is accepted and used.
    override_stub = _StubDbutils()
    override_stub.widgets.text("batch_size", "500")
    override_stub.widgets.text("target_catalog", "demo_catalog")
    override_stub.widgets.text("target_schema", "demo_schema")
    override_stub.widgets.text("secret_scope", "jetstream/evisort/demo")
    override_stub.widgets.text("sync_objects", "groups")
    override_stub.widgets.text("control_plane_catalog", "udl_dataflow_metadata_dev")
    override_stub.widgets.text("control_plane_schema_override", "jetstream_evisort")
    override_cfg = load_config(override_stub)
    validate_config(override_cfg)
    assert override_cfg.get_control_plane_schema() == "udl_dataflow_metadata_dev.jetstream_evisort"
    print("Correctly accepted 'jetstream_evisort' as control_plane_schema_override and routed to it.")

    # control_plane_schema_override: the redundant literal "jetstream" is rejected.
    redundant_override_stub = _StubDbutils()
    redundant_override_stub.widgets.text("batch_size", "500")
    redundant_override_stub.widgets.text("target_catalog", "demo_catalog")
    redundant_override_stub.widgets.text("target_schema", "demo_schema")
    redundant_override_stub.widgets.text("secret_scope", "jetstream/evisort/demo")
    redundant_override_stub.widgets.text("sync_objects", "groups")
    redundant_override_stub.widgets.text("control_plane_schema_override", "jetstream")
    redundant_override_cfg = load_config(redundant_override_stub)
    try:
        validate_config(redundant_override_cfg)
        print("ERROR: expected a ValueError for control_plane_schema_override='jetstream', but none was raised.")
    except ValueError as exc:
        print(f"Correctly rejected redundant control_plane_schema_override='jetstream': {exc}")

    # control_plane_schema_override: anything not starting with jetstream_ is rejected.
    bad_override_stub = _StubDbutils()
    bad_override_stub.widgets.text("batch_size", "500")
    bad_override_stub.widgets.text("target_catalog", "demo_catalog")
    bad_override_stub.widgets.text("target_schema", "demo_schema")
    bad_override_stub.widgets.text("secret_scope", "jetstream/evisort/demo")
    bad_override_stub.widgets.text("sync_objects", "groups")
    bad_override_stub.widgets.text("control_plane_schema_override", "some_other_schema")
    bad_override_cfg = load_config(bad_override_stub)
    try:
        validate_config(bad_override_cfg)
        print("ERROR: expected a ValueError for a control_plane_schema_override not starting with 'jetstream_', but none was raised.")
    except ValueError as exc:
        print(f"Correctly rejected control_plane_schema_override not starting with 'jetstream_': {exc}")

    # batch_size is optional with no default -- omitting it entirely must NOT
    # fail, and must stay genuinely blank (not silently become some
    # hardcoded number like the old default of 500).
    no_batch_size_stub = _StubDbutils()
    no_batch_size_stub.widgets.text("target_catalog", "demo_catalog")
    no_batch_size_stub.widgets.text("target_schema", "demo_schema")
    no_batch_size_stub.widgets.text("secret_scope", "jetstream/evisort/demo")
    no_batch_size_stub.widgets.text("table_name", "evisort_contracts")
    no_batch_size_stub.widgets.text("columns", "Title")
    no_batch_size_cfg = load_config(no_batch_size_stub)
    validate_config(no_batch_size_cfg)
    assert no_batch_size_cfg.batch_size == "", repr(no_batch_size_cfg.batch_size)
    try:
        no_batch_size_cfg.as_int("batch_size")
        print("ERROR: expected as_int('batch_size') to fail on a blank value, but it didn't.")
    except ValueError:
        pass  # expected -- callers must check config.batch_size.strip() before as_int(), as sync_object() does
    print("Correctly left batch_size blank when omitted -- no hardcoded default, means 'no limit' at run time.")

    # force_full_refresh=true must bypass batch_size entirely, matching its own
    # documented promise to "refetch everything."
    ffr_stub = _StubDbutils()
    ffr_stub.widgets.text("target_catalog", "demo_catalog")
    ffr_stub.widgets.text("target_schema", "demo_schema")
    ffr_stub.widgets.text("secret_scope", "jetstream/evisort/demo")
    ffr_stub.widgets.text("table_name", "evisort_contracts")
    ffr_stub.widgets.text("columns", "Title")
    ffr_stub.widgets.text("batch_size", "10")
    ffr_stub.widgets.text("force_full_refresh", "true")
    ffr_cfg = load_config(ffr_stub)
    validate_config(ffr_cfg)
    assert ffr_cfg.as_bool("force_full_refresh") is True
    assert ffr_cfg.as_int("batch_size") == 10  # the configured value is preserved on the config itself...
    # ...but sync_object() (not exercised here: needs real Spark/HTTP, see the
    # note below) is where force_full_refresh actually swaps in
    # UNLIMITED_BATCH_SIZE instead of this value for the run. Confirm the
    # sentinel itself is sane at least:
    assert UNLIMITED_BATCH_SIZE > 10**8, "UNLIMITED_BATCH_SIZE should be a large plain int, not a small or float value"
    print(f"Correctly configured force_full_refresh=true with batch_size=10 — "
          f"sync_object() overrides this to UNLIMITED_BATCH_SIZE={UNLIMITED_BATCH_SIZE} at run time.")

    # validate_source_config() -- pure Python, no Spark/dbutils dependency,
    # so it belongs here rather than only in one-off dev-time stub scripts.
    real_schema = build_config_schema()

    valid_source_config = {
        "sync_objects": "documents,groups,tickets", "table_name": "contracts",
        "columns": "Title", "secret_scope": "evisort_scope", "page_size": "100",
        "on_error": "fail", "environment": "dev", "dry_run": "false", "batch_size": "",
    }
    assert validate_source_config(valid_source_config, real_schema) is None
    print("validate_source_config(): correctly accepted a valid, real-shaped source_config.")

    missing_required = {"sync_objects": "documents", "table_name": "contracts", "columns": "Title"}
    violation = validate_source_config(missing_required, real_schema)
    assert violation is not None and violation[0] == "secret_scope", violation
    print(f"validate_source_config(): correctly rejected missing required 'secret_scope': {violation}")

    bad_type = {"secret_scope": "x", "page_size": "not-a-number"}
    violation2 = validate_source_config(bad_type, real_schema)
    assert violation2 is not None and violation2[0] == "page_size", violation2
    print(f"validate_source_config(): correctly rejected a non-numeric page_size: {violation2}")

    bad_enum = {"secret_scope": "x", "on_error": "ignore"}
    violation3 = validate_source_config(bad_enum, real_schema)
    assert violation3 is not None and violation3[0] == "on_error", violation3
    print(f"validate_source_config(): correctly rejected an invalid on_error enum value: {violation3}")

    # The real depends_on scenario this connector actually needs: sync_objects
    # is multi-value, so "contains" (list membership), not "value" (exact
    # equality), is what table_name's depends_on must use -- a multi-object
    # sync_objects with table_name correctly present must NOT be flagged.
    multi_object_valid = {
        "secret_scope": "x", "sync_objects": "documents,groups,tickets,nodes", "table_name": "contracts",
    }
    assert validate_source_config(multi_object_valid, real_schema) is None
    print("validate_source_config(): correctly allowed table_name with a multi-object sync_objects "
          "that includes 'documents' (depends_on 'contains', not 'value').")

    multi_object_missing = {"secret_scope": "x", "sync_objects": "documents,groups,tickets"}
    violation4 = validate_source_config(multi_object_missing, real_schema)
    assert violation4 is not None and violation4[0] == "table_name", violation4
    print(f"validate_source_config(): correctly caught missing table_name in a multi-object "
          f"sync_objects that includes 'documents': {violation4}")

    print()
    print("NOTE: ControlPlaneManager (control_plane.py) — start_run()'s MERGE-based")
    print("idempotency handling, _refresh_instance()'s source_config/destination")
    print("refresh, and get_stored_config_and_schema() — are NOT exercised by this")
    print("local test suite, same as IngestEngine.write_page() already wasn't. All")
    print("require a real Spark session to verify; this suite only covers pure-Python")
    print("logic (config parsing/validation, field/clause extraction, pagination,")
    print("and validate_source_config()'s own four-check logic, which IS exercised")
    print("above). Verify the Spark-dependent pieces against a real cluster before")
    print("relying on them. The force_full_refresh -> UNLIMITED_BATCH_SIZE override")
    print("inside sync_object() is likewise unexercised here for the same reason.")

