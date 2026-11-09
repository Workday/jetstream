# Jetstream Evisort Connector

A Databricks-native connector for Evisort. Runs as a
Databricks Job, authenticates to the Evisort platform, and syncs whichever
Evisort **objects** are named in the `sync_objects` parameter -- each
streamed and MERGE-written into its own Iceberg table in Unity Catalog.
Seven object types are built and available to select:
`documents`, `groups`, `tickets`, `nodes`, `departments`, `workflows`,
`activities`. A given deployment typically activates only the ones it
needs -- e.g. `sync_objects=documents` alone syncs just the `documents`
object into one table (`table_name`, e.g. `contracts`); setting
`sync_objects=documents,groups,tickets` would sync three tables in that
same run. Which objects actually run is a configuration choice per
deployment, not a fixed set the connector always syncs.

This connector is generic across Evisort *objects*, not across arbitrary
APIs: authentication and the general idea of a paginated REST endpoint are
shared plumbing, while what varies per object -- its host/endpoint, its own
pagination mechanics, field mapping, schema, and natural key -- is declared
once as data via an `EvisortObject`. Real Evisort APIs turn out to paginate
several different ways, and two objects (`nodes`, `activities`) require
discovering IDs from a different object before they can be queried at all;
the object model makes each of those a per-object detail, not a
connector-wide assumption.

## Runtime requirements

- Databricks Runtime 18.x or higher
- Python 3.12+
- Unity Catalog enabled workspace
- Iceberg table format version 3 or higher
- A Databricks secret scope holding the Evisort API key

## Repository structure

```
jetstream-evisort-connector/
|-- evisort_connector.py   # The Databricks Job task. Config, API client, auth, ingest, validation, orchestration.
|-- evisort_objects.py     # The Evisort object catalog: EvisortObject registrations (field maps, schema, pagination).
|-- control_plane.py       # ControlPlaneManager -- not Evisort-specific, reused shape for any connector type.
|-- parameters.yaml        # Parameter specification -- compiled into connector_manifest.config_schema.
|-- README.md
```

Both `control_plane.py` and `evisort_objects.py` are brought into
`evisort_connector.py`'s namespace via `%run ./control_plane` /
`%run ./evisort_objects` -- a Databricks notebook-only mechanism, inert as a
plain comment outside Databricks. For standalone testing
(`python3 evisort_connector.py`), a fallback right after those two `%run`
lines detects the absence of `dbutils` and imports the same two files
directly instead.

`evisort_connector.py` calls `main(spark, dbutils)` automatically whenever
`spark`/`dbutils` are present as globals -- true when it runs as a
Databricks Job task, false when run standalone, which instead exercises its
`__main__` block (see "Local development" below).

## Authentication

Two distinct credentials, not one:

- **The Evisort API key** (`secret_scope`/`secret_key_name`, a long-lived
  value provisioned through Evisort's Admin Console) never expires and is
  never refreshed mid-run.
- **The bearer token** is a short-lived JWT, obtained by exchanging the API
  key via `POST /auth/token`. This is what can expire mid-run.

`BearerTokenHolder` wraps the current token in a mutable object (not a plain
string) so a refresh triggered deep in a page fetch is immediately visible
to every other call using the same holder. On an HTTP 401, `fetch_object_page()`
refreshes the token in place and retries once with the new one; a second 401
after that is treated as a real auth failure, not another expiry. This is
reactive rather than proactive (no "refresh every N pages" schedule) because
neither Evisort's public API documentation nor any spec reviewed while
building this connector states a JWT expiry duration -- refreshing exactly
when a 401 actually happens works regardless of what the real duration is
now or becomes later. Every registered object's calls reuse the one token
obtained against `api_base_url`, except `groups`/`nodes` (a different host
under Managed Entities, same token).

## The Evisort object model

```python
@dataclass(frozen=True)
class EvisortObject:
    object_name: str            # "documents" -- also the sync_objects value and connector_state object_name
    endpoint_path: str          # "/documents"
    list_key: str | None        # "documents" -- JSON key holding the record array; None if the response IS the array
    id_field: str | None        # "evisortId", or None if no natural key is confirmed
    cursor_field: str | None    # "dateModified", or None if this object has no incremental filter
    default_table_name: str     # "evisort_contracts"
    schema_columns: tuple[tuple[str, str], ...]   # this object's own schema declaration
    primary_key: tuple[str, ...]
    flatten_fn: Callable[[dict], dict]
    build_request: Callable     # (api_base_url, marker, page_size, cursor_value, extra_params) -> (method, url, params, body)
    get_next_marker: Callable   # (response_json, request_params, request_body) -> next marker, or None if last page
    api_base_url_override: str | None = None   # for objects on a different host than config.api_base_url
```

`EVISORT_OBJECTS: dict[str, EvisortObject]` is the registry; `sync_objects`
(a config parameter, default `"documents"`) selects which registered objects
a given job instance syncs. Everything downstream -- `iter_object_pages()`,
control-plane wiring, the write path in `IngestEngine` -- takes an
`EvisortObject` as a parameter rather than hardcoding one endpoint's shape.

### Pagination shapes

Six distinct real pagination shapes across the registered objects, each
confirmed against Evisort's actual OpenAPI specs or a real sampled response,
not assumed:

| Object | Pagination | Next page is... |
| --- | --- | --- |
| `documents` | `pageSize` + required `modifiedSince` | a full URL at top-level `next` |
| `groups` / `nodes` | `page[limit]`/`page[offset]` | a full URL nested at `links.next` |
| `tickets` | count-based | no `next` field at all -- self-terminates |
| `departments` | `page[number]`/`page[size]` | a full URL nested at `links.next` |
| `workflows` / `activities` | none | one request returns everything; `get_next_marker` always returns `None` |

### Dependent objects: `nodes` and `activities`

Two objects can't be listed directly -- their real APIs require an ID from
a *different* object on every call:

- **`nodes`**: `GET /api/v1/nodes` requires `filter[group_id]`. There is no
  "list all nodes" mode.
- **`activities`**: `GET /contracts/activities` requires both `entityId`
  and `entityType`. Of the three documented entity types
  (`risk`/`ticket`/`ticket judgment`), only `ticket` IDs are enumerable via
  anything this connector tracks -- `activities` is scoped to
  ticket-activities only, not "all activities" in the tenant.

Both are handled the same way: a dedicated sync function
(`sync_nodes_object()` / `sync_activities_object()` in
`evisort_connector.py`, not the generic `sync_object()`) first calls
`fetch_all_group_ids()` / `fetch_all_ticket_ids()` -- a read-only pass over
the parent object's own pagination, with no destination writes and no
dependency on the parent object being selected in `sync_objects` or ever
having been synced to a table -- then drives one paginated fetch per
discovered parent ID, accumulating into a single `connector_object_run` row
for the child object overall (not one row per parent).

`activities`' response never states which ticket an activity belongs to, so
`sync_activities_object()` injects the driving `ticket_id` into each raw
record (as `_entity_id`) before `flatten_activity()` runs, since the API
gives no other way to derive it.

`batch_size`, when set, is enforced as a **total cap across every parent
combined** for these two objects -- not reset per group/ticket. Once the
running total reaches the cap, any remaining parents are skipped entirely
rather than partially fetched.

## Objects

- **`documents`**: `{target_catalog}.{target_schema}.{table_name}` --
  `table_name` has no default and is required whenever `documents` is in
  `sync_objects`. Columns are 3 identity columns (`evisort_id` primary key,
  `date_modified` incremental cursor, `file_name`) + whatever `columns`
  names, typed `ARRAY<STRING>` (exact Evisort field labels, sanitized) +
  whatever `clauses` names, typed `STRING` (exact Evisort clause/provision
  labels) + 3 surrogate columns. At least one of `columns`/`clauses` must be
  non-empty. A field and a clause with the identical exact name (e.g.
  `Governing Law` in both) is allowed -- the clause side gets a
  `_clause_text` suffix automatically.
- **`groups`**: `evisort_groups` (fixed name), 12 columns, always all 12 --
  no column-selection mechanism exists for this or any object below.
- **`tickets`**: `evisort_tickets` (fixed name), 11 columns.
  `assignee_ids`/`participant_ids` are comma-joined integer lists, not
  exploded into child rows.
- **`nodes`**: `evisort_nodes` (fixed name), 11 columns. Dependent on
  `groups` for ID discovery only -- see above.
- **`departments`**: `evisort_departments` (fixed name), 9 columns, built
  from a full real example in Evisort's official docs.
- **`workflows`**: `evisort_workflows` (fixed name), 32 columns. Confirmed
  against 2 real records. Deeply nested/variable-shaped fields (`stages`,
  `fields`, `conditions`, `fieldMappings`, `implicitFields`, `signers`,
  `versions`, `prefilledFormData`, `fieldLinks`) are stored as raw JSON
  string columns rather than a guessed sub-schema; everything else is a
  real typed column.
- **`activities`**: `evisort_activities` (fixed name), 12 columns. Schema
  is fully typed from the official spec.

`resolve_documents_object(config)` builds the actual per-run `documents`
schema from `config.columns`/`config.clauses`; every other object uses its
fixed `*_SCHEMA_COLUMNS` in `evisort_objects.py` unchanged.

## Surrogate columns

| Column | Type | Description |
| --- | --- | --- |
| `_jetstream_source_object` | STRING | `{endpoint_path}/{id}` -- the API resource this row came from. |
| `_jetstream_synced` | TIMESTAMP | `current_timestamp()` when this row was inserted or its real content last changed -- not touched when a record matches an existing row with byte-for-byte identical content (see `rows_unchanged` below). |
| `_jetstream_deleted` | BOOLEAN | Always `FALSE`. None of Evisort's endpoints expose a delete signal, so there is no soft-delete detection for any object. |
| `_jetstream_id` | STRING | Content-hash fallback key, added when `id_field=None`. All 7 registered objects have a real `id_field`, so this condition is never met today -- included for objects that might be registered later without a confirmed natural key. |
| `_jetstream_index` | BIGINT | Added alongside `_jetstream_id` -- the row's 0-based position within its write batch. Same condition as `_jetstream_id`. |

## Control plane

State is written to a shared catalog
(`{control_plane_catalog}.jetstream.*` by default, or
`{control_plane_catalog}.{control_plane_schema_override}.*` when
`control_plane_schema_override` is set, for example `jetstream_evisort` to
keep a test instance's rows separate from the shared `jetstream` schema).
This connector never creates, alters, or drops that schema or its tables.

The connector instance is looked up primarily by `databricks_job_id`
(discovered automatically at runtime -- see "Job identity" below), falling
back to a business-key lookup
(`connector_type_id`/`destination_catalog`/`destination_schema`/`table_name`/
`api_base_url`) only when running outside an actual Databricks Job task
(interactive runs, dry-run standalone mode), where a job-id-based lookup
would be unsafe. `created_by` is populated via `current_user()`, with known
service principal application IDs mapped to readable names
(`KNOWN_SERVICE_PRINCIPALS` in `control_plane.py`) -- unrecognized
identities (a human email, or an unmapped service principal) pass through
unchanged.

Per object synced this run: one `connector_object_run` row, one
`connector_table_registry` row, and -- for cursor-capable objects only --
one `connector_state` cursor (`object_name=<object_name>`,
`state_key=<cursor_field>`, e.g. `state_key='dateModified'` for
`documents`), checkpointed after every page. `state_key` is derived
directly from the object's own `cursor_field`, not a fixed label shared
across every object.

`connector_run` inserts are idempotent, keyed on
`(connector_instance_id, idempotency_key)` via `MERGE` -- a genuine retry of
the same Databricks job run reuses the existing row instead of creating a
duplicate, and every subsequent call in that run correctly targets the
pre-existing row's ID rather than a fresh one.

`connector_run`/`connector_object_run.rows_inserted`/`rows_updated` reflect
the real split from each MERGE's own operation metrics
(`DESCRIBE HISTORY ... operationMetrics`), not "every row this page
processed counted as an insert" -- a record whose primary key already
existed updates that row and is counted as an update, not an insert. Falls
back to counting every processed row as an insert (the old behavior) only
if the real metrics aren't available for a given table (printed as a
warning when this happens, not silent). Post-run validation
(`validate_object_run()`) checks `rows_inserted + rows_updated +
rows_unchanged` for this same reason -- a run that's entirely unchanged
(e.g. a `force_full_refresh` re-sync of already-current data,
`rows_inserted=0, rows_updated=0`) is not a data-loss failure.

`rows_updated` specifically means the row's real content genuinely
differs from what was already stored -- the MERGE's `WHEN MATCHED` clause
is conditional on at least one business column being `IS DISTINCT FROM`
its existing value (excluding the `_jetstream_*` surrogate columns
themselves from that comparison, since `_jetstream_synced` would always
differ otherwise). A record that matches an existing row with
byte-for-byte identical content -- e.g. most rows during a
`force_full_refresh` re-sync of data that hasn't actually changed -- does
not fire the `UPDATE` at all, and is counted separately as
`rows_unchanged` (the simple remainder: records processed this page minus
real `rows_inserted` minus real `rows_updated`, since every record falls
into exactly one of those three buckets).

`connector_object_run.extended_metrics` records `api_calls` -- the number
of successful page fetches this object's sync made, not a transient-retry
count (a page retried once after a 429/5xx still counts once, not once per
attempt).

`connector_table_registry.row_count_estimate` is a real
`SELECT COUNT(*)` against the destination table after each object's sync,
not the count of rows *this run* processed -- a small incremental run
touching 1 row does not overwrite a table's real row count down to 1.
Left unchanged (not overwritten with a wrong value) if a real count can't
be read for any reason.

Incremental cursor advancement (`checkpoint_cursor()`) compares candidate
and stored cursor values as real parsed datetimes, not raw strings, so
mixed timestamp formatting (e.g. a `Z` suffix on one value, `+00:00` on
another) can't cause a genuinely newer cursor to be silently rejected.
Falls back to a plain string comparison only if a value doesn't parse as a
datetime at all -- e.g. a future object's cursor field that isn't a
timestamp.

`resolve_cursor()` requests `modifiedSince` one microsecond past the
stored `documents` checkpoint, not the stored value itself -- Evisort's
`modifiedSince` filter is inclusive (`dateModified >= modifiedSince`), so
requesting the exact value last checkpointed would re-match the document
that set it, forever, on every subsequent incremental run (its own
timestamp never advances, so `checkpoint_cursor()`'s only-advance-forward
check would never move past it either). Only applied to the genuinely
stored checkpoint -- not to `modified_since_override`, which is used
exactly as given.

### Runtime Validation

Every run re-validates the persisted `connector_instance.source_config`
against the persisted `connector_manifest.config_schema` -- the real
stored values, read back from the tables, not the in-memory config
object -- as the first step after `connector_run` is inserted and before
any connection to Evisort:

```
register_instance -> start_run -> get_stored_config_and_schema
-> runtime_validation -> authenticate -> connection_test -> ...
```

`build_source_config(config)` persists every current parameter except
`target_catalog`/`target_schema` (those are separate `connector_instance`
columns, not part of `source_config`'s JSON), built from `_WIDGET_SPEC`'s
own key list so a new parameter is automatically included. `validate_source_config(source_config, config_schema)`
in `control_plane.py` checks four things: every required parameter is
present, every present value's type matches its schema entry (a string
value that genuinely parses as an int/bool satisfies an integer/boolean
type, since every Databricks widget value is a string by nature), every
enum-constrained value is one of the allowed values, and every
`depends_on` condition resolves correctly. `depends_on` supports both
exact equality (`{"parameter": X, "value": Y}`) and list-membership on a
comma-separated field (`{"parameter": X, "contains": Y}`) -- `table_name`
uses the latter, since `sync_objects` is itself a multi-value field
(`"documents,groups,tickets"`), not a single value.

On a violation, the run is marked `status='failed'` on `connector_run`
with the specific parameter and rule named, one `connector_log` row is
written with `event_type='config_validation_failed'`, and the run stops
before ever calling Evisort's API. The exception still propagates out of
`main()` (rather than a plain early return) so the actual Databricks Job
Run correctly shows failed in the Jobs UI; an `already_handled_failure`
flag tells the generic top-level exception handler this specific failure
was already recorded, so it doesn't overwrite the specific message with
its own generic one.

### Job identity

`databricks_job_id`/`databricks_job_run_id`/`databricks_workspace_id` are
discovered automatically via `dbruntime.databricks_repl_context` -- no
manual configuration needed in the normal case. The classic
`dbutils.notebook.entry_point...getContext()` API is used as a fallback for
older runtimes, since it has been found to fail entirely under
Spark-Connect-based compute. The three config fields of the same name exist
only as a rare manual override, for a task type neither route covers --
leave them blank normally.

## Parameters

See `parameters.yaml` for the full list with defaults and descriptions.
Notable ones:

| Parameter | Default | Description |
| --- | --- | --- |
| `sync_objects` | `documents` | Comma-separated `EVISORT_OBJECTS` keys to sync this run: `documents`, `groups`, `tickets`, `nodes`, `departments`, `workflows`, `activities`. |
| `table_name` | *(required if `documents` in `sync_objects`)* | `documents`' destination table name. Unused otherwise. |
| `columns` / `clauses` | *(at least one required if `documents` in `sync_objects`)* | Comma-separated **exact** Evisort field/clause names. No fixed catalog. Unused otherwise. |
| `secret_scope` | *(required)* | Databricks secret scope holding the Evisort API key. |
| `control_plane_schema_override` | *(blank)* | Route control-plane writes to `jetstream_<name>` instead of the shared `jetstream` schema. Must start with `jetstream_`. |
| `databricks_job_id` / `databricks_job_run_id` / `databricks_workspace_id` | *(blank)* | Manual override only -- leave blank; discovered automatically. |
| `sla_target_minutes` | `1440` | Used by `connector_health` to compute `is_stale`. `0` disables staleness alerting. |
| `batch_size` | *(blank)* | Maximum records **per object** per run, across all pages (or across every parent combined, for `nodes`/`activities`). Blank means no limit, not a hardcoded fallback. Ignored entirely (treated as unlimited) when `force_full_refresh=true`. |
| `dry_run` | `false` | Fetches and flattens up to `preview_documents_count` records per configured object and prints every non-null field, without writing data or state. |
| `force_full_refresh` | `false` | Ignores the stored cursor for every cursor-capable object, and ignores `batch_size` entirely -- fetches everything, matching its own documented promise. Recorded as `run_type=full_resync`. |
| `on_error` | `fail` | `fail`: the first malformed record stops the run. `skip`: dropped and logged; run continues. Per-record flattening only -- a page that can't be fetched at all always stops the run. |

## Local development

```bash
python3.12 -m venv .venv
./.venv/bin/pip install requests
./.venv/bin/python evisort_connector.py
```

Running `evisort_connector.py` directly (not via `%run` inside Databricks)
falls through to a plain `import` of `control_plane.py` and
`evisort_objects.py` instead, so every name those two files would otherwise
inject via `%run` is still available.

This runs the file's `__main__` block: field/clause lookup checks for
`documents` (including a field with no fixed provenance at all, to prove
selection isn't restricted to any fixed catalog), schema-consistency checks
for every fixed-schema object, pagination-resolution checks covering all
real pagination shapes, a `_jetstream_id` fallback-key test,
`validate_source_config()`'s four checks against the real compiled schema
(including the multi-object `sync_objects` scenario that specifically
needs `depends_on`'s `contains` mode, not `value`), and extensive config
loading/validation -- including `sync_objects`, `columns`/`clauses`
collision handling, `control_plane_schema_override` validation, and
`batch_size`/`force_full_refresh` interaction -- all without pyspark,
`dbutils`, or a live API connection.

`control_plane.py` has its own `__main__` block covering UUIDv7 generation
and SQL literal formatting. Its SQL logic -- idempotent run tracking,
instance lookup, service-principal name mapping, cursor comparison, row
count tracking -- and `IngestEngine.write_page()` are exercised via
targeted tests during development.
