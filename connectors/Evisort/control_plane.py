# Databricks notebook source
# control_plane.py
#
# Jetstream control plane wiring for the Evisort connector. This is the one
# file in this repo that is not Evisort-specific: it implements the
# platform-mandated pattern every Jetstream connector uses: register in
# connector_registry/connector_manifest/connector_instance, track runs in
# connector_run/connector_object_run, checkpoint cursors in connector_state,
# log events in connector_log, all against the shared catalog
# ({control_plane_catalog}.jetstream.*). Kept in its own file because this
# shape is identical across every connector type; only the constants at the
# top of ControlPlaneManager and the source_config keys change per connector.
#
# This connector has exactly one logical "object" per run, the /documents
# endpoint, so it records a single connector_object_run row per run
# (updated incrementally as pages are processed), rather than one row per
# document. connector_state's cursor is checkpointed after every page for
# the same reason: a crash resumes from the last checkpointed page instead
# of redoing the whole run.
#
# No %run dependency: every type this file uses (JetstreamConfig) appears
# only in string-form type hints (via `from __future__ import annotations`),
# so this file has no load-order requirement relative to evisort_connector.py.

from __future__ import annotations

import json
import os
import time
from datetime import datetime, timezone


# ---------------------------------------------------------------------------
# Status constants
# ---------------------------------------------------------------------------

RUN_STATUS_RUNNING = "running"
RUN_STATUS_SUCCESS = "success"
RUN_STATUS_FAILED = "failed"

OBJECT_STATUS_RUNNING = "running"
OBJECT_STATUS_SUCCESS = "success"
OBJECT_STATUS_FAILED = "failed"


# ---------------------------------------------------------------------------
# UUIDv7 generator
# ---------------------------------------------------------------------------

def generate_uuid7() -> str:
    """
    Generate a UUIDv7: 48-bit unix_ts_ms prefix + random suffix.

    UUIDv7 identifiers sort chronologically, which improves Iceberg partition
    pruning and file clustering compared to random UUIDv4. Python 3.12 does
    not have uuid.uuid7() (added in 3.14), so this is a manual implementation.
    """
    ts_ms = int(time.time() * 1000)
    rand = os.urandom(10)
    b = bytearray(16)
    b[0] = (ts_ms >> 40) & 0xFF
    b[1] = (ts_ms >> 32) & 0xFF
    b[2] = (ts_ms >> 24) & 0xFF
    b[3] = (ts_ms >> 16) & 0xFF
    b[4] = (ts_ms >> 8) & 0xFF
    b[5] = ts_ms & 0xFF
    b[6] = 0x70 | (rand[0] & 0x0F)   # version 7
    b[7] = rand[1]
    b[8] = 0x80 | (rand[2] & 0x3F)   # variant 10
    b[9] = rand[3]
    b[10] = rand[4]
    b[11] = rand[5]
    b[12] = rand[6]
    b[13] = rand[7]
    b[14] = rand[8]
    b[15] = rand[9]
    h = b.hex()
    return f"{h[0:8]}-{h[8:12]}-{h[12:16]}-{h[16:20]}-{h[20:32]}"


def get_databricks_context(dbutils) -> tuple[str, str, str, str]:
    """
    Read workspace_id, job_id, job_run_id, and job_name for this run.

    Tries dbruntime.databricks_repl_context.get_context() first -- a pure
    Python module, not the Py4J/JVM-bridged
    dbutils.notebook.entry_point...getContext() API below. Per the
    Jetstream standard's Instance Bootstrapping section, databricks_job_id is
    "Databricks' own stable identity for the job... Nobody sets it" --
    it's meant to be discovered automatically, not manually configured
    per job. dbruntime.databricks_repl_context is undocumented by
    Databricks but reliable across compute types including serverless/
    Spark Connect (Databricks' own knowledge-base article for finding a
    workspace ID recommends get_context().workspaceId for exactly this
    reason).

    Falls back to the classic dbutils.notebook.entry_point...getContext()
    API if the dbruntime import or any expected attribute isn't
    available (e.g. an older runtime, or a task type dbruntime doesn't
    cover). That classic API is unreliable under Spark-Connect-based
    compute: ctx.jobId() (and the other context accessors) can return an
    object without a .getOrElse(String) method, raising Py4JException
    "Method getOrElse([class java.lang.String]) does not exist" instead
    of the classic Scala Option the accessor code assumes.

    Both fallback paths print their failure rather than swallowing it
    silently, so a genuine context-read failure is distinguishable in job
    logs from an intentional interactive run (local tests, dry-run
    standalone mode, or `%run` without a Job Task) -- all of which are
    expected to return all-empty and are not errors.
    """
    try:
        from dbruntime.databricks_repl_context import get_context as _get_repl_context
        ctx = _get_repl_context()
        workspace_id = str(getattr(ctx, "workspaceId", "") or "")
        job_id = str(getattr(ctx, "jobId", "") or "")
        job_run_id = str(getattr(ctx, "idInJob", "") or "")
        job_name = str(getattr(ctx, "jobName", "") or "")
        if workspace_id or job_id or job_run_id or job_name:
            return workspace_id, job_id, job_run_id, job_name
        # ctx resolved but every field was empty (e.g. a genuinely
        # interactive run, not a Job Task) -- this is a legitimate empty
        # result, not a failure, so return it as-is rather than falling
        # through to the classic API below.
        return "", "", "", ""
    except Exception as exc:
        print(
            f"get_databricks_context: dbruntime.databricks_repl_context unavailable "
            f"({type(exc).__name__}: {exc}). Falling back to the classic dbutils "
            f"context API."
        )

    try:
        ctx = dbutils.notebook.entry_point.getDbutils().notebook().getContext()
        workspace_id = ctx.workspaceId().getOrElse("")
        job_id = ctx.jobId().getOrElse("")
        job_run_id = ctx.jobRunId().getOrElse("")
        job_name = ctx.jobName().getOrElse("")
        return workspace_id, job_id, job_run_id, job_name
    except Exception as exc:
        print(
            f"get_databricks_context: no Databricks Job context available via either "
            f"route ({type(exc).__name__}: {exc}). Falling back to empty context "
            f"(expected for interactive runs and dry-run mode)."
        )
        return "", "", "", ""


KNOWN_SERVICE_PRINCIPALS = {
    # application_id -> human-readable name. Databricks' current_user()
    # returns the raw application id for a service-principal-authenticated
    # session (unlike an interactive/manual run, where it's already a
    # readable email address) -- this table exists purely to make
    # connector_instance.created_by legible for the small, known set of
    # service principals that actually run this job. Add an entry here
    # whenever a new one starts running it; anything not listed falls back
    # to the raw id untouched, so nothing silently goes missing.
    "0552381f-e136-47af-8758-9c36e12eabbe": "sp-jetstream-dev",
    "0db5f013-a86f-493a-8ec7-e03aae320b58": "sp-jetstream-prod",
}


def get_current_user(spark) -> str:
    """
    Return the identity Spark is currently running as: an engineer's email
    for an interactive or manually triggered run, or a human-readable
    service principal name (via KNOWN_SERVICE_PRINCIPALS above) for a
    scheduled Databricks Job run -- falling back to the raw application id
    if the service principal running isn't in that table yet. Populates
    connector_instance.created_by.
    """
    try:
        rows = spark.sql("SELECT current_user() AS u").collect()
        raw_identity = rows[0]["u"] or ""
        known_name = KNOWN_SERVICE_PRINCIPALS.get(raw_identity)
        return f"{known_name} ({raw_identity})" if known_name else raw_identity
    except Exception as exc:
        print(f"get_current_user: could not resolve current_user() ({type(exc).__name__}: {exc}).")
        return ""


def _is_int_like(value) -> bool:
    if isinstance(value, bool):
        return False
    if isinstance(value, int):
        return True
    if isinstance(value, str):
        try:
            int(value)
            return True
        except ValueError:
            return False
    return False


def _is_number_like(value) -> bool:
    if isinstance(value, bool):
        return False
    if isinstance(value, (int, float)):
        return True
    if isinstance(value, str):
        try:
            float(value)
            return True
        except ValueError:
            return False
    return False


def _is_bool_like(value) -> bool:
    if isinstance(value, bool):
        return True
    if isinstance(value, str):
        return value.strip().lower() in ("true", "false")
    return False


_JSON_SCHEMA_TYPE_CHECKS = {
    # Accepts a string representation that genuinely parses as the target
    # type, not just a native Python int/bool/float -- every Databricks
    # Job widget value is a string by nature, regardless of connector,
    # so source_config storing "100" for an integer-typed parameter (or
    # "true" for a boolean one) is the normal case for any connector on
    # this framework, not an Evisort-specific quirk. Without this, this
    # shared check would reject essentially every real stored
    # source_config immediately, on its very first required numeric or
    # boolean field.
    "string": lambda v: isinstance(v, str),
    "integer": _is_int_like,
    "number": _is_number_like,
    "boolean": _is_bool_like,
    "array": lambda v: isinstance(v, list),
    "object": lambda v: isinstance(v, dict),
}


def validate_source_config(source_config: dict, config_schema: str) -> tuple[str, str] | None:
    """
    Generic, connector-agnostic Runtime Validation, per the Jetstream standard's
    section of the same name -- meant to be built once as a shared
    function in the Jetstream framework, not reimplemented per connector.
    Checks exactly the four things that section specifies, in order:

        1. required-presence  -- every name in config_schema["required"]
           is present in source_config and non-empty.
        2. type-matching       -- every present value's Python type matches
           its property's declared "type" (a string or a list of
           acceptable strings, e.g. ["integer", "string"]).
        3. enum-membership     -- every present value with a declared
           "enum" is one of the listed values.
        4. depends_on resolution -- a property with a "depends_on":
           {"parameter": X, "value": Y} entry is required exactly when
           source_config[X] == Y, and must be absent/empty otherwise.

    This is NOT a substitute for a connector's own richer business-rule
    validation (e.g. this Evisort connector's own validate_config(), which
    enforces rules like "at least one of columns/clauses" that don't
    reduce to these four categories) -- the Jetstream standard explicitly frames
    this as an ADDITIONAL layer, re-checking after admission in case
    source_config and config_schema drifted apart since then, not instead
    of validating at admission time.

    Returns (parameter_name, rule_description) for the FIRST violation
    found, or None if source_config is fully valid against config_schema.
    Deliberately stops at the first failure rather than collecting all of
    them, matching the doc's own "Fail if..."/"Fail on..." framing of each
    check as an immediate stop condition.
    """
    schema = json.loads(config_schema)
    properties = schema.get("properties", {})
    required = schema.get("required", [])

    def _is_present(value) -> bool:
        return value not in (None, "")

    for param in required:
        if not _is_present(source_config.get(param)):
            return param, f"required parameter '{param}' is missing"

    for param, value in source_config.items():
        prop_schema = properties.get(param)
        if prop_schema is None or not _is_present(value):
            continue
        expected_types = prop_schema.get("type")
        if expected_types is None:
            continue
        if isinstance(expected_types, str):
            expected_types = [expected_types]
        if not any(_JSON_SCHEMA_TYPE_CHECKS.get(t, lambda v: True)(value) for t in expected_types):
            return param, f"value {value!r} does not match expected type(s) {expected_types}"

    for param, value in source_config.items():
        prop_schema = properties.get(param)
        if prop_schema is None or not _is_present(value):
            continue
        allowed = prop_schema.get("enum")
        if allowed is not None and value not in allowed:
            return param, f"value {value!r} is not one of the allowed values {allowed}"

    for param, prop_schema in properties.items():
        depends_on = prop_schema.get("depends_on")
        if not depends_on:
            continue
        ref_param = depends_on["parameter"]
        ref_actual = source_config.get(ref_param)
        if "value" in depends_on:
            # The Jetstream standard's literal model: exact equality.
            condition_met = ref_actual == depends_on["value"]
        elif "contains" in depends_on:
            # Jetstream-specific extension beyond the doc's literal model,
            # needed because a field like sync_objects is itself a
            # multi-value comma-separated string, not a single value --
            # exact equality alone both misses real violations (a
            # multi-object config with table_name genuinely missing) and
            # false-positives on valid configs (a multi-object config with
            # table_name correctly present, incorrectly flagged as "must
            # not be present" because the whole string didn't equal
            # "documents" exactly). Confirmed empirically, not theoretical
            # -- both failure modes reproduced in testing before this was
            # added.
            wanted = depends_on["contains"]
            actual_list = [v.strip() for v in (ref_actual or "").split(",")]
            condition_met = wanted in actual_list
        else:
            continue  # malformed depends_on entry -- skip rather than crash
        param_present = _is_present(source_config.get(param))
        if condition_met and not param_present:
            return param, f"required because '{ref_param}' matches its depends_on condition, but is missing"
        if not condition_met and param_present:
            return param, f"must not be present unless '{ref_param}' matches its depends_on condition"

    return None


def resolve_databricks_context(dbutils, config: "JetstreamConfig") -> tuple[str, str, str, str]:
    """
    Resolve workspace_id, job_id, job_run_id, and job_name for this run.

    get_databricks_context() now discovers all four automatically in the
    normal case (see that function's docstring) -- no manual job
    parameter configuration needed, matching the Jetstream standard's "Nobody
    sets it" principle for databricks_job_id.

    config.databricks_workspace_id/databricks_job_id/databricks_job_run_id
    remain as a manual override ONLY, for the rare case where automatic
    discovery genuinely can't work (e.g. a future task type neither
    dbruntime nor the classic context API covers). Leave these three
    config fields blank in the normal case -- they are not part of the
    expected setup. When set, they're expected to hold a resolved
    Databricks Jobs dynamic value reference (e.g. a job parameter whose
    value is the literal string "{{job.id}}"); a value that still
    contains "{{" (an unresolved reference) is treated as unset rather
    than stored as garbage.

    job_name has no dynamic value reference as of this writing, so it
    always comes from get_databricks_context() regardless of the
    override fields.
    """
    def _resolved(raw_value: str) -> str:
        value = raw_value.strip()
        return "" if "{{" in value else value

    ctx_workspace_id, ctx_job_id, ctx_job_run_id, job_name = get_databricks_context(dbutils)
    workspace_id = _resolved(config.databricks_workspace_id) or ctx_workspace_id
    job_id = _resolved(config.databricks_job_id) or ctx_job_id
    job_run_id = _resolved(config.databricks_job_run_id) or ctx_job_run_id
    return workspace_id, job_id, job_run_id, job_name


# ---------------------------------------------------------------------------
# ControlPlaneManager
# ---------------------------------------------------------------------------

class ControlPlaneManager:
    """
    Manages all reads and writes against the shared Jetstream control plane
    catalog. One instance is created per job run. connector_instance_id is
    resolved once via ensure_registered(), by databricks_job_id when
    running as an actual Databricks Job task (per the Jetstream standard's
    Instance Bootstrapping section), falling back to a business-key lookup
    only for non-Job-task runs, and reused for every subsequent call in
    the run. See ensure_registered()'s own docstring for the full rationale.

    Control plane schema: {config.control_plane_catalog}.jetstream
    """

    CONNECTOR_TYPE_ID = "evisort"
    CONNECTOR_TYPE_NAME = "Evisort"
    CONNECTOR_CATEGORY = "application_api"
    CONNECTOR_VERSION = "1.0.0"

    _REQUIRED_TABLES = [
        "connector_registry",
        "connector_manifest",
        "connector_instance",
        "connector_run",
        "connector_object_run",
        "connector_table_registry",
        "connector_schema_change_event",
        "connector_state",
        "connector_log",
    ]

    def __init__(self, spark, config: "JetstreamConfig", dbutils=None) -> None:
        self.spark = spark
        self.config = config
        self.dbutils = dbutils
        self._cp_schema = config.get_control_plane_schema()
        self.connector_instance_id: str | None = None

    def _t(self, table_name: str) -> str:
        return f"{self._cp_schema}.{table_name}"

    # ------------------------------------------------------------------
    # Control plane validation
    # ------------------------------------------------------------------

    def ensure_tables_exist(self) -> None:
        """
        Validate that the shared Jetstream control plane schema and all nine
        required tables exist. Fails immediately with a clear error if any
        table is missing.

        This connector never creates, alters, or drops the jetstream schema
        or its tables. They are created once per catalog by running
        control_plane_standards/provision_jetstream_schema.py.
        """
        missing = []
        for table in self._REQUIRED_TABLES:
            try:
                self.spark.sql(f"DESCRIBE TABLE {self._t(table)}")
            except Exception:
                missing.append(table)

        if missing:
            raise RuntimeError(
                f"Control plane tables not found in '{self._cp_schema}': "
                f"{', '.join(missing)}. "
                f"The shared catalog must be provisioned before running connectors."
            )
        print(f"Control plane tables validated in {self._cp_schema}.")

    # ------------------------------------------------------------------
    # Registration
    # ------------------------------------------------------------------

    def ensure_registered(self, instance_name: str, job_id: str, config_schema: str, source_config: dict) -> str:
        """
        Ensure connector_registry, connector_manifest, and connector_instance
        rows exist for this pipeline.

        The connector_instance is looked up by databricks_job_id, per the
        Jetstream standard's Instance Bootstrapping section: it's the only stable
        identity a job knows about itself before any Jetstream row exists,
        and it's what lets every run find its way back to the same row
        instead of creating a new one, even across a config edit (renamed
        table_name, changed columns, etc.) that a business-key lookup would
        have treated as a different instance.

        Falls back to the previous business-key lookup (connector_type_id,
        destination_catalog, destination_schema, table_name, api_base_url)
        ONLY when job_id is empty, i.e. this isn't actually running as a
        Databricks Job task (interactive %run, local dry-run testing). A
        pure job_id lookup would be unsafe there: every such run shares the
        same empty job_id, so two genuinely different configs run
        interactively could collide onto the same instance row and silently
        overwrite each other's source_config.

        Args:
            instance_name: Human-readable name for the instance (job name).
            job_id: databricks_job_id from get_databricks_context(). Empty
                string when not running as an actual Databricks Job task.
            config_schema: JSON Schema string compiled from parameters.yaml,
                stored in connector_manifest.config_schema.
            source_config: Every current parameter value except
                target_catalog/target_schema (build_source_config() in
                evisort_connector.py) -- persisted as connector_instance.
                source_config, and refreshed on every run so Runtime
                Validation is checking what's actually configured now, not
                a stale snapshot from whenever the instance was first
                created.

        Sets self.connector_instance_id and returns it.
        """
        now = datetime.now(timezone.utc)
        self._upsert_connector_registry(now)
        self._upsert_connector_manifest(now, config_schema)
        self.connector_instance_id = self._find_or_create_instance(instance_name, job_id, source_config, now)
        print(f"Connector instance registered: {self.connector_instance_id}")
        return self.connector_instance_id

    def get_stored_config_and_schema(self) -> tuple[dict, str]:
        """
        Read back connector_instance.source_config and this instance's
        pinned manifest's config_schema, exactly as persisted -- the two
        things Runtime Validation checks against each other. Reads the
        real stored values rather than reconstructing them from the
        in-memory config object, since the whole point is to catch
        drift between what's stored and what's expected (per the
        Jetstream standard's Runtime Validation section: "A config_schema can
        change to a new manifest_version after an instance was already
        created, and source_config can be edited directly... Validating
        on every run closes that gap.").
        """
        row = self.spark.sql(f"""
        SELECT i.source_config, m.config_schema
        FROM {self._t('connector_instance')} i
        JOIN {self._t('connector_manifest')} m
          ON m.connector_type_id = i.connector_type_id
         AND m.manifest_version = i.config_manifest_version
        WHERE i.connector_instance_id = {self._s(self.connector_instance_id)}
        """).collect()
        if not row:
            raise RuntimeError(
                f"Could not read back source_config/config_schema for connector_instance_id="
                f"{self.connector_instance_id!r} -- instance or its pinned manifest is missing."
            )
        source_config = json.loads(row[0]["source_config"]) if row[0]["source_config"] else {}
        config_schema = row[0]["config_schema"]
        return source_config, config_schema

    def _upsert_connector_registry(self, now: datetime) -> None:
        ts = self._fmt(now)
        self.spark.sql(f"""
        MERGE INTO {self._t('connector_registry')} AS t
        USING (SELECT
            {self._s(self.CONNECTOR_TYPE_ID)}    AS connector_type_id,
            {self._s(self.CONNECTOR_TYPE_NAME)}   AS connector_type_name,
            {self._s(self.CONNECTOR_CATEGORY)}    AS category,
            {self._s("Evisort is a contract intelligence and lifecycle management platform. This connector syncs Evisort's documents, managed entities, and workflow tickets into Unity Catalog, including contract metadata, extracted fields, and clause text.")} AS description,
            {self._s('jetstream-evisort-connector/')} AS repo_path,
            {self._s(self.CONNECTOR_VERSION)}     AS connector_version,
            {self._s(self.config.owner_team)}     AS maintainer,
            TRUE                                   AS is_active,
            {ts}                                   AS created_at,
            {ts}                                   AS updated_at
        ) AS s
        ON t.connector_type_id = s.connector_type_id
        WHEN MATCHED THEN UPDATE SET
            connector_type_name = s.connector_type_name,
            description         = s.description,
            connector_version   = s.connector_version,
            maintainer          = s.maintainer,
            updated_at          = s.updated_at
        WHEN NOT MATCHED THEN INSERT *
        """)

    def _upsert_connector_manifest(self, now: datetime, config_schema: str) -> None:
        """
        Matches on (connector_type_id, manifest_version) -- the real business
        key -- rather than manifest_id. manifest_id must be a UUIDv7 per the
        Jetstream standard (section 3.4: every identifier except connector_type_id
        is a UUID, generated once, never reused), so it can't double as the
        match key: a value regenerated fresh on every call would never equal
        a previously inserted row's id, and every run would insert a new
        duplicate manifest row instead of leaving the existing one alone.
        Matching on the business key means the UUID is only actually used
        the first time this manifest_version is registered; every run after
        that is a no-op against the already-inserted row.
        """
        ts = self._fmt(now)
        self.spark.sql(f"""
        MERGE INTO {self._t('connector_manifest')} AS t
        USING (SELECT
            {self._s(generate_uuid7())}                              AS manifest_id,
            {self._s(self.CONNECTOR_TYPE_ID)}                        AS connector_type_id,
            {self._s(self.CONNECTOR_VERSION)}                        AS manifest_version,
            {self._s(config_schema)}                                 AS config_schema,
            TRUE                                                      AS is_current,
            {ts}                                                      AS created_at
        ) AS s
        ON t.connector_type_id = s.connector_type_id AND t.manifest_version = s.manifest_version
        WHEN NOT MATCHED THEN INSERT *
        """)

    def _find_or_create_instance(self, instance_name: str, job_id: str, source_config: dict, now: datetime) -> str:
        if job_id:
            lookup_sql = f"""
            SELECT connector_instance_id
            FROM {self._t('connector_instance')}
            WHERE databricks_job_id = {self._s(job_id)}
            LIMIT 1
            """
        else:
            # Fallback for non-Job-task runs only -- see ensure_registered()'s
            # docstring for why job_id-based lookup is unsafe here.
            table = self._escape(self.config.table_name)
            api_url = self._escape(self.config.api_base_url)
            lookup_sql = f"""
            SELECT connector_instance_id
            FROM {self._t('connector_instance')}
            WHERE connector_type_id = {self._s(self.CONNECTOR_TYPE_ID)}
              AND destination_catalog = {self._s(self.config.target_catalog)}
              AND destination_schema  = {self._s(self.config.target_schema)}
              AND (databricks_job_id IS NULL OR databricks_job_id = '')
              AND get_json_object(source_config, '$.table_name')    = '{table}'
              AND get_json_object(source_config, '$.api_base_url')  = '{api_url}'
            LIMIT 1
            """

        rows = self.spark.sql(lookup_sql).collect()
        if rows:
            existing_id = rows[0]["connector_instance_id"]
            self._refresh_instance(existing_id, instance_name, source_config, now)
            return existing_id

        new_id = generate_uuid7()
        source_config_json = json.dumps(source_config)
        ts = self._fmt(now)
        created_by = get_current_user(self.spark)
        self.spark.sql(f"""
        INSERT INTO {self._t('connector_instance')}
        (connector_instance_id, connector_type_id, instance_name, environment,
         owner_team, source_config, config_manifest_version, databricks_job_id,
         destination_catalog, destination_schema, schedule_cron,
         sla_target_minutes, status, tags, created_by, created_at, updated_at)
        VALUES (
            {self._s(new_id)},
            {self._s(self.CONNECTOR_TYPE_ID)},
            {self._s(instance_name or self.config.table_name)},
            {self._s(self.config.environment)},
            {self._s(self.config.owner_team)},
            {self._s(source_config_json)},
            {self._s(self.CONNECTOR_VERSION)},
            {self._s(job_id)},
            {self._s(self.config.target_catalog)},
            {self._s(self.config.target_schema)},
            NULL,
            {self.config.as_int('sla_target_minutes')},
            'active',
            NULL,
            {self._s(created_by or None)},
            {ts},
            {ts}
        )
        """)
        return new_id

    def _refresh_instance(self, instance_id: str, instance_name: str, source_config: dict, now: datetime) -> None:
        """
        Refresh this instance's row from the current run's config, per the
        Jetstream standard's Instance Bootstrapping section: "Re-read current job
        parameters and refresh source_config, destination_catalog, and
        destination_schema on that row, in case someone edited the job's
        parameters since the last run." Also refreshes instance_name.
        Previously this only touched instance_name; source_config and the
        destination fields were never refreshed after the instance's first
        run, so an edited job parameter (other than the ones used in the
        lookup key itself) would silently never reach the stored row.
        """
        source_config_json = json.dumps(source_config)
        self.spark.sql(f"""
        UPDATE {self._t('connector_instance')}
        SET instance_name       = {self._s(instance_name or self.config.table_name)},
            source_config       = {self._s(source_config_json)},
            destination_catalog = {self._s(self.config.target_catalog)},
            destination_schema  = {self._s(self.config.target_schema)},
            owner_team          = {self._s(self.config.owner_team)},
            sla_target_minutes  = {self.config.as_int('sla_target_minutes')},
            updated_at          = {self._fmt(now)}
        WHERE connector_instance_id = {self._s(instance_id)}
        """)

    # ------------------------------------------------------------------
    # Run lifecycle
    # ------------------------------------------------------------------

    def start_run(self, run_id: str, run_start_time: datetime, run_type: str, trigger_type: str) -> str:
        """
        Insert a connector_run row (status='running'), unless a row already
        exists for this (connector_instance_id, idempotency_key) pair, e.g.
        because this is a Databricks-triggered retry of a task attempt that
        already inserted a row for the same job_run_id. Only the run-level
        row; per-object connector_object_run rows are created separately
        via start_object_run(), once per object synced this run, since a
        single run can touch more than one Evisort object (documents
        today, more as they're added, see EVISORT_OBJECTS).

        Returns:
            The run_id to use for every subsequent call this run
            (start_object_run, end_run, log_event, etc.), the caller's own
            run_id if this was a fresh insert, or the PRE-EXISTING row's
            run_id if a retry matched an already-inserted row. Callers must
            use the returned value, not necessarily the run_id passed in.
        """
        if self.connector_instance_id is None:
            raise RuntimeError("ensure_registered() must be called before start_run().")

        workspace_id, job_id, job_run_id, _ = resolve_databricks_context(self.dbutils, self.config)
        run_date = run_start_time.strftime("%Y-%m-%d")
        ts = self._fmt(run_start_time)
        idempotency_key = job_run_id or run_id

        self.spark.sql(f"""
        MERGE INTO {self._t('connector_run')} AS t
        USING (SELECT
            {self._s(run_id)}                     AS run_id,
            {self._s(self.connector_instance_id)}  AS connector_instance_id,
            {self._s(idempotency_key)}             AS idempotency_key,
            {self._s(run_type)}                    AS run_type,
            {self._s(trigger_type)}                AS trigger_type,
            'running'                               AS status,
            {ts}                                    AS start_time,
            {self._s(workspace_id)}                 AS databricks_workspace_id,
            {self._s(job_id)}                       AS databricks_job_id,
            {self._s(job_run_id)}                   AS databricks_job_run_id,
            0                                        AS retry_count,
            DATE '{run_date}'                       AS run_date
        ) AS s
        ON t.connector_instance_id = s.connector_instance_id
           AND t.idempotency_key    = s.idempotency_key
        WHEN NOT MATCHED THEN INSERT (
            run_id, connector_instance_id, idempotency_key, run_type, trigger_type,
            status, start_time, databricks_workspace_id, databricks_job_id, databricks_job_run_id,
            retry_count, run_date
        ) VALUES (
            s.run_id, s.connector_instance_id, s.idempotency_key, s.run_type, s.trigger_type,
            s.status, s.start_time, s.databricks_workspace_id, s.databricks_job_id, s.databricks_job_run_id,
            s.retry_count, s.run_date
        )
        """)

        resolved = self.spark.sql(f"""
        SELECT run_id
        FROM {self._t('connector_run')}
        WHERE connector_instance_id = {self._s(self.connector_instance_id)}
          AND idempotency_key = {self._s(idempotency_key)}
        LIMIT 1
        """).collect()
        resolved_run_id = resolved[0]["run_id"] if resolved else run_id
        if resolved_run_id != run_id:
            print(f"start_run: idempotency_key '{idempotency_key}' already had a run — "
                  f"reusing existing run_id '{resolved_run_id}' instead of '{run_id}'.")
        return resolved_run_id

    @staticmethod
    def _table_has_column(spark, table_fqn: str, column_name: str) -> bool:
        """
        Whether table_fqn actually has column_name, checked live rather
        than assumed. Confirmed against the Jetstream standard's own DDL
        (section 6.4 vs 6.5): connector_run never has rows_unchanged --
        only connector_object_run does. This check exists so that stays
        correct without hardcoding the assumption into end_run() directly,
        and as a general defense against any other column this connector
        doesn't control turning out absent on a given deployment.
        """
        try:
            columns = {row["col_name"] for row in spark.sql(f"DESCRIBE TABLE {table_fqn}").collect()}
            return column_name in columns
        except Exception:
            return False

    def end_run(self, run_id: str, status: str, metrics: dict) -> None:
        """
        Finalize the connector_run row with the aggregate metrics summed
        across every object synced this run. Per-object rows are finalized
        individually via end_object_run(), not here.
        metrics keys: rows_ingested_total, rows_inserted, rows_updated,
        rows_unchanged, rows_deleted, tables_synced, error_message.

        rows_unchanged is only written if connector_run actually has that
        column -- see _table_has_column()'s docstring for why this can't
        be assumed even though the platform's own schema calls for it.
        """
        now = datetime.now(timezone.utc)
        table_fqn = self._t('connector_run')
        start_row = self.spark.sql(f"""
        SELECT start_time FROM {table_fqn} WHERE run_id = {self._s(run_id)}
        """).collect()

        duration = None
        if start_row and start_row[0]["start_time"]:
            duration = int((now - start_row[0]["start_time"].replace(tzinfo=timezone.utc)).total_seconds())

        rows_unchanged_clause = ""
        if self._table_has_column(self.spark, table_fqn, "rows_unchanged"):
            rows_unchanged_clause = f", rows_unchanged = {self._n(metrics.get('rows_unchanged'))}"
        else:
            print(f"NOTE: '{table_fqn}' has no rows_unchanged column -- expected, per the platform "
                  f"doc's own DDL (connector_run doesn't define this column; connector_object_run does).")

        self.spark.sql(f"""
        UPDATE {table_fqn} SET
            status              = {self._s(status)},
            end_time            = {self._fmt(now)},
            duration_seconds    = {self._n(duration)},
            rows_ingested_total = {self._n(metrics.get('rows_ingested_total'))},
            rows_inserted       = {self._n(metrics.get('rows_inserted'))},
            rows_updated        = {self._n(metrics.get('rows_updated'))}{rows_unchanged_clause},
            rows_deleted        = {self._n(metrics.get('rows_deleted'))},
            tables_synced       = {self._n(metrics.get('tables_synced'))},
            error_message       = {self._s(metrics.get('error_message', '') or '')}
        WHERE run_id = {self._s(run_id)}
        """)

    # ------------------------------------------------------------------
    # Per-object run tracking
    # ------------------------------------------------------------------
    #
    # One connector_object_run row per Evisort object synced this run
    # (documents today; a future object registered in EVISORT_OBJECTS adds
    # its own row the same way), matching the Jetstream standard's own
    # description of connector_object_run as "One row per destination table
    # touched within a single run," generalized from this connector's earlier
    # one-object-per-run simplification.

    def start_object_run(self, run_id: str, object_name: str, source_object: str, target_table: str) -> str:
        """
        Insert a connector_object_run row (status='running') for one
        Evisort object. Returns object_run_id for record_page_progress()
        and end_object_run() calls scoped to this object.
        """
        if self.connector_instance_id is None:
            raise RuntimeError("ensure_registered() must be called before start_object_run().")
        object_run_id = generate_uuid7()
        now = datetime.now(timezone.utc)
        run_date = now.strftime("%Y-%m-%d")
        self.spark.sql(f"""
        INSERT INTO {self._t('connector_object_run')}
        (object_run_id, run_id, connector_instance_id, destination_table,
         source_object, status, rows_read, rows_inserted, rows_updated,
         rows_deleted, rows_unchanged, bytes_written, start_time, end_time,
         error_message, extended_metrics, run_date)
        VALUES (
            {self._s(object_run_id)}, {self._s(run_id)}, {self._s(self.connector_instance_id)},
            {self._s(target_table)}, {self._s(source_object)},
            'running', 0, 0, 0, 0, 0, NULL, {self._fmt(now)}, NULL, NULL, NULL, DATE '{run_date}'
        )
        """)
        return object_run_id

    def record_page_progress(self, object_run_id: str, api_calls: int, rows_read: int, rows_written: int) -> None:
        """
        Update one object's running object_run row after each page of
        that object is flattened and written, called once per page,
        mirroring the checkpoint-after-each-batch pattern rather than
        writing counts once at the very end.

        api_calls counts successful page fetches, not raw HTTP attempts --
        a page that needed a retry (e.g. a transient 429/5xx handled by
        _request_with_backoff) still counts once here, not once per
        attempt. Reflects pagination progress, not literal request volume
        against Evisort's API.
        """
        extended_metrics = json.dumps({"api_calls": api_calls})
        self.spark.sql(f"""
        UPDATE {self._t('connector_object_run')} SET
            rows_read = {rows_read}, rows_inserted = {rows_written},
            extended_metrics = {self._s(extended_metrics)}
        WHERE object_run_id = {self._s(object_run_id)}
        """)

    def end_object_run(
        self, object_run_id: str, status: str, rows_inserted: int, rows_updated: int = 0,
        rows_unchanged: int = 0, error_message: str = "",
    ) -> None:
        """Finalize one object's connector_object_run row."""
        now = datetime.now(timezone.utc)
        self.spark.sql(f"""
        UPDATE {self._t('connector_object_run')} SET
            status         = {self._s(status)},
            end_time       = {self._fmt(now)},
            rows_inserted  = {rows_inserted},
            rows_updated   = {rows_updated},
            rows_unchanged = {rows_unchanged},
            error_message  = {self._s(error_message or '')}
        WHERE object_run_id = {self._s(object_run_id)}
        """)

    # ------------------------------------------------------------------
    # State (incremental cursor checkpointing, per object)
    # ------------------------------------------------------------------

    def get_cursor(self, object_name: str, cursor_field: str) -> str | None:
        """
        Return the stored incremental cursor for one object, or None if
        never checkpointed.

        state_key is derived directly from cursor_field (the object's own
        EvisortObject.cursor_field, e.g. "dateModified") rather than a
        fixed constant shared across every object, so a future object with
        a differently-named cursor field (e.g. "updatedDate") automatically
        gets its own correctly-named state row with zero code change here.
        """
        if self.connector_instance_id is None:
            raise RuntimeError("ensure_registered() must be called before get_cursor().")
        rows = self.spark.sql(f"""
        SELECT state_value FROM {self._t('connector_state')}
        WHERE connector_instance_id = {self._s(self.connector_instance_id)}
          AND object_name = {self._s(object_name)}
          AND state_key   = {self._s(cursor_field)}
        """).collect()
        return rows[0]["state_value"] if rows and rows[0]["state_value"] is not None else None

    @staticmethod
    def _cursor_is_not_newer(candidate: str, current: str) -> bool:
        """
        True if candidate should NOT advance the stored cursor past current
        -- i.e. candidate is not strictly newer.

        Parses both as real datetimes and compares chronologically, rather
        than comparing the raw strings -- a plain string comparison is only
        correct if every value shares the exact same width/precision/
        timezone-offset representation, which isn't guaranteed to hold
        forever even where it has been observed to hold today (checked
        against 100 real Evisort dateModified values, all in the identical
        "+00:00" microsecond-precision shape -- but one sample isn't a
        guarantee). Falls back to the original string comparison if either
        value doesn't parse as a real datetime -- e.g. a future object's
        cursor_field that isn't a timestamp at all (an offset or a token),
        where string/lexical ordering may be exactly what's intended.
        """
        try:
            return datetime.fromisoformat(candidate) <= datetime.fromisoformat(current)
        except (ValueError, TypeError):
            return candidate <= current

    def checkpoint_cursor(self, object_name: str, cursor_field: str, cursor_value: str) -> None:
        """
        Upsert one object's incremental cursor immediately after a page has
        been written, not once at the end of the run, so a crash mid-run
        leaves the cursor at the last successfully written page. Only
        advances the cursor forward, since records are not guaranteed to
        arrive in strict cursor-field order within a page.

        state_key = cursor_field, same derivation as get_cursor() above --
        the two must always agree, which passing the same obj.cursor_field
        value at both call sites guarantees.
        """
        current = self.get_cursor(object_name, cursor_field)
        if current is not None and self._cursor_is_not_newer(cursor_value, current):
            return
        now = datetime.now(timezone.utc)
        state_id = generate_uuid7()
        self.spark.sql(f"""
        MERGE INTO {self._t('connector_state')} AS t
        USING (SELECT
            {self._s(state_id)} AS state_id,
            {self._s(self.connector_instance_id)} AS connector_instance_id,
            {self._s(object_name)} AS object_name,
            {self._s(cursor_field)} AS state_key,
            {self._s(cursor_value)} AS state_value,
            {self._fmt(now)} AS updated_at
        ) AS s
        ON t.connector_instance_id = s.connector_instance_id
           AND t.object_name = s.object_name AND t.state_key = s.state_key
        WHEN MATCHED THEN UPDATE SET state_value = s.state_value, updated_at = s.updated_at
        WHEN NOT MATCHED THEN INSERT *
        """)

    # ------------------------------------------------------------------
    # Table registry
    # ------------------------------------------------------------------

    def update_table_registry(
        self,
        target_table: str,
        source_object: str,
        primary_key_columns: tuple[str, ...],
        row_count_estimate: int | None = None,
    ) -> str:
        """
        Upsert a connector_table_registry row for one destination table.
        Called once per object synced this run; a multi-object instance
        ends up with one connector_table_registry row per object's table.
        """
        now = datetime.now(timezone.utc)
        parts = target_table.split(".")
        dest_catalog = parts[0] if len(parts) >= 3 else self.config.target_catalog
        dest_schema = parts[1] if len(parts) >= 3 else self.config.target_schema
        dest_table = parts[2] if len(parts) >= 3 else parts[-1]
        pk_array_sql = "ARRAY(" + ", ".join(self._s(k) for k in primary_key_columns) + ")"

        lookup = self.spark.sql(f"""
        SELECT table_id FROM {self._t('connector_table_registry')}
        WHERE connector_instance_id = {self._s(self.connector_instance_id)}
          AND destination_catalog   = {self._s(dest_catalog)}
          AND destination_schema    = {self._s(dest_schema)}
          AND destination_table     = {self._s(dest_table)}
        LIMIT 1
        """).collect()

        ts = self._fmt(now)
        if lookup:
            table_id = lookup[0]["table_id"]
            # row_count_estimate=None means "no real count available this
            # call" (e.g. the registration-time call before any sync has
            # run, or a real COUNT(*) failed) -- must NOT overwrite an
            # existing real estimate with NULL in that case, so it's only
            # included in SET at all when a real value is actually known.
            row_count_clause = f", row_count_estimate = {row_count_estimate}" if row_count_estimate is not None else ""
            self.spark.sql(f"""
            UPDATE {self._t('connector_table_registry')} SET
                last_synced_at = {ts}, updated_at = {ts}{row_count_clause}
            WHERE table_id = {self._s(table_id)}
            """)
        else:
            table_id = generate_uuid7()
            self.spark.sql(f"""
            INSERT INTO {self._t('connector_table_registry')}
            (table_id, connector_instance_id, destination_catalog,
             destination_schema, destination_table, source_object,
             primary_key_columns, table_format, is_active,
             first_synced_at, last_synced_at, row_count_estimate,
             created_at, updated_at)
            VALUES (
                {self._s(table_id)}, {self._s(self.connector_instance_id)},
                {self._s(dest_catalog)}, {self._s(dest_schema)}, {self._s(dest_table)},
                {self._s(source_object)}, {pk_array_sql}, 'iceberg', TRUE,
                {ts}, {ts}, {self._n(row_count_estimate)}, {ts}, {ts}
            )
            """)
        return table_id

    # ------------------------------------------------------------------
    # Event log / schema change
    # ------------------------------------------------------------------

    def log_event(self, run_id: str | None, event_type: str, severity: str, message: str, event_data: dict | None = None) -> None:
        """Insert a row into connector_log."""
        log_id = generate_uuid7()
        now = datetime.now(timezone.utc)
        event_json = json.dumps(event_data) if event_data else None
        self.spark.sql(f"""
        INSERT INTO {self._t('connector_log')}
        (log_id, connector_instance_id, run_id, event_type, severity, message, event_data, created_at)
        VALUES (
            {self._s(log_id)}, {self._s(self.connector_instance_id)},
            {self._s(run_id) if run_id else 'NULL'}, {self._s(event_type)}, {self._s(severity)},
            {self._s(message)}, {self._s(event_json) if event_json else 'NULL'}, {self._fmt(now)}
        )
        """)

    def record_schema_change(
        self,
        table_id: str,
        change_type: str,
        column_name: str | None = None,
        old_type: str | None = None,
        new_type: str | None = None,
    ) -> None:
        """
        Insert into connector_schema_change_event for a DDL change.

        table_id is passed explicitly (from update_table_registry()'s
        return value) rather than tracked as manager state, with multiple
        objects sharing one ControlPlaneManager per run, "whichever table
        was registered most recently" is not a safe implicit default.
        """
        event_id = generate_uuid7()
        now = datetime.now(timezone.utc)
        self.spark.sql(f"""
        INSERT INTO {self._t('connector_schema_change_event')}
        (change_event_id, table_id, connector_instance_id, change_type, column_name, old_type, new_type, detected_at, applied_at)
        VALUES (
            {self._s(event_id)}, {self._s(table_id)}, {self._s(self.connector_instance_id)},
            {self._s(change_type)}, {self._s(column_name) if column_name else 'NULL'},
            {self._s(old_type) if old_type else 'NULL'}, {self._s(new_type) if new_type else 'NULL'},
            {self._fmt(now)}, {self._fmt(now)}
        )
        """)

    # ------------------------------------------------------------------
    # SQL formatting helpers
    # ------------------------------------------------------------------

    @staticmethod
    def _escape(value: str) -> str:
        return value.replace("'", "''")

    @classmethod
    def _s(cls, value: str | None) -> str:
        return "NULL" if value is None else f"'{cls._escape(str(value))}'"

    @staticmethod
    def _n(value) -> str:
        return "NULL" if value is None else str(value)

    @staticmethod
    def _fmt(dt: datetime) -> str:
        return f"TIMESTAMP '{dt.strftime('%Y-%m-%d %H:%M:%S.%f')}'"


# ---------------------------------------------------------------------------
# Standalone debug entry point
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    print("UUIDv7 samples:")
    for _ in range(3):
        print(f"  {generate_uuid7()}")

    print("\nSQL literal formatting samples:")
    mgr = ControlPlaneManager.__new__(ControlPlaneManager)
    for label, result in [
        ("string", mgr._s("O'Brien")),
        ("none", mgr._s(None)),
        ("int", mgr._n(42)),
        ("ts", mgr._fmt(datetime(2024, 6, 1, 12, 0, 0))),
    ]:
        print(f"  {label}: {result}")

