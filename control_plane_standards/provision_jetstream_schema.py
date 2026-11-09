# Databricks notebook source
# provision_jetstream_schema.py
#
# Provisions the Jetstream control plane: a schema named "jetstream", the nine
# control plane tables, and the connector_health view, in the catalog passed
# as a job parameter.
#
# Example:
#   catalog_name = my_metadata_catalog  ->  my_metadata_catalog.jetstream
#
# Every connector instance registers into this schema and reports through it.
# The definitions match the control plane standard in
# control_plane_standards/jetstream-control-plane-and-data-standards.md.
#
# This script never drops the schema or any table. It uses CREATE SCHEMA IF NOT
# EXISTS and CREATE TABLE IF NOT EXISTS throughout, since the schema holds live
# connector data once any connector instance has registered into it. Re-running
# this job is safe and idempotent: it creates whatever is missing and leaves
# existing tables and rows untouched.

# COMMAND ----------

dbutils.widgets.text("catalog_name", "", "Metadata catalog name")

# COMMAND ----------

catalog_name = dbutils.widgets.get("catalog_name").strip()

if not catalog_name:
    raise ValueError(
        "catalog_name is required and cannot be empty. "
        "Pass the name of the catalog that should hold the Jetstream "
        "control plane as the catalog_name job parameter."
    )

CATALOG = catalog_name
SCHEMA  = "jetstream"
FQS     = f"{CATALOG}.{SCHEMA}"          # fully qualified schema

print(f"catalog           : {CATALOG}")
print(f"schema            : {SCHEMA}")
print(f"fully qualified   : {FQS}")

# COMMAND ----------

# ---------------------------------------------------------------------------
# Create the schema if it does not already exist
# ---------------------------------------------------------------------------

spark.sql(f"CREATE SCHEMA IF NOT EXISTS {FQS}")
print(f"Schema '{FQS}' ready.")

# COMMAND ----------

# ---------------------------------------------------------------------------
# Helper
# ---------------------------------------------------------------------------

def run_ddl(label: str, sql: str) -> None:
    print(f"Creating {label} if not present...")
    spark.sql(sql)
    print(f"  OK - {label}")

# COMMAND ----------

# ---------------------------------------------------------------------------
# 1. connector_registry
# ---------------------------------------------------------------------------

run_ddl("connector_registry", f"""
CREATE TABLE IF NOT EXISTS {FQS}.connector_registry (
    connector_type_id    STRING  NOT NULL  COMMENT 'Slug identifier, e.g. s3, zoom, evisort',
    connector_type_name  STRING  NOT NULL,
    category             STRING  NOT NULL  COMMENT 'file_object_storage | database | application_api | event_streaming',
    description          STRING,
    repo_path            STRING,
    connector_version    STRING,
    maintainer           STRING,
    is_active            BOOLEAN NOT NULL,
    created_at           TIMESTAMP NOT NULL,
    updated_at           TIMESTAMP NOT NULL
)
USING ICEBERG
TBLPROPERTIES ('format-version' = '3')
""")

# COMMAND ----------

# ---------------------------------------------------------------------------
# 2. connector_manifest
# ---------------------------------------------------------------------------

run_ddl("connector_manifest", f"""
CREATE TABLE IF NOT EXISTS {FQS}.connector_manifest (
    manifest_id          STRING  NOT NULL  COMMENT 'UUIDv7',
    connector_type_id    STRING  NOT NULL,
    manifest_version     STRING  NOT NULL,
    config_schema        STRING  NOT NULL  COMMENT 'JSON Schema document compiled from parameters.yaml',
    is_current           BOOLEAN NOT NULL,
    created_at           TIMESTAMP NOT NULL
)
USING ICEBERG
TBLPROPERTIES ('format-version' = '3')
""")

# COMMAND ----------

# ---------------------------------------------------------------------------
# 3. connector_instance
# ---------------------------------------------------------------------------

run_ddl("connector_instance", f"""
CREATE TABLE IF NOT EXISTS {FQS}.connector_instance (
    connector_instance_id    STRING  NOT NULL  COMMENT 'UUIDv7, generated once at creation',
    connector_type_id        STRING  NOT NULL,
    instance_name            STRING  NOT NULL,
    environment              STRING  NOT NULL  COMMENT 'dev | staging | prod',
    owner_team               STRING,
    source_config            STRING            COMMENT 'JSON, validated against connector_manifest',
    config_manifest_version  STRING,
    databricks_job_id        STRING            COMMENT 'Set on first run; used to find this row on every subsequent run',
    destination_catalog      STRING  NOT NULL,
    destination_schema       STRING  NOT NULL,
    schedule_cron            STRING,
    sla_target_minutes       INT,
    status                   STRING  NOT NULL  COMMENT 'active | paused | broken | incomplete',
    tags                     MAP<STRING, STRING>,
    created_by               STRING,
    created_at               TIMESTAMP NOT NULL,
    updated_at               TIMESTAMP NOT NULL
)
USING ICEBERG
TBLPROPERTIES ('format-version' = '3')
""")

# COMMAND ----------

# ---------------------------------------------------------------------------
# 4. connector_run
# ---------------------------------------------------------------------------

run_ddl("connector_run", f"""
CREATE TABLE IF NOT EXISTS {FQS}.connector_run (
    run_id                   STRING  NOT NULL  COMMENT 'UUIDv7',
    connector_instance_id    STRING  NOT NULL,
    idempotency_key          STRING            COMMENT 'Prevents duplicate rows on retry',
    run_type                 STRING  NOT NULL  COMMENT 'historical | incremental | full_resync | table_resync',
    trigger_type             STRING  NOT NULL  COMMENT 'scheduled | manual | api | backfill',
    status                   STRING  NOT NULL  COMMENT 'running | success | failed | failed_with_error | cancelled | rescheduled',
    start_time               TIMESTAMP NOT NULL,
    end_time                 TIMESTAMP,
    duration_seconds         BIGINT,
    rows_ingested_total      BIGINT,
    rows_inserted            BIGINT,
    rows_updated             BIGINT,
    rows_deleted             BIGINT,
    bytes_processed          BIGINT,
    tables_synced            INT,
    databricks_workspace_id  STRING  NOT NULL,
    databricks_job_id        STRING  NOT NULL,
    databricks_job_run_id    STRING  NOT NULL,
    error_code               STRING,
    error_message            STRING,
    retry_count              INT     DEFAULT 0,
    triggered_by             STRING,
    run_date                 DATE    NOT NULL   COMMENT 'Partition column'
)
USING ICEBERG
PARTITIONED BY (run_date)
TBLPROPERTIES ('format-version' = '3')
""")

# COMMAND ----------

# ---------------------------------------------------------------------------
# 5. connector_object_run
# ---------------------------------------------------------------------------

run_ddl("connector_object_run", f"""
CREATE TABLE IF NOT EXISTS {FQS}.connector_object_run (
    object_run_id          STRING  NOT NULL  COMMENT 'UUIDv7',
    run_id                 STRING  NOT NULL,
    connector_instance_id  STRING  NOT NULL  COMMENT 'Denormalized for direct filtering without join',
    destination_table      STRING  NOT NULL,
    source_object          STRING  NOT NULL  COMMENT 'File path, source table, or API endpoint',
    status                 STRING  NOT NULL  COMMENT 'success | failed | skipped',
    rows_read              BIGINT,
    rows_inserted          BIGINT,
    rows_updated           BIGINT,
    rows_deleted           BIGINT,
    rows_unchanged         BIGINT,
    bytes_written          BIGINT,
    start_time             TIMESTAMP NOT NULL,
    end_time               TIMESTAMP,
    error_message          STRING,
    metrics_facet           STRING           COMMENT 'Names the JSON shape of extended_metrics, e.g. s3_metrics | kafka_metrics | database_metrics | file_metrics',
    extended_metrics        STRING           COMMENT 'JSON. Connector-specific metrics (e.g. file_etag, file_size_bytes for s3; partition, offset_start for kafka). Query with get_json_object().',
    run_date                DATE    NOT NULL  COMMENT 'Partition column'
)
USING ICEBERG
PARTITIONED BY (run_date)
TBLPROPERTIES ('format-version' = '3')
""")

# COMMAND ----------

# ---------------------------------------------------------------------------
# 6. connector_table_registry
# ---------------------------------------------------------------------------

run_ddl("connector_table_registry", f"""
CREATE TABLE IF NOT EXISTS {FQS}.connector_table_registry (
    table_id               STRING  NOT NULL  COMMENT 'UUIDv7',
    connector_instance_id  STRING  NOT NULL,
    destination_catalog    STRING  NOT NULL,
    destination_schema     STRING  NOT NULL,
    destination_table      STRING  NOT NULL,
    source_object          STRING,
    primary_key_columns    ARRAY<STRING>,
    table_format           STRING,
    is_active              BOOLEAN NOT NULL,
    first_synced_at        TIMESTAMP,
    last_synced_at         TIMESTAMP,
    row_count_estimate     BIGINT,
    created_at              TIMESTAMP NOT NULL,
    updated_at              TIMESTAMP NOT NULL
)
USING ICEBERG
TBLPROPERTIES ('format-version' = '3')
""")

# COMMAND ----------

# ---------------------------------------------------------------------------
# 7. connector_schema_change_event
# ---------------------------------------------------------------------------

run_ddl("connector_schema_change_event", f"""
CREATE TABLE IF NOT EXISTS {FQS}.connector_schema_change_event (
    change_event_id        STRING  NOT NULL  COMMENT 'UUIDv7',
    table_id                STRING  NOT NULL,
    connector_instance_id   STRING  NOT NULL  COMMENT 'Denormalized for direct filtering without join',
    change_type             STRING  NOT NULL  COMMENT 'create_table | add_column | drop_column | alter_type | drop_table',
    column_name             STRING,
    old_type                STRING,
    new_type                STRING,
    detected_at              TIMESTAMP NOT NULL,
    applied_at               TIMESTAMP
)
USING ICEBERG
TBLPROPERTIES ('format-version' = '3')
""")

# COMMAND ----------

# ---------------------------------------------------------------------------
# 8. connector_state
# ---------------------------------------------------------------------------

run_ddl("connector_state", f"""
CREATE TABLE IF NOT EXISTS {FQS}.connector_state (
    state_id                STRING  NOT NULL  COMMENT 'UUIDv7',
    connector_instance_id   STRING  NOT NULL,
    object_name             STRING  NOT NULL  COMMENT 'File pattern, table, stream topic, or __global__',
    state_key               STRING  NOT NULL  COMMENT 'e.g. etag | last_processed_key | watermark | offset',
    state_value              STRING           COMMENT 'JSON-encoded or scalar, interpreted per state_key',
    updated_at                TIMESTAMP NOT NULL
)
USING ICEBERG
TBLPROPERTIES ('format-version' = '3')
""")

# COMMAND ----------

# ---------------------------------------------------------------------------
# 9. connector_log
# ---------------------------------------------------------------------------

run_ddl("connector_log", f"""
CREATE TABLE IF NOT EXISTS {FQS}.connector_log (
    log_id                  STRING  NOT NULL  COMMENT 'UUIDv7',
    connector_instance_id   STRING  NOT NULL,
    run_id                  STRING            COMMENT 'Nullable, not every event is tied to a run',
    event_type              STRING  NOT NULL  COMMENT 'e.g. sync_start | sync_end | auth_error | schema_change_detected | config_validation_failed',
    severity                STRING  NOT NULL  COMMENT 'INFO | WARNING | SEVERE',
    message                 STRING,
    event_data              STRING            COMMENT 'JSON-encoded event-specific payload',
    created_at               TIMESTAMP NOT NULL,
    log_date                 DATE    NOT NULL  COMMENT 'Partition column'
)
USING ICEBERG
PARTITIONED BY (log_date)
TBLPROPERTIES ('format-version' = '3')
""")

# COMMAND ----------

# ---------------------------------------------------------------------------
# 10. connector_health (view)
# ---------------------------------------------------------------------------
#
# CREATE OR REPLACE is used here rather than IF NOT EXISTS, since a view has
# no state of its own to lose. Recreating it on every run keeps its
# definition current with this script.

run_ddl("connector_health (view)", f"""
CREATE OR REPLACE VIEW {FQS}.connector_health AS
WITH latest_run AS (
    SELECT
        *,
        ROW_NUMBER() OVER (
            PARTITION BY connector_instance_id
            ORDER BY start_time DESC
        ) AS rn
    FROM {FQS}.connector_run
)
SELECT
    i.connector_instance_id,
    i.instance_name,
    i.environment,
    i.owner_team,
    i.status                                          AS instance_status,
    lr.run_id                                         AS last_run_id,
    lr.status                                         AS last_run_status,
    lr.start_time                                     AS last_run_start_time,
    lr.end_time                                       AS last_run_end_time,
    lr.duration_seconds                               AS last_run_duration_seconds,
    i.sla_target_minutes,
    TIMESTAMPDIFF(MINUTE, lr.end_time, CURRENT_TIMESTAMP()) AS minutes_since_last_run,
    CASE
        WHEN i.sla_target_minutes > 0
         AND TIMESTAMPDIFF(MINUTE, lr.end_time, CURRENT_TIMESTAMP()) > i.sla_target_minutes
        THEN TRUE
        ELSE FALSE
    END                                               AS is_stale
FROM {FQS}.connector_instance i
LEFT JOIN latest_run lr
    ON lr.connector_instance_id = i.connector_instance_id
    AND lr.rn = 1
""")

# COMMAND ----------

# ---------------------------------------------------------------------------
# Summary
# ---------------------------------------------------------------------------

tables = spark.sql(f"SHOW TABLES IN {FQS}").collect()
print("=" * 60)
print(f"Provisioning complete: {FQS}")
print("=" * 60)
for t in tables:
    print(f"  {t['tableName']}")
print("=" * 60)
