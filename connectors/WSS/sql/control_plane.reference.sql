-- Jetstream control-plane DDL.
--
-- Run this once in the Unity Catalog that will hold connector metadata.
-- The statements below create metadata_prod.jetstream. Replace metadata_prod
-- with metadata_dev, or with your own catalog, before running.
--
-- Connector jobs read and write rows in these tables. They do not create them.
-- A dev sandbox may use a schema named jetstream_<connector>, for example
-- jetstream_wss. Production uses the shared jetstream schema.
--
-- Constraints note: spec §6 declares informational PK/FK/UNIQUE constraints
-- on each table. Databricks/UC currently rejects these on Iceberg tables
-- ("Tables with constraints are required to use the DELTA provider"), so
-- they are omitted here. Uniqueness is enforced in application code
-- (find_existing_run, upsert_state MERGE, _find_or_create_instance, etc.),
-- which matches the spec's own "informational, non-enforced" wording (§3.6).

CREATE SCHEMA IF NOT EXISTS metadata_prod.jetstream;

-- 6.1 connector_registry
CREATE TABLE metadata_prod.jetstream.connector_registry (
    connector_type_id    STRING  NOT NULL  COMMENT 'Slug identifier, e.g. wss',
    connector_type_name  STRING  NOT NULL,
    category             STRING  NOT NULL  COMMENT 'file_object_storage | database | application_api | event_streaming',
    description          STRING,
    repo_path            STRING,
    connector_version    STRING,
    maintainer           STRING,
    is_active            BOOLEAN NOT NULL,
    created_at           TIMESTAMP NOT NULL,
    updated_at           TIMESTAMP NOT NULL
) USING ICEBERG
TBLPROPERTIES ('format-version' = '3');

-- 6.2 connector_manifest
CREATE TABLE metadata_prod.jetstream.connector_manifest (
    manifest_id          STRING  NOT NULL  COMMENT 'UUIDv7',
    connector_type_id    STRING  NOT NULL,
    manifest_version     STRING  NOT NULL,
    config_schema        STRING  NOT NULL  COMMENT 'JSON Schema document compiled from parameters.yaml',
    is_current           BOOLEAN NOT NULL,
    created_at           TIMESTAMP NOT NULL
) USING ICEBERG
TBLPROPERTIES ('format-version' = '3');

-- 6.3 connector_instance
CREATE TABLE metadata_prod.jetstream.connector_instance (
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
) USING ICEBERG
TBLPROPERTIES ('format-version' = '3');

-- 6.4 connector_run
CREATE TABLE metadata_prod.jetstream.connector_run (
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
    run_date                 DATE    NOT NULL  COMMENT 'Partition column'
) USING ICEBERG
PARTITIONED BY (run_date)
TBLPROPERTIES ('format-version' = '3');

-- 6.5 connector_object_run
CREATE TABLE metadata_prod.jetstream.connector_object_run (
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
    metrics_facet          STRING            COMMENT 'Names the shape of extended_metrics for this row, e.g. s3_metrics, kafka_metrics, database_metrics, file_metrics. Mandatory for unstructured/file-copy destination objects.',
    extended_metrics       STRING            COMMENT 'JSON. Connector-specific metrics (e.g. file_etag, file_size_bytes for s3; partition, offset_start for kafka). Query with get_json_object().',
    run_date               DATE    NOT NULL  COMMENT 'Partition column'
) USING ICEBERG
PARTITIONED BY (run_date)
TBLPROPERTIES ('format-version' = '3');

-- 6.6 connector_table_registry
CREATE TABLE metadata_prod.jetstream.connector_table_registry (
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
    created_at             TIMESTAMP NOT NULL,
    updated_at             TIMESTAMP NOT NULL
) USING ICEBERG
TBLPROPERTIES ('format-version' = '3');

-- 6.7 connector_schema_change_event
CREATE TABLE metadata_prod.jetstream.connector_schema_change_event (
    change_event_id        STRING  NOT NULL  COMMENT 'UUIDv7',
    table_id               STRING  NOT NULL,
    connector_instance_id  STRING  NOT NULL  COMMENT 'Denormalized for direct filtering without join',
    change_type            STRING  NOT NULL  COMMENT 'create_table | add_column | drop_column | alter_type | drop_table',
    column_name            STRING,
    old_type               STRING,
    new_type               STRING,
    detected_at            TIMESTAMP NOT NULL,
    applied_at             TIMESTAMP
) USING ICEBERG
TBLPROPERTIES ('format-version' = '3');

-- 6.8 connector_state
CREATE TABLE metadata_prod.jetstream.connector_state (
    state_id               STRING  NOT NULL  COMMENT 'UUIDv7',
    connector_instance_id  STRING  NOT NULL,
    object_name            STRING  NOT NULL  COMMENT 'File pattern, table, stream topic, or __global__',
    state_key              STRING  NOT NULL  COMMENT 'e.g. etag | last_processed_key | watermark | offset',
    state_value            STRING            COMMENT 'JSON-encoded or scalar, interpreted per state_key',
    updated_at             TIMESTAMP NOT NULL
) USING ICEBERG
TBLPROPERTIES ('format-version' = '3');

-- 6.9 connector_log
CREATE TABLE metadata_prod.jetstream.connector_log (
    log_id                 STRING  NOT NULL  COMMENT 'UUIDv7',
    connector_instance_id  STRING  NOT NULL,
    run_id                 STRING            COMMENT 'Nullable — not every event is tied to a run',
    event_type             STRING  NOT NULL  COMMENT 'e.g. sync_start | sync_end | auth_error | schema_change_detected | config_validation_failed',
    severity               STRING  NOT NULL  COMMENT 'INFO | WARNING | SEVERE',
    message                STRING,
    event_data             STRING            COMMENT 'JSON-encoded event-specific payload',
    created_at             TIMESTAMP NOT NULL,
    log_date               DATE    NOT NULL  COMMENT 'Partition column'
) USING ICEBERG
PARTITIONED BY (log_date)
TBLPROPERTIES ('format-version' = '3');

-- 10. connector_health (view) — recommended operational view (staleness monitor)
CREATE OR REPLACE VIEW metadata_prod.jetstream.connector_health AS
WITH latest_run AS (
    SELECT
        *,
        ROW_NUMBER() OVER (
            PARTITION BY connector_instance_id
            ORDER BY start_time DESC
        ) AS rn
    FROM metadata_prod.jetstream.connector_run
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
FROM metadata_prod.jetstream.connector_instance i
LEFT JOIN latest_run lr
    ON lr.connector_instance_id = i.connector_instance_id
   AND lr.rn = 1;
