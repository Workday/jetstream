-- Jetstream control-plane DDL.
--
-- Run this once in the Unity Catalog that will hold connector metadata.
-- The statements below create metadata_prod.jetstream. Replace metadata_prod
-- with metadata_dev, or with your own catalog, before running.
--
-- Connector jobs read and write rows in these tables. They do not create them.
-- A dev sandbox may use a schema named jetstream_<connector>, for example
-- jetstream_juniper_mist. Production uses the shared jetstream schema.
--
-- Constraints note: informational primary keys are omitted. Databricks Unity
-- Catalog rejects PRIMARY KEY constraints on Iceberg tables
-- ("Tables with constraints are required to use the DELTA provider").
-- Uniqueness is enforced by the connector's MERGE statements.

CREATE SCHEMA IF NOT EXISTS metadata_prod.jetstream;

-- ----------------------------------------------------------------------------
-- connector_registry
-- One row per connector type, workspace-wide.
-- ----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS metadata_prod.jetstream.connector_registry (
    connector_type_id   STRING        NOT NULL,
    connector_type_name STRING        NOT NULL,
    category            STRING        NOT NULL,
    description         STRING,
    repo_path           STRING,
    connector_version   STRING,
    maintainer          STRING,
    is_active           BOOLEAN       NOT NULL DEFAULT TRUE,
    created_at          TIMESTAMP     NOT NULL,
    updated_at          TIMESTAMP     NOT NULL
) USING ICEBERG;

-- ----------------------------------------------------------------------------
-- connector_manifest
-- Compiled JSON Schema from parameters.yaml; versioned.
-- ----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS metadata_prod.jetstream.connector_manifest (
    manifest_id         STRING        NOT NULL,
    connector_type_id   STRING        NOT NULL,
    manifest_version    STRING        NOT NULL,
    config_schema       STRING        NOT NULL,
    is_current          BOOLEAN       NOT NULL DEFAULT TRUE,
    created_at          TIMESTAMP     NOT NULL
) USING ICEBERG;

-- ----------------------------------------------------------------------------
-- connector_instance
-- One row per Databricks Job; keyed on databricks_job_id.
-- ----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS metadata_prod.jetstream.connector_instance (
    connector_instance_id   STRING    NOT NULL,
    connector_type_id       STRING    NOT NULL,
    instance_name           STRING    NOT NULL,
    environment             STRING    NOT NULL,
    owner_team              STRING,
    source_config           STRING,
    config_manifest_version STRING,
    databricks_job_id       STRING    NOT NULL,
    destination_catalog     STRING    NOT NULL,
    destination_schema      STRING    NOT NULL,
    schedule_cron           STRING,
    sla_target_minutes      INTEGER,
    status                  STRING    NOT NULL DEFAULT 'active',
    tags                    STRING,
    created_by              STRING,
    created_at              TIMESTAMP NOT NULL,
    updated_at              TIMESTAMP NOT NULL
) USING ICEBERG;

-- ----------------------------------------------------------------------------
-- connector_run
-- One row per job execution.
-- ----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS metadata_prod.jetstream.connector_run (
    run_id                  STRING    NOT NULL,
    connector_instance_id   STRING    NOT NULL,
    idempotency_key         STRING    NOT NULL,
    run_type                STRING    NOT NULL,
    trigger_type            STRING,
    status                  STRING    NOT NULL,
    start_time              TIMESTAMP NOT NULL,
    end_time                TIMESTAMP,
    duration_seconds        BIGINT,
    rows_ingested_total     BIGINT    DEFAULT 0,
    rows_inserted           BIGINT    DEFAULT 0,
    rows_updated            BIGINT    DEFAULT 0,
    rows_deleted            BIGINT    DEFAULT 0,
    bytes_processed         BIGINT,
    tables_synced           INTEGER   DEFAULT 0,
    databricks_workspace_id STRING,
    databricks_job_id       STRING,
    databricks_job_run_id   STRING,
    error_code              STRING,
    error_message           STRING,
    retry_count             INTEGER   DEFAULT 0,
    triggered_by            STRING,
    run_date                DATE      NOT NULL
) USING ICEBERG
PARTITIONED BY (run_date);

-- ----------------------------------------------------------------------------
-- connector_object_run
-- One row per API object per run.
-- ----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS metadata_prod.jetstream.connector_object_run (
    object_run_id           STRING    NOT NULL,
    run_id                  STRING    NOT NULL,
    connector_instance_id   STRING    NOT NULL,
    destination_table       STRING    NOT NULL,
    source_object           STRING    NOT NULL,
    status                  STRING    NOT NULL,
    rows_read               BIGINT    DEFAULT 0,
    rows_inserted           BIGINT    DEFAULT 0,
    rows_updated            BIGINT    DEFAULT 0,
    rows_deleted            BIGINT    DEFAULT 0,
    rows_unchanged          BIGINT    DEFAULT 0,
    bytes_written           BIGINT,
    start_time              TIMESTAMP NOT NULL,
    end_time                TIMESTAMP,
    error_message           STRING,
    run_date                DATE      NOT NULL
) USING ICEBERG
PARTITIONED BY (run_date);

-- ----------------------------------------------------------------------------
-- connector_table_registry
-- One row per destination table.
-- ----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS metadata_prod.jetstream.connector_table_registry (
    table_id                STRING    NOT NULL,
    connector_instance_id   STRING    NOT NULL,
    destination_catalog     STRING    NOT NULL,
    destination_schema      STRING    NOT NULL,
    destination_table       STRING    NOT NULL,
    source_object           STRING,
    primary_key_columns     ARRAY<STRING>,
    table_format            STRING    NOT NULL DEFAULT 'iceberg',
    is_active               BOOLEAN   NOT NULL DEFAULT TRUE,
    first_synced_at         TIMESTAMP NOT NULL,
    last_synced_at          TIMESTAMP NOT NULL,
    row_count_estimate      BIGINT,
    created_at              TIMESTAMP NOT NULL,
    updated_at              TIMESTAMP NOT NULL
) USING ICEBERG;

-- ----------------------------------------------------------------------------
-- connector_state
-- Cursor/watermark state between runs.
-- ----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS metadata_prod.jetstream.connector_state (
    state_id                STRING    NOT NULL,
    connector_instance_id   STRING    NOT NULL,
    object_name             STRING    NOT NULL,
    state_key               STRING    NOT NULL,
    state_value             STRING,
    updated_at              TIMESTAMP NOT NULL
) USING ICEBERG;

-- ----------------------------------------------------------------------------
-- connector_schema_change_event
-- Schema drift history.
-- ----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS metadata_prod.jetstream.connector_schema_change_event (
    change_event_id         STRING    NOT NULL,
    table_id                STRING    NOT NULL,
    connector_instance_id   STRING    NOT NULL,
    change_type             STRING    NOT NULL,
    column_name             STRING,
    old_type                STRING,
    new_type                STRING,
    detected_at             TIMESTAMP NOT NULL,
    applied_at              TIMESTAMP
) USING ICEBERG;

-- ----------------------------------------------------------------------------
-- connector_log
-- Append-only event log.
-- ----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS metadata_prod.jetstream.connector_log (
    log_id                  STRING    NOT NULL,
    connector_instance_id   STRING    NOT NULL,
    run_id                  STRING,
    event_type              STRING    NOT NULL,
    severity                STRING    NOT NULL,
    message                 STRING,
    event_data              STRING,
    created_at              TIMESTAMP NOT NULL,
    log_date                DATE      NOT NULL
) USING ICEBERG
PARTITIONED BY (log_date);

-- ----------------------------------------------------------------------------
-- connector_health (view)
-- Derived staleness monitor.
-- ----------------------------------------------------------------------------
CREATE OR REPLACE VIEW metadata_prod.jetstream.connector_health AS
SELECT
    ci.connector_instance_id,
    ci.instance_name,
    ci.environment,
    ci.destination_catalog,
    ci.destination_schema,
    ci.sla_target_minutes,
    cr.run_id          AS last_run_id,
    cr.status          AS last_run_status,
    cr.start_time      AS last_run_start,
    cr.end_time        AS last_run_end,
    cr.duration_seconds AS last_duration_seconds,
    TIMESTAMPDIFF(MINUTE, cr.end_time, current_timestamp()) AS minutes_since_last_run,
    CASE
        WHEN cr.status = 'success' AND TIMESTAMPDIFF(MINUTE, cr.end_time, current_timestamp()) <= ci.sla_target_minutes
            THEN 'healthy'
        WHEN cr.status IN ('failed', 'failed_with_error')
            THEN 'failed'
        WHEN TIMESTAMPDIFF(MINUTE, cr.end_time, current_timestamp()) > ci.sla_target_minutes
            THEN 'stale'
        ELSE 'unknown'
    END AS health_status
FROM metadata_prod.jetstream.connector_instance ci
LEFT JOIN (
    SELECT connector_instance_id, run_id, status, start_time, end_time, duration_seconds,
           ROW_NUMBER() OVER (PARTITION BY connector_instance_id ORDER BY start_time DESC) AS rn
    FROM metadata_prod.jetstream.connector_run
) cr ON cr.connector_instance_id = ci.connector_instance_id AND cr.rn = 1
WHERE ci.status = 'active';

-- ============================================================================
-- Useful lineage queries for Juniper Mist instances
-- ============================================================================

-- F1: What tables does this instance sync?
-- SELECT * FROM metadata_prod.jetstream.connector_table_registry
-- WHERE connector_instance_id = '<instance_id>' AND is_active = true;

-- F2: What did a specific run touch?
-- SELECT source_object, status, rows_read, rows_inserted, rows_updated, rows_deleted, bytes_written
-- FROM metadata_prod.jetstream.connector_object_run
-- WHERE run_id = '<run_id>'
-- ORDER BY source_object;

-- F3: Last 7 days of runs with status
-- SELECT run_id, run_type, status, start_time, duration_seconds,
--        rows_ingested_total, rows_inserted, rows_updated, tables_synced
-- FROM metadata_prod.jetstream.connector_run
-- WHERE connector_instance_id = '<instance_id>'
--   AND run_date >= current_date() - INTERVAL 7 DAYS
-- ORDER BY start_time DESC;

-- F4: All failures with causes
-- SELECT run_id, status, start_time, error_code, error_message
-- FROM metadata_prod.jetstream.connector_run
-- WHERE connector_instance_id = '<instance_id>'
--   AND status IN ('failed', 'failed_with_error')
-- ORDER BY start_time DESC;

-- F5: connector_health staleness check
-- SELECT * FROM metadata_prod.jetstream.connector_health
-- WHERE connector_type_id = 'juniper_mist';
