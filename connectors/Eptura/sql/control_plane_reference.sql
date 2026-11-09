-- Jetstream control-plane DDL.
--
-- Run this once in the Unity Catalog that will hold connector metadata.
-- The statements below create metadata_prod.jetstream. Replace metadata_prod
-- with metadata_dev, or with your own catalog, before running.
--
-- Connector jobs read and write rows in these tables. They do not create them.
-- A dev sandbox may use a schema named jetstream_<connector>, for example
-- jetstream_eptura. Production uses the shared jetstream schema.
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
    connector_type_id    STRING NOT NULL COMMENT 'Slug identifier, e.g. s3, eptura, evisort',
    connector_type_name  STRING NOT NULL,
    category             STRING NOT NULL,
    description          STRING,
    repo_path            STRING,
    connector_version    STRING,
    maintainer           STRING,
    is_active            BOOLEAN NOT NULL DEFAULT TRUE,
    created_at           TIMESTAMP NOT NULL,
    updated_at           TIMESTAMP NOT NULL
) USING ICEBERG
TBLPROPERTIES ('format-version' = '3');

-- 6.2 connector_manifest
CREATE TABLE metadata_prod.jetstream.connector_manifest (
    manifest_id        STRING NOT NULL,
    connector_type_id  STRING NOT NULL,
    manifest_version   STRING NOT NULL,
    config_schema      STRING NOT NULL COMMENT 'JSON Schema document',
    is_current         BOOLEAN NOT NULL DEFAULT TRUE,
    created_at         TIMESTAMP NOT NULL
) USING ICEBERG
TBLPROPERTIES ('format-version' = '3');

-- 6.3 connector_instance
CREATE TABLE metadata_prod.jetstream.connector_instance (
    connector_instance_id    STRING NOT NULL,
    connector_type_id        STRING NOT NULL,
    instance_name            STRING NOT NULL,
    environment              STRING NOT NULL,
    owner_team               STRING,
    source_config            STRING COMMENT 'JSON, validated against connector_manifest',
    config_manifest_version  STRING,
    databricks_job_id        STRING,
    destination_catalog      STRING NOT NULL,
    destination_schema       STRING NOT NULL,
    schedule_cron            STRING,
    sla_target_minutes       INT,
    status                   STRING NOT NULL,
    tags                     MAP<STRING, STRING>,
    created_by               STRING,
    created_at               TIMESTAMP NOT NULL,
    updated_at               TIMESTAMP NOT NULL
) USING ICEBERG
TBLPROPERTIES ('format-version' = '3');

-- 6.4 connector_run
CREATE TABLE metadata_prod.jetstream.connector_run (
    run_id                   STRING NOT NULL COMMENT 'Also the OpenLineage run identifier',
    connector_instance_id    STRING NOT NULL,
    idempotency_key          STRING,
    run_type                 STRING NOT NULL,
    trigger_type             STRING NOT NULL,
    status                   STRING NOT NULL,
    start_time               TIMESTAMP NOT NULL,
    end_time                 TIMESTAMP,
    duration_seconds         BIGINT,
    rows_ingested_total      BIGINT,
    rows_inserted            BIGINT,
    rows_updated             BIGINT,
    rows_deleted             BIGINT,
    bytes_processed          BIGINT,
    tables_synced            INT,
    databricks_workspace_id  STRING NOT NULL,
    databricks_job_id        STRING NOT NULL,
    databricks_job_run_id    STRING NOT NULL,
    error_code               STRING,
    error_message            STRING,
    retry_count              INT DEFAULT 0,
    triggered_by             STRING,
    run_date                 DATE NOT NULL
) USING ICEBERG
PARTITIONED BY (run_date)
TBLPROPERTIES ('format-version' = '3');

-- 6.5 connector_object_run
CREATE TABLE metadata_prod.jetstream.connector_object_run (
    object_run_id          STRING NOT NULL,
    run_id                 STRING NOT NULL,
    connector_instance_id  STRING NOT NULL,
    destination_table      STRING NOT NULL,
    source_object          STRING NOT NULL,
    status                 STRING NOT NULL,
    rows_read              BIGINT,
    rows_inserted          BIGINT,
    rows_updated           BIGINT,
    rows_deleted           BIGINT,
    rows_unchanged         BIGINT,
    bytes_written          BIGINT,
    start_time             TIMESTAMP NOT NULL,
    end_time               TIMESTAMP,
    error_message          STRING,
    run_date               DATE NOT NULL
) USING ICEBERG
PARTITIONED BY (run_date)
TBLPROPERTIES ('format-version' = '3');

-- 6.6 connector_table_registry
CREATE TABLE metadata_prod.jetstream.connector_table_registry (
    table_id               STRING NOT NULL,
    connector_instance_id  STRING NOT NULL,
    destination_catalog    STRING NOT NULL,
    destination_schema     STRING NOT NULL,
    destination_table      STRING NOT NULL,
    source_object          STRING,
    primary_key_columns    ARRAY<STRING>,
    table_format           STRING,
    is_active              BOOLEAN NOT NULL DEFAULT TRUE,
    first_synced_at        TIMESTAMP,
    last_synced_at         TIMESTAMP,
    row_count_estimate     BIGINT,
    created_at             TIMESTAMP NOT NULL,
    updated_at             TIMESTAMP NOT NULL
) USING ICEBERG
TBLPROPERTIES ('format-version' = '3');

-- 6.7 connector_schema_change_event
CREATE TABLE metadata_prod.jetstream.connector_schema_change_event (
    change_event_id        STRING NOT NULL,
    table_id               STRING NOT NULL,
    connector_instance_id  STRING NOT NULL,
    change_type            STRING NOT NULL,
    column_name            STRING,
    old_type               STRING,
    new_type               STRING,
    detected_at            TIMESTAMP NOT NULL,
    applied_at             TIMESTAMP
) USING ICEBERG
TBLPROPERTIES ('format-version' = '3');

-- 6.8 connector_state
CREATE TABLE metadata_prod.jetstream.connector_state (
    state_id               STRING NOT NULL,
    connector_instance_id  STRING NOT NULL,
    object_name            STRING NOT NULL,
    state_key              STRING NOT NULL,
    state_value            STRING,
    updated_at             TIMESTAMP NOT NULL
) USING ICEBERG
TBLPROPERTIES ('format-version' = '3');

-- 6.9 connector_log
CREATE TABLE metadata_prod.jetstream.connector_log (
    log_id                 STRING NOT NULL,
    connector_instance_id  STRING NOT NULL,
    run_id                 STRING,
    event_type             STRING NOT NULL,
    severity               STRING NOT NULL,
    message                STRING,
    event_data             STRING,
    created_at             TIMESTAMP NOT NULL,
    log_date               DATE NOT NULL
) USING ICEBERG
PARTITIONED BY (log_date)
TBLPROPERTIES ('format-version' = '3');

-- 8. Recommended operational view (staleness monitor)
CREATE OR REPLACE VIEW metadata_prod.jetstream.connector_health AS
WITH latest_run AS (
    SELECT *,
      ROW_NUMBER() OVER (
        PARTITION BY connector_instance_id ORDER BY start_time DESC
      ) AS rn
    FROM metadata_prod.jetstream.connector_run
)
SELECT
    i.connector_instance_id,
    i.instance_name,
    i.environment,
    i.status AS instance_status,
    lr.status AS last_run_status,
    lr.end_time AS last_run_end_time,
    i.sla_target_minutes,
    TIMESTAMPDIFF(MINUTE, lr.end_time, CURRENT_TIMESTAMP()) AS minutes_since_last_run,
    CASE
      WHEN TIMESTAMPDIFF(MINUTE, lr.end_time, CURRENT_TIMESTAMP()) > i.sla_target_minutes
      THEN TRUE ELSE FALSE
    END AS is_stale
FROM metadata_prod.jetstream.connector_instance i
LEFT JOIN latest_run lr
  ON lr.connector_instance_id = i.connector_instance_id AND lr.rn = 1;
