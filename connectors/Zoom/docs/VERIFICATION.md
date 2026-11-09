# Zoom connector — after a run

Use this after a Databricks Job run. Replace the placeholders with the catalogs and schemas from the job parameters.

| Placeholder | Meaning | Example |
|---|---|---|
| `<META>` | Metadata catalog and schema | `metadata_dev.jetstream` or `metadata_dev.jetstream_zoom` |
| `<DEST>` | Destination catalog and schema | `raw_dev.zoom` |
| `<INSTANCE>` | `instance_name` from the job | `zoom` |

Take `<RUN_ID>` from the notebook output, or from the latest `connector_run` row.

## 1. First successful run

The Databricks task is Success, and the notebook prints a `run_id`.

```sql
SELECT connector_type_id, is_active
FROM <META>.connector_registry
WHERE connector_type_id = 'zoom';

SELECT instance_name, status, destination_catalog, destination_schema
FROM <META>.connector_instance
WHERE instance_name = '<INSTANCE>';

SELECT run_id, status, rows_ingested_total, databricks_job_id
FROM <META>.connector_run
WHERE connector_instance_id = (
  SELECT connector_instance_id FROM <META>.connector_instance
  WHERE instance_name = '<INSTANCE>'
)
ORDER BY start_time DESC
LIMIT 1;
```

Expect `is_active = true`, instance `status = active`, and the latest run `status = success` with `databricks_job_id` populated.

```sql
SELECT source_object, status, rows_read, rows_inserted, rows_updated, rows_deleted
FROM <META>.connector_object_run
WHERE run_id = '<RUN_ID>'
ORDER BY source_object;

SELECT destination_catalog, destination_schema, destination_table
FROM <META>.connector_table_registry
WHERE connector_instance_id = (
  SELECT connector_instance_id FROM <META>.connector_instance
  WHERE instance_name = '<INSTANCE>'
);
```

Expect one successful `connector_object_run` per object you enabled, and one registry row per destination table. Those rows belong to `<DEST>` only.

## 2. Destination tables

Primary objects, when enabled: `users`, `groups`, `rooms`, `room_locations`, `room_devices`.

Optional objects, only when listed in `optional_objects_enabled`: `dashboard_meetings`, `dashboard_meeting_participants`.

```sql
SELECT COUNT(*) AS rows, COUNT(id) AS ids
FROM <DEST>.users
WHERE NOT _jetstream_deleted;
```

Repeat for each synced table. `rows` equals `ids`. `_jetstream_synced` and `_jetstream_deleted` exist. `_jetstream_deleted` is false for rows the latest snapshot returned.

## 3. Second run

Run the same job again without changing parameters.

The same `connector_instance` row is reused. For a primary object that did not change in Zoom, `rows_inserted` is near zero and `rows_updated` covers the existing keys. Incremental objects (`dashboard_meetings`, `dashboard_meeting_participants`) add only the window since the stored cursor.

## 4. Forced failure

Set `secret_scope` to a scope that does not exist and trigger the job once. Then put the real scope back.

```sql
SELECT status, error_code, error_message
FROM <META>.connector_run
ORDER BY start_time DESC
LIMIT 1;

SELECT event_type, severity, message
FROM <META>.connector_log
WHERE run_id = '<RUN_ID>' AND severity = 'SEVERE';
```

Expect `status = failed` and a severe log row. No new destination rows are written for that run.

## 5. Health

```sql
SELECT instance_name, is_stale, minutes_since_last_success
FROM <META>.connector_health
WHERE instance_name = '<INSTANCE>';
```

After a success inside `sla_target_minutes`, `is_stale` is false.
