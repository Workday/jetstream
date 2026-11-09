# WSS connector — after a run

Use this after a Databricks Job run. Replace the placeholders with the catalogs and schemas from the job parameters.

| Placeholder | Meaning | Example |
|---|---|---|
| `<META>` | Metadata catalog and schema | `metadata_dev.jetstream` or `metadata_dev.jetstream_wss` |
| `<DEST>` | Destination catalog and schema | `raw_dev.wss` |
| `<INSTANCE>` | `instance_name` from the job | `wss` |
| `<OBJECT>` | One synced object | `contracts` |

Take `<RUN_ID>` from the notebook output, or from the latest `connector_run` row.

## 1. First successful run

The Databricks task is Success, and the notebook prints a run summary.

```sql
SELECT connector_type_id, connector_type_name, is_active
FROM <META>.connector_registry
WHERE connector_type_id = 'wss';

SELECT instance_name, status, destination_catalog, destination_schema
FROM <META>.connector_instance
WHERE instance_name = '<INSTANCE>';

SELECT run_id, status, run_type, rows_ingested_total, databricks_job_id
FROM <META>.connector_run
WHERE connector_instance_id = (
  SELECT connector_instance_id FROM <META>.connector_instance
  WHERE instance_name = '<INSTANCE>'
)
ORDER BY start_time DESC
LIMIT 5;
```

Expect `is_active = true`, instance `status = active`, and a latest run `status = success`.

```sql
SELECT source_object, destination_table, status, rows_read, rows_inserted
FROM <META>.connector_object_run
WHERE run_id = '<RUN_ID>'
ORDER BY source_object;

SELECT object_name, state_key, state_value
FROM <META>.connector_state
WHERE connector_instance_id = (
  SELECT connector_instance_id FROM <META>.connector_instance
  WHERE instance_name = '<INSTANCE>'
)
ORDER BY object_name, state_key;
```

Expect one `connector_object_run` per object that the task actually called. A child object whose parent table is missing is skipped.

## 2. Destination tables

```sql
SELECT COUNT(*) AS rows
FROM <DEST>.<OBJECT>;

SELECT _jetstream_source_object, COUNT(*) AS rows
FROM <DEST>.<OBJECT>
GROUP BY _jetstream_source_object;
```

`_jetstream_synced` and `_jetstream_deleted` exist. Loads append, so a second run can add rows for the same source id.

Response files are under `/Volumes/<destination_catalog>/<destination_schema>/api_responses/<OBJECT>/`. Attachment files are under `/Volumes/<destination_catalog>/<destination_schema>/attachments`.

## 3. Second run

Run the same job again without changing parameters.

The same `connector_instance` row is reused. Incremental objects request the window after the stored cursor. Full objects append another snapshot.

## 4. Forced failure

Set `secret_scope` to a scope that does not exist and trigger the job once. Then put the real scope back.

```sql
SELECT status, error_code, error_message
FROM <META>.connector_run
ORDER BY start_time DESC
LIMIT 1;

SELECT event_type, severity, message
FROM <META>.connector_log
WHERE severity = 'SEVERE'
ORDER BY created_at DESC
LIMIT 20;
```

Expect a failed run. No new destination rows are written for the objects that failed before extract.

## 5. Health

```sql
SELECT instance_name, last_run_status, is_stale, minutes_since_last_run
FROM <META>.connector_health
WHERE instance_name = '<INSTANCE>';
```

After a success inside `sla_target_minutes`, `is_stale` is false.
