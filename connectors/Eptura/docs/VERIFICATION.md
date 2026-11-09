# Eptura connector — after a run

Use this after a Databricks Job run. Replace the placeholders with the catalogs and schemas from the job parameters.

| Placeholder | Meaning | Example |
|---|---|---|
| `<META>` | Metadata catalog and schema | `metadata_dev.jetstream` or `metadata_dev.jetstream_eptura` |
| `<DEST>` | Destination catalog and schema | `raw_dev.eptura` |
| `<INSTANCE>` | `instance_name` from the job | `eptura` |

Take `<RUN_ID>` from the notebook output, or from the latest `connector_run` row.

## 1. First successful run

The Databricks task is Success, and the notebook prints a `run_id`.

```sql
SELECT connector_type_id, is_active
FROM <META>.connector_registry
WHERE connector_type_id = 'eptura';

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

SELECT destination_table
FROM <META>.connector_table_registry
WHERE connector_instance_id = (
  SELECT connector_instance_id FROM <META>.connector_instance
  WHERE instance_name = '<INSTANCE>'
);
```

Expect one successful object row for each enabled entity. With `enabled_objects` empty, that is `rooms`, `buildings`, `neighborhoods`, `floors`, and `users`. `rows_ingested_total` on the run equals the sum of `rows_read`. Every registry row is a table in `<DEST>`.

## 2. Destination tables

```sql
SELECT COUNT(*) AS rows, COUNT(id) AS ids
FROM <DEST>.rooms
WHERE NOT _jetstream_deleted;
```

Repeat for `buildings`, `neighborhoods`, `floors`, and `users`. `rows` equals `ids`. `_jetstream_synced` and `_jetstream_deleted` exist.

Nested fields are structs, not JSON strings. This path resolves on `rooms`:

```sql
SELECT id, floor.building.name AS building
FROM <DEST>.rooms
WHERE NOT _jetstream_deleted
LIMIT 5;
```

Column notes are in [FIELD_MAPPING.md](FIELD_MAPPING.md).

## 3. Second run

Run the same job again without changing parameters.

The same `connector_instance` row is reused. For an entity that did not change in Eptura, `rows_inserted` is near zero and `rows_updated` covers the existing ids. Rows are not appended as a second copy.

## 4. Forced failure

Set `secret_scope` to a scope that does not exist and trigger the job once. Then put the real scope back.

```sql
SELECT status, error_code, error_message
FROM <META>.connector_run
ORDER BY start_time DESC
LIMIT 1;

SELECT event_type, severity
FROM <META>.connector_log
WHERE run_id = '<RUN_ID>' AND severity = 'SEVERE';
```

Expect `status = failed` and a severe log row.

## 5. Health

```sql
SELECT instance_name, is_stale
FROM <META>.connector_health
WHERE instance_name = '<INSTANCE>';
```

After a success inside `sla_target_minutes`, `is_stale` is false.

## 6. A cancelled job left a run open

Cancelling a Databricks job can leave `connector_run.status = 'running'` because Spark is already gone when the connector tries to close the row.

```sql
SELECT run_id, start_time
FROM <META>.connector_run
WHERE status = 'running'
  AND start_time < current_timestamp() - INTERVAL 30 MINUTES;

UPDATE <META>.connector_run
SET status = 'failed',
    end_time = current_timestamp(),
    error_code = 'JobCancelled',
    error_message = 'Closed by operator after the job was cancelled'
WHERE status = 'running'
  AND start_time < current_timestamp() - INTERVAL 30 MINUTES;
```

Run the `UPDATE` only for rows you have confirmed are abandoned.
