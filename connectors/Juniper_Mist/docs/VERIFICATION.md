# Juniper Mist connector — after a run

Use this after a Databricks Job run. Replace the placeholders with the catalogs and schemas from the job parameters.

| Placeholder | Meaning | Example |
|---|---|---|
| `<META>` | Metadata catalog and schema | `metadata_dev.jetstream` or `metadata_dev.jetstream_juniper_mist` |
| `<DEST>` | Destination catalog and schema | `raw_dev.juniper_mist` |
| `<INSTANCE>` | `instance_name` from the job | `juniper_mist` |

Take `<RUN_ID>` from the notebook output, or from the latest `connector_run` row.

## 1. First successful run

The Databricks task is Success, and the notebook prints a `run_id`.

```sql
SELECT connector_type_id, is_active
FROM <META>.connector_registry
WHERE connector_type_id = 'juniper_mist';

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
SELECT source_object, status, rows_read, rows_inserted, rows_updated
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

With `enabled_objects` empty, expect a row per object in the README table. Optional objects may be `success` with a warning in `connector_log` when the API returns nothing. A primary object with `status = failed` fails the run. Every registry row is a table in `<DEST>`.

## 2. Destination tables

Full-load objects, for example `sites` and `inventory`:

```sql
SELECT COUNT(*) AS rows, COUNT(id) AS ids
FROM <DEST>.sites
WHERE NOT _jetstream_deleted;
```

`rows` equals `ids` for objects whose key is `id`. `_jetstream_synced` and `_jetstream_deleted` exist.

Incremental objects (`zone_visits`, `zone_visits_by_zone`, `clients_search`, `guests_search`) accumulate history. A second run should not reload the whole history. Check `connector_state` for a non-null cursor:

```sql
SELECT object_name, state_value
FROM <META>.connector_state
WHERE connector_instance_id = (
  SELECT connector_instance_id FROM <META>.connector_instance
  WHERE instance_name = '<INSTANCE>'
)
  AND object_name = 'zone_visits';
```

Snapshot objects (`device_stats`, `clients`, `map_clients`, `unconnected_clients`) append. Duplicate keys across runs are expected. Read one batch with:

```sql
SELECT *
FROM <DEST>.clients
WHERE _jetstream_synced = (SELECT MAX(_jetstream_synced) FROM <DEST>.clients);
```

## 3. Second run

Run the same job again without changing parameters.

The same `connector_instance` row is reused. Full-load objects that did not change in Mist show `rows_inserted` near zero. Snapshot objects insert another batch. Incremental objects insert only the new window.

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

Expect `status = failed` and a severe log row. The message names the missing key `juniper_mist_token`.

## 5. Health

```sql
SELECT instance_name, is_stale
FROM <META>.connector_health
WHERE instance_name = '<INSTANCE>';
```

After a success inside `sla_target_minutes`, `is_stale` is false.
