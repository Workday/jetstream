# ServiceNow connector — after a run

Use this after a Databricks Job run. Replace the placeholders with the catalogs and schemas from the job parameters.

| Placeholder | Meaning | Example |
|---|---|---|
| `<META>` | Metadata catalog and schema | `metadata_dev.jetstream` or `metadata_dev.jetstream_servicenow` |
| `<DEST>` | Destination catalog and schema | `raw_dev.servicenow` |
| `<INSTANCE>` | `instance_name` from the job | `servicenow` |
| `<TABLE>` | ServiceNow table on this task | `incident` |

Take `<RUN_ID>` from the notebook output, or from the latest `connector_run` row.

## 1. First successful run

The Databricks task is Success, and the notebook prints a `run_id`.

```sql
SELECT connector_type_id, is_active
FROM <META>.connector_registry
WHERE connector_type_id = 'servicenow';

SELECT instance_name, status, destination_catalog, destination_schema
FROM <META>.connector_instance
WHERE instance_name = '<INSTANCE>';

SELECT run_id, status, run_type, rows_ingested_total, databricks_job_id, idempotency_key
FROM <META>.connector_run
WHERE connector_instance_id = (
  SELECT connector_instance_id FROM <META>.connector_instance
  WHERE instance_name = '<INSTANCE>'
)
ORDER BY start_time DESC
LIMIT 5;
```

Expect `is_active = true`, instance `status = active`, and the latest run `status = success` with `databricks_job_id` populated. A three-task job writes three `connector_run` rows for one parent job run. Their `idempotency_key` values end in the task key.

```sql
SELECT source_object, destination_table, status, rows_read, rows_inserted
FROM <META>.connector_object_run
WHERE run_id = '<RUN_ID>'
ORDER BY source_object;

SELECT destination_catalog, destination_schema, destination_table, source_object
FROM <META>.connector_table_registry
WHERE connector_instance_id = (
  SELECT connector_instance_id FROM <META>.connector_instance
  WHERE instance_name = '<INSTANCE>'
);

SELECT object_name, state_key, state_value
FROM <META>.connector_state
WHERE connector_instance_id = (
  SELECT connector_instance_id FROM <META>.connector_instance
  WHERE instance_name = '<INSTANCE>'
)
ORDER BY object_name, state_key;
```

Expect one successful `connector_object_run` for the slice that task owns. `cursor`, `attachment_cursor`, and `journal_cursor` are separate state keys. A base task writes `cursor` only.

## 2. Destination tables

Parent and journal tables are append-only. Row counts grow when ServiceNow returns rows. `_jetstream_deleted` stays false. `_jetstream_synced` is the batch timestamp. Filter it when you want one poll.

```sql
SELECT COUNT(*) AS rows
FROM <DEST>.<TABLE>;

SELECT _jetstream_source_object, COUNT(*) AS rows
FROM <DEST>.<TABLE>
GROUP BY _jetstream_source_object;
```

Attachment metadata, after an attachments task:

```sql
SELECT download_status, COUNT(*) AS rows
FROM <DEST>.<TABLE>_attachments
GROUP BY download_status;
```

Binaries are files under `/Volumes/<destination_catalog>/<destination_schema>/attachments/<TABLE>/`.

Journal rows, after a journals task:

```sql
SELECT element, COUNT(*) AS rows
FROM <DEST>.<TABLE>_journals
GROUP BY element
ORDER BY rows DESC;
```

## 3. Second run

Run the same job again without changing parameters.

The same `connector_instance` row is reused. A base task appends rows whose watermark is newer than `cursor` minus `lookback_minutes`. An unchanged table can append zero rows and still move nothing when the batch is empty. The cursor stays at the previous value when the batch is empty and a cursor already exists.

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
SELECT instance_name, last_run_status, is_stale, minutes_since_last_run
FROM <META>.connector_health
WHERE instance_name = '<INSTANCE>';
```

After a success inside `sla_target_minutes`, `is_stale` is false.
