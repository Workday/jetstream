# ServiceNow connector

A Databricks Job that syncs ServiceNow tables into Unity Catalog Iceberg tables, and records each run in the shared Jetstream control plane.

One job is one connector instance. One task extracts one ServiceNow table and one slice of that table. Every task runs `notebook/01_run.py`. Create another job, with its own parameters, to sync another ServiceNow instance. The code does not change between environments.

Authentication is an OAuth refresh-token grant. The access token is requested from `{instance}/oauth_token.do` on each run.

## What it syncs

`enabled_objects` is the Table API name (`incident`, `change_request`, `problem`, or any other table the account can read). Put one table on each task.

| `sync_target` | ServiceNow source | Destination | Load |
|---|---|---|---|
| `base` | `GET /api/now/table/{table}` | `{table}` | Append. Incremental on `watermark_column` (default `sys_updated_on`). |
| `attachments` | `sys_attachment` where `table_name` is the parent table, then `GET /api/now/attachment/{sys_id}/file` | `{table}_attachments`, plus files on a volume | Append metadata, then MERGE `download_status`. |
| `journals` | `sys_journal_field` where `name` is the parent table | `{table}_journals` | Append. Incremental on `sys_created_on`. |

Each table lands in `<destination_catalog>.<destination_schema>.<name>`.

Every table has `_jetstream_synced`, `_jetstream_deleted`, and `_jetstream_source_object`. Parent and journal loads append the rows returned by that poll. `_jetstream_deleted` stays false. A later edit to a ServiceNow row arrives as another appended row. Comments and work notes belong on the journals task.

Attachment files land at:

`/Volumes/<destination_catalog>/<destination_schema>/attachments/<table>/<parent_sys_id>/<attachment_sys_id>/<file_name>`

Files larger than 100 MB, and empty files, are stored with `download_status=skipped`.

## Before you run

1. A Databricks workspace with Unity Catalog, and a classic job cluster (Databricks Runtime 15.4 LTS or newer, single-user access mode).
2. Two catalogs you control. By default the job looks for a metadata catalog named `metadata_dev` when `environment=dev`, and `metadata_prod` when `environment=prod`. In dev you may pass `metadata_catalog` to use a different catalog. Production rejects any other metadata catalog. `destination_catalog` is always a job parameter. It is not chosen from `environment`.
3. The control-plane tables. The job reads and writes them. It does not create them. Run [sql/control_plane.reference.sql](sql/control_plane.reference.sql) once in the metadata catalog. The script creates `metadata_prod.jetstream`. Replace that catalog name if you are using `metadata_dev` or a dev sandbox catalog. For a sandbox, set `metadata_schema` to `jetstream_servicenow` so this instance does not share the `jetstream` schema. Production must use `jetstream`.
4. A destination schema and an attachments volume. [sql/provision_raw_dev.sql](sql/provision_raw_dev.sql) is an example for `raw_dev.servicenow`.
5. A ServiceNow OAuth application that can issue a refresh token, with read access to the tables you will sync.

The job's run-as identity needs `USE CATALOG` on the metadata and destination catalogs, `USE SCHEMA`, `SELECT`, and `MODIFY` on the metadata schema, `CREATE TABLE` on the destination schema, `READ VOLUME` and `WRITE VOLUME` on the attachments volume, and `READ` on the secret scope. Set the job to `max_concurrent_runs: 1`.

## Secrets

```bash
databricks secrets create-scope jetstream_servicenow
databricks secrets put-secret jetstream_servicenow servicenow-client-id --string-value "<client id>"
databricks secrets put-secret jetstream_servicenow servicenow-client-secret --string-value "<client secret>"
databricks secrets put-secret jetstream_servicenow servicenow-refresh-token --string-value "<refresh token>"
databricks secrets put-secret jetstream_servicenow servicenow-base-url --string-value "https://example.service-now.com"
```

The scope name is yours. Pass it as `secret_scope`. The four key names are fixed. Credential values stay in the secret scope. The control plane stores the scope name only.

Use a separate scope for each ServiceNow instance.

## Create the job

Point every notebook task at `notebook/01_run.py`. Start from [docs/examples/example_job_params_dev.yaml](docs/examples/example_job_params_dev.yaml). A multi-task layout is in [docs/examples/example_job_multi_task.yaml](docs/examples/example_job_multi_task.yaml). The full list is [connectors/servicenow/parameters.yaml](connectors/servicenow/parameters.yaml).

| Parameter | Required | Default | Purpose |
|---|---|---|---|
| `environment` | yes | | `dev` or `prod`. Selects `metadata_dev` or `metadata_prod` when `metadata_catalog` is empty. |
| `instance_name` | yes | | Name of this deployment, unique in the metadata catalog. |
| `destination_catalog` | yes | | Catalog for the ServiceNow tables. Example: `raw_dev`. |
| `destination_schema` | yes | `servicenow` | Schema for the ServiceNow tables. |
| `secret_scope` | yes | | Scope created above. |
| `metadata_catalog` | no | from `environment` | Dev only. Production must stay on `metadata_prod`. |
| `metadata_schema` | no | `jetstream` | Dev sandbox may use `jetstream_servicenow`. |
| `enabled_objects` | no | `incident` | ServiceNow table name. One name per task. |
| `sync_target` | no | `base` | `base`, `attachments`, or `journals`. |
| `primary_key` | no | `sys_id` | Registry primary key. Comma-separated composite keys are allowed. |
| `watermark_column` | no | `sys_updated_on` | Parent incremental column. Journals always use `sys_created_on`. |
| `attachment_meta_table` | no | `{table}_attachments` | Attachment metadata table. |
| `journals_meta_table` | no | `{table}_journals` | Journal table. |
| `download_attachments` | no | `true` | Download binaries when `sync_target=attachments`. |
| `attachment_volume_base` | no | `/Volumes/<catalog>/<schema>/attachments` | Volume root. |
| `page_size` | no | `10000` | Table API page size. |
| `lookback_minutes` | no | `15` | Subtracted from the cursor lower bound. |
| `from_datetime` / `to_datetime` | no | empty | Ad hoc window. `to_datetime` defaults to now (UTC). |
| `retry_max_attempts` | no | `5` | Retries on HTTP 429 and 5xx. |
| `mode` | no | `run` | `run`, `full_resync`, `retry_failed_downloads`, or `bootstrap_only`. |
| `skip_bootstrap` | no | `false` | `true` on object tasks that depend on a bootstrap task. |
| `trigger_type` | no | `scheduled` | Stored on `connector_run`. |
| `owner_team` | no | | Written to `connector_instance`. |
| `sla_target_minutes` | no | `1440` | Used by the `connector_health` view. |

On the first successful run the job registers its connector type, manifest, and instance. Later runs reuse that instance, keyed by the Databricks job id.

Run a `bootstrap_only` task first when several tasks share one job. Object tasks set `skip_bootstrap=true` and `depends_on` that task, so they do not update `connector_instance` at the same time. Databricks task keys must be unique. Use `{table}`, `{table}_attachments`, and `{table}_journals`.

| Task key | `sync_target` | `connector_run.idempotency_key` |
|---|---|---|
| `incident` | `base` | `{job_run_id}_incident` |
| `incident_attachments` | `attachments` | `{job_run_id}_incident_attachments` |
| `incident_journals` | `journals` | `{job_run_id}_incident_journals` |
| any attachments task | `retry_failed_downloads` | `{key}:retry_downloads` |

`enabled_objects` stays the ServiceNow table name on all three tasks.

## Run modes

`mode=run` reads rows newer than the stored cursor, minus `lookback_minutes`. When `from_datetime` is set, the task reads that window instead and moves the cursor forward to the later of the stored cursor and the batch maximum.

`mode=full_resync` reads the table with no watermark filter and stores the batch maximum.

`mode=retry_failed_downloads` re-downloads attachment rows whose `download_status` is `failed`, `failed_with_error`, or `pending`. It runs on an attachments task. On a base or journals task it records a successful object run and skips the download. The cursor does not move.

`mode=bootstrap_only` registers the connector instance and does not extract.

A task that sets an unknown `sync_target` fails. If every object in the task fails, the task fails. A failed binary download leaves the attachments task successful so the metadata cursor can still advance. Catch up with `mode=retry_failed_downloads`.

## Cursors

Cursors are ServiceNow timestamps, `YYYY-MM-DD HH:MM:SS`. Incremental filters use `>` plus `lookback_minutes`.

| `sync_target` | State key | ServiceNow field | Empty cursor |
|---|---|---|---|
| `base` | `cursor` | `watermark_column` | Full pull, then the batch maximum. If the batch is empty, the current UTC time. |
| `attachments` | `attachment_cursor` | `sys_updated_on` on `sys_attachment` | Same, independent of the parent cursor. |
| `journals` | `journal_cursor` | `sys_created_on` on `sys_journal_field` | Same, independent of the parent cursor. |

Older keys `watermark`, `attachment_watermark`, and `journal_watermark` are read once and copied onto the current key. A cursor is never moved backward.

## After the first run

Confirm the control plane and the destination tables with [docs/VERIFICATION.md](docs/VERIFICATION.md).

## Develop

```bash
python3 -m pip install -r requirements.txt
python3 -m pytest tests/ -q
```

Tests do not call ServiceNow or Spark. The writers run only inside Databricks.

Table selection lives in `src/objects.py`. Table API paging lives in `src/servicenow_client.py`.

## Troubleshooting

| What you see | What to do |
|---|---|
| `Missing required secret key(s)` | The scope or a key name is wrong. The keys are `servicenow-client-id`, `servicenow-client-secret`, `servicenow-refresh-token`, and `servicenow-base-url`. |
| `ServiceNow token refresh failed (401)` | The client id, secret, or refresh token does not match this instance. Confirm `servicenow-base-url` has no trailing path. |
| `skip_bootstrap=true but no connector_instance` | Run the `bootstrap_only` task once before the object tasks. |
| `metadata_catalog override` | Production must use `metadata_prod` and `metadata_schema=jetstream`. |
| `unknown sync_target` | Use `base`, `attachments`, or `journals`. |
| Attachment rows stay `pending` or `failed` | Re-run that attachments task with `mode=retry_failed_downloads`. |
| `download_status=skipped` | The file was empty or larger than 100 MB. Those rows are not retried. |
| Parent row counts grow on every run | Expected. This connector appends. Filter `_jetstream_synced` to one batch. |
