# WSS connector

A Databricks Job that syncs the Strategic Sourcing API at `https://api.us.workdayspend.com` into Unity Catalog Iceberg tables, and records each run in the shared Jetstream control plane.

One job is one connector instance. Object names and endpoints live in [connectors/wss/api_catalog.yaml](connectors/wss/api_catalog.yaml). The code does not change between environments.

## What it syncs

`enabled_objects` is a comma-separated list of names from the catalog. Empty, or the value `all`, syncs every object.

| Kind | `api_type` | What a run does |
|---|---|---|
| Reference data | `FULL` | Reads the current collection and appends it. |
| Incremental | `INCR` | Reads the window after the stored cursor, minus `lookback_minutes`. |
| Child of another object | `INCR_GET_ID` | Calls the parent path once per id already stored in the parent table. |

Child objects skip the run when the parent table does not exist yet. Sync the parent in an earlier task, or list the parent before the child in the same job.

| Group | Objects |
|---|---|
| Contracts | `contracts`, `contract_types` |
| Projects | `projects`, `project_types` |
| Payments | `payment_types`, `payment_currencies`, `payment_terms` |
| Events | `events`, `event_templates`, `event_worksheets`, `event_bids`, `event_worksheets_list_line_items`, `bid_line_items` |
| Awards | `awards`, `awards_line_items` |
| Suppliers | `supplier_companies`, `supplier_contacts`, `suppliers`, and the supplier reference objects in the catalog |
| People | `users` (SCIM) |
| Reports | `contract_reports`, `project_reports`, `supplier_reports`, and the other `*_reports` entries |

Each object lands in `<destination_catalog>.<destination_schema>.<object>`.

Rows include `_jetstream_synced`, `_jetstream_deleted`, and `_jetstream_source_object`. Loads append. Filter `_jetstream_synced` when you want one poll.

API response backups land at `/Volumes/<catalog>/<schema>/api_responses/<object>/<timestamp>/response.json`.

Attachment binaries land under `/Volumes/<catalog>/<schema>/attachments`.

## Authentication

The API uses three header patterns. The catalog sets `auth_pattern` per object.

| Pattern | Headers |
|---|---|
| `pattern_3` | `X-Api-Key`, `X-User-Token`, `X-User-Email` |
| `pattern_1` | Same as pattern 3, plus the `com.workdayspend.api` version media type |
| `pattern_2` | `X-Api-Key` and HTTP Basic of `user_email:user_token` for SCIM |

## Before you run

1. A Databricks workspace with Unity Catalog, and a classic job cluster (Databricks Runtime 15.4 LTS or newer, single-user access mode).
2. Two catalogs you control. By default the job looks for `metadata_dev` when `environment=dev`, and `metadata_prod` when `environment=prod`. In dev you may pass `metadata_catalog` to use a different catalog. Production rejects any other metadata catalog. `destination_catalog` is always a job parameter.
3. The control-plane tables. The job reads and writes them. It does not create them. Run [sql/control_plane.reference.sql](sql/control_plane.reference.sql) once. The script creates `metadata_prod.jetstream`. Replace that catalog for `metadata_dev`. A sandbox should set `metadata_schema` to `jetstream_wss`. Production must use `jetstream`.
4. A destination schema and two volumes. [sql/provision_raw_dev.sql](sql/provision_raw_dev.sql) is an example for `raw_dev.wss`.
5. An API key, user token, and user email for the sourcing account.

The job's run-as identity needs `USE CATALOG` on the metadata and destination catalogs, `USE SCHEMA`, `SELECT`, and `MODIFY` on the metadata schema, `CREATE TABLE` on the destination schema, `READ VOLUME` and `WRITE VOLUME` on both volumes, and `READ` on the secret scope. Set the job to `max_concurrent_runs: 1`.

## Secrets

```bash
databricks secrets create-scope jetstream_wss
databricks secrets put-secret jetstream_wss wss_api_key --string-value "<api key>"
databricks secrets put-secret jetstream_wss wss_user_token --string-value "<user token>"
databricks secrets put-secret jetstream_wss wss_user_email --string-value "<user email>"
```

The scope name is yours. Pass it as `secret_scope`. The three key names above are the ones the job reads first. Hyphenated aliases `wss-api-key`, `wss-user-token`, and `wss-user-email` are also accepted. Credential values stay in the secret scope.

## Create the job

Point a notebook task at `notebook/01_run.py`. Start from [docs/examples/example_job_params_dev.yaml](docs/examples/example_job_params_dev.yaml). The full list is [connectors/wss/parameters.yaml](connectors/wss/parameters.yaml).

| Parameter | Required | Default | Purpose |
|---|---|---|---|
| `environment` | yes | | `dev` or `prod`. Selects `metadata_dev` or `metadata_prod` when `metadata_catalog` is empty. |
| `instance_name` | yes | | Name of this deployment. |
| `destination_catalog` | yes | | Catalog for the WSS tables. Example: `raw_dev`. |
| `destination_schema` | yes | `wss` | Schema for the WSS tables. |
| `secret_scope` | yes | | Scope created above. |
| `metadata_catalog` | no | from `environment` | Dev only. Production must stay on `metadata_prod`. |
| `metadata_schema` | no | `jetstream` | Dev sandbox may use `jetstream_wss`. |
| `enabled_objects` | no | all catalog objects | Comma-separated names. |
| `download_attachments` | no | `true` | Download binaries for objects that declare attachments. |
| `attachment_volume_base` | no | `/Volumes/<catalog>/<schema>/attachments` | Attachment volume. |
| `raw_json_volume_base` | no | `/Volumes/<catalog>/<schema>/api_responses` | Response backup volume. |
| `page_size` | no | `100` | API page size. |
| `lookback_minutes` | no | `30` | Subtracted from an incremental cursor. |
| `from_datetime` / `to_datetime` | no | empty | Optional extract window. |
| `mode` | no | `run` | `run`, `full_resync`, or `bootstrap_only`. |
| `owner_team` | no | | Written to `connector_instance`. |
| `sla_target_minutes` | no | `1440` | Used by `connector_health`. |

`mode=bootstrap_only` registers the connector instance and does not extract. `mode=run` syncs `enabled_objects`. `mode=full_resync` reloads those objects without the stored cursor.

On the first successful bootstrap the job registers its connector type, manifest, and instance. Later runs reuse that instance, keyed by the Databricks job id.

## After the first run

Confirm the control plane and a sample of destination tables with [docs/VERIFICATION.md](docs/VERIFICATION.md).

## Develop

```bash
python3 -m pip install -r requirements.txt
python3 -m pytest tests/ -q
```

Tests do not call the API or Spark. The writers run only inside Databricks.

A new object needs an entry in `connectors/wss/api_catalog.yaml`. Parents must exist as tables before child objects run.

## Troubleshooting

| What you see | What to do |
|---|---|
| `Missing required secret key(s)` | The scope or a key name is wrong. Use `wss_api_key`, `wss_user_token`, and `wss_user_email`. |
| HTTP 401 or 403 | The key, token, or email does not match this account. |
| `Parent table missing` | Run the parent object first. Child endpoints read ids from that table. |
| `unknown` object is skipped | The name is not in `connectors/wss/api_catalog.yaml`. |
| `metadata_catalog override` | Production must use `metadata_prod` and `metadata_schema=jetstream`. |
| Row counts grow on every run | Expected. This connector appends. Filter `_jetstream_synced` to one batch. |
