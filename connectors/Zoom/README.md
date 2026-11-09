# Zoom connector

A Databricks Job that syncs Zoom users, groups, and Zoom Rooms into Unity Catalog Iceberg tables, and records each run in the shared Jetstream control plane.

One job is one connector instance. Create another job, with its own parameters, to sync another Zoom account. The code does not change between environments.

## What it syncs

| Object | When it runs | Load | Zoom scope |
|---|---|---|---|
| `users` | Primary. Empty `enabled_objects` syncs all primary objects. | Full snapshot, MERGE, soft delete | `user:read:list_users:admin` |
| `groups` | Primary | Full snapshot, MERGE, soft delete | `group:read:list_groups:admin` |
| `rooms` | Primary | Full snapshot, MERGE, soft delete | `zoom_rooms:read:list_rooms:admin` |
| `room_locations` | Primary | Full snapshot, MERGE, soft delete | `zoom_rooms:read:list_locations:admin` |
| `room_devices` | Primary | Full snapshot, MERGE, soft delete | `zoom_rooms:read:list_devices:admin` |
| `dashboard_meetings` | Only if listed in `optional_objects_enabled` | Incremental | `dashboard:read:list_meetings:admin` |
| `dashboard_meeting_participants` | Only if listed in `optional_objects_enabled` | Incremental | `dashboard:read:list_meeting_participants:admin` |

Each object lands in `<destination_catalog>.<destination_schema>.<object>`.

Every table has `_jetstream_synced`, `_jetstream_deleted`, and `_jetstream_source_object`. Rows a full snapshot no longer returns are kept and marked `_jetstream_deleted = true`.

## Before you run

1. A Databricks workspace with Unity Catalog, and a classic job cluster (Databricks Runtime 15.4 LTS or newer, single-user access mode).
2. Two catalogs you control. By default the job looks for a metadata catalog named `metadata_dev` when `environment=dev`, and `metadata_prod` when `environment=prod`. In dev you may pass `metadata_catalog` to use a different catalog. Production rejects any other metadata catalog. `destination_catalog` is always a job parameter. It is not chosen from `environment`.
3. The control-plane tables. The job reads and writes them. It does not create them. Run [sql/control_plane.reference.sql](sql/control_plane.reference.sql) once in the metadata catalog. The script creates `metadata_prod.jetstream`. Replace that catalog name if you are using `metadata_dev` or a dev sandbox catalog. For a sandbox, set `metadata_schema` to `jetstream_zoom` so this instance does not share the `jetstream` schema. Production must use `jetstream`.
4. A destination schema, for example `CREATE SCHEMA IF NOT EXISTS raw_dev.zoom`.
5. A Zoom Server-to-Server OAuth app whose scopes match the objects you will sync. Note the account id, client id, and client secret.

The job's run-as identity needs `USE CATALOG` on the metadata and destination catalogs, `USE SCHEMA`, `SELECT`, and `MODIFY` on the metadata schema, `CREATE TABLE` on the destination schema, and `READ` on the secret scope. Set the job to `max_concurrent_runs: 1`.

## Secrets

```bash
databricks secrets create-scope jetstream_zoom
databricks secrets put-secret jetstream_zoom zoom_account_id --string-value "<account id>"
databricks secrets put-secret jetstream_zoom zoom_client_id --string-value "<client id>"
databricks secrets put-secret jetstream_zoom zoom_client_secret --string-value "<client secret>"
```

The scope name is yours. Pass it as `secret_scope`. The three key names are fixed.

## Create the job

Point a notebook task at `notebook/01_run.py`. Set job parameters from [docs/examples/example_job_params_dev.yaml](docs/examples/example_job_params_dev.yaml). The full list is [connectors/zoom/parameters.yaml](connectors/zoom/parameters.yaml).

| Parameter | Required | Default | Purpose |
|---|---|---|---|
| `environment` | yes | | `dev` or `prod`. Selects `metadata_dev` or `metadata_prod` when `metadata_catalog` is empty. |
| `instance_name` | yes | | Name of this deployment, unique in the metadata catalog. |
| `destination_catalog` | yes | | Catalog for the Zoom tables. Example: `raw_dev`. |
| `destination_schema` | yes | | Schema for the Zoom tables. Example: `zoom`. |
| `secret_scope` | yes | | Scope created above. |
| `metadata_catalog` | no | from `environment` | Dev only. Production must stay on `metadata_prod`. |
| `metadata_schema` | no | `jetstream` | Dev sandbox may use `jetstream_zoom`. |
| `enabled_objects` | no | all primary | Comma-separated primary objects. |
| `optional_objects_enabled` | no | none | Comma-separated optional objects. |
| `mode` | no | `run` | `run` or `full_resync`. |
| `objects` | no | | Used only when `mode=full_resync`. |
| `owner_team` | no | | Written to `connector_instance`. |
| `sla_target_minutes` | no | `1440` | Used by the `connector_health` view. |

`zoom_base_url` defaults to `https://api.zoom.us/v2`. Change it only for a Zoom Government or other isolated tenant.

On the first successful run the job registers its connector type, manifest, and instance. Later runs reuse that instance, keyed by the Databricks job id.

## Run modes

`mode=run` syncs `enabled_objects` and `optional_objects_enabled`. The `objects` parameter is ignored.

`mode=full_resync` reloads only the objects listed in `objects`, resets their cursors, and leaves every other object alone.

## After the first run

Confirm the control plane and the destination tables with [docs/VERIFICATION.md](docs/VERIFICATION.md).

## Develop

```bash
python3 -m pip install -r requirements.txt
python3 -m pytest tests/ -q
```

Tests stub HTTP. The Spark writer runs only inside Databricks.

Object definitions live in `src/objects.py`. HTTP paths live in `src/extract.py`.
