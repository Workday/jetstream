# Eptura connector

A Databricks Job that syncs Eptura iOffice rooms, buildings, neighborhoods, floors, and users into Unity Catalog Iceberg tables, and records each run in the shared Jetstream control plane.

One job is one connector instance. Curated models (employees, spaces, occupancy) stay downstream. This job lands the raw snapshot.

## What it syncs

| Entity | API path | Primary key |
|---|---|---|
| `rooms` | `/external/api/rest/v2/rooms` | `id` |
| `buildings` | `/external/api/rest/v2/buildings` | `id` |
| `neighborhoods` | `/external/api/rest/v2/neighborhoods` | `id` |
| `floors` | `/external/api/rest/v2/floors` | `id` |
| `users` | `/external/api/rest/v2/users` | `id` |

Each entity lands in `<destination_catalog>.<destination_schema>.<entity>`.

Every run reads the full collection and MERGEs on `id`. Rows the API stops returning are kept and marked `_jetstream_deleted = true`. Nested JSON stays as `STRUCT` and `ARRAY` columns. Column notes are in [docs/FIELD_MAPPING.md](docs/FIELD_MAPPING.md).

There is no `spaces` endpoint. A space is a row in `rooms` (`type.name` covers Workstation, Phone Room, Huddle Room, and conference rooms).

iOffice can filter some collections with `modifiedOrCreatedAfter`. This connector does not use that filter. A cursor cannot report deletions, and a nightly full read of these five collections is small. If that changes, add a periodic full reconcile rather than switching to incremental alone.

## Before you run

1. A Databricks workspace with Unity Catalog. **Serverless compute cannot resolve the Eptura host** (`Temporary failure in name resolution`). Use a classic job cluster in a network that can reach your iOffice site.
2. Two catalogs you control. By default the job looks for `metadata_dev` when `environment=dev`, and `metadata_prod` when `environment=prod`. In dev you may pass `metadata_catalog` to use a different catalog. Production rejects any other metadata catalog. `destination_catalog` is always a job parameter.
3. The control-plane tables. The job reads and writes them. It does not create them. Run [sql/control_plane_reference.sql](sql/control_plane_reference.sql) once. The script creates `metadata_prod.jetstream`. Replace that catalog name for `metadata_dev`. A sandbox should set `metadata_schema` to `jetstream_eptura`. Production must use `jetstream`.
4. A destination schema, for example `CREATE SCHEMA IF NOT EXISTS raw_dev.eptura`.
5. An Eptura API account and the OAuth client registered for your tenant. `redirect_uri` is required and must be the redirect URI registered for that client.

The job's run-as identity needs `USE CATALOG` on both catalogs, `USE SCHEMA`, `SELECT`, and `MODIFY` on the metadata schema, `CREATE TABLE` on the destination schema, and `READ` on the secret scope. Keep `max_concurrent_runs` at 1.

## Secrets

```bash
databricks secrets create-scope jetstream_eptura
databricks secrets put-secret jetstream_eptura eptura_host --string-value "https://your-tenant.ioffice.com"
databricks secrets put-secret jetstream_eptura eptura_username --string-value "<user>"
databricks secrets put-secret jetstream_eptura eptura_password --string-value "<password>"
databricks secrets put-secret jetstream_eptura eptura_client_id --string-value "<client id>"
```

The scope name is yours. Pass it as `secret_scope`. The four key names are fixed. `eptura_host` is the site base URL with no path.

## Create the job

Point a notebook task at `notebook/01_run.py`, or deploy [databricks.yml](databricks.yml) after you set `existing_cluster_id`, `policy_id`, catalogs, and `redirect_uri` for your workspace.

```bash
databricks auth login --host https://<workspace>.cloud.databricks.com
databricks bundle validate -t sandbox
databricks bundle deploy -t sandbox
databricks bundle run eptura_probe -t sandbox
databricks bundle run eptura_sync -t sandbox
```

Run the probe first. It checks OAuth and does not write tables. The bundle schedule is paused until you unpause it.

If you create the job by hand, start from [docs/examples/example_job_params_dev.yaml](docs/examples/example_job_params_dev.yaml). The full list is [connectors/eptura/parameters.yaml](connectors/eptura/parameters.yaml).

| Parameter | Required | Default | Purpose |
|---|---|---|---|
| `environment` | yes | | `dev` or `prod`. Selects the metadata catalog when `metadata_catalog` is empty. |
| `instance_name` | yes | | Name of this deployment. |
| `destination_catalog` | yes | | Catalog for the Eptura tables. Example: `raw_dev`. |
| `destination_schema` | yes | | Schema for the Eptura tables. Example: `eptura`. |
| `secret_scope` | yes | | Scope created above. |
| `redirect_uri` | yes | | OAuth redirect URI registered for your Eptura client. |
| `metadata_catalog` | no | from `environment` | Dev only. |
| `metadata_schema` | no | `jetstream` | Sandbox: `jetstream_eptura`. |
| `enabled_objects` | no | all five | Comma-separated entities. |
| `objects` | no | | This run only. Must be a subset of `enabled_objects`. |
| `tombstone_guard_pct` | no | `20` | Fail an entity instead of soft-deleting more than this percent of its live rows. |
| `api_base_path` | no | `/external/api/rest/v2` | Path under `eptura_host`. |
| `owner_team` | no | | Written to `connector_instance`. |
| `sla_target_minutes` | no | `1440` | Used by `connector_health`. |

`trigger_type` is read from the Databricks run context. You do not set it.

On the first successful run the job registers its connector type, manifest, and instance. Later runs reuse that instance.

`notebook/99_connectivity_probe.py` also lists other iOffice collections the account can read. Adding one of those to the sync is a code change in `src/api_objects.py`, not a parameter.

## After the first run

Confirm the control plane and the five tables with [docs/VERIFICATION.md](docs/VERIFICATION.md).

## Develop

```bash
python3 -m pip install -r requirements.txt
python3 -m pytest tests/ -q
```

Tests stub HTTP. The Spark writer runs only inside Databricks.

To add an entity, append an `ObjectCfg` in `src/api_objects.py`. Set `fetch_detail=True` when the list call omits fields that `GET /{name}/{id}` returns. The name is both the API path segment and the destination table.

## Troubleshooting

| What you see | What to do |
|---|---|
| `Temporary failure in name resolution` | The cluster cannot reach the Eptura host. Use classic compute with egress. Serverless will not work. |
| An entity fails and almost nothing is soft-deleted | The tombstone guard fired. A short API response looks like a mass delete. Check the source, then raise `tombstone_guard_pct` only if the drop is real. |
| `connector_run.status` stays `running` after you cancel the job | Databricks stops Python and Spark together, so the job cannot close the row. Close it with the query in [docs/VERIFICATION.md](docs/VERIFICATION.md). |
| A run failed | Read `connector_run.error_message`, then `connector_object_run` and `connector_log` for that `run_id`. The notebook prints the id at the start. |

API reference: [iOffice REST API](https://ioffice.github.io/api/) and [Eptura OAuth](https://knowledge.eptura.com/Workplace/Integrations/REST_API/Using_OAuth_2.0_to_Access_the_API).
