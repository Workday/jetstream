# Juniper Mist connector

A Databricks Job that syncs Juniper Mist organization, site, and occupancy data into Unity Catalog Iceberg tables, and records each run in the shared Jetstream control plane.

Authentication is a static API token (`Authorization: Token <value>`), Mist API v1. One job is one connector instance.

## What it syncs

Primary objects run on every sync. Optional objects run as well when `enabled_objects` is empty. If you set `enabled_objects`, only those names run. An optional object that fails is a warning. A primary object that fails fails the run.

| Object | Key | Load | Parent |
|---|---|---|---|
| `org` | `id` | full | — |
| `sitegroups` | `id` | full | — |
| `inventory` | `id` | full | — |
| `device_stats` | `id` | snapshot | — |
| `org_wlans` | `id` | full | — |
| `sites` | `id` | full | — |
| `maps` | `id` | full | sites |
| `zones` | `id` | full | sites |
| `zone_stats` | `id` | full | sites |
| `site_wlans` | `id` | full | sites |
| `site_settings` | `site_id` | full | sites |
| `zone_visits` | `site_id`, `mac`, `enter_epoch` | incremental | sites |
| `zone_visits_by_zone` | `site_id`, `scope_id`, `mac`, `enter_epoch` | incremental | sites |
| `clients_search` | `site_id`, `mac`, `timestamp` | incremental | sites |
| `mapstacks` | `id` | full, optional | sites |
| `rssizones` | `id` | full, optional | sites |
| `rssizone_stats` | `id` | full, optional | sites |
| `clients` | `site_id`, `mac` | snapshot, optional | sites |
| `assets` | `site_id`, `mac` | full, optional | sites |
| `sdkclients` | `site_id`, `id` | full, optional | sites |
| `guests` | `site_id`, `mac` | full, optional | sites |
| `guests_search` | `site_id`, `mac`, `timestamp` | incremental, optional | sites |
| `map_clients` | `site_id`, `map_id`, `mac` | snapshot, optional | maps |
| `unconnected_clients` | `site_id`, `map_id`, `mac` | snapshot, optional | maps |

**full** reads the current collection and MERGEs it. Missing rows are soft-deleted.

**incremental** reads `[last cursor − lookback, now]` and MERGEs that window. The cursor is stored in `connector_state`. Reset one object with `mode=full_resync` and `objects=<name>`.

**snapshot** appends the live set. These APIs have no history, so each run is a new point in time. Filter with `_jetstream_synced` to read one snapshot. Duplicate device or client keys across runs are expected.

Column notes are in [docs/FIELD_MAPPING.md](docs/FIELD_MAPPING.md).

`site_filter` is a comma-separated list of site-name substrings. It limits history and live-occupancy fan-out only. Maps, zones, and site settings still sync for every site. Matching is case-insensitive.

## Before you run

1. A Databricks workspace with Unity Catalog and a classic job cluster.
2. Two catalogs you control. By default the job looks for `metadata_dev` when `environment=dev`, and `metadata_prod` when `environment=prod`. In dev you may pass `metadata_catalog` to use a different catalog. Production rejects any other metadata catalog. `destination_catalog` is always a job parameter.
3. The control-plane tables. The job reads and writes them. It does not create them. Run [sql/control_plane_reference.sql](sql/control_plane_reference.sql) in the metadata catalog. The statements use the schema name `jetstream`. Create that schema in your catalog first (`CREATE SCHEMA IF NOT EXISTS <catalog>.jetstream`), or use `metadata_schema=jetstream_juniper_mist` for a sandbox and adjust the script to that schema. Production must use `jetstream`.
4. A destination schema, for example `CREATE SCHEMA IF NOT EXISTS raw_dev.juniper_mist`.
5. A Mist API token and the organization UUID.

The job's run-as identity needs `USE CATALOG` on both catalogs, `USE SCHEMA`, `SELECT`, and `MODIFY` on the metadata schema, `CREATE TABLE` on the destination schema, and `READ` on the secret scope. Keep `max_concurrent_runs` at 1.

## Secrets

```bash
databricks secrets create-scope jetstream_juniper_mist
databricks secrets put-secret jetstream_juniper_mist juniper_mist_token --string-value "<api token>"
```

The scope name is yours. The key `juniper_mist_token` is fixed. `juniper_mist_org_id` and `juniper_mist_base_url` are job parameters. You can store those two as secrets with the same key names if you would rather not put them on the job. The widget wins when it is set.

## Create the job

Point a notebook task at `notebook/01_run.py`. Start from [docs/examples/example_job_params_dev.yaml](docs/examples/example_job_params_dev.yaml). The full list is [connectors/juniper_mist/parameters.yaml](connectors/juniper_mist/parameters.yaml).

Run `notebook/99_connectivity_probe.py` first with the same `secret_scope`, org id, and base URL. It does not write tables.

| Parameter | Required | Default | Purpose |
|---|---|---|---|
| `environment` | yes | | `dev` or `prod`. Selects the metadata catalog when `metadata_catalog` is empty. |
| `instance_name` | yes | | Name of this deployment. |
| `destination_catalog` | yes | | Catalog for the Mist tables. Example: `raw_dev`. |
| `destination_schema` | yes | | Schema for the Mist tables. Example: `juniper_mist`. |
| `secret_scope` | yes | | Scope created above. |
| `juniper_mist_org_id` | yes | | Organization UUID from the Mist dashboard. |
| `juniper_mist_base_url` | no | `https://api.mist.com/api/v1` | Use the EU, APAC, or Gov cloud host if that is your region. |
| `metadata_catalog` | no | from `environment` | Dev only. |
| `metadata_schema` | no | `jetstream` | Sandbox: `jetstream_juniper_mist`. |
| `enabled_objects` | no | all 24 | Comma-separated object names. |
| `site_filter` | no | all sites | Substrings of site names for history and occupancy fan-out. |
| `lookback_days` | no | `7` | First-run window for incremental objects. |
| `full_load_interval_hours` | no | `0` | Skip full-load objects that succeeded more recently than this. `0` always runs them. Snapshot and incremental objects are never skipped. |
| `rate_limit_qps` | no | `5` | Shared across fan-out workers. Mist is about 200 requests per minute per token. Drop to `3` if you see HTTP 429. |
| `mode` | no | `run` | `run`, `full_resync`, or `discover`. |
| `objects` | no | | Used with `full_resync`. |
| `owner_team` | no | | Written to `connector_instance`. |
| `sla_target_minutes` | no | `1440` | Used by `connector_health`. |

`mode=discover` calls `GET /self` and prints token privileges. It does not write.

On the first successful run the job registers its connector type, manifest, and instance. Later runs reuse that instance.

## After the first run

Confirm the control plane and a sample of destination tables with [docs/VERIFICATION.md](docs/VERIFICATION.md).

## Develop

```bash
python3 -m pip install -r requirements.txt
python3 -m pytest tests/ -q
```

Tests stub HTTP. No Spark runtime is required.

A new object needs an `ObjectCfg` in `src/api_objects.py` and an `ENDPOINTS` entry in `src/api_extractor.py`. Parents must be listed before children.

## Troubleshooting

| What you see | What to do |
|---|---|
| `juniper_mist_token is missing` | The scope or the key name is wrong. The key is `juniper_mist_token`. |
| HTTP 401 | Regenerate the token under Organization, Settings, API Tokens. |
| HTTP 429 in `connector_log` | Lower `rate_limit_qps` to `3`. |
| `unknown Juniper Mist object(s)` | The name in `enabled_objects` is not in `src/api_objects.py`. |
| `site_filter matches 0 sites` | Run `mode=discover` and compare the substrings with site names. |
| Snapshot tables grow on every run | Expected. Filter `_jetstream_synced` to one batch. |
| Full-load objects did not run | `full_load_interval_hours` skipped them. Look for `object_interval_skipped` in `connector_log`. |
