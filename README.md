# Jetstream

![Databricks](https://img.shields.io/badge/Databricks-Unity_Catalog-red) ![Table format](https://img.shields.io/badge/Table_Format-Apache_Iceberg-blue) ![Python](https://img.shields.io/badge/Python-3-blue) ![Connectors](https://img.shields.io/badge/Connectors-7-green) ![Status](https://img.shields.io/badge/Status-Active-brightgreen)

Jetstream is an open source data ingestion platform for Databricks. It is a collection of production-grade connectors that move data from SaaS applications, APIs and file stores into Unity Catalog Iceberg tables, plus a shared control plane standard that makes every connector report its health, lineage and freshness in exactly the same way.

Moving data from SaaS platforms and enterprise systems into a lakehouse often means paying a third-party vendor for per-row licensing, sending data through someone else's infrastructure, and accepting a proprietary data plane. Jetstream takes a different approach. Everything runs inside your own Databricks environment as ordinary Databricks Jobs, data lands in open Apache Iceberg tables that you own, and each connector reports into a shared, documented control plane.

Jetstream was built at Workday to ingest enterprise data into Databricks. It is open sourced here so that anyone can run these connectors, learn from the patterns, or build new connectors on the same foundation.

## Why Jetstream

- **Native Iceberg ingestion.** Connectors write Iceberg tables directly in Unity Catalog, using idempotent MERGE writes and soft deletes (some file-based loads use overwrite or append, as configured). Iceberg brings ACID transactions, schema evolution, time travel and multi-engine interoperability, with no proprietary table format in between.
- **Runs inside your security boundary.** Connectors execute as Databricks Jobs in your workspace. Source data goes from the source system to your storage with no third-party service in the path.
- **One control plane for every connector.** Every job, run, destination table, cursor, schema change and log line is recorded in one shared set of tables, so pipeline health can be checked from one place instead of connector by connector.
- **Standards over snowflakes.** Connectors follow a published specification for configuration, identifiers, system columns and run tracking. A new connector looks and behaves like the existing ones.
- **No per-connector licensing cost, no vendor lock-in.** Everything is plain Python, Databricks Jobs and SQL that you can read, run and modify.
- **Open metadata.** Control plane entities map directly onto OpenLineage concepts (a connector instance is a Job, a run is a Run, a registered table is a Dataset), so lineage can be exported to tools that already speak that standard.
- **Config-driven, environment-neutral.** Each connector's behavior is set through job parameters validated against a declared schema, so the same code promotes from development to production without code changes.
- **Fits AI-assisted development.** The standards include a self-contained build brief that can be handed to a coding assistant to scaffold a new connector.

## Why open source

We are releasing Jetstream publicly for a few reasons:

- **A vendor-neutral option for Databricks ingestion.** Teams should be able to land enterprise data in open Iceberg tables without per-row licensing or a closed data plane.
- **Connectors are a community-sized problem.** There are hundreds of SaaS platforms and APIs worth integrating. A shared framework and a published standard make it practical for others to build, maintain and share connectors that behave consistently.
- **Better software through review.** Public scrutiny helps surface schema edge cases, performance bottlenecks in parallel processing, and security issues that a single team can miss.
- **Open standards for governance.** Jetstream's control plane follows OpenLineage concepts and defines standard system columns for change tracking and metadata. We hope the control plane specification is useful to others, whether or not they use our connectors.

## Connectors

All connectors live under [`connectors/`](connectors/). Each one is self-contained, with its own code, parameter definition, tests and README. Start with the README in a connector's folder for setup, parameters and object details.

| Connector | Source | What it syncs | Destination |
|---|---|---|---|
| [Eptura](connectors/Eptura/) | Eptura iOffice (Workplace REST API v2, OAuth 2.0) | Rooms, buildings, neighborhoods, floors and users, as full snapshots merged on `id` with soft deletes | Iceberg tables |
| [Evisort](connectors/Evisort/) | Evisort contract platform API | Seven selectable objects: documents, groups, tickets, nodes, departments, workflows, activities | Iceberg tables |
| [GDrive](connectors/GDrive/) | Google Drive (service account) | Files from configured folders or file IDs. All file types are landed to a Volume, and structured files (CSV, XLSX, Sheets) are parsed into tables | UC Volume plus Iceberg tables |
| [Juniper_Mist](connectors/Juniper_Mist/) | Juniper Mist REST API v1 (API token) | Building occupancy and network data across 24 objects (14 primary, 10 optional), with per-site and per-map fan-out | Iceberg tables |
| [ServiceNow](connectors/ServiceNow/) | ServiceNow tables | Any ServiceNow table, with separate extracts for the base table, attachments and journal entries | Iceberg tables plus attachments in a Volume |
| [WSS](connectors/WSS/) | Workday Strategic Sourcing REST APIs | More than 30 catalogued endpoints (contracts, projects, payments and more), full and incremental, with attachment download | Iceberg tables plus attachments in a Volume |
| [Zoom](connectors/Zoom/) | Zoom (Server-to-Server OAuth) | Users, groups, rooms, room locations, room devices, plus optional dashboard meetings and participants | Iceberg tables |

The seven connectors above are the first batch. More will follow as development continues, and we welcome contributions from the community. Examples of sources that would make good additions include Marketo, Salesforce, Workday RaaS, SFTP, Jira and Adobe. See [Contributing](#contributing) if you would like to propose one.

## How it works

Every Jetstream connector is a Databricks Job. The job's parameters are the only configuration, and the same connector code can back any number of independent jobs.

```
        Databricks Job parameters
                  |
                  v
   +-------------------------------+
   |  Connector run                |
   |  1. bootstrap or find instance|
   |  2. validate config           |
   |  3. extract from source       |
   |  4. MERGE into Iceberg        |
   |  5. record run, state, logs   |
   +---------------+---------------+
                   |
        +----------+-----------+
        v                      v
  Synced business data     Control plane
  (your destination)       (run history, state, logs)
```

The key ideas:

- **Connector type versus connector instance.** A connector type is the code (for example `zoom`). A connector instance is one configured, scheduled deployment of it, with its own source, destination, run history and state. Many instances of one type run independently.
- **Self-registering instances.** No one creates an instance by hand. On its first run, a job looks itself up by its Databricks job ID, registers the connector type and its configuration schema if needed, inserts its own instance row, and reuses it on every later run.
- **Validate before moving data.** Each run checks its parameters against a schema compiled from the connector's `parameters.yaml` before it connects to the source.
- **Data and metadata stay separate.** Synced business data lands in the destination you configure. The control plane holds only run history, state and logs, never business data and never credential values.
- **System columns on every table.** Destination tables carry `_jetstream_synced` and `_jetstream_deleted` on all tables, and `_jetstream_id` and `_jetstream_index` on tables without a natural key, so consumers can see when a row last changed and whether it was deleted.

## Control plane

The control plane is a set of nine Iceberg tables shared by every connector, distinguished by `connector_instance_id`:

| Table | Purpose |
|---|---|
| `connector_registry` | One row per connector type available |
| `connector_manifest` | Versioned, validated configuration schema per connector type |
| `connector_instance` | One row per configured job |
| `connector_run` | One row per execution |
| `connector_object_run` | Table-level (or file-level) detail within a run |
| `connector_table_registry` | Every destination table, with freshness and size |
| `connector_schema_change_event` | New or changed columns detected at the source |
| `connector_state` | Cursors and watermarks for incremental syncs |
| `connector_log` | Warnings, errors and notable events |

Most connectors include a reference SQL file (for example `sql/control_plane_reference.sql`) showing the table shapes. The full specification, including column definitions, identifier rules, worked examples and sample health queries, is in [`control_plane_standards/`](control_plane_standards/).

## Scope

These connectors produce the raw layer only: faithful copies of source data with system columns added. Curated and consumption transformations are intentionally out of scope, and downstream jobs build on top of the raw tables.

## Repository layout

```
jetstream/
|-- README.md
|-- LICENSE
|-- CONTRIBUTING.md
|-- CODE_OF_CONDUCT.md
|-- CONTRIBUTORS.md
|-- control_plane_standards/      Control plane and data standards
|-- connectors/
    |-- Eptura/
    |-- Evisort/
    |-- GDrive/
    |-- Juniper_Mist/
    |-- ServiceNow/
    |-- WSS/
    |-- Zoom/
```

Most connectors share the same internal shape:

```
connectors/<Connector>/
|-- README.md                      Setup, parameters, objects, behavior
|-- connectors/<type>/parameters.yaml   Single source of truth for accepted configuration
|-- notebook/                      Databricks entrypoint notebook (01_run) and probes
|-- src/                           Connector code, plus src/jetstream/ platform helpers
|-- sql/                           Control plane reference DDL
|-- docs/                          Field mappings, deployment notes, test evidence (some connectors)
|-- tests/                         Unit tests
|-- requirements.txt
```

The Evisort connector uses a flatter layout, with the job logic in a small set of top-level Python files. See its README for details.

## Getting started

1. **Pick a connector** from the table above and read its README. It lists the source credentials you need, the available objects and every job parameter.
2. **Prepare Databricks.** You need a workspace with Unity Catalog and Iceberg support, a destination for the synced data, a catalog for the control plane tables (create them by running `control_plane_standards/provision_jetstream_schema.py`), and a secret scope holding the source system credentials. Credentials are always passed as secret scope references, never as plain parameter values.
3. **Create a Databricks Job** that runs the connector's entrypoint notebook, and set the job parameters described in the connector's `parameters.yaml`. Add a schedule.
4. **Run the job.** The first run registers the connector instance. Every run then records its status, row counts, state and logs in the control plane.
5. **Check health** with the sample queries in the standards document, for example last run status per instance, tables not synced within their SLA, and recent schema changes.

To build a new connector, start from [`control_plane_standards/`](control_plane_standards/). It defines the registration, run, per-object, state and logging steps every connector follows, and includes a build brief for AI coding assistants.

## Testing

Most connectors include unit tests, and some (Juniper_Mist, for example) are designed to run without Spark or a Databricks workspace. Where a `tests/` folder exists, run it from the connector's folder:

```bash
pip install -r requirements.txt
pytest tests/
```

Some connectors also ship Databricks notebooks for validation and connectivity probing against a real workspace. See each connector's README for what is available.

## Contributing

Contributions are welcome, whether bug fixes, documentation improvements, new objects for an existing connector, or new connectors that follow the control plane standards. Please read [CONTRIBUTING.md](CONTRIBUTING.md) and the [Code of Conduct](CODE_OF_CONDUCT.md) before opening a pull request.

## Contributors

The people who built the connectors in this repository are listed in [CONTRIBUTORS.md](CONTRIBUTORS.md). Thank you to everyone who has worked on Jetstream.

## License

See the [LICENSE](LICENSE) file in this repository.
