# Control Plane Standards

This folder contains the specification that every Jetstream connector follows. It defines how connectors report their activity to a shared control plane, and which system columns are added to every table they write. If you are building a new connector, or want to understand how existing ones behave, start here.

## Contents

| File | Description |
|---|---|
| [`jetstream-control-plane-and-data-standards.md`](jetstream-control-plane-and-data-standards.md) | The full standard: control plane architecture, table definitions and DDL, instance bootstrapping, runtime validation, worked examples, sample monitoring queries, system column definitions, an entity relationship diagram, and a build brief for AI coding assistants |
| [`provision_jetstream_schema.py`](provision_jetstream_schema.py) | Databricks notebook that creates the `jetstream` schema, the nine control plane tables, and the `connector_health` view in the catalog you pass as a job parameter. Safe to re-run: it never drops or alters existing objects |

## What the standard covers

- **Control plane.** Nine Iceberg tables shared by every connector instance: a registry of connector types, versioned configuration manifests, connector instances, runs, per-object run details, a registry of destination tables, schema change events, incremental state, and logs.
- **Identifiers and credentials.** UUIDv7 identifiers, a clear separation between connector types and connector instances, and a rule that credential values are never stored.
- **OpenLineage alignment.** A connector instance maps to a Job, a run to a Run, and a registered table to a Dataset, so lineage can be exported to any compatible tool.
- **System columns.** The `_jetstream_*` columns added to destination tables, plus extensions for files, databases, applications and APIs, event streaming, and history mode.
- **Parameter specification.** How each connector declares its accepted configuration in a `parameters.yaml` file, how instances register themselves on first run, and how configuration is validated at runtime.
- **Worked examples.** Three end-to-end walkthroughs (a file-based connector, an API-based connector, and a connector with mixed table and file destinations) using fictional data.
- **Build brief.** A self-contained block you can give to an AI coding assistant to scaffold a new connector that follows the standard.

## Setting up the control plane

Before running any connector, create the control plane once per environment:

1. Import `provision_jetstream_schema.py` into your Databricks workspace as a notebook.
2. Run it as a job, or interactively, with the `catalog_name` parameter set to the catalog that should hold the control plane. The catalog must already exist and you need permission to create schemas and tables in it.
3. Configure your connector jobs to use that catalog and the `jetstream` schema. Each connector's README lists the relevant parameters.

## A note on catalog names

The SQL in the standard uses two placeholders, `<metadata_catalog>` and `<raw_catalog>`, for the catalog that holds control plane tables and the catalog that holds synced business data. Substitute the names you use in your environment. The individual connector READMEs explain how to set the destination for each connector.

## Contributing to the standard

Changes to the control plane tables or system columns affect every connector, so please open an issue to discuss them before submitting a pull request. See [CONTRIBUTING.md](../CONTRIBUTING.md) for details.
