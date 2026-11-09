# Contributing to Jetstream

Thanks for your interest in improving Jetstream. This project is a set of Databricks connectors plus the control plane standards they follow. Many teams will run these connectors in their own workspaces and adapt them to their own sources, so we want it to be easy to fix problems, add objects, and build new connectors that behave like the existing ones.

Please read our [Code of Conduct](CODE_OF_CONDUCT.md) before participating.

## What we welcome

| Contribution type | Process |
|---|---|
| Bug fixes in an existing connector | Open a pull request |
| Documentation improvements (READMEs, field mappings, examples) | Open a pull request |
| Additional tests or test documentation | Open a pull request |
| A new object or endpoint for an existing connector | Open a pull request, or an issue first if it changes the destination schema or the connector's structure |
| A new connector | Open an issue first with a short proposal (see "Proposing a new connector") |
| Changes to the control plane standards or system columns | Open an issue first to discuss. These affect every connector |
| Changes to the shared helpers in `src/jetstream/` | Open an issue first if the change should apply across connectors |

If you are not sure which row applies, open an issue and ask.

## Licensing of contributions

By submitting a pull request, you confirm that you have the right to contribute the code and that it is provided under the same terms as the rest of the project, as stated in the [LICENSE](LICENSE) file. Do not submit code you do not have the right to share, including code copied from other projects with incompatible terms or code from your employer that you are not authorized to release.

## How the repository is organized

Each connector lives in its own folder under `connectors/` and is self-contained: its own code, parameter definition, tests, SQL reference and README. The platform-wide rules live in `control_plane_standards/`. Read the standards before making changes that touch run tracking, state, configuration or destination columns.

## Development setup

You do not need a Databricks workspace for most contributions. Unit tests are written to run locally, and many changes (parameter handling, config validation, request building, pagination, transformations) can be developed and tested without one.

```bash
git clone <repository-url>
cd jetstream/connectors/<Connector>

python3 -m venv .venv && source .venv/bin/activate
pip install -r requirements.txt
pytest tests/                 # where the connector has a tests folder
```

Use the Python version and runtime requirements listed in the connector's README, since they can differ between connectors.

To test against a real source system and a real Databricks workspace, you will need your own credentials and your own workspace. When you do:

- Run development jobs against a non-production environment, with its own copy of the control plane tables created from the DDL in `control_plane_standards/`, never against the control plane you use for real pipelines.
- Use a throwaway destination for synced data.
- Store credentials in a Databricks secret scope and pass the scope name as a job parameter. Never put credential values in parameters, code, notebooks, test fixtures or logs.

## Making changes

Follow the existing patterns. The connectors are deliberately consistent so that anyone who has read one can work on another.

**Configuration**

- Each connector has one `parameters.yaml` under `connectors/<type>/`. It is the single source of truth for what the connector accepts, and the validation schema is compiled from it. Do not document accepted parameters only in a README or a code comment, and do not hand-write the compiled schema.
- If you add or change a parameter, update `parameters.yaml`, the connector's config handling, the README parameter table, and any example job parameters in `docs/examples/`.

**Control plane behavior**

- Connectors only insert and update rows in the control plane tables. They never create, alter or drop them.
- Every run follows the same lifecycle: find or register the instance, validate the configuration before connecting to the source, record the run, record each object processed, save cursors in the shared state table, and log notable events. Do not create private state tables in a destination schema.
- Generate identifiers as UUIDv7. Do not use auto-incrementing integers.
- Never write credential values into `source_config`, state or log event data.

**Destination data**

- Destination tables carry the Jetstream system columns defined in the standards (`_jetstream_synced` and `_jetstream_deleted` on every table, plus `_jetstream_id` and `_jetstream_index` where a table has no natural key). Connectors must detect and skip any source column that collides with a `_jetstream_` name.
- File-copy destinations (for example attachments written to a Volume) follow the file-copy conventions in the standards, including mandatory row and byte counts.
- Prefer idempotent writes. Re-running a job with the same inputs should not duplicate data.

**Code**

- Use type hints and docstrings for public functions.
- Match the style and structure of the connector you are changing. Do not reformat unrelated code in the same pull request.
- Keep dependencies minimal. Open an issue before adding a new third-party package.
- Handle API rate limits, retries and token refresh the way the existing connectors do, and fail with clear error messages that name the parameter or object involved.

## Proposing a new connector

Open an issue before you start so we can agree on scope. A good proposal covers:

1. The source system and its API, including authentication method and any rate limits.
2. The connector category: file or object storage, database, application or API, or event streaming.
3. The objects or endpoints to sync, with primary keys and whether each is full or incremental.
4. How deletes will be detected, or why they cannot be.
5. Any attachments or files that need to be landed alongside table data.
6. Who will maintain it.

A new connector pull request is complete when it includes:

- Connector code organized like the existing ones, including the platform helpers it needs.
- `connectors/<type>/parameters.yaml`.
- An entrypoint notebook for the Databricks job.
- Control plane reference SQL, if the connector ships one.
- Unit tests that run without a workspace.
- A README covering what it syncs, how authentication works, every job parameter, how to schedule it, and known limitations.
- Field mappings or example job parameters where they help users configure it.
- Evidence of testing against a real or sandbox instance of the source, described in the pull request.

The control plane standards include a self-contained build brief designed to be given to an AI coding assistant. You are welcome to use AI tools, but you are responsible for the result. Review, run and understand everything you submit.

## Pull request process

1. **Open an issue first** for non-trivial changes so we can align on approach.
2. **Keep pull requests focused.** One connector, one concern. Split unrelated changes.
3. **Run the tests** for every connector you touched and make sure they pass.
4. **Update documentation** whenever behavior, parameters, objects or destination tables change.
5. **Check for sensitive content.** No secrets, tokens, hostnames, account identifiers, internal team names, or real customer or employee data in code, tests, fixtures, notebooks, logs or documentation. Use obviously fake values in examples.
6. **Write a clear description.** Explain what changed, why, how you tested it, and whether it changes destination schemas or the control plane. Call out anything that would affect existing deployments.

A maintainer will review your pull request. We may ask for changes, and we may decline contributions that do not fit the project's direction. We will try to explain why.

## Breaking changes

Connectors are run by many people on schedules. Changes that rename or retype destination columns, change primary keys, alter how deletes are recorded, or remove or rename parameters can break existing deployments and downstream consumers. Mark these clearly in the pull request, document the migration path in the connector's README or changelog, and expect extra discussion.

## Reporting issues

- **Bugs:** Open an issue with the connector name, what you expected, what happened, relevant job parameters (with secrets removed), and log or error output.
- **Feature requests:** Open an issue describing the use case, not only the change you want.
- **Security vulnerabilities:** Do not open a public issue. Follow the process in [SECURITY.md](SECURITY.md).
- **Questions:** Check the connector README and the standards first, then open an issue if the answer is not there.
