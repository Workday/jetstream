# Jetstream Google Drive Connector

A modular, parameter-driven connector that ingests data from Google Drive into Unity Catalog (`raw_<dev|prod>`) as part of the **Jetstream Platform**. Each initiative runs as an independent, scheduled Databricks Job with specific configuration parameters (targeting distinct `folder_id`s or `file_id`s), making the framework reusable for future use cases without code changes.

Aligned with the Jetstream control-plane and data standards described in the [Control Plane Tables](#jetstream-control-plane-tables) section below.

---

## Architecture

The connector operates in a **two-stage pipeline** within a single job run:

```
Google Drive API
      │
      ▼
┌─────────────────────────────────────────────────────┐
│  Stage 1: Extract & Land (extract.py)               │
│  ─ Authenticate via OAuth (Service Account)         │
│  ─ Recursively fetch files from folder_ids          │
│  ─ Directly fetch individual file_ids               │
│  ─ Download ALL file types (csv, xlsx, docx, pdf…)  │
│  ─ Land raw bytes to UC Volume preserving hierarchy │
│  ─ Date-partition: /YYYY-MM-DD/ per run             │
└─────────────────────────────────────────────────────┘
      │
      ▼
┌─────────────────────────────────────────────────────┐
│  Stage 2: Parse & Load (load.py)                    │
│  ─ Scan Volume subfolders per table_mappings        │
│  ─ Filter for structured files (.csv, .xlsx, Sheets)│
│  ─ Skip non-structured files (.docx, .pdf, .pptx)  │
│  ─ Parse tabular data via Spark (all cols → string) │
│  ─ Detect schema changes vs existing table          │
│  ─ Write to Iceberg (overwrite or append per mapping)│
│  ─ Record schema changes to control plane           │
└─────────────────────────────────────────────────────┘
      │
      ▼
  raw_<env>.gdrive.<target_table>
```

### Volume Layout Example (Stage 1 output)

```
/Volumes/raw_dev/landing/inbound/gdrive/2026-08-28/
├── Team_A/
│   ├── Polls/
│   │   ├── polls_april.xlsx
│   │   └── polls_export.csv
│   ├── Q&A/
│   │   └── qna_export.xlsx
│   ├── Surveys/
│   │   └── survey_results.xlsx
│   └── Reg Data/
│       └── event_summary_export.csv   ← no table_mapping, skipped in Stage 2
└── Team_B/
    ├── Polls/
    ├── Q&A/
    ├── Survey/
    └── Reg Data/
```

### Stage 2 Mapping Example

| Volume Source Folder | Formats | Write Mode | Target Iceberg Table |
| --- | --- | --- | --- |
| `Team_A/Polls/` | .csv, .xlsx | overwrite | `raw_dev.gdrive.team_a_polls` |
| `Team_A/Q&A/` | .xlsx, .csv | overwrite | `raw_dev.gdrive.team_a_qna` |
| `Team_A/Surveys/` | .xlsx, .csv | overwrite | `raw_dev.gdrive.team_a_surveys` |

---

## Project Structure

```
jetstream-gdrive-connector/
├── connectors/
│   └── gdrive/
│       └── parameters.yaml        # Parameter schema (single source of truth)
├── notebooks/
│   └── run_gdrive_connector       # Entry-point notebook for Databricks Job
├── src/
│   ├── jetstream/                 # Platform-shared modules
│   │   ├── bootstrap.py           # Self-registration (registry/manifest/instance)
│   │   ├── ctx.py                 # DatabricksContext + MetaCtx helpers
│   │   ├── ids.py                 # UUID generation
│   │   ├── params.py              # YAML → JSON Schema compilation
│   │   └── validate.py            # source_config validation
│   ├── __init__.py
│   ├── auth.py                    # Google OAuth token refresh
│   ├── config.py                  # Typed dataclasses + widget resolver
│   ├── extract.py                 # Stage 1: Drive API listing + download + volume write
│   ├── load.py                    # Stage 2: Volume → Iceberg structured parsing
│   ├── objects.py                 # Object catalog (files_metadata, files)
│   ├── pipeline.py                # Top-level orchestrator (run entry point)
│   └── state.py                   # connector_run / connector_state / logging
├── tests/
├── requirements.txt
├── .gitignore
└── README.md
```

---

## Configuration

All per-instance configuration lives in **Databricks Job parameters** (read via `dbutils.widgets.getAll()`) — NOT in YAML files inside this repo. The schema is defined in [`connectors/gdrive/parameters.yaml`](./connectors/gdrive/parameters.yaml).

### Required Parameters

| Parameter | Type | Description | Example |
| --- | --- | --- | --- |
| `environment` | string | `dev` or `prod` | `dev` |
| `instance_name` | string | Unique deployment name | `gdrive_example` |
| `destination_catalog` | string | UC catalog for raw data | `raw_dev` |
| `destination_schema` | string | Schema under catalog | `gdrive` |
| `secret_scope` | string | Databricks secret scope with OAuth keys | `gdrive-oauth-creds` |
| `folder_ids` | string | Comma-separated Drive FOLDER IDs | `<folder_id_a>,<folder_id_b>` |

### Optional Parameters (with defaults)

| Parameter | Default | Description |
| --- | --- | --- |
| `metadata_catalog` | derived from env | Override metadata catalog location |
| `metadata_schema` | `jetstream` | Override to `jetstream_gdrive` in dev |
| `enabled_objects` | all | Comma-separated: `files_metadata`, `files` |
| `owner_team` | `""` | Team responsible for this instance |
| `tags` | `{}` | JSON object of key-value labels (`MAP<STRING, STRING>`) |
| `sla_target_minutes` | `1440` | Max minutes between successful runs |
| `preserve_hierarchy` | `true` | Replicate Drive folder structure in Volume |
| `date_partitioned` | `true` | Insert `/YYYY-MM-DD/` directory per run |
| `file_destination_type` | `volume` | `volume` or `s3` |
| `volume_path` | `""` | UC Volume path for file landing |
| `mode` | `run` | Run mode: `run` (incremental) or `full_resync` (reset cursors) |
| `file_ids` | `""` | Comma-separated Drive FILE IDs (individual files, not folders) |
| `table_mappings` | `[]` | JSON array of structured Iceberg table mappings |
| `drive_api_base_url` | `https://www.googleapis.com/drive/v3` | Drive API endpoint |
| `token_uri` | `https://oauth2.googleapis.com/token` | OAuth token endpoint |
| `rate_limit_qps` | `10` | Client-side API throttle (requests/sec) |
| `timeout_seconds` | `60` | Per-request HTTP timeout |
| `retry_max_attempts` | `5` | Max retries on 429/5xx |
| `file_export_format` | `""` | Preferred export for Google-native files |
| `max_file_bytes` | `0` | Skip files larger than N bytes (0 = no limit) |
| `databricks_job_id` | `""` | Job ID — use `{{job_id}}` dynamic reference |
| `databricks_job_run_id` | `""` | Run ID — use `{{run_id}}` dynamic reference; used as idempotency key |
| `databricks_workspace_id` | `""` | Workspace org ID (static, e.g. `1234567890123456`) |

### Secrets (Databricks Secret Scope)

Stored in the scope named by `gdrive-oauth-creds`, NOT as job parameters:

| Key | Description |
| --- | --- |
| `client_id` | Google OAuth client ID |
| `client_secret` | Google OAuth client secret |
| `refresh_token` | Google OAuth refresh token |

---

## Table Mappings

The `table_mappings` parameter controls Stage 2 (structured file → Iceberg table routing). Passed as a JSON array:

```json
[
  {"source_type": "folder", "target_table": "team_a_polls",
   "source_subfolder": "Team_A/Polls", "description": "Polls"},
  {"source_type": "folder", "target_table": "team_a_qna",
   "source_subfolder": "Team_A/Q&A", "description": "Q&A"},
  {"source_type": "folder", "target_table": "team_a_surveys",
   "source_subfolder": "Team_A/Surveys", "description": "Surveys"},
  {"source_type": "folder", "target_table": "team_b_polls",
   "source_subfolder": "Team_B/Polls", "write_mode": "append",
   "description": "Team B Polls (append mode example)"}
]
```

**Entry fields:**

| Field | Required | Description |
| --- | --- | --- |
| `source_type` | Yes | `folder` or `file` |
| `target_table` | Yes | Destination table name (catalog/schema from top-level params) |
| `source_subfolder` | Yes | Relative path in Volume for Stage 2 reads |
| `supported_formats` | No | Array of accepted formats. Default: `["csv", "xlsx", "xls", "google_sheets"]` |
| `recursive` | No | Recurse into subfolders (default: `true`) |
| `write_mode` | No | `overwrite` (default) = truncate + replace; `append` = add rows to existing table |
| `description` | No | Human-readable label |

---

## Jetstream Control Plane Tables

The connector self-registers and writes metadata to the following Jetstream tables (per the spec):

| Table | Purpose |
| --- | --- |
| `connector_registry` | One row per connector type |
| `connector_manifest` | Versioned JSON Schema for config validation |
| `connector_instance` | One row per deployed job instance |
| `connector_run` | One row per execution (success/failure/metrics) |
| `connector_object_run` | Per-table detail within a run |
| `connector_table_registry` | Catalog of all destination tables ever created |
| `connector_state` | Cursors, watermarks, offsets |
| `connector_log` | Append-only event log (lifecycle + errors) |
| `connector_schema_change_event` | DDL change tracking (create_table, add/drop column, alter type) |

All reside in `jetstream_metadata_<env>.jetstream` (or override via `metadata_catalog`/`metadata_schema`).  
Example dev override: `jetstream_metadata_dev.jetstream_gdrive`.

---

## Usage

### 1. Configure Secrets

```bash
databricks secrets create-scope gdrive-oauth-creds
databricks secrets put-secret gdrive-oauth-creds client_id --string-value "..."
databricks secrets put-secret gdrive-oauth-creds client_secret --string-value "..."
databricks secrets put-secret gdrive-oauth-creds refresh_token --string-value "..."
```

### 2. Create a Databricks Job

Create a job with the entry-point notebook (`notebooks/run_gdrive_connector`) and set the required parameters listed above.

**Job:** [Jetstream GDrive - gdrive_example](https://<your-workspace>.cloud.databricks.com/jobs/<job_id>)

### 3. Run

The job triggers the full pipeline:

```
Stage 1a: files_metadata → Drive metadata → Iceberg (raw_dev.gdrive.files_metadata)
Stage 1b: files          → Download all files → UC Volume
Stage 2:  table_mappings → Parse structured files → Iceberg tables
```

---

## Example Deployment

**Job:** [Jetstream GDrive - gdrive_example](https://<your-workspace>.cloud.databricks.com/jobs/<job_id>)  
**Schedule:** Daily  
**Compute:** Serverless (USER_ISOLATION)

### Job Parameters (as configured)

| Parameter | Value |
| --- | --- |
| `environment` | `dev` |
| `instance_name` | `gdrive_example` |
| `destination_catalog` | `raw_dev` |
| `destination_schema` | `gdrive` |
| `metadata_schema` | `jetstream_gdrive` |
| `secret_scope` | `gdrive-oauth-creds` |
| `folder_ids` | `<folder_id_a>,<folder_id_b>` |
| `file_ids` | `<file_id_1>,<file_id_2>` |
| `volume_path` | `/Volumes/raw_dev/landing/inbound/gdrive` |
| `enabled_objects` | `files_metadata,files` |
| `mode` | `run` |
| `owner_team` | `data-engineering` |
| `tags` | `{"initiative": "example", "domain": "marketing"}` |
| `databricks_job_id` | `{{job_id}}` (resolved at runtime) |
| `databricks_job_run_id` | `{{run_id}}` (resolved at runtime) |
| `databricks_workspace_id` | `1234567890123456` |

### Data Sources

**Folders (folder_ids):**

| Source | Type | Drive ID |
| --- | --- | --- |
| Team_A | Folder | `<folder_id_a>` |
| Team_B | Folder | `<folder_id_b>` |

**Individual Files (file_ids):**

| Source | Type | Drive ID 
| --- | --- | --- 
| Sample Insights | Spreadsheet | `<file_id_1>` 
| Sample Slide Deck | Presentation (Google Slides) | `<file_id_2>` 


## Write Mode Behavior

| Table | Write Mode | Behavior |
| --- | --- | --- |
| `files_metadata` | **MERGE** (upsert on `file_id`) | Incremental — new files inserted, existing files updated |
| Stage 2 Iceberg tables | **Configurable per mapping** | `overwrite` (default) = truncate + replace; `append` = add rows |


### How to configure per table_mapping:
```json
[
  {"source_type": "folder", "target_table": "my_table", "source_subfolder": "path", "write_mode": "overwrite"},
  {"source_type": "folder", "target_table": "my_history", "source_subfolder": "path", "write_mode": "append"}
]
```

**`overwrite`** (default): Every run reads all files in the subfolder and replaces the entire Iceberg table (`createOrReplace`). Best for dimension-like data or when source files represent the full current state.

**`append`**: Every run reads all files in the subfolder and appends to the Iceberg table. Best for event/fact data where you want to accumulate history across runs. Caution: running `full_resync` with `append` will duplicate data unless you manually truncate first.
### Table Mappings

| Source Subfolder | Target Iceberg Table | Write Mode | Description |
| --- | --- | --- | --- |
| `Team_A/Polls` | `raw_dev.gdrive.team_a_polls` | overwrite | Polls |
| `Team_A/Q&A` | `raw_dev.gdrive.team_a_qna` | overwrite | Q&A |
| `Team_A/Surveys` | `raw_dev.gdrive.team_a_surveys` | overwrite | Surveys |
| `Team_B/Polls` | `raw_dev.gdrive.team_b_polls` | overwrite | Team B Polls |
| `Team_B/Q&A` | `raw_dev.gdrive.team_b_qna` | overwrite | Team B Q&A |
| `Team_B/Survey` | `raw_dev.gdrive.team_b_surveys` | overwrite | Team B Surveys |
| `Sample Insights.xlsx` | `raw_dev.gdrive.sample_insights` | overwrite | Sample insights (file) |

---

## Development

### Running Tests

```bash
pytest tests/ -v
```

### Key Design Decisions

* **Parameter-driven:** No code changes needed for new initiatives — just a new job with different parameters.
* **Two-stage pipeline:** Raw files always preserved in Volume (replay/debug); structured parsing is decoupled.
* **Jetstream-aligned:** Full control plane integration (self-registration, run tracking, state management, health monitoring).
* **Hierarchy control:** `preserve_hierarchy` and `date_partitioned` flags give flexibility for flat vs. structured Volume layouts.
* **Incremental extraction:** `modifiedTime` cursor stored in `connector_state` — only fetches new/changed files on each run.
* **folder_ids vs file_ids:** Explicit separation — folders are recursed, individual files are downloaded directly. Mismatched IDs fail loudly with clear errors.
* **Fail-loud, no silent skip:** Unresolvable Drive IDs raise `DriveListingError` with full context. Valid files still download before failure is reported.
* **Configurable write_mode:** Per-table-mapping choice of `overwrite` (truncate + replace) or `append` (accumulate rows).
* **Serverless-compatible:** Uses native `excel` format reader (not `com.crealytics.spark.excel`), `inferSchema=false`, and all-string normalization for safe schema unions. Job context (`job_id`, `run_id`, `workspace_id`) resolved via `{{job_id}}`/`{{run_id}}` dynamic value references in job parameters (Strategy 4 in `ctx.py`) since `getContext().tags()` and Spark conf are blocked on serverless.
* **USER_ISOLATION support:** Context resolution chain: (1) `getContext().tags()`, (2) Spark conf, (3) `getContext().toJson()`, (4) widget params with dynamic references. First strategy that produces a valid `job_id` wins.
* **Idempotency:** `connector_run.idempotency_key` = `{databricks_job_run_id}_{instance_name}` (e.g. `123456789012345_gdrive_example`). Prevents duplicate rows on retry; isolates multiple instances sharing a multi-task job.
* **Schema change tracking:** Before each Stage 2 Iceberg write, the incoming DataFrame schema is compared against the existing table. Changes (`create_table`, `add_column`, `drop_column`, `alter_type`) are recorded in `connector_schema_change_event` with the `table_id` FK to `connector_table_registry`.
* **Lifecycle logging:** `connector_log` captures events at every stage: `run_started`, `file_download_completed`, `object_load_completed`, `object_load_skipped`, `run_completed`, plus all error events (`config_validation_failed`, `run_failed`, `object_failed`, `stage2_load_failed`).

---

## License

See [LICENSE](./LICENSE).