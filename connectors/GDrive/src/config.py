"""Typed views over a validated `source_config` dict.

Under the Jetstream platform model, per-instance configuration lives in
Databricks Job parameters (read via `dbutils.widgets.getAll()`), not in a
YAML file inside this repo. The compiled JSON Schema for those parameters
lives in `connectors/gdrive/parameters.yaml`.

This module keeps the small `SourceCfg` / `ObjectCfg` / `RetryCfg` /
`FileDestination` dataclasses so `src/http.py`, `src/auth.py`, and
`src/extract.py` stay simple and testable without needing the whole platform
stack. All they see is a frozen dataclass.

The resolver hydrates those dataclasses from the widget dict + the Databricks
secret scope, once per run, in `resolve_run_config()`.
"""
from __future__ import annotations

import json
from dataclasses import dataclass, field
from typing import Any, Dict, List, Mapping, Optional, Sequence


# ---------------------------------------------------------------------------
# Small typed views
# ---------------------------------------------------------------------------

@dataclass(frozen=True)
class RetryCfg:
    max_attempts: int = 5
    backoff_seconds: float = 1.0
    backoff_multiplier: float = 2.0
    retry_on_status: Sequence[int] = (429, 500, 502, 503, 504)


@dataclass(frozen=True)
class SourceCfg:
    drive_api_base: str
    token_uri: str
    client_id: str             # resolved from Databricks secret
    client_secret: str         # resolved
    refresh_token: str         # resolved
    rate_limit_qps: float = 10
    retry: RetryCfg = field(default_factory=RetryCfg)
    timeout_seconds: int = 60


@dataclass(frozen=True)
class ObjectCfg:
    name: str
    # 'metadata'  -> parsed rows into an Iceberg destination table
    # 'file_copy' -> whole file bytes into a UC Volume or S3 prefix
    kind: str
    primary_key: Sequence[str] = ()
    load_strategy: str = "full"          # 'full' | 'incremental_cursor'
    cursor_field: Optional[str] = None
    table_format: str = "iceberg"        # 'iceberg' | 'volume' | 's3_prefix'
    optional: bool = False


@dataclass(frozen=True)
class FileDestination:
    """Where the `files` (file_copy) object lands whole file bytes.

    Stage 1 downloads all files from Drive into this destination.  When
    preserve_hierarchy is True, the folder structure from Google Drive is
    replicated under the volume/S3 path.  When date_partitioned is True,
    a /YYYY-MM-DD/ directory is inserted so each run lands in an isolated
    daily partition.

    Example volume layout (preserve_hierarchy=True, date_partitioned=True):
        /Volumes/raw_dev/landing/inbound/gdrive/2026-08-28/Team_A/
        ├── Polls/
        │   ├── august_polls.csv
        │   └── poll_summary.docx
        ├── QnA/
        │   └── Q3_qna_data.xlsx
        └── Surveys/
            └── user_feedback.xlsx
    """
    kind: str = "volume"               # 'volume' | 's3'
    volume_path: str = ""
    s3_bucket: str = ""
    s3_prefix: str = ""
    s3_role_arn: str = ""
    s3_role_session_name: str = "jetstream-gdrive"
    preserve_hierarchy: bool = True    # Replicate Drive folder structure in storage
    date_partitioned: bool = True      # Insert /YYYY-MM-DD/ directory per run

    @property
    def table_format(self) -> str:
        """connector_table_registry.table_format value (spec §6.6/§6.10)."""
        return "s3_prefix" if self.kind == "s3" else "volume"

    @property
    def destination_label(self) -> str:
        """Value stored in destination_table for the file-copy object (spec §6.10)."""
        if self.kind == "s3":
            return f"s3://{self.s3_bucket}/{self.s3_prefix.strip('/')}"
        return self.volume_path

    def validate(self) -> None:
        """Raise ValueError if this destination is not fully configured."""
        if self.kind == "volume":
            if not self.volume_path:
                raise ValueError(
                    "file_destination_type=volume requires a non-empty volume_path"
                )
        elif self.kind == "s3":
            missing = [
                k for k, v in (
                    ("s3_bucket", self.s3_bucket),
                    ("s3_prefix", self.s3_prefix),
                    ("s3_role_arn", self.s3_role_arn),
                ) if not v
            ]
            if missing:
                raise ValueError(
                    f"file_destination_type=s3 requires: {', '.join(missing)}"
                )
        else:
            raise ValueError(
                f"file_destination_type must be 'volume' or 's3', got {self.kind!r}"
            )


# ---------------------------------------------------------------------------
# Structured Iceberg table mapping
# ---------------------------------------------------------------------------

# Supported MIME types / extensions for structured ingestion into Iceberg.
STRUCTURED_FORMATS = frozenset({"csv", "xlsx", "xls", "google_sheets"})


@dataclass(frozen=True)
class TableMapping:
    """Maps a Volume subfolder (or single file path) to a target Iceberg table.

    Used by Stage 2 of the connector pipeline:

    **Stage 1 (extract.py):** Downloads all files from top-level `folder_ids`
    into the raw Volume, preserving folder hierarchy.

    **Stage 2 (load.py):** For each TableMapping, scans `source_subfolder`
    within the daily Volume directory, filters by `supported_formats` (skipping
    .docx, .pdf, .pptx etc.), and writes parsed tabular data to `target_table`.

    Examples:
        Folder mode:  source_subfolder="Team_A/Polls" -> scan all files -> one Iceberg table
        File mode:    source_subfolder="Team_A/insights.xlsx" -> single file -> one Iceberg table
    """
    source_type: str                     # 'folder' | 'file'
    target_table: str                    # Destination table name (without catalog/schema)
    source_subfolder: str = ""           # Relative path in Volume for Stage 2 reads
    supported_formats: Sequence[str] = field(default_factory=lambda: list(STRUCTURED_FORMATS))
    recursive: bool = True               # For folder mode: recurse into subfolders
    write_mode: str = "overwrite"        # 'overwrite' (truncate+replace) | 'append'
    description: str = ""                # Optional human-readable label

    def __post_init__(self) -> None:
        if self.source_type not in ("folder", "file"):
            raise ValueError(
                f"TableMapping.source_type must be 'folder' or 'file', "
                f"got {self.source_type!r}"
            )
        if self.write_mode not in ("overwrite", "append"):
            raise ValueError(
                f"TableMapping.write_mode must be 'overwrite' or 'append', "
                f"got {self.write_mode!r}"
            )
        if not self.target_table:
            raise ValueError("TableMapping.target_table must be non-empty")
        if not self.source_subfolder:
            raise ValueError("TableMapping.source_subfolder must be non-empty")


# ---------------------------------------------------------------------------
# Runtime resolver
# ---------------------------------------------------------------------------

# Convention: every gdrive instance stores its OAuth material under one secret
# scope, with these three fixed keys.
_SECRET_KEYS = {
    "client_id": "client_id",
    "client_secret": "client_secret",
    "refresh_token": "refresh_token",
}


@dataclass(frozen=True)
class PlatformRouting:
    """Non-secret routing values needed before the run row is opened.

    Everything here can be read directly from widget values without any
    Databricks secret lookups. Used by pipeline.run() for the metadata
    catalog + bootstrap + connector_instance columns, so a bad secret
    scope doesn't crash the pipeline before it has a `connector_run`
    row to log the failure against.
    """
    environment: str
    instance_name: str
    destination_catalog: str
    destination_schema: str
    metadata_catalog: str
    metadata_schema: str
    sla_target_minutes: int
    owner_team: str
    tags: Mapping[str, str] = field(default_factory=dict)


def resolve_platform_routing(widgets: Mapping[str, Any]) -> PlatformRouting:
    """Extract non-secret routing values from a coerced widget dict."""
    def _str(k: str, default: str = "") -> str:
        v = widgets.get(k, default)
        return default if v is None else str(v).strip()

    def _int(k: str, default: int) -> int:
        v = widgets.get(k)
        return default if v in (None, "") else int(v)

    return PlatformRouting(
        environment=_str("environment"),
        instance_name=_str("instance_name"),
        destination_catalog=_str("destination_catalog"),
        destination_schema=_str("destination_schema"),
        metadata_catalog=_str("metadata_catalog"),
        metadata_schema=_str("metadata_schema", "jetstream") or "jetstream",
        sla_target_minutes=_int("sla_target_minutes", 1440),
        owner_team=_str("owner_team"),
        tags=_parse_tags(widgets.get("tags", "")),
    )


@dataclass(frozen=True)
class RunConfig:
    """Everything the pipeline needs for one run, after widget + secret resolution."""
    # From platform routing widgets
    environment: str
    instance_name: str
    destination_catalog: str
    destination_schema: str
    metadata_catalog: str
    metadata_schema: str
    sla_target_minutes: int
    owner_team: str
    # Google Drive source auth + tuning
    source: SourceCfg
    # File-copy destination for the `files` object
    file_destination: FileDestination
    # What to sync
    folder_ids_csv: str
    file_ids_csv: str
    enabled_objects_csv: str
    file_export_format: str
    max_file_bytes: int
    # Fields with defaults must come last
    tags: Mapping[str, str] = field(default_factory=dict)
    # Structured Iceberg table mappings (folder/file -> target table)
    table_mappings: Sequence[TableMapping] = field(default_factory=list)


def resolve_run_config(widgets: Mapping[str, Any], dbutils: Any = None) -> RunConfig:
    """Build a RunConfig from a validated widget dict.

    Callers are expected to have already run
    `jetstream.validate.validate_source_config(widgets, config_schema)` so
    required fields are present and typed. Defaults from `parameters.yaml`
    are re-applied here for the optional fields, since `getAll()` returns
    string values only.
    """
    def _str(k: str, default: str = "") -> str:
        v = widgets.get(k, default)
        return default if v is None else str(v).strip()

    def _int(k: str, default: int) -> int:
        v = widgets.get(k)
        return default if v in (None, "") else int(v)

    def _float(k: str, default: float) -> float:
        v = widgets.get(k)
        return default if v in (None, "") else float(v)

    secret_scope = _str("secret_scope")
    resolve = _secret_resolver(secret_scope, dbutils)

    retry = RetryCfg(
        max_attempts=_int("retry_max_attempts", 5),
        backoff_seconds=1.0,
        backoff_multiplier=2.0,
        retry_on_status=(429, 500, 502, 503, 504),
    )
    source = SourceCfg(
        drive_api_base=_str("drive_api_base_url", "https://www.googleapis.com/drive/v3").rstrip("/"),
        token_uri=_str("token_uri", "https://oauth2.googleapis.com/token"),
        client_id=resolve(_SECRET_KEYS["client_id"]),
        client_secret=resolve(_SECRET_KEYS["client_secret"]),
        refresh_token=resolve(_SECRET_KEYS["refresh_token"]),
        rate_limit_qps=_float("rate_limit_qps", 10),
        retry=retry,
        timeout_seconds=_int("timeout_seconds", 60),
    )
    def _bool(k: str, default: bool) -> bool:
        v = widgets.get(k)
        if v in (None, ""):
            return default
        if isinstance(v, bool):
            return v
        return str(v).strip().lower() in ("true", "1", "yes")

    file_destination = FileDestination(
        kind=_str("file_destination_type", "volume") or "volume",
        volume_path=_str("volume_path"),
        s3_bucket=_str("s3_bucket"),
        s3_prefix=_str("s3_prefix"),
        s3_role_arn=_str("s3_role_arn"),
        s3_role_session_name=_str("s3_role_session_name", "jetstream-gdrive") or "jetstream-gdrive",
        preserve_hierarchy=_bool("preserve_hierarchy", True),
        date_partitioned=_bool("date_partitioned", True),
    )
    return RunConfig(
        environment=_str("environment"),
        instance_name=_str("instance_name"),
        destination_catalog=_str("destination_catalog"),
        destination_schema=_str("destination_schema"),
        metadata_catalog=_str("metadata_catalog"),
        metadata_schema=_str("metadata_schema", "jetstream") or "jetstream",
        sla_target_minutes=_int("sla_target_minutes", 1440),
        owner_team=_str("owner_team"),
        tags=_parse_tags(widgets.get("tags", "")),
        source=source,
        file_destination=file_destination,
        folder_ids_csv=_str("folder_ids"),
        file_ids_csv=_str("file_ids"),
        enabled_objects_csv=_str("enabled_objects"),
        file_export_format=_str("file_export_format"),
        max_file_bytes=_int("max_file_bytes", 0),
        table_mappings=_parse_table_mappings(widgets.get("table_mappings", "")),
    )


def source_config_for_manifest(widgets: Mapping[str, Any]) -> dict:
    """Return the subset of widget values that goes into
    connector_instance.source_config (JSON). Excludes destination_catalog
    and destination_schema, which are separate top-level columns."""
    exclude = {"destination_catalog", "destination_schema"}
    return {k: v for k, v in widgets.items() if k not in exclude}


def _parse_table_mappings(raw: Any) -> List[TableMapping]:
    """Parse the table_mappings widget value into a list of TableMapping objects.

    Accepts a JSON array string, e.g.:
        [
          {"source_id": "<folder_id>", "source_type": "folder",
           "target_table": "team_a_polls"},
          {"source_id": "<file_id>", "source_type": "file",
           "target_table": "sample_insights"}
        ]
    Optional keys per entry: supported_formats, recursive, description.
    Returns an empty list for None/empty.
    """
    if not raw:
        return []
    if isinstance(raw, list):
        entries = raw
    else:
        try:
            # Strip trailing commas before ] or } (common user mistake).
            import re
            cleaned = re.sub(r",\s*([\]\}])", r"\1", str(raw).strip())
            entries = json.loads(cleaned)
        except (json.JSONDecodeError, TypeError):
            raise ValueError(
                f"table_mappings must be a valid JSON array, got: {raw!r}"
            )
    if not isinstance(entries, list):
        raise ValueError(
            f"table_mappings must be a JSON array, not {type(entries).__name__}"
        )

    mappings: List[TableMapping] = []
    for i, entry in enumerate(entries):
        if not isinstance(entry, dict):
            raise ValueError(
                f"table_mappings[{i}] must be an object, got {type(entry).__name__}"
            )
        formats = entry.get("supported_formats", list(STRUCTURED_FORMATS))
        mappings.append(TableMapping(
            source_type=str(entry.get("source_type", "folder")),
            target_table=str(entry.get("target_table", "")),
            source_subfolder=str(entry.get("source_subfolder", "")),
            supported_formats=list(formats) if isinstance(formats, list) else [formats],
            recursive=bool(entry.get("recursive", True)),
            description=str(entry.get("description", "")),
        ))
    return mappings


def _parse_tags(raw: Any) -> Dict[str, str]:
    """Parse a tags widget value into a Map of string keys to string values.

    Accepts either a JSON object string (e.g. '{"team": "data", "env": "prod"}')
    or an already-parsed dict. Returns an empty dict for None/empty.
    """
    if not raw:
        return {}
    if isinstance(raw, dict):
        return {str(k): str(v) for k, v in raw.items()}
    try:
        import re
        cleaned = re.sub(r",\s*([\]\}])", r"\1", str(raw).strip())
        parsed = json.loads(cleaned)
    except (json.JSONDecodeError, TypeError):
        raise ValueError(
            f"tags must be a valid JSON object of key-value strings, got: {raw!r}"
        )
    if not isinstance(parsed, dict):
        raise ValueError(
            f"tags must be a JSON object (map), not {type(parsed).__name__}"
        )
    return {str(k): str(v) for k, v in parsed.items()}


def _secret_resolver(scope: str, dbutils: Any):
    if not scope:
        return lambda key: f"<no-scope:{key}>"
    if dbutils is None:
        return lambda key: f"<{scope}:{key}>"
    return lambda key: dbutils.secrets.get(scope=scope, key=key)
