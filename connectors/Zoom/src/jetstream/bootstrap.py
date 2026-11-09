"""Self-bootstrap logic (spec §7 "Instance Bootstrapping").

Called at the top of every run. Ensures three rows exist:
  * connector_registry (one per connector type, workspace-wide)
  * connector_manifest (compiled from parameters.yaml, versioned)
  * connector_instance  (one per Databricks job — located by databricks_job_id)

On first run for a given job, all three are inserted. On every later run,
we locate the instance by databricks_job_id and refresh its source_config,
destination_catalog, destination_schema, updated_at fields.

Returns the connector_instance_id and the manifest_version that this run's
source_config should be validated against.
"""
from __future__ import annotations

import json
from pathlib import Path
from typing import Any, Mapping, Tuple

from src.jetstream.ctx import DatabricksContext, MetaCtx
from src.jetstream.ids import uuid7
from src.jetstream.params import compile_parameters_yaml


DEFAULT_MANIFEST_VERSION = "1.0.0"


def ensure_registered_and_get_instance(
    spark: Any,
    meta: MetaCtx,
    *,
    connector_type_id: str,
    connector_type_name: str,
    category: str,                  # 'file_object_storage' | 'database' | 'application_api' | 'event_streaming'
    description: str | None = None,
    parameters_yaml_path: str | Path,
    connector_version: str,
    maintainer: str,
    repo_path: str,
    source_config: Mapping[str, Any],
    destination_catalog: str,
    destination_schema: str,
    instance_name: str,
    environment: str,
    sla_target_minutes: int,
    owner_team: str,
    db_ctx: DatabricksContext,
) -> Tuple[str, str, Mapping[str, Any]]:
    """Ensure registry/manifest/instance rows exist and return
    (connector_instance_id, manifest_version, compiled_schema)."""
    compiled_schema = compile_parameters_yaml(parameters_yaml_path)
    _ensure_registry_row(
        spark, meta,
        connector_type_id=connector_type_id,
        connector_type_name=connector_type_name,
        category=category,
        description=description,
        connector_version=connector_version,
        maintainer=maintainer,
        repo_path=repo_path,
    )
    manifest_version = _ensure_manifest_row(
        spark, meta,
        connector_type_id=connector_type_id,
        compiled_schema=compiled_schema,
        version=DEFAULT_MANIFEST_VERSION,
    )
    instance_id = _find_or_create_instance(
        spark, meta,
        connector_type_id=connector_type_id,
        instance_name=instance_name,
        environment=environment,
        owner_team=owner_team,
        source_config=source_config,
        manifest_version=manifest_version,
        destination_catalog=destination_catalog,
        destination_schema=destination_schema,
        sla_target_minutes=sla_target_minutes,
        db_ctx=db_ctx,
    )
    return instance_id, manifest_version, compiled_schema


# ---------------------------------------------------------------------------
# connector_registry
# ---------------------------------------------------------------------------

def _ensure_registry_row(
    spark: Any,
    meta: MetaCtx,
    *,
    connector_type_id: str,
    connector_type_name: str,
    category: str,
    description: str | None,
    connector_version: str,
    maintainer: str,
    repo_path: str,
) -> None:
    spark.sql(
        f"""
        MERGE INTO {meta.qual('connector_registry')} t
        USING (SELECT
                 :connector_type_id   AS connector_type_id,
                 :connector_type_name AS connector_type_name,
                 :category            AS category,
                 :description         AS description,
                 :connector_version   AS connector_version,
                 :maintainer          AS maintainer,
                 :repo_path           AS repo_path
              ) s
        ON t.connector_type_id = s.connector_type_id
        WHEN MATCHED THEN UPDATE SET
            connector_type_name = s.connector_type_name,
            category            = s.category,
            description         = s.description,
            connector_version   = s.connector_version,
            maintainer          = s.maintainer,
            repo_path           = s.repo_path,
            is_active           = TRUE,
            updated_at          = current_timestamp()
        WHEN NOT MATCHED THEN INSERT (
            connector_type_id, connector_type_name, category, description,
            repo_path, connector_version, maintainer, is_active,
            created_at, updated_at
        ) VALUES (
            s.connector_type_id, s.connector_type_name, s.category, s.description,
            s.repo_path, s.connector_version, s.maintainer, TRUE,
            current_timestamp(), current_timestamp()
        )
        """,
        args={
            "connector_type_id": connector_type_id,
            "connector_type_name": connector_type_name,
            "category": category,
            "description": description,
            "connector_version": connector_version,
            "maintainer": maintainer,
            "repo_path": repo_path,
        },
    )


# ---------------------------------------------------------------------------
# connector_manifest
# ---------------------------------------------------------------------------

def _ensure_manifest_row(
    spark: Any,
    meta: MetaCtx,
    *,
    connector_type_id: str,
    compiled_schema: Mapping[str, Any],
    version: str,
) -> str:
    """Insert a manifest row on first-ever registration; return the version to use."""
    rows = spark.sql(
        f"""
        SELECT manifest_version
        FROM {meta.qual('connector_manifest')}
        WHERE connector_type_id = :ct AND is_current = TRUE
        ORDER BY created_at DESC
        LIMIT 1
        """,
        args={"ct": connector_type_id},
    ).collect()
    if rows:
        return rows[0]["manifest_version"]

    spark.sql(
        f"""
        INSERT INTO {meta.qual('connector_manifest')} (
            manifest_id, connector_type_id, manifest_version,
            config_schema, is_current, created_at
        ) VALUES (
            :manifest_id, :ct, :version, :schema, TRUE, current_timestamp()
        )
        """,
        args={
            "manifest_id": uuid7(),
            "ct": connector_type_id,
            "version": version,
            "schema": json.dumps(compiled_schema, default=str),
        },
    )
    return version


# ---------------------------------------------------------------------------
# connector_instance
# ---------------------------------------------------------------------------

def _find_or_create_instance(
    spark: Any,
    meta: MetaCtx,
    *,
    connector_type_id: str,
    instance_name: str,
    environment: str,
    owner_team: str,
    source_config: Mapping[str, Any],
    manifest_version: str,
    destination_catalog: str,
    destination_schema: str,
    sla_target_minutes: int,
    db_ctx: DatabricksContext,
) -> str:
    """Locate a connector_instance by databricks_job_id; create it on first run.

    On every subsequent run for the same job, refresh source_config,
    destination_catalog, destination_schema, and audit columns so a job-param
    edit picks up on the next run without any manual step.
    """
    rows = spark.sql(
        f"""
        SELECT connector_instance_id
        FROM {meta.qual('connector_instance')}
        WHERE databricks_job_id = :job_id
        LIMIT 1
        """,
        args={"job_id": db_ctx.job_id},
    ).collect()

    source_config_json = json.dumps(dict(source_config), default=str)

    if rows:
        instance_id = rows[0]["connector_instance_id"]
        spark.sql(
            f"""
            UPDATE {meta.qual('connector_instance')}
            SET instance_name           = :instance_name,
                environment             = :environment,
                owner_team              = :owner_team,
                source_config           = :source_config,
                config_manifest_version = :manifest_version,
                destination_catalog     = :destination_catalog,
                destination_schema      = :destination_schema,
                sla_target_minutes      = :sla,
                status                  = 'active',
                updated_at              = current_timestamp()
            WHERE connector_instance_id = :id
            """,
            args={
                "id": instance_id,
                "instance_name": instance_name,
                "environment": environment,
                "owner_team": owner_team,
                "source_config": source_config_json,
                "manifest_version": manifest_version,
                "destination_catalog": destination_catalog,
                "destination_schema": destination_schema,
                "sla": int(sla_target_minutes),
            },
        )
        return instance_id

    instance_id = uuid7()
    spark.sql(
        f"""
        INSERT INTO {meta.qual('connector_instance')} (
            connector_instance_id, connector_type_id, instance_name, environment,
            owner_team, source_config, config_manifest_version,
            databricks_job_id, destination_catalog, destination_schema,
            schedule_cron, sla_target_minutes, status, tags,
            created_by, created_at, updated_at
        ) VALUES (
            :id, :connector_type_id, :instance_name, :environment,
            :owner_team, :source_config, :manifest_version,
            :job_id, :destination_catalog, :destination_schema,
            NULL, :sla, 'active', NULL,
            :created_by, current_timestamp(), current_timestamp()
        )
        """,
        args={
            "id": instance_id,
            "connector_type_id": connector_type_id,
            "instance_name": instance_name,
            "environment": environment,
            "owner_team": owner_team,
            "source_config": source_config_json,
            "manifest_version": manifest_version,
            "job_id": db_ctx.job_id,
            "destination_catalog": destination_catalog,
            "destination_schema": destination_schema,
            "sla": int(sla_target_minutes),
            "created_by": db_ctx.triggered_by or "system",
        },
    )
    return instance_id
