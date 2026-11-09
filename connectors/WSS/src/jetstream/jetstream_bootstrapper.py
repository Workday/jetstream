"""Self-bootstrap logic for connector registration and instance creation."""
from __future__ import annotations

import json
from pathlib import Path
from typing import Any, Mapping, Optional, Tuple

from src.jetstream.jetstream_context import DatabricksContext, MetaCtx
from src.jetstream.jetstream_id_generator import uuid7
from src.jetstream.jetstream_param_compiler import compile_parameters_yaml
from src.retry import exec_with_concurrent_retry

DEFAULT_MANIFEST_VERSION = "1.0.0"

def _spark_sql(spark: Any, query: str, args: Optional[Mapping[str, Any]] = None) -> Any:
    def _do() -> Any:
        if args is not None:
            return spark.sql(query, args=args)
        return spark.sql(query)
    return exec_with_concurrent_retry(_do)

def ensure_registered_and_get_instance(
    spark: Any,
    meta: MetaCtx,
    *,
    connector_type_id: str,
    connector_type_name: str,
    category: str,
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
    compiled_schema = compile_parameters_yaml(parameters_yaml_path)
    _ensure_registry_row(
        spark, meta,
        connector_type_id=connector_type_id,
        connector_type_name=connector_type_name,
        category=category,
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

def lookup_instance_id(spark: Any, meta: MetaCtx, *, db_ctx: DatabricksContext) -> Optional[str]:
    rows = _spark_sql(
        spark,
        f"SELECT connector_instance_id FROM {meta.qual('connector_instance')} WHERE databricks_job_id = :job_id LIMIT 1",
        args={"job_id": db_ctx.job_id},
    ).collect()
    return rows[0]["connector_instance_id"] if rows else None

def _ensure_registry_row(spark: Any, meta: MetaCtx, *, connector_type_id: str, connector_type_name: str, category: str, connector_version: str, maintainer: str, repo_path: str) -> None:
    existing = _spark_sql(
        spark,
        f"SELECT connector_type_name, category, connector_version, maintainer, repo_path, is_active FROM {meta.qual('connector_registry')} WHERE connector_type_id = :connector_type_id LIMIT 1",
        args={"connector_type_id": connector_type_id},
    ).collect()
    if existing:
        row = existing[0]
        unchanged = (
            row["connector_type_name"] == connector_type_name
            and row["category"] == category
            and row["connector_version"] == connector_version
            and row["maintainer"] == maintainer
            and row["repo_path"] == repo_path
            and row["is_active"] is True
        )
        if unchanged:
            return

    _spark_sql(
        spark,
        f"""
        MERGE INTO {meta.qual('connector_registry')} t
        USING (SELECT
                 :connector_type_id   AS connector_type_id,
                 :connector_type_name AS connector_type_name,
                 :category            AS category,
                 :connector_version   AS connector_version,
                 :maintainer          AS maintainer,
                 :repo_path           AS repo_path
              ) s
        ON t.connector_type_id = s.connector_type_id
        WHEN MATCHED THEN UPDATE SET
            connector_type_name = s.connector_type_name,
            category            = s.category,
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
            s.connector_type_id, s.connector_type_name, s.category, NULL,
            s.repo_path, s.connector_version, s.maintainer, TRUE,
            current_timestamp(), current_timestamp()
        )
        """,
        args={
            "connector_type_id": connector_type_id,
            "connector_type_name": connector_type_name,
            "category": category,
            "connector_version": connector_version,
            "maintainer": maintainer,
            "repo_path": repo_path,
        },
    )

def _ensure_manifest_row(spark: Any, meta: MetaCtx, *, connector_type_id: str, compiled_schema: Mapping[str, Any], version: str) -> str:
    rows = _spark_sql(
        spark,
        f"SELECT manifest_version FROM {meta.qual('connector_manifest')} WHERE connector_type_id = :ct AND is_current = TRUE ORDER BY created_at DESC LIMIT 1",
        args={"ct": connector_type_id},
    ).collect()
    if rows:
        return rows[0]["manifest_version"]

    try:
        _spark_sql(
            spark,
            f"INSERT INTO {meta.qual('connector_manifest')} (manifest_id, connector_type_id, manifest_version, config_schema, is_current, created_at) VALUES (:manifest_id, :ct, :version, :schema, TRUE, current_timestamp())",
            args={"manifest_id": uuid7(), "ct": connector_type_id, "version": version, "schema": json.dumps(compiled_schema, default=str)},
        )
        return version
    except Exception:
        rows = _spark_sql(
            spark,
            f"SELECT manifest_version FROM {meta.qual('connector_manifest')} WHERE connector_type_id = :ct AND is_current = TRUE ORDER BY created_at DESC LIMIT 1",
            args={"ct": connector_type_id},
        ).collect()
        if rows:
            return rows[0]["manifest_version"]
        raise

def _find_or_create_instance(
    spark: Any, meta: MetaCtx, *, connector_type_id: str, instance_name: str, environment: str,
    owner_team: str, source_config: Mapping[str, Any], manifest_version: str,
    destination_catalog: str, destination_schema: str, sla_target_minutes: int, db_ctx: DatabricksContext,
) -> str:
    def _lookup_full() -> Optional[dict]:
        rows = _spark_sql(
            spark,
            f"SELECT connector_instance_id, instance_name, environment, owner_team, source_config, config_manifest_version, destination_catalog, destination_schema, sla_target_minutes, status FROM {meta.qual('connector_instance')} WHERE databricks_job_id = :job_id LIMIT 1",
            args={"job_id": db_ctx.job_id},
        ).collect()
        return rows[0].asDict() if rows else None

    source_config_json = json.dumps(dict(source_config), default=str)
    existing = _lookup_full()

    if existing:
        instance_id = existing["connector_instance_id"]
        sla_val = int(sla_target_minutes) if str(sla_target_minutes).strip().isdigit() else 1440
        unchanged = (
            existing["instance_name"] == instance_name
            and existing["environment"] == environment
            and existing["owner_team"] == owner_team
            and existing["source_config"] == source_config_json
            and existing["config_manifest_version"] == manifest_version
            and existing["destination_catalog"] == destination_catalog
            and existing["destination_schema"] == destination_schema
            and existing["sla_target_minutes"] == sla_val
            and existing["status"] == "active"
        )
        if unchanged:
            return instance_id

        _spark_sql(
            spark,
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
    try:
        _spark_sql(
            spark,
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
                "sla": int(sla_target_minutes) if str(sla_target_minutes).strip().isdigit() else 60,
                "created_by": db_ctx.triggered_by or "system",
            },
        )
        return instance_id
    except Exception:
        rows = _spark_sql(
            spark,
            f"SELECT connector_instance_id FROM {meta.qual('connector_instance')} WHERE databricks_job_id = :job_id LIMIT 1",
            args={"job_id": db_ctx.job_id},
        ).collect()
        if rows:
            return rows[0]["connector_instance_id"]
        raise