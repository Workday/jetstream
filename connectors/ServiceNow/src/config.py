"""Typed views over Job widget configuration."""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any, List, Mapping, Optional, Sequence

from src.objects import ObjectCfg, select_objects
from src.secrets import ServiceNowCredentials


@dataclass(frozen=True)
class RetryCfg:
    max_attempts: int = 5
    backoff_seconds: float = 1.0
    backoff_multiplier: float = 2.0
    retry_on_status: Sequence[int] = (429, 500, 502, 503, 504)


@dataclass(frozen=True)
class PlatformRouting:
    environment: str
    instance_name: str
    destination_catalog: str
    destination_schema: str
    metadata_catalog: str
    metadata_schema: str
    sla_target_minutes: int
    owner_team: str
    attachment_volume_base: str


@dataclass
class ObjectRunConfig:
    object_name: str
    sn_table: str
    sync_target: str
    fields: List[str]
    primary_key: str
    primary_key_columns: List[str]
    watermark_columns: List[str]
    lookback_minutes: int
    write_mode: str
    table_format: str
    destination_catalog: str
    destination_schema: str
    destination_table: str
    page_size: int
    attachment_volume_path: str
    attachment_meta_table: str = ""
    journals_meta_table: str = ""
    download_attachments: bool = True
    force_full: bool = False
    retry: RetryCfg = field(default_factory=RetryCfg)
    from_datetime: str = ""
    to_datetime: str = ""
    run_ts: datetime = field(default_factory=lambda: datetime.now(timezone.utc))

    @property
    def target_table(self) -> str:
        return f"{self.destination_catalog}.{self.destination_schema}.{self.destination_table}"

    @property
    def attachment_meta_fqn(self) -> str:
        name = self.attachment_meta_table or f"{self.destination_table}_attachments"
        return f"{self.destination_catalog}.{self.destination_schema}.{name}"

    @property
    def journals_meta_fqn(self) -> str:
        name = self.journals_meta_table or f"{self.destination_table}_journals"
        return f"{self.destination_catalog}.{self.destination_schema}.{name}"

    @property
    def is_adhoc_window(self) -> bool:
        return bool((self.from_datetime or "").strip())

    @property
    def use_cursors(self) -> bool:
        return not self.force_full and not self.is_adhoc_window

    @property
    def force_full_attachments(self) -> bool:
        return self.force_full

    @property
    def source_object(self) -> str:
        return f"/api/now/table/{self.sn_table}"

    @property
    def fetch_all_fields(self) -> bool:
        return not self.fields or self.fields == ["*"] or self.fields == ["ALL"]

    @property
    def run_ts_folder(self) -> str:
        return self.run_ts.strftime("%Y-%m-%d_%H-%M-%S")

    @property
    def extract_kind(self) -> str:
        if self.force_full:
            return "full"
        if self.is_adhoc_window:
            return "adhoc"
        return "incremental"


@dataclass(frozen=True)
class RunConfig:
    environment: str
    instance_name: str
    destination_catalog: str
    destination_schema: str
    metadata_catalog: str
    metadata_schema: str
    sla_target_minutes: int
    owner_team: str
    secret_scope: str
    attachment_volume_base: str
    page_size: int
    lookback_minutes: int
    download_attachments: bool
    sync_target: str
    attachment_meta_table: str
    journals_meta_table: str
    primary_key: str
    watermark_column: str
    enabled_objects_csv: str
    objects: List[ObjectCfg] = field(default_factory=list)
    retry: RetryCfg = field(default_factory=RetryCfg)
    from_datetime: str = ""
    to_datetime: str = ""
    credentials: Optional[ServiceNowCredentials] = None

    @property
    def is_adhoc_window(self) -> bool:
        return bool((self.from_datetime or "").strip())


def resolve_platform_routing(widgets: Mapping[str, Any]) -> PlatformRouting:
    def _str(k: str, default: str = "") -> str:
        v = widgets.get(k, default)
        return default if v is None else str(v).strip()

    def _int(k: str, default: int) -> int:
        v = widgets.get(k)
        return default if v in (None, "") else int(v)

    env = _str("environment", "dev")
    dest_catalog = _str("destination_catalog") or f"raw_{env}"
    dest_schema = _str("destination_schema") or "servicenow"
    vol = _str("attachment_volume_base")
    if not vol:
        vol = f"/Volumes/{dest_catalog}/{dest_schema}/attachments"

    return PlatformRouting(
        environment=env,
        instance_name=_str("instance_name") or f"servicenow_{env}",
        destination_catalog=dest_catalog,
        destination_schema=dest_schema,
        metadata_catalog=_str("metadata_catalog"),
        metadata_schema=_str("metadata_schema", "jetstream") or "jetstream",
        sla_target_minutes=_int("sla_target_minutes", 1440),
        owner_team=_str("owner_team"),
        attachment_volume_base=vol.rstrip("/"),
    )


def resolve_run_config(widgets: Mapping[str, Any], dbutils: Any = None) -> RunConfig:
    routing = resolve_platform_routing(widgets)

    def _str(k: str, default: str = "") -> str:
        v = widgets.get(k, default)
        return default if v is None else str(v).strip()

    def _int(k: str, default: int) -> int:
        v = widgets.get(k)
        return default if v in (None, "") else int(v)

    def _bool(k: str, default: bool = True) -> bool:
        v = widgets.get(k)
        if v in (None, ""):
            return default
        return str(v).strip().lower() in ("1", "true", "yes", "y")

    secret_scope = _str("secret_scope")
    enabled = _str("enabled_objects", "incident")
    target = _str("sync_target", "base")
    
    att_meta = _str("attachment_meta_table")
    j_meta = _str("journals_meta_table")
    
    primary_key = _str("primary_key", "sys_id") or "sys_id"
    watermark_column = _str("watermark_column", "sys_updated_on") or "sys_updated_on"
    
    objects = select_objects(
        enabled,
        attachment_meta_table=att_meta,
        journals_meta_table=j_meta,
        primary_key=primary_key,
        watermark_column=watermark_column,
    )

    return RunConfig(
        environment=routing.environment,
        instance_name=routing.instance_name,
        destination_catalog=routing.destination_catalog,
        destination_schema=routing.destination_schema,
        metadata_catalog=routing.metadata_catalog,
        metadata_schema=routing.metadata_schema,
        sla_target_minutes=routing.sla_target_minutes,
        owner_team=routing.owner_team,
        secret_scope=secret_scope,
        attachment_volume_base=routing.attachment_volume_base,
        page_size=_int("page_size", 10000),
        lookback_minutes=_int("lookback_minutes", 15),
        download_attachments=_bool("download_attachments", True),
        sync_target=target,
        attachment_meta_table=att_meta,
        journals_meta_table=j_meta,
        primary_key=primary_key,
        watermark_column=watermark_column,
        credentials=None,
        enabled_objects_csv=enabled,
        objects=objects,
        retry=RetryCfg(max_attempts=max(1, _int("retry_max_attempts", 5))),
        from_datetime=_str("from_datetime"),
        to_datetime=_str("to_datetime"),
    )


def build_object_run_config(
    obj: ObjectCfg,
    cfg: RunConfig,
    *,
    force_full: bool = False,
) -> ObjectRunConfig:
    lookback = cfg.lookback_minutes if cfg.lookback_minutes is not None else obj.lookback_minutes
    fields = list(obj.default_fields)
    vol_base = cfg.attachment_volume_base.rstrip("/")
    
    meta = obj.attachment_meta_table or f"{obj.name}_attachments"
    j_meta = obj.journals_meta_table or f"{obj.name}_journals"
    
    return ObjectRunConfig(
        object_name=obj.name,
        sn_table=obj.sn_table,
        sync_target=cfg.sync_target,
        fields=fields,
        primary_key=obj.primary_key,
        primary_key_columns=list(obj.primary_key_columns) or [obj.primary_key or "sys_id"],
        watermark_columns=list(obj.watermark_columns),
        lookback_minutes=lookback,
        write_mode=obj.write_mode,
        table_format=obj.table_format or "iceberg",
        destination_catalog=cfg.destination_catalog,
        destination_schema=cfg.destination_schema,
        destination_table=obj.name,
        page_size=cfg.page_size,
        attachment_volume_path=f"{vol_base}/{obj.name}" if vol_base else "",
        attachment_meta_table=meta,
        journals_meta_table=j_meta,
        download_attachments=cfg.download_attachments,
        force_full=force_full,
        retry=cfg.retry,
        from_datetime=cfg.from_datetime if not force_full else "",
        to_datetime=cfg.to_datetime if not force_full else "",
    )


def source_config_for_manifest(widgets: Mapping[str, Any]) -> dict:
    exclude = {"destination_catalog", "destination_schema"}
    return {k: v for k, v in widgets.items() if k not in exclude}
