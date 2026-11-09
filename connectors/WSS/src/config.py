"""Configuration loader for WSS connector."""
from __future__ import annotations

import os
from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional
import yaml

_REPO_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))


def _safe_int(val: Any, default: int) -> int:
    if val is None or str(val).strip() == "":
        return default
    try:
        return int(val)
    except (ValueError, TypeError):
        return default


def _safe_float(val: Any, default: float) -> float:
    if val is None or str(val).strip() == "":
        return default
    try:
        return float(val)
    except (ValueError, TypeError):
        return default


@dataclass
class RetryCfg:
    max_retries: int = 10
    base_backoff_sec: float = 10.0
    max_backoff_sec: float = 300.0
    status_codes: List[int] = field(default_factory=lambda: [429, 500, 502, 503, 504])

    def to_dict(self) -> Dict[str, Any]:
        return {
            "max_retries": self.max_retries,
            "base_backoff_sec": self.base_backoff_sec,
            "max_backoff_sec": self.max_backoff_sec,
            "status_codes": self.status_codes,
        }


@dataclass
class RoutingConfig:
    destination_catalog: str = "raw_dev"
    destination_schema: str = "wss"
    raw_json_volume_base: str = ""
    attachment_volume_path: str = ""
    environment: str = "dev"
    is_sandbox: bool = False
    metadata_catalog: str = "metadata_dev"
    metadata_schema: str = "jetstream"
    control_catalog: str = ""
    control_schema: str = ""
    instance_name: str = "wss"
    sla_target_minutes: int = 1440
    owner_team: str = ""

    def get(self, key: str, default: Any = "") -> Any:
        return getattr(self, key, default)


@dataclass
class ObjectConfig:
    name: str
    endpoint: str
    api_type: str = "FULL"
    auth_pattern: str = "pattern_3"
    primary_key: str = "id"
    dependency_object: Optional[str] = None
    parent_param_key: Optional[str] = None
    has_attachments: bool = False


@dataclass
class ObjectRunConfig:
    object_name: str
    source_object: str
    target_table: str
    endpoint: str
    api_type: str
    auth_pattern: str
    primary_key: str
    primary_key_columns: List[str]
    load_strategy: str
    use_cursors: bool
    page_size: int = 100
    lookback_minutes: int = 30
    destination_catalog: str = ""
    destination_schema: str = ""
    raw_json_volume_base: str = ""
    attachment_volume_path: str = ""
    has_attachments: bool = False
    download_attachments: bool = True
    dependency_object: Optional[str] = None
    parent_param_key: Optional[str] = None
    from_datetime: Optional[str] = None
    to_datetime: Optional[str] = None
    retry: Dict[str, Any] = field(default_factory=dict)

    @property
    def name(self) -> str:
        return self.object_name


@dataclass
class ConnectorConfig:
    connector_instance_id: str = "wss_default"
    secret_scope: str = "jetstream_wss"
    page_size: int = 100
    lookback_minutes: int = 30
    download_attachments: bool = True
    routing: RoutingConfig = field(default_factory=RoutingConfig)
    retry_config: Dict[str, Any] = field(default_factory=dict)
    retry: RetryCfg = field(default_factory=RetryCfg)
    raw_json_volume_base: str = ""
    attachment_volume_path: str = ""
    from_datetime: Optional[str] = None
    to_datetime: Optional[str] = None
    credentials: Any = None
    is_sandbox: bool = False
    objects: List[ObjectRunConfig] = field(default_factory=list)


Config = ConnectorConfig


def source_config_for_manifest(*args: Any, **kwargs: Any) -> Dict[str, Any]:
    cfg = kwargs.get("cfg") or (args[0] if args else None)
    obj_cfg = kwargs.get("obj_cfg") or (args[1] if len(args) > 1 else None)

    manifest_dict: Dict[str, Any] = {
        "connector_type": "wss",
        "connector_instance_id": getattr(cfg, "connector_instance_id", "wss_default") if cfg else "wss_default",
        "provider": "WSS",
    }

    if obj_cfg:
        manifest_dict.update({
            "object_name": getattr(obj_cfg, "name", getattr(obj_cfg, "object_name", "")),
            "endpoint": getattr(obj_cfg, "endpoint", ""),
            "api_type": getattr(obj_cfg, "api_type", ""),
            "auth_pattern": getattr(obj_cfg, "auth_pattern", ""),
        })

    return manifest_dict


def resolve_platform_routing(*args: Any, **kwargs: Any) -> RoutingConfig:
    cfg = args[0] if args else kwargs.get("cfg") or kwargs.get("widget_params") or {}

    if isinstance(cfg, RoutingConfig):
        return cfg

    if hasattr(cfg, "routing") and getattr(cfg, "routing", None):
        r = getattr(cfg, "routing")
        if isinstance(r, RoutingConfig):
            return r

    if isinstance(cfg, dict):
        env = str(cfg.get("environment") or os.environ.get("ENVIRONMENT", "dev") or "dev").strip().lower()
        dest_catalog = str(cfg.get("destination_catalog") or os.environ.get("DESTINATION_CATALOG", "") or f"raw_{env}")
        dest_schema = str(cfg.get("destination_schema") or os.environ.get("DESTINATION_SCHEMA", "") or "wss")
        attachment_volume = str(
            cfg.get("attachment_volume_base")
            or cfg.get("attachment_volume_path")
            or os.environ.get("ATTACHMENT_VOLUME_PATH", "")
            or f"/Volumes/{dest_catalog}/{dest_schema}/attachments"
        )
        raw_json_volume = str(
            cfg.get("raw_json_volume_base")
            or os.environ.get("RAW_JSON_VOLUME_BASE", "")
            or f"/Volumes/{dest_catalog}/{dest_schema}/api_responses"
        )
        meta_default = "metadata_prod" if env == "prod" else "metadata_dev"
        return RoutingConfig(
            destination_catalog=dest_catalog,
            destination_schema=dest_schema,
            raw_json_volume_base=raw_json_volume,
            attachment_volume_path=attachment_volume.rstrip("/"),
            environment=env,
            is_sandbox=bool(str(cfg.get("is_sandbox", False)).lower() in ("true", "1", "yes")),
            metadata_catalog=str(cfg.get("metadata_catalog") or os.environ.get("METADATA_CATALOG", "") or meta_default),
            metadata_schema=str(cfg.get("metadata_schema") or os.environ.get("METADATA_SCHEMA", "") or "jetstream"),
            control_catalog=str(cfg.get("control_catalog") or os.environ.get("CONTROL_CATALOG", "")),
            control_schema=str(cfg.get("control_schema") or os.environ.get("CONTROL_SCHEMA", "")),
            instance_name=str(cfg.get("instance_name") or "wss"),
            sla_target_minutes=_safe_int(cfg.get("sla_target_minutes"), 1440),
            owner_team=str(cfg.get("owner_team") or ""),
        )

    return RoutingConfig()


def parse_object_config(name: str, raw_obj: Dict[str, Any]) -> ObjectConfig:
    resolved_auth = (
        raw_obj.get("auth_pattern")
        or raw_obj.get("pattern")
        or raw_obj.get("auth")
        or "pattern_3"
    )

    return ObjectConfig(
        name=name,
        endpoint=str(raw_obj.get("endpoint", "")),
        api_type=str(raw_obj.get("api_type", "FULL")).upper(),
        auth_pattern=str(resolved_auth).strip().lower(),
        primary_key=str(raw_obj.get("primary_key", "id")),
        dependency_object=raw_obj.get("dependency_object"),
        parent_param_key=raw_obj.get("parent_param_key"),
        has_attachments=bool(raw_obj.get("has_attachments", False)),
    )


def load_catalog(catalog_path: str) -> Dict[str, ObjectConfig]:
    if not os.path.exists(catalog_path):
        return {}

    with open(catalog_path, "r", encoding="utf-8") as f:
        raw_data = yaml.safe_load(f) or {}

    if "objects" in raw_data and isinstance(raw_data["objects"], dict):
        raw_data = raw_data["objects"]
    elif "apis" in raw_data and isinstance(raw_data["apis"], dict):
        raw_data = raw_data["apis"]

    configs = {}
    for obj_name, obj_data in raw_data.items():
        if isinstance(obj_data, dict):
            configs[obj_name] = parse_object_config(obj_name, obj_data)

    return configs


def load_config(config_path: str, dbutils: Any = None) -> ConnectorConfig:
    if not os.path.exists(config_path):
        return ConnectorConfig()

    with open(config_path, "r", encoding="utf-8") as f:
        raw_data = yaml.safe_load(f) or {}

    routing_data = raw_data.get("routing", {})
    routing = RoutingConfig(
        destination_catalog=str(routing_data.get("destination_catalog", "")),
        destination_schema=str(routing_data.get("destination_schema", "")),
        raw_json_volume_base=str(routing_data.get("raw_json_volume_base", "")),
        attachment_volume_path=str(routing_data.get("attachment_volume_path", "")),
        environment=str(routing_data.get("environment", raw_data.get("environment", "dev"))),
        is_sandbox=bool(routing_data.get("is_sandbox", raw_data.get("is_sandbox", False))),
        metadata_catalog=str(routing_data.get("metadata_catalog", "")),
        metadata_schema=str(routing_data.get("metadata_schema", "")),
        control_catalog=str(routing_data.get("control_catalog", "")),
        control_schema=str(routing_data.get("control_schema", "")),
        instance_name=str(raw_data.get("instance_name") or "wss"),
        sla_target_minutes=_safe_int(raw_data.get("sla_target_minutes"), 1440),
        owner_team=str(raw_data.get("owner_team") or ""),
    )

    retry_data = raw_data.get("retry", {})
    retry = RetryCfg(
        max_retries=_safe_int(retry_data.get("max_retries"), 10),
        base_backoff_sec=_safe_float(retry_data.get("base_backoff_sec"), 10.0),
        max_backoff_sec=_safe_float(retry_data.get("max_backoff_sec"), 300.0),
        status_codes=list(retry_data.get("status_codes", [429, 500, 502, 503, 504])),
    )

    return ConnectorConfig(
        connector_instance_id=str(raw_data.get("connector_instance_id") or "wss_default"),
        secret_scope=str(raw_data.get("secret_scope") or "jetstream_wss"),
        page_size=_safe_int(raw_data.get("page_size"), 100),
        lookback_minutes=_safe_int(raw_data.get("lookback_minutes"), 30),
        download_attachments=bool(raw_data.get("download_attachments", True)),
        routing=routing,
        retry_config=retry.to_dict(),
        retry=retry,
        raw_json_volume_base=routing.raw_json_volume_base,
        attachment_volume_path=routing.attachment_volume_path,
    )


def build_object_run_config(
    obj_cfg: Any,
    cfg: ConnectorConfig,
    force_full: bool = False,
) -> ObjectRunConfig:
    if isinstance(obj_cfg, ObjectRunConfig):
        if force_full:
            obj_cfg.load_strategy = "full_refresh"
            obj_cfg.use_cursors = False
        return obj_cfg

    dest_cat = cfg.routing.destination_catalog
    dest_sch = cfg.routing.destination_schema

    target_table = f"`{dest_cat}`.`{dest_sch}`.`{obj_cfg.name}`"

    load_strategy = (
        "full_refresh"
        if force_full or obj_cfg.api_type == "FULL"
        else "incremental_cursor"
    )

    return ObjectRunConfig(
        object_name=obj_cfg.name,
        source_object=obj_cfg.name,
        target_table=target_table,
        endpoint=obj_cfg.endpoint,
        api_type=obj_cfg.api_type,
        auth_pattern=obj_cfg.auth_pattern,
        primary_key=obj_cfg.primary_key,
        primary_key_columns=[obj_cfg.primary_key],
        load_strategy=load_strategy,
        use_cursors=(not force_full and obj_cfg.api_type in ("INCR", "INCR_GET_ID")),
        page_size=_safe_int(getattr(cfg, "page_size", 100), 100),
        lookback_minutes=_safe_int(getattr(cfg, "lookback_minutes", 30), 30),
        destination_catalog=dest_cat,
        destination_schema=dest_sch,
        raw_json_volume_base=getattr(cfg, "raw_json_volume_base", ""),
        attachment_volume_path=getattr(cfg, "attachment_volume_path", ""),
        has_attachments=getattr(obj_cfg, "has_attachments", False),
        download_attachments=getattr(cfg, "download_attachments", True),
        dependency_object=getattr(obj_cfg, "dependency_object", None),
        parent_param_key=getattr(obj_cfg, "parent_param_key", None),
        from_datetime=getattr(cfg, "from_datetime", None),
        to_datetime=getattr(cfg, "to_datetime", None),
        retry=getattr(cfg, "retry_config", {}),
    )


def resolve_run_config(widget_params: Dict[str, Any], dbutils: Any = None) -> ConnectorConfig:
    default_config = os.path.join(_REPO_ROOT, "connectors", "wss", "config.yaml")
    default_catalog = os.path.join(_REPO_ROOT, "connectors", "wss", "api_catalog.yaml")

    config_path = str(widget_params.get("config_path") or default_config)
    catalog_path = str(widget_params.get("catalog_path") or default_catalog)

    if not os.path.isabs(catalog_path) or not os.path.exists(catalog_path):
        catalog_path = os.path.join(_REPO_ROOT, "connectors", "wss", "api_catalog.yaml")

    if not os.path.isabs(config_path) or not os.path.exists(config_path):
        config_path = os.path.join(_REPO_ROOT, "connectors", "wss", "config.yaml")

    cfg = load_config(config_path, dbutils=dbutils)
    cfg.routing = resolve_platform_routing(widget_params)

    # 1. Resolve secrets from dbutils
    from src.auth import WSSCredentials
    secret_scope = str(widget_params.get("secret_scope") or cfg.secret_scope)
    creds_dict: Dict[str, str] = {}

    if dbutils:
        for target_key, aliases in {
            "api_key": ["api_key", "wss_api_key", "wss-api-key"],
            "user_token": ["user_token", "wss_user_token", "wss-user-token"],
            "user_email": ["user_email", "wss_user_email", "wss-user-email"],
        }.items():
            for alias in aliases:
                try:
                    val = dbutils.secrets.get(scope=secret_scope, key=alias)
                    if val:
                        creds_dict[target_key] = val
                        break
                except Exception:
                    pass

    cfg.credentials = WSSCredentials(
        api_key=creds_dict.get("api_key") or str(widget_params.get("api_key", "")),
        user_token=creds_dict.get("user_token") or str(widget_params.get("user_token", "")),
        user_email=creds_dict.get("user_email") or str(widget_params.get("user_email", "")),
    )
    cfg.is_sandbox = bool(widget_params.get("is_sandbox", False) or cfg.routing.is_sandbox)

    # 2. Resolve catalog map from absolute path
    catalog_map = load_catalog(catalog_path)

    # 3. Read enabled_objects widget parameter
    target_objects_raw = (
        widget_params.get("enabled_objects")
        or widget_params.get("objects")
        or widget_params.get("object_name")
    )

    if isinstance(target_objects_raw, str) and target_objects_raw.strip():
        selected_names = [x.strip() for x in target_objects_raw.split(",") if x.strip()]
    elif isinstance(target_objects_raw, list):
        selected_names = [str(x).strip() for x in target_objects_raw if str(x).strip()]
    else:
        selected_names = list(catalog_map.keys())

    if len(selected_names) == 1 and selected_names[0].lower() in ("all", "*"):
        selected_names = list(catalog_map.keys())

    run_objects: List[ObjectRunConfig] = []
    for name in selected_names:
        if name in catalog_map:
            run_objects.append(build_object_run_config(catalog_map[name], cfg))

    cfg.objects = run_objects
    return cfg
