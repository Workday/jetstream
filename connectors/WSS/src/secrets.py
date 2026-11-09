"""Read WSS credentials from Databricks secret scope."""
from __future__ import annotations

from dataclasses import dataclass
from typing import Any

KEY_API_KEY = "wss_api_key"
KEY_USER_TOKEN = "wss_user_token"
KEY_USER_EMAIL = "wss_user_email"

REQUIRED_KEYS = (KEY_API_KEY, KEY_USER_TOKEN, KEY_USER_EMAIL)
DEFAULT_SECRET_SCOPE = "jetstream_wss"

@dataclass(frozen=True)
class WSSCredentials:
    api_key: str
    user_token: str
    user_email: str

def _get_dbutils(dbutils: Any = None) -> Any:
    if dbutils is not None:
        return dbutils
    try:
        from pyspark.dbutils import DBUtils
        from pyspark.sql import SparkSession
        return DBUtils(SparkSession.builder.getOrCreate())
    except Exception as exc:
        raise RuntimeError("Could not resolve dbutils.") from exc

def load_wss_credentials(scope: str = DEFAULT_SECRET_SCOPE, dbutils: Any = None) -> WSSCredentials:
    du = _get_dbutils(dbutils)
    values: dict[str, str | None] = {}
    missing: list[str] = []

    key_aliases = {
        KEY_API_KEY: ["wss_api_key", "wss-api-key"],
        KEY_USER_TOKEN: ["wss_user_token", "wss-user-token"],
        KEY_USER_EMAIL: ["wss_user_email", "wss-user-email"],
    }

    for target_key, aliases in key_aliases.items():
        val = None
        for alias in aliases:
            try:
                val = du.secrets.get(scope=scope, key=alias)
                if val:
                    break
            except Exception:
                pass
        if not val:
            missing.append(target_key)
        values[target_key] = val

    if missing:
        raise KeyError(f"Missing required secret key(s) {missing} in scope '{scope}'.")

    return WSSCredentials(
        api_key=values[KEY_API_KEY],
        user_token=values[KEY_USER_TOKEN],
        user_email=values[KEY_USER_EMAIL],
    )
