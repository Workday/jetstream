"""Read ServiceNow OAuth credentials from a Databricks secret scope.

The scope name is a job parameter (`secret_scope`). Example: jetstream_servicenow.

Expected secret keys
--------------------
    servicenow-client-id       OAuth application client ID
    servicenow-client-secret   OAuth application client secret
    servicenow-refresh-token   Long-lived refresh token
    servicenow-base-url        Instance base URL, for example
                               https://example.service-now.com
"""

from __future__ import annotations

from dataclasses import dataclass

KEY_CLIENT_ID = "servicenow-client-id"
KEY_CLIENT_SECRET = "servicenow-client-secret"
KEY_REFRESH_TOKEN = "servicenow-refresh-token"
KEY_BASE_URL = "servicenow-base-url"

REQUIRED_KEYS = (KEY_CLIENT_ID, KEY_CLIENT_SECRET, KEY_REFRESH_TOKEN, KEY_BASE_URL)
DEFAULT_SECRET_SCOPE = "jetstream_servicenow"


@dataclass(frozen=True)
class ServiceNowCredentials:
    client_id: str
    client_secret: str
    refresh_token: str
    instance_url: str

    @property
    def token_url(self) -> str:
        return f"{self.instance_url.rstrip('/')}/oauth_token.do"

    def masked(self) -> dict:
        return {
            "instance_url": self.instance_url,
            "client_id": _mask(self.client_id),
            "client_secret": _mask(self.client_secret),
            "refresh_token": _mask(self.refresh_token),
        }


def _mask(value: str, keep: int = 4) -> str:
    if not value:
        return ""
    if len(value) <= keep:
        return "*" * len(value)
    return value[:keep] + "*" * (len(value) - keep)


def _get_dbutils(dbutils=None):
    if dbutils is not None:
        return dbutils
    try:
        return dbutils  # type: ignore[name-defined]  # noqa: F821
    except NameError:
        pass
    try:
        from pyspark.dbutils import DBUtils  # type: ignore
        from pyspark.sql import SparkSession

        return DBUtils(SparkSession.builder.getOrCreate())
    except Exception as exc:  # pragma: no cover
        raise RuntimeError(
            "Could not resolve `dbutils`. Pass it explicitly: "
            "load_servicenow_credentials(scope, dbutils=dbutils)"
        ) from exc


def load_servicenow_credentials(
    scope: str = DEFAULT_SECRET_SCOPE,
    dbutils=None,
) -> ServiceNowCredentials:
    """Read and validate ServiceNow OAuth credentials from a secret scope."""
    du = _get_dbutils(dbutils)

    values: dict[str, str | None] = {}
    missing: list[str] = []
    for key in REQUIRED_KEYS:
        try:
            val = du.secrets.get(scope=scope, key=key)
        except Exception:  # noqa: BLE001
            val = None
        if not val:
            missing.append(key)
        values[key] = val

    if missing:
        raise KeyError(
            f"Missing required secret key(s) {missing} in scope '{scope}'. "
            f"Expected keys: {list(REQUIRED_KEYS)}"
        )

    return ServiceNowCredentials(
        client_id=values[KEY_CLIENT_ID],  # type: ignore[arg-type]
        client_secret=values[KEY_CLIENT_SECRET],  # type: ignore[arg-type]
        refresh_token=values[KEY_REFRESH_TOKEN],  # type: ignore[arg-type]
        instance_url=values[KEY_BASE_URL].rstrip("/"),  # type: ignore[union-attr]
    )
