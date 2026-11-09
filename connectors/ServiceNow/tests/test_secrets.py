"""Secret-scope contract. Values are never stored in this package."""
from __future__ import annotations

from src.secrets import DEFAULT_SECRET_SCOPE, REQUIRED_KEYS, ServiceNowCredentials


def test_scope_and_key_names():
    assert DEFAULT_SECRET_SCOPE == "jetstream_servicenow"
    assert REQUIRED_KEYS == (
        "servicenow-client-id",
        "servicenow-client-secret",
        "servicenow-refresh-token",
        "servicenow-base-url",
    )


def test_token_url_uses_instance_origin():
    creds = ServiceNowCredentials(
        client_id="client",
        client_secret="secret",
        refresh_token="refresh",
        instance_url="https://example.service-now.com/",
    )
    assert creds.token_url == "https://example.service-now.com/oauth_token.do"
    masked = creds.masked()
    assert masked["client_secret"] != "secret"
    assert "secret" not in masked["client_secret"]
