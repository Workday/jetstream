"""Juniper Mist API token provider.

Mist uses a simple static API token — no OAuth2 flow, no token refresh,
no expiry to manage. The token is stamped as `Authorization: Token <value>`
on every request.

`MistTokenProvider.__call__()` returns the token string for injection into
`http_client.HttpClient`. `granted_scopes()` always returns an empty set
because Mist does not use OAuth scopes; the mandatory-scope gate in
`pipeline_orchestrator` is bypassed automatically.
"""
from __future__ import annotations

from typing import Set


class MistAuthError(RuntimeError):
    """Raised when the API token is missing or rejected."""


class MistTokenProvider:
    """Token provider for Juniper Mist API.

    The token is resolved once (at connector_config resolution time) and
    held for the lifetime of the run. No background refresh is required.
    """

    def __init__(self, api_token: str) -> None:
        if not api_token or api_token.startswith("<"):
            raise MistAuthError(
                "juniper_mist_token is missing or unresolved. "
                "Verify that secret_scope is set and the scope contains "
                "the key 'juniper_mist_token'."
            )
        self._token = api_token

    def __call__(self) -> str:
        """Return the bearer token value (without the 'Token ' prefix)."""
        return self._token

    def granted_scopes(self) -> Set[str]:
        """Mist uses API token auth, not OAuth scopes. Always returns empty set."""
        return set()

    def auth_header(self) -> str:
        """Return the full Authorization header value."""
        return f"Token {self._token}"
