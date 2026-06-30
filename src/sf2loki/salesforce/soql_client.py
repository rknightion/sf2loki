"""SOQL REST API client with pagination and 401-retry logic.

Ref: DESIGN.md §7.
"""

from __future__ import annotations

from collections.abc import AsyncIterator

import httpx

from sf2loki.auth.jwt_auth import TokenProvider
from sf2loki.config import SalesforceConfig


class SoqlError(Exception):
    """Raised when the Salesforce SOQL query endpoint returns a non-2xx (non-401) response."""


class SoqlClient:
    """Thin async wrapper around the Salesforce REST query endpoint.

    Handles:
    - Authorization header injection via :class:`~sf2loki.auth.jwt_auth.TokenProvider`.
    - Automatic pagination via ``nextRecordsUrl``.
    - One transparent retry on 401 (token invalidation + re-mint).
    - :class:`SoqlError` on any other non-2xx response.
    """

    def __init__(
        self,
        cfg: SalesforceConfig,
        tokens: TokenProvider,
        client: httpx.AsyncClient,
    ) -> None:
        self._cfg = cfg
        self._tokens = tokens
        self._client = client

    async def query(self, soql: str) -> AsyncIterator[dict[str, object]]:
        """Execute *soql* and yield each record, following pagination.

        Yields records as-is (Salesforce ``attributes`` key included if present).
        Raises :class:`SoqlError` on non-2xx responses other than 401 (which
        triggers a single retry with a fresh token).
        """
        tok = await self._tokens.token()
        base_url = tok.instance_url
        url: str | None = f"{base_url}/services/data/v{self._cfg.api_version}/query"
        params: dict[str, str] | None = {"q": soql}

        while url is not None:
            headers = {"Authorization": f"Bearer {tok.value}"}
            response = await self._client.get(url, params=params, headers=headers)

            if response.status_code == 401:
                # Invalidate and retry exactly once with a fresh token.
                self._tokens.invalidate()
                tok = await self._tokens.token()
                headers = {"Authorization": f"Bearer {tok.value}"}
                response = await self._client.get(url, params=params, headers=headers)

            if not response.is_success:
                raise SoqlError(f"SOQL query failed: HTTP {response.status_code} — {response.text}")

            body = response.json()
            for record in body.get("records", []):
                yield record

            # Pagination: nextRecordsUrl is an absolute path (e.g. /services/data/...)
            if body.get("done") is False and body.get("nextRecordsUrl"):
                url = f"{base_url}{body['nextRecordsUrl']}"
                params = None  # Query string is embedded in the URL for page 2+
            else:
                url = None
