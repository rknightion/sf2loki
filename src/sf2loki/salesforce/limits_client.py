"""Salesforce org-limits REST client.

Fetches ``/services/data/vXX.0/limits`` — a cheap, single GET returning the
org's resource limits (API requests, storage, streaming events, async Apex, ...)
as ``{"<LimitName>": {"Max": N, "Remaining": M}, ...}``. Feeds the limits gauges.
"""

from __future__ import annotations

import httpx

from sf2loki.auth.jwt_auth import TokenProvider
from sf2loki.config import SalesforceConfig


class LimitsError(Exception):
    """Raised when the Salesforce limits endpoint returns a non-2xx (non-401) response."""


class LimitsClient:
    """Thin async client for the Salesforce limits endpoint (one 401-retry)."""

    def __init__(
        self,
        cfg: SalesforceConfig,
        tokens: TokenProvider,
        client: httpx.AsyncClient,
    ) -> None:
        self._cfg = cfg
        self._tokens = tokens
        self._client = client

    async def fetch(self) -> dict[str, dict[str, int]]:
        """Return ``{limit_name: {"Max": int, "Remaining": int}}`` for every limit.

        Entries lacking top-level ``Max``/``Remaining`` (or otherwise malformed)
        are skipped rather than raising.
        """
        tok = await self._tokens.token()
        url = f"{tok.instance_url}/services/data/v{self._cfg.api_version}/limits"
        headers = {"Authorization": f"Bearer {tok.value}"}
        response = await self._client.get(url, headers=headers)

        if response.status_code == 401:
            self._tokens.invalidate()
            tok = await self._tokens.token()
            headers = {"Authorization": f"Bearer {tok.value}"}
            response = await self._client.get(url, headers=headers)

        if not response.is_success:
            raise LimitsError(f"limits query failed: HTTP {response.status_code} — {response.text}")

        result: dict[str, dict[str, int]] = {}
        for name, info in response.json().items():
            if isinstance(info, dict) and "Max" in info and "Remaining" in info:
                try:
                    result[name] = {"Max": int(info["Max"]), "Remaining": int(info["Remaining"])}
                except TypeError, ValueError:
                    continue
        return result
