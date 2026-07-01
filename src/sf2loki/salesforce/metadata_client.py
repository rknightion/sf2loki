"""Salesforce metadata client: lightweight describeGlobal-based discovery.

Currently used to enumerate the org's Real-Time Event Monitoring streaming
channels (the ``*EventStream`` platform events) so the Pub/Sub source can
subscribe to all of them via the ``"*"`` topic wildcard.
"""

from __future__ import annotations

import httpx

from sf2loki.auth.jwt_auth import TokenProvider
from sf2loki.config import SalesforceConfig


class MetadataClient:
    """Reads global sObject metadata via the REST describeGlobal endpoint."""

    def __init__(
        self,
        sf_cfg: SalesforceConfig,
        tokens: TokenProvider,
        client: httpx.AsyncClient,
    ) -> None:
        self._cfg = sf_cfg
        self._tokens = tokens
        self._client = client

    async def list_event_stream_topics(self) -> list[str]:
        """Discover the org's RTEM streaming channels as Pub/Sub topic names.

        Returns ``/event/<Name>`` for every sObject whose name ends in
        ``EventStream`` (the Event Monitoring streaming convention, e.g.
        ``LoginEventStream`` -> ``/event/LoginEventStream``), sorted.
        """
        tok = await self._tokens.token()
        url = f"{tok.instance_url}/services/data/v{self._cfg.api_version}/sobjects/"
        headers = {"Authorization": f"Bearer {tok.value}"}
        response = await self._client.get(url, headers=headers)
        if response.status_code == 401:
            self._tokens.invalidate()
            tok = await self._tokens.token()
            response = await self._client.get(url, headers={"Authorization": f"Bearer {tok.value}"})
        response.raise_for_status()
        names = [
            str(s["name"])
            for s in response.json().get("sobjects", [])
            if str(s.get("name", "")).endswith("EventStream")
        ]
        return sorted(f"/event/{name}" for name in names)
