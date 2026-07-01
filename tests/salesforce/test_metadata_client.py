"""Tests for MetadataClient — describeGlobal-based RTEM stream discovery."""

from __future__ import annotations

from datetime import UTC, datetime, timedelta

import httpx
import pytest
import respx

from sf2loki.auth.jwt_auth import AccessToken
from sf2loki.config import SalesforceConfig
from sf2loki.salesforce.metadata_client import MetadataClient


class FakeTokenProvider:
    def __init__(self) -> None:
        self.invalidated = False

    async def token(self) -> AccessToken:
        return AccessToken(
            value="tok",
            instance_url="https://x.my.salesforce.com",
            expires_at=datetime.now(UTC) + timedelta(hours=1),
        )

    async def org_id(self) -> str:
        return "00Dxx"

    def invalidate(self) -> None:
        self.invalidated = True


def _sf_cfg() -> SalesforceConfig:
    return SalesforceConfig(
        client_id="cid", username="svc@example.com", private_key="K", api_version="60.0"
    )


@pytest.mark.asyncio
@respx.mock
async def test_list_event_stream_topics_filters_and_formats() -> None:
    respx.get("https://x.my.salesforce.com/services/data/v60.0/sobjects/").mock(
        return_value=httpx.Response(
            200,
            json={
                "sobjects": [
                    {"name": "LoginEventStream"},
                    {"name": "ApiEventStream"},
                    {"name": "Account"},  # not a stream
                    {"name": "LoginEvent"},  # stored object, not a stream
                    {"name": "MyCustom__e"},  # custom platform event, not RTEM
                ]
            },
        )
    )
    client = MetadataClient(_sf_cfg(), FakeTokenProvider(), httpx.AsyncClient())
    topics = await client.list_event_stream_topics()
    assert topics == ["/event/ApiEventStream", "/event/LoginEventStream"]
