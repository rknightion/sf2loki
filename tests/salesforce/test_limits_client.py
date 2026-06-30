"""Tests for the Salesforce org-limits REST client (salesforce/limits_client.py)."""

from __future__ import annotations

from datetime import UTC, datetime, timedelta

import httpx
import pytest
import respx
from pydantic import SecretStr

from sf2loki.auth.jwt_auth import AccessToken
from sf2loki.config import SalesforceConfig
from sf2loki.salesforce.limits_client import LimitsClient, LimitsError

INSTANCE_URL = "https://x.my.salesforce.com"
LIMITS_URL = f"{INSTANCE_URL}/services/data/v60.0/limits"


def make_cfg() -> SalesforceConfig:
    return SalesforceConfig(
        client_id="cid",
        username="svc@example.com",
        private_key=SecretStr("DUMMYKEY"),
    )


class FakeTokenProvider:
    """Minimal token provider — no real JWT machinery needed in tests."""

    def __init__(self) -> None:
        self.invalidated = False

    async def token(self) -> AccessToken:
        value = "tok-refreshed" if self.invalidated else "tok"
        return AccessToken(
            value=value,
            instance_url=INSTANCE_URL,
            expires_at=datetime.now(UTC) + timedelta(hours=1),
        )

    def invalidate(self) -> None:
        self.invalidated = True


_SAMPLE = {
    "DailyApiRequests": {"Max": 15000, "Remaining": 14998},
    "DataStorageMB": {"Max": 1024, "Remaining": 1000},
    # An entry with nested per-app sublimits still exposes top-level Max/Remaining.
    "DailyAsyncApexExecutions": {"Max": 250000, "Remaining": 250000, "SomeApp": {"Max": 1}},
    # Junk entry without Max/Remaining is skipped.
    "Weird": {"Foo": 1},
}


@pytest.mark.asyncio
@respx.mock
async def test_fetch_returns_parsed_limits() -> None:
    respx.get(LIMITS_URL).mock(return_value=httpx.Response(200, json=_SAMPLE))

    async with httpx.AsyncClient() as http:
        client = LimitsClient(make_cfg(), FakeTokenProvider(), http)  # type: ignore[arg-type]
        limits = await client.fetch()

    assert limits["DailyApiRequests"] == {"Max": 15000, "Remaining": 14998}
    assert limits["DataStorageMB"] == {"Max": 1024, "Remaining": 1000}
    assert limits["DailyAsyncApexExecutions"] == {"Max": 250000, "Remaining": 250000}
    assert "Weird" not in limits  # no Max/Remaining → skipped


@pytest.mark.asyncio
@respx.mock
async def test_fetch_retries_once_on_401() -> None:
    route = respx.get(LIMITS_URL).mock(
        side_effect=[
            httpx.Response(401, json={"error": "expired"}),
            httpx.Response(200, json=_SAMPLE),
        ]
    )
    tokens = FakeTokenProvider()

    async with httpx.AsyncClient() as http:
        client = LimitsClient(make_cfg(), tokens, http)  # type: ignore[arg-type]
        limits = await client.fetch()

    assert route.call_count == 2
    assert tokens.invalidated is True
    assert "DailyApiRequests" in limits


@pytest.mark.asyncio
@respx.mock
async def test_fetch_raises_on_non_2xx() -> None:
    respx.get(LIMITS_URL).mock(return_value=httpx.Response(500, text="boom"))

    async with httpx.AsyncClient() as http:
        client = LimitsClient(make_cfg(), FakeTokenProvider(), http)  # type: ignore[arg-type]
        with pytest.raises(LimitsError):
            await client.fetch()
