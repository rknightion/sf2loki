"""Tests for SoqlClient — SOQL REST API query execution."""

from __future__ import annotations

from datetime import UTC, datetime, timedelta

import httpx
import pytest
import respx

from sf2loki.auth.jwt_auth import AccessToken
from sf2loki.config import SalesforceConfig
from sf2loki.salesforce.soql_client import SoqlClient, SoqlError

# ---------------------------------------------------------------------------
# Fake TokenProvider (no real JWT machinery needed in tests)


class FakeTokenProvider:
    """Minimal token provider for unit tests."""

    def __init__(
        self, token_value: str = "tok", instance_url: str = "https://x.my.salesforce.com"
    ) -> None:
        self._token_value = token_value
        self._invalidated = False
        self._token_call_count = 0

    async def token(self) -> AccessToken:
        self._token_call_count += 1
        # Return different token after invalidation to let tests verify retry
        value = f"{self._token_value}-refreshed" if self._invalidated else self._token_value
        return AccessToken(
            value=value,
            instance_url="https://x.my.salesforce.com",
            expires_at=datetime.now(UTC) + timedelta(hours=1),
        )

    async def org_id(self) -> str:
        return "00Dxx"

    def invalidate(self) -> None:
        self._invalidated = True


def make_cfg(api_version: str = "60.0") -> SalesforceConfig:
    return SalesforceConfig(
        client_id="cid",
        username="svc@example.com",
        private_key="DUMMYKEY",
        api_version=api_version,
    )


# ---------------------------------------------------------------------------
# Tests


@pytest.mark.asyncio
@respx.mock
async def test_query_paginates_and_yields_all_records() -> None:
    """Yields records across a paginated response (done=false -> nextRecordsUrl)."""
    instance_url = "https://x.my.salesforce.com"
    first_url = f"{instance_url}/services/data/v60.0/query"
    next_path = "/services/data/v60.0/query/01gxx000000001"
    next_url = f"{instance_url}{next_path}"

    respx.get(first_url).mock(
        return_value=httpx.Response(
            200,
            json={
                "records": [{"Id": "a1"}, {"Id": "a2"}],
                "done": False,
                "nextRecordsUrl": next_path,
            },
        )
    )
    respx.get(next_url).mock(
        return_value=httpx.Response(
            200,
            json={
                "records": [{"Id": "a3"}],
                "done": True,
            },
        )
    )

    tokens = FakeTokenProvider()
    async with httpx.AsyncClient() as client:
        soql = SoqlClient(make_cfg(), tokens, client)
        records = [r async for r in soql.query("SELECT Id FROM Account")]

    assert [r["Id"] for r in records] == ["a1", "a2", "a3"]
    assert respx.calls.call_count == 2


@pytest.mark.asyncio
@respx.mock
async def test_401_invalidates_and_retries_with_fresh_token() -> None:
    """On a 401, invalidate() is called and the request retried once with a fresh token."""
    instance_url = "https://x.my.salesforce.com"
    query_url = f"{instance_url}/services/data/v60.0/query"

    call_count = 0

    def side_effect(request: httpx.Request) -> httpx.Response:
        nonlocal call_count
        call_count += 1
        auth = request.headers.get("Authorization", "")
        if "refreshed" in auth:
            # Second call with refreshed token — succeed
            return httpx.Response(200, json={"records": [{"Id": "ok"}], "done": True})
        # First call — 401
        return httpx.Response(401, text="Unauthorized")

    respx.get(query_url).mock(side_effect=side_effect)

    tokens = FakeTokenProvider()
    async with httpx.AsyncClient() as client:
        soql = SoqlClient(make_cfg(), tokens, client)
        records = [r async for r in soql.query("SELECT Id FROM Account")]

    assert [r["Id"] for r in records] == ["ok"]
    assert tokens._invalidated is True
    assert call_count == 2


@pytest.mark.asyncio
@respx.mock
async def test_500_raises_soql_error() -> None:
    """A 500 response raises SoqlError with status and body info."""
    instance_url = "https://x.my.salesforce.com"
    query_url = f"{instance_url}/services/data/v60.0/query"

    respx.get(query_url).mock(return_value=httpx.Response(500, text="Internal Server Error"))

    tokens = FakeTokenProvider()
    async with httpx.AsyncClient() as client:
        soql = SoqlClient(make_cfg(), tokens, client)
        with pytest.raises(SoqlError) as exc_info:
            async for _ in soql.query("SELECT Id FROM Account"):
                pass

    assert "500" in str(exc_info.value)
