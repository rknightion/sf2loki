"""Tests for ApexLogClient — Tooling API ApexLog listing + body download."""

from __future__ import annotations

from datetime import UTC, datetime, timedelta

import httpx
import pytest
import respx

from sf2loki.auth.jwt_auth import AccessToken
from sf2loki.config import SalesforceConfig
from sf2loki.obs.metrics import Metrics
from sf2loki.salesforce.apexlog_client import (
    ApexLogClient,
    ApexLogError,
    ApexLogThrottledError,
)

INSTANCE = "https://x.my.salesforce.com"
TOOLING_QUERY = f"{INSTANCE}/services/data/v60.0/tooling/query"


class FakeTokenProvider:
    def __init__(self) -> None:
        self._invalidated = False

    async def token(self) -> AccessToken:
        val = "tok-refreshed" if self._invalidated else "tok"
        return AccessToken(
            value=val, instance_url=INSTANCE, expires_at=datetime.now(UTC) + timedelta(hours=1)
        )

    async def org_id(self) -> str:
        return "00Dxx"

    def invalidate(self) -> None:
        self._invalidated = True


def make_cfg() -> SalesforceConfig:
    return SalesforceConfig(
        client_id="cid", username="svc@example.com", private_key="K", api_version="60.0"
    )


def _log_record(log_id: str = "07L1", length: int = 100) -> dict:
    return {
        "Id": log_id,
        "LogUserId": "005u",
        "LogLength": length,
        "Operation": "/services/data/v60.0/tooling/executeAnonymous",
        "Request": "Api",
        "Status": "Success",
        "StartTime": "2026-07-02T08:33:23.000+0000",
        "Application": "Unknown",
        "DurationMilliseconds": 100,
        "Location": "Monitoring",
    }


@pytest.mark.asyncio
@respx.mock
async def test_list_logs_maps_fields_and_no_user_filter() -> None:
    route = respx.get(TOOLING_QUERY).mock(
        return_value=httpx.Response(200, json={"records": [_log_record()], "done": True})
    )
    async with httpx.AsyncClient() as client:
        c = ApexLogClient(make_cfg(), FakeTokenProvider(), client)
        logs = await c.list_logs(since="2026-07-02T00:00:00Z", users=[], page_size=200)

    assert len(logs) == 1
    m = logs[0]
    assert (m.id, m.log_user_id, m.log_length, m.status) == ("07L1", "005u", 100, "Success")
    assert m.start_time == "2026-07-02T08:33:23.000+0000"
    sent_q = route.calls[0].request.url.params["q"]
    assert "FROM ApexLog" in sent_q
    assert "LogUser.Username" not in sent_q  # empty users -> no filter


@pytest.mark.asyncio
@respx.mock
async def test_list_logs_adds_username_filter() -> None:
    route = respx.get(TOOLING_QUERY).mock(
        return_value=httpx.Response(200, json={"records": [], "done": True})
    )
    async with httpx.AsyncClient() as client:
        c = ApexLogClient(make_cfg(), FakeTokenProvider(), client)
        await c.list_logs(since="2026-07-02T00:00:00Z", users=["a@x.com", "b@x.com"], page_size=200)

    q = route.calls[0].request.url.params["q"]
    assert "LogUser.Username IN ('a@x.com','b@x.com')" in q


@pytest.mark.asyncio
@respx.mock
async def test_list_logs_throttle_raises_apexlog_throttled() -> None:
    respx.get(TOOLING_QUERY).mock(return_value=httpx.Response(403, text="REQUEST_LIMIT_EXCEEDED"))
    async with httpx.AsyncClient() as client:
        c = ApexLogClient(make_cfg(), FakeTokenProvider(), client)
        with pytest.raises(ApexLogThrottledError):
            await c.list_logs(since="2026-07-02T00:00:00Z", users=[], page_size=200)


@pytest.mark.asyncio
@respx.mock
async def test_download_body_returns_text_and_counts_bytes() -> None:
    body = "60.0 APEX_CODE,FINEST\nUSER_DEBUG|hello"
    respx.get(f"{INSTANCE}/services/data/v60.0/tooling/sobjects/ApexLog/07L1/Body").mock(
        return_value=httpx.Response(200, text=body, headers={"content-type": "text/plain"})
    )
    metrics = Metrics()
    async with httpx.AsyncClient() as client:
        c = ApexLogClient(make_cfg(), FakeTokenProvider(), client, metrics=metrics)
        got = await c.download_body("07L1")
    assert got == body


@pytest.mark.asyncio
@respx.mock
async def test_download_body_401_retries_once() -> None:
    url = f"{INSTANCE}/services/data/v60.0/tooling/sobjects/ApexLog/07L1/Body"
    respx.get(url).mock(side_effect=[httpx.Response(401), httpx.Response(200, text="body-ok")])
    async with httpx.AsyncClient() as client:
        c = ApexLogClient(make_cfg(), FakeTokenProvider(), client)
        assert await c.download_body("07L1") == "body-ok"


@pytest.mark.asyncio
@respx.mock
async def test_download_body_error_raises() -> None:
    respx.get(f"{INSTANCE}/services/data/v60.0/tooling/sobjects/ApexLog/07L1/Body").mock(
        return_value=httpx.Response(404, text="not found")
    )
    async with httpx.AsyncClient() as client:
        c = ApexLogClient(make_cfg(), FakeTokenProvider(), client)
        with pytest.raises(ApexLogError):
            await c.download_body("07L1")


@pytest.mark.asyncio
@respx.mock
async def test_count_active_traceflags() -> None:
    respx.get(TOOLING_QUERY).mock(
        return_value=httpx.Response(
            200, json={"records": [{"Id": "7tf1"}, {"Id": "7tf2"}], "done": True}
        )
    )
    async with httpx.AsyncClient() as client:
        c = ApexLogClient(make_cfg(), FakeTokenProvider(), client)
        assert await c.count_active_traceflags() == 2
