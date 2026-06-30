"""Tests for EventLogObjectsSource — SOQL-based polling source."""

from __future__ import annotations

import asyncio
from datetime import UTC, datetime, timedelta
from urllib.parse import parse_qs, urlparse

import httpx
import pytest
import respx

from sf2loki.auth.jwt_auth import AccessToken
from sf2loki.config import EventLogObjectConfig, EventLogObjectsConfig, SalesforceConfig
from sf2loki.sources.eventlog_objects_source import EventLogObjectsSource
from sf2loki.state.file_store import FileCheckpointStore

# ---------------------------------------------------------------------------
# Shared fakes


class FakeTokenProvider:
    async def token(self) -> AccessToken:
        return AccessToken(
            value="tok",
            instance_url="https://x.my.salesforce.com",
            expires_at=datetime.now(UTC) + timedelta(hours=1),
        )

    async def org_id(self) -> str:
        return "00Dxx"

    def invalidate(self) -> None:
        pass


def make_sf_cfg(api_version: str = "60.0") -> SalesforceConfig:
    return SalesforceConfig(
        client_id="cid",
        username="svc@example.com",
        private_key="DUMMYKEY",
        api_version=api_version,
    )


def make_elo_cfg(
    name: str = "LoginEvent",
    lookback: timedelta = timedelta(hours=1),
    poll_interval: timedelta = timedelta(seconds=0),
) -> EventLogObjectsConfig:
    return EventLogObjectsConfig(
        enabled=True,
        objects=[
            EventLogObjectConfig(
                name=name,
                timestamp_field="EventDate",
                poll_interval=poll_interval,
                lookback=lookback,
            )
        ],
    )


def _query_url(instance: str = "https://x.my.salesforce.com", version: str = "60.0") -> str:
    return f"{instance}/services/data/v{version}/query"


# ---------------------------------------------------------------------------
# Tests


@pytest.mark.asyncio
@respx.mock
async def test_no_stored_watermark_yields_log_entries(tmp_path: pytest.TempPathFactory) -> None:
    """With no watermark, events() queries using now-lookback and yields LogEntry per record."""
    store = FileCheckpointStore(tmp_path / "state.json")  # type: ignore[arg-type]

    event_date = "2026-06-30T10:00:00Z"
    respx.get(_query_url()).mock(
        return_value=httpx.Response(
            200,
            json={
                "records": [
                    {"EventDate": event_date, "UserId": "005xx", "EventType": "Login"},
                    {"EventDate": "2026-06-30T10:01:00Z", "UserId": "005yy", "EventType": "Login"},
                ],
                "done": True,
            },
        )
    )

    cfg = make_elo_cfg()
    sf_cfg = make_sf_cfg()
    tokens = FakeTokenProvider()

    async with httpx.AsyncClient() as client:
        from sf2loki.salesforce.soql_client import SoqlClient

        soql = SoqlClient(sf_cfg, tokens, client)
        source = EventLogObjectsSource(cfg, soql, sm_fields=[], poll_once=True)
        stop = asyncio.Event()
        entries = [e async for e in source.events(store, stop)]

    assert len(entries) == 2
    for entry in entries:
        assert entry.labels["source"] == "eventlog_objects"
        assert entry.labels["event_type"] == "LoginEvent"
        assert entry.checkpoint.key == "eventlog_objects:LoginEvent"

    # Ascending timestamp order
    assert entries[0].timestamp <= entries[1].timestamp

    # Checkpoint values are the EventDate strings
    assert entries[0].checkpoint.value == "2026-06-30T10:00:00Z"
    assert entries[1].checkpoint.value == "2026-06-30T10:01:00Z"


@pytest.mark.asyncio
@respx.mock
async def test_entry_timestamp_uses_configured_timestamp_field(
    tmp_path: pytest.TempPathFactory,
) -> None:
    """The LogEntry timestamp comes from the configured timestamp_field, not ingest time.

    Regression: a record whose time column is neither EventDate nor CreatedDate
    (e.g. LoginHistory.LoginTime) must still get its event time as the entry
    timestamp, not now().
    """
    store = FileCheckpointStore(tmp_path / "state.json")  # type: ignore[arg-type]

    login_time = "2026-06-30T10:00:00Z"
    respx.get(_query_url()).mock(
        return_value=httpx.Response(
            200,
            json={"records": [{"LoginTime": login_time, "UserId": "005xx"}], "done": True},
        )
    )

    cfg = make_elo_cfg(name="LoginHistory")
    cfg.objects[0].timestamp_field = "LoginTime"
    sf_cfg = make_sf_cfg()
    tokens = FakeTokenProvider()

    async with httpx.AsyncClient() as client:
        from sf2loki.salesforce.soql_client import SoqlClient

        soql = SoqlClient(sf_cfg, tokens, client)
        source = EventLogObjectsSource(cfg, soql, sm_fields=[], poll_once=True)
        stop = asyncio.Event()
        entries = [e async for e in source.events(store, stop)]

    assert len(entries) == 1
    assert entries[0].timestamp == datetime(2026, 6, 30, 10, 0, 0, tzinfo=UTC)


@pytest.mark.asyncio
@respx.mock
async def test_sm_fields_promoted_to_structured_metadata(tmp_path: pytest.TempPathFactory) -> None:
    """UserId in sm_fields appears in structured_metadata, not in labels."""
    store = FileCheckpointStore(tmp_path / "state.json")  # type: ignore[arg-type]

    respx.get(_query_url()).mock(
        return_value=httpx.Response(
            200,
            json={
                "records": [{"EventDate": "2026-06-30T10:00:00Z", "UserId": "005xx"}],
                "done": True,
            },
        )
    )

    cfg = make_elo_cfg()
    sf_cfg = make_sf_cfg()
    tokens = FakeTokenProvider()

    async with httpx.AsyncClient() as client:
        from sf2loki.salesforce.soql_client import SoqlClient

        soql = SoqlClient(sf_cfg, tokens, client)
        source = EventLogObjectsSource(cfg, soql, sm_fields=["UserId"], poll_once=True)
        stop = asyncio.Event()
        entries = [e async for e in source.events(store, stop)]

    assert len(entries) == 1
    assert entries[0].structured_metadata.get("UserId") == "005xx"
    assert "UserId" not in entries[0].labels


@pytest.mark.asyncio
@respx.mock
async def test_watermark_resume_uses_stored_watermark(tmp_path: pytest.TempPathFactory) -> None:
    """When a watermark is stored, the SOQL WHERE clause uses that watermark."""
    store = FileCheckpointStore(tmp_path / "state.json")  # type: ignore[arg-type]
    # Salesforce REST serializes EventDate with a no-colon offset; the stored
    # watermark is that raw value, which must be normalized to a SOQL-legal
    # literal before being interpolated into the WHERE clause.
    stored_wm = "2026-06-30T09:00:00.000+0000"
    await store.commit("eventlog_objects:LoginEvent", stored_wm)

    captured_q: list[str] = []

    def capture(request: httpx.Request) -> httpx.Response:
        qs = parse_qs(urlparse(str(request.url)).query)
        if "q" in qs:
            captured_q.append(qs["q"][0])
        return httpx.Response(200, json={"records": [], "done": True})

    respx.get(_query_url()).mock(side_effect=capture)

    cfg = make_elo_cfg()
    sf_cfg = make_sf_cfg()
    tokens = FakeTokenProvider()

    async with httpx.AsyncClient() as client:
        from sf2loki.salesforce.soql_client import SoqlClient

        soql = SoqlClient(sf_cfg, tokens, client)
        source = EventLogObjectsSource(cfg, soql, sm_fields=[], poll_once=True)
        stop = asyncio.Event()
        _ = [e async for e in source.events(store, stop)]

    assert len(captured_q) == 1
    # The raw +0000 watermark is normalized to a SOQL-legal Z literal in the query.
    assert "2026-06-30T09:00:00.000Z" in captured_q[0]
    assert "2026-06-30T09:00:00.000+0000" not in captured_q[0]
    # Verify it's in the WHERE clause
    assert "WHERE EventDate >" in captured_q[0]


@pytest.mark.asyncio
@respx.mock
async def test_stop_event_prevents_yield(tmp_path: pytest.TempPathFactory) -> None:
    """When stop is set before iteration, events() returns without yielding."""
    store = FileCheckpointStore(tmp_path / "state.json")  # type: ignore[arg-type]

    # Mock in case a request sneaks through — but we don't expect any
    respx.get(_query_url()).mock(
        return_value=httpx.Response(200, json={"records": [], "done": True})
    )

    cfg = make_elo_cfg()
    sf_cfg = make_sf_cfg()
    tokens = FakeTokenProvider()

    async with httpx.AsyncClient() as client:
        from sf2loki.salesforce.soql_client import SoqlClient

        soql = SoqlClient(sf_cfg, tokens, client)
        source = EventLogObjectsSource(cfg, soql, sm_fields=[], poll_once=True)
        stop = asyncio.Event()
        stop.set()  # Signal stop BEFORE starting
        entries = [e async for e in source.events(store, stop)]

    assert entries == []


@pytest.mark.asyncio
@respx.mock
async def test_stop_during_inter_cycle_sleep_returns_promptly(
    tmp_path: pytest.TempPathFactory,
) -> None:
    """Setting stop during the between-cycles sleep returns promptly, not after poll_interval."""
    store = FileCheckpointStore(tmp_path / "state.json")  # type: ignore[arg-type]
    respx.get(_query_url()).mock(
        return_value=httpx.Response(200, json={"records": [], "done": True})
    )

    # poll_once=False with a long poll_interval: without stop-awareness in the
    # inter-cycle sleep, events() would block for the full interval.
    cfg = make_elo_cfg(poll_interval=timedelta(seconds=60))
    sf_cfg = make_sf_cfg()
    tokens = FakeTokenProvider()

    async with httpx.AsyncClient() as client:
        from sf2loki.salesforce.soql_client import SoqlClient

        soql = SoqlClient(sf_cfg, tokens, client)
        source = EventLogObjectsSource(cfg, soql, sm_fields=[], poll_once=False)
        stop = asyncio.Event()

        async def consume() -> None:
            async for _ in source.events(store, stop):
                pass

        task = asyncio.create_task(consume())
        await asyncio.sleep(0.05)  # let the first (empty) cycle complete and enter the sleep
        stop.set()
        await asyncio.wait_for(task, timeout=1.0)  # << would be 60s without the fix
