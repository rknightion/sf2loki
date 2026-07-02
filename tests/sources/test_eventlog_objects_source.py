"""Tests for EventLogObjectsSource — SOQL-based polling source."""

from __future__ import annotations

import asyncio
import json
import re
from datetime import UTC, datetime, timedelta
from urllib.parse import parse_qs, urlparse

import httpx
import pytest
import respx

from sf2loki.auth.jwt_auth import AccessToken
from sf2loki.config import EventLogObjectConfig, EventLogObjectsConfig, SalesforceConfig
from sf2loki.obs.metrics import Metrics
from sf2loki.salesforce.soql_client import to_soql_datetime_literal
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

    # Checkpoint values are JSON: the record's EventDate as watermark + id window.
    assert json.loads(entries[0].checkpoint.value)["last_ts"] == "2026-06-30T10:00:00Z"
    assert json.loads(entries[1].checkpoint.value)["last_ts"] == "2026-06-30T10:01:00Z"


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
    # Verify it's in the WHERE clause; >= (not >) so a timestamp tie at a page
    # boundary can't be skipped (the id window dedups the boundary records).
    assert "WHERE EventDate >=" in captured_q[0]


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


# ---------------------------------------------------------------------------
# Checkpoint poisoning (C3): null/garbage timestamps must never produce a
# broken watermark (empty WHERE literal -> MALFORMED_QUERY crash-loop).


@pytest.mark.asyncio
@respx.mock
async def test_null_timestamp_record_carries_previous_watermark(
    tmp_path: pytest.TempPathFactory,
) -> None:
    """A record with a null/missing timestamp field is still shipped, but its
    checkpoint carries the PREVIOUS good watermark, never ''/'None'."""
    store = FileCheckpointStore(tmp_path / "state.json")  # type: ignore[arg-type]

    respx.get(_query_url()).mock(
        return_value=httpx.Response(
            200,
            json={
                "records": [
                    {"Id": "a", "EventDate": "2026-06-30T10:00:00Z"},
                    {"Id": "b", "EventDate": None},  # null timestamp
                    {"Id": "c", "EventDate": "2026-06-30T10:02:00Z"},
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
        entries = [e async for e in source.events(store, asyncio.Event())]

    assert len(entries) == 3  # all shipped, including the null-timestamp record
    wm_a = json.loads(entries[0].checkpoint.value)["last_ts"]
    wm_b = json.loads(entries[1].checkpoint.value)["last_ts"]
    wm_c = json.loads(entries[2].checkpoint.value)["last_ts"]
    assert wm_a == "2026-06-30T10:00:00Z"
    assert wm_b == "2026-06-30T10:00:00Z"  # carried, not "" / "None"
    assert wm_c == "2026-06-30T10:02:00Z"


@pytest.mark.asyncio
@respx.mock
async def test_garbage_stored_watermark_falls_back_to_lookback(
    tmp_path: pytest.TempPathFactory,
) -> None:
    """A poisoned/legacy-corrupt stored watermark (empty, 'None', garbage) must
    not be interpolated into SOQL — fall back to now-lookback with a warning."""
    store = FileCheckpointStore(tmp_path / "state.json")  # type: ignore[arg-type]
    await store.commit("eventlog_objects:LoginEvent", "None")

    captured_q: list[str] = []

    def capture(request: httpx.Request) -> httpx.Response:
        captured_q.append(parse_qs(urlparse(str(request.url)).query)["q"][0])
        return httpx.Response(200, json={"records": [], "done": True})

    respx.get(_query_url()).mock(side_effect=capture)

    cfg = make_elo_cfg(lookback=timedelta(hours=2))
    sf_cfg = make_sf_cfg()
    tokens = FakeTokenProvider()

    async with httpx.AsyncClient() as client:
        from sf2loki.salesforce.soql_client import SoqlClient

        soql = SoqlClient(sf_cfg, tokens, client)
        source = EventLogObjectsSource(cfg, soql, sm_fields=[], poll_once=True)
        _ = [e async for e in source.events(store, asyncio.Event())]

    assert len(captured_q) == 1
    q = captured_q[0]
    assert "None" not in q
    # The literal is ~now-2h, not garbage.
    wm = q.split(">= ")[1].split(" ")[0]
    wm_dt = datetime.fromisoformat(wm.replace("Z", "+00:00"))
    expected = datetime.now(UTC) - timedelta(hours=2)
    assert abs((expected - wm_dt).total_seconds()) < 5


@pytest.mark.asyncio
@respx.mock
async def test_empty_stored_watermark_falls_back_to_lookback(
    tmp_path: pytest.TempPathFactory,
) -> None:
    store = FileCheckpointStore(tmp_path / "state.json")  # type: ignore[arg-type]
    await store.commit("eventlog_objects:LoginEvent", "")

    captured_q: list[str] = []

    def capture(request: httpx.Request) -> httpx.Response:
        captured_q.append(parse_qs(urlparse(str(request.url)).query)["q"][0])
        return httpx.Response(200, json={"records": [], "done": True})

    respx.get(_query_url()).mock(side_effect=capture)

    cfg = make_elo_cfg()
    sf_cfg = make_sf_cfg()
    tokens = FakeTokenProvider()

    async with httpx.AsyncClient() as client:
        from sf2loki.salesforce.soql_client import SoqlClient

        soql = SoqlClient(sf_cfg, tokens, client)
        source = EventLogObjectsSource(cfg, soql, sm_fields=[], poll_once=True)
        _ = [e async for e in source.events(store, asyncio.Event())]  # must not raise

    assert len(captured_q) == 1
    # A well-formed datetime literal, not "WHERE EventDate >= " (empty).
    assert "WHERE EventDate >=  " not in captured_q[0]
    assert "WHERE EventDate >= 20" in captured_q[0]


# ---------------------------------------------------------------------------
# Tie-loss + throughput (C6): >= cursor with an id-dedup window, and a
# drain-until-short-page loop within each cycle.


@pytest.mark.asyncio
@respx.mock
async def test_boundary_tie_records_deduped_by_id(
    tmp_path: pytest.TempPathFactory,
) -> None:
    """Records at the exact watermark timestamp are re-fetched by the >= query
    but deduped via the id window — no re-emit, and no tie loss."""
    store = FileCheckpointStore(tmp_path / "state.json")  # type: ignore[arg-type]
    tie_ts = "2026-06-30T10:00:00Z"
    await store.commit(
        "eventlog_objects:LoginEvent",
        json.dumps({"last_ts": tie_ts, "ids": ["a"]}),
    )

    respx.get(_query_url()).mock(
        return_value=httpx.Response(
            200,
            json={
                "records": [
                    {"Id": "a", "EventDate": tie_ts},  # already seen: deduped
                    {"Id": "b", "EventDate": tie_ts},  # same-ts sibling: MUST emit
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
        entries = [e async for e in source.events(store, asyncio.Event())]

    assert len(entries) == 1
    assert '"Id": "b"' in entries[0].line
    final = json.loads(entries[0].checkpoint.value)
    assert final["last_ts"] == tie_ts
    assert "a" in final["ids"] and "b" in final["ids"]


@pytest.mark.asyncio
@respx.mock
async def test_drain_until_short_page_catches_up_within_one_cycle(
    tmp_path: pytest.TempPathFactory,
) -> None:
    """A full page (200 records) triggers a follow-up query in the SAME cycle,
    so throughput is not capped at 200 per poll interval."""
    store = FileCheckpointStore(tmp_path / "state.json")  # type: ignore[arg-type]

    base = datetime(2026, 6, 30, 10, 0, 0, tzinfo=UTC)
    page1 = [
        {"Id": f"r{i}", "EventDate": (base + timedelta(seconds=i)).strftime("%Y-%m-%dT%H:%M:%SZ")}
        for i in range(200)
    ]
    page2 = [{"Id": "r200", "EventDate": "2026-06-30T11:00:00Z"}]

    captured_q: list[str] = []

    def side_effect(request: httpx.Request) -> httpx.Response:
        captured_q.append(parse_qs(urlparse(str(request.url)).query)["q"][0])
        records = page1 if len(captured_q) == 1 else page2
        return httpx.Response(200, json={"records": records, "done": True})

    respx.get(_query_url()).mock(side_effect=side_effect)

    cfg = make_elo_cfg()
    sf_cfg = make_sf_cfg()
    tokens = FakeTokenProvider()

    async with httpx.AsyncClient() as client:
        from sf2loki.salesforce.soql_client import SoqlClient

        soql = SoqlClient(sf_cfg, tokens, client)
        source = EventLogObjectsSource(cfg, soql, sm_fields=[], poll_once=True)
        entries = [e async for e in source.events(store, asyncio.Event())]

    assert len(entries) == 201
    assert len(captured_q) == 2
    # The follow-up query advances the cursor to the last record's timestamp.
    assert "2026-06-30T10:03:19" in captured_q[1]  # base + 199s


@pytest.mark.asyncio
@respx.mock
async def test_full_page_of_only_seen_ids_terminates_cycle(
    tmp_path: pytest.TempPathFactory,
) -> None:
    """Pathological tie overflow (a full page of already-seen ids) must not
    loop forever within a cycle."""
    store = FileCheckpointStore(tmp_path / "state.json")  # type: ignore[arg-type]
    tie_ts = "2026-06-30T10:00:00Z"
    ids = [f"r{i}" for i in range(200)]
    await store.commit(
        "eventlog_objects:LoginEvent",
        json.dumps({"last_ts": tie_ts, "ids": ids}),
    )

    call_count = 0

    def side_effect(request: httpx.Request) -> httpx.Response:
        nonlocal call_count
        call_count += 1
        return httpx.Response(
            200,
            json={"records": [{"Id": i, "EventDate": tie_ts} for i in ids], "done": True},
        )

    respx.get(_query_url()).mock(side_effect=side_effect)

    cfg = make_elo_cfg()
    sf_cfg = make_sf_cfg()
    tokens = FakeTokenProvider()

    async with httpx.AsyncClient() as client:
        from sf2loki.salesforce.soql_client import SoqlClient

        soql = SoqlClient(sf_cfg, tokens, client)
        source = EventLogObjectsSource(cfg, soql, sm_fields=[], poll_once=True)
        entries = [e async for e in source.events(store, asyncio.Event())]

    assert entries == []
    assert call_count == 1  # terminated, no infinite drain loop


# ---------------------------------------------------------------------------
# Cycle-level resiliency (C2/C5): SOQL failures must not crash the process;
# REQUEST_LIMIT_EXCEEDED aborts the rest of the cycle.


@pytest.mark.asyncio
@respx.mock
async def test_soql_error_skips_cycle_without_raising(
    tmp_path: pytest.TempPathFactory,
) -> None:
    store = FileCheckpointStore(tmp_path / "state.json")  # type: ignore[arg-type]

    calls = 0

    def side_effect(request: httpx.Request) -> httpx.Response:
        nonlocal calls
        calls += 1
        if calls == 1:
            return httpx.Response(500, text="Internal Server Error")
        return httpx.Response(
            200,
            json={
                "records": [{"Id": "a", "EventDate": "2026-06-30T10:00:00Z"}],
                "done": True,
            },
        )

    respx.get(_query_url()).mock(side_effect=side_effect)

    cfg = make_elo_cfg()
    sf_cfg = make_sf_cfg()
    tokens = FakeTokenProvider()

    async with httpx.AsyncClient() as client:
        from sf2loki.salesforce.soql_client import SoqlClient

        soql = SoqlClient(sf_cfg, tokens, client)
        source = EventLogObjectsSource(cfg, soql, sm_fields=[], poll_once=True)

        # Cycle 1: the 500 is contained — no exception, no entries.
        entries = [e async for e in source.events(store, asyncio.Event())]
        assert entries == []

        # Cycle 2: proceeds normally.
        entries2 = [e async for e in source.events(store, asyncio.Event())]
        assert len(entries2) == 1


@pytest.mark.asyncio
@respx.mock
async def test_throttled_aborts_rest_of_cycle(
    tmp_path: pytest.TempPathFactory,
) -> None:
    """REQUEST_LIMIT_EXCEEDED on the first object stops the remaining objects
    this cycle instead of burning more of the exhausted API budget."""
    store = FileCheckpointStore(tmp_path / "state.json")  # type: ignore[arg-type]

    call_count = 0

    def side_effect(request: httpx.Request) -> httpx.Response:
        nonlocal call_count
        call_count += 1
        return httpx.Response(
            403, json=[{"message": "limit", "errorCode": "REQUEST_LIMIT_EXCEEDED"}]
        )

    respx.get(_query_url()).mock(side_effect=side_effect)

    cfg = EventLogObjectsConfig(
        enabled=True,
        objects=[
            EventLogObjectConfig(name="LoginEvent", poll_interval=timedelta(seconds=0)),
            EventLogObjectConfig(name="ApiEvent", poll_interval=timedelta(seconds=0)),
        ],
    )
    sf_cfg = make_sf_cfg()
    tokens = FakeTokenProvider()

    async with httpx.AsyncClient() as client:
        from sf2loki.salesforce.soql_client import SoqlClient

        soql = SoqlClient(sf_cfg, tokens, client)
        source = EventLogObjectsSource(cfg, soql, sm_fields=[], poll_once=True)
        entries = [e async for e in source.events(store, asyncio.Event())]  # must not raise

    assert entries == []
    assert call_count == 1  # second object never queried this cycle


# ---------------------------------------------------------------------------
# Per-object poll_interval timers (issue #18): each object polls on ITS OWN
# interval, not the first object's.


@pytest.mark.asyncio
@respx.mock
async def test_per_object_poll_intervals_schedule_independently(
    tmp_path: pytest.TempPathFactory,
) -> None:
    """A fast-interval object polls many times while a slow-interval object in
    the same source polls only once (its next due time is far in the future)."""
    store = FileCheckpointStore(tmp_path / "state.json")  # type: ignore[arg-type]
    calls: dict[str, int] = {"LoginEvent": 0, "ApiEvent": 0}

    def side_effect(request: httpx.Request) -> httpx.Response:
        q = parse_qs(urlparse(str(request.url)).query)["q"][0]
        for name in calls:
            if f"FROM {name} " in q:
                calls[name] += 1
        return httpx.Response(200, json={"records": [], "done": True})

    respx.get(_query_url()).mock(side_effect=side_effect)

    cfg = EventLogObjectsConfig(
        enabled=True,
        objects=[
            EventLogObjectConfig(name="LoginEvent", poll_interval=timedelta(milliseconds=20)),
            EventLogObjectConfig(name="ApiEvent", poll_interval=timedelta(seconds=10)),
        ],
    )
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
        await asyncio.sleep(0.3)
        stop.set()
        await asyncio.wait_for(task, timeout=1.0)

    # The slow object is due only once in 0.3s; the fast one many times.
    assert calls["ApiEvent"] == 1
    assert calls["LoginEvent"] >= 4


# ---------------------------------------------------------------------------
# Poll-error counter (issue #19): contained cycle failures must be countable.


@pytest.mark.asyncio
@respx.mock
async def test_soql_error_increments_poll_error_counter(
    tmp_path: pytest.TempPathFactory,
) -> None:
    store = FileCheckpointStore(tmp_path / "state.json")  # type: ignore[arg-type]
    respx.get(_query_url()).mock(return_value=httpx.Response(500, text="boom"))

    cfg = make_elo_cfg()
    metrics = Metrics()
    async with httpx.AsyncClient() as client:
        from sf2loki.salesforce.soql_client import SoqlClient

        soql = SoqlClient(make_sf_cfg(), FakeTokenProvider(), client)
        source = EventLogObjectsSource(cfg, soql, sm_fields=[], poll_once=True, metrics=metrics)
        entries = [e async for e in source.events(store, asyncio.Event())]

    assert entries == []
    assert (
        metrics.registry.get_sample_value(
            "sf2loki_soql_poll_errors_total",
            {"source": "eventlog_objects", "object": "LoginEvent"},
        )
        == 1.0
    )


@pytest.mark.asyncio
@respx.mock
async def test_throttled_cycle_increments_poll_error_counter(
    tmp_path: pytest.TempPathFactory,
) -> None:
    store = FileCheckpointStore(tmp_path / "state.json")  # type: ignore[arg-type]
    respx.get(_query_url()).mock(
        return_value=httpx.Response(
            403, json=[{"message": "limit", "errorCode": "REQUEST_LIMIT_EXCEEDED"}]
        )
    )

    cfg = make_elo_cfg()
    metrics = Metrics()
    async with httpx.AsyncClient() as client:
        from sf2loki.salesforce.soql_client import SoqlClient

        soql = SoqlClient(make_sf_cfg(), FakeTokenProvider(), client, metrics=metrics)
        source = EventLogObjectsSource(cfg, soql, sm_fields=[], poll_once=True, metrics=metrics)
        _ = [e async for e in source.events(store, asyncio.Event())]

    assert (
        metrics.registry.get_sample_value(
            "sf2loki_soql_poll_errors_total",
            {"source": "eventlog_objects", "object": "LoginEvent"},
        )
        == 1.0
    )


# ---------------------------------------------------------------------------
# Deterministic timestamp fallback (issue #20): a record with an unparseable
# timestamp gets the PREVIOUS watermark as its entry time (stable across
# replays), not now(UTC) — and the fallback is counted.


@pytest.mark.asyncio
@respx.mock
async def test_null_timestamp_uses_previous_watermark_and_counts_fallback(
    tmp_path: pytest.TempPathFactory,
) -> None:
    store = FileCheckpointStore(tmp_path / "state.json")  # type: ignore[arg-type]
    # Watermark within 1h of now so the Loki OOO clamp does not kick in.
    wm_dt = (datetime.now(UTC) - timedelta(minutes=30)).replace(microsecond=0)
    wm = wm_dt.strftime("%Y-%m-%dT%H:%M:%SZ")
    await store.commit("eventlog_objects:LoginEvent", json.dumps({"last_ts": wm, "ids": []}))

    respx.get(_query_url()).mock(
        return_value=httpx.Response(
            200,
            json={"records": [{"Id": "b", "EventDate": None}], "done": True},
        )
    )

    cfg = make_elo_cfg()
    metrics = Metrics()
    async with httpx.AsyncClient() as client:
        from sf2loki.salesforce.soql_client import SoqlClient

        soql = SoqlClient(make_sf_cfg(), FakeTokenProvider(), client)
        source = EventLogObjectsSource(cfg, soql, sm_fields=[], poll_once=True, metrics=metrics)
        entries = [e async for e in source.events(store, asyncio.Event())]

    assert len(entries) == 1
    # Stable fallback: the previous watermark, NOT ingest time.
    assert entries[0].timestamp == wm_dt
    assert (
        metrics.registry.get_sample_value(
            "sf2loki_timestamp_fallbacks_total", {"source": "eventlog_objects"}
        )
        == 1.0
    )


@pytest.mark.asyncio
@respx.mock
async def test_parseable_timestamps_never_count_fallback(
    tmp_path: pytest.TempPathFactory,
) -> None:
    store = FileCheckpointStore(tmp_path / "state.json")  # type: ignore[arg-type]
    respx.get(_query_url()).mock(
        return_value=httpx.Response(
            200,
            json={
                "records": [{"Id": "a", "EventDate": "2026-06-30T10:00:00Z"}],
                "done": True,
            },
        )
    )

    cfg = make_elo_cfg()
    metrics = Metrics()
    async with httpx.AsyncClient() as client:
        from sf2loki.salesforce.soql_client import SoqlClient

        soql = SoqlClient(make_sf_cfg(), FakeTokenProvider(), client)
        source = EventLogObjectsSource(cfg, soql, sm_fields=[], poll_once=True, metrics=metrics)
        entries = [e async for e in source.events(store, asyncio.Event())]

    assert len(entries) == 1
    assert (
        metrics.registry.get_sample_value(
            "sf2loki_timestamp_fallbacks_total", {"source": "eventlog_objects"}
        )
        is None
    )


# ---------------------------------------------------------------------------
# Transforms + deterministic sampling (issues #27 / #26)


from sf2loki.config import TransformRule  # noqa: E402
from sf2loki.shaping import should_keep  # noqa: E402


@pytest.mark.asyncio
@respx.mock
async def test_sampled_out_record_enters_id_window(tmp_path: pytest.TempPathFactory) -> None:
    """A sampled-out record emits no entry but its Id STILL enters the dedup id
    window (committed by a later kept record) so the next poll does not re-fetch
    and re-drop it forever."""
    rate = 0.5
    dropped_id = next(f"005A{i}" for i in range(1000) if not should_keep(f"005A{i}", rate))
    kept_id = next(f"005B{i}" for i in range(1000) if should_keep(f"005B{i}", rate))
    store = FileCheckpointStore(tmp_path / "state.json")  # type: ignore[arg-type]

    respx.get(_query_url()).mock(
        return_value=httpx.Response(
            200,
            json={
                "records": [
                    {"EventDate": "2026-06-30T10:00:00Z", "Id": dropped_id, "UserId": "u1"},
                    {"EventDate": "2026-06-30T10:01:00Z", "Id": kept_id, "UserId": "u2"},
                ],
                "done": True,
            },
        )
    )

    cfg = make_elo_cfg()
    cfg.objects[0].sample = rate
    sf_cfg = make_sf_cfg()
    tokens = FakeTokenProvider()
    metrics = Metrics()

    async with httpx.AsyncClient() as client:
        from sf2loki.salesforce.soql_client import SoqlClient

        soql = SoqlClient(sf_cfg, tokens, client)
        source = EventLogObjectsSource(cfg, soql, sm_fields=[], metrics=metrics, poll_once=True)
        entries = [e async for e in source.events(store, asyncio.Event())]

    # Only the kept record is emitted...
    assert len(entries) == 1
    assert entries[0].line.find(kept_id) != -1
    # ...but its committed checkpoint window includes the sampled-out record's Id.
    window = json.loads(entries[0].checkpoint.value)["ids"]
    assert dropped_id in window
    assert kept_id in window
    sampled = metrics.registry.get_sample_value(
        "sf2loki_entries_sampled_out_total",
        {"source": "eventlog_objects", "event_type": "LoginEvent"},
    )
    assert sampled == 1.0


@pytest.mark.asyncio
@respx.mock
async def test_watermark_uses_original_timestamp_despite_redaction(
    tmp_path: pytest.TempPathFactory,
) -> None:
    """A transform redacting the timestamp column must NOT corrupt the SOQL
    watermark cursor: the checkpoint keeps the real EventDate value."""
    store = FileCheckpointStore(tmp_path / "state.json")  # type: ignore[arg-type]
    respx.get(_query_url()).mock(
        return_value=httpx.Response(
            200,
            json={
                "records": [{"EventDate": "2026-06-30T10:00:00Z", "Id": "005x", "UserId": "u"}],
                "done": True,
            },
        )
    )

    cfg = make_elo_cfg()
    cfg.transforms.append(TransformRule(action="drop_field", fields=["EventDate"]))
    sf_cfg = make_sf_cfg()
    tokens = FakeTokenProvider()

    async with httpx.AsyncClient() as client:
        from sf2loki.salesforce.soql_client import SoqlClient

        soql = SoqlClient(sf_cfg, tokens, client)
        source = EventLogObjectsSource(cfg, soql, sm_fields=[], poll_once=True)
        entries = [e async for e in source.events(store, asyncio.Event())]

    assert len(entries) == 1
    # EventDate redacted from the emitted line...
    assert "EventDate" not in entries[0].line
    # ...but the checkpoint watermark keeps the real value (query cursor intact).
    assert json.loads(entries[0].checkpoint.value)["last_ts"] == "2026-06-30T10:00:00Z"


# ---------------------------------------------------------------------------
# Big-object DESC descending-drain mode (issue #34): Big Objects reject
# ORDER BY ASC and expose no nextRecordsUrl pagination — drain newest-first
# with a ratcheting <= upper bound, dedup, and re-sort ascending before emit.


def make_big_object_cfg(
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
                big_object=True,
            )
        ],
    )


@pytest.mark.asyncio
@respx.mock
async def test_big_object_query_uses_desc_and_emits_ascending(
    tmp_path: pytest.TempPathFactory,
) -> None:
    """A big_object drains DESC in one short page, then emits oldest-first."""
    store = FileCheckpointStore(tmp_path / "state.json")  # type: ignore[arg-type]
    captured: list[str] = []

    def _capture(request: httpx.Request) -> httpx.Response:
        captured.append(parse_qs(urlparse(str(request.url)).query)["q"][0])
        return httpx.Response(
            200,
            json={
                "records": [
                    {"Id": "b", "EventDate": "2026-06-30T10:02:00.000+0000"},
                    {"Id": "a", "EventDate": "2026-06-30T10:01:00.000+0000"},
                ],
                "done": True,
            },
        )

    respx.get(_query_url()).mock(side_effect=_capture)

    async with httpx.AsyncClient() as client:
        from sf2loki.salesforce.soql_client import SoqlClient

        soql = SoqlClient(make_sf_cfg(), FakeTokenProvider(), client)
        source = EventLogObjectsSource(make_big_object_cfg(), soql, sm_fields=[], poll_once=True)
        entries = [e async for e in source.events(store, asyncio.Event())]

    # DESC order in the query, no ASC anywhere
    assert "ORDER BY EventDate DESC" in captured[0]
    assert "ASC" not in captured[0]
    # Emitted oldest-first (a before b) despite the DESC fetch
    assert [json.loads(e.checkpoint.value)["last_ts"] for e in entries] == [
        "2026-06-30T10:01:00.000+0000",
        "2026-06-30T10:02:00.000+0000",
    ]
    # Final committed watermark is the NEWEST row's timestamp
    assert json.loads(entries[-1].checkpoint.value)["last_ts"] == "2026-06-30T10:02:00.000+0000"


@pytest.mark.asyncio
@respx.mock
async def test_big_object_ratchets_upper_bound_across_pages(
    tmp_path: pytest.TempPathFactory,
) -> None:
    """A full page triggers a second query bounded by the oldest seen (<=), deduped."""
    store = FileCheckpointStore(tmp_path / "state.json")  # type: ignore[arg-type]
    captured: list[str] = []

    # page 1: 200 rows newest->oldest (minute 200 down to 1), oldest = 10:01:00 (Id "p1-199").
    # Built from a real datetime so every timestamp is valid and strictly descending.
    base = datetime(2026, 6, 30, 10, 0, 0, tzinfo=UTC)
    page1 = [
        {
            "Id": f"p1-{i}",
            "EventDate": (base + timedelta(minutes=200 - i)).strftime("%Y-%m-%dT%H:%M:%S.000+0000"),
        }
        for i in range(200)
    ]
    oldest1 = page1[-1]["EventDate"]  # 2026-06-30T10:01:00.000+0000
    # page 2: short page, older, PLUS a re-fetched boundary tie at oldest1 (deduped)
    page2 = [
        {"Id": "p1-199", "EventDate": oldest1},  # boundary tie, already seen
        {"Id": "p2-a", "EventDate": "2026-06-30T09:59:00.000+0000"},
    ]

    def _capture(request: httpx.Request) -> httpx.Response:
        q = parse_qs(urlparse(str(request.url)).query)["q"][0]
        captured.append(q)
        body = page1 if len(captured) == 1 else page2
        return httpx.Response(200, json={"records": body, "done": True})

    respx.get(_query_url()).mock(side_effect=_capture)

    async with httpx.AsyncClient() as client:
        from sf2loki.salesforce.soql_client import SoqlClient

        soql = SoqlClient(make_sf_cfg(), FakeTokenProvider(), client)
        source = EventLogObjectsSource(make_big_object_cfg(), soql, sm_fields=[], poll_once=True)
        entries = [e async for e in source.events(store, asyncio.Event())]

    assert len(captured) == 2  # ratcheted to a second page
    # 2nd query re-bounds with <= the oldest of page 1 (tie-safe, not strict <)
    assert f"EventDate <= {to_soql_datetime_literal(oldest1)}" in captured[1]
    # 201 distinct ids emitted (200 from page1 + 1 new from page2; the tie deduped)
    ids = [json.loads(e.checkpoint.value)["ids"][-1] for e in entries]
    assert ids.count("p1-199") == 1
    assert "p2-a" in ids
    assert len(entries) == 201


@pytest.mark.asyncio
@respx.mock
async def test_big_object_full_page_all_seen_terminates(
    tmp_path: pytest.TempPathFactory,
) -> None:
    """A full page that adds no new ids stops the drain (no hot loop)."""
    store = FileCheckpointStore(tmp_path / "state.json")  # type: ignore[arg-type]
    page = [{"Id": f"x{i}", "EventDate": "2026-06-30T10:00:00.000+0000"} for i in range(200)]

    calls = {"n": 0}

    def _capture(request: httpx.Request) -> httpx.Response:
        calls["n"] += 1
        # Always return the SAME full page of ties -> second query adds nothing new
        return httpx.Response(200, json={"records": page, "done": True})

    respx.get(_query_url()).mock(side_effect=_capture)

    async with httpx.AsyncClient() as client:
        from sf2loki.salesforce.soql_client import SoqlClient

        soql = SoqlClient(make_sf_cfg(), FakeTokenProvider(), client)
        source = EventLogObjectsSource(make_big_object_cfg(), soql, sm_fields=[], poll_once=True)
        entries = [e async for e in source.events(store, asyncio.Event())]

    assert len(entries) == 200  # the 200 distinct ties, emitted once
    assert calls["n"] == 2  # one ratchet, then the all-seen guard stops it


@pytest.mark.asyncio
@respx.mock
async def test_big_object_throttle_aborts_without_crashing(
    tmp_path: pytest.TempPathFactory,
) -> None:
    store = FileCheckpointStore(tmp_path / "state.json")  # type: ignore[arg-type]
    respx.get(_query_url()).mock(return_value=httpx.Response(403, text="REQUEST_LIMIT_EXCEEDED"))
    async with httpx.AsyncClient() as client:
        from sf2loki.salesforce.soql_client import SoqlClient

        soql = SoqlClient(make_sf_cfg(), FakeTokenProvider(), client)
        source = EventLogObjectsSource(make_big_object_cfg(), soql, sm_fields=[], poll_once=True)
        entries = [e async for e in source.events(store, asyncio.Event())]
    assert entries == []  # throttle contained, no exception escapes


@pytest.mark.asyncio
@respx.mock
async def test_big_object_boundary_record_deduped_across_cycles(
    tmp_path: pytest.TempPathFactory,
) -> None:
    """A big_object's >= watermark query is inclusive (tie-safe), so a poll cycle
    that resumes from a stored checkpoint re-fetches the boundary record whose
    timestamp equals the committed watermark. ``_drain_big_object`` only dedups
    WITHIN one drain (its own ``collected`` dict) — it has no memory of the
    PREVIOUS cycle's committed id window — so without a cross-cycle filter that
    boundary record would be re-emitted as a duplicate LogEntry every cycle.
    Mirrors ``test_boundary_tie_records_deduped_by_id`` for the ASC path."""
    store = FileCheckpointStore(tmp_path / "state.json")  # type: ignore[arg-type]
    boundary_ts = "2026-06-30T10:00:00.000+0000"
    boundary_id = "a"
    await store.commit(
        "eventlog_objects:LoginEvent",
        json.dumps({"last_ts": boundary_ts, "ids": [boundary_id]}),
    )

    # DESC drain: newest first. "b" is strictly newer than the committed
    # watermark; "a" is the already-committed boundary record re-fetched by
    # the inclusive >= cursor.
    respx.get(_query_url()).mock(
        return_value=httpx.Response(
            200,
            json={
                "records": [
                    {"Id": "b", "EventDate": "2026-06-30T10:01:00.000+0000"},
                    {"Id": boundary_id, "EventDate": boundary_ts},
                ],
                "done": True,
            },
        )
    )

    async with httpx.AsyncClient() as client:
        from sf2loki.salesforce.soql_client import SoqlClient

        soql = SoqlClient(make_sf_cfg(), FakeTokenProvider(), client)
        source = EventLogObjectsSource(make_big_object_cfg(), soql, sm_fields=[], poll_once=True)
        entries = [e async for e in source.events(store, asyncio.Event())]

    assert len(entries) == 1
    assert '"Id": "b"' in entries[0].line
    final = json.loads(entries[0].checkpoint.value)
    assert boundary_id in final["ids"] and "b" in final["ids"]


@pytest.mark.asyncio
@respx.mock
async def test_asc_big_object_error_logs_hint(
    tmp_path: pytest.TempPathFactory, caplog: pytest.LogCaptureFixture
) -> None:
    """An ASC query against a Big Object logs a hint to set big_object: true."""
    store = FileCheckpointStore(tmp_path / "state.json")  # type: ignore[arg-type]
    respx.get(_query_url()).mock(
        return_value=httpx.Response(
            400,
            json=[
                {
                    "message": (
                        "Unsupported order direction on filter column: EVENTDATE : ASCENDING"
                    ),
                    "errorCode": "BIG_OBJECT_UNSUPPORTED_OPERATION",
                }
            ],
        )
    )
    async with httpx.AsyncClient() as client:
        from sf2loki.salesforce.soql_client import SoqlClient

        soql = SoqlClient(make_sf_cfg(), FakeTokenProvider(), client)
        source = EventLogObjectsSource(make_elo_cfg(), soql, sm_fields=[], poll_once=True)
        with caplog.at_level("WARNING"):
            entries = [e async for e in source.events(store, asyncio.Event())]

    assert entries == []
    assert "big_object: true" in caplog.text


@pytest.mark.asyncio
@respx.mock
async def test_big_object_drain_aborts_on_stop_without_committing(
    tmp_path: pytest.TempPathFactory,
) -> None:
    """A stop set mid-drain abandons the whole window (emits nothing, no checkpoint).

    Emitting a partial newest slice would advance the watermark past the
    un-drained older records and lose them, so the drain returns empty on stop.
    """
    store = FileCheckpointStore(tmp_path / "state.json")  # type: ignore[arg-type]
    stop = asyncio.Event()

    base = datetime(2026, 6, 30, 10, 0, 0, tzinfo=UTC)
    full_page = [
        {
            "Id": f"p-{i}",
            "EventDate": (base + timedelta(minutes=200 - i)).strftime("%Y-%m-%dT%H:%M:%S.000+0000"),
        }
        for i in range(200)  # a full page -> the drain would ratchet to page 2
    ]
    calls = {"n": 0}

    def _capture(request: httpx.Request) -> httpx.Response:
        calls["n"] += 1
        stop.set()  # signal shutdown after the first page is fetched
        return httpx.Response(200, json={"records": full_page, "done": True})

    respx.get(_query_url()).mock(side_effect=_capture)

    async with httpx.AsyncClient() as client:
        from sf2loki.salesforce.soql_client import SoqlClient

        soql = SoqlClient(make_sf_cfg(), FakeTokenProvider(), client)
        source = EventLogObjectsSource(make_big_object_cfg(), soql, sm_fields=[], poll_once=True)
        entries = [e async for e in source.events(store, stop)]

    assert entries == []  # nothing emitted from an aborted drain
    assert calls["n"] == 1  # page 2 was never queried (stop caught at the loop top)
    assert await store.load("eventlog_objects:LoginEvent") is None  # watermark uncommitted


# ---------------------------------------------------------------------------
# Issue #38: a full page of >_PAGE_LIMIT records sharing ONE timestamp must
# drain COMPLETELY (not permanently stall) on both the ASC and DESC paths, and
# a stall that repeats at the SAME boundary across cycles must escalate.


@pytest.mark.asyncio
@respx.mock
async def test_asc_more_than_page_limit_ties_drains_completely(
    tmp_path: pytest.TempPathFactory,
) -> None:
    """>_PAGE_LIMIT records sharing one timestamp must drain COMPLETELY within
    a cycle via the ASC path's Id tiebreak, not stall after the first page."""
    store = FileCheckpointStore(tmp_path / "state.json")  # type: ignore[arg-type]
    tie_ts = "2026-06-30T10:00:00Z"
    ids = [f"r{i:03d}" for i in range(250)]

    call_count = 0

    def side_effect(request: httpx.Request) -> httpx.Response:
        # Query-aware: WITHOUT the Id tiebreak fix, every query is identical
        # (plain ``>= wm``) and this always returns the same first 200 (the
        # real stall behaviour) -- the fixed code sends ``Id > 'rNNN'`` on the
        # follow-up query, which this mock honours to serve the NEXT slice.
        nonlocal call_count
        call_count += 1
        q = parse_qs(urlparse(str(request.url)).query)["q"][0]
        match = re.search(r"Id > '(\w+)'", q)
        page_ids = [i for i in ids if i > match.group(1)][:200] if match else ids[:200]
        return httpx.Response(
            200,
            json={"records": [{"Id": i, "EventDate": tie_ts} for i in page_ids], "done": True},
        )

    respx.get(_query_url()).mock(side_effect=side_effect)

    cfg = make_elo_cfg()
    sf_cfg = make_sf_cfg()
    tokens = FakeTokenProvider()

    async with httpx.AsyncClient() as client:
        from sf2loki.salesforce.soql_client import SoqlClient

        soql = SoqlClient(sf_cfg, tokens, client)
        source = EventLogObjectsSource(cfg, soql, sm_fields=[], poll_once=True)
        entries = [e async for e in source.events(store, asyncio.Event())]

    assert len(entries) == 250  # ALL 250 tied records emitted, not just the first page
    assert call_count == 2
    final = json.loads(entries[-1].checkpoint.value)
    assert final["last_ts"] == tie_ts
    assert "r249" in final["ids"]


@pytest.mark.asyncio
@respx.mock
async def test_big_object_more_than_page_limit_ties_drains_completely(
    tmp_path: pytest.TempPathFactory,
) -> None:
    """The DESC symmetric fix: >_PAGE_LIMIT tied records at the ratchet bound
    must fully drain via a page-aware id-window escape (NOT IN pagination),
    not stop after the first page. Big Objects reject a compound ORDER BY, so
    this mirrors the ASC path's Id tiebreak without one."""
    store = FileCheckpointStore(tmp_path / "state.json")  # type: ignore[arg-type]
    tie_ts = "2026-06-30T10:00:00.000+0000"
    ids = [f"x{i:03d}" for i in range(250)]

    call_count = 0

    def side_effect(request: httpx.Request) -> httpx.Response:
        nonlocal call_count
        call_count += 1
        if call_count == 1:
            page_ids = ids[:200]
        elif call_count == 2:
            page_ids = ids[200:]
        else:
            page_ids = []
        return httpx.Response(
            200,
            json={"records": [{"Id": i, "EventDate": tie_ts} for i in page_ids], "done": True},
        )

    respx.get(_query_url()).mock(side_effect=side_effect)

    async with httpx.AsyncClient() as client:
        from sf2loki.salesforce.soql_client import SoqlClient

        soql = SoqlClient(make_sf_cfg(), FakeTokenProvider(), client)
        source = EventLogObjectsSource(make_big_object_cfg(), soql, sm_fields=[], poll_once=True)
        entries = [e async for e in source.events(store, asyncio.Event())]

    assert len(entries) == 250
    assert call_count == 3
    finals = {json.loads(e.checkpoint.value)["last_ts"] for e in entries}
    assert finals == {tie_ts}


@pytest.mark.asyncio
@respx.mock
async def test_asc_repeated_stall_at_same_boundary_escalates_to_error_and_counts(
    tmp_path: pytest.TempPathFactory, caplog: pytest.LogCaptureFixture
) -> None:
    """A stall that repeats at the SAME watermark boundary across cycles is a
    permanent halt, not a one-off busy poll -- escalate to ERROR and increment
    the watermark_stalls metric (only from the 2nd occurrence onward)."""
    store = FileCheckpointStore(tmp_path / "state.json")  # type: ignore[arg-type]
    tie_ts = "2026-06-30T10:00:00Z"
    ids = [f"r{i:03d}" for i in range(200)]
    await store.commit(
        "eventlog_objects:LoginEvent",
        json.dumps({"last_ts": tie_ts, "ids": ids}),
    )

    # Every query returns the SAME already-seen full page: a genuine repeated
    # stall (every id truly already committed, tiebreak can't get past it).
    respx.get(_query_url()).mock(
        return_value=httpx.Response(
            200,
            json={"records": [{"Id": i, "EventDate": tie_ts} for i in ids], "done": True},
        )
    )

    cfg = make_elo_cfg()
    metrics = Metrics()
    async with httpx.AsyncClient() as client:
        from sf2loki.salesforce.soql_client import SoqlClient

        soql = SoqlClient(make_sf_cfg(), FakeTokenProvider(), client)
        source = EventLogObjectsSource(cfg, soql, sm_fields=[], poll_once=True, metrics=metrics)

        with caplog.at_level("WARNING"):
            entries1 = [e async for e in source.events(store, asyncio.Event())]
        assert entries1 == []
        assert metrics.registry.get_sample_value(
            "sf2loki_watermark_stalls_total",
            {"source": "eventlog_objects", "object": "LoginEvent"},
        ) in (None, 0.0)

        caplog.clear()
        with caplog.at_level("WARNING"):
            entries2 = [e async for e in source.events(store, asyncio.Event())]
        assert entries2 == []

    assert any(r.levelname == "ERROR" for r in caplog.records)
    assert (
        metrics.registry.get_sample_value(
            "sf2loki_watermark_stalls_total",
            {"source": "eventlog_objects", "object": "LoginEvent"},
        )
        == 1.0
    )


# ---------------------------------------------------------------------------
# Issue #46: bound the big_object DESC drain's memory via max_catchup_records.


@pytest.mark.asyncio
@respx.mock
async def test_big_object_catchup_bounded_by_max_catchup_records(
    tmp_path: pytest.TempPathFactory,
) -> None:
    """A post-outage catch-up over a big backlog must not RETAIN the whole gap
    in memory. max_catchup_records bounds how much of the (fully-swept,
    newest-first) window is kept -- the newest overflow is evicted (not lost:
    the committed watermark stops at the retained boundary, so a later cycle's
    ordinary >= cursor re-discovers exactly that evicted newer slice)."""
    store = FileCheckpointStore(tmp_path / "state.json")  # type: ignore[arg-type]

    base = datetime(2026, 6, 30, 0, 0, 0, tzinfo=UTC)

    def make_page(start_minute: int, count: int) -> list[dict[str, str]]:
        return [
            {
                "Id": f"p{start_minute + i:04d}",
                "EventDate": (base - timedelta(minutes=start_minute + i)).strftime(
                    "%Y-%m-%dT%H:%M:%S.000+0000"
                ),
            }
            for i in range(count)
        ]

    page1 = make_page(0, 200)  # newest 200 minutes (0..199 minutes old)
    page2 = make_page(200, 200)  # next 200 (200..399 minutes old)
    page3 = make_page(400, 50)  # oldest 50 (400..449 minutes old) -- short page

    call_count = 0

    def side_effect(request: httpx.Request) -> httpx.Response:
        nonlocal call_count
        call_count += 1
        page = {1: page1, 2: page2, 3: page3}.get(call_count, [])
        return httpx.Response(200, json={"records": page, "done": True})

    respx.get(_query_url()).mock(side_effect=side_effect)

    cfg = make_big_object_cfg(lookback=timedelta(days=2))
    cfg.objects[0].max_catchup_records = 250

    async with httpx.AsyncClient() as client:
        from sf2loki.salesforce.soql_client import SoqlClient

        soql = SoqlClient(make_sf_cfg(), FakeTokenProvider(), client)
        source = EventLogObjectsSource(cfg, soql, sm_fields=[], poll_once=True)
        entries = [e async for e in source.events(store, asyncio.Event())]

    # Bounded: not all 450 records are retained/emitted from one drain, even
    # though the full backlog was swept over the network to find the true tail.
    assert len(entries) == 250
    assert call_count == 3  # the full sweep still ran, all the way to a short page

    ids_kept = {json.loads(e.line)["Id"] for e in entries}
    assert "p0000" not in ids_kept  # newest-overflow evicted (recoverable later)
    assert "p0199" not in ids_kept
    assert "p0200" in ids_kept  # boundary + everything older retained
    assert "p0449" in ids_kept  # oldest record retained

    committed_wm = json.loads(entries[-1].checkpoint.value)["last_ts"]
    assert committed_wm == page2[0]["EventDate"]  # boundary of the retained window


# ---------------------------------------------------------------------------
# Issue #64: a fully-filtered/sampled cycle must still durably advance the
# watermark via a checkpoint_only token, so the next cycle doesn't re-fetch
# and deterministically re-drop the identical window forever.


@pytest.mark.asyncio
@respx.mock
async def test_fully_sampled_out_cycle_emits_checkpoint_only_and_advances_watermark(
    tmp_path: pytest.TempPathFactory,
) -> None:
    store = FileCheckpointStore(tmp_path / "state.json")  # type: ignore[arg-type]

    respx.get(_query_url()).mock(
        return_value=httpx.Response(
            200,
            json={
                "records": [
                    {"EventDate": "2026-06-30T10:00:00Z", "Id": "005a", "UserId": "u1"},
                    {"EventDate": "2026-06-30T10:01:00Z", "Id": "005b", "UserId": "u2"},
                ],
                "done": True,
            },
        )
    )

    cfg = make_elo_cfg()
    cfg.objects[0].sample = 0.0  # deterministically drops every row
    sf_cfg = make_sf_cfg()
    tokens = FakeTokenProvider()

    async with httpx.AsyncClient() as client:
        from sf2loki.salesforce.soql_client import SoqlClient

        soql = SoqlClient(sf_cfg, tokens, client)
        source = EventLogObjectsSource(cfg, soql, sm_fields=[], poll_once=True)
        entries = [e async for e in source.events(store, asyncio.Event())]

    # No real log entry (everything sampled out) but a checkpoint_only token
    # carrying the advanced watermark rides through so it durably commits.
    assert len(entries) == 1
    assert entries[0].checkpoint_only is True
    assert entries[0].line == ""
    final = json.loads(entries[0].checkpoint.value)
    assert final["last_ts"] == "2026-06-30T10:01:00Z"
    assert "005a" in final["ids"] and "005b" in final["ids"]


@pytest.mark.asyncio
@respx.mock
async def test_partially_filtered_cycle_does_not_emit_checkpoint_only(
    tmp_path: pytest.TempPathFactory,
) -> None:
    """When at least one row IS emitted, no extra checkpoint_only token is
    needed -- the real entry's own checkpoint already carries the advance."""
    store = FileCheckpointStore(tmp_path / "state.json")  # type: ignore[arg-type]

    respx.get(_query_url()).mock(
        return_value=httpx.Response(
            200,
            json={
                "records": [{"EventDate": "2026-06-30T10:00:00Z", "Id": "005a"}],
                "done": True,
            },
        )
    )

    cfg = make_elo_cfg()
    async with httpx.AsyncClient() as client:
        from sf2loki.salesforce.soql_client import SoqlClient

        soql = SoqlClient(make_sf_cfg(), FakeTokenProvider(), client)
        source = EventLogObjectsSource(cfg, soql, sm_fields=[], poll_once=True)
        entries = [e async for e in source.events(store, asyncio.Event())]

    assert len(entries) == 1
    assert entries[0].checkpoint_only is False
