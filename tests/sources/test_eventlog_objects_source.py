"""Tests for EventLogObjectsSource — SOQL-based polling source."""

from __future__ import annotations

import asyncio
import json
from datetime import UTC, datetime, timedelta
from urllib.parse import parse_qs, urlparse

import httpx
import pytest
import respx

from sf2loki.auth.jwt_auth import AccessToken
from sf2loki.config import EventLogObjectConfig, EventLogObjectsConfig, SalesforceConfig
from sf2loki.obs.metrics import Metrics
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
