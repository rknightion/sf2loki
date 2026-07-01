"""Tests for PubSubSource — async generator source consuming Salesforce Pub/Sub API."""

from __future__ import annotations

import asyncio
import base64
import json
from collections.abc import AsyncIterator
from datetime import UTC, datetime

import grpc
import grpc.aio
import pytest
import structlog.testing

from sf2loki.config import PubSubConfig
from sf2loki.obs.metrics import Metrics
from sf2loki.salesforce.pubsub_client import DecodedEvent, KeepaliveEvent, preset_for
from sf2loki.sources.pubsub_source import PubSubSource, _jitter

# ---------------------------------------------------------------------------
# Helpers / fakes
# ---------------------------------------------------------------------------

EPOCH_MS = int(datetime(2024, 1, 15, 12, 0, 0, tzinfo=UTC).timestamp() * 1000)
REPLAY_ID_1 = b"\x00\x01"
REPLAY_ID_2 = b"\x00\x02"
STORED_REPLAY_ID = b"\x09\x09"
TOPIC = "/event/LoginEventStream"


def make_event(
    topic: str = TOPIC,
    replay_id: bytes = REPLAY_ID_1,
    user_id: str = "005000000000001",
) -> DecodedEvent:
    return DecodedEvent(
        topic=topic,
        replay_id=replay_id,
        schema_id="schema1",
        payload={"EventDate": EPOCH_MS, "UserId": user_id},
    )


class FakeCheckpointStore:
    """In-memory CheckpointStore for tests."""

    def __init__(self, initial: dict[str, str] | None = None) -> None:
        self._data: dict[str, str] = dict(initial or {})

    async def load(self, key: str) -> str | None:
        return self._data.get(key)

    async def commit(self, key: str, value: str) -> None:
        self._data[key] = value


class FakePubSubClient:
    """Fake PubSubClient.

    - Yields a fixed sequence of DecodedEvents on the FIRST subscribe call.
    - On subsequent calls, raises ``StopAsyncIteration`` immediately (empty stream)
      so that reconnect loops inside _run_topic remain live (they'll back off and
      retry) but tests can terminate by setting the stop event.
    - Records call arguments so tests can assert on replay_preset / replay_id.

    The ``stop`` event is set automatically once the first batch of events has been
    yielded, allowing drain loops in tests to terminate promptly without hanging.
    """

    def __init__(
        self,
        events_by_topic: dict[str, list[DecodedEvent]],
        *,
        stop_after_first: asyncio.Event | None = None,
    ) -> None:
        self._events = events_by_topic
        self._call_counts: dict[str, int] = {}
        self._stop_after_first = stop_after_first
        self.calls: list[dict[str, object]] = []

    async def subscribe(
        self,
        topic: str,
        *,
        replay_preset: int,
        replay_id: bytes = b"",
        num_requested: int | None = None,
    ) -> AsyncIterator[DecodedEvent]:
        n = self._call_counts.get(topic, 0)
        self._call_counts[topic] = n + 1
        self.calls.append({"topic": topic, "replay_preset": replay_preset, "replay_id": replay_id})
        if n == 0:
            for ev in self._events.get(topic, []):
                yield ev
            # After exhausting first batch, signal stop so the reconnect loop
            # terminates promptly.
            if self._stop_after_first is not None:
                self._stop_after_first.set()
        # Subsequent calls: empty stream (immediate close) so reconnect backoff
        # triggers; the stop event will interrupt the backoff sleep.

    async def aclose(self) -> None:
        pass


def _rpc_error(code: grpc.StatusCode) -> grpc.aio.AioRpcError:
    """Build a real AioRpcError with the given status code (as raised client-side)."""
    return grpc.aio.AioRpcError(code, grpc.aio.Metadata(), grpc.aio.Metadata(), "rejected", "")


class ScriptedClient:
    """Fake client whose subscribe() follows a per-call script.

    Each script entry is either an Exception (raised after the call is
    recorded) or a list of items; list items are yielded in order, except that
    an Exception inside the list is raised at that point (mid-stream failure).
    After the script is exhausted every stream is empty (immediate clean close).
    ``on_call`` maps a 0-based call index to an asyncio.Event set when that
    call starts, letting tests deterministically wait for a given attempt.
    """

    def __init__(
        self,
        scripts: list[Exception | list[DecodedEvent | KeepaliveEvent | Exception]],
        *,
        on_call: dict[int, asyncio.Event] | None = None,
    ) -> None:
        self.scripts = scripts
        self.calls: list[dict[str, object]] = []
        self._on_call = on_call or {}

    async def subscribe(
        self,
        topic: str,
        *,
        replay_preset: int,
        replay_id: bytes = b"",
        num_requested: int | None = None,
    ) -> AsyncIterator[DecodedEvent | KeepaliveEvent]:
        idx = len(self.calls)
        self.calls.append({"topic": topic, "replay_preset": replay_preset, "replay_id": replay_id})
        signal = self._on_call.get(idx)
        if signal is not None:
            signal.set()
        if idx < len(self.scripts):
            script = self.scripts[idx]
            if isinstance(script, Exception):
                raise script
            for item in script:
                if isinstance(item, Exception):
                    raise item
                yield item
        # Script exhausted: empty stream (immediate clean close).

    async def aclose(self) -> None:
        pass


def make_cfg(
    topics: list[str] | None = None,
    include: list[str] | None = None,
    exclude: list[str] | None = None,
    replay_preset: str = "LATEST",
) -> PubSubConfig:
    kwargs: dict[str, object] = {
        "replay_preset": replay_preset,
        "topics": topics or [TOPIC],
    }
    if include is not None:
        kwargs["include"] = include
    if exclude is not None:
        kwargs["exclude"] = exclude
    return PubSubConfig(**kwargs)


def make_source(
    cfg: PubSubConfig | None = None,
    client: FakePubSubClient | None = None,
    sm_fields: list[str] | None = None,
    stop: asyncio.Event | None = None,
    metrics: Metrics | None = None,
) -> PubSubSource:
    if cfg is None:
        cfg = make_cfg()
    if client is None:
        client = FakePubSubClient({TOPIC: [make_event()]}, stop_after_first=stop)
    return PubSubSource(
        cfg, client, sm_fields=sm_fields or [], reconnect_backoff=0.01, metrics=metrics
    )


async def collect_n(
    src: PubSubSource,
    state: FakeCheckpointStore,
    stop: asyncio.Event,
    n: int,
    max_wait: float = 5.0,
) -> list:
    """Consume up to n entries from src.events(), setting stop once n entries are seen.

    Uses wait_for to guard against hangs in tests.
    """
    entries: list = []

    async def _consume() -> None:
        async for entry in src.events(state, stop):
            entries.append(entry)
            if len(entries) >= n:
                stop.set()

    await asyncio.wait_for(_consume(), timeout=max_wait)
    return entries


# ---------------------------------------------------------------------------
# resolve_topics tests
# ---------------------------------------------------------------------------


def test_resolve_topics_no_filters() -> None:
    """All topics pass when include is ['*'] and exclude is empty."""
    cfg = make_cfg(topics=[TOPIC, "/event/Other"])
    src = make_source(cfg=cfg)
    assert src.resolve_topics() == [TOPIC, "/event/Other"]


def test_resolve_topics_include_glob() -> None:
    """Only topics matching include glob are kept."""
    cfg = make_cfg(
        topics=["/event/LoginEventStream", "/event/LogoutEventStream", "/other/Foo"],
        include=["/event/*"],
    )
    src = make_source(cfg=cfg)
    assert src.resolve_topics() == ["/event/LoginEventStream", "/event/LogoutEventStream"]


def test_resolve_topics_exclude_glob() -> None:
    """Topics matching exclude glob are dropped even if they match include."""
    cfg = make_cfg(
        topics=["/event/LoginEventStream", "/event/AnomalyEventStream"],
        include=["/event/*"],
        exclude=["*Anomaly*"],
    )
    src = make_source(cfg=cfg)
    assert src.resolve_topics() == ["/event/LoginEventStream"]


def test_resolve_topics_dedupes() -> None:
    """Duplicate topics are removed, first occurrence wins."""
    cfg = make_cfg(topics=[TOPIC, TOPIC, "/event/Other"])
    src = make_source(cfg=cfg)
    assert src.resolve_topics() == [TOPIC, "/event/Other"]


def test_resolve_topics_empty_when_all_excluded() -> None:
    """Empty list returned when all topics are excluded."""
    cfg = make_cfg(
        topics=["/event/LoginEventStream"],
        include=["*"],
        exclude=["*"],
    )
    src = make_source(cfg=cfg)
    assert src.resolve_topics() == []


# ---------------------------------------------------------------------------
# events() basic yield tests
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_events_yields_log_entry_with_correct_labels() -> None:
    """events() yields a LogEntry with source=pubsub, event_type=LoginEventStream labels."""
    stop = asyncio.Event()
    ev = make_event(replay_id=REPLAY_ID_1)
    client = FakePubSubClient({TOPIC: [ev]}, stop_after_first=stop)
    src = make_source(cfg=make_cfg(topics=[TOPIC]), client=client)
    state = FakeCheckpointStore()

    entries = await collect_n(src, state, stop, n=1)

    assert len(entries) >= 1
    entry = entries[0]
    assert entry.labels == {"source": "pubsub", "event_type": "LoginEventStream"}


@pytest.mark.asyncio
async def test_events_yields_correct_line() -> None:
    """The log line is canonical (sorted-key) JSON of the full payload."""
    stop = asyncio.Event()
    ev = make_event(replay_id=REPLAY_ID_1)
    client = FakePubSubClient({TOPIC: [ev]}, stop_after_first=stop)
    src = make_source(cfg=make_cfg(topics=[TOPIC]), client=client)
    state = FakeCheckpointStore()

    entries = await collect_n(src, state, stop, n=1)

    assert len(entries) >= 1
    parsed = json.loads(entries[0].line)
    assert parsed == ev.payload


@pytest.mark.asyncio
async def test_events_yields_correct_checkpoint() -> None:
    """Checkpoint key and value are set correctly from topic and replay_id."""
    stop = asyncio.Event()
    ev = make_event(replay_id=REPLAY_ID_1)
    client = FakePubSubClient({TOPIC: [ev]}, stop_after_first=stop)
    src = make_source(cfg=make_cfg(topics=[TOPIC]), client=client)
    state = FakeCheckpointStore()

    entries = await collect_n(src, state, stop, n=1)

    assert len(entries) >= 1
    cp = entries[0].checkpoint
    assert cp.key == f"pubsub:{TOPIC}"
    assert cp.value == base64.b64encode(REPLAY_ID_1).decode("ascii")


@pytest.mark.asyncio
async def test_events_yields_correct_timestamp() -> None:
    """Timestamp is extracted from EventDate epoch-millis."""
    stop = asyncio.Event()
    ev = make_event(replay_id=REPLAY_ID_1)
    client = FakePubSubClient({TOPIC: [ev]}, stop_after_first=stop)
    src = make_source(cfg=make_cfg(topics=[TOPIC]), client=client)
    state = FakeCheckpointStore()

    entries = await collect_n(src, state, stop, n=1)

    expected_ts = datetime.fromtimestamp(EPOCH_MS / 1000, tz=UTC)
    assert entries[0].timestamp == expected_ts


@pytest.mark.asyncio
async def test_cdc_event_timestamp_from_commit_timestamp() -> None:
    """A CDC change event (no EventDate/CreatedDate) uses ChangeEventHeader.commitTimestamp.

    This keeps a replayed CDC duplicate byte-identical (stable timestamp) so Loki's
    native dedup collapses it; otherwise it would fall back to ingest time (now()).
    """
    stop = asyncio.Event()
    cdc = DecodedEvent(
        topic="/data/AccountChangeEvent",
        replay_id=REPLAY_ID_1,
        schema_id="cdc1",
        payload={
            "Name": "Acme",
            "ChangeEventHeader": {
                "entityName": "Account",
                "changeType": "CREATE",
                "commitTimestamp": EPOCH_MS,
            },
        },
    )
    client = FakePubSubClient({"/data/AccountChangeEvent": [cdc]}, stop_after_first=stop)
    src = make_source(cfg=make_cfg(topics=["/data/AccountChangeEvent"]), client=client)
    state = FakeCheckpointStore()

    entries = await collect_n(src, state, stop, n=1)

    assert entries[0].timestamp == datetime.fromtimestamp(EPOCH_MS / 1000, tz=UTC)


@pytest.mark.asyncio
async def test_eventdate_takes_precedence_over_commit_timestamp() -> None:
    """When both EventDate and a CDC header exist, EventDate wins (top-level event time)."""
    stop = asyncio.Event()
    other_ms = EPOCH_MS + 60_000
    ev = DecodedEvent(
        topic=TOPIC,
        replay_id=REPLAY_ID_1,
        schema_id="s",
        payload={
            "EventDate": EPOCH_MS,
            "ChangeEventHeader": {"commitTimestamp": other_ms},
        },
    )
    client = FakePubSubClient({TOPIC: [ev]}, stop_after_first=stop)
    src = make_source(cfg=make_cfg(topics=[TOPIC]), client=client)
    state = FakeCheckpointStore()

    entries = await collect_n(src, state, stop, n=1)

    assert entries[0].timestamp == datetime.fromtimestamp(EPOCH_MS / 1000, tz=UTC)


# ---------------------------------------------------------------------------
# Structured metadata tests
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_structured_metadata_promotion() -> None:
    """sm_fields causes UserId to land in structured_metadata, not a label."""
    stop = asyncio.Event()
    ev = make_event(user_id="005xxx")
    client = FakePubSubClient({TOPIC: [ev]}, stop_after_first=stop)
    src = make_source(cfg=make_cfg(topics=[TOPIC]), client=client, sm_fields=["UserId"])
    state = FakeCheckpointStore()

    entries = await collect_n(src, state, stop, n=1)

    assert len(entries) >= 1
    entry = entries[0]
    assert entry.structured_metadata.get("UserId") == "005xxx"
    assert "UserId" not in entry.labels


# ---------------------------------------------------------------------------
# Replay / resume tests
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_replay_resume_uses_stored_checkpoint() -> None:
    """When state has a stored replay_id, subscribe is called with CUSTOM preset + that id."""
    stored_b64 = base64.b64encode(STORED_REPLAY_ID).decode("ascii")
    state = FakeCheckpointStore({f"pubsub:{TOPIC}": stored_b64})
    stop = asyncio.Event()
    client = FakePubSubClient({TOPIC: [make_event()]}, stop_after_first=stop)
    src = make_source(cfg=make_cfg(topics=[TOPIC]), client=client)

    await collect_n(src, state, stop, n=1)

    assert len(client.calls) >= 1
    call = client.calls[0]
    assert call["replay_preset"] == preset_for("CUSTOM")
    assert call["replay_id"] == STORED_REPLAY_ID


@pytest.mark.asyncio
async def test_replay_fallback_when_custom_preset_no_stored_id() -> None:
    """When replay_preset=CUSTOM but no stored id, falls back to LATEST."""
    cfg = make_cfg(topics=[TOPIC], replay_preset="CUSTOM")
    state = FakeCheckpointStore()  # empty - no stored id
    stop = asyncio.Event()
    client = FakePubSubClient({TOPIC: [make_event()]}, stop_after_first=stop)
    src = make_source(cfg=cfg, client=client)

    await collect_n(src, state, stop, n=1)

    assert len(client.calls) >= 1
    call = client.calls[0]
    assert call["replay_preset"] == preset_for("LATEST")
    assert call["replay_id"] == b""


@pytest.mark.asyncio
async def test_replay_earliest_preset_no_stored_id() -> None:
    """When replay_preset=EARLIEST and no stored id, uses EARLIEST."""
    cfg = make_cfg(topics=[TOPIC], replay_preset="EARLIEST")
    state = FakeCheckpointStore()
    stop = asyncio.Event()
    client = FakePubSubClient({TOPIC: [make_event()]}, stop_after_first=stop)
    src = make_source(cfg=cfg, client=client)

    await collect_n(src, state, stop, n=1)

    assert len(client.calls) >= 1
    call = client.calls[0]
    assert call["replay_preset"] == preset_for("EARLIEST")


# ---------------------------------------------------------------------------
# Termination / backpressure tests
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_events_terminates_after_finite_stream() -> None:
    """A finite fake stream: stop set after all expected events are yielded."""
    events = [make_event(replay_id=bytes([i])) for i in range(5)]
    stop = asyncio.Event()
    client = FakePubSubClient({TOPIC: events}, stop_after_first=stop)
    src = make_source(cfg=make_cfg(topics=[TOPIC]), client=client)
    state = FakeCheckpointStore()

    entries = await collect_n(src, state, stop, n=5)
    assert len(entries) == 5


@pytest.mark.asyncio
async def test_events_terminates_multiple_topics() -> None:
    """Events from multiple topics all yield and then terminate cleanly."""
    topic2 = "/event/LogoutEventStream"
    stop = asyncio.Event()

    events1 = [make_event(topic=TOPIC, replay_id=b"\x01")]
    events2 = [make_event(topic=topic2, replay_id=b"\x02")]
    # Do NOT use stop_after_first here — both topics need to yield before stop is set.
    client = FakePubSubClient({TOPIC: events1, topic2: events2})
    cfg = make_cfg(topics=[TOPIC, topic2])
    src = PubSubSource(cfg, client, sm_fields=[], reconnect_backoff=0.01)
    state = FakeCheckpointStore()

    # Collect exactly 2 entries (one per topic) then signal stop.
    entries = await collect_n(src, state, stop, n=2)
    assert len(entries) == 2


@pytest.mark.asyncio
async def test_events_no_topics_terminates_immediately() -> None:
    """When resolve_topics returns [], events() terminates without yielding."""
    cfg = make_cfg(topics=[], include=["*"])
    stop = asyncio.Event()
    client = FakePubSubClient({}, stop_after_first=stop)
    src = make_source(cfg=cfg, client=client)
    state = FakeCheckpointStore()

    entries: list = []

    async def consume() -> None:
        async for entry in src.events(state, stop):
            entries.append(entry)

    await asyncio.wait_for(consume(), timeout=5.0)
    assert entries == []


@pytest.mark.asyncio
async def test_stop_event_causes_prompt_return() -> None:
    """Setting stop while events() is active causes it to return promptly."""

    async def infinite_subscribe(
        topic: str,
        *,
        replay_preset: int,
        replay_id: bytes = b"",
        num_requested: int | None = None,
    ) -> AsyncIterator[DecodedEvent]:
        i = 0
        while True:
            yield make_event(replay_id=bytes([i % 256]))
            i += 1
            await asyncio.sleep(0)  # yield control

    class InfiniteClient:
        subscribe = staticmethod(infinite_subscribe)

        async def aclose(self) -> None:
            pass

    src = PubSubSource(
        make_cfg(topics=[TOPIC]),
        InfiniteClient(),  # type: ignore[arg-type]
        sm_fields=[],
        reconnect_backoff=0.01,
    )
    state = FakeCheckpointStore()
    stop = asyncio.Event()

    count = 0

    async def consume() -> None:
        nonlocal count
        async for _ in src.events(state, stop):
            count += 1
            if count >= 3:
                stop.set()

    await asyncio.wait_for(consume(), timeout=5.0)
    assert count >= 3


# ---------------------------------------------------------------------------
# name attribute test
# ---------------------------------------------------------------------------


def test_source_name() -> None:
    """PubSubSource.name is 'pubsub'."""
    src = make_source()
    assert src.name == "pubsub"


# ---------------------------------------------------------------------------
# Metrics wiring
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_reconnect_increments_metric() -> None:
    """Each reconnect (not the initial connection) increments pubsub_reconnects."""
    stop = asyncio.Event()
    # No stop_after_first: the fake client keeps returning empty streams after the
    # first batch, forcing _run_topic to reconnect repeatedly until stop is set.
    client = FakePubSubClient({TOPIC: [make_event()]})
    metrics = Metrics()
    src = make_source(cfg=make_cfg(topics=[TOPIC]), client=client, metrics=metrics)
    state = FakeCheckpointStore()

    entries: list = []

    async def consume() -> None:
        async for entry in src.events(state, stop):
            entries.append(entry)

    task = asyncio.create_task(consume())
    await asyncio.sleep(0.2)  # let several reconnect cycles elapse (backoff stays ~0.01s)
    stop.set()
    await asyncio.wait_for(task, timeout=5.0)

    val = metrics.registry.get_sample_value("sf2loki_pubsub_reconnects_total", {"topic": TOPIC})
    assert val is not None and val >= 1.0


@pytest.mark.asyncio
async def test_stream_error_is_logged() -> None:
    """A subscribe error is logged (previously swallowed silently, hiding failures)."""

    class RaisingClient:
        async def subscribe(
            self,
            topic: str,
            *,
            replay_preset: int,
            replay_id: bytes = b"",
            num_requested: int | None = None,
        ) -> AsyncIterator[DecodedEvent]:
            if False:  # make this an async generator
                yield  # pragma: no cover
            raise RuntimeError("boom-subscribe")

        async def aclose(self) -> None:
            pass

    stop = asyncio.Event()
    src = PubSubSource(
        make_cfg(topics=[TOPIC]), RaisingClient(), sm_fields=[], reconnect_backoff=0.01
    )
    state = FakeCheckpointStore()

    with structlog.testing.capture_logs() as captured:

        async def consume() -> None:
            async for _ in src.events(state, stop):
                pass

        task = asyncio.create_task(consume())
        await asyncio.sleep(0.1)
        stop.set()
        await asyncio.wait_for(task, timeout=5.0)

    errors = [e for e in captured if e["event"] == "pubsub stream error"]
    assert errors, "subscribe failure was not logged"
    assert "boom-subscribe" in errors[0]["error"]


# ---------------------------------------------------------------------------
# Replay-id self-healing on INVALID_ARGUMENT (B1)
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_invalid_replay_id_falls_back_to_earliest() -> None:
    """INVALID_ARGUMENT while resuming from a stored replay id → resubscribe EARLIEST.

    Salesforce rejects an expired/corrupt CUSTOM replay id with INVALID_ARGUMENT;
    retrying with the same dead id forever is a permanent silent outage. The
    source must discard the id, count a fallback, log ERROR, and go EARLIEST
    (bounded duplicates, deduped by Loki) — never LATEST (guaranteed loss).
    """
    stored_b64 = base64.b64encode(STORED_REPLAY_ID).decode("ascii")
    state = FakeCheckpointStore({f"pubsub:{TOPIC}": stored_b64})
    stop = asyncio.Event()
    client = ScriptedClient(
        [
            _rpc_error(grpc.StatusCode.INVALID_ARGUMENT),
            [make_event(replay_id=REPLAY_ID_1)],
        ]
    )
    metrics = Metrics()
    src = PubSubSource(
        make_cfg(topics=[TOPIC]),
        client,  # type: ignore[arg-type]
        sm_fields=[],
        reconnect_backoff=0.01,
        metrics=metrics,
    )

    with structlog.testing.capture_logs() as captured:
        entries = await collect_n(src, state, stop, n=1)

    assert len(entries) == 1
    assert len(client.calls) >= 2
    assert client.calls[0]["replay_preset"] == preset_for("CUSTOM")
    assert client.calls[0]["replay_id"] == STORED_REPLAY_ID
    assert client.calls[1]["replay_preset"] == preset_for("EARLIEST")
    assert client.calls[1]["replay_id"] == b""

    val = metrics.registry.get_sample_value(
        "sf2loki_pubsub_replay_fallbacks_total", {"topic": TOPIC}
    )
    assert val == 1.0

    errors = [e for e in captured if e["log_level"] == "error"]
    assert errors, "replay fallback was not logged at ERROR"
    assert "data gap" in errors[0]["event"]


@pytest.mark.asyncio
async def test_invalid_argument_without_custom_position_retries_loudly() -> None:
    """INVALID_ARGUMENT while NOT resuming from a replay id = config error.

    No replay fallback (there is no id to discard); every attempt logs at
    ERROR so the loop is permanently red and diagnosable, not silent.
    """
    state = FakeCheckpointStore()  # no stored id → LATEST
    stop = asyncio.Event()
    third_call = asyncio.Event()
    client = ScriptedClient(
        [_rpc_error(grpc.StatusCode.INVALID_ARGUMENT)] * 5,
        on_call={2: third_call},
    )
    metrics = Metrics()
    src = PubSubSource(
        make_cfg(topics=[TOPIC]),
        client,  # type: ignore[arg-type]
        sm_fields=[],
        reconnect_backoff=0.01,
        metrics=metrics,
    )

    entries: list = []

    async def consume() -> None:
        async for entry in src.events(state, stop):
            entries.append(entry)

    with structlog.testing.capture_logs() as captured:
        task = asyncio.create_task(consume())
        await asyncio.wait_for(third_call.wait(), timeout=5.0)
        stop.set()
        await asyncio.wait_for(task, timeout=5.0)

    errors = [e for e in captured if e["log_level"] == "error"]
    assert len(errors) >= 2, "config-error INVALID_ARGUMENT must log ERROR on every attempt"
    for call in client.calls:
        assert call["replay_preset"] == preset_for("LATEST")
    assert (
        metrics.registry.get_sample_value("sf2loki_pubsub_replay_fallbacks_total", {"topic": TOPIC})
        is None
    )


# ---------------------------------------------------------------------------
# Keepalive checkpointing (B2)
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_keepalive_enqueues_checkpoint_only_entry() -> None:
    """A keepalive's latest_replay_id is enqueued as a checkpoint_only LogEntry.

    Routed through the queue (not committed directly) so the pipeline's
    commit-after-push ordering is preserved.
    """
    stop = asyncio.Event()
    ka = KeepaliveEvent(topic=TOPIC, latest_replay_id=b"\x00\x05")
    client = ScriptedClient([[ka]])
    src = PubSubSource(
        make_cfg(topics=[TOPIC]),
        client,  # type: ignore[arg-type]
        sm_fields=[],
        reconnect_backoff=0.01,
    )
    state = FakeCheckpointStore()

    entries = await collect_n(src, state, stop, n=1)

    assert len(entries) == 1
    entry = entries[0]
    assert entry.checkpoint_only is True
    assert entry.line == ""
    assert dict(entry.labels) == {}
    assert dict(entry.structured_metadata) == {}
    assert entry.checkpoint.key == f"pubsub:{TOPIC}"
    assert entry.checkpoint.value == base64.b64encode(b"\x00\x05").decode("ascii")


@pytest.mark.asyncio
async def test_keepalive_with_unchanged_replay_id_is_not_reemitted() -> None:
    """Repeated keepalives with the same latest_replay_id emit only one entry."""
    stop = asyncio.Event()
    ka5 = KeepaliveEvent(topic=TOPIC, latest_replay_id=b"\x00\x05")
    ka5_dup = KeepaliveEvent(topic=TOPIC, latest_replay_id=b"\x00\x05")
    ka6 = KeepaliveEvent(topic=TOPIC, latest_replay_id=b"\x00\x06")
    client = ScriptedClient([[ka5, ka5_dup, ka6]])
    src = PubSubSource(
        make_cfg(topics=[TOPIC]),
        client,  # type: ignore[arg-type]
        sm_fields=[],
        reconnect_backoff=0.01,
    )
    state = FakeCheckpointStore()

    entries = await collect_n(src, state, stop, n=2)

    assert len(entries) == 2
    values = [e.checkpoint.value for e in entries]
    assert values == [
        base64.b64encode(b"\x00\x05").decode("ascii"),
        base64.b64encode(b"\x00\x06").decode("ascii"),
    ]


@pytest.mark.asyncio
async def test_reconnect_resumes_from_keepalive_replay_id() -> None:
    """After a keepalive, a reconnect subscribes CUSTOM from the keepalive's id."""
    stop = asyncio.Event()
    second_call = asyncio.Event()
    ka = KeepaliveEvent(topic=TOPIC, latest_replay_id=b"\x00\x05")
    client = ScriptedClient([[ka]], on_call={1: second_call})
    src = PubSubSource(
        make_cfg(topics=[TOPIC]),
        client,  # type: ignore[arg-type]
        sm_fields=[],
        reconnect_backoff=0.01,
    )
    state = FakeCheckpointStore()

    async def consume() -> None:
        async for _ in src.events(state, stop):
            pass

    task = asyncio.create_task(consume())
    await asyncio.wait_for(second_call.wait(), timeout=5.0)
    stop.set()
    await asyncio.wait_for(task, timeout=5.0)

    assert client.calls[1]["replay_preset"] == preset_for("CUSTOM")
    assert client.calls[1]["replay_id"] == b"\x00\x05"


# ---------------------------------------------------------------------------
# Backoff reset + jitter (B5)
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_backoff_resets_after_healthy_connection() -> None:
    """Backoff returns to base once a connection delivers an event.

    Without the reset, the known session-expiry churn (a failure every few
    minutes) ratchets every topic to max_backoff permanently.
    """
    base = 0.01
    stop = asyncio.Event()
    fifth_call = asyncio.Event()
    err = _rpc_error(grpc.StatusCode.UNAVAILABLE)
    # Attempts 1-3 fail immediately; attempt 4 delivers an event (healthy) then
    # fails mid-stream; attempt 5 signals the test to stop.
    client = ScriptedClient(
        [err, err, err, [make_event(replay_id=REPLAY_ID_1), err]],
        on_call={4: fifth_call},
    )
    src = PubSubSource(
        make_cfg(topics=[TOPIC]),
        client,  # type: ignore[arg-type]
        sm_fields=[],
        reconnect_backoff=base,
        max_backoff=30.0,
    )
    state = FakeCheckpointStore()

    async def consume() -> None:
        async for _ in src.events(state, stop):
            pass

    with structlog.testing.capture_logs() as captured:
        task = asyncio.create_task(consume())
        await asyncio.wait_for(fifth_call.wait(), timeout=5.0)
        stop.set()
        await asyncio.wait_for(task, timeout=5.0)

    warnings = [e for e in captured if e["event"] == "pubsub stream error"]
    assert len(warnings) >= 4
    # Attempts 1-3 ratchet the backoff; attempt 4 received an event first, so
    # its failure must be logged with the backoff reset to base.
    assert warnings[2]["backoff"] > base
    assert warnings[3]["backoff"] == pytest.approx(base)


def test_jitter_bounds() -> None:
    """_jitter returns a uniform value in [0, backoff/2]."""
    for _ in range(200):
        j = _jitter(10.0)
        assert 0.0 <= j <= 5.0


@pytest.mark.asyncio
async def test_reconnect_sleep_applies_jitter(monkeypatch: pytest.MonkeyPatch) -> None:
    """Reconnect sleeps include jitter so topics don't reconnect in lockstep."""
    jitter_calls: list[float] = []

    def fake_jitter(backoff: float) -> float:
        jitter_calls.append(backoff)
        return 0.0

    monkeypatch.setattr("sf2loki.sources.pubsub_source._jitter", fake_jitter)

    stop = asyncio.Event()
    second_call = asyncio.Event()
    client = ScriptedClient(
        [_rpc_error(grpc.StatusCode.UNAVAILABLE)],
        on_call={1: second_call},
    )
    src = PubSubSource(
        make_cfg(topics=[TOPIC]),
        client,  # type: ignore[arg-type]
        sm_fields=[],
        reconnect_backoff=0.01,
    )
    state = FakeCheckpointStore()

    async def consume() -> None:
        async for _ in src.events(state, stop):
            pass

    task = asyncio.create_task(consume())
    await asyncio.wait_for(second_call.wait(), timeout=5.0)
    stop.set()
    await asyncio.wait_for(task, timeout=5.0)

    assert jitter_calls, "reconnect sleep did not apply jitter"


# ---------------------------------------------------------------------------
# Sentinel put on cancelled shutdown (B7)
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_cancelled_topic_task_with_full_queue_does_not_hang() -> None:
    """Force-cancelling _run_topic with a full queue must not hang on the sentinel.

    When tasks are force-cancelled the consumer is gone; an awaited put on a
    full queue that nobody drains would block forever.
    """
    stop = asyncio.Event()
    hang = asyncio.Event()

    class HangingClient:
        async def subscribe(
            self,
            topic: str,
            *,
            replay_preset: int,
            replay_id: bytes = b"",
            num_requested: int | None = None,
        ) -> AsyncIterator[DecodedEvent]:
            if False:  # make this an async generator
                yield  # pragma: no cover
            await hang.wait()

        async def aclose(self) -> None:
            pass

    src = PubSubSource(
        make_cfg(topics=[TOPIC]),
        HangingClient(),  # type: ignore[arg-type]
        sm_fields=[],
        reconnect_backoff=0.01,
    )
    state = FakeCheckpointStore()
    queue: asyncio.Queue = asyncio.Queue(maxsize=1)
    queue.put_nowait(None)  # fill the queue so a sentinel put would block

    task = asyncio.create_task(src._run_topic(TOPIC, state, stop, queue))
    await asyncio.sleep(0.05)  # let the task reach the subscribe await
    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await asyncio.wait_for(task, timeout=1.0)


# ---------------------------------------------------------------------------
# Topic discovery / "*" wildcard


class FakeStreamDiscoverer:
    def __init__(self, topics: list[str], *, error: bool = False, fail_times: int = 0) -> None:
        self.topics = topics
        self.error = error
        self.fail_times = fail_times
        self.calls = 0

    async def list_event_stream_topics(self) -> list[str]:
        self.calls += 1
        if self.error or self.calls <= self.fail_times:
            raise RuntimeError("discovery down")
        return list(self.topics)


def _resolver_src(topics, *, include=None, exclude=None, discoverer=None) -> PubSubSource:
    cfg = PubSubConfig(topics=topics, include=include or ["/event/*"], exclude=exclude or [])
    return PubSubSource(
        cfg,
        object(),  # type: ignore[arg-type]
        sm_fields=[],
        topic_discoverer=discoverer,
        reconnect_backoff=0.01,  # keep discovery-retry sleeps short in tests
    )


@pytest.mark.asyncio
async def test_wildcard_discovers_and_merges_topics() -> None:
    disc = FakeStreamDiscoverer(["/event/ApiEventStream", "/event/LoginEventStream"])
    src = _resolver_src(["*", "/event/LoginEventStream"], discoverer=disc)
    topics = await src._resolve_topics()
    assert disc.calls == 1
    assert sorted(topics) == ["/event/ApiEventStream", "/event/LoginEventStream"]


@pytest.mark.asyncio
async def test_wildcard_respects_exclude_globs() -> None:
    disc = FakeStreamDiscoverer(["/event/ApiEventStream", "/event/LoginEventStream"])
    src = _resolver_src(["*"], exclude=["*ApiEventStream"], discoverer=disc)
    assert await src._resolve_topics() == ["/event/LoginEventStream"]


@pytest.mark.asyncio
async def test_no_wildcard_skips_discovery() -> None:
    disc = FakeStreamDiscoverer(["/event/ApiEventStream"])
    src = _resolver_src(["/event/LoginEventStream"], discoverer=disc)
    assert await src._resolve_topics() == ["/event/LoginEventStream"]
    assert disc.calls == 0


@pytest.mark.asyncio
async def test_discovery_failure_retries_then_falls_back_to_explicit() -> None:
    """A persistently failing discovery is retried, then falls back with an ERROR log.

    One transient failure must not silently reduce the subscription set for
    the process lifetime (the old behavior tried once and warned).
    """
    disc = FakeStreamDiscoverer([], error=True)
    src = _resolver_src(["*", "/event/LoginEventStream"], discoverer=disc)
    with structlog.testing.capture_logs() as captured:
        assert await src._resolve_topics() == ["/event/LoginEventStream"]
    assert disc.calls == 3, "discovery must be retried before giving up"
    errors = [e for e in captured if e["log_level"] == "error"]
    assert errors, "final discovery failure must be logged at ERROR"


@pytest.mark.asyncio
async def test_discovery_transient_failure_retries_then_succeeds() -> None:
    disc = FakeStreamDiscoverer(["/event/ApiEventStream"], fail_times=2)
    src = _resolver_src(["*", "/event/LoginEventStream"], discoverer=disc)
    topics = await src._resolve_topics()
    assert disc.calls == 3
    assert sorted(topics) == ["/event/ApiEventStream", "/event/LoginEventStream"]


def test_resolve_topics_sync_excludes_wildcard_marker() -> None:
    src = _resolver_src(["*", "/event/LoginEventStream"])
    assert src.resolve_topics() == ["/event/LoginEventStream"]
