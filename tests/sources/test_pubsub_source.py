"""Tests for PubSubSource — async generator source consuming Salesforce Pub/Sub API."""

from __future__ import annotations

import asyncio
import base64
import json
from collections.abc import AsyncIterator
from datetime import UTC, datetime

import pytest

from sf2loki.config import PubSubConfig
from sf2loki.obs.metrics import Metrics
from sf2loki.salesforce.pubsub_client import DecodedEvent, preset_for
from sf2loki.sources.pubsub_source import PubSubSource

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
