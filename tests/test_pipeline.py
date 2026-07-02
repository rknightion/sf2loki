"""Tests for the Pipeline batching / commit / retry / drop behaviour."""

from __future__ import annotations

import asyncio
import contextlib
from collections.abc import AsyncIterator, Awaitable, Callable
from datetime import UTC, datetime, timedelta

import pytest
import structlog.testing

from sf2loki import app as app_module
from sf2loki.app import Pipeline
from sf2loki.config import LokiBatchConfig
from sf2loki.model import Batch, CheckpointToken, LogEntry
from sf2loki.obs.metrics import Metrics
from sf2loki.sinks.base import PermanentSinkError, RetryableSinkError

# The `wait_until` fixture (tests/conftest.py): bounded poll-for-condition,
# used throughout this file in place of a fixed `asyncio.sleep(N)` wherever
# the sleep was really "wait for the other task to reach some state" rather
# than an assertion about a real elapsed duration.
WaitUntil = Callable[..., Awaitable[None]]


def _entry(key: str, value: str, line: str = "{}") -> LogEntry:
    return LogEntry(
        timestamp=datetime.now(UTC),
        labels={"source": "test", "event_type": "Thing"},
        line=line,
        structured_metadata={},
        checkpoint=CheckpointToken(key=key, value=value),
    )


def _keepalive(key: str, value: str) -> LogEntry:
    """A checkpoint-only entry (e.g. a Pub/Sub keepalive latest_replay_id)."""
    return LogEntry(
        timestamp=datetime.now(UTC),
        labels={},
        line="",
        structured_metadata={},
        checkpoint=CheckpointToken(key=key, value=value),
        checkpoint_only=True,
    )


class FakeSource:
    name = "test"

    def __init__(self, entries: list[LogEntry], *, block: asyncio.Event | None = None) -> None:
        self._entries = entries
        self._block = block

    async def events(self, state: object, stop: asyncio.Event) -> AsyncIterator[LogEntry]:
        for e in self._entries:
            yield e
        if self._block is not None:
            await self._block.wait()


class FakeSink:
    def __init__(self, *, fail_times: int = 0, permanent: bool = False) -> None:
        self.pushed: list[Batch] = []
        self._fail_times = fail_times
        self._permanent = permanent
        self.attempts = 0

    async def push(self, batch: Batch) -> None:
        self.attempts += 1
        if self._permanent:
            raise PermanentSinkError("nope")
        if self._fail_times > 0:
            self._fail_times -= 1
            raise RetryableSinkError("transient")
        self.pushed.append(batch)

    async def aclose(self) -> None:
        return None


class FakeState:
    def __init__(self) -> None:
        self.committed: dict[str, str] = {}

    async def load(self, key: str) -> str | None:
        return self.committed.get(key)

    async def commit(self, key: str, value: str) -> None:
        self.committed[key] = value


def _batch_cfg(**kw: object) -> LokiBatchConfig:
    base: dict[str, object] = {
        "max_entries": 100,
        "max_bytes": 10**9,
        "flush_interval": timedelta(seconds=0.05),
    }
    base.update(kw)
    return LokiBatchConfig(**base)  # type: ignore[arg-type]


def _pipeline(source: FakeSource, sink: FakeSink, state: FakeState, **kw: object) -> Pipeline:
    return Pipeline(
        sources=[source],
        sink=sink,
        state=state,
        batch=_batch_cfg(**kw),
        metrics=Metrics(),
    )


async def test_happy_path_pushes_and_commits() -> None:
    src = FakeSource([_entry("pubsub:/event/X", "v1"), _entry("pubsub:/event/X", "v2")])
    sink = FakeSink()
    state = FakeState()
    pipe = _pipeline(src, sink, state, max_entries=2)

    await asyncio.wait_for(pipe.run(asyncio.Event()), timeout=2)

    assert sum(len(b.entries) for b in sink.pushed) == 2
    # Last token per key wins.
    assert state.committed == {"pubsub:/event/X": "v2"}


async def test_static_labels_injected() -> None:
    src = FakeSource([_entry("k", "v")])
    sink = FakeSink()
    state = FakeState()
    pipe = _pipeline(src, sink, state)
    pipe.set_static_labels({"job": "sf2loki", "sf_org_id": "00D", "environment": "prod"})

    await asyncio.wait_for(pipe.run(asyncio.Event()), timeout=2)

    (batch,) = sink.pushed
    labels = batch.entries[0].labels
    assert labels["job"] == "sf2loki"
    assert labels["sf_org_id"] == "00D"
    assert labels["environment"] == "prod"
    assert labels["source"] == "test"  # source-set labels preserved


async def test_permanent_error_drops_and_advances() -> None:
    src = FakeSource([_entry("k", "v")])
    sink = FakeSink(permanent=True)
    state = FakeState()
    pipe = _pipeline(src, sink, state)

    await asyncio.wait_for(pipe.run(asyncio.Event()), timeout=2)

    assert sink.pushed == []  # nothing successfully pushed
    assert state.committed == {"k": "v"}  # but the checkpoint advanced past the poison batch


async def test_permanent_error_counts_all_dropped_entries() -> None:
    # A permanent error on a multi-entry batch drops every entry; the per-entry
    # drop counter must reflect N (with the error's reason), not 1.
    src = FakeSource([_entry("k", "1"), _entry("k", "2"), _entry("k", "3")])
    sink = FakeSink(permanent=True)
    state = FakeState()
    metrics = Metrics()
    pipe = Pipeline(
        sources=[src], sink=sink, state=state, batch=_batch_cfg(max_entries=10), metrics=metrics
    )

    await asyncio.wait_for(pipe.run(asyncio.Event()), timeout=2)

    dropped = metrics.registry.get_sample_value(
        "sf2loki_loki_entries_dropped_total", {"reason": "permanent"}
    )
    assert dropped == 3.0
    # loki_push{outcome} counts batch outcomes only — no "dropped" outcome anymore.
    assert (
        metrics.registry.get_sample_value("sf2loki_loki_push_total", {"outcome": "dropped"}) is None
    )


async def test_permanent_drop_is_logged_with_count_and_reason() -> None:
    src = FakeSource([_entry("k", "1"), _entry("k", "2")])
    sink = FakeSink(permanent=True)
    state = FakeState()
    pipe = _pipeline(src, sink, state, max_entries=10)

    with structlog.testing.capture_logs() as captured:
        await asyncio.wait_for(pipe.run(asyncio.Event()), timeout=2)

    drops = [e for e in captured if e["log_level"] == "error" and e.get("entries") == 2]
    assert drops, f"no ERROR drop log with entry count; got {captured}"
    assert drops[0]["reason"] == "permanent"


async def test_retryable_then_success(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(app_module, "_RETRY_BACKOFF_BASE", 0.0)
    src = FakeSource([_entry("k", "v")])
    sink = FakeSink(fail_times=2)
    state = FakeState()
    pipe = _pipeline(src, sink, state)

    await asyncio.wait_for(pipe.run(asyncio.Event()), timeout=2)

    assert sink.attempts == 3  # two failures + one success
    assert len(sink.pushed) == 1
    assert state.committed == {"k": "v"}


async def test_flush_by_interval_before_source_finishes(wait_until: WaitUntil) -> None:
    block = asyncio.Event()
    src = FakeSource([_entry("k", "v")], block=block)
    sink = FakeSink()
    state = FakeState()
    pipe = _pipeline(src, sink, state, max_entries=1000)  # never hit size threshold

    task = asyncio.create_task(pipe.run(asyncio.Event()))
    # Wait for the consumer to hit the flush_interval (0.05s) while the source is blocked.
    await wait_until(lambda: len(sink.pushed) == 1)
    assert len(sink.pushed) == 1  # flushed by interval, not by source completion

    block.set()
    await asyncio.wait_for(task, timeout=2)


async def test_multiple_sources_all_drained() -> None:
    s1 = FakeSource([_entry("a", "1")])
    s2 = FakeSource([_entry("b", "2")])
    sink = FakeSink()
    state = FakeState()
    pipe = Pipeline(
        sources=[s1, s2], sink=sink, state=state, batch=_batch_cfg(max_entries=1), metrics=Metrics()
    )

    await asyncio.wait_for(pipe.run(asyncio.Event()), timeout=2)

    assert state.committed == {"a": "1", "b": "2"}


async def test_no_sources_returns_immediately() -> None:
    pipe = Pipeline(
        sources=[], sink=FakeSink(), state=FakeState(), batch=_batch_cfg(), metrics=Metrics()
    )
    await asyncio.wait_for(pipe.run(asyncio.Event()), timeout=2)


# --- Commit metrics (last_replay_commit_ts / watermark_ts) ------------------


async def test_commit_updates_last_replay_commit_ts_for_pubsub_key() -> None:
    src = FakeSource([_entry("pubsub:/event/LoginEventStream", "AAEC")])
    sink = FakeSink()
    state = FakeState()
    metrics = Metrics()
    pipe = Pipeline(sources=[src], sink=sink, state=state, batch=_batch_cfg(), metrics=metrics)

    await asyncio.wait_for(pipe.run(asyncio.Event()), timeout=2)

    val = metrics.registry.get_sample_value(
        "sf2loki_last_replay_commit_timestamp_seconds",
        {"topic": "/event/LoginEventStream"},
    )
    assert val is not None and val > 0.0


async def test_commit_updates_watermark_ts_for_eventlog_objects_key() -> None:
    src = FakeSource([_entry("eventlog_objects:LoginEvent", "2024-06-01T12:00:00.000Z")])
    sink = FakeSink()
    state = FakeState()
    metrics = Metrics()
    pipe = Pipeline(sources=[src], sink=sink, state=state, batch=_batch_cfg(), metrics=metrics)

    await asyncio.wait_for(pipe.run(asyncio.Event()), timeout=2)

    val = metrics.registry.get_sample_value(
        "sf2loki_watermark_timestamp_seconds",
        {"source": "eventlog_objects", "object": "LoginEvent"},
    )
    expected = datetime(2024, 6, 1, 12, 0, 0, tzinfo=UTC).timestamp()
    assert val is not None and abs(val - expected) < 1.0


async def test_flush_retry_loop_is_stop_aware(
    monkeypatch: pytest.MonkeyPatch, wait_until: WaitUntil
) -> None:
    """Stop fired mid-backoff aborts the retry loop promptly (no full backoff wait)."""
    monkeypatch.setattr(app_module, "_RETRY_BACKOFF_BASE", 5.0)
    src = FakeSource([_entry("k", "v")])
    sink = FakeSink(fail_times=10**6)  # always retryable-fails
    state = FakeState()
    pipe = _pipeline(src, sink, state)

    stop = asyncio.Event()
    task = asyncio.create_task(pipe.run(stop))
    await wait_until(lambda: sink.attempts >= 1)  # first push attempt failed, now in backoff
    stop.set()
    await asyncio.wait_for(task, timeout=0.5)  # would be ~5s without the fix

    # Batch was abandoned uncommitted (not dropped) — it will be retried after restart.
    assert state.committed == {}


async def test_commit_updates_watermark_ts_for_eventlogfile_key() -> None:
    import json

    val_json = json.dumps({"last_created": "2024-06-01T12:00:00.000Z", "ids": ["0AT1"]})
    src = FakeSource([_entry("eventlogfile:Login", val_json)])
    sink = FakeSink()
    state = FakeState()
    metrics = Metrics()
    pipe = Pipeline(sources=[src], sink=sink, state=state, batch=_batch_cfg(), metrics=metrics)

    await asyncio.wait_for(pipe.run(asyncio.Event()), timeout=2)

    val = metrics.registry.get_sample_value(
        "sf2loki_watermark_timestamp_seconds",
        {"source": "eventlogfile", "object": "Login"},
    )
    expected = datetime(2024, 6, 1, 12, 0, 0, tzinfo=UTC).timestamp()
    assert val is not None and abs(val - expected) < 1.0


async def test_commit_eventlogfile_watermark_falls_back_on_bad_json() -> None:
    src = FakeSource([_entry("eventlogfile:Login", "not-json")])
    sink = FakeSink()
    state = FakeState()
    metrics = Metrics()
    pipe = Pipeline(sources=[src], sink=sink, state=state, batch=_batch_cfg(), metrics=metrics)

    await asyncio.wait_for(pipe.run(asyncio.Event()), timeout=2)

    val = metrics.registry.get_sample_value(
        "sf2loki_watermark_timestamp_seconds",
        {"source": "eventlogfile", "object": "Login"},
    )
    assert val is not None and val > 0.0


# --- Consumer-task death must crash Pipeline.run (A5) -----------------------


class ExplodingState(FakeState):
    """Checkpoint store whose commit fails like a full/read-only volume."""

    async def commit(self, key: str, value: str) -> None:
        raise OSError("disk full")


async def test_consumer_death_crashes_run_instead_of_hanging() -> None:
    """A fatal consumer exception (e.g. checkpoint write OSError) must propagate
    out of Pipeline.run promptly — not leave producers blocked forever."""
    block = asyncio.Event()  # producer never finishes on its own
    src = FakeSource([_entry("k", "v")], block=block)
    sink = FakeSink()
    pipe = _pipeline(src, sink, ExplodingState(), max_entries=1)

    with pytest.raises(OSError, match="disk full"):
        await asyncio.wait_for(pipe.run(asyncio.Event()), timeout=2)


# --- checkpoint_only entries (A6) --------------------------------------------


async def test_checkpoint_only_rides_batch_but_never_reaches_sink() -> None:
    """Real entry + keepalive for the same key in one flush: the sink sees only
    the real entry; the keepalive's (later) token is committed after the push."""
    src = FakeSource(
        [
            _entry("pubsub:/event/X", "v1", line="real event"),
            _keepalive("pubsub:/event/X", "v2"),
        ]
    )
    sink = FakeSink()
    state = FakeState()
    pipe = _pipeline(src, sink, state, max_entries=2)

    await asyncio.wait_for(pipe.run(asyncio.Event()), timeout=2)

    (batch,) = sink.pushed
    assert [e.line for e in batch.entries] == ["real event"]
    assert all(not e.checkpoint_only for e in batch.entries)
    assert state.committed == {"pubsub:/event/X": "v2"}  # keepalive token won, post-push


async def test_only_keepalives_flush_skips_sink_and_commits_directly() -> None:
    src = FakeSource([_keepalive("pubsub:/event/X", "v9")])
    sink = FakeSink()
    state = FakeState()
    pipe = _pipeline(src, sink, state)

    await asyncio.wait_for(pipe.run(asyncio.Event()), timeout=2)

    assert sink.attempts == 0  # sink never touched
    assert state.committed == {"pubsub:/event/X": "v9"}


async def test_keepalive_token_not_committed_when_push_abandoned(
    monkeypatch: pytest.MonkeyPatch, wait_until: WaitUntil
) -> None:
    """Commit-after-push invariant: a keepalive queued behind a real entry must
    not commit if that entry's push never succeeded."""
    monkeypatch.setattr(app_module, "_RETRY_BACKOFF_BASE", 5.0)
    src = FakeSource([_entry("k", "v1"), _keepalive("k", "v2")])
    sink = FakeSink(fail_times=10**6)
    state = FakeState()
    pipe = _pipeline(src, sink, state, max_entries=2)

    stop = asyncio.Event()
    task = asyncio.create_task(pipe.run(stop))
    await wait_until(lambda: sink.attempts >= 1)  # first push attempt failed, now in backoff
    stop.set()
    await asyncio.wait_for(task, timeout=0.5)

    assert state.committed == {}  # neither the real token nor the keepalive


async def test_keepalive_skips_static_labels_and_ingest_metrics() -> None:
    keepalive = _keepalive("pubsub:/event/X", "v1")
    src = FakeSource([keepalive])
    sink = FakeSink()
    state = FakeState()
    metrics = Metrics()
    pipe = Pipeline(sources=[src], sink=sink, state=state, batch=_batch_cfg(), metrics=metrics)
    pipe.set_static_labels({"job": "sf2loki"})

    await asyncio.wait_for(pipe.run(asyncio.Event()), timeout=2)

    assert keepalive.labels == {}  # static labels not merged
    ingested = metrics.registry.get_sample_value(
        "sf2loki_events_ingested_total", {"source": "test", "event_type": "unknown"}
    )
    assert ingested is None  # not counted as an ingested event


async def test_keepalive_commit_fires_commit_metric() -> None:
    src = FakeSource([_keepalive("pubsub:/event/LoginEventStream", "AAEC")])
    sink = FakeSink()
    state = FakeState()
    metrics = Metrics()
    pipe = Pipeline(sources=[src], sink=sink, state=state, batch=_batch_cfg(), metrics=metrics)

    await asyncio.wait_for(pipe.run(asyncio.Event()), timeout=2)

    val = metrics.registry.get_sample_value(
        "sf2loki_last_replay_commit_timestamp_seconds",
        {"topic": "/event/LoginEventStream"},
    )
    assert val is not None and val > 0.0


# --- per-lane queues: bulk cannot starve streaming (issue #53) ---------------


def _lane_entry(source: str, key: str, value: str, line: str = "{}") -> LogEntry:
    """A real entry tagged with a ``source`` label so a lane-aware sink can tell
    which lane pushed it (streaming vs bulk)."""
    return LogEntry(
        timestamp=datetime.now(UTC),
        labels={"source": source, "event_type": "Thing"},
        line=line,
        structured_metadata={},
        checkpoint=CheckpointToken(key=key, value=value),
    )


async def test_saturated_bulk_lane_does_not_starve_streaming(wait_until: WaitUntil) -> None:
    """The core #53 guarantee: while a bulk push is fully blocked, the streaming
    lane keeps pushing to completion — bulk head-of-line-blocking is impossible.

    On the old single-queue/single-consumer pipeline the consumer pulls the
    first-enqueued bulk entry, blocks on its gated push, and NO streaming entry
    is ever pushed (the shared consumer is wedged). With per-source-class lanes
    the streaming consumer drains its own queue independently.
    """
    bulk_gate = asyncio.Event()

    class LaneAwareSink:
        def __init__(self) -> None:
            self.pushed: list[Batch] = []

        async def push(self, batch: Batch) -> None:
            # Block every bulk push until the test opens the gate; stream is free.
            if batch.entries[0].labels.get("source") == "eventlogfile":
                await bulk_gate.wait()
            self.pushed.append(batch)

        async def aclose(self) -> None:
            return None

    bulk_src = FakeSource(
        [_lane_entry("eventlogfile", "eventlogfile:Login", str(i)) for i in range(50)]
    )
    bulk_src.name = "eventlogfile"
    stream_src = FakeSource([_lane_entry("pubsub", "pubsub:/event/X", str(i)) for i in range(3)])
    stream_src.name = "pubsub"

    sink = LaneAwareSink()
    state = FakeState()
    # Bulk source listed first so its producer enqueues entry 0 before the
    # consumer runs — on the old code that deterministically wedges the shared
    # consumer on the gated bulk push.
    pipe = Pipeline(
        sources=[bulk_src, stream_src],
        sink=sink,
        state=state,
        batch=_batch_cfg(max_entries=1),
        metrics=Metrics(),
    )
    stop = asyncio.Event()
    task = asyncio.create_task(pipe.run(stop))
    try:
        # All 3 streaming entries reach the sink while every bulk push is blocked.
        await wait_until(
            lambda: (
                sum(1 for b in sink.pushed for e in b.entries if e.labels.get("source") == "pubsub")
                == 3
            )
        )
        # ...and not a single bulk entry got through (the bulk lane is fully gated).
        assert not any(
            e.labels.get("source") == "eventlogfile" for b in sink.pushed for e in b.entries
        )
        # Release the gate: bulk drains and the whole run completes cleanly.
        bulk_gate.set()
        await asyncio.wait_for(task, timeout=5)
    finally:
        bulk_gate.set()
        stop.set()
        task.cancel()
        with contextlib.suppress(asyncio.CancelledError):
            await task

    assert state.committed == {"eventlogfile:Login": "49", "pubsub:/event/X": "2"}


async def test_two_lanes_both_commit_through_real_file_store(tmp_path: object) -> None:
    """Concurrent commits from two lane consumers both persist (issue #53).

    Guards the design's "no shared pipeline lock needed" conclusion: the real
    FileCheckpointStore serialises commit_many under its own asyncio.Lock, so
    two lanes committing disjoint keys never lose an update or self-conflict.
    """
    from pathlib import Path

    from sf2loki.state.file_store import FileCheckpointStore

    assert isinstance(tmp_path, Path)
    store = FileCheckpointStore(tmp_path / "state.json")
    bulk_src = FakeSource(
        [_lane_entry("eventlogfile", "eventlogfile:Login", str(i)) for i in range(20)]
    )
    bulk_src.name = "eventlogfile"
    stream_src = FakeSource([_lane_entry("pubsub", "pubsub:/event/X", str(i)) for i in range(20)])
    stream_src.name = "pubsub"
    pipe = Pipeline(
        sources=[bulk_src, stream_src],
        sink=FakeSink(),
        state=store,
        batch=_batch_cfg(max_entries=1),
        metrics=Metrics(),
    )

    await asyncio.wait_for(pipe.run(asyncio.Event()), timeout=5)

    assert await store.load("eventlogfile:Login") == "19"
    assert await store.load("pubsub:/event/X") == "19"


# --- queue_depth updated by the producer (A8) --------------------------------


async def test_producer_updates_queue_depth_gauge() -> None:
    """The gauge must reflect queue growth even while the consumer is stuck."""
    metrics = Metrics()
    pipe = Pipeline(
        sources=[], sink=FakeSink(), state=FakeState(), batch=_batch_cfg(), metrics=metrics
    )
    lane = pipe._new_lane()
    src = FakeSource([_entry("k", "1"), _entry("k", "2"), _entry("k", "3")])

    await pipe._produce(src, lane, asyncio.Event())

    # No consumer ran: the gauge was set by the producer after each put.
    assert metrics.registry.get_sample_value("sf2loki_queue_depth") == 3.0


# --- byte-aware queue bounding (issue #16) ---------------------------------


def test_queue_maxsize_defaults_from_batch_config() -> None:
    pipe = Pipeline(
        sources=[],
        sink=FakeSink(),
        state=FakeState(),
        batch=_batch_cfg(queue_maxsize=123),
        metrics=Metrics(),
    )
    assert pipe._queue_maxsize == 123


def test_explicit_queue_maxsize_overrides_batch_config() -> None:
    pipe = Pipeline(
        sources=[],
        sink=FakeSink(),
        state=FakeState(),
        batch=_batch_cfg(queue_maxsize=123),
        metrics=Metrics(),
        queue_maxsize=7,
    )
    assert pipe._queue_maxsize == 7


async def test_producer_blocks_on_byte_budget_and_resumes_as_consumer_drains(
    wait_until: WaitUntil,
) -> None:
    # cost per entry = 100-byte line + 64 overhead = 164; budget 150 admits one
    # entry at a time (admitted while under budget, blocked once at/over it).
    line = "x" * 100
    entries = [_entry("k", str(i), line=line) for i in range(3)]
    pipe = Pipeline(
        sources=[],
        sink=FakeSink(),
        state=FakeState(),
        batch=_batch_cfg(queue_max_bytes=150),
        metrics=Metrics(),
    )
    lane = pipe._new_lane()
    task = asyncio.create_task(pipe._produce(FakeSource(entries), lane, asyncio.Event()))

    await wait_until(lambda: lane.queue.qsize() == 1)  # entry 0 admitted, entry 1 blocked on bytes
    assert not task.done()

    # Drain one item the way the consumer does; the producer resumes.
    item = await lane.queue.get()
    await pipe._release(lane, item)
    await wait_until(lambda: lane.queue.qsize() == 1)  # entry 1 admitted, entry 2 blocked

    item = await lane.queue.get()
    await pipe._release(lane, item)
    item = await asyncio.wait_for(lane.queue.get(), timeout=1)
    await pipe._release(lane, item)
    sentinel = await asyncio.wait_for(lane.queue.get(), timeout=1)
    assert not isinstance(sentinel, LogEntry)
    await asyncio.wait_for(task, timeout=1)


async def test_single_oversized_entry_still_admitted() -> None:
    # An entry whose cost alone exceeds the whole budget must still flow
    # end-to-end (never deadlock the pipeline).
    src = FakeSource([_entry("k", "v", line="y" * 10_000)])
    sink = FakeSink()
    state = FakeState()
    pipe = Pipeline(
        sources=[src],
        sink=sink,
        state=state,
        batch=_batch_cfg(queue_max_bytes=100),
        metrics=Metrics(),
    )

    await asyncio.wait_for(pipe.run(asyncio.Event()), timeout=2)

    assert sum(len(b.entries) for b in sink.pushed) == 1
    assert state.committed == {"k": "v"}


async def test_count_bound_still_enforced_when_byte_accounting_disabled(
    wait_until: WaitUntil,
) -> None:
    entries = [_entry("k", str(i)) for i in range(105)]
    pipe = Pipeline(
        sources=[],
        sink=FakeSink(),
        state=FakeState(),
        batch=_batch_cfg(queue_maxsize=100, queue_max_bytes=0),
        metrics=Metrics(),
    )
    lane = pipe._new_lane()  # queue maxsize == pipe._queue_maxsize (100)
    task = asyncio.create_task(pipe._produce(FakeSource(entries), lane, asyncio.Event()))

    await wait_until(lambda: lane.queue.qsize() == 100)  # count bound reached
    assert not task.done()

    while not task.done():
        await asyncio.wait_for(lane.queue.get(), timeout=1)
        await asyncio.sleep(0)
    await asyncio.wait_for(task, timeout=1)


async def test_checkpoint_only_entries_cost_no_bytes() -> None:
    # Keepalives (and the sentinel) must never block on the byte budget.
    keepalives = [_keepalive("k", str(i)) for i in range(5)]
    pipe = Pipeline(
        sources=[],
        sink=FakeSink(),
        state=FakeState(),
        batch=_batch_cfg(queue_max_bytes=1),
        metrics=Metrics(),
    )
    lane = pipe._new_lane()

    await asyncio.wait_for(pipe._produce(FakeSource(keepalives), lane, asyncio.Event()), timeout=1)

    assert lane.queue.qsize() == 6  # 5 keepalives + sentinel, none blocked


async def test_clean_shutdown_with_byte_blocked_producer(
    monkeypatch: pytest.MonkeyPatch, wait_until: WaitUntil
) -> None:
    """stop must unblock a producer stuck on the byte budget (via consumer drain)."""
    monkeypatch.setattr(app_module, "_RETRY_BACKOFF_BASE", 0.01)
    line = "x" * 200
    src = FakeSource([_entry("k", str(i), line=line) for i in range(10)])
    sink = FakeSink(fail_times=10**6)  # sink down: consumer stuck retrying
    pipe = Pipeline(
        sources=[src],
        sink=sink,
        state=FakeState(),
        batch=_batch_cfg(max_entries=1, queue_max_bytes=100),
        metrics=Metrics(),
    )

    stop = asyncio.Event()
    task = asyncio.create_task(pipe.run(stop))
    await wait_until(lambda: sink.attempts >= 1)  # producer now blocked on bytes, consumer in retry
    assert not task.done()
    stop.set()
    await asyncio.wait_for(task, timeout=2)


# --- sink failing-since + last push success metric (issue #17) ---------------


async def test_flush_success_sets_last_push_metric_and_clears_failing_since(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(app_module, "_RETRY_BACKOFF_BASE", 0.0)
    sink = FakeSink(fail_times=2)
    metrics = Metrics()
    pipe = Pipeline(sources=[], sink=sink, state=FakeState(), batch=_batch_cfg(), metrics=metrics)

    lane = pipe._new_lane()
    await asyncio.wait_for(pipe._flush(lane, [_entry("k", "v")], asyncio.Event()), timeout=2)

    assert pipe.sink_failing_since is None  # cleared on the eventual success
    val = metrics.registry.get_sample_value("sf2loki_last_push_success_timestamp_seconds")
    assert val is not None and val > 0.0


async def test_failing_since_set_while_sink_retries(
    monkeypatch: pytest.MonkeyPatch, wait_until: WaitUntil
) -> None:
    monkeypatch.setattr(app_module, "_RETRY_BACKOFF_BASE", 5.0)
    src = FakeSource([_entry("k", "v")])
    sink = FakeSink(fail_times=10**6)
    pipe = _pipeline(src, sink, FakeState())
    assert pipe.sink_failing_since is None

    stop = asyncio.Event()
    task = asyncio.create_task(pipe.run(stop))
    await wait_until(lambda: pipe.sink_failing_since is not None)
    assert pipe.sink_failing_since is not None
    stop.set()
    await asyncio.wait_for(task, timeout=1)


async def test_permanent_drop_clears_failing_since(monkeypatch: pytest.MonkeyPatch) -> None:
    """A permanent drop advances the pipeline — it is not a stuck sink."""
    monkeypatch.setattr(app_module, "_RETRY_BACKOFF_BASE", 0.0)

    class RetryThenPermanent:
        def __init__(self) -> None:
            self.attempts = 0

        async def push(self, batch: Batch) -> None:
            self.attempts += 1
            if self.attempts <= 2:
                raise RetryableSinkError("transient")
            raise PermanentSinkError("nope")

        async def aclose(self) -> None:
            return None

    src = FakeSource([_entry("k", "v")])
    pipe = _pipeline(src, RetryThenPermanent(), FakeState())  # type: ignore[arg-type]

    await asyncio.wait_for(pipe.run(asyncio.Event()), timeout=2)

    assert pipe.sink_failing_since is None


async def test_commit_watermark_falls_back_to_now_on_unparseable_value() -> None:
    """A checkpoint value that isn't a parseable timestamp doesn't crash the pipeline."""
    src = FakeSource([_entry("eventlog_objects:LoginEvent", "not-a-timestamp")])
    sink = FakeSink()
    state = FakeState()
    metrics = Metrics()
    pipe = Pipeline(sources=[src], sink=sink, state=state, batch=_batch_cfg(), metrics=metrics)

    await asyncio.wait_for(pipe.run(asyncio.Event()), timeout=2)

    val = metrics.registry.get_sample_value(
        "sf2loki_watermark_timestamp_seconds",
        {"source": "eventlog_objects", "object": "LoginEvent"},
    )
    assert val is not None and val > 0.0  # best-effort "now" fallback, not a crash
