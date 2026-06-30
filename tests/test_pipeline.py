"""Tests for the Pipeline batching / commit / retry / drop behaviour."""

from __future__ import annotations

import asyncio
from collections.abc import AsyncIterator
from datetime import UTC, datetime, timedelta

import pytest

from sf2loki import app as app_module
from sf2loki.app import Pipeline
from sf2loki.config import LokiBatchConfig
from sf2loki.model import Batch, CheckpointToken, LogEntry
from sf2loki.obs.metrics import Metrics
from sf2loki.sinks.base import PermanentSinkError, RetryableSinkError


def _entry(key: str, value: str, line: str = "{}") -> LogEntry:
    return LogEntry(
        timestamp=datetime.now(UTC),
        labels={"source": "test", "event_type": "Thing"},
        line=line,
        structured_metadata={},
        checkpoint=CheckpointToken(key=key, value=value),
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


async def test_flush_by_interval_before_source_finishes() -> None:
    block = asyncio.Event()
    src = FakeSource([_entry("k", "v")], block=block)
    sink = FakeSink()
    state = FakeState()
    pipe = _pipeline(src, sink, state, max_entries=1000)  # never hit size threshold

    task = asyncio.create_task(pipe.run(asyncio.Event()))
    # Give the consumer time to hit the flush_interval (0.05s) while the source is blocked.
    await asyncio.sleep(0.2)
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


async def test_flush_retry_loop_is_stop_aware(monkeypatch: pytest.MonkeyPatch) -> None:
    """Stop fired mid-backoff aborts the retry loop promptly (no full backoff wait)."""
    monkeypatch.setattr(app_module, "_RETRY_BACKOFF_BASE", 5.0)
    src = FakeSource([_entry("k", "v")])
    sink = FakeSink(fail_times=10**6)  # always retryable-fails
    state = FakeState()
    pipe = _pipeline(src, sink, state)

    stop = asyncio.Event()
    task = asyncio.create_task(pipe.run(stop))
    await asyncio.sleep(0.05)  # let the first push attempt fail and enter backoff
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
