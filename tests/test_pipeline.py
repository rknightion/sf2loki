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
