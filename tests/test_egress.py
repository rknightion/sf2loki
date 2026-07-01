"""Tests for EgressGovernor (rate caps + daily byte budget) and its pipeline wiring."""

from __future__ import annotations

import asyncio
import json
from collections.abc import AsyncIterator
from datetime import UTC, datetime, timedelta

import pytest
import structlog.testing

from sf2loki.app import Pipeline
from sf2loki.config import EgressConfig, LokiBatchConfig
from sf2loki.egress import BUDGET_KEY, EgressGovernor, _TokenBucket
from sf2loki.model import Batch, CheckpointToken, LogEntry
from sf2loki.obs.metrics import Metrics
from sf2loki.sinks.base import RetryableSinkError

# ---------------------------------------------------------------------------
# Test doubles


class FakeState:
    def __init__(self) -> None:
        self.committed: dict[str, str] = {}

    async def load(self, key: str) -> str | None:
        return self.committed.get(key)

    async def commit(self, key: str, value: str) -> None:
        self.committed[key] = value


class FakeClock:
    def __init__(self, t: float = 0.0) -> None:
        self.t = t

    def __call__(self) -> float:
        return self.t

    def advance(self, dt: float) -> None:
        self.t += dt


class FixedUTC:
    """utcnow() returning a mutable instant the test can flip."""

    def __init__(self, dt: datetime) -> None:
        self.dt = dt

    def __call__(self) -> datetime:
        return self.dt


class RolloverUTC:
    """Returns *before* for the first *flips* calls, then *after* (models a day roll)."""

    def __init__(self, before: datetime, after: datetime, flips: int) -> None:
        self.before = before
        self.after = after
        self.flips = flips
        self.n = 0

    def __call__(self) -> datetime:
        self.n += 1
        return self.before if self.n <= self.flips else self.after


_DAY1 = datetime(2026, 7, 1, 12, 0, 0, tzinfo=UTC)
_DAY2 = datetime(2026, 7, 2, 12, 0, 0, tzinfo=UTC)


def _egress(**kw: object) -> EgressConfig:
    return EgressConfig(**kw)  # type: ignore[arg-type]


def _gov(cfg: EgressConfig, state: FakeState, metrics: Metrics, **kw: object) -> EgressGovernor:
    return EgressGovernor(cfg, state=state, metrics=metrics, **kw)  # type: ignore[arg-type]


# ---------------------------------------------------------------------------
# _TokenBucket unit maths (deterministic, no sleeps)


def test_token_bucket_admits_up_to_capacity_then_delays() -> None:
    clock = FakeClock()
    b = _TokenBucket(rate=10.0, clock=clock)
    # Starts full: 10 tokens.
    assert b.wait_time(10) == 0.0
    b.consume(10)
    # Empty now: 5 more tokens need 5/10 = 0.5s.
    assert b.wait_time(5) == pytest.approx(0.5)
    clock.advance(0.5)  # refill 5 tokens
    assert b.wait_time(5) == 0.0


def test_token_bucket_over_capacity_never_waits_forever() -> None:
    clock = FakeClock()
    b = _TokenBucket(rate=10.0, clock=clock)
    b.consume(10)  # empty
    # A request larger than capacity (20 > 10): capped at capacity, so once the
    # bucket refills to full it is admitted (never an unbounded wait).
    assert b.wait_time(20) == pytest.approx(1.0)  # 10 tokens / 10 rate
    clock.advance(1.0)
    assert b.wait_time(20) == 0.0


def test_token_bucket_refill_caps_at_capacity() -> None:
    clock = FakeClock()
    b = _TokenBucket(rate=10.0, clock=clock)
    clock.advance(100)  # would add 1000, but caps at 10
    assert b.wait_time(10) == 0.0
    b.consume(10)
    assert b.wait_time(1) == pytest.approx(0.1)


# ---------------------------------------------------------------------------
# enabled / disabled


def test_enabled_reflects_any_control() -> None:
    m = Metrics()
    s = FakeState()
    assert not _gov(_egress(), s, m).enabled
    assert _gov(_egress(max_lines_per_second=1), s, m).enabled
    assert _gov(_egress(max_bytes_per_second=1), s, m).enabled
    assert _gov(_egress(daily_byte_budget=1), s, m).enabled


async def test_disabled_admit_no_delay_and_true() -> None:
    gov = _gov(_egress(), FakeState(), Metrics())
    await gov.start()
    stop = asyncio.Event()
    # No controls: admit is a no-op that always allows.
    assert await asyncio.wait_for(gov.admit(1000, 10**9, stop), timeout=1) is True


async def test_rate_cap_admits_when_tokens_available() -> None:
    clock = FakeClock()
    gov = _gov(
        _egress(max_lines_per_second=100, max_bytes_per_second=1000),
        FakeState(),
        Metrics(),
        clock=clock,
    )
    await gov.start()
    stop = asyncio.Event()
    # Both buckets start full: exactly capacity is admitted with no wait.
    assert await asyncio.wait_for(gov.admit(100, 1000, stop), timeout=1) is True


async def test_rate_cap_stop_during_throttle_returns_promptly() -> None:
    clock = FakeClock()
    gov = _gov(_egress(max_bytes_per_second=1.0), FakeState(), Metrics(), clock=clock)
    await gov.start()
    stop = asyncio.Event()
    await gov.admit(0, 1, stop)  # drain the byte bucket
    # A huge request would wait a very long time; stop must short-circuit it.
    task = asyncio.create_task(gov.admit(0, 10**6, stop))
    await asyncio.sleep(0)
    stop.set()
    assert await asyncio.wait_for(task, timeout=1) is True


# ---------------------------------------------------------------------------
# Daily byte budget: accumulate / persist / restart / rollover / 80% warn


async def test_budget_accumulates_and_persists() -> None:
    state = FakeState()
    metrics = Metrics()
    gov = _gov(_egress(daily_byte_budget=1000), state, metrics, utcnow=FixedUTC(_DAY1))
    await gov.start()
    await gov.record(10, 100)
    await gov.record(1, 50)
    assert metrics.registry.get_sample_value("sf2loki_egress_budget_used_bytes") == 150
    stored = json.loads(state.committed[BUDGET_KEY])
    assert stored == {"date": "2026-07-01", "bytes": 150}


async def test_budget_resumes_across_restart() -> None:
    state = FakeState()
    g1 = _gov(_egress(daily_byte_budget=1000), state, Metrics(), utcnow=FixedUTC(_DAY1))
    await g1.start()
    await g1.record(1, 400)
    # Fresh governor, same store, same day -> resumes the counter.
    m2 = Metrics()
    g2 = _gov(_egress(daily_byte_budget=1000), state, m2, utcnow=FixedUTC(_DAY1))
    await g2.start()
    assert m2.registry.get_sample_value("sf2loki_egress_budget_used_bytes") == 400
    await g2.record(1, 100)
    assert m2.registry.get_sample_value("sf2loki_egress_budget_used_bytes") == 500


async def test_budget_restart_on_new_day_resets() -> None:
    state = FakeState()
    g1 = _gov(_egress(daily_byte_budget=1000), state, Metrics(), utcnow=FixedUTC(_DAY1))
    await g1.start()
    await g1.record(1, 400)
    # Restart on a later UTC day: the persisted counter is discarded.
    m2 = Metrics()
    g2 = _gov(_egress(daily_byte_budget=1000), state, m2, utcnow=FixedUTC(_DAY2))
    await g2.start()
    assert m2.registry.get_sample_value("sf2loki_egress_budget_used_bytes") == 0


async def test_budget_rollover_resets_mid_run() -> None:
    state = FakeState()
    metrics = Metrics()
    clock = FakeClock()
    utc = FixedUTC(_DAY1)
    gov = _gov(_egress(daily_byte_budget=1000), state, metrics, clock=clock, utcnow=utc)
    await gov.start()
    await gov.record(1, 500)
    assert metrics.registry.get_sample_value("sf2loki_egress_budget_used_bytes") == 500
    utc.dt = _DAY2  # UTC day rolls over
    await gov.record(1, 100)
    # Rollover resets before adding: 100, not 600.
    assert metrics.registry.get_sample_value("sf2loki_egress_budget_used_bytes") == 100


async def test_budget_warns_once_at_80_percent_and_rearms_on_rollover() -> None:
    state = FakeState()
    utc = FixedUTC(_DAY1)
    gov = _gov(_egress(daily_byte_budget=1000), state, Metrics(), utcnow=utc)
    await gov.start()
    with structlog.testing.capture_logs() as logs:
        await gov.record(1, 800)  # -> 80%
        await gov.record(1, 50)  # still >= 80%, must NOT warn again
        warns = [e for e in logs if e.get("event") == "daily byte budget at 80%"]
        assert len(warns) == 1
    utc.dt = _DAY2  # rollover re-arms the one-shot
    with structlog.testing.capture_logs() as logs:
        await gov.record(1, 800)
        warns = [e for e in logs if e.get("event") == "daily byte budget at 80%"]
        assert len(warns) == 1


# ---------------------------------------------------------------------------
# Budget action: drop vs pause


async def test_budget_drop_returns_false_no_sleep() -> None:
    state = FakeState()
    metrics = Metrics()
    gov = _gov(
        _egress(daily_byte_budget=100, budget_action="drop"), state, metrics, utcnow=FixedUTC(_DAY1)
    )
    await gov.start()
    await gov.record(1, 100)  # exactly at budget
    stop = asyncio.Event()
    # Next batch would exceed -> refused, immediately.
    assert await asyncio.wait_for(gov.admit(1, 10, stop), timeout=1) is False
    # Drop mode never pauses.
    assert metrics.registry.get_sample_value("sf2loki_egress_paused") in (None, 0)
    assert gov.degraded_reason() is None


async def test_budget_pause_blocks_until_rollover_then_proceeds() -> None:
    state = FakeState()
    metrics = Metrics()
    # Both instants sit 0.01s from a UTC midnight, so EVERY pause-loop timeout is
    # tiny regardless of which call reads which value; `after` is a different UTC
    # day, so the first date check that reads it triggers the rollover. flips is
    # generous so setup + a few loop iterations stay on `before`.
    before = datetime(2026, 7, 1, 23, 59, 59, 990000, tzinfo=UTC)
    after = datetime(2026, 7, 2, 23, 59, 59, 990000, tzinfo=UTC)
    utc = RolloverUTC(before, after, flips=20)
    gov = _gov(_egress(daily_byte_budget=100, budget_action="pause"), state, metrics, utcnow=utc)
    await gov.start()
    await gov.record(1, 100)  # fill to budget
    stop = asyncio.Event()
    # This exceeds the budget: pause until the (fake) next UTC midnight, then go.
    assert await asyncio.wait_for(gov.admit(1, 10, stop), timeout=2) is True
    # After rollover the counter reset and pause cleared.
    assert metrics.registry.get_sample_value("sf2loki_egress_paused") == 0
    assert metrics.registry.get_sample_value("sf2loki_egress_budget_used_bytes") == 0
    assert gov.degraded_reason() is None


async def test_budget_pause_sets_paused_and_degraded_reason_while_waiting() -> None:
    state = FakeState()
    metrics = Metrics()
    gov = _gov(
        _egress(daily_byte_budget=100, budget_action="pause"),
        state,
        metrics,
        utcnow=FixedUTC(_DAY1),  # static: never rolls over, stays paused
    )
    await gov.start()
    await gov.record(1, 100)
    stop = asyncio.Event()
    task = asyncio.create_task(gov.admit(1, 10, stop))
    # Give the pause loop a moment to enter.
    for _ in range(5):
        await asyncio.sleep(0)
        if gov.degraded_reason() is not None:
            break
    assert metrics.registry.get_sample_value("sf2loki_egress_paused") == 1
    reason = gov.degraded_reason()
    assert reason is not None
    assert "pushes paused until 2026-07-02" in reason
    # stop unwinds the pause promptly.
    stop.set()
    assert await asyncio.wait_for(task, timeout=1) is True


async def test_pause_reason_only_in_pause_mode() -> None:
    gov = _gov(_egress(daily_byte_budget=100, budget_action="drop"), FakeState(), Metrics())
    await gov.start()
    assert gov.degraded_reason() is None  # drop mode never degrades readiness


# ---------------------------------------------------------------------------
# Pipeline integration


def _entry(key: str, value: str, line: str = "hello") -> LogEntry:
    return LogEntry(
        timestamp=datetime.now(UTC),
        labels={"source": "test", "event_type": "Thing"},
        line=line,
        structured_metadata={},
        checkpoint=CheckpointToken(key=key, value=value),
    )


def _keepalive(key: str, value: str) -> LogEntry:
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

    def __init__(self, entries: list[LogEntry]) -> None:
        self._entries = entries

    async def events(self, state: object, stop: asyncio.Event) -> AsyncIterator[LogEntry]:
        for e in self._entries:
            yield e


class FakeSink:
    def __init__(self, *, fail_times: int = 0) -> None:
        self.pushed: list[Batch] = []
        self._fail_times = fail_times
        self.attempts = 0

    async def push(self, batch: Batch) -> None:
        self.attempts += 1
        if self._fail_times > 0:
            self._fail_times -= 1
            raise RetryableSinkError("transient")
        self.pushed.append(batch)

    async def aclose(self) -> None:
        return None


class SpyGovernor:
    """Minimal governor stand-in that records calls (and can block admit)."""

    def __init__(self, *, admit_result: bool = True, block: asyncio.Event | None = None) -> None:
        self.enabled = True
        self.started = False
        self.admit_calls: list[tuple[int, int]] = []
        self.record_calls: list[tuple[int, int]] = []
        self._admit_result = admit_result
        self._block = block

    async def start(self) -> None:
        self.started = True

    async def admit(self, lines: int, bytes_: int, stop: asyncio.Event) -> bool:
        self.admit_calls.append((lines, bytes_))
        if self._block is not None:
            await self._block.wait()
        return self._admit_result

    async def record(self, lines: int, bytes_: int) -> None:
        self.record_calls.append((lines, bytes_))

    def degraded_reason(self) -> str | None:
        return None


def _batch_cfg() -> LokiBatchConfig:
    return LokiBatchConfig(max_entries=100, max_bytes=10**9, flush_interval=timedelta(seconds=0.05))


def _pipeline(
    source: FakeSource, sink: FakeSink, state: FakeState, governor: object, metrics: Metrics
) -> Pipeline:
    return Pipeline(
        sources=[source],
        sink=sink,
        state=state,
        batch=_batch_cfg(),
        metrics=metrics,
        governor=governor,  # type: ignore[arg-type]
    )


async def test_pipeline_starts_governor() -> None:
    gov = SpyGovernor()
    pipe = _pipeline(FakeSource([_entry("k", "v")]), FakeSink(), FakeState(), gov, Metrics())
    await asyncio.wait_for(pipe.run(asyncio.Event()), timeout=2)
    assert gov.started is True


async def test_pipeline_drop_counts_and_commits_checkpoint() -> None:
    metrics = Metrics()
    state = FakeState()
    sink = FakeSink()
    gov = _gov(
        _egress(daily_byte_budget=1, budget_action="drop"),
        state,
        metrics,
        utcnow=FixedUTC(_DAY1),
    )
    pipe = _pipeline(FakeSource([_entry("pubsub:/event/X", "v1")]), sink, state, gov, metrics)
    await asyncio.wait_for(pipe.run(asyncio.Event()), timeout=2)
    # Over-budget batch dropped: never pushed, but the checkpoint advanced.
    assert sink.pushed == []
    assert state.committed["pubsub:/event/X"] == "v1"
    dropped = metrics.registry.get_sample_value(
        "sf2loki_loki_entries_dropped_total", {"reason": "budget"}
    )
    assert dropped == 1


async def test_pipeline_records_usage_on_successful_push() -> None:
    metrics = Metrics()
    state = FakeState()
    sink = FakeSink()
    gov = _gov(_egress(daily_byte_budget=10**9), state, metrics, utcnow=FixedUTC(_DAY1))
    entry = _entry("pubsub:/event/X", "v1", line="hello")
    pipe = _pipeline(FakeSource([entry]), sink, state, gov, metrics)
    await asyncio.wait_for(pipe.run(asyncio.Event()), timeout=2)
    assert sum(len(b.entries) for b in sink.pushed) == 1
    expected = len(b"hello")
    assert metrics.registry.get_sample_value("sf2loki_egress_budget_used_bytes") == expected
    assert json.loads(state.committed[BUDGET_KEY])["bytes"] == expected


async def test_pipeline_keepalive_only_batch_bypasses_governor() -> None:
    gov = SpyGovernor()
    state = FakeState()
    sink = FakeSink()
    pipe = _pipeline(FakeSource([_keepalive("pubsub:/event/X", "v9")]), sink, state, gov, Metrics())
    await asyncio.wait_for(pipe.run(asyncio.Event()), timeout=2)
    # Governor never consulted for a checkpoint-only-only flush.
    assert gov.admit_calls == []
    assert gov.record_calls == []
    assert sink.pushed == []  # keepalives are never pushed
    assert state.committed["pubsub:/event/X"] == "v9"  # but its token commits


async def test_pipeline_retry_does_not_double_admit() -> None:
    gov = SpyGovernor()
    state = FakeState()
    sink = FakeSink(fail_times=2)  # two retryable failures, then success
    pipe = _pipeline(FakeSource([_entry("k", "v")]), sink, state, gov, Metrics())
    await asyncio.wait_for(pipe.run(asyncio.Event()), timeout=5)
    assert sink.attempts == 3  # retried twice
    assert len(gov.admit_calls) == 1  # admitted once, before the retry loop
    assert len(gov.record_calls) == 1  # recorded once, after eventual success


async def test_pipeline_pause_blocks_flush_until_admitted() -> None:
    # A blocking admit models a budget pause: the flush must not push until it
    # returns, so the queue backs up behind it (structural backpressure).
    gate = asyncio.Event()
    gov = SpyGovernor(block=gate)
    state = FakeState()
    sink = FakeSink()
    pipe = _pipeline(FakeSource([_entry("k", "v")]), sink, state, gov, Metrics())
    task = asyncio.create_task(pipe.run(asyncio.Event()))
    # Let the flush reach admit and block there.
    for _ in range(10):
        await asyncio.sleep(0)
        if gov.admit_calls:
            break
    assert gov.admit_calls  # admit reached
    assert sink.pushed == []  # blocked before push
    gate.set()  # release the pause
    await asyncio.wait_for(task, timeout=2)
    assert sum(len(b.entries) for b in sink.pushed) == 1
