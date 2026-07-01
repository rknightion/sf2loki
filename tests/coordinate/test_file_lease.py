"""Tests for the file-lease coordinator (fake clock + fake sleep, no real waits)."""

from __future__ import annotations

import asyncio
import json
from collections.abc import Callable
from datetime import UTC, datetime, timedelta
from pathlib import Path

import pytest

from sf2loki.config import FileLeaseConfig
from sf2loki.coordinate.base import StateFenceError
from sf2loki.coordinate.file_lease import FileLeaseCoordinator
from sf2loki.state.file_store import FileCheckpointStore

_BASE = datetime(2026, 1, 1, 12, 0, 0, tzinfo=UTC)


class FakeClock:
    """Controllable wall clock injected as ``utcnow``."""

    def __init__(self, start: datetime = _BASE) -> None:
        self.now = start

    def utcnow(self) -> datetime:
        return self.now

    def advance(self, seconds: float) -> None:
        self.now += timedelta(seconds=seconds)


class ScriptedSleep:
    """Instant async sleep that runs a scripted side-effect per call.

    Each entry in *actions* is a callable (or None) invoked on the matching
    ``sleep`` call, letting a test mutate the lease file / set the stop event at
    a precise point in the coordinator's loop without ever waiting for real time.
    """

    def __init__(self, actions: list[Callable[[], None] | None] | None = None) -> None:
        self._actions = list(actions or [])
        self.calls = 0

    async def __call__(self, seconds: float) -> None:
        idx = self.calls
        self.calls += 1
        if idx < len(self._actions):
            action = self._actions[idx]
            if action is not None:
                action()


def _cfg(path: Path, *, ttl: int = 30, renew: int = 10) -> FileLeaseConfig:
    return FileLeaseConfig(
        path=path, ttl=timedelta(seconds=ttl), renew_interval=timedelta(seconds=renew)
    )


def _write_lease(path: Path, holder: str, expires_at: datetime) -> None:
    path.write_text(json.dumps({"holder": holder, "expires_at": expires_at.isoformat()}))


def _read_holder(path: Path) -> str:
    return json.loads(path.read_text())["holder"]


# --------------------------------------------------------------------------
# Lease file I/O
# --------------------------------------------------------------------------


def test_write_then_read_roundtrip(tmp_path: Path) -> None:
    clock = FakeClock()
    coord = FileLeaseCoordinator(_cfg(tmp_path / "l.lease"), holder="A", utcnow=clock.utcnow)
    coord._write(clock.now)
    lease = coord._read()
    assert lease is not None
    assert lease.holder == "A"
    assert lease.expires_at == _BASE + timedelta(seconds=30)


def test_read_absent_returns_none(tmp_path: Path) -> None:
    coord = FileLeaseCoordinator(_cfg(tmp_path / "nope.lease"), holder="A")
    assert coord._read() is None


def test_read_unparseable_returns_none(tmp_path: Path) -> None:
    path = tmp_path / "l.lease"
    path.write_text("{ this is not json")
    coord = FileLeaseCoordinator(_cfg(path), holder="A")
    assert coord._read() is None


# --------------------------------------------------------------------------
# Acquire / takeover
# --------------------------------------------------------------------------


async def test_takeover_after_expiry(tmp_path: Path) -> None:
    """A standby takes over an expired foreign lease and stamps its own holder."""
    path = tmp_path / "l.lease"
    _write_lease(path, "A", _BASE + timedelta(seconds=5))  # expires at 12:00:05

    clock = FakeClock(_BASE + timedelta(seconds=40))  # well past A's expiry
    coord = FileLeaseCoordinator(_cfg(path), holder="B", utcnow=clock.utcnow, sleep=ScriptedSleep())
    stop = asyncio.Event()

    assert await coord._acquire(stop) is True
    assert _read_holder(path) == "B"


async def test_acquire_absent_lease(tmp_path: Path) -> None:
    path = tmp_path / "l.lease"
    clock = FakeClock()
    coord = FileLeaseCoordinator(_cfg(path), holder="B", utcnow=clock.utcnow, sleep=ScriptedSleep())
    assert await coord._acquire(asyncio.Event()) is True
    assert _read_holder(path) == "B"


async def test_acquire_returns_false_when_stop_fires_during_standby(tmp_path: Path) -> None:
    """A live foreign holder keeps us standing by; stop ends the wait, no takeover."""
    path = tmp_path / "l.lease"
    _write_lease(path, "A", _BASE + timedelta(seconds=30))  # live

    clock = FakeClock()
    stop = asyncio.Event()
    sleep = ScriptedSleep([stop.set])  # first poll -> shut down
    coord = FileLeaseCoordinator(_cfg(path), holder="B", utcnow=clock.utcnow, sleep=sleep)

    assert await coord._acquire(stop) is False
    assert _read_holder(path) == "A"  # never contested a live lease


async def test_verification_read_race_loser_backs_off(tmp_path: Path) -> None:
    """Two writers rename over an expired lease; the loser (foreign holder on the
    verification re-read) backs off and does not clobber the winner."""
    path = tmp_path / "l.lease"
    clock = FakeClock()
    stop = asyncio.Event()

    def competitor_wins() -> None:
        # During B's verification delay, A lands its rename last.
        _write_lease(path, "A", clock.now + timedelta(seconds=30))

    # sleep #0 = verification delay (A wins here); sleep #1 = loser back-off (stop).
    sleep = ScriptedSleep([competitor_wins, stop.set])
    coord = FileLeaseCoordinator(_cfg(path), holder="B", utcnow=clock.utcnow, sleep=sleep)

    assert await coord._acquire(stop) is False
    assert _read_holder(path) == "A"  # winner intact; loser backed off


# --------------------------------------------------------------------------
# Hold / renew
# --------------------------------------------------------------------------


async def test_hold_renews_then_loses_on_foreign_holder(tmp_path: Path) -> None:
    """A re-read showing a foreign holder before a renew surrenders leadership."""
    path = tmp_path / "l.lease"
    clock = FakeClock()
    coord = FileLeaseCoordinator(_cfg(path), holder="B", utcnow=clock.utcnow, sleep=None)  # type: ignore[arg-type]
    coord._write(clock.now)  # B holds

    def foreign_takes_over() -> None:
        _write_lease(path, "A", clock.now + timedelta(seconds=30))

    coord._sleep = ScriptedSleep([foreign_takes_over])
    stop = asyncio.Event()

    await coord._hold(stop)  # returns => leadership lost
    assert _read_holder(path) == "A"  # B did not renew over A


async def test_hold_surrenders_on_renewal_failure_past_ttl(tmp_path: Path) -> None:
    """Renewal writes failing continuously past one ttl means storage is
    unreachable — assume the lease lapsed and surrender."""
    path = tmp_path / "l.lease"
    clock = FakeClock()
    coord = FileLeaseCoordinator(_cfg(path), holder="B", utcnow=clock.utcnow, sleep=None)  # type: ignore[arg-type]
    coord._write(clock.now)  # B holds, last_ok = base

    def jump_past_ttl() -> None:
        clock.advance(31)  # > ttl (30)

    coord._sleep = ScriptedSleep([jump_past_ttl])

    def boom(_now: datetime) -> None:
        raise OSError("shared storage unreachable")

    coord._write = boom  # type: ignore[assignment]
    stop = asyncio.Event()

    await coord._hold(stop)  # returns => surrendered after ttl of failure


async def test_hold_returns_when_stop_fires(tmp_path: Path) -> None:
    path = tmp_path / "l.lease"
    clock = FakeClock()
    stop = asyncio.Event()
    coord = FileLeaseCoordinator(
        _cfg(path), holder="B", utcnow=clock.utcnow, sleep=ScriptedSleep([stop.set])
    )
    coord._write(clock.now)
    await coord._hold(stop)  # first pause sets stop -> returns immediately


# --------------------------------------------------------------------------
# run(): strict on_acquire / on_lose alternation
# --------------------------------------------------------------------------


async def test_run_alternates_acquire_and_lose(tmp_path: Path) -> None:
    path = tmp_path / "l.lease"
    clock = FakeClock()
    stop = asyncio.Event()
    calls: list[str] = []

    async def on_acquire() -> None:
        calls.append("acquire")

    async def on_lose() -> None:
        calls.append("lose")

    def foreign_after_acquire() -> None:
        _write_lease(path, "A", clock.now + timedelta(seconds=30))

    # sleeps in order:
    #  #0 verify (win) ; #1 hold pause (foreign appears -> lose) ; #2 standby poll (stop)
    sleep = ScriptedSleep([None, foreign_after_acquire, stop.set])
    coord = FileLeaseCoordinator(_cfg(path), holder="B", utcnow=clock.utcnow, sleep=sleep)

    await coord.run(on_acquire=on_acquire, on_lose=on_lose, stop=stop)

    assert calls == ["acquire", "lose"]
    assert coord.is_leader is False


# --------------------------------------------------------------------------
# Fencing (state store integration)
# --------------------------------------------------------------------------


async def test_check_fence_raises_when_not_leader(tmp_path: Path) -> None:
    coord = FileLeaseCoordinator(_cfg(tmp_path / "l.lease"), holder="B")
    with pytest.raises(StateFenceError):
        coord.check_fence()
    coord._is_leader = True
    coord.check_fence()  # no raise while leading


async def test_fenced_commit_raises_before_writing(tmp_path: Path) -> None:
    state_path = tmp_path / "state.json"
    coord = FileLeaseCoordinator(_cfg(tmp_path / "l.lease"), holder="B")  # not leader
    store = FileCheckpointStore(state_path)
    store.set_fence(coord.check_fence)

    with pytest.raises(StateFenceError):
        await store.commit("cursor", "10")
    assert not state_path.exists()  # nothing written

    coord._is_leader = True
    await store.commit("cursor", "10")  # now permitted
    assert await store.load("cursor") == "10"
    store.close()


async def test_invariant_stale_leader_fenced_new_leader_resumes(tmp_path: Path) -> None:
    """At-least-once across failover: A holds, B takes over after expiry, A's next
    commit is fenced (never persisted), and B resumes from A's last committed
    checkpoint. (``_is_leader`` is set here as run() would on each transition.)"""
    state_path = tmp_path / "state.json"
    lease_path = tmp_path / "leader.lease"
    stop = asyncio.Event()

    clock_a = FakeClock(_BASE)
    clock_b = FakeClock(_BASE)
    coord_a = FileLeaseCoordinator(
        _cfg(lease_path), holder="A", utcnow=clock_a.utcnow, sleep=ScriptedSleep()
    )
    coord_b = FileLeaseCoordinator(
        _cfg(lease_path), holder="B", utcnow=clock_b.utcnow, sleep=ScriptedSleep()
    )

    # A acquires leadership and commits a checkpoint.
    assert await coord_a._acquire(stop) is True
    coord_a._is_leader = True
    store_a = FileCheckpointStore(state_path)
    store_a.set_fence(coord_a.check_fence)
    await store_a.commit("cursor", "10")

    # A's lease expires; B takes over.
    clock_b.now = _BASE + timedelta(seconds=40)
    assert await coord_b._acquire(stop) is True
    coord_b._is_leader = True
    # A's hold loop would detect the foreign holder and surrender leadership.
    coord_a._is_leader = False

    # A's next commit (a further-advanced cursor) is fenced -> never persisted.
    with pytest.raises(StateFenceError):
        await store_a.commit("cursor", "20")
    store_a.close()  # fenced/restarting A releases the state lock

    # B resumes from A's LAST COMMITTED checkpoint (10), so 10..20 re-ingest
    # (duplicate, not loss).
    store_b = FileCheckpointStore(state_path)
    store_b.set_fence(coord_b.check_fence)
    assert await store_b.load("cursor") == "10"
    store_b.close()
