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
from sf2loki.coordinate.file_lease import FileLeaseCoordinator, _LeaseReadError
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
    coord._write(clock.now, 3)
    lease = coord._read()
    assert lease is not None
    assert lease.holder == "A"
    assert lease.expires_at == _BASE + timedelta(seconds=30)
    assert lease.epoch == 3


def test_read_absent_returns_none(tmp_path: Path) -> None:
    coord = FileLeaseCoordinator(_cfg(tmp_path / "nope.lease"), holder="A")
    assert coord._read() is None


def test_read_unparseable_returns_none(tmp_path: Path) -> None:
    path = tmp_path / "l.lease"
    path.write_text("{ this is not json")
    coord = FileLeaseCoordinator(_cfg(path), holder="A")
    assert coord._read() is None


def test_read_missing_epoch_field_defaults_to_zero(tmp_path: Path) -> None:
    """Back-compat: a lease document written before ``epoch`` existed reads as 0."""
    path = tmp_path / "l.lease"
    path.write_text(
        json.dumps({"holder": "A", "expires_at": (_BASE + timedelta(seconds=30)).isoformat()})
    )
    coord = FileLeaseCoordinator(_cfg(path), holder="B")
    lease = coord._read()
    assert lease is not None
    assert lease.epoch == 0


def test_read_raises_lease_read_error_on_non_missing_oserror(tmp_path: Path) -> None:
    """A transient OSError other than FileNotFoundError (e.g. an unreadable/
    non-regular path) must NOT be folded into "absent" -- issue #50."""
    path = tmp_path / "l.lease"
    path.mkdir()  # read_text() on a directory raises IsADirectoryError (an OSError)
    coord = FileLeaseCoordinator(_cfg(path), holder="A")
    with pytest.raises(_LeaseReadError):
        coord._read()


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


async def test_acquire_backs_off_on_transient_read_error_without_contesting(
    tmp_path: Path,
) -> None:
    """A transient OSError reading the lease (not FileNotFoundError) must not be
    treated as an absent/claimable lease -- issue #50. Back off and re-poll
    instead of writing over a lease that may still be live."""
    path = tmp_path / "l.lease"
    path.mkdir()  # read_text() on a directory raises IsADirectoryError (an OSError)
    clock = FakeClock()
    stop = asyncio.Event()
    sleep = ScriptedSleep([stop.set])  # back off once, then stop
    coord = FileLeaseCoordinator(_cfg(path), holder="B", utcnow=clock.utcnow, sleep=sleep)

    def must_not_contest(_now: datetime, _epoch: int) -> None:
        raise AssertionError("a transient read error must never be treated as claimable")

    coord._write = must_not_contest  # type: ignore[assignment]

    assert await coord._acquire(stop) is False


# --------------------------------------------------------------------------
# Hold / renew
# --------------------------------------------------------------------------


async def test_hold_renews_then_loses_on_foreign_holder(tmp_path: Path) -> None:
    """A re-read showing a foreign holder before a renew surrenders leadership."""
    path = tmp_path / "l.lease"
    clock = FakeClock()
    coord = FileLeaseCoordinator(_cfg(path), holder="B", utcnow=clock.utcnow, sleep=None)  # type: ignore[arg-type]
    coord._write(clock.now, 1)  # B holds

    def foreign_takes_over() -> None:
        _write_lease(path, "A", clock.now + timedelta(seconds=30))

    coord._sleep = ScriptedSleep([foreign_takes_over])
    stop = asyncio.Event()

    await coord._hold(stop)  # returns => leadership lost
    assert _read_holder(path) == "A"  # B did not renew over A


async def test_hold_missing_lease_verifies_before_rewriting(tmp_path: Path) -> None:
    """Lease file deleted mid-hold (operator `rm leader.lease`); B must pause and
    verify nobody else claimed it BEFORE rewriting -- issue #50 (old code read
    None and rewrote immediately with no verify pause, on just the renew-pause
    sleep call, widening the window for a concurrent standby's claim to be
    silently clobbered). Nobody else claims it here, so B recreates the lease
    and converges to being the sole leader."""
    path = tmp_path / "l.lease"
    clock = FakeClock()
    coord = FileLeaseCoordinator(_cfg(path), holder="B", utcnow=clock.utcnow, sleep=None)  # type: ignore[arg-type]
    coord._write(clock.now, 1)  # B holds
    coord._epoch = 1

    sleeps_before_write: list[int] = []
    real_write = coord._write

    def spy_write(now: datetime, epoch: int) -> None:
        sleeps_before_write.append(coord._sleep.calls)  # type: ignore[attr-defined]
        real_write(now, epoch)

    coord._write = spy_write  # type: ignore[assignment]

    def delete_lease() -> None:
        path.unlink()

    # sleep #0 = renew pause (operator deletes the file here);
    # sleep #1 = the contested-path verify pause (nobody else claims it);
    # sleep #2 = next renew pause -> stop.
    stop = asyncio.Event()
    coord._sleep = ScriptedSleep([delete_lease, None, stop.set])

    await coord._hold(stop)  # stop fires after a clean renewal cycle

    # The first write after the deletion only happens once BOTH the renew
    # pause AND the verify pause have run (2 sleep calls) -- not immediately
    # after the renew pause alone (1 call), which is what the old code did.
    assert sleeps_before_write[0] == 2
    assert _read_holder(path) == "B"  # recreated the lease, remains sole leader


async def test_hold_deleted_lease_surrenders_if_foreign_claim_appears_during_verify(
    tmp_path: Path,
) -> None:
    """Lease deleted mid-hold; if another holder claims it during the verify pause,
    B must surrender WITHOUT ever having blindly rewritten the lease first --
    issue #50 (the "two leaders" scenario: old code skipped the surrender check
    on a None re-read and wrote its own renewal immediately, before the foreign
    claim was even visible, then only noticed a whole renew_interval later)."""
    path = tmp_path / "l.lease"
    clock = FakeClock()
    coord = FileLeaseCoordinator(_cfg(path), holder="B", utcnow=clock.utcnow, sleep=None)  # type: ignore[arg-type]
    coord._write(clock.now, 1)  # B holds

    write_calls: list[int] = []
    real_write = coord._write

    def spy_write(now: datetime, epoch: int) -> None:
        write_calls.append(epoch)
        real_write(now, epoch)

    coord._write = spy_write  # type: ignore[assignment]

    def delete_lease() -> None:
        path.unlink()

    def foreign_claims_during_verify() -> None:
        _write_lease(path, "A", clock.now + timedelta(seconds=30))

    # sleep #0 = renew pause (deletion happens); sleep #1 = verify pause (A claims it).
    coord._sleep = ScriptedSleep([delete_lease, foreign_claims_during_verify])
    stop = asyncio.Event()

    await coord._hold(stop)  # returns => surrendered, did not clobber A

    assert write_calls == []  # never blindly wrote while the claim was unresolved
    assert _read_holder(path) == "A"  # B did not rewrite over A's claim


async def test_hold_transient_read_error_does_not_trigger_takeover_by_self(
    tmp_path: Path,
) -> None:
    """A read error while holding must not cause B to blindly stamp a fresh lease
    without going through the contested verify path -- issue #50 consequence 2
    (a transient error must never look like "safe to claim")."""
    path = tmp_path / "l.lease"
    clock = FakeClock()
    coord = FileLeaseCoordinator(_cfg(path), holder="B", utcnow=clock.utcnow, sleep=None)  # type: ignore[arg-type]
    coord._write(clock.now, 1)  # B holds

    real_read = coord._read
    calls = {"n": 0}

    def flaky_read() -> object:
        calls["n"] += 1
        if calls["n"] == 1:
            raise _LeaseReadError("transient NFS error")
        return real_read()

    coord._read = flaky_read  # type: ignore[assignment]

    write_calls: list[int] = []
    real_write = coord._write

    def spy_write(now: datetime, epoch: int) -> None:
        write_calls.append(epoch)
        real_write(now, epoch)

    coord._write = spy_write  # type: ignore[assignment]

    def foreign_claims_during_verify() -> None:
        _write_lease(path, "A", clock.now + timedelta(seconds=30))

    # sleep #0 = renew pause (read error happens on the next read);
    # sleep #1 = verify pause (A claims it in the meantime).
    coord._sleep = ScriptedSleep([None, foreign_claims_during_verify])
    stop = asyncio.Event()

    await coord._hold(stop)  # returns => surrendered rather than clobbering A

    assert write_calls == []  # never blindly wrote while the read error was unresolved
    assert _read_holder(path) == "A"


async def test_hold_preserves_epoch_across_renewal(tmp_path: Path) -> None:
    """The epoch fence token is unchanged by a normal renewal in _hold."""
    path = tmp_path / "l.lease"
    clock = FakeClock()
    coord = FileLeaseCoordinator(_cfg(path), holder="B", utcnow=clock.utcnow, sleep=None)  # type: ignore[arg-type]
    coord._write(clock.now, 5)
    coord._epoch = 5
    stop = asyncio.Event()
    coord._sleep = ScriptedSleep([None, stop.set])

    await coord._hold(stop)

    lease = coord._read()
    assert lease is not None
    assert lease.epoch == 5


async def test_hold_surrenders_on_renewal_failure_past_ttl(tmp_path: Path) -> None:
    """Renewal writes failing continuously past one ttl means storage is
    unreachable — assume the lease lapsed and surrender."""
    path = tmp_path / "l.lease"
    clock = FakeClock()
    coord = FileLeaseCoordinator(_cfg(path), holder="B", utcnow=clock.utcnow, sleep=None)  # type: ignore[arg-type]
    coord._write(clock.now, 1)  # B holds, last_ok = base

    def jump_past_ttl() -> None:
        clock.advance(31)  # > ttl (30)

    coord._sleep = ScriptedSleep([jump_past_ttl])

    def boom(_now: datetime, _epoch: int) -> None:
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
    coord._write(clock.now, 1)
    await coord._hold(stop)  # first pause sets stop -> returns immediately


# --------------------------------------------------------------------------
# Epoch fencing token (issue #47)
# --------------------------------------------------------------------------


def test_epoch_is_zero_before_first_acquisition(tmp_path: Path) -> None:
    coord = FileLeaseCoordinator(_cfg(tmp_path / "l.lease"), holder="A")
    assert coord.epoch == 0


async def test_epoch_strictly_increases_after_takeover(tmp_path: Path) -> None:
    """After a takeover, the new leader's epoch is strictly greater than the
    prior holder's -- a durable fence token independent of the local
    ``is_leader`` boolean (issue #47)."""
    path = tmp_path / "l.lease"
    clock = FakeClock()
    stop = asyncio.Event()

    coord_a = FileLeaseCoordinator(
        _cfg(path), holder="A", utcnow=clock.utcnow, sleep=ScriptedSleep()
    )
    assert await coord_a._acquire(stop) is True
    epoch_a = coord_a.epoch
    assert epoch_a == 1

    clock.advance(40)  # well past A's ttl (30s) -> expired
    coord_b = FileLeaseCoordinator(
        _cfg(path), holder="B", utcnow=clock.utcnow, sleep=ScriptedSleep()
    )
    assert await coord_b._acquire(stop) is True
    epoch_b = coord_b.epoch

    assert epoch_b > epoch_a
    lease = coord_b._read()
    assert lease is not None
    assert lease.epoch == epoch_b


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
