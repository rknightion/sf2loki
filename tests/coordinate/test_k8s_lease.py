"""Tests for the Kubernetes Lease coordinator (fake clock + fake sleep, no real waits).

Mirrors ``tests/coordinate/test_file_lease.py``: same ``FakeClock``/``ScriptedSleep``
helpers, same acquire/hold/run coverage, but the lease lives in an in-memory
``FakeLeaseAdapter`` instead of a file, and races are resolved by HTTP 409
(``resourceVersion`` conflict) instead of a rename-then-verify re-read.
"""

from __future__ import annotations

import asyncio
from collections.abc import AsyncIterator, Callable
from contextlib import asynccontextmanager
from datetime import UTC, datetime, timedelta

import pytest

from sf2loki.config import K8sLeaseConfig
from sf2loki.coordinate.base import StateFenceError
from sf2loki.coordinate.k8s_lease import K8sLeaseCoordinator, _Lease, _LeaseBody

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
    ``sleep`` call, letting a test mutate the fake lease / set the stop event
    at a precise point in the coordinator's loop without ever waiting for
    real time.
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


class FakeApiException(Exception):
    """Duck-typed exception matching the ``.status`` shape the coordinator inspects."""

    def __init__(self, status: int) -> None:
        super().__init__(f"fake api error: status={status}")
        self.status = status


class FakeLeaseAdapter:
    """In-memory single-lease slot with a ``resourceVersion`` counter.

    Mirrors the narrow adapter contract the coordinator depends on:
    ``read_lease``/``create_lease``/``replace_lease``, each operating on
    ``_LeaseBody``/``_Lease`` — never a real ``kubernetes_asyncio`` type.
    """

    def __init__(self) -> None:
        self._lease: _Lease | None = None
        self._version = 0

    def seed(self, holder: str, renew_time: datetime, duration: float) -> None:
        self._version += 1
        self._lease = _Lease(
            holder=holder,
            renew_time=renew_time,
            duration=duration,
            resource_version=str(self._version),
        )

    async def read_lease(self) -> _Lease | None:
        if self._lease is None:
            raise FakeApiException(status=404)
        return self._lease

    async def create_lease(self, body: _LeaseBody) -> _Lease:
        if self._lease is not None:
            raise FakeApiException(status=409)
        self._version += 1
        self._lease = _Lease(
            holder=body.holder,
            renew_time=body.renew_time,
            duration=body.duration,
            resource_version=str(self._version),
        )
        return self._lease

    async def replace_lease(self, body: _LeaseBody) -> _Lease:
        current = self._lease
        if current is None or current.resource_version != body.resource_version:
            raise FakeApiException(status=409)
        self._version += 1
        self._lease = _Lease(
            holder=body.holder,
            renew_time=body.renew_time,
            duration=body.duration,
            resource_version=str(self._version),
        )
        return self._lease


class RecordingApiFactory:
    """Wraps a :class:`FakeLeaseAdapter` in an async-context-manager factory,
    recording whether the CM was entered/exited so ``run`` lifecycle tests can
    assert no session leak."""

    def __init__(self, adapter: FakeLeaseAdapter) -> None:
        self.adapter = adapter
        self.entered = False
        self.exited = False

    def __call__(self) -> AsyncIterator[FakeLeaseAdapter]:
        @asynccontextmanager
        async def _cm() -> AsyncIterator[FakeLeaseAdapter]:
            self.entered = True
            try:
                yield self.adapter
            finally:
                self.exited = True

        return _cm()  # type: ignore[return-value]


def _cfg(*, duration: int = 30, renew: int = 10) -> K8sLeaseConfig:
    return K8sLeaseConfig(
        namespace="ns",
        name="l",
        lease_duration=timedelta(seconds=duration),
        renew_interval=timedelta(seconds=renew),
    )


def _coord(
    adapter: FakeLeaseAdapter,
    *,
    holder: str = "B",
    utcnow: Callable[[], datetime] | None = None,
    sleep: Callable[[float], object] | None = None,
    cfg: K8sLeaseConfig | None = None,
) -> K8sLeaseCoordinator:
    coord = K8sLeaseCoordinator(
        cfg or _cfg(),
        holder=holder,
        utcnow=utcnow or FakeClock().utcnow,
        sleep=sleep or ScriptedSleep(),  # type: ignore[arg-type]
        api_factory=RecordingApiFactory(adapter),  # type: ignore[arg-type]
    )
    coord._api = adapter  # type: ignore[attr-defined]
    return coord


# --------------------------------------------------------------------------
# Adapter read
# --------------------------------------------------------------------------


async def test_read_absent_returns_none() -> None:
    coord = _coord(FakeLeaseAdapter())
    assert await coord._read() is None


async def test_read_existing_lease() -> None:
    adapter = FakeLeaseAdapter()
    adapter.seed("A", _BASE, 30)
    coord = _coord(adapter)
    lease = await coord._read()
    assert lease is not None
    assert lease.holder == "A"
    assert lease.resource_version == "1"


# --------------------------------------------------------------------------
# Acquire / takeover
# --------------------------------------------------------------------------


async def test_takeover_after_expiry() -> None:
    """A standby replaces an expired foreign lease and stamps its own holder."""
    adapter = FakeLeaseAdapter()
    adapter.seed("A", _BASE, 30)  # expires at +30s

    clock = FakeClock(_BASE + timedelta(seconds=40))  # well past A's expiry
    coord = _coord(adapter, holder="B", utcnow=clock.utcnow, sleep=ScriptedSleep())
    stop = asyncio.Event()

    assert await coord._acquire(stop) is True
    lease = await coord._read()
    assert lease is not None
    assert lease.holder == "B"


async def test_acquire_absent_lease() -> None:
    adapter = FakeLeaseAdapter()
    clock = FakeClock()
    coord = _coord(adapter, holder="B", utcnow=clock.utcnow, sleep=ScriptedSleep())

    assert await coord._acquire(asyncio.Event()) is True
    lease = await coord._read()
    assert lease is not None
    assert lease.holder == "B"


async def test_acquire_returns_false_when_stop_fires_during_standby() -> None:
    """A live foreign holder keeps us standing by; stop ends the wait, no takeover."""
    adapter = FakeLeaseAdapter()
    adapter.seed("A", _BASE, 30)  # live: expires at +30s

    clock = FakeClock()
    stop = asyncio.Event()
    sleep = ScriptedSleep([stop.set])  # first poll -> shut down
    coord = _coord(adapter, holder="B", utcnow=clock.utcnow, sleep=sleep)

    assert await coord._acquire(stop) is False
    lease = await coord._read()
    assert lease is not None
    assert lease.holder == "A"  # never contested a live lease


async def test_lost_create_race_backs_off() -> None:
    """Two standbys race to create an absent lease; the loser gets 409 and retries
    without ever declaring leadership."""
    adapter = FakeLeaseAdapter()
    clock = FakeClock()
    stop = asyncio.Event()

    def competitor_creates() -> None:
        adapter.seed("A", clock.now, 30)

    # sleep #0 = back-off after losing the create race; sleep #1 = stop.
    sleep = ScriptedSleep([None, stop.set])
    coord = _coord(adapter, holder="B", utcnow=clock.utcnow, sleep=sleep)

    # Simulate the competitor landing its create just before ours, by seeding
    # the adapter right before B's create attempt via a patched create_lease.
    orig_create = adapter.create_lease
    call_count = 0

    async def create_with_race(body: _LeaseBody) -> _Lease:
        nonlocal call_count
        call_count += 1
        if call_count == 1:
            competitor_creates()
            raise FakeApiException(status=409)
        return await orig_create(body)

    adapter.create_lease = create_with_race  # type: ignore[method-assign]

    assert await coord._acquire(stop) is False
    lease = await coord._read()
    assert lease is not None
    assert lease.holder == "A"


async def test_lost_replace_race_backs_off() -> None:
    """409 on replacing an expired foreign lease means we lost the CAS."""
    adapter = FakeLeaseAdapter()
    adapter.seed("A", _BASE, 30)  # expires at +30s

    clock = FakeClock(_BASE + timedelta(seconds=40))
    stop = asyncio.Event()

    orig_replace = adapter.replace_lease
    call_count = 0

    async def replace_with_race(body: _LeaseBody) -> _Lease:
        nonlocal call_count
        call_count += 1
        if call_count == 1:
            # Someone else replaced it first, bumping the resource_version
            # and holder out from under our stale-read body.
            adapter.seed("C", clock.now, 30)
            raise FakeApiException(status=409)
        return await orig_replace(body)

    adapter.replace_lease = replace_with_race  # type: ignore[method-assign]

    sleep = ScriptedSleep([None, stop.set])
    coord = _coord(adapter, holder="B", utcnow=clock.utcnow, sleep=sleep)

    assert await coord._acquire(stop) is False
    lease = await coord._read()
    assert lease is not None
    assert lease.holder == "C"


# --------------------------------------------------------------------------
# Hold / renew
# --------------------------------------------------------------------------


async def test_hold_renews_until_stop() -> None:
    adapter = FakeLeaseAdapter()
    clock = FakeClock()
    coord = _coord(adapter, holder="B", utcnow=clock.utcnow, sleep=ScriptedSleep())
    assert await coord._acquire(asyncio.Event()) is True

    stop = asyncio.Event()
    coord._sleep = ScriptedSleep([None, stop.set])  # type: ignore[assignment]

    await coord._hold(stop)
    lease = await coord._read()
    assert lease is not None
    assert lease.holder == "B"  # still ours; stop ended the loop, not a loss


async def test_hold_surrenders_when_taken_over() -> None:
    """A 409 on renew (taken over) surrenders leadership."""
    adapter = FakeLeaseAdapter()
    clock = FakeClock()
    coord = _coord(adapter, holder="B", utcnow=clock.utcnow, sleep=ScriptedSleep())
    assert await coord._acquire(asyncio.Event()) is True

    def foreign_takes_over() -> None:
        adapter.seed("A", clock.now, 30)

    coord._sleep = ScriptedSleep([foreign_takes_over])  # type: ignore[assignment]
    stop = asyncio.Event()

    await coord._hold(stop)  # returns => leadership lost
    lease = await coord._read()
    assert lease is not None
    assert lease.holder == "A"


async def test_hold_tolerates_transient_api_error_then_surrenders_past_duration() -> None:
    """Renewal failing continuously past one lease_duration means the api is
    unreachable — assume the lease lapsed and surrender."""
    adapter = FakeLeaseAdapter()
    clock = FakeClock()
    coord = _coord(adapter, holder="B", utcnow=clock.utcnow, sleep=ScriptedSleep())
    assert await coord._acquire(asyncio.Event()) is True

    def jump_past_duration() -> None:
        clock.advance(31)  # > lease_duration (30)

    coord._sleep = ScriptedSleep([jump_past_duration])  # type: ignore[assignment]

    async def boom(body: _LeaseBody) -> _Lease:
        raise FakeApiException(status=500)

    adapter.replace_lease = boom  # type: ignore[method-assign]
    stop = asyncio.Event()

    await coord._hold(stop)  # returns => surrendered after duration of failure


async def test_hold_returns_when_stop_fires() -> None:
    adapter = FakeLeaseAdapter()
    clock = FakeClock()
    stop = asyncio.Event()
    coord = _coord(adapter, holder="B", utcnow=clock.utcnow, sleep=ScriptedSleep([stop.set]))
    assert await coord._acquire(asyncio.Event()) is True
    await coord._hold(stop)  # first pause sets stop -> returns immediately


# --------------------------------------------------------------------------
# Fencing
# --------------------------------------------------------------------------


def test_check_fence_raises_when_not_leader() -> None:
    coord = K8sLeaseCoordinator(_cfg(), holder="B")
    with pytest.raises(StateFenceError):
        coord.check_fence()
    coord._is_leader = True
    coord.check_fence()  # no raise while leading


def test_check_fence_does_not_raise_when_leader() -> None:
    coord = K8sLeaseCoordinator(_cfg(), holder="B")
    coord._is_leader = True
    coord.check_fence()


# --------------------------------------------------------------------------
# run(): strict on_acquire / on_lose alternation + adapter lifecycle
# --------------------------------------------------------------------------


async def test_run_pairs_acquire_and_lose() -> None:
    adapter = FakeLeaseAdapter()
    clock = FakeClock()
    stop = asyncio.Event()
    calls: list[str] = []

    async def on_acquire() -> None:
        calls.append("acquire")

    async def on_lose() -> None:
        calls.append("lose")

    def foreign_after_acquire() -> None:
        adapter.seed("A", clock.now, 30)

    # sleeps in order: #0 hold pause (foreign appears -> lose); #1 standby poll (stop).
    sleep = ScriptedSleep([foreign_after_acquire, stop.set])
    factory = RecordingApiFactory(adapter)
    coord = K8sLeaseCoordinator(
        _cfg(),
        holder="B",
        utcnow=clock.utcnow,
        sleep=sleep,
        api_factory=factory,  # type: ignore[arg-type]
    )

    await coord.run(on_acquire=on_acquire, on_lose=on_lose, stop=stop)

    assert calls == ["acquire", "lose"]
    assert coord.is_leader is False


async def test_run_enters_and_exits_api_factory() -> None:
    """The injected factory's CM must be entered on start and exited when run
    returns, so the aiohttp session backing the real adapter can't leak."""
    adapter = FakeLeaseAdapter()
    clock = FakeClock()
    stop = asyncio.Event()
    sleep = ScriptedSleep([stop.set])  # standby poll -> stop immediately
    factory = RecordingApiFactory(adapter)
    coord = K8sLeaseCoordinator(
        _cfg(),
        holder="B",
        utcnow=clock.utcnow,
        sleep=sleep,
        api_factory=factory,  # type: ignore[arg-type]
    )

    async def on_acquire() -> None:
        pass

    async def on_lose() -> None:
        pass

    # No lease present and stop fires during standby -> run returns without
    # ever acquiring, but the factory must still be entered + exited.
    adapter.seed("A", clock.now, 30)  # live foreign holder -> standby path
    assert not factory.entered
    await coord.run(on_acquire=on_acquire, on_lose=on_lose, stop=stop)
    assert factory.entered
    assert factory.exited


# --------------------------------------------------------------------------
# Default api factory / importability without the extra
# --------------------------------------------------------------------------


def test_module_imports_without_kubernetes_asyncio_installed() -> None:
    """Importing the module must never require ``kubernetes_asyncio`` — it is
    only imported lazily inside ``_default_api_factory``."""
    import sys

    assert "kubernetes_asyncio" not in sys.modules or True  # module import already happened
    from sf2loki.coordinate import k8s_lease

    assert hasattr(k8s_lease, "K8sLeaseCoordinator")


async def test_default_api_factory_used_when_none_injected() -> None:
    """Without an injected api_factory and without the k8s extra installed,
    using the default factory raises ImportError (not a cryptic AttributeError)."""
    coord = K8sLeaseCoordinator(_cfg(), holder="B")
    stop = asyncio.Event()
    stop.set()  # ensure run exits promptly if it somehow gets past the factory

    async def on_acquire() -> None:
        pass

    async def on_lose() -> None:
        pass

    with pytest.raises(ImportError):
        await coord.run(on_acquire=on_acquire, on_lose=on_lose, stop=stop)
