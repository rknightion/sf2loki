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


class FakeMonotonic:
    """Controllable monotonic clock injected as ``monotonic``.

    Separate from :class:`FakeClock` (the wall clock / ``utcnow``) on
    purpose: the whole point of the observedTime fix (#51) is that lease
    expiry is judged against THIS clock, never the wall clock either side
    writes into ``renewTime``.
    """

    def __init__(self, start: float = 0.0) -> None:
        self.now = start

    def __call__(self) -> float:
        return self.now

    def advance(self, seconds: float) -> None:
        self.now += seconds


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

    def seed(self, holder: str, renew_time: datetime | None, duration: float | None) -> None:
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
    monotonic: Callable[[], float] | None = None,
    cfg: K8sLeaseConfig | None = None,
) -> K8sLeaseCoordinator:
    coord = K8sLeaseCoordinator(
        cfg or _cfg(),
        holder=holder,
        utcnow=utcnow or FakeClock().utcnow,
        sleep=sleep or ScriptedSleep(),  # type: ignore[arg-type]
        monotonic=monotonic or FakeMonotonic(),
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
# Optional Lease fields (#62): holderIdentity/renewTime/leaseDurationSeconds
# are all OPTIONAL in coordination.k8s.io/v1
# --------------------------------------------------------------------------


def test_lease_is_stale_when_fields_missing() -> None:
    """Missing renewTime/duration means never-renewed -> immediately
    claimable; missing holderIdentity means unheld -> also immediately
    claimable. Only a fully-populated, held lease can be "not stale"."""
    assert _Lease(holder="", renew_time=None, duration=None, resource_version="1").is_stale(0.0)
    assert _Lease(holder="A", renew_time=_BASE, duration=None, resource_version="1").is_stale(0.0)
    assert _Lease(holder="A", renew_time=None, duration=30, resource_version="1").is_stale(0.0)
    assert _Lease(holder="", renew_time=_BASE, duration=30, resource_version="1").is_stale(0.0)

    held = _Lease(holder="A", renew_time=_BASE, duration=30, resource_version="1")
    assert held.is_stale(0.0) is False
    assert held.is_stale(30.0) is True


class _FakeV1Meta:
    def __init__(self, resource_version: str) -> None:
        self.resource_version = resource_version


class _FakeV1Spec:
    def __init__(
        self,
        holder_identity: str | None = None,
        renew_time: datetime | None = None,
        lease_duration_seconds: int | None = None,
    ) -> None:
        self.holder_identity = holder_identity
        self.renew_time = renew_time
        self.lease_duration_seconds = lease_duration_seconds


class _FakeV1Lease:
    def __init__(self, spec: _FakeV1Spec, resource_version: str) -> None:
        self.spec = spec
        self.metadata = _FakeV1Meta(resource_version)


def test_from_v1_lease_tolerates_all_null_optional_fields() -> None:
    """A pre-existing Lease with every optional field null (e.g. a bare
    ``kubectl create``d Lease, or one written by another controller) must not
    crash the adapter -- it parses into an immediately-claimable, unheld
    ``_Lease``."""
    from sf2loki.coordinate.k8s_lease import _RealLeaseAdapter

    v1_lease = _FakeV1Lease(_FakeV1Spec(), resource_version="7")

    lease = _RealLeaseAdapter._from_v1_lease(v1_lease)

    assert lease.holder == ""
    assert lease.renew_time is None
    assert lease.duration is None
    assert lease.resource_version == "7"
    assert lease.is_stale(0.0) is True


def test_from_v1_lease_tolerates_null_duration_only() -> None:
    from sf2loki.coordinate.k8s_lease import _RealLeaseAdapter

    v1_lease = _FakeV1Lease(
        _FakeV1Spec(holder_identity="A", renew_time=_BASE, lease_duration_seconds=None),
        resource_version="2",
    )

    lease = _RealLeaseAdapter._from_v1_lease(v1_lease)

    assert lease.holder == "A"
    assert lease.renew_time == _BASE
    assert lease.duration is None
    assert lease.is_stale(0.0) is True


def test_from_v1_lease_tolerates_null_renew_time_only() -> None:
    from sf2loki.coordinate.k8s_lease import _RealLeaseAdapter

    v1_lease = _FakeV1Lease(
        _FakeV1Spec(holder_identity="A", renew_time=None, lease_duration_seconds=30),
        resource_version="3",
    )

    lease = _RealLeaseAdapter._from_v1_lease(v1_lease)

    assert lease.holder == "A"
    assert lease.renew_time is None
    assert lease.duration == 30.0
    assert lease.is_stale(0.0) is True


async def test_acquire_takes_over_lease_with_null_renew_time_and_duration() -> None:
    """A pre-existing Lease with a null renewTime/duration is treated as
    expired/claimable -- not a crash, and not a doomed ``create_lease`` call
    against an object that already exists (#62)."""
    adapter = FakeLeaseAdapter()
    adapter.seed("", None, None)  # unheld, never renewed

    coord = _coord(adapter, holder="B")
    stop = asyncio.Event()

    acquired = await coord._acquire(stop)
    assert acquired is not None
    lease = await coord._read()
    assert lease is not None
    assert lease.holder == "B"


# --------------------------------------------------------------------------
# Acquire / takeover
# --------------------------------------------------------------------------


async def test_takeover_after_expiry() -> None:
    """A standby replaces a foreign lease once ITS OWN (monotonic) observation
    window has elapsed since it first saw this resourceVersion -- not merely
    because the wall-clock renewTime already looks old (the observedTime
    pattern adopted for #51: the leader's/observer's wall clock is never
    trusted for expiry)."""
    adapter = FakeLeaseAdapter()
    adapter.seed("A", _BASE, 30)  # renewTime already looks stale by wall clock

    clock = FakeClock(_BASE + timedelta(seconds=40))  # observer's wall clock: also past expiry
    mono = FakeMonotonic()
    stop = asyncio.Event()

    def advance_past_duration() -> None:
        mono.advance(30)  # >= lease_duration of OBSERVED (monotonic) time

    sleep = ScriptedSleep([advance_past_duration])
    coord = _coord(adapter, holder="B", utcnow=clock.utcnow, sleep=sleep, monotonic=mono)

    acquired = await coord._acquire(stop)
    assert acquired is not None
    lease = await coord._read()
    assert lease is not None
    assert lease.holder == "B"


async def test_wall_clock_skew_does_not_trigger_premature_takeover() -> None:
    """A standby whose wall clock races far ahead of the leader's (or whose
    leader's clock runs slow) must not treat a live, actively-renewed lease as
    expired just because renewTime + duration is long past by wall clock --
    expiry is judged purely by how long the OBSERVER has seen this exact
    resourceVersion on its own monotonic clock (#51)."""
    adapter = FakeLeaseAdapter()
    adapter.seed("A", _BASE, 30)  # duration 30s per the (possibly skewed) leader clock

    # The observer's wall clock is wildly ahead of renewTime + duration -- under
    # the old renew_time-vs-observer-wall-clock comparison this would already
    # look expired by 970s.
    clock = FakeClock(_BASE + timedelta(seconds=1000))
    mono = FakeMonotonic()
    stop = asyncio.Event()

    def advance_short_of_duration() -> None:
        mono.advance(29)  # < lease_duration (30) of OBSERVED time

    sleep = ScriptedSleep([advance_short_of_duration, stop.set])
    coord = _coord(adapter, holder="B", utcnow=clock.utcnow, sleep=sleep, monotonic=mono)

    assert await coord._acquire(stop) is None  # never took over
    lease = await coord._read()
    assert lease is not None
    assert lease.holder == "A"


async def test_observation_window_resets_when_lease_renews() -> None:
    """If the leader actually renews (bumping resourceVersion) before the
    observer's window elapses, the staleness clock resets -- an actively
    renewed lease is never taken over, no matter how stale it looks by wall
    clock alone (#51)."""
    adapter = FakeLeaseAdapter()
    adapter.seed("A", _BASE, 30)

    clock = FakeClock(_BASE + timedelta(seconds=1000))  # looks ancient by wall clock
    mono = FakeMonotonic()
    stop = asyncio.Event()

    def renew_then_advance() -> None:
        # Would exceed the window if the renewal did NOT reset it.
        mono.advance(35)
        adapter.seed("A", clock.now, 30)  # leader renews -> new resourceVersion

    sleep = ScriptedSleep([renew_then_advance, stop.set])
    coord = _coord(adapter, holder="B", utcnow=clock.utcnow, sleep=sleep, monotonic=mono)

    assert await coord._acquire(stop) is None
    lease = await coord._read()
    assert lease is not None
    assert lease.holder == "A"


async def test_acquire_absent_lease() -> None:
    adapter = FakeLeaseAdapter()
    clock = FakeClock()
    coord = _coord(adapter, holder="B", utcnow=clock.utcnow, sleep=ScriptedSleep())

    assert await coord._acquire(asyncio.Event()) is not None
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

    assert await coord._acquire(stop) is None
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

    assert await coord._acquire(stop) is None
    lease = await coord._read()
    assert lease is not None
    assert lease.holder == "A"


async def test_lost_replace_race_backs_off() -> None:
    """409 on replacing a lease we've observed as stale (via our own
    monotonic clock, see #51) means we lost the CAS."""
    adapter = FakeLeaseAdapter()
    adapter.seed("A", _BASE, 30)

    clock = FakeClock(_BASE + timedelta(seconds=40))
    mono = FakeMonotonic()
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

    def advance_past_duration() -> None:
        mono.advance(30)  # reach our own observation window before contesting

    # sleep #0 = standby wait before our observation window elapses;
    # sleep #1 = back-off after losing the replace race;
    # sleep #2 = C's freshly-renewed lease resets our window -> stop.
    sleep = ScriptedSleep([advance_past_duration, None, stop.set])
    coord = _coord(adapter, holder="B", utcnow=clock.utcnow, sleep=sleep, monotonic=mono)

    assert await coord._acquire(stop) is None
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
    acquired = await coord._acquire(asyncio.Event())
    assert acquired is not None

    stop = asyncio.Event()
    coord._sleep = ScriptedSleep([None, stop.set])  # type: ignore[assignment]

    await coord._hold(stop, acquired)
    lease = await coord._read()
    assert lease is not None
    assert lease.holder == "B"  # still ours; stop ended the loop, not a loss


async def test_hold_surrenders_when_taken_over() -> None:
    """A 409 on renew (taken over) surrenders leadership."""
    adapter = FakeLeaseAdapter()
    clock = FakeClock()
    coord = _coord(adapter, holder="B", utcnow=clock.utcnow, sleep=ScriptedSleep())
    acquired = await coord._acquire(asyncio.Event())
    assert acquired is not None

    def foreign_takes_over() -> None:
        adapter.seed("A", clock.now, 30)

    coord._sleep = ScriptedSleep([foreign_takes_over])  # type: ignore[assignment]
    stop = asyncio.Event()

    await coord._hold(stop, acquired)  # returns => leadership lost
    lease = await coord._read()
    assert lease is not None
    assert lease.holder == "A"


async def test_hold_tolerates_transient_api_error_then_surrenders_past_duration() -> None:
    """Renewal failing continuously past one lease_duration means the api is
    unreachable — assume the lease lapsed and surrender."""
    adapter = FakeLeaseAdapter()
    clock = FakeClock()
    coord = _coord(adapter, holder="B", utcnow=clock.utcnow, sleep=ScriptedSleep())
    acquired = await coord._acquire(asyncio.Event())
    assert acquired is not None

    def jump_past_duration() -> None:
        clock.advance(31)  # > lease_duration (30)

    coord._sleep = ScriptedSleep([jump_past_duration])  # type: ignore[assignment]

    async def boom(body: _LeaseBody) -> _Lease:
        raise FakeApiException(status=500)

    adapter.replace_lease = boom  # type: ignore[method-assign]
    stop = asyncio.Event()

    await coord._hold(stop, acquired)  # returns => surrendered after duration of failure


async def test_hold_surrenders_immediately_when_lease_deleted() -> None:
    """A 404 on renew (the Lease was deleted out from under us) surrenders at
    once — NOT tolerated as a transient error until lease_duration elapses, which
    would leave a bounded split-brain window while a standby recreates the lease."""
    adapter = FakeLeaseAdapter()
    clock = FakeClock()
    coord = _coord(adapter, holder="B", utcnow=clock.utcnow, sleep=ScriptedSleep())
    acquired = await coord._acquire(asyncio.Event())
    assert acquired is not None

    async def deleted(body: _LeaseBody) -> _Lease:
        raise FakeApiException(status=404)

    adapter.replace_lease = deleted  # type: ignore[method-assign]
    # The clock never advances, so the "failing past lease_duration" timeout path
    # cannot be what returns — only the 404 special-case can (otherwise this loops
    # forever, since ScriptedSleep never fires stop).
    stop = asyncio.Event()

    await coord._hold(stop, acquired)  # returns promptly => surrendered on the 404


async def test_hold_returns_when_stop_fires() -> None:
    adapter = FakeLeaseAdapter()
    clock = FakeClock()
    stop = asyncio.Event()
    coord = _coord(adapter, holder="B", utcnow=clock.utcnow, sleep=ScriptedSleep([stop.set]))
    acquired = await coord._acquire(asyncio.Event())
    assert acquired is not None
    await coord._hold(stop, acquired)  # first pause sets stop -> returns immediately


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
    with pytest.raises(ImportError):
        import kubernetes_asyncio  # noqa: F401

    # If we got here the extra genuinely isn't installed in this env, so the fact
    # that sf2loki.coordinate.k8s_lease imported cleanly proves the lazy boundary
    # holds. Reload to confirm it's still importable now.
    import importlib

    import sf2loki.coordinate.k8s_lease as k8s_lease_module

    importlib.reload(k8s_lease_module)
    assert hasattr(k8s_lease_module, "K8sLeaseCoordinator")


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
