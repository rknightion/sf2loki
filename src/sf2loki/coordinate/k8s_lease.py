"""Kubernetes-Lease :class:`Coordinator`: active-passive failover via ``coordination.k8s.io/v1``.

The leader renews a Lease object (``holderIdentity`` + ``renewTime``); a
standby watches and takes over once the lease has gone stale. Staleness is
judged with client-go's ``observedTime`` pattern, never by comparing the
leader-written ``renewTime`` against the observer's own wall clock: each
coordinator tracks (on its own injected monotonic clock) when it last saw
this Lease's ``resourceVersion`` change, and only treats it as expired once
``lease_duration`` seconds have elapsed on THAT clock. This deliberately
ignores cross-host wall-clock skew — see ``_observe``/``_Lease.is_stale`` and
GH #51. Optimistic concurrency uses the Lease's ``resourceVersion``: a lost
compare-and-swap comes back as HTTP 409, which doubles as the race signal —
unlike the file lease, no pause-then-verify re-read is needed after a
contested write.

This module mirrors ``coordinate/file_lease.py``'s ``run`` → ``_acquire`` →
``_hold`` → ``_pause`` loop shape, injected ``utcnow``/``sleep``, and
``check_fence``/``is_leader``/``holder`` surface.

The coordinator talks to a thin adapter (``read_lease``/``create_lease``/
``replace_lease`` over ``_LeaseBody``/``_Lease``), never the raw
``CoordinationV1Api`` or a ``V1Lease``, so this module never imports
``kubernetes_asyncio`` at top level. That keeps it importable — and
unit-testable with an injected fake adapter — even when the optional ``k8s``
extra is not installed; only actually using the default adapter factory
without the extra raises ``ImportError``.
"""

from __future__ import annotations

import asyncio
import contextlib
import os
import time
from collections.abc import AsyncIterator, Awaitable, Callable
from contextlib import AbstractAsyncContextManager, asynccontextmanager
from dataclasses import dataclass
from datetime import UTC, datetime
from typing import TYPE_CHECKING, Any, Protocol

from sf2loki.coordinate.base import StateFenceError
from sf2loki.obs.logging import get_logger

if TYPE_CHECKING:
    from sf2loki.config import K8sLeaseConfig

log = get_logger(__name__)

K8sApiFactory = Callable[[], AbstractAsyncContextManager[Any]]


def _default_utcnow() -> datetime:
    return datetime.now(UTC)


@dataclass(frozen=True, slots=True)
class _Lease:
    """``holderIdentity``/``renewTime``/``leaseDurationSeconds`` are all
    OPTIONAL in ``coordination.k8s.io/v1`` (a bare ``kubectl create``d Lease,
    or one written by another controller, may leave any of them unset) —
    ``renew_time``/``duration`` are ``None`` rather than assumed present
    (GH #62); ``holder`` is ``""`` (never ``None``) when ``holderIdentity`` is
    unset, meaning unheld.
    """

    holder: str
    renew_time: datetime | None
    duration: float | None
    resource_version: str

    def is_stale(self, staleness: float) -> bool:
        """True if this lease should be treated as expired/claimable.

        *staleness* is seconds elapsed on the OBSERVER's own monotonic clock
        since this lease's ``resource_version`` was last seen to change (see
        ``K8sLeaseCoordinator._observe``) — never the leader's wall-clock
        ``renew_time`` (GH #51: cross-host wall-clock skew must never cause a
        premature takeover).

        A Lease that has never been renewed (``renew_time``/``duration``
        missing) or that nobody currently holds (``holder`` unset) is
        immediately claimable — there is no active renewal to protect
        against clock skew for, so no observation window is needed (GH #62).
        """
        if not self.holder or self.renew_time is None or self.duration is None:
            return True
        return staleness >= self.duration


@dataclass(frozen=True, slots=True)
class _LeaseBody:
    """What the coordinator asks the adapter to write.

    ``resource_version`` is ``None`` for a create (no lease exists yet) and
    set to the last-known version for a replace (carries the optimistic-lock
    check into the adapter/API call).
    """

    holder: str
    renew_time: datetime
    duration: float
    resource_version: str | None


class _LeaseApi(Protocol):
    """The narrow adapter contract the coordinator depends on.

    Deliberately not the raw ``CoordinationV1Api`` (which needs ``V1Lease``
    objects and would force a top-level ``kubernetes_asyncio`` import).
    Satisfied by both the real adapter (:class:`_RealLeaseAdapter`) and any
    test fake.
    """

    async def read_lease(self) -> _Lease | None: ...
    async def create_lease(self, body: _LeaseBody) -> _Lease: ...
    async def replace_lease(self, body: _LeaseBody) -> _Lease: ...


def _status(exc: Exception) -> int | None:
    """HTTP status from a ``kubernetes_asyncio`` ``ApiException``-shaped error.

    Duck-typed (``getattr(exc, "status", None)``) rather than
    ``except ApiException`` so this module never needs to import
    ``kubernetes_asyncio`` just to catch its exception type.
    """
    status = getattr(exc, "status", None)
    return status if isinstance(status, int) else None


_NOT_FOUND = 404
_CONFLICT = 409


class K8sLeaseCoordinator:
    """Lease-based leader election over a Kubernetes ``Lease`` object.

    ``utcnow`` supplies the wall clock written into the lease's ``renewTime``
    (injected in tests) — it is never read back for expiry math (GH #51);
    ``monotonic`` supplies the observer's own clock that IS used for expiry,
    via the ``observedTime`` pattern (see ``_observe``); ``sleep`` performs
    the interval waits (injected so tests never sleep for real). ``holder``
    is the ``holderIdentity`` written into the lease — the app derives
    ``hostname-pid`` when config and ``$HOSTNAME`` are both blank.

    ``run`` owns the adapter's lifecycle: the ``Coordinator`` protocol has no
    ``close()`` and ``app.py`` never closes the coordinator, so the
    api-factory context manager is entered at the top of ``run`` and exited
    in its ``finally`` — never held open outside a ``run`` call.
    """

    def __init__(
        self,
        cfg: K8sLeaseConfig,
        *,
        holder: str | None = None,
        utcnow: Callable[[], datetime] = _default_utcnow,
        sleep: Callable[[float], Awaitable[None]] = asyncio.sleep,
        monotonic: Callable[[], float] = time.monotonic,
        api_factory: K8sApiFactory | None = None,
    ) -> None:
        self._cfg = cfg
        self._duration: float = cfg.lease_duration.total_seconds()
        self._renew: float = cfg.renew_interval.total_seconds()
        self._holder: str = (
            holder
            or cfg.identity
            or os.environ.get("HOSTNAME")
            or f"{os.uname().nodename}-{os.getpid()}"
        )
        self._utcnow = utcnow
        self._sleep = sleep
        self._monotonic = monotonic
        self._api_factory = api_factory
        self._api: _LeaseApi | None = None
        self._is_leader: bool = False
        # observedTime bookkeeping (GH #51): the resource_version we last saw
        # and the monotonic time at which we first saw it — never reset by
        # wall-clock reads, only by an actual observed content change.
        self._observed_version: str | None = None
        self._observed_at: float = 0.0

    # ------------------------------------------------------------------
    # Fencing contract (consumed by the state store via set_fence)

    @property
    def is_leader(self) -> bool:
        return self._is_leader

    def check_fence(self) -> None:
        """Raise :class:`StateFenceError` unless this instance currently leads.

        Wired into the checkpoint store as a pre-commit fence so a stale
        leader cannot advance checkpoints after losing the lease.
        """
        if not self._is_leader:
            raise StateFenceError(
                f"refusing checkpoint commit: {self._holder} does not hold the "
                f"lease {self._cfg.namespace}/{self._cfg.name} (leadership lost) — "
                "the new leader owns the checkpoints now"
            )

    @property
    def holder(self) -> str:
        return self._holder

    @property
    def _require_api(self) -> _LeaseApi:
        """The adapter set by ``run()`` for the duration of its call.

        Only ``_acquire``/``_hold``/``_read`` reach for this, and only ever
        while ``run`` is on the stack (it sets ``self._api`` before entering
        the acquire/hold loop and clears it in ``finally``), so the assert
        should never fire in practice — it exists to keep mypy strict happy
        without leaking ``Any`` through the adapter boundary.
        """
        assert self._api is not None, "k8s lease api not initialized; call within run()"
        return self._api

    # ------------------------------------------------------------------
    # Coordinator protocol

    async def run(
        self,
        *,
        on_acquire: Callable[[], Awaitable[None]],
        on_lose: Callable[[], Awaitable[None]],
        stop: asyncio.Event,
    ) -> None:
        """Standby → acquire → on_acquire → hold → on_lose → standby, until stop.

        ``on_acquire`` and ``on_lose`` are awaited in strict alternation:
        every acquisition is paired with exactly one loss via
        ``try/finally``. Owns the api adapter's lifecycle for the duration
        of the call.
        """
        factory = self._api_factory or self._default_api_factory
        async with factory() as api:
            self._api = api
            try:
                while not stop.is_set():
                    acquired = await self._acquire(stop)
                    if acquired is None:
                        return  # stop fired while standing by
                    self._is_leader = True
                    log.info(
                        "acquired k8s lease",
                        holder=self._holder,
                        lease=f"{self._cfg.namespace}/{self._cfg.name}",
                    )
                    await on_acquire()
                    try:
                        await self._hold(stop, acquired)
                    finally:
                        self._is_leader = False
                        log.info(
                            "released k8s lease",
                            holder=self._holder,
                            lease=f"{self._cfg.namespace}/{self._cfg.name}",
                        )
                        await on_lose()
            finally:
                self._is_leader = False
                self._api = None

    # ------------------------------------------------------------------
    # observedTime bookkeeping (GH #51)

    def _observe(self, lease: _Lease) -> float:
        """Record when *lease*'s ``resource_version`` was first seen (on our
        own monotonic clock), returning the staleness — seconds elapsed since
        then — as of this call.

        This is client-go leaderelection's ``observedTime`` pattern: a
        content change (any successful renew or takeover bumps
        ``resourceVersion``) resets the window; the leader's/observer's wall
        clock never enters into it, so cross-host clock skew cannot cause a
        premature (or a delayed) takeover.
        """
        now = self._monotonic()
        if lease.resource_version != self._observed_version:
            self._observed_version = lease.resource_version
            self._observed_at = now
        return now - self._observed_at

    # ------------------------------------------------------------------
    # Standby / acquire

    async def _acquire(self, stop: asyncio.Event) -> _Lease | None:
        """Block until this instance owns the lease; return the acquired lease
        (whose ``resource_version`` seeds the renew loop), or ``None`` if stop
        fires while standing by."""
        while not stop.is_set():
            lease = await self._read()
            now = self._utcnow()
            if lease is None:
                try:
                    acquired = await self._require_api.create_lease(
                        _LeaseBody(
                            holder=self._holder,
                            renew_time=now,
                            duration=self._duration,
                            resource_version=None,
                        )
                    )
                except Exception as exc:
                    if _status(exc) == _CONFLICT:
                        log.info("lost k8s-lease create race; backing off", holder=self._holder)
                        if await self._pause(self._renew, stop):
                            return None
                        continue
                    log.warning("cannot create k8s lease; retrying", error=str(exc))
                    if await self._pause(self._renew, stop):
                        return None
                    continue
                return acquired
            elif lease.is_stale(self._observe(lease)):
                try:
                    acquired = await self._require_api.replace_lease(
                        _LeaseBody(
                            holder=self._holder,
                            renew_time=now,
                            duration=self._duration,
                            resource_version=lease.resource_version,
                        )
                    )
                except Exception as exc:
                    if _status(exc) == _CONFLICT:
                        log.info("lost k8s-lease replace race; backing off", holder=self._holder)
                        if await self._pause(self._renew, stop):
                            return None
                        continue
                    log.warning("cannot replace k8s lease; retrying", error=str(exc))
                    if await self._pause(self._renew, stop):
                        return None
                    continue
                return acquired
            else:
                # A live foreign holder: poll at the renew interval until expiry.
                if await self._pause(self._renew, stop):
                    return None
        return None

    # ------------------------------------------------------------------
    # Hold / renew

    async def _hold(self, stop: asyncio.Event, acquired: _Lease) -> None:
        """Renew the lease until leadership is lost or stop fires.

        ``acquired`` is the lease returned by :meth:`_acquire`; its
        ``resource_version`` seeds the first renew, so we skip an opening
        read — the create/replace that won leadership already handed it to us.
        """
        last_ok = self._utcnow()
        resource_version: str | None = acquired.resource_version
        while not stop.is_set():
            if await self._pause(self._renew, stop):
                return
            now = self._utcnow()
            # Re-read before renewing: a foreign holder means we were fenced
            # out during a pause/GC gap — surrender immediately.
            lease = await self._read()
            if lease is not None and lease.holder != self._holder:
                log.warning(
                    "k8s lease taken over by another holder; surrendering",
                    holder=self._holder,
                    new_holder=lease.holder,
                )
                return
            if lease is not None:
                resource_version = lease.resource_version
            try:
                renewed = await self._require_api.replace_lease(
                    _LeaseBody(
                        holder=self._holder,
                        renew_time=now,
                        duration=self._duration,
                        resource_version=resource_version,
                    )
                )
                resource_version = renewed.resource_version
                last_ok = now
            except Exception as exc:
                status = _status(exc)
                if status == _CONFLICT:
                    log.warning(
                        "k8s lease renewal lost the CAS; surrendering",
                        holder=self._holder,
                    )
                    return
                if status == _NOT_FOUND:
                    # The Lease was deleted out from under us (e.g. kubectl delete).
                    # Surrender immediately so a standby can recreate and lead —
                    # holding is_leader here would leave a bounded split-brain
                    # window until the create/recreate race resolves.
                    log.warning(
                        "k8s lease disappeared (deleted); surrendering",
                        holder=self._holder,
                    )
                    return
                # Can't reach the API. Tolerate transient failures, but if
                # we've been unable to renew for a full lease_duration the
                # lease has (or will have) expired for everyone — assume
                # we've lost it.
                if (now - last_ok).total_seconds() >= self._duration:
                    log.warning(
                        "k8s lease renewal failing past lease_duration; surrendering",
                        holder=self._holder,
                        error=str(exc),
                    )
                    return
                log.warning("k8s lease renewal failed; will retry", error=str(exc))

    # ------------------------------------------------------------------
    # Timing helper

    async def _pause(self, seconds: float, stop: asyncio.Event) -> bool:
        """Sleep up to *seconds*, returning early once stop is set.

        Returns True if stop fired (caller should give up), else False.
        """
        if stop.is_set():
            return True
        sleeper = asyncio.ensure_future(self._sleep(seconds))
        waiter = asyncio.ensure_future(stop.wait())
        try:
            await asyncio.wait({sleeper, waiter}, return_when=asyncio.FIRST_COMPLETED)
        finally:
            for task in (sleeper, waiter):
                if not task.done():
                    task.cancel()
                    with contextlib.suppress(asyncio.CancelledError):
                        await task
        return stop.is_set()

    # ------------------------------------------------------------------
    # Adapter read

    async def _read(self) -> _Lease | None:
        """Read + parse the lease via the adapter; None if absent or unreadable."""
        try:
            return await self._require_api.read_lease()
        except Exception as exc:
            if _status(exc) == _NOT_FOUND:
                return None
            log.warning("cannot read k8s lease; treating as absent", error=str(exc))
            return None

    # ------------------------------------------------------------------
    # Default adapter factory (lazy import — needs the ``k8s`` extra)

    def _default_api_factory(self) -> AbstractAsyncContextManager[Any]:
        from kubernetes_asyncio import client, config  # type: ignore[import-not-found]

        cfg = self._cfg

        @asynccontextmanager
        async def _cm() -> AsyncIterator[Any]:
            if cfg.kubeconfig is not None:
                await config.load_kube_config(config_file=str(cfg.kubeconfig))
            else:
                config.load_incluster_config()
            api_client = client.ApiClient()
            try:
                yield _RealLeaseAdapter(client.CoordinationV1Api(api_client), cfg)
            finally:
                await api_client.close()

        return _cm()


class _RealLeaseAdapter:
    """Translates ``_LeaseBody``/``_Lease`` to/from ``kubernetes_asyncio`` ``V1Lease``.

    Only constructed inside :meth:`K8sLeaseCoordinator._default_api_factory`
    (after the lazy import), so this class is never touched — and
    ``kubernetes_asyncio`` never imported — unless the default factory is
    actually used.
    """

    def __init__(self, api: Any, cfg: K8sLeaseConfig) -> None:
        self._api = api
        self._cfg = cfg

    async def read_lease(self) -> _Lease | None:
        lease = await self._api.read_namespaced_lease(self._cfg.name, self._cfg.namespace)
        return self._from_v1_lease(lease)

    async def create_lease(self, body: _LeaseBody) -> _Lease:
        from kubernetes_asyncio import client

        v1_lease = client.V1Lease(
            metadata=client.V1ObjectMeta(name=self._cfg.name),
            spec=client.V1LeaseSpec(
                holder_identity=body.holder,
                renew_time=body.renew_time,
                lease_duration_seconds=int(body.duration),
            ),
        )
        created = await self._api.create_namespaced_lease(self._cfg.namespace, v1_lease)
        return self._from_v1_lease(created)

    async def replace_lease(self, body: _LeaseBody) -> _Lease:
        from kubernetes_asyncio import client

        v1_lease = client.V1Lease(
            metadata=client.V1ObjectMeta(
                name=self._cfg.name, resource_version=body.resource_version
            ),
            spec=client.V1LeaseSpec(
                holder_identity=body.holder,
                renew_time=body.renew_time,
                lease_duration_seconds=int(body.duration),
            ),
        )
        replaced = await self._api.replace_namespaced_lease(
            self._cfg.name, self._cfg.namespace, v1_lease
        )
        return self._from_v1_lease(replaced)

    @staticmethod
    def _from_v1_lease(lease: Any) -> _Lease:
        """``holderIdentity``/``renewTime``/``leaseDurationSeconds`` are all
        OPTIONAL on ``V1LeaseSpec`` (GH #62) — a pre-existing Lease (freshly
        ``kubectl create``d, or written by another controller) may have any
        of them unset. Map missing fields to ``_Lease``'s claimable-by-default
        sentinels instead of blowing up (``float(None)`` et al.) here or,
        worse, later in ``_Lease.is_stale``/``_hold``.
        """
        spec = lease.spec
        duration_seconds = spec.lease_duration_seconds
        return _Lease(
            holder=spec.holder_identity or "",
            renew_time=spec.renew_time,
            duration=float(duration_seconds) if duration_seconds is not None else None,
            resource_version=lease.metadata.resource_version,
        )
