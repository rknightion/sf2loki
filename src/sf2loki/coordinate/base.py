"""The Coordinator seam: leadership for active-passive HA (future).

Single-instance deployments use ``NoopCoordinator`` (always leader). A
lease-based coordinator can be added later with no changes to sources/sink.
"""

from __future__ import annotations

import asyncio
from collections.abc import Awaitable, Callable
from typing import Protocol


class StateFenceError(RuntimeError):
    """A checkpoint commit was attempted by an instance that is not the leader.

    Raised by :meth:`FileLeaseCoordinator.check_fence` (wired into the state
    store as a pre-commit fence) so a *stale* leader — one that lost the lease
    during a GC/scheduling pause but has an in-flight commit — cannot advance
    checkpoints and race the new leader. The pushed data is already durable in
    the sink, so a fenced commit costs at most a bounded re-ingest after the
    new leader resumes (at-least-once), never data loss.

    Home: this lives in the ``coordinate`` package (not ``state``) because the
    fence is a leadership contract; the state store stays agnostic and only
    invokes an opaque fence callable.
    """


class Coordinator(Protocol):
    async def run(
        self,
        *,
        on_acquire: Callable[[], Awaitable[None]],
        on_lose: Callable[[], Awaitable[None]],
        stop: asyncio.Event,
    ) -> None: ...


class NoopCoordinator:
    """Always the leader: acquire immediately, hold leadership until ``stop``."""

    async def run(
        self,
        *,
        on_acquire: Callable[[], Awaitable[None]],
        on_lose: Callable[[], Awaitable[None]],
        stop: asyncio.Event,
    ) -> None:
        await on_acquire()
        await stop.wait()
