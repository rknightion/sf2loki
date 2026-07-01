"""The Coordinator seam: leadership for active-passive HA (future).

Single-instance deployments use ``NoopCoordinator`` (always leader). A
lease-based coordinator can be added later with no changes to sources/sink.
"""

from __future__ import annotations

import asyncio
from collections.abc import Awaitable, Callable
from typing import Protocol


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
