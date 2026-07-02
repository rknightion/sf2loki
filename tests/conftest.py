"""Shared test helpers."""

from __future__ import annotations

import asyncio
from collections.abc import Awaitable, Callable

import pytest

from sf2loki.config import Config


@pytest.fixture
def config_with() -> Callable[..., Config]:
    """Factory for a minimal valid Config, with shallow per-section overrides."""

    def _make(**overrides: object) -> Config:
        base: dict[str, object] = {
            "salesforce": {
                "client_id": "cid",
                "username": "svc@example.com",
                "private_key": "DUMMYKEY",
            },
            "sink": {"loki": {"url": "http://loki:3100/loki/api/v1/push"}},
        }
        base.update(overrides)
        return Config(**base)

    return _make


@pytest.fixture
def wait_until() -> Callable[..., Awaitable[None]]:
    """Factory for a bounded poll-for-condition coroutine.

    Replaces a fixed ``await asyncio.sleep(N)`` used as a synchronization
    primitive ("let the other task reach some state") with a deterministic
    poll: it returns as soon as *predicate* becomes true (no wasted wall
    time) and raises loudly if it never does within *timeout_s* (no silent
    false-negative pass the way an under-sized fixed sleep could produce
    under load).

    Parameter named ``timeout_s`` (not ``timeout``) to sidestep ASYNC109
    (which wants an ``asyncio.timeout()`` context manager for a `timeout`
    param) — that would cancel the polling loop itself rather than just
    reporting the condition never held, which is the wrong failure mode here.
    """

    async def _wait_until(
        predicate: Callable[[], bool],
        *,
        timeout_s: float = 2.0,
        interval: float = 0.001,
    ) -> None:
        loop = asyncio.get_running_loop()
        deadline = loop.time() + timeout_s
        while not predicate():
            if loop.time() >= deadline:
                raise AssertionError(f"condition not met within {timeout_s}s")
            await asyncio.sleep(interval)
        await asyncio.sleep(0)  # one more yield so any pending callback settles

    return _wait_until
