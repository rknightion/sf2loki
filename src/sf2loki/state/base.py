"""The CheckpointStore seam: durable resume state, keyed per source stream."""

from __future__ import annotations

from typing import Protocol


class CheckpointStore(Protocol):
    async def load(self, key: str) -> str | None: ...

    async def commit(self, key: str, value: str) -> None: ...
