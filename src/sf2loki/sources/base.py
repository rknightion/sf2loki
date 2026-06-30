"""The Source seam: every ingest module is an async generator of LogEntry.

A single ``Pipeline`` consumes ``events()`` from all enabled sources, so
streaming (Pub/Sub) and polling (SOQL) sources share all batching, push, and
checkpoint logic. Each yielded entry carries the ``CheckpointToken`` that
becomes durable once it — and everything before it on the same key — is pushed.
"""

from __future__ import annotations

import asyncio
from collections.abc import AsyncIterator
from typing import TYPE_CHECKING, Protocol

from sf2loki.model import LogEntry

if TYPE_CHECKING:
    from sf2loki.state.base import CheckpointStore


class Source(Protocol):
    name: str

    def events(self, state: CheckpointStore, stop: asyncio.Event) -> AsyncIterator[LogEntry]: ...
