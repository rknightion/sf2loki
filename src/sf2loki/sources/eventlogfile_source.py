"""EventLogFile source stub — Phase 3 placeholder (DESIGN.md §8).

This module satisfies the Source protocol structurally but raises
``NotImplementedError`` immediately on ``events()``; it exists so the rest
of the pipeline can reference it before ingestion logic is built out.
"""

from __future__ import annotations

import asyncio
from collections.abc import AsyncIterator

from sf2loki.config import EventLogFileConfig
from sf2loki.model import LogEntry
from sf2loki.state.base import CheckpointStore


class EventLogFileSource:
    """Unimplemented EventLogFile ingest source (DESIGN.md §8, Phase 3)."""

    name = "eventlogfile"

    def __init__(self, cfg: EventLogFileConfig) -> None:
        self._cfg = cfg

    def events(self, state: CheckpointStore, stop: asyncio.Event) -> AsyncIterator[LogEntry]:
        raise NotImplementedError(
            "EventLogFile ingestion is not yet implemented; see DESIGN.md §8 (Phase 3)."
        )
