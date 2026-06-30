"""Tests for EventLogFileSource stub (Phase 3 / DESIGN.md §8)."""

from __future__ import annotations

import asyncio
from unittest.mock import MagicMock

import pytest

from sf2loki.config import EventLogFileConfig
from sf2loki.sources.eventlogfile_source import EventLogFileSource


class TestEventLogFileSource:
    def _cfg(self, *, enabled: bool = False) -> EventLogFileConfig:
        return EventLogFileConfig(enabled=enabled)

    def test_name(self) -> None:
        src = EventLogFileSource(self._cfg())
        assert src.name == "eventlogfile"

    def test_events_raises_not_implemented(self) -> None:
        src = EventLogFileSource(self._cfg())
        store = MagicMock()
        with pytest.raises(NotImplementedError) as exc_info:
            src.events(store, asyncio.Event())
        msg = str(exc_info.value)
        assert "DESIGN" in msg
        assert "§8" in msg or "8" in msg

    def test_structural_source_compatibility(self) -> None:
        # Verify EventLogFileSource satisfies the Source protocol structurally.
        from sf2loki.sources.base import Source

        src: Source = EventLogFileSource(self._cfg())
        assert src.name == "eventlogfile"
