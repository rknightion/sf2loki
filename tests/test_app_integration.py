"""Integration tests for App.build wiring (no network)."""

from __future__ import annotations

import asyncio
from typing import Any

import pytest

from sf2loki.app import App, _drain_with_grace
from sf2loki.config import Config
from sf2loki.sinks.loki.labels import LabelGuardError
from sf2loki.sources.eventlog_objects_source import EventLogObjectsSource
from sf2loki.sources.eventlogfile_source import EventLogFileSource
from sf2loki.sources.overlap import OverlapError
from sf2loki.sources.pubsub_source import PubSubSource


def _cfg(**over: Any) -> Config:
    base: dict[str, Any] = {
        "salesforce": {
            "client_id": "cid",
            "username": "svc@example.com",
            "private_key": "DUMMY",
        },
        "sink": {"loki": {"url": "http://loki:3100/loki/api/v1/push"}},
        "sources": {
            "pubsub": {"enabled": False},
            "eventlog_objects": {"enabled": False},
            "eventlogfile": {"enabled": False},
        },
    }
    for k, v in over.items():
        base[k] = v
    return Config(**base)


def _source_types(appn: App) -> list[type]:
    return [type(s) for s in appn._pipeline._sources]


def test_build_pubsub_only() -> None:
    cfg = _cfg(sources={"pubsub": {"enabled": True, "topics": ["/event/LoginEventStream"]}})
    appn = App.build(cfg)
    assert _source_types(appn) == [PubSubSource]


def test_build_eventlog_objects_only() -> None:
    cfg = _cfg(
        sources={
            "pubsub": {"enabled": False},
            "eventlog_objects": {"enabled": True, "objects": [{"name": "LoginEvent"}]},
        }
    )
    appn = App.build(cfg)
    assert _source_types(appn) == [EventLogObjectsSource]


def test_build_all_sources() -> None:
    cfg = _cfg(
        sources={
            "pubsub": {"enabled": True, "topics": ["/event/X"]},
            "eventlog_objects": {"enabled": True, "objects": [{"name": "LoginEvent"}]},
            # Disjoint categories (x / login / report) so the overlap guard passes.
            "eventlogfile": {"enabled": True, "event_types": ["Report"]},
        }
    )
    appn = App.build(cfg)
    assert set(_source_types(appn)) == {
        PubSubSource,
        EventLogObjectsSource,
        EventLogFileSource,
    }


def test_build_no_sources() -> None:
    appn = App.build(_cfg())
    assert _source_types(appn) == []


def test_build_rejects_disallowed_label() -> None:
    cfg = _cfg(sink={"loki": {"url": "http://x/loki/api/v1/push", "labels": {"user_id": "bad"}}})
    with pytest.raises(LabelGuardError):
        App.build(cfg)


def test_build_rejects_overlapping_category() -> None:
    # Streaming LoginEventStream AND polling LoginEvent = the same data twice.
    cfg = _cfg(
        sources={
            "pubsub": {"enabled": True, "topics": ["/event/LoginEventStream"]},
            "eventlog_objects": {"enabled": True, "objects": [{"name": "LoginEvent"}]},
        }
    )
    with pytest.raises(OverlapError):
        App.build(cfg)


def test_build_allows_overlap_when_opted_in() -> None:
    cfg = _cfg(
        sources={
            "allow_overlap": True,
            "pubsub": {"enabled": True, "topics": ["/event/LoginEventStream"]},
            "eventlog_objects": {"enabled": True, "objects": [{"name": "LoginEvent"}]},
        }
    )
    appn = App.build(cfg)
    assert {PubSubSource, EventLogObjectsSource} == set(_source_types(appn))


# ---------------------------------------------------------------------------
# _drain_with_grace: bounds how long shutdown waits after `stop` fires
# ---------------------------------------------------------------------------


async def test_drain_with_grace_returns_once_coro_finishes_on_its_own() -> None:
    """A coroutine that finishes before stop is ever set returns immediately."""

    async def quick() -> None:
        return None

    stop = asyncio.Event()
    await asyncio.wait_for(_drain_with_grace(quick(), stop, grace=10.0), timeout=1.0)


async def test_drain_with_grace_lets_a_stop_aware_coro_finish_within_grace() -> None:
    """A coroutine that notices stop and exits quickly returns well before grace expires."""

    async def stop_aware() -> None:
        await stop.wait()

    stop = asyncio.Event()

    async def trigger_stop() -> None:
        await asyncio.sleep(0.05)
        stop.set()

    trigger_task = asyncio.create_task(trigger_stop())
    try:
        await asyncio.wait_for(_drain_with_grace(stop_aware(), stop, grace=10.0), timeout=1.0)
    finally:
        await trigger_task


async def test_drain_with_grace_force_cancels_after_grace_expires() -> None:
    """A coroutine that ignores stop is force-cancelled once grace elapses."""

    async def ignores_stop() -> None:
        await asyncio.sleep(1000)

    stop = asyncio.Event()
    stop.set()  # already "shutting down"

    # grace=0.1s: must return promptly (force-cancelled), not hang for 1000s.
    await asyncio.wait_for(_drain_with_grace(ignores_stop(), stop, grace=0.1), timeout=1.0)


async def test_drain_with_grace_propagates_exception_from_finished_task() -> None:
    """A real exception raised before stop fires propagates, not swallowed."""

    async def fails() -> None:
        raise ValueError("boom")

    stop = asyncio.Event()
    with pytest.raises(ValueError, match="boom"):
        await asyncio.wait_for(_drain_with_grace(fails(), stop, grace=10.0), timeout=1.0)
