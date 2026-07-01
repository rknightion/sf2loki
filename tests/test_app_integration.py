"""Integration tests for App.build wiring (no network)."""

from __future__ import annotations

import asyncio
import time
from typing import Any, ClassVar

import httpx
import pytest

from sf2loki import app as app_module
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


@pytest.mark.parametrize("key", ["source", "event_type"])
def test_build_rejects_reserved_static_label(key: str) -> None:
    """Static sink.loki.labels overriding per-entry identity labels would collapse
    all stream separation — must fail fast at startup."""
    cfg = _cfg(sink={"loki": {"url": "http://x/loki/api/v1/push", "labels": {key: "x"}}})
    with pytest.raises(LabelGuardError, match=key):
        App.build(cfg)


def test_build_sets_explicit_http_timeouts() -> None:
    """Both shared clients get explicit timeouts (not httpx's 5s-everywhere default)."""
    appn = App.build(_cfg())
    for client in (appn._pipeline._sink._client, appn._tokens._client):  # type: ignore[attr-defined]
        t = client.timeout
        assert t.connect == 10.0
        assert t.read == 30.0
        assert t.write == 30.0
        assert t.pool == 30.0


# ---------------------------------------------------------------------------
# Startup auth probe: bad credentials fail fast in every configuration
# ---------------------------------------------------------------------------


class _BadCredsTokens:
    """TokenProvider stand-in whose mint always fails (bad credentials)."""

    async def token(self) -> object:
        raise RuntimeError("bad credentials")

    async def org_id(self) -> str:
        raise AssertionError("org_id must not be needed when salesforce.org_id is set")


async def test_run_fails_fast_on_bad_credentials_even_with_org_id_configured() -> None:
    """With org_id configured, run() must still mint a token before set_ready so
    bad credentials exit nonzero at startup instead of retrying forever."""
    cfg = _cfg(
        salesforce={
            "client_id": "cid",
            "username": "svc@example.com",
            "private_key": "DUMMY",
            "org_id": "00D000000000001",
        },
        service={"health_addr": "127.0.0.1:0"},
    )
    appn = App.build(cfg)
    appn._tokens = _BadCredsTokens()  # type: ignore[assignment]
    try:
        with pytest.raises(RuntimeError, match="bad credentials"):
            await asyncio.wait_for(appn.run(), timeout=5)
        assert not appn._health.ready  # never went ready
    finally:
        await appn._health.stop()


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
# Queue bounds wiring (issue #16)
# ---------------------------------------------------------------------------


def test_pipeline_queue_maxsize_wired_from_batch_config() -> None:
    cfg = _cfg(sink={"loki": {"url": "http://x/loki/api/v1/push", "batch": {"queue_maxsize": 250}}})
    appn = App.build(cfg)
    assert appn._pipeline._queue_maxsize == 250


# ---------------------------------------------------------------------------
# Readiness degradation wiring (issue #17)
# ---------------------------------------------------------------------------


async def test_readyz_degrades_after_prolonged_sink_failure_and_recovers() -> None:
    cfg = _cfg(service={"health_addr": "127.0.0.1:0", "unready_after_sink_failing": "10m"})
    appn = App.build(cfg)
    health = appn._health
    await health.start("127.0.0.1:0")
    try:
        health.set_ready()
        async with httpx.AsyncClient() as client:
            base = f"http://127.0.0.1:{health.port}"
            r = await client.get(f"{base}/readyz")
            assert r.status_code == 200

            # Sink failing continuously for an hour (> 10m threshold).
            appn._pipeline._sink_failing_since = time.monotonic() - 3600
            r = await client.get(f"{base}/readyz")
            assert r.status_code == 503
            assert r.text.startswith("degraded: loki pushes failing for")
            # Liveness must NOT degrade for sink failures.
            r = await client.get(f"{base}/healthz")
            assert r.status_code == 200

            # Failing for less than the threshold: still ready.
            appn._pipeline._sink_failing_since = time.monotonic() - 60
            r = await client.get(f"{base}/readyz")
            assert r.status_code == 200

            # Next successful push clears the mark -> ready again.
            appn._pipeline._sink_failing_since = None
            r = await client.get(f"{base}/readyz")
            assert r.status_code == 200
    finally:
        await health.stop()


async def test_readyz_degradation_disabled_when_threshold_zero() -> None:
    cfg = _cfg(service={"health_addr": "127.0.0.1:0", "unready_after_sink_failing": 0})
    appn = App.build(cfg)
    health = appn._health
    await health.start("127.0.0.1:0")
    try:
        health.set_ready()
        appn._pipeline._sink_failing_since = time.monotonic() - 10**6
        async with httpx.AsyncClient() as client:
            r = await client.get(f"http://127.0.0.1:{health.port}/readyz")
        assert r.status_code == 200
    finally:
        await health.stop()


# ---------------------------------------------------------------------------
# owned_categories wiring into PubSubSource (mirror of the ELF exclusion)
# ---------------------------------------------------------------------------


class _RecordingPubSubSource:
    """Stands in for PubSubSource to capture App.build's constructor kwargs."""

    captured: ClassVar[dict[str, Any]] = {}

    def __init__(self, cfg: Any, client: Any, **kwargs: Any) -> None:
        type(self).captured = dict(kwargs)

    def resolve_topics(self) -> list[str]:
        return []


def test_build_passes_owned_categories_to_pubsub(monkeypatch: pytest.MonkeyPatch) -> None:
    """Pub/Sub must be told which categories eventlog_objects + explicit ELF
    event types own, so its wildcard discovery can auto-exclude them."""
    monkeypatch.setattr(app_module, "PubSubSource", _RecordingPubSubSource)
    cfg = _cfg(
        sources={
            "pubsub": {"enabled": True, "topics": ["/event/ApiEventStream"]},
            "eventlog_objects": {"enabled": True, "objects": [{"name": "LoginEvent"}]},
            "eventlogfile": {"enabled": True, "event_types": ["Report"]},
        }
    )
    App.build(cfg)
    assert _RecordingPubSubSource.captured["owned_categories"] == frozenset({"login", "report"})


def test_build_passes_empty_owned_categories_when_overlap_allowed(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(app_module, "PubSubSource", _RecordingPubSubSource)
    cfg = _cfg(
        sources={
            "allow_overlap": True,
            "pubsub": {"enabled": True, "topics": ["/event/LoginEventStream"]},
            "eventlog_objects": {"enabled": True, "objects": [{"name": "LoginEvent"}]},
        }
    )
    App.build(cfg)
    assert _RecordingPubSubSource.captured["owned_categories"] == frozenset()


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
