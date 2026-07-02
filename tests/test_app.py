"""App.build wiring tests for the egress governor + composite readiness degradation."""

from __future__ import annotations

import time
from datetime import date
from typing import Any

import httpx

from sf2loki.app import App
from sf2loki.config import Config
from sf2loki.egress import EgressGovernor


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


# ---------------------------------------------------------------------------
# Governor construction / wiring


def test_no_governor_when_egress_unconfigured() -> None:
    appn = App.build(_cfg())
    assert appn._pipeline._governor is None


def test_governor_wired_when_rate_cap_set() -> None:
    appn = App.build(
        _cfg(sink={"loki": {"url": "http://x/push", "egress": {"max_lines_per_second": 100}}})
    )
    assert isinstance(appn._pipeline._governor, EgressGovernor)


def test_governor_wired_when_budget_set() -> None:
    appn = App.build(
        _cfg(sink={"loki": {"url": "http://x/push", "egress": {"daily_byte_budget": 1000}}})
    )
    assert isinstance(appn._pipeline._governor, EgressGovernor)


# ---------------------------------------------------------------------------
# Composite readiness degradation


async def test_no_governor_preserves_sink_only_degradation() -> None:
    cfg = _cfg(service={"health_addr": "127.0.0.1:0", "unready_after_sink_failing": "10m"})
    appn = App.build(cfg)
    assert appn._pipeline._governor is None
    health = appn._health
    await health.start("127.0.0.1:0")
    try:
        health.set_ready()
        lane = appn._pipeline._new_lane()  # failing-since is per-lane now (issue #53)
        async with httpx.AsyncClient() as client:
            base = f"http://127.0.0.1:{health.port}"
            # Sink failing beyond threshold -> degraded (unchanged behaviour).
            lane.failing_since = time.monotonic() - 3600
            r = await client.get(f"{base}/readyz")
            assert r.status_code == 503
            assert r.text.startswith("degraded: loki pushes failing for")
            # Recovers when the mark clears.
            lane.failing_since = None
            r = await client.get(f"{base}/readyz")
            assert r.status_code == 200
    finally:
        await health.stop()


async def test_paused_budget_reason_wins_over_sink_failing() -> None:
    cfg = _cfg(
        service={"health_addr": "127.0.0.1:0", "unready_after_sink_failing": "10m"},
        sink={
            "loki": {
                "url": "http://x/push",
                "egress": {"daily_byte_budget": 100, "budget_action": "pause"},
            }
        },
    )
    appn = App.build(cfg)
    gov = appn._pipeline._governor
    assert isinstance(gov, EgressGovernor)
    health = appn._health
    await health.start("127.0.0.1:0")
    try:
        health.set_ready()
        lane = appn._pipeline._new_lane()  # failing-since is per-lane now (issue #53)
        async with httpx.AsyncClient() as client:
            base = f"http://127.0.0.1:{health.port}"
            # Sink also failing beyond threshold...
            lane.failing_since = time.monotonic() - 3600
            # ...but the budget-pause reason is checked first and wins.
            gov._paused = True
            gov._date = date(2026, 7, 1)
            r = await client.get(f"{base}/readyz")
            assert r.status_code == 503
            assert "daily byte budget exhausted" in r.text
            assert "2026-07-02" in r.text
            # Not paused -> falls through to the sink-failing reason.
            gov._paused = False
            r = await client.get(f"{base}/readyz")
            assert r.status_code == 503
            assert r.text.startswith("degraded: loki pushes failing for")
    finally:
        await health.stop()


async def test_drop_mode_budget_does_not_degrade_readiness() -> None:
    cfg = _cfg(
        service={"health_addr": "127.0.0.1:0", "unready_after_sink_failing": 0},
        sink={
            "loki": {
                "url": "http://x/push",
                "egress": {"daily_byte_budget": 100, "budget_action": "drop"},
            }
        },
    )
    appn = App.build(cfg)
    gov = appn._pipeline._governor
    assert isinstance(gov, EgressGovernor)
    health = appn._health
    await health.start("127.0.0.1:0")
    try:
        health.set_ready()
        gov._paused = True  # drop mode never reports degraded even if flagged
        async with httpx.AsyncClient() as client:
            r = await client.get(f"http://127.0.0.1:{health.port}/readyz")
        assert r.status_code == 200
    finally:
        await health.stop()


def test_apexlog_source_built_when_enabled() -> None:
    from sf2loki.sources.apexlog_source import ApexLogSource

    appn = App.build(
        _cfg(
            sources={
                "pubsub": {"enabled": False},
                "eventlog_objects": {"enabled": False},
                "eventlogfile": {"enabled": False},
                "apexlog": {"enabled": True},
            }
        )
    )
    assert any(isinstance(s, ApexLogSource) for s in appn._pipeline._sources)


def test_apexlog_source_absent_when_disabled() -> None:
    from sf2loki.sources.apexlog_source import ApexLogSource

    appn = App.build(_cfg())
    assert not any(isinstance(s, ApexLogSource) for s in appn._pipeline._sources)
