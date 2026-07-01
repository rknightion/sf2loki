"""Startup logging: the app should announce what it is running on launch."""

from __future__ import annotations

from typing import Any

import structlog.testing

from sf2loki.app import App
from sf2loki.config import Config


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


def test_startup_log_lists_enabled_sources() -> None:
    cfg = _cfg(
        sources={
            "pubsub": {"enabled": True, "topics": ["/event/LoginEventStream"]},
            "eventlogfile": {"enabled": True, "event_types": ["Report"]},
        }
    )
    app = App.build(cfg)

    with structlog.testing.capture_logs() as captured:
        app._emit_startup_log()

    starting = [e for e in captured if e["event"] == "sf2loki starting"]
    assert starting, f"no 'sf2loki starting' log emitted; got {[e['event'] for e in captured]}"
    entry = starting[0]
    assert entry["pubsub_topics"] == ["/event/LoginEventStream"]
    assert entry["eventlogfile_event_types"] == ["Report"]
    assert "loki:3100" in entry["sink"]


def test_startup_log_reports_no_sources() -> None:
    app = App.build(_cfg())

    with structlog.testing.capture_logs() as captured:
        app._emit_startup_log()

    starting = [e for e in captured if e["event"] == "sf2loki starting"]
    assert starting
    assert starting[0]["pubsub_topics"] == []
    assert starting[0]["eventlog_objects"] == []
    assert starting[0]["eventlogfile_event_types"] == []
