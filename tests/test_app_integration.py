"""Integration tests for App.build wiring (no network)."""

from __future__ import annotations

from typing import Any

import pytest

from sf2loki.app import App
from sf2loki.config import Config
from sf2loki.sinks.loki.labels import LabelGuardError
from sf2loki.sources.eventlog_objects_source import EventLogObjectsSource
from sf2loki.sources.eventlogfile_source import EventLogFileSource
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
            "eventlogfile": {"enabled": True},
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
