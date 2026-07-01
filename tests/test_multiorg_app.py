"""Multi-org App.build wiring + startup-auth semantics (no network)."""

from __future__ import annotations

from typing import Any

import pytest

from sf2loki.app import App, _org_auth_degraded_check, _OrgAuth
from sf2loki.auth.jwt_auth import AuthError
from sf2loki.config import Config
from sf2loki.sources.eventlog_objects_source import EventLogObjectsSource
from sf2loki.sources.org_adapter import OrgSource
from sf2loki.sources.pubsub_source import PubSubSource


def _sf(**over: Any) -> dict[str, Any]:
    base: dict[str, Any] = {"client_id": "cid", "username": "svc@example.com", "private_key": "PK"}
    base.update(over)
    return base


def _sink() -> dict[str, Any]:
    return {"loki": {"url": "http://loki:3100/loki/api/v1/push"}}


def _multi_cfg(**over: Any) -> Config:
    base: dict[str, Any] = {
        "orgs": [
            {
                "name": "prod",
                "salesforce": _sf(),
                "sources": {"pubsub": {"enabled": True, "topics": ["/event/LoginEventStream"]}},
            },
            {
                "name": "emea",
                "salesforce": _sf(client_id="emea"),
                "sources": {
                    "pubsub": {"enabled": False},
                    "eventlog_objects": {"enabled": True, "objects": [{"name": "ApiEvent"}]},
                },
            },
        ],
        "sink": _sink(),
    }
    base.update(over)
    return Config(**base)


def _source_types(appn: App) -> list[type]:
    return [type(s) for s in appn._pipeline._sources]


# --- build wiring -----------------------------------------------------------


def test_two_orgs_produce_wrapped_sources_from_both() -> None:
    appn = App.build(_multi_cfg())
    assert _source_types(appn) == [OrgSource, OrgSource]
    # Both orgs share ONE pipeline and ONE sink.
    assert appn._pipeline is not None
    assert len(appn._orgs) == 2
    assert {o.name for o in appn._orgs} == {"prod", "emea"}
    # The wrapped inner source names are preserved (source label stays clean).
    assert {s.name for s in appn._pipeline._sources} == {"pubsub", "eventlog_objects"}


def test_multi_org_flag_set() -> None:
    assert App.build(_multi_cfg())._multi_org is True


def test_per_org_limits_pollers() -> None:
    cfg = _multi_cfg(
        orgs=[
            {"name": "prod", "salesforce": _sf(limits={"enabled": True})},
            {"name": "emea", "salesforce": _sf(client_id="e", limits={"enabled": True})},
        ]
    )
    appn = App.build(cfg)
    assert len(appn._limits_pollers) == 2


def test_org_label_in_allowed_labels_lets_build_succeed() -> None:
    # Build wires the sink label guard; org must be an allowed label or this raises.
    App.build(_multi_cfg())


# --- single-org (legacy) path is bit-identical ------------------------------


def _single_cfg(**over: Any) -> Config:
    base: dict[str, Any] = {
        "salesforce": _sf(),
        "sink": _sink(),
        "sources": {"pubsub": {"enabled": True, "topics": ["/event/LoginEventStream"]}},
    }
    base.update(over)
    return Config(**base)


def test_single_org_uses_raw_sources_no_orgsource() -> None:
    appn = App.build(_single_cfg())
    # No OrgSource wrapper: raw PubSubSource, no org label, unprefixed checkpoints.
    assert _source_types(appn) == [PubSubSource]
    assert appn._multi_org is False
    assert len(appn._orgs) == 1
    assert appn._orgs[0].name == ""


def test_single_org_eventlog_objects_raw() -> None:
    cfg = _single_cfg(
        sources={
            "pubsub": {"enabled": False},
            "eventlog_objects": {"enabled": True, "objects": [{"name": "LoginEvent"}]},
        }
    )
    assert _source_types(App.build(cfg)) == [EventLogObjectsSource]


# --- startup auth semantics -------------------------------------------------


class _OkTokens:
    def __init__(self) -> None:
        self._has = False

    async def token(self) -> object:
        self._has = True
        return object()

    def has_token(self) -> bool:
        return self._has


class _FailTokens:
    async def token(self) -> object:
        raise AuthError("bad credentials")

    def has_token(self) -> bool:
        return False


async def test_partial_org_auth_failure_degrades_but_runs() -> None:
    appn = App.build(_multi_cfg())
    fail = _FailTokens()
    appn._orgs = [
        _OrgAuth("prod", _OkTokens(), "production"),  # type: ignore[arg-type]
        _OrgAuth("emea", fail, "production"),  # type: ignore[arg-type]
    ]
    # Does NOT raise — the healthy org keeps running.
    await appn._probe_orgs()
    assert "emea" in appn._degraded_orgs  # failing org recorded for the readiness reason
    assert "prod" not in appn._degraded_orgs


async def test_all_orgs_auth_failing_fails_fast() -> None:
    appn = App.build(_multi_cfg())
    appn._orgs = [
        _OrgAuth("prod", _FailTokens(), "production"),  # type: ignore[arg-type]
        _OrgAuth("emea", _FailTokens(), "production"),  # type: ignore[arg-type]
    ]
    with pytest.raises(AuthError):
        await appn._probe_orgs()


async def test_org_degraded_check_reports_and_recovers() -> None:
    fail = _FailTokens()
    check = _org_auth_degraded_check({"emea": fail})  # type: ignore[dict-item]
    assert check() == "degraded: org emea auth failing"

    ok = _OkTokens()
    await ok.token()  # org recovered (has a valid token now)
    recovered = _org_auth_degraded_check({"emea": ok})  # type: ignore[dict-item]
    assert recovered() is None
