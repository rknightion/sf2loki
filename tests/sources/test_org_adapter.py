"""OrgSource adapter + OrgCheckpointView: label injection, key prefixing, migration."""

from __future__ import annotations

import asyncio
from collections.abc import AsyncIterator
from datetime import UTC, datetime

import pytest

from sf2loki.auth.jwt_auth import AuthError
from sf2loki.model import CheckpointToken, LogEntry
from sf2loki.sources.org_adapter import OrgSource
from sf2loki.state.org_view import OrgCheckpointView, org_prefix


def _entry(key: str, *, checkpoint_only: bool = False) -> LogEntry:
    return LogEntry(
        timestamp=datetime.now(UTC),
        labels={} if checkpoint_only else {"source": "pubsub", "event_type": "Login"},
        line="" if checkpoint_only else "{}",
        structured_metadata={},
        checkpoint=CheckpointToken(key=key, value="v1"),
        checkpoint_only=checkpoint_only,
    )


class _RecordingSource:
    name = "pubsub"

    def __init__(self, entries: list[LogEntry]) -> None:
        self._entries = entries
        self.seen_state: object = None

    async def events(self, state: object, stop: asyncio.Event) -> AsyncIterator[LogEntry]:
        self.seen_state = state
        for e in self._entries:
            # Exercise the source's own load through the view it was handed.
            await state.load("pubsub:/event/LoginEventStream")  # type: ignore[attr-defined]
            yield e


class _FakeStore:
    def __init__(self, preload: dict[str, str] | None = None) -> None:
        self.data: dict[str, str] = dict(preload or {})
        self.loads: list[str] = []
        self.commits: list[tuple[str, str]] = []

    async def load(self, key: str) -> str | None:
        self.loads.append(key)
        return self.data.get(key)

    async def commit(self, key: str, value: str) -> None:
        self.commits.append((key, value))
        self.data[key] = value


async def _drain(src: OrgSource, store: _FakeStore) -> list[LogEntry]:
    stop = asyncio.Event()
    return [e async for e in src.events(store, stop)]


# --- label injection --------------------------------------------------------


async def test_injects_org_environment_and_sf_org_id() -> None:
    src = OrgSource(
        _RecordingSource([_entry("pubsub:/event/LoginEventStream")]),
        org="prod",
        environment="sandbox",
        org_id_provider=_resolved("00Dxx"),
        legacy_fallback=True,
    )
    entries = await _drain(src, _FakeStore())
    assert entries[0].labels["org"] == "prod"
    assert entries[0].labels["environment"] == "sandbox"
    assert entries[0].labels["sf_org_id"] == "00Dxx"
    # Inner labels preserved.
    assert entries[0].labels["source"] == "pubsub"
    assert entries[0].labels["event_type"] == "Login"


async def test_checkpoint_only_entries_get_no_labels() -> None:
    src = OrgSource(
        _RecordingSource([_entry("pubsub:x", checkpoint_only=True)]),
        org="prod",
        environment="production",
        org_id_provider=_resolved("00Dxx"),
    )
    entries = await _drain(src, _FakeStore())
    assert entries[0].labels == {}  # keepalive stays unlabelled
    # ...but its checkpoint key is still prefixed.
    assert entries[0].checkpoint.key == "org=prod:pubsub:x"


async def test_sf_org_id_omitted_when_resolution_fails() -> None:
    async def _boom() -> str:
        raise RuntimeError("auth down")

    src = OrgSource(
        _RecordingSource([_entry("pubsub:x")]),
        org="prod",
        environment="production",
        org_id_provider=_boom,
    )
    entries = await _drain(src, _FakeStore())
    assert "sf_org_id" not in entries[0].labels  # never crashes; label simply absent
    assert entries[0].labels["org"] == "prod"


async def test_name_is_inner_name_not_prefixed() -> None:
    src = OrgSource(_RecordingSource([]), org="prod", environment="production")
    assert src.name == "pubsub"  # keeps `source` label clean; org is its own dimension


# --- checkpoint key prefixing ----------------------------------------------


async def test_rewrites_checkpoint_key_with_prefix() -> None:
    src = OrgSource(
        _RecordingSource([_entry("pubsub:/event/LoginEventStream")]),
        org="emea",
        environment="production",
        org_id_provider=_resolved("00D"),
    )
    entries = await _drain(src, _FakeStore())
    assert entries[0].checkpoint.key == "org=emea:pubsub:/event/LoginEventStream"
    assert entries[0].checkpoint.value == "v1"


async def test_source_load_reads_prefixed_key() -> None:
    store = _FakeStore()
    src = OrgSource(
        _RecordingSource([_entry("pubsub:x")]),
        org="prod",
        environment="production",
        org_id_provider=_resolved("00D"),
    )
    await _drain(src, store)
    assert "org=prod:pubsub:/event/LoginEventStream" in store.loads


# --- OrgCheckpointView legacy fallback --------------------------------------


async def test_view_load_prefers_prefixed() -> None:
    store = _FakeStore({"org=prod:pubsub:x": "NEW", "pubsub:x": "OLD"})
    view = OrgCheckpointView(store, prefix="org=prod:", legacy_fallback=True)
    assert await view.load("pubsub:x") == "NEW"


async def test_view_load_falls_back_to_legacy_for_first_org() -> None:
    store = _FakeStore({"pubsub:x": "OLD"})  # only the unprefixed legacy key exists
    view = OrgCheckpointView(store, prefix="org=prod:", legacy_fallback=True)
    assert await view.load("pubsub:x") == "OLD"  # migrates transparently


async def test_view_no_legacy_fallback_for_non_first_org() -> None:
    store = _FakeStore({"pubsub:x": "OLD"})
    view = OrgCheckpointView(store, prefix="org=emea:", legacy_fallback=False)
    assert await view.load("pubsub:x") is None  # must NOT inherit another org's legacy state


async def test_view_commit_writes_prefixed() -> None:
    store = _FakeStore()
    view = OrgCheckpointView(store, prefix="org=prod:", legacy_fallback=True)
    await view.commit("pubsub:x", "v9")
    assert store.commits == [("org=prod:pubsub:x", "v9")]


async def test_view_passthrough_for_set_fence_and_close() -> None:
    calls: list[str] = []

    class _StoreWithFence(_FakeStore):
        def set_fence(self, fence: object) -> None:
            calls.append("set_fence")

        def close(self) -> None:
            calls.append("close")

    view = OrgCheckpointView(_StoreWithFence(), prefix="org=p:", legacy_fallback=False)
    view.set_fence(lambda: None)  # type: ignore[attr-defined]
    view.close()  # type: ignore[attr-defined]
    assert calls == ["set_fence", "close"]


# --- per-org auth-failure containment (isolation) ---------------------------


class _AuthFailingThenOkSource:
    name = "eventlog_objects"

    def __init__(self, entries: list[LogEntry]) -> None:
        self._entries = entries
        self.attempts = 0

    async def events(self, state: object, stop: asyncio.Event) -> AsyncIterator[LogEntry]:
        self.attempts += 1
        if self.attempts == 1:
            raise AuthError("token mint failed")  # escapes the polling clients today
        for e in self._entries:
            yield e


async def test_auth_error_is_contained_and_inner_restarts(monkeypatch: pytest.MonkeyPatch) -> None:
    import sf2loki.sources.org_adapter as mod

    monkeypatch.setattr(mod, "_RETRY_BACKOFF_BASE", 0.001)
    inner = _AuthFailingThenOkSource([_entry("eventlog_objects:LoginEvent")])
    src = OrgSource(inner, org="prod", environment="production", org_id_provider=_resolved("00D"))
    entries = await _drain(src, _FakeStore())
    assert inner.attempts == 2  # first raised AuthError, adapter restarted the generator
    assert len(entries) == 1
    assert entries[0].labels["org"] == "prod"  # healthy org keeps streaming after recovery


async def test_auth_error_supervisor_exits_on_stop(monkeypatch: pytest.MonkeyPatch) -> None:
    import sf2loki.sources.org_adapter as mod

    monkeypatch.setattr(mod, "_RETRY_BACKOFF_BASE", 60.0)  # long, so we rely on stop to exit

    class _AlwaysFails:
        name = "eventlog_objects"

        async def events(self, state: object, stop: asyncio.Event) -> AsyncIterator[LogEntry]:
            raise AuthError("still down")
            yield  # pragma: no cover - marks this an async generator

    stop = asyncio.Event()
    src = OrgSource(_AlwaysFails(), org="prod", environment="production")
    gen = src.events(_FakeStore(), stop)
    # First iteration hits AuthError then awaits the (long) backoff on stop; set stop
    # so the supervisor returns promptly instead of crashing the pipeline or hanging.
    stop.set()
    collected = [e async for e in gen]
    assert collected == []  # contained: no crash, no entries, clean exit


# --- prefix non-collision ---------------------------------------------------


@pytest.mark.parametrize(
    "namespace",
    ["pubsub:", "eventlogfile:", "eventlog_objects:", "backfill:", "egress:"],
)
def test_org_prefix_does_not_collide_with_existing_namespaces(namespace: str) -> None:
    prefix = org_prefix("prod")
    assert prefix == "org=prod:"
    assert not prefix.startswith(namespace)
    assert not namespace.startswith(prefix)


def _resolved(value: str):
    async def _inner() -> str:
        return value

    return _inner
