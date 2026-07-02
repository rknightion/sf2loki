"""Tests for ApexLogSource — Tooling API debug-log polling."""

from __future__ import annotations

import asyncio
import json

import pytest

from sf2loki.config import ApexLogConfig
from sf2loki.salesforce.apexlog_client import ApexLogMeta, ApexLogThrottledError
from sf2loki.sources.apexlog_source import ApexLogSource
from sf2loki.state.file_store import FileCheckpointStore


def _meta(
    log_id: str, length: int = 50, start: str = "2026-07-02T08:00:00.000+0000"
) -> ApexLogMeta:
    return ApexLogMeta(
        id=log_id,
        log_user_id="005u",
        log_length=length,
        operation="exec",
        request="Api",
        status="Success",
        start_time=start,
        application="App",
        duration_ms=10,
        location="Monitoring",
    )


class FakeApexClient:
    def __init__(
        self, pages: list[list[ApexLogMeta]], bodies: dict[str, str] | None = None
    ) -> None:
        self._pages = pages
        self._bodies = bodies or {}
        self.list_calls: list[tuple[str, tuple[str, ...]]] = []

    async def list_logs(self, since, users, page_size):
        self.list_calls.append((since, tuple(users)))
        return self._pages.pop(0) if self._pages else []

    async def download_body(self, log_id):
        return self._bodies.get(log_id, f"body-of-{log_id}")

    async def count_active_traceflags(self):
        return 1


@pytest.mark.asyncio
async def test_emits_body_as_line_with_metadata_sm(tmp_path) -> None:
    state = FileCheckpointStore(tmp_path / "state.json")
    client = FakeApexClient(pages=[[_meta("07L1")]], bodies={"07L1": "DEBUG|hello world"})
    src = ApexLogSource(ApexLogConfig(enabled=True), client, sm_fields=[], poll_once=True)

    entries = [e async for e in src.events(state, asyncio.Event())]

    assert len(entries) == 1
    e = entries[0]
    assert e.line == "DEBUG|hello world"
    assert e.labels == {"source": "apexlog", "event_type": "apexlog"}
    assert e.structured_metadata["LogUserId"] == "005u"
    assert e.structured_metadata["Operation"] == "exec"
    assert e.structured_metadata["Status"] == "Success"
    assert "body_skipped" not in e.structured_metadata
    assert e.checkpoint.key == "apexlog"
    assert json.loads(e.checkpoint.value)["last_ts"] == "2026-07-02T08:00:00.000+0000"


@pytest.mark.asyncio
async def test_oversize_body_skipped_metadata_still_emitted(tmp_path) -> None:
    state = FileCheckpointStore(tmp_path / "state.json")
    client = FakeApexClient(pages=[[_meta("07L1", length=999_999)]])
    src = ApexLogSource(
        ApexLogConfig(enabled=True, max_body_bytes=1000), client, sm_fields=[], poll_once=True
    )
    entries = [e async for e in src.events(state, asyncio.Event())]

    assert len(entries) == 1
    assert entries[0].structured_metadata["body_skipped"] == "true"
    assert entries[0].structured_metadata["body_skip_reason"] == "size"
    assert "07L1" in entries[0].line  # metadata still queryable in the line


@pytest.mark.asyncio
async def test_user_filter_passed_through(tmp_path) -> None:
    state = FileCheckpointStore(tmp_path / "state.json")
    client = FakeApexClient(pages=[[]])
    src = ApexLogSource(
        ApexLogConfig(enabled=True, users=["a@x.com"]), client, sm_fields=[], poll_once=True
    )
    _ = [e async for e in src.events(state, asyncio.Event())]
    assert client.list_calls[0][1] == ("a@x.com",)


@pytest.mark.asyncio
async def test_boundary_id_deduped_via_stored_window(tmp_path) -> None:
    """A pre-committed checkpoint's id window dedups the re-fetched boundary log.

    The source yields checkpoints but the *pipeline* commits them, so cross-cycle
    dedup is tested by pre-seeding the store (mirroring the eventlog_objects
    tests) and running ONE poll whose page re-returns the boundary log + a new one.
    """
    state = FileCheckpointStore(tmp_path / "state.json")
    tie = "2026-07-02T08:00:00.000+0000"
    await state.commit("apexlog", json.dumps({"last_ts": tie, "ids": ["07L1"]}))
    client = FakeApexClient(
        pages=[[_meta("07L1", start=tie), _meta("07L2", start="2026-07-02T08:00:01.000+0000")]]
    )
    src = ApexLogSource(ApexLogConfig(enabled=True), client, sm_fields=[], poll_once=True)

    entries = [e async for e in src.events(state, asyncio.Event())]
    assert [e.structured_metadata["Id"] for e in entries] == ["07L2"]  # 07L1 deduped
    assert client.list_calls[0][0] == tie  # queried from the stored watermark


@pytest.mark.asyncio
async def test_disabled_source_emits_nothing(tmp_path) -> None:
    state = FileCheckpointStore(tmp_path / "state.json")
    src = ApexLogSource(
        ApexLogConfig(enabled=False), FakeApexClient(pages=[]), sm_fields=[], poll_once=True
    )
    assert [e async for e in src.events(state, asyncio.Event())] == []


@pytest.mark.asyncio
async def test_throttle_backs_off_without_crashing(tmp_path) -> None:
    state = FileCheckpointStore(tmp_path / "state.json")

    class ThrottleClient(FakeApexClient):
        async def list_logs(self, since, users, page_size):
            raise ApexLogThrottledError("limit exceeded")

    src = ApexLogSource(
        ApexLogConfig(enabled=True), ThrottleClient(pages=[]), sm_fields=[], poll_once=True
    )
    assert [e async for e in src.events(state, asyncio.Event())] == []
