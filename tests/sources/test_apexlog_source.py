"""Tests for ApexLogSource — Tooling API debug-log polling."""

from __future__ import annotations

import asyncio
import json
import logging
from datetime import timedelta

import pytest

from sf2loki.config import ApexLogConfig, TransformRule
from sf2loki.obs.metrics import Metrics
from sf2loki.salesforce.apexlog_client import ApexLogMeta, ApexLogThrottledError
from sf2loki.sources.apexlog_source import ApexLogSource
from sf2loki.state.file_store import FileCheckpointStore

_PAGE_LIMIT = 200  # mirrors apexlog_source._PAGE_LIMIT


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
        self.since_id_calls: list[str] = []

    async def list_logs(self, since, users, page_size, since_id=""):
        self.list_calls.append((since, tuple(users)))
        self.since_id_calls.append(since_id)
        return self._pages.pop(0) if self._pages else []

    async def download_body(self, log_id):
        return self._bodies.get(log_id, f"body-of-{log_id}")

    async def count_active_traceflags(self):
        return 1


class TieBoundaryClient(FakeApexClient):
    """Mimics the real backend's ``(StartTime, Id)`` compound-cursor filtering.

    Holds the full universe of logs and applies the same cursor predicate the
    real SOQL WHERE clause uses (see ``ApexLogClient.list_logs``), so this
    proves the SOURCE threads ``since_id`` correctly to drain a tied page
    across multiple listing calls.
    """

    def __init__(self, all_logs: list[ApexLogMeta]) -> None:
        super().__init__(pages=[])
        self._all = sorted(all_logs, key=lambda m: (m.start_time, m.id))

    async def list_logs(self, since, users, page_size, since_id=""):
        self.list_calls.append((since, tuple(users)))
        self.since_id_calls.append(since_id)
        if since_id:
            filtered = [m for m in self._all if (m.start_time, m.id) > (since, since_id)]
        else:
            filtered = [m for m in self._all if m.start_time >= since]
        return filtered[:page_size]


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
        async def list_logs(self, since, users, page_size, since_id=""):
            raise ApexLogThrottledError("limit exceeded")

    src = ApexLogSource(
        ApexLogConfig(enabled=True), ThrottleClient(pages=[]), sm_fields=[], poll_once=True
    )
    assert [e async for e in src.events(state, asyncio.Event())] == []


# ---------------------------------------------------------------------------
# #39: (StartTime, Id) compound cursor — a full page tied at one StartTime
# must drain completely instead of pinning the watermark forever.


@pytest.mark.asyncio
async def test_full_page_tie_at_one_starttime_drains_completely(tmp_path) -> None:
    """>_PAGE_LIMIT logs sharing one StartTime all drain via the Id tiebreak.

    Before the #39 fix, a page this size at one StartTime would repeat forever
    (the watermark can only advance to a StartTime a full page doesn't exceed)
    and every log past the first _PAGE_LIMIT would be silently dropped forever.
    """
    state = FileCheckpointStore(tmp_path / "state.json")
    tie = "2026-07-02T08:00:00.000+0000"
    # A checkpoint watermark strictly before `tie` (rather than relying on the
    # now-lookback default) keeps this test independent of wall-clock time.
    seed = json.dumps({"last_ts": "2026-07-02T07:00:00.000+0000", "ids": []})
    await state.commit("apexlog", seed)

    total = _PAGE_LIMIT + 50  # spans two listing pages
    all_logs = [_meta(f"07L{i:04d}", start=tie) for i in range(total)]
    client = TieBoundaryClient(all_logs)
    src = ApexLogSource(ApexLogConfig(enabled=True), client, sm_fields=[], poll_once=True)

    entries = [e async for e in src.events(state, asyncio.Event())]

    assert len(entries) == total
    ids = [e.structured_metadata["Id"] for e in entries]
    assert ids == sorted(ids)  # drained in (StartTime, Id) order
    assert len(set(ids)) == total  # every log delivered exactly once, no gaps

    # The listing was continued via the Id tiebreak, not re-fetched blindly.
    assert client.since_id_calls[0] == ""
    assert client.since_id_calls[1] == f"07L{_PAGE_LIMIT - 1:04d}"

    final_ckpt = json.loads(entries[-1].checkpoint.value)
    assert final_ckpt["last_ts"] == tie


@pytest.mark.asyncio
async def test_repeated_full_page_stall_escalates_to_error_and_metric(
    tmp_path, caplog: pytest.LogCaptureFixture
) -> None:
    """A full page that is STILL all-already-seen (tiebreak not progressing)

    is a genuine stall: the first occurrence is a quiet WARNING, a repeat
    escalates to ERROR and increments ``metrics.watermark_stalls`` — this is
    the defense-in-depth alarm for the class of bug #39 fixes, exercised here
    via a backend that (unrealistically) ignores since_id entirely.
    """
    state = FileCheckpointStore(tmp_path / "state.json")
    tie = "2026-07-02T08:00:00.000+0000"
    seeded_ids = [f"07L{i:04d}" for i in range(_PAGE_LIMIT)]
    await state.commit("apexlog", json.dumps({"last_ts": tie, "ids": seeded_ids}))
    stuck_page = [_meta(sid, start=tie) for sid in seeded_ids]

    stop = asyncio.Event()

    class StuckClient(FakeApexClient):
        async def list_logs(self, since, users, page_size, since_id=""):
            self.list_calls.append((since, tuple(users)))
            if len(self.list_calls) >= 2:
                stop.set()
            return list(stuck_page)

    metrics = Metrics()
    cfg = ApexLogConfig(enabled=True, poll_interval=timedelta(seconds=0))
    client = StuckClient(pages=[])
    src = ApexLogSource(cfg, client, sm_fields=[], metrics=metrics, poll_once=False)

    with caplog.at_level(logging.WARNING, logger="sf2loki.sources.apexlog_source"):
        entries = [e async for e in src.events(state, stop)]

    assert entries == []
    levels = [r.levelname for r in caplog.records]
    assert levels.count("WARNING") == 1
    assert levels.count("ERROR") == 1
    assert "consecutive cycles" in caplog.text
    assert (
        metrics.registry.get_sample_value(
            "sf2loki_watermark_stalls_total", {"source": "apexlog", "object": "apexlog"}
        )
        == 1.0
    )


# ---------------------------------------------------------------------------
# #64: a cycle whose rows are all filtered/sampled out must still commit its
# cursor advance, via a checkpoint_only entry, instead of silently churning.


@pytest.mark.asyncio
async def test_checkpoint_only_emitted_when_all_filtered_out(tmp_path) -> None:
    state = FileCheckpointStore(tmp_path / "state.json")
    client = FakeApexClient(
        pages=[[_meta("07L1"), _meta("07L2", start="2026-07-02T08:00:01.000+0000")]]
    )
    cfg = ApexLogConfig(
        enabled=True,
        transforms=[TransformRule(action="drop_row", match={"Status": "Success"})],
    )
    src = ApexLogSource(cfg, client, sm_fields=[], poll_once=True)

    entries = [e async for e in src.events(state, asyncio.Event())]

    assert len(entries) == 1
    only = entries[0]
    assert only.checkpoint_only is True
    assert only.line == ""
    assert only.checkpoint.key == "apexlog"
    payload = json.loads(only.checkpoint.value)
    assert payload["last_ts"] == "2026-07-02T08:00:01.000+0000"
    assert payload["ids"] == ["07L1", "07L2"]

    # Durable: committing the token advances the persisted watermark, so a
    # restart resumes past the fully-dropped window instead of re-fetching it.
    await state.commit("apexlog", only.checkpoint.value)
    stored = await state.load("apexlog")
    assert stored is not None
    assert json.loads(stored)["last_ts"] == "2026-07-02T08:00:01.000+0000"


@pytest.mark.asyncio
async def test_checkpoint_only_not_emitted_when_nothing_polled(tmp_path) -> None:
    """An empty page (nothing new) must NOT synthesize a spurious checkpoint_only
    entry — only genuine cursor progress with zero real entries does."""
    state = FileCheckpointStore(tmp_path / "state.json")
    client = FakeApexClient(pages=[[]])
    src = ApexLogSource(ApexLogConfig(enabled=True), client, sm_fields=[], poll_once=True)

    entries = [e async for e in src.events(state, asyncio.Event())]
    assert entries == []
