"""Tests for EventLogFileSource — ELF listing/download polling source.

Uses a duck-typed fake :class:`EventLogFileClient` (no respx) so the
checkpoint-carrying invariant can be tested deterministically, mirroring the
convention in tests/sources/test_eventlog_objects_source.py.
"""

from __future__ import annotations

import asyncio
import json
from collections.abc import AsyncIterator
from dataclasses import dataclass, field
from datetime import UTC, datetime, timedelta
from typing import Literal

import pytest

from sf2loki.config import EventLogFileConfig, EventLogFileTypeConfig
from sf2loki.model import LogEntry
from sf2loki.obs.metrics import Metrics
from sf2loki.salesforce.eventlogfile_client import (
    EventLogFileError,
    EventLogFileMeta,
    EventLogFileThrottledError,
)
from sf2loki.sources.eventlogfile_source import EventLogFileSource
from sf2loki.state.file_store import FileCheckpointStore

# ---------------------------------------------------------------------------
# Fakes


class _ConcurrencyProbe:
    """Tracks how many callers are simultaneously "inside" a probed section.

    Used to PROVE the ``cfg.concurrency`` bound structurally rather than by
    guessing at timing: ``enter()`` increments then genuinely suspends (via
    ``asyncio.sleep``), which is what lets sibling tasks actually interleave
    in the first place — without a real suspension point, asyncio would just
    run each task to completion before starting the next, and "concurrent"
    tasks would never actually overlap.
    """

    def __init__(self) -> None:
        self.current = 0
        self.max_seen = 0

    async def enter(self, delay: float) -> None:
        self.current += 1
        self.max_seen = max(self.max_seen, self.current)
        await asyncio.sleep(delay)

    def exit(self) -> None:
        self.current -= 1


@dataclass
class FakeEventLogFileClient:
    """Duck-typed stand-in for EventLogFileClient with deterministic responses.

    ``download`` is an async generator, matching the real streaming client: the
    fetch happens before the first row is yielded, so failures surface at the
    first ``anext()``. Ids listed in ``errors`` raise :class:`EventLogFileError`
    (as the real client does on a non-2xx LogFile response, e.g. a body-not-ready
    404); ids in ``throttled`` raise :class:`EventLogFileThrottledError`; ids in
    ``mid_stream_errors`` yield their first row then fail (CSV parse error
    mid-file). ``list_error`` / ``discover_error`` raise on listing/discovery —
    the real client wraps everything (incl. inner SOQL failures) into the
    EventLogFileError family, so that is what the fake raises too.

    ``files_by_type`` overrides ``files`` per event type (falls back to
    ``files`` when a type has no entry) — lets a concurrency test give
    different types different files without needing separate client
    instances (the source uses ONE client for every type). ``probe`` +
    ``probe_delay`` instrument ``list_files`` to prove genuine overlap
    between concurrently-processed types (see :class:`_ConcurrencyProbe`).
    ``download_delay`` sleeps at the top of ``download`` (before the
    throttled/error checks) — keeps a file's download "in flight" long
    enough for a sibling type to start (or finish) concurrently.
    """

    files: list[EventLogFileMeta]
    rows_by_id: dict[str, list[dict[str, str]]]
    errors: set[str] = field(default_factory=set)
    throttled: set[str] = field(default_factory=set)
    mid_stream_errors: set[str] = field(default_factory=set)
    list_error: Exception | None = None
    discovered_types: list[str] = field(default_factory=list)
    discover_error: bool = False
    skew: timedelta | None = None
    files_by_type: dict[str, list[EventLogFileMeta]] = field(default_factory=dict)
    probe: _ConcurrencyProbe | None = None
    probe_delay: float = 0.02
    download_delay: float = 0.0
    list_calls: list[tuple[str, str, str, int]] = field(default_factory=list)
    download_calls: list[str] = field(default_factory=list)
    discover_calls: list[str] = field(default_factory=list)

    def clock_skew(self) -> timedelta | None:
        return self.skew

    async def list_files(
        self, event_type: str, interval: str, since: str, page_size: int
    ) -> list[EventLogFileMeta]:
        self.list_calls.append((event_type, interval, since, page_size))
        if self.probe is not None:
            await self.probe.enter(self.probe_delay)
        try:
            if self.list_error is not None:
                raise self.list_error
            return list(self.files_by_type.get(event_type, self.files))
        finally:
            if self.probe is not None:
                self.probe.exit()

    async def list_event_types(self, interval: str) -> list[str]:
        self.discover_calls.append(interval)
        if self.discover_error:
            raise EventLogFileError("discovery failed")
        return list(self.discovered_types)

    async def download(self, file_meta: EventLogFileMeta) -> AsyncIterator[dict[str, str]]:
        self.download_calls.append(file_meta.id)
        if self.download_delay:
            await asyncio.sleep(self.download_delay)
        if file_meta.id in self.throttled:
            raise EventLogFileThrottledError(
                f"download throttled for {file_meta.id}: HTTP 403 REQUEST_LIMIT_EXCEEDED"
            )
        if file_meta.id in self.errors:
            raise EventLogFileError(f"download failed for {file_meta.id}: HTTP 404")
        rows = self.rows_by_id.get(file_meta.id, [])
        for i, row in enumerate(rows):
            yield row
            if i == 1 and file_meta.id in self.mid_stream_errors:
                raise EventLogFileError(f"CSV parse failed mid-file for {file_meta.id}")


async def _run_cycle(source: EventLogFileSource, store: FileCheckpointStore) -> list[LogEntry]:
    """One poll cycle + a pipeline-style commit of the final entry's checkpoint."""
    entries = [e async for e in source.events(store, asyncio.Event())]
    if entries:
        await store.commit(entries[-1].checkpoint.key, entries[-1].checkpoint.value)
    return entries


def _created_ago(delta: timedelta) -> str:
    """A CreatedDate literal (Salesforce ``+0000`` style) *delta* before now."""
    return (datetime.now(UTC) - delta).strftime("%Y-%m-%dT%H:%M:%S.000+0000")


def make_file_meta(
    *,
    id: str = "0ATxx0000000001",
    event_type: str = "Login",
    interval: str = "Hourly",
    created_date: str = "2026-06-30T01:00:00.000+0000",
) -> EventLogFileMeta:
    return EventLogFileMeta(
        id=id,
        event_type=event_type,
        interval=interval,
        log_date=created_date,
        created_date=created_date,
        sequence=1,
        length=100,
    )


def make_elf_cfg(
    *,
    event_types: list[str] | None = None,
    interval: Literal["Hourly", "Daily"] = "Hourly",
    poll_interval: timedelta = timedelta(seconds=0),
    lookback: timedelta = timedelta(hours=24),
    page_size: int = 1000,
    timestamp_column: str = "TIMESTAMP_DERIVED",
    settle_window: timedelta = timedelta(0),
    download_max_age: timedelta = timedelta(hours=24),
    concurrency: int = 1,
) -> EventLogFileConfig:
    return EventLogFileConfig(
        enabled=True,
        interval=interval,
        event_types=event_types if event_types is not None else ["Login"],
        poll_interval=poll_interval,
        lookback=lookback,
        timestamp_column=timestamp_column,
        page_size=page_size,
        settle_window=settle_window,
        download_max_age=download_max_age,
        concurrency=concurrency,
    )


# ---------------------------------------------------------------------------
# Tests


@pytest.mark.asyncio
async def test_no_checkpoint_uses_now_lookback_and_yields_entry_per_row(
    tmp_path: pytest.TempPathFactory,
) -> None:
    store = FileCheckpointStore(tmp_path / "state.json")  # type: ignore[arg-type]
    file_meta = make_file_meta(id="f1")
    rows = [
        {"TIMESTAMP_DERIVED": "20260630010000.000", "EVENT_TYPE": "Login"},
        {"TIMESTAMP_DERIVED": "20260630010100.000", "EVENT_TYPE": "Login"},
    ]
    client = FakeEventLogFileClient(files=[file_meta], rows_by_id={"f1": rows})
    cfg = make_elf_cfg(lookback=timedelta(hours=24))
    source = EventLogFileSource(cfg, client, sm_fields=[], poll_once=True)  # type: ignore[arg-type]
    stop = asyncio.Event()
    entries = [e async for e in source.events(store, stop)]

    assert len(entries) == 2
    for entry in entries:
        assert entry.labels == {"source": "eventlogfile", "event_type": "Login"}
        assert entry.checkpoint.key == "eventlogfile:Login"

    event_type, interval, since, page_size = client.list_calls[0]
    assert event_type == "Login"
    assert interval == "Hourly"
    assert page_size == 1000
    since_dt = datetime.strptime(since, "%Y-%m-%dT%H:%M:%SZ").replace(tzinfo=UTC)
    expected = datetime.now(UTC) - timedelta(hours=24)
    assert abs((expected - since_dt).total_seconds()) < 5


@pytest.mark.asyncio
async def test_interval_from_cfg_used_in_listing_query(
    tmp_path: pytest.TempPathFactory,
) -> None:
    store = FileCheckpointStore(tmp_path / "state.json")  # type: ignore[arg-type]
    client = FakeEventLogFileClient(files=[], rows_by_id={})
    cfg = make_elf_cfg(interval="Daily")
    source = EventLogFileSource(cfg, client, sm_fields=[], poll_once=True)  # type: ignore[arg-type]
    stop = asyncio.Event()
    _ = [e async for e in source.events(store, stop)]

    assert client.list_calls[0][1] == "Daily"


@pytest.mark.asyncio
async def test_sm_fields_promoted_to_structured_metadata(
    tmp_path: pytest.TempPathFactory,
) -> None:
    store = FileCheckpointStore(tmp_path / "state.json")  # type: ignore[arg-type]
    file_meta = make_file_meta(id="f1")
    rows = [{"TIMESTAMP_DERIVED": "20260630010000.000", "USER_ID": "005xx"}]
    client = FakeEventLogFileClient(files=[file_meta], rows_by_id={"f1": rows})
    cfg = make_elf_cfg()
    source = EventLogFileSource(cfg, client, sm_fields=["USER_ID"], poll_once=True)  # type: ignore[arg-type]
    stop = asyncio.Event()
    entries = [e async for e in source.events(store, stop)]

    assert len(entries) == 1
    assert entries[0].structured_metadata.get("USER_ID") == "005xx"
    assert "USER_ID" not in entries[0].labels


@pytest.mark.asyncio
async def test_per_type_sm_fields_override_global(
    tmp_path: pytest.TempPathFactory,
) -> None:
    """A type's structured_metadata_fields overrides the global sm_fields."""
    store = FileCheckpointStore(tmp_path / "state.json")  # type: ignore[arg-type]
    file_meta = make_file_meta(id="f1", event_type="API")
    rows = [{"TIMESTAMP_DERIVED": "20260630010000.000", "API_TYPE": "REST", "USER_ID": "005"}]
    client = FakeEventLogFileClient(files=[file_meta], rows_by_id={"f1": rows})
    cfg = EventLogFileConfig(
        enabled=True,
        interval="Hourly",
        event_types=[EventLogFileTypeConfig(name="API", structured_metadata_fields=["API_TYPE"])],
        poll_interval=timedelta(seconds=0),
        lookback=timedelta(hours=24),
    )
    # Global sm_fields would promote USER_ID, but the per-type override wins.
    source = EventLogFileSource(cfg, client, sm_fields=["USER_ID"], poll_once=True)  # type: ignore[arg-type]
    stop = asyncio.Event()
    entries = [e async for e in source.events(store, stop)]

    assert len(entries) == 1
    # level is always injected; the per-type override drives the rest.
    assert entries[0].structured_metadata == {"API_TYPE": "REST", "level": "info"}
    assert "USER_ID" not in entries[0].structured_metadata


@pytest.mark.asyncio
async def test_empty_per_type_sm_fields_suppresses_global(
    tmp_path: pytest.TempPathFactory,
) -> None:
    """An explicit empty list (not None) suppresses the global sm_fields."""
    store = FileCheckpointStore(tmp_path / "state.json")  # type: ignore[arg-type]
    file_meta = make_file_meta(id="f1", event_type="API")
    rows = [{"TIMESTAMP_DERIVED": "20260630010000.000", "USER_ID": "005"}]
    client = FakeEventLogFileClient(files=[file_meta], rows_by_id={"f1": rows})
    cfg = EventLogFileConfig(
        enabled=True,
        interval="Hourly",
        event_types=[EventLogFileTypeConfig(name="API", structured_metadata_fields=[])],
        poll_interval=timedelta(seconds=0),
        lookback=timedelta(hours=24),
    )
    source = EventLogFileSource(cfg, client, sm_fields=["USER_ID"], poll_once=True)  # type: ignore[arg-type]
    stop = asyncio.Event()
    entries = [e async for e in source.events(store, stop)]

    # USER_ID suppressed by the empty per-type list; level still injected.
    assert entries[0].structured_metadata == {"level": "info"}


@pytest.mark.asyncio
async def test_per_type_labels_promoted_to_stream_labels(
    tmp_path: pytest.TempPathFactory,
) -> None:
    store = FileCheckpointStore(tmp_path / "state.json")  # type: ignore[arg-type]
    file_meta = make_file_meta(id="f1", event_type="API")
    rows = [{"TIMESTAMP_DERIVED": "20260630010000.000", "API_TYPE": "REST"}]
    client = FakeEventLogFileClient(files=[file_meta], rows_by_id={"f1": rows})
    cfg = EventLogFileConfig(
        enabled=True,
        interval="Hourly",
        event_types=[EventLogFileTypeConfig(name="API", labels=["API_TYPE"])],
        poll_interval=timedelta(seconds=0),
        lookback=timedelta(hours=24),
    )
    source = EventLogFileSource(cfg, client, sm_fields=[], poll_once=True)  # type: ignore[arg-type]
    stop = asyncio.Event()
    entries = [e async for e in source.events(store, stop)]

    assert entries[0].labels == {
        "source": "eventlogfile",
        "event_type": "API",
        "API_TYPE": "REST",
    }


@pytest.mark.asyncio
async def test_promoted_label_cannot_clobber_source_identity(
    tmp_path: pytest.TempPathFactory,
) -> None:
    """Even if a row has a column colliding with event_type, reserved keys win."""
    store = FileCheckpointStore(tmp_path / "state.json")  # type: ignore[arg-type]
    file_meta = make_file_meta(id="f1", event_type="API")
    # event_type is reserved so it can't be configured for promotion; simulate a
    # benign promoted column whose VALUE differs from the event_type to prove the
    # reserved key is not overwritten.
    rows = [{"TIMESTAMP_DERIVED": "20260630010000.000", "REQUEST_STATUS": "S"}]
    client = FakeEventLogFileClient(files=[file_meta], rows_by_id={"f1": rows})
    cfg = EventLogFileConfig(
        enabled=True,
        interval="Hourly",
        event_types=[EventLogFileTypeConfig(name="API", labels=["REQUEST_STATUS"])],
        poll_interval=timedelta(seconds=0),
        lookback=timedelta(hours=24),
    )
    source = EventLogFileSource(cfg, client, sm_fields=[], poll_once=True)  # type: ignore[arg-type]
    stop = asyncio.Event()
    entries = [e async for e in source.events(store, stop)]

    assert entries[0].labels["source"] == "eventlogfile"
    assert entries[0].labels["event_type"] == "API"
    assert entries[0].labels["REQUEST_STATUS"] == "S"


@pytest.mark.asyncio
async def test_dynamic_csv_headers_across_files(
    tmp_path: pytest.TempPathFactory,
) -> None:
    """Different files may have different ELF column sets; each row is shaped independently."""
    store = FileCheckpointStore(tmp_path / "state.json")  # type: ignore[arg-type]
    f1 = make_file_meta(id="f1", created_date="2026-06-30T01:00:00.000+0000")
    f2 = make_file_meta(id="f2", created_date="2026-06-30T02:00:00.000+0000")
    rows1 = [{"TIMESTAMP_DERIVED": "20260630010000.000", "A": "1"}]
    rows2 = [{"TIMESTAMP_DERIVED": "20260630020000.000", "B": "2", "C": "3"}]
    client = FakeEventLogFileClient(files=[f1, f2], rows_by_id={"f1": rows1, "f2": rows2})
    cfg = make_elf_cfg()
    source = EventLogFileSource(cfg, client, sm_fields=[], poll_once=True)  # type: ignore[arg-type]
    stop = asyncio.Event()
    entries = [e async for e in source.events(store, stop)]

    assert len(entries) == 2
    assert '"A": "1"' in entries[0].line
    assert '"B": "2"' in entries[1].line
    assert '"C": "3"' in entries[1].line


@pytest.mark.asyncio
async def test_already_seen_file_id_is_skipped(
    tmp_path: pytest.TempPathFactory,
) -> None:
    store = FileCheckpointStore(tmp_path / "state.json")  # type: ignore[arg-type]
    key = "eventlogfile:Login"
    await store.commit(key, json.dumps({"last_created": "2026-06-30T00:00:00Z", "ids": ["f1"]}))

    f1 = make_file_meta(id="f1")
    client = FakeEventLogFileClient(
        files=[f1], rows_by_id={"f1": [{"TIMESTAMP_DERIVED": "20260630010000.000"}]}
    )
    cfg = make_elf_cfg()
    source = EventLogFileSource(cfg, client, sm_fields=[], poll_once=True)  # type: ignore[arg-type]
    stop = asyncio.Event()
    entries = [e async for e in source.events(store, stop)]

    assert entries == []
    assert client.download_calls == []


@pytest.mark.asyncio
async def test_stop_set_before_iteration_yields_nothing(
    tmp_path: pytest.TempPathFactory,
) -> None:
    store = FileCheckpointStore(tmp_path / "state.json")  # type: ignore[arg-type]
    client = FakeEventLogFileClient(files=[make_file_meta()], rows_by_id={})
    cfg = make_elf_cfg()
    source = EventLogFileSource(cfg, client, sm_fields=[], poll_once=True)  # type: ignore[arg-type]
    stop = asyncio.Event()
    stop.set()
    entries = [e async for e in source.events(store, stop)]

    assert entries == []
    assert client.list_calls == []


@pytest.mark.asyncio
async def test_stop_during_inter_cycle_sleep_returns_promptly(
    tmp_path: pytest.TempPathFactory,
) -> None:
    store = FileCheckpointStore(tmp_path / "state.json")  # type: ignore[arg-type]
    client = FakeEventLogFileClient(files=[], rows_by_id={})
    cfg = make_elf_cfg(poll_interval=timedelta(seconds=60))
    source = EventLogFileSource(cfg, client, sm_fields=[], poll_once=False)  # type: ignore[arg-type]
    stop = asyncio.Event()

    async def consume() -> None:
        async for _ in source.events(store, stop):
            pass

    task = asyncio.create_task(consume())
    await asyncio.sleep(0.05)  # let the first (empty) cycle complete and enter the sleep
    stop.set()
    await asyncio.wait_for(task, timeout=1.0)  # << would be 60s without the fix


@pytest.mark.asyncio
async def test_checkpoint_carrying_invariant_across_files(
    tmp_path: pytest.TempPathFactory,
) -> None:
    """At-least-once checkpoint carrying (see EventLogFileSource docstring).

    f1 has rows a,b; f2 has rows c,d. Rows a (not last of f1) and c (not last
    of f2) must carry the PRE-file checkpoint (a: pre-f1; c: pre-f2 == post-f1).
    The last row of each file (b, d) carries the ADVANCED post-file checkpoint
    with that file's id newly present in "ids".
    """
    store = FileCheckpointStore(tmp_path / "state.json")  # type: ignore[arg-type]
    f1 = make_file_meta(id="f1", created_date="2026-06-30T01:00:00.000+0000")
    f2 = make_file_meta(id="f2", created_date="2026-06-30T02:00:00.000+0000")
    rows1 = [
        {"TIMESTAMP_DERIVED": "20260630010000.000", "ROW": "a"},
        {"TIMESTAMP_DERIVED": "20260630010001.000", "ROW": "b"},
    ]
    rows2 = [
        {"TIMESTAMP_DERIVED": "20260630020000.000", "ROW": "c"},
        {"TIMESTAMP_DERIVED": "20260630020001.000", "ROW": "d"},
    ]
    client = FakeEventLogFileClient(files=[f1, f2], rows_by_id={"f1": rows1, "f2": rows2})
    cfg = make_elf_cfg()
    source = EventLogFileSource(cfg, client, sm_fields=[], poll_once=True)  # type: ignore[arg-type]
    stop = asyncio.Event()
    entries = [e async for e in source.events(store, stop)]

    assert len(entries) == 4
    row_a, row_b, row_c, row_d = entries

    pre_f1 = json.loads(row_a.checkpoint.value)
    assert pre_f1["ids"] == []

    post_f1 = json.loads(row_b.checkpoint.value)
    assert post_f1["ids"] == [["f1", f1.created_date]]
    assert post_f1["last_created"] == f1.created_date

    # c is the first (non-last) row of f2: it carries the pre-f2 checkpoint,
    # which is exactly the post-f1 checkpoint b just advanced to.
    assert json.loads(row_c.checkpoint.value) == post_f1

    post_f2 = json.loads(row_d.checkpoint.value)
    assert post_f2["ids"] == [["f1", f1.created_date], ["f2", f2.created_date]]
    assert post_f2["last_created"] == f2.created_date


@pytest.mark.asyncio
async def test_zero_row_file_contributes_nothing_to_checkpoint(
    tmp_path: pytest.TempPathFactory,
) -> None:
    """A file with zero rows is downloaded but never folded into the carried checkpoint."""
    store = FileCheckpointStore(tmp_path / "state.json")  # type: ignore[arg-type]
    f1 = make_file_meta(id="f1", created_date="2026-06-30T01:00:00.000+0000")
    f_empty = make_file_meta(id="f_empty", created_date="2026-06-30T01:30:00.000+0000")
    f2 = make_file_meta(id="f2", created_date="2026-06-30T02:00:00.000+0000")
    rows1 = [{"TIMESTAMP_DERIVED": "20260630010000.000"}]
    rows2 = [{"TIMESTAMP_DERIVED": "20260630020000.000"}]
    client = FakeEventLogFileClient(
        files=[f1, f_empty, f2],
        rows_by_id={"f1": rows1, "f_empty": [], "f2": rows2},
    )
    cfg = make_elf_cfg()
    source = EventLogFileSource(cfg, client, sm_fields=[], poll_once=True)  # type: ignore[arg-type]
    stop = asyncio.Event()
    entries = [e async for e in source.events(store, stop)]

    assert len(entries) == 2
    assert "f_empty" in client.download_calls

    last_checkpoint = json.loads(entries[-1].checkpoint.value)
    assert [i for i, _ in last_checkpoint["ids"]] == ["f1", "f2"]


# ---------------------------------------------------------------------------
# Resiliency: transient download failures must not crash the connector, and the
# checkpoint must not advance past a file we couldn't read (ko.md §7.4).


@pytest.mark.asyncio
async def test_download_failure_does_not_crash_and_stops_cycle_at_failed_file(
    tmp_path: pytest.TempPathFactory,
) -> None:
    """A transient download error (e.g. body-not-ready 404) is caught — the
    connector does not crash — and the per-type file loop STOPS at the failed
    file for this cycle: processing a later file would advance the watermark
    past the failed one, silently losing it forever (listing is
    CreatedDate >= watermark)."""
    store = FileCheckpointStore(tmp_path / "state.json")  # type: ignore[arg-type]
    f1 = make_file_meta(id="f1", created_date=_created_ago(timedelta(hours=3)))
    f_bad = make_file_meta(id="f_bad", created_date=_created_ago(timedelta(hours=2)))
    f3 = make_file_meta(id="f3", created_date=_created_ago(timedelta(hours=1)))
    client = FakeEventLogFileClient(
        files=[f1, f_bad, f3],
        rows_by_id={
            "f1": [{"TIMESTAMP_DERIVED": "20260630010000.000", "ROW": "a"}],
            "f_bad": [{"TIMESTAMP_DERIVED": "20260630020000.000", "ROW": "b"}],
            "f3": [{"TIMESTAMP_DERIVED": "20260630030000.000", "ROW": "c"}],
        },
        errors={"f_bad"},
    )
    cfg = make_elf_cfg()
    source = EventLogFileSource(cfg, client, sm_fields=[], poll_once=True)  # type: ignore[arg-type]

    entries = await _run_cycle(source, store)

    # Only f1 emitted this cycle; f_bad failed and f3 was NOT attempted.
    assert len(entries) == 1
    assert client.download_calls == ["f1", "f_bad"]

    # Next cycle (error resolved): f_bad and f3 are ingested, in order — nothing lost.
    client.errors.clear()
    entries2 = await _run_cycle(source, store)
    assert len(entries2) == 2
    assert '"ROW": "b"' in entries2[0].line
    assert '"ROW": "c"' in entries2[1].line
    assert client.download_calls == ["f1", "f_bad", "f_bad", "f3"]


@pytest.mark.asyncio
async def test_transient_download_failure_does_not_advance_checkpoint_past_file(
    tmp_path: pytest.TempPathFactory,
) -> None:
    """After a cycle where f_bad fails transiently, the committed watermark must
    still allow f_bad to be re-listed next cycle (last_created stays at the last
    successful file BEFORE it), and the files after it are ingested on a later
    cycle — order preserved, nothing lost."""
    store = FileCheckpointStore(tmp_path / "state.json")  # type: ignore[arg-type]
    f1 = make_file_meta(id="f1", created_date=_created_ago(timedelta(hours=3)))
    f_bad = make_file_meta(id="f_bad", created_date=_created_ago(timedelta(hours=2)))
    f3 = make_file_meta(id="f3", created_date=_created_ago(timedelta(hours=1)))
    client = FakeEventLogFileClient(
        files=[f1, f_bad, f3],
        rows_by_id={
            "f1": [{"TIMESTAMP_DERIVED": "20260630010000.000"}],
            "f_bad": [{"TIMESTAMP_DERIVED": "20260630020000.000"}],
            "f3": [{"TIMESTAMP_DERIVED": "20260630030000.000"}],
        },
        errors={"f_bad"},
    )
    cfg = make_elf_cfg()
    source = EventLogFileSource(cfg, client, sm_fields=[], poll_once=True)  # type: ignore[arg-type]

    entries = await _run_cycle(source, store)

    # The committed checkpoint must not have advanced past f_bad: its watermark
    # is f1's CreatedDate (< f_bad's), and neither f_bad nor f3 is recorded.
    final = json.loads(entries[-1].checkpoint.value)
    recorded_ids = [i for i, _ in final["ids"]]
    assert recorded_ids == ["f1"]
    assert final["last_created"] == f1.created_date

    # Cycle 2 (still failing): f_bad is re-listed and re-attempted, f3 still deferred.
    entries2 = await _run_cycle(source, store)
    assert entries2 == []
    assert client.download_calls == ["f1", "f_bad", "f_bad"]

    # Cycle 3 (recovered): f_bad then f3 ingested; watermark advances to f3.
    client.errors.clear()
    entries3 = await _run_cycle(source, store)
    assert len(entries3) == 2
    final3 = json.loads(entries3[-1].checkpoint.value)
    assert [i for i, _ in final3["ids"]] == ["f1", "f_bad", "f3"]
    assert final3["last_created"] == f3.created_date


@pytest.mark.asyncio
async def test_mid_stream_download_failure_stops_cycle_without_advancing(
    tmp_path: pytest.TempPathFactory,
) -> None:
    """A failure AFTER some rows were yielded (e.g. CSV parse error mid-file) is
    treated like a transient failure: rows already emitted carried the PRE-file
    checkpoint, the cycle stops, and the whole file is retried next cycle."""
    store = FileCheckpointStore(tmp_path / "state.json")  # type: ignore[arg-type]
    f1 = make_file_meta(id="f1", created_date=_created_ago(timedelta(hours=3)))
    f2 = make_file_meta(id="f2", created_date=_created_ago(timedelta(hours=2)))
    client = FakeEventLogFileClient(
        files=[f1, f2],
        rows_by_id={
            "f1": [
                {"TIMESTAMP_DERIVED": "20260630010000.000", "ROW": "a"},
                {"TIMESTAMP_DERIVED": "20260630010001.000", "ROW": "b"},
                {"TIMESTAMP_DERIVED": "20260630010002.000", "ROW": "c"},
            ],
            "f2": [{"TIMESTAMP_DERIVED": "20260630020000.000", "ROW": "d"}],
        },
        mid_stream_errors={"f1"},
    )
    cfg = make_elf_cfg()
    source = EventLogFileSource(cfg, client, sm_fields=[], poll_once=True)  # type: ignore[arg-type]

    entries = await _run_cycle(source, store)

    # Only the row(s) before the failure emitted, all carrying the PRE-file
    # checkpoint (empty), and f2 was not attempted.
    assert len(entries) == 1
    assert json.loads(entries[-1].checkpoint.value)["ids"] == []
    assert client.download_calls == ["f1"]

    # Recovery: the whole file is re-processed from the start next cycle.
    client.mid_stream_errors.clear()
    entries2 = await _run_cycle(source, store)
    assert len(entries2) == 4  # a, b, c (f1 re-read in full) + d (f2)
    final = json.loads(entries2[-1].checkpoint.value)
    assert [i for i, _ in final["ids"]] == ["f1", "f2"]


@pytest.mark.asyncio
async def test_persistently_failing_old_file_is_abandoned_past_max_age(
    tmp_path: pytest.TempPathFactory,
) -> None:
    """A failing file older than download_max_age is abandoned: its id IS folded into
    the checkpoint (and the watermark advances) so it can't wedge the watermark forever."""
    store = FileCheckpointStore(tmp_path / "state.json")  # type: ignore[arg-type]
    f_ancient = make_file_meta(id="f_ancient", created_date=_created_ago(timedelta(hours=48)))
    f_recent = make_file_meta(id="f_recent", created_date=_created_ago(timedelta(hours=1)))
    client = FakeEventLogFileClient(
        files=[f_ancient, f_recent],
        rows_by_id={"f_recent": [{"TIMESTAMP_DERIVED": "20260630030000.000"}]},
        errors={"f_ancient"},
    )
    cfg = make_elf_cfg(download_max_age=timedelta(hours=24))
    source = EventLogFileSource(cfg, client, sm_fields=[], poll_once=True)  # type: ignore[arg-type]
    stop = asyncio.Event()
    entries = [e async for e in source.events(store, stop)]

    assert len(entries) == 1  # only f_recent has rows
    final = json.loads(entries[-1].checkpoint.value)
    # The abandoned ancient file is recorded so it won't be retried indefinitely.
    assert [i for i, _ in final["ids"]] == ["f_ancient", "f_recent"]
    assert final["last_created"] == f_recent.created_date


@pytest.mark.asyncio
async def test_mid_file_failure_past_max_age_is_abandoned_and_type_keeps_progressing(
    tmp_path: pytest.TempPathFactory,
) -> None:
    """Issue #41: a MID-file failure (e.g. csv.Error on an oversized field) on a
    file older than download_max_age gets the SAME abandon escape as the
    first-row failure path — previously the mid-file path always returned
    without abandoning, so the failing file (the watermark boundary) was
    re-downloaded and re-failed every cycle, wedging the whole EventType."""
    store = FileCheckpointStore(tmp_path / "state.json")  # type: ignore[arg-type]
    f_ancient = make_file_meta(id="f_ancient", created_date=_created_ago(timedelta(hours=48)))
    f_recent = make_file_meta(id="f_recent", created_date=_created_ago(timedelta(hours=1)))
    client = FakeEventLogFileClient(
        files=[f_ancient, f_recent],
        rows_by_id={
            "f_ancient": [
                {"TIMESTAMP_DERIVED": "20260630010000.000", "ROW": "a"},
                {"TIMESTAMP_DERIVED": "20260630010001.000", "ROW": "b"},
            ],
            "f_recent": [{"TIMESTAMP_DERIVED": "20260630030000.000", "ROW": "c"}],
        },
        mid_stream_errors={"f_ancient"},
    )
    cfg = make_elf_cfg(download_max_age=timedelta(hours=24))
    source = EventLogFileSource(cfg, client, sm_fields=[], poll_once=True)  # type: ignore[arg-type]

    entries = await _run_cycle(source, store)

    # Row "a" (before the mid-file fault) still emits with the pre-file
    # checkpoint; "b" (never confirmed non-last) is discarded along with the
    # rest of the abandoned file; f_recent still processes normally this same
    # cycle — the EventType keeps progressing instead of wedging.
    assert len(entries) == 2
    assert '"ROW": "a"' in entries[0].line
    assert '"ROW": "c"' in entries[1].line
    final_mid = json.loads(entries[-1].checkpoint.value)
    assert [i for i, _ in final_mid["ids"]] == ["f_ancient", "f_recent"]
    assert final_mid["last_created"] == f_recent.created_date

    # Next cycle: f_ancient's watermark has been passed, so it is no longer
    # re-listed/re-downloaded — confirms the type keeps progressing rather
    # than wedging on the same failing file forever.
    client.mid_stream_errors.clear()
    client.download_calls.clear()
    entries2 = await _run_cycle(source, store)
    assert entries2 == []
    assert client.download_calls == []


@pytest.mark.asyncio
async def test_fully_sampled_out_cycle_emits_checkpoint_only_and_advances_watermark(
    tmp_path: pytest.TempPathFactory,
) -> None:
    """Issue #64: when every row in a cycle is sampled out, the in-memory
    cursor still advances past the processed file, but without a
    checkpoint_only token nothing durable would advance — the next cycle (and
    every cycle after a restart) would re-list, re-download, and re-drop the
    exact same file forever."""
    store = FileCheckpointStore(tmp_path / "state.json")  # type: ignore[arg-type]
    f1 = make_file_meta(id="f1", created_date=_created_ago(timedelta(hours=1)))
    rows = [{"TIMESTAMP_DERIVED": "20260630010000.000", "ROW": "a"}]
    client = FakeEventLogFileClient(files=[f1], rows_by_id={"f1": rows})
    cfg = make_elf_cfg(poll_interval=timedelta(0))
    cfg.event_types[0].sample = 0.0  # deterministically drops every row
    source = EventLogFileSource(cfg, client, sm_fields=[], poll_once=True)  # type: ignore[arg-type]

    entries = await _run_cycle(source, store)

    assert len(entries) == 1
    assert entries[0].checkpoint_only is True
    assert entries[0].line == ""
    assert entries[0].labels == {}

    raw = await store.load("eventlogfile:Login")
    assert raw is not None
    final_sampled = json.loads(raw)
    assert [i for i, _ in final_sampled["ids"]] == ["f1"]
    assert final_sampled["last_created"] == f1.created_date

    # Next cycle: f1 is not re-downloaded — the watermark durably advanced.
    client.download_calls.clear()
    entries2 = await _run_cycle(source, store)
    assert entries2 == []
    assert client.download_calls == []


@pytest.mark.asyncio
async def test_normal_cycle_does_not_emit_extra_checkpoint_only_entry(
    tmp_path: pytest.TempPathFactory,
) -> None:
    """A cycle where the last emitted entry already carries the final cursor
    state must NOT also emit a trailing checkpoint_only entry."""
    store = FileCheckpointStore(tmp_path / "state.json")  # type: ignore[arg-type]
    f1 = make_file_meta(id="f1", created_date=_created_ago(timedelta(hours=1)))
    rows = [{"TIMESTAMP_DERIVED": "20260630010000.000", "ROW": "a"}]
    client = FakeEventLogFileClient(files=[f1], rows_by_id={"f1": rows})
    cfg = make_elf_cfg()
    source = EventLogFileSource(cfg, client, sm_fields=[], poll_once=True)  # type: ignore[arg-type]

    entries = await _run_cycle(source, store)

    assert len(entries) == 1
    assert entries[0].checkpoint_only is False


@pytest.mark.asyncio
async def test_settle_window_skips_too_fresh_files(
    tmp_path: pytest.TempPathFactory,
) -> None:
    """Files created within settle_window are not downloaded this cycle (avoids pulling
    half-written hourly files); older files still process."""
    store = FileCheckpointStore(tmp_path / "state.json")  # type: ignore[arg-type]
    f_old = make_file_meta(id="f_old", created_date=_created_ago(timedelta(hours=2)))
    f_fresh = make_file_meta(id="f_fresh", created_date=_created_ago(timedelta(minutes=1)))
    client = FakeEventLogFileClient(
        files=[f_old, f_fresh],
        rows_by_id={
            "f_old": [{"TIMESTAMP_DERIVED": "20260630010000.000"}],
            "f_fresh": [{"TIMESTAMP_DERIVED": "20260630020000.000"}],
        },
    )
    cfg = make_elf_cfg(settle_window=timedelta(minutes=10))
    source = EventLogFileSource(cfg, client, sm_fields=[], poll_once=True)  # type: ignore[arg-type]
    stop = asyncio.Event()
    entries = [e async for e in source.events(store, stop)]

    # f_fresh is within the 10m settle window: not downloaded, not recorded.
    assert client.download_calls == ["f_old"]
    assert len(entries) == 1
    final = json.loads(entries[-1].checkpoint.value)
    assert [i for i, _ in final["ids"]] == ["f_old"]


@pytest.mark.asyncio
async def test_default_hourly_settle_window_defers_recent_file(
    tmp_path: pytest.TempPathFactory,
) -> None:
    """Issue #66: EventLogFileConfig auto-defaults settle_window to 5m when
    interval=Hourly and the operator left it unset (config-level default,
    already in place). This confirms the SOURCE honours that default
    end-to-end: a file created within the last 5 minutes is deferred (not
    downloaded) this cycle, without needing an explicit settle_window."""
    store = FileCheckpointStore(tmp_path / "state.json")  # type: ignore[arg-type]
    f_fresh = make_file_meta(id="f_fresh", created_date=_created_ago(timedelta(minutes=1)))
    client = FakeEventLogFileClient(
        files=[f_fresh],
        rows_by_id={"f_fresh": [{"TIMESTAMP_DERIVED": "20260630010000.000"}]},
    )
    cfg = EventLogFileConfig(
        enabled=True,
        interval="Hourly",  # settle_window left unset -> auto-defaults to 5m
        event_types=["Login"],
        poll_interval=timedelta(seconds=0),
        lookback=timedelta(hours=24),
    )
    assert cfg.settle_window == timedelta(minutes=5)  # the config-level default landed

    source = EventLogFileSource(cfg, client, sm_fields=[], poll_once=True)  # type: ignore[arg-type]
    entries = [e async for e in source.events(store, asyncio.Event())]

    # Within the (auto-defaulted) 5m settle window: deferred, not downloaded,
    # and the checkpoint is untouched (nothing to re-list differently).
    assert client.download_calls == []
    assert entries == []


# ---------------------------------------------------------------------------
# Re-issued daily files (C4): Salesforce regenerates DAILY EventLogFile records
# in place — SAME Id, CreatedDate bumped, blob replaced with the full superset.
# Hourly late events instead create NEW sibling records (new Id, Sequence++).


@pytest.mark.asyncio
async def test_daily_reissued_file_same_id_newer_created_is_reprocessed(
    tmp_path: pytest.TempPathFactory,
) -> None:
    """A daily file re-listed with the SAME Id but a NEWER CreatedDate carries
    late rows (full superset) — it must be re-downloaded, not skipped."""
    store = FileCheckpointStore(tmp_path / "state.json")  # type: ignore[arg-type]
    created_v1 = _created_ago(timedelta(hours=6))
    created_v2 = _created_ago(timedelta(hours=1))
    f_v1 = make_file_meta(id="fd", interval="Daily", created_date=created_v1)
    client = FakeEventLogFileClient(
        files=[f_v1],
        rows_by_id={"fd": [{"TIMESTAMP_DERIVED": "20260630010000.000", "ROW": "a"}]},
    )
    cfg = make_elf_cfg(interval="Daily")
    source = EventLogFileSource(cfg, client, sm_fields=[], poll_once=True)  # type: ignore[arg-type]

    entries = await _run_cycle(source, store)
    assert len(entries) == 1

    # Salesforce regenerates the record in place: same Id, bumped CreatedDate,
    # superset content.
    client.files = [make_file_meta(id="fd", interval="Daily", created_date=created_v2)]
    client.rows_by_id["fd"] = [
        {"TIMESTAMP_DERIVED": "20260630010000.000", "ROW": "a"},
        {"TIMESTAMP_DERIVED": "20260630013000.000", "ROW": "late"},
    ]
    entries2 = await _run_cycle(source, store)

    # Re-processed in full (duplicate row "a" is the accepted at-least-once cost).
    assert len(entries2) == 2
    assert '"ROW": "late"' in entries2[1].line
    final = json.loads(entries2[-1].checkpoint.value)
    assert final["ids"] == [["fd", created_v2]]
    assert final["last_created"] == created_v2


@pytest.mark.asyncio
async def test_same_id_same_created_not_reprocessed(
    tmp_path: pytest.TempPathFactory,
) -> None:
    """An unchanged file (same Id, same CreatedDate) re-listed on the next cycle
    is skipped — hourly sibling records (NEW ids) are still ingested exactly once."""
    store = FileCheckpointStore(tmp_path / "state.json")  # type: ignore[arg-type]
    f1 = make_file_meta(id="f1", created_date=_created_ago(timedelta(hours=3)))
    client = FakeEventLogFileClient(
        files=[f1],
        rows_by_id={"f1": [{"TIMESTAMP_DERIVED": "20260630010000.000"}]},
    )
    cfg = make_elf_cfg()
    source = EventLogFileSource(cfg, client, sm_fields=[], poll_once=True)  # type: ignore[arg-type]

    entries = await _run_cycle(source, store)
    assert len(entries) == 1

    # Cycle 2: f1 unchanged, plus a NEW hourly sibling record (new Id, Sequence++).
    f2 = make_file_meta(id="f2", created_date=_created_ago(timedelta(hours=1)))
    client.files = [f1, f2]
    client.rows_by_id["f2"] = [{"TIMESTAMP_DERIVED": "20260630030000.000", "ROW": "s2"}]
    entries2 = await _run_cycle(source, store)

    assert len(entries2) == 1  # only the sibling; f1 not re-ingested
    assert '"ROW": "s2"' in entries2[0].line
    assert client.download_calls == ["f1", "f2"]

    # Cycle 3: nothing new -> nothing ingested.
    entries3 = await _run_cycle(source, store)
    assert entries3 == []


@pytest.mark.asyncio
async def test_legacy_checkpoint_plain_id_list_loads_and_matches_any_created(
    tmp_path: pytest.TempPathFactory,
) -> None:
    """A pre-upgrade checkpoint carries bare id strings; they must load without
    crashing and match ANY CreatedDate (legacy semantics: skip)."""
    store = FileCheckpointStore(tmp_path / "state.json")  # type: ignore[arg-type]
    key = "eventlogfile:Login"
    await store.commit(key, json.dumps({"last_created": "2026-06-30T00:00:00Z", "ids": ["f1"]}))

    # f1 re-listed with a (different) CreatedDate: legacy id matches any -> skipped.
    f1 = make_file_meta(id="f1", created_date=_created_ago(timedelta(hours=2)))
    f2 = make_file_meta(id="f2", created_date=_created_ago(timedelta(hours=1)))
    client = FakeEventLogFileClient(
        files=[f1, f2],
        rows_by_id={
            "f1": [{"TIMESTAMP_DERIVED": "20260630010000.000"}],
            "f2": [{"TIMESTAMP_DERIVED": "20260630020000.000"}],
        },
    )
    cfg = make_elf_cfg()
    source = EventLogFileSource(cfg, client, sm_fields=[], poll_once=True)  # type: ignore[arg-type]

    entries = await _run_cycle(source, store)

    assert client.download_calls == ["f2"]
    assert len(entries) == 1
    final = json.loads(entries[-1].checkpoint.value)
    # The legacy bare id is carried through unchanged; the new file is a pair.
    assert final["ids"] == ["f1", ["f2", f2.created_date]]


# ---------------------------------------------------------------------------
# Cycle-level resiliency (C2/C5): listing failures and API throttling must not
# crash the connector or hammer remaining work in the same cycle.


@pytest.mark.asyncio
async def test_listing_failure_skips_cycle_without_raising(
    tmp_path: pytest.TempPathFactory,
) -> None:
    store = FileCheckpointStore(tmp_path / "state.json")  # type: ignore[arg-type]
    f1 = make_file_meta(id="f1", created_date=_created_ago(timedelta(hours=2)))
    client = FakeEventLogFileClient(
        files=[f1],
        rows_by_id={"f1": [{"TIMESTAMP_DERIVED": "20260630010000.000"}]},
        list_error=EventLogFileError("listing failed: HTTP 503"),
    )
    cfg = make_elf_cfg()
    source = EventLogFileSource(cfg, client, sm_fields=[], poll_once=True)  # type: ignore[arg-type]

    entries = await _run_cycle(source, store)  # must not raise
    assert entries == []
    assert client.download_calls == []

    # Next cycle proceeds normally once the error clears.
    client.list_error = None
    entries2 = await _run_cycle(source, store)
    assert len(entries2) == 1


@pytest.mark.asyncio
@pytest.mark.parametrize("concurrency", [1, 4])
async def test_throttled_listing_aborts_rest_of_cycle(
    tmp_path: pytest.TempPathFactory, concurrency: int
) -> None:
    """REQUEST_LIMIT_EXCEEDED on one type's listing must stop the remaining
    types this cycle (backing off until the next poll) instead of burning more
    of the exhausted API budget. Holds at concurrency=1 (byte-identical to the
    old sequential loop) and at concurrency=4 (our fake client never actually
    suspends mid-call, so even "concurrent" tasks still run one-at-a-time to
    completion before the next is scheduled — see the dedicated
    ``test_throttle_lets_already_running_type_finish`` for genuine overlap)."""
    store = FileCheckpointStore(tmp_path / "state.json")  # type: ignore[arg-type]
    client = FakeEventLogFileClient(
        files=[],
        rows_by_id={},
        list_error=EventLogFileThrottledError("HTTP 403 REQUEST_LIMIT_EXCEEDED"),
    )
    cfg = make_elf_cfg(event_types=["Login", "API", "Report"], concurrency=concurrency)
    source = EventLogFileSource(cfg, client, sm_fields=[], poll_once=True)  # type: ignore[arg-type]

    entries = await _run_cycle(source, store)  # must not raise
    assert entries == []
    assert len(client.list_calls) == 1  # aborted after the first throttled type


@pytest.mark.asyncio
@pytest.mark.parametrize("concurrency", [1, 4])
async def test_throttled_download_aborts_rest_of_cycle(
    tmp_path: pytest.TempPathFactory, concurrency: int
) -> None:
    store = FileCheckpointStore(tmp_path / "state.json")  # type: ignore[arg-type]
    f1 = make_file_meta(id="f1", created_date=_created_ago(timedelta(hours=2)))
    f2 = make_file_meta(id="f2", created_date=_created_ago(timedelta(hours=1)))
    client = FakeEventLogFileClient(
        files=[f1, f2],
        rows_by_id={
            "f1": [{"TIMESTAMP_DERIVED": "20260630010000.000"}],
            "f2": [{"TIMESTAMP_DERIVED": "20260630020000.000"}],
        },
        throttled={"f1"},
    )
    cfg = make_elf_cfg(event_types=["Login", "API"], concurrency=concurrency)
    source = EventLogFileSource(cfg, client, sm_fields=[], poll_once=True)  # type: ignore[arg-type]

    entries = await _run_cycle(source, store)  # must not raise
    assert entries == []
    assert client.download_calls == ["f1"]  # f2 skipped
    assert len(client.list_calls) == 1  # second type never listed this cycle


# ---------------------------------------------------------------------------
# EventType discovery / wildcard


def _wildcard_cfg(*, exclude: list[str] | None = None, extra: list[object] | None = None):
    from sf2loki.config import EventLogFileConfig

    return EventLogFileConfig(
        enabled=True,
        interval="Hourly",
        event_types=["*", *(extra or [])],
        exclude=exclude or [],
        poll_interval=timedelta(seconds=0),
        lookback=timedelta(hours=24),
    )


@pytest.mark.asyncio
async def test_wildcard_discovers_and_processes_all_types(tmp_path) -> None:
    store = FileCheckpointStore(tmp_path / "s.json")  # type: ignore[arg-type]
    client = FakeEventLogFileClient(
        files=[], rows_by_id={}, discovered_types=["API", "Report", "Search"]
    )
    source = EventLogFileSource(_wildcard_cfg(), client, sm_fields=[], poll_once=True)  # type: ignore[arg-type]
    _ = [e async for e in source.events(store, asyncio.Event())]
    assert client.discover_calls == ["Hourly"]
    assert sorted({c[0] for c in client.list_calls}) == ["API", "Report", "Search"]


@pytest.mark.asyncio
async def test_wildcard_exclude_drops_types(tmp_path) -> None:
    store = FileCheckpointStore(tmp_path / "s.json")  # type: ignore[arg-type]
    client = FakeEventLogFileClient(
        files=[], rows_by_id={}, discovered_types=["API", "ApexCallout", "Report"]
    )
    source = EventLogFileSource(
        _wildcard_cfg(exclude=["ApexCallout"]),
        client,
        sm_fields=[],
        poll_once=True,  # type: ignore[arg-type]
    )
    _ = [e async for e in source.events(store, asyncio.Event())]
    assert sorted({c[0] for c in client.list_calls}) == ["API", "Report"]


@pytest.mark.asyncio
async def test_wildcard_includes_explicit_not_discovered(tmp_path) -> None:
    store = FileCheckpointStore(tmp_path / "s.json")  # type: ignore[arg-type]
    client = FakeEventLogFileClient(files=[], rows_by_id={}, discovered_types=["API"])
    source = EventLogFileSource(
        _wildcard_cfg(extra=["Report"]),
        client,
        sm_fields=[],
        poll_once=True,  # type: ignore[arg-type]
    )
    _ = [e async for e in source.events(store, asyncio.Event())]
    assert sorted({c[0] for c in client.list_calls}) == ["API", "Report"]


@pytest.mark.asyncio
async def test_wildcard_explicit_override_wins(tmp_path) -> None:
    store = FileCheckpointStore(tmp_path / "s.json")  # type: ignore[arg-type]
    client = FakeEventLogFileClient(
        files=[make_file_meta(id="f1", event_type="Report")],
        rows_by_id={
            "f1": [{"TIMESTAMP_DERIVED": "2026-06-30T01:00:00.000Z", "RPT": "r1", "OTHER": "o"}]
        },
        discovered_types=["Report"],
    )
    cfg = _wildcard_cfg(extra=[{"name": "Report", "structured_metadata_fields": ["RPT"]}])
    source = EventLogFileSource(cfg, client, sm_fields=["OTHER"], poll_once=True)  # type: ignore[arg-type]
    entries = [e async for e in source.events(store, asyncio.Event())]
    assert len(entries) == 1
    # per-type override promotes RPT (not the global OTHER); level always injected
    assert entries[0].structured_metadata == {"RPT": "r1", "level": "info"}


@pytest.mark.asyncio
async def test_discovery_failure_falls_back_to_explicit(tmp_path) -> None:
    store = FileCheckpointStore(tmp_path / "s.json")  # type: ignore[arg-type]
    client = FakeEventLogFileClient(files=[], rows_by_id={}, discover_error=True)
    source = EventLogFileSource(
        _wildcard_cfg(extra=["API"]),
        client,
        sm_fields=[],
        poll_once=True,  # type: ignore[arg-type]
    )
    _ = [e async for e in source.events(store, asyncio.Event())]  # must not raise
    assert client.discover_calls == ["Hourly"]
    assert sorted({c[0] for c in client.list_calls}) == ["API"]


@pytest.mark.asyncio
async def test_no_wildcard_skips_discovery(tmp_path) -> None:
    store = FileCheckpointStore(tmp_path / "s.json")  # type: ignore[arg-type]
    client = FakeEventLogFileClient(files=[], rows_by_id={}, discovered_types=["API"])
    source = EventLogFileSource(
        make_elf_cfg(event_types=["Login"]), client, sm_fields=[], poll_once=True
    )  # type: ignore[arg-type]
    _ = [e async for e in source.events(store, asyncio.Event())]
    assert client.discover_calls == []
    assert sorted({c[0] for c in client.list_calls}) == ["Login"]


# ---------------------------------------------------------------------------
# Poll-error counter (issue #19): listing/discovery cycle failures must be
# countable (downloads are already counted by eventlogfile_download_errors).


@pytest.mark.asyncio
async def test_listing_failure_increments_poll_error_counter(tmp_path) -> None:
    store = FileCheckpointStore(tmp_path / "s.json")  # type: ignore[arg-type]
    client = FakeEventLogFileClient(
        files=[], rows_by_id={}, list_error=EventLogFileError("listing failed: HTTP 503")
    )
    metrics = Metrics()
    source = EventLogFileSource(
        make_elf_cfg(), client, sm_fields=[], metrics=metrics, poll_once=True
    )  # type: ignore[arg-type]
    _ = [e async for e in source.events(store, asyncio.Event())]

    assert (
        metrics.registry.get_sample_value(
            "sf2loki_soql_poll_errors_total", {"source": "eventlogfile", "object": "Login"}
        )
        == 1.0
    )


@pytest.mark.asyncio
async def test_discovery_failure_increments_poll_error_counter(tmp_path) -> None:
    store = FileCheckpointStore(tmp_path / "s.json")  # type: ignore[arg-type]
    client = FakeEventLogFileClient(files=[], rows_by_id={}, discover_error=True)
    metrics = Metrics()
    source = EventLogFileSource(
        _wildcard_cfg(), client, sm_fields=[], metrics=metrics, poll_once=True
    )  # type: ignore[arg-type]
    _ = [e async for e in source.events(store, asyncio.Event())]

    assert (
        metrics.registry.get_sample_value(
            "sf2loki_soql_poll_errors_total", {"source": "eventlogfile", "object": "discovery"}
        )
        == 1.0
    )


@pytest.mark.asyncio
async def test_download_failure_does_not_increment_poll_error_counter(tmp_path) -> None:
    """Download failures are counted by eventlogfile_download_errors (in the
    client) — soql_poll_errors must NOT double-count them."""
    store = FileCheckpointStore(tmp_path / "s.json")  # type: ignore[arg-type]
    f1 = make_file_meta(id="f1", created_date=_created_ago(timedelta(hours=2)))
    client = FakeEventLogFileClient(
        files=[f1],
        rows_by_id={"f1": [{"TIMESTAMP_DERIVED": "20260630010000.000"}]},
        errors={"f1"},
    )
    metrics = Metrics()
    source = EventLogFileSource(
        make_elf_cfg(), client, sm_fields=[], metrics=metrics, poll_once=True
    )  # type: ignore[arg-type]
    _ = [e async for e in source.events(store, asyncio.Event())]

    assert (
        metrics.registry.get_sample_value(
            "sf2loki_soql_poll_errors_total", {"source": "eventlogfile", "object": "Login"}
        )
        is None
    )


# ---------------------------------------------------------------------------
# Deterministic timestamp fallback (issue #20): an unparseable row timestamp
# falls back to the FILE's CreatedDate (stable across replays), counted via
# timestamp_fallbacks{source="eventlogfile"}; >1h-old fallbacks clamp near now.


@pytest.mark.asyncio
async def test_unparseable_row_timestamp_falls_back_to_file_created_date(tmp_path) -> None:
    store = FileCheckpointStore(tmp_path / "s.json")  # type: ignore[arg-type]
    created = _created_ago(timedelta(minutes=30))  # within 1h: no OOO clamp
    f1 = make_file_meta(id="f1", created_date=created)
    rows = [
        {"TIMESTAMP_DERIVED": "garbage", "ROW": "bad"},
        {"TIMESTAMP_DERIVED": "20260630010000.000", "ROW": "good"},
    ]
    client = FakeEventLogFileClient(files=[f1], rows_by_id={"f1": rows})
    metrics = Metrics()
    source = EventLogFileSource(
        make_elf_cfg(), client, sm_fields=[], metrics=metrics, poll_once=True
    )  # type: ignore[arg-type]
    entries = [e async for e in source.events(store, asyncio.Event())]

    assert len(entries) == 2
    expected = datetime.strptime(created, "%Y-%m-%dT%H:%M:%S.%f%z")
    assert entries[0].timestamp == expected  # file CreatedDate, not now()
    assert entries[1].timestamp == datetime(2026, 6, 30, 1, 0, 0, tzinfo=UTC)
    # Only the unparseable row counted.
    assert (
        metrics.registry.get_sample_value(
            "sf2loki_timestamp_fallbacks_total", {"source": "eventlogfile"}
        )
        == 1.0
    )


@pytest.mark.asyncio
async def test_old_created_date_fallback_is_clamped_near_now(tmp_path) -> None:
    """A fallback >1h old would be rejected by Loki's OOO guard (dropping the
    row entirely) — clamp near now instead; the fallback is still counted."""
    store = FileCheckpointStore(tmp_path / "s.json")  # type: ignore[arg-type]
    f1 = make_file_meta(id="f1", created_date=_created_ago(timedelta(hours=3)))
    rows = [{"TIMESTAMP_DERIVED": "garbage", "ROW": "bad"}]
    client = FakeEventLogFileClient(files=[f1], rows_by_id={"f1": rows})
    metrics = Metrics()
    source = EventLogFileSource(
        make_elf_cfg(), client, sm_fields=[], metrics=metrics, poll_once=True
    )  # type: ignore[arg-type]
    entries = [e async for e in source.events(store, asyncio.Event())]

    assert len(entries) == 1
    age = datetime.now(UTC) - entries[0].timestamp
    assert timedelta(0) <= age < timedelta(minutes=10)
    assert (
        metrics.registry.get_sample_value(
            "sf2loki_timestamp_fallbacks_total", {"source": "eventlogfile"}
        )
        == 1.0
    )


# ---------------------------------------------------------------------------
# Carried-id window (issue #21a): ALL files sharing the watermark CreatedDate
# must stay in the carried window — a bulk backfill (>200 files with one
# CreatedDate) must not re-download uncovered files forever.


@pytest.mark.asyncio
async def test_bulk_files_sharing_one_created_date_all_deduped_next_cycle(tmp_path) -> None:
    store = FileCheckpointStore(tmp_path / "s.json")  # type: ignore[arg-type]
    created = _created_ago(timedelta(hours=2))
    n = 250  # exceeds the old 200-pair cap
    files = [make_file_meta(id=f"f{i:03d}", created_date=created) for i in range(n)]
    rows_by_id = {
        f"f{i:03d}": [{"TIMESTAMP_DERIVED": "20260630010000.000", "N": str(i)}] for i in range(n)
    }
    client = FakeEventLogFileClient(files=files, rows_by_id=rows_by_id)
    cfg = make_elf_cfg()
    source = EventLogFileSource(cfg, client, sm_fields=[], poll_once=True)  # type: ignore[arg-type]

    entries = await _run_cycle(source, store)
    assert len(entries) == n
    assert len(client.download_calls) == n

    # Cycle 2 re-lists everything at the tied CreatedDate (>= watermark): ALL
    # files must dedup — zero re-downloads, zero duplicate rows.
    entries2 = await _run_cycle(source, store)
    assert entries2 == []
    assert len(client.download_calls) == n


# ---------------------------------------------------------------------------
# Clock-skew hardening (issue #21b): CreatedDate comparisons use Salesforce
# server time (from the Date response header) when the local clock is skewed.


@pytest.mark.asyncio
async def test_clock_skew_applied_to_settle_gate(tmp_path) -> None:
    """Local clock 10 minutes BEHIND Salesforce: a file created 'now' by the
    local clock is really 10 minutes old in server time, so it is settled and
    must be processed (unadjusted, the settle gate would skip it)."""
    store = FileCheckpointStore(tmp_path / "s.json")  # type: ignore[arg-type]
    f1 = make_file_meta(id="f1", created_date=_created_ago(timedelta(0)))
    client = FakeEventLogFileClient(
        files=[f1],
        rows_by_id={"f1": [{"TIMESTAMP_DERIVED": "20260630010000.000"}]},
        skew=timedelta(minutes=10),  # server time = local now + 10m
    )
    cfg = make_elf_cfg(settle_window=timedelta(minutes=5))
    source = EventLogFileSource(cfg, client, sm_fields=[], poll_once=True)  # type: ignore[arg-type]
    entries = [e async for e in source.events(store, asyncio.Event())]

    assert client.download_calls == ["f1"]
    assert len(entries) == 1


@pytest.mark.asyncio
async def test_small_clock_skew_is_ignored(tmp_path) -> None:
    """|skew| <= 30s (Date-header noise / latency) must NOT shift comparisons."""
    store = FileCheckpointStore(tmp_path / "s.json")  # type: ignore[arg-type]
    f1 = make_file_meta(id="f1", created_date=_created_ago(timedelta(0)))
    client = FakeEventLogFileClient(
        files=[f1],
        rows_by_id={"f1": [{"TIMESTAMP_DERIVED": "20260630010000.000"}]},
        skew=timedelta(seconds=20),
    )
    cfg = make_elf_cfg(settle_window=timedelta(minutes=5))
    source = EventLogFileSource(cfg, client, sm_fields=[], poll_once=True)  # type: ignore[arg-type]
    entries = [e async for e in source.events(store, asyncio.Event())]

    # Fresh file stays unsettled: 20s of skew is within the ignore threshold.
    assert client.download_calls == []
    assert entries == []


# ---------------------------------------------------------------------------
# Transforms + deterministic sampling (issues #27 / #26)


from sf2loki.config import TransformRule  # noqa: E402
from sf2loki.shaping import should_keep  # noqa: E402


@pytest.mark.asyncio
async def test_discovered_type_inherits_wildcard_sample() -> None:
    """A wildcard-discovered EventType inherits the "*" entry's sample rate."""
    cfg = EventLogFileConfig(
        enabled=True,
        interval="Hourly",
        event_types=[EventLogFileTypeConfig(name="*", sample=0.25)],
    )
    client = FakeEventLogFileClient(
        files=[], rows_by_id={}, discovered_types=["Login", "ApiTotalUsage"]
    )
    src = EventLogFileSource(cfg, client, sm_fields=[])

    resolved = await src._resolve_event_types()

    assert {t.name for t in resolved} == {"Login", "ApiTotalUsage"}
    assert all(t.sample == 0.25 for t in resolved)


@pytest.mark.asyncio
async def test_transform_redacting_timestamp_column_triggers_fallback(
    tmp_path: pytest.TempPathFactory,
) -> None:
    """A transform that drops the timestamp column makes the row fall back to the
    file's CreatedDate — proving transforms run BEFORE timestamp extraction."""
    store = FileCheckpointStore(tmp_path / "state.json")  # type: ignore[arg-type]
    created = _created_ago(timedelta(minutes=1))
    file_meta = make_file_meta(id="f1", created_date=created)
    # Row has ONLY TIMESTAMP_DERIVED as a time source; dropping it forces fallback.
    rows = [{"TIMESTAMP_DERIVED": "20260630010000.000", "USER": "x"}]
    client = FakeEventLogFileClient(files=[file_meta], rows_by_id={"f1": rows})
    cfg = make_elf_cfg(event_types=["Login"], poll_interval=timedelta(0))
    cfg.transforms.append(TransformRule(action="drop_field", fields=["TIMESTAMP_DERIVED"]))
    metrics = Metrics()
    src = EventLogFileSource(cfg, client, sm_fields=[], metrics=metrics, poll_once=True)

    entries = await _run_cycle(src, store)

    assert len(entries) == 1
    assert "TIMESTAMP_DERIVED" not in entries[0].line
    fallbacks = metrics.registry.get_sample_value(
        "sf2loki_timestamp_fallbacks_total", {"source": "eventlogfile"}
    )
    assert fallbacks == 1.0


@pytest.mark.asyncio
async def test_sampled_out_row_not_emitted_but_others_flow(
    tmp_path: pytest.TempPathFactory,
) -> None:
    """A sampled-out row emits no entry (counted) while kept rows still flow."""
    rate = 0.5
    dropped_req = next(f"req-{i}" for i in range(1000) if not should_keep(f"req-{i}", rate))
    kept_req = next(f"req-{i}" for i in range(1000) if should_keep(f"req-{i}", rate))
    store = FileCheckpointStore(tmp_path / "state.json")  # type: ignore[arg-type]
    file_meta = make_file_meta(id="f1", created_date=_created_ago(timedelta(minutes=1)))
    rows = [
        {"TIMESTAMP_DERIVED": "20260630010000.000", "REQUEST_ID": dropped_req},
        {"TIMESTAMP_DERIVED": "20260630010001.000", "REQUEST_ID": kept_req},
    ]
    client = FakeEventLogFileClient(files=[file_meta], rows_by_id={"f1": rows})
    cfg = make_elf_cfg(poll_interval=timedelta(0))
    cfg.event_types[0].sample = rate
    metrics = Metrics()
    src = EventLogFileSource(cfg, client, sm_fields=[], metrics=metrics, poll_once=True)

    entries = await _run_cycle(src, store)

    assert len(entries) == 1
    assert entries[0].line.find(kept_req) != -1
    sampled = metrics.registry.get_sample_value(
        "sf2loki_entries_sampled_out_total", {"source": "eventlogfile", "event_type": "Login"}
    )
    assert sampled == 1.0


# ---------------------------------------------------------------------------
# Bounded-concurrency per-cycle processing (issue #25)


@pytest.mark.asyncio
async def test_concurrency_is_bounded_and_reached(tmp_path: pytest.TempPathFactory) -> None:
    """At most cfg.concurrency types are ever "in flight" at once, and that
    many really DO overlap (not just "never exceeded the limit" by accident —
    the probe's max_seen must actually hit the configured value)."""
    store = FileCheckpointStore(tmp_path / "state.json")  # type: ignore[arg-type]
    n_types = 6
    concurrency = 2
    probe = _ConcurrencyProbe()
    client = FakeEventLogFileClient(files=[], rows_by_id={}, probe=probe, probe_delay=0.02)
    cfg = make_elf_cfg(event_types=[f"Type{i}" for i in range(n_types)], concurrency=concurrency)
    source = EventLogFileSource(cfg, client, sm_fields=[], poll_once=True)  # type: ignore[arg-type]

    entries = [e async for e in source.events(store, asyncio.Event())]

    assert entries == []  # no files configured — this test is purely about scheduling
    assert len(client.list_calls) == n_types  # every type still got processed
    assert probe.current == 0  # every probe entry was matched by an exit
    assert probe.max_seen == concurrency  # bound reached exactly, never exceeded


@pytest.mark.asyncio
async def test_concurrent_types_interleave_and_commit_independent_checkpoints(
    tmp_path: pytest.TempPathFactory,
) -> None:
    """Two types running concurrently must each commit their OWN correct
    checkpoint — proven with genuine overlap (the probe), not just by luck of
    fast synchronous fakes never actually yielding control.

    Mirrors production: the pipeline commits the most recent checkpoint value
    per KEY once a batch flushes, so this drives ``events()`` directly and
    keeps only the last value seen per key — exactly what a real flush does —
    rather than the single-type ``_run_cycle`` helper (which only keeps the
    very last entry of the whole stream and would silently ignore the other
    type's checkpoint entirely).
    """
    store = FileCheckpointStore(tmp_path / "state.json")  # type: ignore[arg-type]
    login_file = make_file_meta(id="login-f1", created_date="2026-06-30T01:00:00.000+0000")
    api_file = make_file_meta(id="api-f1", created_date="2026-06-30T01:05:00.000+0000")
    probe = _ConcurrencyProbe()
    client = FakeEventLogFileClient(
        files=[],
        rows_by_id={
            "login-f1": [
                {"TIMESTAMP_DERIVED": "20260630010000.000", "ROW": "login-a"},
                {"TIMESTAMP_DERIVED": "20260630010001.000", "ROW": "login-b"},
            ],
            "api-f1": [
                {"TIMESTAMP_DERIVED": "20260630010500.000", "ROW": "api-a"},
                {"TIMESTAMP_DERIVED": "20260630010501.000", "ROW": "api-b"},
            ],
        },
        files_by_type={"Login": [login_file], "API": [api_file]},
        probe=probe,
        probe_delay=0.02,
    )
    cfg = make_elf_cfg(event_types=["Login", "API"], concurrency=2)
    source = EventLogFileSource(cfg, client, sm_fields=[], poll_once=True)  # type: ignore[arg-type]

    last_per_key: dict[str, str] = {}
    entries: list[LogEntry] = []
    async for entry in source.events(store, asyncio.Event()):
        entries.append(entry)
        last_per_key[entry.checkpoint.key] = entry.checkpoint.value
    for key, value in last_per_key.items():
        await store.commit(key, value)

    # Genuine overlap actually happened (not a coincidence of fast fakes).
    assert probe.max_seen == 2

    assert len(entries) == 4
    assert {e.checkpoint.key for e in entries} == {"eventlogfile:Login", "eventlogfile:API"}

    login_raw = await store.load("eventlogfile:Login")
    assert login_raw is not None
    login_ckpt = json.loads(login_raw)
    assert login_ckpt["last_created"] == login_file.created_date
    assert [i for i, _ in login_ckpt["ids"]] == ["login-f1"]

    api_raw = await store.load("eventlogfile:API")
    assert api_raw is not None
    api_ckpt = json.loads(api_raw)
    assert api_ckpt["last_created"] == api_file.created_date
    assert [i for i, _ in api_ckpt["ids"]] == ["api-f1"]


@pytest.mark.asyncio
async def test_throttle_lets_already_running_type_finish_but_skips_unstarted(
    tmp_path: pytest.TempPathFactory,
) -> None:
    """A throttle on one type must not cut off a SIBLING type that's already
    running concurrently — that type finishes under its own unchanged
    internal logic. Only a type that hasn't started yet (still queued behind
    the semaphore) checks the shared throttle event and skips outright."""
    store = FileCheckpointStore(tmp_path / "state.json")  # type: ignore[arg-type]
    login_file = make_file_meta(id="login-f1", created_date=_created_ago(timedelta(hours=1)))
    api_file = make_file_meta(id="api-f1", created_date=_created_ago(timedelta(hours=1)))
    client = FakeEventLogFileClient(
        files=[],
        rows_by_id={
            "login-f1": [{"TIMESTAMP_DERIVED": "20260630010000.000"}],
            "api-f1": [{"TIMESTAMP_DERIVED": "20260630010000.000"}],
        },
        files_by_type={"Login": [login_file], "API": [api_file]},
        throttled={"login-f1"},
        # Keeps both Login's and API's downloads "in flight" together before
        # Login throttles, so API is genuinely already running (not skipped)
        # by the time the shared throttle event gets set.
        download_delay=0.02,
    )
    cfg = make_elf_cfg(event_types=["Login", "API", "Report"], concurrency=2)
    source = EventLogFileSource(cfg, client, sm_fields=[], poll_once=True)  # type: ignore[arg-type]

    entries = [e async for e in source.events(store, asyncio.Event())]

    # Login throttled (no entries); API was already running and got to
    # finish; Report never started (never even listed).
    assert {e.checkpoint.key for e in entries} == {"eventlogfile:API"}
    assert "login-f1" in client.download_calls
    assert "api-f1" in client.download_calls
    assert "Report" not in {c[0] for c in client.list_calls}


@pytest.mark.asyncio
async def test_cycle_gauge_set_after_each_poll_cycle(tmp_path: pytest.TempPathFactory) -> None:
    store = FileCheckpointStore(tmp_path / "state.json")  # type: ignore[arg-type]
    file_meta = make_file_meta(id="f1")
    rows = [{"TIMESTAMP_DERIVED": "20260630010000.000"}]
    client = FakeEventLogFileClient(files=[file_meta], rows_by_id={"f1": rows})
    cfg = make_elf_cfg()
    metrics = Metrics()
    source = EventLogFileSource(cfg, client, sm_fields=[], metrics=metrics, poll_once=True)  # type: ignore[arg-type]

    _ = [e async for e in source.events(store, asyncio.Event())]

    value = metrics.registry.get_sample_value("sf2loki_eventlogfile_cycle_seconds")
    assert value is not None
    assert value >= 0.0


@pytest.mark.asyncio
async def test_stop_responsive_with_multiple_concurrent_types(
    tmp_path: pytest.TempPathFactory,
) -> None:
    """Setting `stop` mid-cycle, while several types are running concurrently,
    must still make the generator terminate promptly instead of hanging or
    running every type to completion regardless."""
    store = FileCheckpointStore(tmp_path / "state.json")  # type: ignore[arg-type]
    files_by_type = {
        f"Type{i}": [make_file_meta(id=f"f{i}", created_date=_created_ago(timedelta(hours=1)))]
        for i in range(4)
    }
    rows_by_id = {
        f"f{i}": [
            {"TIMESTAMP_DERIVED": "20260630010000.000"},
            {"TIMESTAMP_DERIVED": "20260630010001.000"},
        ]
        for i in range(4)
    }
    client = FakeEventLogFileClient(
        files=[],
        rows_by_id=rows_by_id,
        files_by_type=files_by_type,
        download_delay=0.02,
    )
    cfg = make_elf_cfg(event_types=list(files_by_type), concurrency=4)
    source = EventLogFileSource(cfg, client, sm_fields=[], poll_once=True)  # type: ignore[arg-type]
    stop = asyncio.Event()

    async def consume() -> list[LogEntry]:
        collected: list[LogEntry] = []
        async for entry in source.events(store, stop):
            collected.append(entry)
            stop.set()  # stop as soon as ANYTHING is emitted
        return collected

    entries = await asyncio.wait_for(consume(), timeout=2.0)

    # Prompt termination is the point of this test; some in-flight files may
    # still have completed their current row before noticing `stop`, so we
    # only assert it didn't hang and didn't yield everything from every type.
    assert len(entries) < 4 * 2


# ---------------------------------------------------------------------------
# Per-org clock-skew isolation (issue #68): each EventLogFileClient must track
# its OWN clock skew even when multiple clients (one per org, in a multi-org
# deployment) share a single httpx.AsyncClient — a hook registered on the
# shared client would let one org's response overwrite another's skew.

from datetime import UTC as _UTC  # noqa: E402
from email.utils import format_datetime as _format_datetime  # noqa: E402

import httpx as _httpx  # noqa: E402
import respx as _respx  # noqa: E402

from sf2loki.auth.jwt_auth import AccessToken as _AccessToken  # noqa: E402
from sf2loki.config import SalesforceConfig as _SalesforceConfig  # noqa: E402
from sf2loki.salesforce.eventlogfile_client import (  # noqa: E402
    EventLogFileClient as _EventLogFileClient,
)


class _FakeTokenProvider:
    """Minimal token provider, mirroring tests/salesforce/test_eventlogfile_client.py."""

    def __init__(self, instance_url: str) -> None:
        self._instance_url = instance_url

    async def token(self) -> _AccessToken:
        return _AccessToken(
            value="tok",
            instance_url=self._instance_url,
            expires_at=datetime.now(_UTC) + timedelta(hours=1),
        )

    async def org_id(self) -> str:
        return "00Dxx"

    def invalidate(self) -> None:
        pass


def _make_sf_cfg() -> _SalesforceConfig:
    return _SalesforceConfig(
        client_id="cid", username="svc@example.com", private_key="DUMMYKEY", api_version="60.0"
    )


@pytest.mark.asyncio
@_respx.mock
async def test_per_org_clients_on_shared_httpx_client_track_own_clock_skew() -> None:
    """Two EventLogFileClients (simulating two orgs) sharing ONE httpx.AsyncClient
    must each capture their own Date-header skew from their OWN LogFile
    download responses — never via a hook on the shared client, which would
    let whichever org's response landed last clobber the other's skew."""
    org_a_meta = EventLogFileMeta(
        id="0ATa",
        event_type="Login",
        interval="Hourly",
        log_date="2026-06-30T00:00:00.000+0000",
        created_date="2026-06-30T01:00:00.000+0000",
        sequence=1,
        length=10,
    )
    org_b_meta = EventLogFileMeta(
        id="0ATb",
        event_type="Login",
        interval="Hourly",
        log_date="2026-06-30T00:00:00.000+0000",
        created_date="2026-06-30T01:00:00.000+0000",
        sequence=1,
        length=10,
    )

    now = datetime.now(_UTC)
    skew_a = timedelta(minutes=10)
    skew_b = timedelta(minutes=-20)

    def _logfile_url(instance: str, file_id: str) -> str:
        return f"{instance}/services/data/v60.0/sobjects/EventLogFile/{file_id}/LogFile"

    _respx.get(_logfile_url("https://org-a.my.salesforce.com", "0ATa")).mock(
        return_value=_httpx.Response(
            200,
            text="A,B\r\n1,2\r\n",
            headers={"Date": _format_datetime(now + skew_a, usegmt=True)},
        )
    )
    _respx.get(_logfile_url("https://org-b.my.salesforce.com", "0ATb")).mock(
        return_value=_httpx.Response(
            200,
            text="A,B\r\n3,4\r\n",
            headers={"Date": _format_datetime(now + skew_b, usegmt=True)},
        )
    )

    async with _httpx.AsyncClient() as shared_client:
        client_a = _EventLogFileClient(
            _make_sf_cfg(),
            _FakeTokenProvider("https://org-a.my.salesforce.com"),
            shared_client,
        )
        client_b = _EventLogFileClient(
            _make_sf_cfg(),
            _FakeTokenProvider("https://org-b.my.salesforce.com"),
            shared_client,
        )

        assert client_a.clock_skew() is None
        assert client_b.clock_skew() is None

        rows_a = [r async for r in client_a.download(org_a_meta)]
        rows_b = [r async for r in client_b.download(org_b_meta)]

    assert rows_a == [{"A": "1", "B": "2"}]
    assert rows_b == [{"A": "3", "B": "4"}]

    skew_a_seen = client_a.clock_skew()
    skew_b_seen = client_b.clock_skew()
    assert skew_a_seen is not None
    assert skew_b_seen is not None
    # Each client's own skew, not stomped on by the other's response.
    assert abs(skew_a_seen - skew_a) < timedelta(seconds=5)
    assert abs(skew_b_seen - skew_b) < timedelta(seconds=5)
