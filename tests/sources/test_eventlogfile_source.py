"""Tests for EventLogFileSource — ELF listing/download polling source.

Uses a duck-typed fake :class:`EventLogFileClient` (no respx) so the
checkpoint-carrying invariant can be tested deterministically, mirroring the
convention in tests/sources/test_eventlog_objects_source.py.
"""

from __future__ import annotations

import asyncio
import json
from dataclasses import dataclass, field
from datetime import UTC, datetime, timedelta
from typing import Literal

import pytest

from sf2loki.config import EventLogFileConfig
from sf2loki.salesforce.eventlogfile_client import EventLogFileMeta
from sf2loki.sources.eventlogfile_source import EventLogFileSource
from sf2loki.state.file_store import FileCheckpointStore

# ---------------------------------------------------------------------------
# Fakes


@dataclass
class FakeEventLogFileClient:
    """Duck-typed stand-in for EventLogFileClient with deterministic responses."""

    files: list[EventLogFileMeta]
    rows_by_id: dict[str, list[dict[str, str]]]
    list_calls: list[tuple[str, str, str, int]] = field(default_factory=list)
    download_calls: list[str] = field(default_factory=list)

    async def list_files(
        self, event_type: str, interval: str, since: str, page_size: int
    ) -> list[EventLogFileMeta]:
        self.list_calls.append((event_type, interval, since, page_size))
        return list(self.files)

    async def download(self, file_meta: EventLogFileMeta) -> list[dict[str, str]]:
        self.download_calls.append(file_meta.id)
        return self.rows_by_id.get(file_meta.id, [])


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
) -> EventLogFileConfig:
    return EventLogFileConfig(
        enabled=True,
        interval=interval,
        event_types=event_types if event_types is not None else ["Login"],
        poll_interval=poll_interval,
        lookback=lookback,
        timestamp_column=timestamp_column,
        page_size=page_size,
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
    assert post_f1["ids"] == ["f1"]
    assert post_f1["last_created"] == f1.created_date

    # c is the first (non-last) row of f2: it carries the pre-f2 checkpoint,
    # which is exactly the post-f1 checkpoint b just advanced to.
    assert json.loads(row_c.checkpoint.value) == post_f1

    post_f2 = json.loads(row_d.checkpoint.value)
    assert post_f2["ids"] == ["f1", "f2"]
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
    assert last_checkpoint["ids"] == ["f1", "f2"]
