"""EventLogFile source: lists + downloads Salesforce EventLogFile CSVs.

Ref: DESIGN.md §8.

Each poll cycle, for every configured ``event_type``: list new EventLogFile
records since the last checkpoint, download and parse each file's CSV body,
and yield a :class:`~sf2loki.model.LogEntry` per row.

Checkpoint format (JSON string, per ``eventlogfile:<event_type>`` key):
``{"last_created": "<iso-8601 CreatedDate>", "ids": ["<file id>", ...]}``.
``ids`` is a rolling window (last 200) of recently-processed file ids, used to
dedup files whose ``CreatedDate`` ties with (or falls before) the watermark.

At-least-once checkpoint carrying (CRITICAL invariant): within a single
EventLogFile, every row except the last carries the *pre-file* checkpoint
(the value in effect before this file started); only the file's last row
carries the *advanced* (post-file) checkpoint, which is the first checkpoint
value to include this file's id. Rationale: the pipeline commits the most
recent checkpoint per key once a batch is flushed. If a batch flushes
mid-file, it must commit the PRE-file checkpoint so that a crash re-processes
the entire file from the start (Loki dedups already-sent rows via repeated
identical entries, so reprocessing is safe). Advancing the checkpoint before
the file is fully emitted would risk losing the file's unflushed tail rows on
a crash.

A file with zero rows contributes nothing to the checkpoint: its id is never
folded into the carried "ids" set (there's no "last row" to carry it). Such a
file may be re-listed and re-downloaded on a subsequent cycle — harmless,
since there are no rows to (re-)emit or dedup.
"""

from __future__ import annotations

import asyncio
import contextlib
import json
from collections.abc import AsyncIterator, Sequence
from datetime import UTC, datetime
from typing import Protocol

from sf2loki.config import EventLogFileConfig
from sf2loki.model import CheckpointToken, LogEntry
from sf2loki.obs.metrics import Metrics
from sf2loki.salesforce.eventlogfile_client import EventLogFileMeta
from sf2loki.shaping import extract_timestamp, route_fields
from sf2loki.state.base import CheckpointStore

_MAX_CARRIED_IDS = 200


class _EventLogFileClientLike(Protocol):
    """Structural seam EventLogFileSource depends on (satisfied by EventLogFileClient)."""

    async def list_files(
        self, event_type: str, interval: str, since: str, page_size: int
    ) -> list[EventLogFileMeta]: ...

    async def download(self, file_meta: EventLogFileMeta) -> list[dict[str, str]]: ...


class EventLogFileSource:
    """Polls Salesforce EventLogFile listing + downloads CSVs, yielding LogEntry per row.

    Satisfies the :class:`~sf2loki.sources.base.Source` protocol.
    """

    name = "eventlogfile"

    def __init__(
        self,
        cfg: EventLogFileConfig,
        client: _EventLogFileClientLike,
        *,
        sm_fields: Sequence[str],
        metrics: Metrics | None = None,
        poll_once: bool = False,
    ) -> None:
        self._cfg = cfg
        self._client = client
        self._sm_fields = sm_fields
        self._metrics = metrics if metrics is not None else Metrics()
        self._poll_once = poll_once

    async def events(
        self,
        state: CheckpointStore,
        stop: asyncio.Event,
    ) -> AsyncIterator[LogEntry]:
        while True:
            if stop.is_set():
                return

            for event_type in self._cfg.event_types:
                if stop.is_set():
                    return

                async for entry in self._process_event_type(event_type, state, stop):
                    yield entry

            if self._poll_once:
                return

            with contextlib.suppress(TimeoutError):
                await asyncio.wait_for(
                    stop.wait(),
                    timeout=self._cfg.poll_interval.total_seconds(),
                )

    async def _process_event_type(
        self,
        event_type: str,
        state: CheckpointStore,
        stop: asyncio.Event,
    ) -> AsyncIterator[LogEntry]:
        key = f"eventlogfile:{event_type}"
        default_since = (datetime.now(UTC) - self._cfg.lookback).strftime("%Y-%m-%dT%H:%M:%SZ")

        raw = await state.load(key)
        if raw is None:
            since = default_since
            ids: list[str] = []
        else:
            parsed: dict[str, object] = json.loads(raw)
            since = str(parsed.get("last_created") or default_since)
            raw_ids = parsed.get("ids", [])
            ids = [str(i) for i in raw_ids] if isinstance(raw_ids, list) else []

        # "current" is the carried checkpoint in effect BEFORE the next file is
        # processed; it starts as the pre-cycle checkpoint loaded above.
        current_last_created = since
        current_ids = ids

        files = await self._client.list_files(
            event_type, self._cfg.interval, since, self._cfg.page_size
        )
        seen = set(current_ids)
        files = [f for f in files if f.id not in seen]

        for file_meta in files:
            if stop.is_set():
                return

            rows = await self._client.download(file_meta)
            if not rows:
                # Zero-row files contribute nothing to the checkpoint (see module
                # docstring); skip without advancing `current`.
                continue

            advanced_last_created = file_meta.created_date
            advanced_ids = [*current_ids, file_meta.id][-_MAX_CARRIED_IDS:]
            last_idx = len(rows) - 1

            for i, row in enumerate(rows):
                if stop.is_set():
                    return

                if i == last_idx:
                    carried_last_created, carried_ids = advanced_last_created, advanced_ids
                else:
                    carried_last_created, carried_ids = current_last_created, current_ids

                ts = extract_timestamp(row, field_names=(self._cfg.timestamp_column, "TIMESTAMP"))
                line, sm = route_fields(row, self._sm_fields)
                labels: dict[str, str] = {
                    "source": "eventlogfile",
                    "event_type": event_type,
                }
                checkpoint_value = json.dumps(
                    {"last_created": carried_last_created, "ids": carried_ids},
                    sort_keys=True,
                )

                self._metrics.eventlogfile_rows_ingested.labels(event_type=event_type).inc()

                yield LogEntry(
                    timestamp=ts,
                    labels=labels,
                    line=line,
                    structured_metadata=sm,
                    checkpoint=CheckpointToken(key=key, value=checkpoint_value),
                )

            current_last_created, current_ids = advanced_last_created, advanced_ids
