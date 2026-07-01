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
import logging
from collections.abc import AsyncIterator, Sequence
from datetime import UTC, datetime
from typing import Protocol

from sf2loki.config import EVENT_TYPE_WILDCARD, EventLogFileConfig, EventLogFileTypeConfig
from sf2loki.model import CheckpointToken, LogEntry
from sf2loki.obs.metrics import Metrics
from sf2loki.salesforce.eventlogfile_client import EventLogFileError, EventLogFileMeta
from sf2loki.shaping import extract_timestamp, promote_labels, route_fields
from sf2loki.state.base import CheckpointStore

_MAX_CARRIED_IDS = 200

_log = logging.getLogger(__name__)


def _parse_created(created_date: str) -> datetime | None:
    """Parse a Salesforce CreatedDate literal to an aware datetime, or None.

    Handles both the SOQL ``+0000`` offset form and an ISO ``Z`` form (a value
    echoed back from a checkpoint). Returns None on anything unparseable so the
    caller can fall back to "process it" (never skip/abandon on a parse failure).
    """
    for fmt in ("%Y-%m-%dT%H:%M:%S.%f%z", "%Y-%m-%dT%H:%M:%S%z"):
        with contextlib.suppress(ValueError):
            return datetime.strptime(created_date, fmt)
    with contextlib.suppress(ValueError):
        return datetime.fromisoformat(created_date.replace("Z", "+00:00"))
    return None


class _EventLogFileClientLike(Protocol):
    """Structural seam EventLogFileSource depends on (satisfied by EventLogFileClient)."""

    async def list_files(
        self, event_type: str, interval: str, since: str, page_size: int
    ) -> list[EventLogFileMeta]: ...

    async def list_event_types(self, interval: str) -> list[str]: ...

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

            for type_cfg in await self._resolve_event_types():
                if stop.is_set():
                    return

                async for entry in self._process_event_type(type_cfg, state, stop):
                    yield entry

            if self._poll_once:
                return

            with contextlib.suppress(TimeoutError):
                await asyncio.wait_for(
                    stop.wait(),
                    timeout=self._cfg.poll_interval.total_seconds(),
                )

    async def _resolve_event_types(self) -> list[EventLogFileTypeConfig]:
        """The EventTypes to ingest this cycle.

        Without the ``"*"`` wildcard this is just the explicitly-configured types.
        With it, discover every EventType the org currently produces for the
        interval, drop any in ``exclude``, and fold in the explicit per-type
        entries (which always win, so their structured-metadata / label overrides
        apply). Re-run each cycle, so newly-enabled EventTypes are picked up
        without a restart. A discovery failure is non-fatal: fall back to the
        explicit types so a transient error can't stop ingestion.
        """
        explicit = [t for t in self._cfg.event_types if t.name != EVENT_TYPE_WILDCARD]
        if not self._cfg.discover:
            return explicit

        try:
            discovered = await self._client.list_event_types(self._cfg.interval)
        except EventLogFileError as exc:
            _log.warning(
                "eventlogfile: EventType discovery failed; using %d explicit type(s) only: %s",
                len(explicit),
                exc,
            )
            return explicit

        exclude = set(self._cfg.exclude)
        resolved = list(explicit)
        names = {t.name for t in explicit}
        for name in discovered:
            if name in names or name in exclude:
                continue
            names.add(name)
            resolved.append(EventLogFileTypeConfig(name=name))
        return resolved

    async def _process_event_type(
        self,
        type_cfg: EventLogFileTypeConfig,
        state: CheckpointStore,
        stop: asyncio.Event,
    ) -> AsyncIterator[LogEntry]:
        event_type = type_cfg.name
        # Per-type structured-metadata override; None means "use the global set".
        sm_fields = (
            type_cfg.structured_metadata_fields
            if type_cfg.structured_metadata_fields is not None
            else self._sm_fields
        )
        label_fields = type_cfg.labels
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

        now = datetime.now(UTC)
        settle_window = self._cfg.settle_window
        download_max_age = self._cfg.download_max_age

        for file_meta in files:
            if stop.is_set():
                return

            created = _parse_created(file_meta.created_date)

            # Settle gate: an Hourly file created within the settle window may still
            # be half-written; skip it this cycle WITHOUT advancing `current` so it
            # is re-listed (and settled) next cycle. Disabled when settle_window==0.
            if settle_window and created is not None and (now - created) < settle_window:
                _log.debug(
                    "eventlogfile: skipping unsettled file %s (created %s, within %s)",
                    file_meta.id,
                    file_meta.created_date,
                    settle_window,
                )
                continue

            try:
                rows = await self._client.download(file_meta)
            except EventLogFileError as exc:
                # A transient download failure (e.g. body-not-ready 404, or a 5xx)
                # must NOT crash the connector (ko.md §7.4). The client already
                # incremented eventlogfile_download_errors. Skip the file WITHOUT
                # advancing `current`, so it is retried next cycle — UNLESS it is
                # older than download_max_age, in which case treat it as gone and
                # advance past it so it can't wedge the watermark forever.
                age = (now - created) if created is not None else None
                if age is not None and age > download_max_age:
                    _log.warning(
                        "eventlogfile: abandoning file %s after download failure "
                        "(created %s, older than download_max_age %s): %s",
                        file_meta.id,
                        file_meta.created_date,
                        download_max_age,
                        exc,
                    )
                    current_last_created = file_meta.created_date
                    current_ids = [*current_ids, file_meta.id][-_MAX_CARRIED_IDS:]
                else:
                    _log.warning(
                        "eventlogfile: skipping file %s after download failure "
                        "(will retry next cycle): %s",
                        file_meta.id,
                        exc,
                    )
                continue
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
                line, sm = route_fields(row, sm_fields)
                # Promoted labels first, then reserved keys — reserved win so a
                # promoted column can never clobber source identity.
                labels: dict[str, str] = {
                    **promote_labels(row, label_fields),
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
