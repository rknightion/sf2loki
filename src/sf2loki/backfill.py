"""`sf2loki backfill`: one-shot historical EventLogFile backfill into Loki.

Separate from the daemon: its own state file (the daemon holds an exclusive
flock on the main one) with a ``backfill:`` checkpoint namespace, so it is
resumable and safe to run while the service is up. Solves Loki's out-of-order
window explicitly — either distinct ``backfill="true"`` streams (default) or
ingest-time timestamps with the true event time in structured metadata.

Checkpoint format (JSON, per ``backfill:{interval}:{event_type}`` key):
``{"last_created": "<iso CreatedDate>", "done_ids": ["<file id>", ...]}``.
Unlike the daemon's carried-ids window (which must survive an unbounded
polling loop), a backfill run processes a bounded window once, so ``done_ids``
is simply every file id already pushed AT ``last_created`` — files with a
CreatedDate strictly newer than the watermark are always new; a re-listed
file at exactly the watermark boundary is skipped only if its id is in
``done_ids``. A file that yields zero rows to push (empty CSV, or every row
filtered out) never advances the checkpoint — mirrors the daemon's own
"nothing to lose, safe to re-list" behavior (see eventlogfile_source.py).

Ordering: EventLogFileClient.list_files already orders ``(CreatedDate, Id)``
ascending; this module defensively re-sorts client-side after the
``until``/resume filters (which can reorder nothing but keeps the invariant
explicit). Concurrent file DOWNLOADS are bounded by a semaphore, but the
downloaded rows are always shaped and pushed to Loki in strict file order —
concurrency only overlaps network I/O, never emission order. Because a
backfill window can exceed the SOQL ``LIMIT`` used for one listing call, each
event type is listed repeatedly (checkpointing between pages) until a short
page (or the ``until`` cutoff) signals the window is exhausted.
"""

from __future__ import annotations

import asyncio
import json
import sys
import time
from collections.abc import Callable, Mapping, Sequence
from dataclasses import dataclass, field
from datetime import UTC, date, datetime, timedelta
from typing import Any

import httpx

from sf2loki.auth.jwt_auth import TokenProvider
from sf2loki.config import EVENT_TYPE_WILDCARD, Config, EventLogFileTypeConfig
from sf2loki.model import Batch, CheckpointToken, LogEntry
from sf2loki.obs.logging import get_logger
from sf2loki.obs.metrics import Metrics
from sf2loki.salesforce.eventlogfile_client import (
    EventLogFileClient,
    EventLogFileError,
    EventLogFileMeta,
)
from sf2loki.shaping import extract_timestamp_checked, promote_labels, route_fields
from sf2loki.sinks.base import PermanentSinkError, RetryableSinkError
from sf2loki.sinks.loki.sink import LokiSink
from sf2loki.state.file_store import FileCheckpointStore
from sf2loki.transforms import compile_rules

log = get_logger(__name__)

_LOG_PREFIX = "sf2loki backfill"

# Same shared-client timeout as the daemon (app.py._HTTP_TIMEOUT) — large ELF
# blobs and slow Loki pushes churn as transport errors under httpx's 5s default.
_HTTP_TIMEOUT = httpx.Timeout(connect=10.0, read=30.0, write=30.0, pool=30.0)

# Our own retry loop on top of LokiSink's internal (tenacity) retries — this is
# a foreground CLI, not a daemon, so a persistently-failing sink aborts the
# whole run loudly instead of retrying forever.
_RETRY_BACKOFF_BASE = 1.0
_RETRY_BACKOFF_MAX = 30.0
_MAX_CONSECUTIVE_PUSH_FAILURES = 10

# Guard thresholds (module docstring / issue #23).
_RETENTION_WARN_AGE = timedelta(days=30)
_LOKI_OOO_WARN_AGE = timedelta(days=7)

# Reasons LokiSink's internal 400/413 split-and-drop path (sinks/loki/sink.py)
# tags dropped entries with; summed at the end of the run for the "rows
# dropped" summary stat (see _dropped_from_metrics).
_DROP_REASONS: tuple[str, ...] = ("bad_request", "oversized_413")


def parse_backfill_date(value: str) -> datetime:
    """Parse a YYYY-MM-DD CLI argument into an aware UTC midnight datetime."""
    try:
        d = date.fromisoformat(value)
    except ValueError:
        raise ValueError(f"invalid date {value!r} (expected YYYY-MM-DD)") from None
    return datetime(d.year, d.month, d.day, tzinfo=UTC)


# A row filter takes a downloaded CSV row and returns the (possibly mutated)
# row to ship, or None to skip it entirely. run_backfill wires this to the
# same transforms engine the daemon's EventLogFile source uses.
RowFilter = Callable[[dict[str, str]], dict[str, Any] | None]


def _apply_row_filters(row: dict[str, str]) -> dict[str, Any] | None:
    """Default row filter: identity (used when no transforms are configured)."""
    return row


def _parse_created(created_date: str) -> datetime | None:
    """Parse a Salesforce CreatedDate literal to an aware datetime, or None.

    Mirrors ``eventlogfile_source._parse_created`` (kept local rather than
    imported since that module is owned by another lane): handles both the
    SOQL ``+0000`` offset form and an ISO ``Z`` form (echoed back from a
    checkpoint).
    """
    for fmt in ("%Y-%m-%dT%H:%M:%S.%f%z", "%Y-%m-%dT%H:%M:%S%z"):
        try:
            return datetime.strptime(created_date, fmt)
        except ValueError:
            continue
    try:
        return datetime.fromisoformat(created_date.replace("Z", "+00:00"))
    except ValueError:
        return None


# Sentinel for an unparseable CreatedDate when sorting/filtering — sorts first
# (oldest) rather than crashing, and never accidentally satisfies `< until`
# comparisons against a real cutoff in a way that hides the file (it does
# satisfy `< until` for any real `until`, but an unparseable CreatedDate is a
# Salesforce data anomaly outside this module's ability to reason about).
_UNPARSEABLE_SENTINEL = datetime.min.replace(tzinfo=UTC)


def _since_literal(dt: datetime) -> str:
    """Format an aware datetime as the ``since`` literal EventLogFileClient expects."""
    return dt.astimezone(UTC).strftime("%Y-%m-%dT%H:%M:%SZ")


class _IngestClock:
    """Monotonically increasing timestamp source for ``--ingest-timestamps`` mode.

    Ticks by one microsecond per call so pushed timestamps never regress
    regardless of concurrent download completion order (rows are always
    shaped and ticked from the single-threaded main flow, in file/row order).
    """

    def __init__(self, start: datetime) -> None:
        self._next = start

    def next(self) -> datetime:
        ts = self._next
        self._next = self._next + timedelta(microseconds=1)
        return ts


@dataclass
class _RunStats:
    """Accumulated counters for the end-of-run summary."""

    files_processed: int = 0
    rows_attempted: int = 0
    bytes_attempted: int = 0
    api_calls: int = 0


@dataclass
class _TypeCursor:
    """In-memory view of a per-event-type checkpoint, advanced as files succeed."""

    last_created: str | None
    done_ids: list[str] = field(default_factory=list)

    def advance(self, file_meta: EventLogFileMeta) -> None:
        if self.last_created == file_meta.created_date:
            if file_meta.id not in self.done_ids:
                self.done_ids.append(file_meta.id)
        else:
            self.last_created = file_meta.created_date
            self.done_ids = [file_meta.id]

    def serialize(self) -> str:
        return json.dumps(
            {"last_created": self.last_created, "done_ids": self.done_ids}, sort_keys=True
        )


def _dropped_from_metrics(metrics: Metrics) -> int:
    """Sum LokiSink's per-entry drop counter across the reasons it can tag.

    LokiSink drops entries internally (400/413 split-and-drop, see
    sinks/loki/sink.py) without always propagating an exception to the
    caller, so the accurate total lives in the shared metrics counter rather
    than anything this module can tally from return values alone.
    """
    total = 0.0
    for reason in _DROP_REASONS:
        value = metrics.registry.get_sample_value(
            "sf2loki_loki_entries_dropped_total", {"reason": reason}
        )
        if value is not None:
            total += value
    return int(total)


def _warn_retention(since: datetime, ingest_timestamps: bool) -> None:
    """Print + log WARNs when the requested window risks ELF retention or Loki OOO rejects."""
    age = datetime.now(UTC) - since
    if age > _RETENTION_WARN_AGE:
        msg = (
            f"{_LOG_PREFIX}: --since is {age.days}d in the past, beyond ELF retention "
            "with the add-on; free tier keeps 1 day"
        )
        log.warning(msg)
        print(msg, file=sys.stderr)
    if not ingest_timestamps and age > _LOKI_OOO_WARN_AGE:
        msg = (
            f"{_LOG_PREFIX}: --since is {age.days}d in the past; Loki's "
            "reject_old_samples_max_age default is 168h (7d) — raise it or use "
            "--ingest-timestamps"
        )
        log.warning(msg)
        print(msg, file=sys.stderr)


async def _resolve_event_types(
    cfg: Config,
    client: EventLogFileClient,
    event_types: list[str] | None,
    interval: str,
) -> list[str]:
    """Resolve which ELF EventTypes to backfill (see module docstring §1)."""
    if event_types:
        return list(event_types)
    configured = [
        t.name for t in cfg.sources.eventlogfile.event_types if t.name != EVENT_TYPE_WILDCARD
    ]
    if configured:
        return configured
    try:
        return await client.list_event_types(interval)
    except EventLogFileError as exc:
        msg = f"{_LOG_PREFIX}: EventType discovery failed: {exc}"
        log.error(msg)
        print(msg, file=sys.stderr)
        return []


def _resolve_type_overrides(cfg: Config, event_type: str) -> tuple[Sequence[str], Sequence[str]]:
    """Per-type structured-metadata/label overrides, mirroring EventLogFileSource."""
    type_cfg: EventLogFileTypeConfig | None = next(
        (t for t in cfg.sources.eventlogfile.event_types if t.name == event_type), None
    )
    sm_fields = (
        type_cfg.structured_metadata_fields
        if type_cfg is not None and type_cfg.structured_metadata_fields is not None
        else cfg.sink.loki.structured_metadata_fields
    )
    label_fields = type_cfg.labels if type_cfg is not None else []
    return sm_fields, label_fields


def _shape_file_rows(
    rows: list[dict[str, str]],
    *,
    event_type: str,
    key: str,
    file_meta: EventLogFileMeta,
    timestamp_column: str,
    sm_fields: Sequence[str],
    label_fields: Sequence[str],
    static_labels: Mapping[str, str],
    ingest_timestamps: bool,
    ingest_clock: _IngestClock,
    row_filter: RowFilter = _apply_row_filters,
) -> list[LogEntry]:
    """Filter + shape one file's rows into LogEntry objects, ready to batch/push.

    Default (label) strategy: true event timestamps, ``backfill="true"``
    label. ``--ingest-timestamps``: monotonically ticking ingest-time
    timestamps, true event time preserved in structured metadata
    ``event_time``, no backfill label. Entries are sorted by timestamp in
    label mode only — rows within one ELF CSV are "approximately"
    time-ordered, so this guarantees per-stream ordering for that file's
    push(es); ingest-timestamps mode is already non-decreasing by
    construction (the clock only ticks forward).
    """
    created = _parse_created(file_meta.created_date)
    entries: list[LogEntry] = []
    for raw_row in rows:
        row = row_filter(dict(raw_row))
        if row is None:
            continue
        line, sm = route_fields(row, sm_fields)
        labels: dict[str, str] = {
            **promote_labels(row, label_fields),
            **static_labels,
            "source": "eventlogfile",
            "event_type": event_type,
        }
        if ingest_timestamps:
            event_ts, _ = extract_timestamp_checked(
                row, field_names=(timestamp_column, "TIMESTAMP"), fallback=created
            )
            sm = {**sm, "event_time": event_ts.isoformat()}
            ts = ingest_clock.next()
        else:
            labels["backfill"] = "true"
            ts, _ = extract_timestamp_checked(
                row, field_names=(timestamp_column, "TIMESTAMP"), fallback=created
            )
        entries.append(
            LogEntry(
                timestamp=ts,
                labels=labels,
                line=line,
                structured_metadata=sm,
                checkpoint=CheckpointToken(key=key, value=""),
            )
        )
    if not ingest_timestamps:
        entries.sort(key=lambda e: e.timestamp)
    return entries


def _chunk_entries(
    entries: list[LogEntry], *, max_entries: int, max_bytes: int
) -> list[list[LogEntry]]:
    """Greedily split *entries* into pushable chunks respecting the batch config."""
    chunks: list[list[LogEntry]] = []
    current: list[LogEntry] = []
    current_bytes = 0
    for entry in entries:
        entry_bytes = len(entry.line.encode("utf-8"))
        over_count = max_entries > 0 and len(current) >= max_entries
        over_bytes = max_bytes > 0 and current and current_bytes + entry_bytes > max_bytes
        if current and (over_count or over_bytes):
            chunks.append(current)
            current, current_bytes = [], 0
        current.append(entry)
        current_bytes += entry_bytes
    if current:
        chunks.append(current)
    return chunks


async def _push_with_retry(sink: LokiSink, batch: Batch, metrics: Metrics) -> bool:
    """Push *batch*, retrying RetryableSinkError with capped backoff.

    Returns True once the batch is disposed of — either pushed, or
    permanently rejected (dropped + counted, see PermanentSinkError). Returns
    False when the run must abort: :data:`_MAX_CONSECUTIVE_PUSH_FAILURES`
    straight RetryableSinkErrors, each already having exhausted LokiSink's own
    internal retry budget.
    """
    backoff = _RETRY_BACKOFF_BASE
    failures = 0
    while True:
        try:
            await sink.push(batch)
        except RetryableSinkError as exc:
            failures += 1
            log.warning("backfill: loki push failed, retrying", attempt=failures, error=str(exc))
            if failures >= _MAX_CONSECUTIVE_PUSH_FAILURES:
                msg = (
                    f"{_LOG_PREFIX}: aborting after {failures} consecutive Loki push "
                    f"failures: {exc}"
                )
                log.error(msg)
                print(msg, file=sys.stderr)
                return False
            await asyncio.sleep(min(backoff, _RETRY_BACKOFF_MAX))
            backoff = min(backoff * 2, _RETRY_BACKOFF_MAX)
            continue
        except PermanentSinkError as exc:
            metrics.loki_entries_dropped.labels(reason=exc.reason).inc(len(batch.entries))
            log.error(
                "backfill: dropping undeliverable batch",
                entries=len(batch.entries),
                reason=exc.reason,
                error=str(exc),
            )
            return True
        else:
            return True


async def _download_file(
    client: EventLogFileClient, file_meta: EventLogFileMeta, stats: _RunStats
) -> tuple[EventLogFileMeta, list[dict[str, str]], EventLogFileError | None]:
    """Download one file's rows. Each call is exactly one metered blob GET."""
    stats.api_calls += 1
    try:
        rows = [row async for row in client.download(file_meta)]
    except EventLogFileError as exc:
        return file_meta, [], exc
    return file_meta, rows, None


async def _process_file(
    *,
    file_meta: EventLogFileMeta,
    rows: list[dict[str, str]],
    event_type: str,
    key: str,
    cfg: Config,
    sink: LokiSink,
    store: FileCheckpointStore,
    metrics: Metrics,
    sm_fields: Sequence[str],
    label_fields: Sequence[str],
    static_labels: Mapping[str, str],
    ingest_timestamps: bool,
    ingest_clock: _IngestClock,
    stats: _RunStats,
    cursor: _TypeCursor,
    row_filter: RowFilter = _apply_row_filters,
) -> bool:
    """Shape, batch, and push one file's rows; commit its checkpoint on success.

    Returns False only when a push had to abort the whole run (retries
    exhausted) — everything else (including permanently-dropped rows) is
    still a "success" from this file's point of view: it was disposed of and
    the checkpoint can safely advance past it.
    """
    stats.files_processed += 1
    entries = _shape_file_rows(
        rows,
        event_type=event_type,
        key=key,
        file_meta=file_meta,
        timestamp_column=cfg.sources.eventlogfile.timestamp_column,
        sm_fields=sm_fields,
        label_fields=label_fields,
        static_labels=static_labels,
        ingest_timestamps=ingest_timestamps,
        ingest_clock=ingest_clock,
        row_filter=row_filter,
    )
    if not entries:
        # Nothing to push (empty CSV, or every row filtered): the checkpoint is
        # intentionally NOT advanced — harmless to re-list/re-download later,
        # same convention as the daemon (eventlogfile_source.py docstring).
        return True

    batch_cfg = cfg.sink.loki.batch
    for chunk in _chunk_entries(
        entries, max_entries=batch_cfg.max_entries, max_bytes=batch_cfg.max_bytes
    ):
        stats.rows_attempted += len(chunk)
        stats.bytes_attempted += sum(len(e.line.encode("utf-8")) for e in chunk)
        if not await _push_with_retry(sink, Batch(entries=chunk), metrics):
            return False

    cursor.advance(file_meta)
    await store.commit(key, cursor.serialize())
    return True


# Outcomes for one event type's listing page (see _process_event_type).
_CONTINUE = "continue"  # keep listing further pages for this type
_STOP_TYPE = "stop_type"  # non-fatal: give up on this type, move to the next
_ABORT_RUN = "abort_run"  # fatal: push retries exhausted, exit the whole run


async def _process_files(
    files: list[EventLogFileMeta],
    *,
    event_type: str,
    key: str,
    cfg: Config,
    client: EventLogFileClient,
    sink: LokiSink,
    store: FileCheckpointStore,
    metrics: Metrics,
    sm_fields: Sequence[str],
    label_fields: Sequence[str],
    static_labels: Mapping[str, str],
    ingest_timestamps: bool,
    ingest_clock: _IngestClock,
    concurrency: int,
    stats: _RunStats,
    cursor: _TypeCursor,
    row_filter: RowFilter = _apply_row_filters,
) -> str:
    """Download (bounded concurrency) and push *files* in order; return an outcome."""
    sem = asyncio.Semaphore(max(1, concurrency))

    async def _bounded(
        file_meta: EventLogFileMeta,
    ) -> tuple[EventLogFileMeta, list[dict[str, str]], EventLogFileError | None]:
        async with sem:
            return await _download_file(client, file_meta, stats)

    tasks = [asyncio.ensure_future(_bounded(fm)) for fm in files]
    try:
        for task in tasks:
            file_meta, rows, error = await task
            if error is not None:
                msg = f"{_LOG_PREFIX}: download of {file_meta.id} ({event_type}) failed: {error}"
                log.error(msg)
                print(msg, file=sys.stderr)
                return _STOP_TYPE
            ok = await _process_file(
                file_meta=file_meta,
                rows=rows,
                event_type=event_type,
                key=key,
                cfg=cfg,
                sink=sink,
                store=store,
                metrics=metrics,
                sm_fields=sm_fields,
                label_fields=label_fields,
                static_labels=static_labels,
                ingest_timestamps=ingest_timestamps,
                ingest_clock=ingest_clock,
                stats=stats,
                cursor=cursor,
                row_filter=row_filter,
            )
            if not ok:
                return _ABORT_RUN
        return _CONTINUE
    finally:
        for task in tasks:
            if not task.done():
                task.cancel()


async def _process_event_type(
    *,
    event_type: str,
    cfg: Config,
    client: EventLogFileClient,
    sink: LokiSink,
    store: FileCheckpointStore,
    metrics: Metrics,
    since: datetime,
    until: datetime | None,
    interval: str,
    ingest_timestamps: bool,
    concurrency: int,
    static_labels: Mapping[str, str],
    ingest_clock: _IngestClock,
    stats: _RunStats,
    row_filter: RowFilter = _apply_row_filters,
) -> bool:
    """Backfill one EventType end-to-end (paging through listings until exhausted).

    Returns False when the run must abort entirely (push retries exhausted);
    True otherwise, including the non-fatal "gave up on this type" path (a
    listing or download failure just stops this type — see module docstring).
    """
    key = f"backfill:{interval}:{event_type}"
    raw = await store.load(key)
    if raw is not None:
        parsed: dict[str, object] = json.loads(raw)
        raw_ids = parsed.get("done_ids", [])
        cursor = _TypeCursor(
            last_created=(str(parsed["last_created"]) if parsed.get("last_created") else None),
            done_ids=[str(i) for i in raw_ids] if isinstance(raw_ids, list) else [],
        )
    else:
        cursor = _TypeCursor(last_created=None, done_ids=[])

    sm_fields, label_fields = _resolve_type_overrides(cfg, event_type)
    page_size = cfg.sources.eventlogfile.page_size

    # Decoupled from `cursor.last_created` (the PERSISTED watermark, which only
    # advances when a file is actually pushed): this is purely the listing
    # cursor's within-run progress, so a listing page that turns out to be
    # entirely already-done (ids at the exact `cursor.last_created` boundary)
    # still lets the next page move forward past it instead of re-fetching the
    # same page forever.
    next_since_literal = (
        cursor.last_created if cursor.last_created is not None else _since_literal(since)
    )

    while True:
        try:
            raw_files = await client.list_files(event_type, interval, next_since_literal, page_size)
        except EventLogFileError as exc:
            msg = f"{_LOG_PREFIX}: listing {event_type} failed: {exc}"
            log.error(msg)
            print(msg, file=sys.stderr)
            return True

        if not raw_files:
            break

        raw_files.sort(
            key=lambda f: (_parse_created(f.created_date) or _UNPARSEABLE_SENTINEL, f.id)
        )
        page_count = len(raw_files)

        oldest_created = _parse_created(raw_files[0].created_date) or _UNPARSEABLE_SENTINEL
        if until is not None and oldest_created >= until:
            # Ascending order: every file from here on (this page and beyond)
            # is at/past the cutoff too — nothing left to do for this type.
            break

        files = raw_files
        if until is not None:
            files = [
                f
                for f in files
                if (_parse_created(f.created_date) or _UNPARSEABLE_SENTINEL) < until
            ]

        if cursor.last_created is not None:
            done_at_boundary = set(cursor.done_ids)
            files = [
                f
                for f in files
                if not (f.created_date == cursor.last_created and f.id in done_at_boundary)
            ]

        new_since_literal = raw_files[-1].created_date

        if not files:
            if page_count < page_size:
                break
            if new_since_literal == next_since_literal:
                # Pathological: more files share this exact CreatedDate boundary
                # than fit in one page, and none of them advanced the listing
                # cursor — paging further would just re-fetch the same page
                # forever. Give up on this type rather than looping forever;
                # a larger eventlogfile.page_size resolves it.
                msg = (
                    f"{_LOG_PREFIX}: {event_type}: more than {page_size} files share "
                    "one CreatedDate boundary; increase eventlogfile.page_size to "
                    "fully backfill this window"
                )
                log.warning(msg)
                print(msg, file=sys.stderr)
                break
            next_since_literal = new_since_literal
            continue

        outcome = await _process_files(
            files,
            event_type=event_type,
            key=key,
            cfg=cfg,
            client=client,
            sink=sink,
            store=store,
            metrics=metrics,
            sm_fields=sm_fields,
            label_fields=label_fields,
            static_labels=static_labels,
            ingest_timestamps=ingest_timestamps,
            ingest_clock=ingest_clock,
            concurrency=concurrency,
            stats=stats,
            cursor=cursor,
            row_filter=row_filter,
        )
        if outcome == _ABORT_RUN:
            return False
        if outcome == _STOP_TYPE:
            return True

        if page_count < page_size:
            break
        next_since_literal = new_since_literal

    return True


def _print_summary(stats: _RunStats, metrics: Metrics, elapsed: float) -> None:
    dropped = _dropped_from_metrics(metrics)
    pushed = max(stats.rows_attempted - dropped, 0)
    print(
        f"{_LOG_PREFIX} summary: files={stats.files_processed} rows_pushed={pushed} "
        f"rows_dropped={dropped} bytes_pushed={stats.bytes_attempted} "
        f"api_calls={stats.api_calls} elapsed={elapsed:.1f}s"
    )


async def run_backfill(
    cfg: Config,
    *,
    since: datetime,
    until: datetime | None,
    event_types: list[str] | None,
    interval: str,
    ingest_timestamps: bool,
    concurrency: int,
) -> int:
    """Backfill ELF history for [since, until) into Loki; return an exit code."""
    _warn_retention(since, ingest_timestamps)

    sf_http = httpx.AsyncClient(timeout=_HTTP_TIMEOUT)
    loki_http = httpx.AsyncClient(timeout=_HTTP_TIMEOUT)
    metrics = Metrics()
    tokens = TokenProvider(cfg.salesforce, sf_http, metrics=metrics)
    client = EventLogFileClient(cfg.salesforce, tokens, sf_http, metrics=metrics)
    sink = LokiSink(cfg.sink.loki, loki_http, metrics=metrics)

    state_path = cfg.state.file.path
    backfill_state_path = state_path.with_name(f"{state_path.stem}-backfill{state_path.suffix}")
    store = FileCheckpointStore(backfill_state_path)

    stats = _RunStats()
    ingest_clock = _IngestClock(datetime.now(UTC))
    static_labels = dict(cfg.sink.loki.labels)
    # Same redaction/filter rules the daemon's EventLogFile source applies —
    # backfilled history must not leak fields the live path redacts.
    salt = cfg.sources.transform_salt.get_secret_value() if cfg.sources.transform_salt else ""
    pipeline = compile_rules(
        cfg.sources.eventlogfile.transforms, salt=salt, source="eventlogfile", metrics=metrics
    )
    row_filter: RowFilter = pipeline.apply if pipeline else _apply_row_filters
    t0 = time.monotonic()
    exit_code = 0
    try:
        resolved_types = await _resolve_event_types(cfg, client, event_types, interval)
        for event_type in resolved_types:
            ok = await _process_event_type(
                event_type=event_type,
                cfg=cfg,
                client=client,
                sink=sink,
                store=store,
                metrics=metrics,
                since=since,
                until=until,
                interval=interval,
                ingest_timestamps=ingest_timestamps,
                concurrency=concurrency,
                static_labels=static_labels,
                ingest_clock=ingest_clock,
                stats=stats,
                row_filter=row_filter,
            )
            if not ok:
                exit_code = 1
                break
    finally:
        store.close()
        await sink.aclose()
        await sf_http.aclose()
        await loki_http.aclose()

    _print_summary(stats, metrics, time.monotonic() - t0)
    return exit_code
