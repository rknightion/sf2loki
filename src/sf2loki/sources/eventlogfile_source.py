"""EventLogFile source: lists + downloads Salesforce EventLogFile CSVs.

Ref: docs/sources/eventlogfile.md.

Each poll cycle, for every configured ``event_type``: list new EventLogFile
records since the last checkpoint, download and parse each file's CSV body,
and yield a :class:`~sf2loki.model.LogEntry` per row.

Checkpoint format (JSON string, per ``eventlogfile:<event_type>`` key):
``{"last_created": "<iso-8601 CreatedDate>", "ids": [["<file id>", "<CreatedDate>"], ...]}``.
``ids`` is a window of recently-processed files, used to dedup files whose
``CreatedDate`` ties with (or falls before) the watermark: ALL pairs at the
watermark's CreatedDate are kept (the ``>=`` re-list boundary needs every one
of them), plus a capped tail (last 200) of older pairs.
Each entry is an ``[id, created_date]`` pair — Salesforce regenerates DAILY
files **in place** (same Id, CreatedDate bumped, blob replaced with the full
superset including late rows), so a re-listed id with a NEWER CreatedDate must
be re-processed, not skipped (duplicate rows are the accepted at-least-once
cost; byte-identical replays dedupe in Loki). Hourly late events instead create
NEW sibling records (new Id, Sequence++), which plain id-dedup handles.
Backward compatibility: a pre-upgrade checkpoint carries bare id strings; a
legacy id matches ANY CreatedDate (old semantics preserved).

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

Watermark safety (CRITICAL invariant): a transient download failure STOPS the
per-type file loop for the cycle. Files are listed ``CreatedDate >= watermark``
in (CreatedDate, Id) order, so letting a LATER successful file advance the
watermark would put the failed file permanently below the listing window —
silent data loss on a common transient 404/5xx. Because unprocessed files form
an ordered suffix, stopping (without advancing) re-lists the failed file and
everything after it next cycle. The one exception is the ``download_max_age``
abandon path: a file that keeps failing past that age is deliberately skipped
*with* the watermark advanced so it can't wedge ingestion forever.

A file with zero rows contributes nothing to the checkpoint: its id is never
folded into the carried "ids" set (there's no "last row" to carry it). Such a
file may be re-listed and re-downloaded on a subsequent cycle — harmless,
since there are no rows to (re-)emit or dedup.

Concurrency (``cfg.concurrency``, per poll cycle): event types are independent
— each has its own checkpoint key (``eventlogfile:<event_type>``) and its own
carried watermark/ids state — so up to ``concurrency`` types are processed at
once via one task per type, gated by an ``asyncio.Semaphore``. Processing
*within* a type stays strictly sequential (unchanged from the single-type
logic below); only the across-type ordering becomes concurrent. Entries from
concurrently-running types interleave into the yielded stream, which is safe
because the pipeline commits the most recent checkpoint value *per key* — an
interleaved Login row and API row never contend for the same key. A
REQUEST_LIMIT_EXCEEDED throttle sets a shared ``asyncio.Event``: a type that
hasn't started yet (still waiting on the semaphore) skips entirely; a type
already running finishes under its own unchanged internal logic (which may
itself detect the same throttle and stop early). See ``_process_cycle``.
"""

from __future__ import annotations

import asyncio
import contextlib
import json
import logging
import random
import time
from collections.abc import AsyncIterator, Sequence
from datetime import UTC, datetime, timedelta
from typing import Protocol

from sf2loki.config import EVENT_TYPE_WILDCARD, EventLogFileConfig, EventLogFileTypeConfig
from sf2loki.model import CheckpointToken, LogEntry
from sf2loki.obs.metrics import Metrics
from sf2loki.salesforce.eventlogfile_client import (
    EventLogFileError,
    EventLogFileMeta,
    EventLogFileThrottledError,
)
from sf2loki.shaping import extract_timestamp_checked, promote_labels, route_fields, should_keep
from sf2loki.sources.overlap import category_of_elf
from sf2loki.state.base import CheckpointStore
from sf2loki.transforms import compile_rules

# Cap for carried (id, created_date) pairs OLDER than the current watermark.
# Pairs AT the watermark are always all kept — the next listing is
# ``CreatedDate >= watermark``, so exactly those are needed to dedup the
# re-listed boundary (capping them would re-download uncovered files forever
# when >cap files share one CreatedDate, e.g. a bulk backfill).
_MAX_CARRIED_IDS = 200

# The carried window growing past this many pairs means something anomalous is
# stamping thousands of files with one CreatedDate — worth a WARNING.
_CARRIED_IDS_WARN_THRESHOLD = 5000

# Consecutive per-type cycle failures at which the skip log escalates to ERROR.
_ERROR_LOG_THRESHOLD = 3

# Clock skew (Salesforce server time - local now) below this is ignored: the
# Date header has 1s resolution and includes network latency noise.
_SKEW_APPLY_THRESHOLD = timedelta(seconds=30)
# Skew beyond this logs a WARNING (once per process): the local clock is
# meaningfully wrong and CreatedDate comparisons are being adjusted.
_SKEW_WARN_THRESHOLD = timedelta(seconds=60)

# A carried checkpoint entry: (file id, CreatedDate). created is None for
# entries loaded from a legacy (bare-id) checkpoint — matches any CreatedDate.
_CarriedId = tuple[str, str | None]

_log = logging.getLogger(__name__)


class _CycleDone:
    """Sentinel enqueued on the bridge queue once every per-type worker for a
    cycle has finished (successfully, via early return, or via exception)."""


_CYCLE_DONE = _CycleDone()


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


def _parse_carried_ids(raw_ids: object) -> list[_CarriedId]:
    """Decode the checkpoint "ids" window, accepting both formats.

    New format: ``[["<id>", "<created_date>"], ...]``. Legacy format: bare id
    strings — loaded with ``created=None`` so they match any CreatedDate.
    """
    if not isinstance(raw_ids, list):
        return []
    out: list[_CarriedId] = []
    for item in raw_ids:
        if isinstance(item, str):
            out.append((item, None))
        elif isinstance(item, (list, tuple)) and len(item) == 2:
            out.append((str(item[0]), str(item[1])))
    return out


def _serialize_carried_ids(ids: Sequence[_CarriedId]) -> list[object]:
    """Encode the ids window; legacy (created=None) entries round-trip as bare strings."""
    return [fid if created is None else [fid, created] for fid, created in ids]


def _append_carried_id(ids: Sequence[_CarriedId], file_meta: EventLogFileMeta) -> list[_CarriedId]:
    """Fold *file_meta* into the ids window (deduped by id, older pairs capped).

    Dropping an existing pair for the same id matters for re-issued daily files:
    the window must record the NEWEST processed CreatedDate, not accumulate
    stale versions.

    Trimming keeps ALL pairs whose created_date equals the new watermark
    (*file_meta*'s CreatedDate): the next listing re-fetches everything at
    ``CreatedDate >= watermark``, so that boundary set is exactly what the
    ``>=`` re-list dedup needs and is naturally bounded by how many files
    Salesforce stamps with one CreatedDate. Only pairs at OLDER CreatedDates
    (which can never re-list, apart from a regenerated daily file — handled by
    the same-id replacement above) and legacy bare-id entries are capped to a
    tail of ``_MAX_CARRIED_IDS``.
    """
    kept = [(fid, created) for fid, created in ids if fid != file_meta.id]
    combined = [*kept, (file_meta.id, file_meta.created_date)]
    at_watermark = [pair for pair in combined if pair[1] == file_meta.created_date]
    older = [pair for pair in combined if pair[1] != file_meta.created_date]
    result = [*older[-_MAX_CARRIED_IDS:], *at_watermark]
    if len(ids) <= _CARRIED_IDS_WARN_THRESHOLD < len(result):
        _log.warning(
            "eventlogfile: carried checkpoint id window exceeded %d pairs (%d files "
            "share CreatedDate %s) — checkpoint size is growing abnormally",
            _CARRIED_IDS_WARN_THRESHOLD,
            len(at_watermark),
            file_meta.created_date,
        )
    return result


def _is_already_processed(file_meta: EventLogFileMeta, seen: dict[str, str | None]) -> bool:
    """True when *file_meta* was already processed at its CURRENT CreatedDate.

    A legacy entry (created=None) matches any CreatedDate. A re-listed id with a
    strictly NEWER CreatedDate is a regenerated daily file and must be
    re-processed. Unparseable dates fall back to inequality (re-process on any
    change — at-least-once beats data loss).
    """
    if file_meta.id not in seen:
        return False
    seen_created = seen[file_meta.id]
    if seen_created is None:  # legacy checkpoint entry: matches any CreatedDate
        return True
    if file_meta.created_date == seen_created:
        return True
    listed = _parse_created(file_meta.created_date)
    recorded = _parse_created(seen_created)
    if listed is not None and recorded is not None:
        return listed <= recorded
    return False


class _EventLogFileClientLike(Protocol):
    """Structural seam EventLogFileSource depends on (satisfied by EventLogFileClient)."""

    async def list_files(
        self, event_type: str, interval: str, since: str, page_size: int
    ) -> list[EventLogFileMeta]: ...

    async def list_event_types(self, interval: str) -> list[str]: ...

    def download(self, file_meta: EventLogFileMeta) -> AsyncIterator[dict[str, str]]: ...

    def clock_skew(self) -> timedelta | None: ...


class EventLogFileSource:
    """Polls Salesforce EventLogFile listing + downloads CSVs, yielding LogEntry per row.

    Satisfies the :class:`~sf2loki.sources.base.Source` protocol. All Salesforce
    failures (listing, discovery, download — HTTP, SOQL or transport) are
    contained per cycle: the affected work is skipped with a WARNING (ERROR
    after ``_ERROR_LOG_THRESHOLD`` consecutive failures), and the poll loop
    retries next cycle; checkpoints make the retry safe. A 403
    ``REQUEST_LIMIT_EXCEEDED`` additionally aborts the REST of the cycle so an
    exhausted API budget isn't hammered further.
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
        exclude_categories: frozenset[str] = frozenset(),
        transform_salt: str = "",
    ) -> None:
        self._cfg = cfg
        self._client = client
        self._sm_fields = sm_fields
        self._metrics = metrics if metrics is not None else Metrics()
        self._poll_once = poll_once
        # Precompiled redaction/filter pipeline (empty when no transforms).
        self._transforms = compile_rules(
            cfg.transforms, salt=transform_salt, source=self.name, metrics=self._metrics
        )
        # Categories already owned by a higher-priority source (a Pub/Sub stream or
        # a stored-event poll). Discovered wildcard types in these categories are
        # skipped so the same events aren't ingested twice — unless the operator set
        # sources.allow_overlap, in which case app wiring passes an empty set here
        # (keep both the real-time-lean stream and the richer ELF rows).
        self._exclude_categories = exclude_categories
        # Consecutive cycle-level failures per event type (log escalation).
        self._consecutive_failures: dict[str, int] = {}
        # Set when a cycle hits REQUEST_LIMIT_EXCEEDED: abort remaining work
        # until the next poll interval.
        self._cycle_throttled = False
        # Clock skew (server - local) applied to CreatedDate comparisons this
        # cycle; recomputed once per poll cycle from the client's last-seen
        # Salesforce Date header. Zero when unknown or negligible.
        self._cycle_skew = timedelta(0)
        self._skew_warned = False

    async def events(
        self,
        state: CheckpointStore,
        stop: asyncio.Event,
    ) -> AsyncIterator[LogEntry]:
        while True:
            if stop.is_set():
                return

            self._cycle_throttled = False
            type_cfgs = await self._resolve_event_types()
            # Once per cycle (after discovery, whose response may have updated
            # the client's last-seen Date header): how far the local clock is
            # from Salesforce server time.
            self._cycle_skew = self._compute_cycle_skew()

            cycle_start = time.monotonic()
            async for entry in self._process_cycle(type_cfgs, state, stop):
                yield entry
            self._metrics.eventlogfile_cycle_seconds.set(time.monotonic() - cycle_start)

            if self._poll_once:
                return

            timeout = self._cfg.poll_interval.total_seconds()
            if timeout > 0:
                # ±10% jitter desynchronizes multi-instance / multi-source cycles.
                timeout *= random.uniform(0.9, 1.1)  # jitter, not cryptographic
            with contextlib.suppress(TimeoutError):
                await asyncio.wait_for(stop.wait(), timeout=timeout)

    async def _process_cycle(
        self,
        type_cfgs: list[EventLogFileTypeConfig],
        state: CheckpointStore,
        stop: asyncio.Event,
    ) -> AsyncIterator[LogEntry]:
        """Process every configured type this cycle, up to ``cfg.concurrency`` at once.

        Each type gets its own task, gated by ``semaphore`` so at most
        ``concurrency`` run concurrently; processing *within* a type stays
        strictly sequential via the unchanged ``_process_event_type``. Because
        an async generator can't be safely driven by multiple concurrent
        callers, each worker task funnels its entries through a small bounded
        queue (the "bridge") that this method drains and re-yields — the
        bound (``maxsize=1``) means a slow downstream consumer (the caller of
        this generator) stalls the queue, which stalls every worker's next
        ``put``, which — since a worker awaits that ``put`` before listing or
        downloading its next file — propagates backpressure all the way to
        the Salesforce calls, exactly as the fully-sequential version did.

        Throttle abort: when a worker's ``_process_event_type`` run finishes
        having set ``self._cycle_throttled`` (REQUEST_LIMIT_EXCEEDED), this
        sets ``throttle_event``. A type that hasn't started yet (still queued
        behind the semaphore) checks the event and skips outright — mirroring
        the old "break the remaining types" abort. A type already running is
        NOT interrupted: it keeps going under its own unchanged internal
        logic (which independently detects and reacts to the same throttle).
        """
        queue: asyncio.Queue[LogEntry | _CycleDone] = asyncio.Queue(maxsize=1)
        throttle_event = asyncio.Event()
        semaphore = asyncio.Semaphore(self._cfg.concurrency)

        async def worker(type_cfg: EventLogFileTypeConfig) -> None:
            if throttle_event.is_set() or stop.is_set():
                return
            async with semaphore:
                # Re-check after acquiring: the event may have been set while
                # this type was queued behind the semaphore.
                if throttle_event.is_set() or stop.is_set():
                    return
                async for entry in self._process_event_type(type_cfg, state, stop):
                    await queue.put(entry)
                if self._cycle_throttled:
                    throttle_event.set()

        async def run_workers() -> list[BaseException | None]:
            try:
                return await asyncio.gather(
                    *(worker(type_cfg) for type_cfg in type_cfgs),
                    return_exceptions=True,
                )
            finally:
                # Always signal completion, even if gather() itself blew up,
                # so the consumer loop below never hangs waiting on the queue.
                await queue.put(_CYCLE_DONE)

        runner = asyncio.ensure_future(run_workers())
        try:
            while True:
                item = await queue.get()
                if isinstance(item, _CycleDone):
                    break
                yield item
            results = await runner
        except BaseException:
            # The consumer side is unwinding (e.g. GeneratorExit from an
            # early aclose(), or cancellation) — stop the workers too instead
            # of leaving them running unattended in the background.
            runner.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await runner
            raise

        for result in results:
            if isinstance(result, BaseException):
                # An UNEXPECTED failure in one type's worker (the expected SF
                # error paths are already contained inside
                # _process_event_type and never raise). Every other worker
                # has already run to completion by this point — gather()
                # with return_exceptions=True never cancels siblings — so
                # this type's failure genuinely couldn't affect another's.
                raise result

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
            if isinstance(exc, EventLogFileThrottledError):
                self._cycle_throttled = True
            self._metrics.soql_poll_errors.labels(source="eventlogfile", object="discovery").inc()
            _log.warning(
                "eventlogfile: EventType discovery failed; using %d explicit type(s) only: %s",
                len(explicit),
                exc,
            )
            return explicit

        exclude = set(self._cfg.exclude)
        resolved = list(explicit)
        names = {t.name for t in explicit}
        # Discovered (wildcard) types inherit the "*" entry's sample rate, so a
        # single `sample` on the wildcard applies to every discovered type.
        wildcard = next((t for t in self._cfg.event_types if t.name == EVENT_TYPE_WILDCARD), None)
        wildcard_sample = wildcard.sample if wildcard is not None else 1.0
        for name in discovered:
            if name in names or name in exclude:
                continue
            # Skip a discovered type whose category another source already owns
            # (unless allow_overlap emptied this set — then ingest it too).
            if category_of_elf(name) in self._exclude_categories:
                continue
            names.add(name)
            resolved.append(EventLogFileTypeConfig(name=name, sample=wildcard_sample))
        return resolved

    def _compute_cycle_skew(self) -> timedelta:
        """Clock skew (server - local) to apply to CreatedDate comparisons.

        Salesforce stamps CreatedDate with ITS clock; the settle gate,
        first-run ``default_since`` and ``download_max_age`` compare it against
        local now(UTC). A skewed local clock silently shifts those windows, so
        when the client has seen a Salesforce ``Date`` response header and the
        skew is beyond noise (>30s), comparisons are adjusted to server time.
        Returns zero (today's behavior) when the skew is unknown or small.
        """
        skew = self._client.clock_skew()
        if skew is None or abs(skew) <= _SKEW_APPLY_THRESHOLD:
            return timedelta(0)
        if abs(skew) > _SKEW_WARN_THRESHOLD and not self._skew_warned:
            self._skew_warned = True
            _log.warning(
                "eventlogfile: local clock is %s away from Salesforce server time; "
                "adjusting CreatedDate comparisons (settle window, lookback, "
                "download_max_age) by that skew",
                skew,
            )
        return skew

    def _record_cycle_failure(self, event_type: str, what: str, exc: Exception) -> None:
        """Log a contained per-cycle failure, escalating after repeated ones."""
        count = self._consecutive_failures.get(event_type, 0) + 1
        self._consecutive_failures[event_type] = count
        if isinstance(exc, EventLogFileThrottledError):
            self._cycle_throttled = True
            _log.error(
                "eventlogfile[%s]: Salesforce API request limit exceeded during %s; "
                "backing off until the next poll interval: %s",
                event_type,
                what,
                exc,
            )
            return
        level = logging.ERROR if count >= _ERROR_LOG_THRESHOLD else logging.WARNING
        _log.log(
            level,
            "eventlogfile[%s]: %s failed (%d consecutive failure(s); will retry next cycle): %s",
            event_type,
            what,
            count,
            exc,
        )

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
        # "now" in Salesforce server terms (skew is zero when unknown/small):
        # CreatedDate values are stamped by Salesforce's clock, so the settle
        # gate / lookback / download_max_age comparisons below must use it too.
        now = datetime.now(UTC) + self._cycle_skew
        default_since = (now - self._cfg.lookback).strftime("%Y-%m-%dT%H:%M:%SZ")

        raw = await state.load(key)
        if raw is None:
            since = default_since
            ids: list[_CarriedId] = []
        else:
            parsed: dict[str, object] = json.loads(raw)
            since = str(parsed.get("last_created") or default_since)
            ids = _parse_carried_ids(parsed.get("ids", []))

        # "current" is the carried checkpoint in effect BEFORE the next file is
        # processed; it starts as the pre-cycle checkpoint loaded above.
        current_last_created = since
        current_ids = ids
        # The checkpoint value of the most recently YIELDED entry this cycle
        # (None if none yet) — used at the end to detect a cursor that
        # advanced (files processed/abandoned) without any entry actually
        # carrying that final state downstream (issue #64: e.g. every row
        # filtered/sampled out, or the last file abandoned past
        # download_max_age with no rows emitted).
        last_yielded_value: str | None = None

        try:
            files = await self._client.list_files(
                event_type, self._cfg.interval, since, self._cfg.page_size
            )
        except EventLogFileError as exc:
            # Listing failure (SOQL/HTTP/transport) is contained: skip this
            # type for the cycle; the unchanged checkpoint retries it next poll.
            self._metrics.soql_poll_errors.labels(source="eventlogfile", object=event_type).inc()
            self._record_cycle_failure(event_type, "listing", exc)
            return

        # Last-wins per id: a re-processed daily file appends a newer pair, and
        # the freshest CreatedDate is what future skips compare against.
        seen: dict[str, str | None] = dict(current_ids)
        files = [f for f in files if not _is_already_processed(f, seen)]

        settle_window = self._cfg.settle_window
        download_max_age = self._cfg.download_max_age

        for file_meta in files:
            if stop.is_set():
                return

            created = _parse_created(file_meta.created_date)

            # Settle gate: an Hourly file created within the settle window may still
            # be half-written; skip it this cycle WITHOUT advancing `current` so it
            # is re-listed (and settled) next cycle. Disabled when settle_window==0.
            # Files are ordered by CreatedDate, so everything after it is fresher
            # and equally unsettled — no later file can advance past it either.
            if settle_window and created is not None and (now - created) < settle_window:
                _log.debug(
                    "eventlogfile: skipping unsettled file %s (created %s, within %s)",
                    file_meta.id,
                    file_meta.created_date,
                    settle_window,
                )
                continue

            row_iter = aiter(self._client.download(file_meta))
            try:
                try:
                    pending: dict[str, str] = await anext(row_iter)
                except StopAsyncIteration:
                    # Zero-row files contribute nothing to the checkpoint (see
                    # module docstring); skip without advancing `current`.
                    continue
                except EventLogFileError as exc:
                    if isinstance(exc, EventLogFileThrottledError):
                        self._record_cycle_failure(event_type, "download", exc)
                        return
                    # A transient download failure (e.g. body-not-ready 404, or a
                    # 5xx) must NOT crash the connector (ko.md §7.4). The client
                    # already incremented eventlogfile_download_errors.
                    age = (now - created) if created is not None else None
                    if age is not None and age > download_max_age:
                        # Abandon: advance past it so it can't wedge the watermark
                        # forever, then keep going with the later files.
                        _log.warning(
                            "eventlogfile: abandoning file %s after download failure "
                            "(created %s, older than download_max_age %s): %s",
                            file_meta.id,
                            file_meta.created_date,
                            download_max_age,
                            exc,
                        )
                        current_last_created = file_meta.created_date
                        current_ids = _append_carried_id(current_ids, file_meta)
                        continue
                    # Transient: STOP the file loop for this cycle. Processing a
                    # later file would advance the watermark past this one and
                    # lose it forever (see module docstring). The unprocessed
                    # files form a suffix and are re-listed next cycle.
                    self._record_cycle_failure(event_type, f"download of {file_meta.id}", exc)
                    return

                advanced_last_created = file_meta.created_date
                advanced_ids = _append_carried_id(current_ids, file_meta)
                # Hoisted once per file (issue #69): every non-last row of this
                # file shares the identical pre-file checkpoint value, and the
                # last row shares the identical advanced value — rebuilding
                # json.dumps(sort_keys=True) per ROW was pure waste over a
                # window that can hold up to 200 pairs.
                pre_file_value = json.dumps(
                    {
                        "last_created": current_last_created,
                        "ids": _serialize_carried_ids(current_ids),
                    },
                    sort_keys=True,
                )
                advanced_value = json.dumps(
                    {
                        "last_created": advanced_last_created,
                        "ids": _serialize_carried_ids(advanced_ids),
                    },
                    sort_keys=True,
                )

                # One-row lookahead: only when the NEXT row is known to exist is
                # the pending row emitted with the pre-file checkpoint; the final
                # row (lookahead exhausted) carries the advanced checkpoint.
                while True:
                    if stop.is_set():
                        return
                    try:
                        nxt = await anext(row_iter)
                    except StopAsyncIteration:
                        last_entry = self._make_entry(
                            pending,
                            event_type=event_type,
                            key=key,
                            sm_fields=sm_fields,
                            label_fields=label_fields,
                            checkpoint_value=advanced_value,
                            sample=type_cfg.sample,
                            ts_fallback=created,
                        )
                        if last_entry is not None:
                            last_yielded_value = advanced_value
                            yield last_entry
                        break
                    except EventLogFileError as exc:
                        if isinstance(exc, EventLogFileThrottledError):
                            self._record_cycle_failure(
                                event_type, f"download of {file_meta.id} (mid-file)", exc
                            )
                            return
                        # A transient mid-file failure (e.g. csv.Error on an
                        # oversized field, issue #41): rows emitted so far
                        # carried the PRE-file checkpoint, so retrying the
                        # whole file next cycle is safe.
                        age = (now - created) if created is not None else None
                        if age is not None and age > download_max_age:
                            # Abandon exactly like the first-row path: the
                            # unemitted "pending" row (and anything after it)
                            # is dropped, but the file is still marked
                            # processed (advanced_last_created/advanced_ids,
                            # already computed above) via the tail assignment
                            # below, so it can't wedge the EventType forever.
                            _log.warning(
                                "eventlogfile: abandoning file %s after mid-file download "
                                "failure (created %s, older than download_max_age %s): %s",
                                file_meta.id,
                                file_meta.created_date,
                                download_max_age,
                                exc,
                            )
                            break
                        # Stop the cycle here for the same watermark-safety
                        # reason as above: the unprocessed suffix (this file
                        # and everything after it) is re-listed next cycle.
                        self._record_cycle_failure(
                            event_type, f"download of {file_meta.id} (mid-file)", exc
                        )
                        return
                    entry = self._make_entry(
                        pending,
                        event_type=event_type,
                        key=key,
                        sm_fields=sm_fields,
                        label_fields=label_fields,
                        checkpoint_value=pre_file_value,
                        sample=type_cfg.sample,
                        ts_fallback=created,
                    )
                    if entry is not None:
                        last_yielded_value = pre_file_value
                        yield entry
                    pending = nxt
            finally:
                # Async generators expose aclose(); close eagerly on every exit
                # path (incl. early returns) so no half-consumed generator lingers.
                aclose = getattr(row_iter, "aclose", None)
                if aclose is not None:
                    with contextlib.suppress(Exception):
                        await aclose()

            current_last_created, current_ids = advanced_last_created, advanced_ids

        # Issue #64: the cursor may have advanced this cycle (files fully
        # processed or abandoned past download_max_age) without any yielded
        # entry actually carrying that final state downstream — e.g. every
        # row of every file was filtered/sampled out, or the last file was
        # abandoned mid-file/first-row with no rows emitted. Without a durable
        # commit here, the next cycle (and every cycle after a restart)
        # re-fetches and deterministically re-drops/re-abandons the exact same
        # window forever. Mirrors the pubsub source's keepalive pattern: a
        # checkpoint_only token rides the same queue/commit path as a real
        # entry but is never sent to the sink.
        final_value = json.dumps(
            {"last_created": current_last_created, "ids": _serialize_carried_ids(current_ids)},
            sort_keys=True,
        )
        cursor_advanced = (current_last_created, current_ids) != (since, ids)
        if cursor_advanced and final_value != last_yielded_value:
            yield LogEntry(
                timestamp=datetime.now(UTC),
                labels={},
                line="",
                structured_metadata={},
                checkpoint=CheckpointToken(key=key, value=final_value),
                checkpoint_only=True,
            )

        # The whole type processed without a contained failure: reset escalation.
        self._consecutive_failures.pop(event_type, None)

    def _make_entry(
        self,
        row: dict[str, str],
        *,
        event_type: str,
        key: str,
        sm_fields: Sequence[str],
        label_fields: Sequence[str],
        checkpoint_value: str,
        sample: float,
        ts_fallback: datetime | None = None,
    ) -> LogEntry | None:
        # Redaction/filter transforms run BEFORE shaping (route_fields, label
        # promotion, timestamp extraction) so redacting the timestamp column
        # triggers the fallback below. A drop_row match returns None (the row is
        # not emitted; the file's checkpoint still advances via later rows).
        if self._transforms.apply(row) is None:
            return None
        # Deterministic per-type sampling: a sampled-out row emits no entry but
        # the file still counts as processed and the checkpoint/watermark
        # advances exactly as for a kept row (rows never checkpoint individually).
        if sample < 1.0 and not should_keep(
            row.get("REQUEST_ID") or json.dumps(row, sort_keys=True), sample
        ):
            self._metrics.entries_sampled_out.labels(source=self.name, event_type=event_type).inc()
            return None

        # An unparseable row timestamp falls back to the FILE's CreatedDate
        # (stable across replays, so re-emitted rows stay byte-identical and
        # dedup in Loki) rather than now(UTC); extract_timestamp_checked clamps
        # a >1h-old fallback near now to stay inside Loki's OOO window.
        ts, used_fallback = extract_timestamp_checked(
            row,
            field_names=(self._cfg.timestamp_column, "TIMESTAMP"),
            fallback=ts_fallback,
        )
        if used_fallback:
            self._metrics.timestamp_fallbacks.labels(source="eventlogfile").inc()
        line, sm = route_fields(row, sm_fields)
        # Promoted labels first, then reserved keys — reserved win so a
        # promoted column can never clobber source identity.
        labels: dict[str, str] = {
            **promote_labels(row, label_fields),
            "source": "eventlogfile",
            "event_type": event_type,
        }

        self._metrics.eventlogfile_rows_ingested.labels(event_type=event_type).inc()

        return LogEntry(
            timestamp=ts,
            labels=labels,
            line=line,
            structured_metadata=sm,
            checkpoint=CheckpointToken(key=key, value=checkpoint_value),
        )
