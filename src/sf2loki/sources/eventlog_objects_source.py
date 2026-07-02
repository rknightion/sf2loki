"""EventLog Objects source: polls Salesforce SOQL for EventLog object records.

Ref: DESIGN.md §7.

Supports any sObject that Salesforce surfaces as a queryable EventLog object
(e.g. LoginEvent, ApiEvent). Uses FIELDS(ALL) which requires LIMIT <=200
(Salesforce documented constraint).

Checkpoint format (JSON string, per ``eventlog_objects:<name>`` key):
``{"last_ts": "<timestamp_field value>", "ids": ["<record Id>", ...]}``.
The query cursor is ``timestamp_field >= last_ts`` (not ``>``): with a strict
``>`` a timestamp tie at the page boundary (the 201st record sharing the
200th's timestamp) would be skipped forever. The re-fetched boundary records
are deduped via the rolling ``ids`` window (mirroring the ELF source's
carried-ids design). Within a cycle, full pages trigger follow-up queries
(drain-until-short-page) so throughput is not capped at 200 per poll interval.
Backward compatibility: a pre-upgrade checkpoint is the bare timestamp string;
it loads as ``last_ts`` with an empty id window (the single boundary record is
re-emitted once — an accepted at-least-once duplicate).

Checkpoint poisoning guards: a record with a null/unparseable timestamp is
still shipped but carries the previous good watermark (never ``""``/``"None"``,
which would render ``WHERE EventDate > `` -> MALFORMED_QUERY -> crash-loop),
and a loaded watermark is validated before being interpolated into SOQL —
garbage falls back to the lookback default with a WARNING.

Big Objects (the stored RTEM event family — LoginEvent, ApiEvent,
FileEventStore, *EventStore, ...) reject ORDER BY ASC and have no
nextRecordsUrl pagination. Set ``big_object: true`` per object to poll them:
the source then drains newest-first (ORDER BY <ts> DESC) with a ratcheting
upper bound and re-sorts each cycle's window ascending before emitting, so the
watermark/dedup/checkpoint semantics match the ASC path. FIELDS(ALL) itself
works on Big Objects; only the ASC order was the problem.
"""

from __future__ import annotations

import asyncio
import contextlib
import json
import logging
import random
from collections.abc import AsyncIterator, Sequence
from datetime import UTC, datetime

from sf2loki.config import EventLogObjectConfig, EventLogObjectsConfig
from sf2loki.model import CheckpointToken, LogEntry
from sf2loki.obs.metrics import Metrics
from sf2loki.salesforce.soql_client import (
    SoqlClient,
    SoqlError,
    SoqlThrottledError,
    to_soql_datetime_literal,
)
from sf2loki.shaping import extract_timestamp_checked, route_fields, should_keep
from sf2loki.state.base import CheckpointStore
from sf2loki.transforms import compile_rules

# FIELDS(ALL) requires LIMIT <=200 (Salesforce documented constraint).
_PAGE_LIMIT = 200

# Rolling id-dedup window carried in the checkpoint. Must exceed _PAGE_LIMIT so
# a full page of boundary ties can never evict its own ids mid-drain.
_MAX_CARRIED_IDS = 500

# Consecutive per-object cycle failures at which the skip log escalates to ERROR.
_ERROR_LOG_THRESHOLD = 3

_log = logging.getLogger(__name__)


def _is_valid_watermark(value: str) -> bool:
    """True when *value* parses as an ISO-8601-ish datetime (SF ``+0000`` included)."""
    return _watermark_datetime(value) is not None


def _watermark_datetime(value: str) -> datetime | None:
    """Parse a watermark string to an aware datetime, or None if unparseable."""
    if not value:
        return None
    try:
        dt = datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError:
        return None
    return dt if dt.tzinfo is not None else dt.replace(tzinfo=UTC)


def _parse_checkpoint(raw: str) -> tuple[str, list[str]]:
    """Decode a stored checkpoint, accepting both formats.

    New format: JSON ``{"last_ts": ..., "ids": [...]}``. Legacy format: the bare
    timestamp string (loads with an empty id window).
    """
    try:
        parsed = json.loads(raw)
    except ValueError:
        return raw, []
    if isinstance(parsed, dict):
        wm = str(parsed.get("last_ts") or "")
        raw_ids = parsed.get("ids", [])
        ids = [str(i) for i in raw_ids] if isinstance(raw_ids, list) else []
        return wm, ids
    # Some other JSON scalar (a legacy value that happened to parse): treat the
    # raw string as the timestamp.
    return raw, []


class EventLogObjectsSource:
    """Polls Salesforce EventLog sObjects via SOQL and yields :class:`~sf2loki.model.LogEntry`.

    Satisfies the :class:`~sf2loki.sources.base.Source` protocol. SOQL failures
    are contained per cycle (WARNING, escalating to ERROR after repeated
    consecutive failures) — the poll loop sleeps and retries; checkpoints make
    the retry safe. A 403 ``REQUEST_LIMIT_EXCEEDED`` aborts the rest of the
    cycle so an exhausted API budget isn't hammered further.
    """

    name = "eventlog_objects"

    def __init__(
        self,
        cfg: EventLogObjectsConfig,
        soql: SoqlClient,
        *,
        sm_fields: Sequence[str],
        metrics: Metrics | None = None,
        poll_once: bool = False,
        transform_salt: str = "",
    ) -> None:
        self._cfg = cfg
        self._soql = soql
        self._sm_fields = sm_fields
        self._metrics = metrics if metrics is not None else Metrics()
        # poll_once=True runs a single cycle and returns (useful in tests to avoid
        # an infinite polling loop).
        self._poll_once = poll_once
        self._consecutive_failures: dict[str, int] = {}
        self._cycle_throttled = False
        # Precompiled redaction/filter pipeline (empty when no transforms).
        self._transforms = compile_rules(
            cfg.transforms, salt=transform_salt, source=self.name, metrics=self._metrics
        )

    async def events(
        self,
        state: CheckpointStore,
        stop: asyncio.Event,
    ) -> AsyncIterator[LogEntry]:
        """Yield log entries for all enabled EventLog objects.

        Each wake of the polling loop, per DUE object (each object is scheduled
        on its OWN ``poll_interval``, with ±10% jitter):
        1. Load the stored watermark + id window (or default to now-lookback).
        2. Issue SOQL ``WHERE timestamp_field >= <watermark> ORDER BY ASC LIMIT 200``,
           repeating while full pages return (drain-until-short-page), deduping
           already-seen record ids.
        3. Yield a :class:`~sf2loki.model.LogEntry` per new record, with a
           :class:`~sf2loki.model.CheckpointToken` carrying the advanced
           watermark + id window (so the pipeline can commit it after push).
        4. Sleep until the earliest per-object due time (stop-aware).

        Stop semantics: checks ``stop`` before each cycle and between records;
        returns promptly when set.

        The pipeline (not this source) is responsible for committing checkpoints
        to ``state``; we only read the watermark here.
        """
        if not self._cfg.objects:
            return

        loop = asyncio.get_running_loop()
        # Per-object next-due times on the monotonic loop clock; every object
        # is due immediately on startup.
        next_due: dict[str, float] = dict.fromkeys(
            (obj.name for obj in self._cfg.objects), loop.time()
        )

        while True:
            if stop.is_set():
                return

            self._cycle_throttled = False
            wake = loop.time()
            for obj in self._cfg.objects:
                if next_due[obj.name] > wake:
                    continue  # not due yet: its own interval hasn't elapsed
                if stop.is_set():
                    return

                async for entry in self._process_object(obj, state, stop):
                    yield entry

                next_due[obj.name] = loop.time() + self._jittered_interval(obj)

                if self._cycle_throttled:
                    # API budget exhausted: defer every still-due object a full
                    # interval — polling them now would burn more of the
                    # exhausted budget.
                    now = loop.time()
                    for other in self._cfg.objects:
                        if next_due[other.name] <= now:
                            next_due[other.name] = now + self._jittered_interval(other)
                    break

            if self._poll_once:
                return

            timeout = max(0.0, min(next_due.values()) - loop.time())
            with contextlib.suppress(TimeoutError):
                await asyncio.wait_for(stop.wait(), timeout=timeout)

    @staticmethod
    def _jittered_interval(obj: EventLogObjectConfig) -> float:
        """The object's poll interval in seconds, with ±10% jitter.

        Jitter desynchronizes multi-instance / multi-object cycles (same idiom
        as the EventLogFile source).
        """
        interval = obj.poll_interval.total_seconds()
        if interval > 0:
            interval *= random.uniform(0.9, 1.1)  # jitter, not cryptographic
        return interval

    async def _process_object(
        self,
        obj: EventLogObjectConfig,
        state: CheckpointStore,
        stop: asyncio.Event,
    ) -> AsyncIterator[LogEntry]:
        key = f"eventlog_objects:{obj.name}"

        # --- 1. Resolve watermark + id window ---
        raw = await state.load(key)
        if raw is None:
            watermark = ""
            window: list[str] = []
        else:
            watermark, window = _parse_checkpoint(raw)

        if not _is_valid_watermark(watermark):
            if raw is not None:
                # A stored-but-garbage watermark ("", "None", junk) would render a
                # malformed WHERE clause and crash-loop until hand-edited.
                _log.warning(
                    "eventlog_objects[%s]: stored watermark %r is not a valid "
                    "datetime; falling back to now-lookback (%s)",
                    obj.name,
                    watermark,
                    obj.lookback,
                )
            default_wm = datetime.now(UTC) - obj.lookback
            # Format as SOQL datetime literal: ISO-8601 with Z suffix.
            watermark = default_wm.strftime("%Y-%m-%dT%H:%M:%SZ")

        if obj.big_object:
            try:
                records = await self._drain_big_object(obj, watermark)
            except SoqlThrottledError as exc:
                self._cycle_throttled = True
                self._metrics.soql_poll_errors.labels(
                    source="eventlog_objects", object=obj.name
                ).inc()
                _log.error(
                    "eventlog_objects[%s]: Salesforce API request limit exceeded; "
                    "backing off until the next poll interval: %s",
                    obj.name,
                    exc,
                )
                return
            except SoqlError as exc:
                count = self._consecutive_failures.get(obj.name, 0) + 1
                self._consecutive_failures[obj.name] = count
                self._metrics.soql_poll_errors.labels(
                    source="eventlog_objects", object=obj.name
                ).inc()
                level = logging.ERROR if count >= _ERROR_LOG_THRESHOLD else logging.WARNING
                _log.log(
                    level,
                    "eventlog_objects[%s]: big-object SOQL poll failed (%d consecutive "
                    "failure(s); will retry next cycle): %s",
                    obj.name,
                    count,
                    exc,
                )
                return

            seen = set(window)
            records = [r for r in records if str(r.get("Id") or "") not in seen]

            for record in records:
                if stop.is_set():
                    return
                entry, watermark, window = self._emit_record(obj, key, record, watermark, window)
                if entry is not None:
                    yield entry
            self._consecutive_failures.pop(obj.name, None)
            return

        # --- 2/3. Drain pages until a short page (or no progress) ---
        while True:
            # A stored watermark is the raw value Salesforce returned (e.g.
            # "…+0000"), which is NOT a legal SOQL literal — reformat before
            # interpolating it into the WHERE clause.
            # FIELDS(ALL) is a Salesforce convenience that selects every field;
            # it requires LIMIT <=200 (platform constraint). ``>=`` (with the id
            # window) instead of ``>`` so a timestamp tie at the page boundary
            # is never skipped.
            soql = (
                f"SELECT FIELDS(ALL) FROM {obj.name} "
                f"WHERE {obj.timestamp_field} >= {to_soql_datetime_literal(watermark)} "
                f"ORDER BY {obj.timestamp_field} ASC "
                f"LIMIT {_PAGE_LIMIT}"
            )

            try:
                page = [record async for record in self._soql.query(soql)]
            except SoqlThrottledError as exc:
                self._cycle_throttled = True
                self._metrics.soql_poll_errors.labels(
                    source="eventlog_objects", object=obj.name
                ).inc()
                _log.error(
                    "eventlog_objects[%s]: Salesforce API request limit exceeded; "
                    "backing off until the next poll interval: %s",
                    obj.name,
                    exc,
                )
                return
            except SoqlError as exc:
                count = self._consecutive_failures.get(obj.name, 0) + 1
                self._consecutive_failures[obj.name] = count
                self._metrics.soql_poll_errors.labels(
                    source="eventlog_objects", object=obj.name
                ).inc()
                level = logging.ERROR if count >= _ERROR_LOG_THRESHOLD else logging.WARNING
                _log.log(
                    level,
                    "eventlog_objects[%s]: SOQL poll failed (%d consecutive "
                    "failure(s); will retry next cycle): %s",
                    obj.name,
                    count,
                    exc,
                )
                if "BIG_OBJECT_UNSUPPORTED_OPERATION" in str(exc):
                    _log.warning(
                        "eventlog_objects[%s]: this looks like a Salesforce Big Object "
                        "(it rejected ORDER BY ASC). Set `big_object: true` on this "
                        "object in config to poll it (DESC descending-drain mode).",
                        obj.name,
                    )
                return

            seen = set(window)
            new_records = [r for r in page if str(r.get("Id") or "") not in seen]

            if not new_records:
                if len(page) >= _PAGE_LIMIT:
                    _log.warning(
                        "eventlog_objects[%s]: a full page at watermark %s contained "
                        "only already-seen records (>%d records share one timestamp?); "
                        "stopping this cycle to avoid a hot loop",
                        obj.name,
                        watermark,
                        _PAGE_LIMIT,
                    )
                break

            for record in new_records:
                if stop.is_set():
                    return
                entry, watermark, window = self._emit_record(obj, key, record, watermark, window)
                if entry is not None:
                    yield entry

            self._consecutive_failures.pop(obj.name, None)

            if len(page) < _PAGE_LIMIT:
                break  # short page: caught up for this cycle

    def _emit_record(
        self,
        obj: EventLogObjectConfig,
        key: str,
        record: dict[str, object],
        watermark: str,
        window: list[str],
    ) -> tuple[LogEntry | None, str, list[str]]:
        """Advance the cursor from *record* and build its LogEntry (or None if dropped).

        Shared by the ASC and big-object fetch paths — the ONLY difference between
        the two modes is how records are fetched/ordered; per-record shaping,
        watermark/id-window advance, transforms, sampling, and checkpointing are
        identical. Returns the advanced (watermark, window) so the caller threads
        them into the next record (and into the committed CheckpointToken).

        The watermark/window advance from the ORIGINAL record BEFORE transforms —
        the watermark is the query cursor and the id is the dedup key, so both must
        use the real Salesforce values even when a transform redacts them. This runs
        for dropped/sampled-out records too, so their Id enters the dedup window.
        The watermark advances only on a VALID timestamp; a null/garbage one keeps
        the previous good watermark (never ""/"None", which would poison the next
        WHERE clause).
        """
        ts_field_val = str(record.get(obj.timestamp_field) or "")
        if _is_valid_watermark(ts_field_val):
            watermark = ts_field_val
        record_id = str(record.get("Id") or "")
        if record_id:
            window = [*window, record_id][-_MAX_CARRIED_IDS:]

        if self._transforms.apply(record) is None:
            return None, watermark, window
        if obj.sample < 1.0 and not should_keep(
            record_id or json.dumps(record, sort_keys=True, default=str), obj.sample
        ):
            self._metrics.entries_sampled_out.labels(
                source="eventlog_objects", event_type=obj.name
            ).inc()
            return None, watermark, window

        ts, used_fallback = extract_timestamp_checked(
            record,
            field_names=(obj.timestamp_field, "EventDate", "CreatedDate"),
            fallback=_watermark_datetime(watermark),
        )
        if used_fallback:
            self._metrics.timestamp_fallbacks.labels(source="eventlog_objects").inc()
        line, sm = route_fields(record, self._sm_fields)

        labels: dict[str, str] = {"source": "eventlog_objects", "event_type": obj.name}
        checkpoint = CheckpointToken(
            key=key,
            value=json.dumps({"ids": window, "last_ts": watermark}, sort_keys=True),
        )
        entry = LogEntry(
            timestamp=ts,
            labels=labels,
            line=line,
            structured_metadata=sm,
            checkpoint=checkpoint,
        )
        return entry, watermark, window

    async def _drain_big_object(
        self, obj: EventLogObjectConfig, watermark: str
    ) -> list[dict[str, object]]:
        """Fetch this cycle's window for a Big Object, returned sorted ASC.

        Big Objects reject ORDER BY ASC and expose no nextRecordsUrl pagination
        (verified against the dev org), so we page newest-first with ORDER BY
        <ts> DESC and a ratcheting upper bound: each full page lowers the bound to
        its oldest timestamp (``<=`` so a tie straddling the 200-row boundary is
        re-fetched and deduped, never skipped — the mirror of the ASC path's
        ``>=`` + id-window). Drains until a short page or a full page that adds no
        new id (all-ties guard). The collected window is sorted ASCENDING before
        return so :meth:`_emit_record` advances the watermark forward monotonically
        exactly as in the ASC mode — keeping crash-safety identical.

        Memory note: this buffers one cycle's window (bounded by poll_interval of
        data in steady state; a first-run lookback or post-outage catch-up buffers
        the whole window). Large historical backfills are a separate command, not
        this poll path.
        """
        collected: dict[str, dict[str, object]] = {}
        upper: str | None = None
        while True:
            clauses = [f"{obj.timestamp_field} >= {to_soql_datetime_literal(watermark)}"]
            if upper is not None:
                clauses.append(f"{obj.timestamp_field} <= {to_soql_datetime_literal(upper)}")
            soql = (
                f"SELECT FIELDS(ALL) FROM {obj.name} "
                f"WHERE {' AND '.join(clauses)} "
                f"ORDER BY {obj.timestamp_field} DESC "
                f"LIMIT {_PAGE_LIMIT}"
            )
            page = [record async for record in self._soql.query(soql)]
            if not page:
                break

            new_in_page = 0
            for record in page:
                rid = str(record.get("Id") or "")
                if rid and rid not in collected:
                    collected[rid] = record
                    new_in_page += 1

            if len(page) < _PAGE_LIMIT:
                break  # short page: window fully drained
            if new_in_page == 0:
                _log.warning(
                    "eventlog_objects[%s]: a full DESC page at bound %s added no new "
                    "records (>%d rows share one timestamp?); stopping drain to avoid "
                    "a hot loop",
                    obj.name,
                    upper,
                    _PAGE_LIMIT,
                )
                break

            # Lower the bound to this page's oldest timestamp for the next sub-query.
            oldest_rec = min(
                page,
                key=lambda r: (
                    _watermark_datetime(str(r.get(obj.timestamp_field) or ""))
                    or datetime.max.replace(tzinfo=UTC)
                ),
            )
            upper = str(oldest_rec.get(obj.timestamp_field) or "")

        return sorted(
            collected.values(),
            key=lambda r: (
                _watermark_datetime(str(r.get(obj.timestamp_field) or ""))
                or datetime.min.replace(tzinfo=UTC)
            ),
        )
