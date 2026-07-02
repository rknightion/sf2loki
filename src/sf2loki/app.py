"""Composition root and the shared Pipeline.

``Pipeline`` splits its sources into lanes by class (streaming vs bulk, see
:func:`_lane_of`) and fans each lane's :class:`~sf2loki.sources.base.Source`\\ s
into that lane's own bounded queue, drained by that lane's own emit worker which
batches by size/bytes/interval, pushes to the sink, and commits checkpoints on
success. Per-lane queues stop a bulk drain (a Daily ELF / big-object cycle) from
head-of-line-blocking realtime streaming (issue #53). ``App`` wires the concrete
implementations together from config and owns process lifecycle (signals,
metrics/health servers, graceful shutdown).
"""

from __future__ import annotations

import asyncio
import contextlib
import importlib.util
import inspect
import json
import os
import signal
import socket
import time
from collections.abc import Awaitable, Callable, Mapping, Sequence
from dataclasses import dataclass
from datetime import UTC, datetime
from typing import TYPE_CHECKING, Any

import httpx

from sf2loki.auth.jwt_auth import AuthError, TokenProvider
from sf2loki.config import (
    EVENT_TYPE_WILDCARD,
    Config,
    ConfigError,
    LokiBatchConfig,
    OrgConfig,
    telemetry_headers,
)
from sf2loki.coordinate.base import Coordinator, NoopCoordinator, StateFenceError
from sf2loki.coordinate.file_lease import FileLeaseCoordinator
from sf2loki.egress import EgressGovernor
from sf2loki.model import Batch, LogEntry
from sf2loki.obs.health import Health
from sf2loki.obs.logging import configure_logging, get_logger
from sf2loki.obs.metrics import Metrics
from sf2loki.sinks.base import PermanentSinkError, RetryableSinkError
from sf2loki.sinks.loki.sink import LokiSink
from sf2loki.sources.apexlog_source import ApexLogSource
from sf2loki.sources.eventlog_objects_source import EventLogObjectsSource
from sf2loki.sources.eventlogfile_source import EventLogFileSource
from sf2loki.sources.org_adapter import OrgSource
from sf2loki.sources.overlap import (
    category_of_elf,
    category_of_pubsub,
    category_of_stored_object,
    check_overlap,
)
from sf2loki.sources.pubsub_source import PubSubSource
from sf2loki.state import build_store

if TYPE_CHECKING:
    from sf2loki.obs.limits_poller import LimitsPoller
    from sf2loki.sinks.base import Sink
    from sf2loki.sources.base import Source
    from sf2loki.state.base import CheckpointStore

log = get_logger(__name__)

# Internal sentinel a producer puts on the queue when its source is exhausted.
_SENTINEL: object = object()

# Pipeline-level retry backoff for a batch the sink reports as retryable.
_RETRY_BACKOFF_BASE = 1.0
_RETRY_BACKOFF_MAX = 30.0

# Fixed per-entry overhead added to len(line) for the approximate byte
# accounting shared by batch flushing and the queue byte budget (labels,
# timestamps, object headers — cheap to compute, close enough to bound memory).
_QUEUE_ENTRY_OVERHEAD = 64

# Source classes get separate lanes so a bulk drain (a Daily ELF / big-object
# cycle of millions of rows) can't head-of-line-block realtime streaming
# (issue #53). pubsub is the only streaming class; every other source
# (eventlogfile/eventlog_objects/apexlog) is bulk. Classification is by
# source.name, which OrgSource preserves verbatim, so multi-org works unchanged.
_STREAMING_SOURCE_NAMES = frozenset({"pubsub"})


def _lane_of(source: Source) -> str:
    """The lane class for *source*: ``streaming`` (pubsub) or ``bulk`` (all else)."""
    return "streaming" if source.name in _STREAMING_SOURCE_NAMES else "bulk"


@dataclass
class _LaneState:
    """One ingestion lane: its own bounded queue, byte budget, and push worker.

    Splitting sources into lanes (streaming vs bulk) gives each lane an
    independent queue + consumer, so a saturated bulk lane's backpressure and
    slow Loki pushes cannot block the streaming lane's producers or its pushes.
    Per-key FIFO (and thus commit monotonicity) is preserved because each
    source's checkpoint keys are disjoint and stay within one lane.
    """

    name: str
    queue: asyncio.Queue[LogEntry | object]
    byte_cond: asyncio.Condition
    queued_bytes: int = 0
    n_producers: int = 0
    # Monotonic instant this lane's sink pushes started failing continuously
    # (None while healthy). Per-lane so a healthy streaming lane cannot clear a
    # failing bulk lane's outage mark and mask /readyz degradation (issue #53).
    failing_since: float | None = None


def build_static_labels(
    *, environment: str, org_id: str, operator_labels: Mapping[str, str]
) -> dict[str, str]:
    """Deployment-wide stream labels applied to every emitted entry.

    Always sets ``job`` + ``service_name`` (so these streams surface under the
    sf2loki exporter in Grafana rather than as ``unknown_service``) and derives
    ``environment`` from the Salesforce ``environment`` toggle. Operator-supplied
    ``sink.loki.labels`` are merged last, so they win over these defaults (e.g. to
    point ``service_name`` at the monitored system or override ``environment``).
    All keys must be in :data:`~sf2loki.sinks.loki.labels.ALLOWED_LABELS`; the
    per-entry identity keys in
    :data:`~sf2loki.sinks.loki.labels.RESERVED_STATIC_LABELS` (``source``,
    ``event_type``) are rejected at startup by the sink's label guard.
    """
    return {
        "job": "sf2loki",
        "service_name": "sf2loki",
        "environment": environment,
        "sf_org_id": org_id,
        **operator_labels,
    }


# Fixed budget for closing resources (http/grpc clients) during shutdown — kept
# short and separate from shutdown_grace (which bounds the pipeline drain) so
# the two don't stack past the container runtime's stop timeout.
_CLOSE_TIMEOUT: float = 5.0

# Explicit timeouts for the shared HTTP clients: httpx defaults to 5s for
# everything, which makes large Loki pushes / slow Salesforce responses churn
# as transport errors.
_HTTP_TIMEOUT = httpx.Timeout(connect=10.0, read=30.0, write=30.0, pool=30.0)


class Pipeline:
    """Drain sources into batches and push them to the sink, committing on success.

    Sources are split into lanes by class (:func:`_lane_of`: ``pubsub`` →
    streaming, everything else → bulk); each lane has its own queue, emit worker,
    and byte budget, so a saturated bulk lane can't starve realtime streaming
    (issue #53). With a single lane behaviour is identical to the old shared
    queue + single consumer.

    Backpressure is structural, two-dimensional, and PER LANE: a slow sink leaves
    a lane's bounded queue full — by entry count (``batch.queue_maxsize``) or by
    approximate bytes (``batch.queue_max_bytes``) — which blocks only THAT lane's
    producers and therefore suspends only those sources' event streams. The byte
    bound caps worst-case buffered memory during a sink outage (entries can be
    ~max_line_bytes each, so a count bound alone could buffer gigabytes); because
    each lane carries the full ``queue_max_bytes``, worst-case buffered memory is
    ``n_lanes * queue_max_bytes`` (<= 2x — at most a streaming and a bulk lane).
    """

    def __init__(
        self,
        *,
        sources: Sequence[Source],
        sink: Sink,
        state: CheckpointStore,
        batch: LokiBatchConfig,
        metrics: Metrics,
        static_labels: Mapping[str, str] | None = None,
        queue_maxsize: int | None = None,
        governor: EgressGovernor | None = None,
    ) -> None:
        self._sources = list(sources)
        self._sink = sink
        self._state = state
        self._batch = batch
        self._metrics = metrics
        self._governor = governor
        self._static_labels: dict[str, str] = dict(static_labels or {})
        # Explicit override for tests; production wiring takes the config value.
        self._queue_maxsize = batch.queue_maxsize if queue_maxsize is None else queue_maxsize
        # Lanes built per run() (streaming vs bulk); byte budget + failing-since
        # are per-lane (see _LaneState). Held on the pipeline so the readiness
        # property can aggregate across lanes.
        self._lanes: list[_LaneState] = []
        # Last-observed qsize per lane name; the unlabelled queue_depth gauge is
        # their sum (the operationally meaningful "how much is buffered now").
        self._lane_depths: dict[str, int] = {}

    @property
    def sink_failing_since(self) -> float | None:
        """``time.monotonic()`` of the first failure of the current sink outage.

        Aggregated across lanes as the EARLIEST (min) failing lane, so the
        longest-running outage drives the /readyz degradation check — a healthy
        lane can never mask a wedged one. None while every lane is healthy (or
        after a permanent drop, which advances the pipeline rather than wedging).
        """
        times = [lane.failing_since for lane in self._lanes if lane.failing_since is not None]
        return min(times) if times else None

    def _new_lane(self, name: str = "lane") -> _LaneState:
        """Construct a lane (its own bounded queue + byte condition) and register it.

        Registered on ``self._lanes`` so the ``sink_failing_since`` property sees
        it. The queue takes ``self._queue_maxsize`` so the per-entry count bound
        applies per lane exactly as it did for the single shared queue.
        """
        lane = _LaneState(
            name=name,
            queue=asyncio.Queue(maxsize=self._queue_maxsize),
            byte_cond=asyncio.Condition(),
        )
        self._lanes.append(lane)
        return lane

    def _publish_queue_depth(self, lane: _LaneState) -> None:
        """Record *lane*'s current depth and publish the sum across all lanes."""
        self._lane_depths[lane.name] = lane.queue.qsize()
        self._metrics.queue_depth.set(sum(self._lane_depths.values()))

    def set_static_labels(self, labels: Mapping[str, str]) -> None:
        """Set the deployment-wide labels merged into every entry (job/org/env)."""
        self._static_labels = dict(labels)

    async def run(self, stop: asyncio.Event) -> None:
        """Run all producers and one consumer PER LANE until ``stop`` and drained.

        Sources are split into lanes by class (streaming vs bulk, see
        :func:`_lane_of`) so a bulk drain can't head-of-line-block streaming
        (issue #53). Each lane gets its own queue + consumer + byte budget, so
        up to ``n_lanes`` Loki pushes are in flight concurrently. When every
        source classifies to one lane this is byte-identical to the old single
        queue + single consumer.

        Crash semantics generalise across N lanes: a consumer returns normally
        only after seeing every sentinel from ITS lane's producers, so if ALL
        consumers returned normally then all producers finished too. Therefore a
        consumer finishing while producers still run can only mean a consumer
        RAISED (e.g. an ``OSError`` from the checkpoint write) — producers, which
        would otherwise block forever on a full queue, are cancelled and the
        exception re-raised so the process exits nonzero and restarts. A producer
        exception is surfaced by ``producers_done`` (gather resolves on the first
        one), never masked by its lane's consumer returning on the sentinel.
        """
        if not self._sources:
            return
        # Load the persisted daily-budget counter before the first flush so
        # admission decisions are deterministic from the start.
        if self._governor is not None:
            await self._governor.start()
        # Fresh lane state per run (queues are fresh too); clears any stale
        # outage mark / depth left by a previous acquisition so a re-acquired
        # leader doesn't report degraded.
        self._lanes = []
        self._lane_depths = {}
        grouped: dict[str, list[Source]] = {}
        for src in self._sources:
            grouped.setdefault(_lane_of(src), []).append(src)
        producer_tasks: list[asyncio.Task[None]] = []
        consumer_tasks: list[asyncio.Task[None]] = []
        for name, srcs in grouped.items():
            lane = self._new_lane(name)
            lane.n_producers = len(srcs)
            for src in srcs:
                producer_tasks.append(asyncio.create_task(self._produce(src, lane, stop)))
            consumer_tasks.append(asyncio.create_task(self._consume(lane, stop)))
        producers_done = asyncio.gather(*producer_tasks)
        consumers_done = asyncio.gather(*consumer_tasks)
        try:
            await asyncio.wait(
                {producers_done, consumers_done}, return_when=asyncio.FIRST_COMPLETED
            )
            if consumers_done.done() and not producers_done.done():
                # A consumer died mid-stream: crash out instead of hanging.
                producers_done.cancel()
                exc = consumers_done.exception()
                if exc is not None:
                    log.error("pipeline consumer failed; aborting", error=str(exc))
                    raise exc
                return
            await producers_done
            # Producers each enqueue a sentinel in their finally; each lane's
            # consumer returns once it has seen one per producer and flushed.
            await consumers_done
        finally:
            producers_done.cancel()
            consumers_done.cancel()
            await asyncio.gather(producers_done, consumers_done, return_exceptions=True)

    async def _produce(self, source: Source, lane: _LaneState, stop: asyncio.Event) -> None:
        try:
            async for entry in source.events(self._state, stop):
                if not entry.checkpoint_only:
                    if self._static_labels:
                        entry.labels = {**entry.labels, **self._static_labels}
                    event_type = entry.labels.get("event_type", "unknown")
                    self._metrics.events_ingested.labels(
                        source=source.name, event_type=event_type
                    ).inc()
                    lag = (datetime.now(UTC) - entry.timestamp).total_seconds()
                    self._metrics.ingest_lag.labels(event_type=event_type).observe(lag)
                await self._charge(lane, entry)
                await lane.queue.put(entry)
                # Also updated here (not just in the consumer) so the gauge keeps
                # tracking queue growth while the consumer is stuck in sink retry.
                self._publish_queue_depth(lane)
        finally:
            # Sentinels carry no bytes, so this put can never block on the byte
            # budget — a cancelled/finished producer always delivers its sentinel.
            await lane.queue.put(_SENTINEL)

    @staticmethod
    def _entry_cost(item: LogEntry | object) -> int:
        """Approximate queued *memory* for one item (the queue byte budget).

        checkpoint_only entries (keepalive tokens with an empty line) and the
        internal sentinel are free — they must never contribute to, or block
        on, the byte budget.

        Counts the line plus the labels dict, the structured_metadata dict, and
        the checkpoint value string — an ELF/eventlog_objects entry carries a
        multi-KB carried-id-window checkpoint string, so ignoring those (as the
        original len(line)+64 did) undercounts real RAM at the bound by up to
        ~2x, precisely during the sink outage the byte budget exists to bound.
        """
        if not isinstance(item, LogEntry) or item.checkpoint_only:
            return 0
        cost = len(item.line.encode("utf-8")) + _QUEUE_ENTRY_OVERHEAD
        for key, value in item.labels.items():
            cost += len(key) + len(value)
        for key, value in item.structured_metadata.items():
            cost += len(key) + len(value)
        cost += len(item.checkpoint.value)
        return cost

    async def _charge(self, lane: _LaneState, entry: LogEntry) -> None:
        """Producer side: wait for *lane*'s byte-budget headroom, then account *entry*.

        The budget is PER LANE: a saturated bulk lane blocks only bulk producers,
        never the streaming lane's producer (issue #53). Admission rule: admit
        while the lane is *under* budget — an admitted entry may overshoot it.
        This guarantees a single entry larger than the whole budget is still
        admitted once the lane drains (blocking it until "it fits" would deadlock:
        it never fits, and the consumer would have nothing to drain). Cancellation
        while waiting is safe: nothing has been accounted yet, and Condition.wait
        re-raises after reacquiring the lock.
        """
        budget = self._batch.queue_max_bytes
        if budget <= 0:  # 0 disables byte accounting entirely
            return
        cost = self._entry_cost(entry)
        if cost == 0:
            return
        async with lane.byte_cond:
            await lane.byte_cond.wait_for(lambda: lane.queued_bytes < budget)
            lane.queued_bytes += cost

    async def _release(self, lane: _LaneState, item: LogEntry | object) -> None:
        """Consumer side: return *item*'s bytes to *lane*'s budget and wake producers."""
        if self._batch.queue_max_bytes <= 0:
            return
        cost = self._entry_cost(item)
        if cost == 0:
            return
        async with lane.byte_cond:
            lane.queued_bytes -= cost
            lane.byte_cond.notify_all()

    async def _consume(self, lane: _LaneState, stop: asyncio.Event) -> None:
        flush_interval = self._batch.flush_interval.total_seconds()
        loop = asyncio.get_running_loop()
        active = lane.n_producers
        batch: list[LogEntry] = []
        approx_bytes = 0
        deadline: float | None = None

        while True:
            try:
                if deadline is None:
                    item = await lane.queue.get()
                else:
                    timeout = max(0.0, deadline - loop.time())
                    item = await asyncio.wait_for(lane.queue.get(), timeout)
            except TimeoutError:
                await self._flush(lane, batch, stop)
                batch, approx_bytes, deadline = [], 0, None
                continue

            self._publish_queue_depth(lane)
            await self._release(lane, item)

            if item is _SENTINEL:
                active -= 1
                if active == 0:
                    await self._flush(lane, batch, stop)
                    return
                continue

            assert isinstance(item, LogEntry)
            batch.append(item)
            if not item.checkpoint_only:
                approx_bytes += len(item.line.encode("utf-8")) + _QUEUE_ENTRY_OVERHEAD
            if deadline is None:
                deadline = loop.time() + flush_interval
            if len(batch) >= self._batch.max_entries or approx_bytes >= self._batch.max_bytes:
                await self._flush(lane, batch, stop)
                batch, approx_bytes, deadline = [], 0, None

    async def _flush(self, lane: _LaneState, entries: list[LogEntry], stop: asyncio.Event) -> None:
        if not entries:
            return
        # checkpoint_only entries (e.g. Pub/Sub keepalive replay_ids) ride the
        # batch for FIFO commit ordering but are never handed to the sink.
        all_entries = Batch(entries=list(entries))
        real = [e for e in entries if not e.checkpoint_only]
        if not real:
            # Nothing to push: FIFO ordering guarantees any real entry for the
            # same key was already pushed in an earlier flush, so committing
            # these tokens directly preserves the commit-after-push invariant.
            await self._commit(all_entries)
            return
        batch = Batch(entries=real)
        # Egress governor: rate caps + daily byte budget. Admission is per-batch,
        # BEFORE the retry loop (retries never re-admit). lines/bytes are computed
        # once and reused for record() after a successful push.
        lines = len(real)
        bytes_ = 0
        if self._governor is not None:
            bytes_ = sum(len(e.line.encode("utf-8")) for e in real)
            admitted = await self._governor.admit(lines, bytes_, stop)
            if not admitted:
                # Drop-mode budget exhaustion: discard this batch (counted) but
                # advance checkpoints — the permanent-drop shape, never wedged.
                self._metrics.loki_entries_dropped.labels(reason="budget").inc(len(real))
                log.warning(
                    "dropping over-budget batch and advancing checkpoint",
                    entries=len(real),
                    bytes=bytes_,
                )
                await self._commit(all_entries)
                return
        backoff = _RETRY_BACKOFF_BASE
        while True:
            t0 = asyncio.get_running_loop().time()
            try:
                await self._sink.push(batch)
            except RetryableSinkError:
                self._metrics.loki_push.labels(outcome="retried").inc()
                if lane.failing_since is None:
                    lane.failing_since = time.monotonic()
                if stop.is_set():
                    # Shutting down: abandon this batch uncommitted rather than
                    # hammering a failing sink past the grace period — it will
                    # be retried in full after restart (checkpoint never advanced).
                    return
                with contextlib.suppress(TimeoutError):
                    await asyncio.wait_for(stop.wait(), timeout=backoff)
                    return  # stop fired during backoff
                backoff = min(backoff * 2, _RETRY_BACKOFF_MAX)
                continue
            except PermanentSinkError as exc:
                # Poison batch: drop it, count the gap loudly, and advance past
                # it. Count every dropped entry (a permanent error on a
                # multi-entry batch drops all of them), not just one.
                # Not a stuck sink (it answered, the batch advanced) — clear the
                # continuous-failure mark so readiness doesn't degrade for it.
                lane.failing_since = None
                self._metrics.loki_entries_dropped.labels(reason=exc.reason).inc(len(batch.entries))
                log.error(
                    "dropping undeliverable batch and advancing checkpoint",
                    entries=len(batch.entries),
                    reason=exc.reason,
                    error=str(exc),
                )
                await self._commit(all_entries)
                return
            else:
                lane.failing_since = None
                self._metrics.loki_push.labels(outcome="success").inc()
                self._metrics.last_push_success_ts.set(datetime.now(UTC).timestamp())
                self._metrics.loki_push_duration.observe(asyncio.get_running_loop().time() - t0)
                log.debug("pushed batch to loki", entries=len(batch.entries))
                if self._governor is not None:
                    await self._governor.record(lines, bytes_)
                await self._commit(all_entries)
                return

    async def _commit(self, batch: Batch) -> None:
        # The last token per key wins (sources emit in monotonic order).
        last: dict[str, str] = {}
        for entry in batch.entries:
            last[entry.checkpoint.key] = entry.checkpoint.value
        # One multi-key write per flush (one serialize + one fsync / one object
        # PUT) instead of one full-document rewrite per key. commit_many is a
        # duck-typed optimisation on the real stores; fall back to per-key commit
        # for any store (e.g. a test fake) that doesn't implement it.
        commit_many = getattr(self._state, "commit_many", None)
        if commit_many is not None:
            await commit_many(last)
        else:
            for key, value in last.items():
                await self._state.commit(key, value)
        for key, value in last.items():
            self._record_commit_metric(key, value)

    def reset_state(self) -> None:
        """Invalidate the checkpoint store's cached document (on leadership loss).

        Duck-typed like ``commit_many``: only the real stores implement
        ``reset``. Clearing the cache + CAS token on demotion means a later
        re-acquisition re-reads fresh checkpoints (another instance may have led
        in between) instead of serving stale values and crashing on the first
        conditional-write conflict.
        """
        reset = getattr(self._state, "reset", None)
        if callable(reset):
            reset()

    def _record_commit_metric(self, key: str, value: str) -> None:
        if key.startswith("pubsub:"):
            topic = key.removeprefix("pubsub:")
            self._metrics.last_replay_commit_ts.labels(topic=topic).set(
                datetime.now(UTC).timestamp()
            )
        elif key.startswith("eventlog_objects:"):
            obj = key.removeprefix("eventlog_objects:")
            self._metrics.watermark_ts.labels(source="eventlog_objects", object=obj).set(
                _parse_watermark_seconds(value)
            )
        elif key.startswith("eventlogfile:"):
            event_type = key.removeprefix("eventlogfile:")
            self._metrics.watermark_ts.labels(source="eventlogfile", object=event_type).set(
                _parse_eventlogfile_watermark(value)
            )
        elif key == "apexlog":
            self._metrics.watermark_ts.labels(source="apexlog", object="apexlog").set(
                _parse_apexlog_watermark(value)
            )


def _parse_watermark_seconds(value: str) -> float:
    """Best-effort parse of a checkpoint value into a Unix timestamp.

    This feeds an observability gauge only — an unparseable value falls back
    to "now" rather than raising, since it must never break checkpoint commit.
    """
    try:
        return datetime.fromisoformat(value.replace("Z", "+00:00")).timestamp()
    except ValueError:
        return datetime.now(UTC).timestamp()


def _parse_eventlogfile_watermark(value: str) -> float:
    """Parse an eventlogfile checkpoint (JSON {"last_created", "ids"}) → Unix ts.

    Falls back to "now" on any parse error — observability only, never raises.
    """
    try:
        last_created = json.loads(value).get("last_created", "")
        return _parse_watermark_seconds(str(last_created))
    except ValueError, AttributeError, TypeError:
        return datetime.now(UTC).timestamp()


def _parse_apexlog_watermark(value: str) -> float:
    """Parse an apexlog checkpoint (JSON {"last_ts", "ids"}) → Unix ts.

    Falls back to "now" on any parse error — observability only, never raises.
    """
    try:
        return _parse_watermark_seconds(str(json.loads(value).get("last_ts", "")))
    except ValueError, AttributeError, TypeError:
        return datetime.now(UTC).timestamp()


async def _drain_with_grace(awaitable: Awaitable[None], stop: asyncio.Event, grace: float) -> None:
    """Run *awaitable* to completion, but bound how long it may run after ``stop`` fires.

    Every producer/consumer loop in this codebase is expected to notice ``stop``
    and exit on its own (the normal, fast path). This is the backstop: if
    something doesn't — a stuck call, a bug — the drain is force-cancelled
    *grace* seconds after shutdown was requested, so the process still exits
    within roughly ``shutdown_grace`` instead of hanging indefinitely.
    """
    task: asyncio.Task[None] = asyncio.ensure_future(awaitable)
    stop_waiter = asyncio.ensure_future(stop.wait())
    try:
        done, _ = await asyncio.wait({task, stop_waiter}, return_when=asyncio.FIRST_COMPLETED)
        if task in done:
            await task  # propagate any exception; no-op if it finished cleanly
            return
        try:
            await asyncio.wait_for(task, timeout=grace)
        except TimeoutError:
            log.warning("pipeline did not drain within shutdown_grace; cancelling")
            task.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await task
    finally:
        if not stop_waiter.done():
            stop_waiter.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await stop_waiter


def _build_state(cfg: Config, *, exclusive_lock: bool = True) -> CheckpointStore:
    return build_store(cfg.state, exclusive_lock=exclusive_lock)


def _format_failing_duration(seconds: float) -> str:
    """Human-short duration for the /readyz degradation body ('17m' / '45s')."""
    return f"{int(seconds // 60)}m" if seconds >= 60 else f"{int(seconds)}s"


def _sink_degradation_check(pipeline: Pipeline, threshold: float) -> Callable[[], str | None]:
    """Readiness predicate: degrade once the sink has failed continuously > *threshold* s.

    Evaluated per /readyz request, so it recovers by itself on the next
    successful push (which clears ``pipeline.sink_failing_since``).
    """

    def check() -> str | None:
        since = pipeline.sink_failing_since
        if since is None:
            return None
        elapsed = time.monotonic() - since
        if elapsed <= threshold:
            return None
        return f"degraded: loki pushes failing for {_format_failing_duration(elapsed)}"

    return check


def _composite_degraded_check(
    checks: Sequence[Callable[[], str | None]],
) -> Callable[[], str | None]:
    """Compose readiness predicates: return the first non-None reason, else None."""

    def check() -> str | None:
        for c in checks:
            reason = c()
            if reason is not None:
                return reason
        return None

    return check


def deployment_static_labels(operator_labels: Mapping[str, str]) -> dict[str, str]:
    """Deployment-wide labels for MULTI-org mode (``sf_org_id``/``environment`` are per-org).

    In multi-org mode each org's :class:`~sf2loki.sources.org_adapter.OrgSource`
    injects ``org`` + ``sf_org_id`` + ``environment`` per entry (they differ per
    org), so the shared pipeline sets only ``job``/``service_name`` plus the
    operator's ``sink.loki.labels``. (Single-org keeps :func:`build_static_labels`
    unchanged.)
    """
    return {"job": "sf2loki", "service_name": "sf2loki", **operator_labels}


@dataclass(frozen=True, slots=True)
class _OrgAuth:
    """Per-org auth handle used by the startup probe and the degraded-org check."""

    name: str
    tokens: TokenProvider
    environment: str


@dataclass
class _OrgSources:
    """Raw (unwrapped) sources + identity for one org, produced by _build_org_sources."""

    sources: list[Source]
    closers: list[Callable[[], Awaitable[None]]]
    pubsub_topics: list[str]
    stored_objects: list[str]
    elf_event_types: list[str]


def _build_org_sources(
    org: OrgConfig,
    *,
    tokens: TokenProvider,
    metrics: Metrics,
    sf_http: httpx.AsyncClient,
    sm_fields: Sequence[str],
) -> _OrgSources:
    """Construct one org's enabled sources (raw, pre-OrgSource wrapping).

    Factored out of :meth:`App.build` so single-org and every multi-org entry
    share identical per-source construction. ``metrics`` is the org-scoped proxy
    (``Metrics.for_org``), so every instrument these components touch gains the
    ``org`` label; ``sm_fields`` is the deployment-wide sink setting.
    """
    sf_cfg = org.salesforce
    transform_salt = (
        org.sources.transform_salt.get_secret_value() if org.sources.transform_salt else ""
    )
    # Compliance: an `action: hash` rule with no transform_salt produces
    # unsalted SHA-256 — reversible for low-entropy PII (IPs, usernames) by
    # rainbow table, silently defeating the redaction the operator configured.
    # Warn loudly at startup / --check (doctor surfaces the same check).
    from sf2loki.transforms import unsalted_hash_warnings

    _all_transform_rules = [
        *org.sources.pubsub.transforms,
        *org.sources.eventlog_objects.transforms,
        *org.sources.eventlogfile.transforms,
        *org.sources.apexlog.transforms,
    ]
    for warning in unsalted_hash_warnings(_all_transform_rules, transform_salt):
        log.warning(
            "hash transform has no transform_salt — hashes of low-entropy values "
            "(IPs, usernames) are reversible by table lookup; set sources.transform_salt",
            org=org.name or "single",
            rule=warning,
        )
    sources: list[Source] = []
    closers: list[Callable[[], Awaitable[None]]] = []
    pubsub_topics: list[str] = []

    stored_objects: list[str] = (
        [o.name for o in org.sources.eventlog_objects.objects]
        if org.sources.eventlog_objects.enabled
        else []
    )
    elf_event_types: list[str] = (
        [t.name for t in org.sources.eventlogfile.event_types if t.name != EVENT_TYPE_WILDCARD]
        if org.sources.eventlogfile.enabled
        else []
    )

    if org.sources.pubsub.enabled:
        from sf2loki.salesforce.metadata_client import MetadataClient
        from sf2loki.salesforce.pubsub_client import PubSubClient

        if org.sources.allow_overlap:
            pubsub_owned: frozenset[str] = frozenset()
        else:
            pubsub_owned = frozenset(
                [category_of_stored_object(o) for o in stored_objects]
                + [category_of_elf(t) for t in elf_event_types]
            )

        pubsub_client = PubSubClient(org.sources.pubsub, tokens, metrics=metrics)
        pubsub_src = PubSubSource(
            org.sources.pubsub,
            pubsub_client,
            sm_fields=sm_fields,
            metrics=metrics,
            bridge_max_bytes=org.sources.pubsub.bridge_max_bytes,
            topic_discoverer=MetadataClient(sf_cfg, tokens, sf_http),
            owned_categories=pubsub_owned,
            transform_salt=transform_salt,
        )
        sources.append(pubsub_src)
        pubsub_topics = pubsub_src.resolve_topics()
        closers.append(pubsub_client.aclose)

    if org.sources.eventlog_objects.enabled:
        from sf2loki.salesforce.soql_client import SoqlClient

        soql = SoqlClient(sf_cfg, tokens, sf_http, metrics=metrics)
        sources.append(
            EventLogObjectsSource(
                org.sources.eventlog_objects,
                soql,
                sm_fields=sm_fields,
                metrics=metrics,
                transform_salt=transform_salt,
            )
        )

    if org.sources.eventlogfile.enabled:
        from sf2loki.salesforce.eventlogfile_client import EventLogFileClient

        if org.sources.allow_overlap:
            elf_owned: frozenset[str] = frozenset()
        else:
            elf_owned = frozenset(
                [category_of_pubsub(t) for t in pubsub_topics]
                + [category_of_stored_object(o) for o in stored_objects]
            )

        elf_client = EventLogFileClient(sf_cfg, tokens, sf_http, metrics=metrics)
        sources.append(
            EventLogFileSource(
                org.sources.eventlogfile,
                elf_client,
                sm_fields=sm_fields,
                metrics=metrics,
                exclude_categories=elf_owned,
                transform_salt=transform_salt,
            )
        )

    if org.sources.apexlog.enabled:
        from sf2loki.salesforce.apexlog_client import ApexLogClient

        apex_client = ApexLogClient(sf_cfg, tokens, sf_http, metrics=metrics)
        sources.append(
            ApexLogSource(
                org.sources.apexlog,
                apex_client,
                sm_fields=sm_fields,
                metrics=metrics,
                transform_salt=transform_salt,
            )
        )

    # Fail fast if one event category is fed by more than one source WITHIN this
    # org (cross-org overlap is fine — different orgs, different events). ApexLog
    # is a distinct developer-log category with no cross-source collision, so it
    # is intentionally not part of the overlap check.
    check_overlap(
        pubsub_topics=pubsub_topics,
        stored_objects=stored_objects,
        elf_event_types=elf_event_types,
        allow_overlap=org.sources.allow_overlap,
    )
    return _OrgSources(
        sources=sources,
        closers=closers,
        pubsub_topics=pubsub_topics,
        stored_objects=stored_objects,
        elf_event_types=elf_event_types,
    )


def _org_auth_degraded_check(
    failed: Mapping[str, TokenProvider],
) -> Callable[[], str | None]:
    """Readiness predicate: degrade while a startup-failed org still lacks a token.

    ``failed`` is populated by the startup probe (multi-org). An org that later
    mints a token reactively (its sources retry on auth) drops out of the reason,
    so readiness recovers on its own.
    """

    def check() -> str | None:
        for name, tokens in failed.items():
            if not tokens.has_token():
                return f"degraded: org {name} auth failing"
        return None

    return check


@dataclass(frozen=True, slots=True)
class _StartupInfo:
    """Summary of what the app is configured to run, for the startup banner."""

    orgs: list[str]
    pubsub_topics: list[str]
    eventlog_objects: list[str]
    eventlogfile_event_types: list[str]
    sink_url: str
    limits_enabled: bool


class App:
    """The running service: wires implementations and owns process lifecycle."""

    def __init__(
        self,
        *,
        cfg: Config,
        pipeline: Pipeline,
        tokens: TokenProvider,
        orgs: Sequence[_OrgAuth],
        metrics: Metrics,
        health: Health,
        closers: Sequence[Callable[[], Awaitable[None]]],
        coordinator: Coordinator,
        limits_pollers: Sequence[LimitsPoller] = (),
        degraded_orgs: dict[str, TokenProvider] | None = None,
        startup: _StartupInfo | None = None,
    ) -> None:
        self._cfg = cfg
        self._pipeline = pipeline
        # Primary org's token provider. For single-org (legacy) this is the only
        # org and drives the fail-fast startup probe (tests replace it directly).
        self._tokens = tokens
        self._orgs = list(orgs)
        # Multi-org unless the single resolved org is the legacy empty-name one.
        self._multi_org = not (len(self._orgs) == 1 and self._orgs[0].name == "")
        self._metrics = metrics
        self._health = health
        self._closers = list(closers)
        self._coordinator = coordinator
        self._limits_pollers = list(limits_pollers)
        # Populated by the multi-org startup probe; read by the degraded-org
        # readiness check wired in build().
        self._degraded_orgs = degraded_orgs if degraded_orgs is not None else {}
        self._startup = startup

    @classmethod
    def build(cls, cfg: Config) -> App:
        """Composition root — construct every wired-up implementation (no network)."""
        configure_logging(cfg.service.log_level, cfg.service.log_format)
        metrics = Metrics(
            telemetry=cfg.service.telemetry,
            otlp_headers=telemetry_headers(cfg.service.telemetry, cfg.sink.loki),
        )
        health = Health()

        sf_http = httpx.AsyncClient(timeout=_HTTP_TIMEOUT)

        loki_http = httpx.AsyncClient(timeout=_HTTP_TIMEOUT)
        sink = LokiSink(cfg.sink.loki, loki_http, metrics=metrics)

        # A real coordinator (not noop) is the exclusivity mechanism, so the file
        # store must NOT also take its process-lifetime sidecar flock: under a
        # file-lease HA pair sharing the state volume the flock either crash-loops
        # the promoted standby (where it propagates) or is a silent no-op (NFS
        # local_lock) — either way it doesn't fit the HA topology it ships with.
        state = _build_state(cfg, exclusive_lock=cfg.coordinate.type == "noop")

        # Leadership coordinator: noop (standalone, always leader) or a real
        # active-passive coordinator. The fence is wired into the state store
        # ONLY for a real coordinator, so a stale leader cannot commit
        # checkpoints; standalone deployments stay entirely unfenced.
        coordinator: Coordinator
        fence: Callable[[], None] | None = None
        epoch_source: Callable[[], int | None] | None = None
        if cfg.coordinate.type == "file_lease":
            fl_cfg = cfg.coordinate.file_lease
            holder = fl_cfg.holder_id or f"{socket.gethostname()}-{os.getpid()}"
            file_lease = FileLeaseCoordinator(fl_cfg, holder=holder)
            coordinator = file_lease
            fence = file_lease.check_fence
            # Durable epoch fence for the CAS-less shared-file store: a stale
            # leader whose commit carries an epoch older than the doc's is
            # rejected at write time, matching the ETag/generation CAS the object
            # stores get for free (the boolean check_fence can lag a renew).
            epoch_source = lambda: file_lease.epoch  # noqa: E731
        elif cfg.coordinate.type == "k8s_lease":
            if importlib.util.find_spec("kubernetes_asyncio") is None:
                raise ConfigError(
                    "coordinate.type is 'k8s_lease' but the Kubernetes dependencies are "
                    "not installed; install the extra: pip install 'sf2loki[k8s]'"
                )
            from sf2loki.coordinate.k8s_lease import K8sLeaseCoordinator

            # Holder defaults to the pod name ($HOSTNAME) inside the coordinator.
            k8s_lease = K8sLeaseCoordinator(cfg.coordinate.k8s_lease)
            coordinator = k8s_lease
            fence = k8s_lease.check_fence
        else:
            coordinator = NoopCoordinator()
        set_fence = getattr(state, "set_fence", None)
        if set_fence is not None and fence is not None:
            set_fence(fence)
        set_epoch = getattr(state, "set_epoch", None)
        if set_epoch is not None and epoch_source is not None:
            set_epoch(epoch_source)

        sm_fields = cfg.sink.loki.structured_metadata_fields

        # One TokenProvider + client set + source set per org. Single-org (legacy)
        # resolves to a single empty-name org: sources are used raw, no org label,
        # unprefixed checkpoints — bit-identical to pre-multi-org behaviour.
        resolved = cfg.resolved_orgs()
        sources: list[Source] = []
        closers: list[Callable[[], Awaitable[None]]] = []
        org_auths: list[_OrgAuth] = []
        limits_pollers: list[LimitsPoller] = []
        agg_topics: list[str] = []
        agg_objects: list[str] = []
        agg_elf: list[str] = []

        for index, org in enumerate(resolved):
            org_metrics = metrics.for_org(org.name)
            org_tokens = TokenProvider(org.salesforce, sf_http, metrics=org_metrics)
            org_auths.append(
                _OrgAuth(name=org.name, tokens=org_tokens, environment=org.salesforce.environment)
            )
            built = _build_org_sources(
                org, tokens=org_tokens, metrics=org_metrics, sf_http=sf_http, sm_fields=sm_fields
            )
            closers.extend(built.closers)
            agg_topics.extend(built.pubsub_topics)
            agg_objects.extend(built.stored_objects)
            agg_elf.extend(built.elf_event_types)

            if org.name:
                # Multi-org: scope each source (org label, sf_org_id/environment
                # per entry, prefixed checkpoints). The FIRST org falls back to
                # unprefixed legacy checkpoint keys so an upgraded single-org
                # deployment migrates its existing state transparently.
                for src in built.sources:
                    sources.append(
                        OrgSource(
                            src,
                            org=org.name,
                            environment=org.salesforce.environment,
                            org_id_provider=org_tokens.org_id,
                            legacy_fallback=(index == 0),
                        )
                    )
            else:
                sources.extend(built.sources)

            if org.salesforce.limits.enabled:
                from sf2loki.obs.limits_poller import LimitsPoller
                from sf2loki.salesforce.limits_client import LimitsClient

                limits_pollers.append(
                    LimitsPoller(
                        LimitsClient(org.salesforce, org_tokens, sf_http),
                        org_metrics,
                        org.salesforce.limits.poll_interval,
                    )
                )

        # Egress guardrails (rate caps + daily byte budget). Only wired in when a
        # control is actually configured; otherwise the pipeline runs governor-free
        # and behaviour is unchanged.
        egress_governor = EgressGovernor(cfg.sink.loki.egress, state=state, metrics=metrics)
        governor = egress_governor if egress_governor.enabled else None

        pipeline = Pipeline(
            sources=sources,
            sink=sink,
            state=state,
            batch=cfg.sink.loki.batch,  # queue_maxsize/queue_max_bytes ride in here
            metrics=metrics,
            governor=governor,
        )

        # Readiness degradation: a composite of independent predicates, first
        # non-None reason wins. Order: budget-pause (a deliberate operator-visible
        # state) > a failing org's auth (multi-org) > the sink failing. Liveness
        # (/healthz) is deliberately unaffected — data is checkpointed and retried.
        degraded_orgs: dict[str, TokenProvider] = {}
        degraded_checks: list[Callable[[], str | None]] = []
        if governor is not None:
            # degraded_reason() self-gates to pause mode, so it is inert for
            # rate-cap-only or drop-mode configurations.
            degraded_checks.append(governor.degraded_reason)
        if len(resolved) > 1 or (resolved and resolved[0].name):
            degraded_checks.append(_org_auth_degraded_check(degraded_orgs))
        unready_after = cfg.service.unready_after_sink_failing.total_seconds()
        if unready_after > 0:
            degraded_checks.append(_sink_degradation_check(pipeline, unready_after))
        if degraded_checks:
            health.set_degraded_check(_composite_degraded_check(degraded_checks))

        closers.extend([sink.aclose, sf_http.aclose, loki_http.aclose])

        # Release the state store's exclusive-instance lock last, after the
        # pipeline has drained and committed its final checkpoints.
        state_close = getattr(state, "close", None)
        if callable(state_close):
            # The file store's close() is sync; the s3/gcs stores' close() is
            # async (closes the aiobotocore/gcloud-aio client session). Await the
            # coroutine forms — a bare call would construct-and-discard it, leaking
            # the connector on every shutdown (getattr types it Any, so mypy can't
            # see the missing await).
            if inspect.iscoroutinefunction(state_close):

                async def _close_state() -> None:
                    await state_close()
            else:

                async def _close_state() -> None:
                    state_close()

            closers.append(_close_state)
        return cls(
            cfg=cfg,
            pipeline=pipeline,
            tokens=org_auths[0].tokens,
            orgs=org_auths,
            metrics=metrics,
            health=health,
            closers=closers,
            coordinator=coordinator,
            limits_pollers=limits_pollers,
            degraded_orgs=degraded_orgs,
            startup=_StartupInfo(
                orgs=[o.name for o in resolved],
                pubsub_topics=agg_topics,
                eventlog_objects=agg_objects,
                eventlogfile_event_types=agg_elf,
                sink_url=cfg.sink.loki.url,
                limits_enabled=any(o.salesforce.limits.enabled for o in resolved),
            ),
        )

    def _emit_startup_log(self) -> None:
        """Announce, at INFO, what the app is configured to run."""
        s = self._startup
        log.info(
            "sf2loki starting",
            orgs=[o.name for o in self._orgs] if self._multi_org else "single",
            pubsub_topics=s.pubsub_topics if s else [],
            eventlog_objects=s.eventlog_objects if s else [],
            eventlogfile_event_types=s.eventlogfile_event_types if s else [],
            sink=s.sink_url if s else "",
            org_limit_metrics=s.limits_enabled if s else False,
            log_level=self._cfg.service.log_level,
            health_addr=self._cfg.service.health_addr,
        )

    async def run(self) -> None:
        """Install signal handlers, run the pipeline under the coordinator, shut down."""
        self._emit_startup_log()

        stop = asyncio.Event()
        loop = asyncio.get_running_loop()
        for sig in (signal.SIGTERM, signal.SIGINT):
            with contextlib.suppress(NotImplementedError):
                loop.add_signal_handler(sig, stop.set)

        await self._health.start(self._cfg.service.health_addr)

        # Startup auth probe.
        if not self._multi_org:
            # Single-org (legacy): mint a token in EVERY configuration (even when
            # salesforce.org_id is set, so org_id() never touches the network) so
            # bad credentials fail fast and exit nonzero here, before readiness —
            # instead of every source loop retrying forever while healthy.
            await self._tokens.token()
            assert self._cfg.salesforce is not None  # single-org => top-level set
            org_id = self._cfg.salesforce.org_id or await self._tokens.org_id()
            log.info(
                "authenticated to salesforce org",
                org_id=org_id,
                environment=self._cfg.salesforce.environment,
            )
            self._pipeline.set_static_labels(
                build_static_labels(
                    environment=self._cfg.salesforce.environment,
                    org_id=org_id,
                    operator_labels=self._cfg.sink.loki.labels,
                )
            )
        else:
            await self._probe_orgs()
            # Multi-org: sf_org_id + environment are injected per entry by each
            # org's OrgSource, so the shared pipeline carries only deployment-wide
            # labels (job/service_name + operator sink.loki.labels).
            self._pipeline.set_static_labels(deployment_static_labels(self._cfg.sink.loki.labels))

        grace = self._cfg.service.shutdown_grace.total_seconds()

        # Leadership lifecycle. on_acquire/on_lose are driven by the coordinator
        # and may fire REPEATEDLY (a passive replica can acquire, lose, and
        # re-acquire leadership over its lifetime). Each acquisition gets its own
        # pipeline-run stop event so the pipeline can be started and torn down
        # per acquisition; the global ``stop`` still shuts everything down.
        crash: list[BaseException] = []
        current: dict[str, Any] = {}

        def _on_pipeline_done(task: asyncio.Task[None], run_stop: asyncio.Event) -> None:
            if task.cancelled():
                return
            exc = task.exception()
            if exc is not None:
                # A real pipeline crash (e.g. a checkpoint write failure): take
                # the whole process down and exit nonzero so it restarts.
                crash.append(exc)
                stop.set()
                return
            # Clean completion while we never asked it to stop means the sources
            # exhausted on their own (a finite run) -> shut down. If run_stop was
            # set this was a deliberate leadership loss/shutdown -> stay up.
            if not run_stop.is_set():
                stop.set()

        async def on_acquire() -> None:
            self._metrics.leader.set(1)
            self._health.set_ready()
            log.info("sf2loki ready — streaming to Loki")
            run_stop = asyncio.Event()
            poller_tasks = [
                asyncio.create_task(poller.run(run_stop)) for poller in self._limits_pollers
            ]
            pipeline_task = asyncio.create_task(self._run_pipeline(run_stop, grace))
            pipeline_task.add_done_callback(lambda t: _on_pipeline_done(t, run_stop))
            current["run_stop"] = run_stop
            current["pipeline_task"] = pipeline_task
            current["poller_tasks"] = poller_tasks

        async def on_lose() -> None:
            self._metrics.leader.set(0)
            log.info("leadership lost — standing by")
            await self._stop_acquisition(current)
            # Invalidate the cached checkpoint document AFTER the pipeline has
            # drained: another instance may lead before we re-acquire, so the next
            # acquisition must re-read fresh state rather than serve stale
            # checkpoints (re-ingest) or crash on the first CAS conflict.
            self._pipeline.reset_state()
            self._health.set_not_ready("standby")

        try:
            await self._coordinator.run(on_acquire=on_acquire, on_lose=on_lose, stop=stop)
        finally:
            # Drain the active acquisition (if any) even when the coordinator
            # returns without an on_lose (e.g. NoopCoordinator on global stop).
            await self._stop_acquisition(current)
            self._metrics.leader.set(0)
            with contextlib.suppress(TimeoutError):
                await asyncio.wait_for(self._shutdown(), timeout=_CLOSE_TIMEOUT)
            self._health.set_not_ready()
            await self._health.stop()
            self._metrics.shutdown()

        # Surface a pipeline crash after clean resource shutdown so the process
        # exits nonzero (and is restarted) rather than exiting 0 on a failure.
        if crash:
            raise crash[0]

    async def _run_pipeline(self, run_stop: asyncio.Event, grace: float) -> None:
        """Run one leadership acquisition's pipeline until *run_stop* and drained.

        A :class:`StateFenceError` (we were fenced mid-commit because leadership
        was lost) is absorbed here: it is a leadership transition, not a fatal
        crash — the coordinator drives the move back to standby. Any other
        exception propagates so the pipeline-done callback can crash the process.
        """
        try:
            await _drain_with_grace(self._pipeline.run(run_stop), run_stop, grace)
        except StateFenceError:
            log.warning("checkpoint commit fenced — leadership lost; standing by")

    async def _probe_orgs(self) -> None:
        """Mint each org's startup token; fail fast only if EVERY org fails.

        Some-fail is a deliberate multi-org semantic (unlike single-org's absolute
        fail-fast): the healthy orgs stream while each failing org is logged at
        ERROR, recorded for the degraded-org readiness reason, and left to retry
        auth reactively (its sources mint on their own API calls). All-fail means
        nothing can run, so we re-raise (exit nonzero → restart), as before.
        """

        async def _probe(org: _OrgAuth) -> tuple[_OrgAuth, BaseException | None]:
            try:
                await org.tokens.token()
                log.info(
                    "authenticated to salesforce org",
                    org=org.name,
                    environment=org.environment,
                )
                return (org, None)
            except AuthError as exc:
                return (org, exc)

        # Probe all orgs concurrently (10 orgs x ~300 ms serial = multi-second
        # startup otherwise); the per-org some-fail/all-fail semantics below are
        # order-preserving because gather preserves input order.
        results = await asyncio.gather(*(_probe(org) for org in self._orgs))
        failed: list[tuple[str, BaseException]] = []
        for org, exc in results:
            if exc is not None:
                failed.append((org.name, exc))
                self._degraded_orgs[org.name] = org.tokens
                log.error(
                    "org auth failed at startup; continuing with healthy orgs "
                    "(this org will retry auth reactively)",
                    org=org.name,
                    error=str(exc),
                )
        if len(failed) == len(self._orgs):
            raise failed[0][1]

    @staticmethod
    async def _stop_acquisition(current: dict[str, Any]) -> None:
        """Stop and drain the current acquisition's pipeline + pollers (idempotent)."""
        run_stop = current.pop("run_stop", None)
        pipeline_task = current.pop("pipeline_task", None)
        poller_tasks = current.pop("poller_tasks", None)
        if run_stop is not None:
            run_stop.set()
        if pipeline_task is not None:
            # Exceptions are captured by the done callback; drain quietly here.
            await asyncio.gather(pipeline_task, return_exceptions=True)
        if poller_tasks:
            for poller_task in poller_tasks:
                poller_task.cancel()
            await asyncio.gather(*poller_tasks, return_exceptions=True)

    async def _shutdown(self) -> None:
        for close in self._closers:
            with contextlib.suppress(Exception):
                await close()
