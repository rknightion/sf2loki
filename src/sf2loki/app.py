"""Composition root and the shared Pipeline.

``Pipeline`` fans every enabled :class:`~sf2loki.sources.base.Source` into one
bounded queue and drains it with a single sequential emit worker that batches by
size/bytes/interval, pushes to the sink, and commits checkpoints on success.
``App`` wires the concrete implementations together from config and owns process
lifecycle (signals, metrics/health servers, graceful shutdown).
"""

from __future__ import annotations

import asyncio
import contextlib
import json
import signal
import time
from collections.abc import Awaitable, Callable, Mapping, Sequence
from dataclasses import dataclass
from datetime import UTC, datetime
from typing import TYPE_CHECKING, Any

import httpx

from sf2loki.auth.jwt_auth import TokenProvider
from sf2loki.config import (
    EVENT_TYPE_WILDCARD,
    Config,
    LokiBatchConfig,
    telemetry_headers,
)
from sf2loki.coordinate.base import NoopCoordinator
from sf2loki.egress import EgressGovernor
from sf2loki.model import Batch, LogEntry
from sf2loki.obs.health import Health
from sf2loki.obs.logging import configure_logging, get_logger
from sf2loki.obs.metrics import Metrics
from sf2loki.sinks.base import PermanentSinkError, RetryableSinkError
from sf2loki.sinks.loki.sink import LokiSink
from sf2loki.sources.eventlog_objects_source import EventLogObjectsSource
from sf2loki.sources.eventlogfile_source import EventLogFileSource
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

    Backpressure is structural and two-dimensional: a slow sink leaves the
    bounded queue full — by entry count (``batch.queue_maxsize``) or by
    approximate bytes (``batch.queue_max_bytes``) — which blocks producers and
    therefore suspends each source's event stream — Salesforce stops being
    asked for more. The byte bound caps worst-case buffered memory during a
    sink outage (entries can be ~max_line_bytes each, so a count bound alone
    could buffer gigabytes).
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
        # Approximate bytes currently sitting in the queue, guarded by the
        # condition; producers wait on it when over batch.queue_max_bytes.
        self._queued_bytes = 0
        self._byte_cond = asyncio.Condition()
        # Monotonic instant the sink started failing continuously (None while
        # healthy); feeds the /readyz degradation check installed by App.build.
        self._sink_failing_since: float | None = None

    @property
    def sink_failing_since(self) -> float | None:
        """``time.monotonic()`` of the first failure of the current sink outage.

        None while the sink is healthy (or after a permanent drop, which
        advances the pipeline rather than wedging it).
        """
        return self._sink_failing_since

    def set_static_labels(self, labels: Mapping[str, str]) -> None:
        """Set the deployment-wide labels merged into every entry (job/org/env)."""
        self._static_labels = dict(labels)

    async def run(self, stop: asyncio.Event) -> None:
        """Run all producers and the single consumer until ``stop`` and drained.

        FIRST_EXCEPTION semantics for the consumer: it can only finish normally
        after seeing every producer's sentinel, so if it completes while
        producers are still running it died (e.g. an ``OSError`` from the
        checkpoint file write). In that case producers — which would otherwise
        block forever on the full queue — are cancelled and the exception is
        re-raised so the process exits nonzero and gets restarted (checkpoints
        are safe; the batch is simply retried after restart).
        """
        if not self._sources:
            return
        # Load the persisted daily-budget counter before the first flush so
        # admission decisions are deterministic from the start.
        if self._governor is not None:
            await self._governor.start()
        self._queued_bytes = 0  # fresh accounting per run (the queue is fresh too)
        queue: asyncio.Queue[LogEntry | object] = asyncio.Queue(maxsize=self._queue_maxsize)
        producers = [asyncio.create_task(self._produce(src, queue, stop)) for src in self._sources]
        consumer = asyncio.create_task(self._consume(queue, len(producers), stop))
        producers_done = asyncio.gather(*producers)
        try:
            first_done: set[asyncio.Future[Any]] = {producers_done, consumer}
            await asyncio.wait(first_done, return_when=asyncio.FIRST_COMPLETED)
            if consumer.done() and not producers_done.done():
                # Consumer died mid-stream: crash out instead of hanging.
                producers_done.cancel()
                exc = consumer.exception()
                if exc is not None:
                    log.error("pipeline consumer failed; aborting", error=str(exc))
                    raise exc
                return
            await producers_done
            # Producers each enqueue a sentinel in their finally; the consumer
            # returns once it has seen one per producer and flushed the tail.
            await consumer
        finally:
            producers_done.cancel()
            consumer.cancel()
            await asyncio.gather(producers_done, consumer, return_exceptions=True)

    async def _produce(
        self, source: Source, queue: asyncio.Queue[LogEntry | object], stop: asyncio.Event
    ) -> None:
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
                await self._charge(entry)
                await queue.put(entry)
                # Also updated here (not just in the consumer) so the gauge keeps
                # tracking queue growth while the consumer is stuck in sink retry.
                self._metrics.queue_depth.set(queue.qsize())
        finally:
            # Sentinels carry no bytes, so this put can never block on the byte
            # budget — a cancelled/finished producer always delivers its sentinel.
            await queue.put(_SENTINEL)

    @staticmethod
    def _entry_cost(item: LogEntry | object) -> int:
        """Approximate queued bytes for one item (same accounting as _consume).

        checkpoint_only entries (keepalive tokens with an empty line) and the
        internal sentinel are free — they must never contribute to, or block
        on, the byte budget.
        """
        if not isinstance(item, LogEntry) or item.checkpoint_only:
            return 0
        return len(item.line.encode("utf-8")) + _QUEUE_ENTRY_OVERHEAD

    async def _charge(self, entry: LogEntry) -> None:
        """Producer side: wait for byte-budget headroom, then account *entry*.

        Admission rule: admit while the queue is *under* budget — an admitted
        entry may overshoot it. This guarantees a single entry larger than the
        whole budget is still admitted once the queue drains (blocking it until
        "it fits" would deadlock: it never fits, and the consumer would have
        nothing to drain). Cancellation while waiting is safe: nothing has been
        accounted yet, and Condition.wait re-raises after reacquiring the lock.
        """
        budget = self._batch.queue_max_bytes
        if budget <= 0:  # 0 disables byte accounting entirely
            return
        cost = self._entry_cost(entry)
        if cost == 0:
            return
        async with self._byte_cond:
            await self._byte_cond.wait_for(lambda: self._queued_bytes < budget)
            self._queued_bytes += cost

    async def _release(self, item: LogEntry | object) -> None:
        """Consumer side: return *item*'s bytes to the budget and wake producers."""
        if self._batch.queue_max_bytes <= 0:
            return
        cost = self._entry_cost(item)
        if cost == 0:
            return
        async with self._byte_cond:
            self._queued_bytes -= cost
            self._byte_cond.notify_all()

    async def _consume(
        self, queue: asyncio.Queue[LogEntry | object], n_producers: int, stop: asyncio.Event
    ) -> None:
        flush_interval = self._batch.flush_interval.total_seconds()
        loop = asyncio.get_running_loop()
        active = n_producers
        batch: list[LogEntry] = []
        approx_bytes = 0
        deadline: float | None = None

        while True:
            try:
                if deadline is None:
                    item = await queue.get()
                else:
                    timeout = max(0.0, deadline - loop.time())
                    item = await asyncio.wait_for(queue.get(), timeout)
            except TimeoutError:
                await self._flush(batch, stop)
                batch, approx_bytes, deadline = [], 0, None
                continue

            self._metrics.queue_depth.set(queue.qsize())
            await self._release(item)

            if item is _SENTINEL:
                active -= 1
                if active == 0:
                    await self._flush(batch, stop)
                    return
                continue

            assert isinstance(item, LogEntry)
            batch.append(item)
            if not item.checkpoint_only:
                approx_bytes += len(item.line.encode("utf-8")) + _QUEUE_ENTRY_OVERHEAD
            if deadline is None:
                deadline = loop.time() + flush_interval
            if len(batch) >= self._batch.max_entries or approx_bytes >= self._batch.max_bytes:
                await self._flush(batch, stop)
                batch, approx_bytes, deadline = [], 0, None

    async def _flush(self, entries: list[LogEntry], stop: asyncio.Event) -> None:
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
                if self._sink_failing_since is None:
                    self._sink_failing_since = time.monotonic()
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
                self._sink_failing_since = None
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
                self._sink_failing_since = None
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
        for key, value in last.items():
            await self._state.commit(key, value)
            self._record_commit_metric(key, value)

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


def _build_state(cfg: Config) -> CheckpointStore:
    return build_store(cfg.state)


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


@dataclass(frozen=True, slots=True)
class _StartupInfo:
    """Summary of what the app is configured to run, for the startup banner."""

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
        metrics: Metrics,
        health: Health,
        closers: Sequence[Callable[[], Awaitable[None]]],
        limits_poller: LimitsPoller | None = None,
        startup: _StartupInfo | None = None,
    ) -> None:
        self._cfg = cfg
        self._pipeline = pipeline
        self._tokens = tokens
        self._metrics = metrics
        self._health = health
        self._closers = list(closers)
        self._limits_poller = limits_poller
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
        tokens = TokenProvider(cfg.salesforce, sf_http, metrics=metrics)

        loki_http = httpx.AsyncClient(timeout=_HTTP_TIMEOUT)
        sink = LokiSink(cfg.sink.loki, loki_http, metrics=metrics)

        state = _build_state(cfg)
        sm_fields = cfg.sink.loki.structured_metadata_fields
        transform_salt = (
            cfg.sources.transform_salt.get_secret_value() if cfg.sources.transform_salt else ""
        )

        sources: list[Source] = []
        closers: list[Callable[[], Awaitable[None]]] = []
        pubsub_topics: list[str] = []

        # Identifiers for the polling sources come straight from config, so
        # extract them before any source is constructed: the Pub/Sub source
        # (built first) needs to know which categories they own.
        stored_objects: list[str] = (
            [o.name for o in cfg.sources.eventlog_objects.objects]
            if cfg.sources.eventlog_objects.enabled
            else []
        )
        # ELF wildcard expands at poll time, so its discovered types aren't
        # known here; the overlap guard and category ownership see only the
        # explicit types (use exclude to keep a discovered category off ELF
        # when another source owns it).
        elf_event_types: list[str] = (
            [t.name for t in cfg.sources.eventlogfile.event_types if t.name != EVENT_TYPE_WILDCARD]
            if cfg.sources.eventlogfile.enabled
            else []
        )

        if cfg.sources.pubsub.enabled:
            from sf2loki.salesforce.metadata_client import MetadataClient
            from sf2loki.salesforce.pubsub_client import PubSubClient

            # Categories another source already owns (stored event objects +
            # explicit ELF event types): Pub/Sub wildcard discovery auto-excludes
            # these so the same events aren't ingested twice — unless
            # allow_overlap, where the operator wants both (pass empty set).
            if cfg.sources.allow_overlap:
                pubsub_owned: frozenset[str] = frozenset()
            else:
                pubsub_owned = frozenset(
                    [category_of_stored_object(o) for o in stored_objects]
                    + [category_of_elf(t) for t in elf_event_types]
                )

            pubsub_client = PubSubClient(cfg.sources.pubsub, tokens, metrics=metrics)
            pubsub_src = PubSubSource(
                cfg.sources.pubsub,
                pubsub_client,
                sm_fields=sm_fields,
                metrics=metrics,
                topic_discoverer=MetadataClient(cfg.salesforce, tokens, sf_http),
                owned_categories=pubsub_owned,
                transform_salt=transform_salt,
            )
            sources.append(pubsub_src)
            pubsub_topics = pubsub_src.resolve_topics()
            closers.append(pubsub_client.aclose)

        if cfg.sources.eventlog_objects.enabled:
            from sf2loki.salesforce.soql_client import SoqlClient

            soql = SoqlClient(cfg.salesforce, tokens, sf_http, metrics=metrics)
            sources.append(
                EventLogObjectsSource(
                    cfg.sources.eventlog_objects,
                    soql,
                    sm_fields=sm_fields,
                    metrics=metrics,
                    transform_salt=transform_salt,
                )
            )

        if cfg.sources.eventlogfile.enabled:
            from sf2loki.salesforce.eventlogfile_client import EventLogFileClient

            # Mirror image of pubsub_owned: categories the higher-priority
            # sources feed. The ELF wildcard auto-excludes these discovered
            # types so the same events aren't ingested twice — unless
            # allow_overlap, where the operator wants both the real-time-lean
            # stream and the richer ELF rows (pass empty set).
            if cfg.sources.allow_overlap:
                elf_owned: frozenset[str] = frozenset()
            else:
                elf_owned = frozenset(
                    [category_of_pubsub(t) for t in pubsub_topics]
                    + [category_of_stored_object(o) for o in stored_objects]
                )

            elf_client = EventLogFileClient(cfg.salesforce, tokens, sf_http, metrics=metrics)
            sources.append(
                EventLogFileSource(
                    cfg.sources.eventlogfile,
                    elf_client,
                    sm_fields=sm_fields,
                    metrics=metrics,
                    exclude_categories=elf_owned,
                    transform_salt=transform_salt,
                )
            )

        # Fail fast if one event category is fed by more than one source (which
        # would ingest duplicate events). Bypass with sources.allow_overlap.
        check_overlap(
            pubsub_topics=pubsub_topics,
            stored_objects=stored_objects,
            elf_event_types=elf_event_types,
            allow_overlap=cfg.sources.allow_overlap,
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
        # non-None reason wins. The budget-pause reason is checked first (an
        # exhausted budget is a deliberate, operator-visible state), then the
        # sink-failing reason. Liveness (/healthz) is deliberately unaffected —
        # data is checkpointed and retried, so a restart would not help.
        degraded_checks: list[Callable[[], str | None]] = []
        if governor is not None:
            # degraded_reason() self-gates to pause mode, so it is inert for
            # rate-cap-only or drop-mode configurations.
            degraded_checks.append(governor.degraded_reason)
        unready_after = cfg.service.unready_after_sink_failing.total_seconds()
        if unready_after > 0:
            degraded_checks.append(_sink_degradation_check(pipeline, unready_after))
        if degraded_checks:
            health.set_degraded_check(_composite_degraded_check(degraded_checks))

        limits_poller: LimitsPoller | None = None
        if cfg.salesforce.limits.enabled:
            from sf2loki.obs.limits_poller import LimitsPoller
            from sf2loki.salesforce.limits_client import LimitsClient

            limits_poller = LimitsPoller(
                LimitsClient(cfg.salesforce, tokens, sf_http),
                metrics,
                cfg.salesforce.limits.poll_interval,
            )

        closers.extend([sink.aclose, sf_http.aclose, loki_http.aclose])

        # Release the state store's exclusive-instance lock last, after the
        # pipeline has drained and committed its final checkpoints.
        state_close = getattr(state, "close", None)
        if callable(state_close):

            async def _close_state() -> None:
                state_close()

            closers.append(_close_state)
        return cls(
            cfg=cfg,
            pipeline=pipeline,
            tokens=tokens,
            metrics=metrics,
            health=health,
            closers=closers,
            limits_poller=limits_poller,
            startup=_StartupInfo(
                pubsub_topics=pubsub_topics,
                eventlog_objects=stored_objects,
                eventlogfile_event_types=elf_event_types,
                sink_url=cfg.sink.loki.url,
                limits_enabled=cfg.salesforce.limits.enabled,
            ),
        )

    def _emit_startup_log(self) -> None:
        """Announce, at INFO, what the app is configured to run."""
        s = self._startup
        log.info(
            "sf2loki starting",
            pubsub_topics=s.pubsub_topics if s else [],
            eventlog_objects=s.eventlog_objects if s else [],
            eventlogfile_event_types=s.eventlogfile_event_types if s else [],
            sink=s.sink_url if s else "",
            org_limit_metrics=s.limits_enabled if s else False,
            environment=self._cfg.salesforce.environment,
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

        # Startup auth probe: mint a token in EVERY configuration (even when
        # salesforce.org_id is set, in which case org_id() never touches the
        # network) so bad credentials fail fast and exit nonzero here, before
        # readiness — instead of every source loop retrying forever while the
        # process reports healthy.
        await self._tokens.token()

        # Resolve the org id once and assemble deployment-wide labels.
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

        coordinator = NoopCoordinator()
        grace = self._cfg.service.shutdown_grace.total_seconds()

        async def on_acquire() -> None:
            self._health.set_ready()
            log.info("sf2loki ready — streaming to Loki")
            poller_task: asyncio.Task[None] | None = None
            if self._limits_poller is not None:
                poller_task = asyncio.create_task(self._limits_poller.run(stop))
            try:
                await _drain_with_grace(self._pipeline.run(stop), stop, grace)
            finally:
                if poller_task is not None:
                    poller_task.cancel()
                    with contextlib.suppress(asyncio.CancelledError):
                        await poller_task

        async def on_lose() -> None:
            self._health.set_not_ready()

        try:
            await coordinator.run(on_acquire=on_acquire, on_lose=on_lose, stop=stop)
        finally:
            with contextlib.suppress(TimeoutError):
                await asyncio.wait_for(self._shutdown(), timeout=_CLOSE_TIMEOUT)
            self._health.set_not_ready()
            await self._health.stop()
            self._metrics.shutdown()

    async def _shutdown(self) -> None:
        for close in self._closers:
            with contextlib.suppress(Exception):
                await close()
