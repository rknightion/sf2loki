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
import signal
from collections.abc import Awaitable, Callable, Mapping, Sequence
from datetime import UTC, datetime
from typing import TYPE_CHECKING

import httpx

from sf2loki.auth.jwt_auth import TokenProvider
from sf2loki.config import Config, LokiBatchConfig
from sf2loki.coordinate.base import NoopCoordinator
from sf2loki.model import Batch, LogEntry
from sf2loki.obs.health import Health
from sf2loki.obs.logging import configure_logging, get_logger
from sf2loki.obs.metrics import Metrics, start_metrics_server
from sf2loki.sinks.base import PermanentSinkError, RetryableSinkError
from sf2loki.sinks.loki.sink import LokiSink
from sf2loki.sources.eventlog_objects_source import EventLogObjectsSource
from sf2loki.sources.eventlogfile_source import EventLogFileSource
from sf2loki.sources.pubsub_source import PubSubSource
from sf2loki.state.configmap_store import ConfigMapCheckpointStore
from sf2loki.state.file_store import FileCheckpointStore

if TYPE_CHECKING:
    from sf2loki.sinks.base import Sink
    from sf2loki.sources.base import Source
    from sf2loki.state.base import CheckpointStore

log = get_logger(__name__)

# Internal sentinel a producer puts on the queue when its source is exhausted.
_SENTINEL: object = object()

# Pipeline-level retry backoff for a batch the sink reports as retryable.
_RETRY_BACKOFF_BASE = 1.0
_RETRY_BACKOFF_MAX = 30.0

# Fixed budget for closing resources (http/grpc clients) during shutdown — kept
# short and separate from shutdown_grace (which bounds the pipeline drain) so
# the two don't stack past k8s's terminationGracePeriodSeconds.
_CLOSE_TIMEOUT: float = 5.0


class Pipeline:
    """Drain sources into batches and push them to the sink, committing on success.

    Backpressure is structural: a slow sink leaves the bounded queue full, which
    blocks producers on ``queue.put`` and therefore suspends each source's event
    stream — Salesforce stops being asked for more.
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
        queue_maxsize: int = 10_000,
    ) -> None:
        self._sources = list(sources)
        self._sink = sink
        self._state = state
        self._batch = batch
        self._metrics = metrics
        self._static_labels: dict[str, str] = dict(static_labels or {})
        self._queue_maxsize = queue_maxsize

    def set_static_labels(self, labels: Mapping[str, str]) -> None:
        """Set the deployment-wide labels merged into every entry (job/org/env)."""
        self._static_labels = dict(labels)

    async def run(self, stop: asyncio.Event) -> None:
        """Run all producers and the single consumer until ``stop`` and drained."""
        if not self._sources:
            return
        queue: asyncio.Queue[LogEntry | object] = asyncio.Queue(maxsize=self._queue_maxsize)
        producers = [asyncio.create_task(self._produce(src, queue, stop)) for src in self._sources]
        consumer = asyncio.create_task(self._consume(queue, len(producers), stop))
        try:
            await asyncio.gather(*producers)
            # Producers each enqueue a sentinel in their finally; the consumer
            # returns once it has seen one per producer and flushed the tail.
            await consumer
        finally:
            for task in (*producers, consumer):
                task.cancel()
            await asyncio.gather(*producers, consumer, return_exceptions=True)

    async def _produce(
        self, source: Source, queue: asyncio.Queue[LogEntry | object], stop: asyncio.Event
    ) -> None:
        try:
            async for entry in source.events(self._state, stop):
                if self._static_labels:
                    entry.labels = {**entry.labels, **self._static_labels}
                event_type = entry.labels.get("event_type", "unknown")
                self._metrics.events_ingested.labels(
                    source=source.name, event_type=event_type
                ).inc()
                lag = (datetime.now(UTC) - entry.timestamp).total_seconds()
                self._metrics.ingest_lag.labels(event_type=event_type).set(lag)
                await queue.put(entry)
        finally:
            await queue.put(_SENTINEL)

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

            if item is _SENTINEL:
                active -= 1
                if active == 0:
                    await self._flush(batch, stop)
                    return
                continue

            assert isinstance(item, LogEntry)
            batch.append(item)
            approx_bytes += len(item.line.encode("utf-8")) + 64
            if deadline is None:
                deadline = loop.time() + flush_interval
            if len(batch) >= self._batch.max_entries or approx_bytes >= self._batch.max_bytes:
                await self._flush(batch, stop)
                batch, approx_bytes, deadline = [], 0, None

    async def _flush(self, entries: list[LogEntry], stop: asyncio.Event) -> None:
        if not entries:
            return
        batch = Batch(entries=list(entries))
        backoff = _RETRY_BACKOFF_BASE
        while True:
            t0 = asyncio.get_running_loop().time()
            try:
                await self._sink.push(batch)
            except RetryableSinkError:
                self._metrics.loki_push.labels(outcome="retried").inc()
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
            except PermanentSinkError:
                # Poison batch: drop it, count the gap, and advance past it.
                self._metrics.loki_push.labels(outcome="dropped").inc()
                await self._commit(batch)
                return
            else:
                self._metrics.loki_push.labels(outcome="success").inc()
                self._metrics.loki_push_duration.observe(asyncio.get_running_loop().time() - t0)
                await self._commit(batch)
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


def _parse_watermark_seconds(value: str) -> float:
    """Best-effort parse of a checkpoint value into a Unix timestamp.

    This feeds an observability gauge only — an unparseable value falls back
    to "now" rather than raising, since it must never break checkpoint commit.
    """
    try:
        return datetime.fromisoformat(value.replace("Z", "+00:00")).timestamp()
    except ValueError:
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
    if cfg.state.store == "configmap":
        return ConfigMapCheckpointStore.from_service_account(
            name=cfg.state.configmap_name,
            namespace=cfg.state.namespace,
        )
    return FileCheckpointStore(cfg.state.file.path)


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
    ) -> None:
        self._cfg = cfg
        self._pipeline = pipeline
        self._tokens = tokens
        self._metrics = metrics
        self._health = health
        self._closers = list(closers)

    @classmethod
    def build(cls, cfg: Config) -> App:
        """Composition root — construct every wired-up implementation (no network)."""
        configure_logging(cfg.service.log_level, cfg.service.log_format)
        metrics = Metrics()
        health = Health()

        sf_http = httpx.AsyncClient()
        tokens = TokenProvider(cfg.salesforce, sf_http, metrics=metrics)

        loki_http = httpx.AsyncClient()
        sink = LokiSink(cfg.sink.loki, loki_http, metrics=metrics)

        state = _build_state(cfg)
        sm_fields = cfg.sink.loki.structured_metadata_fields

        sources: list[Source] = []
        closers: list[Callable[[], Awaitable[None]]] = []

        if cfg.sources.pubsub.enabled:
            from sf2loki.salesforce.pubsub_client import PubSubClient

            pubsub_client = PubSubClient(cfg.sources.pubsub, tokens, metrics=metrics)
            sources.append(
                PubSubSource(
                    cfg.sources.pubsub, pubsub_client, sm_fields=sm_fields, metrics=metrics
                )
            )
            closers.append(pubsub_client.aclose)

        if cfg.sources.eventlog_objects.enabled:
            from sf2loki.salesforce.soql_client import SoqlClient

            soql = SoqlClient(cfg.salesforce, tokens, sf_http)
            sources.append(
                EventLogObjectsSource(cfg.sources.eventlog_objects, soql, sm_fields=sm_fields)
            )

        if cfg.sources.eventlogfile.enabled:
            sources.append(EventLogFileSource(cfg.sources.eventlogfile))

        pipeline = Pipeline(
            sources=sources,
            sink=sink,
            state=state,
            batch=cfg.sink.loki.batch,
            metrics=metrics,
        )

        closers.extend([sink.aclose, sf_http.aclose, loki_http.aclose])
        return cls(
            cfg=cfg,
            pipeline=pipeline,
            tokens=tokens,
            metrics=metrics,
            health=health,
            closers=closers,
        )

    async def run(self) -> None:
        """Install signal handlers, run the pipeline under the coordinator, shut down."""
        stop = asyncio.Event()
        loop = asyncio.get_running_loop()
        for sig in (signal.SIGTERM, signal.SIGINT):
            with contextlib.suppress(NotImplementedError):
                loop.add_signal_handler(sig, stop.set)

        metrics_server = start_metrics_server(
            self._cfg.service.metrics_addr, self._metrics.registry
        )
        await self._health.start(self._cfg.service.health_addr)

        # Resolve the org id once and assemble deployment-wide labels.
        org_id = self._cfg.salesforce.org_id or await self._tokens.org_id()
        self._pipeline.set_static_labels(
            {"job": "sf2loki", "sf_org_id": org_id, **self._cfg.sink.loki.labels}
        )

        coordinator = NoopCoordinator()
        grace = self._cfg.service.shutdown_grace.total_seconds()

        async def on_acquire() -> None:
            self._health.set_ready()
            await _drain_with_grace(self._pipeline.run(stop), stop, grace)

        async def on_lose() -> None:
            self._health.set_not_ready()

        try:
            await coordinator.run(on_acquire=on_acquire, on_lose=on_lose, stop=stop)
        finally:
            with contextlib.suppress(TimeoutError):
                await asyncio.wait_for(self._shutdown(), timeout=_CLOSE_TIMEOUT)
            self._health.set_not_ready()
            await self._health.stop()
            metrics_server[0].shutdown()

    async def _shutdown(self) -> None:
        for close in self._closers:
            with contextlib.suppress(Exception):
                await close()
