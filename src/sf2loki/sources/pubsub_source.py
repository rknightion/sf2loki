"""PubSubSource: async streaming source for the Salesforce Pub/Sub API.

Consumes one or more Pub/Sub topics concurrently, decodes events via
PubSubClient, applies shaping, and yields LogEntry objects.  Each yielded
entry carries a CheckpointToken so the pipeline can persist resume state
durably once the entry is pushed to Loki.
"""

from __future__ import annotations

import asyncio
import base64
import fnmatch
import random
from collections.abc import AsyncIterator, Mapping, Sequence
from datetime import UTC, datetime
from typing import TYPE_CHECKING, Protocol

import grpc
import grpc.aio

from sf2loki.config import PubSubConfig
from sf2loki.model import CheckpointToken, LogEntry
from sf2loki.obs.logging import get_logger
from sf2loki.obs.metrics import Metrics
from sf2loki.salesforce.pubsub_client import (
    DecodedEvent,
    KeepaliveEvent,
    PubSubClient,
    preset_for,
)
from sf2loki.shaping import extract_timestamp, route_fields

if TYPE_CHECKING:
    from sf2loki.state.base import CheckpointStore

log = get_logger(__name__)

# Topic wildcard: discover and subscribe to every RTEM streaming channel the org
# exposes (still subject to the include/exclude globs).
TOPIC_WILDCARD = "*"

# Attempts made to discover wildcard topics before falling back to the explicit
# topic list (with an ERROR log — a transient discovery failure must not
# silently shrink the subscription set for the process lifetime).
_DISCOVERY_ATTEMPTS = 3


def _jitter(backoff: float) -> float:
    """Uniform jitter added to reconnect sleeps.

    Without it every topic reconnects in lockstep after an org-wide failure
    (e.g. token expiry), hammering Salesforce with simultaneous subscribes.
    """
    return random.uniform(0.0, backoff / 2)


class _StreamDiscovererLike(Protocol):
    """Structural seam for RTEM stream discovery (satisfied by MetadataClient)."""

    async def list_event_stream_topics(self) -> list[str]: ...


class PubSubSource:
    """Streaming source that subscribes to Salesforce Pub/Sub topics.

    Parameters
    ----------
    cfg:
        Pub/Sub configuration (topics, include/exclude globs, replay preset, etc.).
    client:
        PubSubClient (or duck-typed fake) used to call the Subscribe RPC.
    sm_fields:
        List of payload field names to promote to structured metadata instead
        of embedding in the JSON log line.
    queue_maxsize:
        Bound for the internal queue.  When full, producers block — this is
        deliberate backpressure: the gRPC stream stalls and Salesforce stops
        sending new events.
    reconnect_backoff:
        Initial reconnect delay in seconds (doubles on each attempt up to
        *max_backoff*).
    max_backoff:
        Upper bound for reconnect backoff (seconds).
    """

    name: str = "pubsub"

    def __init__(
        self,
        cfg: PubSubConfig,
        client: PubSubClient,
        *,
        sm_fields: Sequence[str],
        queue_maxsize: int = 1000,
        reconnect_backoff: float = 1.0,
        max_backoff: float = 30.0,
        metrics: Metrics | None = None,
        topic_discoverer: _StreamDiscovererLike | None = None,
    ) -> None:
        self._cfg = cfg
        self._client = client
        self._sm_fields = list(sm_fields)
        self._queue_maxsize = queue_maxsize
        self._reconnect_backoff = reconnect_backoff
        self._max_backoff = max_backoff
        self._metrics = metrics if metrics is not None else Metrics()
        self._topic_discoverer = topic_discoverer

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def _filter(self, topics: Sequence[str]) -> list[str]:
        """De-dupe *topics* and apply include/exclude globs, preserving order.

        A topic is kept only if it matches at least one include glob AND matches
        no exclude glob. The ``"*"`` discovery marker is never a literal topic.
        """
        seen: set[str] = set()
        result: list[str] = []
        for topic in topics:
            if topic == TOPIC_WILDCARD or topic in seen:
                continue
            seen.add(topic)
            if not any(fnmatch.fnmatchcase(topic, pat) for pat in self._cfg.include):
                continue
            if any(fnmatch.fnmatchcase(topic, pat) for pat in self._cfg.exclude):
                continue
            result.append(topic)
        return result

    def resolve_topics(self) -> list[str]:
        """The explicitly-configured topics (filtered). Excludes the ``"*"`` marker.

        Used at startup for the overlap guard; discovered topics (from ``"*"``)
        are resolved later in :meth:`events`, so they aren't reflected here.
        """
        return self._filter(self._cfg.topics)

    async def _resolve_topics(self) -> list[str]:
        """Topics to subscribe to this run, expanding the ``"*"`` wildcard.

        When ``"*"`` is present and a discoverer is wired, discover every RTEM
        stream (with retries — see :meth:`_discover_with_retry`) and merge it
        with any explicit topics. A persistent discovery failure is non-fatal —
        fall back to the explicit topics.
        """
        topics = list(self._cfg.topics)
        if TOPIC_WILDCARD in topics and self._topic_discoverer is not None:
            discovered = await self._discover_with_retry()
            topics += [t for t in discovered if t not in topics]
        return self._filter(topics)

    async def _discover_with_retry(self) -> list[str]:
        """Discover RTEM stream topics, retrying transient failures with backoff.

        Discovery runs once per :meth:`events` call, so a single transient
        failure would otherwise silently shrink the subscription set for the
        process lifetime. Retries ``_DISCOVERY_ATTEMPTS`` times; on final
        failure logs at ERROR and returns [] (explicit topics still subscribe).
        """
        assert self._topic_discoverer is not None
        delay = self._reconnect_backoff
        for attempt in range(1, _DISCOVERY_ATTEMPTS + 1):
            try:
                return await self._topic_discoverer.list_event_stream_topics()
            except Exception as exc:
                if attempt == _DISCOVERY_ATTEMPTS:
                    log.error(
                        "pubsub stream discovery failed; wildcard topics DROPPED for this run "
                        "(explicit topics unaffected) — restart to retry",
                        attempts=attempt,
                        error=repr(exc),
                    )
                    return []
                log.warning(
                    "pubsub stream discovery failed; retrying",
                    attempt=attempt,
                    error=repr(exc),
                    delay=delay,
                )
                await asyncio.sleep(delay + _jitter(delay))
                delay = min(delay * 2, self._max_backoff)
        return []  # unreachable; keeps the type checker happy

    async def events(self, state: CheckpointStore, stop: asyncio.Event) -> AsyncIterator[LogEntry]:
        """Yield decoded log entries from all resolved topics.

        Spawns one asyncio Task per topic.  Each task streams events into a
        bounded queue; the main loop drains the queue and yields entries.
        Backpressure is structural: a full queue stalls the producer task,
        which stalls the gRPC receive loop, which stops Salesforce sending.

        Each task puts exactly one ``None`` sentinel on the queue when it
        finishes (normal or error), enabling deterministic termination.
        """
        topics = await self._resolve_topics()
        if not topics:
            return

        queue: asyncio.Queue[LogEntry | None] = asyncio.Queue(maxsize=self._queue_maxsize)

        tasks = [
            asyncio.create_task(self._run_topic(topic, state, stop, queue)) for topic in topics
        ]

        sentinels_remaining = len(tasks)
        try:
            while sentinels_remaining > 0:
                item = await queue.get()
                if item is None:
                    sentinels_remaining -= 1
                else:
                    yield item
        finally:
            # Cancel any tasks still running and await them to avoid leaks.
            for task in tasks:
                task.cancel()
            await asyncio.gather(*tasks, return_exceptions=True)

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    async def _run_topic(
        self,
        topic: str,
        state: CheckpointStore,
        stop: asyncio.Event,
        queue: asyncio.Queue[LogEntry | None],
    ) -> None:
        """Manage the subscribe loop for a single topic.

        Implements exponential-backoff reconnect with uniform jitter (so all
        topics don't resubscribe in lockstep after an org-wide failure).
        Backoff resets to base once a connection proves healthy — defined as
        having received at least one event or keepalive on it — or on a clean
        stream end.  Resumes from the last seen replay_id (event or keepalive).
        """
        # Determine initial replay position.
        stored = await state.load(f"pubsub:{topic}")
        if stored:
            replay_id = base64.b64decode(stored)
            preset: int = preset_for("CUSTOM")
        else:
            if self._cfg.replay_preset == "CUSTOM":
                # No stored id and CUSTOM requested → fall back to LATEST.
                preset = preset_for("LATEST")
            else:
                preset = preset_for(self._cfg.replay_preset)
            replay_id = b""

        backoff = self._reconnect_backoff
        attempt = 0

        log.info("pubsub subscribing", topic=topic, preset=preset, resuming=bool(stored))
        try:
            while not stop.is_set():
                if attempt > 0:
                    self._metrics.pubsub_reconnects.labels(topic=topic).inc()
                    log.info("pubsub reconnecting", topic=topic, attempt=attempt, backoff=backoff)
                attempt += 1
                received = False  # any event/keepalive seen on THIS connection
                try:
                    async for ev in self._client.subscribe(
                        topic,
                        replay_preset=preset,
                        replay_id=replay_id,
                        num_requested=self._cfg.default_num_requested,
                    ):
                        if stop.is_set():
                            return
                        if not received:
                            received = True
                            # Connection proved healthy: reset the backoff so
                            # recurring mid-stream failures (e.g. the known
                            # session-expiry churn every few minutes) don't
                            # ratchet every topic to max_backoff permanently.
                            backoff = self._reconnect_backoff
                        if isinstance(ev, KeepaliveEvent):
                            if ev.latest_replay_id == replay_id:
                                continue  # unchanged — avoid checkpoint churn
                            replay_id = ev.latest_replay_id
                            preset = preset_for("CUSTOM")
                            await queue.put(self._keepalive_entry(topic, ev.latest_replay_id))
                            continue
                        entry = self._to_log_entry(topic, ev)
                        # Update resume position for reconnect.
                        replay_id = ev.replay_id
                        preset = preset_for("CUSTOM")
                        await queue.put(entry)  # blocks when full = backpressure

                    # Stream ended normally.
                    if stop.is_set():
                        return
                    backoff = self._reconnect_backoff  # reset on clean close
                    # Brief pause before reconnecting.
                    if await self._sleep_or_stop(stop, backoff):
                        return
                except Exception as exc:
                    if stop.is_set():
                        return
                    if self._is_invalid_argument(exc):
                        if preset == preset_for("CUSTOM"):
                            # Salesforce rejects an expired (outside the 72h
                            # retention window) or corrupt replay id with
                            # INVALID_ARGUMENT — indistinguishably. Retrying
                            # with the same dead id would loop forever, a
                            # permanent silent outage. Discard it and restart
                            # from EARLIEST: bounded (≤72h) duplicates that
                            # Loki's byte-identical dedup collapses. Never
                            # LATEST — that guarantees loss.
                            log.error(
                                "pubsub replay id rejected (expired or corrupt); falling back "
                                "to EARLIEST — possible data gap if the id aged out of "
                                "Salesforce's 72h retention; up to 72h of events may be "
                                "re-delivered",
                                topic=topic,
                                error=repr(exc),
                            )
                            self._metrics.pubsub_replay_fallbacks.labels(topic=topic).inc()
                            replay_id = b""
                            preset = preset_for("EARLIEST")
                        else:
                            # INVALID_ARGUMENT with no replay id in play is a
                            # genuine config error (e.g. bad num_requested).
                            # Keep retrying with backoff, but loudly: a
                            # permanently-red ERROR loop beats a crash and
                            # beats silent WARN-level retries.
                            log.error(
                                "pubsub subscribe rejected with INVALID_ARGUMENT while not "
                                "resuming from a replay id — likely a configuration error; "
                                "retrying with backoff",
                                topic=topic,
                                error=repr(exc),
                                backoff=backoff,
                            )
                    else:
                        log.warning(
                            "pubsub stream error", topic=topic, error=repr(exc), backoff=backoff
                        )
                    # Back off then reconnect.
                    if await self._sleep_or_stop(stop, backoff):
                        return
                    backoff = min(backoff * 2, self._max_backoff)
        finally:
            # Always put a sentinel so the drain loop can terminate. put_nowait
            # first: on force-cancel, an awaited put on a full queue that nobody
            # drains would hang forever.
            try:
                queue.put_nowait(None)
            except asyncio.QueueFull:
                task = asyncio.current_task()
                if task is None or not task.cancelling():
                    # Not being cancelled → the consumer is still draining, so a
                    # brief awaited put keeps termination deterministic.
                    await queue.put(None)

    async def _sleep_or_stop(self, stop: asyncio.Event, delay: float) -> bool:
        """Sleep *delay* plus uniform jitter; return True if *stop* fired meanwhile."""
        try:
            await asyncio.wait_for(stop.wait(), timeout=delay + _jitter(delay))
            return True
        except TimeoutError:
            return False

    @staticmethod
    def _is_invalid_argument(exc: BaseException) -> bool:
        """True when *exc* is a gRPC INVALID_ARGUMENT rejection.

        Salesforce uses this status (error code trailer
        ``...fetch.replayid.corrupted``) for both expired and corrupt replay
        ids — the two are not distinguishable and are handled identically.
        """
        return (
            isinstance(exc, grpc.aio.AioRpcError) and exc.code() == grpc.StatusCode.INVALID_ARGUMENT
        )

    def _keepalive_entry(self, topic: str, latest_replay_id: bytes) -> LogEntry:
        """A checkpoint_only LogEntry carrying a keepalive ``latest_replay_id``.

        Routed through the queue rather than committed directly to the state
        store: pipeline FIFO ordering guarantees every real entry queued ahead
        of it is pushed to Loki before this token commits, preserving the
        commit-after-push at-least-once invariant.
        """
        return LogEntry(
            timestamp=datetime.now(UTC),
            labels={},
            line="",
            structured_metadata={},
            checkpoint=CheckpointToken(
                key=f"pubsub:{topic}",
                value=base64.b64encode(latest_replay_id).decode("ascii"),
            ),
            checkpoint_only=True,
        )

    @staticmethod
    def _event_timestamp(payload: Mapping[str, object]) -> datetime:
        """Best event time for a Pub/Sub payload, preferring stable event-time fields.

        Order: ``EventDate`` (RTEM streams) → ``CreatedDate`` (platform events) →
        ``ChangeEventHeader.commitTimestamp`` (CDC change events, epoch millis) →
        ingest time. Using the CDC commit timestamp (rather than falling back to
        ``now()``) keeps a replayed change event byte-identical, so Loki's native
        dedup collapses at-least-once duplicates instead of storing them twice.
        """
        header = payload.get("ChangeEventHeader")
        commit_ts = header.get("commitTimestamp") if isinstance(header, Mapping) else None
        ts_source = {
            "EventDate": payload.get("EventDate"),
            "CreatedDate": payload.get("CreatedDate"),
            "commitTimestamp": commit_ts,
        }
        return extract_timestamp(
            ts_source, field_names=("EventDate", "CreatedDate", "commitTimestamp")
        )

    def _to_log_entry(self, topic: str, ev: DecodedEvent) -> LogEntry:
        """Convert a DecodedEvent to a LogEntry ready for the pipeline."""
        event_type = topic.rstrip("/").rsplit("/", 1)[-1]
        labels: dict[str, str] = {"source": "pubsub", "event_type": event_type}
        line, sm = route_fields(ev.payload, self._sm_fields)
        timestamp = self._event_timestamp(ev.payload)
        checkpoint = CheckpointToken(
            key=f"pubsub:{topic}",
            value=base64.b64encode(ev.replay_id).decode("ascii"),
        )
        return LogEntry(
            timestamp=timestamp,
            labels=labels,
            line=line,
            structured_metadata=sm,
            checkpoint=checkpoint,
        )
