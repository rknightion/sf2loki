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
from collections.abc import AsyncIterator, Callable, Mapping, Sequence
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
from sf2loki.shaping import extract_timestamp_checked, route_fields, should_keep
from sf2loki.sources.overlap import category_of_pubsub
from sf2loki.transforms import compile_rules

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

# Approximate per-entry overhead (dict/label/checkpoint scaffolding) added to
# the raw line length when byte-charging the internal bridge queue (#56).
# Mirrors app.py's pipeline-level _QUEUE_ENTRY_OVERHEAD accounting so the two
# budgets reason about memory the same way; kept as an independent constant
# here since the bridge queue is upstream of and separate from the pipeline's
# own byte budget.
_BRIDGE_ENTRY_OVERHEAD = 64


def _entry_bytes(item: LogEntry | None) -> int:
    """Approximate bytes *item* holds in the bridge queue.

    ``None`` (the sentinel) and checkpoint_only entries (keepalive/dropped/
    sampled-out tokens with an empty line) are free — they must never
    contribute to, or block on, the bridge byte budget.
    """
    if item is None or item.checkpoint_only:
        return 0
    # Memoized (issue #69): the bridge is the FIRST place a streaming entry's
    # line is measured, so caching it here means the downstream pipeline byte
    # accounting, governor, and sink size estimate all reuse this one encode.
    return item.line_nbytes() + _BRIDGE_ENTRY_OVERHEAD


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
    bridge_max_bytes:
        Byte budget for the internal bridge queue (independent of
        *queue_maxsize*'s entry-count bound) — see #56. A topic producer
        blocks once queued bytes reach this budget, until :meth:`events`
        dequeues enough to free headroom. ``0`` (default) disables byte
        accounting entirely (entry-count bound via *queue_maxsize* still
        applies).
    reconnect_backoff:
        Initial reconnect delay in seconds (doubles on each attempt up to
        *max_backoff*).
    max_backoff:
        Upper bound for reconnect backoff (seconds).
    owned_categories:
        Canonical event categories (see :mod:`sf2loki.sources.overlap`) owned by
        *other* enabled sources.  Wildcard-DISCOVERED topics in these categories
        are dropped at discovery time (startup and periodic) so ``topics: ["*"]``
        can't double-ingest a category the startup guard never saw.  EXPLICITLY
        configured topics are never filtered — the startup guard already
        validated those and the operator was explicit.
    """

    name: str = "pubsub"

    def __init__(
        self,
        cfg: PubSubConfig,
        client: PubSubClient,
        *,
        sm_fields: Sequence[str],
        queue_maxsize: int = 1000,
        bridge_max_bytes: int = 0,
        reconnect_backoff: float = 1.0,
        max_backoff: float = 30.0,
        metrics: Metrics | None = None,
        topic_discoverer: _StreamDiscovererLike | None = None,
        owned_categories: frozenset[str] = frozenset(),
        transform_salt: str = "",
    ) -> None:
        self._cfg = cfg
        self._client = client
        self._sm_fields = list(sm_fields)
        self._queue_maxsize = queue_maxsize
        self._bridge_max_bytes = bridge_max_bytes
        self._bridge_queued_bytes = 0
        self._bridge_byte_cond = asyncio.Condition()
        self._reconnect_backoff = reconnect_backoff
        self._max_backoff = max_backoff
        self._metrics = metrics if metrics is not None else Metrics()
        self._topic_discoverer = topic_discoverer
        self._owned_categories = owned_categories
        # Precompiled redaction/filter pipeline (empty when no transforms).
        self._transforms = compile_rules(
            cfg.transforms, salt=transform_salt, source=self.name, metrics=self._metrics
        )
        # Per-topic stable timestamp fallback for fieldless events (#42): the
        # most recent field-derived timestamp seen on the topic, so a
        # redelivered fieldless event (e.g. after a reconnect) gets the SAME
        # fallback both times instead of a fresh now() — required for Loki's
        # byte-identical dedup to collapse the at-least-once duplicate.
        self._last_good_ts: dict[str, datetime] = {}

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

    def _filter_owned(self, discovered: Sequence[str]) -> list[str]:
        """Drop DISCOVERED topics whose category another enabled source owns.

        Applies only to wildcard-discovered topics (callers must exclude the
        explicit list first); each skip is logged at INFO with the owning
        reason so the operator can see why a stream isn't being ingested.
        """
        if not self._owned_categories:
            return list(discovered)
        kept: list[str] = []
        for topic in discovered:
            category = category_of_pubsub(topic)
            if category in self._owned_categories:
                log.info(
                    "pubsub discovered topic skipped: its event category is already "
                    "ingested by another configured source (overlap guard)",
                    topic=topic,
                    category=category,
                )
                continue
            kept.append(topic)
        return kept

    def _discovered_additions(
        self, discovered: Sequence[str], have: set[str] | Sequence[str]
    ) -> list[str]:
        """New subscribable topics from a discovery pass.

        Applies include/exclude globs, drops topics already in *have* (explicit
        or already subscribed), then the owned-category filter (#15).
        """
        new = [t for t in self._filter(discovered) if t not in have]
        return self._filter_owned(new)

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
            topics += self._discovered_additions(discovered, set(topics))
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
                        "pubsub stream discovery failed; wildcard topics DROPPED for this pass "
                        "(explicit topics unaffected) — retried at the next periodic "
                        "re-discovery, or on restart when re-discovery is disabled",
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

        Each producer task puts exactly one ``None`` sentinel on the queue when
        it finishes (normal or error), enabling deterministic termination.  The
        sentinel accounting is *dynamic*: when wildcard re-discovery is enabled
        (``"*"`` in topics, discoverer wired, ``rediscovery_interval > 0``) a
        background task re-runs discovery periodically and spawns a producer
        for each newly appeared topic mid-run, incrementing the count — so the
        drain loop terminates only once every producer (including late-spawned
        ones and the re-discovery task itself) has sentinelled.
        """
        topics = await self._resolve_topics()
        rediscovery_interval = (
            self._cfg.rediscovery_interval.total_seconds()
            if TOPIC_WILDCARD in self._cfg.topics and self._topic_discoverer is not None
            else 0.0
        )
        if not topics and rediscovery_interval <= 0:
            return

        queue: asyncio.Queue[LogEntry | None] = asyncio.Queue(maxsize=self._queue_maxsize)
        tasks: list[asyncio.Task[None]] = []
        subscribed: set[str] = set(topics)
        sentinels_remaining = 0

        def _spawn_topic(topic: str) -> None:
            nonlocal sentinels_remaining
            sentinels_remaining += 1
            tasks.append(asyncio.create_task(self._run_topic(topic, state, stop, queue)))

        for topic in topics:
            _spawn_topic(topic)

        if rediscovery_interval > 0:
            sentinels_remaining += 1  # the re-discovery task sentinels too
            tasks.append(
                asyncio.create_task(
                    self._rediscover_loop(
                        rediscovery_interval, subscribed, _spawn_topic, stop, queue
                    )
                )
            )

        try:
            while sentinels_remaining > 0:
                item = await queue.get()
                if item is None:
                    sentinels_remaining -= 1
                else:
                    # Item is leaving the bridge queue for the pipeline's own
                    # (separate) byte budget — free its bridge-queue bytes now.
                    await self._bridge_release(item)
                    yield item
        finally:
            # Cancel any tasks still running and await them to avoid leaks.
            for task in tasks:
                task.cancel()
            await asyncio.gather(*tasks, return_exceptions=True)

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    async def _rediscover_loop(
        self,
        interval: float,
        subscribed: set[str],
        spawn_topic: Callable[[str], None],
        stop: asyncio.Event,
        queue: asyncio.Queue[LogEntry | None],
    ) -> None:
        """Periodically re-run wildcard discovery and spawn newly appeared topics (#14).

        A channel enabled in the org after startup (a new ``*EventStream``
        entity, more Event Monitoring streams turned on) would otherwise stay
        invisible until restart.  Additions go through the same glob and
        owned-category filters as startup discovery; removals are ignored (a
        deleted channel's stream just ends).  Stop-aware, and a discovery
        failure only logs — it must never kill the source.
        """
        explicit = set(self._filter(self._cfg.topics))
        try:
            while not stop.is_set():
                if await self._sleep_or_stop(stop, interval):
                    return
                try:
                    discovered = await self._discover_with_retry()
                    if not discovered:
                        continue  # persistent failure already logged at ERROR
                    additions = self._discovered_additions(discovered, subscribed)
                    removed = subscribed - explicit - set(self._filter(discovered))
                    if removed:
                        log.debug(
                            "pubsub rediscovery: previously discovered topics no longer "
                            "listed; their streams will simply end",
                            topics=sorted(removed),
                        )
                    for topic in additions:
                        if stop.is_set():
                            return
                        log.info(
                            "pubsub rediscovery: subscribing to newly discovered topic",
                            topic=topic,
                        )
                        subscribed.add(topic)
                        spawn_topic(topic)
                except Exception as exc:  # never kill the source from here
                    log.warning(
                        "pubsub rediscovery pass failed; retrying at next interval",
                        error=repr(exc),
                    )
        finally:
            await self._put_sentinel(queue)

    async def _bridge_charge(self, item: LogEntry | None) -> None:
        """Producer side: wait for bridge byte-budget headroom, then account *item* (#56).

        Mirrors app.py's pipeline-level ``_charge``: admits while the queue is
        *under* budget (an admitted item may overshoot it), so a single item
        larger than the whole budget is still admitted once the queue drains
        rather than deadlocking. A no-op when *bridge_max_bytes* is 0
        (disabled) or *item* is free (sentinel / checkpoint_only).
        """
        if self._bridge_max_bytes <= 0:
            return
        cost = _entry_bytes(item)
        if cost == 0:
            return
        async with self._bridge_byte_cond:
            await self._bridge_byte_cond.wait_for(
                lambda: self._bridge_queued_bytes < self._bridge_max_bytes
            )
            self._bridge_queued_bytes += cost

    async def _bridge_release(self, item: LogEntry | None) -> None:
        """Consumer side: return *item*'s bridge-queue bytes and wake waiting producers."""
        if self._bridge_max_bytes <= 0:
            return
        cost = _entry_bytes(item)
        if cost == 0:
            return
        async with self._bridge_byte_cond:
            self._bridge_queued_bytes -= cost
            self._bridge_byte_cond.notify_all()

    async def _enqueue(self, queue: asyncio.Queue[LogEntry | None], item: LogEntry) -> None:
        """Put *item* on the bridge queue, respecting both bounds: entry-count
        (``queue_maxsize``, via the queue's own maxsize) and bytes
        (``bridge_max_bytes``, via :meth:`_bridge_charge`, #56)."""
        await self._bridge_charge(item)
        await queue.put(item)

    @staticmethod
    async def _put_sentinel(queue: asyncio.Queue[LogEntry | None]) -> None:
        """Always put a sentinel so the drain loop can terminate.

        put_nowait first: on force-cancel, an awaited put on a full queue that
        nobody drains would hang forever.
        """
        try:
            queue.put_nowait(None)
        except asyncio.QueueFull:
            task = asyncio.current_task()
            if task is None or not task.cancelling():
                # Not being cancelled → the consumer is still draining, so a
                # brief awaited put keeps termination deterministic.
                await queue.put(None)

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

        The initial replay position is loaded from *state* INSIDE the
        reconnect loop below (not before it), so a transient checkpoint-store
        error backs off and retries like any other stream error instead of
        killing this task without its sentinel (#45) — a missing sentinel
        wedges :meth:`events`'s drain loop forever; force-cancel on shutdown
        then looks like a clean completion and the process exits 0 under
        docker ``restart: on-failure``. A CORRUPT stored value (failed base64
        decode) is never retried — the same bytes always fail the same way —
        it instead falls back to the configured ``replay_preset`` immediately,
        with an ERROR log and the ``checkpoint_load_errors`` metric.
        """
        backoff = self._reconnect_backoff
        attempt = 0
        stream_up = self._metrics.pubsub_stream_up.labels(topic=topic)
        replay_id = b""
        preset: int = preset_for("LATEST")
        position_loaded = False

        try:
            while not stop.is_set():
                if not position_loaded:
                    try:
                        stored = await state.load(f"pubsub:{topic}")
                    except Exception as exc:
                        log.warning(
                            "pubsub checkpoint load failed; retrying",
                            topic=topic,
                            error=repr(exc),
                            backoff=backoff,
                        )
                        if await self._sleep_or_stop(stop, backoff):
                            return
                        backoff = min(backoff * 2, self._max_backoff)
                        continue
                    if stored:
                        try:
                            replay_id = base64.b64decode(stored)
                            preset = preset_for("CUSTOM")
                        except Exception as exc:
                            log.error(
                                "pubsub stored replay id corrupt; falling back to the "
                                "configured replay_preset",
                                topic=topic,
                                error=repr(exc),
                            )
                            self._metrics.checkpoint_load_errors.labels(source=self.name).inc()
                            replay_id = b""
                            preset = (
                                preset_for("LATEST")
                                if self._cfg.replay_preset == "CUSTOM"
                                else preset_for(self._cfg.replay_preset)
                            )
                    else:
                        # No stored id: CUSTOM with nothing to resume from
                        # falls back to LATEST; otherwise use the configured
                        # preset directly.
                        preset = (
                            preset_for("LATEST")
                            if self._cfg.replay_preset == "CUSTOM"
                            else preset_for(self._cfg.replay_preset)
                        )
                        replay_id = b""
                    position_loaded = True
                    log.info(
                        "pubsub subscribing", topic=topic, preset=preset, resuming=bool(replay_id)
                    )

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
                            stream_up.set(1)
                        if isinstance(ev, KeepaliveEvent):
                            if ev.latest_replay_id == replay_id:
                                continue  # unchanged — avoid checkpoint churn
                            replay_id = ev.latest_replay_id
                            preset = preset_for("CUSTOM")
                            await self._enqueue(
                                queue, self._keepalive_entry(topic, ev.latest_replay_id)
                            )
                            continue
                        entry = self._shape_event(topic, ev)
                        # Update resume position for reconnect (even for a
                        # dropped/sampled-out event — its checkpoint still commits).
                        replay_id = ev.replay_id
                        preset = preset_for("CUSTOM")
                        await self._enqueue(queue, entry)  # blocks when full = backpressure

                    # Stream ended normally.
                    stream_up.set(0)
                    if stop.is_set():
                        return
                    backoff = self._reconnect_backoff  # reset on clean close
                    # Brief pause before reconnecting.
                    if await self._sleep_or_stop(stop, backoff):
                        return
                except Exception as exc:
                    stream_up.set(0)
                    if stop.is_set():
                        return
                    if self._is_invalid_argument(exc) and preset == preset_for("CUSTOM"):
                        # narrowed by _is_invalid_argument's isinstance check
                        assert isinstance(exc, grpc.aio.AioRpcError)
                        trailer_confirms = self._trailer_confirms_replay_corruption(exc)
                        if trailer_confirms is False:
                            # A trailer IS present but does not identify
                            # replay-id corruption/expiry — some OTHER
                            # INVALID_ARGUMENT while resuming (e.g. a
                            # malformed subscribe argument after a client/API
                            # change). Re-draining from EARLIEST here would be
                            # a large, silent, misattributed re-ingest.
                            # Retry normally, keeping the replay position.
                            log.error(
                                "pubsub subscribe rejected with INVALID_ARGUMENT while "
                                "resuming, but the error trailer does not confirm replay-id "
                                "corruption/expiry; retrying with backoff and keeping the "
                                "replay position (NOT falling back to EARLIEST)",
                                topic=topic,
                                error=repr(exc),
                                backoff=backoff,
                            )
                        else:
                            # Trailer confirms corruption, OR no trailer was
                            # present at all (fall back to the legacy
                            # code-only heuristic). Salesforce rejects an
                            # expired (outside the 72h retention window) or
                            # corrupt replay id with INVALID_ARGUMENT.
                            # Retrying with the same dead id would loop
                            # forever, a permanent silent outage. Discard it
                            # and restart from EARLIEST: bounded (≤72h)
                            # duplicates that Loki's byte-identical dedup
                            # collapses. Never LATEST — that guarantees loss.
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
                    elif self._is_invalid_argument(exc):
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
            stream_up.set(0)  # task exiting: this topic is definitionally down
            await self._put_sentinel(queue)

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

        Bare status-code heuristic only — does NOT confirm replay-id
        corruption/expiry specifically (see :meth:`_trailer_confirms_replay_corruption`
        for that); used as a fallback when no error-info trailer is present (#65).
        """
        return (
            isinstance(exc, grpc.aio.AioRpcError) and exc.code() == grpc.StatusCode.INVALID_ARGUMENT
        )

    # Salesforce's error-info trailer identifies a genuinely corrupt/expired
    # replay id with an error code in the ``...replayid.corrupted`` family.
    _REPLAY_CORRUPTION_MARKER = "replayid.corrupted"

    @classmethod
    def _trailer_confirms_replay_corruption(cls, exc: grpc.aio.AioRpcError) -> bool | None:
        """Inspect *exc*'s gRPC trailer for Salesforce's replay-id
        corruption/expiry error-info (#65).

        Returns ``True`` when a trailer entry confirms it, ``False`` when a
        trailer is present but does NOT mention it (a different, unrelated
        INVALID_ARGUMENT — e.g. a malformed subscribe argument after a
        client-library or Salesforce API change), and ``None`` when no
        trailer metadata is available at all — callers fall back to the
        bare-status-code heuristic (:meth:`_is_invalid_argument`) in that case.
        """
        trailer = exc.trailing_metadata()
        if not trailer:
            return None
        for _key, value in trailer:
            text = value if isinstance(value, str) else value.decode("utf-8", "replace")
            if cls._REPLAY_CORRUPTION_MARKER in text.lower():
                return True
        return False

    @staticmethod
    def _event_type(topic: str) -> str:
        """The event-type label / metric dimension: the topic's trailing segment."""
        return topic.rstrip("/").rsplit("/", 1)[-1]

    def _sample_rate(self, topic: str) -> float:
        """Keep rate for *topic*: the first ``cfg.sample`` glob that matches, else 1.0."""
        for pattern, rate in self._cfg.sample.items():
            if fnmatch.fnmatchcase(topic, pattern):
                return rate
        return 1.0

    def _shape_event(self, topic: str, ev: DecodedEvent) -> LogEntry:
        """Apply transforms then sampling, returning a full or checkpoint-only entry.

        Transforms mutate ``ev.payload`` in place. A ``drop_row`` match or a
        deterministic sampling drop yields a checkpoint_only entry (built exactly
        like a keepalive) so the event's replay id STILL commits — dropping it
        without committing would replay it forever.
        """
        if self._transforms.apply(ev.payload) is None:
            # drop_row matched (rows_filtered already counted in the pipeline).
            return self._keepalive_entry(topic, ev.replay_id)
        if not should_keep(ev.replay_id.hex(), self._sample_rate(topic)):
            self._metrics.entries_sampled_out.labels(
                source=self.name, event_type=self._event_type(topic)
            ).inc()
            return self._keepalive_entry(topic, ev.replay_id)
        return self._to_log_entry(topic, ev)

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

    def _event_timestamp(self, topic: str, payload: Mapping[str, object]) -> datetime:
        """Best event time for a Pub/Sub payload, preferring stable event-time fields.

        Order: ``EventDate`` (RTEM streams) → ``CreatedDate`` (platform events) →
        ``ChangeEventHeader.commitTimestamp`` (CDC change events, epoch millis) →
        a stable per-topic fallback (#42). A fieldless event (custom platform
        event or CDC channel with none of the above) previously got
        ``now()`` fresh on every delivery — a replayed duplicate then got a
        DIFFERENT timestamp each time, defeating Loki's byte-identical dedup
        and firing no metric. Instead, fall back to the most recent
        field-derived timestamp seen on this topic (``_last_good_ts``,
        clamped near now if stale by :func:`extract_timestamp_checked`) so a
        redelivered fieldless event gets the SAME timestamp every time, and
        count the fallback.
        """
        header = payload.get("ChangeEventHeader")
        commit_ts = header.get("commitTimestamp") if isinstance(header, Mapping) else None
        ts_source = {
            "EventDate": payload.get("EventDate"),
            "CreatedDate": payload.get("CreatedDate"),
            "commitTimestamp": commit_ts,
        }
        ts, used_fallback = extract_timestamp_checked(
            ts_source,
            field_names=("EventDate", "CreatedDate", "commitTimestamp"),
            fallback=self._last_good_ts.get(topic),
        )
        if used_fallback:
            self._metrics.timestamp_fallbacks.labels(source=self.name).inc()
        else:
            self._last_good_ts[topic] = ts
        return ts

    def _to_log_entry(self, topic: str, ev: DecodedEvent) -> LogEntry:
        """Convert a DecodedEvent to a LogEntry ready for the pipeline."""
        event_type = self._event_type(topic)
        labels: dict[str, str] = {"source": "pubsub", "event_type": event_type}
        line, sm = route_fields(ev.payload, self._sm_fields)
        timestamp = self._event_timestamp(topic, ev.payload)
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
