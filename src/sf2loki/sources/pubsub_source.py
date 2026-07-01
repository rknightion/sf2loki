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
from collections.abc import AsyncIterator, Mapping, Sequence
from datetime import datetime
from typing import TYPE_CHECKING

from sf2loki.config import PubSubConfig
from sf2loki.model import CheckpointToken, LogEntry
from sf2loki.obs.logging import get_logger
from sf2loki.obs.metrics import Metrics
from sf2loki.salesforce.pubsub_client import DecodedEvent, PubSubClient, preset_for
from sf2loki.shaping import extract_timestamp, route_fields

if TYPE_CHECKING:
    from sf2loki.state.base import CheckpointStore

log = get_logger(__name__)


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
    ) -> None:
        self._cfg = cfg
        self._client = client
        self._sm_fields = list(sm_fields)
        self._queue_maxsize = queue_maxsize
        self._reconnect_backoff = reconnect_backoff
        self._max_backoff = max_backoff
        self._metrics = metrics if metrics is not None else Metrics()

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def resolve_topics(self) -> list[str]:
        """Return the de-duped, filtered list of topics to subscribe to.

        Applies include/exclude glob filters (fnmatch.fnmatchcase).  A topic
        is kept only if it matches at least one include glob AND matches no
        exclude glob.  Order is preserved.
        """
        seen: set[str] = set()
        result: list[str] = []
        for topic in self._cfg.topics:
            if topic in seen:
                continue
            seen.add(topic)
            # Must match at least one include pattern
            if not any(fnmatch.fnmatchcase(topic, pat) for pat in self._cfg.include):
                continue
            # Must not match any exclude pattern
            if any(fnmatch.fnmatchcase(topic, pat) for pat in self._cfg.exclude):
                continue
            result.append(topic)
        return result

    async def events(self, state: CheckpointStore, stop: asyncio.Event) -> AsyncIterator[LogEntry]:
        """Yield decoded log entries from all resolved topics.

        Spawns one asyncio Task per topic.  Each task streams events into a
        bounded queue; the main loop drains the queue and yields entries.
        Backpressure is structural: a full queue stalls the producer task,
        which stalls the gRPC receive loop, which stops Salesforce sending.

        Each task puts exactly one ``None`` sentinel on the queue when it
        finishes (normal or error), enabling deterministic termination.
        """
        topics = self.resolve_topics()
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

        Implements exponential-backoff reconnect: on a clean stream end, resets
        the backoff and reconnects after a short delay.  On an exception, backs
        off then reconnects.  In both cases, resumes from the last seen replay_id.
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
                try:
                    async for ev in self._client.subscribe(
                        topic,
                        replay_preset=preset,
                        replay_id=replay_id,
                        num_requested=self._cfg.default_num_requested,
                    ):
                        if stop.is_set():
                            return
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
                    try:
                        await asyncio.wait_for(stop.wait(), timeout=backoff)
                        return  # stop was set during the sleep
                    except TimeoutError:
                        pass

                except Exception as exc:
                    if stop.is_set():
                        return
                    log.warning(
                        "pubsub stream error", topic=topic, error=repr(exc), backoff=backoff
                    )
                    # Transient error: back off then reconnect.
                    try:
                        await asyncio.wait_for(stop.wait(), timeout=backoff)
                        return  # stop was set during backoff
                    except TimeoutError:
                        pass
                    backoff = min(backoff * 2, self._max_backoff)
        finally:
            # Always put a sentinel so the drain loop can terminate.
            await queue.put(None)

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
