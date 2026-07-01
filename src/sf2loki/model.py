"""Vendor-neutral data types shared across sources, sink, and state.

These are the data half of the frozen seams; changing them ripples everywhere.
"""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass, field
from datetime import datetime


@dataclass(frozen=True, slots=True)
class CheckpointToken:
    """Resume cursor for a single source stream.

    ``key`` namespaces the stream (e.g. ``"pubsub:/event/LoginEventStream"`` or
    ``"eventlog_objects:LoginEvent"``); ``value`` is the opaque resume position
    (base64 replay_id for streaming, ISO-8601 EventDate for polling).
    """

    key: str
    value: str


@dataclass(slots=True)
class LogEntry:
    """A single decoded event ready to ship to the sink.

    ``checkpoint_only`` entries carry a checkpoint advance with no log payload
    (e.g. a Pub/Sub keepalive's ``latest_replay_id``). The pipeline never sends
    them to the sink; it only commits their token — after any real entries
    queued ahead of them have been pushed, preserving the commit-after-push
    at-least-once invariant.
    """

    timestamp: datetime
    labels: Mapping[str, str]
    line: str
    structured_metadata: Mapping[str, str]
    checkpoint: CheckpointToken
    checkpoint_only: bool = False


@dataclass(slots=True)
class Batch:
    """A batch of entries flushed to the sink as one push."""

    entries: list[LogEntry] = field(default_factory=list)
