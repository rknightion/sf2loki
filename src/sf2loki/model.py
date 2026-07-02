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
    # Memoized UTF-8 byte length of ``line`` (issue #69 item 2); -1 = not yet
    # computed. Excluded from equality/repr so two otherwise-equal entries stay
    # equal regardless of whether the length has been cached. Reset to -1 by any
    # code that mutates ``line`` (the sink's line cap) so the memo can't go stale.
    _line_nbytes: int = field(default=-1, compare=False, repr=False)

    def line_nbytes(self) -> int:
        """UTF-8 byte length of ``line``, computed once and memoized.

        The line is finalized by the source's shaping before the entry enters
        the pipeline, where its byte length is needed up to ~4x purely for byte
        accounting (queue byte budget on charge + release, consumer batch
        accounting, governor admission, sink pre-encode size estimate) before the
        one unavoidable wire encode. Encoding once removes those redundant UTF-8
        passes on the hot path. Any code that reassigns ``line`` MUST reset
        ``_line_nbytes`` to -1 (see the Loki sink's line cap).
        """
        if self._line_nbytes < 0:
            self._line_nbytes = len(self.line.encode("utf-8"))
        return self._line_nbytes


@dataclass(slots=True)
class Batch:
    """A batch of entries flushed to the sink as one push."""

    entries: list[LogEntry] = field(default_factory=list)
