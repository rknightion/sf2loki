"""The Sink seam: where batches go. Loki now; OTLP could be added behind this.

The sink performs its own bounded internal retries and only raises when it
cannot make progress: ``RetryableSinkError`` (the Pipeline should back off and
retry the same batch) or ``PermanentSinkError`` (drop the batch, count a gap,
and advance the checkpoint past it — never stall on a poison batch).
"""

from __future__ import annotations

from typing import Protocol

from sf2loki.model import Batch


class RetryableSinkError(Exception):
    """Delivery failed but the data is fine (transport/429/5xx, or an auth/config
    error such as 401/403); retry the same batch — never drop it."""


class PermanentSinkError(Exception):
    """Delivery rejected unrecoverably (single-entry 400/413); drop + advance.

    ``reason`` is a low-cardinality tag for the per-entry drop counter
    (``sf2loki_loki_entries_dropped``), e.g. ``"bad_request"`` / ``"oversized_413"``.
    """

    def __init__(self, message: str, *, reason: str = "permanent") -> None:
        super().__init__(message)
        self.reason = reason


class Sink(Protocol):
    async def push(self, batch: Batch) -> None: ...

    async def aclose(self) -> None: ...
