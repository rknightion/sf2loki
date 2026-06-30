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
    """Delivery failed transiently (transport/429/5xx); retry the same batch."""


class PermanentSinkError(Exception):
    """Delivery rejected unrecoverably (400 / unsplittable 413); drop + advance."""


class Sink(Protocol):
    async def push(self, batch: Batch) -> None: ...

    async def aclose(self) -> None: ...
