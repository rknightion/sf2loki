"""Background poller that turns Salesforce org limits into gauges.

Polls a limits fetcher on an interval and sets the ``sf2loki_salesforce_limit_*``
gauges (one max/remaining per limit name). The loop survives any fetch error
(counted via ``sf2loki_salesforce_limits_poll_errors``) so a transient API
failure never kills it.
"""

from __future__ import annotations

import asyncio
import contextlib
from typing import TYPE_CHECKING, Protocol

from sf2loki.obs.logging import get_logger

if TYPE_CHECKING:
    from datetime import timedelta

    from sf2loki.obs.metrics import Metrics

log = get_logger(__name__)


class _LimitsFetcher(Protocol):
    async def fetch(self) -> dict[str, dict[str, int]]: ...


class LimitsPoller:
    """Polls org limits and publishes them as gauges until ``stop`` is set."""

    def __init__(
        self,
        client: _LimitsFetcher,
        metrics: Metrics,
        poll_interval: timedelta,
        *,
        poll_once: bool = False,
    ) -> None:
        self._client = client
        self._metrics = metrics
        self._poll_interval = poll_interval
        # poll_once=True runs a single cycle and returns (used in tests).
        self._poll_once = poll_once

    async def run(self, stop: asyncio.Event) -> None:
        while True:
            if stop.is_set():
                return
            try:
                limits = await self._client.fetch()
            except Exception as exc:
                self._metrics.salesforce_limits_poll_errors.inc()
                log.warning("salesforce limits poll failed", error=str(exc))
            else:
                for name, info in limits.items():
                    self._metrics.salesforce_limit_max.labels(limit_name=name).set(info["Max"])
                    self._metrics.salesforce_limit_remaining.labels(limit_name=name).set(
                        info["Remaining"]
                    )

            if self._poll_once:
                return
            with contextlib.suppress(TimeoutError):
                await asyncio.wait_for(stop.wait(), timeout=self._poll_interval.total_seconds())
