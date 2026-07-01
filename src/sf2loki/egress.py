"""Egress guardrails: sink rate caps (token bucket) + a persisted daily byte budget.

The :class:`EgressGovernor` sits between the pipeline's batch flush and the sink
push. It enforces, in order:

1. A **daily byte budget** (pre-compression bytes pushed per UTC day). The used
   counter persists in the :class:`~sf2loki.state.base.CheckpointStore` so a
   restart resumes the same day's total. When a batch would exceed the budget:

   * ``budget_action="drop"`` — the batch is refused (``admit`` returns False);
     the pipeline drops it (counted, checkpoints still advance) and keeps running.
   * ``budget_action="pause"`` — pushes (and therefore checkpoints) are held until
     the next UTC midnight. Data is delayed, never lost — bounded only by
     Salesforce-side retention. Readiness reports degraded while paused.

2. **Rate caps** — independent token buckets on lines/second and bytes/second.
   Over-rate flushes sleep the shortfall (lossless backpressure that propagates
   upstream through the bounded queue to poll/stream flow control). Both the
   monotonic clock (buckets) and the UTC wall clock (budget day) are injectable
   so the whole thing is testable without real sleeps.
"""

from __future__ import annotations

import asyncio
import json
import time
from collections.abc import Callable
from datetime import UTC, date, datetime, timedelta
from typing import TYPE_CHECKING, Literal

from sf2loki.obs.logging import get_logger

if TYPE_CHECKING:
    from sf2loki.config import EgressConfig
    from sf2loki.obs.metrics import Metrics
    from sf2loki.state.base import CheckpointStore

log = get_logger(__name__)

# State-store key for the persisted daily-budget counter. Namespaced under
# "egress:" so it can never collide with a source checkpoint key (pubsub:/
# eventlogfile:/eventlog_objects:/backfill:).
BUDGET_KEY = "egress:budget"

# Minimum seconds between successive over-budget WARNINGs in drop mode, so a
# sustained over-budget stream logs at a bounded rate instead of per batch.
_DROP_LOG_INTERVAL = 60.0


def _default_utcnow() -> datetime:
    return datetime.now(UTC)


def _seconds_until_next_utc_midnight(now: datetime) -> float:
    """Seconds from *now* (UTC-aware) to the next 00:00:00 UTC."""
    next_day = now.date() + timedelta(days=1)
    midnight = datetime(next_day.year, next_day.month, next_day.day, tzinfo=UTC)
    return max(0.0, (midnight - now).total_seconds())


class _TokenBucket:
    """A standard token bucket: capacity = one second of *rate*, refilled from a clock.

    ``wait_time(amount)`` returns how long to sleep before *amount* tokens are
    available (0 if available now) without mutating balance beyond a refill;
    ``consume(amount)`` deducts them (may go negative for a batch larger than the
    capacity, which then rate-limits proportionally). The required amount is
    capped at capacity so an over-capacity request can never wait forever.
    """

    __slots__ = ("_capacity", "_clock", "_last", "_rate", "_tokens")

    def __init__(self, rate: float, clock: Callable[[], float]) -> None:
        self._rate = rate
        self._capacity = rate  # one second of headroom
        self._tokens = rate
        self._clock = clock
        self._last = clock()

    def _refill(self) -> None:
        now = self._clock()
        if now > self._last:
            self._tokens = min(self._capacity, self._tokens + (now - self._last) * self._rate)
            self._last = now

    def wait_time(self, amount: float) -> float:
        self._refill()
        need = min(amount, self._capacity)
        if self._tokens >= need:
            return 0.0
        return (need - self._tokens) / self._rate

    def consume(self, amount: float) -> None:
        self._refill()
        self._tokens -= amount


class EgressGovernor:
    """Rate-cap + daily-byte-budget gate for sink pushes (see module docstring)."""

    def __init__(
        self,
        cfg: EgressConfig,
        *,
        state: CheckpointStore,
        metrics: Metrics,
        clock: Callable[[], float] = time.monotonic,
        utcnow: Callable[[], datetime] = _default_utcnow,
    ) -> None:
        self._cfg = cfg
        self._state = state
        self._metrics = metrics
        self._clock = clock
        self._utcnow = utcnow
        self._budget: int = cfg.daily_byte_budget
        self._action: Literal["pause", "drop"] = cfg.budget_action

        self._line_bucket = (
            _TokenBucket(cfg.max_lines_per_second, clock) if cfg.max_lines_per_second > 0 else None
        )
        self._byte_bucket = (
            _TokenBucket(cfg.max_bytes_per_second, clock) if cfg.max_bytes_per_second > 0 else None
        )

        # Daily-budget day-scoped state (initialised in start()).
        self._date: date = self._utcnow().date()
        self._used: int = 0
        self._paused: bool = False
        # One-shot latches, re-armed on UTC-day rollover.
        self._warned_80: bool = False
        self._pause_logged: bool = False
        self._last_drop_log: float | None = None

    @property
    def enabled(self) -> bool:
        """True when any control (rate cap or budget) is configured."""
        return (
            self._cfg.max_lines_per_second > 0
            or self._cfg.max_bytes_per_second > 0
            or self._budget > 0
        )

    async def start(self) -> None:
        """Load the persisted day counter; a date mismatch resets to today/0."""
        self._date = self._utcnow().date()
        self._used = 0
        if self._budget > 0:
            raw = await self._state.load(BUDGET_KEY)
            if raw:
                try:
                    data = json.loads(raw)
                    stored_date = date.fromisoformat(str(data["date"]))
                    if stored_date == self._date:
                        self._used = int(data["bytes"])
                except ValueError, KeyError, TypeError:
                    # Corrupt/legacy value: treat as a fresh day rather than crash.
                    self._used = 0
            self._metrics.egress_budget_used_bytes.set(self._used)

    def _rollover_to(self, new_date: date) -> None:
        self._date = new_date
        self._used = 0
        self._warned_80 = False
        self._pause_logged = False
        self._last_drop_log = None
        self._metrics.egress_budget_used_bytes.set(0)

    def _maybe_rollover(self) -> None:
        today = self._utcnow().date()
        if today != self._date:
            self._rollover_to(today)

    async def admit(self, lines: int, bytes_: int, stop: asyncio.Event) -> bool:
        """Gate a batch of *lines*/*bytes_* before it is pushed.

        Returns True when the batch may be pushed (possibly after sleeping for
        rate caps or pausing for a budget day-rollover), or False when it must be
        dropped (drop-mode budget exhaustion). If *stop* fires while waiting, it
        returns True so the pipeline can push its final batch and wind down.
        """
        if self._budget > 0:
            self._maybe_rollover()
            if self._used + bytes_ > self._budget:
                if self._action == "drop":
                    self._log_drop(bytes_)
                    return False
                await self._pause_until_rollover_or_stop(stop)
                if stop.is_set():
                    return True

        await self._throttle(lines, bytes_, stop)
        return True

    def _log_drop(self, bytes_: int) -> None:
        now = self._clock()
        if self._last_drop_log is None or now - self._last_drop_log >= _DROP_LOG_INTERVAL:
            self._last_drop_log = now
            log.warning(
                "daily byte budget exhausted; dropping over-budget batch",
                budget=self._budget,
                used=self._used,
                batch_bytes=bytes_,
            )

    async def _pause_until_rollover_or_stop(self, stop: asyncio.Event) -> None:
        self._paused = True
        self._metrics.egress_paused.set(1)
        if not self._pause_logged:
            self._pause_logged = True
            log.error(
                "daily byte budget exhausted; pausing pushes until next UTC day",
                budget=self._budget,
                used=self._used,
            )
        while not stop.is_set():
            now = self._utcnow()
            timeout = _seconds_until_next_utc_midnight(now)
            try:
                await asyncio.wait_for(stop.wait(), timeout=timeout)
                return  # stop fired during the pause
            except TimeoutError:
                pass
            if self._utcnow().date() != self._date:
                self._rollover_to(self._utcnow().date())
                self._paused = False
                self._metrics.egress_paused.set(0)
                return

    async def _throttle(self, lines: int, bytes_: int, stop: asyncio.Event) -> None:
        if self._line_bucket is None and self._byte_bucket is None:
            return
        while not stop.is_set():
            wait = 0.0
            if self._line_bucket is not None:
                wait = max(wait, self._line_bucket.wait_time(lines))
            if self._byte_bucket is not None:
                wait = max(wait, self._byte_bucket.wait_time(bytes_))
            if wait <= 0:
                if self._line_bucket is not None:
                    self._line_bucket.consume(lines)
                if self._byte_bucket is not None:
                    self._byte_bucket.consume(bytes_)
                return
            try:
                await asyncio.wait_for(stop.wait(), timeout=wait)
                return  # stop fired mid-throttle
            except TimeoutError:
                continue  # tokens should have refilled; re-evaluate

    async def record(self, lines: int, bytes_: int) -> None:
        """Account *bytes_* against the current UTC day and persist the counter."""
        if self._budget <= 0:
            return
        self._maybe_rollover()
        self._used += bytes_
        self._metrics.egress_budget_used_bytes.set(self._used)
        await self._state.commit(
            BUDGET_KEY, json.dumps({"date": self._date.isoformat(), "bytes": self._used})
        )
        if not self._warned_80 and self._used >= 0.8 * self._budget:
            self._warned_80 = True
            log.warning(
                "daily byte budget at 80%",
                budget=self._budget,
                used=self._used,
                pct=round(100 * self._used / self._budget, 1),
            )

    def degraded_reason(self) -> str | None:
        """Readiness reason while paused by the budget (pause mode only), else None."""
        if self._budget <= 0 or self._action != "pause" or not self._paused:
            return None
        next_day = (self._date + timedelta(days=1)).isoformat()
        return f"degraded: daily byte budget exhausted, pushes paused until {next_day}"
