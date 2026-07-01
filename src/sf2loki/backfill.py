"""`sf2loki backfill`: one-shot historical EventLogFile backfill into Loki.

Separate from the daemon: its own state file (the daemon holds an exclusive
flock on the main one) with a ``backfill:`` checkpoint namespace, so it is
resumable and safe to run while the service is up. Solves Loki's out-of-order
window explicitly — either distinct ``backfill="true"`` streams (default) or
ingest-time timestamps with the true event time in structured metadata.
"""

from __future__ import annotations

from datetime import UTC, date, datetime

from sf2loki.config import Config


def parse_backfill_date(value: str) -> datetime:
    """Parse a YYYY-MM-DD CLI argument into an aware UTC midnight datetime."""
    try:
        d = date.fromisoformat(value)
    except ValueError:
        raise ValueError(f"invalid date {value!r} (expected YYYY-MM-DD)") from None
    return datetime(d.year, d.month, d.day, tzinfo=UTC)


async def run_backfill(
    cfg: Config,
    *,
    since: datetime,
    until: datetime | None,
    event_types: list[str] | None,
    interval: str,
    ingest_timestamps: bool,
    concurrency: int,
) -> int:
    """Backfill ELF history for [since, until) into Loki; return an exit code."""
    raise NotImplementedError("implemented in the backfill lane")
