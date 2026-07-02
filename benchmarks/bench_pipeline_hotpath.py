"""Saturated-pipeline hot-path benchmark for issue #69.

Two measurements:

1. **Encode-accounting microbench** — the exact overhead issue #69 item 2
   targets: on the hot path each entry's line is UTF-8 encoded ~4x purely for
   byte accounting (producer byte-charge, consumer batch accounting, governor
   admission, sink size estimate) before the one unavoidable wire encode. This
   times "encode-every-time" vs "encode-once-and-reuse" over a fixed corpus, so
   the win is visible independent of event-loop noise.

2. **End-to-end pipeline throughput** — drives the real ``Pipeline`` with a
   flooding in-memory source and a no-op sink (byte budget + governor on) and
   reports entries/sec. This shows the item-2 win diluted by the costs it does
   NOT remove (json.dumps in the source, asyncio scheduling), which is the
   honest real-world signal for deciding whether items 3/4 are worth chasing.

Run: ``uv run python benchmarks/bench_pipeline_hotpath.py``
Not a pytest test (no assertions; it prints timings) — kept out of the gate.
"""

from __future__ import annotations

import asyncio
import json
import time
from datetime import UTC, datetime, timedelta

from sf2loki.app import Pipeline
from sf2loki.config import LokiBatchConfig
from sf2loki.model import Batch, CheckpointToken, LogEntry
from sf2loki.obs.metrics import Metrics

_N = 200_000


def _make_entries(n: int) -> list[LogEntry]:
    """A realistic corpus: ~600-byte JSON lines, a few labels + metadata."""
    entries: list[LogEntry] = []
    ts = datetime.now(UTC)
    for i in range(n):
        payload = {
            "EventDate": "2026-07-02T12:00:00.000Z",
            "EventIdentifier": f"evt-{i:09d}",
            "SourceIp": "203.0.113.42",
            "UserId": f"005XX0000012{i % 997:03d}",
            "Application": "sf2loki-benchmark-application-name",
            "Status": "Success",
            "URI": "/services/data/v60.0/sobjects/Account/describe",
            "Bytes": i,
        }
        line = json.dumps(payload, sort_keys=True, default=str)
        entries.append(
            LogEntry(
                timestamp=ts,
                labels={"source": "eventlog_objects", "event_type": "ApiEvent"},
                line=line,
                structured_metadata={"level": "info", "EventIdentifier": f"evt-{i:09d}"},
                checkpoint=CheckpointToken(key="eventlog_objects:ApiEvent", value=str(i)),
            )
        )
    return entries


def _bench_encode_accounting(entries: list[LogEntry]) -> None:
    # Old: encode the line every place it's accounted (4x/entry).
    t0 = time.perf_counter()
    total = 0
    for e in entries:
        total += len(e.line.encode("utf-8"))  # producer byte-charge
        total += len(e.line.encode("utf-8"))  # consumer batch accounting
        total += len(e.line.encode("utf-8"))  # governor admission
        total += len(e.line.encode("utf-8"))  # sink size estimate
    old = time.perf_counter() - t0

    # New: encode once, reuse (what LogEntry.line_nbytes() memoization gives).
    for e in entries:
        e._line_nbytes = -1  # reset any memo from a prior phase
    t0 = time.perf_counter()
    total2 = 0
    for e in entries:
        nb = e.line_nbytes()  # first call encodes...
        total2 += nb + e.line_nbytes() + e.line_nbytes() + e.line_nbytes()  # ...rest are free
    new = time.perf_counter() - t0

    assert total == total2, (total, total2)
    print(f"  encode-every-time : {old * 1e3:8.1f} ms  ({len(entries)} entries x4 encodes)")
    print(f"  encode-once-reuse : {new * 1e3:8.1f} ms")
    print(f"  speedup           : {old / new:8.2f}x  ({(old - new) * 1e3:.1f} ms saved)")


class _NoopSink:
    async def push(self, batch: Batch) -> None:
        return None

    async def aclose(self) -> None:
        return None


class _FloodSource:
    name = "eventlog_objects"

    def __init__(self, entries: list[LogEntry]) -> None:
        self._entries = entries

    async def events(self, state: object, stop: asyncio.Event) -> object:
        for e in self._entries:
            yield e


async def _bench_pipeline(entries: list[LogEntry]) -> None:
    for e in entries:
        e._line_nbytes = -1
    batch = LokiBatchConfig(
        max_entries=1000,
        max_bytes=4_000_000,
        flush_interval=timedelta(seconds=1),
        queue_maxsize=10_000,
        queue_max_bytes=256 * 1024 * 1024,
    )
    pipe = Pipeline(
        sources=[_FloodSource(entries)],
        sink=_NoopSink(),  # type: ignore[arg-type]
        state=_FakeState(),  # type: ignore[arg-type]
        batch=batch,
        metrics=Metrics(),
    )
    t0 = time.perf_counter()
    await pipe.run(asyncio.Event())
    dt = time.perf_counter() - t0
    print(
        f"  processed {len(entries)} entries in {dt * 1e3:8.1f} ms  "
        f"({len(entries) / dt / 1000:.0f}k entries/sec)"
    )


class _FakeState:
    async def load(self, key: str) -> str | None:
        return None

    async def commit_many(self, items: object) -> None:
        return None


def main() -> None:
    entries = _make_entries(_N)
    print(f"corpus: {_N} entries, ~{len(entries[0].line)} bytes/line\n")
    print("[1] encode-accounting microbench (issue #69 item 2):")
    _bench_encode_accounting(entries)
    print("\n[2] end-to-end pipeline throughput (no-op sink):")
    asyncio.run(_bench_pipeline(entries))


if __name__ == "__main__":
    main()
