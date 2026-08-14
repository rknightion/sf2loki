---
id: SFL-0004
title: >-
  egress: only one of two budget-pause waiters detects the UTC-day rollover -
  the other lane stalls ~24h while readiness reports ready
status: To Do
assignee: []
created_date: '2026-08-14 16:58'
labels:
  - followup
  - phase-1
milestone: m-0
dependencies: []
references:
  - 'https://github.com/rknightion/sf2loki/issues/88'
ordinal: 4000
---

## Description

<!-- SECTION:DESCRIPTION:BEGIN -->
## What

`EgressGovernor._pause_until_rollover_or_stop` (`src/sf2loki/egress.py:206-228`) detects the UTC-day rollover by comparing the wall clock against the governor's **shared mutable** `self._date`, and only *after* its sleep:

```python
while not stop.is_set():
    now = self._utcnow()
    timeout = _seconds_until_next_utc_midnight(now)
    try:
        await asyncio.wait_for(stop.wait(), timeout=timeout)
        return                      # stop fired during the pause
    except TimeoutError:
        pass
    if self._utcnow().date() != self._date:      # egress.py:224
        self._rollover_to(self._utcnow().date())
        self._paused = False
        self._metrics.egress_paused.set(0)
        return
```

The pipeline runs one `_LaneState` + one `_consume` task per lane class (`src/sf2loki/app.py:265-277`; `_lane_of` at `src/sf2loki/app.py:90-92` sends `pubsub` to the streaming lane and every other source to the bulk lane), and every lane's `_flush` awaits **one shared governor** (`src/sf2loki/app.py:441`, constructed once at `src/sf2loki/app.py:1031-1040`). Nothing serialises `_flush`/`admit` across lanes - no lock, no shared push worker - so when the daily budget is exhausted under `budget_action: "pause"` both lanes park inside this loop concurrently.

At the next UTC midnight both `wait_for` timers fire in the same event-loop iteration. The first waiter to resume runs straight through the date check with no intervening `await`: it calls `_rollover_to(...)` (`src/sf2loki/egress.py:161-167`), which advances `self._date`, resets `self._used`, and re-arms the log latches; it then clears `self._paused` and sets `egress_paused` to 0. When the second waiter resumes, `self._utcnow().date() != self._date` is now **False**, so it does not return: it loops, recomputes `_seconds_until_next_utc_midnight` from an instant just past midnight, and sleeps a full extra day.

Reproduced by executing the module directly with a wall-advancing fake `utcnow` anchored 0.2s before a UTC midnight and two concurrent `admit(1, 10, stop)` calls: after the fake midnight one waiter returned, the other stayed parked with a pending timer of `86399.55s`, while `_paused` was `False` and the `sf2loki_egress_paused` gauge read `0`.

Two secondary defects in the same block:

- The `stop`-fired return at `src/sf2loki/egress.py:221` never clears `self._paused` or the `egress_paused` gauge, so both are left latched at shutdown.
- `_paused` and the gauge are per-governor, not per-waiter, so any waiter's exit clears the paused state for all of them.

Note for whoever implements this: the losing waiter must recover via the guarded `_maybe_rollover()` (`src/sf2loki/egress.py:169-172`), never an unconditional `_rollover_to`. A second unconditional rollover would zero `self._used` again after the new day has already accumulated bytes, letting that day silently exceed the budget.

## Why it matters

With `daily_byte_budget` set and the default `budget_action: "pause"`, budget exhaustion on day D pauses both lanes until D+1 00:00 UTC. One lane resumes; the other holds its batch - and therefore its checkpoints (`_commit` is only reached after a successful push, `src/sf2loki/app.py:494-496`) - until D+2 00:00 UTC. Its bounded queue then fills and backpressures its own producers, silently suspending that whole class of ingestion (all of EventLogFile / eventlog_objects / apexlog / polled objects if the bulk lane loses, or all Pub/Sub streaming if the streaming lane loses) for an extra 24 hours.

Nothing reports it:

- `degraded_reason()` self-gates on `self._paused` (`src/sf2loki/egress.py:272`), which the winning waiter cleared, so /readyz returns 200 "ready".
- The only other predicate that could notice a silent lane, `_sink_degradation_check` (`src/sf2loki/app.py:626-642`), reads `pipeline.sink_failing_since`, which is set solely on `RetryableSinkError` (`src/sf2loki/app.py:460-461`). A lane blocked inside `admit` never pushes, so it never marks itself failing.
- `sf2loki_egress_paused` reads 0 and `egress_budget_used_bytes` looks healthy, so a dashboard or alert on the pause gauge cannot catch it either.

This directly breaks the documented contract that readiness reports degraded for the whole duration of a budget pause (`src/sf2loki/egress.py:12-14`, `src/sf2loki/config.py:912-918`). The extra day of delay is also not always lossless: an ingestion window pushed past a Salesforce-side retention boundary (Pub/Sub replay window, debug-log retention, EventLogFile retention on orgs without extended retention) is unrecoverable.

The stall is not guaranteed to end after exactly one day. The losing waiter's re-armed timer fires at the next midnight, and any other lane's `record()`/`admit()` call that reaches `_maybe_rollover()` first in that same event-loop iteration advances `self._date` again, so the loser can miss the boundary repeatedly. It clears only on a `stop` (shutdown/restart) or on a midnight where it happens to win the race.

## Proposed approach

Make rollover detection per-waiter, check it at the top of the loop before computing any sleep, and make the paused flag/gauge lifecycle reference-counted:

1. Capture `paused_date = self._date` on entry to `_pause_until_rollover_or_stop`.
2. Restructure the loop so the date comparison happens **before** the sleep, against `paused_date`:
   ```python
   while not stop.is_set():
       now = self._utcnow()
       if now.date() != paused_date:
           self._maybe_rollover()   # guarded: no-op if another waiter already rolled
           return
       try:
           await asyncio.wait_for(stop.wait(), timeout=_seconds_until_next_utc_midnight(now))
           return
       except TimeoutError:
           continue
   ```
   A waiter whose pause began on `paused_date` therefore returns as soon as the wall clock is on a different day, whether or not another waiter already advanced `self._date`.
3. Add an integer `self._pause_waiters` counter. Increment on entry; in a `finally`, decrement and clear `self._paused` + `self._metrics.egress_paused.set(0)` only when it reaches 0. This fixes the shared-flag problem, the `stop`-path latch at `src/sf2loki/egress.py:221`, and keeps `degraded_reason()` truthful for as long as any lane is held.
4. Leave the `_pause_logged` re-arm inside `_rollover_to` as is; the one-shot ERROR is per day, not per waiter.

No config or metric surface changes. `_maybe_rollover()` already guards against a duplicate `_used` reset, so step 2 is safe for the losing waiter.

---

Imported from GitHub issue #88 on 2026-08-14, when this repo migrated from GitHub Issues to Backlog.md. The original issue has been deleted; its verbatim body, labels and comments are preserved in `archive/issues-dump.json` (`jq '.[] | select(.number == 88)' archive/issues-dump.json`).

<sub>Filed from the 2026-07-30 full-repo audit (11 finder lanes + adversarial verification per finding).</sub>
<!-- SECTION:DESCRIPTION:END -->

## Acceptance Criteria
<!-- AC:BEGIN -->
- [ ] #1 `_pause_until_rollover_or_stop` compares the wall clock against a date captured at pause entry, evaluated at the top of the loop before any sleep, and recovers via `_maybe_rollover()` rather than an unconditional `_rollover_to`.
- [ ] #2 `_paused` and the `egress_paused` gauge are reference-counted across concurrent waiters and cleared in a `finally`, including on the `stop`-fired path.
- [ ] #3 Test in `tests/test_egress.py`: two concurrent `admit()` calls both paused by an exhausted budget, one fake-clock rollover - **both** coroutines complete within a short `asyncio.wait_for` timeout (a regression fails by timing out, since the loser would re-arm ~86400s).
- [ ] #4 Test: while at least one waiter is still parked (e.g. one released by `stop`, one still waiting on a static clock), `degraded_reason()` is not None and `sf2loki_egress_paused` == 1; both clear only after the last waiter returns.
- [ ] #5 Test: after two waiters resume across one rollover, the day counter was reset exactly once - `record()` a known byte count afterwards and assert `egress_budget_used_bytes` equals that count (guards against a double `_rollover_to` zeroing an already-accumulated day).
- [ ] #6 Test: `stop.set()` during a pause clears `_paused` and drives the `egress_paused` gauge to 0.
- [ ] #7 Existing single-waiter coverage (`tests/test_egress.py:260-306`) still passes unchanged.
- [ ] #8 `just gate` green.
<!-- AC:END -->

## Definition of Done
<!-- DOD:BEGIN -->
- [ ] #1 just gate is green (ruff check + ruff format --check + mypy src + pytest) — run it, don't assert it
- [ ] #2 just gen-config run and its output committed, if config.py changed (CI drift gate fails otherwise)
- [ ] #3 committed straight to main with a conventional-commit message, and pushed
<!-- DOD:END -->
