---
id: SFL-0008
title: >-
  pubsub: bridge byte budget is never reset per run - charges leaked on a
  mid-drain teardown stall every topic after leadership re-acquisition
status: To Do
assignee: []
created_date: '2026-08-14 16:58'
labels:
  - followup
  - phase-3
milestone: m-0
dependencies: []
references:
  - 'https://github.com/rknightion/sf2loki/issues/92'
ordinal: 8000
---

## Description

<!-- SECTION:DESCRIPTION:BEGIN -->
## What

`PubSubSource`'s internal bridge byte budget (#56) is accounted in per-instance state that is never reset between `events()` runs, and entries left in the bridge queue at teardown are never released. The charged-but-never-released bytes accumulate for the process lifetime and eventually exceed the budget, at which point every topic producer blocks forever.

The accounting:

- `self._bridge_queued_bytes = 0` is set once, in the constructor (`src/sf2loki/sources/pubsub_source.py:148`), alongside `self._bridge_byte_cond` (`:149`).
- `_bridge_charge` (`src/sf2loki/sources/pubsub_source.py:398-416`) waits on `wait_for(lambda: self._bridge_queued_bytes < self._bridge_max_bytes)` (`:413-415`) then adds the entry cost (`:416`). Admission is "admit while strictly under budget", so once `_bridge_queued_bytes >= _bridge_max_bytes` with no consumer running, the predicate can never become true again.
- `_bridge_release` (`:418-427`) is the only decrement, and it is called from exactly one place: the `events()` drain loop, immediately before yielding a dequeued entry (`:335`).

The gap:

- `events()` creates a fresh queue per run (`src/sf2loki/sources/pubsub_source.py:304`) and fresh `tasks` / `sentinels_remaining` (`:305-307`), but does not reset `_bridge_queued_bytes`.
- `events()`'s `finally` (`:337-341`) cancels the topic tasks and gathers them. It does not drain the queue or release the bytes charged for entries still in it. Those entries die with the local queue; their charge stays on the instance.

Sources are long-lived and reused across leadership acquisitions:

- `_build_org_sources` constructs one `PubSubSource` per org at startup and wires the budget (`src/sf2loki/app.py:758-768`, `bridge_max_bytes=org.sources.pubsub.bridge_max_bytes` at `:763`).
- `Pipeline` stores them once (`self._sources = list(sources)`, `src/sf2loki/app.py:183`; single `Pipeline(...)` construction at `src/sf2loki/app.py:1034`).
- `on_acquire` re-runs `self._pipeline.run` on every acquisition (`src/sf2loki/app.py:1184-1196` → `_run_pipeline`, `:1227-1238`), and `_produce` calls the same instance's `events()` again (`src/sf2loki/app.py:303`). Multi-org changes nothing: `OrgSource` wraps and delegates to the same inner instance (`src/sf2loki/sources/org_adapter.py:66`, `:93`).

The pipeline already resets its own per-run state for exactly this reason - `self._lanes = []` / `self._lane_depths = {}` at `src/sf2loki/app.py:265-266`, and `reset_state()` on demote (`src/sf2loki/app.py:1206`, issue #48). The source-side bridge accounting was missed.

Two teardown paths cancel producers mid-drain while the process stays alive (a crash-and-restart would clear the state, so those are harmless):

1. Grace-timeout cancel: `_drain_with_grace` cancels the pipeline task `shutdown_grace` seconds after `run_stop` fires (`src/sf2loki/app.py:603-609`); `Pipeline.run`'s `finally` cancels the producers (`src/sf2loki/app.py:296-299`); the `CancelledError` lands in `_produce`'s `async for` (`src/sf2loki/app.py:303`) and closes the generator at its `await queue.get()`, running the `finally` at `src/sf2loki/sources/pubsub_source.py:337` with entries still queued.
2. Commit fence on leadership loss: a `StateFenceError` from `_commit` inside `_flush` propagates out of `_consume`, so `Pipeline.run` takes the consumer-died branch, cancels producers and re-raises (`src/sf2loki/app.py:284-291`); `_run_pipeline` absorbs `StateFenceError` as a leadership transition rather than a crash (`src/sf2loki/app.py:1237-1238`). Same mid-flight producer cancel, same surviving process.

Reproduced against current `main` with a real `PubSubSource` (`bridge_max_bytes=300`, one topic, a client that yields events then holds the stream open): consume one entry from `events()`, let the producer refill, then `await gen.aclose()`. `_bridge_queued_bytes` is `508` after teardown. A second `events()` call on the same instance logs `pubsub subscribing` and then delivers nothing (2 s timeout, counter still `508`). Control: identical script with `src._bridge_queued_bytes = 0` inserted before the second run delivers the entry immediately. The counter is the cause.

Secondary leak on the same path: `_enqueue` charges before putting (`src/sf2loki/sources/pubsub_source.py:429-434`), so a topic task cancelled while awaiting `queue.put` on a full queue has charged an entry that never reaches the queue at all.

## Why it matters

`bridge_max_bytes` defaults to `134_217_728` (128 MiB) - `src/sf2loki/config.py:390` - so the budget is armed in every default deployment; no operator opt-in is required.

Failure sequence under `file_lease` or `k8s_lease` HA, single process:

1. A sink outage backs the pipeline lane queues up; `_produce` blocks on `lane.queue.put` (`src/sf2loki/app.py:314`), the `events()` drain loop stops dequeuing, and the bridge queue fills with charged entries.
2. Leadership is lost (lease flap, or a fenced commit) and producers are force-cancelled by one of the two paths above. Every charged entry still in the bridge queue leaks.
3. Re-acquisition on the same process runs `events()` again. Once accumulated leaked bytes reach the budget, the first `_enqueue` of every topic blocks forever inside `_bridge_charge`. Subscriptions connect and `pubsub subscribing` is logged, then nothing: no entries, no error, no reconnect, no checkpoint advance. `pubsub_stream_up` reads 1, so the stall is invisible to the existing dashboards and alerts.
4. Pub/Sub replay ids age out of Salesforce's 72 h retention window, so an undetected stall past 72 h converts to real data loss on the next restart (the `EARLIEST` fallback at `src/sf2loki/sources/pubsub_source.py:605-626` cannot recover events older than the window).

Magnitude: `queue_maxsize` is not wired through to `PubSubSource`, so its bridge queue keeps the constructor default of 1000 entries (`src/sf2loki/sources/pubsub_source.py:134`) and one teardown leaks at most ~1000 x (line bytes + 64). Against the 128 MiB default that is single-digit MB per cycle for typical event sizes, so the full stall normally needs either a lowered `bridge_max_bytes` or accumulation over repeated leadership flaps. The leak is monotonic and never reclaimed while the process lives, and every cycle permanently shrinks the effective budget - backpressure tightens silently long before the hard stall.

## Proposed approach

1. Reset the accounting at the start of each run, in `events()` immediately after the fresh queue is created (`src/sf2loki/sources/pubsub_source.py:304`) and before any topic task is spawned (`:314-325`):

   ```python
   async with self._bridge_byte_cond:
       self._bridge_queued_bytes = 0
       self._bridge_byte_cond.notify_all()
   ```

   Safe at that point: the previous run's topic tasks were cancelled and awaited in its `finally` (`:339-341`), and one `Source` produces from a single `_produce` task per run (`src/sf2loki/app.py:276`), so no producer of a prior run can still be inside `_bridge_charge`. `notify_all()` is needed so any waiter that somehow survives re-evaluates the predicate instead of sleeping on a stale one.

2. Belt-and-braces in `events()`'s `finally` (`:337-341`), after the `gather`: drain the queue with `get_nowait()` until empty and `_bridge_release` each non-`None` item, so the release path is symmetric even if the reset in step 1 is ever moved. Reset (step 1) is the authoritative fix - a fresh queue means zero queued bytes by construction - and covers the charged-but-never-enqueued case from `_enqueue` that a drain cannot see.

3. Document the invariant in the `events()` docstring (`:278-294`) next to the existing sentinel-accounting note: the bridge byte accounting is per-run state and must be zeroed whenever the queue is recreated.

4. Consider wiring `queue_maxsize` from `sink.loki.batch.queue_maxsize` at the call site (`src/sf2loki/app.py:758-767`) so the bridge's entry-count bound is configured rather than hardcoded at 1000. Optional, separable from the correctness fix; if taken, mention it in `docs/sources/pubsub.md`.

---

Imported from GitHub issue #92 on 2026-08-14, when this repo migrated from GitHub Issues to Backlog.md. The original issue has been deleted; its verbatim body, labels and comments are preserved in `archive/issues-dump.json` (`jq '.[] | select(.number == 92)' archive/issues-dump.json`).

<sub>Filed from the 2026-07-30 full-repo audit (11 finder lanes + adversarial verification per finding).</sub>
<!-- SECTION:DESCRIPTION:END -->

## Acceptance Criteria
<!-- AC:BEGIN -->
- [ ] #1 `events()` zeroes `_bridge_queued_bytes` under `_bridge_byte_cond` with a `notify_all()` before spawning topic tasks.
- [ ] #2 `events()`'s `finally` releases the bytes for every entry left in the bridge queue after the topic tasks are gathered.
- [ ] #3 Regression test: with `bridge_max_bytes` small (e.g. 300) and a client that yields entries then holds the stream open, pull one entry from `events()`, let the producer refill, `await gen.aclose()`, assert `_bridge_queued_bytes == 0`, then call `events()` again on the same instance and assert an entry is delivered within a bounded `wait_for`. The test must fail before the fix (it times out on the second run) and pass after.
- [ ] #4 Regression test: `_bridge_queued_bytes` is back to 0 after a normal, fully drained `events()` run completes (guards against the reset masking a release bug rather than fixing one).
- [ ] #5 Regression test at the pipeline level: run `Pipeline.run` twice on the same `Pipeline` whose sources include a `PubSubSource` with a small `bridge_max_bytes`, cancelling the first run mid-drain, and assert the second run yields entries - pins the leadership re-acquire path in `src/sf2loki/app.py:1184-1196`.
- [ ] #6 Existing bridge tests (`tests/sources/test_pubsub_source.py:1783-1852`) still pass unchanged.
- [ ] #7 `just gate` green.
<!-- AC:END -->

## Definition of Done
<!-- DOD:BEGIN -->
- [ ] #1 just gate is green (ruff check + ruff format --check + mypy src + pytest) — run it, don't assert it
- [ ] #2 just gen-config run and its output committed, if config.py changed (CI drift gate fails otherwise)
- [ ] #3 committed straight to main with a conventional-commit message, and pushed
<!-- DOD:END -->
