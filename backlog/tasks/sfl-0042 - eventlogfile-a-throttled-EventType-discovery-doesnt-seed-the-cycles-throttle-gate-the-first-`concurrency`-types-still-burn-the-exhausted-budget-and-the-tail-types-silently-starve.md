---
id: SFL-0042
title: >-
  eventlogfile: a throttled EventType discovery doesn't seed the cycle's
  throttle gate - the first `concurrency` types still burn the exhausted budget
  and the tail types silently starve
status: To Do
assignee: []
created_date: '2026-08-14 16:58'
labels:
  - followup
  - phase-2
milestone: m-0
dependencies: []
references:
  - 'https://github.com/rknightion/sf2loki/issues/126'
ordinal: 42000
---

## Description

<!-- SECTION:DESCRIPTION:BEGIN -->
## What

`EventLogFileSource` resolves the cycle's EventTypes *before* starting the cycle's per-type workers, and the throttle state produced by discovery never reaches the workers' gate in time.

- `src/sf2loki/sources/eventlogfile_source.py:292-293` — each cycle clears `self._cycle_throttled`, then calls `await self._resolve_event_types()`.
- `src/sf2loki/sources/eventlogfile_source.py:414-423` — a discovery failure is contained: on `EventLogFileThrottledError` it sets `self._cycle_throttled = True` (line 416), counts a `soql_poll_errors` sample, logs a WARNING, and returns the explicit types.
- `src/sf2loki/sources/eventlogfile_source.py:342-344` — `_process_cycle` then constructs a **fresh, unset** `throttle_event` plus the concurrency semaphore.
- `src/sf2loki/sources/eventlogfile_source.py:346-357` — the per-type worker gates *only* on `throttle_event` (lines 347 and 352). `self._cycle_throttled` is read only at line 356, i.e. **after** a worker has already driven `_process_event_type` to completion.

`self._cycle_throttled` has exactly one reader (line 356), so the assignment at line 416 exists solely to abort the cycle — but it cannot take effect until at least one worker has finished. Two distinct consequences follow.

### Symptom 1 — extra 403s against an already-exhausted budget

Up to `concurrency` per-type listing SOQL calls are issued after discovery has already returned `REQUEST_LIMIT_EXCEEDED`. This contradicts the class contract at `src/sf2loki/sources/eventlogfile_source.py:239-241`: "A 403 `REQUEST_LIMIT_EXCEEDED` additionally aborts the REST of the cycle so an exhausted API budget isn't hammered further."

Reproduced against current `main` (throttled `list_event_types`, all per-type listings also throttled, 10 explicit types, `concurrency: 4`):

```
list_calls after throttled discovery: ['Login', 'API', 'Report', 'ApexExecution']
```

Four wasted 403s per cycle, plus four spurious `soql_poll_errors{source="eventlogfile",object=<type>}` increments and four `_record_cycle_failure` ERROR lines (`:466-488`) that name per-type listings as the failing operation when the budget was already known to be gone.

### Symptom 2 — deterministic starvation of the tail of the type list

The stale `self._cycle_throttled = True` makes the **first worker to complete** set `throttle_event` at `:356-357` *even when that worker's own listing and downloads succeeded*. `throttle_event.set()` runs inside `async with semaphore` (`:349`), so it is visible before the permit is released; every still-queued worker therefore returns at the re-check on line 352 — silently, with no log line and no metric.

Reproduced with a throttled discovery but perfectly healthy per-type listings/downloads, 10 explicit types, `concurrency: 4` (the default, `src/sf2loki/config.py:735-743`):

```
list_calls: ['Login', 'API', 'Report', 'ApexExecution']
healthy types skipped: {'URI', 'LightningInteraction', 'Dashboard', 'ContentTransfer', 'Sites', 'Wave'}
```

Because `asyncio.gather` creates the worker tasks in `type_cfgs` order and the semaphore admits `type_cfgs[0:concurrency]` first, the *same* leading four types win every cycle. Discovery is re-attempted every cycle (`:293`), so while the org stays over its API limit the trailing types never list, never download, and never advance their watermark. That contradicts the documented intent of the fallback at `src/sf2loki/sources/eventlogfile_source.py:405-406`: "A discovery failure is non-fatal: fall back to the explicit types so a transient error can't stop ingestion."

### Test coverage gap

The throttled-discovery path is untested. The fake client's `list_event_types` raises a plain `EventLogFileError`, never `EventLogFileThrottledError` (`tests/sources/test_eventlogfile_source.py:123-127`), so `test_discovery_failure_falls_back_to_explicit` (`tests/sources/test_eventlogfile_source.py:1120`) exercises only the non-throttle branch. `test_throttled_listing_aborts_rest_of_cycle` (`tests/sources/test_eventlogfile_source.py:993`) covers the per-type listing throttle, and its own docstring concedes that at `concurrency=4` it passes only because the fake client never suspends mid-call.

## Why it matters

While a Salesforce org is over its 24h API limit, every ELF poll cycle:

1. burns `concurrency` further REST calls that are certain to 403, deepening the limit breach and inflating `soql_poll_errors` and the per-type consecutive-failure escalation counters (`_consecutive_failures`, `:468-469`, which drive the ERROR escalation at `:480-488`) with failures attributable to the exhausted budget rather than to those types;
2. processes only the first `concurrency` configured types and silently skips the rest, so with a typical 10-type ELF config six types stop ingesting entirely, with nothing in the logs or metrics naming them.

No data is lost — checkpoints are untouched for skipped types and the tail catches up once the throttle clears — so this is bounded to the degraded window. The cost is misleading telemetry during exactly the incident an operator is trying to diagnose, plus unexplained per-type ingest lag.

## Proposed approach

Seed the gate from the discovery result. In `_process_cycle`, immediately after constructing `throttle_event` (`src/sf2loki/sources/eventlogfile_source.py:343`) and before `run_workers` is scheduled:

```python
throttle_event = asyncio.Event()
if self._cycle_throttled:
    # EventType discovery already hit REQUEST_LIMIT_EXCEEDED this cycle
    # (_resolve_event_types), so the org's API budget is gone: skip every
    # type exactly as a throttled first worker would, instead of firing
    # `concurrency` more doomed listing calls and then starving whichever
    # types happened to queue behind the semaphore.
    throttle_event.set()
```

Chosen over the alternative of not setting `self._cycle_throttled` in `_resolve_event_types` (letting all types be attempted and 403 independently): the class contract at `:239-241` is an explicit abort, and per-type listings against an org over its limit will 403 anyway. Seeding also makes the outcome deterministic — currently which types run depends on config ordering and scheduler timing.

Update the `_process_cycle` docstring (`:334-340`) and the module docstring's concurrency paragraph (`:60-63`) to state that a throttled *discovery* skips the whole cycle, not just the types queued behind a throttled worker.

Also worth a log line: the discovery WARNING at `:418-422` says only that discovery failed and that explicit types will be used, which after this change is actively misleading because none of them are processed. Emit one WARNING when the whole cycle is abandoned because of a throttle (`self._cycle_throttled` true at cycle start), naming the count of types skipped.

---

Imported from GitHub issue #126 on 2026-08-14, when this repo migrated from GitHub Issues to Backlog.md. The original issue has been deleted; its verbatim body, labels and comments are preserved in `archive/issues-dump.json` (`jq '.[] | select(.number == 126)' archive/issues-dump.json`).

<sub>Filed from the 2026-07-30 full-repo audit (11 finder lanes + adversarial verification per finding).</sub>
<!-- SECTION:DESCRIPTION:END -->

## Acceptance Criteria
<!-- AC:BEGIN -->
- [ ] #1 `_process_cycle` sets `throttle_event` before any worker runs whenever `self._cycle_throttled` is already true on entry (`src/sf2loki/sources/eventlogfile_source.py:343`).
- [ ] #2 The discovery-throttle log path states that the cycle was abandoned (not that explicit types will be used).
- [ ] #3 `_process_cycle`/module docstrings describe the seeded gate.
- [ ] #4 Test: `list_event_types` raises `EventLogFileThrottledError`, config has the wildcard plus >`concurrency` explicit types, `concurrency=4` → zero `list_files` calls, zero entries yielded (including zero `checkpoint_only` tokens), and no checkpoint keys written to the store. Requires extending `FakeEventLogFileClient` (`tests/sources/test_eventlogfile_source.py:123-127`) with a throttled-discovery mode, since `discover_error` currently raises only a plain `EventLogFileError`.
- [ ] #5 Test: throttled discovery with healthy per-type listings/downloads → still zero types processed this cycle (pins the deliberate abort and the absence of the leading-`concurrency` carve-out), and a following cycle with discovery recovered processes every type.
- [ ] #6 The fake's throttled `list_files`/`list_event_types` must `await` before raising, so sibling workers are genuinely admitted; without a suspension point the current code passes the first test by accident.
- [ ] #7 Existing behaviour preserved: `test_discovery_failure_falls_back_to_explicit` (`tests/sources/test_eventlogfile_source.py:1120`) still passes — a **non-throttle** discovery failure must keep falling back to the explicit types and process all of them.
- [ ] #8 Existing throttle tests still pass: `test_throttled_listing_aborts_rest_of_cycle` (`:993`), `test_throttled_download_aborts_rest_of_cycle` (`:1019`), `test_throttle_lets_already_running_type_finish_but_skips_unstarted` (`:1523`).
- [ ] #9 `just gate` green.
<!-- AC:END -->

## Definition of Done
<!-- DOD:BEGIN -->
- [ ] #1 just gate is green (ruff check + ruff format --check + mypy src + pytest) — run it, don't assert it
- [ ] #2 just gen-config run and its output committed, if config.py changed (CI drift gate fails otherwise)
- [ ] #3 committed straight to main with a conventional-commit message, and pushed
<!-- DOD:END -->
