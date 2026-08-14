---
id: SFL-0055
title: >-
  tests: big-object drain SoqlError containment and the DESC no-progress stall
  guard are untested
status: To Do
assignee: []
created_date: '2026-08-14 16:58'
labels:
  - followup
  - phase-2
milestone: m-3
dependencies: []
references:
  - 'https://github.com/rknightion/sf2loki/issues/139'
ordinal: 55000
---

## Description

<!-- SECTION:DESCRIPTION:BEGIN -->
## What

`src/sf2loki/sources/eventlog_objects_source.py` sits at 93% statement coverage with the whole suite green. Two of the missed regions are behaviour-bearing error/guard paths on the big-object (DESC) drain that no test in `tests/` executes.

Measured (pytest-cov is not a dev dependency; run it ephemerally):

```
uv run --with pytest-cov --no-sync python -m pytest tests/ -q -p no:randomly \
  --cov=sf2loki.sources.eventlog_objects_source --cov-report=term-missing
# 275 stmts, 19 miss, 93%
# Missing: 136, 228, 247, 335-350, 357, 364, 450, 496, 535, 677-678, 692-698
# 1045 passed, 1 skipped
```

**Gap 1 — `except SoqlError` on the big-object drain (`eventlog_objects_source.py:335-350`).**
`_process_object` wraps `await self._drain_big_object(...)` (line 322) in two handlers. The throttle arm (`SoqlThrottledError`, 323-334) is covered by `tests/sources/test_eventlog_objects_source.py:1097` (`test_big_object_throttle_aborts_without_crashing`, which mocks a 403 `REQUEST_LIMIT_EXCEEDED`). The plain-transient arm is not. The existing transient-failure test, `tests/sources/test_eventlog_objects_source.py:717` (`test_soql_error_increments_poll_error_counter`), builds its config with `make_elo_cfg` (`tests/sources/test_eventlog_objects_source.py:57`), which does not set `big_object=True` — so it exercises the ASC-path duplicate of this handler at `eventlog_objects_source.py:405-425`, leaving 335-350 unexecuted.

The uncovered arm encodes four things: consecutive-failure counting into `self._consecutive_failures` (336-337), `soql_poll_errors{source="eventlog_objects", object=<name>}` increment (338-340), WARNING→ERROR escalation once `count >= _ERROR_LOG_THRESHOLD` (`_ERROR_LOG_THRESHOLD = 3` at line 98, applied at 341), and `return` so the cycle is contained and retried next interval. The counter reset for the success path is at line 362.

**Gap 2 — the DESC no-progress stall guard (`eventlog_objects_source.py:691-698`).**
Inside `_drain_big_object`, when a page is FULL (`len(page) == _PAGE_LIMIT`, so `short_page` is false at 659), spans more than one distinct timestamp (so the single-timestamp tie escape at 683-689 does not apply), and adds no new ids, the drain calls `_record_watermark_stall(obj, upper or watermark, ...)` and `break`s. This is reachable because the ratchet lowers the bound to the page's oldest timestamp *inclusively* (`upper = min(distinct_ts, ...)` at 701, `upper_exclusive = False` at 702, `<=` emitted at 637-639), so an identical repeat page is exactly this state.

The nearest existing test, `tests/sources/test_eventlog_objects_source.py:1068` (`test_big_object_full_page_all_seen_terminates`), returns 200 records that all share ONE timestamp, so it routes through the tie escape (683-689) into the *other* stall call site at 668-676, and it asserts only `calls["n"] == 2`. It asserts neither `metrics.watermark_stalls` nor the repeat-boundary WARNING→ERROR escalation implemented in `_record_watermark_stall` (`eventlog_objects_source.py:558-575`). Net effect: the DESC path has **no** assertion on the stall metric at all, while the ASC path's equivalent is fully pinned by `tests/sources/test_eventlog_objects_source.py:1334` (`test_asc_repeated_stall_at_same_boundary_escalates_to_error_and_counts`). The #38 protection is therefore tested asymmetrically.

**Also uncovered, lower value:** `eventlog_objects_source.py:677-678` (`tie_ids.extend(new_ids); continue`) — the tie escape returning a FULL page that still adds new ids, which needs more than `2 * _PAGE_LIMIT` records at one timestamp. `tests/sources/test_eventlog_objects_source.py:1290` uses 250 ties, so its second page is short (50 rows) and takes the 663-666 resume branch instead. The WARNING→ERROR escalation at line 341 and its ASC twin at line 415 are also never asserted (a three-cycle failure run is nowhere in the suite).

## Why it matters

Both regions are containment code whose whole purpose is that a single bad cycle does not become a permanent outage, and neither has a regression fence:

- 335-350 is the only thing stopping a transient SOQL 5xx during a big-object catch-up drain from propagating out of `_process_object` into the source task. A refactor that hoists the `try` (e.g. unifying the two duplicated handlers, or moving retry into `SoqlClient`) can drop this arm silently: the suite stays green because only the throttle arm is asserted, and the first production symptom is the daemon dying on a post-outage catch-up — precisely when the largest drain is running.
- 691-698 is the DESC analogue of the issue #38 hot-loop/no-progress protection. Without a test, a regression in the no-progress detection either spins `_drain_big_object` in an unbounded query loop against Salesforce (burning API budget until the org limit trips) or terminates without recording the stall, so the operator signal is absent while newer stored events are not being drained.

Scope note on the operator signal: `sf2loki_watermark_stalls` and `sf2loki_soql_poll_errors` are documented at `docs/observability/metrics.md:74` but are not currently wired into `deploy/grafana/rules/alerting/` or `deploy/grafana/dashboards/`, so today the loss is regression protection and log fidelity rather than a broken alert. That is what keeps this low severity rather than a functional defect.

## Proposed approach

Add three tests to `tests/sources/test_eventlog_objects_source.py`, reusing the existing `respx` + `FileCheckpointStore` + `make_big_object_cfg` (line 953) idiom. No production code changes.

1. `test_big_object_soql_error_contained_and_counted` — mock `_query_url()` to return `httpx.Response(500, text="boom")`, build the source with `make_big_object_cfg()` and an explicit `Metrics()`, drain `source.events(store, asyncio.Event())`. Assert the generator completes with `entries == []` (no exception escapes), `metrics.registry.get_sample_value("sf2loki_soql_poll_errors_total", {"source": "eventlog_objects", "object": "LoginEvent"}) == 1.0`, and (via `caplog.at_level("WARNING")`) that the record is `WARNING` and its message contains `big-object SOQL poll failed` — which distinguishes 335-350 from the ASC handler at 405-425.

2. `test_big_object_repeated_soql_error_escalates_to_error` — same mock, one `EventLogObjectsSource` instance reused across three `events()` cycles (`poll_once=True`), so `self._consecutive_failures` accumulates. Assert cycles 1 and 2 log at `WARNING` and cycle 3 logs at `ERROR` (`_ERROR_LOG_THRESHOLD = 3`), and that the counter reaches `3.0`.

3. `test_big_object_desc_stall_at_multi_timestamp_bound_counts` — pin 691-698. Mock a side effect that always returns the SAME full page of `_PAGE_LIMIT` (200) records with at least two distinct `EventDate` values (e.g. 199 rows at `2026-06-30T10:00:00.000+0000` and 1 row at `2026-06-30T10:05:00.000+0000`). Pre-seed the checkpoint (`eventlog_objects:LoginEvent` → `{"last_ts": <older than both>, "ids": [<all 200 ids>]}`): `_drain_big_object` does not consult the checkpoint id window (dedupe happens at `eventlog_objects_source.py:352-353`), so the drain still collects the page while nothing is emitted and the watermark does not move — which makes the stall boundary repeat across cycles. Drain cycle 1 on a shared source instance: assert `entries == []`, exactly 2 HTTP calls (one ratchet, then the guard breaks), one `WARNING` whose message contains `added no new records`, and `watermark_stalls` still `None`/`0.0`. Drain cycle 2 on the same instance: assert an `ERROR` record and `sf2loki_watermark_stalls_total{source="eventlog_objects", object="LoginEvent"} == 1.0`.

Optionally extend `tests/sources/test_eventlog_objects_source.py:1290` (or add a sibling) to more than `2 * _PAGE_LIMIT` tied records so 677-678 executes and the tie escape is shown to paginate across two full pages before the short page resumes the ratchet.

---

Imported from GitHub issue #139 on 2026-08-14, when this repo migrated from GitHub Issues to Backlog.md. The original issue has been deleted; its verbatim body, labels and comments are preserved in `archive/issues-dump.json` (`jq '.[] | select(.number == 139)' archive/issues-dump.json`).

<sub>Filed from the 2026-07-30 full-repo audit (11 finder lanes + adversarial verification per finding).</sub>
<!-- SECTION:DESCRIPTION:END -->

## Acceptance Criteria
<!-- AC:BEGIN -->
- [ ] #1 `tests/sources/test_eventlog_objects_source.py` contains a test that drives `eventlog_objects_source.py:335-350` with a non-throttle `SoqlError` on a `big_object=True` config, asserting the cycle yields no entries and raises nothing, `sf2loki_soql_poll_errors_total{source="eventlog_objects", object="LoginEvent"} == 1.0`, and a `WARNING` containing `big-object SOQL poll failed`.
- [ ] #2 A test asserts the WARNING→ERROR escalation at `_ERROR_LOG_THRESHOLD` (`eventlog_objects_source.py:341`) by reusing one source instance across three failing cycles.
- [ ] #3 A test drives `eventlog_objects_source.py:691-698` with a repeated full page spanning more than one distinct timestamp, asserting the drain terminates after exactly 2 queries (no hot loop) and logs `added no new records`.
- [ ] #4 The same scenario, repeated at the same boundary on a second cycle of the same source instance, asserts `sf2loki_watermark_stalls_total{source="eventlog_objects", object="LoginEvent"} == 1.0` plus an `ERROR` record — closing the ASC/DESC asymmetry against `tests/sources/test_eventlog_objects_source.py:1334`.
- [ ] #5 Coverage run shows `335-350` and `692-698` no longer in the missing list: `uv run --with pytest-cov --no-sync python -m pytest tests/ -q --cov=sf2loki.sources.eventlog_objects_source --cov-report=term-missing`.
- [ ] #6 No production code changed under `src/`.
- [ ] #7 `just gate` green.
<!-- AC:END -->

## Definition of Done
<!-- DOD:BEGIN -->
- [ ] #1 just gate is green (ruff check + ruff format --check + mypy src + pytest) — run it, don't assert it
- [ ] #2 just gen-config run and its output committed, if config.py changed (CI drift gate fails otherwise)
- [ ] #3 committed straight to main with a conventional-commit message, and pushed
<!-- DOD:END -->
