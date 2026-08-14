---
id: SFL-0027
title: >-
  tests: k8s lease hold/acquire error branches are untested - the renew-409 CAS
  surrender has no covering test
status: To Do
assignee: []
created_date: '2026-08-14 16:58'
labels:
  - followup
  - phase-1
milestone: m-3
dependencies: []
references:
  - 'https://github.com/rknightion/sf2loki/issues/111'
ordinal: 27000
---

## Description

<!-- SECTION:DESCRIPTION:BEGIN -->
## What

`src/sf2loki/coordinate/k8s_lease.py` sits at 80% statement coverage (210 statements, 42 missed) and the missed set is concentrated in the two error-handling loops that implement the HA contract. Reproduce with:

```bash
uv run --with pytest-cov pytest tests/coordinate/ --cov=sf2loki.coordinate.k8s_lease --cov-report=term-missing -q
# src/sf2loki/coordinate/k8s_lease.py  210  42  80%
# Missing: 53, 204, 309, 311-314, 330, 332-335, 341, 385-389, 411, 422, 445-446, 454-468, 481-482, 485-486, 489-500, 503-518
```

(454-518 is the lazily-imported `_RealLeaseAdapter`/`_default_api_factory`, unreachable without the `sf2loki[k8s]` extra installed — out of scope here.)

The uncovered behavioural branches:

- **`k8s_lease.py:384-389` — renew lost the CAS (HTTP 409) → surrender.** Never executed anywhere in the suite. `tests/coordinate/test_k8s_lease.py:529` `test_hold_surrenders_when_taken_over` carries the docstring "A 409 on renew (taken over) surrenders leadership", but it seeds holder `"A"` while the coordinator's holder is `"B"` (`test_k8s_lease.py:537-538`), so the pre-renew re-read at `k8s_lease.py:361-368` detects the foreign holder and returns at :368 before `replace_lease` is called. Coverage confirms this structurally: 363-368 are covered, 385-389 are not. `FakeLeaseAdapter.replace_lease` (`test_k8s_lease.py:125-136`) only raises 409 on a `resource_version` mismatch, which cannot happen in the hold loop because `k8s_lease.py:370` refreshes `resource_version` from the re-read on every iteration. The test name and docstring make the gap invisible to a reader.
- **`k8s_lease.py:411` — transient renewal failure below `lease_duration` → tolerate and keep leadership.** The nearest test, `test_k8s_lease.py:549` `test_hold_tolerates_transient_api_error_then_surrenders_past_duration`, jumps the fake clock 31s (> `lease_duration` 30), so it lands on the past-duration surrender at 404-410 and never on the tolerate arm at 411. Despite its name, no test asserts that a single API blip preserves leadership.
- **`k8s_lease.py:311-314` and `332-335` — non-conflict API errors during `_acquire`** (a `create_lease` or `replace_lease` failure whose status is not 409) → log, back off one renew interval, retry. Only the 409 race arms (306-310, 327-331) are covered, by `test_lost_create_race_backs_off` (`test_k8s_lease.py:433`) and `test_lost_replace_race_backs_off` (`test_k8s_lease.py:468`).
- **`k8s_lease.py:445-446` — a non-404 read error is logged and treated as an absent lease.** Untested. This is the same shape as the file-lease defect fixed in #50 (read errors treated as absent, then a blind rewrite). The k8s path is currently safe because a subsequent `create_lease` against an existing object returns 409 and backs off, but nothing pins that.

Because 385-389 never execute under the suite, any regression confined to those lines cannot change a single test outcome. That is a coverage fact, not an estimate: line 384's predicate is exercised only with a False result (by the 404 and 500 tests).

## Why it matters

`k8s_lease.py:384-389` is the guard against the stale-leader split-brain that the fencing work in #47 and the observedTime work in #51 exist to prevent. A regression that removes the `_CONFLICT` arm, or reorders it after the `_NOT_FOUND`/transient handling, makes a lost renew CAS fall through to `411` and be tolerated as a blip: `_is_leader` stays True for up to a full `lease_duration` after another replica has legitimately taken the lease. Both instances' `check_fence()` then pass, and the stale leader's checkpoint commits race the new leader's — exactly the failure #47 was filed against. The suite stays green.

The inverse regression is equally invisible: if the tolerate arm at `411` were changed to surrender on the first non-409, non-404 error, every Kubernetes API hiccup would demote the leader, causing leadership flapping and a full pipeline stop/start cycle (`on_lose` → `on_acquire`) on each one. Nothing in the suite would fail.

The misleading docstring at `test_k8s_lease.py:530` compounds both: a maintainer auditing the hold loop reads the test list, sees a 409-on-renew test, and concludes the branch is pinned.

## Proposed approach

Add tests to `tests/coordinate/test_k8s_lease.py` using the existing `FakeClock` / `FakeMonotonic` / `ScriptedSleep` / `FakeLeaseAdapter` / `FakeApiException` / `_coord` helpers already in that file — no new fixtures needed. The first two recipes below were validated out-of-tree against the current code; both pass and remove 385-389 and 411 from the term-missing set.

1. **`test_hold_surrenders_on_renewal_cas_conflict`** (covers 384-389). Acquire normally, then replace `adapter.replace_lease` with a counting stub that raises `FakeApiException(status=409)`. Leave `read_lease` returning our own holder so the pre-renew check at `k8s_lease.py:362` passes — this is the whole point of the test and what distinguishes it from `test_hold_surrenders_when_taken_over`. Set `coord._sleep = ScriptedSleep()` so stop never fires and the clock never advances, mirroring the "returns promptly or loops forever" invariant already used by `test_hold_surrenders_immediately_when_lease_deleted` (`test_k8s_lease.py:572`). Assert `_hold` returns and the stub was called exactly once (no retry). To make a regression fail loudly instead of hanging the suite, have the stub set the `stop` event on its second call and then assert the call count is 1.

2. **`test_hold_tolerates_transient_error_below_duration`** (covers 411). Acquire normally, then wrap `replace_lease` so the first call raises `FakeApiException(status=500)` and later calls delegate to the original. Script the sleeps as `ScriptedSleep([lambda: clock.advance(5), None, stop.set])` — 5s is well under `lease_duration` 30. Assert `_hold` returns only because stop fired, `replace_lease` was called at least twice, and the lease's holder is still ours.

3. **`test_acquire_retries_on_non_conflict_create_error`** (covers 311-314). Absent lease; `create_lease` raises `FakeApiException(status=500)` on the first call then delegates. Script sleep #0 as a no-op back-off. Assert `_acquire` returns non-None and the stored holder is ours. Mirror the structure of `test_lost_create_race_backs_off` (`test_k8s_lease.py:433`).

4. **`test_acquire_retries_on_non_conflict_replace_error`** (covers 332-335). Seed a foreign holder, advance the monotonic clock past `lease_duration` so the lease is observed stale, and have `replace_lease` raise status 500 once then delegate. Mirror `test_lost_replace_race_backs_off` (`test_k8s_lease.py:468`).

5. **`test_read_error_is_treated_as_absent_without_clobbering`** (covers 445-446). Make `read_lease` raise `FakeApiException(status=500)`, assert `_read()` returns `None`, and assert that a subsequent `_acquire` against a lease object that does in fact exist does not end up with our holder written (the `create_lease` 409 back-off keeps it safe). This documents the current behaviour and blocks a #50-style regression.

Also fix the docstring at `test_k8s_lease.py:530` to describe what that test actually verifies — the pre-renew re-read surrender path at `k8s_lease.py:361-368` — and rename it accordingly (for example `test_hold_surrenders_when_reread_shows_foreign_holder`), so the two surrender paths are distinguishable by name.

---

Imported from GitHub issue #111 on 2026-08-14, when this repo migrated from GitHub Issues to Backlog.md. The original issue has been deleted; its verbatim body, labels and comments are preserved in `archive/issues-dump.json` (`jq '.[] | select(.number == 111)' archive/issues-dump.json`).

<sub>Filed from the 2026-07-30 full-repo audit (11 finder lanes + adversarial verification per finding).</sub>
<!-- SECTION:DESCRIPTION:END -->

## Acceptance Criteria
<!-- AC:BEGIN -->
- [ ] #1 `test_hold_surrenders_on_renewal_cas_conflict` added; it drives `k8s_lease.py:384-389` with `read_lease` still returning our own holder, and asserts exactly one `replace_lease` attempt.
- [ ] #2 `test_hold_tolerates_transient_error_below_duration` added; it drives `k8s_lease.py:411` and asserts leadership is retained across a single non-409/non-404 failure.
- [ ] #3 `test_acquire_retries_on_non_conflict_create_error` added; covers `k8s_lease.py:311-314`.
- [ ] #4 `test_acquire_retries_on_non_conflict_replace_error` added; covers `k8s_lease.py:332-335`.
- [ ] #5 `test_read_error_is_treated_as_absent_without_clobbering` added; covers `k8s_lease.py:445-446` and asserts no blind takeover.
- [ ] #6 `test_hold_surrenders_when_taken_over` (`tests/coordinate/test_k8s_lease.py:529`) renamed and its docstring corrected to describe the pre-renew re-read path at `k8s_lease.py:361-368`, not a renew 409.
- [ ] #7 `uv run --with pytest-cov pytest tests/coordinate/ --cov=sf2loki.coordinate.k8s_lease --cov-report=term-missing -q` no longer reports 309, 311-314, 330, 332-335, 385-389, 411 or 445-446 as missing (the lazily-imported adapter block at 454-518 may remain uncovered without the `sf2loki[k8s]` extra).
- [ ] #8 Mutation check recorded in the closing comment: deleting the `if status == _CONFLICT:` arm at `k8s_lease.py:384-389` makes the new test fail, and changing `411` into a `return` makes the tolerate test fail.
- [ ] #9 `just gate` green.
<!-- AC:END -->

## Definition of Done
<!-- DOD:BEGIN -->
- [ ] #1 just gate is green (ruff check + ruff format --check + mypy src + pytest) — run it, don't assert it
- [ ] #2 just gen-config run and its output committed, if config.py changed (CI drift gate fails otherwise)
- [ ] #3 committed straight to main with a conventional-commit message, and pushed
<!-- DOD:END -->
