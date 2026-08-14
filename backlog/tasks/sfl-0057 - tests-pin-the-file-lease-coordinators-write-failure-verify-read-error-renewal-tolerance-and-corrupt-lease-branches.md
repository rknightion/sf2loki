---
id: SFL-0057
title: >-
  tests: pin the file-lease coordinator's write-failure, verify-read-error,
  renewal-tolerance and corrupt-lease branches
status: To Do
assignee: []
created_date: '2026-08-14 16:58'
labels:
  - followup
  - phase-1
milestone: m-3
dependencies: []
references:
  - 'https://github.com/rknightion/sf2loki/issues/141'
ordinal: 57000
---

## Description

<!-- SECTION:DESCRIPTION:BEGIN -->
## What

`src/sf2loki/coordinate/file_lease.py` sits at 87% statement coverage (189 statements, 25 missed) across the whole test suite. Several of the missed statements are the failure-handling branches that make the HA lease safe, and every one of them would survive a regression that deleted or inverted it.

Measured with `coverage run --source=src/sf2loki/coordinate -m pytest tests` over the full suite (1045 passed): missing lines `66, 200, 210-214, 216, 219-220, 236, 273, 276-277, 300, 311, 348, 352, 355-356, 358, 376-379`.

The behaviour-bearing gaps:

- `file_lease.py:210-214` — `_acquire` catches `OSError` from `self._write(now, new_epoch)`, logs `cannot write file lease; retrying`, pauses one renew interval and retries. `tests/coordinate/test_file_lease.py:178` (`test_acquire_backs_off_on_transient_read_error_without_contesting`) installs a `_write` stub that raises, but it is a tripwire that is deliberately never reached (the lease path is a directory so `_read` raises first). No test drives an acquire-path write failure.
- `file_lease.py:219-220` — `except _LeaseReadError: confirm = None` on the post-write verification re-read, which makes an unreadable lease count as "lost the race" rather than a confirmed win. No test makes the second `_acquire` read fail.
- `file_lease.py:276-277` — `except _LeaseReadError: verify = None` on the `_hold` contested-path verify read, which lets the holder proceed to renew when it still cannot read the lease. `tests/coordinate/test_file_lease.py:299` (`test_hold_transient_read_error_does_not_trigger_takeover_by_self`) raises only on read #1 (the pre-renew read at `file_lease.py:256`, guarded by `calls["n"] == 1`) and lets the verify read reach `real_read()`, so this arm is never taken.
- `file_lease.py:300` — the below-ttl tolerate branch: when a renewal `_write` raises `OSError` but `(now - last_ok).total_seconds() < self._ttl`, the coordinator logs `file lease renewal failed; will retry` and **keeps leadership**. Only the past-ttl surrender at `file_lease.py:293-299` is covered, by `tests/coordinate/test_file_lease.py:361` (`test_hold_surrenders_on_renewal_failure_past_ttl`, which advances the clock 31s against a 30s ttl).
- `file_lease.py:348, 352, 355-356, 358` — the `_read` corrupt-content fallbacks: valid JSON that is not a dict (348), non-string `holder`/`expires_at` (352), an `expires_at` that `datetime.fromisoformat` rejects (355-356), and naive-timestamp normalisation to UTC (358). The single corrupt-content test, `tests/coordinate/test_file_lease.py:90` (`test_read_unparseable_returns_none`), writes `"{ this is not json"` and therefore only exercises the `JSONDecodeError` arm at `file_lease.py:345-346`. The test helper `_write_lease` (`tests/coordinate/test_file_lease.py:61`) always emits a tz-aware `isoformat()`, so line 358 is unreachable from the current tests.

The remaining missed lines are trivia and not the point of this issue: `66` (`_default_utcnow`, always injected in tests), `200/216/236/273/311` (stop-event early returns in `_acquire`/`_hold`/`_pause`), `376-379` (the tmp-file cleanup in `_write`).

## Why it matters

`App._run` does not absorb exceptions from the coordinator: `await self._coordinator.run(...)` at `src/sf2loki/app.py:1210` sits in a `try/finally` whose only job is resource shutdown, and a pipeline crash is re-raised at `src/sf2loki/app.py:1224`. So each of these branches is the difference between a bounded retry and a process exit.

- Drop the tolerate branch at `file_lease.py:300` (for example by turning it into an unconditional `return`) and leadership flaps on every transient shared-storage blip: `_hold` returns, `App` runs `on_lose` → `_stop_acquisition` → `reset_state()` → standby, then re-acquires. Each flap is a full pipeline teardown plus a bounded checkpoint re-ingest. Today's suite stays green.
- Let the acquire-path `_write` `OSError` propagate (`file_lease.py:210`) and a standby that hits one write error on a flaky NFS mount crashes the process instead of retrying — it never becomes leader, and under a restart loop the deployment has no leader at all while storage is briefly unhappy.
- Let `_read` raise on an odd lease document instead of returning `None`-claimable (`file_lease.py:347-358`) and takeover wedges: `_acquire` only catches `_LeaseReadError`, so a `TypeError`/`ValueError` from a hand-edited, legacy-format or partially-written lease escapes and kills the coordinator. Removing the naive-tz normalisation at `file_lease.py:358` alone is enough: `_Lease.expired()` (`file_lease.py:75-76`) then compares a naive `expires_at` against a tz-aware `now` and raises `TypeError`.
- The verify-read-error arms (`219-220`, `276-277`) are the issue #50 semantics — "can't tell right now" must never be resolved as "I won" or "safe to rewrite". A regression there reintroduces the transient dual-leadership window that #50 closed, and no test would notice.

## Proposed approach

All additions go in `tests/coordinate/test_file_lease.py` using the existing `FakeClock` / `ScriptedSleep` harness (no real sleeps, injected clock). Config default from `_cfg` is `ttl=30`, `renew=10`, so `_verify_delay` is 1.0s; sleep call ordering is what the scripted actions key off.

1. **Acquire retries after a write failure** (covers `210-214`). Absent lease; wrap `_write` so the first call raises `OSError("shared storage unreachable")` and later calls delegate to the real implementation. Sleep order: `#0` = the post-failure renew-interval back-off, `#1` = the verification delay on the successful retry. Assert `await coord._acquire(stop) is True`, `_read_holder(path) == "B"`, and that `_write` was attempted twice.
2. **Acquire treats an unreadable verification read as a lost race** (covers `219-220`). Absent lease so the contest proceeds; stub `_read` to return `None` on call #1 and raise `_LeaseReadError("transient NFS error")` on call #2. Script sleep `#1` (the loser back-off) to `stop.set()`. Assert `_acquire` returns `False` and `coord.epoch == 0` — a lease it could not confirm never counts as won.
3. **Hold tolerates an unreadable verify read and still renews** (covers `276-277`). B holds (`coord._write(clock.now, 1)`, `coord._epoch = 1`); stub `_read` to return `None` on call #1 (pre-renew) and raise `_LeaseReadError` on call #2 (the verify read). Sleeps: `#0` renew pause, `#1` verify pause, `#2` `stop.set()`. Assert `_hold` returned only because stop fired (`stop.is_set()`), `_read_holder(path) == "B"`, and the persisted epoch is still 1.
4. **Hold keeps leadership through a sub-ttl renewal failure** (covers `300`). B holds at `_BASE`, `last_ok = _BASE`. Sleeps: `#0` advances the clock 10s (< ttl 30), `#1` advances another 10s (cumulative 20s, still < ttl), `#2` `stop.set()`. `_write` raises `OSError` on the first renewal attempt only, then delegates to the real write. Assert `_hold` did not surrender early (`stop.is_set()` is what ended it), the lease on disk still has `holder == "B"`, its `expires_at` advanced past the pre-failure value, and the epoch is preserved. This test must fail if line 300's `log.warning` is replaced by `return`.
5. **Parametrized `_read` corrupt-content matrix** (covers `348`, `352`, `355-356`, `358`, plus the non-int `epoch` arm at `360`). Write raw text into the lease file and call `coord._read()` directly:
   - `"[1, 2]"` → `None` (348)
   - `{"holder": 1, "expires_at": "<aware iso>"}` → `None` (352)
   - `{"holder": "A", "expires_at": 5}` → `None` (352)
   - `{"holder": "A", "expires_at": "not-a-timestamp"}` → `None` (355-356)
   - `{"holder": "A", "expires_at": "2026-01-01T12:00:30"}` (naive) → lease returned with `expires_at == datetime(2026, 1, 1, 12, 0, 30, tzinfo=UTC)`, and `lease.expired(_BASE) is False` so the aware/naive comparison is exercised rather than just the field value (358)
   - `{"holder": "A", "expires_at": "<aware iso>", "epoch": "x"}` → `lease.epoch == 0` (360's non-int arm)

Follow the file's existing convention of monkeypatching `coord._read` / `coord._write` on the instance with `# type: ignore[assignment]`; keep `mypy --strict` clean.

---

Imported from GitHub issue #141 on 2026-08-14, when this repo migrated from GitHub Issues to Backlog.md. The original issue has been deleted; its verbatim body, labels and comments are preserved in `archive/issues-dump.json` (`jq '.[] | select(.number == 141)' archive/issues-dump.json`).

<sub>Filed from the 2026-07-30 full-repo audit (11 finder lanes + adversarial verification per finding).</sub>
<!-- SECTION:DESCRIPTION:END -->

## Acceptance Criteria
<!-- AC:BEGIN -->
- [ ] #1 `tests/coordinate/test_file_lease.py` gains a test asserting `_acquire` succeeds on the retry cycle after the first `_write` raises `OSError` (covers `file_lease.py:210-214`).
- [ ] #2 A test asserts `_acquire` returns `False` and leaves `epoch == 0` when the verification re-read raises `_LeaseReadError` (covers `file_lease.py:219-220`).
- [ ] #3 A test asserts `_hold` completes a renewal (holder unchanged, epoch preserved) when the contested-path verify read raises `_LeaseReadError` (covers `file_lease.py:276-277`).
- [ ] #4 A test asserts `_hold` keeps leadership and renews on the following tick after a single renewal `_write` failure with `now - last_ok < ttl`, and that it only exits when stop fires (covers `file_lease.py:300`); the test fails if that branch is turned into a surrender.
- [ ] #5 Parametrized `_read` tests cover valid-JSON-not-a-dict, non-string `holder`, non-string `expires_at`, unparseable ISO timestamp, naive-timestamp UTC normalisation (asserted through `_Lease.expired()` as well as the field), and non-int `epoch` (covers `file_lease.py:348, 352, 355-356, 358, 360`).
- [ ] #6 `coverage run --source=src/sf2loki/coordinate -m pytest tests` reports no missed lines in `file_lease.py` other than `66, 200, 216, 236, 273, 311, 376-379`.
- [ ] #7 `just gate` green (`ruff check`, `ruff format --check`, `mypy src`, `pytest`); no production code changed by this issue.
<!-- AC:END -->

## Definition of Done
<!-- DOD:BEGIN -->
- [ ] #1 just gate is green (ruff check + ruff format --check + mypy src + pytest) — run it, don't assert it
- [ ] #2 just gen-config run and its output committed, if config.py changed (CI drift gate fails otherwise)
- [ ] #3 committed straight to main with a conventional-commit message, and pushed
<!-- DOD:END -->
