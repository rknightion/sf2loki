---
id: SFL-0012
title: >-
  coordinate: file lease blind-rewrites the lease when the verify re-read also
  fails — residual #50 window, and the epoch fence regresses
status: To Do
assignee: []
created_date: '2026-08-14 16:58'
labels:
  - followup
  - phase-4
milestone: m-0
dependencies: []
references:
  - 'https://github.com/rknightion/sf2loki/issues/96'
ordinal: 12000
---

## Description

<!-- SECTION:DESCRIPTION:BEGIN -->
## What

`FileLeaseCoordinator._hold` still contains an unconditional lease rewrite on the path where **both** the pre-renew read and the contested-path verify read fail.

Flow in `src/sf2loki/coordinate/file_lease.py`:

1. `file_lease.py:255-263` — the pre-renew `self._read()` raises `_LeaseReadError` (any non-`FileNotFoundError` `OSError`, raised at `file_lease.py:341-342`); it is logged and folded to `lease = None` ("treating as contested").
2. `file_lease.py:264-270` — the foreign-holder surrender branch is skipped, because `lease is None`.
3. `file_lease.py:271-277` — the contested path pauses `self._verify_delay` (`min(1.0, max(0.05, renew * 0.1))`, `file_lease.py:112-114`) and re-reads. A `_LeaseReadError` on that verify read is folded to `verify = None`.
4. `file_lease.py:278-284` — the surrender branch is guarded by `verify is not None`, so it does not fire.
5. `file_lease.py:285-287` — control falls through to `self._write(now, self._epoch)`: an unconditional rewrite of the lease document with this instance's holder and this instance's (possibly older) epoch, while `is_leader` stays `True`.

Two consequences:

- **The #50 blind rewrite is still reachable**, one verify pause deeper than before. A standby's live claim is clobbered by an instance that has no evidence it still owns the lease.
- **The epoch fence regresses.** `_write` stamps `self._epoch`, so a lease carrying epoch `N+1` (written by a standby that took over) is overwritten with epoch `N`. That violates the monotonic-epoch invariant documented at `file_lease.py:27-32` ("bumps by one on every winning acquire/takeover and is otherwise preserved verbatim across renewals").

There is also **no time-based cap on "cannot confirm"**. `last_ok` (`file_lease.py:243`, `file_lease.py:288`) is advanced by every successful *write* and consulted only inside the write-failure branch (`file_lease.py:293-299`). An instance whose reads fail persistently while its renames keep succeeding therefore renews blindly forever, and a process that was frozen past the ttl resumes and renews as if nothing happened.

Reproduced against the current code (both reads raising `_LeaseReadError`, on-disk lease `{"holder": "STANDBY", "expires_at": now+30s, "epoch": 6}`, incumbent `OLD` with `_epoch = 5`): after `_hold` the file reads `{"holder": "OLD", "expires_at": now+30s, "epoch": 5}` and `is_leader` is still `True`.

Existing coverage does not reach this state: `tests/coordinate/test_file_lease.py:299-341` fails only the *first* read (`if calls["n"] == 1`), so the verify read succeeds and the surrender branch fires; `tests/coordinate/test_file_lease.py:221-296` cover lease deletion, not two consecutive read errors.

File-backend only. The Kubernetes backend cannot do this because the renew is a `resourceVersion` compare-and-swap and a lost CAS (HTTP 409) surrenders — `src/sf2loki/coordinate/k8s_lease.py:369-386`. The file backend's rename has no CAS, so the read is the only guard, which is precisely why folding a read error into "proceed" is unsafe.

## Why it matters

Reachable sequences that put a standby on the lease while the incumbent's reads fail and its writes succeed:

- **Incumbent stalled past the ttl** (long GC pause, host freeze, `SIGSTOP`). The lease expires, a standby takes over with epoch `N+1`. The incumbent wakes, both reads fail, and it stamps holder=incumbent/epoch `N` over the standby's claim.
- **Partial shared-storage recovery.** Renames start succeeding again while reads still fail (a stale NFS dentry for the lease path is common exactly after another host renamed a new file over it, so the two reads are correlated, not independent). The write-failure ttl cap at `file_lease.py:293` never fires because writes now succeed.
- **Clock skew** beyond `ttl - renew_interval` on the standby, which is the documented risk this backend accepts (`file_lease.py:5-9`).

Consequences in that window:

1. Both instances are active leaders for up to one `renew_interval` (the standby surrenders at its next renew, when it reads the foreign holder at `file_lease.py:264-270`). Two Pub/Sub subscribers double-deliver — the exact failure the single-instance/active-passive design exists to prevent (`src/sf2loki/coordinate/CLAUDE.md`).
2. If the standby committed checkpoints first, the state document holds epoch `N+1`, so every subsequent commit from the epoch-`N` incumbent is rejected by `src/sf2loki/state/file_store.py:288-292`. The `StateFenceError` is absorbed at `src/sf2loki/app.py:1237`, the pipeline task then completes cleanly with `run_stop` unset, and `src/sf2loki/app.py:1181-1182` sets the global stop — the process shuts down. It restarts with a new holder id, sees the not-yet-expired lease it stamped itself, and waits for it to age out: an ingestion gap of up to one ttl on top of the duplicate window.
3. The lease's epoch stops being monotonic, so it is no longer a sound fence token for anything that assumes monotonicity.

## Proposed approach

Route "cannot confirm the lease" through the same time-based cap the write-failure branch already uses, instead of falling through to a rewrite.

In `_hold`:

1. Add a `last_confirmed: datetime` alongside `last_ok` (`file_lease.py:243`), initialised at `_hold` entry. Advance it on every read that *succeeds* — whether it returned `None` (genuinely absent/corrupt) or a lease whose holder is us. A read that raises `_LeaseReadError` must never advance it.
2. When the pre-renew read raises `_LeaseReadError` (`file_lease.py:257-263`) and `(now - last_confirmed).total_seconds() >= self._ttl`, surrender immediately (return) without the verify pause and without writing: no read has confirmed ownership for a full ttl, so the lease has (or will have) expired for everyone, mirroring `file_lease.py:293-299`.
3. When the verify read also raises `_LeaseReadError` (`file_lease.py:274-277`), apply the same cap: surrender if `now - last_confirmed >= ttl`, otherwise keep the current renew-and-continue behaviour so a short read blip does not cost leadership. Log at WARNING on the unverified renew so the condition is visible.

Rationale for the cap rather than surrendering on the first unverifiable verify read: `_acquire` also backs off without contesting on `_LeaseReadError` (`file_lease.py:189-200`), so surrendering on a two-read blip leaves the deployment with **no** leader until reads recover. The ttl cap keeps the incumbency bias the write-failure branch already encodes while making the blind-write window bounded by the ttl instead of unbounded, and it closes the stalled-incumbent and partial-recovery cases outright (in both, `last_confirmed` is already older than the ttl at the moment of the rewrite).

Keep `_acquire` unchanged. Document the incumbency-bias-with-ttl-cap decision in the module docstring (`file_lease.py:21-25`), which currently describes only the pause+verify discipline, and note the deliberate asymmetry with `_acquire`'s never-guess rule.

---

Imported from GitHub issue #96 on 2026-08-14, when this repo migrated from GitHub Issues to Backlog.md. The original issue has been deleted; its verbatim body, labels and comments are preserved in `archive/issues-dump.json` (`jq '.[] | select(.number == 96)' archive/issues-dump.json`).

<sub>Filed from the 2026-07-30 full-repo audit (11 finder lanes + adversarial verification per finding).</sub>
<!-- SECTION:DESCRIPTION:END -->

## Acceptance Criteria
<!-- AC:BEGIN -->
- [ ] #1 `_hold` tracks a `last_confirmed` timestamp advanced only by reads that succeed (absent-or-ours), never by a `_LeaseReadError`.
- [ ] #2 A `_LeaseReadError` on the pre-renew read with `last_confirmed` older than one ttl surrenders immediately: `_hold` returns without calling `_write` and without taking the verify pause.
- [ ] #3 A `_LeaseReadError` on the verify read with `last_confirmed` older than one ttl surrenders: `_hold` returns without calling `_write`.
- [ ] #4 Within one ttl of the last confirmed read, two consecutive `_LeaseReadError`s still renew (leadership is not dropped on a transient blip) and log at WARNING.
- [ ] #5 Test: both `_read` calls raise `_LeaseReadError` while the on-disk lease holds `{"holder": "A", "epoch": N+1}` and `last_confirmed` is older than the ttl — assert `_hold` returns, `_write` was never called, and the on-disk holder is still `A` with epoch `N+1` (this is the regression test for the reproduction above; on current `main` the file becomes holder=self/epoch `N`).
- [ ] #6 Test: an instance frozen past the ttl (fake clock jumped beyond `ttl`) with a failing read surrenders on the first `_hold` iteration rather than renewing.
- [ ] #7 Existing #50 tests (`tests/coordinate/test_file_lease.py:221-341`) still pass unchanged, including `test_hold_transient_read_error_does_not_trigger_takeover_by_self`.
- [ ] #8 Module docstring in `src/sf2loki/coordinate/file_lease.py` records the unverifiable-lease decision, the ttl cap, and why `_hold` is incumbency-biased while `_acquire` is not.
- [ ] #9 `just gate` green.
<!-- AC:END -->

## Definition of Done
<!-- DOD:BEGIN -->
- [ ] #1 just gate is green (ruff check + ruff format --check + mypy src + pytest) — run it, don't assert it
- [ ] #2 just gen-config run and its output committed, if config.py changed (CI drift gate fails otherwise)
- [ ] #3 committed straight to main with a conventional-commit message, and pushed
<!-- DOD:END -->
