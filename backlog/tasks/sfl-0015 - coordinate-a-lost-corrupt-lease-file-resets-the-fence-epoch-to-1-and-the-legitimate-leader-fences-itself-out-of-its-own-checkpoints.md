---
id: SFL-0015
title: >-
  coordinate: a lost/corrupt lease file resets the fence epoch to 1 and the
  legitimate leader fences itself out of its own checkpoints
status: To Do
assignee: []
created_date: '2026-08-14 16:58'
labels:
  - followup
  - phase-4
milestone: m-0
dependencies: []
references:
  - 'https://github.com/rknightion/sf2loki/issues/99'
ordinal: 15000
---

## Description

<!-- SECTION:DESCRIPTION:BEGIN -->
## What

Under `coordinate.type: file_lease` with `state.store: file`, the fence epoch is tracked in two places that are never reconciled:

- The **lease file** is the only input to the epoch the winning acquirer takes: `new_epoch = (lease.epoch if lease is not None else 0) + 1` (`src/sf2loki/coordinate/file_lease.py:207`), adopted into `self._epoch` after the verify re-read (`file_lease.py:226`).
- `_read` returns `None` for a **missing** lease (`file_lease.py:339-340`), for **corrupt/unparseable** content (`file_lease.py:343-348`), and coerces a non-int `epoch` field to `0` (`file_lease.py:359-360`). All three collapse to "absent", so the next acquire starts again at epoch **1**.
- The **state document** independently persists the high-water epoch under `__fence_epoch__` (`src/sf2loki/state/file_store.py:19`, written at `:295`) and rejects any commit carrying a lower one: `if stored is not None and mine is not None and stored > mine: raise StateFenceError` (`file_store.py:288-292`; the same check guards `delete` at `file_store.py:260-264`).
- `app.py` wires the two together for the file-lease topology only (`epoch_source = lambda: file_lease.epoch`, `app.py:948`; `set_epoch(epoch_source)`, `app.py:966-968`).

Nothing reads `__fence_epoch__` back into the coordinator. `__fence_epoch__` appears in `src/` only in `state/file_store.py` and in `statecmd.py`'s reserved-key filter, so a lease file that is deleted, re-provisioned, hand-edited badly, or truncated to garbage silently rewinds the coordinator's epoch below the value already durably recorded in the state document.

Reproduced against current `main` (acquire at epoch 7 → commit writes `__fence_epoch__: "7"` → delete the lease file only → next acquire wins with epoch 1 and `is_leader == True`, so the boolean fence at `file_lease.py:125-136` passes → the first `commit` raises `StateFenceError: stale leader (epoch 1) rejected: state file ... was already advanced to epoch 7 by a newer leader`). A lease file containing non-JSON produces the same epoch 1.

Scope: `file_lease` + `state.store: file` only. `k8s_lease` never gets an epoch source (`app.py:948` is the sole `set_epoch` wiring) and the S3/GCS stores use ETag/generation CAS instead of the epoch key.

## Why it matters

The instance that is fenced is the **sole legitimate leader** — it holds the lease and nothing else is running. Consequences:

1. `_commit` (`app.py:498-513`) has no handler, so `StateFenceError` propagates out of `pipeline.run`. `_run_pipeline` absorbs it as a leadership transition (`app.py:1237-1238`) and returns cleanly; `_on_pipeline_done` then sees a clean completion with `run_stop` unset and calls `stop.set()` (`app.py:1181-1182`). The process shuts down and **exits 0** on the first checkpoint commit after acquiring.
2. Each restart re-acquires and bumps the epoch by exactly one, so ingestion is down for `(stored_epoch - 1)` cycles of at least one lease `ttl` plus startup. A long-lived HA pair that has failed over many times has a correspondingly large `__fence_epoch__`, turning a few minutes into an effectively permanent outage. Under a `restart: on-failure` policy (exit 0 is not a failure) the process never comes back at all.
3. Every fenced cycle re-ingests the events pushed before the rejected commit, since the batch lands in Loki before the commit.
4. The documented recovery paths are blocked. `state set` / `state delete` both refuse `__fence_epoch__` (`src/sf2loki/statecmd.py:63`, `:181`, `:207`; pinned by `tests/test_statecmd.py:473` and `:517`), so the only way out is hand-editing the state JSON — and nothing in `docs/deployment/high-availability.md` mentions the failure mode or the fix.

Deleting the lease file is a recognised operational event in this project's own history: issue #50 named "lease-file deletion (operator cleanup)" as its trigger. #50's fix (`_read` raising `_LeaseReadError` on transient `OSError`, `_hold` treating absence as contested) and #47's fix (the epoch token) landed together in 6e6ec77 and were never reconciled with each other.

## Proposed approach

Seed the winning epoch from the **maximum** of the lease's epoch and the state document's persisted epoch, so the fence stays globally monotonic even when the lease is lost. Raising the epoch is always safe: it is strictly greater than both observed values, so it cannot let a genuinely stale leader through, and two standbys racing an expired lease still compute the same value and are still resolved by the existing rename + verify-read discipline.

1. Add a fresh-read accessor to the file store, e.g. `FileCheckpointStore.persisted_epoch() -> int | None`, returning `int(self._read_file_fresh().get(_EPOCH_KEY))` (or `None` when absent/unset). It must bypass `_cache` for the same reason `_commit_many_epoch_fenced` does (`file_store.py:274-283`).
2. Add an optional `epoch_floor: Callable[[], int | None] | None = None` constructor argument to `FileLeaseCoordinator`. In `_acquire`, replace `file_lease.py:207` with a floor-aware derivation: `base = max(lease.epoch if lease is not None else 0, epoch_floor() or 0)` then `new_epoch = base + 1`. Evaluate the floor **at each acquire**, not once at startup, so a standby promoted after another instance advanced the document still clears it. A raising/failing floor callable must be logged and treated as `0` (never fatal — leader election must not depend on the state backend being readable).
3. Wire it in the composition root next to the existing epoch plumbing (`app.py:937-948`): pass `epoch_floor=getattr(state, "persisted_epoch", None)` (duck-typed, exactly like `set_fence`/`set_epoch`/`commit_many` elsewhere), so a non-file store simply supplies no floor.
4. Log at WARNING when the floor exceeds the lease epoch — that is the signal the lease file was lost or rewritten, and it is worth surfacing.
5. Add the operator escape hatch and document it: allow `sf2loki state set __fence_epoch__ <n>` when `--force` is passed (`statecmd.py:181`, `:207`), and add a short recovery note to `docs/deployment/high-availability.md`'s fencing section describing the symptom (`stale leader (epoch N) rejected` on a sole leader) and the fix.

---

Imported from GitHub issue #99 on 2026-08-14, when this repo migrated from GitHub Issues to Backlog.md. The original issue has been deleted; its verbatim body, labels and comments are preserved in `archive/issues-dump.json` (`jq '.[] | select(.number == 99)' archive/issues-dump.json`).

<sub>Filed from the 2026-07-30 full-repo audit (11 finder lanes + adversarial verification per finding).</sub>
<!-- SECTION:DESCRIPTION:END -->

## Acceptance Criteria
<!-- AC:BEGIN -->
- [ ] #1 `FileLeaseCoordinator` accepts an optional `epoch_floor` callable and derives the winning epoch as `max(lease_epoch, floor) + 1` in `_acquire` (replacing `src/sf2loki/coordinate/file_lease.py:207`).
- [ ] #2 `FileCheckpointStore.persisted_epoch()` reads `__fence_epoch__` fresh from disk (never from `_cache`) and returns `None` when the file or key is absent.
- [ ] #3 `app.py` passes the store's `persisted_epoch` as the floor for `coordinate.type: file_lease`, duck-typed so `s3`/`gcs` stores (no such method) keep working unchanged.
- [ ] #4 Test: state doc carries `__fence_epoch__ = "7"`, lease file **absent** → `_acquire` yields epoch 8 and a subsequent `commit` succeeds (this is the regression test for the reproduction above; it fails before the fix with `StateFenceError`).
- [ ] #5 Test: state doc carries `__fence_epoch__ = "7"`, lease file present but **corrupt/non-JSON** → `_acquire` yields epoch 8, not 1.
- [ ] #6 Test: lease epoch 9 with a stored epoch of 3 → new epoch is 10 (the lease still wins when it is ahead; the floor only ever raises).
- [ ] #7 Test: a floor callable that raises is logged and treated as absent — acquisition still succeeds with `lease_epoch + 1`.
- [ ] #8 Test: `epoch_floor=None` (or a store with no `persisted_epoch`) reproduces today's derivation exactly; existing epoch tests in `tests/coordinate/test_file_lease.py` (`:97-106`, `:344-358`, `:404-429`) stay green.
- [ ] #9 Test: `state set __fence_epoch__ N --force` succeeds and writes the key; without `--force` it still exits non-zero with the "reserved" message (`tests/test_statecmd.py:473`, `:517` updated accordingly).
- [ ] #10 `docs/deployment/high-availability.md` fencing section documents the symptom, the automatic reconciliation, and the `--force` recovery command.
- [ ] #11 `just gate` green.
<!-- AC:END -->

## Definition of Done
<!-- DOD:BEGIN -->
- [ ] #1 just gate is green (ruff check + ruff format --check + mypy src + pytest) — run it, don't assert it
- [ ] #2 just gen-config run and its output committed, if config.py changed (CI drift gate fails otherwise)
- [ ] #3 committed straight to main with a conventional-commit message, and pushed
<!-- DOD:END -->
