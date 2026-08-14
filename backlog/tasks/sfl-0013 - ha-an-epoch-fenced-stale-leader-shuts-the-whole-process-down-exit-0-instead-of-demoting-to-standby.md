---
id: SFL-0013
title: >-
  ha: an epoch-fenced stale leader shuts the whole process down (exit 0) instead
  of demoting to standby
status: To Do
assignee: []
created_date: '2026-08-14 16:58'
labels:
  - followup
  - phase-1
milestone: m-0
dependencies: []
references:
  - 'https://github.com/rknightion/sf2loki/issues/97'
ordinal: 13000
---

## Description

<!-- SECTION:DESCRIPTION:BEGIN -->
## What

`App._run_pipeline` (`src/sf2loki/app.py:1227-1238`) absorbs `StateFenceError` and returns normally, documented as "a leadership transition, not a fatal crash — the coordinator drives the move back to standby". The pipeline task therefore **completes cleanly**, and the done callback misclassifies that completion as a finite run:

```python
# src/sf2loki/app.py:1168-1182
def _on_pipeline_done(task: asyncio.Task[None], run_stop: asyncio.Event) -> None:
    if task.cancelled():
        return
    exc = task.exception()
    if exc is not None:
        crash.append(exc)
        stop.set()
        return
    # Clean completion while we never asked it to stop means the sources
    # exhausted on their own (a finite run) -> shut down. ...
    if not run_stop.is_set():
        stop.set()          # <-- GLOBAL stop event
```

`run_stop` is only ever set by `_stop_acquisition` (`src/sf2loki/app.py:1281-1287`), reached from `on_lose` (`src/sf2loki/app.py:1198-1206`) — i.e. only after the coordinator itself has noticed the loss. There are two fences, and only one of them satisfies that precondition:

- **Boolean fence** (`src/sf2loki/coordinate/file_lease.py:126-135`, `src/sf2loki/coordinate/k8s_lease.py:190-198`), checked first in `FileCheckpointStore.commit_many` (`src/sf2loki/state/file_store.py:216-228`). When it raises, the coordinator has already set `_is_leader = False` in `_hold`'s finally and awaits `on_lose` in the same task step (`src/sf2loki/coordinate/file_lease.py:163-168`); `on_lose` reaches `run_stop.set()` with no await boundary in between, so `run_stop` is set and the callback correctly leaves the process up. This is the case the docstring was written for.
- **Epoch fence** (`src/sf2loki/state/file_store.py:273-296`, wired for `coordinate.type: file_lease` + the file store at `src/sf2loki/app.py:938-948`, added by #47) is the opposite case *by construction*: it exists to reject a stale leader's commit **before** its local boolean has caught up. `_hold` re-reads the lease only once per `renew_interval` (`src/sf2loki/coordinate/file_lease.py:241-270`; defaults `renew_interval: 10s`, `ttl: 30s`, `src/sf2loki/config.py:1092-1101`), and `docs/deployment/high-availability.md:104-108` documents exactly that lag. So whenever the epoch fence fires, `is_leader` is still `True`, `run_stop` is unset, and the callback sets the global stop.

Propagation path, with nothing swallowing or retrying the error: `Pipeline._commit` (`src/sf2loki/app.py:497-509`) → `_flush` → `_consume` → `Pipeline.run` re-raises the consumer exception (`src/sf2loki/app.py:284-290`) → `_drain_with_grace` (`src/sf2loki/app.py:596-602`) → absorbed at `src/sf2loki/app.py:1237`. The file store has no commit-retry wrapper and `OrgView` forwards verbatim (`src/sf2loki/state/org_view.py:65-68`).

With `crash` empty, `App.run` returns without raising (`src/sf2loki/app.py:1224-1225`) and `cli.main` returns `0` (`src/sf2loki/cli.py:269-270`).

Reproduced with a stub pipeline raising `StateFenceError` under a coordinator that holds leadership until the global stop fires: the process logs `checkpoint commit fenced — leadership lost; standing by`, then `leadership lost — standing by`, and `App.run()` returns cleanly — a full shutdown with exit code 0 while claiming to stand by.

Note that for a long-running service the `not run_stop.is_set()` branch is otherwise only reachable when there are no sources at all (`Pipeline.run` returns immediately at `src/sf2loki/app.py:256`), so the fence-absorbed completion is in practice the main way that branch fires.

## Why it matters

`file_lease` HA pair on a shared NFS/EFS state volume. Leader A's lease renewal starts failing (NFS write errors, a stall, or a VM pause); `_hold` tolerates renewal failure for a full `ttl` before surrendering (`src/sf2loki/coordinate/file_lease.py:286-300`), while standby B takes over at `ttl` expiry and bumps the epoch to N+1. Inside that overlap A's consumer flushes an in-flight batch, and the commit is epoch-fenced (`stored N+1 > mine N`) — the fence working exactly as designed. Instead of demoting to standby and waiting to re-acquire, A's whole process shuts down.

Consequences:

- The HA pair silently degrades to a single instance. Under `systemd Restart=on-failure` or docker `restart: on-failure`, A never comes back, because the clean exit looks like a deliberate stop.
- The degradation is invisible to the shipped observability: `sum(sf2loki_leader)` is still exactly `1`, which `docs/deployment/high-availability.md:110-117` documents as the healthy state. There is no alert on replica count, so the missing hot spare is only discovered the next time the surviving leader dies — at which point ingestion stops with no failover.
- The log line ("standing by") and the exit code (0, "asked to stop") both describe something the process is not doing.

No data loss: the batch already landed in Loki and the new leader owns the checkpoints, so semantics stay at-least-once. The defect is loss of redundancy plus a misleading exit code.

## Proposed approach

Distinguish "fenced" from "finite sources exhausted" so the callback does not set the global stop for a fence-absorbed completion.

1. Make the fenced case explicit rather than indistinguishable from a clean return. Either:
   - have `_run_pipeline` (`src/sf2loki/app.py:1227-1238`) record the fence on the per-acquisition state (e.g. `current["fenced"] = True`, or a `fenced: list[bool]` closure alongside `crash` at `src/sf2loki/app.py:1165`) and have `_on_pipeline_done` skip `stop.set()` when that flag is set; or
   - re-raise a private marker (`class _Fenced(Exception)`) that `_on_pipeline_done` recognises before the `crash.append` branch and treats as a leadership transition: no `crash.append`, no `stop.set()`.
   Whichever shape, reset the flag per acquisition so a later fence-free finite run still shuts down.
2. On the fenced path, demote immediately rather than waiting up to `renew_interval` for `on_lose`: drop the leader gauge (`self._metrics.leader.set(0)`), mark not-ready (`self._health.set_not_ready("standby")`) and invalidate the cached checkpoint document (`self._pipeline.reset_state()`). The pipeline is already fully torn down at that point (`Pipeline.run` cancels its producers in its finally, `src/sf2loki/app.py:424-427`), so nothing can commit against the invalidated cache. `on_lose` repeats all three idempotently when the coordinator catches up, so this is additive.
3. Leave the crash path and the finite-run path unchanged: a non-fence exception must still append to `crash`, set the global stop, and exit nonzero.
4. Do **not** "fix" this by exiting nonzero on a fence — that still drops the replica out of the pair for a restart cycle. Demote-in-place is the intent stated in `_run_pipeline`'s docstring.
5. Add a short note to the Fencing section of `docs/deployment/high-availability.md` (around line 96-108) stating that a fenced commit demotes the replica to standby in place and never terminates the process.

---

Imported from GitHub issue #97 on 2026-08-14, when this repo migrated from GitHub Issues to Backlog.md. The original issue has been deleted; its verbatim body, labels and comments are preserved in `archive/issues-dump.json` (`jq '.[] | select(.number == 97)' archive/issues-dump.json`).

## Additional evidence (parallel review lanes)

There is also a non-racy route to the same exit: at src/sf2loki/coordinate/file_lease.py:271-287, a stale leader whose lease read and verify re-read both fail with `_LeaseReadError` falls through to `self._write(now, self._epoch)` and keeps `_is_leader = True` indefinitely — `on_lose` never fires, `run_stop` is never set, and every subsequent commit is epoch-fenced by the new leader's document, so the process reliably logs "checkpoint commit fenced — leadership lost; standing by" and then fully exits with code 0. That blind-rewrite path is tracked separately in #96; this issue owns the fence-to-demotion (not fence-to-exit) behaviour in app.py.

The genuine demotion path is unaffected and must stay that way: src/sf2loki/coordinate/file_lease.py:174-177 sets `_is_leader = False` and then awaits `on_lose`, which reaches `run_stop.set()` (src/sf2loki/app.py:1281-1287 via app.py:1198-1201) with no intervening await, so the boolean fence can never fire with `run_stop` unset — only the epoch fence can.

<sub>Filed from the 2026-07-30 full-repo audit (11 finder lanes + adversarial verification per finding).</sub>
<!-- SECTION:DESCRIPTION:END -->

## Acceptance Criteria
<!-- AC:BEGIN -->
- [ ] #1 A fence-absorbed pipeline completion no longer sets the global stop: test with a stub pipeline whose `run()` raises `StateFenceError` and a coordinator that holds leadership until told to stop — `App.run()` does not return, `on_lose` has not been called, and the coordinator can subsequently drive `on_lose`/`on_acquire` so a fresh pipeline run starts (`pipeline.runs == 2`).
- [ ] #2 Test: immediately after a fenced completion and before `on_lose` fires, `sf2loki_leader` reads `0` and `/readyz` reports `503` with reason `standby`.
- [ ] #3 Test: a fence raised during a deliberate demotion (`run_stop` already set) still leaves the process up and is not counted as a crash (guards the existing boolean-fence drain path).
- [ ] #4 Test: clean completion with `run_stop` unset and no fence (finite/empty-source run) still sets the global stop and `App.run()` returns — the branch at `src/sf2loki/app.py:1181` must not be removed outright.
- [ ] #5 Test: a non-fence pipeline exception still propagates out of `App.run()` — `tests/test_app_integration.py:462-486` keeps passing unchanged.
- [ ] #6 Integration-level test over the real `FileLeaseCoordinator` + `FileCheckpointStore`: with the lease file advanced to epoch N+1 by a foreign holder while the coordinator's `is_leader` is still `True`, a commit raises `StateFenceError`, the app demotes to standby, and the process stays alive.
- [ ] #7 `docs/deployment/high-availability.md` Fencing section states the demote-not-exit behaviour.
- [ ] #8 `just gate` green (ruff + `mypy --strict` + pytest).
<!-- AC:END -->

## Definition of Done
<!-- DOD:BEGIN -->
- [ ] #1 just gate is green (ruff check + ruff format --check + mypy src + pytest) — run it, don't assert it
- [ ] #2 just gen-config run and its output committed, if config.py changed (CI drift gate fails otherwise)
- [ ] #3 committed straight to main with a conventional-commit message, and pushed
<!-- DOD:END -->
