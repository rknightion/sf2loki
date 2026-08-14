---
id: SFL-0018
title: >-
  cli: `sf2loki state` lock refusal never fires under a non-noop coordinator -
  the live leader silently reverts an operator's checkpoint repair
status: To Do
assignee: []
created_date: '2026-08-14 16:58'
labels:
  - followup
  - phase-5
milestone: m-0
dependencies: []
references:
  - 'https://github.com/rknightion/sf2loki/issues/102'
ordinal: 18000
---

## Description

<!-- SECTION:DESCRIPTION:BEGIN -->
## What

`sf2loki state show|set|delete` relies on the file store's sidecar flock to detect a running daemon, but the daemon does not take that flock in any HA deployment, so the guard is dead code exactly where it is most needed.

- `statecmd.py:110` builds the store with `exclusive_lock=not force` (flock ON unless `--force`).
- `app.py:929` builds the daemon's store with `exclusive_lock=cfg.coordinate.type == "noop"` (issue #49: a real coordinator is the exclusivity mechanism, and a process-lifetime flock crash-loops a promoted standby). Under `file_lease`/`k8s_lease`, `file_store.py:117-120` returns before `os.open`, so the `<state file>.lock` sidecar is never created or held.
- Consequently the CLI's flock acquisition succeeds uncontested, `StateFileLockError` is never raised, and the refusal branch at `statecmd.py:117-126` is unreachable. The promise made at `statecmd.py:12-21`, `docs/deployment/state.md:33` ("`state show/set/delete` refuses to run while the daemon holds the lock") and `docs/reference/cli.md:113` does not hold in the topology `docs/deployment/high-availability.md:86-87` prescribes for the file backend: `coordinate.type: file_lease` with `state.store: file` on a shared NFS/EFS export - i.e. precisely the case where the state file is reachable from an operator box while the leader is live.
- The epoch fence does not substitute for the missing guard. `statecmd` keeps `__fence_epoch__` inside the store's `_cache` (it is filtered only from the `show` listing, `statecmd.py:63,160`), so its whole-document flush writes the epoch back unchanged and the daemon's epoch-CAS check at `file_store.py:288-292` never trips.
- No other layer catches it: `cli.py:243-250` passes only config path/key/value/`force` into `statecmd`, and `config.py` has no validator rejecting `state.store: file` alongside a non-noop coordinator.

Two distinct clobber shapes on the daemon side, depending on whether `set_epoch` is installed:

1. `coordinate.type: file_lease` + `state.store: file` (the documented HA pairing) - `epoch_source` is wired at `app.py:948,966-968`, so `_commit_many_epoch_fenced` (`file_store.py:283-297`) re-reads the file fresh and merges only the keys it is committing. Unrelated operator edits survive; the repaired key does not. `eventlog_objects_source.py:292` reloads its watermark from the store each cycle (served from `_cache`), and `app.py:505-510` commits that source's own token after every successful push, so the leader's next push for the repaired key writes the poison-derived value straight back over it. Where the wedged source emits nothing and therefore commits nothing, the daemon's `_cache` simply stays stale and the repair does not take effect until some unrelated key's commit refreshes the cache. Either way the repair is unreliable on a live leader, and neither side logs anything.
2. `coordinate.type: k8s_lease` + `state.store: file` (a shared RWX volume; not the documented pairing, but nothing forbids it) - `epoch_source` stays `None` (`app.py:955-960`), so the daemon's commit path is `file_store.py:222-226`: `_ensure_loaded` (one cached read at startup) + `_cache.update(items)` + `_flush()` writes the **whole** stale document. That reverts unrelated `state set` values and resurrects `state delete`d keys.

The object-store backends are unaffected: their ETag/generation CAS surfaces a live concurrent writer as `StateStoreConflictError`, which `statecmd.py:127-132` reports actionably.

Test coverage does not cover the HA case: `tests/test_statecmd.py:546-620` simulate "the daemon is running" with a default `FileCheckpointStore` (`exclusive_lock=True`), which only models `coordinate.type: noop`.

## Why it matters

`docs/deployment/state.md` is the poison-checkpoint runbook, reached from the `sf2loki ingest lag high` / `sf2loki no recent Loki push` alerts, and it is the tool of last resort for a wedged watermark. Nothing in the runbook tells the operator to stop the leader first - the flock refusal was the mechanism meant to enforce that. On a `file_lease` HA pair the operator runs `sf2loki state set eventlog_objects:LoginEvent <good-watermark>`, gets `sf2loki: set eventlog_objects:LoginEvent = <good-watermark>` and exit 0, and within one push interval the leader has written the poison value back. The source stays wedged, the alert keeps firing, and the operator's mental model says the checkpoint was already repaired - so the next escalation step is taken against a false premise. Under `k8s_lease` with a file store the blast radius is wider: an unrelated key's `delete` reappears and other keys regress, producing unexplained duplicate re-ingestion windows.

## Proposed approach

Make `statecmd` coordinator-aware, since the flock can no longer answer "is a daemon running?".

1. In `statecmd._run`, after `load(config_path)` succeeds and before `build_store`, inspect `cfg.coordinate.type`. When it is not `"noop"` and `force` is false, print an explicit refusal to stderr and return `_OPERATION_ERROR_EXIT_CODE`:
   - state that the daemon does not hold the state-file lock under a coordinator (so the lock cannot protect this command), that the leader must be stopped (or the whole pair) before repairing checkpoints, and that `--force` bypasses the check.
2. When `cfg.coordinate.type == "file_lease"`, enrich that message with live lease facts: read `cfg.coordinate.file_lease.path` (same JSON shape parsed at `coordinate/file_lease.py:327-361`: `holder`, `expires_at`, `epoch`) and, when the document parses and `expires_at` is in the future, name the current holder and expiry ("`holder=<id>` is renewing the lease, expires at `<ts>`"). An absent, corrupt, or expired lease still refuses, but says the lease looks stale so no leader is likely live. Reuse a small local parser or expose a module-level read helper rather than instantiating `FileLeaseCoordinator` (its `_read` is private and its constructor pulls in run-loop state).
3. Apply the refusal to all three subcommands, matching the existing flock semantics (`show` refuses today under `noop`, and a `show` against a live leader reports values that may be mid-flight). Keep `--force` as the single escape hatch for both the flock and this check, and update its help text at `cli.py:160-164`, `cli.py:172-176`, `cli.py:184-188`.
4. Docs:
   - `docs/deployment/state.md:33` - correct the `file` row: the flock only guards `coordinate.type: noop`; under a coordinator the CLI refuses on the coordinator instead, and repairs require stopping the leader.
   - `docs/deployment/state.md` - add a short "repairing checkpoints in an HA pair" step to the runbook: stop the leader (and the standby, or it takes over and re-wedges), repair, restart.
   - `docs/reference/cli.md:113,123` - restate `--force` as bypassing both the exclusive lock and the HA refusal.
   - `docs/deployment/high-availability.md` - cross-reference from the shared-state section (around line 81-91) that live checkpoint repair is not safe on a running leader.
5. Optional hardening, separable: reject `state.store: file` with `coordinate.type: k8s_lease` in `config.py`, or wire `epoch_source` for `k8s_lease` too, so the whole-document-from-stale-cache rewrite at `file_store.py:222-226` cannot reach a shared file. Note in the change which option was taken and why.

---

Imported from GitHub issue #102 on 2026-08-14, when this repo migrated from GitHub Issues to Backlog.md. The original issue has been deleted; its verbatim body, labels and comments are preserved in `archive/issues-dump.json` (`jq '.[] | select(.number == 102)' archive/issues-dump.json`).

<sub>Filed from the 2026-07-30 full-repo audit (11 finder lanes + adversarial verification per finding).</sub>
<!-- SECTION:DESCRIPTION:END -->

## Acceptance Criteria
<!-- AC:BEGIN -->
- [ ] #1 `statecmd._run` refuses `set`/`delete`/`show` with a non-zero exit when `cfg.coordinate.type` is `file_lease` or `k8s_lease` and `--force` was not passed; the message names the coordinator type and says the leader must be stopped.
- [ ] #2 `--force` still performs the operation under a non-noop coordinator (both the flock bypass and this new refusal), unchanged from today's `--force` path.
- [ ] #3 `coordinate.type: noop` behaviour is byte-identical to today: the flock refusal at `statecmd.py:117-126` remains the only guard, and existing `tests/test_statecmd.py:546-620` pass unmodified.
- [ ] #4 For `file_lease` with a readable, unexpired lease document, the refusal message includes the lease holder and `expires_at`; with an absent/corrupt/expired lease it still refuses but says the lease looks stale.
- [ ] #5 Test: `state set` against a config with `coordinate.type: file_lease` and `state.store: file` returns exit 1 and leaves the state file byte-identical (pins that no write happened).
- [ ] #6 Test: the same config with `--force` writes the key (pins the escape hatch).
- [ ] #7 Test: `state show` and `state delete` under `coordinate.type: k8s_lease` both refuse without `--force`.
- [ ] #8 Test: the refusal message contains the holder id and expiry parsed from a hand-written `file_lease` lease JSON with a future `expires_at`, and the stale-lease wording for one with a past `expires_at`.
- [ ] #9 Regression test pinning the mechanism this issue exists for: a `FileCheckpointStore(path, exclusive_lock=False)` standing in for an HA daemon does not block `statecmd`'s flock acquisition - assert `run_state_set` returns 0 today, and 1 after the fix, against that same fixture.
- [ ] #10 `docs/deployment/state.md:33`, the new HA repair step, `docs/reference/cli.md:113,123` and the `docs/deployment/high-availability.md` cross-reference all land; no doc still claims the lock refusal protects an HA deployment.
- [ ] #11 `just gate` green.
<!-- AC:END -->

## Definition of Done
<!-- DOD:BEGIN -->
- [ ] #1 just gate is green (ruff check + ruff format --check + mypy src + pytest) — run it, don't assert it
- [ ] #2 just gen-config run and its output committed, if config.py changed (CI drift gate fails otherwise)
- [ ] #3 committed straight to main with a conventional-commit message, and pushed
<!-- DOD:END -->
