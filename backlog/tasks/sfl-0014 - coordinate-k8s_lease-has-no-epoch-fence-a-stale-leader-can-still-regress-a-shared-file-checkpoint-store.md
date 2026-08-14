---
id: SFL-0014
title: >-
  coordinate: k8s_lease has no epoch fence - a stale leader can still regress a
  shared file checkpoint store
status: To Do
assignee: []
created_date: '2026-08-14 16:58'
labels:
  - followup
  - phase-4
milestone: m-0
dependencies: []
references:
  - 'https://github.com/rknightion/sf2loki/issues/98'
ordinal: 14000
---

## Description

<!-- SECTION:DESCRIPTION:BEGIN -->
## What

The durable epoch fence added for #47 is wired only for the `file_lease` coordinator. The `k8s_lease` coordinator gets the lagging boolean fence and nothing else, so a `coordinate.type: k8s_lease` + `state.store: file` deployment on shared storage has no write-time fencing at all.

Composition root, `src/sf2loki/app.py`:

- `app.py:937` — `epoch_source: Callable[[], int | None] | None = None`.
- `app.py:938-948` — the `file_lease` branch sets **both** `fence = file_lease.check_fence` and `epoch_source = lambda: file_lease.epoch`.
- `app.py:949-960` — the `k8s_lease` branch sets **only** `fence = k8s_lease.check_fence`; `epoch_source` stays `None`.
- `app.py:966-968` — `set_epoch` is therefore never called on the store; only `set_fence` (`app.py:964-965`) is.
- `app.py:929` — `state = _build_state(cfg, exclusive_lock=cfg.coordinate.type == "noop")` disables the file store's process-lifetime `.lock` flock sidecar for **any** non-noop coordinator (honoured at `file_store.py:118-120`), removing the last local backstop.

`K8sLeaseCoordinator` has no epoch to wire: its fencing surface is `is_leader` (`src/sf2loki/coordinate/k8s_lease.py:186`), `check_fence` (`k8s_lease.py:189`) and `holder` (`k8s_lease.py:203`) — no counterpart to `file_lease.py:143 def epoch`. The module never reads or writes `spec.leaseTransitions`; `_Lease` (`k8s_lease.py:57-90`), `_LeaseBody` (`k8s_lease.py:93-106`) and `_RealLeaseAdapter._from_v1_lease` (`k8s_lease.py:494-513`) carry only `holder`/`renew_time`/`duration`/`resource_version`.

Consequence in the store. With `_epoch_fn is None`, `FileCheckpointStore.commit_many` takes the cache path at `src/sf2loki/state/file_store.py:222-226`: `_ensure_loaded()` (loads the document once, `file_store.py:159-164`) → `self._cache.update(items)` → `_flush()`. `_flush` (`file_store.py:166-190`) serialises the **whole** cache and `os.replace`s the file with no compare-and-swap. `delete()` mirrors it (`file_store.py:238-248`). So a commit from an instance whose cache predates another leader's writes silently rewrites every key in the document, not just the keys in `items`. The epoch-fenced path (`file_store.py:274-296`) is what re-reads fresh and rejects `stored > mine` — and it is unreachable here.

The dual-writer window is real for this coordinator:

- `check_fence` (`k8s_lease.py:189-200`) reads only `self._is_leader`, which flips in `_hold`'s `finally` (`k8s_lease.py:243-251`).
- `_hold` (`k8s_lease.py:322-395`) evaluates leadership once per `renew_interval`; `_read` swallows API errors and returns `None` (`k8s_lease.py:420-428`), so the foreign-holder surrender at `k8s_lease.py:334-341` cannot fire while the API server is unreachable; the renew-failure surrender only triggers once `now - last_ok >= lease_duration` (`k8s_lease.py:377-385`).
- A standby that can reach the API takes over as soon as `staleness >= duration` (`_Lease.is_stale`, `k8s_lease.py:81-90`).
- `App.on_lose` → `Pipeline.reset_state()` (`app.py:1198-1207`, `app.py:517-528`) invalidates the cache only **after** demotion is noticed, i.e. after the window has closed.

Nothing rejects or warns about the combination:

- `_build_state`/`build_store` (`app.py:617-618`) never inspects `cfg.coordinate`.
- No validator ties `CoordinateConfig` to `StateConfig` — the relevant ones are `StateConfig._require_bucket_for_remote` (`src/sf2loki/config.py:1069-1074`), `FileLeaseConfig`'s renew/ttl check (`config.py:1111`), `K8sLeaseConfig`'s renew/duration check (`config.py:1161`) and `Config._validate_org_topology` (`config.py:1384`).
- `doctor` dispatches on `coordinate.type` (`src/sf2loki/doctor.py:596-606`) and never cross-checks the configured state backend.
- Docs are advisory only: `docs/deployment/high-availability.md:88` recommends `s3`/`gcs` for `k8s_lease`, and the fencing section scopes the epoch mechanism to "the `file_lease` coordinator specifically" (`high-availability.md:104-110`). `deploy/helm/values.yaml:179` notes the emptyDir file store is single-replica-only, and `deploy/helm/templates/deployment.yaml:9` guards only `replicaCount>1` without `ha.enabled` — a shared RWX volume mounted at `stateDir` via `extraVolumes`, or the off-cluster `k8s_lease.kubeconfig` path against an NFS state dir, both render fine.

#47's body cited `k8s_lease.py` as part of the same defect, but the epoch work landed after the coordinator (`0ec2a18` k8s lease 2026-07-02 14:15 → `73101d7` epoch wiring 17:37) and covered `file_lease` only. #47 is closed with no comments recording the k8s path as descoped, and no test pins the current behaviour (`set_epoch` appears only in `tests/state/test_file_store.py` and `tests/test_statecmd.py`; `tests/test_app_integration.py:389` asserts only the missing-extra `ConfigError`).

## Why it matters

Two replicas with `coordinate.type: k8s_lease` sharing `state.store: file` on an RWX (NFS/EFS) volume:

1. Leader A's last successful Lease renew is at T0; it then loses API-server connectivity (or stalls the event loop past `lease_duration`).
2. Standby B observes the Lease's `resourceVersion` unchanged for `lease_duration` on its own monotonic clock, takes over, and commits advanced watermarks into the shared state file.
3. A stays `is_leader == True` until its next renew tick at or after T0 + `lease_duration` — up to a full `renew_interval` after B's takeover, longer across a stall. Every commit A makes in that window passes `check_fence`, then `_flush` (`file_store.py:166-190`) rewrites the entire document from A's pre-takeover cache, wiping B's advanced watermarks for **all** keys, including sources A is not even running.
4. B (or the next leader after a restart/demote, which re-reads the file) resumes from the regressed watermarks and re-queries and re-pushes everything since them.

No data is lost (commit still follows a successful push), but the re-ingest is unbounded by the documented one-lease-duration window, costs Salesforce API calls and Loki ingest, and produces duplicates outside Loki's per-stream reject window. There is no error, no log warning and no doctor FAIL anywhere in the path, so an operator has no signal that the topology they configured is unfenced — while the equivalent `file_lease` topology is fenced and the `s3`/`gcs` stores get ETag/generation CAS for free.

## Proposed approach

Two parts; part 2 alone is acceptable as a stop-gap but part 1 is the actual fix.

**1. Give `K8sLeaseCoordinator` an epoch and wire it.** `spec.leaseTransitions` is the canonical monotonic takeover counter in `coordination.k8s.io/v1` (client-go's leaderelection increments it whenever `holderIdentity` changes), so it needs no new storage:

- Add `transitions: int | None` to `_Lease` (`k8s_lease.py:57-90`) and `transitions: int` to `_LeaseBody` (`k8s_lease.py:93-106`); map `spec.lease_transitions` in `_RealLeaseAdapter._from_v1_lease` (`k8s_lease.py:494-513`, treat missing as `None` per the #62 optional-field discipline) and set `lease_transitions=body.transitions` in `create_lease`/`replace_lease` (`k8s_lease.py:459-492`).
- In `_acquire` (`k8s_lease.py:288-320`): a create writes `transitions=1`; a takeover replace writes `(lease.transitions or 0) + 1`. Store the acquired value in `self._epoch`.
- In `_hold` (`k8s_lease.py:322-395`): renewals **preserve** the current value (never increment) so a renew does not bump the epoch; refresh `self._epoch` from the re-read lease when present.
- Expose `@property def epoch(self) -> int` mirroring `file_lease.py:143-147` (0 before the first acquisition).
- In `app.py:949-960`, add `epoch_source = lambda: k8s_lease.epoch` so `app.py:966-968` installs it.

**2. Close the silent-misconfiguration hole.** Add a startup check that `coordinate.type` in `{file_lease, k8s_lease}` with `state.store: file` is a shared-storage topology the operator has to mean: at minimum a loud WARN log in `App.build`, plus a `doctor` check that reports the coordinator/state-backend pairing (extend `_check_coordinator`, `doctor.py:596-606`, or add a sibling check) so `sf2loki doctor` surfaces `k8s_lease` + `file` explicitly rather than passing silently.

**Known trap to document with the fix.** The persisted `__fence_epoch__` (`file_store.py:19`) outlives the Lease. Deleting and recreating the Lease resets `leaseTransitions` to 0/absent, after which every legitimate new leader is rejected by `_commit_many_epoch_fenced` (`file_store.py:284-292`) forever. `file_lease` has the same hazard if the lease file is deleted. The recovery is deleting the reserved key (`src/sf2loki/statecmd.py:35` already documents `__fence_epoch__` as internal bookkeeping) — state it in `docs/deployment/high-availability.md`'s fencing section and in `docs/deployment/state.md`.

Docs to update alongside: `docs/deployment/high-availability.md:104-110` (the epoch mechanism is no longer `file_lease`-specific) and `high-availability.md:88` (keep the `s3`/`gcs` recommendation, but say what happens with `file` on shared storage).

---

Imported from GitHub issue #98 on 2026-08-14, when this repo migrated from GitHub Issues to Backlog.md. The original issue has been deleted; its verbatim body, labels and comments are preserved in `archive/issues-dump.json` (`jq '.[] | select(.number == 98)' archive/issues-dump.json`).

<sub>Filed from the 2026-07-30 full-repo audit (11 finder lanes + adversarial verification per finding).</sub>
<!-- SECTION:DESCRIPTION:END -->

## Acceptance Criteria
<!-- AC:BEGIN -->
- [ ] #1 `K8sLeaseCoordinator` exposes `epoch: int`, sourced from the Lease's `leaseTransitions`, bumped exactly once per winning acquisition and unchanged by renewals.
- [ ] #2 `app.py`'s `k8s_lease` branch sets `epoch_source`, so `FileCheckpointStore.set_epoch` is installed for `coordinate.type: k8s_lease` exactly as it is for `file_lease`.
- [ ] #3 `_RealLeaseAdapter` reads and writes `spec.leaseTransitions`, tolerating a missing value (`None` → treated as 0) per the #62 optional-field rule.
- [ ] #4 A `k8s_lease` + `state.store: file` config is no longer silent: a WARN at startup and a `doctor` result that names the pairing.
- [ ] #5 `docs/deployment/high-availability.md` fencing section covers both lease coordinators and documents the `__fence_epoch__` reset recovery.
- [ ] #6 Test in `tests/coordinate/test_k8s_lease.py`: the fake adapter records `leaseTransitions`; a create yields `epoch == 1`; N renewals leave `epoch` unchanged; a takeover of a lease with `transitions=3` yields `epoch == 4`; a lease with `leaseTransitions` unset is taken over without error and yields `epoch == 1`.
- [ ] #7 Test in `tests/test_app_integration.py`: building the app with `coordinate.type: k8s_lease` (injected/fake api factory or the `find_spec` guard pattern used at `tests/test_app_integration.py:389`) installs **both** the fence and the epoch source on the file store — asserted by observing that a commit takes the epoch-fenced path, not just by attribute presence.
- [ ] #8 Test pinning the regression: a store with `set_fence(lambda: None)` (boolean still wrongly says leader) and `set_epoch` from a stale k8s epoch is rejected with `StateFenceError` when the file's `__fence_epoch__` is newer — mirroring `tests/state/test_file_store.py:313-330` but driven through the k8s coordinator's `epoch`.
- [ ] #9 `just gate` green.
<!-- AC:END -->

## Definition of Done
<!-- DOD:BEGIN -->
- [ ] #1 just gate is green (ruff check + ruff format --check + mypy src + pytest) — run it, don't assert it
- [ ] #2 just gen-config run and its output committed, if config.py changed (CI drift gate fails otherwise)
- [ ] #3 committed straight to main with a conventional-commit message, and pushed
<!-- DOD:END -->
