---
id: SFL-0040
title: >-
  ha: object-store lease coordinator (s3_lease / gcs_lease) for stateless
  platforms
status: To Do
assignee: []
created_date: '2026-08-14 16:58'
labels:
  - followup
  - phase-3
  - roadmap
milestone: m-4
dependencies: []
references:
  - 'https://github.com/rknightion/sf2loki/issues/124'
ordinal: 40000
---

## Description

<!-- SECTION:DESCRIPTION:BEGIN -->
# ha: object-store lease coordinator (s3_lease / gcs_lease) for stateless platforms

## What

`CoordinateConfig.type` accepts exactly `noop | file_lease | k8s_lease` (`src/sf2loki/config.py:1184`), and `src/sf2loki/coordinate/` holds only `base.py`, `file_lease.py`, `k8s_lease.py`. Neither real coordinator fits stateless non-Kubernetes compute:

- `file_lease` needs a shared POSIX filesystem. `FileLeaseConfig` is documented as "File lease on shared storage (NFS/EFS) for active-passive failover" (`src/sf2loki/config.py:1079`), and `docs/deployment/high-availability.md:86-88` instructs mounting the same NFS/EFS export on both hosts. Cloud Run has no such mount; on ECS Fargate an EFS mount is exactly the persistent volume the stateless deployment path exists to avoid.
- `k8s_lease` requires a Kubernetes API server and the `sf2loki[k8s]` extra (`src/sf2loki/config.py:1123`, `src/sf2loki/coordinate/k8s_lease.py`).

The docs steer operators straight into this hole. `docs/architecture.md:272-273` states "S3 and GCS need no mounted volume, so they're the fit for stateless compute (Fargate, Cloud Run, ECS with ephemeral storage)"; `README.md:433-437` and `docs/index.md:30` repeat it. But `docs/deployment/high-availability.md:86-91` maps coordinators to state backends as only:

- `file_lease` → shared NFS/EFS + `state.store: file`
- `k8s_lease` → `state.store: s3` or `gcs`

There is no row for "object-store state, no Kubernetes". That combination — the recommended stateless setup — has no coordinator at all, so HA is unreachable there.

The arbitration primitive already exists in-repo. `S3CheckpointStore` performs ETag compare-and-swap: `IfNoneMatch: "*"` for the first write and `IfMatch: <etag>` for updates (`src/sf2loki/state/s3_store.py:284-287`, `src/sf2loki/state/s3_store.py:335-338`), raising `StateStoreConflictError` on a 412. `GcsCheckpointStore` does the generation-precondition equivalent — `ifGenerationMatch: "0"` for the first write, the current generation for an update (`src/sf2loki/state/gcs_store.py:236-239`, `src/sf2loki/state/gcs_store.py:286-289`). Both extras are already declared (`pyproject.toml:29-30`: `s3 = ["aiobotocore>=2.21"]`, `gcs = ["gcloud-aio-storage>=9.0"]`). `FileLeaseCoordinator` (`src/sf2loki/coordinate/file_lease.py`, 387 lines) already implements acquire/hold/renew/takeover over an atomically-replaced `{holder, expires_at, epoch}` JSON document, and `K8sLeaseCoordinator` (`src/sf2loki/coordinate/k8s_lease.py`, 536 lines) already implements the variant where a lost CAS *is* the contention signal, so no pause-then-reread step is needed (`docs/deployment/high-availability.md:69-71`). An object with conditional writes gives the same document semantics as both.

## Why it matters

An operator deploys the documented stateless configuration — ECS Fargate or Cloud Run with `state.store: s3` — and wants automatic failover. No `coordinate.type` value serves them. The options are: bolt EFS onto a Fargate task purely to host a 100-byte lease file (reintroducing the volume the object-store checkpoint path removed), migrate to Kubernetes, or run a single task and absorb the full platform reschedule time on every host failure. That reschedule is minutes of ingestion gap. For Pub/Sub streaming sources the gap is bounded by Salesforce's retention window rather than lost outright, but EventLogFile and SOQL-polled lag accrues for the whole outage and the connector is dark to alerting.

The asymmetry is the point: the checkpoint document already lives in an object store with conditional writes strong enough to arbitrate leadership, and the lease is a strictly smaller problem than the checkpoint document already solved there.

## Proposed approach

**Config** (`src/sf2loki/config.py`)

- Extend the literal at `src/sf2loki/config.py:1184` to `Literal["noop", "file_lease", "k8s_lease", "s3_lease", "gcs_lease"]` and update the field description.
- Add `S3LeaseConfig` and `GcsLeaseConfig` alongside `FileLeaseConfig` (`src/sf2loki/config.py:1078`) and `K8sLeaseConfig` (`src/sf2loki/config.py:1123`). Mirror the connection fields of the corresponding state configs (`S3StateConfig` at `src/sf2loki/config.py:994`, `GcsStateConfig` at `src/sf2loki/config.py:1023`): `bucket`, `key`/`object_name`, plus `region`/`endpoint_url` for S3 and `service_file` for GCS. Lease-lifecycle fields match `FileLeaseConfig`: `ttl`, `renew_interval` (validated `< ttl/2`), `holder_id` (blank → `hostname-pid`).
- Add `s3_lease` / `gcs_lease` sub-model fields to `CoordinateConfig`, and a `model_validator` that (a) errors when the selected lease type has an empty `bucket`, mirroring `StateConfig._require_bokcet_for_remote` shape at `src/sf2loki/config.py:1068-1074`, and (b) errors when the lease object is the *same* bucket+key as the configured checkpoint object — the lease and the checkpoint document must be separate objects, since each keeps its own cached ETag/generation.

**Coordinators** (`src/sf2loki/coordinate/s3_lease.py`, `src/sf2loki/coordinate/gcs_lease.py`)

Reuse the `run` → `_acquire` → `_hold` → `_pause` loop shape and injected `utcnow`/`sleep` seams from `src/sf2loki/coordinate/k8s_lease.py`, storing `{holder, expires_at, epoch}` as a small JSON object:

- **First acquire**: conditional PUT with `IfNoneMatch: "*"` (S3) / `ifGenerationMatch: "0"` (GCS).
- **Renew and takeover**: conditional PUT with `IfMatch: <etag>` / `ifGenerationMatch: <generation>`. A 412 means another replica wrote first — that is the contention signal, so back off to standby with no pause-then-verify step, exactly as `k8s_lease` treats a 409.
- **Staleness on the observer's own clock, not the leader's wall-clock.** Do not judge expiry by comparing the leader-written `expires_at` against local wall-clock time — that is the defect closed in #51. Follow the `k8s_lease` pattern documented at `docs/deployment/high-availability.md:72-80`: track, on the observer's monotonic clock, how long since the lease object's ETag/generation last changed, and only contest once `ttl` has elapsed on that clock. Keep writing `expires_at` into the document for human inspection, but never read it back for takeover math. This makes the object-store coordinators NTP-independent like `k8s_lease`, unlike `file_lease`.
- **Never treat a transient read failure as "lease absent"** — the defect closed in #50. Distinguish a 404/`NoSuchKey` (genuinely absent, claimable) from a 5xx, timeout, or connection reset (unknown — back off without contesting). Apply bounded retry on transient errors, consistent with the state-store retry added in #44.
- **Epoch**: bump on every winning acquire/takeover, preserve verbatim across renewals, expose via an `epoch` property for parity with `file_lease`. It is not wired into the state store for these types — see wiring below.
- **Lazy import and thin adapter.** Keep the module importable and unit-testable without the extra installed, as `k8s_lease` does: a thin adapter (`read_lease`/`write_lease` over a small dataclass) with `aiobotocore` / `gcloud-aio-storage` imported inside the default factory only. `run()` owns the client lifecycle — enter the session/`Storage` context at the top and exit in `finally`, because the `Coordinator` protocol has no `close()` and `app.py` never closes the coordinator (`src/sf2loki/coordinate/CLAUDE.md`).

**Wiring** (`src/sf2loki/app.py`)

- Add branches to the dispatch at `src/sf2loki/app.py:938-961`, each raising `ConfigError` with the install hint when the extra is missing (copy the `importlib.util.find_spec` guard at `src/sf2loki/app.py:950-954`).
- Set `fence = coordinator.check_fence` only. Do **not** wire `set_epoch`: the durable epoch exists for the CAS-less file store (`src/sf2loki/app.py:944-948`), while the S3/GCS checkpoint stores' own ETag/generation CAS already rejects a losing writer (`docs/deployment/high-availability.md:110-113`). Both stores already accept the fence (`src/sf2loki/state/s3_store.py:184-191`, `src/sf2loki/state/gcs_store.py:139-146`).
- No change needed at `src/sf2loki/app.py:929` — `exclusive_lock=cfg.coordinate.type == "noop"` already generalises correctly to a new non-noop type.

**Doctor** (`src/sf2loki/doctor.py`)

`_check_coordinator` currently falls through to the Kubernetes probe for *any* type that is not `noop` or `file_lease` (`src/sf2loki/doctor.py:595-606`), so without a new branch an `s3_lease` deployment would silently probe Kubernetes and report a misleading result. Add explicit branches that do a HEAD plus a conditional-PUT/delete round-trip against a probe key (never the live lease object — same discipline as `_COORDINATOR_LEASE_PROBE_NAME` at `src/sf2loki/doctor.py:81`), reporting FAIL on a permissions error and distinguishing "conditional writes unsupported by this endpoint" for non-AWS S3-compatible endpoints.

**Docs**

- `docs/deployment/high-availability.md`: new sections for each type; fix "Three `coordinate.type` implementations" at line 18; add rows to the "Shared state, regardless of coordinator" list (lines 86-91) covering object-store lease + object-store state; note the no-NTP-concern property alongside the `k8s_lease` note.
- `docs/architecture.md:275-300` coordinator list; `README.md` HA section; a stateless-HA example (ECS Fargate + `state.store: s3` + `coordinate.type: s3_lease`).
- Run `just gen-config` to regenerate `config.example.yaml` and `docs/config-reference.md`, or the drift gate in `tests/test_config_artifacts_drift.py` fails.
- Note the S3 caveat: `If-Match` conditional PUT is required, so a non-AWS S3-compatible endpoint must support it; MinIO/R2 support should be stated rather than assumed.

---

Imported from GitHub issue #124 on 2026-08-14, when this repo migrated from GitHub Issues to Backlog.md. The original issue has been deleted; its verbatim body, labels and comments are preserved in `archive/issues-dump.json` (`jq '.[] | select(.number == 124)' archive/issues-dump.json`).

<sub>Filed from the 2026-07-30 full-repo audit (11 finder lanes + adversarial verification per finding).</sub>
<!-- SECTION:DESCRIPTION:END -->

## Acceptance Criteria
<!-- AC:BEGIN -->
- [ ] #1 `coordinate.type: s3_lease` and `coordinate.type: gcs_lease` are accepted by the config model, with sub-models carrying bucket/key/connection plus `ttl`/`renew_interval`/`holder_id`.
- [ ] #2 Config validation errors when the selected lease type has an empty `bucket`, and when the lease object collides with the configured checkpoint object (same bucket + same key/object name).
- [ ] #3 `ConfigError` with the `pip install 'sf2loki[s3]'` / `'sf2loki[gcs]'` install hint when the extra is absent, raised from the `app.py` factory before any client construction.
- [ ] #4 `src/sf2loki/coordinate/s3_lease.py` and `gcs_lease.py` import cleanly with neither extra installed (test asserts import succeeds in the bare environment).
- [ ] #5 `tests/coordinate/test_s3_lease.py` and `tests/coordinate/test_gcs_lease.py`, structured like `tests/coordinate/test_k8s_lease.py` with a fake adapter and injected `utcnow`/`sleep`, pin: - [ ] first acquire against an absent object uses the create-only precondition (`IfNoneMatch: "*"` / `ifGenerationMatch: "0"`) and sets `epoch` to 1 - [ ] renew preserves `epoch` verbatim and uses the current ETag/generation precondition - [ ] a 412 on renew or takeover demotes to standby and calls `on_lose` — no pause-then-reread round trip is issued - [ ] takeover after the lease goes stale bumps `epoch` by exactly one - [ ] a transient read error (5xx/timeout) does **not** trigger takeover and does not blind-write the lease (regression shape of #50) - [ ] a leader-written `expires_at` far in the past or future does not drive takeover timing — staleness is judged only by observed ETag/generation change on the observer's clock (regression shape of #51) - [ ] two standbys racing one expired lease produce exactly one winner, the loser observing 412 - [ ] `check_fence()` raises `StateFenceError` once leadership is lost - [ ] `run()` closes the underlying client/session on both normal exit and exception
- [ ] #6 `tests/coordinate/` or an app-wiring test asserts `set_fence` is called with the new coordinator's `check_fence` and that `set_epoch` is **not** wired for these types.
- [ ] #7 A doctor test asserts `coordinate.type: s3_lease` / `gcs_lease` reaches its own probe branch (not the Kubernetes probe), PASSes on a writable lease object, and FAILs on a permissions error.
- [ ] #8 `sf2loki_leader` behaves as documented under the new coordinators: `1` on the leader, `0` on the standby, and `/readyz` returns 503 on the standby.
- [ ] #9 `docs/deployment/high-availability.md` documents both types, corrects the "Three implementations" count, and its shared-state guidance covers object-store lease + object-store state; `docs/architecture.md` and `README.md` HA sections updated with a stateless-HA example.
- [ ] #10 `just gen-config` run and committed — `config.example.yaml` plus `docs/config-reference.md` regenerated, `tests/test_config_artifacts_drift.py` green.
- [ ] #11 `just gate` green (ruff, `mypy --strict`, pytest).
<!-- AC:END -->

## Definition of Done
<!-- DOD:BEGIN -->
- [ ] #1 just gate is green (ruff check + ruff format --check + mypy src + pytest) — run it, don't assert it
- [ ] #2 just gen-config run and its output committed, if config.py changed (CI drift gate fails otherwise)
- [ ] #3 committed straight to main with a conventional-commit message, and pushed
<!-- DOD:END -->
