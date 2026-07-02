# CLAUDE.md — src/sf2loki/coordinate

The `Coordinator` seam (`base.py`) for active-passive HA. `NoopCoordinator`
(always leader, the single-instance default), `FileLeaseCoordinator`
(`file_lease.py`, lease on shared storage), and `K8sLeaseCoordinator`
(`k8s_lease.py`, a `coordination.k8s.io/v1` Lease) all implement it with zero
changes required in sources/sinks/state — see `../CLAUDE.md` and DESIGN.md §13
for why sf2loki is single-instance-only without a coordinator (Pub/Sub has no
consumer-group semantics; two subscribers double-deliver).

## Lease mechanics (`file_lease.py`)
- A small JSON `{"holder", "expires_at"}` document on storage shared by every
  replica (NFS/EFS/shared volume). The leader renews `expires_at` faster than
  the ttl; a standby takes over once it's gone stale.
- Expiry is **wall-clock**, compared across hosts — replicas must be
  NTP-synced, and the ttl needs headroom above worst-case clock skew.
- Deliberately **not** `flock` — unreliable over NFS and doesn't survive a
  holder that dies without releasing. Instead: atomic tmp-then-rename write
  (same durability pattern as `state/file_store.py`) plus a brief pause +
  re-read after a contested rename, to detect losing a takeover race.

## Kubernetes Lease (`k8s_lease.py`)
- Same `run`→`_acquire`→`_hold`→`_pause` loop shape as the file lease (injected
  `utcnow`/`sleep`, same `check_fence`/`is_leader`/`holder`), but over a
  `coordination.k8s.io/v1` Lease (`holderIdentity` + `renewTime`; expiry =
  `renewTime + leaseDurationSeconds`). Optimistic concurrency uses the Lease
  `resourceVersion` — a lost CAS returns HTTP 409, which IS the race signal, so
  there is **no** pause-then-verify step (unlike the file lease's rename+reread).
- Talks to a thin **adapter** (`read_lease`/`create_lease`/`replace_lease` over
  `_LeaseBody`/`_Lease`), never the raw `CoordinationV1Api` — so
  `kubernetes_asyncio` is imported lazily (inside `_default_api_factory` /
  `_RealLeaseAdapter`) and the module stays importable/testable without the
  `sf2loki[k8s]` extra. Errors are duck-typed via `getattr(exc, "status", None)`
  (never `except ApiException`, which would force a top-level import).
- **`run()` owns the adapter lifecycle** (enter the api context manager at the
  top, exit in `finally`): the `Coordinator` protocol has no `close()` and
  `app.py` never closes the coordinator, so the aiohttp session must not outlive
  a `run` call. RBAC: the pod's ServiceAccount needs `get`/`create`/`update` on
  `leases` in the configured namespace (see README HA section).

## Fencing (`StateFenceError`, defined in `base.py`)
A stale leader — one that lost the lease mid-commit (e.g. a GC pause) — must
not be allowed to advance checkpoints and race the new leader. `app.py` wires
`FileLeaseCoordinator`'s fence check into the state store via
`state.set_fence(...)` (a duck-typed optional hook — `NoopCoordinator` and
`FileCheckpointStore`/`S3CheckpointStore` all support it, but the
`CheckpointStore` protocol itself doesn't declare it). A fenced commit is not
data loss: the batch already landed in the sink, so the cost is at most a
bounded re-ingest after the new leader resumes (at-least-once).

The fence lives here, not in `state/`, because it's a leadership contract —
the state store stays agnostic and only invokes an opaque callable.
