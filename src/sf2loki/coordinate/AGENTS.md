# src/sf2loki/coordinate

The `Coordinator` seam for active-passive HA: `NoopCoordinator` (always leader, the single-instance
default), `FileLeaseCoordinator` (a lease document on shared storage) and `K8sLeaseCoordinator` (a
`coordination.k8s.io/v1` Lease). Adding one changes nothing in sources, sinks or state.
`docs/deployment/high-availability.md` is the operator-facing version.

## Deliberate, do not "fix"

- The file lease does not use `flock`: unreliable over NFS, and it does not survive a holder that
  dies without releasing. Expiry is wall-clock compared across hosts, so replicas must be NTP-synced
  and the ttl needs headroom above worst-case clock skew.
- The file lease pauses and re-reads after a contested rename, to detect losing a takeover race. The
  Kubernetes lease deliberately has no equivalent step: a lost `resourceVersion` compare-and-swap
  comes back as HTTP 409, which is itself the race signal.
- `k8s_lease.py` talks to a thin adapter (`read_lease` / `create_lease` / `replace_lease`), never
  the raw `CoordinationV1Api`, so `kubernetes_asyncio` is imported lazily and the module stays
  importable and testable without the `sf2loki[k8s]` extra. Errors are duck-typed on
  `getattr(exc, "status", None)`; `except ApiException` would force that top-level import back.
- `run()` owns the adapter lifecycle - enter the api context manager at the top, exit in `finally`.
  The `Coordinator` protocol has no `close()` and `app.py` never closes the coordinator, so the
  aiohttp session must not outlive a `run` call.
- The Kubernetes RBAC is `get` / `create` / `update` on `leases` and never `delete`
  (`deploy/helm/templates/rbac.yaml`).

## Fencing (`StateFenceError`, in `base.py`)

A stale leader - one that lost the lease mid-commit, say to a GC pause - must not advance
checkpoints and race the new leader. `app.py` wires the active coordinator's `check_fence` into the
state store through `state.set_fence(...)`, a duck-typed optional hook: `FileCheckpointStore`,
`S3CheckpointStore` and `GcsCheckpointStore` all implement it, but the `CheckpointStore` protocol
does not declare it.

A fenced commit is not data loss. The batch already landed in the sink, so the cost is at most a
bounded re-ingest once the new leader resumes (at-least-once). The fence lives here rather than in
`state/` because it is a leadership contract: the state store stays agnostic and only invokes an
opaque callable.
