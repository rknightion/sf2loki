# High Availability

sf2loki runs **standalone by default**: a single instance is always the leader and
streams continuously (`coordinate.type: noop`). For hands-off failover, run two
replicas in an **active-passive** pair — exactly one replica (the leader) ingests at a
time, and the other stands by and takes over if the leader dies.

## Why single-instance, and why active-passive rather than scale-out

The Salesforce Pub/Sub API has no consumer-group semantics: two subscribers on the
same topic each get delivered every event independently, so a second *active* replica
doesn't share load, it **double-delivers**. sf2loki cannot horizontally scale by simply
running more replicas — HA here means one hot spare, not more throughput. Run exactly
one active replica at a time (a stop-then-start rollout, never overlapping) unless a
coordinator is arbitrating leadership between two replicas for you.

## Coordinators

Three `coordinate.type` implementations, selectable in config, with zero changes
required in sources/sinks/state:

### `noop` — single instance (default)

Always leader. No lease, no shared storage, no failover. This is what you get if
`coordinate` is unset.

### `file_lease` — shared-file lease

A small JSON document — `{holder, expires_at, epoch}` — on storage shared by both
replicas (NFS/EFS/a shared volume). The leader renews `expires_at` every
`renew_interval`; the standby polls the lease and takes over once it's gone stale.

```yaml
coordinate:
  type: file_lease
  file_lease:
    path: /var/lib/sf2loki/leader.lease   # on shared storage, same for both replicas
    ttl: 30s              # failover time: standby takes over this long after renewals stop
    renew_interval: 10s   # must be < ttl/2, so one missed renewal never costs leadership
    holder_id: ""         # blank -> hostname-pid; set explicitly if hostnames aren't unique
```

- Expiry is **wall-clock**, compared across hosts — replicas must be NTP-synced, and
  `ttl` needs headroom above worst-case clock skew.
- Deliberately not `flock` (unreliable over NFS, doesn't survive a holder that dies
  without releasing). Takeover uses an atomic tmp-then-rename write plus a brief
  pause-and-re-read after a contested rename, so two standbys racing an expired lease
  never both win.
- The shared checkpoint store re-reads the lease fresh at commit time (bypassing its
  own cache) — see [Fencing](#fencing) below.

### `k8s_lease` — Kubernetes `coordination.k8s.io/v1` Lease

No shared volume needed; requires the `sf2loki[k8s]` extra.

```yaml
coordinate:
  type: k8s_lease
  k8s_lease:
    namespace: sf2loki        # namespace holding the Lease (default: default)
    name: sf2loki-leader      # Lease object name, shared by all replicas
    identity: ""              # blank -> pod name ($HOSTNAME); set if pod names aren't unique
    lease_duration: 30s       # failover time: standby takes over this long after renewals stop
    renew_interval: 10s       # must be < lease_duration/2
    # kubeconfig: ~/.kube/config   # omit for in-cluster config; set for out-of-cluster dev
```

- Optimistic concurrency uses the Lease's `resourceVersion`: a lost update returns HTTP
  409, which *is* the race signal, so — unlike `file_lease` — there's no
  pause-then-verify step needed.
- **No NTP concern.** Staleness is judged by each replica's own **observedTime**
  (the same pattern as client-go's `leaderelection`): a replica tracks, on its own
  monotonic clock, how long it's been since it last saw the Lease's `resourceVersion`
  change, and only takes over once `lease_duration` has elapsed on *that* clock. The
  leader's `renewTime` (wall-clock) is written for observability/`kubectl describe` but
  never read back for staleness math, so cross-host clock skew can't cause a premature
  or delayed takeover.
- The pod's ServiceAccount needs `get`/`create`/`update` on `leases` in the coordinator's
  namespace — see [Kubernetes](kubernetes.md) for the full RBAC + Deployment example.

## Shared state, regardless of coordinator

Both replicas run the same config, and **the checkpoint state store must also be
shared** so the standby resumes exactly where the leader left off:

- `file_lease` — mount the same NFS/EFS export (or shared volume) on both hosts and use
  `state.store: file` pointed at it.
- `k8s_lease` — use `state.store: s3` or `gcs` (no shared volume available/needed on
  Kubernetes).

See [State & Checkpoints](state.md) for backend detail.

## Fencing

A stale leader — one that lost the lease mid-commit (e.g. a GC pause) — must not be
allowed to advance checkpoints and race the new leader. Each coordinator wires its
fence check into the state store via an optional `set_fence(...)` hook: a commit is
rejected with `StateFenceError` if the writer is no longer holding the lease at commit
time. This is not data loss — the batch already landed in Loki, so the cost is at most
a bounded re-ingest after the new leader resumes. Semantics throughout are
**at-least-once**: expect duplicate log lines around a takeover (Loki's per-stream
reject window dedupes exact duplicates), never gaps.

For the `file_lease` coordinator specifically, the shared-file checkpoint store
re-reads the lease fresh at commit time (bypassing its own cache) and compares an
`epoch` fence token that increments on every winning acquire — this catches a stale
leader even before its own local "am I still the leader?" flag has caught up (which
only refreshes once per `renew_interval`). The S3/GCS state stores don't need the epoch
mechanism: their own ETag/generation-preconditioned compare-and-swap already rejects a
losing writer independently.

## Observability

`sf2loki_leader` is `1` on the active leader (and on any standalone instance), `0` on
the standby — `sum(sf2loki_leader)` across replicas should always be exactly `1`; a
sustained `0` (leaderless gap) or `2` (split-brain) means the coordinator has failed to
converge. See [Metrics](../observability/metrics.md) for the metric and
[Alerts](../observability/alerts.md) for the shipped alert pack — there is currently no
dedicated alert on `sf2loki_leader` cardinality in that pack, so alerting on it is a
gap to close yourself if you need it.

!!! warning "`/readyz` on the standby is `503` forever, by design"
    That's correct for a load balancer / target-group check (routes traffic only to the
    leader) but it is **never** safe as a liveness/instance-restart check on an HA
    replica — a restart-on-`/readyz`-failure policy restart-loops the standby forever
    and defeats failover. See [Deployment](index.md#health-endpoints) and
    [Kubernetes](kubernetes.md#the-two-sharp-edges) for the concrete
    readiness-vs-liveness split on each platform.

## See also

- [Kubernetes](kubernetes.md) — the runnable manifest example for `k8s_lease`.
- [State & Checkpoints](state.md) — checkpoint backends and inspection/repair.
- [Configuration reference](../config-reference.md) — `CoordinateConfig` /
  `FileLeaseConfig` / `K8sLeaseConfig` field details.
- [Architecture](../architecture.md) — the `Coordinator` seam in the broader design.
