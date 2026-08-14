---
id: doc-0003
title: Closed GitHub issues (2026-07-01 to 2026-07-03)
type: other
created_date: '2026-08-14 17:01'
updated_date: '2026-08-14 17:02'
---
> **The closed history of this repo's GitHub issue tracker: 63 issues, all closed as completed,
> spanning 2026-07-01 to 2026-07-03.** These issues no longer exist on GitHub — they were deleted
> on 2026-08-14 when the repo migrated to this tracker. Their full bodies, labels and comments are
> preserved in `archive/issues-dump.json`, which is the record.
>
> Read one with:
>
> ```bash
> jq '.[] | select(.number == 44)' archive/issues-dump.json
> ```
>
> The archive is committed unredacted; `archive/README.md` documents the sweep that established
> there was nothing in it needing redaction.
>
> **Why an index rather than 63 `Done` tasks.** Importing closed work as tasks would create a second
> ID space over the same history — backlog IDs follow creation order, so an `SFL-` number could
> never be made to match the `#NN` already cited throughout this repo's commit messages, code
> comments and docs — and 63 `Done` rows would compete with the board's only real signal, which is
> what is left. This index costs one file and keeps the original numbering as the only one.
>
> The **open** work of that tracker became the 71 tasks on this board (`backlog task list --plain`).

## The closed set

The SHA column is the commit whose message closed the issue, recovered from `git log` closing
keywords. 60 of the 63 have one; the remaining 3 were closed by hand, so no commit claims them.

| # | Closed | Title | Closing commit |
|---|---|---|---|
| 14 | 2026-07-01 | pubsub: periodic re-discovery of new streaming channels while running | `dd617c9` |
| 15 | 2026-07-01 | overlap guard: extend to runtime-discovered Pub/Sub topics | `dd617c9` |
| 16 | 2026-07-01 | pipeline: byte-aware bounding (or configurable size) for the internal queue | `2664aaa` |
| 17 | 2026-07-01 | obs: derive readiness/health from pipeline liveness, not just startup | `2664aaa` |
| 18 | 2026-07-01 | eventlog_objects: per-object poll_interval timers | `d802659` |
| 19 | 2026-07-01 | obs: error counter for eventlog_objects poll failures | `d802659` |
| 20 | 2026-07-01 | shaping: deterministic timestamp fallback for unparseable event timestamps | `d802659` |
| 21 | 2026-07-01 | minor hardening backlog from the 2026-07-01 operational audit | `d802659` |
| 22 | 2026-07-01 | cli: `sf2loki doctor` — live preflight that validates the whole path end to end | `5a8d26d` |
| 23 | 2026-07-01 | cli: one-shot historical backfill command for EventLogFile | `df6f2f0` |
| 24 | 2026-07-02 | sink: OTLP/HTTP logs export as an alternative to the Loki push API | — |
| 25 | 2026-07-01 | perf: bounded-concurrency EventLogFile processing per cycle | `9c9a821` |
| 26 | 2026-07-01 | cost: egress guardrails — per-type sampling, rate caps, and a daily byte budget | `1fa4de7` |
| 27 | 2026-07-01 | compliance: field redaction and row-filter rules (PII controls) | `68e7356` |
| 28 | 2026-07-01 | obs: ship a provisioned Grafana alert-rule pack alongside the dashboard | `1920764` |
| 29 | 2026-07-01 | ha: active-passive failover via a real Coordinator implementation | `6fa5a54` |
| 30 | 2026-07-01 | state: pluggable remote checkpoint stores (object storage) for stateless deployments | `07ed785` |
| 31 | 2026-07-01 | feat: multi-org ingestion from a single process | `57b314a` |
| 32 | 2026-07-02 | dist: publish to PyPI with trusted publishing (pip/pipx/uv install path) | `0fa7f99` |
| 33 | 2026-07-02 | source: ApexLog (debug log) ingestion via the Tooling API | — |
| 34 | 2026-07-02 | source: Event Log Objects (big-object storage, up to 1y retention) as an alternative ELF backend | — |
| 35 | 2026-07-02 | docs: first-class guidance + preset for custom platform events and CDC topics | `6198fe3` |
| 36 | 2026-07-02 | ha: Kubernetes Lease coordinator backend | `bb00032` |
| 37 | 2026-07-02 | state: GCS checkpoint store backend | `bb00032` |
| 38 | 2026-07-02 | eventlog_objects: watermark stalls and silently drops newer data when >=200 records share one timestamp | `40496fb` |
| 39 | 2026-07-02 | apexlog: >200 logs at one StartTime pin the watermark and drop newer logs | `4538201` |
| 40 | 2026-07-02 | backfill: namespace the checkpoint key per org — cross-org runs silently skip files | `4bd1b0c` |
| 41 | 2026-07-02 | eventlogfile: a mid-file csv.Error permanently stalls the whole event type | `bf2f0ff` |
| 42 | 2026-07-02 | pubsub: fieldless events get a non-deterministic now() timestamp — breaks Loki dedup on replay, uncounted | `7397e5a` |
| 43 | 2026-07-02 | pubsub: decode/schema failures are invisible and unbounded — label decode_errors by topic, detect stuck loops | `7397e5a` |
| 44 | 2026-07-02 | state: a transient s3/gcs error on checkpoint commit crashes the daemon — add bounded retry | `b7dc419` |
| 45 | 2026-07-02 | pubsub: checkpoint load runs outside the per-topic retry loop — a transient state error wedges the drain and exits 0 | `7397e5a` |
| 46 | 2026-07-02 | eventlog_objects: bound big-object DESC drain memory — a post-outage catch-up can OOM | `40496fb` |
| 47 | 2026-07-02 | coordinate: the commit fence is a lagging local boolean — a stale leader can still regress file-store checkpoints | `73101d7` |
| 48 | 2026-07-02 | state: checkpoint cache is never invalidated on leadership loss — stale checkpoints after demote->promote | `73101d7` |
| 49 | 2026-07-02 | state: file-store flock is process-lifetime — a promoted standby crash-loops (or is unprotected) under file-lease HA | `73101d7` |
| 50 | 2026-07-02 | coordinate: file lease treats read errors/deletion as absent and rewrites blindly — transient dual leadership | `6e6ec77` |
| 51 | 2026-07-02 | coordinate: k8s lease expiry trusts leader-written renewTime against the observer clock — skew causes premature takeover | `da39549` |
| 52 | 2026-07-02 | state: s3/gcs checkpoint store close() is async but never awaited — client session leaks on every shutdown | `73101d7` |
| 53 | 2026-07-02 | pipeline: one shared queue and a single serial push worker — a bulk ELF drain head-of-line-blocks realtime streams | `33b579f` |
| 54 | 2026-07-02 | perf: each checkpoint key rewrites the full state document (two fsyncs / one object PUT) inside the flush path | `73101d7` |
| 55 | 2026-07-02 | perf: Loki protobuf/snappy encoding blocks the event loop inside the single consumer | `8ec2ae8` |
| 56 | 2026-07-02 | pipeline: byte-budget coverage gaps — per-source bridge queues unbounded by bytes, entry cost undercounts real memory | `73101d7` |
| 57 | 2026-07-02 | deploy: docker-compose defaults customers onto the :main edge image — default to a released tag | `c0c9bba` |
| 58 | 2026-07-02 | docs: the dashboard and alert pack silently depend on OTLP metric-suffix translation (_total/_bucket) | `9df258b` |
| 59 | 2026-07-02 | cli: doctor validates a state dir the deployment doesn't use — probe the configured state backend, OTLP endpoint, and coordinator | `1735bef` |
| 60 | 2026-07-02 | deploy: ship a Kubernetes Deployment+RBAC example for k8s_lease and document readiness-vs-liveness for HA standbys | `c0c9bba` |
| 61 | 2026-07-02 | deploy: docker stop_grace_period (10s default) truncates service.shutdown_grace (25s) — align and document ECS stopTimeout | `c0c9bba` |
| 62 | 2026-07-02 | coordinate: k8s lease adapter crashes on a Lease with null renewTime/leaseDurationSeconds | `da39549` |
| 63 | 2026-07-02 | cli: state inspect/reset command + poison-checkpoint recovery runbook | `62fab37` |
| 64 | 2026-07-02 | sources: emit checkpoint_only tokens so fully-filtered poll cycles advance the watermark durably | `bf2f0ff` |
| 65 | 2026-07-02 | pubsub: gate the EARLIEST re-drain on the corrupted-replay error trailer, not bare INVALID_ARGUMENT | `7397e5a` |
| 66 | 2026-07-02 | eventlogfile: default a non-zero settle_window for Hourly interval (server-incomplete blob tail loss) | `bf2f0ff` |
| 67 | 2026-07-02 | compliance: warn when a hash transform runs without transform_salt (reversible pseudonyms) | `73101d7` |
| 68 | 2026-07-02 | salesforce: per-org ELF clock-skew hooks stack on the shared httpx client; startup auth probe is sequential | `73101d7` |
| 69 | 2026-07-02 | perf: hot-path micro-overheads — per-row checkpoint serialization, repeated UTF-8 encodes, per-event now() | `98868dd` |
| 70 | 2026-07-02 | tests: replace real-time sleeps with deterministic synchronization in concurrency tests | `9314cb1` |
| 71 | 2026-07-02 | minor hardening backlog from the 2026-07-02 production-readiness audit | `c97224d` |
| 72 | 2026-07-02 | docs: refresh DESIGN.md drift (Python version, ELF stub claim, checkpoint value format, label table) + drop dead sink.type field | `c0c9bba` |
| 73 | 2026-07-03 | Publish a production-ready Helm chart as an OCI artifact | `06a2a82` |
| 74 | 2026-07-03 | Retire the deploy/k8s example manifests in favour of the Helm chart | `06a2a82` |
| 75 | 2026-07-03 | Generate the Helm chart's default config from the Pydantic schema (with drift gate) | `06a2a82` |
| 76 | 2026-07-03 | Docs site: redesign & rebrand alignment + SEO/LLM discoverability | `4bf82de` |
