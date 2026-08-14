---
id: SFL-0062
title: >-
  pubsub: opt-in ManagedSubscribe mode - server-side replay commits via Managed
  Event Subscriptions
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
  - 'https://github.com/rknightion/sf2loki/issues/146'
ordinal: 62000
---

## Description

<!-- SECTION:DESCRIPTION:BEGIN -->
## What

The Pub/Sub client only ever uses the unmanaged `Subscribe` RPC. `PubSubClient.subscribe()` (`src/sf2loki/salesforce/pubsub_client.py:270-311`) opens the stream at `pubsub_client.py:311`:

```python
call = self._stub().Subscribe(metadata=await self._metadata())
```

and writes a `FetchRequest` carrying `topic_name` / `replay_preset` / `replay_id` (`pubsub_client.py:314-321`). Every replay cursor therefore lives on the sf2loki side: the source emits a `CheckpointToken` per event (`src/sf2loki/model.py:14-23`, `src/sf2loki/sources/pubsub_source.py:787-796`) plus `checkpoint_only` tokens for keepalives and sampled-out events (`pubsub_source.py:711-745`), and `app.py` persists them through the `CheckpointStore` seam after the batch is pushed (`src/sf2loki/app.py:499-515`, protocol at `src/sf2loki/state/base.py:8-11`). On restart the source loads the stored replay id back (`pubsub_source.py:468-512`).

Salesforce offers a server-side alternative that is already fully wired into the vendored proto and the committed stubs but never called:

- `rpc ManagedSubscribe (stream ManagedFetchRequest) returns (stream ManagedFetchResponse)` — `proto/pubsub_api.proto:410`
- `ManagedFetchRequest` with `subscription_id` / `developer_name` / `num_requested` / `commit_replay_id_request` — `proto/pubsub_api.proto:236-256`
- `ManagedFetchResponse` with `events` / `latest_replay_id` / `pending_num_requested` / `commit_response` — `proto/pubsub_api.proto:268-280`
- `CommitReplayRequest` (`commit_request_id`, `replay_id`) and `CommitReplayResponse` (`commit_request_id`, `replay_id`, `error`, `process_time`) — `proto/pubsub_api.proto:289-311`
- generated client stub — `src/sf2loki/salesforce/_generated/pubsub_api_pb2_grpc.py:79-80`; messages present in the descriptor at `_generated/pubsub_api_pb2.py:27`

In managed mode the subscription is a `ManagedEventSubscription` Tooling/Metadata API record in the org (fields: `topicName`, `state`, `defaultReplay`, `errorRecoveryReplay`, `label`, `DeveloperName`). The client subscribes by subscription id or developer name and acknowledges progress by setting `commit_replay_id_request` on a subsequent `ManagedFetchRequest`; Salesforce stores the committed replay id and resumes any reconnecting client from it. `sources.pubsub.replay_preset` and the stored replay id become inert for a managed topic — the start position comes from the record's `defaultReplay`, and `errorRecoveryReplay` governs the expired-cursor case that `sources.pubsub` currently handles client-side.

`src/sf2loki/config.py:329-397` (`PubSubConfig`) has no `managed` field, and a repo-wide grep for `ManagedSubscribe|ManagedFetch|managed_subscribe|CommitReplayRequest|ManagedEventSubscription` outside `_generated/` matches only `proto/pubsub_api.proto`. Nothing in `docs/` or `README.md` mentions managed subscriptions.

### Upstream facts (verified against Salesforce docs, 2026-07-30)

These are binding constraints on the design; do not re-derive them:

- **Availability:** `ManagedEventSubscription` (Metadata API and Tooling API) is available in **API version 60.0 and later**. The current default `salesforce.api_version` is `"60.0"` (`src/sf2loki/config.py:203-205`), so there is no version floor above the default to gate on.
- **Still Beta, not GA.** Both the vendored proto (`proto/pubsub_api.proto:225-228` and `:401-409`, "This feature is part of an open beta release ... Beta Services Terms") and the live Pub/Sub API guide carry the Beta notice at v67.0. This is the primary reason the mode must be opt-in and default off.
- **`topicName` eligibility is the open question.** The documented field description covers "the topic name of the platform event or change event or the channel name of a custom platform event channel or custom or standard change data capture channel" — it does **not** mention Real-Time Event Monitoring events. sf2loki's default Pub/Sub topics are RTEM streams (`/event/LoginEventStream`, `/event/ApiEventStream`; see the `PubSubConfig.topics` examples at `config.py:352-357` and the wildcard discovery of `*EventStream` channels in `src/sf2loki/salesforce/metadata_client.py`). RTEM support must be confirmed before implementation (see the spike below); if unsupported, the feature scopes to custom platform events and CDC channels only, which is the territory covered by the custom-PE/CDC preset already shipped.
- **One client per subscription.** "A managed subscription is unique per client and can't be shared with other clients for the same Salesforce org." It is not consumer-group semantics and does not relax the single-instance rule — active-passive HA via the `Coordinator` seam is still required.
- **Limit:** 200 unique managed subscriptions per org. Managed subscriptions otherwise carry the same allocations as `Subscribe`, and the 100-event cap on requested events applies per `ManagedSubscribe` call exactly as it does per `Subscribe` call (matching `default_num_requested` at `config.py:337-345`).
- **States:** `RUN` delivers events, `STOP` halts delivery **and clears committed replay IDs**, `PAUSE` is internal-only. Configuration changes can take several minutes to take effect.
- **Permissions:** create/update/delete of the record needs *Customize Application*; query/retrieve needs *View Setup and Configuration*. The connector's integration user normally has neither, so auto-provisioning records is a privilege escalation and must not be the default.

## Why it matters

Two costs exist purely because the replay cursor is client-side:

1. **A Pub/Sub-only deployment on Fargate / Cloud Run needs an S3 or GCS bucket solely to remember replay ids.** `state.backend: file` is unusable without a persistent volume, so the only stateless option today is an object-store checkpoint store. With managed subscriptions there is no cursor to persist for `pubsub:` keys at all.
2. **Failover re-ingests up to one lease TTL of already-delivered events.** `docs/architecture.md:297-305` documents the bound: a fenced commit is not data loss, but the cost is "at most a bounded re-ingest (up to one lease `ttl`/`lease_duration`) after the new leader resumes", because the commit lives in a store the old leader may not have flushed. A promoted standby attaching to a managed subscription resumes from the last server-committed replay id instead, cutting that window to whatever the outgoing leader had not yet acked.

The benefit is real but narrow, which is why this is low severity: a deployment that also runs `eventlogfile`, `eventlog_objects` or `apexlog` still needs a `CheckpointStore` for those keys, and `coordinate.backend: file_lease` still needs shared storage regardless. The clean win is Pub/Sub-only plus either single-instance or `k8s_lease`.

## Proposed approach

### Step 0 — spike first, against the DEV org (`.env.dev`), before writing production code

Create a `ManagedEventSubscription` record via the Tooling API REST endpoint (`POST /services/data/vXX.X/tooling/sobjects/ManagedEventSubscription`) with `Metadata.topicName` set to an RTEM stream the DEV org exposes, and confirm (a) the record is accepted, (b) `ManagedSubscribe` with that `developer_name` delivers events, (c) `commit_replay_id_request` returns a `commit_response` with no `error`. Record the outcome on this issue as a comment before building. If RTEM topics are rejected, narrow the scope to custom platform events / CDC channels and say so in the docs; do not silently ship a mode that fails on the default topic set.

### Config

Add a nested block to `PubSubConfig` (`src/sf2loki/config.py:329`), off by default:

```yaml
sources:
  pubsub:
    managed:
      enabled: false                     # opt-in; Salesforce Beta feature
      name_template: "sf2loki_{topic}"   # -> DeveloperName, sanitized
      auto_create: false                 # needs Customize Application; default off
      default_replay: LATEST             # informational for auto_create / doctor
      error_recovery_replay: LATEST
```

`name_template` renders a valid `DeveloperName` from the topic (strip `/event/`, `/data/`, replace non-`[A-Za-z0-9_]` with `_`, enforce the length cap); the mapping must be pure and unit-tested so the same topic always resolves to the same record. Regenerate `config.example.yaml` and `docs/config-reference.md` with `just gen-config` or the drift gate (`tests/test_config_artifacts_drift.py`) fails.

Reject at config validation, with an explicit `ConfigError`: `managed.enabled: true` combined with `topics: ["*"]` while `auto_create` is false (a discovered topic has no record and would fail at subscribe time). Also warn-and-ignore, rather than silently honour, `replay_preset` when `managed.enabled` is true — the record owns the start position.

### Client

Add `PubSubClient.managed_subscribe(...)` alongside `subscribe()` (`pubsub_client.py:270`), yielding the same `DecodedEvent | KeepaliveEvent` union so the `Source` seam is untouched. It drives one `ManagedSubscribe` stream per subscription: initial `ManagedFetchRequest(developer_name=..., num_requested=n)`, credit top-ups on the same low-watermark rule as `subscribe()` (`pubsub_client.py:307-308`), and it drains an `asyncio.Queue[bytes]` of pending replay ids into `commit_replay_id_request` on outbound requests. Reuse the existing stall watchdog, `_handle_rpc_error` (`pubsub_client.py:433`) and the per-topic health/decode-error accounting (`pubsub_client.py:444-497`) unchanged. Surface `ManagedFetchResponse.commit_response.error` as a new counter (a commit that silently fails means the server cursor is not advancing, which is invisible otherwise).

### Commit routing — an adapter, not a seam change

The commit ack must reach the gRPC stream that owns the topic, but `app.py:499-515` commits opaque `key -> value` pairs through `CheckpointStore`. Introduce a `ManagedReplayStore` that *implements* `CheckpointStore` (`state/base.py:8-11`) and wraps the configured store: keys with the `pubsub:` prefix whose topic is managed are handed to that stream's commit queue; every other key delegates to the wrapped store. Implement `commit_many` and `reset` so the duck-typed optimisations in `app.py:506-509` and `app.py:518` keep working. Wire it in the composition root only. Consequences that fall out for free: `app.py`'s commit-after-push at-least-once invariant is preserved unchanged, and the `checkpoint_only` keepalive entries built at `pubsub_source.py:729-745` become commit acks of `latest_replay_id` with no source-side change.

In managed mode the source must skip the checkpoint *load* path (`pubsub_source.py:468-512`) for managed topics, since there is nothing to load.

When `managed.enabled` is true and every configured source is Pub/Sub-only, no durable checkpoint store is required; document that `state.backend: file` on an ephemeral filesystem is then acceptable, and make sure the startup path does not warn about it.

### Provisioning and doctor

Prefer verification over creation. Extend `_check_pubsub` (`src/sf2loki/doctor.py:249`) to query `ManagedEventSubscription` through the existing tooling-mode `SoqlClient` (`src/sf2loki/salesforce/soql_client.py:76-95`, `tooling=True` targets `/tooling/query`) for each resolved topic and FAIL with the exact record to create when: the record is missing, `state != RUN`, or `topicName` does not match the configured topic. `auto_create: true` performs the `POST /tooling/sobjects/ManagedEventSubscription` at startup and must fail loudly (not warn) when the integration user lacks Customize Application.

Multi-org: `DeveloperName` is scoped per org, so no org suffix is needed in the name, but each org lane resolves and holds its own subscription independently (`src/sf2loki/sources/org_adapter.py`).

### Docs

New section in `docs/sources/pubsub.md` covering the Beta status, the RTEM-eligibility outcome from the spike, the 200-per-org limit, the single-client rule (managed subscriptions are **not** consumer groups — HA stays active-passive), the required org permission, and that `state: STOP` clears committed cursors. Add a note to `docs/architecture.md` near the bounded-re-ingest paragraph (`architecture.md:297-305`) that managed mode moves the Pub/Sub cursor server-side and shrinks that window.

---

Imported from GitHub issue #146 on 2026-08-14, when this repo migrated from GitHub Issues to Backlog.md. The original issue has been deleted; its verbatim body, labels and comments are preserved in `archive/issues-dump.json` (`jq '.[] | select(.number == 146)' archive/issues-dump.json`).

<sub>Filed from the 2026-07-30 full-repo audit (11 finder lanes + adversarial verification per finding).</sub>
<!-- SECTION:DESCRIPTION:END -->

## Acceptance Criteria
<!-- AC:BEGIN -->
- [ ] #1 Spike outcome posted as a comment on this issue: whether an RTEM stream topic is accepted as `ManagedEventSubscription.topicName` in the DEV org, and whether `ManagedSubscribe` + `commit_replay_id_request` round-trips cleanly. Scope narrowed to custom PE / CDC channels if RTEM is rejected.
- [ ] #2 `sources.pubsub.managed` block added to `PubSubConfig` (`src/sf2loki/config.py:329`), `enabled` and `auto_create` both defaulting to false; `config.example.yaml` + `docs/config-reference.md` regenerated so `tests/test_config_artifacts_drift.py` passes.
- [ ] #3 `PubSubClient.managed_subscribe()` implemented, yielding the same `DecodedEvent | KeepaliveEvent` union as `subscribe()`; `Source`, `Sink`, `CheckpointStore` and `Coordinator` protocol definitions unchanged.
- [ ] #4 `ManagedReplayStore` implements `CheckpointStore` including `commit_many` and `reset`, routes `pubsub:` keys for managed topics to the owning stream and delegates all other keys to the wrapped store; `app.py:499-515` unchanged.
- [ ] #5 Test: a fake gRPC stream asserts the first outbound message is a `ManagedFetchRequest` carrying `developer_name` (not a `FetchRequest`), and that no `topic_name`/`replay_preset`/`replay_id` is sent in managed mode.
- [ ] #6 Test: committing a `pubsub:<topic>` `CheckpointToken` through `ManagedReplayStore` emits a `CommitReplayRequest` with the decoded replay id on the owning stream and performs **no** write to the wrapped store; committing a non-`pubsub:` key writes through to the wrapped store.
- [ ] #7 Test: a `checkpoint_only` keepalive entry (built as at `pubsub_source.py:729-745`) commits `latest_replay_id` as an ack, preserving commit-after-push ordering relative to real entries queued ahead of it.
- [ ] #8 Test: `ManagedFetchResponse.commit_response.error` increments the new commit-failure counter and logs at ERROR.
- [ ] #9 Test: flow-control credit top-up in managed mode follows the same low-watermark rule as `subscribe()`, and `num_requested` stays within the 1-100 bound.
- [ ] #10 Test: config validation rejects `managed.enabled: true` with `topics: ["*"]` and `auto_create: false`, and rejects/warns on a conflicting `replay_preset`.
- [ ] #11 Test: managed mode skips the checkpoint-load path for managed topics (no `load()` call on the wrapped store for those keys).
- [ ] #12 Test: `doctor` reports FAIL with the record to create when the `ManagedEventSubscription` is missing, when `state != RUN`, and when `topicName` mismatches; PASS when present and running.
- [ ] #13 `docs/sources/pubsub.md` documents Beta status, eligible topic types per the spike, the 200-per-org limit, the single-client rule and that HA remains active-passive, the Customize Application requirement, and the `STOP`-clears-cursors behaviour; `docs/architecture.md` notes the shortened failover re-ingest window.
- [ ] #14 `just gate` green (ruff + `mypy --strict` + pytest).
<!-- AC:END -->

## Definition of Done
<!-- DOD:BEGIN -->
- [ ] #1 just gate is green (ruff check + ruff format --check + mypy src + pytest) — run it, don't assert it
- [ ] #2 just gen-config run and its output committed, if config.py changed (CI drift gate fails otherwise)
- [ ] #3 committed straight to main with a conventional-commit message, and pushed
<!-- DOD:END -->
