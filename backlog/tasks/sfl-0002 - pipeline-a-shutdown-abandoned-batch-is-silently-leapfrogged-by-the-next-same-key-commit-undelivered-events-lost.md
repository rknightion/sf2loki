---
id: SFL-0002
title: >-
  pipeline: a shutdown-abandoned batch is silently leapfrogged by the next
  same-key commit - undelivered events lost
status: To Do
assignee: []
created_date: '2026-08-14 16:58'
labels:
  - followup
  - phase-1
milestone: m-0
dependencies: []
references:
  - 'https://github.com/rknightion/sf2loki/issues/86'
ordinal: 2000
---

## Description

<!-- SECTION:DESCRIPTION:BEGIN -->
## What

`Pipeline._flush` abandons a batch uncommitted when the sink reports `RetryableSinkError` during shutdown — either because `stop` is already set (`src/sf2loki/app.py:462-466`) or because `stop` fires during the retry backoff (`src/sf2loki/app.py:467-469`). Abandoning the batch is correct; what is missing is any state marking the affected checkpoint keys as undeliverable for the rest of the run.

`_flush` simply returns, and `_consume` (`src/sf2loki/app.py:388-418`) keeps looping: it has no `stop` check and exits only once it has seen one sentinel per producer. The remaining queue backlog (already-fetched entries plus the producers' sentinels) is therefore drained into further batches, and every one of them still calls `_commit` (`src/sf2loki/app.py:499-513`), which writes last-token-per-key with no ordering guard.

Streaming entries all share a single checkpoint key per topic (`pubsub:<topic>`, value = that event's base64 replay id, `src/sf2loki/sources/pubsub_source.py:787-789`) with monotonically increasing values in FIFO order. A commit from a later batch therefore advances the key past entries that were never delivered.

Two distinct paths advance the checkpoint after an abandonment:

1. **Sink recovers mid-shutdown.** The next batch pushes successfully and the success path commits (`src/sf2loki/app.py:492-497`).
2. **Sink still failing — no recovery required.** A following flush containing only `checkpoint_only` entries (a Pub/Sub keepalive `latest_replay_id`, `src/sf2loki/sources/pubsub_source.py:728-745`; an eventlogfile trailing cursor token, `src/sf2loki/sources/eventlogfile_source.py:726-733`) takes the no-push branch at `src/sf2loki/app.py:427-431` and commits directly. The comment there — "FIFO ordering guarantees any real entry for the same key was already pushed in an earlier flush, so committing these tokens directly preserves the commit-after-push invariant" — is exactly what the abandon path falsifies.

The `PermanentSinkError` drop path (`src/sf2loki/app.py:472-491`) and the egress-budget drop path (`src/sf2loki/app.py:443-453`) also commit deliberately, so they leapfrog an earlier abandonment the same way.

Three code comments currently assert the invariant that is being violated: `src/sf2loki/app.py:463-465` ("checkpoint never advanced" — true only of that one batch), `src/sf2loki/app.py:428-430`, and the `_LaneState` docstring at `src/sf2loki/app.py:102-103` ("Per-key FIFO (and thus commit monotonicity) is preserved").

Reproduced against current `main` with the real `Pipeline` and fakes modelled on `tests/test_pipeline.py`:

- Source yields r1,r2,r3,r4 on key `pubsub:/event/X`, `max_entries=2`, sink raises `RetryableSinkError` on its first push and sets `stop`, then succeeds. Result: sink received only `[r3, r4]`; committed state is `{"pubsub:/event/X": "r4"}`. r1 and r2 were never delivered.
- Same source with r1,r2 followed by a keepalive token r9, sink always fails and sets `stop`. Result: one push attempt, nothing delivered, committed state is `{"pubsub:/event/X": "r9"}`.

Existing coverage does not catch this: `tests/test_pipeline.py:279-296` and `tests/test_pipeline.py:394-417` both assert `state.committed == {}` for a **single** abandoned flush, never for a later flush on the same key.

## Why it matters

Silent, permanent data loss on a routine operation: a deploy or restart (SIGTERM) that lands during a Loki blip while a backlog sits in the lane queue. Concretely, Loki returns 429/5xx for one batch, the operator restarts the service, the first batch is abandoned, and any later flush for the same key advances the checkpoint. Pub/Sub resumes from the committed replay id on restart, so the abandoned events are behind the subscription position and are never re-fetched. The same shape applies to `eventlogfile` and `eventlog_objects`, where a later watermark token commits past rows/files that were dropped on the floor.

Nothing reports it. Abandoned entries do not increment `loki_entries_dropped` (that counter is only touched on the permanent-error and budget-drop paths, `src/sf2loki/app.py:443-453` and `src/sf2loki/app.py:479`); the only signal is `loki_push{outcome="retried"}`, which looks identical to a retry that later succeeded. Grafana shows a clean checkpoint that has moved forward.

The window is wide, not a narrow race: `_drain_with_grace` (`src/sf2loki/app.py:587-614`) intentionally lets the pipeline keep draining for `service.shutdown_grace` seconds after `stop` fires, and once `stop` is set the retry loop returns on the *first* failure per batch, so the whole backlog is burned through inside the grace window.

## Proposed approach

Track abandonment per lane and suppress commits for the affected keys for the remainder of the run. Delivery of later batches can continue (at-least-once duplicates are tolerable and are re-deduplicated by Loki on replay); only the checkpoint advance must be blocked, so that everything from the abandoned position is re-ingested after restart.

1. Add `abandoned_keys: set[str] = field(default_factory=set)` to `_LaneState` (`src/sf2loki/app.py:96-114`). Lanes are rebuilt per run (`self._lanes = []` at `src/sf2loki/app.py:265`, `_new_lane`), so a re-acquired leader starts clean.
2. In both shutdown-abandon returns (`src/sf2loki/app.py:462-466` and `src/sf2loki/app.py:467-469`), before returning: add every `entry.checkpoint.key` in `all_entries` (real **and** `checkpoint_only`, since the tokens riding the batch belong to the same keys) to `lane.abandoned_keys`, log a warning naming the keys and the entry count, and increment a new counter (for example `pipeline_entries_abandoned_total{reason="shutdown"}`) so the loss window is observable in the dashboards.
3. Change `_commit` to `_commit(self, batch: Batch, lane: _LaneState)` and drop any key present in `lane.abandoned_keys` from the `last` map before the `commit_many`/`commit` call and before `_record_commit_metric`. Update all four call sites: the keepalive-only branch (`src/sf2loki/app.py:431`), the budget-drop branch (`src/sf2loki/app.py:452`), the `PermanentSinkError` branch (`src/sf2loki/app.py:490`) and the success branch (`src/sf2loki/app.py:496`).
4. Fix the three comments that assert the old invariant: `src/sf2loki/app.py:428-430`, `src/sf2loki/app.py:463-465`, and the `_LaneState` docstring at `src/sf2loki/app.py:102-103`.

A coarser variant — a single `lane.abandoned` boolean that makes `_flush` a no-op (no push, no commit) for the rest of the run — is also safe and simpler, but it holds back deliverable data for unrelated keys sharing the lane (multi-org, or several ELF event types) and gives up telemetry that would otherwise have shipped. Prefer the per-key set; document the choice in the code comment either way.

---

Imported from GitHub issue #86 on 2026-08-14, when this repo migrated from GitHub Issues to Backlog.md. The original issue has been deleted; its verbatim body, labels and comments are preserved in `archive/issues-dump.json` (`jq '.[] | select(.number == 86)' archive/issues-dump.json`).

## Additional evidence (parallel review lanes)

Two further independent reproductions of the same defect confirmed the cross-batch cases with `max_entries=1` (the real entry and the later token in separate batches): a keepalive token following an abandoned real entry commits `{"pubsub:/event/X": "v2"}` with zero batches ever pushed, and a recovered sink commits past the abandoned batch. Downstream there is no monotonicity backstop: `FileCheckpointStore.commit_many` is a blind `self._cache.update(items)` (src/sf2loki/state/file_store.py:216-226) and the remote stores mirror it.

<sub>Filed from the 2026-07-30 full-repo audit (11 finder lanes + adversarial verification per finding).</sub>
<!-- SECTION:DESCRIPTION:END -->

## Acceptance Criteria
<!-- AC:BEGIN -->
- [ ] #1 `_LaneState` carries per-lane abandoned checkpoint keys, populated by both shutdown-abandon returns in `_flush`.
- [ ] #2 `_commit` never writes (and never records a commit metric for) a key in the lane's abandoned set.
- [ ] #3 Abandonment emits a WARNING log naming the affected keys/entry count and increments a dedicated abandoned-entries counter.
- [ ] #4 Test: batch 1 on key `pubsub:/event/X` is abandoned during shutdown, batch 2 on the same key pushes successfully — the key is not committed (currently commits batch 2's value).
- [ ] #5 Test: batch 1 abandoned during shutdown, batch 2 is `checkpoint_only`-only (keepalive) — the direct-commit branch at `src/sf2loki/app.py:427-431` commits nothing (currently commits the keepalive value with the sink still down).
- [ ] #6 Test: batch 1 abandoned during shutdown, batch 2 on the same key raises `PermanentSinkError` (and, separately, is rejected by the egress governor) — neither drop path commits the abandoned key.
- [ ] #7 Test: a key untouched by the abandoned batch still commits normally in the same lane after an abandonment.
- [ ] #8 Test: a second `Pipeline.run` (re-acquired leadership) starts with an empty abandoned set and commits normally.
- [ ] #9 Existing `tests/test_pipeline.py::test_flush_retry_loop_is_stop_aware` and `tests/test_pipeline.py::test_keepalive_token_not_committed_when_push_abandoned` still pass unchanged.
- [ ] #10 Comments at `src/sf2loki/app.py:102-103`, `src/sf2loki/app.py:428-430` and `src/sf2loki/app.py:463-465` describe the actual invariant.
- [ ] #11 `just gate` green.
<!-- AC:END -->

## Definition of Done
<!-- DOD:BEGIN -->
- [ ] #1 just gate is green (ruff check + ruff format --check + mypy src + pytest) — run it, don't assert it
- [ ] #2 just gen-config run and its output committed, if config.py changed (CI drift gate fails otherwise)
- [ ] #3 committed straight to main with a conventional-commit message, and pushed
<!-- DOD:END -->
