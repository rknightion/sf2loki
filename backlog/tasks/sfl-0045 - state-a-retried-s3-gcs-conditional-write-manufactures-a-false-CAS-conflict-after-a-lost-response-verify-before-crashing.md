---
id: SFL-0045
title: >-
  state: a retried s3/gcs conditional write manufactures a false CAS conflict
  after a lost response - verify before crashing
status: To Do
assignee: []
created_date: '2026-08-14 16:58'
labels:
  - followup
  - phase-4
milestone: m-0
dependencies: []
references:
  - 'https://github.com/rknightion/sf2loki/issues/129'
ordinal: 45000
---

## Description

<!-- SECTION:DESCRIPTION:BEGIN -->
## What

`S3CheckpointStore.commit_many` builds the conditional-write precondition **once**, outside the retry
closure, and the retry re-sends it verbatim:

- `put_kwargs` is assembled at `src/sf2loki/state/s3_store.py:279-287` — `IfNoneMatch="*"` when
  `self._etag is None`, otherwise `IfMatch=self._etag`.
- `_do_put` (`src/sf2loki/state/s3_store.py:289-304`) closes over that dict and is re-invoked by
  `_retry_transient` up to `_MAX_ATTEMPTS = 4` (`src/sf2loki/state/s3_store.py:40`, `134-147`, `306`).
- `self._cache` / `self._etag` are updated only after a fully successful call
  (`src/sf2loki/state/s3_store.py:307-308`).

`_is_transient` (`src/sf2loki/state/s3_store.py:112-131`) classifies as retryable: the botocore codes
in `_TRANSIENT_CODES` (`SlowDown`, `InternalError`, `ServiceUnavailable`, `RequestTimeout`, ...), any
HTTP status `>= 500`, and bare `TimeoutError | ConnectionError | OSError`. Several of those are
**indeterminate-outcome** failures on a PUT: S3 can have applied the object write before the response
was lost (connection reset while reading the response, a 500 `InternalError` after the write landed).
aiobotocore's own botocore retry layer re-sends the identical request — same `IfMatch` header — on
socket errors and 5xx as well, so the same hazard exists one layer below `_retry_transient`.

When attempt 1 applied but did not acknowledge, the object's ETag has already moved. Attempt 2 sends
the now-stale `IfMatch` (or `IfNoneMatch: *` on a first write whose object now exists) and S3 answers
`412 PreconditionFailed`. `src/sf2loki/state/s3_store.py:296-303` unconditionally converts that into
`StateStoreConflictError` with the message *"another sf2loki instance is writing the same state object
... point each instance at its own key"*. The module comment at `src/sf2loki/state/s3_store.py:36-38`
records the assumption behind this — "it means another writer won the race, not a transient blip" —
which is precisely what is untrue on a retry attempt.

`StateStoreConflictError` is deliberately not retryable and there is no handler for it in the daemon
(the only one is the CLI at `src/sf2loki/statecmd.py:127`), so it terminates the process:

- `Pipeline._commit` (`src/sf2loki/app.py:499-510`) has no exception handling and is awaited from
  `_flush` / `_consume` (`src/sf2loki/app.py:431`, `451`, `486`, `496`).
- `Pipeline.run` re-raises a dead consumer's exception (`src/sf2loki/app.py:284-290`).
- `_run_pipeline` absorbs only `StateFenceError` (`src/sf2loki/app.py:1236-1238`).
- `_on_pipeline_done` records the crash and stops the app (`src/sf2loki/app.py:1168-1176`); `run()`
  re-raises at `src/sf2loki/app.py:1225` → nonzero exit.

`GcsCheckpointStore` has the identical shape: the `ifGenerationMatch` precondition is built at
`src/sf2loki/state/gcs_store.py:236-240`, the closure is retried at
`src/sf2loki/state/gcs_store.py:262`, and the 412 → `StateStoreConflictError` conversion is at
`src/sf2loki/state/gcs_store.py:252-259`. A lost-ack **first** upload moves the generation off `0`, so
the retry's `ifGenerationMatch: "0"` fails. Both stores repeat the pattern in `delete()`
(`src/sf2loki/state/s3_store.py:330-357`, `src/sf2loki/state/gcs_store.py:286-312`).

Related but distinct prior work: #44 added the bounded transient retry and explicitly kept the 412
fail-fast; that retry is what opens this window. #48 fixed a different source of the same
false-conflict (a stale CAS token across demote → promote) with `reset()`
(`src/sf2loki/state/s3_store.py:193-203`), which does not apply here because leadership never changed.

## Why it matters

Single-instance deployment with `state.store: s3`. A network blip drops the connection after S3 applied
the checkpoint PUT but before the 200 arrived. The retry re-sends the stale precondition, gets 412, and
the daemon exits nonzero — dropping every gRPC Pub/Sub stream and re-authing all orgs on restart. That
is exactly the harm #44 was filed to remove, reintroduced in the window #44's retry was meant to cover.

There is no data loss: the applied checkpoint is durable, and the restarted process re-reads the object
in `_ensure_loaded` and resumes correctly. The costs are (a) a spurious crash/restart under an ordinary
transient condition, and (b) an error message that asserts a split-brain which does not exist, pointing
the operator at a non-existent second instance and inviting a wrong config change (a new key per
instance) in response.

## Proposed approach

Distinguish a self-inflicted 412 from a genuine CAS loss by verifying the object's current content
before declaring a conflict, and only on a **retry** attempt (a 412 on the very first PUT of a call is
still a genuine race and must keep failing fast).

S3 (`src/sf2loki/state/s3_store.py`, `commit_many` and `delete`):

1. Track attempts inside the call: a `nonlocal`/mutable counter incremented at the top of `_do_put`.
2. On 412 with `attempts == 1`, raise `StateStoreConflictError` exactly as today (no extra request).
3. On 412 with `attempts > 1`, re-`get_object` the key once (through `_retry_transient` so the probe is
   itself blip-tolerant) and compare the returned body to the `body` bytes this call wrote:
   - identical → this call's own earlier attempt landed. Log a warning naming the condition
     (applied-but-unacknowledged write recovered) and return a synthetic success carrying the fetched
     `ETag`, so the caller's `self._cache = new_cache` / `self._etag = resp["ETag"]` at
     `src/sf2loki/state/s3_store.py:307-308` re-syncs the CAS token.
   - different, or the probe fails, or the object is absent → raise `StateStoreConflictError` (keep the
     current message).

GCS (`src/sf2loki/state/gcs_store.py`, `commit_many` and `delete`): same structure, using `download`
for the body plus `download_metadata` for the new `generation` to re-sync `self._generation`.

Factor the attempt-counting + verify-on-retry logic so both methods in each store share it rather than
duplicating it four times. Byte comparison is sound because the written document is deterministic
(`json.dumps(new_cache).encode("utf-8")`).

---

Imported from GitHub issue #129 on 2026-08-14, when this repo migrated from GitHub Issues to Backlog.md. The original issue has been deleted; its verbatim body, labels and comments are preserved in `archive/issues-dump.json` (`jq '.[] | select(.number == 129)' archive/issues-dump.json`).

<sub>Filed from the 2026-07-30 full-repo audit (11 finder lanes + adversarial verification per finding).</sub>
<!-- SECTION:DESCRIPTION:END -->

## Acceptance Criteria
<!-- AC:BEGIN -->
- [ ] #1 A 412 on a **retry** attempt whose object content matches what the call wrote no longer raises; the commit is reported as successful and the store's cached ETag/generation is refreshed.
- [ ] #2 A 412 on the **first** PUT attempt of a call still raises `StateStoreConflictError` immediately, with no additional GET/download issued.
- [ ] #3 A 412 on a retry attempt whose object content **differs** from what the call wrote still raises `StateStoreConflictError`.
- [ ] #4 The recovered self-conflict is logged at WARNING so the condition is observable in production.
- [ ] #5 Both `commit_many` and `delete` are covered, in both `s3_store.py` and `gcs_store.py`.
- [ ] #6 `tests/state/test_s3_store.py`: fake backend whose `put_object` **applies** the write and then raises a 500 `InternalError` on the first call → `commit()` returns without raising, the document contains the committed keys, and an immediately following `commit()` of a different key succeeds (proving the ETag was re-synced).
- [ ] #7 `tests/state/test_s3_store.py`: same shape where the retry's 412 accompanies a **third-party** document body → `StateStoreConflictError` still raised.
- [ ] #8 `tests/state/test_gcs_store.py`: the two equivalents, including the first-upload case where the lost-ack write moves the generation off `0` and the retry sends `ifGenerationMatch: "0"`.
- [ ] #9 Existing `test_precondition_conflict_is_not_retried` (`tests/state/test_s3_store.py:479-499`) and its GCS counterpart still pass unchanged, including their `put_object_calls == 2` assertion.
- [ ] #10 A test covering the same applied-but-unacknowledged case for `delete()` in at least one store.
- [ ] #11 `just gate` green.
<!-- AC:END -->

## Definition of Done
<!-- DOD:BEGIN -->
- [ ] #1 just gate is green (ruff check + ruff format --check + mypy src + pytest) — run it, don't assert it
- [ ] #2 just gen-config run and its output committed, if config.py changed (CI drift gate fails otherwise)
- [ ] #3 committed straight to main with a conventional-commit message, and pushed
<!-- DOD:END -->
