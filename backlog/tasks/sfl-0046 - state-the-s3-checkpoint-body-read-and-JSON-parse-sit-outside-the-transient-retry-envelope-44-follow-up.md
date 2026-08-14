---
id: SFL-0046
title: >-
  state: the s3 checkpoint body read and JSON parse sit outside the
  transient-retry envelope (#44 follow-up)
status: To Do
assignee: []
created_date: '2026-08-14 16:58'
labels:
  - followup
  - phase-4
milestone: m-0
dependencies: []
references:
  - 'https://github.com/rknightion/sf2loki/issues/130'
ordinal: 46000
---

## Description

<!-- SECTION:DESCRIPTION:BEGIN -->
## What

`S3CheckpointStore._ensure_loaded` retries only the GetObject *call*, not the body transfer or the parse.

`src/sf2loki/state/s3_store.py:230-247`:

```python
async def _do_get() -> dict[str, Any] | None:
    try:
        result: dict[str, Any] = await client.get_object(
            Bucket=self._cfg.bucket, Key=self._cfg.key
        )
        return result
    except Exception as exc:
        if _error_code(exc) in _NOT_FOUND_CODES:
            return None
        raise

resp = await _retry_transient(_do_get)      # s3_store.py:241 - retried
if resp is None:
    ...
body = await resp["Body"].read()            # s3_store.py:246 - NOT retried
data = json.loads(body)                     # s3_store.py:247 - NOT translated
```

aiobotocore's `get_object` resolves once the response *headers* arrive; `resp["Body"]` is an unread `StreamingBody`. The byte transfer at `s3_store.py:246` is therefore a separate network operation sitting outside the bounded retry that issue #44 added, and the parse at `s3_store.py:247` has no corrupt-object translation.

Two distinct defects:

1. **Mid-transfer failure is not retried (S3 only).** A connection reset or a botocore read timeout between response headers and the end of the small body read propagates raw out of `_ensure_loaded`. `GcsCheckpointStore` has no equivalent hole — `gcs_store.py:200-204` wraps the whole `client.download(...)` (which returns `bytes`) inside `_retry_transient`.
2. **A truncated or non-UTF-8 body raises a bare `json.JSONDecodeError`/`UnicodeDecodeError` (S3 and GCS).** The branch three lines below at `s3_store.py:248-252` raises `StateObjectCorruptError` naming `s3://{bucket}/{key}` for a non-dict document, and `file_store.py:143-150` catches `json.JSONDecodeError`/`UnicodeDecodeError` and raises `StateFileCorruptError` naming the path plus the recovery step. The S3/GCS parse paths (`s3_store.py:247`, `gcs_store.py:205`) do neither, so the operator gets `Expecting value: line 1 column 1 (char 0)` with no bucket, no key, and no pointer to the documented recovery (`docs/deployment/state.md:80` — `state delete` when "the checkpoint's own file/object is what's corrupt").

Neither exception is contained anywhere upstream:

- `Pipeline._commit` (`src/sf2loki/app.py:499-513`) calls `commit_many` with no exception handling; `commit_many` calls `_ensure_loaded` at `s3_store.py:274`. `_flush`/`_consume` (`app.py:419-497`) catch only `RetryableSinkError`, `PermanentSinkError` and `TimeoutError`, so the exception escapes the consumer task and `_on_pipeline_done` (`app.py:1168-1177`) records it in `crash` and sets `stop` — the whole process exits nonzero. This is the crash shape issue #44 was filed to remove.
- `sf2loki state show/set/delete` reaches the same code via `_whole_document` (`src/sf2loki/statecmd.py:139-140`); `_run_with_store` (`statecmd.py:114-131`) handles only `StateFileLockError` and `StateStoreConflictError`, so a corrupt object prints a traceback rather than an operator message.

Secondary problem the fix must account for: a botocore body-read failure (`ReadTimeoutError`, `ResponseStreamingError`) is rooted in `BotoCoreError`, not the builtin `OSError`/`ConnectionError` family, and carries no `.response` dict. `_is_transient` (`s3_store.py:112-131`) classifies on a botocore error code, an HTTP status >= 500, or `isinstance(exc, TimeoutError | ConnectionError | OSError)` — none of which match. Moving the read inside the retried closure is therefore necessary but not sufficient; the classifier needs to recognise that family too (by duck-typed class name, since this module deliberately never imports botocore or aiohttp — see the module docstring at `s3_store.py:9-14`).

No test covers either defect. The load-retry tests fail the *call*, not the transfer (`tests/state/test_s3_store.py:434-457` monkeypatches `backend.get_object`; `tests/state/test_gcs_store.py:442-463` monkeypatches `download_metadata`), and the test double `FakeStreamingBody.read` (`tests/state/test_s3_store.py:40-45`) cannot raise. The corrupt-document test (`tests/state/test_s3_store.py:277-286`) covers only a valid-JSON non-dict array.

## Why it matters

`state.store = s3` with a shared bucket is the required backend for the active-passive HA topology (`docs/deployment/high-availability.md`). `_ensure_loaded` runs on the first `load` of each process and again on the first `commit_many` after every `reset()` (leadership demote then re-promote, `s3_store.py:193-202`), so the exposed window is every startup and every failover — precisely when the object store is most likely to be answering slowly.

Concrete failure: the daemon is promoted to leader, the first flush calls `commit_many` → `_ensure_loaded`, the GetObject response headers arrive, and the connection resets (or the botocore read timeout fires) before the body is fully read. Instead of the bounded retry #44 added for that class of blip, the process crashes and restarts — dropping every gRPC Pub/Sub stream and re-authing every org, the exact churn #44 set out to prevent. No checkpoint is lost (nothing was committed), so the cost is restart churn, not data loss.

Second failure: an object truncated by a non-CAS writer, a hand-edit, or a partial upload makes both the daemon and `sf2loki state show` fail with a bare JSON parse error that names neither the bucket/key nor the recovery step, while the equivalent file-store failure (`file_store.py:145-150`) spells both out.

## Proposed approach

1. Move the body read inside the retried closure in `s3_store.py`, so a mid-transfer failure re-issues the GET rather than propagating. Return the ETag alongside the bytes so the caller keeps the value it currently reads from `resp.get("ETag")` at `s3_store.py:254`:

   ```python
   async def _do_get() -> tuple[bytes, str | None] | None:
       try:
           resp = await client.get_object(Bucket=self._cfg.bucket, Key=self._cfg.key)
           body: bytes = await resp["Body"].read()
           return body, resp.get("ETag")
       except Exception as exc:
           if _error_code(exc) in _NOT_FOUND_CODES:
               return None
           raise
   ```

   Re-reading the whole small document on retry is correct: the read is idempotent and the ETag is re-fetched with it, so a retry cannot pair one generation's bytes with another's ETag.

2. Extend `_is_transient` (`s3_store.py:112-131`) to classify botocore's streaming/connection failures, which have neither a botocore error code nor an HTTP status. Match on class name to preserve the no-botocore-import property, e.g. `type(exc).__name__ in {"ReadTimeoutError", "ResponseStreamingError", "ConnectTimeoutError", "EndpointConnectionError", "ConnectionClosedError", "IncompleteReadError", "ClientPayloadError"}`. Verify the real hierarchy against the installed `botocore`/`aiohttp` before finalising the set, and keep the existing precondition-conflict fail-fast behaviour untouched (`s3_store.py:117-119` docstring; `StateStoreConflictError` must never be retried).

3. Wrap the parse in both remote stores with the same translation the file store uses. In `s3_store.py` around line 247 and `gcs_store.py` around line 205:

   ```python
   try:
       data = json.loads(body)
   except (json.JSONDecodeError, UnicodeDecodeError) as exc:
       raise StateObjectCorruptError(
           f"state object s3://{self._cfg.bucket}/{self._cfg.key} is corrupt ({exc}); "
           "refusing to start rather than silently discarding checkpoints. Use "
           "`sf2loki state delete KEY` (or replace the object) to reset the affected "
           "sources to their lookback defaults."
       ) from exc
   ```

   Keep the existing non-dict branch as-is; both paths then raise the same exception type.

4. Add `StateObjectCorruptError` (and `StateFileCorruptError`) to the handled-exception list in `statecmd._run_with_store` (`statecmd.py:114-131`), printing the message plus the `state delete` recovery hint and returning `_OPERATION_ERROR_EXIT_CODE` instead of a traceback.

5. Make the S3 test double able to fail mid-read: give `FakeStreamingBody` (`tests/state/test_s3_store.py:40-45`) an injectable failure so `read()` can raise on the first N calls.

---

Imported from GitHub issue #130 on 2026-08-14, when this repo migrated from GitHub Issues to Backlog.md. The original issue has been deleted; its verbatim body, labels and comments are preserved in `archive/issues-dump.json` (`jq '.[] | select(.number == 130)' archive/issues-dump.json`).

<sub>Filed from the 2026-07-30 full-repo audit (11 finder lanes + adversarial verification per finding).</sub>
<!-- SECTION:DESCRIPTION:END -->

## Acceptance Criteria
<!-- AC:BEGIN -->
- [ ] #1 `S3CheckpointStore._ensure_loaded` performs the body read inside the closure passed to `_retry_transient`; no network or stream operation remains between the retry envelope and the parse.
- [ ] #2 `_is_transient` in `s3_store.py` classifies botocore streaming/connection failures (no `.response` dict, not builtin `OSError`) as transient, with the exact class-name set verified against the installed `botocore`/`aiohttp`.
- [ ] #3 `StateStoreConflictError` and the 412 precondition conflict are still never retried (existing `test_precondition_conflict_is_not_retried` stays green).
- [ ] #4 A truncated or invalid-UTF-8 checkpoint object raises `StateObjectCorruptError` naming `s3://bucket/key` / `gs://bucket/object` and the recovery step, from both `s3_store.py` and `gcs_store.py`.
- [ ] #5 `sf2loki state show` against a corrupt object prints the actionable message and exits with the operation-error code, not a traceback.
- [ ] #6 Test: `FakeStreamingBody.read()` raises a transient-shaped error on the first call then succeeds → `store.load(...)` returns the value and `backend.get_object_calls == 2` (proving the GET was re-issued, not just the read retried).
- [ ] #7 Test: `FakeStreamingBody.read()` always raises a transient-shaped error → the underlying exception surfaces after `_MAX_ATTEMPTS` attempts (monkeypatched low, as in `tests/state/test_s3_store.py:407-431`).
- [ ] #8 Test: an object whose body is `b'{"k1": "v1"'` (truncated) raises `StateObjectCorruptError` mentioning the bucket and key, for both the S3 and GCS stores.
- [ ] #9 Test: an object whose body is invalid UTF-8 (e.g. `b"\xff\xfe"`) raises `StateObjectCorruptError`, not `UnicodeDecodeError`.
- [ ] #10 Test: `run_state_show` against a corrupt object returns the operation-error exit code and writes the recovery text to stderr.
- [ ] #11 `just gate` green.
<!-- AC:END -->

## Definition of Done
<!-- DOD:BEGIN -->
- [ ] #1 just gate is green (ruff check + ruff format --check + mypy src + pytest) — run it, don't assert it
- [ ] #2 just gen-config run and its output committed, if config.py changed (CI drift gate fails otherwise)
- [ ] #3 committed straight to main with a conventional-commit message, and pushed
<!-- DOD:END -->
