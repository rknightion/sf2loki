---
id: SFL-0054
title: >-
  tests: pin the untested legs of the s3/gcs checkpoint retry classifier (raw
  5xx, OSError family, non-Exception)
status: To Do
assignee: []
created_date: '2026-08-14 16:58'
labels:
  - followup
  - phase-1
milestone: m-3
dependencies: []
references:
  - 'https://github.com/rknightion/sf2loki/issues/138'
ordinal: 54000
---

## Description

<!-- SECTION:DESCRIPTION:BEGIN -->
## What

`_is_transient` in both object-storage checkpoint stores classifies which errors the bounded retry added by #44 will retry. Three of its four decision legs have no test.

`src/sf2loki/state/s3_store.py:112-131`:

```python
def _is_transient(exc: BaseException) -> bool:
    if not isinstance(exc, Exception):          # 120  -> 121 UNCOVERED
        return False
    if _error_code(exc) in _TRANSIENT_CODES:    # 122  -> 123 covered
        return True
    status = _status_code(exc)
    if status is not None and status >= 500:    # 125  -> 126 UNCOVERED
        return True
    # TCP resets / connection drops surface as bare OSError-family exceptions
    # with no botocore response shape at all.
    if isinstance(exc, TimeoutError | ConnectionError | OSError):  # 129 -> 130 UNCOVERED
        return True
    return False
```

`src/sf2loki/state/gcs_store.py:70-86` is the same shape minus the error-code leg: line 78 (non-`Exception` guard) and line 85 (OSError family) are uncovered; the `status >= 500` leg at gcs_store.py:80-81 *is* covered.

Measured coverage over `tests/state` + `tests/test_statecmd.py`:

```
Name                             Stmts   Miss  Cover   Missing
src/sf2loki/state/gcs_store.py     140      8    94%   78, 85, 163-167, 310
src/sf2loki/state/s3_store.py      170     15    91%   76, 89, 121, 126, 130, 208-215, 336, 355
```

Why the legs never execute: every exception double in the test suite is `FakeClientError(code, status)` (tests/state/test_s3_store.py:29-37), `FakeGcsError(status)` (tests/state/test_gcs_store.py:29), or their `test_statecmd.py` equivalents, and the complete set of values raised anywhere is `NoSuchKey`/404, `PreconditionFailed`/412, `SlowDown`/503, `InternalError`/500. Both s3 5xx doubles carry a code that is already in `_TRANSIENT_CODES` (s3_store.py:97-109), so `_is_transient` returns at s3_store.py:123 and the HTTP-status fallback at 125-126 is never reached. No test in the repository raises an OSError-family exception into either store. `_is_transient` has no direct unit test.

The three uncovered legs are live and correct today — verified by calling the predicate directly:

| input | `s3_store._is_transient` | `gcs_store._is_transient` |
| --- | --- | --- |
| code `SomethingNew`, HTTP 500 | `True` (via :126) | n/a |
| code `SomethingNew`, HTTP 400 | `False` | n/a |
| `ConnectionResetError` | `True` (via :130) | `True` (via :85) |
| `TimeoutError` | `True` (via :130) | `True` (via :85) |
| `asyncio.CancelledError` | `False` (via :121) | `False` (via :78) |

So the work here is regression pins for behaviour that already works, not a bug fix.

## Why it matters

The retry exists because a transient object-store error on a checkpoint commit used to crash the daemon (#44): the exception propagates out of `commit_many` (s3_store.py:306, gcs_store.py:262) through the pipeline consumer and kills the process, dropping every gRPC stream and re-authing all orgs on restart. #44's acceptance bar was literally "503s twice then succeeds; a 412 still raises `StateStoreConflictError`", which is exactly what tests/state/test_s3_store.py:407, 434, 461, 480 and tests/state/test_gcs_store.py:411, 442 assert — so the code-agnostic legs were never pinned.

Consequences of the gap:

- **OSError-family leg (s3_store.py:130, gcs_store.py:85).** aiohttp surfaces connection failures as `ClientOSError`/`ClientConnectorError`, both `OSError` subclasses, so for the GCS path this is the leg a real TCP reset lands on. Dropping the `isinstance` arm — or adding an early `return False` after the error-code check — reinstates the #44 crash-loop for reset/timeout errors while all six existing retry tests stay green, because every one of them carries a transient botocore code or a 5xx status.
- **Raw-5xx leg (s3_store.py:126).** `_TRANSIENT_CODES` is an AWS-specific allowlist. S3-compatible stores (MinIO, R2, Ceph) return codes outside it with 500/502/503 statuses; the status fallback is the only thing that retries those. Untested, it can be narrowed or reordered silently.
- **Non-`Exception` guard (s3_store.py:121, gcs_store.py:78).** `tenacity/asyncio/__init__.py:119` catches `BaseException`, so the retry predicate is asked about `asyncio.CancelledError` too. This guard is what stops a cancellation during shutdown from being treated as retryable and looped over up to `_MAX_ATTEMPTS` with backoff, delaying or hanging shutdown. Nothing asserts it.

Exposure is bounded: both stores are opt-in extras (`sf2loki[s3]`, `sf2loki[gcs]` — pyproject.toml:29-30) and the file store is the default backend, so this is a silent-regression window rather than a present defect.

## Proposed approach

Add table-driven tests in `tests/state/test_s3_store.py` and `tests/state/test_gcs_store.py`. No new dependency and no extras install needed — the existing in-memory fakes and injected `client_factory` cover it.

1. **Direct predicate tests** (cheapest, pins all legs including the guard). Parametrize over `sf2loki.state.s3_store._is_transient` and `sf2loki.state.gcs_store._is_transient`:
   - s3: `FakeClientError("SlowDown", 503)` -> True; `FakeClientError("SomethingNew", 500)` -> True; `FakeClientError("SomethingNew", 502)` -> True; `FakeClientError("AccessDenied", 403)` -> False; `FakeClientError("SomethingNew", 400)` -> False; `ConnectionResetError("reset")` -> True; `TimeoutError()` -> True; `OSError("broken pipe")` -> True; `StateStoreConflictError("cas")` -> False; `asyncio.CancelledError()` -> False.
   - gcs: `FakeGcsError(503)` -> True; `FakeGcsError(404)` -> False; `FakeGcsError(412)` -> False; `ConnectionResetError("reset")` -> True; `TimeoutError()` -> True; `StateStoreConflictError("cas")` -> False; `asyncio.CancelledError()` -> False.
2. **Behavioural tests through the store** (pins that the classification is actually wired into `_retry_transient`), modelled on `test_commit_survives_two_transient_errors_then_succeeds` (tests/state/test_s3_store.py:407) — monkeypatch `_MAX_ATTEMPTS`/`_WAIT_MIN`/`_WAIT_MAX` the same way so they stay sub-millisecond:
   - s3 `put_object` raises `ConnectionResetError` once then delegates to the real fake -> `commit` completes, `load` round-trips the value, call count is 2.
   - s3 `put_object` raises `FakeClientError("SomethingNew", 500)` once then succeeds -> same assertions (this is the only way to reach s3_store.py:126).
   - gcs `upload` raises `ConnectionResetError` once then succeeds -> same assertions.
   - Mirror at least the `ConnectionResetError` case on the load path (`get_object` / `download_metadata`).
3. **Negative controls.** Keep `test_precondition_conflict_is_not_retried` (tests/state/test_s3_store.py:480, and the gcs equivalent) untouched, and add a non-retryable behavioural control: `put_object`/`upload` raising a 403-shaped error must surface on the first attempt with exactly one call — proving the widened tests did not turn the classifier into "retry everything".
4. Assert exact attempt counts, never just "eventually succeeded" — an attempt-count assertion is what fails if a leg silently stops being retryable.

**Open question to verify during implementation, do not assume:** aiobotocore may wrap transport failures in `botocore.exceptions.EndpointConnectionError` / `ConnectionClosedError`, which are `BotoCoreError` subclasses and (unlike aiohttp's `ClientOSError`) may not be `OSError` subclasses at all — in which case s3_store.py:130 is not the leg a real TCP reset lands on for the S3 backend, and `_is_transient` needs the botocore wrapper shapes added. botocore is not installed in the dev venv (it ships only with the `s3` extra), so this was not checkable here. Check the MRO against the installed `aiobotocore>=2.21`; if the wrappers are not `OSError` subclasses, file a follow-up for the classifier rather than widening scope here, and leave the comment at s3_store.py:127-129 corrected to match reality.

---

Imported from GitHub issue #138 on 2026-08-14, when this repo migrated from GitHub Issues to Backlog.md. The original issue has been deleted; its verbatim body, labels and comments are preserved in `archive/issues-dump.json` (`jq '.[] | select(.number == 138)' archive/issues-dump.json`).

<sub>Filed from the 2026-07-30 full-repo audit (11 finder lanes + adversarial verification per finding).</sub>
<!-- SECTION:DESCRIPTION:END -->

## Acceptance Criteria
<!-- AC:BEGIN -->
- [ ] #1 `_is_transient` in `src/sf2loki/state/s3_store.py` has a parametrized unit test covering all four legs: transient error code, unknown code with HTTP >= 500, OSError-family with no response shape, and non-`Exception` (`asyncio.CancelledError`) -> `False`.
- [ ] #2 `_is_transient` in `src/sf2loki/state/gcs_store.py` has the equivalent parametrized unit test, including the OSError-family leg (gcs_store.py:85) and the non-`Exception` guard (gcs_store.py:78).
- [ ] #3 Behavioural test: `S3CheckpointStore.commit` survives a single `ConnectionResetError` from `put_object` and the value round-trips via `load`, with the attempt count asserted exactly.
- [ ] #4 Behavioural test: `S3CheckpointStore.commit` survives a single error shaped `Error.Code="SomethingNew"` + `ResponseMetadata.HTTPStatusCode=500` (a code outside `_TRANSIENT_CODES`), with the attempt count asserted exactly.
- [ ] #5 Behavioural test: `GcsCheckpointStore.commit` survives a single `ConnectionResetError` from `upload`, with the attempt count asserted exactly.
- [ ] #6 Behavioural test on the load path: a single `ConnectionResetError` from `get_object` (s3) / `download_metadata` (gcs) is retried and `load` returns the seeded value.
- [ ] #7 Negative control: a 403-shaped error is not retried — exactly one call, exception surfaces from `commit`.
- [ ] #8 The existing precondition-conflict-not-retried tests (tests/state/test_s3_store.py:480 and the gcs equivalent) still pass unmodified.
- [ ] #9 Coverage over `tests/state` shows lines 121, 126, 130 of `s3_store.py` and lines 78, 85 of `gcs_store.py` executed (`uv run --with pytest-cov pytest tests/state --cov=sf2loki.state.s3_store --cov=sf2loki.state.gcs_store --cov-report=term-missing`).
- [ ] #10 New tests need no optional extra installed and add no measurable runtime (retry knobs monkeypatched to zero wait, as at tests/state/test_s3_store.py:410-412).
- [ ] #11 The aiobotocore-wrapper open question above is resolved in writing: either confirmed that `OSError` is the right shape for the S3 backend, or a follow-up issue filed for the classifier.
- [ ] #12 `just gate` green.
<!-- AC:END -->

## Definition of Done
<!-- DOD:BEGIN -->
- [ ] #1 just gate is green (ruff check + ruff format --check + mypy src + pytest) — run it, don't assert it
- [ ] #2 just gen-config run and its output committed, if config.py changed (CI drift gate fails otherwise)
- [ ] #3 committed straight to main with a conventional-commit message, and pushed
<!-- DOD:END -->
