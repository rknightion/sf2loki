---
id: SFL-0029
title: >-
  tests: pin the ApexLog error-recovery paths - listing failure, mid-page
  throttle abort, body-download fallback
status: To Do
assignee: []
created_date: '2026-08-14 16:58'
labels:
  - followup
  - phase-2
milestone: m-3
dependencies: []
references:
  - 'https://github.com/rknightion/sf2loki/issues/113'
ordinal: 29000
---

## Description

<!-- SECTION:DESCRIPTION:BEGIN -->
## What

Three error-recovery branches in the ApexLog source and three in its client have no covering test. Measured with stdlib `trace` over `tests/sources/test_apexlog_source.py`, `tests/salesforce/test_apexlog_client.py`, `tests/test_doctor.py`, `tests/test_app.py` (79 passed); the repo venv has neither `coverage` nor `pytest-cov`, so `just gate` does not report this.

Uncovered statement lines in `src/sf2loki/sources/apexlog_source.py`:

| lines | behaviour |
| --- | --- |
| 190-204 | `except ApexLogError` around `list_logs`: increments `self._consecutive_failures`, increments `soql_poll_errors{source=apexlog,object=apexlog}`, logs at WARNING below `_ERROR_LOG_THRESHOLD` (3, defined at :60) and ERROR at or above it, then `return`s so the cycle ends cleanly and retries next poll |
| 267-273 | `except ApexLogThrottledError` around `await self._build_entry(...)`: a throttle on the *body* download aborts the whole remaining page, increments `soql_poll_errors`, logs ERROR, `return`s |
| 344-345 | `_resolve_line`: the bare `except ApexLogThrottledError: raise` that propagates a throttle to the caller |
| 346-353 | `_resolve_line`: plain `ApexLogError` -> `apexlog_bodies_skipped{reason=download_error}`, WARNING, and a metadata-only line with `body_skipped="true"` / `body_skip_reason="download_error"` |

Uncovered statement lines in `src/sf2loki/salesforce/apexlog_client.py`:

| lines | behaviour |
| --- | --- |
| 146-147 | `SoqlError` -> `ApexLogError("ApexLog listing failed: ...")` |
| 184-186 | `httpx.HTTPError` on the Body GET -> `apexlog_download_errors{reason=transport}` + `ApexLogError` |
| 199-204 | HTTP 403 whose body contains `REQUEST_LIMIT_EXCEEDED` -> `salesforce_api_throttled{api=apexlog_body}` + `ApexLogThrottledError` |
| 113-116 | the `since_id` compound-cursor `WHERE` clause (`StartTime > lit OR (StartTime = lit AND Id > 'since_id')`) from #39 - never asserted at the client level; only a source-level fake (`TieBoundaryClient`, `tests/sources/test_apexlog_source.py:59`) reimplements the predicate |
| 163-166 | `count_active_traceflags` throttle/error mapping |

What the existing tests do cover: the listing-throttle path only (`tests/sources/test_apexlog_source.py:159` raises `ApexLogThrottledError` from `list_logs`, hitting `apexlog_source.py:186-189`; `tests/salesforce/test_apexlog_client.py:97` covers `apexlog_client.py:144-145`), the oversize body skip (`tests/sources/test_apexlog_source.py:103`, `reason="size"`), and body download 200 / 401-retry / 404 (`tests/salesforce/test_apexlog_client.py:107`, `:121`, `:131`). `tests/sources/test_apexlog_source.py` does not import `ApexLogError` at all, and `FakeApexClient.download_body` (`:52`) can never raise.

## Why it matters

`ApexLogError` escaping `_poll` kills the process. `src/sf2loki/app.py:301-322` `_produce` wraps `source.events(...)` in `try`/`finally` with no `except`, so the exception propagates through `producers_done` (`app.py:278`) into `_run_pipeline`; `app.py:1168-1176` `_on_pipeline_done` then appends it to `crash` and calls `stop.set()`, taking the daemon down nonzero. A refactor that drops the handler at `apexlog_source.py:190` turns one transient Tooling API 500 into a full restart, and no test fails.

`ApexLogThrottledError` subclasses `ApexLogError` (`src/sf2loki/salesforce/apexlog_client.py:45`). Deleting the bare re-raise at `apexlog_source.py:344-345` therefore does not break anything visibly - the throttle falls through to `except ApexLogError` at :346 and becomes a per-log `download_error` skip. The source then keeps issuing one Body GET per remaining log in the page against an already-exhausted org API budget, exactly the outcome the abort at :267-273 exists to prevent. The suite stays green through that regression.

The sibling sources already pin equivalent behaviour, so ApexLog is the outlier rather than a deliberate omission: `tests/sources/test_eventlog_objects_source.py:717` (`soql_poll_errors` on a listing 500), `:742` (`soql_poll_errors` on a 403 `REQUEST_LIMIT_EXCEEDED`), `:1335` (WARNING-then-ERROR escalation asserted via `caplog`), and `tests/sources/test_eventlogfile_source.py:1189` (download failures counted by the client counter, not double-counted as a poll error).

## Proposed approach

Blocker first: `src/sf2loki/salesforce/apexlog_client.py:55` reads `except TypeError, ValueError:` (Python 2 syntax). The module does not parse, so the suite cannot collect. Restore the parenthesized tuple `except (TypeError, ValueError):` before anything else; the `_as_int` None branch (`:51-52`) and that fallback (`:56`) are themselves untested and can be pinned with a two-line table test.

Extend `tests/sources/test_apexlog_source.py` using the existing `FakeApexClient` subclass pattern (`:162` `ThrottleClient` is the template) and a real `Metrics()` instance, reading counters via `metrics.registry.get_sample_value("<name>_total", {labels})` as the eventlog_objects tests do:

1. `test_listing_error_ends_cycle_and_counts` - subclass whose `list_logs` raises `ApexLogError("boom")`. Assert the `events()` generator completes with no entries and no exception, and `sf2loki_soql_poll_errors_total{source=apexlog,object=apexlog} == 1.0`.
2. `test_consecutive_listing_errors_escalate_to_error` - `poll_once=False`, `poll_interval=timedelta(seconds=0)`, a `stop` event the fake sets after its third call (mirroring `StuckClient` at `tests/sources/test_apexlog_source.py:231`). With `caplog.at_level(logging.WARNING, logger="sf2loki.sources.apexlog_source")`, assert failures 1 and 2 log WARNING and failure 3 logs ERROR (`_ERROR_LOG_THRESHOLD == 3`), and that the record text carries the consecutive count.
3. `test_body_throttle_aborts_rest_of_page` - three logs in one page; `download_body` returns normally for log 1 and raises `ApexLogThrottledError` for log 2. Assert exactly one entry was yielded (log 1), `download_body` was never called for log 3 (record calls on the fake), `soql_poll_errors` incremented once, and no exception escaped.
4. `test_body_download_error_ships_metadata_only` - `download_body` raises `ApexLogError`. Assert the entry IS yielded, `structured_metadata["body_skipped"] == "true"`, `structured_metadata["body_skip_reason"] == "download_error"`, the log id appears in `entry.line` (the metadata JSON fallback), and `sf2loki_apexlog_bodies_skipped_total{reason=download_error} == 1.0`.
5. Guard against the silent regression in 3: a `_consecutive_failures` reset check is already implicit at `apexlog_source.py:277`; add an assertion in test 3 that the yielded log-1 entry carries a checkpoint, so the "checkpoint from the last yielded entry is already safe" comment at :269-270 is pinned.

Extend `tests/salesforce/test_apexlog_client.py` with `respx`:

6. `test_download_body_throttle_raises_apexlog_throttled` - mock the Body URL with `httpx.Response(403, text="REQUEST_LIMIT_EXCEEDED")`. Assert `ApexLogThrottledError`, `sf2loki_salesforce_api_throttled_total{api=apexlog_body} == 1.0`, and `sf2loki_apexlog_download_errors_total{reason="HTTP 403"} == 1.0` (both counters fire; :195-198 runs before :199).
7. `test_download_body_transport_error_counted` - `respx.get(...).mock(side_effect=httpx.ConnectError("refused"))`. Assert `ApexLogError` and `sf2loki_apexlog_download_errors_total{reason=transport} == 1.0`.
8. `test_list_logs_soql_error_raises_apexlog_error` - mock the tooling query with a non-throttle failure (`httpx.Response(500, text="boom")`). Assert `ApexLogError` and that it is not an `ApexLogThrottledError`.
9. `test_list_logs_since_id_uses_compound_predicate` - call `list_logs(since=..., users=[], page_size=200, since_id="07L0042")` and assert the sent `q` param contains both `StartTime >` and `Id > '07L0042'`, and that the no-`since_id` call instead contains `StartTime >=`.

Optionally add `coverage` to the dev dependency group so the gap is measurable from `just gate` rather than requiring an ad-hoc tracer; that is a separate decision and not required to close this issue.

---

Imported from GitHub issue #113 on 2026-08-14, when this repo migrated from GitHub Issues to Backlog.md. The original issue has been deleted; its verbatim body, labels and comments are preserved in `archive/issues-dump.json` (`jq '.[] | select(.number == 113)' archive/issues-dump.json`).

## Scope note

#93 changes the body-download failure handling this issue would pin (adding bounded retry instead of one-shot fallback). Sequence that fix first and pin the post-fix behaviour, or land these tests as the TDD harness for that fix.

<sub>Filed from the 2026-07-30 full-repo audit (11 finder lanes + adversarial verification per finding).</sub>
<!-- SECTION:DESCRIPTION:END -->

## Acceptance Criteria
<!-- AC:BEGIN -->
- [ ] #1 `src/sf2loki/salesforce/apexlog_client.py:55` restored to `except (TypeError, ValueError):` so the module parses and the suite collects
- [ ] #2 `tests/sources/test_apexlog_source.py`: `list_logs` raising `ApexLogError` ends the cycle with zero entries, no exception escapes `events()`, and `sf2loki_soql_poll_errors_total{source=apexlog,object=apexlog}` is 1
- [ ] #3 `tests/sources/test_apexlog_source.py`: consecutive listing failures log WARNING for the first two and ERROR on the third (`_ERROR_LOG_THRESHOLD`), asserted via `caplog` records' `levelname`
- [ ] #4 `tests/sources/test_apexlog_source.py`: `download_body` raising `ApexLogThrottledError` on log 2 of a 3-log page yields only log 1, never calls `download_body` for log 3, increments `soql_poll_errors`, and raises nothing to the caller
- [ ] #5 `tests/sources/test_apexlog_source.py`: `download_body` raising plain `ApexLogError` still yields the entry with `body_skipped="true"` and `body_skip_reason="download_error"`, and increments `sf2loki_apexlog_bodies_skipped_total{reason=download_error}`
- [ ] #6 `tests/salesforce/test_apexlog_client.py`: a 403 containing `REQUEST_LIMIT_EXCEEDED` on `/tooling/sobjects/ApexLog/<id>/Body` raises `ApexLogThrottledError` and increments `sf2loki_salesforce_api_throttled_total{api=apexlog_body}`
- [ ] #7 `tests/salesforce/test_apexlog_client.py`: an `httpx` transport error on the Body GET raises `ApexLogError` and increments `sf2loki_apexlog_download_errors_total{reason=transport}`
- [ ] #8 `tests/salesforce/test_apexlog_client.py`: a non-throttle SOQL failure in `list_logs` raises `ApexLogError` and not `ApexLogThrottledError`
- [ ] #9 `tests/salesforce/test_apexlog_client.py`: `list_logs` with a non-empty `since_id` emits the compound `StartTime > ... OR (StartTime = ... AND Id > '<since_id>')` predicate; without it, `StartTime >=`
- [ ] #10 Every statement line in `src/sf2loki/sources/apexlog_source.py:190-204`, `:267-273`, `:344-353` and `src/sf2loki/salesforce/apexlog_client.py:113-116`, `:146-147`, `:184-186`, `:199-204` executes under the suite
- [ ] #11 `just gate` green (`ruff check`, `ruff format --check`, `mypy src`, `pytest`)
<!-- AC:END -->

## Definition of Done
<!-- DOD:BEGIN -->
- [ ] #1 just gate is green (ruff check + ruff format --check + mypy src + pytest) — run it, don't assert it
- [ ] #2 just gen-config run and its output committed, if config.py changed (CI drift gate fails otherwise)
- [ ] #3 committed straight to main with a conventional-commit message, and pushed
<!-- DOD:END -->
