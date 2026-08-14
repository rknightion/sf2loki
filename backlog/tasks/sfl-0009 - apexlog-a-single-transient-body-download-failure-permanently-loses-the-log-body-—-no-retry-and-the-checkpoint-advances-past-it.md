---
id: SFL-0009
title: >-
  apexlog: a single transient body-download failure permanently loses the log
  body — no retry, and the checkpoint advances past it
status: To Do
assignee: []
created_date: '2026-08-14 16:58'
labels:
  - followup
  - phase-2
milestone: m-0
dependencies: []
references:
  - 'https://github.com/rknightion/sf2loki/issues/93'
ordinal: 9000
---

## Description

<!-- SECTION:DESCRIPTION:BEGIN -->
## What

The ApexLog source treats any non-throttle body-download failure as a permanent per-log skip, and the entry it emits in place of the body still carries a checkpoint that has already advanced past that log. One transient error therefore destroys the debug-log body — the payload the source exists to ship — even though Salesforce still holds it for the remainder of the 24h ApexLog retention.

Control flow, in order, inside `ApexLogSource._poll`:

1. `src/sf2loki/sources/apexlog_source.py:241-251` advances the in-memory cursor *before* the body is fetched: `watermark = m.start_time`, `window.append(m.id)`, `since_id = m.id`.
2. `src/sf2loki/sources/apexlog_source.py:264` serializes that already-advanced position into `ckpt` (`{"ids": [...m.id], "last_ts": watermark}`).
3. `src/sf2loki/sources/apexlog_source.py:266` calls `_build_entry`, which calls `_resolve_line`, which calls `ApexLogClient.download_body`.
4. `src/sf2loki/sources/apexlog_source.py:342-353`: `ApexLogThrottledError` is re-raised (cycle aborts, safe), but **every other** `ApexLogError` is swallowed — `apexlog_bodies_skipped{reason="download_error"}` is incremented, `body_skipped="true"` / `body_skip_reason="download_error"` are set on structured metadata, and the metadata JSON line is returned as the log line.
5. `src/sf2loki/sources/apexlog_source.py:274` yields that entry with the checkpoint from step 2. Once the pipeline commits it, the log is durably marked processed: the next listing uses `StartTime > since OR (StartTime = since AND Id > since_id)` (`src/sf2loki/salesforce/apexlog_client.py:113-118`) and the `seen` filter at `src/sf2loki/sources/apexlog_source.py:206-207` drops it anyway. The body is unrecoverable.

There is no retry at any layer:

- `ApexLogClient.download_body` (`src/sf2loki/salesforce/apexlog_client.py:169-212`) retries exactly once, and only on HTTP 401 (`:190-193`). A transport failure raises on the first attempt (`:183-188`); any other non-2xx raises on the first attempt (`:195-208`).
- The shared Salesforce HTTP client is `httpx.AsyncClient(timeout=_HTTP_TIMEOUT)` (`src/sf2loki/app.py:919`) with `read=30.0` (`src/sf2loki/app.py:149`) and the default transport, i.e. `retries=0`. A 30s read timeout on one `GET /tooling/sobjects/ApexLog/<id>/Body` is terminal.

The EventLogFile source encodes the opposite, correct rule for the same class of failure: `src/sf2loki/sources/eventlogfile_source.py:601-606` stops the file loop for the cycle **without** advancing the watermark so the file is re-listed next cycle, and only abandons (advancing past it) once the file is older than `eventlogfile.download_max_age` (`src/sf2loki/config.py:721`). ApexLog has no equivalent.

No test pins the current behaviour: `tests/sources/test_apexlog_source.py` covers only the size-based skip (`:103-112`), throttle backoff (`:159`), tied-page drain, stall escalation and `checkpoint_only`; `tests/salesforce/test_apexlog_client.py:131` asserts only that a download error raises. `docs/sources/apexlog.md:38-46` and `docs/config-reference.md:141` document only the `max_body_bytes` size skip — the download-error fallback is undocumented and was never an accepted design decision (issue #33 body and comments do not mention it).

## Why it matters

A poll cycle can list up to `_PAGE_LIMIT` (200) new rows and drain multiple pages, downloading bodies serially, one REST call per log. At that call volume a single read timeout or one Salesforce 5xx is routine. When it happens:

- that log ships as a metadata-only line with `body_skipped="true"`, so it looks superficially present in Loki while the actual debug-log text is gone;
- the checkpoint advances past it, so no later cycle and no restart can recover it;
- the body was still downloadable for hours (ApexLog rows live ~24h under TraceFlag retention), making a next-cycle retry both cheap and safe.

The failure is silent apart from a WARNING and a counter, and the lost content is exactly what the developer-facing source is for (body search, `REQUEST_ID` correlation with EventLogFile/RTEM rows).

## Proposed approach

Two complementary changes; both are needed — the retry alone still loses the log on a sustained blip, and the deferral alone must not be able to wedge the source.

**1. Bounded in-call retry in `ApexLogClient.download_body` (`src/sf2loki/salesforce/apexlog_client.py:169-212`).**

- Classify: retry on `httpx.HTTPError` (transport/timeout) and on HTTP 429 / 5xx. Do not retry 4xx other than 429 (a 404 for a purged body is permanent).
- Up to 3 total attempts with jittered backoff (e.g. 0.5s, 1.5s), keeping the existing single 401 token re-mint orthogonal to the attempt counter.
- `ApexLogThrottledError` (403 `REQUEST_LIMIT_EXCEEDED`, `:199-204`) must still raise on the first occurrence with no retry — the API budget is already exhausted.
- Keep incrementing `apexlog_download_errors{reason=...}` per failed attempt so retries stay visible.

**2. Age-capped deferral in the source, mirroring the EventLogFile rule.**

- Add `apexlog.download_max_age: Duration` to `ApexLogConfig` (`src/sf2loki/config.py`), default `2h` — comfortably inside the ~24h ApexLog retention.
- In `_poll`, catch `ApexLogError` from `_build_entry` alongside the existing `ApexLogThrottledError` handler at `src/sf2loki/sources/apexlog_source.py:267-273`. When `now - StartTime <= download_max_age`: log a WARNING and `return` from the generator **without** yielding the entry. Because `_poll` reloads the durable checkpoint from the store on every cycle (`src/sf2loki/sources/apexlog_source.py:158-163`), the committed position is still the one carried by the last successfully yielded entry — which excludes this log's id — so the next cycle re-lists and retries it. Rows after it in the page form a suffix and are re-listed too.
- The deferral must `return`, never `break`: falling through to the `checkpoint_only` block at `src/sf2loki/sources/apexlog_source.py:282-296` would durably commit the advanced `(watermark, window)` — which already includes the failed log's id from `:248-251` — and reintroduce the loss. Alternatively roll back `watermark`/`window`/`since_id` to the pre-row values before breaking; the `return` is simpler and matches `eventlogfile_source.py:606`.
- When `now - StartTime > download_max_age`: keep today's behaviour (metadata-only line, `body_skip_reason="download_error"`, checkpoint advances) so a permanently un-downloadable log cannot wedge the source, and log at WARNING naming the abandon.
- `_resolve_line` no longer swallows the error itself; the age decision needs `m.start_time` and belongs with the cursor logic in `_poll`. Its docstring (`src/sf2loki/sources/apexlog_source.py:331-336`) must be updated.

**3. Artifacts and docs.** `apexlog.download_max_age` is a config change, so `just gen-config` must regenerate `config.example.yaml` and `docs/config-reference.md` (drift gate: `tests/test_config_artifacts_drift.py`). Document the retry, the deferral and the cap in `docs/sources/apexlog.md`.

---

Imported from GitHub issue #93 on 2026-08-14, when this repo migrated from GitHub Issues to Backlog.md. The original issue has been deleted; its verbatim body, labels and comments are preserved in `archive/issues-dump.json` (`jq '.[] | select(.number == 93)' archive/issues-dump.json`).

<sub>Filed from the 2026-07-30 full-repo audit (11 finder lanes + adversarial verification per finding).</sub>
<!-- SECTION:DESCRIPTION:END -->

## Acceptance Criteria
<!-- AC:BEGIN -->
- [ ] #1 `ApexLogClient.download_body` retries a transient failure: respx sequence `500, 500, 200` returns the body; test asserts three GETs were made.
- [ ] #2 `download_body` retries a transport error (`httpx.ReadTimeout` then 200) and returns the body.
- [ ] #3 `download_body` does **not** retry a permanent 404 — one GET, `ApexLogError` raised.
- [ ] #4 `download_body` raises `ApexLogThrottledError` on the first 403 `REQUEST_LIMIT_EXCEEDED` with no retry and no extra GET.
- [ ] #5 The existing single-401-re-mint test (`tests/salesforce/test_apexlog_client.py:121`) still passes unchanged.
- [ ] #6 Source test: three new logs whose `StartTime` is inside `download_max_age`, body download raising `ApexLogError` for the second — only log 1 is emitted, no metadata-only entry for log 2, no `checkpoint_only` entry, and the last yielded checkpoint's `ids`/`last_ts` exclude log 2 and log 3.
- [ ] #7 Source test: after that deferred cycle, a second cycle with a working fake client re-lists log 2 and emits its real body (drives the same `CheckpointStore`, proving retry-next-cycle recovery).
- [ ] #8 Source test: a log whose `StartTime` is older than `download_max_age` with a failing body download ships the metadata-only line with `body_skip_reason="download_error"`, increments `apexlog_bodies_skipped{reason="download_error"}`, and advances the checkpoint past it (no wedge).
- [ ] #9 Source test: the deferral path does not emit a `checkpoint_only` entry (guards against re-committing the advanced cursor via `apexlog_source.py:282-296`).
- [ ] #10 `apexlog.download_max_age` present in `ApexLogConfig` with a documented default; `just gen-config` run and `tests/test_config_artifacts_drift.py` green.
- [ ] #11 `docs/sources/apexlog.md` documents bounded retry, next-cycle deferral, and the `download_max_age` abandon cap.
- [ ] #12 `just gate` green (ruff + `mypy --strict` + pytest).
<!-- AC:END -->

## Definition of Done
<!-- DOD:BEGIN -->
- [ ] #1 just gate is green (ruff check + ruff format --check + mypy src + pytest) — run it, don't assert it
- [ ] #2 just gen-config run and its output committed, if config.py changed (CI drift gate fails otherwise)
- [ ] #3 committed straight to main with a conventional-commit message, and pushed
<!-- DOD:END -->
