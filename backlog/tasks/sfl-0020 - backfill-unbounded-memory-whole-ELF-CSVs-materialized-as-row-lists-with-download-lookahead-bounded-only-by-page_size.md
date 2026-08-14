---
id: SFL-0020
title: >-
  backfill: unbounded memory - whole ELF CSVs materialized as row lists, with
  download lookahead bounded only by page_size
status: To Do
assignee: []
created_date: '2026-08-14 16:58'
labels:
  - followup
  - phase-5
milestone: m-0
dependencies: []
references:
  - 'https://github.com/rknightion/sf2loki/issues/104'
ordinal: 20000
---

## Description

<!-- SECTION:DESCRIPTION:BEGIN -->
## What

`sf2loki backfill` buffers entire EventLogFile CSVs as parsed row lists, and prefetches downloads arbitrarily far ahead of pushes. Two compounding defects on the same path:

**1. Per-file materialization.** `_download_file` (`src/sf2loki/backfill.py:387-396`) drains the whole async iterator into a list:

```python
rows = [row async for row in client.download(file_meta)]
```

`EventLogFileClient.download` is an async generator built specifically to avoid this. Its docstring (`src/sf2loki/salesforce/eventlogfile_client.py:228-232`) states the body is streamed to a spooled temp file and parsed incrementally "so peak RAM is O(row) instead of O(file) — Salesforce documents ELF blobs exceeding 100MB", and the spool constant carries the same rationale (`src/sf2loki/salesforce/eventlogfile_client.py:34-37`: buffering those blobs in RAM, "let alone as decoded str + parsed rows, is not acceptable"). The backfill caller discards that property. `_process_file` then builds a second full-size structure from the same data — `entries` at `src/sf2loki/backfill.py:426-438` — before the first push at `src/sf2loki/backfill.py:446-452`, so peak per file is the row dicts plus one `LogEntry` per row. A ~70-column ELF CSV parsed into `dict[str, str]` rows costs several times the raw CSV bytes.

**2. Unbounded download lookahead.** `_process_files` (`src/sf2loki/backfill.py:504-545`) creates a task for every file in the listing page up front:

```python
tasks = [asyncio.ensure_future(_bounded(fm)) for fm in files]
```

and `_bounded` holds the semaphore only across the download (`src/sf2loki/backfill.py:509-510`), releasing it on return — before the strictly sequential push loop (`src/sf2loki/backfill.py:514-540`) has consumed the rows. So the semaphore bounds concurrent HTTP streams, not resident row lists: as soon as one download finishes, the next acquires the slot and runs while the pusher is still awaiting Loki. A repro with the identical structure (semaphore released on download return, all tasks pre-created, slow awaiting consumer) shows that even at `--concurrency 1` with 20 files, all 20 downloads complete before the first push finishes.

Worse, consumed results are never released: `asyncio.Future.result()` does not clear `_result`, and the `tasks` list holds every task reference for the whole lifetime of `_process_files`, so already-pushed files' row lists stay reachable until the function returns. The effective bound is the full listing page — `eventlogfile.page_size`, default 1000 (`src/sf2loki/config.py:710`).

**3. The documented memory contract is wrong.** `src/sf2loki/cli.py:132-136` describes `--concurrency` (default 2) as "Concurrent file downloads (each spools up to 8 MiB)" and `docs/reference/cli.md:83` repeats it. Actual peak is O(page_size x parsed rows per file), unbounded by any config value. `backfill.py` never reads `EventLogFileMeta.length` (`LogFileLength`), so there is no size-based gate either.

The daemon path already does this correctly: `src/sf2loki/sources/eventlogfile_source.py:571` streams row by row via `row_iter = aiter(self._client.download(file_meta))`, bridged to the consumer through `asyncio.Queue(maxsize=1)` (`src/sf2loki/sources/eventlogfile_source.py:342`) so a slow sink stops the downloader.

## Why it matters

Backfilling a historical window of a busy org's API/Login ELF history is the command's core use case (issue #23). Such a window lists many large daily CSVs; while the first file's chunks grind through Loki retries (`_push_with_retry`, `src/sf2loki/backfill.py:346-384`, up to 10 consecutive retryable failures with backoff to 30 s), the downloader keeps completing further files, each held fully parsed, and nothing is freed as pushes complete. RSS grows to multiple GB and the process is OOM-killed. The run is resumable (the per-file checkpoint commits at `src/sf2loki/backfill.py:454-455`), so no data is lost, but the same window fails repeatedly and cannot be completed — and the operator has no config knob to bound it, because the documented knob does not control what it claims to.

None of the 22 tests in `tests/test_backfill.py` exercise memory or download/push interleaving, so nothing pins the correct behaviour.

## Proposed approach

Restore the streaming contract and bound the prefetch.

**Stream per file.** Replace the `list[dict[str, str]]` hand-off with a live iterator. Have `_process_file` consume rows incrementally, accumulating only until the batch limits are reached (`cfg.sink.loki.batch.max_entries` / `max_bytes`, the same thresholds `_chunk_entries` uses at `src/sf2loki/backfill.py:325-343`), push that chunk, then continue. Resident rows per file drop to one chunk. Preserve two existing behaviours:

- **Checkpoint semantics unchanged.** `cursor.advance` + `store.commit` still run only after the file's last chunk is pushed, and a file yielding zero entries must still not advance the checkpoint (`src/sf2loki/backfill.py:439-443`).
- **Label-mode sort.** `_shape_file_rows` sorts a file's entries by timestamp in non-`--ingest-timestamps` mode (`src/sf2loki/backfill.py:320-321`) to guarantee per-stream ordering for that file's pushes. Chunked streaming can only sort within a chunk. Either sort per chunk and accept that Loki accepts out-of-order within its window (v2.4+ per-stream out-of-order is allowed), or make the per-file sort conditional on a bounded row count with a documented fallback. Pick one and record the choice in the module docstring at `src/sf2loki/backfill.py:1-31`, which currently documents ordering guarantees.

**Bound the lookahead.** Stop pre-creating a task per file. Use a sliding window of at most `concurrency` outstanding downloads created lazily (e.g. a `deque` of tasks, topped up after each consumption) and drop each task reference once consumed so its result becomes collectable. Because `download` fetches the entire body to the spool before yielding the first row (`src/sf2loki/salesforce/eventlogfile_client.py:241-252`, spool caps RAM at `_SPOOL_MAX_MEMORY_BYTES` = 8 MiB then goes to disk), a prefetch task can force the body fetch with a single `anext()` and hand the live iterator to the sequential pusher. Peak RAM then really is `concurrency x 8 MiB` of spool plus one chunk, matching the documented claim; the remainder of each prefetched blob sits on disk.

**Clean up on abort.** The current `finally` cancels unfinished tasks (`src/sf2loki/backfill.py:542-545`). With live iterators, also `aclose()` every un-consumed iterator so the spooled temp files are released on the `_ABORT_RUN` and `_STOP_TYPE` paths.

**Fix the docs.** Update `src/sf2loki/cli.py:132-136` and `docs/reference/cli.md:83` to state the real bound once it is real.

---

Imported from GitHub issue #104 on 2026-08-14, when this repo migrated from GitHub Issues to Backlog.md. The original issue has been deleted; its verbatim body, labels and comments are preserved in `archive/issues-dump.json` (`jq '.[] | select(.number == 104)' archive/issues-dump.json`).

<sub>Filed from the 2026-07-30 full-repo audit (11 finder lanes + adversarial verification per finding).</sub>
<!-- SECTION:DESCRIPTION:END -->

## Acceptance Criteria
<!-- AC:BEGIN -->
- [ ] #1 `_download_file`'s full-list materialization at `src/sf2loki/backfill.py:393` is gone; the backfill path consumes `EventLogFileClient.download` incrementally.
- [ ] #2 Downloads no longer run ahead without bound: a test with a fake client and a push that awaits records the download/push interleaving and asserts at most `concurrency` (+1 in flight) files are downloaded before the first file's push completes, for `concurrency=1` and `concurrency=2`.
- [ ] #3 Per-file resident rows are bounded: a test with a fake client yielding N rows and `sink.loki.batch.max_entries=k` asserts the largest structure handed to a push is <= k entries, and that pushes begin before the fake client has yielded its last row (proving streaming, not buffer-then-chunk).
- [ ] #4 Consumed downloads are released: a test asserts no reference to a pushed file's rows survives in the prefetch bookkeeping after that file completes (e.g. via `weakref` on the yielded row objects, or by asserting the task container length stays bounded).
- [ ] #5 Un-consumed row iterators are closed on abort: a test drives `_MAX_CONSECUTIVE_PUSH_FAILURES` retryable push failures (mirroring `test_exit_code_1_after_exhausting_consecutive_push_failures`, `tests/test_backfill.py:749`) and asserts every outstanding iterator had `aclose()` called / its spool closed.
- [ ] #6 Existing guards still pass unchanged: `test_files_pushed_oldest_file_first` (`tests/test_backfill.py:281`), `test_rows_pushed_oldest_to_newest_within_a_file` (`tests/test_backfill.py:252`), `test_pages_through_multiple_listing_pages` (`tests/test_backfill.py:537`), and the resume/checkpoint tests at `tests/test_backfill.py:433` and `:487`.
- [ ] #7 A file that yields zero pushable entries (empty CSV or every row filtered) still does not advance the checkpoint.
- [ ] #8 `--concurrency` help text (`src/sf2loki/cli.py:132-136`) and `docs/reference/cli.md:83` state the actual memory bound; the `src/sf2loki/backfill.py` module docstring records the streaming design and the label-mode ordering decision.
- [ ] #9 `just gate` green (ruff + `mypy --strict` + pytest).
<!-- AC:END -->

## Definition of Done
<!-- DOD:BEGIN -->
- [ ] #1 just gate is green (ruff check + ruff format --check + mypy src + pytest) — run it, don't assert it
- [ ] #2 just gen-config run and its output committed, if config.py changed (CI drift gate fails otherwise)
- [ ] #3 committed straight to main with a conventional-commit message, and pushed
<!-- DOD:END -->
