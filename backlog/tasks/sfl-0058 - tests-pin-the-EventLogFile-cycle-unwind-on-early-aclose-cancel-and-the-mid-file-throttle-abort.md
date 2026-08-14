---
id: SFL-0058
title: >-
  tests: pin the EventLogFile cycle unwind on early aclose/cancel and the
  mid-file throttle abort
status: To Do
assignee: []
created_date: '2026-08-14 16:58'
labels:
  - followup
  - phase-2
milestone: m-3
dependencies: []
references:
  - 'https://github.com/rknightion/sf2loki/issues/142'
ordinal: 58000
---

## Description

<!-- SECTION:DESCRIPTION:BEGIN -->
## What

Two branches in `src/sf2loki/sources/eventlogfile_source.py` are executed by zero tests. Verified with `coverage run --source=src/sf2loki/sources -m pytest tests/` on current `main`:

```
src/sf2loki/sources/eventlogfile_source.py  282 stmts  18 miss  94%
missing: 137-139, 149, 186, 215, 378-385, 395, 416, 438, 553, 655-658, 756
```

**1. Consumer-side unwind cleanup — `eventlogfile_source.py:378-385`**

`_process_cycle` runs the per-event-type workers in a single `run_workers()` task (`eventlogfile_source.py:359-370`) and drains their entries out of a `maxsize=1` bridge queue, re-yielding each one. The unwind guard is:

```python
except BaseException:
    runner.cancel()
    with contextlib.suppress(asyncio.CancelledError):
        await runner
    raise
```

It fires when the *consumer* side stops pulling: `GeneratorExit` delivered at the `yield item` (`eventlogfile_source.py:376`) by an early `aclose()`/asyncgen finalization, or `CancelledError` raised inside `await queue.get()` (`eventlogfile_source.py:373`). No test reaches it. Every ELF test drains the generator to exhaustion via `_run_cycle` (`tests/sources/test_eventlogfile_source.py:146-151`), and the stop-event tests (`tests/sources/test_eventlogfile_source.py:418`, `:434`, `:1577`) exit through the normal `_CycleDone` path at `eventlogfile_source.py:374-377`. A repo-wide grep for `aclose`/`GeneratorExit` in `tests/` finds no ELF case; the only unwind test of this shape is `tests/salesforce/test_pubsub_client.py:832-848`, which targets the Pub/Sub client.

**2. Mid-file throttle abort — `eventlogfile_source.py:653-658`**

```python
except EventLogFileError as exc:
    if isinstance(exc, EventLogFileThrottledError):
        self._record_cycle_failure(event_type, f"download of {file_meta.id} (mid-file)", exc)
        return
```

`test_throttled_download_aborts_rest_of_cycle` (`tests/sources/test_eventlogfile_source.py:1019`) covers the *first-row* throttle at `eventlogfile_source.py:580-582` only, because the fake client raises `EventLogFileThrottledError` before yielding any row (`tests/sources/test_eventlogfile_source.py:133-136`). The fake's `mid_stream_errors` hook raises a plain `EventLogFileError` (`tests/sources/test_eventlogfile_source.py:142-143`), so it drives the transient branch at `eventlogfile_source.py:659-686` (issue #41, test at `tests/sources/test_eventlogfile_source.py:689`). Line 654 is only ever evaluated False; 655-658 never run.

## Why it matters

**Unwind cleanup (378-385).** This is a live shutdown path, not dead code. `app.py:301-321` consumes `source.events()` with a bare `async for`, and `app.py:286` / `app.py:297` cancel the producer tasks when a lane consumer dies and on every shutdown. Drop `runner.cancel()` (or swallow the `raise`) and the per-type workers at `eventlogfile_source.py:346-357` survive the generator: parked forever on `await queue.put(entry)` against a consumer that no longer exists, or mid-`download` still fetching multi-MB ELF blobs. Under file-lease/k8s-lease HA that means a **demoted standby keeps downloading EventLogFile blobs and burning the org's API budget** after it has lost leadership. Nothing in the suite would notice the regression.

**Mid-file throttle (653-658).** The two mid-file branches differ in a data-loss-relevant way. The transient branch may `break` at `eventlogfile_source.py:679` when the file is older than `download_max_age`, which falls through to the tail assignment at `eventlogfile_source.py:709` and marks the file **processed** — deliberately dropping its unemitted rows so a permanently-broken file cannot wedge the EventType (issue #41). The throttled branch must instead `return`, leaving the file unprocessed (a throttle is transient, so dropping rows would be pure loss), and `_record_cycle_failure` sets `_cycle_throttled` (`eventlogfile_source.py:470-471`), which `_process_cycle` converts into `throttle_event` (`eventlogfile_source.py:356-357`) so types still queued behind the semaphore skip outright. Collapsing the branches — an easy simplification during a refactor — turns a throttle on an old file into silent row loss plus continued API spend against an exhausted budget.

Note for scope: with the shipped client this branch is currently **unreachable**. `EventLogFileThrottledError` is only raised from `_fetch_to_spool` (`src/sf2loki/salesforce/eventlogfile_client.py:299-304`), which completes before the first row is yielded (`eventlogfile_client.py:230-232`); the only post-first-row failure the client produces is `EventLogFileError` from `csv.Error` (`eventlogfile_client.py:263-267`). The source codes against `_EventLogFileClientLike` (`eventlogfile_source.py:249`), which permits it, so this is a guard test that pins intent against both a source-side refactor and a future client that surfaces a 403 mid-stream — not a live bug.

## Proposed approach

Both tests go in `tests/sources/test_eventlogfile_source.py` and reuse `FakeEventLogFileClient` / `make_elf_cfg` / `make_file_meta`.

**Test A — early `aclose()` cancels the workers.** Extend `FakeEventLogFileClient` with a `download_gate: asyncio.Event | None` and a `download_cancelled: list[str]`: when the gate is set for a file id, `download` awaits it inside `try/except asyncio.CancelledError`, appends the file id to `download_cancelled`, and re-raises. Configure two event types with `concurrency=2` and files whose first type yields one row immediately while the second type's download blocks on the gate. Then:

```python
agen = source.events(store, asyncio.Event())
first = await anext(agen)          # one entry drained from the bridge queue
await agen.aclose()                # GeneratorExit -> the 378-385 unwind
```

Assert: `download_cancelled == ["<gated id>"]` (the in-flight worker observed cancellation), `agen.aclose()` returned without raising, no `download_calls` were added after the `aclose()` (drive the loop with `await asyncio.sleep(0)` a couple of times first), and no pending task remains whose coroutine name is `run_workers` (`asyncio.all_tasks()` filter, or assert the recorded runner task `.done()` via a monkeypatched `asyncio.ensure_future` if a direct handle is preferred). Avoid real-time sleeps for synchronization (issue #70) — gate on `asyncio.Event`.

**Test B — mid-file throttle aborts the cycle without abandoning the file.** Add a `mid_stream_throttled: set[str]` field to `FakeEventLogFileClient` that yields row 0 then raises `EventLogFileThrottledError` at the same point `mid_stream_errors` raises today (`tests/sources/test_eventlogfile_source.py:142-143`). Mirror `test_mid_file_failure_past_max_age_is_abandoned_and_type_keeps_progressing` (`:689`) exactly — same `download_max_age=timedelta(hours=24)`, same ancient/recent file pair, two event types — but with `mid_stream_throttled={"f_ancient"}`. Assert:

- exactly one entry emitted (row `"a"`, carrying the **pre-file** checkpoint), and `f_recent` was **not** downloaded this cycle;
- the emitted checkpoint's `ids` do not contain `f_ancient` and `last_created` has not advanced past it — i.e. the throttled file was not abandoned, unlike the transient path at `:679`/`:709`;
- the second event type was never listed (`len(client.list_calls) == 1`) — the `_cycle_throttled` → `throttle_event` abort;
- a second cycle with `mid_stream_throttled` cleared re-downloads `f_ancient` and emits its rows, proving no data was lost.

---

Imported from GitHub issue #142 on 2026-08-14, when this repo migrated from GitHub Issues to Backlog.md. The original issue has been deleted; its verbatim body, labels and comments are preserved in `archive/issues-dump.json` (`jq '.[] | select(.number == 142)' archive/issues-dump.json`).

<sub>Filed from the 2026-07-30 full-repo audit (11 finder lanes + adversarial verification per finding).</sub>
<!-- SECTION:DESCRIPTION:END -->

## Acceptance Criteria
<!-- AC:BEGIN -->
- [ ] #1 `FakeEventLogFileClient` gains a `download_gate` + `download_cancelled` hook and a `mid_stream_throttled` set, documented in its docstring alongside the existing `errors` / `throttled` / `mid_stream_errors` hooks.
- [ ] #2 New test: an early `await agen.aclose()` mid-cycle cancels the in-flight per-type download worker (observed via a `finally`/`except CancelledError` flag in the fake), completes without raising, issues no further client calls, and leaves no live `run_workers` task — covering `eventlogfile_source.py:378-385`.
- [ ] #3 New test: a mid-file `EventLogFileThrottledError` on a file older than `download_max_age` aborts the cycle, does **not** mark the file processed (checkpoint `ids`/`last_created` unchanged for that file), skips the remaining event type's listing, and the file's rows are re-emitted on the next cycle — covering `eventlogfile_source.py:655-658`.
- [ ] #4 Both tests synchronize on `asyncio.Event`, not real-time sleeps (issue #70 convention).
- [ ] #5 `coverage run --source=src/sf2loki/sources -m pytest tests/` reports `378-385` and `655-658` no longer missing for `src/sf2loki/sources/eventlogfile_source.py`.
- [ ] #6 `just gate` green.
<!-- AC:END -->

## Definition of Done
<!-- DOD:BEGIN -->
- [ ] #1 just gate is green (ruff check + ruff format --check + mypy src + pytest) — run it, don't assert it
- [ ] #2 just gen-config run and its output committed, if config.py changed (CI drift gate fails otherwise)
- [ ] #3 committed straight to main with a conventional-commit message, and pushed
<!-- DOD:END -->
