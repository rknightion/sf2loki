---
id: SFL-0037
title: >-
  cli: backfill --dry-run — preview file count, source bytes and API calls
  before pushing a historical window
status: To Do
assignee: []
created_date: '2026-08-14 16:58'
labels:
  - followup
  - phase-1
  - roadmap
milestone: m-4
dependencies: []
references:
  - 'https://github.com/rknightion/sf2loki/issues/121'
ordinal: 37000
---

## Description

<!-- SECTION:DESCRIPTION:BEGIN -->
## What

`sf2loki backfill` has no way to preview the size of a window before it starts pushing.

The subparser at `src/sf2loki/cli.py:99-142` defines `--since`, `--until`, `--event-types`, `--interval`, `--ingest-timestamps`, `--concurrency`, `--org` — no preview/estimate flag. The dispatch block at `src/sf2loki/cli.py:202-240` calls `run_backfill` unconditionally.

`run_backfill` (`src/sf2loki/backfill.py:707`) goes straight to execution:

- builds a live `LokiSink` (`src/sf2loki/backfill.py:740`) and a `FileCheckpointStore` on the `-backfill` sibling state file (`src/sf2loki/backfill.py:743-744`);
- resolves event types (`src/sf2loki/backfill.py:759` → `_resolve_event_types`, `src/sf2loki/backfill.py:225`);
- per type, pages listings and immediately downloads + pushes (`_process_event_type`, `src/sf2loki/backfill.py:548` → `_process_files`, `:483` → `_download_file`, `:387`, each call exactly one metered ELF blob GET → `_process_file`, `:400`, which pushes and then commits the checkpoint at `:455`).

The only volume report is `_print_summary` (`src/sf2loki/backfill.py:697`), printed *after* the run.

The volume signal needed for a preview is already fetched for free. The listing SOQL selects `LogFileLength` (`src/sf2loki/salesforce/eventlogfile_client.py:169`) and parses it into `EventLogFileMeta.length` (`src/sf2loki/salesforce/eventlogfile_client.py:190`, field declared at `:90`). Nothing in the backfill path reads `.length` today. So per-type file counts and total source bytes for a window are computable from listing calls alone — no blob downloads, no pushes.

Two related facts verified while scoping this, both of which the preview output and docs should state:

1. **Backfill is not bounded by the egress budget.** `EgressGovernor` is constructed only in `src/sf2loki/app.py:1031-1032` for the daemon pipeline; `src/sf2loki/backfill.py` never imports it and pushes through `LokiSink` directly (`src/sf2loki/backfill.py:740`, `_push_with_retry` at `:345`). A `sink.loki.egress` daily byte budget does not cap a backfill run. `docs/sources/cost-controls.md` does not mention backfill at all.
2. **Backfill does not apply per-type `sample`.** Sampling is applied only in the live sources (`src/sf2loki/sources/eventlogfile_source.py:431,646,694`, `eventlog_objects_source.py:497`, `apexlog_source.py:256`, `pubsub_source.py:704-705`). `backfill.py` applies transforms/row filters (`src/sf2loki/backfill.py:770-775`, `_shape_file_rows` at `:291`) but never `EventLogFileTypeConfig.sample`. A preview therefore must not print a "post-sampling rows" estimate — there is no sampling in this path to model.

## Why it matters

An operator runs `sf2loki backfill --since 2026-01-01` for a high-volume type such as `ApiTotalUsage` against a Grafana Cloud stack shared with production streaming. There is no supported way to answer "how much will this push?" first. The costs are discovered only mid-run:

- billed Loki ingest for the pushed bytes;
- per-tenant ingest-rate pressure on the same stack the live daemon writes to, since no egress budget bounds the run (fact 1 above);
- one metered Salesforce ELF blob GET per file (`src/sf2loki/backfill.py:391`), against the org's daily API allowance;
- in the default label mode, a `backfill="true"` stream per type (`src/sf2loki/backfill.py:305`), doubling stream count for the window.

Aborting mid-run is safe (the checkpoint is resumable) but the bytes already pushed are already billed and already resident in Loki. The information needed to size the window correctly costs only listing SOQL calls and is already on the wire.

## Proposed approach

Add `--dry-run` to the backfill subparser in `src/sf2loki/cli.py:99-142`, plumbed through to `run_backfill` as a keyword argument.

Behaviour of `run_backfill(..., dry_run=True)`:

- Build `TokenProvider` and `EventLogFileClient` as normal (listing needs auth). Do **not** construct `LokiSink` (`src/sf2loki/backfill.py:740`) — a preview must never open a sink or push.
- Open the backfill `FileCheckpointStore` **read-only in effect**: load existing cursors so the preview reports *remaining* work on a resumed window, but never call `store.commit`. Simplest implementation: factor the listing loop out of `_process_event_type` (`src/sf2loki/backfill.py:548`) so both modes share it, and skip the `_process_files` call in dry-run mode. The existing boundary guards must be preserved verbatim in the shared loop: the `until` early break (`:620-625`), the `done_ids` boundary filter (`:637-644`), and the "more files than `page_size` share one CreatedDate" bail-out (`:652-668`).
- Accumulate per event type: `files`, `sum(EventLogFileMeta.length)`, and `api_calls_if_run` (one blob GET per file, matching `src/sf2loki/backfill.py:391`), plus the earliest/latest `CreatedDate` actually seen.
- Print a per-type table plus a total, then return 0 without downloading or pushing. Label the byte figure as *source CSV bytes* (`LogFileLength`) and state plainly that pushed volume differs: rows are re-encoded as JSON lines with labels and structured metadata (`_shape_file_rows`, `src/sf2loki/backfill.py:291`; `route_fields`/`promote_labels`), and row filters/transforms can remove rows. Do not print a fabricated post-sampling estimate (see fact 2).
- Add `--json` to emit the same numbers machine-readably, mirroring `doctor --json` (`src/sf2loki/cli.py:83-89`) so CI or a wrapper script can gate on a threshold.
- Keep the existing `_warn_retention` call (`src/sf2loki/backfill.py:205`, invoked at `:736`) in dry-run mode — the retention and Loki out-of-order-window warnings are exactly what a preview should surface.

Docs:

- `docs/reference/cli.md:71-88`: add `--dry-run` and `--json` to the flag table, with a worked example showing the preview output and the recommendation to run it before any window longer than a few days.
- `docs/sources/cost-controls.md`: state that a backfill run is **not** counted against `sink.loki.egress` budgets (the governor is daemon-only, `src/sf2loki/app.py:1031-1032`), and that `backfill --dry-run` is the supported way to size a window; cross-link from `docs/reference/cli.md`.

---

Imported from GitHub issue #121 on 2026-08-14, when this repo migrated from GitHub Issues to Backlog.md. The original issue has been deleted; its verbatim body, labels and comments are preserved in `archive/issues-dump.json` (`jq '.[] | select(.number == 121)' archive/issues-dump.json`).

<sub>Filed from the 2026-07-30 full-repo audit (11 finder lanes + adversarial verification per finding).</sub>
<!-- SECTION:DESCRIPTION:END -->

## Acceptance Criteria
<!-- AC:BEGIN -->
- [ ] #1 `sf2loki backfill --dry-run --since ... [--until ...] [--event-types ...] [--interval ...] [--org ...]` prints per-event-type `files`, source bytes (`sum(LogFileLength)`), and the blob-GET count a real run would spend, plus a total line, then exits 0.
- [ ] #2 `--dry-run` performs zero blob downloads and zero Loki pushes, and constructs no `LokiSink`.
- [ ] #3 `--dry-run` never writes the backfill state file: no `store.commit` call, and the state file's contents (or absence) are byte-identical before and after.
- [ ] #4 `--dry-run` respects an existing checkpoint: a window already partly backfilled reports only the files that a resumed run would still process (`done_ids` boundary filtering preserved).
- [ ] #5 `--dry-run --json` emits the same figures as parseable JSON.
- [ ] #6 Preview output states that source CSV bytes are a proxy for pushed bytes, and that a backfill run is not bounded by `sink.loki.egress`.
- [ ] #7 Retention / Loki out-of-order warnings (`_warn_retention`, `src/sf2loki/backfill.py:205`) still fire in dry-run mode.
- [ ] #8 `tests/test_backfill.py`: a fake `EventLogFileClient` whose `list_files` returns multiple pages with known `length` values — assert the printed/JSON totals equal the expected file count and byte sum, and assert `download` was never called.
- [ ] #9 `tests/test_backfill.py`: dry-run against a pre-populated backfill state file — assert the reported file count excludes the already-done ids at the watermark boundary, and assert the state file is unchanged after the call.
- [ ] #10 `tests/test_backfill.py`: dry-run with an `until` cutoff — assert files at/after the cutoff are excluded from the totals, and that the `page_size`-boundary bail-out path still terminates (no infinite listing loop) in dry-run mode.
- [ ] #11 `tests/test_cli.py`: `--dry-run` and `--json` parse and reach `run_backfill` with the expected keyword arguments; the flags are absent-by-default so existing invocations are unchanged.
- [ ] #12 `docs/reference/cli.md` flag table and `docs/sources/cost-controls.md` updated as described; `just gate` green.
<!-- AC:END -->

## Definition of Done
<!-- DOD:BEGIN -->
- [ ] #1 just gate is green (ruff check + ruff format --check + mypy src + pytest) — run it, don't assert it
- [ ] #2 just gen-config run and its output committed, if config.py changed (CI drift gate fails otherwise)
- [ ] #3 committed straight to main with a conventional-commit message, and pushed
<!-- DOD:END -->
