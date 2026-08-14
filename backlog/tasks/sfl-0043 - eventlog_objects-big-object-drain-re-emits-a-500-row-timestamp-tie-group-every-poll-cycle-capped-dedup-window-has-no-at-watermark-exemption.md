---
id: SFL-0043
title: >-
  eventlog_objects: big-object drain re-emits a >500-row timestamp tie group
  every poll cycle (capped dedup window has no at-watermark exemption)
status: To Do
assignee: []
created_date: '2026-08-14 16:58'
labels:
  - followup
  - phase-2
milestone: m-0
dependencies: []
references:
  - 'https://github.com/rknightion/sf2loki/issues/127'
ordinal: 43000
---

## Description

<!-- SECTION:DESCRIPTION:BEGIN -->
## What

The `big_object: true` DESC drain has no server-side secondary cursor, so the checkpoint id window is its ONLY cross-cycle dedup — and that window is capped unconditionally at 500 ids. A timestamp tie group larger than the cap is therefore partially evicted every cycle and its overflow is re-emitted indefinitely.

Mechanics:

- `_drain_big_object` always fetches `{timestamp_field} >= {watermark}` (`src/sf2loki/sources/eventlog_objects_source.py:630`) with `ORDER BY {timestamp_field} DESC` (`:641`). Big Objects reject a compound `ORDER BY ts, Id`, so the ASC path's Id tiebreak (`:378-387`, `ts > wm OR (ts = wm AND Id > last_id)`) is unavailable here.
- The intra-cycle tie escape (`:683-689`, `Id NOT IN (...)` scoped to the tied timestamp) exists only inside one drain: `tie_ids` is a local, discarded on return.
- Consequently the only cross-cycle filter is the checkpoint window: `seen = set(window); records = [r for r in records if _id_of(r) not in seen]` (`:352-353`).
- That window is truncated to the last `_MAX_CARRIED_IDS` = 500 ids in `_emit_record` (`:493`, constant at `:95`). The comment at `:93-95` justifies 500 only against `_PAGE_LIMIT` (200) — "so a full page of boundary ties can never evict its own ids mid-drain" — but the multi-page tie escape collects 200×k ids in a single cycle by design, so that reasoning does not cover this path.

When N records share one timestamp `T` and N > 500, cycle 1 emits all N and commits a window holding only the last 500 ids. Cycle 2's `>= T` re-fetch returns all N again, N-500 survive the `seen` filter, get re-emitted, and their ids evict an equal number of older boundary ids from the window. The window rotates through the tie group forever; the watermark cannot advance (every row is at `T`), so the loop persists until a strictly newer record arrives.

The sibling EventLogFile source already solved exactly this for its own `CreatedDate >= watermark` re-list: `src/sf2loki/sources/eventlogfile_source.py:91-96` documents it ("Pairs AT the watermark are always all kept ... capping them would re-download uncovered files forever when >cap files share one CreatedDate"), implemented in `_append_carried_id` at `:180-193`, with a `_CARRIED_IDS_WARN_THRESHOLD` = 5000 anomaly warning at `:98-100`. `eventlog_objects` has no equivalent exemption.

Unaffected paths, for scope: the ASC path is safe because its Id tiebreak excludes already-emitted boundary rows server-side regardless of window size; `apexlog_source.py:59` has the same 500 cap but also the tiebreak (`:177`), so it is safe too.

Reproduced against the real source (`FileCheckpointStore`, respx SOQL fake honouring `Id NOT IN` and the upper bounds, 800 rows all stamped `2026-06-30T10:00:00.000+0000`, committing the last entry's checkpoint per cycle as the pipeline does):

```
cycle 1: soql_calls=6 emitted=800 window=500 last_ts=2026-06-30T10:00:00.000+0000
cycle 2: soql_calls=6 emitted=300 window=500 last_ts=2026-06-30T10:00:00.000+0000
cycle 3: soql_calls=6 emitted=300 window=500 last_ts=2026-06-30T10:00:00.000+0000
... unchanged through cycle 7
```

## Why it matters

A quiet custom big object (or a `*EventStore` object with a second-granularity or bulk-loaded timestamp field — the trigger class the source's own docstring names at `:35-37`) that receives a >500-row load stamped with one timestamp and then goes idle re-pushes the overflow slice on every poll interval, indefinitely:

- Duplicate log lines to Loki. Loki silently drops exact duplicates only while the entries are inside its accept/dedup window; a tie group older than that window produces either rejected pushes or genuine duplicates in the index.
- Continuous consumption of the egress byte budget / rate caps and the `entries` metrics by re-sent data.
- Wasted Salesforce API quota: the full multi-page tie-escape sweep (6 SOQL calls for 800 rows in the repro) runs every cycle with nothing new to show for it.
- Entirely silent. `new_ids` is non-empty on every cycle (the rows are new to the drain's local `collected`), so `_record_watermark_stall` never fires, `sf2loki_watermark_stalls_total` stays 0, and the only external signal is a static `sf2loki_watermark_ts` plus duplicate rows.

No data is lost, and the condition self-clears once a strictly newer record advances the watermark — hence low severity, not a stall (#38 covered the stall; its body explicitly left the window out of scope: "`_MAX_CARRIED_IDS` governs eviction, not progress").

## Proposed approach

Mirror the ELF rule — never cap ids that sit AT the current watermark, since those are exactly what the `>=` re-fetch returns. Implement it with an additive checkpoint field rather than changing the element type of `ids`, so every existing reader and stored checkpoint keeps working:

1. Extend the checkpoint JSON to `{"last_ts": ..., "ids": [...], "boundary_ids": [...]}`. `boundary_ids` holds every id whose timestamp equals `last_ts` (uncapped); `ids` keeps its existing meaning — a tail of at most `_MAX_CARRIED_IDS` ids at strictly OLDER timestamps. `_parse_checkpoint` (`:119-136`) returns `[]` for a missing `boundary_ids`, so today's checkpoints and the legacy bare-timestamp form load unchanged (their `ids` tail already covers the boundary they were written at).
2. In `_emit_record` (`:464-523`), replace the unconditional trim at `:493` with a watermark-transition fold:
   - when the record's valid timestamp differs from the current watermark, the old boundary set has become historical: `ids = [*ids, *boundary_ids][-_MAX_CARRIED_IDS:]`, `boundary_ids = []`, then advance the watermark;
   - append `record_id` to `boundary_ids` (records with a null/unparseable timestamp keep the previous watermark and so belong to the current boundary set — the safe side, since a `>=` re-fetch can return them).
   Records arrive ASC on both paths, so the watermark only ever moves forward and the fold happens at most once per distinct timestamp.
3. Dedup against the union: `seen = set(window) | set(boundary)` at `:352` and at `:433` (the ASC path — harmless there, and it keeps the two paths identical).
4. ASC-path cursor: `last_id` must remain the id of the newest-emitted record — `boundary_ids[-1] if boundary_ids else (ids[-1] if ids else "")` (`:378`). A legacy/current checkpoint's `ids[-1]` is by construction its boundary id, so behaviour is unchanged on the first cycle after upgrade.
5. Mirror ELF's anomaly warning: log once at WARNING when `boundary_ids` crosses a threshold (reuse 5000, per `eventlogfile_source.py:98-100`) — thousands of rows at one timestamp means the checkpoint document is growing abnormally.
6. Thread `boundary_ids` through `_checkpoint_value` (`:525-538`, extend the cache key tuple), `_checkpoint_only_entry` (`:540-556`), and the `(watermark, window)`-changed comparisons at `:363` and `:461`.
7. Update the module docstring's checkpoint description (`:9-19`, `:48-54`) and `docs/sources/eventlog-objects.md:38`.
8. Guard against the secondary risk this exposes: the intra-cycle tie escape interpolates the whole tie group into `Id NOT IN (...)` (`:634`), so an 800-id list with real 18-character Salesforce ids approaches the SOQL/GET URI length limit. Either bound the escape's id list and rely on the (now complete) boundary window across cycles, or assert the query length and fail loudly instead of emitting a malformed query.

---

Imported from GitHub issue #127 on 2026-08-14, when this repo migrated from GitHub Issues to Backlog.md. The original issue has been deleted; its verbatim body, labels and comments are preserved in `archive/issues-dump.json` (`jq '.[] | select(.number == 127)' archive/issues-dump.json`).

<sub>Filed from the 2026-07-30 full-repo audit (11 finder lanes + adversarial verification per finding).</sub>
<!-- SECTION:DESCRIPTION:END -->

## Acceptance Criteria
<!-- AC:BEGIN -->
- [ ] #1 `_MAX_CARRIED_IDS` no longer applies to ids at the current watermark on the big-object path; only strictly-older ids are capped.
- [ ] #2 Checkpoint format is additive (`boundary_ids`), and a stored checkpoint in the current `{"last_ts", "ids"}` form or the legacy bare-timestamp form loads without error and without re-emitting its recorded ids.
- [ ] #3 Test: `big_object` with N = 800 records all sharing one timestamp, driven over 3+ poll cycles with the previous cycle's checkpoint committed — cycle 1 emits 800, every later cycle emits 0 entries (a `checkpoint_only` token is acceptable) and issues no duplicate LogEntry. This is the regression that fails on current `main` (300 re-emitted per cycle).
- [ ] #4 Test: after the tie group, a strictly newer record arrives — the watermark advances to it, the 800 boundary ids are folded into the capped `ids` tail (so the checkpoint stops growing), and only the new record is emitted.
- [ ] #5 Test: mixed timestamps on the big-object path — ids at older timestamps are still capped to `_MAX_CARRIED_IDS`, proving the exemption is scoped to the boundary and the checkpoint does not grow without bound.
- [ ] #6 Test: a `boundary_ids` set past the warn threshold logs the anomaly WARNING exactly once.
- [ ] #7 Existing tie/dedup tests still pass unmodified in intent: `tests/sources/test_eventlog_objects_source.py:1290` (250 ties drain completely in one cycle), `:1113` (single boundary record deduped across cycles), and `test_asc_repeated_stall_at_same_boundary_escalates_to_error_and_counts` (200 bare-string ids committed in the legacy shape still dedup and still escalate).
- [ ] #8 `just gate` green.
<!-- AC:END -->

## Definition of Done
<!-- DOD:BEGIN -->
- [ ] #1 just gate is green (ruff check + ruff format --check + mypy src + pytest) — run it, don't assert it
- [ ] #2 just gen-config run and its output committed, if config.py changed (CI drift gate fails otherwise)
- [ ] #3 committed straight to main with a conventional-commit message, and pushed
<!-- DOD:END -->
