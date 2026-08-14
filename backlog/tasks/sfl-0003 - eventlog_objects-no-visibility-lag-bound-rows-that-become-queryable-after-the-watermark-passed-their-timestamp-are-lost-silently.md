---
id: SFL-0003
title: >-
  eventlog_objects: no visibility-lag bound - rows that become queryable after
  the watermark passed their timestamp are lost silently
status: To Do
assignee: []
created_date: '2026-08-14 16:58'
labels:
  - followup
  - phase-2
milestone: m-0
dependencies: []
references:
  - 'https://github.com/rknightion/sf2loki/issues/87'
ordinal: 3000
---

## Description

<!-- SECTION:DESCRIPTION:BEGIN -->
## What

`EventLogObjectsSource` advances its per-object watermark to the newest `timestamp_field` value it fetched in a cycle and never re-scans below it. There is no upper bound on the poll query, so the watermark can advance through a time region in which Salesforce has not finished making all rows queryable. Any row that becomes visible later, carrying a `timestamp_field` value below the committed watermark, can never match a subsequent query and is dropped permanently with no metric and no log.

Mechanism, in code:

- `_emit_record` sets `watermark = ts_field_val` for every record whose timestamp parses (`src/sf2loki/sources/eventlog_objects_source.py:488-490`). It runs in page order on the ASC path (`:451`) and in ascending-sorted order on the big-object path (`:358`, sorted at `:704-710`), so the cycle's committed watermark is the **maximum** `timestamp_field` value fetched. It is committed durably through the `CheckpointToken` built at `:515` (or the `checkpoint_only` token at `:548`).
- Next cycle's lower bound, ASC path (`:378-387`): `(<ts> > <wm> OR (<ts> = <wm> AND Id > '<last_id>'))` whenever the carried id window is non-empty, and bare `<ts> >= <wm>` only when it is empty. The window is non-empty after any record that had an `Id` (`:491-493`), so the strict `>` form is the steady state.
- Next cycle's lower bound, big-object DESC drain (`:630`): `<ts> >= <wm>`.
- **No upper bound exists.** The ASC query (`:388-393`) has none. In `_drain_big_object`, `upper` starts as `None` (`:619`) and is only ever lowered from the drain's own page contents (`:701`, applied at `:635-637`) — that is a within-cycle pagination ratchet, not a now-minus-lag settle gate.
- `lookback` (`:299-312`) applies only on first run or after a garbage stored watermark, so it is not a re-scan window either.

Millisecond precision is preserved into the SOQL literal (`src/sf2loki/salesforce/soql_client.py:35-53`), so no truncation accidentally widens the query.

Secondary gap on the ASC path: a late row at *exactly* the watermark instant also fails the tiebreak when its `Id` sorts below `window[-1]`, because the clause is `Id > last_id` and `last_id` is the highest Id already seen at that instant (`:378`, `:381-385`).

Neither loss is observable. `metrics.watermark_stalls` (`:558-574`) fires only when a full page returns exclusively already-seen ids — a different condition that a late-arrival gap never triggers. `soql_poll_errors` requires an actual SOQL failure.

`EventLogObjectConfig` (`src/sf2loki/config.py:411-462`) has no settle/lag field, and `docs/sources/eventlog-objects.md:19-42` documents the cursor as gap-free ("recovery is a gap-free re-fetch, never a gap in coverage") while stating the design assumption as "a datetime field that only ever increases" — value order, not visibility order.

## Why it matters

Row commit order and row timestamp order are independent. Two concurrent logins: R1 with `EventDate` 12:00:00.100 becomes queryable at 12:00:02; R2 with `EventDate` 12:00:01.500 becomes queryable at 12:00:01.600. A poll at 12:00:01.800 returns R2 only and commits watermark `2026-01-01T12:00:01.500Z`. The next poll issues `EventDate > 2026-01-01T12:00:01.500Z OR (EventDate = ... AND Id > '<R2 Id>')`. R1 sits below the bound forever. A security-relevant login record is absent from Loki, and nothing in the metrics or logs indicates it.

For standard objects on the ASC path (`LoginHistory`, `SetupAuditTrail`) the exposed window is the transaction commit-to-visibility skew — small per occurrence, but recurring on every busy org and unbounded in aggregate over the service's lifetime. For `big_object: true` (the stored RTEM family: `LoginEvent`, `ApiEvent`, `*EventStore`) Salesforce persists rows into the big object asynchronously after the event, with no documented cross-row ordering guarantee, so the exposed window is wide and losses are systematic rather than incidental.

This is the same hazard already accepted and fixed for the sibling source: `EventLogFileConfig.settle_window` (`src/sf2loki/config.py:711-720`) with a mode-conditional non-zero default (`src/sf2loki/config.py:750-759`) and the settle gate at `src/sf2loki/sources/eventlogfile_source.py:548-569`, deliberately compared against Salesforce-clock now (`src/sf2loki/sources/eventlogfile_source.py:505-508`). `eventlog_objects` shipped without the equivalent. The connector targets compliance and security telemetry, where silent permanent omission is worse than latency.

## Proposed approach

Add a per-object settle window that bounds every poll query from above, so the watermark can only advance through a region Salesforce has finished materialising.

1. **Config** — add to `EventLogObjectConfig` (`src/sf2loki/config.py:411-462`):

   ```python
   settle_window: Duration = Field(
       default=timedelta(0),
       description=(
           "Ignore rows whose timestamp_field is newer than now-settle_window, so the "
           "watermark never advances through a region where Salesforce is still making "
           "rows queryable (a row that lands late with an older timestamp would then be "
           "permanently below the cursor). Left unset it defaults to 5m for "
           "big_object: true (stored RTEM events are persisted asynchronously) and 0 "
           "for standard/custom objects. Costs up to settle_window of extra ingest lag."
       ),
   )
   ```

   Add a `model_validator(mode="after")` mirroring `_default_hourly_settle_window` (`src/sf2loki/config.py:750-759`): if `"settle_window" not in self.model_fields_set and self.big_object`, set it to `timedelta(minutes=5)`. Explicit `0` must remain honoured as "disabled".

2. **ASC path** (`src/sf2loki/sources/eventlog_objects_source.py:368-393`) — when `settle_window` is non-zero, append `AND {timestamp_field} < {to_soql_datetime_literal(bound)}` where `bound = (datetime.now(UTC) - settle_window)`, computed **once per cycle** (not per page) so the drain-until-short-page loop uses a stable bound and cannot spin. Keep the existing lower-bound/tiebreak clause unchanged.

3. **Big-object DESC path** (`src/sf2loki/sources/eventlog_objects_source.py:617-702`) — initialise `upper = bound` / `upper_exclusive = True` instead of `upper = None` when `settle_window` is non-zero, so the very first page is already capped. The tie-escape branch (`:631-634`, `:661-678`) needs no change: it only ever narrows to a `tie_ts` that was itself observed below the bound.

4. **Stall interaction** — verify `_record_watermark_stall` (`:558-574`) is not tripped by an empty settled window. A cycle where every visible row is newer than the bound returns zero rows, which takes the `not new_records` branch with `len(page) < _PAGE_LIMIT` (`:436-445`) and breaks without logging. Add a test that pins this (no spurious WARNING/ERROR, no `watermark_stalls` increment).

5. **Clock reference** — `timestamp_field` values are stamped by Salesforce's clock while the bound is computed from local `now()`, so a skewed host shifts the window. `SoqlClient` has no skew hook today (only `salesforce/eventlogfile_client.py:124-151` does). Either add the same `Date`-header skew accessor to `SoqlClient` and apply it as `eventlogfile_source` does (`:505-508`), or document the local-clock dependency and require NTP. Adding the hook is preferred for parity; if deferred, say so explicitly in the docs rather than leaving it implicit.

6. **Docs** — document the field in the `EventLogObjectConfig` table at `docs/sources/eventlog-objects.md:44-54`, add a short "late-arriving rows" subsection explaining the completeness-versus-latency tradeoff and why `big_object: true` defaults non-zero, and correct the "never a gap in coverage" wording at `docs/sources/eventlog-objects.md:38-42` to scope it to crash recovery.

7. Run `just gen-config` (config surface changed; the drift gate in `tests/test_config_artifacts_drift.py` fails otherwise) and `just gate`.

---

Imported from GitHub issue #87 on 2026-08-14, when this repo migrated from GitHub Issues to Backlog.md. The original issue has been deleted; its verbatim body, labels and comments are preserved in `archive/issues-dump.json` (`jq '.[] | select(.number == 87)' archive/issues-dump.json`).

<sub>Filed from the 2026-07-30 full-repo audit (11 finder lanes + adversarial verification per finding).</sub>
<!-- SECTION:DESCRIPTION:END -->

## Acceptance Criteria
<!-- AC:BEGIN -->
- [ ] #1 `EventLogObjectConfig.settle_window` exists with default `0`, and a validator defaults it to `5m` when unset and `big_object: true`.
- [ ] #2 An explicit `settle_window: 0` on a `big_object: true` entry stays `0` (validator only fires on unset).
- [ ] #3 ASC-path query text includes `AND <timestamp_field> < <bound>` when `settle_window` is non-zero, and is byte-identical to today's query when it is `0`.
- [ ] #4 The ASC bound is computed once per cycle and reused across every page of the drain-until-short-page loop.
- [ ] #5 Big-object DESC drain's first page carries `<timestamp_field> < <bound>` when `settle_window` is non-zero; the ratchet and the `Id NOT IN` tie escape still function below it.
- [ ] #6 Test: `test_asc_settle_window_excludes_unsettled_rows` — a fake SOQL client asserting the emitted query contains the upper bound and that a row newer than the bound is not emitted this cycle but IS emitted on a later cycle whose bound has moved past it.
- [ ] #7 Test: `test_asc_late_arriving_row_below_watermark_is_captured_with_settle_window` — cycle 1 exposes only the newer row, cycle 2 additionally exposes an older-timestamped row that landed late; with a settle window covering the visibility delay, both rows reach the sink. The same scenario with `settle_window: 0` loses the late row (pins the regression this issue describes).
- [ ] #8 Test: `test_big_object_settle_window_bounds_first_page` — the first DESC query carries the exclusive upper bound.
- [ ] #9 Test: `test_settle_window_zero_preserves_existing_query` — no behaviour change when disabled.
- [ ] #10 Test: a cycle in which every visible row is newer than the bound emits nothing, logs no stall WARNING/ERROR, and does not increment `watermark_stalls`.
- [ ] #11 `just gen-config` re-run: `config.example.yaml` and `docs/config-reference.md` include the new key and the drift gate passes.
- [ ] #12 `docs/sources/eventlog-objects.md` documents the key, the tradeoff, the `big_object` default, and the clock reference; the "never a gap in coverage" claim is scoped to crash recovery.
- [ ] #13 `just gate` green.
<!-- AC:END -->

## Definition of Done
<!-- DOD:BEGIN -->
- [ ] #1 just gate is green (ruff check + ruff format --check + mypy src + pytest) — run it, don't assert it
- [ ] #2 just gen-config run and its output committed, if config.py changed (CI drift gate fails otherwise)
- [ ] #3 committed straight to main with a conventional-commit message, and pushed
<!-- DOD:END -->
