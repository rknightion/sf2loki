---
id: SFL-0011
title: >-
  eventlog_objects: the big-object tie escape grows Id NOT IN without bound - a
  >~500-row tie group halts the object permanently
status: To Do
assignee: []
created_date: '2026-08-14 16:58'
labels:
  - followup
  - phase-2
milestone: m-0
dependencies: []
references:
  - 'https://github.com/rknightion/sf2loki/issues/95'
ordinal: 11000
---

## Description

<!-- SECTION:DESCRIPTION:BEGIN -->
## What

The big-object DESC drain's tie escape (issue #38, `src/sf2loki/sources/eventlog_objects_source.py:576-702`) accumulates an unbounded `Id NOT IN (...)` exclusion list, and the resulting SOQL is sent as a GET query parameter. Past roughly 500-600 tied ids the request exceeds the Salesforce request-URI ceiling, every drain attempt for that object fails, and the object never ingests again.

The mechanism:

- A full DESC page whose rows all share one timestamp cannot lower the ratchet bound, so the drain switches to the escape and seeds `tie_ids` from that whole page (`eventlog_objects_source.py:683-689`).
- Each subsequent escape query appends `Id NOT IN (<tie_ids>)` to the WHERE clause (`:631-634`) and, while pages keep coming back full, extends `tie_ids` by every newly-seen id and loops (`:677-678`). The loop only exits on a short page (`:661-666`) or when a full page adds nothing new (`:667-676`).
- Nothing caps `tie_ids`. `max_catchup_records` bounds only the retained `collected` window (`:654-656`); `_MAX_CARRIED_IDS = 500` (`:95`) bounds only the checkpoint id window. A tie group of N rows drives `tie_ids` to N.
- `_soql_id_list` (`:152-154`) renders `'<18-char id>', ` — about 22 raw characters per id. `SoqlClient.query` passes the entire statement as a GET query parameter (`src/sf2loki/salesforce/soql_client.py:96-97`, `:102`), and httpx percent-encodes `'` to `%27` and `,` to `%2C`, so each id costs roughly 28 encoded characters. The ~16 KB Salesforce request-URI limit therefore lands at about 550-600 ids; the 100,000-character SOQL statement limit lands near 4,300.

Once the ceiling is crossed the request returns non-2xx, `SoqlClient.query` raises `SoqlError` (`soql_client.py:122`), and `_process_object` catches it at `:335-350`: it increments `soql_poll_errors`, logs (escalating to ERROR after `_ERROR_LOG_THRESHOLD` consecutive cycles), and returns. Every page already collected in that cycle is discarded and no checkpoint is committed, so the next cycle rebuilds the identical over-long clause from the same watermark and fails identically. `watermark_stalls` never increments, because the query errors before the stall check at `:667-676` is reached.

Contrast with the other two cursor paths, which are bounded by construction: the ASC path uses a compound `(ts > wm OR (ts = wm AND Id > last_id))` cursor (`:380-392`), and `apexlog_source` uses the same single-id tiebreak (`src/sf2loki/sources/apexlog_source.py:174`). Only the DESC escape grows its clause per page.

Existing coverage stops well short of the ceiling: `tests/sources/test_eventlog_objects_source.py:1288-1329` drains a 250-row tie group, so `tie_ids` peaks at 200 in the second query and the growth is never exercised.

## Why it matters

The escape's own docstring (`eventlog_objects_source.py:590-597`) promises to page the tied boundary "until a short page confirms it is exhausted" — an unbounded promise the implementation cannot keep. Issue #38 and the module docstring (`:35-46`) both accept ">200 records share one timestamp (bulk loads, second-granularity fields)" as a realistic premise; the same premise at 3x scale defeats the escape that was built for it.

Concrete failure: a bulk load writes 1,000 rows into a big object polled with a second-granularity `timestamp_field`, all stamped `2026-07-30T00:00:00.000Z`, straddling the watermark. The drain enters the escape and pages 200 -> 400 -> 600 ids; somewhere past ~550 the listing GET exceeds the URI ceiling. From that cycle on the object emits nothing: `soql_poll_errors` climbs and the log is loud, but the halt is permanent — no cycle can ever produce a shorter query, because the clause is rebuilt from scratch to the same length every time. Only the affected object stops (per-object isolation holds at `:243-252`); other objects and other sources keep running, which makes the halt easy to miss without a per-object alert.

## Proposed approach

1. Add a character budget for the exclusion clause, expressed in characters rather than a raw id count so it stays correct if id length or escaping changes — for example a module constant `_MAX_TIE_CLAUSE_CHARS` around 8,000 (comfortably inside the ~16 KB encoded URI ceiling after the rest of the query and percent-encoding). Compute the rendered length before appending to `tie_ids` at `:677`.
2. When the budget is reached and the page is still full, stop extending: route to `_record_watermark_stall` with the `tie_ts` boundary and a message naming the clause budget as the cause, then `break` — the same terminal path the escape already uses at `:667-676`. That turns an unrecoverable error loop into the project's existing loud-stall signal (`watermark_stalls` metric plus ERROR on repeat).
3. Document the consequence explicitly in the docstring at `:590-597`: taking the stall `break` returns `collected` and so advances the watermark to the newest row in the window, which permanently skips the un-drained tail of the tie group. That loss already exists on the `:667-676` path; the new path must not silently widen it without the metric. Consider a dedicated counter for rows skipped at a tie boundary if the stall metric proves too coarse to alert on.
4. Optionally raise the ceiling as a follow-up rather than in this change: the raw SOQL statement limit (100,000 characters) is far above the URI limit, so moving the listing query off a GET query parameter would push the wall out roughly 8x. Verify against the dev org before relying on it — do not change the transport in the same change as the cap.
5. Keep the cap independent of `_MAX_CARRIED_IDS` (`:95`). They solve different problems: one bounds the query, the other bounds the persisted dedup window. Note in a comment that a tie group larger than `_MAX_CARRIED_IDS` cannot fully fit in the checkpoint window, so the next cycle re-fetches part of the group and re-emits it (an accepted at-least-once duplicate, deduped downstream by identical timestamp plus line).

---

Imported from GitHub issue #95 on 2026-08-14, when this repo migrated from GitHub Issues to Backlog.md. The original issue has been deleted; its verbatim body, labels and comments are preserved in `archive/issues-dump.json` (`jq '.[] | select(.number == 95)' archive/issues-dump.json`).

<sub>Filed from the 2026-07-30 full-repo audit (11 finder lanes + adversarial verification per finding).</sub>
<!-- SECTION:DESCRIPTION:END -->

## Acceptance Criteria
<!-- AC:BEGIN -->
- [ ] #1 A module constant bounds the rendered `Id NOT IN (...)` clause length; `tie_ids` cannot grow past it in `_drain_big_object` (`src/sf2loki/sources/eventlog_objects_source.py:677`).
- [ ] #2 Hitting the budget on a still-full page calls `_record_watermark_stall` at the `tie_ts` boundary and breaks the drain instead of issuing an over-long query.
- [ ] #3 The docstring at `eventlog_objects_source.py:590-597` states the clause budget, the stall outcome, and that the un-drained tail of an over-large tie group is skipped with the watermark advance.
- [ ] #4 Test: a tie group of ~1,200 rows at one timestamp drains through the escape and every request URL captured by the mock transport stays under the budget — asserted on the actual request URI length, not on `tie_ids` length, so percent-encoding is covered.
- [ ] #5 Test: the same over-large tie group produces no `SoqlError`, emits the rows collected before the cap, and increments `sf2loki_watermark_stalls_total` for `{source="eventlog_objects", object=...}` on the second consecutive cycle at the same boundary (mirroring `tests/sources/test_eventlog_objects_source.py:1332-1385`).
- [ ] #6 Test: the existing 250-row case (`tests/sources/test_eventlog_objects_source.py:1288-1329`) still drains completely in 3 queries with no stall recorded — the cap must not fire below its budget.
- [ ] #7 `just gate` green.
<!-- AC:END -->

## Definition of Done
<!-- DOD:BEGIN -->
- [ ] #1 just gate is green (ruff check + ruff format --check + mypy src + pytest) — run it, don't assert it
- [ ] #2 just gen-config run and its output committed, if config.py changed (CI drift gate fails otherwise)
- [ ] #3 committed straight to main with a conventional-commit message, and pushed
<!-- DOD:END -->
