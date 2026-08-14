---
id: SFL-0028
title: >-
  tests: the ApexLog compound (StartTime, Id) cursor SOQL from the #39 fix is
  asserted nowhere
status: To Do
assignee: []
created_date: '2026-08-14 16:58'
labels:
  - followup
  - phase-2
milestone: m-3
dependencies: []
references:
  - 'https://github.com/rknightion/sf2loki/issues/112'
ordinal: 28000
---

## Description

<!-- SECTION:DESCRIPTION:BEGIN -->
## What

`ApexLogClient.list_logs` builds two different cursor predicates depending on whether a `since_id` tiebreak is supplied (`src/sf2loki/salesforce/apexlog_client.py:112-126`):

```python
ts_literal = to_soql_datetime_literal(since)          # :112
if since_id:
    where = [                                          # :114
        f"(StartTime > {ts_literal} OR (StartTime = {ts_literal} AND Id > '{since_id}'))"  # :115
    ]
else:
    where = [f"StartTime >= {ts_literal}"]             # :118
...
    f"ORDER BY StartTime ASC, Id ASC LIMIT {page_size}"  # :125
```

Lines 113-116 plus the `, Id ASC` on line 125 are the entire production fix for #39, introduced by commit `4538201` ("fix(apexlog): compound (StartTime, Id) cursor + checkpoint-only tokens", `Closes #39`), which changed `ORDER BY StartTime ASC` to `ORDER BY StartTime ASC, Id ASC` and added the `since_id` branch.

No test constructs that query.

- `tests/salesforce/test_apexlog_client.py` has three `list_logs` tests (lines 62-102). All three call `list_logs(since=..., users=..., page_size=200)` — lines 70, 89, 102 — and never pass `since_id`, so the default `""` always selects the plain `StartTime >= {lit}` branch at `apexlog_client.py:118`. The `since_id` branch is never executed.
- The only two assertions on the emitted SOQL string in the whole suite are `tests/salesforce/test_apexlog_client.py:76-78` (`"FROM ApexLog" in sent_q`, `"LogUser.Username" not in sent_q`) and `tests/salesforce/test_apexlog_client.py:91-92` (the `LogUser.Username IN (...)` filter). Neither touches the cursor predicate, the `>` vs `>=` choice, the `'{since_id}'` literal quoting, or the `ORDER BY` tiebreak.
- The tiebreak semantics are exercised only against a hand-written fake. `tests/sources/test_apexlog_source.py:59-79` defines `TieBoundaryClient`, whose docstring states it "applies the same cursor predicate the real SOQL WHERE clause uses (see `ApexLogClient.list_logs`), so this proves the SOURCE threads `since_id` correctly", and whose `list_logs` reimplements the predicate in Python at line 76: `filtered = [m for m in self._all if (m.start_time, m.id) > (since, since_id)]`. The fake asserts the source's threading (`since_id_calls` at lines 205-206), not the string that reaches Salesforce.
- Commit `4538201` added 171 lines to `tests/sources/test_apexlog_source.py` and 0 lines to `tests/salesforce/test_apexlog_client.py`.
- `tests/test_app.py:150-169` only asserts `ApexLogSource` wiring/absence, not query construction.

The sibling fix for the same bug class already meets the bar this one misses. #38's cursor for eventlog_objects is built at `src/sf2loki/sources/eventlog_objects_source.py:374-384` and is pinned at the wire level by `tests/sources/test_eventlog_objects_source.py:1242` (`test_asc_more_than_page_limit_ties_drains_completely`), which mocks the HTTP layer with a query-aware `side_effect` that parses the outbound SOQL (`re.search(r"Id > '(\w+)'", q)`, line 1261) and only serves the next 200-row slice when the real tiebreak is present — plus a direct string assertion `assert "ORDER BY EventDate DESC" in captured[0]` at line 1004. The ApexLog path has no equivalent.

Secondary inconsistency in the same expression: `apexlog_client.py:115` interpolates `since_id` into the SOQL raw, while the equivalent eventlog_objects cursor routes it through `_escape_soql_string` (`src/sf2loki/sources/eventlog_objects_source.py:147`, applied at `:384`). Currently benign — `since_id` comes from `window[-1]` (`src/sf2loki/sources/apexlog_source.py:177`), i.e. an Id previously returned by Salesforce — but the asymmetry is undefended by any test.

## Why it matters

The ApexLog watermark cannot advance past a `StartTime` that a full page does not exceed. Without the Id tiebreak, >200 ApexLog rows sharing one `StartTime` (parallel Apex execution produces exactly this) return the identical page on every poll forever: newer logs are silently dropped and the same page is re-fetched indefinitely. That is #39, and it was severe enough to warrant a dedicated fix.

Every way of regressing that fix currently leaves the suite fully green:

- `Id >= '{since_id}'` instead of `Id >` — the boundary row is re-returned every cycle; with a full tied page the cursor never advances.
- Dropping the `OR (StartTime = ... AND Id > ...)` arm, leaving bare `StartTime > {lit}` — the whole tied bucket beyond the first page is skipped permanently.
- Reverting to `StartTime >= {lit}` in the `since_id` branch — the original #39 stall returns verbatim.
- Losing `, Id ASC` from line 125 — `(StartTime, Id)` stops being a total order, so an advancing `since_id` no longer guarantees forward progress within a tied `StartTime`; the drain can loop or skip depending on the server's arbitrary intra-tie ordering.
- Breaking the `'{since_id}'` quoting — the query is malformed and every poll fails, surfacing as `ApexLogError` rather than as a cursor bug.

In all five cases `TieBoundaryClient` keeps passing, because it asserts its own Python reimplementation of the predicate rather than the SOQL the client emits. The source-level test proves the source threads `since_id`; nothing proves the client turns `since_id` into a query Salesforce will honour.

## Proposed approach

Add client-level tests to `tests/salesforce/test_apexlog_client.py`, reusing the existing `respx` harness and the `route.calls[0].request.url.params["q"]` capture already used at lines 76 and 91.

1. `test_list_logs_with_since_id_builds_compound_cursor` — call `await c.list_logs(since="2026-07-02T00:00:00Z", users=[], page_size=200, since_id="07L000000000001")` against a mocked `TOOLING_QUERY` returning `{"records": [], "done": True}`, then assert the captured `q` contains:
   - `StartTime > <expected literal>` where the literal is `to_soql_datetime_literal("2026-07-02T00:00:00Z")` (compute it in the test rather than hardcoding, so the literal format stays owned by `soql_client`),
   - `OR (StartTime = <expected literal> AND Id > '07L000000000001')`,
   - `ORDER BY StartTime ASC, Id ASC`,
   - and that `StartTime >=` does **not** appear.
2. `test_list_logs_without_since_id_uses_inclusive_first_poll_predicate` — same call with the `since_id` argument omitted; assert `StartTime >= <literal>` is present and neither `StartTime >` (as the sole predicate) nor `Id >` appears. This pins the first-ever-poll branch at `apexlog_client.py:118`, which is what makes the initial poll inclusive of the boundary instant.
3. `test_list_logs_since_id_combines_with_username_filter` — pass both `since_id` and `users=["a@x.com"]`; assert the compound cursor is parenthesised as one conjunct and joined with ` AND LogUser.Username IN ('a@x.com')`. This is the assertion that catches a lost outer paren, which would silently change operator precedence so the `OR` arm escapes the user filter.
4. Add a drain-level regression test mirroring `tests/sources/test_eventlog_objects_source.py:1242`: drive `ApexLogSource` through a real `ApexLogClient` over a `respx` mock whose `side_effect` parses the outbound `q`, extracts `Id > '(\w+)'`, and serves the next slice only when the tiebreak is present (returning the same first page when it is absent, reproducing the #39 stall). Seed >`_PAGE_LIMIT` (200, `src/sf2loki/sources/apexlog_source.py:58`) rows sharing one `StartTime` and assert every row is emitted exactly once. This is the test that fails on any of the five regressions above, end to end, rather than on string shape alone.
5. Route `since_id` through the same escaping helper the eventlog_objects cursor uses, so the two cursor builders are consistent. `_escape_soql_string` currently lives at `src/sf2loki/sources/eventlog_objects_source.py:147`; a `salesforce`-layer client importing from `sources` is the wrong direction, so move it to `src/sf2loki/salesforce/soql_client.py` next to `to_soql_datetime_literal`, re-export or update the eventlog_objects call site at `:384`, and apply it at `apexlog_client.py:115`. Cover it with a test passing a `since_id` containing a single quote and asserting the emitted SOQL is escaped, not broken.

Keep `TieBoundaryClient` as-is — it covers the source's threading logic, which is a different seam.

---

Imported from GitHub issue #112 on 2026-08-14, when this repo migrated from GitHub Issues to Backlog.md. The original issue has been deleted; its verbatim body, labels and comments are preserved in `archive/issues-dump.json` (`jq '.[] | select(.number == 112)' archive/issues-dump.json`).

<sub>Filed from the 2026-07-30 full-repo audit (11 finder lanes + adversarial verification per finding).</sub>
<!-- SECTION:DESCRIPTION:END -->

## Acceptance Criteria
<!-- AC:BEGIN -->
- [ ] #1 `test_list_logs_with_since_id_builds_compound_cursor` in `tests/salesforce/test_apexlog_client.py` asserts the captured SOQL contains `StartTime > <literal>`, `OR (StartTime = <literal> AND Id > '<since_id>')`, and `ORDER BY StartTime ASC, Id ASC`, and does not contain `StartTime >=`.
- [ ] #2 `test_list_logs_without_since_id_uses_inclusive_first_poll_predicate` asserts the no-`since_id` branch emits `StartTime >= <literal>` with no `Id >` term.
- [ ] #3 `test_list_logs_since_id_combines_with_username_filter` asserts the compound cursor stays parenthesised as a single conjunct when joined with the `LogUser.Username IN (...)` filter.
- [ ] #4 A drain-level test drives `ApexLogSource` through a real `ApexLogClient` against a query-aware `respx` mock (modelled on `tests/sources/test_eventlog_objects_source.py:1242`), seeds >200 rows sharing one `StartTime`, and asserts all rows are emitted exactly once; the mock returns the identical first page when the outbound query carries no `Id >` tiebreak, so the test fails if the tiebreak is removed.
- [ ] #5 Mutation check recorded in the closing comment: flipping `apexlog_client.py:115` to `Id >=`, deleting the `OR` arm, and deleting `, Id ASC` from `apexlog_client.py:125` each fail at least one new test. Revert all three afterwards.
- [ ] #6 `_escape_soql_string` lives in `src/sf2loki/salesforce/soql_client.py`, is applied to `since_id` at `apexlog_client.py:115`, the eventlog_objects call site at `src/sf2loki/sources/eventlog_objects_source.py:384` is updated to import it from its new home, and a test asserts a `since_id` containing a single quote is escaped in the emitted SOQL.
- [ ] #7 `just gate` green (`ruff check`, `ruff format --check`, `mypy src`, `pytest`).
- [ ] #8 Line coverage of `src/sf2loki/salesforce/apexlog_client.py:113-116` confirmed by a coverage run, not asserted.
<!-- AC:END -->

## Definition of Done
<!-- DOD:BEGIN -->
- [ ] #1 just gate is green (ruff check + ruff format --check + mypy src + pytest) — run it, don't assert it
- [ ] #2 just gen-config run and its output committed, if config.py changed (CI drift gate fails otherwise)
- [ ] #3 committed straight to main with a conventional-commit message, and pushed
<!-- DOD:END -->
