---
id: SFL-0050
title: >-
  cli: backfill --event-types skips the SOQL-identifier guard config enforces,
  and "*" silently backfills nothing
status: To Do
assignee: []
created_date: '2026-08-14 16:58'
labels:
  - followup
  - phase-5
milestone: m-0
dependencies: []
references:
  - 'https://github.com/rknightion/sf2loki/issues/134'
ordinal: 50000
---

## Description

<!-- SECTION:DESCRIPTION:BEGIN -->
## What

`sf2loki backfill --event-types` accepts arbitrary strings and interpolates them into the EventLogFile listing SOQL without the identifier validation the config model enforces for the same values.

Path, in order:

- `src/sf2loki/cli.py:114-118` declares `--event-types` as a free-form string (no `choices`, no `type=`).
- `src/sf2loki/cli.py:224-228` splits on commas and strips whitespace. That is the only processing applied.
- `run_backfill` (`src/sf2loki/backfill.py:707-759`) does not validate the argument. `_resolve_event_types` short-circuits at `src/sf2loki/backfill.py:232-233` (`if event_types: return list(event_types)`), bypassing both the configured-type list and the discovery fallback at `src/sf2loki/backfill.py:239-240`.
- `_process_event_type` passes the raw string to `client.list_files` (`src/sf2loki/backfill.py:607`), which interpolates it unquoted: `src/sf2loki/salesforce/eventlogfile_client.py:171` builds `f"WHERE EventType='{event_type}' AND Interval='{interval}' "`. No escaping helper covers this operand (`to_soql_datetime_literal` normalises only the `CreatedDate` literal).

The config-sourced path guards exactly this input. `src/sf2loki/config.py:404-408` defines `_SOQL_IDENTIFIER_PATTERN = r"^[A-Za-z0-9_]+$"` with the rationale that object/field/EventType names are interpolated into SOQL and "a stray quote would otherwise surface as a runtime MALFORMED_QUERY crash mid-poll". `src/sf2loki/config.py:606-614` applies it to `EventLogFileTypeConfig.name`, allowing only `EVENT_TYPE_WILDCARD` (`src/sf2loki/config.py:560`) as an exception. The CLI flag reaches the identical interpolation site with none of that.

Two concrete consequences:

1. **Non-identifier input produces a failed listing and exit code 0.** A quote or backslash yields malformed SOQL. Salesforce returns MALFORMED_QUERY, `SoqlError` is wrapped by `_wrap_soql_error` (`src/sf2loki/salesforce/eventlogfile_client.py:192-194`, incrementing `eventlogfile_download_errors{reason="listing"}`), and the handler at `src/sf2loki/backfill.py:608-612` logs one line and **returns True** — the non-fatal "give up on this type" path. Because it returns True, the `if not ok: exit_code = 1` branch at `src/sf2loki/backfill.py:780-782` never fires and the run exits 0. That non-fatal path exists to absorb transient listing failures; an argument that can never succeed should be rejected before the run starts, not funnelled through it.

2. **`--event-types '*'` silently backfills nothing.** `EventType='*'` is syntactically legal SOQL that matches no records, so `list_files` returns an empty page, `src/sf2loki/backfill.py:615-616` breaks out, and the run exits 0 with no error line at all. The same `*` in config means "discover the types this org produces" (`src/sf2loki/backfill.py:234-240`, pinned by `tests/test_backfill.py:814-834`). The flag is also the only way an operator with explicit configured types can ask for discovery, since the discovery fallback fires only when there are no explicit configured types — and `*` is the obvious thing to try. `docs/reference/cli.md:80` and `README.md:484-487` document neither behaviour.

## Why it matters

`sf2loki backfill --event-types "Login,O'Brien"` (paste artifact, smart quote, stray backslash) backfills `Login`, prints one stderr line for the second type, and exits 0. In a wrapper script or CI job that checks only the exit status, the run is indistinguishable from a complete one, and the operator has to notice a single stderr line to learn a requested event type was never listed.

`sf2loki backfill --since 2026-06-01 --event-types '*'` exits 0 having emitted nothing, with no error line — only `files=0 rows_pushed=0` in the summary (`src/sf2loki/backfill.py:693-698`) distinguishes it from a genuinely empty window. Backfill windows are frequently expected to be sparse, so a zero-file summary is not a reliable signal.

Both are cheap to close, and the codebase already treats this validation as necessary — the config validator's own comment states the reason. Scope is operator ergonomics and exit-code correctness, not security: the operator already controls the config and credentials, so no privilege boundary is crossed by the malformed query.

## Proposed approach

Validate at parse time in `cli.py`, before any network work, so the failure surfaces as a config-shaped error rather than a mid-run listing failure.

1. Export the existing regex rather than duplicating it: add `_SOQL_IDENTIFIER_RE` (or a small `is_soql_identifier(value: str) -> bool` helper) to `src/sf2loki/config.py`'s public surface alongside `EVENT_TYPE_WILDCARD`, and import it in `cli.py`. Do not introduce a second copy of the pattern — the two must not be able to drift.
2. In the `backfill` branch of `cli.py` (immediately after the split at `src/sf2loki/cli.py:224-228`), reject any entry that is neither a bare identifier nor `EVENT_TYPE_WILDCARD`, printing to stderr in the established style (`print(f"sf2loki: {msg}", file=sys.stderr)`) and returning `_CONFIG_ERROR_EXIT_CODE` (`src/sf2loki/cli.py:43`, currently 2). Message should name the offending value and the rule, mirroring the config validator's wording at `src/sf2loki/config.py:611-614`.
3. Handle `*` explicitly. Preferred: map it to the discovery path — if the parsed list is exactly `["*"]`, pass `event_types=None` through to `run_backfill` *and* force discovery, which means `_resolve_event_types` needs a way to request discovery irrespective of configured types (e.g. a `discover: bool = False` parameter checked before the `configured` branch at `src/sf2loki/backfill.py:234-238`). Rejecting `*` with a clear message that points at the discovery behaviour is an acceptable smaller fix; silently querying `EventType='*'` is not.
4. Mixing `*` with named types is ambiguous — reject that combination explicitly rather than picking a winner.
5. Update `docs/reference/cli.md:80` and the backfill example prose in `README.md:484-487` to state the accepted form (bare `[A-Za-z0-9_]+` names) and whatever `*` ends up doing.

Do not change the non-fatal listing-failure path at `src/sf2loki/backfill.py:608-612`; it is deliberate for transient failures, and front-loading the validation removes the unreachable-argument case from it.

---

Imported from GitHub issue #134 on 2026-08-14, when this repo migrated from GitHub Issues to Backlog.md. The original issue has been deleted; its verbatim body, labels and comments are preserved in `archive/issues-dump.json` (`jq '.[] | select(.number == 134)' archive/issues-dump.json`).

<sub>Filed from the 2026-07-30 full-repo audit (11 finder lanes + adversarial verification per finding).</sub>
<!-- SECTION:DESCRIPTION:END -->

## Acceptance Criteria
<!-- AC:BEGIN -->
- [ ] #1 `_SOQL_IDENTIFIER_RE` (or an equivalent helper) is importable from `src/sf2loki/config.py` and used by `cli.py`; the pattern string exists exactly once in the codebase.
- [ ] #2 `sf2loki backfill --since ... --event-types "Login,O'Brien"` exits with `_CONFIG_ERROR_EXIT_CODE` and prints a message naming `O'Brien` and the `[A-Za-z0-9_]+` rule, without issuing any Salesforce request.
- [ ] #3 `--event-types '*'` either triggers EventType discovery (regardless of configured types) or is rejected with a message pointing at how to get discovery. It never issues `WHERE EventType='*'`.
- [ ] #4 `--event-types 'Login,*'` is rejected as ambiguous.
- [ ] #5 Valid input is unchanged: `--event-types Login,API` still resolves to `["Login", "API"]` and reaches `list_files`.
- [ ] #6 `tests/test_cli.py` gains cases asserting the exit code and stderr text for a quote-bearing value, a backslash-bearing value, and the `*` handling, all without network mocking (validation must happen before `run_backfill` is entered — assert `run_backfill` is not called).
- [ ] #7 `tests/test_backfill.py:790-811 test_explicit_event_types_override_config` and `tests/test_backfill.py:814-834 test_wildcard_only_config_triggers_discovery` still pass unmodified.
- [ ] #8 If discovery-via-`*` is implemented: a `tests/test_backfill.py` case with explicit configured types plus the discovery request asserts `list_event_types` is called and the configured types are ignored.
- [ ] #9 `docs/reference/cli.md` and `README.md` describe the accepted `--event-types` values and the `*` behaviour.
- [ ] #10 `just gate` green.
<!-- AC:END -->

## Definition of Done
<!-- DOD:BEGIN -->
- [ ] #1 just gate is green (ruff check + ruff format --check + mypy src + pytest) — run it, don't assert it
- [ ] #2 just gen-config run and its output committed, if config.py changed (CI drift gate fails otherwise)
- [ ] #3 committed straight to main with a conventional-commit message, and pushed
<!-- DOD:END -->
