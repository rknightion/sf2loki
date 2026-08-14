---
id: SFL-0066
title: >-
  salesforce: SOQL interpolation guards are inconsistent - apexlog Id cursor
  unescaped, backfill --event-types unvalidated
status: To Do
assignee: []
created_date: '2026-08-14 16:58'
labels:
  - followup
  - phase-1
milestone: m-2
dependencies: []
references:
  - 'https://github.com/rknightion/sf2loki/issues/150'
ordinal: 66000
---

## Description

<!-- SECTION:DESCRIPTION:BEGIN -->
## What

Two values reach a SOQL `WHERE` clause through raw f-string interpolation without the escaping or charset validation the codebase applies to the same value on its sibling path.

**1. The apexlog compound-cursor Id (`src/sf2loki/salesforce/apexlog_client.py:113-116`).**

```python
if since_id:
    where = [
        f"(StartTime > {ts_literal} OR (StartTime = {ts_literal} AND Id > '{since_id}'))"
    ]
```

`since_id` originates at `src/sf2loki/sources/apexlog_source.py:177` (`since_id = window[-1] if window else ""`). `window` is decoded from the persisted checkpoint by `_parse_checkpoint` (`src/sf2loki/sources/apexlog_source.py:80-90`), which coerces each element with `str(i)` and applies no charset check. Only the *timestamp* half of the compound cursor is validated: `src/sf2loki/sources/apexlog_source.py:165-172` runs `_is_valid_watermark` (defined at `:76`) and falls back to `now - lookback` with a WARNING when the stored value is unusable. The Id half has no equivalent guard.

The sibling source handles the identical construct correctly: `src/sf2loki/sources/eventlog_objects_source.py:147-149` defines `_escape_soql_string`, `:384` applies it (`Id > '{_escape_soql_string(last_id)}'`), and `:154` applies it to `Id IN` literal lists via `_soql_id_list`.

The checkpoint value is writable by a documented operator command: `run_state_set` (`src/sf2loki/statecmd.py:173-189`) commits the supplied string verbatim, with only a reserved-key guard at `:181`. `SoqlClient.query` (`src/sf2loki/salesforce/soql_client.py:86-97`) forwards the assembled string as the `q` parameter with no sanitization.

**2. The backfill `--event-types` CLI values (`src/sf2loki/salesforce/eventlogfile_client.py:171`).**

```python
f"WHERE EventType='{event_type}' AND Interval='{interval}' "
```

Path: `src/sf2loki/cli.py:115-118` declares `--event-types` with no constraint (contrast `--interval` at `src/sf2loki/cli.py:119-124`, which uses `choices=["Daily", "Hourly"]`); `src/sf2loki/cli.py:224-228` splits it on commas; `_resolve_event_types` returns the list verbatim (`src/sf2loki/backfill.py:232-233`); `src/sf2loki/backfill.py:607` passes each value to `client.list_files`.

The config path for the same value is guarded. `EventLogFileTypeConfig._validate_labels` (`src/sf2loki/config.py:605-614`) applies `_SOQL_IDENTIFIER_RE` (`src/sf2loki/config.py:407-408`, pattern `^[A-Za-z0-9_]+$`), and its own comment states the invariant: "EventType names are interpolated into the SOQL file-listing query; a non-identifier (stray quote, semicolon, ...) would crash at poll time with MALFORMED_QUERY". The CLI flag bypasses that validator entirely, so a documented invariant is enforced on one of two paths into the same interpolation site.

No test pins either behaviour. The only assertion over an interpolated Id cursor is `tests/sources/test_eventlog_objects_source.py:1261`, on the sibling source.

## Why it matters

A checkpoint id containing a single quote permanently wedges the apexlog source. Walk-through with `{"last_ts": "2026-07-30T10:00:00Z", "ids": ["07L000000000ABC'"]}` written via `sf2loki state set apexlog ...` (a plausible hand-repair during the poison-checkpoint recovery documented for #63, or a corrupted/externally-edited state object):

1. `src/sf2loki/sources/apexlog_source.py:158-163` loads and parses it; the watermark passes `_is_valid_watermark` at `:165`, so no fallback fires.
2. `:177` sets `since_id = "07L000000000ABC'"`.
3. `:183` calls `list_logs`, which emits `... AND Id > '07L000000000ABC'')` at `src/sf2loki/salesforce/apexlog_client.py:115`.
4. Salesforce returns 400 MALFORMED_QUERY; `SoqlError` becomes `ApexLogError` (`src/sf2loki/salesforce/apexlog_client.py:146-147`); `src/sf2loki/sources/apexlog_source.py:190-204` increments `soql_poll_errors`, logs, and returns.
5. Every subsequent cycle repeats identically. Nothing distinguishes this from a transient API failure in the metrics, and there is no self-heal - recovery requires `sf2loki state delete apexlog`, which re-lists the whole lookback window and re-pushes records already in Loki.

The validated-timestamp path already demonstrates the correct shape for this failure: warn and fall back to a safe cursor rather than emit a query that cannot succeed.

For backfill, an operator typo or a shell-mangled `--event-types` value surfaces as an opaque `MALFORMED_QUERY` from the Salesforce API mid-run rather than the clear config-time rejection the same value gets from `src/sf2loki/config.py:610`.

Neither path crosses a privilege boundary - an operator able to write checkpoint state or invoke `backfill` already holds the org credentials and can query Salesforce directly, so a crafted `WHERE` clause reads nothing new. This is a robustness and error-surface defect, not an injection vulnerability.

## Proposed approach

1. Hoist the escaping/validation helpers into a shared module alongside the existing `to_soql_datetime_literal` (`src/sf2loki/salesforce/soql_client.py:35-53`), which is already the home for "make a persisted value SOQL-legal":
   - `escape_soql_string(value: str) -> str` - move the body of `_escape_soql_string` (`src/sf2loki/sources/eventlog_objects_source.py:147-149`) verbatim.
   - `is_salesforce_id(value: str) -> bool` - `re.fullmatch(r"[A-Za-z0-9]{15,18}", value)`.
2. Update `src/sf2loki/sources/eventlog_objects_source.py` to import the shared `escape_soql_string` (keep `_soql_id_list` local; it is only used there) so there is one definition.
3. In `src/sf2loki/sources/apexlog_source.py`, guard the Id half of the cursor next to the existing watermark guard: after `:177`, if `since_id` is non-empty and fails `is_salesforce_id`, log a WARNING naming the rejected value and set `since_id = ""` (the first-poll `StartTime >= ts` predicate, which is safe - the id window still dedups at the boundary). Mirror the message shape used at `:167-171`.
4. In `src/sf2loki/salesforce/apexlog_client.py:115`, additionally route `since_id` through `escape_soql_string` as defence in depth, so any future caller cannot emit a malformed literal.
5. In `src/sf2loki/cli.py:224-228`, validate each parsed `--event-types` value against `_SOQL_IDENTIFIER_RE` before dispatch; on failure print the same message shape as `src/sf2loki/config.py:611-613` and return `_CONFIG_ERROR_EXIT_CODE` (2), matching the other pre-flight failures in that block. Export `_SOQL_IDENTIFIER_RE` from `src/sf2loki/config.py` under a public name rather than importing the underscore-prefixed symbol.
6. Consider rejecting an obviously-malformed value at the write surface too: `run_state_set` (`src/sf2loki/statecmd.py:173-189`) could warn (not block - it must remain able to write arbitrary recovery values) when the value parses as JSON with an `ids` list whose members are not Salesforce Ids.

---

Imported from GitHub issue #150 on 2026-08-14, when this repo migrated from GitHub Issues to Backlog.md. The original issue has been deleted; its verbatim body, labels and comments are preserved in `archive/issues-dump.json` (`jq '.[] | select(.number == 150)' archive/issues-dump.json`).

## Scope note

The operator-facing contract problems of the same flag — `--event-types '*'` silently backfilling nothing and a never-succeeding value exiting 0 — are tracked separately in #134; the shared fix point is the same `_SOQL_IDENTIFIER_RE` validation applied at the CLI boundary.

<sub>Filed from the 2026-07-30 full-repo audit (11 finder lanes + adversarial verification per finding).</sub>
<!-- SECTION:DESCRIPTION:END -->

## Acceptance Criteria
<!-- AC:BEGIN -->
- [ ] #1 `escape_soql_string` and `is_salesforce_id` live in one shared location under `src/sf2loki/salesforce/`; `src/sf2loki/sources/eventlog_objects_source.py` imports the shared escaper instead of defining its own.
- [ ] #2 `ApexLogClient.list_logs` escapes `since_id` before interpolation; a test asserts the generated SOQL for `since_id="x'y"` contains an escaped literal and no unbalanced quote.
- [ ] #3 `ApexLogSource._poll` drops a non-Salesforce-Id `since_id`, logs a WARNING, and issues the plain `StartTime >= ts` predicate; a test seeds the checkpoint with `{"last_ts": "<valid ts>", "ids": ["07L000000000ABC'"]}` and asserts the source completes a normal cycle (the fake client records `since_id == ""`) instead of raising or returning zero entries.
- [ ] #4 A regression test asserts the poisoned checkpoint does not wedge the source across two consecutive `_poll` cycles (previously: MALFORMED_QUERY every cycle forever).
- [ ] #5 A test asserts a valid 15- and 18-character Id is preserved as the tiebreak (the guard does not regress the #39 tied-page drain).
- [ ] #6 `sf2loki backfill --event-types "Login',(SELECT"` exits 2 with a message naming the invalid EventType, before any Salesforce call; a test covers the rejection and a second test covers a valid comma-separated list still reaching `_resolve_event_types` unchanged.
- [ ] #7 `just gate` green (`ruff check`, `ruff format --check`, `mypy src`, `pytest`).
<!-- AC:END -->

## Definition of Done
<!-- DOD:BEGIN -->
- [ ] #1 just gate is green (ruff check + ruff format --check + mypy src + pytest) — run it, don't assert it
- [ ] #2 just gen-config run and its output committed, if config.py changed (CI drift gate fails otherwise)
- [ ] #3 committed straight to main with a conventional-commit message, and pushed
<!-- DOD:END -->
