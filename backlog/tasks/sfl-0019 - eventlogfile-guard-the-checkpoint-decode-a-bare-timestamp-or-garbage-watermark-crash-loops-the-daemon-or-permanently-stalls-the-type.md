---
id: SFL-0019
title: >-
  eventlogfile: guard the checkpoint decode - a bare-timestamp or garbage
  watermark crash-loops the daemon or permanently stalls the type
status: To Do
assignee: []
created_date: '2026-08-14 16:58'
labels:
  - followup
  - phase-2
milestone: m-0
dependencies: []
references:
  - 'https://github.com/rknightion/sf2loki/issues/103'
ordinal: 19000
---

## Description

<!-- SECTION:DESCRIPTION:BEGIN -->
## What

`EventLogFileSource._process_event_type` decodes its stored checkpoint with no error handling and no validation:

```python
# src/sf2loki/sources/eventlogfile_source.py:511-518
raw = await state.load(key)
if raw is None:
    since = default_since
    ids: list[_CarriedId] = []
else:
    parsed: dict[str, object] = json.loads(raw)          # :516 - no try/except, no isinstance
    since = str(parsed.get("last_created") or default_since)  # :517 - no watermark validation
    ids = _parse_carried_ids(parsed.get("ids", []))
```

Three distinct failure shapes follow from a stored value the source did not write itself:

1. **Non-JSON value (e.g. a bare `2026-07-01T00:00:00Z`)** - `json.loads` raises `JSONDecodeError`.
2. **Non-dict JSON (a quoted scalar `"2026-07-01T00:00:00Z"`, or a list)** - `json.loads` returns `str`/`list`, and `parsed.get` raises `AttributeError`.
3. **Well-formed dict with a garbage `last_created`** - no exception; the garbage flows into `since` unvalidated.

Shapes 1 and 2 are fatal to the whole process. `eventlogfile_source.py:388-395` deliberately re-raises any non-contained worker exception (`raise result`), which escapes `_process_cycle` and `events()` (`eventlogfile_source.py:300`). `Pipeline._produce` (`src/sf2loki/app.py:301-321`) wraps the `async for` in a `try`/`finally` with no `except`. `OrgSource.events` (`src/sf2loki/sources/org_adapter.py:106-120`) contains only `AuthError`. `_on_pipeline_done` (`src/sf2loki/app.py:1168-1177`) records the exception and `src/sf2loki/app.py:1224-1225` re-raises it, so the process exits nonzero. The stored value is unchanged on disk/in the object store, so the supervisor restart hits the identical exception - a permanent crash loop that takes down every source and every org, not just the affected EventType.

Shape 3 is a permanent silent stall. `to_soql_datetime_literal` (`src/sf2loki/salesforce/soql_client.py:44-53`) returns unparseable input **unchanged** by design, and `EventLogFileClient.list_files` interpolates it unquoted (`src/sf2loki/salesforce/eventlogfile_client.py:172-178`):

```
AND CreatedDate >= {since_literal}
```

so the query is `MALFORMED_QUERY`. That is wrapped into the `EventLogFileError` family and contained as a per-cycle listing skip (`eventlogfile_source.py:534-543`), which leaves the poison checkpoint in place and retries the same malformed query every poll interval forever. There is no fallback to `now - lookback`.

Both sibling sources already guard all three shapes, so this is an asymmetry rather than a design decision:

- `src/sf2loki/sources/eventlog_objects_source.py:119-136` - `_parse_checkpoint` catches `ValueError` and returns the raw string as the watermark (legacy bare-timestamp shape), and returns `raw, []` for a non-dict JSON scalar.
- `src/sf2loki/sources/eventlog_objects_source.py:297-312` - `_is_valid_watermark` check, WARNING log, fallback to `now - lookback`.
- `src/sf2loki/sources/apexlog_source.py:80-90` and `:165-172` - identical pair.
- `src/sf2loki/app.py:565-585` shows the codebase already treats this value as untrusted on the observability path: `_parse_eventlogfile_watermark` catches `ValueError`/`AttributeError`/`TypeError` around the same `json.loads(value).get("last_created")`. The source itself does not.

`src/sf2loki/backfill.py:583` repeats the same unguarded `parsed: dict[str, object] = json.loads(raw)` (lower blast radius: one-shot CLI, separate state file).

## Why it matters

The documented recovery runbook produces failure shape 1. `docs/deployment/state.md:44` lists `eventlogfile:<EventType>` (e.g. `eventlogfile:ApiTotalUsage`) in the SOQL-polling checkpoint-key table, and `docs/deployment/state.md:70-72` then says `state set KEY VALUE` "moves the checkpoint to an exact, known-good position (an **ISO-8601 timestamp** for SOQL-polled sources, a base64 `replay_id` for Pub/Sub)". `run_state_set` (`src/sf2loki/statecmd.py:173-189`) writes the string verbatim with no shape validation, and `docs/reference/cli.md:129-135` only shows a Pub/Sub example, so nothing tells the operator that an EventLogFile key needs the JSON `{"last_created": ..., "ids": [...]}` envelope.

So an operator following the shipped runbook to unstick one EventType instead takes the entire daemon into a restart loop, with a `JSONDecodeError` traceback that names `json.loads`, not the checkpoint they just wrote. Recovery requires knowing to run `state delete` on that key. The same crash reaches production via any other route that puts a non-envelope value in the store (hand-edited file store, a partially written object, a value copied from the `eventlog_objects`/`apexlog` legacy bare-timestamp shape).

Reproduced against current `main` using the existing fakes in `tests/sources/test_eventlogfile_source.py` plus a real `FileCheckpointStore`:

| stored value for `eventlogfile:Login` | observed |
| --- | --- |
| `2026-07-01T00:00:00Z` | `json.decoder.JSONDecodeError: Extra data: line 1 column 5` raised out of `events()` |
| `{"last_created": "junk"}` | no raise; client received `list_files("Login", "Hourly", since="junk", 1000)` |

## Proposed approach

Mirror the sibling sources exactly, so all four polling sources share one contract.

1. Add module-level helpers to `src/sf2loki/sources/eventlogfile_source.py` next to `_parse_carried_ids`:
   - `_watermark_datetime(value: str) -> datetime | None` and `_is_valid_watermark(value: str) -> bool`, copied from `eventlog_objects_source.py:103-117` (`datetime.fromisoformat(value.replace("Z", "+00:00"))`, `ValueError` -> `None`, naive -> assume UTC).
   - `_parse_checkpoint(raw: str) -> tuple[str, list[_CarriedId]]`: `json.loads` inside `try/except ValueError`; on failure return `(raw, [])` (legacy bare-timestamp shape). If the decoded object is a `dict`, return `(str(parsed.get("last_created") or ""), _parse_carried_ids(parsed.get("ids", [])))`. For any other JSON type (scalar, list) return `(raw, [])` rather than calling `.get`.
2. In `_process_event_type` replace `eventlogfile_source.py:511-518` with a call to `_parse_checkpoint`, then validate: if `not _is_valid_watermark(since)`, log at WARNING naming the event type and the rejected value (match the wording at `eventlog_objects_source.py:301-309`), increment the existing checkpoint-error counter if one is wired for this source (otherwise reuse `self._metrics.soql_poll_errors.labels(source="eventlogfile", object=event_type)`), and fall back to `default_since` with an empty carried-id window. Falling back to `default_since` re-lists the lookback window, which is a bounded duplicate window, not data loss (ingestion is at-least-once and Loki dedupes exact duplicates).
3. Apply the same `_parse_checkpoint` treatment to `src/sf2loki/backfill.py:583`.
4. Fix the runbook so it stops prescribing the crashing value: in `docs/deployment/state.md` state the exact per-source value shapes - `eventlog_objects:<Object>` and `apexlog` accept a bare ISO-8601 timestamp *or* the `{"last_ts", "ids"}` envelope, `eventlogfile:<EventType>` takes `{"last_created": "<ISO-8601>", "ids": []}`, `pubsub:<topic>` takes the replay envelope - and add an `eventlogfile` example to `docs/reference/cli.md`'s `state set` section. Recommend `state show` first to copy the live shape.

Optionally (separate, do not couple): have `run_state_set` (`src/sf2loki/statecmd.py:173-189`) warn when a value written to an `eventlogfile:` key does not decode to a JSON object carrying `last_created`.

---

Imported from GitHub issue #103 on 2026-08-14, when this repo migrated from GitHub Issues to Backlog.md. The original issue has been deleted; its verbatim body, labels and comments are preserved in `archive/issues-dump.json` (`jq '.[] | select(.number == 103)' archive/issues-dump.json`).

<sub>Filed from the 2026-07-30 full-repo audit (11 finder lanes + adversarial verification per finding).</sub>
<!-- SECTION:DESCRIPTION:END -->

## Acceptance Criteria
<!-- AC:BEGIN -->
- [ ] #1 `_parse_checkpoint` and `_is_valid_watermark` exist in `src/sf2loki/sources/eventlogfile_source.py` and are the only path by which a stored ELF checkpoint reaches `since`/`ids`.
- [ ] #2 A bare-timestamp checkpoint (`2026-07-01T00:00:00Z`) is accepted as `last_created` with an empty id window; no exception escapes `events()`.
- [ ] #3 A checkpoint that is not valid JSON at all (`{not json`), a JSON scalar (`"2026-07-01T00:00:00Z"`), and a JSON list (`[]`) each fall back to `now - lookback` with a WARNING and no exception.
- [ ] #4 A JSON object with an unparseable `last_created` (`{"last_created": "junk"}`) falls back to `now - lookback` with a WARNING; `list_files` is never called with a non-datetime `since`.
- [ ] #5 The fallback resets the carried-id window to empty (a garbage envelope's `ids` are not trusted against the fallback watermark).
- [ ] #6 `src/sf2loki/backfill.py:583` uses the same guarded decode; a poison backfill checkpoint does not raise out of the backfill run.
- [ ] #7 Test: `tests/sources/test_eventlogfile_source.py::test_poison_checkpoint_falls_back_to_lookback_without_raising` - table-driven over the values above, asserting no exception escapes `events()`, that the `since` seen by the fake client parses as a datetime within the configured lookback, and that a WARNING was logged (`caplog`).
- [ ] #8 Test: `tests/sources/test_eventlogfile_source.py::test_legacy_bare_timestamp_checkpoint_is_used_as_watermark` - a bare ISO timestamp is used as `since` verbatim (after `to_soql_datetime_literal` normalisation), not discarded in favour of the lookback.
- [ ] #9 Test: `tests/test_backfill.py` case covering a non-JSON stored backfill checkpoint.
- [ ] #10 Test: a pipeline-level test asserting that a poisoned `eventlogfile:*` checkpoint does not surface as a pipeline crash (guards the `eventlogfile_source.py:395` -> `app.py:301-321` -> `app.py:1224` path that currently exits the process).
- [ ] #11 `docs/deployment/state.md` documents the exact accepted value shape per checkpoint-key family, and `docs/reference/cli.md`'s `state set` section carries an `eventlogfile:` example.
- [ ] #12 `just gate` green.
<!-- AC:END -->

## Definition of Done
<!-- DOD:BEGIN -->
- [ ] #1 just gate is green (ruff check + ruff format --check + mypy src + pytest) — run it, don't assert it
- [ ] #2 just gen-config run and its output committed, if config.py changed (CI drift gate fails otherwise)
- [ ] #3 committed straight to main with a conventional-commit message, and pushed
<!-- DOD:END -->
