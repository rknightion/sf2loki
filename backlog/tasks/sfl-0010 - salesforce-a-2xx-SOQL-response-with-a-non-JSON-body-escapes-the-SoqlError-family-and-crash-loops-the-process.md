---
id: SFL-0010
title: >-
  salesforce: a 2xx SOQL response with a non-JSON body escapes the SoqlError
  family and crash-loops the process
status: To Do
assignee: []
created_date: '2026-08-14 16:58'
labels:
  - followup
  - phase-2
milestone: m-0
dependencies: []
references:
  - 'https://github.com/rknightion/sf2loki/issues/94'
ordinal: 10000
---

## Description

<!-- SECTION:DESCRIPTION:BEGIN -->
## What

`SoqlClient.query` promises callers a single exception family. `src/sf2loki/salesforce/soql_client.py:63-66`:

> `SoqlError` on any other non-2xx response or transport failure — callers only ever see the SoqlError family.

The response-body parse breaks that promise. The `try` at `soql_client.py:101-113` wraps only the two `self._client.get(...)` calls and catches `httpx.HTTPError`, normalising transport failures into `SoqlError`. The body parse sits outside it:

- `soql_client.py:124` — `body = response.json()`. On a 2xx whose body is not JSON this raises `json.JSONDecodeError`, which subclasses `ValueError`, not `httpx.HTTPError`. Nothing normalises it.
- `soql_client.py:125` — `body.get("records", [])` assumes a JSON **object**. A 2xx whose body is a JSON array (the shape Salesforce uses for error payloads) raises `AttributeError` instead.

Neither exception is contained anywhere downstream:

| layer | handler | file:line |
| --- | --- | --- |
| eventlog_objects DESC drain | `except SoqlThrottledError` / `except SoqlError` only | `src/sf2loki/sources/eventlog_objects_source.py:323`, `:335` |
| eventlog_objects ASC drain | `except SoqlThrottledError` / `except SoqlError` only | `src/sf2loki/sources/eventlog_objects_source.py:397`, `:409` |
| ELF listing / EventType discovery | re-wraps `SoqlError` into `EventLogFileError` only | `src/sf2loki/salesforce/eventlogfile_client.py:193`, `:213` |
| ApexLog listing / TraceFlag count | re-wraps `SoqlError`/`SoqlThrottledError` only | `src/sf2loki/salesforce/apexlog_client.py:144`, `:146`, `:163`, `:165` |
| ELF source / ApexLog source poll loops | catch only their client's own family (`EventLogFileError`, `ApexLogError`) | `src/sf2loki/sources/eventlogfile_source.py:414`, `:536`, `:579`, `:653`; `src/sf2loki/sources/apexlog_source.py:186`, `:190`, `:267`, `:344`, `:346` |
| multi-org wrapper | `except AuthError` only | `src/sf2loki/sources/org_adapter.py:120` |

So the decode error escapes the source generator and walks all the way out:

1. `Pipeline._produce` iterates `source.events(...)` with no exception handling (`src/sf2loki/app.py:300-320`).
2. `Pipeline.run` re-raises it via `await producers_done` (`src/sf2loki/app.py:279-292`).
3. `App._run_pipeline` absorbs only `StateFenceError` (`src/sf2loki/app.py:1236-1239`).
4. `_on_pipeline_done` records it in `crash` and sets the global stop (`src/sf2loki/app.py:1168-1177`).
5. `App.run` re-raises after resource shutdown (`src/sf2loki/app.py:1222-1225`), `uvloop.run(app.run())` propagates (`src/sf2loki/cli.py:270`), the process exits nonzero.

This directly contradicts the documented containment contract in `src/sf2loki/sources/eventlogfile_source.py:232-241` ("All Salesforce failures (listing, discovery, download — HTTP, SOQL or transport) are contained per cycle: the affected work is skipped with a WARNING ... and the poll loop retries next cycle").

The same unguarded parse exists at `src/sf2loki/salesforce/limits_client.py:54` and `src/sf2loki/salesforce/metadata_client.py:47`, but those callers already contain it with a catch-all (`src/sf2loki/obs/limits_poller.py:51-53`, `src/sf2loki/sources/pubsub_source.py:255-266`). `soql_client.py` is the only unguarded parse whose call path has no catch-all.

A smaller instance of the same gap: `src/sf2loki/salesforce/eventlogfile_client.py:181` does `str(record["Id"])` inside a `try` that catches `SoqlError` only, so a listing row without `Id` raises `KeyError` out of the same path.

## Why it matters

An egress proxy, captive gateway, or edge maintenance page in front of the connector that answers `api.salesforce.com` with `200 text/html` (or an empty/truncated 200 body) turns every poll cycle into an unhandled `JSONDecodeError`. The designed behaviour is a WARNING plus a retry next cycle, with `soql_poll_errors` incremented and checkpoints making the retry safe. The actual behaviour is a nonzero process exit and a container restart loop for the duration of the proxy fault.

Blast radius is wider than the affected source:

- Single-org: the healthy pubsub streaming lane is torn down along with the failing polling source, so streaming ingestion stops too.
- Multi-org: `org_adapter.py:120` contains only `AuthError`, so one org's malformed body takes every configured org down — the exact outcome that per-org isolation exists to prevent.
- Restart also drops all in-flight batches and re-runs startup auth probes against an already-degraded network path.

## Proposed approach

Normalise the body parse inside `SoqlClient.query` so all three callers inherit the existing per-cycle containment for free.

In `src/sf2loki/salesforce/soql_client.py`, replace `body = response.json()` (line 124) with a guarded parse that also validates the decoded shape:

```python
try:
    body = response.json()
except ValueError as exc:
    raise SoqlError(
        f"SOQL response was not JSON (HTTP {response.status_code}, "
        f"content-type {response.headers.get('content-type', 'unknown')!r}): "
        f"{exc} — body starts: {response.text[:200]!r}"
    ) from exc
if not isinstance(body, dict):
    raise SoqlError(
        f"SOQL response JSON was {type(body).__name__}, expected an object "
        f"(HTTP {response.status_code}) — body starts: {response.text[:200]!r}"
    )
```

Truncate the echoed body (200 chars is enough to identify a proxy splash page) so a large HTML page does not flood the log. Update the class docstring at `soql_client.py:63-66` to state that a malformed/non-object 2xx body is also normalised into `SoqlError`, keeping the "callers only ever see the SoqlError family" contract literally true.

Separately harden `src/sf2loki/salesforce/eventlogfile_client.py:181`: use `record.get("Id")` and skip a row with no id (WARNING, `eventlogfile_download_errors` with `reason="listing"`), or raise `EventLogFileError`, so a malformed listing row cannot raise `KeyError` past the `except SoqlError` at line 193.

No caller changes are needed — `eventlog_objects_source.py:335`, `eventlogfile_client.py:193` and `apexlog_client.py:146` already route `SoqlError` into skip-cycle-and-retry with the right metric.

---

Imported from GitHub issue #94 on 2026-08-14, when this repo migrated from GitHub Issues to Backlog.md. The original issue has been deleted; its verbatim body, labels and comments are preserved in `archive/issues-dump.json` (`jq '.[] | select(.number == 94)' archive/issues-dump.json`).

<sub>Filed from the 2026-07-30 full-repo audit (11 finder lanes + adversarial verification per finding).</sub>
<!-- SECTION:DESCRIPTION:END -->

## Acceptance Criteria
<!-- AC:BEGIN -->
- [ ] #1 `SoqlClient.query` raises `SoqlError` (not `json.JSONDecodeError`) on a 200 whose body is not JSON; the message names the status code, the content-type and a truncated body prefix.
- [ ] #2 `SoqlClient.query` raises `SoqlError` on a 200 whose body decodes to a JSON array or scalar rather than an object.
- [ ] #3 The truncation cap is applied so an HTML page cannot dump unbounded text into the log line.
- [ ] #4 `soql_client.py:63-66` docstring updated to cover malformed/non-object 2xx bodies.
- [ ] #5 `eventlogfile_client.list_files` no longer raises `KeyError` on a listing record with no `Id`.
- [ ] #6 `tests/salesforce/test_soql_client.py`: a mock transport returning `200` with `text/html` body → `pytest.raises(SoqlError)`; asserts the exception is not a `ValueError` leak by matching the "not JSON" message.
- [ ] #7 `tests/salesforce/test_soql_client.py`: a mock transport returning `200` with body `[]` (JSON array) → `pytest.raises(SoqlError)`.
- [ ] #8 `tests/salesforce/test_soql_client.py`: a 200 with an empty body → `SoqlError`.
- [ ] #9 `tests/sources/test_eventlog_objects_source.py`: a fake SOQL client that raises the new `SoqlError` for one cycle → the source generator does NOT propagate, `soql_poll_errors{source="eventlog_objects"}` increments once, and the next cycle polls again from the same checkpoint (pins the per-cycle containment contract).
- [ ] #10 `tests/salesforce/test_eventlogfile_client.py`: a listing record with no `Id` → no `KeyError`; the row is skipped or an `EventLogFileError` is raised.
- [ ] #11 `just gate` green.
<!-- AC:END -->

## Definition of Done
<!-- DOD:BEGIN -->
- [ ] #1 just gate is green (ruff check + ruff format --check + mypy src + pytest) — run it, don't assert it
- [ ] #2 just gen-config run and its output committed, if config.py changed (CI drift gate fails otherwise)
- [ ] #3 committed straight to main with a conventional-commit message, and pushed
<!-- DOD:END -->
