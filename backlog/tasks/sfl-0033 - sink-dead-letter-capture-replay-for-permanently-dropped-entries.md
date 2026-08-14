---
id: SFL-0033
title: 'sink: dead-letter capture + replay for permanently dropped entries'
status: To Do
assignee: []
created_date: '2026-08-14 16:58'
labels:
  - followup
  - phase-2
  - roadmap
milestone: m-4
dependencies: []
references:
  - 'https://github.com/rknightion/sf2loki/issues/117'
ordinal: 33000
---

## Description

<!-- SECTION:DESCRIPTION:BEGIN -->
## What

Permanently dropped entries are counted and logged, but their content is never persisted anywhere, and the checkpoint advances past them — so the data is unrecoverable from both sf2loki and Salesforce.

Three drop sites, all count-and-log only:

| Site | Behaviour |
| --- | --- |
| `src/sf2loki/app.py:472-487` | `PermanentSinkError` from the sink: increments `sf2loki_loki_entries_dropped{reason}` by `len(batch.entries)`, logs `entries=<count>, reason=..., error=...`, then `await self._commit(all_entries)` |
| `src/sf2loki/app.py:441-452` | Budget-exhaustion drop mode: increments `{reason="budget"}`, logs count+bytes, commits |
| `src/sf2loki/sinks/loki/sink.py:274-288` | 400/413 bisection: per undeliverable half, increments the counter by `len(half.entries)`, logs count+reason, returns |

The one-entry terminal case raises at `src/sf2loki/sinks/loki/sink.py:265-268`. `src/sf2loki/backfill.py:373-381` repeats the same count-and-log shape for the one-shot backfill path.

No log line, metric, or file carries the dropped payload. `rg -ni 'dead.?letter|dlq|quarantin'` over `src/`, `docs/`, `tests/`, `config.example.yaml` and `deploy/` returns nothing; the only spool in the tree is the EventLogFile download spool in `src/sf2loki/salesforce/eventlogfile_client.py`.

The content is fully available at every drop point: `LogEntry` (`src/sf2loki/model.py:26-62`) carries `timestamp`, `labels`, `line`, `structured_metadata` and `checkpoint`, and `Batch.entries` is in scope at each site. Nothing is written because no writer exists.

## Why it matters

A Loki-side limit change (lower `max_line_size`, stricter per-stream limits, a rejected label) turns a burst of otherwise-valid events into `bad_request` drops. The operator sees `sf2loki_loki_entries_dropped{reason="bad_request"}` step by 347 and a log line reading `dropping undeliverable batch and advancing checkpoint entries=347 reason=bad_request`. From that they cannot determine which events were lost, cannot answer a compliance question about the gap, and cannot re-ingest: the checkpoint has already advanced past those records, so recovery requires hand-editing checkpoint state (`sf2loki state set`, added in #63) to rewind a whole source stream, re-delivering everything in the window — and the offending entries will be rejected identically on the second pass.

This is Salesforce Event Monitoring data: login history, setup audit trail, API access. A silent, content-free gap in that stream is the specific failure the connector exists to prevent. #63 solved advancing *past* a poison checkpoint; it deliberately did not address recovering the payload.

## Proposed approach

A `DeadLetterWriter` seam, off by default, injected from the composition root into both the sink and the pipeline (the sink's bisection path drops inside `LokiSink`, so a pipeline-only writer would miss those entries; the same instance injected into `LokiSink.__init__` at `src/sf2loki/sinks/loki/sink.py:100-114` also covers `backfill.py`, which drives `LokiSink` directly).

**Config** — new nested model on `LokiConfig` (`src/sf2loki/config.py:922`):

```yaml
sink:
  loki:
    dead_letter:
      enabled: false            # default off; no behaviour change unless set
      path: /var/lib/sf2loki/deadletter
      max_bytes: 67108864       # total spool budget; oldest file pruned first
      max_file_bytes: 8388608   # rotate at this size
      retention: 7d
      include_budget_drops: false  # budget drops are an intentional cost control
```

Regenerate the config artifacts with `just gen-config` — `config.example.yaml` and `docs/config-reference.md` are generated and gated by `tests/test_config_artifacts_drift.py`.

**Record shape** — one NDJSON object per dropped entry:

```json
{"ts_unix_nano": 1751400000000000000, "labels": {...}, "structured_metadata": {...},
 "line": "...", "reason": "bad_request", "checkpoint_key": "pubsub:/event/LoginEventStream",
 "checkpoint_value": "...", "truncated": false, "captured_at": "2026-07-30T12:00:00Z"}
```

`line` is already post-transform: redaction/hashing/row-filtering run in source shaping before the entry enters the pipeline, so a replay needs no re-transform and cannot re-expose raw PII. Set `truncated: true` when the capture happens after `_cap_lines` (`src/sf2loki/sinks/loki/sink.py:243`) mutated the line — that method rewrites `line` in place before encoding, so the sink-side capture path sees the capped string, not the original. Capture on the pipeline path (`app.py`) before the sink is entered where possible.

**Write semantics** — append to `<path>/<instance>-<utc-date>-<seq>.ndjson`, mode `0600`, dir `0700`; rotate on `max_file_bytes`; prune oldest on `max_bytes` or `retention`. Include an instance discriminator in the filename so a promoted standby under file-lease HA (`src/sf2loki/coordinate/file_lease.py`) sharing the storage path never appends into or overwrites the previous leader's file. Writes must be off the event loop (thread executor or `asyncio.to_thread`), consistent with the existing async-encode treatment in the sink.

**Failure containment** — a dead-letter write error must never stall or crash the pipeline or block the checkpoint advance: log at `error`, increment `sf2loki_deadletter_write_errors`, proceed with the existing drop-and-commit path unchanged. Spool exhaustion is not backpressure.

**Metrics** — declared beside the existing counters in `src/sf2loki/obs/metrics.py:312-331`:
- `sf2loki_deadletter_entries_written{reason}` (counter)
- `sf2loki_deadletter_bytes_written` (counter)
- `sf2loki_deadletter_write_errors` (counter)

**Replay verb** — `sf2loki deadletter list --config c.yaml` and `sf2loki deadletter replay [FILE ...] --config c.yaml [--dry-run]`, following the nested-subcommand pattern already used for `state` (`src/sf2loki/cli.py:144-186`) and building the sink the way `src/sf2loki/doctor.py:362` does. Replay reconstructs `LogEntry` objects from the NDJSON, pushes them in batches through `LokiSink`, and on success removes (or renames to `.replayed`) the file. Entries that are permanently rejected a second time move to a sibling `.failed` file rather than looping; report counts to stdout. Replay never touches checkpoint state.

**Docs** — add the three metrics to the table in `docs/observability/metrics.md` (existing drop-counter row at `docs/observability/metrics.md:61`), a config section, and a recovery runbook section covering: read the spool, decide whether the rejection cause has been fixed Loki-side, then replay. Link it from the drop-related alert entries in `docs/observability/alerts.md`, and note in the docs that spool files hold event content at the same sensitivity as the shipped lines.

---

Imported from GitHub issue #117 on 2026-08-14, when this repo migrated from GitHub Issues to Backlog.md. The original issue has been deleted; its verbatim body, labels and comments are preserved in `archive/issues-dump.json` (`jq '.[] | select(.number == 117)' archive/issues-dump.json`).

## Additional evidence (parallel review lanes)

Placement constraint for the fix: the sink-internal 400/413 bisection absorbs undeliverable halves inside `LokiSink.push` (src/sf2loki/sinks/loki/sink.py:274-288) and returns normally, so a dead-letter hook wired only into the pipeline's `PermanentSinkError` handler would miss that path entirely — the capture point must cover both sites (and src/sf2loki/backfill.py:373-381 for the one-shot path). Retention math makes the drops irreversible in practice: Pub/Sub replay is ~72h, EventLogFile blobs 30d (24h on some editions), ApexLog debug logs far shorter — by the time the `loki_entries_dropped` spike is noticed, the originals may already be gone, and `sf2loki state set` (#63) re-fetches everything after the cursor rather than the specific rejected rows.

<sub>Filed from the 2026-07-30 full-repo audit (11 finder lanes + adversarial verification per finding).</sub>
<!-- SECTION:DESCRIPTION:END -->

## Acceptance Criteria
<!-- AC:BEGIN -->
- [ ] #1 `sink.loki.dead_letter` config model added with `enabled: false` default; `just gen-config` re-run and `tests/test_config_artifacts_drift.py` green.
- [ ] #2 `DeadLetterWriter` implemented, constructed once in the composition root, injected into `LokiSink` and the pipeline; `None` when disabled and every call site tolerates `None`.
- [ ] #3 Pipeline `PermanentSinkError` drop (`src/sf2loki/app.py:472-487`) writes one record per dropped entry before committing.
- [ ] #4 Sink bisection drop (`src/sf2loki/sinks/loki/sink.py:274-288`) writes one record per dropped entry in each undeliverable half.
- [ ] #5 Budget drop (`src/sf2loki/app.py:441-452`) writes records only when `include_budget_drops` is true.
- [ ] #6 `backfill.py`'s drop path (`src/sf2loki/backfill.py:373-381`) is covered by the sink-level writer.
- [ ] #7 Three new metrics emitted and documented in `docs/observability/metrics.md`.
- [ ] #8 `sf2loki deadletter list` and `sf2loki deadletter replay` implemented, including `--dry-run`, `.replayed`/`.failed` handling, and no checkpoint mutation.
- [ ] #9 Runbook section added and linked from the drop-related alert entries in `docs/observability/alerts.md`.
- [ ] #10 Test: `tests/test_pipeline.py` — a sink raising `PermanentSinkError` produces one NDJSON record per entry with the expected `reason`, `checkpoint_key`/`checkpoint_value`, labels, structured metadata and line, and the checkpoint still advances.
- [ ] #11 Test: `tests/test_pipeline.py` — budget drop writes nothing with `include_budget_drops: false`, and writes records with it true.
- [ ] #12 Test: `tests/sinks/test_sink.py` — a 400 on a multi-entry batch where one half stays poison writes records for exactly the poison half and none for the delivered half.
- [ ] #13 Test: rotation and pruning — exceeding `max_file_bytes` opens a new file; exceeding `max_bytes` prunes the oldest; files created `0600`.
- [ ] #14 Test: a writer whose write raises does not propagate — the drop path still counts, logs, commits, and increments `sf2loki_deadletter_write_errors`.
- [ ] #15 Test: round-trip — records captured from a drop are replayed by the CLI through a fake sink and reproduce the original `LogEntry` values byte-for-byte on `line`, `labels` and `structured_metadata`.
- [ ] #16 Test: `enabled: false` (default) is a no-op — no directory created, no writes, existing drop behaviour byte-identical.
- [ ] #17 `just gate` green.
<!-- AC:END -->

## Definition of Done
<!-- DOD:BEGIN -->
- [ ] #1 just gate is green (ruff check + ruff format --check + mypy src + pytest) — run it, don't assert it
- [ ] #2 just gen-config run and its output committed, if config.py changed (CI drift gate fails otherwise)
- [ ] #3 committed straight to main with a conventional-commit message, and pushed
<!-- DOD:END -->
