---
id: SFL-0032
title: >-
  cli: doctor's Loki test write uses a bare payload — a backend that rejects
  structured metadata passes preflight, then drops 100% of real data
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
  - 'https://github.com/rknightion/sf2loki/issues/116'
ordinal: 32000
---

## Description

<!-- SECTION:DESCRIPTION:BEGIN -->
## What

`sf2loki doctor`'s Loki check pushes a probe entry whose payload shape is unrelated to the shape the running daemon emits, so the one class of push rejection that causes permanent data loss is invisible to the preflight.

`_check_loki` (`src/sf2loki/doctor.py:358-393`) builds the probe entry inline:

- `labels={"source": "sf2loki-doctor"}` (`src/sf2loki/doctor.py:367`)
- `structured_metadata={}` (`src/sf2loki/doctor.py:368`)

Neither matches production:

- **Structured metadata.** Real entries populate `structured_metadata` from the configured routing: `src/sf2loki/sources/pubsub_source.py:785,795`, `src/sf2loki/sources/eventlog_objects_source.py:512,520`, `src/sf2loki/sources/eventlogfile_source.py:499-500,776,792` (per-type override falling back to the global list), and `src/sf2loki/backfill.py:254-256,316`. `apexlog` is stronger still: `src/sf2loki/sources/apexlog_source.py:305-308,324` promotes **every** non-empty metadata field unconditionally, so structured metadata is emitted even when `sink.loki.structured_metadata_fields` is the default empty list (`src/sf2loki/config.py:966`).
- **Static labels.** `LokiSink.__init__` (`src/sf2loki/sinks/loki/sink.py:100-114`) does not merge static labels; it only guards `cfg.labels`. Static labels are attached by the pipeline from `build_static_labels` (`src/sf2loki/app.py:117-138`, wired at `src/sf2loki/app.py:1144-1150`) — `job`, `service_name`, `environment`, `sf_org_id`, plus the operator's `sink.loki.labels`. The doctor never enters that path, so the probe stream carries one label.

The rejection path is a permanent drop with checkpoint advance. A `400` on a single-entry batch raises `PermanentSinkError(..., reason="bad_request")` (`src/sf2loki/sinks/loki/sink.py:263-267`). The pipeline's `PermanentSinkError` branch counts `loki_entries_dropped{reason="bad_request"}`, logs `dropping undeliverable batch and advancing checkpoint`, and calls `await self._commit(all_entries)` (`src/sf2loki/app.py:471-487`). The batch is gone and the watermark has moved past it — no retry, no recovery short of a manual `state reset` plus a Salesforce-side re-read window that may no longer exist.

## Why it matters

Structured metadata requires Loki schema v13 + TSDB + `allow_structured_metadata: true`. That is the default on Grafana Cloud but not self-hosted or in Alloy's embedded Loki — documented at `README.md:594-595`, while `README.md:10` advertises self-hosted Loki and local Alloy (`loki.source.api`) as supported targets. A backend with structured metadata disabled returns `400` for pushes that carry it and `2xx` for pushes that do not.

Concrete failure: operator points sf2loki at a self-hosted Loki still on schema v11, configures `sink.loki.structured_metadata_fields` (recommended for cardinality control in `docs/sources/pii-and-sampling.md`, and pre-populated at `config.example.yaml:357`) or simply enables `sources.apexlog`. `sf2loki doctor` reports `loki PASS — pushed 1 test line in Nms`, exit 0, because the bare probe has no structured metadata. The daemon starts, and every real batch is `400`'d, dropped as `bad_request`, and checkpointed past. Ingestion loss is 100% and permanent, starting immediately after a green preflight.

This inverts the contract the doctor advertises: `docs/reference/cli.md:52-58` says the test write exists "to confirm the write path". The check currently confirms URL, tenant, auth, encoding, and compression, but not deliverability of the payload the daemon will actually send.

No test pins probe payload shape today — `tests/test_doctor.py` covers only the PASS-latency case (line 488) and the 401 tenant hint (line 509).

## Proposed approach

1. **Compute whether real entries will carry structured metadata**, in a helper (e.g. `_emits_structured_metadata(cfg) -> bool`) that returns `True` when any of:
   - `cfg.sink.loki.structured_metadata_fields` is non-empty (`src/sf2loki/config.py:966`);
   - any `cfg.sources.eventlogfile.event_types` entry has a non-empty `structured_metadata_fields` (`src/sf2loki/config.py:580`);
   - `cfg.sources.apexlog.enabled` — unconditional, per `src/sf2loki/sources/apexlog_source.py:305-308`.
2. **Mirror the shape in the probe entry.** When the helper is `True`, set one structured metadata key (`{"sf2loki_doctor": "1"}`) on the probe. Merge the static labels `build_static_labels(environment=sf.environment, org_id=org_id, operator_labels=cfg.sink.loki.labels)` would produce, keeping the per-entry `source="sf2loki-doctor"` (it is in `RESERVED_STATIC_LABELS`, `src/sf2loki/sinks/loki/labels.py:14`, so it can never come from operator labels). Import `build_static_labels` from `sf2loki.app` — `doctor.py` already imports `App` from there (`src/sf2loki/doctor.py:32`), so no new dependency edge.
3. **Thread `org_id` into the check.** `_check_auth` already resolves it (`src/sf2loki/doctor.py:155`) but `run_doctor` discards it at `src/sf2loki/doctor.py:887`. Capture it and pass it to `_check_loki`. The loki check deliberately still runs when auth failed (`src/sf2loki/doctor.py:900`, pinned by `tests/test_doctor.py:257`), so fall back to `sf.org_id or ""` and omit `sf_org_id` when it is unknown rather than sending an empty label value.
4. **Classify a 400 with a bare fallback retry.** On `PermanentSinkError` when the probe carried structured metadata, retry once with structured metadata stripped. If the bare write succeeds, FAIL with a targeted detail: `Loki accepted a bare line but rejected structured metadata — the backend needs schema v13 + TSDB + allow_structured_metadata: true, or clear sink.loki.structured_metadata_fields (note: apexlog always emits structured metadata)`. If the bare write also fails, report the original error unchanged. This gives a precise diagnosis without adding a config or CLI flag. Only this one 400 case performs a second write.
5. **Update the "exactly one write" wording** in `docs/reference/cli.md:52-58`, the PASS detail string (`src/sf2loki/doctor.py:391`), and `docs/troubleshooting.md` to reflect: one test line, plus one bare diagnostic retry only when the first is rejected. Add a troubleshooting row for the new FAIL detail.

Non-goals (record the reasoning so they are not "fixed" later):

- **Do not probe the out-of-order/reject window** (`reject_old_samples_max_age`, `README.md:493-503`). A probe timestamped in the past would write real-looking data at a fake time into the operator's index; the backfill docs already own that dimension.
- **Do not probe ELF promoted labels** (`promote_labels`, `src/sf2loki/sources/eventlogfile_source.py:781`). Promoted keys are ordinary valid Loki label names, so they cannot produce the 400 this issue is about.

---

Imported from GitHub issue #116 on 2026-08-14, when this repo migrated from GitHub Issues to Backlog.md. The original issue has been deleted; its verbatim body, labels and comments are preserved in `archive/issues-dump.json` (`jq '.[] | select(.number == 116)' archive/issues-dump.json`).

<sub>Filed from the 2026-07-30 full-repo audit (11 finder lanes + adversarial verification per finding).</sub>
<!-- SECTION:DESCRIPTION:END -->

## Acceptance Criteria
<!-- AC:BEGIN -->
- [ ] #1 `_emits_structured_metadata(cfg)` returns `True` for a non-empty global `sink.loki.structured_metadata_fields`, `True` for an ELF per-type `structured_metadata_fields` with an empty global list, `True` when `sources.apexlog.enabled` with both lists empty, and `False` when none apply.
- [ ] #2 The doctor probe entry carries `structured_metadata={"sf2loki_doctor": "1"}` when the helper is `True`, and `{}` when it is `False`.
- [ ] #3 The doctor probe entry's labels equal `build_static_labels(...)` merged with `source="sf2loki-doctor"`, with `sf_org_id` present when the org id resolved and absent (not empty-valued) when auth failed.
- [ ] #4 Test: `respx` mock returning `400` for a body containing structured metadata and `204` for one without → `loki FAIL` whose detail names `allow_structured_metadata` and schema v13, and `run_doctor` exits `1`.
- [ ] #5 Test: `respx` mock returning `400` for every push → `loki FAIL` reporting the original `PermanentSinkError` message, with no structured-metadata-specific hint.
- [ ] #6 Test: config with no structured metadata anywhere → exactly one POST, body carries no structured metadata, `loki PASS`.
- [ ] #7 Test: asserts the posted labels include `job`, `service_name`, `environment`, `sf_org_id`, the operator's `sink.loki.labels`, and `source="sf2loki-doctor"`.
- [ ] #8 Test: auth-failure path (extending `tests/test_doctor.py:257`) still reaches the loki check and posts without an empty-valued `sf_org_id`.
- [ ] #9 `docs/reference/cli.md` and `docs/troubleshooting.md` describe the mirrored payload shape, the conditional second diagnostic write, and how to read the new FAIL detail.
- [ ] #10 `just gate` green.
<!-- AC:END -->

## Definition of Done
<!-- DOD:BEGIN -->
- [ ] #1 just gate is green (ruff check + ruff format --check + mypy src + pytest) — run it, don't assert it
- [ ] #2 just gen-config run and its output committed, if config.py changed (CI drift gate fails otherwise)
- [ ] #3 committed straight to main with a conventional-commit message, and pushed
<!-- DOD:END -->
