---
id: SFL-0034
title: 'shaping: runtime cardinality guard for promoted per-type stream labels'
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
  - 'https://github.com/rknightion/sf2loki/issues/118'
ordinal: 34000
---

## Description

<!-- SECTION:DESCRIPTION:BEGIN -->
## What

`sources.eventlogfile.event_types[].labels` promotes arbitrary ELF CSV columns to real Loki stream labels. That promotion is completely unbounded at runtime.

- `promote_labels` (`src/sf2loki/shaping.py:134-142`) is a plain dict comprehension over `label_fields`. No distinct-value tracking, no cap, no metric. Its docstring pushes the obligation onto callers: *"callers MUST restrict `label_fields` to low-cardinality columns to avoid a Loki stream-cardinality explosion."*
- Call sites merge the result straight into the entry label set: `src/sf2loki/sources/eventlogfile_source.py:781` (live source) and `src/sf2loki/backfill.py:295` (backfill command). Reserved keys (`source`, `event_type`) are merged after so they win, but promoted values themselves are untouched.
- The `ALLOWED_LABELS` allowlist (`src/sf2loki/sinks/loki/labels.py:7-9`) does **not** cover this path. `guard_static_labels` is called exactly once, at `src/sf2loki/sinks/loki/sink.py:108`, against `sink.loki.labels` only. Per-entry labels bypass the allowlist by design (they must — `backfill.py:301` also sets `backfill="true"`, which is not on the allowlist either).
- Config validation of `EventLogFileTypeConfig` (`src/sf2loki/config.py:588-620`) enforces only two things about `labels`: the key is not in `_RESERVED_LABEL_KEYS` (`config.py:552`), and the column is not simultaneously `drop_field`-ed (`config.py:772-786`). Cardinality is prose only: `config.py:591-593` — *"Keep these LOW cardinality — each distinct value is a new Loki stream"* — which propagates verbatim into `config.example.yaml:145` and `docs/config-reference.md:130`.
- Neither `--check` nor `doctor` evaluates it. `doctor`'s only label reference is its own probe push (`src/sf2loki/doctor.py:366`).
- The field's `examples=[["DELEGATED_USER"]]` (`config.py:595`) puts a username-shaped column into the generated example config and into `docs/sources/eventlogfile.md:77`.

Net: the one surface in the project that can mint unbounded Loki streams has no runtime control, no observability, and no preflight signal.

Related but not overlapping: the egress guardrails from #26 (per-type sampling, rate caps, daily byte budget) bound *volume*, not cardinality — `docs/sources/cost-controls.md:78` states this explicitly.

## Why it matters

An operator sets `labels: [USER_ID]` (or `REQUEST_ID`, `SESSION_KEY`, `URI`) on a busy ELF type. Nothing complains at config load, at `--check`, at `doctor`, or in the logs. Each distinct column value mints a new Loki stream. Two outcomes, both bad:

1. The tenant hits Loki's `max_streams_per_user`. Pushes start failing with 429/`per-user streams limit exceeded` — **tenant-wide**, so every other writer into the same Grafana Cloud stack starts failing too, not just sf2loki.
2. Stream count and index cost climb until it shows up on the bill.

`CLAUDE.md` and `docs/sources/index.md:70-89` both call label cardinality load-bearing and point at the allowlist as "the load-bearing control", but that control does not reach this path. A prose warning in a config description is the only thing between an operator and a tenant-wide write outage.

Secondary, same surface: `promote_labels` also applies no length bound to the stringified value (`shaping.py:142`). A long column value produces a label value over Loki's `max_label_value_length` (default 2048), which rejects the push.

## Proposed approach

**1. A bounded distinct-value tracker in `shaping.py`.**

Add a small class next to `promote_labels`:

```python
class LabelCardinalityGuard:
    """Bounds distinct values per (event_type, label); demotes a column that blows the cap."""
    def __init__(self, limit: int) -> None: ...
    def filter(self, event_type: str, labels: dict[str, str]) -> tuple[dict[str, str], dict[str, str]]:
        """Return (kept_labels, demoted) — demoted go to structured metadata."""
```

Semantics, all load-bearing:

- `limit <= 0` disables the guard entirely (tracker is a pass-through, no memory cost).
- Per `(event_type, label)` key, hold a `set[str]` of values seen. While `len(set) <= limit`, keep promoting and add the value.
- **Tripping is sticky for the process lifetime.** Once a `(event_type, label)` pair exceeds the cap it is recorded in a `tripped` set and never promoted again, even if later rows only carry already-seen values. Non-sticky demotion would flap the label set row by row, which creates *more* streams and risks per-stream out-of-order rejects. On trip, drop the value set for that pair to release memory.
- Memory is bounded at `limit` values per tripped-or-tracking pair; the pairs themselves are bounded by `len(event_types) * len(labels)` from config.
- The tracker resets on restart. This is containment, not accounting — document it.

**2. Demote, don't drop.** A demoted column's value moves into structured metadata for that entry, so no data is lost — it just stops being an indexed stream label. That is exactly what the rest of the pipeline does with high-cardinality fields (`route_fields`, `shaping.py:33-55`).

**3. Config key.** `sources.label_value_limit: int = 100` on `SourcesConfig` (alongside `allow_overlap`/`transform_salt`, `src/sf2loki/config.py:819-844`), `ge=0`, `0` disables. Description must state that exceeding the cap demotes the column to structured metadata rather than dropping data. Run `just gen-config` after.

**4. Metric.** `sf2loki_label_cardinality_exceeded` counter with attributes `event_type`, `label`, created alongside the existing instruments in `src/sf2loki/obs/metrics.py` (unsuffixed at creation — the OTLP exporter adds `_total`, see `docs/observability/metrics.md:40-43`). Add the row to the metrics table in `docs/observability/metrics.md`.

**5. Log once per trip at ERROR**, naming the event type, the column, the observed count, and the remedy (remove it from `labels`, or add it to `structured_metadata_fields`). Once per `(event_type, label)`, not per row — same rate-limiting discipline as the over-budget WARN in `src/sf2loki/egress.py:46-47`.

**6. Wire both call sites**: `src/sf2loki/sources/eventlogfile_source.py:781` and `src/sf2loki/backfill.py:295`. The ELF source can hold the tracker on the instance; `backfill` constructs one per run and threads it alongside the existing `label_fields` parameter (`backfill.py:270`, `410`, `494`).

**7. `doctor` preflight (static, cheap).** Add a `labels` check following the `_check_transforms` pattern (`src/sf2loki/doctor.py:401-421`) — config-only, no API call. WARN when any promoted column name matches a known-high-cardinality shape: ends with `_ID`/`ID`, or contains `IP`, `URI`, `URL`, `KEY`, `SESSION`, `EMAIL`, `TIMESTAMP`, `BYTES`, `MS`, `SIZE`, `COUNT`. Report the columns and the configured `label_value_limit`. PASS when `labels` is empty everywhere or nothing matches; the WARN text must say the guard will contain it at runtime rather than implying a hard failure. Sampling a live ELF file to measure real distinct counts is explicitly **out of scope** — it needs a download per type and the static heuristic plus the runtime guard already covers the failure.

Out of scope for this issue (note only): promoted column names are not validated as legal LogQL label identifiers, and label values are not length-capped. ELF CSV headers are always `[A-Z0-9_]+` so the first is low risk in practice.

---

Imported from GitHub issue #118 on 2026-08-14, when this repo migrated from GitHub Issues to Backlog.md. The original issue has been deleted; its verbatim body, labels and comments are preserved in `archive/issues-dump.json` (`jq '.[] | select(.number == 118)' archive/issues-dump.json`).

<sub>Filed from the 2026-07-30 full-repo audit (11 finder lanes + adversarial verification per finding).</sub>
<!-- SECTION:DESCRIPTION:END -->

## Acceptance Criteria
<!-- AC:BEGIN -->
- [ ] #1 `sources.label_value_limit` added to `SourcesConfig` (`src/sf2loki/config.py`), default `100`, `ge=0`, `0` disables; `just gen-config` re-run so `config.example.yaml` and `docs/config-reference.md` match (the `tests/test_config_artifacts_drift.py` gate stays green).
- [ ] #2 `LabelCardinalityGuard` added to `src/sf2loki/shaping.py` with sticky per-`(event_type, label)` demotion and a bounded value set.
- [ ] #3 `sf2loki_label_cardinality_exceeded{event_type,label}` counter added in `src/sf2loki/obs/metrics.py` and documented in the `docs/observability/metrics.md` table.
- [ ] #4 Guard wired into `src/sf2loki/sources/eventlogfile_source.py` (`_shape_row`) and `src/sf2loki/backfill.py` (`_shape_rows`); demoted columns land in the entry's structured metadata.
- [ ] #5 `doctor` gains a `labels` check (static name heuristic, PASS/WARN/SKIP) reporting suspicious promoted columns and the configured limit.
- [ ] #6 `docs/sources/eventlogfile.md` (per-type routing section) and `docs/sources/index.md#label-cardinality-discipline` document the guard, the default, that it demotes rather than drops, and that the tracker resets on restart.
- [ ] #7 Test: promoting a column with `limit + 1` distinct values keeps the first `limit` promoted, then demotes the column to structured metadata for every subsequent row.
- [ ] #8 Test: demotion is sticky — after tripping, a row carrying an already-seen value is still demoted (no label-set flapping).
- [ ] #9 Test: the counter increments exactly once per tripped `(event_type, label)`, not once per row; the ERROR log is emitted once.
- [ ] #10 Test: `label_value_limit: 0` disables the guard — a column with many distinct values stays promoted and the counter stays at zero.
- [ ] #11 Test: a second event type promoting the same column name is tracked independently (tripping type A does not demote type B).
- [ ] #12 Test: demoted values appear in structured metadata (no data loss) and reserved keys `source`/`event_type` are untouched by the guard.
- [ ] #13 Test: `doctor`'s `labels` check WARNs on `labels: [USER_ID]` and PASSes on an empty `labels`.
- [ ] #14 Test: the same guard applies on the backfill path (`backfill._shape_rows`).
- [ ] #15 `just gate` green.
<!-- AC:END -->

## Definition of Done
<!-- DOD:BEGIN -->
- [ ] #1 just gate is green (ruff check + ruff format --check + mypy src + pytest) — run it, don't assert it
- [ ] #2 just gen-config run and its output committed, if config.py changed (CI drift gate fails otherwise)
- [ ] #3 committed straight to main with a conventional-commit message, and pushed
<!-- DOD:END -->
