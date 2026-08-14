---
id: SFL-0061
title: >-
  obs: sf2loki_loki_entries_dropped carries only `reason` - attribute drops by
  source/event_type/org
status: To Do
assignee: []
created_date: '2026-08-14 16:58'
labels:
  - followup
  - phase-1
  - roadmap
milestone: m-1
dependencies: []
references:
  - 'https://github.com/rknightion/sf2loki/issues/145'
ordinal: 61000
---

## Description

<!-- SECTION:DESCRIPTION:BEGIN -->
## What

`sf2loki_loki_entries_dropped` is declared at `src/sf2loki/obs/metrics.py:315-320` and incremented at four sites. Every one of them passes `reason` as the sole attribute:

| site | reason value | count |
|---|---|---|
| `src/sf2loki/app.py:445` | `"budget"` (egress-governor exhaustion, drop mode) | `len(real)` |
| `src/sf2loki/app.py:479` | `exc.reason` (`PermanentSinkError` on the whole batch) | `len(batch.entries)` |
| `src/sf2loki/sinks/loki/sink.py:279-281` | `exc.reason` (400/413 split-and-drop of one half) | `len(half.entries)` |
| `src/sf2loki/backfill.py:375` | `exc.reason` (backfill permanent drop) | `len(batch.entries)` |

Every dropped `LogEntry` already carries its identity in `entry.labels`. `source` and `event_type` are per-entry identity labels — so much so that they are reserved against static override (`src/sf2loki/sinks/loki/labels.py:9-14`). In multi-org, `org`, `environment` and `sf_org_id` are injected per entry at `src/sf2loki/sources/org_adapter.py:110-113`. The attribution is present in memory at drop time and thrown away.

The structured logs at the drop sites do not recover it: `src/sf2loki/app.py:480-486`, `src/sf2loki/sinks/loki/sink.py:283-288` and `src/sf2loki/backfill.py:376-381` emit only an entry **count** plus `reason`. Nothing in the process records *which* stream lost data.

This is the last unattributed loss signal on the metric surface. `sf2loki_events_ingested` is per `source`+`event_type` (`src/sf2loki/app.py:307-310`), `sf2loki_ingest_lag_seconds` is per `event_type` (`src/sf2loki/app.py:312-313`), and Pub/Sub decode failures were deliberately made per-topic in #43.

Two adjacent gaps found while confirming this, in scope for the same change:

- **No Grafana coverage at all.** `rg entries_dropped deploy/grafana/` matches nothing: none of the five dashboards (`deploy/grafana/dashboards/*.json`) and none of the eleven rules (`deploy/grafana/rules/{recording,alerting}/`) reference the drop counter. Permanent data loss is currently invisible on the shipped pack even in aggregate.
- **Docs are already wrong.** `docs/observability/metrics.md:61` lists this metric's labels as `(none)`; it has had `reason` since it was introduced.

## Why it matters

A 400 `bad_request` spike (one malformed event shape, one org's oversized rows, one event type whose JSON line trips a Loki limit) currently produces `sf2loki_loki_entries_dropped_total{reason="bad_request"}` climbing and nothing else. The operator's first two questions on a loss event are:

1. Which event type is losing data — is it the compliance-critical `LoginEvent`/`ApiEvent` stream, or noisy `ApexExecution` rows nobody audits?
2. In a multi-org deployment, which org is affected — is this one tenant's data or all of them?

Neither is answerable from metrics. The only path is to correlate the counter's rise against `ERROR`/`WARNING` log lines that themselves carry only a count and a reason, then guess from timing which lane was flushing. That guess decides whether the incident is a compliance-reportable audit-trail gap or a cosmetic one, and it is being made without data.

Because there is no drop panel or alert either, the loss is typically noticed indirectly — a missing-data question from a downstream consumer days later — rather than at the time.

## Proposed approach

Aggregate per-entry at drop time, keyed off the labels the entries already carry.

1. **Add one shared helper** so all four sites behave identically. `src/sf2loki/model.py` imports nothing internal, so `src/sf2loki/obs/metrics.py` can import `LogEntry` without a cycle (verified: `tests/test_seams_import.py` imposes no layering constraint here). Suggested shape, in `obs/metrics.py` next to the instrument:

   ```python
   _DROP_ATTR_KEYS = ("source", "event_type", "org")

   def record_entries_dropped(
       metrics: Metrics, entries: Sequence[LogEntry], reason: str
   ) -> None:
       """Increment the drop counter once per (source, event_type, org) group."""
       groups: dict[tuple[tuple[str, str], ...], int] = defaultdict(int)
       for e in entries:
           key = tuple((k, e.labels[k]) for k in _DROP_ATTR_KEYS if k in e.labels)
           groups[key] += 1
       for key, n in groups.items():
           metrics.loki_entries_dropped.labels(reason=reason, **dict(key)).inc(n)
   ```

   Omit an attribute rather than emitting a placeholder when the label is absent (single-org has no `org`), matching how `org` is absent from single-org Loki streams. Fall back to `event_type="unknown"` only if that mirrors the existing `app.py:307` convention — pick one and state it in the docstring.

2. **Call it at all four sites**, replacing the bare `.labels(reason=...)` calls: `app.py:445` (pass `real`), `app.py:479` (pass `batch.entries`), `sinks/loki/sink.py:279-281` (pass `half.entries`), `backfill.py:375` (pass `batch.entries`). `checkpoint_only` entries are already excluded at every site.

3. **Fix `backfill.py`'s summary read — this is the landmine.** `_dropped_from_metrics` (`src/sf2loki/backfill.py:190-201`) looks the counter up as `get_sample_value("sf2loki_loki_entries_dropped_total", {"reason": reason})`, and `_MetricsView.get_sample_value` (`src/sf2loki/obs/metrics.py:242-247`) matches on the **full sorted attribute tuple** — a partial attribute set returns `None`. Adding attributes without touching this makes the backfill CLI report `rows_dropped=0` on a run that dropped rows. Change it to sum every data point whose `reason` is in `_DROP_REASONS` regardless of the other attributes; that needs either a new `_MetricsView` method (e.g. `sum_samples(name, match: Mapping[str, str])` doing a subset match over `self._cache`) or an explicit iteration over the cache. Prefer the new view method — the exact-match `get_sample_value` semantics are load-bearing for the rest of the test suite and must not change.

4. **Update the existing exact-match assertions** to the new attribute sets: `tests/test_pipeline.py:169-172`, `tests/test_egress.py:434-437`, `tests/sinks/test_sink.py:466`, `tests/sinks/test_sink.py:499`.

5. **Grafana:** add a drop panel to `deploy/grafana/dashboards/sf2loki-connector-health.json` — `sum by (event_type) (rate(sf2loki_loki_entries_dropped_total[$__rate_interval]))`, plus a second query or breakdown by `reason`. Hand-authored dashboard-schema-v2, no generator; validate and snapshot with `gcx` per `deploy/grafana/README.md`. Keep the `_total` suffix (OTLP translation, #58). Optionally add an alerting rule for a sustained non-zero drop rate under `deploy/grafana/rules/alerting/` — one resource per file.

6. **Docs:** correct `docs/observability/metrics.md:61` to list the real attribute set, and update the `reason` references in `docs/architecture.md:58` and `docs/sources/cost-controls.md:33,73` where they describe the metric's shape. `src/sf2loki/config.py:890` and `deploy/helm/values.yaml:611` mention `loki_entries_dropped{reason="budget"}` in prose — leave those unless they read as an exhaustive label list (a `config.py` docstring edit requires `just gen-config`).

**Cardinality is bounded and cheap.** The key space is `sources` (≤4 kinds) x configured `event_types` x `reasons` (`budget`, `bad_request`, `oversized_413`) x `orgs`. OTel counters materialise a time series only when incremented, so a healthy deployment that drops nothing adds zero series; the space only opens up in proportion to actual loss, which is exactly when the detail is wanted. The grouping loop runs on a cold path (a permanent drop or budget exhaustion), never on the per-event hot path — no interaction with the #69 hot-path work.

**Out of scope, worth a follow-up if wanted:** `sf2loki_events_ingested` is not org-attributed, because `OrgSource` preserves the inner source's name verbatim (`src/sf2loki/sources/org_adapter.py:68`, documented at lines 20-21). So a per-org *drop ratio* against ingest still won't be computable after this change. Adding `org` to `events_ingested` is a separate cardinality decision.

---

Imported from GitHub issue #145 on 2026-08-14, when this repo migrated from GitHub Issues to Backlog.md. The original issue has been deleted; its verbatim body, labels and comments are preserved in `archive/issues-dump.json` (`jq '.[] | select(.number == 145)' archive/issues-dump.json`).

<sub>Filed from the 2026-07-30 full-repo audit (11 finder lanes + adversarial verification per finding).</sub>
<!-- SECTION:DESCRIPTION:END -->

## Acceptance Criteria
<!-- AC:BEGIN -->
- [ ] #1 `sf2loki_loki_entries_dropped` is incremented with `source`, `event_type` and (when present) `org` alongside `reason`, at all four sites: `src/sf2loki/app.py:445`, `src/sf2loki/app.py:479`, `src/sf2loki/sinks/loki/sink.py:279-281`, `src/sf2loki/backfill.py:375`.
- [ ] #2 One shared helper does the grouping; no site hand-rolls its own attribute assembly.
- [ ] #3 Test: a mixed batch of entries spanning two `event_type`s under one source hits the pipeline permanent-drop path and produces exactly two counter series with the correct per-group counts (extends `tests/test_pipeline.py:169-172`).
- [ ] #4 Test: the egress-governor budget drop is attributed the same way (extends `tests/test_egress.py:434-437`).
- [ ] #5 Test: the sink's 400/413 split-and-drop path attributes only the poison half's entries, by their own labels (extends `tests/sinks/test_sink.py:466,499`).
- [ ] #6 Test: a multi-org entry (labels including `org`, per `src/sf2loki/sources/org_adapter.py:110-113`) yields a series carrying that `org`; a single-org entry yields a series with no `org` attribute rather than an empty-string one.
- [ ] #7 Test: the backfill `rows_dropped=N` summary is still correct after attributes are added — `tests/test_backfill.py:682-706` stays green, and a new case drops entries spanning two event types and asserts the summed total (guards the `_dropped_from_metrics` exact-match landmine).
- [ ] #8 `get_sample_value`'s exact-attribute-match semantics are unchanged; any subset-matching is a new, separately tested `_MetricsView` method.
- [ ] #9 `deploy/grafana/dashboards/sf2loki-connector-health.json` has a drop panel breaking loss down by `event_type` (and `reason`), validated with `gcx`.
- [ ] #10 `docs/observability/metrics.md:61` lists the metric's actual attributes; `docs/architecture.md:58` and `docs/sources/cost-controls.md:33,73` are consistent with the new shape.
- [ ] #11 `just gate` green (`ruff` + `mypy --strict` + `pytest`), including the config-artifact drift gate if any `config.py` docstring changed.
<!-- AC:END -->

## Definition of Done
<!-- DOD:BEGIN -->
- [ ] #1 just gate is green (ruff check + ruff format --check + mypy src + pytest) — run it, don't assert it
- [ ] #2 just gen-config run and its output committed, if config.py changed (CI drift gate fails otherwise)
- [ ] #3 committed straight to main with a conventional-commit message, and pushed
<!-- DOD:END -->
