---
id: SFL-0021
title: >-
  obs: ingest-lag alert breaches on every EventLogFile drain - label
  sf2loki_ingest_lag_seconds by source and alert per source
status: To Do
assignee: []
created_date: '2026-08-14 16:58'
labels:
  - followup
  - phase-1
milestone: m-1
dependencies: []
references:
  - 'https://github.com/rknightion/sf2loki/issues/105'
ordinal: 21000
---

## Description

<!-- SECTION:DESCRIPTION:BEGIN -->
## What

`deploy/grafana/rules/alerting/sf2loki-ingest-lag-high.yaml:34` alerts on a p95 taken across **every** event type and **every** source:

```promql
histogram_quantile(0.95, sum by (le)(rate(sf2loki_ingest_lag_seconds_bucket{job="sf2loki"}[5m])))
```

with the threshold `gt 900` at `deploy/grafana/rules/alerting/sf2loki-ingest-lag-high.yaml:48-52`, `for: 10m`, `trigger.interval: 60s`.

That threshold contradicts the instrument's own documented distribution. `src/sf2loki/obs/metrics.py:338-340`:

```
# Histogram (not a gauge): enables p95/p99 lag alerting. Buckets reach 24h
# because EventLogFile lag is legitimately 3-6h — the OTel defaults top out
# at 10000s and would collapse the entire ELF range into +Inf.
```

The bucket boundaries at `src/sf2loki/obs/metrics.py:345-358` run to 86400 for exactly that reason, and `docs/observability/metrics.md:65` restates it. The 3-6h figure is not theoretical: `src/sf2loki/sources/eventlogfile_source.py:770-789` timestamps each `LogEntry` from the CSV row's `timestamp_column` (default `TIMESTAMP_DERIVED`, the event-occurrence time), not the download time, and `src/sf2loki/app.py:311` computes `lag = (datetime.now(UTC) - entry.timestamp).total_seconds()` — so the observed lag is the full Salesforce publication delay. With `interval: Daily` (`src/sf2loki/config.py:668-676`, documented at `docs/sources/eventlogfile.md:33-36` as the option that "works for every org", "~1 day lag") the observations land at or above the top bucket.

There is no way to scope the rule away from EventLogFile, because the histogram carries no `source` label. `src/sf2loki/app.py:312`:

```python
self._metrics.ingest_lag.labels(event_type=event_type).observe(lag)
```

Three lines earlier, the counter next door already carries it (`src/sf2loki/app.py:307-309`: `events_ingested.labels(source=source.name, event_type=event_type)`). `_Histogram.labels` (`src/sf2loki/obs/metrics.py:115`) adds nothing implicit; the org-scoped wrapper (`src/sf2loki/obs/metrics.py:163-167`) adds only `org`. The observation runs for every non-`checkpoint_only` entry from every source, so bulk ELF rows and realtime Pub/Sub rows share one distribution.

The rest of the pack assumes EventLogFile is enabled, so the contradiction is internal to the shipped artifacts rather than hypothetical. `source="eventlogfile"` is pinned in both Loki alert rules (`deploy/grafana/rules/alerting/sf2loki-login-failure-spike.yaml:35`, `deploy/grafana/rules/alerting/sf2loki-apex-callout-error-rate.yaml:35`), all three event-shaped recording rules (`deploy/grafana/rules/recording/sf2loki-rec-login-failures-5m.yaml:28`, `deploy/grafana/rules/recording/sf2loki-rec-apex-callout-errors-5m.yaml:28`, `deploy/grafana/rules/recording/sf2loki-rec-api-usage-5m.yaml:28`), and ~40 panel queries across the four SF-data dashboards (`deploy/grafana/dashboards/sf2loki-security-access.json`, `sf2loki-api-integration.json`, `sf2loki-apex-performance.json`, `sf2loki-overview.json:743`). Push the pack as shipped and EventLogFile is by definition on.

No documentation warns about this. `docs/observability/alerts.md:32` restates the 900s threshold as a bare fact and never cross-references the bucket rationale; `deploy/grafana/README.md` carries only the metric-suffix and cardinality caveats. There is no threshold-tuning note anywhere in `docs/observability/` or the pack README.

## Why it matters

Concrete walk with EventLogFile enabled at the default `interval: Hourly` and `poll_interval: 1h` (`src/sf2loki/config.py:668-698`):

1. Each hourly drain emits rows whose lag is 3-6h into the global distribution.
2. Once ELF rows exceed 5% of the increments in the rule's 5m rate window, the 95th percentile lands in the 3600-21600 bucket range, above 900. That threshold is trivially crossed during a drain: streaming trickles while a drain delivers a whole file's rows in a burst.
3. The rule evaluates every 60s with `for: 10m`. A drain lasting ~5m plus the 5m `rate()` tail yields 10 consecutive breaching evaluations, and the alert fires.
4. Between drains `rate()` sees no ELF increments, p95 collapses back to the streaming range, and the alert resolves. Result: hourly flapping on a completely healthy deployment.

With `interval: Daily` the observations exceed the 86400 boundary and p95 stays pegged at the top of the range for the whole drain, firing daily.

The false negative is the more expensive half. Because the global p95 already sits in the hours whenever ELF is contributing, a genuine Pub/Sub or ApexLog stall moves the aggregate p95 barely at all — the alert cannot distinguish the condition it exists to catch. Operators silence or delete the only latency alert in the pack, and the streaming-lane stall it was meant to catch goes unnoticed.

## Proposed approach

1. Add `source` to the histogram observation at `src/sf2loki/app.py:312`, matching the counter immediately above it:

   ```python
   self._metrics.ingest_lag.labels(source=source.name, event_type=event_type).observe(lag)
   ```

   Cardinality cost is approximately zero: `src/sf2loki/sources/overlap.py:1-15` enforces one event category per source at startup, so `event_type` label values are already partitioned by source. The only case where the label multiplies series is the deliberate `sources.allow_overlap: true` bypass, where it at most doubles them.

2. Split the alert into two rules with per-source thresholds, so each lane is judged against its own realistic distribution:
   - `sf2loki-ingest-lag-high` — scoped to the realtime lanes, `{job="sf2loki", source=~"pubsub|apexlog"}`, threshold 900s, `for: 10m`. Keep the existing `severity: warning` / `service: sf2loki` labels and the `grafanacloud-prom` datasource UID.
   - `sf2loki-ingest-lag-high-bulk` (new file — `gcx resources push` reads one resource per file) — scoped to the bulk lanes, `{job="sf2loki", source=~"eventlogfile|eventlog_objects"}`, threshold above the legitimate ceiling. 8h (28800s, an existing bucket boundary) suits `Hourly`; note in the rule comment and in `docs/observability/alerts.md` that a `Daily` deployment must raise it past ~48h or disable the rule, since `Daily` lag is ~1 day by design.
   - Confirm the exact `source` label values against the `name` property of each source class in `src/sf2loki/sources/` before writing the regexes; `eventlogfile_source.py:783` uses the literal `"eventlogfile"`.

3. Break the connector-health dashboard's lag panels down by `source` so the two populations are visually separable: `deploy/grafana/dashboards/sf2loki-connector-health.json:215` ("Ingest lag p95") and `:572`, `:594`, `:616` ("Ingest lag percentiles") all aggregate globally with `sum by (le)`. Change to `sum by (le, source)` and label the series by source.

4. Document the split. Update the `sf2loki-ingest-lag-high` row in the alert table at `docs/observability/alerts.md:32`, add the new bulk rule's row, and add an explicit note that bulk-source lag is hours by design — cross-referencing `docs/observability/metrics.md:65`. Add the `source` label to the `sf2loki_ingest_lag_seconds` row in the metrics table at `docs/observability/metrics.md:65` (currently "per event type").

5. Add the metric-name/label change to the release notes as a breaking observability change: any operator dashboard or alert reading `sf2loki_ingest_lag_seconds_bucket` gains a new label dimension, and the old single-rule alert is replaced.

---

Imported from GitHub issue #105 on 2026-08-14, when this repo migrated from GitHub Issues to Backlog.md. The original issue has been deleted; its verbatim body, labels and comments are preserved in `archive/issues-dump.json` (`jq '.[] | select(.number == 105)' archive/issues-dump.json`).

<sub>Filed from the 2026-07-30 full-repo audit (11 finder lanes + adversarial verification per finding).</sub>
<!-- SECTION:DESCRIPTION:END -->

## Acceptance Criteria
<!-- AC:BEGIN -->
- [ ] #1 `src/sf2loki/app.py:312` observes `ingest_lag` with both `source` and `event_type`.
- [ ] #2 `tests/obs/test_metrics.py` — extend the existing `test_ingest_lag_histogram` (`tests/obs/test_metrics.py:56-64`) to assert `sf2loki_ingest_lag_seconds_count` is retrievable with the label pair `{source: ..., event_type: ...}`, so the `source` label cannot regress.
- [ ] #3 A pipeline-level test (alongside the existing `_produce` coverage) drives one entry through a fake source named `eventlogfile` and asserts the recorded `ingest_lag` sample carries `source="eventlogfile"` — pins the label at the real call site, not just the instrument.
- [ ] #4 `deploy/grafana/rules/alerting/sf2loki-ingest-lag-high.yaml` selector scoped to the realtime sources, threshold unchanged at 900.
- [ ] #5 `deploy/grafana/rules/alerting/sf2loki-ingest-lag-high-bulk.yaml` exists as its own file, scoped to the bulk sources, threshold at or above 28800, with the `Daily`-interval caveat in a comment.
- [ ] #6 `gcx resources validate -p deploy/grafana/rules/` passes on both rule files.
- [ ] #7 `deploy/grafana/dashboards/sf2loki-connector-health.json` lag panels (lines ~215, ~572, ~594, ~616) aggregate `by (le, source)`; `gcx resources validate -p deploy/grafana/dashboards/sf2loki-connector-health.json` passes.
- [ ] #8 `docs/observability/alerts.md` lists both rules with their thresholds and states that bulk-source lag in the hours is expected, not a fault.
- [ ] #9 `docs/observability/metrics.md:65` records the `source` label on `sf2loki_ingest_lag_seconds`.
- [ ] #10 `just gate` green (ruff + `mypy --strict` + pytest).
<!-- AC:END -->

## Definition of Done
<!-- DOD:BEGIN -->
- [ ] #1 just gate is green (ruff check + ruff format --check + mypy src + pytest) — run it, don't assert it
- [ ] #2 just gen-config run and its output committed, if config.py changed (CI drift gate fails otherwise)
- [ ] #3 committed straight to main with a conventional-commit message, and pushed
<!-- DOD:END -->
