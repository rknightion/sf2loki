---
id: SFL-0006
title: >-
  obs: idle/transition-only gauges stop exporting and noDataState: Ok hides it -
  no-recent-push, leader-anomaly and api-limit-low alerts can't be trusted
status: To Do
assignee: []
created_date: '2026-08-14 16:58'
labels:
  - followup
  - phase-1
milestone: m-1
dependencies: []
references:
  - 'https://github.com/rknightion/sf2loki/issues/90'
ordinal: 6000
---

## Description

<!-- SECTION:DESCRIPTION:BEGIN -->
## What

`sf2loki-no-recent-push` — the pack's only total-ingestion-stall pager, labelled `severity: critical` — is structurally incapable of firing. The same defect silently disables `sf2loki-leader-anomaly` and makes `sf2loki-api-limit-low` flappy.

**The rule.** `deploy/grafana/rules/alerting/sf2loki-no-recent-push.yaml:34` runs an instant query:

```promql
time() - max(sf2loki_last_push_success_timestamp_seconds{job="sf2loki"})
```

with threshold `> 600` (:53), `for: 5m` (:16) and `noDataState: Ok` (:17).

**The gauge.** `sf2loki_last_push_success_timestamp_seconds` is created as a **synchronous** OTel gauge at `src/sf2loki/obs/metrics.py:332` (`meter.create_gauge`) and written at exactly one site — `src/sf2loki/app.py:491`, in the success branch of `Pipeline._flush`. There is no heartbeat and no observable-gauge callback.

**Why the series disappears.** A synchronous gauge maps to `_LastValueAggregation` (`opentelemetry/sdk/metrics/_internal/aggregation.py:1299-1303`). Its `collect()` returns `None` when `self._value is None` and **nulls the stored value after every successful collect** (same file, :409-421). `_ViewInstrumentMatch.collect` drops `None` data points and returns `None` for an empty list (`_view_instrument_match.py:137-149`), so the series is absent from every export batch in which `set()` was not called. The repo already documents this behaviour at `src/sf2loki/obs/metrics.py:210-213` ("`get_metrics_data()` is destructive for gauges (a second collect with no new measurement returns nothing)") — that comment exists because the in-memory test shim had to cache values across reads to work around it.

**The arithmetic.** Export cadence is `PeriodicExportingMetricReader` (`src/sf2loki/obs/metrics.py:282-285`) at a 60s default (`src/sf2loki/config.py:1248-1251`). Last successful push at `P` sets the value to `P`. The next collect at `C ∈ (P, P+60]` emits one sample with timestamp `C` (`time_unix_nano=collection_start_nano`, `aggregation.py:424-430`) and value `P`. No further samples are produced. With Prometheus/Mimir's default 5m `lookback-delta`, that sample is selectable only while `t - C <= 300`. Therefore the largest value the expression can ever return is:

```
t - P  <=  lookback(300s) + export_interval(60s)  =  360s
```

`360 < 600`, so the threshold is unreachable while any data exists; the instant the value would exceed 600 the query returns no data, and `noDataState: Ok` maps that to green. A longer lookback does not rescue it either: `for: 5m` requires the `>600` condition to hold across 5 consecutive evaluations, which needs a lookback of at least 15m.

**No compensating coverage.** Sync counters and histograms *do* keep exporting when idle — the OTLP HTTP exporter defaults to `CUMULATIVE` (`opentelemetry/exporter/otlp/proto/common/_internal/metrics_encoder/__init__.py:69-72`) and `_SumAggregation.collect` substitutes `value = 0` and returns `self._previous_value` on an idle interval (`aggregation.py:339-352`). So during a stall `sf2loki_loki_push_total` stays present with `rate() == 0`, making `sf2loki-loki-push-failing` evaluate `0 / clamp_min(0, 0.0001) * 100 = 0` (never `> 5`), and `sf2loki-ingest-lag-high`'s `histogram_quantile` over all-zero rates yields nothing. A stall in which nothing is attempted trips no rule in the pack.

**Same defect class, same pack:**

| Metric | Write sites | Consequence |
|---|---|---|
| `sf2loki_leader` (`src/sf2loki/obs/metrics.py:578`) | only on leadership transitions — `src/sf2loki/app.py:1185`, `:1199`, `:1215` | `sum(sf2loki_leader{job="sf2loki"})` (`sf2loki-leader-anomaly.yaml:34`) goes permanently NoData→Ok ~5-6 min after startup; a leaderless gap (`sum == 0`) can never be observed |
| `sf2loki_build_info` (`src/sf2loki/obs/metrics.py:605-610`) | once, at `Metrics.__init__` | present for one export interval, then gone |
| `sf2loki_salesforce_limit_max` / `_remaining` (`src/sf2loki/obs/metrics.py:586`, `:592`) | `src/sf2loki/obs/limits_poller.py:57-59`, default 5m interval (`src/sf2loki/config.py:129`) | sits exactly on the 5m lookback boundary, so `sf2loki-api-limit-low` flaps between a value and NoData |

Every other `_Gauge` in `src/sf2loki/obs/metrics.py` (`last_replay_commit_ts` :361, `pubsub_stream_up` :402, `watermark_ts` :411, `queue_depth` :458, `egress_paused` :562, `eventlogfile_cycle_seconds` :570, …) shares the property: it is visible only while it is being actively written, which is precisely not the case during the outage it is meant to describe.

**Docs assert the opposite.** `docs/observability/alerts.md:34` states the rule signals "No successful Loki push in the last 10m". The only caveat in that file (`:45-52`) concerns `add_metric_suffixes`, an unrelated mechanism, and `docs/observability/metrics.md:52` explicitly notes gauges are queried unsuffixed — so the existing caveat does not cover this. No test exercises the rule pack.

**Dashboard is affected too.** The `hlth-lastpush` "Time since last push" stat panel (`deploy/grafana/dashboards/sf2loki-connector-health.json:374`, query at `:395`) uses the same bare-gauge expression, so it goes blank ~6 min into an outage and caps displayed values at ~360s.

## Why it matters

The pipeline stalls (poison checkpoint, wedged queue, sink unreachable for hours, process dead). At stall+6m the series ages out of the lookback window, the rule flips to NoData → `Ok`, and stays green for the entire duration of a total ingestion outage. The only critical pager for "data has stopped flowing" is decorative, and no other rule in the pack covers the case. Under active-passive HA, a leaderless gap — both instances standing by, zero ingestion — is likewise unobservable via `sf2loki-leader-anomaly`.

## Proposed approach

Two independent parts. Both are needed: (1) fixes the signal for dashboards and every consumer; (2) makes the rule robust regardless of publisher cadence and adds process-death coverage.

**1. Publish state-shaped gauges continuously (service-side).** Convert the gauges whose meaning is "current state" from synchronous `create_gauge` to `meter.create_observable_gauge` with a callback, so the SDK invokes the callback on every collect and a data point is emitted every export interval. Keep the existing `.set()` / `.labels(...).set()` call sites unchanged by making `_Gauge` / `_BoundGauge` (`src/sf2loki/obs/metrics.py:85-96`) write into a per-attribute-set value dict that the registered callback yields as `Observation`s. Convert at minimum: `last_push_success_ts` (:332), `leader` (:578), `build_info` (:605), `salesforce_limit_max` / `_remaining` (:586, :592), `watermark_ts` (:411), `last_replay_commit_ts` (:361), `pubsub_stream_up` (:402), `egress_paused` (:562), `queue_depth` (:458). Note `ObservableGauge` also uses `_LastValueAggregation`, so continuity comes from the callback producing a fresh measurement at each collect, not from the aggregation retaining state. The `_MetricsView` cache (`src/sf2loki/obs/metrics.py:206-213`) keeps working unchanged.

Alternative if observable gauges are rejected: a background heartbeat task that re-applies the last known value of each of these gauges once per `telemetry.export_interval`. Less idiomatic and it needs its own lifecycle wiring, so prefer the callback form.

**2. Harden the rules so they cannot silently no-op.**

- `deploy/grafana/rules/alerting/sf2loki-no-recent-push.yaml:34` → wrap the gauge in a range vector so a stale-but-recent sample stays selectable past the threshold:
  ```promql
  time() - max(max_over_time(sf2loki_last_push_success_timestamp_seconds{job="sf2loki"}[30m]))
  ```
  and widen `relativeTimeRange.from` to `30m` to match.
- Set `noDataState: Alerting` on `sf2loki-no-recent-push.yaml:17` and `sf2loki-leader-anomaly.yaml:17`. For these two rules, absence of data *is* the failure condition (process dead, OTLP export broken, metrics disabled), so mapping NoData to `Ok` is wrong by construction. Leave `noDataState: Ok` on the Loki-query rules (`sf2loki-login-failure-spike`, `sf2loki-apex-callout-error-rate`) and on the ratio/quantile rules, where NoData legitimately means "no traffic".
- `sf2loki-leader-anomaly.yaml:34` → `sum(max_over_time(sf2loki_leader{job="sf2loki"}[5m]))` so transition-only writes remain visible; keep `for: 5m` so a failover blip does not page.
- `deploy/grafana/dashboards/sf2loki-connector-health.json:395` → same `max_over_time(...[30m])` treatment so the stat panel keeps counting up instead of going blank.
- `docs/observability/alerts.md` and `docs/observability/metrics.md` → document that OTel last-value gauges are only exported in intervals where they are written, that gauge-backed rules must therefore use a range vector, and that `sf2loki-no-recent-push` / `sf2loki-leader-anomaly` deliberately use `noDataState: Alerting`.

---

Imported from GitHub issue #90 on 2026-08-14, when this repo migrated from GitHub Issues to Backlog.md. The original issue has been deleted; its verbatim body, labels and comments are preserved in `archive/issues-dump.json` (`jq '.[] | select(.number == 90)' archive/issues-dump.json`).

## Additional evidence (parallel review lanes)

- `sf2loki_leader` is a transition-only gauge: it is written once on election/demotion and never refreshed, so it is exported for one interval and then goes stale — the shipped `sf2loki-leader-anomaly.yaml` critical alert can therefore never fire hours after the last transition, exactly when a leaderless gap or split-brain would occur. `sf2loki_egress_paused` (src/sf2loki/obs/metrics.py:562-566) is a third transition-only state gauge with the same shape (currently unused by the pack).
- What is NOT broken: per-replica series do exist. `Resource.create` (src/sf2loki/obs/metrics.py:295-296) auto-injects a random-UUID `service.instance.id` (verified live against the repo venv, SDK 1.43.0), which OTLP->Prometheus maps to the `instance` target label — so `sum()` across replicas is structurally fine. The residual gap is that the UUID regenerates on every process start, and instance identity is undocumented (docs/observability/metrics.md:97 lists `sf2loki_leader` labels as "(none)"; deploy/helm/values.yaml:691 defaults `resource_attributes: {}`).
- Docs drift in the same area: docs/deployment/high-availability.md:114-118 still says there is no dedicated alert on `sf2loki_leader` cardinality in the pack, contradicting the shipped sf2loki-leader-anomaly.yaml.

<sub>Filed from the 2026-07-30 full-repo audit (11 finder lanes + adversarial verification per finding).</sub>
<!-- SECTION:DESCRIPTION:END -->

## Acceptance Criteria
<!-- AC:BEGIN -->
- [ ] #1 `sf2loki_last_push_success_timestamp_seconds`, `sf2loki_leader`, `sf2loki_build_info`, `sf2loki_salesforce_limit_max`, `sf2loki_salesforce_limit_remaining`, `sf2loki_watermark_timestamp_seconds`, `sf2loki_last_replay_commit_timestamp_seconds`, `sf2loki_pubsub_stream_up`, `sf2loki_egress_paused` and `sf2loki_queue_depth` emit a data point on **every** collect after their first write, with no intervening measurement.
- [ ] #2 Test in `tests/obs/` (new file, e.g. `test_metrics_gauge_continuity.py`): set each converted gauge once, call `reader.get_metrics_data()` **twice** against a fresh `InMemoryMetricReader`, and assert the gauge's data point is present in both results with the same value. This test must fail against the current `create_gauge` implementation (that is the regression it pins).
- [ ] #3 Test that a labelled gauge (`salesforce_limit_remaining.labels(limit_name="DailyApiRequests").set(...)`) keeps its attribute set across repeated collects, and that a second `.set()` with a new value replaces rather than duplicates the data point.
- [ ] #4 Test that a gauge never written emits no data point (no spurious zero series).
- [ ] #5 `deploy/grafana/rules/alerting/sf2loki-no-recent-push.yaml` uses `max_over_time(...[30m])`, `relativeTimeRange.from: 30m`, and `noDataState: Alerting`.
- [ ] #6 `deploy/grafana/rules/alerting/sf2loki-leader-anomaly.yaml` uses `max_over_time(...[5m])` and `noDataState: Alerting`.
- [ ] #7 `deploy/grafana/dashboards/sf2loki-connector-health.json` panel `hlth-lastpush` uses the `max_over_time` form and keeps counting past 360s during a simulated stall.
- [ ] #8 New rule-pack lint test (e.g. `tests/test_grafana_rule_pack.py`): parse every YAML under `deploy/grafana/rules/alerting/`, assert each file is a single `AlertRule` document, and assert that any rule whose failure mode is absence of data (allowlist by `metadata.name`: `sf2loki-no-recent-push`, `sf2loki-leader-anomaly`) has `noDataState: Alerting` and that its `expr` references the gauge only inside a range-vector selector. This is currently untested — `rg -l grafana tests` matches only `test_config_artifacts_drift.py` and `test_config.py`.
- [ ] #9 `gcx resources validate -p deploy/grafana/rules/` passes on the edited pack.
- [ ] #10 `docs/observability/alerts.md` and `docs/observability/metrics.md` state the last-value-gauge export semantics, the range-vector requirement for gauge-backed rules, and the deliberate `noDataState: Alerting` choice with its reason.
- [ ] #11 `just gate` green.
<!-- AC:END -->

## Definition of Done
<!-- DOD:BEGIN -->
- [ ] #1 just gate is green (ruff check + ruff format --check + mypy src + pytest) — run it, don't assert it
- [ ] #2 just gen-config run and its output committed, if config.py changed (CI drift gate fails otherwise)
- [ ] #3 committed straight to main with a conventional-commit message, and pushed
<!-- DOD:END -->
