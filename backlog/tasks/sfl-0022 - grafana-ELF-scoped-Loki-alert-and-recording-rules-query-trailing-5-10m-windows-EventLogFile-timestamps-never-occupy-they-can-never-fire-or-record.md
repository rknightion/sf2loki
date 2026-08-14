---
id: SFL-0022
title: >-
  grafana: ELF-scoped Loki alert and recording rules query trailing 5-10m
  windows EventLogFile timestamps never occupy - they can never fire or record
status: To Do
assignee: []
created_date: '2026-08-14 16:58'
labels:
  - followup
  - phase-2
milestone: m-1
dependencies: []
references:
  - 'https://github.com/rknightion/sf2loki/issues/106'
ordinal: 22000
---

## Description

<!-- SECTION:DESCRIPTION:BEGIN -->
## What

All four Grafana-managed recording rules under `deploy/grafana/rules/recording/` evaluate an **instant** LogQL query with a hardcoded trailing `[5m]` range, anchored at evaluation time, every 60s:

| File | Recorded metric | Selector |
|---|---|---|
| `sf2loki-rec-login-failures-5m.yaml:28` | `sf2loki_login_failures:count5m` | `event_type="Login", source="eventlogfile"` |
| `sf2loki-rec-apex-callout-errors-5m.yaml:28` | `sf2loki_apex_callout_errors:count5m` | `event_type="ApexCallout", source="eventlogfile"` |
| `sf2loki-rec-api-usage-5m.yaml:28` | `sf2loki_api_usage:count5m` | `event_type="ApiTotalUsage", source="eventlogfile"` |
| `sf2loki-rec-events-5m.yaml:28` | `sf2loki_events:count5m` | `{job="sf2loki"}` (no source filter) |

Each rule sets `queryType: instant` with `relativeTimeRange: from: 5m / to: 0s` and `trigger.interval: 60s` (`sf2loki-rec-login-failures-5m.yaml:16-25`). The LogQL `[5m]` range is therefore evaluated at the rule's evaluation time, covering log lines whose **entry timestamp** lies in `[now-5m, now]`.

EventLogFile rows are stored in Loki at their own event time, hours in the past — not at ingestion time:

- `eventlogfile_source.py:770-775` extracts the per-row timestamp via `extract_timestamp_checked(row, field_names=(self._cfg.timestamp_column, "TIMESTAMP"))`, where `timestamp_column` defaults to `TIMESTAMP_DERIVED` (`config.py:711-713`), and passes it straight into `LogEntry(timestamp=ts, ...)`.
- `shaping.py:225-231` returns a successfully parsed field verbatim (`return parsed, False`). The `_MAX_FALLBACK_AGE` / `_FALLBACK_CLAMP_MARGIN` clamp at `shaping.py:236-237` is reachable **only** when the parse loop falls through — it applies to unparseable rows, never to a normal row. There is no clamp toward now on the happy path.
- The lag magnitude is documented in the codebase: `obs/metrics.py:339` sizes the `sf2loki_ingest_lag_seconds` buckets to 24h "because EventLogFile lag is legitimately 3-6h", mirrored at `docs/observability/metrics.md:65`. `config.py:669-672` describes `interval: Daily` as "~1d lag". Even under `interval: Hourly` with the 5m settle window (`config.py:711-717`), Salesforce only publishes a blob after its hour closes, so the freshest row is well over 5m old.

ELF rows also form their own Loki stream via the `source="eventlogfile"` label (`eventlogfile_source.py:782`), so the out-of-order acceptance window described at `shaping.py:198-202` is measured against other ELF rows and does not shift these timestamps toward now.

Consequence, for the three ELF-scoped rules: the `[now-5m, now]` window contains zero ELF lines at every evaluation, forever. `sf2loki_login_failures:count5m`, `sf2loki_apex_callout_errors:count5m` and `sf2loki_api_usage:count5m` are never written with data.

`sf2loki-rec-events-5m.yaml:28` has no `source` filter, so it does record Pub/Sub and ApexLog volume — those sources timestamp from near-real-time fields (`pubsub_source.py:770-773` uses `EventDate` / `CreatedDate` / `ChangeEventHeader.commitTimestamp`). But its `by (source, event_type)` output structurally omits every ELF row while `docs/observability/alerts.md:20-23` presents it as "All events".

The intended pattern is visible in the dashboards, which scope ELF panels to the operator's selected range and therefore work: `sf2loki-security-access.json` uses `sum by (LOGIN_STATUS)(count_over_time({job="$job", event_type="Login", source="eventlogfile"} | json LOGIN_STATUS="LOGIN_STATUS" [$__range]))`, and `sf2loki-api-integration.json` uses `[$__auto]` / `[$__range]` throughout. The hardcoded trailing `[5m]` in the recording rules is the outlier.

Two alert rules share the same defect with a `[10m]` window and are in scope for the same fix: `sf2loki-login-failure-spike.yaml:31-35` and `sf2loki-apex-callout-error-rate.yaml:31-35`, both selecting `source="eventlogfile"`. Their `noDataState` behaviour means they fail silent rather than firing.

## Why it matters

`docs/observability/alerts.md:14-16` advertises these metrics as the cheap feed for dashboards and alerts: "so dashboards and alerts can read a cheap metric instead of re-scanning logs". An operator who follows that guidance and builds a login-failure panel, an Apex-error alert, or an API-usage graph on the documented metric names gets a permanently empty result, with no error to diagnose — a recording rule that matches no lines writes no series rather than failing.

For the two Loki-backed alert rules the direction of failure is worse: a login-failure spike or an Apex callout error rate breach can never fire, so the pack provides silent false assurance on exactly the two security/reliability signals it claims to cover.

The four rules also bill Loki query load plus Grafana rule-evaluation quota every 60s indefinitely for data that is empty (three) or systematically undercounted (one).

`grep -rn "count5m"` currently returns only the four defining YAMLs and the docs table, so nothing shipped reads these metrics yet — the cost today is the wasted evaluation and the misleading documentation, not a broken dashboard.

## Proposed approach

1. Re-range the three ELF-scoped recording rules to a window that covers ELF delivery latency. `[6h]` matches the 3-6h figure already documented at `obs/metrics.py:339`; set `relativeTimeRange.from: 6h` to match the LogQL range, and rename each rule and metric to carry the real window (`sf2loki-rec-login-failures-6h.yaml` → `sf2loki_login_failures:count6h`, and likewise for `apex_callout_errors` and `api_usage`) so the name cannot be mistaken for a 5m rate. Keep `trigger.interval: 60s`.
2. Apply the same widening to `sf2loki-login-failure-spike.yaml:31-35` and `sf2loki-apex-callout-error-rate.yaml:31-35` (`[10m]` → a window covering ELF lag, `relativeTimeRange.from` matched), and re-tune each `threshold` for the wider window — a "more than 10 failed logins" threshold calibrated for 10m is not meaningful over 6h.
3. For `sf2loki-rec-events-5m.yaml`, choose one: either add `source!="eventlogfile"` to the selector and rename the metric to state it covers streaming sources only, or widen it the same way as the others. Do not leave a `by (source, ...)` series documented as "All events" while excluding the highest-volume source.
4. Add a `!!! warning` block to `docs/observability/alerts.md` in the Recording rules section (after line 16) stating that any Loki-backed rule selecting `source="eventlogfile"` must use a window wider than the ELF delivery lag, cross-referencing `docs/observability/metrics.md:65`. Update the rule/metric names in the tables at `docs/observability/alerts.md:20-23` and the alert table entries for the two widened alert rules.
5. Add a test that pins the invariant, since the pack has no generator and no drift gate today (`tests/` contains no coverage of `deploy/grafana/rules`). A YAML-parsing test is sufficient: walk every file under `deploy/grafana/rules/{recording,alerting}/`, and for any expression whose selector contains `source="eventlogfile"`, assert the LogQL range duration and `relativeTimeRange.from` both parse to at least 6h and agree with each other. This catches the whole class rather than the four current instances.

---

Imported from GitHub issue #106 on 2026-08-14, when this repo migrated from GitHub Issues to Backlog.md. The original issue has been deleted; its verbatim body, labels and comments are preserved in `archive/issues-dump.json` (`jq '.[] | select(.number == 106)' archive/issues-dump.json`).

## Additional evidence (parallel review lanes)

- Both alert rules set `noDataState: Ok` (deploy/grafana/rules/alerting/sf2loki-apex-callout-error-rate.yaml:17, sf2loki-login-failure-spike.yaml:17), so the permanently-empty result resolves to Normal instead of surfacing as NoData; for the error-rate rule an empty numerator over an empty denominator yields an empty vector, so the threshold expression never evaluates at all.
- The lag is structural, not tunable: ELF `poll_interval` defaults to 1h (src/sf2loki/config.py:696-699), `settle_window` skips files newer than now-5m for Hourly (src/sf2loki/config.py:710-718), and Salesforce generates the files hours after the events they contain; `source="eventlogfile"` is emitted only by the ELF source (src/sf2loki/sources/eventlogfile_source.py:781-785), so no fresher path can ever populate these selectors.
- `sf2loki-rec-events-5m` is the partial case: it pins no `source`, so it records real values for Pub/Sub and ApexLog series and a permanent 0 for every EventLogFile series, while docs/observability/alerts.md:20-23 presents it as "All events".
- Nothing tests the rule pack (tests/test_config_artifacts_drift.py:4-7 records dashboards/rules as hand-authored with no drift gate), and docs/observability/alerts.md:29-30 documents the 10m semantics as intended.
- The mirror-image defect from the same root cause — the Prometheus-side ingest-lag alert firing permanently once ELF is enabled because `sf2loki_ingest_lag_seconds` is unlabelled by source — is tracked separately in #105.

<sub>Filed from the 2026-07-30 full-repo audit (11 finder lanes + adversarial verification per finding).</sub>
<!-- SECTION:DESCRIPTION:END -->

## Acceptance Criteria
<!-- AC:BEGIN -->
- [ ] #1 `sf2loki-rec-login-failures-*.yaml`, `sf2loki-rec-apex-callout-errors-*.yaml` and `sf2loki-rec-api-usage-*.yaml` use a LogQL range of at least 6h with a matching `relativeTimeRange.from`, and their `spec.metric` names state the actual window
- [ ] #2 `sf2loki-login-failure-spike.yaml` and `sf2loki-apex-callout-error-rate.yaml` use a window covering ELF delivery lag, with thresholds re-tuned for that window
- [ ] #3 `sf2loki-rec-events-5m.yaml` either excludes `source="eventlogfile"` and is renamed to say so, or is widened to cover ELF
- [ ] #4 New test asserts that every Loki-backed rule selecting `source="eventlogfile"` has a LogQL range >= 6h and that its `relativeTimeRange.from` matches that range; test fails when the range is reverted to `[5m]`/`[10m]`
- [ ] #5 New test walks both `recording/` and `alerting/` so a future ELF-scoped rule with a short window is caught on the same gate
- [ ] #6 `docs/observability/alerts.md` carries the ELF-lag warning in the Recording rules section, and the rule/metric tables at `docs/observability/alerts.md:20-23` plus the alert table match the renamed rules
- [ ] #7 `just gate` green
- [ ] #8 `gcx resources validate -p deploy/grafana/rules/` passes on the edited pack
<!-- AC:END -->

## Definition of Done
<!-- DOD:BEGIN -->
- [ ] #1 just gate is green (ruff check + ruff format --check + mypy src + pytest) — run it, don't assert it
- [ ] #2 just gen-config run and its output committed, if config.py changed (CI drift gate fails otherwise)
- [ ] #3 committed straight to main with a conventional-commit message, and pushed
<!-- DOD:END -->
