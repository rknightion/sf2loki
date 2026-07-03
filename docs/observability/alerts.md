# Alerts

sf2loki ships a hand-authored **Grafana-managed** alert + recording rule
pack under
[`deploy/grafana/rules/`](https://github.com/rknightion/sf2loki/blob/main/deploy/grafana/rules/)
(`rules.alerting.grafana.app/v0alpha1`) — `recording/` and `alerting/`, one
resource per file. There is no generator; edit the YAML directly.

Only `severity` and `service` labels are set on every rule — routing to a
contact point is left to your notification policy.

## Recording rules

These evaluate a LogQL query against Loki every 60s and record the result as
a Prometheus series (via `targetDatasourceUID: grafanacloud-prom`), so
dashboards and alerts can read a cheap metric instead of re-scanning logs.

| Rule | Recorded metric | Source query |
|---|---|---|
| `sf2loki-rec-login-failures-5m` | `sf2loki_login_failures:count5m` | Failed `Login` events (`LOGIN_STATUS` ≠ `LOGIN_NO_ERROR`), 5m |
| `sf2loki-rec-apex-callout-errors-5m` | `sf2loki_apex_callout_errors:count5m` | `ApexCallout` events with `SUCCESS="0"`, 5m |
| `sf2loki-rec-events-5m` | `sf2loki_events:count5m` | All events, `by (source, event_type)`, 5m |
| `sf2loki-rec-api-usage-5m` | `sf2loki_api_usage:count5m` | `ApiTotalUsage` events, `by (API_FAMILY)`, 5m |

## Alert rules

| Rule | Severity | Signal | Datasource |
|---|---|---|---|
| `sf2loki-login-failure-spike` | warning | More than 10 failed Salesforce logins in the last 10m | Loki (`grafanacloud-logs`) |
| `sf2loki-apex-callout-error-rate` | warning | `ApexCallout` error rate above 10% over the last 10m | Loki (`grafanacloud-logs`) |
| `sf2loki-api-limit-low` | critical | Lowest Salesforce org-limit headroom below 10% (`sf2loki_salesforce_limit_remaining` / `sf2loki_salesforce_limit_max`) | Prometheus/OTLP (`grafanacloud-prom`) |
| `sf2loki-ingest-lag-high` | warning | p95 ingest lag above 15m (900s), sustained 10m (`sf2loki_ingest_lag_seconds_bucket`) | Prometheus/OTLP (`grafanacloud-prom`) |
| `sf2loki-loki-push-failing` | critical | Loki push failure rate above 5% over 5m (`sf2loki_loki_push_total`) | Prometheus/OTLP (`grafanacloud-prom`) |
| `sf2loki-no-recent-push` | critical | No successful Loki push in the last 10m (`sf2loki_last_push_success_timestamp_seconds`) | Prometheus/OTLP (`grafanacloud-prom`) |
| `sf2loki-leader-anomaly` | critical | Active-leader count `sum(sf2loki_leader)` not exactly 1 — 0 = leaderless gap, 2+ = split-brain (`sf2loki_leader`) | Prometheus/OTLP (`grafanacloud-prom`) |

`sf2loki-login-failure-spike` and `sf2loki-apex-callout-error-rate` query
Loki directly; the other connector-health alerts read the metrics
documented in [Metrics](metrics.md), via the companion
[`sf2loki-connector-health.json`](dashboards.md#the-dashboard-suite)
dashboard.

!!! warning "Connector-metric alerts need suffixed names + `add_metric_suffixes`"
    `sf2loki-api-limit-low`, `sf2loki-ingest-lag-high`, `sf2loki-loki-push-failing`,
    and `sf2loki-no-recent-push` query the OpenTelemetry→Prometheus **suffixed**
    metric names (`sf2loki_loki_push_total`, `sf2loki_ingest_lag_seconds_bucket`, …).
    If you route metrics through your own Collector or Grafana Alloy instead of
    Grafana Cloud's OTLP endpoint, `add_metric_suffixes` must stay enabled or
    these rules go permanently `NoData` (mapped to `Ok` by `noDataState: Ok`,
    so they fail silently rather than firing). See
    [Metric-name suffixes](metrics.md#metric-name-suffixes).

## Datasource UIDs

Grafana-managed rules can't template datasources, so every rule embeds a
UID directly: `grafanacloud-logs` for Loki, `grafanacloud-prom` for
Prometheus — the Grafana Cloud defaults. On self-hosted Grafana, replace
both UIDs in each YAML file with your own before pushing.

## Applying with gcx

```bash
gcx resources validate -p deploy/grafana/rules/
gcx resources push     -p deploy/grafana/rules/
```

## Editing

Copy an existing file, change `metadata.name` and `spec.title`, and adjust
the `expressions` map — the alerting condition is the leaf `threshold`
expression (`C`) evaluated against the query result (`A`). Keep the
datasource-UID and metric-suffix caveats above in mind. Re-validate and push
after any change.

## When an alert fires

If a checkpoint is the poison record blocking the pipeline queue (a likely
cause behind `sf2loki-no-recent-push` or `sf2loki-ingest-lag-high`), see
[State & checkpoints](../deployment/state.md) for how to inspect and advance
it with `sf2loki state`. For active-passive deployments, `sf2loki_leader`
(see [Metrics](metrics.md)) shows which instance currently holds the lease —
check [High availability](../deployment/high-availability.md) if pushes stop
after a failover.
