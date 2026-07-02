# Grafana alert & recording rule pack

The rules under [`deploy/grafana/rules/`](../deploy/grafana/rules/) are
**Grafana-managed** resources (`rules.alerting.grafana.app/v0alpha1`), hand-authored
and committed — there is no generator. `gcx resources push` reads one resource per
file, so each rule is its own file, split into `recording/` and `alerting/`.

Apply them with `gcx` (see [`deploy/grafana/README.md`](../deploy/grafana/README.md)):

```bash
gcx resources validate -p deploy/grafana/rules/
gcx resources push     -p deploy/grafana/rules/
```

Rules embed datasource UIDs (Grafana-managed rules can't template them) using the
Grafana Cloud defaults `grafanacloud-logs` / `grafanacloud-prom`; on self-hosted
Grafana, replace those UIDs. The connector-metric alerts depend on the OTLP
metric-name suffixes — see the [suffix note](../deploy/grafana/README.md#metric-name-suffixes-read-this-before-the-dashboards-look-empty).

## Recording rules (LogQL → metric)

These evaluate a LogQL query every 60s and record the result as a Prometheus
series, so dashboards and alerts can read a cheap metric instead of re-scanning
logs.

| Recorded metric | Source query |
|---|---|
| `sf2loki_login_failures:count5m` | failed `Login` events (LOGIN_STATUS ≠ LOGIN_NO_ERROR), 5m |
| `sf2loki_apex_callout_errors:count5m` | `ApexCallout` with SUCCESS=0, 5m |
| `sf2loki_events:count5m` | all events `by (source, event_type)`, 5m |
| `sf2loki_api_usage:count5m` | `ApiTotalUsage` `by (API_FAMILY)`, 5m |

## Alert rules

Only `severity` / `service` labels are set — routing is left to your contact points
and notification policies.

| Alert | Severity | Signal | Datasource |
|---|---|---|---|
| sf2loki login failure spike | warning | >10 failed Salesforce logins in 10m | Loki |
| sf2loki Apex callout error rate high | warning | ApexCallout error rate >10% over 10m | Loki |
| sf2loki Salesforce API limit low | critical | lowest org-limit headroom <10% | Prometheus (OTLP) |
| sf2loki ingest lag high | warning | p95 ingest lag >15m for 10m | Prometheus (OTLP) |
| sf2loki Loki push failing | critical | Loki push failure rate >5% over 5m | Prometheus (OTLP) |
| sf2loki no recent Loki push | critical | no successful push in 10m | Prometheus (OTLP) |

For the connector-health alerts, `sf2loki-connector-health.json` is the companion
dashboard. When a checkpoint is the poison record blocking the queue, see
[`state-runbook.md`](state-runbook.md) for how to inspect/advance it with
`sf2loki state`.

## Editing

Edit the YAML directly and re-validate/push. To add a rule, copy an existing file,
change `metadata.name` and `spec.title`, and adjust the `expressions` map — the
alerting condition is the leaf expression (a `threshold` on the query refId). Keep
the datasource-UID and suffix caveats above in mind.
