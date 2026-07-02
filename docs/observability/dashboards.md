# Dashboards

sf2loki ships five hand-authored **dashboard-schema-v2** dashboards
(`dashboard.grafana.app/v2`) under
[`deploy/grafana/dashboards/`](https://github.com/rknightion/sf2loki/blob/main/deploy/grafana/dashboards/).
There is no generator and no drift gate for these — edit the JSON directly.

## The dashboard suite

| Dashboard | File | Covers |
|---|---|---|
| Overview | [`sf2loki-overview.json`](https://github.com/rknightion/sf2loki/blob/main/deploy/grafana/dashboards/sf2loki-overview.json) | All sources: event volumes, top users/IPs, orgs |
| Security & access | [`sf2loki-security-access.json`](https://github.com/rknightion/sf2loki/blob/main/deploy/grafana/dashboards/sf2loki-security-access.json) | Login/Logout events: failures, geo, TLS, top users |
| API & integration | [`sf2loki-api-integration.json`](https://github.com/rknightion/sf2loki/blob/main/deploy/grafana/dashboards/sf2loki-api-integration.json) | ApiTotalUsage/RestApi/Bulk/Composite events plus Salesforce org limits |
| Apex performance | [`sf2loki-apex-performance.json`](https://github.com/rknightion/sf2loki/blob/main/deploy/grafana/dashboards/sf2loki-apex-performance.json) | ApexCallout events, run-time/CPU/DB-time percentiles, error reports |
| Connector health | [`sf2loki-connector-health.json`](https://github.com/rknightion/sf2loki/blob/main/deploy/grafana/dashboards/sf2loki-connector-health.json) | The connector's own OTLP self-observability (see [Metrics](metrics.md)) |

The four SF-data dashboards query **Loki** (the ingested Salesforce events);
`sf2loki-connector-health.json` queries **Prometheus** (the OTLP metrics
sf2loki emits about itself).

## Datasource binding

Dashboards bind to a datasource **template variable** (`ds_loki` and/or
`ds_prom`) — no datasource UID is baked in, so on import you pick your own
Loki / Prometheus datasource. This differs from the alert/recording rules
(see [Alerts](alerts.md)), which are Grafana-managed and can't template
datasources — they embed the Grafana Cloud UIDs `grafanacloud-logs` /
`grafanacloud-prom` directly and need editing for self-hosted Grafana.

## Query conventions

- **Scoped JSON extraction** — panels use `| json FIELD="FIELD"`, never bare
  `| json`. A bare `| json` parses every field in the line into a label,
  which explodes Loki stream cardinality (one series per unique combination
  of values); scoped extraction pulls out only the fields a panel needs.
- **`by (...)` on the range aggregation** collapses results to the series
  you actually want to plot; numeric fields (`RUN_TIME`, `DB_TOTAL_TIME`,
  etc.) are filtered non-empty, then `unwrap`ed.
- **Loki's 500-series cap** — very-high-cardinality breakdowns (e.g. per API
  resource) aren't feasible as instant queries; those panels use a
  lower-cardinality dimension (users, families) instead.
- **Fixed label allowlist** — panels only filter/group on
  `job`/`source`/`event_type`/`sf_org_id`/`environment`/`org`; everything
  else lives in the JSON line body and is pulled out with scoped `| json`.
- **Connector-health panels query suffixed metric names** —
  `sf2loki_events_ingested_total`, `sf2loki_ingest_lag_seconds_bucket`, etc.
  See [Metric-name suffixes](metrics.md#metric-name-suffixes).

## Applying with gcx

Set your context (`gcx config current-context`), then push. `gcx resources
push` reads one resource per file.

```bash
# folder to hold the dashboards (once)
printf '%s\n' '{"apiVersion":"folder.grafana.app/v1beta1","kind":"Folder","metadata":{"name":"sf2loki"},"spec":{"title":"sf2loki"}}' \
  | gcx resources push -p -

# dashboards
gcx resources validate -p deploy/grafana/dashboards/
gcx resources push     -p deploy/grafana/dashboards/
```

A single dashboard can be pushed directly, or imported manually via Grafana's
**Dashboards → Import**, which also accepts the JSON and prompts for the
datasource:

```bash
gcx resources push -p deploy/grafana/dashboards/sf2loki-overview.json
```

## Editing (snapshot loop)

Edit the JSON, then validate → push → snapshot → eyeball → repeat:

```bash
gcx resources validate -p deploy/grafana/dashboards/sf2loki-overview.json
gcx resources push     -p deploy/grafana/dashboards/sf2loki-overview.json
GCX_AGENT_MODE=true gcx dashboards snapshot sf2loki-overview \
  --output-dir ./snapshots --since 24h --var job=sf2loki --width 1920 --theme dark
```

See also [Metrics](metrics.md) for the instrument reference behind the
connector-health dashboard, and [Alerts](alerts.md) for the companion
alert/recording rule pack.
