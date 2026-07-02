# sf2loki Grafana dashboards & alert pack

Hand-authored Grafana **dashboard schema v2** dashboards and **Grafana-managed**
alert + recording rules for sf2loki. These are committed resources — there is
**no generator** and no drift gate; edit the JSON/YAML directly (ideally with the
`gcx` snapshot loop, see below) and commit the result.

```
dashboards/                         # dashboard.grafana.app/v2 (Kind: Dashboard)
  sf2loki-overview.json             # all sources: volumes, top users/IPs, orgs
  sf2loki-security-access.json      # Login/Logout: failures, geo, TLS, top users
  sf2loki-api-integration.json      # ApiTotalUsage/RestApi/Bulk/Composite + org limits
  sf2loki-apex-performance.json     # ApexCallout + run/CPU/DB-time percentiles, reports
  sf2loki-connector-health.json     # the connector's own OTLP self-observability
rules/
  recording/                        # rules.alerting.grafana.app/v0alpha1 RecordingRule
  alerting/                         # rules.alerting.grafana.app/v0alpha1 AlertRule
```

The four SF-data dashboards query **Loki** (the ingested Salesforce events, parsed
with `| json`); `sf2loki-connector-health.json` queries **Prometheus** (the OTLP
metrics the connector emits about itself).

## Datasources

Dashboards bind to a datasource **template variable** (`ds_loki` and/or `ds_prom`),
so on import you pick your Loki / Prometheus datasource — no UID is baked in. The
alert/recording rules **do** embed datasource UIDs (Grafana-managed rules can't
template them); they use the Grafana Cloud defaults `grafanacloud-logs` /
`grafanacloud-prom`. On self-hosted Grafana, replace those UIDs with your own.

## Metric-name suffixes (read this before the dashboards look empty)

The connector-health dashboard and every alert/recording rule that touches
connector metrics query names with the OpenTelemetry→Prometheus suffixes
`_total` / `_bucket` / `_count` / `_sum` (e.g. `sf2loki_events_ingested_total`,
`sf2loki_ingest_lag_seconds_bucket`). The instruments are created **unsuffixed**
in code (`src/sf2loki/obs/metrics.py`); the suffixes are added when the metrics
are exported to Prometheus.

Grafana Cloud's OTLP endpoint adds them by default. **If you route metrics through
your own OpenTelemetry Collector or Grafana Alloy, keep `add_metric_suffixes`
(a.k.a. `AddMetricSuffixes`) enabled on the Prometheus exporter** — with it off,
the health dashboard and the connector alert rules go silently blank. See the
[main README metrics section](../../README.md#metrics-otlp).

## Applying with gcx

Set your context (`gcx config current-context`), then push. `gcx resources push`
reads **one resource per file**, so the rules are split one-per-file on purpose.

```bash
# folder to hold the dashboards (once)
printf '%s\n' '{"apiVersion":"folder.grafana.app/v1beta1","kind":"Folder","metadata":{"name":"sf2loki"},"spec":{"title":"sf2loki"}}' \
  | gcx resources push -p -

# dashboards
gcx resources validate -p deploy/grafana/dashboards/
gcx resources push     -p deploy/grafana/dashboards/

# alert + recording rules
gcx resources validate -p deploy/grafana/rules/
gcx resources push     -p deploy/grafana/rules/
```

Single dashboard, or manual UI import (Grafana **Dashboards → Import** also
accepts the JSON directly and prompts for the datasource):

```bash
gcx resources push -p deploy/grafana/dashboards/sf2loki-overview.json
```

## Editing (snapshot loop)

Edit the JSON/YAML, then validate → push → snapshot → eyeball → repeat:

```bash
gcx resources validate -p deploy/grafana/dashboards/sf2loki-overview.json
gcx resources push     -p deploy/grafana/dashboards/sf2loki-overview.json
GCX_AGENT_MODE=true gcx dashboards snapshot sf2loki-overview \
  --output-dir ./snapshots --since 24h --var job=sf2loki --width 1920 --theme dark
```

## Conventions baked into the dashboards

- **Scoped JSON extraction** — panels use `| json FIELD="FIELD"`, never bare
  `| json`, which would explode Loki stream cardinality (one series per line).
- **`by (...)` on the range-aggregation** collapses to the series you want; numeric
  fields (`RUN_TIME`, `DB_TOTAL_TIME`, …) are filtered non-empty then `unwrap`ed.
- **Loki's 500-series cap**: very-high-cardinality breakdowns (e.g. per API
  resource) aren't feasible as instant queries — those panels use a
  lower-cardinality dimension (users, families) instead.
- Fixed label allowlist (`job`/`source`/`event_type`/`sf_org_id`/`environment`/
  `org`) — everything else lives in the JSON line body.
