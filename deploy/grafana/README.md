# sf2loki Grafana dashboard

`sf2loki-dashboard.json` monitors both the Salesforce Event Monitoring -> Loki data pipeline
(events ingested, ingest lag SLI, EventLogFile row throughput, Salesforce org API limits) and the
sf2loki connector's own
self-observability (Loki push outcomes/latency, internal queue depth, auth refreshes/errors,
Pub/Sub credits/reconnects, decode errors, replay/watermark staleness, build info). It uses a
`Prometheus datasource` template variable plus a `$job` variable (defaults to `sf2loki`) so every
panel is portable across environments.

To import: either run `gcx dashboards push deploy/grafana/sf2loki-dashboard.json` (or the
equivalent `gcx dashboards` import command for your target stack), or use Grafana's manual
**Dashboards > Import** flow and upload the JSON file directly — Grafana will prompt for the
Prometheus datasource to bind to `DS_PROMETHEUS` on import.

To regenerate after editing panels/queries, run `python deploy/grafana/gen_dashboard.py` from the
repo root and commit the resulting `sf2loki-dashboard.json`.
