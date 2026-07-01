#!/usr/bin/env python3
"""Generator for the sf2loki Grafana dashboard (classic dashboard JSON schema).

sf2loki ships Salesforce Event Monitoring data (real-time Pub/Sub events +
EventLogFile batch rows) into Grafana Loki, and pushes its own metrics via
OTLP for connector self-observability. This dashboard covers Salesforce
Event Monitoring (ingestion volume, org API limits) PLUS the connector's
own health (auth, queue, Pub/Sub, decode errors, replay/watermark
staleness) — visualising the agent shipping the data itself, not just the
data it ships.

Schema: classic Grafana dashboard JSON (schemaVersion 39), NOT the newer
v2 manifest/grafana-foundation-sdk schema used by some other repos in this
account — chosen here deliberately so the JSON is a plain, portable,
import-anywhere artifact (Grafana OSS, Grafana Cloud, any version that
still reads schemaVersion 39, which all current Grafana releases do).
Carries a `__inputs` block + a `DS_PROMETHEUS` templated datasource so
`gcx dashboards push` / manual "Import" both prompt for the target
Prometheus datasource rather than hardcoding a UID.

No external deps (no grafanalib / grafana-foundation-sdk) — pure dict +
json.dump, following the house pattern of build_selfobs_dashboard.py
(synthkit) and self-obs/gen_dashboard.py (genai-otel-bridge): small
panel()/q() builder helpers, hand-assembled grid layout, committed output.

Run:  python3 deploy/grafana/gen_dashboard.py
Emits: deploy/grafana/sf2loki-dashboard.json
"""

from __future__ import annotations

import json
import os

OUT_PATH = os.path.join(os.path.dirname(__file__), "sf2loki-dashboard.json")

# Datasource reference used by every panel/template query — resolved via the
# DS_PROMETHEUS input at import time (classic "templated datasource" pattern).
DS = {"type": "prometheus", "uid": "${DS_PROMETHEUS}"}

JOB = '{job=~"$job"}'  # appended inline into label selectors below

_id = [0]


def nid() -> int:
    _id[0] += 1
    return _id[0]


def target(
    expr: str,
    ref: str = "A",
    legend: str | None = None,
    fmt: str = "time_series",
    instant: bool = False,
) -> dict:
    """One Prometheus query target."""
    t = {
        "datasource": DS,
        "expr": expr,
        "refId": ref,
        "format": fmt,
    }
    if legend is not None:
        t["legendFormat"] = legend
    if instant:
        t["instant"] = True
        t["range"] = False
    else:
        t["range"] = True
    return t


def panel(
    title: str,
    viz: str,
    targets: list[dict],
    *,
    x: int,
    y: int,
    w: int,
    h: int,
    unit: str = "short",
    desc: str = "",
    legend_calcs: list[str] | None = None,
    thresholds: list[dict] | None = None,
    stacking: str | None = None,
    overrides: list[dict] | None = None,
    options_extra: dict | None = None,
    decimals: int | None = None,
    mappings: list[dict] | None = None,
) -> dict:
    """Build one classic-schema panel dict with gridPos."""
    field_defaults: dict = {"unit": unit}
    if decimals is not None:
        field_defaults["decimals"] = decimals
    if mappings:
        field_defaults["mappings"] = mappings
    if thresholds:
        field_defaults["thresholds"] = {"mode": "absolute", "steps": thresholds}
        field_defaults["color"] = {"mode": "thresholds"}
    else:
        field_defaults["color"] = {"mode": "palette-classic"}

    custom: dict = {}
    if viz == "timeseries":
        custom = {
            "drawStyle": "line",
            "lineWidth": 1,
            "fillOpacity": 12,
            "showPoints": "never",
            "spanNulls": False,
        }
        if stacking:
            custom["stacking"] = {"mode": stacking, "group": "A"}
        field_defaults["custom"] = custom
    elif viz == "state-timeline":
        field_defaults["custom"] = {"fillOpacity": 70, "lineWidth": 0, "spanNulls": False}

    options: dict = {}
    if viz == "timeseries":
        options = {
            "legend": {
                "displayMode": "table",
                "placement": "bottom",
                "showLegend": True,
                "calcs": legend_calcs or ["lastNotNull", "max"],
            },
            "tooltip": {"mode": "multi", "sort": "desc"},
        }
    elif viz == "stat":
        options = {
            "reduceOptions": {"calcs": ["lastNotNull"], "values": False},
            "orientation": "auto",
            "colorMode": "value",
            "graphMode": "area",
            "justifyMode": "auto",
            "textMode": "auto",
        }
    elif viz == "bargauge":
        options = {
            "reduceOptions": {"calcs": ["lastNotNull"], "values": False},
            "orientation": "horizontal",
            "displayMode": "gradient",
        }
    elif viz == "state-timeline":
        options = {
            "showValue": "never",
            "rowHeight": 0.85,
            "mergeValues": True,
            "alignValue": "left",
            "legend": {"displayMode": "list", "placement": "bottom", "showLegend": True},
            "tooltip": {"mode": "single", "sort": "none"},
        }
    elif viz == "table":
        options = {"showHeader": True, "cellHeight": "sm"}
    if options_extra:
        options.update(options_extra)

    p = {
        "id": nid(),
        "title": title,
        "description": desc,
        "type": viz,
        "datasource": DS,
        "gridPos": {"x": x, "y": y, "w": w, "h": h},
        "targets": targets,
        "fieldConfig": {"defaults": field_defaults, "overrides": overrides or []},
        "options": options,
    }
    return p


def row(title: str, y: int) -> dict:
    """A row panel (collapsible section header) at the given y offset."""
    return {
        "id": nid(),
        "title": title,
        "type": "row",
        "collapsed": False,
        "gridPos": {"x": 0, "y": y, "w": 24, "h": 1},
        "panels": [],
    }


def thr(steps: list[tuple[float | None, str]]) -> list[dict]:
    return [{"value": v, "color": c} for v, c in steps]


GREEN_YELLOW_RED = thr([(None, "green"), (70, "yellow"), (90, "red")])


# =============================================================================
# ROW 1 — Salesforce ingestion
# =============================================================================
panels: list[dict] = []
y = 0

panels.append(row("Salesforce ingestion", y))
y += 1

panels.append(
    panel(
        "Events ingested rate by event type",
        "timeseries",
        [
            target(
                f"sum by (event_type) (rate(sf2loki_events_ingested_total{JOB}[5m]))",
                legend="{{event_type}}",
            )
        ],
        x=0,
        y=y,
        w=12,
        h=8,
        unit="ops",
        desc="Real-time Pub/Sub event ingestion rate, split by Salesforce event monitoring event_type "
        "(e.g. ApiEvent, LoginEvent, ReportEvent). The headline ingestion-volume signal, showing "
        "event volume by type.",
    )
)
panels.append(
    panel(
        "Events ingested rate by source",
        "timeseries",
        [
            target(
                f"sum by (source) (rate(sf2loki_events_ingested_total{JOB}[5m]))",
                legend="{{source}}",
            )
        ],
        x=12,
        y=y,
        w=12,
        h=8,
        unit="ops",
        desc="Same ingestion rate, split by source connector (e.g. pubsub real-time stream vs eventlogfile "
        "batch poller) — shows which ingestion path is contributing volume.",
    )
)
y += 8

panels.append(
    panel(
        "Ingest lag (SLI) by event type",
        "timeseries",
        [target(f"max by (event_type) (sf2loki_ingest_lag_seconds{JOB})", legend="{{event_type}}")],
        x=0,
        y=y,
        w=12,
        h=8,
        unit="s",
        desc="Difference between ingest time and the event's Salesforce EventDate, per event_type — the "
        "primary SLI for pipeline freshness. Sustained growth means the connector is falling behind "
        "the Salesforce event stream.",
        thresholds=thr([(None, "green"), (60, "yellow"), (300, "red")]),
    )
)
panels.append(
    panel(
        "ELF rows ingested rate",
        "timeseries",
        [
            target(
                f"sum by (event_type) (rate(sf2loki_eventlogfile_rows_ingested_total{JOB}[5m]))",
                legend="{{event_type}}",
            )
        ],
        x=12,
        y=y,
        w=12,
        h=8,
        unit="ops",
        desc="EventLogFile (batch hourly/daily CSV export) ingestion rate, by event_type. Complements the "
        "real-time Pub/Sub row above for event types only available via ELF.",
    )
)
y += 8


# =============================================================================
# ROW 2 — Salesforce org limits
# =============================================================================
panels.append(row("Salesforce org limits (fed by the org-limits poller)", y))
y += 1

panels.append(
    panel(
        "API usage % used by limit",
        "bargauge",
        [
            target(
                f"100 * (1 - sf2loki_salesforce_limit_remaining{JOB} / sf2loki_salesforce_limit_max{JOB})",
                legend="{{limit_name}}",
                instant=True,
            )
        ],
        x=0,
        y=y,
        w=12,
        h=8,
        unit="percent",
        desc="Percentage of each Salesforce org limit consumed (e.g. DailyApiRequests, "
        "DailyBulkApiBatches, EventLogFileSize). Sourced from the periodic org-limits poller — "
        "NOT the real-time event stream, so it updates on the poller's own interval, not per-event.",
        thresholds=GREEN_YELLOW_RED,
        options_extra={"orientation": "horizontal", "displayMode": "gradient"},
    )
)
panels.append(
    panel(
        "Remaining vs max for key limits",
        "timeseries",
        [
            target(
                f"sf2loki_salesforce_limit_remaining{JOB}",
                ref="A",
                legend="remaining · {{limit_name}}",
            ),
            target(f"sf2loki_salesforce_limit_max{JOB}", ref="B", legend="max · {{limit_name}}"),
        ],
        x=12,
        y=y,
        w=12,
        h=8,
        unit="short",
        desc="Raw remaining and max values per Salesforce org limit, from the org-limits poller. Use "
        "alongside the % used panel to see absolute headroom, not just the ratio.",
    )
)
y += 8

panels.append(
    panel(
        "Limits poll errors",
        "timeseries",
        [
            target(
                f"rate(sf2loki_salesforce_limits_poll_errors_total{JOB}[5m])",
                legend="poll errors/s",
            )
        ],
        x=0,
        y=y,
        w=24,
        h=6,
        unit="ops",
        desc="Error rate of the org-limits poller itself (e.g. Salesforce REST /limits API call failing). "
        "Healthy = 0; non-zero means the org-limits panels above are stale or won't update.",
        thresholds=thr([(None, "green"), (0.001, "red")]),
    )
)
y += 6


# =============================================================================
# ROW 3 — Loki sink
# =============================================================================
panels.append(row("Loki sink", y))
y += 1

panels.append(
    panel(
        "Push outcomes rate by outcome",
        "timeseries",
        [
            target(
                f"sum by (outcome) (rate(sf2loki_loki_push_total{JOB}[5m]))", legend="{{outcome}}"
            )
        ],
        x=0,
        y=y,
        w=12,
        h=8,
        unit="ops",
        desc="Loki push batch attempts/s by outcome (success / retried). Sustained retried volume means "
        "delivery to Loki is delayed (data is held and retried, not lost). Per-entry permanent "
        "drops are counted separately in the 'Entries dropped rate by reason' panel below.",
        stacking="normal",
        overrides=[
            {
                "matcher": {"id": "byName", "options": "success"},
                "properties": [{"id": "color", "value": {"mode": "fixed", "fixedColor": "green"}}],
            },
            {
                "matcher": {"id": "byName", "options": "retried"},
                "properties": [{"id": "color", "value": {"mode": "fixed", "fixedColor": "yellow"}}],
            },
        ],
    )
)
panels.append(
    panel(
        "Push duration p50/p95/p99",
        "timeseries",
        [
            target(
                f"histogram_quantile(0.50, sum by (le) (rate(sf2loki_loki_push_duration_seconds_bucket{JOB}[5m])))",
                ref="A",
                legend="p50",
            ),
            target(
                f"histogram_quantile(0.95, sum by (le) (rate(sf2loki_loki_push_duration_seconds_bucket{JOB}[5m])))",
                ref="B",
                legend="p95",
            ),
            target(
                f"histogram_quantile(0.99, sum by (le) (rate(sf2loki_loki_push_duration_seconds_bucket{JOB}[5m])))",
                ref="C",
                legend="p99",
            ),
        ],
        x=12,
        y=y,
        w=12,
        h=8,
        unit="s",
        desc="Loki push request latency quantiles. Rising p95/p99 with stable push rate indicates Loki-side "
        "or network slowness rather than a sf2loki-side problem.",
    )
)
y += 8

panels.append(
    panel(
        "Bytes pushed rate",
        "timeseries",
        [target(f"rate(sf2loki_loki_bytes_pushed_total{JOB}[5m])", legend="bytes/s")],
        x=0,
        y=y,
        w=12,
        h=8,
        unit="Bps",
        desc="Wire bytes/s pushed to Loki. Tracks overall ingest payload volume shipped downstream.",
    )
)
panels.append(
    panel(
        "Lines truncated rate",
        "timeseries",
        [
            target(
                f"sum by (source) (rate(sf2loki_lines_truncated_total{JOB}[5m]))",
                legend="{{source}}",
            )
        ],
        x=12,
        y=y,
        w=12,
        h=8,
        unit="ops",
        desc="Rate of log lines truncated to the configured max_line_bytes before push, by source. "
        "Non-zero means some event payloads are being clipped — a data-fidelity signal, not data loss.",
        thresholds=thr([(None, "green"), (0.001, "yellow")]),
    )
)
y += 8

panels.append(
    panel(
        "Entries dropped rate by reason",
        "timeseries",
        [
            target(
                f"sum by (reason) (rate(sf2loki_loki_entries_dropped_total{JOB}[5m]))",
                legend="{{reason}}",
            )
        ],
        x=0,
        y=y,
        w=18,
        h=6,
        unit="ops",
        desc="Log entries permanently dropped as undeliverable, per reason (bad_request = Loki rejected "
        "the entry with 400, e.g. outside the out-of-order window; oversized_413 = a single entry "
        "too large even alone). Healthy = 0 — every non-zero point is real data loss, logged at "
        "ERROR by the connector and worth alerting on. Auth failures (401/403) are NOT counted "
        "here: those are retried indefinitely without dropping.",
        thresholds=thr([(None, "green"), (0.001, "red")]),
    )
)
panels.append(
    panel(
        "Last successful push age",
        "stat",
        [
            target(
                f"time() - sf2loki_last_push_success_timestamp_seconds{JOB}",
                legend="age",
                instant=True,
            )
        ],
        x=18,
        y=y,
        w=6,
        h=6,
        unit="s",
        desc="Time since the last successful Loki push. Climbs continuously while the sink is down "
        "(pushes retried, nothing lost). The connector's /readyz flips to 503 once pushes have "
        "been failing for longer than service.unready_after_sink_failing (default 15m).",
        thresholds=thr([(None, "green"), (300, "yellow"), (900, "red")]),
    )
)
y += 6


# =============================================================================
# ROW 4 — Connector health
# =============================================================================
panels.append(row("Connector health", y))
y += 1

panels.append(
    panel(
        "Auth refreshes & errors rate",
        "timeseries",
        [
            target(f"rate(sf2loki_auth_refreshes_total{JOB}[5m])", ref="A", legend="refreshes/s"),
            target(f"rate(sf2loki_auth_errors_total{JOB}[5m])", ref="B", legend="errors/s"),
        ],
        x=0,
        y=y,
        w=12,
        h=8,
        unit="ops",
        desc="Salesforce OAuth token refresh rate vs auth error rate. Refreshes-with-no-errors is healthy "
        "steady state; any sustained error rate means credentials/connected-app config need attention.",
        overrides=[
            {
                "matcher": {"id": "byName", "options": "errors/s"},
                "properties": [{"id": "color", "value": {"mode": "fixed", "fixedColor": "red"}}],
            },
        ],
    )
)
panels.append(
    panel(
        "Internal queue depth",
        "timeseries",
        [target(f"sf2loki_queue_depth{JOB}", legend="queue depth")],
        x=12,
        y=y,
        w=12,
        h=8,
        unit="short",
        desc="Depth of the internal in-process event queue between ingestion and the Loki sink. Healthy "
        "≈ 0; sustained growth means the sink can't keep up with ingestion (backpressure risk).",
        thresholds=thr([(None, "green"), (100, "yellow"), (1000, "red")]),
    )
)
y += 8

panels.append(
    panel(
        "Pub/Sub pending credits by topic",
        "timeseries",
        [target(f"sf2loki_pubsub_pending_credits{JOB}", legend="{{topic}}")],
        x=0,
        y=y,
        w=12,
        h=8,
        unit="short",
        desc="Outstanding flow-control credits owed to Salesforce Pub/Sub per topic. A persistently high "
        "or climbing value means the consumer is not keeping pace with the credit window.",
    )
)
panels.append(
    panel(
        "Pub/Sub reconnects rate",
        "timeseries",
        [
            target(
                f"sum by (topic) (rate(sf2loki_pubsub_reconnects_total{JOB}[5m]))",
                legend="{{topic}}",
            )
        ],
        x=12,
        y=y,
        w=12,
        h=8,
        unit="ops",
        desc="Pub/Sub gRPC stream reconnect rate per topic. Occasional reconnects are normal (stream "
        "lifetime limits); a sustained high rate indicates network instability or server-side issues.",
    )
)
y += 8

panels.append(
    panel(
        "Pub/Sub stream up by topic",
        "state-timeline",
        [target(f"sf2loki_pubsub_stream_up{JOB}", legend="{{topic}}")],
        x=0,
        y=y,
        w=8,
        h=8,
        unit="short",
        desc="Per-topic Pub/Sub subscribe stream health: 1 while connected and receiving, 0 while "
        "erroring/reconnecting. Short red blips are normal stream churn; a topic stuck at 0 means "
        "its real-time events aren't flowing (ingestion falls back to nothing for that topic).",
        thresholds=thr([(None, "red"), (1, "green")]),
        mappings=[
            {
                "type": "value",
                "options": {
                    "0": {"text": "down", "color": "red", "index": 1},
                    "1": {"text": "up", "color": "green", "index": 0},
                },
            }
        ],
    )
)
panels.append(
    panel(
        "SOQL poll errors rate",
        "timeseries",
        [
            target(
                f"sum by (source, object) (rate(sf2loki_soql_poll_errors_total{JOB}[5m]))",
                legend="{{source}}/{{object}}",
            )
        ],
        x=8,
        y=y,
        w=8,
        h=8,
        unit="ops",
        desc="Failed SOQL poll cycles per source and object/event type (eventlog_objects and "
        "eventlogfile pollers). Healthy = 0; sustained errors mean that object's watermark isn't "
        "advancing — pair with the replay/watermark age panel.",
        thresholds=thr([(None, "green"), (0.001, "red")]),
    )
)
panels.append(
    panel(
        "Timestamp fallbacks rate",
        "timeseries",
        [
            target(
                f"sum by (source) (rate(sf2loki_timestamp_fallbacks_total{JOB}[5m]))",
                legend="{{source}}",
            )
        ],
        x=16,
        y=y,
        w=8,
        h=8,
        unit="ops",
        desc="Entries whose event timestamp was missing/unparseable and a fallback (e.g. ingest time) "
        "was used, per source. Non-zero means log timestamps in Loki may not match the true "
        "Salesforce event time — a data-fidelity signal, not data loss.",
        thresholds=thr([(None, "green"), (0.001, "yellow")]),
    )
)
y += 8

panels.append(
    panel(
        "Decode errors rate",
        "timeseries",
        [
            target(
                f"sum by (reason) (rate(sf2loki_decode_errors_total{JOB}[5m]))", legend="{{reason}}"
            )
        ],
        x=0,
        y=y,
        w=12,
        h=8,
        unit="ops",
        desc="Avro/payload decode error rate by reason. Healthy = 0; non-zero means events are being "
        "dropped before they ever reach Loki — a silent-data-loss signal worth alerting on.",
        thresholds=thr([(None, "green"), (0.001, "red")]),
    )
)
panels.append(
    panel(
        "Replay/watermark age",
        "stat",
        [
            target(
                f"time() - sf2loki_last_replay_commit_timestamp_seconds{JOB}",
                ref="A",
                legend="replay commit age · {{topic}}",
            ),
            target(
                f"time() - sf2loki_watermark_timestamp_seconds{JOB}",
                ref="B",
                legend="watermark age · {{source}}/{{object}}",
            ),
        ],
        x=12,
        y=y,
        w=12,
        h=8,
        unit="s",
        desc="Age (now minus last committed checkpoint) of the Pub/Sub replay_id commit and the "
        "EventLogFile polling watermark. A climbing age means that checkpoint isn't advancing — "
        "the connector may be stuck, even if the queue/ingestion panels still look fine.",
        legend_calcs=["lastNotNull", "max"],
        thresholds=thr([(None, "green"), (300, "yellow"), (900, "red")]),
    )
)
y += 8

panels.append(
    panel(
        "Build info",
        "table",
        [target(f"sf2loki_build_info{JOB}", instant=True, fmt="table")],
        x=0,
        y=y,
        w=24,
        h=5,
        unit="short",
        desc="Connector build/version metadata per running instance (sf2loki_build_info, value always 1). "
        "Use to confirm which version is deployed and that all instances agree.",
        options_extra={"showHeader": True, "cellHeight": "sm"},
    )
)
y += 5


# =============================================================================
# Templating: datasource + $job
# =============================================================================
templating = {
    "list": [
        {
            "name": "DS_PROMETHEUS",
            "type": "datasource",
            "query": "prometheus",
            "label": "Prometheus datasource",
            "hide": 0,
            "current": {},
            "refresh": 1,
            "options": [],
            "regex": "",
            "multi": False,
            "includeAll": False,
        },
        {
            "name": "job",
            "type": "query",
            "datasource": DS,
            "query": "label_values(sf2loki_build_info, job)",
            "definition": "label_values(sf2loki_build_info, job)",
            "label": "Job",
            "hide": 0,
            "current": {"text": "sf2loki", "value": "sf2loki"},
            "refresh": 2,
            "regex": "",
            "multi": False,
            "includeAll": False,
            "sort": 1,
        },
    ]
}

dashboard = {
    "__inputs": [
        {
            "name": "DS_PROMETHEUS",
            "label": "Prometheus",
            "description": "Prometheus-compatible datasource (e.g. Grafana Mimir / Grafana Cloud) holding sf2loki's OTLP-pushed metrics.",
            "type": "datasource",
            "pluginId": "prometheus",
            "pluginName": "Prometheus",
        }
    ],
    "__requires": [
        {"type": "grafana", "id": "grafana", "name": "Grafana", "version": "10.0.0"},
        {"type": "datasource", "id": "prometheus", "name": "Prometheus", "version": "1.0.0"},
        {"type": "panel", "id": "timeseries", "name": "Time series", "version": ""},
        {"type": "panel", "id": "stat", "name": "Stat", "version": ""},
        {"type": "panel", "id": "bargauge", "name": "Bar gauge", "version": ""},
        {"type": "panel", "id": "state-timeline", "name": "State timeline", "version": ""},
        {"type": "panel", "id": "table", "name": "Table", "version": ""},
        {"type": "panel", "id": "row", "name": "Row", "version": ""},
    ],
    "id": None,
    "uid": "sf2loki-overview",
    "title": "sf2loki — Salesforce → Loki",
    "description": (
        "Monitors the Salesforce Event Monitoring -> Loki pipeline shipped by sf2loki: ingestion "
        "volume/lag, Salesforce org API limits, the Loki sink (incl. last successful push age), "
        "and the connector's own self-observability (auth, internal queue, Pub/Sub stream health, "
        "SOQL poll errors, timestamp fallbacks, decode errors, replay/watermark staleness, "
        "build info)."
    ),
    "tags": ["sf2loki", "salesforce", "loki"],
    "timezone": "browser",
    "schemaVersion": 39,
    "version": 1,
    "editable": True,
    "graphTooltip": 1,
    "refresh": "30s",
    "time": {"from": "now-6h", "to": "now"},
    "timepicker": {
        "refresh_intervals": ["5s", "10s", "30s", "1m", "5m", "15m", "30m", "1h"],
    },
    "templating": templating,
    "annotations": {
        "list": [
            {
                "builtIn": 1,
                "datasource": {"type": "grafana", "uid": "-- Grafana --"},
                "enable": True,
                "hide": True,
                "iconColor": "rgba(0, 211, 255, 1)",
                "name": "Annotations & Alerts",
                "type": "dashboard",
            }
        ]
    },
    "links": [],
    "panels": panels,
}


def main() -> None:
    with open(OUT_PATH, "w") as f:
        json.dump(dashboard, f, indent=2)
        f.write("\n")
    print(f"wrote {OUT_PATH} ({len(panels)} panels/rows)")


if __name__ == "__main__":
    main()
