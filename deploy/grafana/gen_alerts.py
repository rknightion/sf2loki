#!/usr/bin/env python3
"""Generator for the sf2loki Grafana alert-rule pack (file provisioning format).

sf2loki ships its own health metrics via OTLP (see ``src/sf2loki/obs/metrics.py`` and
DESIGN.md §12). This script turns a fixed set of PromQL-based alert rules into a
Grafana Alerting **file provisioning** manifest (``apiVersion: 1`` + ``groups:``),
grouped by failure category and dropped into the "sf2loki" folder.

Every rule's PromQL expression already encodes its own trigger condition via a
comparison operator (``> 0``, ``== 1``, ``< 0.1``, ...), so the query returns an
empty instant vector when healthy and a non-empty one when firing. The Grafana
query/condition pipeline is therefore identical for every rule: run the query (A),
count the rows (B, a "reduce" expression), then fire when that count is above zero
(C, a "threshold" expression) — condition ``C``. This is the standard "does this
PromQL query return any series" pattern for Prometheus-backed Grafana alerting, and
keeps all the actual alerting logic in the (portable, testable) PromQL string
rather than spread across Grafana's condition-builder fields.

Like ``gen_dashboard.py``, this has no external deps beyond pyyaml (already a
project dependency) — pure dict + yaml.dump, mirroring that script's house style
(constants-with-rationale, small builder helpers, committed generated output).

Datasource: alert-rule provisioning has no "prompt on import" equivalent to a
dashboard's ``__inputs`` — Grafana needs a concrete ``datasourceUid`` at apply
time. ``DS_UID`` below defaults to the same ``${DS_PROMETHEUS}`` placeholder token
used by ``gen_dashboard.py`` for consistency, but — unlike the dashboard — nothing
resolves that token for you: either regenerate with
``SF2LOKI_PROMETHEUS_DS_UID=<real-uid> python3 deploy/grafana/gen_alerts.py``, or
``envsubst`` / sed the placeholder in the committed YAML before dropping it into
Grafana's alerting provisioning directory. See ``deploy/grafana/README.md``.

Run:  python3 deploy/grafana/gen_alerts.py
Emits: deploy/grafana/alerts.yaml
"""

from __future__ import annotations

import os

import yaml

OUT_PATH = os.path.join(os.path.dirname(__file__), "alerts.yaml")

FOLDER = "sf2loki"

# Placeholder datasource UID — see module docstring. Overridable at generation
# time so an environment with a known UID can bake it straight into the output.
DS_UID = os.environ.get("SF2LOKI_PROMETHEUS_DS_UID", "${DS_PROMETHEUS}")

# Expression-node datasource for Grafana's built-in reduce/threshold pseudo-queries.
EXPR_UID = "__expr__"


def nid(slug: str) -> str:
    """Deterministic rule uid: stable across regenerations, unique per rule."""
    return f"sf2loki-{slug}"


# =============================================================================
# Thresholds, named with the rationale for each value.
# =============================================================================

# DATA LOSS — any occurrence is real, permanent loss; alert immediately.
ENTRIES_DROPPED_THRESHOLD = 0
REPLAY_FALLBACK_THRESHOLD = 0

# STALLED — the pipeline has effectively stopped making progress.
# 900s (15m) matches service.unready_after_sink_failing's default, i.e. the same
# point at which /readyz would flip to 503 for a failing sink.
PUSH_STALL_AGE_SECONDS = 900
# 4x a typical 1h SOQL/EventLogFile poll interval — comfortably past "the next
# poll just hasn't landed yet" and into "this watermark is stuck".
WATERMARK_STALE_SECONDS = 14400

# DEGRADATION — service is impaired but still making progress.
API_BUDGET_LOW_RATIO = 0.1  # <10% of a daily org limit remaining
# 6h: above the normal EventLogFile batch latency range (3-6h per DESIGN.md §12),
# so this only fires once lag is genuinely abnormal, not just "ELF hasn't run yet".
INGEST_LAG_P95_THRESHOLD_SECONDS = 21600


def q(expr: str, range_seconds: int) -> list[dict]:
    """Build the standard query -> reduce(count) -> threshold(>0) data pipeline.

    ``expr`` must already be a boolean/filtering PromQL expression (e.g. ending
    in ``> 0``), so a non-empty result means "firing". ``range_seconds`` only
    needs to comfortably cover any range-vector window inside ``expr`` (e.g.
    ``rate(...[5m])`` needs >= 300s) plus evaluation margin.
    """
    return [
        {
            "refId": "A",
            "relativeTimeRange": {"from": range_seconds, "to": 0},
            "datasourceUid": DS_UID,
            "model": {
                "editorMode": "code",
                "expr": expr,
                "instant": True,
                "range": False,
                "refId": "A",
            },
        },
        {
            "refId": "B",
            "relativeTimeRange": {"from": range_seconds, "to": 0},
            "datasourceUid": EXPR_UID,
            "model": {
                "type": "reduce",
                "expression": "A",
                "reducer": "count",
                "refId": "B",
            },
        },
        {
            "refId": "C",
            "relativeTimeRange": {"from": range_seconds, "to": 0},
            "datasourceUid": EXPR_UID,
            "model": {
                "type": "threshold",
                "expression": "B",
                "conditions": [{"evaluator": {"type": "gt", "params": [0]}}],
                "refId": "C",
            },
        },
    ]


def rule(
    slug: str,
    title: str,
    expr: str,
    *,
    range_seconds: int,
    for_: str,
    severity: str,
    summary: str,
    description: str,
) -> dict:
    return {
        "uid": nid(slug),
        "title": title,
        "condition": "C",
        "data": q(expr, range_seconds),
        "noDataState": "OK",
        "execErrState": "Error",
        "for": for_,
        "labels": {"severity": severity},
        "annotations": {
            "summary": summary,
            "description": description,
        },
        "isPaused": False,
    }


# =============================================================================
# DATA LOSS (critical) — entries or events are gone and cannot be replayed.
# =============================================================================

entries_dropped_expr = (
    f"sum(rate(sf2loki_loki_entries_dropped_total[5m])) > {ENTRIES_DROPPED_THRESHOLD}"
)

replay_fallback_expr = (
    f"sum(increase(sf2loki_pubsub_replay_fallbacks_total[1h])) > {REPLAY_FALLBACK_THRESHOLD}"
)

DATA_LOSS_RULES = [
    rule(
        "entries-dropped",
        "sf2loki: entries dropped (data loss)",
        entries_dropped_expr,
        range_seconds=600,
        for_="5m",
        severity="critical",
        summary="Log entries are being permanently dropped as undeliverable.",
        description=(
            "Loki is rejecting entries the connector cannot retry (bad_request, e.g. outside "
            "the out-of-order window, or oversized_413). Every non-zero point here is real, "
            "permanent data loss. First response: check the dashboard's 'Entries dropped rate "
            "by reason' panel for the reason label, then check Loki-side limits (out-of-order "
            "window, max entry size) or reduce max_line_bytes if entries are oversized."
        ),
    ),
    rule(
        "replay-fallback",
        "sf2loki: Pub/Sub replay fallback (possible gap)",
        replay_fallback_expr,
        range_seconds=7200,
        for_="0m",
        severity="critical",
        summary="A Pub/Sub subscription restarted from a fallback replay position.",
        description=(
            "The topic's stored replay_id was invalid/expired, so the connector restarted the "
            "subscription from a fallback replay preset — events between the last committed "
            "replay_id and the fallback position may have been skipped for that topic. First "
            "response: identify the affected topic (label) and, if the gap matters, backfill "
            "via SOQL/EventLogFile for that topic and time window."
        ),
    ),
]


# =============================================================================
# STALLED (critical) — the pipeline has stopped making progress.
# =============================================================================

pushes_stalled_expr = (
    f"(time() - sf2loki_last_push_success_timestamp_seconds > {PUSH_STALL_AGE_SECONDS}) "
    "and (sf2loki_queue_depth > 0)"
)

stream_down_expr = "min by (topic) (sf2loki_pubsub_stream_up) == 0"

watermark_stale_expr = f"(time() - sf2loki_watermark_timestamp_seconds) > {WATERMARK_STALE_SECONDS}"

STALLED_RULES = [
    rule(
        "pushes-stalled",
        "sf2loki: pushes to Loki stalled",
        pushes_stalled_expr,
        range_seconds=300,
        for_="5m",
        severity="critical",
        summary="No successful Loki push in 15+ minutes while the queue is non-empty.",
        description=(
            "The connector hasn't successfully pushed to Loki in over 15 minutes while its "
            "internal queue still holds data — the sink is failing, not just idle, and /readyz "
            "has likely flipped to 503. First response: check Loki connectivity/auth "
            "(tenant_id, auth_token) and Loki-side health; if Loki looks healthy, check for a "
            "stuck push (network partition, DNS)."
        ),
    ),
    rule(
        "stream-down",
        "sf2loki: Pub/Sub stream down",
        stream_down_expr,
        range_seconds=300,
        for_="15m",
        severity="critical",
        summary="A Pub/Sub topic's subscribe stream has been down for 15+ minutes.",
        description=(
            "sf2loki_pubsub_stream_up has been 0 for this topic for a sustained period — "
            "real-time events for that topic are not flowing at all (short blips during normal "
            "stream churn are expected and won't reach this duration). First response: check "
            "connector logs for that topic's reconnect/auth errors and Salesforce Pub/Sub API "
            "status; restart the connector if it isn't self-recovering."
        ),
    ),
    rule(
        "watermark-stale",
        "sf2loki: polling watermark stale",
        watermark_stale_expr,
        range_seconds=300,
        for_="30m",
        severity="critical",
        summary="A source/object's SOQL polling watermark hasn't advanced in 4+ hours.",
        description=(
            "sf2loki_watermark_timestamp_seconds for this source/object is over 4 hours old — "
            "4x a typical 1h poll interval, well past a merely-delayed poll. That object's "
            "ingestion is effectively stuck even if other panels look fine. First response: "
            "check 'SOQL poll errors rate' for that source/object and the connector logs for "
            "the poller; verify the SOQL query/permissions are still valid."
        ),
    ),
]


# =============================================================================
# DEGRADATION (warning) — impaired but still making progress.
# =============================================================================

soql_poll_errors_expr = "sum(rate(sf2loki_soql_poll_errors_total[15m])) > 0"
stream_stalls_expr = "sum(increase(sf2loki_pubsub_stream_stalls_total[1h])) > 0"
api_throttled_expr = "sum(rate(sf2loki_salesforce_api_throttled_total[15m])) > 0"
api_budget_low_expr = (
    'sf2loki_salesforce_limit_remaining{limit_name="DailyApiRequests"} / '
    'sf2loki_salesforce_limit_max{limit_name="DailyApiRequests"} '
    f"< {API_BUDGET_LOW_RATIO}"
)
egress_paused_expr = "sf2loki_egress_paused == 1"
ingest_lag_p95_expr = (
    "histogram_quantile(0.95, sum by (le) ("
    "rate(sf2loki_ingest_lag_seconds_bucket[15m]))) "
    f"> {INGEST_LAG_P95_THRESHOLD_SECONDS}"
)

DEGRADATION_RULES = [
    rule(
        "soql-poll-errors",
        "sf2loki: SOQL poll errors",
        soql_poll_errors_expr,
        range_seconds=1800,
        for_="15m",
        severity="warning",
        summary="A source/object's SOQL poll cycles are failing.",
        description=(
            "Sustained SOQL poll failures for a source/object — that object's watermark is not "
            "advancing and its data isn't being ingested. First response: check connector logs "
            "for the SOQL error detail (auth, malformed query, object permissions) for the "
            "affected source/object."
        ),
    ),
    rule(
        "stream-stalls",
        "sf2loki: Pub/Sub stream stalls",
        stream_stalls_expr,
        range_seconds=7200,
        for_="0m",
        severity="warning",
        summary="The Pub/Sub keepalive watchdog force-reconnected a stream.",
        description=(
            "sf2loki forcibly reconnected a topic's subscribe stream because the keepalive "
            "watchdog judged it stalled — usually self-recovers but indicates stream "
            "instability. First response: if this recurs for the same topic, check network "
            "stability between the connector and Salesforce Pub/Sub, and review "
            "reconnect/backoff configuration."
        ),
    ),
    rule(
        "api-throttled",
        "sf2loki: Salesforce API throttled",
        api_throttled_expr,
        range_seconds=1800,
        for_="5m",
        severity="warning",
        summary="Salesforce REST calls are being rejected with REQUEST_LIMIT_EXCEEDED.",
        description=(
            "The connector is hitting Salesforce API rate limits. First response: check the "
            "dashboard's 'API usage % used by limit' panel and consider raising poll intervals "
            "or requesting a higher Salesforce API limit allocation for the org."
        ),
    ),
    rule(
        "api-budget-low",
        "sf2loki: Salesforce daily API budget low",
        api_budget_low_expr,
        range_seconds=300,
        for_="30m",
        severity="warning",
        summary="Less than 10% of the DailyApiRequests org limit remains.",
        description=(
            "The org is close to exhausting its DailyApiRequests limit for the day — "
            "continuing at the current rate risks Salesforce hard-capping all further API "
            "calls today, which would stall every SOQL-based source. First response: check the "
            "org limits dashboard row for which pollers are consuming budget, and reduce poll "
            "frequency or event type scope until the daily limit resets."
        ),
    ),
    rule(
        "egress-paused",
        "sf2loki: egress paused by daily byte budget",
        egress_paused_expr,
        range_seconds=300,
        for_="5m",
        severity="warning",
        summary="Pushes to Loki are paused because the daily egress byte budget was hit.",
        description=(
            "The daily egress byte budget has been exhausted, so pushes to Loki are paused "
            "until the budget resets at the next UTC day — no new data ships to Loki while "
            "this is active. First response: check 'egress budget used bytes' and the "
            "configured daily budget; raise the budget or reduce ingestion scope (sampling, "
            "transform drop_row rules) if this recurs regularly."
        ),
    ),
    rule(
        "ingest-lag-p95-high",
        "sf2loki: ingest lag p95 high",
        ingest_lag_p95_expr,
        range_seconds=1800,
        for_="30m",
        severity="warning",
        summary="p95 ingest lag has exceeded 6 hours, above normal EventLogFile latency.",
        description=(
            "p95 of sf2loki_ingest_lag_seconds has exceeded 6 hours — above the normal 3-6h "
            "EventLogFile batch latency range, so the pipeline is meaningfully behind "
            "Salesforce's event stream for at least one event type. First response: check the "
            "ingest-lag panel by event_type to isolate which path (Pub/Sub real-time vs "
            "EventLogFile batch) is lagging, then check queue depth / Loki push health for "
            "backpressure."
        ),
    ),
]


# =============================================================================
# HYGIENE (info) — data-fidelity signals, not loss or outage.
# =============================================================================

timestamp_fallbacks_expr = "sum(rate(sf2loki_timestamp_fallbacks_total[1h])) > 0"
lines_truncated_expr = "sum(rate(sf2loki_lines_truncated_total[1h])) > 0"

HYGIENE_RULES = [
    rule(
        "timestamp-fallbacks",
        "sf2loki: timestamp fallbacks in use",
        timestamp_fallbacks_expr,
        range_seconds=7200,
        for_="15m",
        severity="info",
        summary="Entries are using a fallback timestamp instead of the true event time.",
        description=(
            "Some entries have a missing/unparseable Salesforce event timestamp and are using "
            "a fallback (e.g. ingest time) instead — a data-fidelity issue, not data loss: "
            "Loki timestamps for those entries may not match the true Salesforce event time. "
            "First response: check 'Timestamp fallbacks rate' by source to see which source is "
            "affected, and check whether Salesforce is returning malformed date fields for "
            "that object."
        ),
    ),
    rule(
        "lines-truncated",
        "sf2loki: log lines truncated",
        lines_truncated_expr,
        range_seconds=7200,
        for_="15m",
        severity="info",
        summary="Log lines are being truncated to max_line_bytes before push.",
        description=(
            "Some event payloads exceed max_line_bytes and are being clipped before push — a "
            "data-fidelity issue, not data loss. First response: check 'Lines truncated rate' "
            "by source and consider raising max_line_bytes for that source if full payloads "
            "are required."
        ),
    ),
]


GROUPS = [
    ("sf2loki-data-loss", DATA_LOSS_RULES),
    ("sf2loki-stalled", STALLED_RULES),
    ("sf2loki-degradation", DEGRADATION_RULES),
    ("sf2loki-hygiene", HYGIENE_RULES),
]

manifest = {
    "apiVersion": 1,
    "groups": [
        {
            "orgId": 1,
            "name": name,
            "folder": FOLDER,
            "interval": "1m",
            "rules": rules,
        }
        for name, rules in GROUPS
    ],
}


def main() -> None:
    with open(OUT_PATH, "w") as f:
        f.write(
            "# Generated by deploy/grafana/gen_alerts.py — do not hand-edit.\n"
            "# Regenerate with: python3 deploy/grafana/gen_alerts.py (or `just gen-grafana`).\n"
        )
        yaml.dump(manifest, f, sort_keys=False, default_flow_style=False, width=100)
    total = sum(len(rules) for _, rules in GROUPS)
    print(f"wrote {OUT_PATH} ({total} rules across {len(GROUPS)} groups)")


if __name__ == "__main__":
    main()
