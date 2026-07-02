# Troubleshooting

Common operational issues and their fixes. Start with [`sf2loki doctor`](reference/cli.md#sf2loki-doctor)
for anything that smells like an auth, permissions, or connectivity problem — it isolates which
layer is failing before you go digging.

## Why do I see periodic Pub/Sub reconnects every N minutes?

A sawtooth on `sf2loki_pubsub_reconnects` / `sf2loki_auth_refreshes` at a regular interval is
almost always your org's **session timeout**, not a fault.

!!! tip "This is expected — tune the session timeout, don't chase it as a bug"
    Neither Salesforce OAuth flow (`jwt_bearer` or `client_credentials`) returns `expires_in` or a
    refresh token — the access token's real lifetime is the org's session timeout (Setup →
    Session Settings), which can be as short as 15 minutes. sf2loki handles expiry reactively: it
    re-mints the token on a 401/`UNAUTHENTICATED` and resubscribes from the stored `replay_id`, so
    there's no data loss, just reconnect churn. To reduce the churn, raise the integration user's
    session timeout (a profile-level Session Settings override works) and set
    `salesforce.token_ttl` to match, so sf2loki proactively re-mints before Salesforce kills the
    session.

## The container crash-loops with a permission error on startup

This is almost always a secret file uid mismatch.

!!! tip "Secret files must be readable by uid 10001"
    The container runs as a non-root user, uid `10001`. Files under `salesforce.private_key_file`,
    `salesforce.client_secret_file`, `sink.loki.auth_token_file`, and similar `*_file` paths must
    be *readable* by that uid or the service fails fast at startup with an actionable "permission
    denied" error. A root-owned `chmod 0600` key file — the natural way to store a private key —
    is exactly the trap. Fix with:

    ```bash
    chmod 640 secrets/*        # or: chown the files to uid 10001
    ```

    The same applies to the checkpoint state directory, but in the other direction — it must be
    *writable* by uid 10001: `mkdir -p state && chmod 770 state && chown 10001 state` (not a
    permissive `777`).

## My load balancer / orchestrator keeps restarting a healthy standby

You've pointed a liveness/restart check at `/readyz` instead of `/healthz`.

!!! tip "`/readyz` is readiness, not liveness — never wire it to a restart policy"
    `/healthz` is liveness: 200 whenever the process is up, even mid-startup or while standing by
    as an HA standby. `/readyz` is readiness: 200 only once auth has resolved and the pipeline is
    actively running, and it degrades to 503 if Loki pushes have failed continuously past
    `service.unready_after_sink_failing`. On an active-passive HA pair the standby's `/readyz` is
    **503 forever, by design** — it never becomes the leader until the active instance fails. A
    Kubernetes `livenessProbe`, an ECS task-level `healthCheck`, or a Docker `HEALTHCHECK` pointed
    at `/readyz` restart-loops the standby continuously and defeats failover. Use `/readyz` only
    for routing decisions (a Kubernetes `readinessProbe`, an ECS target-group health check) and
    `/healthz` for anything that can restart the process. See
    [High Availability](deployment/high-availability.md) for the full readiness-vs-liveness split.

## A dashboard panel is empty even though sf2loki is running

This is almost always the OpenTelemetry→Prometheus metric-name suffix, not a broken connector.

!!! tip "Check `add_metric_suffixes` before assuming metrics aren't flowing"
    Instruments are created **unsuffixed** in code (e.g. `sf2loki_events_ingested`), but appear in
    Prometheus/Grafana with the standard OTel→Prometheus suffixes — `_total`, `_bucket`, `_count`,
    `_sum` (e.g. `sf2loki_events_ingested_total`). Grafana Cloud's OTLP endpoint adds these by
    default. If you route metrics through your own OpenTelemetry Collector or Grafana Alloy
    instead, keep `add_metric_suffixes` (a.k.a. `AddMetricSuffixes`) enabled on the Prometheus
    exporter — with it off, every panel and alert rule that queries the suffixed name goes
    silently blank. See [Metrics Reference](observability/metrics.md) for the full instrument
    list and which name each expects.

## `sf2loki doctor` reports a short EventLogFile menu / no Hourly files

Expected on an org without the Shield/Event Monitoring add-on.

!!! tip "Free and dev orgs get a fixed EventLogFile subset"
    Without the Shield Event Monitoring add-on, an org produces only the free EventLogFile
    subset — Login, Logout, API Total Usage, Apex Unexpected Exception, and the CORS/CSP-violation
    and hostname-redirect types — at **Daily interval only, 1-day retention**. An
    `event_types: ["*"]` wildcard on such an org silently yields just those types; that's
    discovery working correctly, not a bug. `interval: Hourly` additionally needs the add-on's
    hourly opt-in — expect `doctor`'s `entitlement` check to WARN (not FAIL) when a configured
    type or interval isn't available yet, since it may simply not have produced a file recently.
    The full ~70-type catalogue and RTEM streaming channels require the add-on.

## sf2loki refuses to start with an `OverlapError`

You've enabled the same event category on more than one source.

!!! tip "Fix the overlap, or opt into it deliberately"
    Salesforce exposes the same underlying activity through multiple channels — for example
    `/event/LoginEventStream` (Pub/Sub), `LoginEvent` (SOQL-polled), and `Login` (EventLogFile) are
    the *same records* in three costumes. Ingesting one event category from more than one source
    double-counts it in Loki, so sf2loki's startup overlap guard (`src/sf2loki/sources/overlap.py`)
    refuses to start and lists every colliding category. Either disable all but one source for the
    affected category, or set `sources.allow_overlap: true` if the duplication is deliberate (for
    example, relying on Loki to drop byte-identical entries, or intentionally running both a lean
    real-time stream and a richer batch source side by side).
