---
title: FAQ
description: Frequently asked questions about running, configuring, and securing sf2loki, each answer pointing to the authoritative documentation page.
---

# Frequently Asked Questions

Short answers to common questions. Each answer links to the authoritative page for the full
detail - treat those linked pages as the source of truth.

## Getting started

### What's the fastest way to see data in Loki?

One org, one Pub/Sub topic, `client_credentials` auth against the published Docker image - about
5 minutes end to end. See [Getting Started](getting-started.md) for the minimal config and
compose file.

### Do I need the paid Event Monitoring add-on?

For the full ~70 EventLogFile types and RTEM streaming channels, yes. Without Shield/Event
Monitoring, an org still produces a small free EventLogFile subset (Login, Logout, API Total
Usage, Apex Unexpected Exception, and a couple of others) at daily interval and 1-day retention
only. See [Troubleshooting](troubleshooting.md#sf2loki-doctor-reports-a-short-eventlogfile-menu-no-hourly-files).

### Which Python version does it need?

3.14+. You don't need to provision that yourself for the container (nothing runs on the host) or
for `uvx`/`pipx` (they fetch a matching interpreter automatically) - see
[Installation](installation.md).

## Configuration

### How is sf2loki configured?

A YAML file, environment variables prefixed `SF2LOKI_` with `__` as the nesting delimiter (e.g.
`SF2LOKI_SINK__LOKI__URL`), or both - env overrides YAML overrides built-in defaults. Every key,
type, and default is in the [full reference](config-reference.md).

### I enabled `LoginEvent` under `eventlog_objects` and `Login` under `eventlogfile` - startup fails with `OverlapError`. Why?

Those are the same Salesforce activity through two different channels, and ingesting a category
from more than one source double-counts it in Loki. sf2loki's startup guard refuses to start
rather than let that happen silently - fix the overlap or set `sources.allow_overlap: true` if
the duplication is deliberate. See [Sources: one category, one source](sources/index.md#one-category-one-source).

### Can I ingest more than one Salesforce org?

Yes - swap the top-level `salesforce:`/`sources:` config for an `orgs:` list. Each org keeps its
own connection, API limits, and checkpoint-key prefix in one shared Loki sink, and one org's auth
outage doesn't stop the others. See [Configuration](configuration/index.md#salesforce-orgs-connection-and-org-selection)
and [Presets](configuration/presets.md) for a worked example.

### Why does a `*_file` secret path or `${ENV}` reference fail at startup instead of just warning?

By design - sf2loki never starts in a half-configured state. A missing or unreadable secret file,
or an unset referenced environment variable, is fatal immediately, with the offending key named in
the error, rather than surfacing as a confusing auth failure later. See
[Configuration: Secrets](configuration/index.md#secrets).

## Sources & data

### Which source should I use for a given event category?

`pubsub` for low-latency categories with a streaming topic, `eventlog_objects` for categories
you'd rather poll (or that have no stream), and `eventlogfile` for the roughly 70 EventType CSVs
that aren't exposed any other way. `apexlog` is a standalone opt-in with no overlap against the
other three. See [Sources](sources/index.md).

### Why don't I see `user_id` or `source_ip` as a Loki label?

Deliberately excluded. Only a fixed allowlist (`job`, `service_name`, `source`, `event_type`,
`sf_org_id`, `environment`, `org`) is ever promoted to a stream label; a startup guard rejects
anything else. High-cardinality fields travel in structured metadata or the JSON line instead,
which is what keeps active Loki streams in roughly the 30-90 range regardless of event volume. See
[Architecture: label / cardinality strategy](architecture.md#label-cardinality-strategy).

### Can I redact PII before it reaches Loki?

Yes, per source, via declarative `hash` / `mask` / `drop_field` / `regex_replace` / `drop_row`
transform rules, applied before labels and timestamps are extracted. Always set a
`transform_salt`/`transform_salt_file` if you use `hash` - unsalted hashes of low-entropy values
like IPs are trivially reversible by rainbow table. See
[PII Redaction & Sampling](sources/pii-and-sampling.md).

### If I sample or drop rows, does the checkpoint still advance?

Yes - sampling and `drop_row` still commit checkpoints, so nothing gets stuck re-fetching data you
deliberately discarded. Sampling is lossy (the data is gone, not delayed); use the rate caps or
daily byte budget instead if you want lossless volume control. See
[PII Redaction & Sampling: sampling](sources/pii-and-sampling.md#sampling) and
[Cost Controls](sources/cost-controls.md).

## Deployment & operations

### Can I run more than one instance for throughput?

No - Salesforce's Pub/Sub API has no consumer-group semantics, so a second *active* replica on the
same topic double-delivers every event rather than sharing load. HA here means one hot spare
(active-passive), not horizontal scale-out. See
[High Availability](deployment/high-availability.md#why-single-instance-and-why-active-passive-rather-than-scale-out).

### My orchestrator keeps restarting a healthy HA standby - why?

You've pointed a restart/liveness check at `/readyz` instead of `/healthz`. On an active-passive
standby, `/readyz` returns `503` forever by design - it only becomes ready once it's the leader -
so a liveness or task-level health check pointed at it restart-loops the standby and defeats
failover. Use `/readyz` only for routing decisions (a Kubernetes readiness probe, an ECS
target-group check) and `/healthz` for anything that restarts the process. See
[Troubleshooting](troubleshooting.md#my-load-balancer-orchestrator-keeps-restarting-a-healthy-standby).

### The container crash-loops right after I mount my secrets - why?

Almost always a uid mismatch. The container runs as non-root uid `10001`, and secret files (the
private key, the Loki token, etc.) must be *readable* by that uid - a root-owned `chmod 0600` key
is the classic trap. The checkpoint state directory needs the opposite: writable by uid `10001`.
See [Troubleshooting](troubleshooting.md#the-container-crash-loops-with-a-permission-error-on-startup).

### Why do I see periodic Pub/Sub reconnects every N minutes?

Almost certainly your org's session timeout expiring, not a fault - neither OAuth flow returns a
refresh token, so the access token's real lifetime is whatever Session Settings allows (as short
as 15 minutes). sf2loki re-mints reactively on a 401 and resubscribes from the stored `replay_id`,
so there's no data loss, just reconnect churn. See
[Troubleshooting](troubleshooting.md#why-do-i-see-periodic-pubsub-reconnects-every-n-minutes).

### A source's checkpoint looks stuck - how do I unstick it?

Find the key with `sf2loki state show` (checkpoint keys map from the `source`/`object` labels on
`sf2loki_watermark_timestamp_seconds`, e.g. `eventlog_objects:LoginEvent`), then use
`sf2loki state set` or `sf2loki state delete` to move it past a poison record. Treat it as a tool
of last resort once logs and permissions are ruled out. See [State & Checkpoints](deployment/state.md).

## Security

### Are my Salesforce and Loki credentials ever logged?

No - secrets are injected from `*_file` paths or `${ENV}` interpolation only, never written to
logs, error messages, or generated config artifacts. See [Security: credentials and tokens](security.md#credentials-and-tokens).

### Are `/healthz` and `/readyz` safe to expose without authentication?

Yes, by design - they carry only process liveness/readiness state, no Salesforce or Loki data. The
shipped `docker-compose.yml` still binds them to loopback by default; don't expose them to the
public internet without a reason to. See [Security: health endpoints](security.md#health-endpoints).

### What are my obligations if I modify sf2loki and run it as a service for others?

sf2loki is AGPL-3.0-only. Running the unmodified published image carries no source-offer
obligation. Running a *modified* version as a network service - including a hosted/managed
offering - requires offering the modified source to that service's users. See
[Security: license obligations](security.md#license-obligations).
