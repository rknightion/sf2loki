---
title: sf2loki
description: Streams Salesforce Event Monitoring data (Pub/Sub, SOQL-polled objects, EventLogFile, ApexLog) into Grafana Loki — cardinality-disciplined, HA-capable.
image: assets/social-card.png
---

# sf2loki

`sf2loki` is a long-running Python/asyncio service that ships Salesforce **Real-Time Event
Monitoring** (RTEM) data into Grafana Loki. It pulls from four source types — Pub/Sub streaming
(gRPC + Avro), SOQL-polled objects, EventLogFile CSV exports, and ApexLog debug logs — behind one
async-iterator seam, and pushes the result to Loki (or any Loki-compatible endpoint) as
protobuf+snappy, structured-metadata-carrying log lines.

The load-bearing design decisions are what keep it running unattended: a fixed Loki label
allowlist enforced at startup so nothing high-cardinality (`user_id`, `source_ip`, `replay_id`, …)
ever becomes a stream label; an **either/or-per-category** rule that ingests each event category
from exactly one source so the same activity can't double-count; and an active-passive **HA**
model (file lease or Kubernetes `Lease`) for the single Pub/Sub subscriber the API allows, so a
crashed leader fails over without a second instance double-delivering events.

## Features

- **Four pluggable sources** — Pub/Sub streaming for RTEM channels and your own custom platform
  events / Change Data Capture, SOQL polling of stored objects (`LoginHistory`, big objects,
  custom objects), EventLogFile CSV ingestion, and an opt-in ApexLog source for Apex debug logs.
- **Multi-org ingestion** — one process, one shared Loki sink, an `orgs:` list of Salesforce
  connections; each org gets its own `org` stream label and API limits, and one org's outage
  doesn't stop the others.
- **S3 and GCS checkpoint stores** — for stateless deployments (Fargate, Cloud Run, ECS with
  ephemeral storage) that can't mount a persistent volume, with compare-and-swap commits so two
  instances can't silently clobber each other's checkpoints.
- **Active-passive HA** — a shared file lease or a Kubernetes `coordination.k8s.io` `Lease` elects
  exactly one leader, with commit fencing so a stale leader can't race the new one's checkpoints.
- **OTLP self-metrics** — connector health and Salesforce org limits (API usage, storage,
  streaming events) push via OTLP/HTTP, no scrape port needed.
- **PII redaction and sampling** — declarative hash/mask/drop-field/drop-row/regex transforms plus
  deterministic per-type sampling, all opt-in.
- **Cost controls** — sink rate caps and a daily byte budget with a lossless pause mode, so a
  runaway event category can't blow through your Loki bill unattended.

## Where to next?

| | |
|---|---|
| [Getting Started](getting-started.md) | Zero to first log lines in Loki |
| [Installation](installation.md) | Docker, docker-compose, uv/pipx |
| [Configuration](configuration/index.md) | Every config key, default, and env var |
| [Architecture](architecture.md) | Sources, sinks, checkpointing, HA |
| [Sources](sources/index.md) | Pub/Sub, SOQL-polled objects, EventLogFile, ApexLog |
