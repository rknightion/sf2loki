---
title: Why This Shipper
description: A factual, dated account of how sf2loki differs from Salesforce Event Monitoring dashboards, a vendor SIEM connector, or a bespoke EventLogFile download script — and when to pick each.
---

# Why This Shipper

*Last reviewed: 2026-08.*

There is more than one way to get Salesforce Event Monitoring data somewhere useful, and for
some orgs the built-in answer is the whole answer. This page states what `sf2loki` does
differently and why, so you can decide whether it earns a service in your stack. It describes
this codebase; it makes no claims about how anything else is implemented.

## What already exists

**Salesforce's own Event Monitoring dashboards.** First-party, no infrastructure, and they
understand the data model better than any exporter will. They also live in Salesforce, which
is the wrong place if your logs, alerts and on-call workflow live somewhere else.

**A vendor SIEM connector.** If the destination is a SIEM and the requirement is compliance
retention, a supported connector is the shortest path and there is little reason to put
anything in between.

**A script that downloads EventLogFile CSVs on a cron.** For one event type and a modest
volume this is genuinely fine — it is small, and there is nothing to operate. Most of what
follows is about what stops being true once several event categories, streaming sources and a
Loki bill are involved.

## Design choices specific to this shipper

**Four source types behind one seam.** Pub/Sub streaming (gRPC + Avro), SOQL-polled objects,
EventLogFile CSV exports and ApexLog debug logs all arrive through a single async-iterator
interface. They have very different latency and completeness characteristics, and the
[Sources](sources/index.md) pages say which is which — the choice is documented as a decision
rather than left as whichever one you found first.

**Each event category is ingested from exactly one source.** The same activity is often
available through more than one path, so an either/or-per-category rule is enforced rather
than left to configuration discipline. Without it the same login appears twice from two
sources and every count downstream is quietly wrong — a failure that looks like data, not
like an error.

**A fixed Loki label allowlist, enforced at startup.** `user_id`, `source_ip`, `replay_id`
and similar are structural cardinality bombs in a Loki stream, so they cannot become labels;
they travel as structured metadata instead. The allowlist is checked when the process starts,
which means a misconfiguration fails immediately rather than after it has created a few
million streams. See [Cost Controls](sources/cost-controls.md).

**HA that respects the API's actual constraint.** Salesforce allows one Pub/Sub subscriber,
so the model is active-passive with a file lease or a Kubernetes `Lease`, not active-active.
A crashed leader fails over without a second instance double-delivering. See
[High Availability](deployment/high-availability.md).

**Checkpoints are a first-class concern.** Unattended operation means surviving restarts
without gaps or replays; [State & Checkpoints](deployment/state.md) covers what is persisted
and what a restart actually guarantees.

**Redaction and sampling before egress.** [PII Redaction & Sampling](sources/pii-and-sampling.md)
covers what can be stripped or thinned on the way out, which matters because Event Monitoring
data is user activity by definition.

## When to pick something else

**Compliance retention into a SIEM is the requirement.** Use the supported connector. Loki is
a log store optimised for querying by label, not an archive of record.

**You want one event type, occasionally.** A cron and a CSV download is less to run and less
to understand.

**Salesforce's own dashboards answer your questions.** Streaming, leases and a label
allowlist exist for continuous ingestion at volume with a cost ceiling. Below that they are
overhead.

**You need something the API does not expose.** The Salesforce API and your Event Monitoring
licensing are the ceiling — Real-Time Event Monitoring in particular is a licensed add-on,
and no shipper can produce events your org does not emit.

## See also

- [Sources](sources/index.md) — the four ingestion paths and their trade-offs
- [Architecture](architecture.md) — how the seam and the Loki writer fit together
- [Cost Controls](sources/cost-controls.md) — labels, cardinality and volume
- [High Availability](deployment/high-availability.md) — the active-passive model
- [Security](security.md) — credentials and scope
