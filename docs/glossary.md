# Glossary

Salesforce Event Monitoring and Loki terms used throughout these docs.

RTEM (Real-Time Event Monitoring)
: Salesforce's streaming layer for security/activity events (logins, API
calls, report exports, ...), delivered over the Pub/Sub API as one topic
per event type (e.g. `/event/LoginEventStream`). Most RTEM channels require
the Shield Event Monitoring add-on. sf2loki's Pub/Sub source ingests these
directly, or the same underlying records can be reached via their stored
Big Object form or via EventLogFile — pick exactly one channel per category.

EventLogFile (ELF)
: A Salesforce object holding batched Event Monitoring data as downloadable
CSV blobs, generated Hourly or Daily per event type. Retention and the set
of available event types depend on entitlements: without Shield, only a
small free subset at Daily/1-day retention; with the Event Monitoring
add-on, the full ~70-type catalogue and up to 365 days. sf2loki's
EventLogFile source lists new files via SOQL, downloads and parses each CSV
schema-agnostically, and emits one Loki line per row.

Big Object
: A Salesforce storage type for very large, mostly-write-once datasets.
Salesforce persists the stored RTEM event family (`LoginEvent`, `ApiEvent`,
`FileEventStore`, `*EventStore`, ...) as Big Objects, queryable via SOQL but
with restrictions — notably no `ORDER BY ASC` and no aggregates. sf2loki's
event-log-objects source handles this with a `big_object: true` flag that
switches to a newest-first drain with a ratcheting bound.

Platform Event
: A custom, publish-subscribe message type you define in your org (API name
ending `__e`, e.g. `My_Event__e`), delivered over its own Pub/Sub topic
(`/event/My_Event__e`). Unlike RTEM streams, Platform Events count against
your org's event-delivery/publishing allocations.

Change Data Capture (CDC)
: A Salesforce feature that publishes change events (create/update/delete/
undelete) for standard or custom objects as they happen, over dedicated
Pub/Sub channels (`/data/AccountChangeEvent`, `/data/MyObject__ChangeEvent`)
or a curated custom channel (`/data/MyChannel__chn`). CDC's
`ChangeEventHeader.changedFields`/`nulledFields` bitmap fields are shipped
by sf2loki as their raw encoded strings, unexpanded.

Pub/Sub topic
: A named channel on Salesforce's Pub/Sub API (gRPC + Avro) that a client
subscribes to for streaming delivery — RTEM streams, Platform Events, and
CDC channels are all topics, distinguished only by their name prefix
(`/event/...` vs `/data/...`).

Replay id
: An opaque, monotonically increasing per-topic position marker Salesforce
returns with each Pub/Sub event. Storing the latest replay id lets a
subscriber resume a topic exactly where it left off after a restart or
reconnect, instead of replaying from the beginning or missing events.

Watermark
: The polling-source equivalent of a replay id: the timestamp of the latest
row successfully processed for a polled object or file type. Each poll
cycle queries strictly newer rows (`WHERE <timestamp_field> > :watermark`)
and only advances the watermark after a window is fully pushed to Loki, so
a crash mid-cycle re-queries from the last committed point rather than
skipping rows.

Checkpoint
: The durable record of ingestion progress — a per-topic replay id or a
per-object/file-type watermark — persisted to the configured state store
(local file, S3, or GCS) so a restart resumes without data loss or, in the
default case, without unbounded re-ingestion.

Shield / Event Monitoring / Threat Detection (entitlements)
: Salesforce add-on licences that gate most Event Monitoring functionality.
**Shield Event Monitoring** unlocks most RTEM streaming channels, the full
EventLogFile type catalogue, and extended ELF retention. **Threat
Detection** unlocks anomaly-detection channels (e.g. `ApiAnomalyEvent`).
Without these add-ons, an org still exposes a small free EventLogFile
subset (Login, Logout, API Total Usage, and a few others) at Daily
interval and 1-day retention.

Structured metadata
: A Loki feature (schema v13 + TSDB) for attaching arbitrary key/value data
to a log line without it becoming part of a stream's label set. sf2loki
routes every high-cardinality field (user ids, IP addresses, replay ids,
session keys, ...) here instead of onto labels — filterable and queryable,
at no stream-cardinality cost.

Stream / label cardinality
: In Loki, a **stream** is the unique set of label key/value pairs on a log
line; every distinct combination creates a new stream, and Loki's cost and
performance scale with stream count. sf2loki enforces a small, fixed label
allowlist (`job`, `service_name`, `source`, `event_type`, `sf_org_id`,
`environment`, `org`) precisely to keep cardinality bounded — a startup
guard rejects any other field configured as a label.
