# Sources

`sf2loki` ships four pluggable sources under `sources:` in `config.yaml`. Three of them cover the
same underlying Salesforce activity through different channels; the fourth is a standalone
opt-in.

| Source | Config key | Mechanism |
|---|---|---|
| Pub/Sub streaming | `sources.pubsub` | gRPC + Avro, real-time event stream (e.g. `/event/LoginEventStream`) |
| SOQL-polled objects | `sources.eventlog_objects` | SOQL polling of any queryable sObject on a timestamp watermark |
| EventLogFile (CSV) | `sources.eventlogfile` | lists + downloads `EventLogFile` CSV blobs, one row per `LogEntry` |
| ApexLog | `sources.apexlog` | Tooling API polling of Apex debug logs (opt-in, own category) |

See [Pub/Sub Streaming](pubsub.md), [SOQL-Polled Objects](eventlog-objects.md),
[EventLogFile](eventlogfile.md), and [ApexLog](apexlog.md) for per-source detail, plus
[PII Redaction & Sampling](pii-and-sampling.md) and [Cost Controls](cost-controls.md) for
cross-cutting controls every source shares.

## One category, one source

Salesforce frequently exposes the same underlying events through more than one channel —
`/event/LoginEventStream` (streamed), `LoginEvent` (stored, pollable), and the `Login`
EventLogFile are the *same records* in three different costumes. Ingesting one event category
from more than one source double-counts it in Loki.

!!! warning "Double-counting"
    Enabling `LoginEvent` under `eventlog_objects` *and* `Login` under `eventlogfile` ingests the
    same login activity twice. `sf2loki` refuses to start rather than silently double-count.

`sf2loki` enforces **one source per event category** with a fail-fast startup guard
(`src/sf2loki/sources/overlap.py`). It normalizes every enabled source's identifiers — Pub/Sub
topic basenames, stored object names, ELF `EventType`s — down to a canonical category by
stripping the `EventStream`/`EventStore`/`Event` suffix and lowercasing (`LoginEvent` and
`/event/LoginEventStream` both normalize to `login`). A small alias table
(`_CATEGORY_ALIASES`) covers stems that don't already match their category, e.g. `LoginHistory`
→ `login`. Startup fails with an `OverlapError` listing every colliding category if more than
one source feeds it.

Bypass the guard when the duplication is deliberate (for example, relying on Loki to drop
byte-identical entries, or intentionally running both a lean real-time stream and a richer
EventLogFile feed for the same category — they aren't byte-identical, so both flow):

```yaml
sources:
  allow_overlap: true
```

**Wildcard discovery is filtered at runtime, not caught by the startup guard.** Identifiers
discovered at runtime aren't visible to the startup check, so both wildcard sources filter
themselves against the categories owned by other configured sources: ELF discovery
(`event_types: ["*"]`) skips a discovered type whose category another source already owns, and
wildcard Pub/Sub topics (`topics: ["*"]`) are checked the same way — at startup and on every
periodic rediscovery pass (`sources.pubsub.rediscovery_interval`). Each skip is logged at INFO.
Two gaps remain: explicit-vs-explicit collisions are caught only by the startup guard, and an
**explicitly listed** Pub/Sub topic is never runtime-filtered — it either trips the guard at
startup or, under `allow_overlap: true`, is knowingly double-ingested.

Custom platform events (`/event/*__e`) and Change Data Capture (`/data/*ChangeEvent`) are
orthogonal to the RTEM/ELF categories and never collide with them — see
[Pub/Sub Streaming](pubsub.md#custom-platform-events-and-change-data-capture).

**Picking a source per category:**

- Use `pubsub` for low-latency, real-time categories that have a streaming topic.
- Use `eventlog_objects` for categories you'd rather poll than stream, or that have no streaming
  channel — including your own custom objects.
- Use `eventlogfile` for the ~70 EventType CSVs that aren't exposed as either a stream or a
  stored object (most Event Monitoring data).

## Label cardinality discipline

Whichever source you use, only a fixed set of labels is ever promoted to a Loki stream label —
a startup allowlist guard rejects any other static label
(`src/sf2loki/sinks/loki/labels.py:ALLOWED_LABELS`):

```text
job, service_name, source, event_type, sf_org_id, environment, org
```

Everything else — `user_id`, `source_ip`, `session_key`, custom-object fields, ELF columns —
goes into either the structured-metadata block or the JSON log line, never a label, so
per-user/per-IP/per-session values never multiply your stream count. Loki prices and indexes by
label cardinality, so this allowlist is the load-bearing control that keeps a busy Salesforce org
from turning into an unbounded number of streams.

`eventlogfile` has a deliberate, narrow escape hatch to promote a genuinely low-cardinality
column to a real label per event type — see
[EventLogFile: per-type routing](eventlogfile.md#per-type-routing). Adding a stream label
anywhere needs a deliberate reason.
