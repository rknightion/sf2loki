# Configuring sources

How-to recipes for `sources.*` in `config.yaml`. For the full schema and design rationale see
[DESIGN.md](../DESIGN.md) §7, §8, §10, §11 and [config.example.yaml](../config.example.yaml).

## 1. Overview: three sources, one channel per category

`sf2loki` ships three pluggable sources, configured under `sources:`:

| source | config key | mechanism |
|---|---|---|
| Pub/Sub streaming | `sources.pubsub` | gRPC + Avro, real-time event stream (e.g. `/event/LoginEventStream`) |
| Stored object polling | `sources.eventlog_objects` | SOQL polling of any queryable sObject on a timestamp watermark |
| EventLogFile (CSV) | `sources.eventlogfile` | lists + downloads `EventLogFile` CSV blobs, one row per `LogEntry` |

Salesforce frequently exposes the **same underlying events** through more than one of these
channels — `/event/LoginEventStream` (streamed), `LoginEvent` (stored, pollable), and the `Login`
EventLogFile are the *same records* in three different costumes. Ingesting one event category from
more than one source double-counts it in Loki.

`sf2loki` enforces **one source per event category** with a fail-fast startup guard
(`src/sf2loki/sources/overlap.py`): it normalizes every enabled source's identifiers (Pub/Sub
topic basenames, stored object names, ELF `EventType`s) down to a canonical category (stripping
`EventStream`/`EventStore`/`Event` suffixes and lowercasing) and refuses to start if a category
maps to more than one source. For example, enabling `LoginEvent` under `eventlog_objects` *and*
`Login` under `eventlogfile` both resolve to category `login` and trip the guard.

If you've deliberately accepted the duplication (relying on Loki to drop byte-identical entries,
best-effort), bypass it:

```yaml
sources:
  allow_overlap: true
```

> **Wildcard caveat — the guard only sees what's configured at startup.** Identifiers discovered
> at *runtime* are not visible to the startup guard. ELF wildcard discovery
> (`event_types: ["*"]`) compensates by skipping discovered types whose category another source
> already owns, but wildcard-discovered **Pub/Sub topics** (`topics: ["*"]`) get no such runtime
> check — combining `topics: ["*"]` with `event_types: ["*"]` (or with explicit types) can still
> double-ingest a category the guard never saw. Prefer an explicit list on at least one side.

> **Change Data Capture caveat.** `/data/…ChangeEvent` topics are subscribable, but the
> `ChangeEventHeader.changedFields` / `nulledFields` bitmap fields are shipped as their raw
> encoded strings — sf2loki does not expand them into field-name lists.

**Picking a source per category:**
- Use `pubsub` for low-latency, real-time categories that have a streaming topic.
- Use `eventlog_objects` for categories you'd rather poll than stream, or that have no streaming
  channel — including custom objects (see §2).
- Use `eventlogfile` for the ~70 EventType CSVs that aren't exposed as either a stream or a stored
  object (most Event Monitoring data).

## 2. Custom object polling

`eventlog_objects` is not limited to Salesforce's built-in EventLog objects — it polls **any
queryable sObject** via SOQL on a timestamp watermark, including your own custom objects (e.g.
`MyAudit__c`). This provides general-purpose custom object log collection: point the source at an
object + a datetime field that advances monotonically, and it becomes a watermarked log feed.

Each configured object is queried every `poll_interval` as:

```sql
SELECT FIELDS(ALL) FROM <name>
WHERE <timestamp_field> >= <watermark>
ORDER BY <timestamp_field> ASC
LIMIT 200
```

`FIELDS(ALL)` selects every field on the object without hand-listing columns, but it is a
Salesforce convenience that **requires `LIMIT <= 200`** — that limit is hardcoded by the source
and is not configurable. Throughput is *not* capped at 200 per interval, though: a full page
triggers follow-up queries within the same cycle (drain-until-short-page), so backlogs catch up
in one poll. The cursor is `>=` (not `>`) with a rolling record-Id dedup window, so records that
share the exact boundary timestamp are never skipped.

```yaml
sources:
  eventlog_objects:
    enabled: true
    objects:
      - name: MyAudit__c
        timestamp_field: LastModifiedDate   # or CreatedDate — pick a field that only increases
        poll_interval: 5m
        lookback: 1h
```

Config keys (`EventLogObjectConfig`):
- `name` (required) — the sObject API name (standard or custom, e.g. `MyAudit__c`).
- `timestamp_field` (default `EventDate`) — must be set to a real field on a custom object;
  `EventDate` only exists on EventLog objects.
- `poll_interval` (default `5m`) — accepts duration shorthand (`5m`, `1h30m`), ISO-8601 (`PT5M`),
  or plain seconds.
- `lookback` (default `1h`) — initial query window when no checkpoint is stored yet (first run, or
  after a watermark reset).

Requirements and caveats:
- The object must be **API-queryable** via SOQL, and the integration user (the one authenticated
  via the JWT bearer flow) needs **read access** to it and to `timestamp_field`.
- **BigObject/EventStore caveat**: objects in the EventStore family (e.g. `ApiEventStream`) have
  restrictive SOQL support — `FIELDS(ALL)` and `ORDER BY` may not work on them. This source is
  built for standard queryable sObjects (custom objects, `LoginEvent`, `SetupAuditTrail`, etc.);
  EventStore BigObjects are out of scope.
- The watermark is the previous record's `timestamp_field` value, stored per-object under state
  key `eventlog_objects:<name>` as JSON (`{"last_ts": ..., "ids": [...]}` — the Id list is the
  boundary dedup window); a crash mid-poll re-queries from the last committed watermark
  (gap recovery, never a gap in coverage). Pre-existing bare-timestamp checkpoints are read
  transparently and upgraded on the next commit.
- Records whose `timestamp_field` comes back null/unparseable are still shipped but never advance
  the watermark, and a garbage stored watermark falls back to `lookback` with a warning — a bad
  value can't wedge the source in a malformed-query loop.

## 3. Login history & setup audit trail

Several common security/audit categories can be ingested more than one way. Pick exactly one per
category per the overlap rule in §1.

### LoginHistory (standard object, polled)

`LoginHistory` is a standard queryable object — no Shield/Event Monitoring entitlement required,
just standard API access. The overlap guard maps it to the same `login` category as
`LoginEvent`, `/event/LoginEventStream`, and the ELF `Login` type — it covers the same login
activity, so enable exactly one of them.

```yaml
sources:
  eventlog_objects:
    enabled: true
    objects:
      - name: LoginHistory
        timestamp_field: LoginTime
        poll_interval: 5m
        lookback: 1h
```

### SetupAuditTrail (standard object, polled)

`SetupAuditTrail` is also a standard object, available without Event Monitoring add-ons.

```yaml
sources:
  eventlog_objects:
    enabled: true
    objects:
      - name: SetupAuditTrail
        timestamp_field: CreatedDate
        poll_interval: 5m
        lookback: 1h
```

### LoginEvent (real-time object) — pick ONE channel

`LoginEvent` is a **Real-Time Event Monitoring (RTEM)** object and typically requires a Shield /
Event Monitoring entitlement. It's available through two channels that carry the *same* records —
ingest from only one:

Streamed (`pubsub`):

```yaml
sources:
  pubsub:
    enabled: true
    topics:
      - /event/LoginEventStream
```

Polled (`eventlog_objects`):

```yaml
sources:
  eventlog_objects:
    enabled: true
    objects:
      - name: LoginEvent
        timestamp_field: EventDate   # the default
        poll_interval: 5m
        lookback: 1h
```

The overlap guard maps both `/event/LoginEventStream` and `LoginEvent` to category `login` — if
you enable both, startup fails with an `OverlapError` listing the collision. This also means you
cannot combine either of these with the `Login` EventLogFile type (§below) — they're the same
category too.

### Login / Logout EventLogFile types (CSV, polled)

The `Login` and `Logout` EventType CSVs are part of standard Event Monitoring EventLogFile
ingestion (`eventlogfile`), separate from `LoginEvent`/`LoginEventStream` only in delivery
mechanism — they cover the same underlying login activity, so the overlap guard treats `Login`
(via `eventlogfile`) as the same `login` category as `LoginEvent`/`LoginEventStream`.

```yaml
sources:
  eventlogfile:
    enabled: true
    interval: Hourly
    event_types:
      - Login
      - Logout
    poll_interval: 1h
    lookback: 24h
```

**Entitlements summary:**
- `LoginHistory`, `SetupAuditTrail` — standard objects, no Event Monitoring entitlement needed.
- `LoginEvent` / `/event/LoginEventStream` / EventLogFile `Login`/`Logout` — Real-Time/standard
  Event Monitoring categories that typically require a Shield or Event Monitoring add-on license.

## 4. Cardinality note

Whichever source you use, **only the fixed label set** (`job`, `source`, `event_type`,
`sf_org_id`, `environment`) is ever promoted to a Loki stream label by default — a startup
allowlist guard rejects any other static label. Everything else — `user_id`, `source_ip`,
`session_key`, custom-object fields, ELF columns — goes into either the structured-metadata block
or the JSON log line, never a label, so per-user/per-IP/per-session values never multiply your
stream count.

Control what gets promoted to structured metadata (filterable, but not cardinality-priced) via
`sink.loki.structured_metadata_fields`:

```yaml
sink:
  loki:
    structured_metadata_fields:
      - replay_id
      - schema_id
      - event_uuid
      - user_id
      - username
      - source_ip
      - session_key
```

Any field in that list that's present and non-null on a given record is promoted to structured
metadata; everything else stays in the JSON line.

For `eventlogfile`, this can be overridden **per event type** — set
`structured_metadata_fields` on a per-type entry to inherit (`null`/omit), suppress (`[]`), or
replace the global list for just that type, and optionally promote a genuinely low-cardinality
column to a real stream label via `labels` (an explicit escape hatch — config validation rejects
reserved label names and non-identifier names):

```yaml
sources:
  eventlogfile:
    enabled: true
    event_types:
      - Login                       # bare string: inherits the global structured_metadata_fields
      - name: ReportExport
        structured_metadata_fields: [REPORT_ID, OWNER_ID]
        labels: [DELEGATED_USER]    # only ever promote LOW-cardinality columns here
```

`eventlog_objects` has no per-object structured-metadata override — it always uses the global
`sink.loki.structured_metadata_fields` list via the same field-routing logic, so a custom object's
high-cardinality fields (e.g. a custom `User__c` lookup) should be added to that global list if you
want them filterable rather than buried only in the JSON line.
