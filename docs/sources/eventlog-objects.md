# SOQL-polled objects

`sources.eventlog_objects` polls **any queryable sObject** via SOQL on a timestamp watermark —
standard EventLog objects (`LoginEvent`, `ApiEvent`), standard security objects (`LoginHistory`,
`SetupAuditTrail`), or your own custom objects (e.g. `MyAudit__c`). Point it at an object plus a
datetime field that only ever increases, and it becomes a watermarked log feed.

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

## How it works

Each configured object is queried every `poll_interval` as:

```sql
SELECT FIELDS(ALL) FROM <name>
WHERE <timestamp_field> >= <watermark>
ORDER BY <timestamp_field> ASC
LIMIT 200
```

`FIELDS(ALL)` selects every field without hand-listing columns, but Salesforce requires
`LIMIT <= 200` with it — hardcoded by the source, not configurable. Throughput isn't capped at
200/interval, though: a full page triggers follow-up queries within the same cycle
(drain-until-short-page), so backlogs catch up in one poll.

The cursor is `>=` (not `>`) with a rolling record-Id dedup window, so records sharing the exact
boundary timestamp are never skipped. The watermark is the previous record's `timestamp_field`
value, persisted per-object under state key `eventlog_objects:<name>` as
`{"last_ts": ..., "ids": [...]}` (the Id list is the boundary dedup window) — a crash mid-poll
re-queries from the last committed watermark, so recovery is a gap-free re-fetch, never a gap in
coverage. Records whose `timestamp_field` comes back null/unparseable are still shipped but never
advance the watermark, and a garbage stored watermark falls back to `lookback` with a warning — a
bad value can't wedge the source in a malformed-query loop.

## Config keys (`EventLogObjectConfig`, per object)

| Key | Default | Notes |
|---|---|---|
| `name` | _required_ | The sObject API name — standard or custom (e.g. `MyAudit__c`). Must be a bare identifier. |
| `timestamp_field` | `EventDate` | Must be set to a real field on a custom object; `EventDate` only exists on EventLog objects. |
| `poll_interval` | `5m` | Duration shorthand (`5m`, `1h30m`), ISO-8601 (`PT5M`), or plain seconds. |
| `lookback` | `1h` | Initial query window when no checkpoint exists yet (first run, or after a watermark reset). |
| `big_object` | `false` | See [Big Objects](#big-objects-descending-drain) below. |
| `max_catchup_records` | `200000` | Cap on records buffered per `big_object` DESC drain cycle; `0` = unbounded. Ignored on the `ASC` path. |
| `sample` | `1.0` | Keep-fraction, deterministic by record Id. See [PII Redaction & Sampling](pii-and-sampling.md). |

Requirements: the object must be API-queryable via SOQL, and the integration user (the one
authenticated via JWT bearer / client_credentials) needs read access to it and to
`timestamp_field`.

`eventlog_objects` has no per-object structured-metadata override — it always routes fields via
the global `sink.loki.structured_metadata_fields` list, so add a custom object's high-cardinality
fields there if you want them filterable rather than buried only in the JSON line.

## Big Objects: descending drain

The stored RTEM event family (`LoginEvent`, `ApiEvent`, `FileEventStore`, Threat-Detection
`*EventStore`, …) are Salesforce **Big Objects**. Big Objects have restrictive SOQL: they reject
`ORDER BY ASC` (the index is DESC-only), expose no `nextRecordsUrl` pagination, and reject
`COUNT()`/aggregates. `FIELDS(ALL)` itself still works — only ascending order is the problem.

!!! note "`big_object: true`"
    Set `big_object: true` on any object entry backed by a Big Object. Leave it unset (the
    default) for standard/custom objects (`LoginHistory`, `MyAudit__c`, `SetupAuditTrail`), which
    use the plain `ASC` path.

With the flag set, `eventlog_objects_source._drain_big_object` pages **newest-first**
(`ORDER BY <timestamp_field> DESC`) with a ratcheting `<=` upper bound, dedups within the drain
and against the checkpoint's Id-window, then re-sorts each cycle's window ascending before
emitting — so watermark/dedup/checkpoint semantics match the `ASC` path exactly from the pipeline's
point of view. `max_catchup_records` bounds how much a single cycle buffers in memory to do that
re-sort; when a post-outage catch-up spans a large gap, the drain emits the sorted segment it has
and ratchets its upper bound down so catch-up proceeds in bounded chunks across cycles instead of
buffering unbounded history. Historical backfill beyond the poll window is a deferred follow-up,
not this path.

```yaml
sources:
  eventlog_objects:
    enabled: true
    objects:
      - name: LoginEvent
        timestamp_field: EventDate
        poll_interval: 5m
        lookback: 1h
        big_object: true
```

See [`examples/presets/event-log-objects.yaml`](https://github.com/rknightion/sf2loki/blob/main/examples/presets/event-log-objects.yaml).

## Login/audit categories via this source

`LoginEvent`, `LoginHistory`, and the ELF `Login` type all cover the same login activity — enable
exactly one per the [either/or overlap guard](index.md#one-category-one-source). `LoginHistory`
and `SetupAuditTrail` are standard objects, available without a Shield / Event Monitoring
entitlement; `LoginEvent` is a Big Object that typically requires one.
