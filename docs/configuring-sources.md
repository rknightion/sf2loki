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

> **Wildcard caveat — discovered identifiers are filtered at discovery time.** Identifiers
> discovered at *runtime* are not visible to the startup guard, so both wildcards filter
> themselves: ELF discovery (`event_types: ["*"]`) skips discovered types whose category another
> source already owns, and wildcard-discovered **Pub/Sub topics** (`topics: ["*"]`) are likewise
> checked against the categories owned by the other configured sources — at startup *and* on every
> periodic re-discovery pass (`sources.pubsub.rediscovery_interval`); each skip is logged at INFO.
> What remains: **explicit-vs-explicit** collisions are caught only by the startup guard, and
> **explicitly listed Pub/Sub topics are never runtime-filtered** — an explicit topic either
> trips the guard at startup or (under `allow_overlap: true`) is knowingly double-ingested.

> **Change Data Capture caveat.** `/data/…ChangeEvent` topics are subscribable, but the
> `ChangeEventHeader.changedFields` / `nulledFields` bitmap fields are shipped as their raw
> encoded strings — sf2loki does not expand them into field-name lists. See
> [§8](#8-custom-platform-events--change-data-capture) for the full custom-event/CDC treatment.

**Picking a source per category:**
- Use `pubsub` for low-latency, real-time categories that have a streaming topic.
- Use `eventlog_objects` for categories you'd rather poll than stream, or that have no streaming
  channel — including custom objects (see §2).
- Use `eventlogfile` for the ~70 EventType CSVs that aren't exposed as either a stream or a stored
  object (most Event Monitoring data).

A fourth source, `sources.apexlog` (§7), is opt-in and **outside** this either/or-per-category
model: Apex debug logs are their own category with no overlap against the three above, so the
overlap guard doesn't apply to it.

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
- **BigObject/EventStore caveat**: objects in the stored RTEM family (`LoginEvent`, `ApiEvent`,
  `FileEventStore`, `*EventStore`, ...) are Salesforce Big Objects — they reject `ORDER BY ASC`.
  Set `big_object: true` on the object entry to poll them; the source then drains them
  newest-first (`ORDER BY timestamp_field DESC`) and re-sorts each cycle's window ascending
  before emitting, so watermark/dedup/checkpoint behavior matches standard objects. Leave the
  flag unset for standard/custom objects (`LoginHistory`, `MyAudit__c`, `SetupAuditTrail`), which
  use the `ASC` path. See
  [`examples/presets/event-log-objects.yaml`](../examples/presets/event-log-objects.yaml).
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

## 5. PII redaction & sampling

Every source (`pubsub`, `eventlog_objects`, `eventlogfile`) can redact/filter each
decoded payload with declarative **transform rules**, and shed volume with
deterministic **sampling**. Both run at the source decode boundary — *before*
field routing, label promotion, and timestamp extraction — so a redacted column
is redacted everywhere downstream (the JSON log line, structured metadata, and
the fallback timestamp).

### Transform rules

Configured per source under `sources.<source>.transforms`. Actions:

- `hash` — salted SHA-256 → a stable 16-char pseudonym (correlatable within the
  deployment, not reversible without the salt).
- `mask` — format-aware: emails keep the domain (`alice@corp.com` → `***@corp.com`),
  IPv4 truncates to /24 (`203.0.113.7` → `203.0.113.x`), anything else → `***`.
- `drop_field` — remove the field entirely.
- `regex_replace` — `pattern` → `replacement` (backreferences allowed); the
  pattern is validated to compile at config load.
- `drop_row` — drop the whole row/event when EVERY `match` entry matches
  (`fnmatch` glob; a plain string is exact match). Counted in
  `sf2loki_rows_filtered{source, rule}`.

Worked (GDPR-ish) example — hash IPs, mask usernames, drop internal SOQL text:

```yaml
sources:
  transform_salt_file: /etc/sf2loki/secrets/transform-salt   # STRONGLY recommended for hash
  eventlogfile:
    enabled: true
    event_types: ["Login", "API", "ApexExecution"]
    transforms:
      - action: hash
        fields: [SOURCE_IP, CLIENT_IP]
      - action: mask
        fields: [USER_NAME]
      - action: drop_field
        fields: [SOQL_QUERY]           # free-text SOQL can carry PII in literals
      - action: drop_row
        name: drop-monitoring-user     # -> rows_filtered{rule="drop-monitoring-user"}
        match: {USER_NAME: "monitoring@*"}
```

**Salt recommendation:** always set `sources.transform_salt` (or
`transform_salt_file`) when any `hash` rule is configured. Unsalted hashes of
low-entropy values (IPs, usernames) are trivially reversible by rainbow table.
The salt is deployment-wide, so the same input hashes identically across sources
and restarts (correlation stays intact).

**Label safety:** `drop_field` of a column promoted to an ELF stream label is
rejected at config load (it would silently drop the label). Use `hash`/`mask`
instead — a pseudonymised label is fine.

### Sampling (deterministic volume control)

Keep-fraction in `(0, 1]`, deterministic by a stable per-row key (replay_id /
record Id / canonical JSON), so a **replay samples identically** and Loki's
byte-identical dedup stays intact. Sampling is applied AFTER transforms. A
sampled-out row is counted in `sf2loki_entries_sampled_out{source, event_type}`.

- `sources.eventlogfile.event_types[].sample` — per ELF type. Wildcard-discovered
  types (`event_types: ["*"]`) inherit the `*` entry's `sample`.
- `sources.eventlog_objects.objects[].sample` — per polled object.
- `sources.pubsub.sample` — `{topic-glob: rate}`, first matching glob wins.

```yaml
sources:
  eventlogfile:
    event_types:
      - {name: "*", sample: 0.1}       # keep 10% of every discovered type
      - {name: "Login", sample: 1.0}   # ...but 100% of Login (explicit wins)
  pubsub:
    sample: {"/event/ApiEventStream": 0.25}
```

**Caveats / invariants:**

- Sampling and `drop_row` **still advance checkpoints**. A dropped Pub/Sub event
  commits its replay id via a checkpoint-only entry; a dropped ELO record still
  enters the dedup id-window; a dropped ELF row still lets the file's checkpoint
  advance. Nothing gets stuck re-fetching dropped data.
- Sampling is **lossy** — sampled-out data is gone, not delayed. Use rate caps /
  the daily byte budget (below) for lossless volume control.
- If a source's *entire* tail (the last file / page / stream chunk) is
  dropped/sampled-out, that segment's advanced checkpoint isn't persisted until
  the next emitted entry; on restart it is deterministically re-fetched and
  re-dropped (no data loss, minor rework).
- **Backfill:** the `backfill` command applies the same
  `sources.eventlogfile.transforms` (backfilled history never leaks fields the
  live path redacts) but does **not** sample — a backfill is an explicit,
  bounded operation; narrow it with `--since/--until/--event-types` instead.

## 6. Controlling cost

sf2loki gives you three independent egress controls under `sink.loki.egress`
(all OFF by default). They compose — you can run any combination.

### Rate caps (lossless, delayed)

`max_lines_per_second` and `max_bytes_per_second` are token buckets on what's
pushed to Loki (bytes counted pre-compression, the closest proxy to what a
Loki-based platform meters). When a flush would exceed a cap it sleeps the
shortfall rather than dropping anything. That backpressure is structural: a
throttled sink leaves the internal queue full, which suspends the source poll
loops / Pub/Sub flow-control credits — so Salesforce simply isn't asked for more
until the buckets refill. Nothing is lost; delivery is just paced. Set these to
smooth spikes or stay under a contracted ingest rate. `0` disables a cap.

### Daily byte budget (two modes)

`daily_byte_budget` caps pre-compression bytes pushed per UTC day. The used
counter is persisted in the state store (key `egress:budget`), so a restart
resumes the same day's total instead of resetting the cap. It rolls over at
00:00 UTC. sf2loki logs a WARNING at 80% and takes `budget_action` at 100%:

- **`pause` (default — lossless, delayed):** hold all pushes *and* checkpoint
  advances until the next UTC day. No data is lost — it stays unread on the
  Salesforce side and flows once the budget resets — but delivery is delayed,
  bounded only by Salesforce-side event retention (short for streaming/Pub/Sub;
  longer for EventLogFile). While paused, `/readyz` reports **degraded** (503)
  with a reason naming the resume date, so an orchestrator surfaces it; liveness
  (`/healthz`) stays green because a restart wouldn't help. `egress_paused` = 1.
- **`drop` (lossy, counted):** keep running and discard the over-budget batches.
  Checkpoints still advance (the data is deliberately gone, never retried), and
  every dropped entry is counted in `loki_entries_dropped{reason="budget"}`.
  Readiness is NOT degraded — dropping is the configured steady state.

### Sampling vs budget — pick the right lossiness

These solve different problems and combine well:

- **Sampling** (`sources.*.sample`, per event type) is *lossy and cheap*: it
  keeps a deterministic fraction of rows up front, so you pay for a smaller,
  representative stream continuously. Use it to permanently reduce volume of a
  high-volume, low-value event type. Sampled-out rows still advance checkpoints
  (they're gone by design).
- **Budget-pause** is *lossless but delayed*: you keep everything, and overflow
  is deferred to the next day rather than thinned. Use it as a hard daily cost
  ceiling for the whole deployment when you'd rather delay than drop.
- **Budget-drop** is the lossy sibling of pause — a ceiling that sheds load
  instead of delaying it, with the loss counted.

A common setup: sample the noisy types down to a sensible baseline, then put a
`pause` budget behind everything as a backstop so a traffic spike delays rather
than overspends. Rate caps sit underneath both, pacing the push so you never
burst past a per-second ceiling. Metrics to watch: `egress_budget_used_bytes`,
`egress_paused`, `entries_sampled_out`, `loki_entries_dropped{reason="budget"}`.

## 7. Apex debug logs (ApexLog)

`sources.apexlog` is an **opt-in, developer-focused** source that streams Apex
debug logs (`ApexLog`) into Loki via the Tooling API. It sits outside the
one-channel-per-category model above — debug logs are their own category
(`source="apexlog"`), don't overlap Pub/Sub / stored-object / EventLogFile data,
and are **disabled by default** (not part of the free-tier quickstart).

```yaml
sources:
  apexlog:
    enabled: true
    poll_interval: 1m
    lookback: 1h
    users: ["integration@example.com"]   # LogUser.Username filter; [] = all visible
    max_body_bytes: 5242880              # skip the body download above this (5 MiB)
    sample: 1.0
```

**Prerequisite — an active TraceFlag (sf2loki does NOT manage these).** `ApexLog`
rows only exist while a `TraceFlag` is active for a user, and Salesforce keeps
them for ~24h. TraceFlags expire by design, and auto-renewing them is a
compliance decision, so sf2loki deliberately leaves them to you: enable debug
logging out-of-band with e.g. `sf apex tail log` / `sf debug` or **Setup → Debug
Logs**, for the same user(s) you list under `users`. `sf2loki doctor` prints a
`traceflags` WARN row when the source is enabled but no active TraceFlag exists —
i.e. nothing is generating logs.

**What lands in Loki.** One entry per log: the **raw debug-log body is the log
line** (truncated by `sink.loki.batch.max_line_bytes` like any other line), and
the metadata — `LogUserId`, `Operation`, `Status`, `Request`, `Application`,
`DurationMilliseconds`, `Location`, `LogLength`, `Id` — is attached as structured
metadata (queryable, never a stream label; `Operation` is a URL path and would
explode label cardinality). Correlate an ApexLog to RTEM/ELF events by searching
the body — the `REQUEST_ID` that appears in EventLogFile rows also appears inside
the debug-log text.

**API cost (document + budget for it).** Each poll is one Tooling `SELECT` plus
**one body download per new log** (`GET /tooling/sobjects/ApexLog/<id>/Body`).
`max_body_bytes` is the guard: a log whose `LogLength` exceeds it skips the body
download entirely (saving that API call) and still ships the metadata line,
flagged `body_skipped="true"` (`body_skip_reason="size"`). On a busy org with
verbose trace levels this can be a meaningful chunk of the daily API budget —
keep `poll_interval` sane, scope `users` tightly, and watch
`apexlog_logs_ingested`, `apexlog_download_bytes`, and `apexlog_bodies_skipped`.

Watermark/resume works exactly like the stored-object source: a `StartTime >=`
cursor plus a rolling `Id` dedup window (checkpoint key `apexlog`), so restarts
resume without gaps or (beyond at-least-once) duplicates.

## 8. Custom platform events & Change Data Capture

The Pub/Sub source subscribes to **any explicit topic**, not only the RTEM
monitoring streams — so your own custom platform events and Change Data Capture
(CDC) channels stream to Loki with **no engine change**. Just list them under
`sources.pubsub.topics`. See
[`examples/presets/custom-platform-events.yaml`](../examples/presets/custom-platform-events.yaml).

**Channel name shapes:**

| Shape | Example | What it is |
| --- | --- | --- |
| `/event/<Name>__e` | `/event/Order_Shipped__e` | a custom platform event (your app's own event) |
| `/data/<Object>ChangeEvent` | `/data/AccountChangeEvent` | CDC on a standard object |
| `/data/<Object>__ChangeEvent` | `/data/Employee__ChangeEvent` | CDC on a custom object (`__c` → `__ChangeEvent`) |
| `/data/<Name>__chn` | `/data/MyChannel__chn` | a custom channel (a curated CDC/platform-event bundle) |

```yaml
sources:
  pubsub:
    enabled: true
    replay_preset: LATEST          # tip-only; use EARLIEST for a one-time ~72h catch-up
    topics:
      - /event/Order_Shipped__e
      - /data/AccountChangeEvent
    # do NOT use "*" here — that discovers RTEM streams, not your custom/CDC channels
```

**Allocations (budget for it).** Unlike the RTEM monitoring streams
(`LoginEventStream`, …), custom platform events and CDC events **count against
your org's event-delivery / event-publishing allocations**. A busy CDC object
(e.g. every `Account` write) can dwarf RTEM volume — subscribe to the specific
channels you need rather than `"*"`, and reach for `sources.pubsub.sample` (a
`topic-glob → keep-fraction` map, deterministic by `replay_id`) if one channel
is still too hot. Point at the daily byte budget / rate caps in [§6](#6-controlling-cost)
for the sink-side ceiling.

**CDC bitmap fields ship unexpanded.** `/data/…ChangeEvent` payloads carry
`ChangeEventHeader.changedFields` / `nulledFields` as their **raw encoded bitmap
strings** — sf2loki does not expand them into field-name lists. The rest of the
change event (the changed field values, `ChangeEventHeader.entityName`,
`changeType`, `commitTimestamp`, …) is shipped as-is. The event time comes from
`ChangeEventHeader.commitTimestamp` when there's no `EventDate`/`CreatedDate`.

**Overlap guard.** Custom-event and CDC categories are **orthogonal** to the
RTEM/ELF categories (a custom event `/event/Order_Shipped__e` normalises to
`order_shipped__e`; CDC `/data/AccountChangeEvent` to `accountchange`), so they
never collide with `Login`/`API`/etc. — you can stream them alongside any ELF or
stored-object ingestion without tripping the [either/or guard](#1-overview-three-sources-one-channel-per-category)
or setting `allow_overlap`.
