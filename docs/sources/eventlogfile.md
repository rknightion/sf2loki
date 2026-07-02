# EventLogFile

`sources.eventlogfile` ingests Salesforce's `EventLogFile` CSV exports — the workhorse source:
most Event Monitoring activity (~70 `EventType` values) surfaces only here, not as a stream or a
stored object.

```yaml
sources:
  eventlogfile:
    enabled: true
    interval: Hourly
    event_types:
      - Login
      - API
```

## How it works

1. **List.** A SOQL query pages `EventLogFile` rows (`page_size`, default `1000`) for the
   configured `interval`.
2. **Download.** Each listed `LogFile` blob is downloaded via REST.
3. **Parse — schema-agnostic.** The CSV is parsed from its own header row (or
   `LogFileFieldNames`) rather than a hardcoded per-`EventType` schema. There is no static
   ~70-type schema table to keep in sync when Salesforce adds or changes EventLogFile columns.
4. **Emit.** One Loki entry per CSV row (`LogEntry`), timestamped from `timestamp_column`
   (default `TIMESTAMP_DERIVED`).

Checkpointing is per file: `{last_created, ids}`, carried forward by a file's last row, so a
partially-consumed file resumes without re-emitting already-shipped rows and a crash mid-download
re-lists from the last committed file.

## Pick one interval

Ingest exactly **one** of `Hourly` or `Daily` — they're redundant copies of the same events, so
ingesting both double-counts. `Daily` is settled (~1 day lag) and works for every org; `Hourly` is
fresher but needs the Event Monitoring hourly opt-in in Setup, and some orgs generate *only*
`Daily` files. Check which your org produces before choosing `Hourly`.

`settle_window` guards against ingesting a half-written blob: files whose `CreatedDate` is newer
than `now - settle_window` are skipped until the next poll. It defaults to `5m` for `Hourly`
(hourly blobs can be listed while Salesforce is still writing them) and `0` for `Daily` (files
land long after the day closes); set it explicitly to override either.

## Wildcard discovery

Use `event_types: ["*"]` to discover and ingest every `EventType` the org produces for the
configured interval, re-checked each poll so newly enabled types appear with no restart. Use
`exclude` to drop categories owned by another source or high-volume types you don't want:

```yaml
sources:
  eventlogfile:
    enabled: true
    interval: Hourly
    event_types: ["*"]
    exclude: [Login]   # owned by eventlog_objects / pubsub instead
```

Explicit entries always win over discovered ones. Discovered types whose category another
enabled source already owns are skipped automatically — see
[the overlap guard's wildcard caveat](index.md#one-category-one-source).

## Per-type routing

Each item in `event_types` is either a bare string (uses the global
`sink.loki.structured_metadata_fields`, promotes no labels) or a per-type object overriding
`structured_metadata_fields` and/or `labels` for just that type:

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

`structured_metadata_fields` on a per-type entry can inherit (omit / `null`), suppress (`[]`), or
replace the global list for that type only. `labels` is the narrow escape hatch to promote a
genuinely low-cardinality ELF column to a real Loki stream label — config validation rejects
reserved label names and non-identifier names. See
[the label-cardinality discipline](index.md#label-cardinality-discipline) before reaching for it.

!!! warning "Label safety"
    `drop_field` of a column promoted via `labels` is rejected at config load — it would silently
    drop the label. Use `hash`/`mask` instead if you need to redact a promoted column; a
    pseudonymised label is fine. See [PII Redaction & Sampling](pii-and-sampling.md).

## Config keys (`EventLogFileConfig`)

| Key | Default | Notes |
|---|---|---|
| `enabled` | `false` | Enable the source. |
| `interval` | `Hourly` | `Hourly` \| `Daily` — pick one. |
| `event_types` | `[]` | Required when enabled — no sensible "all" default given ~70 types and the either/or model. |
| `exclude` | `[]` | EventTypes to skip when `event_types: ["*"]`; ignored otherwise. |
| `poll_interval` | `1h` | How often to list new files. |
| `lookback` | `24h` | Initial window on first run (no checkpoint). |
| `timestamp_column` | `TIMESTAMP_DERIVED` | Per-row event time column. |
| `page_size` | `1000` | SOQL `LIMIT` for the file-listing query. |
| `settle_window` | `5m` (Hourly) / `0` (Daily) | Skip files newer than `now - settle_window`. |
| `download_max_age` | `24h` | A file whose body keeps failing to download and is older than this is abandoned (checkpoint advances past it) so a permanently-missing file can't wedge the watermark. |
| `concurrency` | `4` | EventTypes processed concurrently per poll cycle; peak memory is roughly `concurrency x 8 MiB` of download spool. |
| `transforms` | `[]` | Redaction/filter rules. See [PII Redaction & Sampling](pii-and-sampling.md). |

## Login/audit categories via this source

The `Login` and `Logout` EventType CSVs cover the same activity as `LoginEvent` /
`/event/LoginEventStream` — enable exactly one channel per the
[either/or overlap guard](index.md#one-category-one-source).
