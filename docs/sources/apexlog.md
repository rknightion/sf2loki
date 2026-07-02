# ApexLog

`sources.apexlog` is an **opt-in, developer-focused** source that streams Apex debug logs
(`ApexLog`) into Loki via the Salesforce Tooling API. It sits outside the
[one-channel-per-category model](index.md#one-category-one-source): debug logs are their own
category (`event_type=apexlog`), don't overlap Pub/Sub / stored-object / EventLogFile data, and
are excluded from the overlap guard entirely. Disabled by default.

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

!!! note "sf2loki does not manage TraceFlags"
    `ApexLog` rows only exist while a `TraceFlag` is active for a user, and Salesforce retains
    them for roughly 24h. TraceFlags expire by design, and auto-renewing them is a compliance
    decision sf2loki deliberately leaves to you: enable debug logging out-of-band with
    `sf apex tail log` / `sf debug`, or Setup → Debug Logs, for the same user(s) listed under
    `users`. `sf2loki doctor` prints a `traceflags` WARN row when the source is enabled but no
    active TraceFlag exists — i.e. nothing is currently generating logs.

## What lands in Loki

One entry per log. The **raw debug-log body is the log line** (truncated by
`sink.loki.batch.max_line_bytes` like any other line), and the metadata — `LogUserId`,
`Operation`, `Status`, `Request`, `Application`, `DurationMilliseconds`, `Location`, `LogLength`,
`Id` — is attached as structured metadata (queryable, never a stream label; `Operation` is a URL
path and would explode label cardinality if promoted). Correlate an ApexLog entry to RTEM/ELF
events by searching the body — the `REQUEST_ID` that appears in EventLogFile rows also appears
inside the debug-log text.

## API cost

Each poll is one Tooling `SELECT` plus **one body download per new log**
(`GET /tooling/sobjects/ApexLog/<id>/Body`). `max_body_bytes` is the guard: a log whose
`LogLength` exceeds it skips the body download entirely (saving that API call) and still ships
the metadata line, flagged `body_skipped="true"` (`body_skip_reason="size"`). On a busy org with
verbose trace levels this can be a meaningful chunk of the daily API budget — keep `poll_interval`
sane, scope `users` tightly, and watch `apexlog_logs_ingested`, `apexlog_download_bytes`, and
`apexlog_bodies_skipped`.

## Config keys (`ApexLogConfig`)

| Key | Default | Notes |
|---|---|---|
| `enabled` | `false` | Enable the source. |
| `poll_interval` | `1m` | How often to poll for new `ApexLog` rows. |
| `lookback` | `1h` | Initial window on first run (no checkpoint). |
| `users` | `[]` | Usernames to filter on (`LogUser.Username`); empty = every `ApexLog` visible to the integration user. |
| `max_body_bytes` | `5242880` (5 MiB) | Skip the body download above this size. |
| `sample` | `1.0` | Keep-fraction, deterministic. See [PII Redaction & Sampling](pii-and-sampling.md). |

Watermark/resume works like the SOQL-polled object source: a `StartTime >=` cursor plus a rolling
`Id` dedup window (checkpoint key `apexlog`), so restarts resume without gaps or (beyond
at-least-once) duplicates. See [SOQL-Polled Objects](eventlog-objects.md) for the general
watermark mechanics this mirrors.
