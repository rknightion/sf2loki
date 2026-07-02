# PII redaction & sampling

Every source (`pubsub`, `eventlog_objects`, `eventlogfile`) can redact or filter each decoded
payload with declarative **transform rules**, and shed volume with deterministic **sampling**.
Both run at the source decode boundary — before field routing, label promotion, and timestamp
extraction — so a redacted column is redacted everywhere downstream: the JSON log line,
structured metadata, and the fallback timestamp.

## Transform rules

Configured per source under `sources.<source>.transforms`. Actions:

| Action | Behavior |
|---|---|
| `hash` | Salted SHA-256 → a stable 16-char pseudonym (correlatable within the deployment, not reversible without the salt). |
| `mask` | Format-aware: emails keep the domain (`alice@corp.com` → `***@corp.com`), IPv4 truncates to /24 (`203.0.113.7` → `203.0.113.x`), anything else → `***`. |
| `drop_field` | Remove the field entirely. |
| `regex_replace` | `pattern` → `replacement` (backreferences allowed); the pattern is validated to compile at config load. |
| `drop_row` | Drop the whole row/event when every `match` entry matches (`fnmatch` glob; a plain string is exact match). Counted in `sf2loki_rows_filtered{source, rule}`. |

Worked example — hash IPs, mask usernames, drop free-text SOQL, and drop a monitoring user's own
noise:

```yaml
sources:
  transform_salt_file: /etc/sf2loki/secrets/transform-salt   # strongly recommended for hash
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

!!! warning "Always salt hash rules"
    Set `sources.transform_salt` (or `transform_salt_file`) whenever any `hash` rule is
    configured. Unsalted hashes of low-entropy values (IPs, usernames) are trivially reversible
    by rainbow table. The salt is deployment-wide, so the same input hashes identically across
    sources and restarts — correlation stays intact.

`drop_field` of a column promoted to an ELF stream label is rejected at config load — it would
silently drop the label. Use `hash`/`mask` instead; a pseudonymised label is fine. See
[EventLogFile: per-type routing](eventlogfile.md#per-type-routing).

## Sampling

Keep-fraction in `(0, 1]`, deterministic by a stable per-row key (`replay_id` / record Id /
canonical JSON), so a **replay samples identically** and Loki's byte-identical dedup stays
intact. Sampling is applied *after* transforms. A sampled-out row is counted in
`sf2loki_entries_sampled_out{source, event_type}`.

| Where | Shape |
|---|---|
| `sources.eventlogfile.event_types[].sample` | Per ELF type; wildcard-discovered types inherit the `*` entry's `sample`. |
| `sources.eventlog_objects.objects[].sample` | Per polled object. |
| `sources.pubsub.sample` | `{topic-glob: rate}`, first matching glob wins. |

```yaml
sources:
  eventlogfile:
    event_types:
      - {name: "*", sample: 0.1}       # keep 10% of every discovered type
      - {name: "Login", sample: 1.0}   # ...but 100% of Login (explicit wins)
  pubsub:
    sample: {"/event/ApiEventStream": 0.25}
```

**Caveats and invariants:**

- Sampling and `drop_row` **still advance checkpoints**. A dropped Pub/Sub event commits its
  replay id via a checkpoint-only entry; a dropped SOQL-polled record still enters the dedup
  id-window; a dropped ELF row still lets the file's checkpoint advance. Nothing gets stuck
  re-fetching dropped data.
- Sampling is **lossy** — sampled-out data is gone, not delayed. Use the rate caps or daily byte
  budget in [Cost Controls](cost-controls.md) for lossless volume control instead.
- If a source's entire tail (the last file / page / stream chunk) is dropped/sampled-out, that
  segment's advanced checkpoint isn't persisted until the next emitted entry; on restart it's
  deterministically re-fetched and re-dropped (no data loss, minor rework).
- **Backfill** applies the same `sources.eventlogfile.transforms` (backfilled history never leaks
  fields the live path redacts) but does **not** sample — a backfill is an explicit, bounded
  operation; narrow it with `--since`/`--until`/`--event-types` instead.
