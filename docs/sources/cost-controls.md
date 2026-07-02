# Cost controls

`sf2loki` gives you three independent egress controls under `sink.loki.egress` (all off by
default), plus the label-cardinality discipline that every source shares. They compose — run any
combination.

## Rate caps (lossless, delayed)

`max_lines_per_second` and `max_bytes_per_second` are token buckets on what's pushed to Loki
(bytes counted pre-compression, the closest proxy to what a Loki-based platform meters). When a
flush would exceed a cap it sleeps the shortfall rather than dropping anything.

That backpressure is structural: a throttled sink leaves the internal queue full, which suspends
the source poll loops / Pub/Sub flow-control credits — so Salesforce simply isn't asked for more
until the buckets refill. Nothing is lost; delivery is just paced. Set these to smooth spikes or
stay under a contracted ingest rate. `0` disables a cap.

## Daily byte budget (two modes)

`daily_byte_budget` caps pre-compression bytes pushed per UTC day. The used counter persists in
the state store (key `egress:budget`), so a restart resumes the same day's total instead of
resetting the cap. It rolls over at 00:00 UTC. sf2loki logs a WARNING at 80% and takes
`budget_action` at 100%:

- **`pause` (default — lossless, delayed).** Hold all pushes *and* checkpoint advances until the
  next UTC day. No data is lost — it stays unread on the Salesforce side and flows once the
  budget resets — but delivery is delayed, bounded only by Salesforce-side event retention (short
  for streaming/Pub/Sub; longer for EventLogFile). While paused, `/readyz` reports **degraded**
  (503) with a reason naming the resume date, so an orchestrator surfaces it; liveness
  (`/healthz`) stays green because a restart wouldn't help. `egress_paused` = 1.
- **`drop` (lossy, counted).** Keep running and discard the over-budget batches. Checkpoints still
  advance (the data is deliberately gone, never retried), and every dropped entry is counted in
  `loki_entries_dropped{reason="budget"}`. Readiness is not degraded — dropping is the configured
  steady state.

```yaml
sink:
  loki:
    egress:
      max_lines_per_second: 500
      max_bytes_per_second: 2_000_000
      daily_byte_budget: 5_000_000_000   # 5 GB/day
      budget_action: pause
```

## Config keys (`EgressConfig`)

| Key | Default | Notes |
|---|---|---|
| `max_lines_per_second` | `0` (disabled) | Token-bucket cap on pushed lines/second. |
| `max_bytes_per_second` | `0` (disabled) | Token-bucket cap on pushed (pre-compression) bytes/second. |
| `daily_byte_budget` | `0` (disabled) | Maximum pre-compression bytes pushed per UTC day. |
| `budget_action` | `pause` | `pause` \| `drop` — behavior when the daily budget is exhausted. |

## Sampling vs budget — pick the right lossiness

These solve different problems and combine well:

- **Sampling** (`sources.*.sample`, per event type — see [PII Redaction & Sampling](pii-and-sampling.md))
  is lossy and cheap: it keeps a deterministic fraction of rows up front, so you pay for a
  smaller, representative stream continuously. Use it to permanently reduce volume of a
  high-volume, low-value event type. Sampled-out rows still advance checkpoints.
- **Budget-pause** is lossless but delayed: you keep everything, and overflow is deferred to the
  next day rather than thinned. Use it as a hard daily cost ceiling for the whole deployment when
  you'd rather delay than drop.
- **Budget-drop** is the lossy sibling of pause — a ceiling that sheds load instead of delaying
  it, with the loss counted.

A common setup: sample the noisy types down to a sensible baseline, then put a `pause` budget
behind everything as a backstop so a traffic spike delays rather than overspends. Rate caps sit
underneath both, pacing the push so you never burst past a per-second ceiling. Metrics to watch:
`egress_budget_used_bytes`, `egress_paused`, `entries_sampled_out`,
`loki_entries_dropped{reason="budget"}`. See [Metrics Reference](../observability/metrics.md) for
the full instrument list.

## Cardinality is the other cost

Rate caps and budgets bound *volume*; they don't bound *cardinality*, which is usually the more
expensive dimension on a Loki-based platform. The fixed label allowlist
(`job`, `service_name`, `source`, `event_type`, `sf_org_id`, `environment`, `org`) is what keeps
per-user/per-IP/per-session values out of your stream count in the first place — see
[the label-cardinality discipline](index.md#label-cardinality-discipline). Before reaching for a
rate cap or budget to control cost, check whether an accidental per-type `labels:` override
(see [EventLogFile: per-type routing](eventlogfile.md#per-type-routing)) is the real driver of
spend — a single high-cardinality label multiplies stream count far more than raw byte volume
does.
