# Checkpoint inspect/repair runbook

`sf2loki state` (issue #63) gives operators a supported way to look at and repair
checkpoints in the CONFIGURED state store — file, s3, or gcs — instead of hand-editing
the file store's JSON (undocumented, risky, and racing the daemon's flock) or having no
option at all on s3/gcs. It's the tool of last resort for the `WatermarkStale` and
`PushesStalled` alerts (see [`alerts.md`](alerts.md)) once logs/permissions have been
ruled out and a checkpoint genuinely needs to be moved past a poison record.

## Command surface

```
sf2loki state show   [--key GLOB] --config config.yaml   # pretty-print checkpoints
sf2loki state set    KEY VALUE     --config config.yaml   # CAS-safe write of one key
sf2loki state delete  KEY          --config config.yaml   # remove a key
```

All three operate through the same `build_store` factory the daemon itself uses, so
they work identically against every backend. `--key` on `show` is an `fnmatch` glob
(default `*`, i.e. every key); values are never redacted — checkpoint state is not
secret.

**The file backend refuses to run unless the daemon is stopped.** Reading or writing
the file store takes the same exclusive `flock` the running daemon holds, so `state
show/set/delete` fails fast with an actionable error ("stop the daemon or pass
`--force`") rather than silently reading stale data or racing a write. Pass `--force`
to bypass the lock only when you are certain the daemon is not actually running against
that state file — bypassing it against a live daemon lets writes race and can clobber
checkpoints. The s3/gcs backends have no such lock (there's no sidecar file to hold),
but a write still goes through the same conditional (CAS) write the daemon uses; if
another writer — the live daemon, or a second operator — raced you, the command reports
a clear "another writer raced; retry" error instead of silently losing data.

## Identifying the stuck key from `watermark_ts`

The `sf2loki_watermark_timestamp_seconds` gauge (`source`, `object` labels) is the
signal behind the `WatermarkStale` alert. Its labels map directly to checkpoint keys:

| `source` label | `object` label | Checkpoint key |
|---|---|---|
| `eventlog_objects` | the SOQL-polled object name (e.g. `LoginEvent`) | `eventlog_objects:LoginEvent` |
| `eventlogfile` | the EventLogFile `EventType` (e.g. `ApiTotalUsage`) | `eventlogfile:ApiTotalUsage` |
| `apexlog` | `apexlog` | `apexlog` (fixed key) |

Pub/Sub streaming checkpoints (`pubsub:<topic>`) aren't covered by `WatermarkStale`
(that alert only tracks the SOQL-polling watermark) — a stuck Pub/Sub stream shows up
as `StreamDown`/`StreamStalls` instead, and its checkpoint key is
`pubsub:<topic path>` (e.g. `pubsub:/event/LoginEventStream`).

In a **multi-org** deployment every key above is additionally prefixed
`org=<name>:` for every org except the first configured one (which also keeps the
legacy unprefixed key as a migration fallback — see `state/org_view.py`). Run
`sf2loki state show` (optionally with `--key 'org=<name>:*'`) to see the exact key as
it actually exists in the configured store rather than guessing the prefix.

Backfill runs (`sf2loki backfill`) use a *separate* state file
(`<state file>-backfill<suffix>`) with keys shaped
`backfill:{interval}:{event_type}` (or `backfill:org={name}:{interval}:{event_type}`)
— point `--config` at a config whose `state.file.path` resolves to that file (or just
inspect the file directly) if a backfill run itself is stuck; it never shares state
with the live daemon.

## `set` vs `delete`, and the duplicate-window consequence

Both commands move a checkpoint past whatever bad/poison value is blocking progress;
they differ in where ingestion resumes from:

- **`state set KEY VALUE`** moves the checkpoint to an exact, known-good position (an
  ISO-8601 timestamp for SOQL-polled sources, a base64 `replay_id` for Pub/Sub). Use
  this when you know precisely which record is poisoning the poll/stream and want to
  resume immediately past it, with the smallest possible re-ingestion window (nothing
  before your chosen value is re-read; nothing after it is skipped).
- **`state delete KEY`** removes the checkpoint entirely. The affected source falls
  back to its configured preset/lookback window on its next poll/subscribe — e.g. a
  SOQL-polled object re-lists from `lookback_hours` ago, an EventLogFile source
  re-lists its configured backfill window, a Pub/Sub subscription starts a **fresh**
  subscribe (no replay position) and only sees events emitted from that point forward.
  Use this when you don't know a good resume value, or the checkpoint's own file/object
  is what's corrupt.

**Both are safe with respect to data loss, but not with respect to duplicates.**
sf2loki's ingestion is at-least-once, and Loki dedupes exact-duplicate log lines within
its per-stream out-of-order/reject window — so:

- `state set` to a timestamp/replay-id **earlier** than the true poison point
  re-ingests everything between the new value and the old (real) watermark — a bounded,
  predictable duplicate window you control by how far back you set it.
- `state delete` re-ingests the source's entire lookback/preset window from scratch —
  a duplicate window bounded by that source's configured lookback (typically hours),
  not by how long the checkpoint was actually stuck.
- For **Pub/Sub**, `state delete` is the more disruptive of the two: since there's no
  "preset window" concept, deleting the checkpoint drops the replay position entirely,
  so a subscription resumes from "now" — events between the last real commit and the
  delete are **not** re-ingested (an actual gap, not a duplicate). Backfill that gap
  via SOQL/EventLogFile for the topic and time window if it matters — the same
  procedure as the `ReplayFallback` alert already documents.

In all cases, prefer `state set` to a value you can justify over `state delete` when
you can identify one — it bounds the re-ingestion window instead of falling back to a
whole lookback period (or, for Pub/Sub, creating a real gap).
