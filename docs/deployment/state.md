# State & Checkpoints

sf2loki resumes rather than replays from the beginning: per-topic `replay_id` and
per-object watermarks are durably checkpointed to the configured `state.store` backend
(`file`, `s3`, or `gcs` — see [Configuration](../configuration/index.md)
and the [`StateConfig` reference](../config-reference.md#stateconfig)). `sf2loki state`
gives operators a supported way to inspect and repair those checkpoints instead of
hand-editing the file store's JSON (undocumented, risky, and racing the daemon's
`flock`) or having no option at all on `s3`/`gcs`.

It's the tool of last resort for a stuck source once logs and permissions have been
ruled out and a checkpoint genuinely needs to be moved past a poison record — see
[Alerts](../observability/alerts.md) for the signals (`sf2loki ingest lag high` and
`sf2loki no recent Loki push` are the ones most likely to point you here) that a
checkpoint is stuck.

## Command surface

```bash
sf2loki state show   [--key GLOB] --config config.yaml   # pretty-print checkpoints
sf2loki state set    KEY VALUE     --config config.yaml   # CAS-safe write of one key
sf2loki state delete  KEY          --config config.yaml   # remove a key
```

All three operate through the same `build_store` factory the daemon itself uses, so
they work identically against every backend. `--key` on `show` is an `fnmatch` glob
(default `*`, every key); values are never redacted — checkpoint state is not secret.

## Backend behavior

| Backend | Concurrency control | Failure mode |
| --- | --- | --- |
| `file` | Exclusive `flock` held for the daemon's lifetime | `state show/set/delete` refuses to run while the daemon holds the lock, failing fast with an actionable error ("stop the daemon or pass `--force`") rather than reading stale data or racing a write. Pass `--force` only when you're certain the daemon isn't actually running against that state file. |
| `s3` | Conditional write (`If-Match` ETag, or `If-None-Match: *` for the first write) | No lock file to hold — a concurrent writer (the live daemon, or a second operator) racing the CLI gets a clear "another writer raced; retry" error instead of silently losing data. |
| `gcs` | Conditional write via generation precondition (`ifGenerationMatch`) | Same fail-fast shape as `s3`, using GCS's generation number instead of an ETag. |

## Identifying the stuck key from `watermark_ts`

The `sf2loki_watermark_timestamp_seconds` gauge (`source`, `object` labels) tracks
SOQL-polling progress. Its labels map directly to checkpoint keys:

| `source` label | `object` label | Checkpoint key |
| --- | --- | --- |
| `eventlog_objects` | the SOQL-polled object name (e.g. `LoginEvent`) | `eventlog_objects:LoginEvent` |
| `eventlogfile` | the EventLogFile `EventType` (e.g. `ApiTotalUsage`) | `eventlogfile:ApiTotalUsage` |
| `apexlog` | `apexlog` | `apexlog` (fixed key) |

Pub/Sub streaming checkpoints aren't covered by `sf2loki_watermark_timestamp_seconds`
(that gauge only tracks SOQL-polling watermarks) — their checkpoint key is
`pubsub:<topic path>` (e.g. `pubsub:/event/LoginEventStream`); a stuck Pub/Sub stream
instead shows up in the `sf2loki_last_push_success_timestamp_seconds`-based alerts.

In a **multi-org** deployment, every key above is additionally prefixed
`org=<name>:` for every org except the first configured one (which also keeps the
legacy unprefixed key as a migration fallback). Run `sf2loki state show` (optionally
with `--key 'org=<name>:*'`) to see the exact key as it actually exists in the
configured store rather than guessing the prefix.

Backfill runs (`sf2loki backfill`) use a *separate* state file
(`<state file>-backfill<suffix>`) with keys shaped `backfill:{interval}:{event_type}`
(or `backfill:org={name}:{interval}:{event_type}`) — point `--config` at a config whose
`state.file.path` resolves to that file (or just inspect the file directly) if a
backfill run itself is stuck; it never shares state with the live daemon.

## `set` vs `delete`, and the duplicate-window consequence

Both commands move a checkpoint past whatever bad/poison value is blocking progress;
they differ in where ingestion resumes from:

- **`state set KEY VALUE`** moves the checkpoint to an exact, known-good position (an
  ISO-8601 timestamp for SOQL-polled sources, a base64 `replay_id` for Pub/Sub). Use
  this when you know precisely which record is poisoning the poll/stream and want to
  resume immediately past it, with the smallest possible re-ingestion window.
- **`state delete KEY`** removes the checkpoint entirely. The affected source falls
  back to its configured preset/lookback window on its next poll/subscribe — a
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
  delete are **not** re-ingested (an actual gap, not a duplicate). Backfill that gap via
  SOQL/EventLogFile for the topic and time window if it matters.

In all cases, prefer `state set` to a value you can justify over `state delete` when
you can identify one — it bounds the re-ingestion window instead of falling back to a
whole lookback period (or, for Pub/Sub, creating a real gap).

## See also

- [High Availability](high-availability.md) — why the state store must be shared
  (`s3`/`gcs`) between replicas in an active-passive pair, and how fencing interacts
  with checkpoint commits.
- [Kubernetes](kubernetes.md) — the HA manifest example that requires a shared backend.
- [Alerts](../observability/alerts.md) — the shipped alert pack; the signals that
  typically precede a `sf2loki state` intervention.
- [Configuration reference](../config-reference.md) — `StateConfig` / `FileStateConfig`
  / `S3StateConfig` / `GcsStateConfig` field details.
