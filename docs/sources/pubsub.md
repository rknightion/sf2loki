# Pub/Sub streaming

`sources.pubsub` subscribes to the Salesforce Pub/Sub API — a gRPC bidirectional stream carrying
Avro-encoded events — for low-latency, real-time ingestion. Each configured topic runs its own
subscription task.

```yaml
sources:
  pubsub:
    enabled: true
    topics:
      - /event/LoginEventStream
    replay_preset: CUSTOM
```

## How it works

- **One task per topic.** Each topic in `topics` (or discovered via `"*"`) gets its own gRPC
  subscription that decodes Avro payloads against the schema the API returns inline.
- **Checkpointing.** Progress is tracked by `replay_id`, the opaque cursor the Pub/Sub API hands
  back per event. On restart, the source resumes from the last committed `replay_id` per topic —
  never a timestamp — so resumption is exact regardless of clock skew.
- **Flow control.** `default_num_requested` (1–100, default `100`) sets the credit batch size
  requested from the API; Salesforce clamps at 100 and rejects a higher ask. When the internal
  bridge queue backs up (sink outage, slow downstream), the source stops requesting new credits
  — backpressure propagates all the way to the gRPC stream rather than buffering unbounded
  events in memory. `bridge_max_bytes` (default 128 MiB) is the byte budget for that internal
  topic→pipeline bridge queue; `0` disables byte accounting.

## Config keys (`PubSubConfig`)

| Key | Default | Notes |
|---|---|---|
| `enabled` | `true` | Enable the source. |
| `endpoint` | `api.pubsub.salesforce.com:7443` | Pub/Sub API gRPC endpoint. |
| `topics` | `[]` | Explicit topics, or `["*"]` to discover every RTEM `*EventStream` channel the org exposes. |
| `include` / `exclude` | `["*"]` / `[]` | Operator glob filters applied to discovered/explicit topics. |
| `replay_preset` | `CUSTOM` | `LATEST` \| `EARLIEST` \| `CUSTOM`; falls back to `LATEST` when no stored `replay_id` exists. |
| `rediscovery_interval` | `1h` | How often wildcard discovery re-runs while the process is up; `0` discovers only at startup. |
| `default_num_requested` | `100` | Flow-control credit batch size (1–100). |
| `bridge_max_bytes` | `134217728` (128 MiB) | Byte budget for the internal topic→pipeline bridge queue; `0` disables. |
| `sample` | `{}` | Topic glob → keep-fraction map, deterministic by `replay_id`. See [PII Redaction & Sampling](pii-and-sampling.md). |
| `transforms` | `[]` | Redaction/filter rules. See [PII Redaction & Sampling](pii-and-sampling.md). |

`replay_preset: EARLIEST` is a one-time catch-up tool (Salesforce retains roughly 72h of replay
history for standard streams) — leave deployments on `LATEST`/`CUSTOM` day to day.

## Custom platform events and Change Data Capture

The Pub/Sub source subscribes to **any explicit topic**, not only the RTEM monitoring streams —
your own custom platform events and Change Data Capture (CDC) channels stream to Loki with no
engine change. See
[`examples/presets/custom-platform-events.yaml`](https://github.com/rknightion/sf2loki/blob/main/examples/presets/custom-platform-events.yaml).

| Shape | Example | What it is |
|---|---|---|
| `/event/<Name>__e` | `/event/Order_Shipped__e` | a custom platform event (your app's own event) |
| `/data/<Object>ChangeEvent` | `/data/AccountChangeEvent` | CDC on a standard object |
| `/data/<Object>__ChangeEvent` | `/data/Employee__ChangeEvent` | CDC on a custom object (`__c` → `__ChangeEvent`) |
| `/data/<Name>__chn` | `/data/MyChannel__chn` | a custom channel (a curated CDC/platform-event bundle) |

```yaml
sources:
  pubsub:
    enabled: true
    replay_preset: LATEST
    topics:
      - /event/Order_Shipped__e
      - /data/AccountChangeEvent
    # do NOT use "*" here — that discovers RTEM streams, not your custom/CDC channels
```

!!! warning "Allocations"
    Unlike the RTEM monitoring streams, custom platform events and CDC events count against your
    org's event-delivery / event-publishing allocations. A busy CDC object (every `Account`
    write, for example) can dwarf RTEM volume — subscribe to the specific channels you need
    rather than `"*"`, and use `sources.pubsub.sample` if one channel is still too hot. See
    [Cost Controls](cost-controls.md) for the sink-side ceiling.

!!! note "CDC bitmap fields ship unexpanded"
    `/data/…ChangeEvent` payloads carry `ChangeEventHeader.changedFields` / `nulledFields` as
    their raw encoded bitmap strings — sf2loki does not expand them into field-name lists. The
    rest of the change event (changed field values, `ChangeEventHeader.entityName`, `changeType`,
    `commitTimestamp`, …) ships as-is. Event time comes from `ChangeEventHeader.commitTimestamp`
    when there's no `EventDate`/`CreatedDate`.

Custom-event and CDC categories are orthogonal to the RTEM/ELF categories — `Order_Shipped__e`
normalizes to `order_shipped__e`, `AccountChangeEvent` to `accountchange` — so they never trip
the [either/or overlap guard](index.md#one-category-one-source).
