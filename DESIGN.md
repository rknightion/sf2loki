# sf2loki — Design

A long-running Python service that ingests Salesforce Event Monitoring data — Real-Time
Event Monitoring (RTEM) streaming events, stored RTEM/event objects, and (later) EventLogFile —
and pushes it to Grafana Loki with strict label-cardinality discipline.

Targets Grafana Cloud Loki and self-hosted Loki / local Alloy (`loki.source.api`). Ships as a
single container image, run via Docker / Docker Compose.

---

## 1. Goals & non-goals

**Goals**
- Stream Salesforce RTEM events via the Pub/Sub API (gRPC + Avro) into Loki, resumably.
- Poll stored event objects via SOQL and ingest EventLogFile (CSV) as **alternative**
  per-category channels — each event category is ingested from exactly ONE source
  (either/or), never streamed-and-polled, so events are not double-counted.
- Push to Loki with a small, fixed, low-cardinality label set; everything high-cardinality goes
  in the JSON log line and/or **structured metadata** — never labels.
- Be a well-behaved, self-observable, containerised service: `/metrics`, `/healthz`, `/readyz`,
  structured logs, retries, graceful shutdown.
- Pluggable **sources** behind one interface and a pluggable **sink** (Loki now, OTLP later).

**Non-goals (now)**
- Multi-replica horizontal scale-out of a single topic (Pub/Sub has no consumer-group semantics —
  see §13). Single replica + a coordinator seam for future active-passive failover.
- EventLogFile ingestion is **stubbed** (§7), designed to drop in as another source.
- Exactly-once delivery. We are **at-least-once**; Loki absorbs duplicate identical entries.

---

## 2. Stack

- **Python 3.12**, **asyncio** throughout. Pub/Sub gRPC streaming (`grpc.aio`), Loki HTTP push
  (`httpx`), and SOQL polling are all I/O-bound — one event loop, `uvloop`, no thread/GIL contention.
- **uv** (deps + lockfile), **ruff**, **mypy --strict**, **pytest** + **pytest-asyncio**, **just**.
- Runtime deps: `grpcio` / `grpcio-tools`, `fastavro`, `httpx`, `pydantic` + `pydantic-settings`,
  `pyjwt[crypto]`, `cryptography`, `protobuf`, `cramjam` (snappy), `opentelemetry-sdk` +
  `opentelemetry-exporter-otlp-proto-http`, `structlog`,
  `tenacity`, `uvloop`.

Why Python over Go (the `genai-otel-bridge` reference is Go): the workload is modest-volume and
I/O-bound (GIL/footprint are not constraints), the specified toolchain is Python, and Salesforce's
**official Pub/Sub client example is Python** (grpcio + avro), which de-risks the trickiest path.

---

## 3. Architecture

Composition-root + frozen-seam design lifted from `genai-otel-bridge`: `cmd`/entrypoint wires
everything; all logic lives behind locked interfaces so implementations can be added without
touching the core.

```
                         ┌──────────────────────────────────────────────┐
                         │                  app.Pipeline                  │
  Salesforce             │                                                │
  ┌───────────┐ gRPC     │  ┌────────────┐   LogEntry   ┌──────────────┐ │   HTTP push
  │ Pub/Sub   │────────► │  │  Source(s) │ ───────────► │   Batcher    │ │ ──────────────► Loki
  │  API      │ Avro     │  │  (async    │   (stream)   │  + Sink.push │ │  (protobuf+snappy
  └───────────┘          │  │  iterators)│              └──────┬───────┘ │   or JSON+gzip)
  ┌───────────┐ REST/    │  └─────┬──────┘                     │ on success
  │ SOQL /    │ SOQL     │        │ events()                   ▼          │
  │ REST API  │────────► │        │                   CheckpointStore.commit
  └───────────┘          │        │                   (replay_id / watermark)
                         └────────┼───────────────────────────────────────┘
                                  │
                    obs: /metrics /healthz /readyz · structlog
```

**Data flow.** Each `Source.events()` yields `LogEntry` objects (already shaped: timestamp, labels,
JSON line, structured metadata, and a `CheckpointToken`). The shared `Pipeline` batches by
size/bytes/interval, calls `Sink.push(batch)`, and on success commits the latest `CheckpointToken`
per key. **Backpressure is structural**: a slow sink suspends consumption of the async generator →
the Pub/Sub source stops topping up flow-control credits → Salesforce stops sending. No silent drops;
lag metrics rise instead (`genai-otel-bridge`'s "block-on-full, never silent loss", via generator
suspension rather than a bounded channel).

---

## 4. Frozen seams

These signatures are the contract every lane codes against. Changing them later is expensive, so
they are locked here.

### `model.py` — vendor-neutral types
```python
@dataclass(frozen=True, slots=True)
class CheckpointToken:
    key: str      # "pubsub:/event/LoginEventStream" | "eventlog_objects:LoginEvent"
    value: str    # base64(replay_id) for streaming; ISO-8601 EventDate for polling

@dataclass(slots=True)
class LogEntry:
    timestamp: datetime                  # event occurrence time (EventDate/CreatedDate)
    labels: Mapping[str, str]            # low-cardinality only; validated against allowlist
    line: str                            # canonical JSON of the full decoded event
    structured_metadata: Mapping[str, str]
    checkpoint: CheckpointToken

@dataclass(slots=True)
class Batch:
    entries: list[LogEntry]
```
Per-key monotonicity is guaranteed by each source emitting in order (replay_id is monotonic per
topic; SOQL is `ORDER BY EventDate`), so "commit the last flushed token per key" is a correct resume
point without explicit sequence numbers.

### `sources/base.py` — producer
```python
class Source(Protocol):
    name: str                            # "pubsub" | "eventlog_objects" | "eventlogfile"
    def events(self, state: "CheckpointStore",
               stop: asyncio.Event) -> AsyncIterator[LogEntry]: ...
```

### `sinks/base.py` — consumer
```python
class RetryableSinkError(Exception): ...     # sink's own retry budget exhausted → Pipeline backs off, retries
class PermanentSinkError(Exception): ...      # 400 / unsplittable 413 → Pipeline drops batch, counts gap, advances

class Sink(Protocol):
    async def push(self, batch: Batch) -> None: ...
    async def aclose(self) -> None: ...
```
The sink does its own bounded internal retries (tenacity); it only raises when it cannot make
progress. Drop-and-advance on `PermanentSinkError` mirrors `genai-otel-bridge`'s reject handling
(never stall the whole pipeline on one poison batch; the gap is counted and alertable).

### `state/base.py` — resume state
```python
class CheckpointStore(Protocol):
    async def load(self, key: str) -> str | None: ...
    async def commit(self, key: str, value: str) -> None: ...
```
Implementation: `file_store` (JSON on a mounted volume, atomic temp-then-rename).

### `coordinate/base.py` — leadership (no-op now, HA later)
```python
class Coordinator(Protocol):
    async def run(self, *, on_acquire, on_lose, stop: asyncio.Event) -> None: ...
```
`NoopCoordinator` acquires immediately (single instance = always leader). A lease-based
implementation can be added later for active-passive failover with **zero** changes to sources/sink.

---

## 5. Salesforce auth — OAuth (JWT bearer or client credentials)

`auth/jwt_auth.py`, server-to-server, no interactive login. Two flows, selected by
`salesforce.auth_mode`:

- **`jwt_bearer`** (default) — private-key-signed assertion (steps below). Most secure: no shared
  secret leaves the secret store.
- **`client_credentials`** — consumer key + `client_secret` (no keypair, cert, or user
  pre-authorisation); the External Client App's **Run As** user supplies identity + permissions. The
  grant body is `grant_type=client_credentials` with `client_id`/`client_secret`; no JWT is minted.
  Same `TokenProvider`, same downstream access token (works unchanged for Pub/Sub, REST/SOQL, ELF).

`salesforce.environment` (`production`|`sandbox`) derives the login URL; an explicit `login_url`
(custom My Domain) overrides it. JWT bearer flow:

1. Mint an RS256 JWT: `iss`=External-Client-App consumer key, `sub`=integration username,
   `aud`=login URL (`https://login.salesforce.com`, `test.salesforce.com`, or the My Domain URL),
   `exp`=now+~3 min. Sign with the private key (file- or env-injected).
2. POST to `{login_url}/services/oauth2/token`, `grant_type=urn:ietf:params:oauth:grant-type:jwt-bearer`,
   `assertion=<jwt>` → `{access_token, instance_url}`.
3. The JWT flow returns **no refresh token** — re-mint a JWT and re-request on expiry or on 401.
4. **org id (`tenantid`)** for Pub/Sub metadata is resolved once via
   `GET {instance_url}/services/oauth2/userinfo` → `organization_id`. Set `salesforce.org_id`
   in config to skip this call entirely (and drop the `openid` scope — see below).

**App config (External Client App — the path Salesforce recommends/now requires for new apps):**
- **OAuth scopes: `api` + `refresh_token` (offline_access).** `api` covers REST/SOQL, the EventLogFile
  `/LogFile` blob download, and the Pub/Sub API (which has no scope of its own — it authenticates with a
  plain access token in the gRPC `accesstoken` header). `refresh_token` is **required by Salesforce's
  pre-authorized JWT bearer path** even though the flow never issues or uses a refresh token — without
  it the grant fails `invalid_request: "refresh_token scope is required and the connected app should be
  installed and preauthorized"` (verified against a dev org). `openid` is needed **only if** `org_id` is
  left unset (for the `/userinfo` call) — prefer setting `org_id` to avoid it. Everything else (`web`,
  `full`, `chatter_api`, `visualforce`, `id`, the `cdp_*`/`sfap_api`/`interaction_api` Data Cloud
  scopes) stays off.
- **Flow Enablement: enable JWT Bearer Flow only.** Leaving Client-Credentials / Auth-Code / Device /
  Token-Exchange off means the app can only mint tokens our one way — attack-surface reduction at no
  cost.
- **Security toggles:** the three defaults (`Require secret for Web Server Flow` / `…Refresh Token
  Flow` / `Require PKCE`) govern flows we don't use — harmless, leave ticked. The refresh-token
  controls (rotation, idle-TTL, IP allowlist) are moot (no refresh token). "Issue JWT-based access
  tokens for named users" changes the access-token *format* (≠ JWT bearer auth) — leave off; opaque,
  server-revocable tokens are preferable here.
- **Policies tab (admin-owned):** Permitted Users = *Admin approved users are pre-authorized*
  (mandatory for JWT bearer) + a permission set on the integration user; add a login-IP restriction
  there if egress IPs are stable. Upload the X.509 public cert under OAuth Settings → JWT Bearer Flow.

```python
class TokenProvider:
    async def token(self) -> AccessToken          # cached; proactive + reactive (401) refresh
    async def org_id(self) -> str
@dataclass
class AccessToken: value: str; instance_url: str; expires_at: datetime
```

---

## 6. Phase 1 — Pub/Sub streaming → Loki

`salesforce/pubsub_client.py`, `salesforce/avro_codec.py`, `sources/pubsub_source.py`.

- **Stubs**: vendored `proto/pubsub_api.proto` (from forcedotcom/pub-sub-api) → generated
  `salesforce/_generated/pubsub_api_pb2{,_grpc}.py` via `just proto` (committed, not built at runtime).
- **Transport**: `grpc.aio.secure_channel("api.pubsub.salesforce.com:7443", …)`. Per-call metadata:
  `accesstoken`, `instanceurl`, `tenantid`.
- **Subscribe** (bidi streaming): send `FetchRequest{topic_name, replay_preset, replay_id,
  num_requested=N}`; receive `FetchResponse{events[], latest_replay_id, pending_num_requested}`.
  **Flow control**: top up credits (send another `FetchRequest`) as `pending_num_requested` drains
  below a low-watermark, so the bus stays fed but bounded.
- **Decode**: each `event.payload` is Avro-decoded with the schema fetched via `GetSchema{schema_id}`,
  **cached by `schema_id`** (immutable per id) in `avro_codec`.
- **Multiplexing**: one asyncio task per topic, each owning its `Subscribe` stream + flow-control
  loop, decoding into a shared bounded `asyncio.Queue` that `events()` drains. Per-topic `replay_id`
  committed independently. Queue-full → topic tasks pause → fewer credits topped up → Salesforce
  backpressure.
- **Resume**: `replay_id` per topic persisted on checkpoint commit. Restart →
  `replay_preset=CUSTOM` from the stored id; if none, fall back to `LATEST` (configurable). A restart
  gap longer than Pub/Sub retention (≈24–72 h) loses the in-between events for that topic (counted via
  the replay-commit-age metric); the streaming channel is not back-filled from SOQL/ELF, since those
  are alternative per-category channels, not catch-up paths (§7, §10).

---

## 7. Phase 2 — Stored data → Loki (SOQL polling)

`salesforce/soql_client.py`, `sources/eventlog_objects_source.py`. Shares auth, sink, state, labels.

Per configured object: poll `SELECT <fields> FROM <obj> WHERE <ts_field> > :watermark
ORDER BY <ts_field>` on a cadence; emit `LogEntry`s; advance the watermark to the max timestamp
**only after the window is fully pushed** (crash → re-query from the last committed watermark =
gap recovery).

**This is an alternative to streaming, not a catch-up for it.** A stored event object (e.g.
`LoginEvent`) is the *persisted form of the same records* streamed on `/event/LoginEventStream`,
so ingesting both double-counts. Pick one channel per category (operator config); the overlap
guard (§10) enforces it. Use polling for categories you'd rather poll than stream, or that have
no streaming channel.

**Caveat (flagged):** Threat-Detection `*EventStore` objects are **BigObjects** with restrictive
SOQL — you may filter only on indexed fields (in index order) and ORDER BY is limited. Handled
per-object via a small object descriptor (which field is the indexed watermark, what's queryable),
not a generic query builder.

---

## 8. Phase 3 — EventLogFile (CSV ingestion)

`salesforce/eventlogfile_client.py`, `sources/eventlogfile_source.py`. Shares auth (SoqlClient for
listing), sink, state, labels, shaping.

Per configured `EventType`: list new `EventLogFile` records via SOQL
(`WHERE EventType=… AND Interval=… AND CreatedDate >= :since ORDER BY CreatedDate, Id`), download each
`LogFile` blob (`GET …/sobjects/EventLogFile/{id}/LogFile`, CSV), parse it **schema-agnostically**
(columns vary per type and API version — read the CSV header / `LogFileFieldNames`, never hardcode the
~70 type schemas), and emit one `LogEntry` per row. Per-row timestamp = `TIMESTAMP_DERIVED`. Emitting
one entry **per row** (not per file) is also what keeps lines under Loki's per-line size limit — see §9.

- **Per-type routing.** `event_types` items are either a bare string (e.g. `Login`) or a per-type
  object `{name, structured_metadata_fields?, labels?}`. `structured_metadata_fields` overrides the
  global `sink.loki.structured_metadata_fields` for that type (omit/`null` → inherit the global; `[]` →
  suppress it). `labels` promotes the named columns to **stream labels** for that type — a deliberate
  cardinality knob: only promote low-cardinality columns (each distinct value is a new stream).
- **Wildcard discovery.** `event_types: ["*"]` discovers every EventType the org produces for the
  interval (via a filtered `SELECT EventType … WHERE Interval=… GROUP BY EventType` — the *unfiltered*
  aggregate under-reports, a Salesforce quirk) and ingests them all, re-checked each poll so
  newly-enabled types appear without a restart. `exclude:` drops types (e.g. a category owned by
  another source, or high-volume ones). Explicit per-type entries are always kept and win over
  discovered defaults. Discovery failure is non-fatal — it falls back to the explicit entries. Caveat:
  the startup overlap guard can't see discovered types, so use `exclude` to keep a discovered category
  off ELF when a stream/object source owns it (e.g. `exclude: [Login]` when Login streams via Pub/Sub).
  Promotion can never clobber the reserved keys (`source`/`event_type`/`job`/`sf_org_id`/`environment`);
  config validation rejects promoting any of them or a non-identifier label name.

- **One interval only.** Hourly and Daily files are redundant copies of the same events; ingesting
  both double-counts. `interval` config selects one (default `Hourly`, ~3–6h fresh; `Daily` is ≥1 day
  and gets wholesale-replaced through the day). This is the "from now" path; backfill is not a goal
  (and Loki rejects entries older than ~1 week — §10).
- **Checkpoint = file-level + Loki native dedup.** State key `eventlogfile:<EventType>`, value
  `{"last_created", "ids"}` — the `CreatedDate` high-water plus a rolling set of recently-processed
  `EventLogFile.Id`s (late hourly files are additive, so the id-set prevents re-ingest while
  `CreatedDate >=` still catches them). The advanced checkpoint is carried only by a file's **last**
  row, so a mid-file batch flush never commits past a partially-sent file (crash → re-process the
  whole file; Loki collapses byte-identical rows). No connector-side row hashing.
- **Rate limits:** ELF rides the standard daily API pool (no separate Event-Monitoring allocation);
  hourly polling is a negligible fraction of even a Developer-edition budget.

Like the other sources it plugs into the same pipeline/sink/state with no core changes; it is subject
to the same either/or overlap guard (§10) as Phase 2.

---

## 9. Loki sink

`sinks/loki/{sink.py,push.py,labels.py}`.

- **Encoding (default): protobuf + snappy** — the canonical `logproto.PushRequest` wire format used
  by Promtail/Alloy/Grafana Agent. Vendored `proto/loki_push.proto` → generated stubs via `just proto`
  (same mechanism as the Pub/Sub proto). Snappy (block format) via `cramjam` (no libsnappy C
  dependency). `Content-Type: application/x-protobuf`.
- **Encoding (debug): JSON + gzip** — `POST /loki/api/v1/push` with
  `{"streams":[{"stream":{…}, "values":[["<ns ts>","<line>", {<structured metadata>}]]}]}`. Selectable
  for human-inspectable payloads in tests/debugging. Structured metadata supported in both encodings.
- **Targets**: Grafana Cloud (`https://logs-prod-*.grafana.net/loki/api/v1/push`, HTTP Basic
  `tenant_id:token`), self-hosted (`X-Scope-OrgID`), or local Alloy `loki.source.api` (URL only, no
  auth) — all the same push API, switched by config.
- **Batching**: `max_entries` / `max_bytes` / `flush_interval`; proactive split before a 413.
- **Per-line cap** (`batch.max_line_bytes`, default 262144 = Loki's `max_line_size` default; `0`
  disables): a line longer than the cap is truncated on a UTF-8 boundary with a `…[truncated, original
  N bytes]` marker before push, and `sf2loki_lines_truncated_total{source}` is incremented. Without
  this, a single oversized line (e.g. a giant ELF `QUERY`/`URI` column) would draw a 400 from Loki and,
  since 400 is permanent, take its **whole batch** down. (One entry per row already prevents the
  whole-file-as-one-line mistake; this guards the rarer fat-single-row case.) Mirror your Loki server's
  `max_line_size`.
- **Retry classification**: 429/5xx/transport → retryable (bounded backoff w/ jitter); 400 / 413
  (unsplittable) / encode error → permanent (drop + count + advance).

> **Loki requirement**: structured metadata needs schema **v13 + TSDB + `allow_structured_metadata:
> true`** (default on Grafana Cloud; must be enabled self-hosted / in Alloy's Loki).

---

## 10. Label & cardinality strategy — the whole point

**Stream labels — low-cardinality, ~constant per deployment:**

| label         | source                | distinct values            |
|---------------|-----------------------|----------------------------|
| `job`         | constant (`sf2loki`)  | 1                          |
| `source`      | module                | 3 (`pubsub`/`eventlog_objects`/`eventlogfile`) |
| `event_type`  | event name            | ~20–50                     |
| `sf_org_id`   | resolved org id       | 1 per deployment           |
| `environment` | operator-set          | 1 per deployment           |

→ **active streams ≈ `source × event_type` ≈ 30–90 per deployment** — comfortably within Grafana
Cloud per-tenant stream limits and cheap in DPM terms.

**Structured metadata — high-cardinality, filterable, NOT labels** (operator-configurable promotion
list, with sensible per-event defaults): `replay_id`, `schema_id`, `event_uuid`/`EventIdentifier`,
`user_id`, `username`, `source_ip`, `session_key`, `request_id`/`api_id`, `related_event_id`.
Queryable as `{event_type="LoginEventStream"} | user_id="005…"` with no stream-cardinality cost.

**`level`** is injected on every entry (`shaping.derive_level`). Salesforce has no single level field,
so it is derived from whatever status the event carries — explicit exceptions/errors and HTTP
`STATUS_CODE` → `error`/`warn`, `REQUEST_STATUS`/`LOGIN_STATUS`/`OPERATION_STATUS`/streaming `Status`
→ `info`/`warn` — defaulting to `info`. We emit `level` (a Loki-recognised level-field name) rather
than `detected_level` directly: Loki's distributor normalises `level` and copies it into the
`detected_level` structured metadata Grafana colours/filters by, and emitting `level` stays portable
where Loki's `discover_log_levels` is off. Level is deliberately structured metadata, **not** a label
— it varies row-to-row within a stream, so a label would fragment streams multiplicatively (current
Loki labelling guidance lists log level as a field to keep off labels).

**Log line**: the full decoded Avro/SOQL event as canonical JSON. **Entry timestamp = event
`EventDate`/`CreatedDate`** (fallback: ingest time).

**Justification.** A Loki stream is one unique label-set. Promoting `user`/`IP`/`session`/`request_id`
to labels multiplies streams by every identity seen → millions of low-throughput streams: blown
per-tenant stream limits, exploding index/DPM cost, and degraded query planning. Structured metadata
delivers the same filterability *without* the cardinality — exactly its design intent. `labels.py`
enforces a **startup allowlist guard**: any *static* label key not in the permitted set fails fast
(mirrors `genai-otel-bridge`'s governance guard).

**Per-type label promotion (ELF escape hatch).** When a deployment genuinely needs a *low*-cardinality
ELF column as a stream label (not just filterable metadata), an `eventlogfile` type can list it under
`labels` (§8). This bypasses the static allowlist by design — it's an explicit, per-type opt-in — so it
is the operator's responsibility to keep the chosen column low-cardinality; config still refuses to let
it shadow a reserved label key. Prefer `structured_metadata_fields` unless you actually need to slice
streams by that column.

**Overlap guard (`sources/overlap.py`).** A second startup guard enforces the either/or model: it
normalises every enabled source's identifiers (Pub/Sub topics, stored object names, ELF event types)
to a canonical *category* and fails fast if one category is fed by more than one source — because
`/event/LoginEventStream`, `LoginEvent`, and the `Login` EventLogFile are the same underlying events.
Two layers cooperate: the guard catches *explicit* collisions at startup, and the ELF `"*"` wildcard
*auto-excludes* categories a higher-priority source (a stream/object) already owns (app wiring passes
those categories to the ELF source). Bypass both with `sources.allow_overlap: true` — then the guard
is a no-op and the wildcard stops auto-excluding, so a category can flow via multiple sources on
purpose (e.g. the real-time-lean stream *and* the richer EventLogFile rows; they are not
byte-identical, so Loki won't collapse them — this is the intended "both" mode).

**Pub/Sub topic discovery.** `pubsub.topics: ["*"]` discovers every RTEM streaming channel via
describeGlobal (`MetadataClient`, the `*EventStream` sObjects → `/event/<Name>`), merges any explicit
topics, and applies the include/exclude globs — so new streams are subscribed without a config change.
Discovery failure is non-fatal (falls back to explicit topics); the startup overlap guard sees only
explicit topics (discovered ones aren't known then), mirroring the ELF wildcard.

> **Backfill caveat (flagged)**: Loki rejects entries older than `reject_old_samples_max_age`
> (default 1 week). The Phase 3 ELF source is "from now" by design; backfill of older events (or
> recovery after >1 week of downtime) needs that limit raised, or ingest-time stamping for the
> over-age tail (counted, not silent).

---

## 11. Config schema

`pydantic-settings`: load from YAML and/or env (`SF2LOKI_…`, `__` nesting). Secrets injectable via
`*_file` fields (mounted secret files) or `${ENV}` interpolation; missing/unreadable secret → fatal
(fail fast, no silent blanks).

```yaml
salesforce:
  login_url: https://login.salesforce.com      # or test. / My Domain
  client_id: ${SF_CLIENT_ID}
  username: svc@example.com
  private_key_file: /etc/sf2loki/secrets/server.key
  api_version: "60.0"
  org_id: null                                  # auto-resolved via userinfo if null

sources:
  pubsub:
    enabled: true
    endpoint: api.pubsub.salesforce.com:7443
    default_num_requested: 100                  # flow-control batch size
    replay_preset: CUSTOM                        # falls back to LATEST when no stored replay_id
    topics: ["/event/LoginEventStream", "/event/ApiAnomalyEvent"]
    include: ["/event/*"]                        # operator inclusion/exclusion globs
    exclude: []
  eventlog_objects:
    enabled: false
    objects:
      - {name: LoginEvent, timestamp_field: EventDate, poll_interval: 5m, lookback: 1h}
  eventlogfile:
    enabled: false
    interval: Hourly                             # Hourly | Daily — pick ONE
    event_types:                                 # bare string, or {name, structured_metadata_fields?, labels?}
      - Login
      - {name: ReportExport, structured_metadata_fields: [REPORT_ID], labels: [DELEGATED_USER]}

sink:
  type: loki
  loki:
    url: https://logs-prod-xx.grafana.net/loki/api/v1/push   # or http://alloy:3100/loki/api/v1/push
    tenant_id: "123456"                          # GC user id / X-Scope-OrgID; omit for Alloy
    auth_token_file: /etc/sf2loki/secrets/loki-token
    encoding: protobuf                           # protobuf (default) | json
    compression: snappy                          # snappy (protobuf) | gzip (json)
    batch: {max_entries: 1000, max_bytes: 1048576, flush_interval: 1s, max_line_bytes: 262144}
    labels: {environment: prod}                  # job + sf_org_id added automatically
    structured_metadata_fields: [replay_id, schema_id, event_uuid, user_id, username, source_ip, session_key]

state:
  store: file                                    # local JSON file (the only backend)
  file: {path: /var/lib/sf2loki/state.json}

service:
  log_level: info
  log_format: json
  health_addr: ":8080"
  shutdown_grace: 25s
  telemetry:                                     # OTLP metrics egress (push; no scrape endpoint)
    enabled: false
    endpoint: ""                                 # GC OTLP gateway .../otlp/v1/metrics, or http://alloy:4318/v1/metrics
    auth: basic                                  # basic (defaults to Loki tenant_id/token) | none
```

---

## 12. Self-observability

`obs/metrics.py` defines all metrics on an OpenTelemetry `Meter`, pushed via **OTLP/HTTP**
(`service.telemetry`, gated on `enabled`) — OTel-native, no Prometheus scrape endpoint. Basic auth
defaults to the Loki sink credentials (Grafana Cloud shares one stack credential for Loki and OTLP).
Names map to the usual Prometheus exposition names (counters gain `_total`):

- `sf2loki_events_ingested_total{source,event_type}`, `sf2loki_decode_errors_total{reason}`
- `sf2loki_loki_push_total{outcome}`, `sf2loki_loki_push_duration_seconds`, `sf2loki_loki_bytes_pushed_total`
- `sf2loki_ingest_lag_seconds{event_type}` — `now − EventDate` (the key SLI)
- `sf2loki_last_replay_commit_timestamp_seconds{topic}`, `sf2loki_pubsub_pending_credits{topic}`,
  `sf2loki_pubsub_reconnects_total{topic}`
- `sf2loki_watermark_timestamp_seconds{source,object}` (Phase 2)
- `sf2loki_salesforce_limit_max{limit_name}`, `sf2loki_salesforce_limit_remaining{limit_name}`,
  `sf2loki_salesforce_limits_poll_errors_total` — org limits (`salesforce.limits`, via `obs/limits_poller.py`)
- `sf2loki_auth_refreshes_total`, `sf2loki_auth_errors_total`, `sf2loki_schema_cache_size`,
  `sf2loki_queue_depth`, `sf2loki_build_info`

A Grafana dashboard for these lives in `deploy/grafana/`.

`obs/health.py` on `health_addr`: `/healthz` (liveness — event loop responsive) and `/readyz`
(readiness — auth obtained + ≥1 source connected + sink reachable). `obs/logging.py`: `structlog`,
JSON or logfmt, level-configurable, instance id in context.

---

## 13. Resilience, lifecycle & HA

- **Retry/backoff** (tenacity, exponential + jitter) for Loki push and the token endpoint.
- **Token refresh**: proactive before expiry; reactive on 401 (re-mint JWT) → reconnect gRPC.
- **Reconnect**: gRPC stream errors (UNAVAILABLE etc.) → backoff + reconnect, resume from committed
  `replay_id`.
- **Graceful shutdown**: SIGTERM → set `stop` event → stop requesting events → flush in-flight batch
  → commit checkpoints → close streams, all within `shutdown_grace` (set the container runtime's
  stop timeout above it).

**HA / single-instance model.** The Pub/Sub API delivers events **independently per subscriber
connection** (no consumer groups) — **two instances both subscribing double-deliver events.**
Therefore the deployment runs **exactly one instance** (stop-then-start, never overlapping) with
`replay_id` checkpointing so a restart resumes without overlap; the brief restart gap is bounded by
Pub/Sub retention and backfilled by Phase 2. The `Coordinator` seam (§4) lets active-passive leader
election drop in later without reshaping sources or sink. Topic-sharding is a future scale path.

---

## 14. Packaging & delivery

- **Dockerfile**: multi-stage — `uv`-based builder (deps + already-committed stubs) → slim,
  non-root runtime (distroless-python or `python:3.12-slim`), `HEALTHCHECK` against `/healthz`.
- **Docker Compose** (`docker-compose.yml`): the published image with `config.docker.yaml` +
  `./secrets` mounted read-only and `./state` bind-mounted at `/var/lib/sf2loki` for durable
  checkpoints. Single service, `restart: unless-stopped`.
- **CI** (GitHub Actions): ruff → mypy → pytest → proto-drift check → multi-arch image build (buildx)
  → gitleaks. Mirrors `genai-otel-bridge`'s green-bar gate.
- **`justfile`**: `setup`, `proto`, `lint`, `type`, `test`, `gate`, `image`, `run`.

---

## 15. Testing

Network is always mocked. TDD throughout.

- **Avro decode** — fixture schemas + binary payloads; schema-cache hit/miss; malformed payload.
- **Label/structured-metadata mapping** — allowlist guard rejects stray label keys; promotion list
  routes high-cardinality fields to structured metadata.
- **Loki payload shaping** — protobuf+snappy round-trip and JSON shape (incl. structured metadata),
  batch splitting at `max_bytes`, retry classification (429 vs 400 vs 413).
- **Watermark/replay** — commit advances per key; restart resumes from last committed; gap recovery
  on simulated crash mid-window; drop-and-advance on permanent sink error.
- **Auth** — JWT assertion construction (claims, signing), token cache + reactive refresh on 401.
- **Pub/Sub flow control** — credit top-up against a fake gRPC servicer / mocked stub.

---

## 16. Salesforce assumptions (flagged, not guessed)

1. org id (`tenantid`) resolved via `/services/oauth2/userinfo` `organization_id`.
2. Naming: `…EventStream` = streaming RTEM channel; `…Event` / `…EventStore` = stored object.
   Anomaly events share the base name for the stream channel (`/event/ApiAnomalyEvent`) and the
   stored BigObject (`ApiAnomalyEventStore`).
3. Threat-Detection `*EventStore` objects are BigObjects → restrictive SOQL (indexed-field filters,
   limited ORDER BY).
4. Pub/Sub replay retention ≈ 24–72 h.
5. Most RTEM streaming channels and all Threat-Detection anomaly channels require the **Shield Event
   Monitoring** add-on (+ Threat Detection for anomalies). Topic inclusion/exclusion is operator
   config; defaults stay conservative.
6. Loki structured metadata requires schema v13 + TSDB + `allow_structured_metadata: true`; Loki
   rejects entries older than `reject_old_samples_max_age` (default 1 w) — relevant to backfill.

---

## 17. Phase status

| Phase | Scope                                   | Status        |
|-------|-----------------------------------------|---------------|
| 0     | Design (this document)                  | done          |
| 1     | Pub/Sub streaming → Loki + foundation   | done          |
| 2     | SOQL polling of stored objects → Loki   | done          |
| 3     | EventLogFile ingestion                  | done          |

Phases 2 and 3 are **alternative per-category channels**, not catch-up paths for Phase 1; the
overlap guard (§10) enforces one source per event category.
