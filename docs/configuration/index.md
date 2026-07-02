# Configuration

sf2loki is configured from a YAML file, environment variables, or both, and
validated against a Pydantic model at startup — an invalid or incomplete
config fails fast with a specific error rather than a runtime surprise.

For the exhaustive field-by-field listing, see the **[full reference](../config-reference.md)**
— every key, type, default, and description, generated straight from the
Pydantic model via `just gen-config`. This page maps the top-level sections
and how they fit together; it does not duplicate that table.

## Loading and precedence

- **YAML file** — passed with `--config config.yaml`. See
  [`config.example.yaml`](https://github.com/rknightion/sf2loki/blob/main/config.example.yaml)
  for a runnable, annotated example (also generated from the schema).
- **Environment variables** — prefix `SF2LOKI_`, with `__` (double
  underscore) separating nested keys. `sink.loki.url` becomes
  `SF2LOKI_SINK__LOKI__URL`.
- **Precedence**: environment overrides YAML, YAML overrides built-in
  defaults.

!!! example "Environment variable nesting"
    ```bash
    export SF2LOKI_SALESFORCE__CLIENT_ID=3MVG9...
    export SF2LOKI_SINK__LOKI__URL=https://logs-prod-006.grafana.net/loki/api/v1/push
    export SF2LOKI_SINK__LOKI__TENANT_ID=123456
    ```

Validate a config without touching the network:

```bash
sf2loki --config config.yaml --check
```

## Secrets

Any field documented as a secret (`SecretStr` in the reference) can be
supplied three ways:

- **`*_file`** — a path to a file containing the secret (e.g.
  `salesforce.private_key_file`, `sink.loki.auth_token_file`). The preferred
  path for containers/Kubernetes, where the file is a mounted secret volume.
- **`${ENV}` interpolation** — reference an environment variable inside a
  YAML string value, e.g. `client_id: ${SF_CLIENT_ID}`.
- **Inline** — set the plain (non-`_file`) key directly in YAML. Fine for
  local dev, not recommended for anything checked into version control.

!!! warning "A missing or unreadable secret is fatal at startup"
    sf2loki never starts in a half-configured state. If a `*_file` path
    doesn't exist or isn't readable by the process user, or a referenced
    `${ENV}` variable is unset, config loading fails immediately with the
    offending key named in the error — not a mysterious runtime auth failure
    later.

## Top-level sections

### `salesforce` / `orgs` — connection and org selection

Exactly **one** of these is set, never both:

- **`salesforce`** — single-org mode: one Salesforce connection (auth mode,
  login URL, credentials) shared by that org's sources.
- **`orgs`** — multi-org mode: a list of `{name, salesforce, sources}`
  entries, each with its own connection and source selection, ingesting into
  one shared sink/state/service. Each org gets its own `org` stream label and
  checkpoint-key prefix; a per-org outage doesn't stop the others. See
  [Presets](presets.md) for a worked multi-org example.

Salesforce-side setup (External Client App, OAuth flow, entitlements) is
covered in the
[README](https://github.com/rknightion/sf2loki/blob/main/README.md#salesforce-setup-oauth)
— operator setup, not config schema, so it isn't repeated here.

### `sources` — event source selection

Which Salesforce channels feed Loki, and how: Pub/Sub streaming, SOQL object
polling, EventLogFile ingestion, and the opt-in ApexLog debug-log source. This
is the section with the most day-to-day tuning (topics, event types, PII
transforms, sampling, cost controls). See [Sources](../sources/index.md) for
the concepts, and [Pub/Sub](../sources/pubsub.md),
[EventLogFile](../sources/eventlogfile.md), and
[event-log objects](../sources/eventlog-objects.md) for per-source detail. In
multi-org mode, `sources` moves inside each `orgs[]` entry instead of sitting
at the top level.

### `sink` — Loki delivery

`sink.loki` holds the push URL, tenant id, auth, wire encoding
(protobuf+snappy by default), batching, static labels, and the
structured-metadata field list; `sink.loki.egress` holds the opt-in rate caps
and daily byte budget. This is where Loki's cardinality discipline is
enforced — see the fixed label allowlist in
[`src/sf2loki/sinks/loki/labels.py`](https://github.com/rknightion/sf2loki/blob/main/src/sf2loki/sinks/loki/labels.py).

### `state` — checkpoint persistence

Where per-topic replay ids and per-object watermarks are durably stored
between restarts: a local JSON file (default) or an S3-/GCS-compatible object
store for stateless deployments. See [State](../deployment/state.md).

### `coordinate` — active-passive HA

Leadership election for running two replicas safely: `noop` (single
instance, the default), a shared file lease, or a Kubernetes `Lease`. See
[High availability](../deployment/high-availability.md).

### `service` (+ `telemetry`) — runtime and observability

Log level/format, the health-check server (`/healthz`, `/readyz`), and the
shutdown grace period; nested under it, `service.telemetry` controls OTLP
metrics egress (endpoint, auth, export interval). See
[Metrics](../observability/metrics.md).

## Presets

[`examples/presets/`](https://github.com/rknightion/sf2loki/tree/main/examples/presets)
has ready-to-merge config fragments for common setups — multi-org, custom
object polling, Big Object polling, login history, setup audit trail, and
custom platform events / CDC. See [Presets](presets.md) for the full list.
