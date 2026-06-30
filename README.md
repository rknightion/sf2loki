# sf2loki

Ship Salesforce Event Monitoring data into Grafana Loki.

`sf2loki` is a long-running Python/asyncio service that ingests Salesforce **Real-Time Event
Monitoring** (RTEM) streaming events via the Pub/Sub API (gRPC + Avro) and stored event objects via
SOQL, then pushes them to Loki with strict label-cardinality discipline. It targets Grafana Cloud
Loki, self-hosted Loki, and local Alloy (`loki.source.api`).

See [DESIGN.md](DESIGN.md) for the full architecture, frozen seams, label strategy, and phase plan.

## Highlights

- **Pluggable sources** behind one async-iterator seam: Pub/Sub streaming (Phase 1), SOQL polling of
  stored objects (Phase 2), EventLogFile CSV ingestion (Phase 3). These are **alternative per-category
  channels** — ingest each event category (Login, API, Report, …) from exactly one source; a fail-fast
  overlap guard refuses to start if a category is enabled on more than one (bypass: `allow_overlap`).
- **Loki sink**: protobuf + snappy by default (canonical push wire format), JSON + gzip as a debug
  encoding. Both carry structured metadata.
- **Cardinality discipline**: a fixed label allowlist (`job`, `source`, `event_type`, `sf_org_id`,
  `environment`); everything high-cardinality (`user_id`, `source_ip`, `replay_id`, …) goes to
  structured metadata or the JSON line — never labels. A startup guard fails fast on stray labels.
  Structured-metadata promotion is configurable globally and **per ELF event type**; per-type `labels`
  is an explicit opt-in escape hatch for the rare low-cardinality column you want as a stream label.
- **Loki-safe lines**: one entry per ELF row (never a whole file as one line), plus a per-line byte cap
  (`batch.max_line_bytes`, default 256 KiB) that truncates an oversized line before push so one fat row
  can't get its whole batch rejected.
- **Resumable**: per-topic `replay_id` / per-object watermark checkpointing to a file or a k8s
  ConfigMap; at-least-once delivery, structural backpressure (no silent drops).
- **Self-observable**: Prometheus `/metrics`, `/healthz`, `/readyz`, structured logs, graceful
  shutdown.

## Salesforce setup (OAuth 2.0 JWT bearer)

The service authenticates server-to-server with the JWT bearer flow — no interactive login, no
refresh token (it re-mints a JWT on expiry / 401).

1. **Generate an RSA keypair and self-signed cert** (the cert goes on the External Client App; the
   private key is mounted into the pod):
   ```bash
   openssl genrsa -out server.key 2048
   openssl req -new -x509 -key server.key -out server.crt -days 3650 \
     -subj "/CN=sf2loki"
   ```
2. **Create an External Client App** (Setup → External Client App Manager → New — the path Salesforce
   recommends, and now requires for new apps in place of Connected Apps):
   - Enable OAuth. Callback URL is a required field but is never invoked by JWT bearer (no redirect) —
     a placeholder like `https://login.salesforce.com/services/oauth2/callback` is fine.
   - **OAuth scope: `api` only** — it covers REST/SOQL, the EventLogFile `/LogFile` download, and the
     Pub/Sub API (no scope of its own; it just needs a valid access token). `refresh_token`/
     `offline_access` is **not** required (JWT bearer issues no refresh token). Add `openid` **only if**
     you leave `salesforce.org_id` unset (it permits the `/userinfo` org-id lookup) — otherwise set
     `org_id` in config and keep `api` alone. Leave all other scopes off.
   - **Flow Enablement: tick *Enable JWT Bearer Flow* only.** Leaving the other flows
     (Client-Credentials, Auth-Code, Device, Token-Exchange) off means the app can only be used our one
     way — pure attack-surface reduction.
   - **Security:** the three default toggles (require secret for web-server / refresh-token flows,
     require PKCE) govern flows we don't use — harmless, leave them ticked. The refresh-token controls
     are moot (no refresh token). Leave *Issue JWT-based access tokens for named users* off (it changes
     the access-token format and is unrelated to JWT bearer auth).
   - **OAuth Settings → JWT Bearer Flow → upload `server.crt`** (the ECA equivalent of "use digital
     signatures").
   - Save; copy the **Consumer Key** (App Settings) → this is `salesforce.client_id`.
3. **Pre-authorise the integration user** (Policies tab — admin-owned): *Permitted Users: Admin
   approved users are pre-authorized* (mandatory for JWT bearer), then assign the app to a Permission
   Set the integration user holds. Add a login-IP restriction here if your egress IPs are stable.
4. **Permission sets / licences** for the integration user:
   - **Shield Event Monitoring** add-on (for most RTEM streaming channels) and **Threat Detection**
     (for anomaly channels such as `ApiAnomalyEvent`).
   - **View Real-Time Event Monitoring Data** (to subscribe to RTEM streams / query stored event
     objects — Phase 1 & Phase 2).
   - **View Event Log Files** (for the Phase 3 EventLogFile path). EventLogFile retention is 1 day
     without Shield, 30 days (up to 365) with the Event Monitoring add-on.
   - **API Enabled** generally, for the Pub/Sub and REST APIs.
5. Set `salesforce.login_url` to `https://login.salesforce.com`, `https://test.salesforce.com`
   (sandbox), or your My Domain URL.

> Topic availability depends on your Shield/Threat-Detection entitlements. Topic inclusion/exclusion
> is operator config (`sources.pubsub.topics` + `include`/`exclude` globs); defaults stay
> conservative.

## Configuration

Config loads from a YAML file and/or environment (`SF2LOKI_*` with `__` nesting; env overrides YAML
overrides defaults). Secrets are injected from `*_file` paths or `${ENV}` interpolation; a missing or
unreadable secret is fatal at startup. See [`config.example.yaml`](config.example.yaml) for the full
annotated schema.

```bash
# run locally against a config file
uv run python -m sf2loki --config config.example.yaml
```

## Kubernetes deployment

Manifests live in [`deploy/k8s/`](deploy/k8s/): `Deployment` (replicas 1 / `Recreate`), `Service`,
`ServiceMonitor`, `PodDisruptionBudget`, RBAC + `ServiceAccount`, `ConfigMap` (config + state), and
`Secret`. The default uses the **ConfigMap checkpoint store** (no PVC; survives reschedules); switch
to `state.store: file` with a PVC if you prefer.

```bash
# edit deploy/k8s/secret.yaml + configmap.yaml first (private key, Loki token, URLs)
kubectl apply -f deploy/k8s/
```

**Why a single replica:** the Pub/Sub API delivers events independently per subscriber connection
(no consumer groups), so two replicas double-deliver. `replicas: 1` + `Recreate` + `replay_id`
checkpointing resumes without overlap; the `Coordinator` seam allows active-passive leader election
later (DESIGN §13).

> **Loki requirement**: structured metadata needs schema **v13 + TSDB + `allow_structured_metadata:
> true`** (default on Grafana Cloud; must be enabled self-hosted / in Alloy's Loki).

## Development

```bash
uv sync            # create the venv from the lockfile
just gate          # ruff + mypy --strict + pytest (the green bar)
just proto         # regenerate gRPC/protobuf stubs (only when proto/ changes)
just image         # build the container image
```

Python 3.14, `uv`-managed. Generated proto stubs are committed; CI fails on drift. Tooling: `uv`,
`ruff`, `mypy --strict`, `pytest` + `pytest-asyncio`, `just`.
