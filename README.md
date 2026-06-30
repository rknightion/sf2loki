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
- **Self-observable (OTel-native)**: all metrics — connector self-observability **and** Salesforce org
  limits (API usage, storage, streaming events) — push via **OTLP/HTTP** (Grafana Cloud or a local
  Alloy `otelcol.receiver.otlp`); plus `/healthz`, `/readyz`, structured logs, graceful shutdown.

## Salesforce setup (OAuth)

The service authenticates server-to-server, no interactive login. Pick one flow via
`salesforce.auth_mode`:

- **`jwt_bearer`** (default) — a private key signs a short-lived JWT assertion, Salesforce returns an
  access token. Most secure: no shared secret ever leaves your secret store. Setup is steps 1–5 below.
- **`client_credentials`** — a consumer key + secret; simplest to set up (no keypair, certificate, or
  user pre-authorisation). See [Client Credentials flow](#alternative-client-credentials-flow).

Both reuse the same External Client App shell and the same integration-user permissions (step 4); the
JWT bearer walkthrough follows.

### 1. Generate the keypair and certificate

JWT bearer uses an asymmetric keypair. You upload the **public certificate** to Salesforce (it uses it
to verify your signed assertions); the **private key** is mounted into the pod (it signs them). The
private key never leaves your secret store; the certificate is not secret.

```bash
# 2048-bit RSA private key  -> stays in your secret store, mounted at salesforce.private_key_file
openssl genrsa -out server.key 2048

# self-signed X.509 public cert (valid 10y) -> uploaded to Salesforce, never deployed
openssl req -new -x509 -key server.key -out server.crt -days 3650 -subj "/CN=sf2loki"
```

| File | Secret? | Where it goes |
|------|---------|---------------|
| `server.key` | **yes** | k8s `Secret` → mounted into the pod → `salesforce.private_key_file` (or `salesforce.private_key`). Never upload it. |
| `server.crt` | no | uploaded to the External Client App (OAuth Settings → JWT Bearer Flow). Not deployed with the service. |

The cert is self-signed on purpose — Salesforce only needs the public key to verify signatures; there's
no chain/CA validation in this flow. Rotate by generating a new keypair and uploading the new cert.

### 2. Create the External Client App

Setup → **External Client App Manager** → **New External Client App**. (External Client Apps are the
path Salesforce recommends and, from Spring '26, requires for new apps in place of Connected Apps.)
Field by field:

**Basic Information**
- **External Client App Name** — e.g. `sf2loki`. **API Name** auto-fills; leave it.
- **Contact Email** — your address.
- **Distribution State** — **Local** (this org only; "Packaged" is only for distributing the app).
- **Contact Phone / Info URL / Logo Image URL / Icon URL / Description** — optional, leave blank.

**API (Enable OAuth Settings)**
- **Enable OAuth** — **✅ tick** (turns on everything below).

**App Settings**
- **Callback URL** — required field, but JWT bearer performs no redirect, so it's stored and never
  invoked. Use a placeholder: `https://login.salesforce.com/services/oauth2/callback`.
- **OAuth Scopes** — move into *Selected*: **`Manage user data via APIs (api)`** and **`Perform requests
  at any time (refresh_token, offline_access)`**. `api` covers REST/SOQL, the EventLogFile `/LogFile`
  download, and the Pub/Sub API (which has no scope of its own — it just needs a valid access token).
  Leave **everything else** in *Available*: `Access the identity URL service (id…)`, `web`,
  `Full access (full)`, `chatter_api`, `visualforce`, and all Data Cloud / platform scopes
  (`cdp_segment_api`, `cdp_identityresolution_api`, `cdp_calculated_insight_api`, `sfap_api`,
  `interaction_api`, `cdp_api`).
  - **`refresh_token` is required** even though JWT bearer never issues or uses a refresh token —
    Salesforce's **pre-authorized** JWT bearer path refuses the grant without it
    (`invalid_request: "refresh_token scope is required and the connected app should be installed and
    preauthorized"`). Verified empirically against a dev org. The scope is present on the app; the
    service never exercises it.
  - `openid` is needed **only if** you leave `salesforce.org_id` unset (it authorises the `/userinfo`
    org-id lookup). Set `org_id` in config to avoid needing it.
- **Introspect all Tokens** — ❌ leave unticked (authorises introspecting *every* token in the org; the
  app can already introspect its own).
- **Configure ID token** — ❌ leave unticked (only relevant when `openid` is requested and an ID token
  is consumed; the connector does neither).

**Flow Enablement** — tick exactly one:
- **Enable JWT Bearer Flow** — **✅ tick**.
- **Enable Client Credentials Flow** — ❌. **Enable Authorization Code and Credentials Flow** — ❌.
  **Enable Device Flow** — ❌. **Enable Token Exchange Flow** — ❌.
  (Each disabled flow is one fewer way to mint a token from this app — least privilege.)

**Security**
- **Require secret for Web Server Flow** — leave **ticked** (default; guards an unused flow, harmless).
- **Require secret for Refresh Token Flow** — leave **ticked** (default; unused flow, harmless).
- **Require PKCE for Supported Authorization Flows** — leave **ticked** (default; applies to auth-code
  flows we don't use).
- **Enable Refresh Token Rotation** — ❌ (no refresh tokens in JWT bearer).
- **Issue JSON Web Token (JWT)-based access tokens for named users** — ❌. *Not* JWT bearer auth despite
  the name — it changes the issued access-token *format* to stateless JWTs. Opaque tokens are preferable
  here (server-side revocable; the service re-mints on 401 anyway).
- **Limit Idle Refresh Token TTL to 30 Days** — ❌ (no refresh tokens). **Enforce Refresh Token IP
  Allowlist** — ❌ (no refresh tokens).

Then: **OAuth Settings → JWT Bearer Flow → upload `server.crt`**, **Save**, and copy the **Consumer
Key** (App Settings) → this is `salesforce.client_id`.

### 3. Pre-authorise the integration user (mandatory)

JWT bearer has no interactive consent, so the user named in the `sub` claim must be pre-authorised.
On the app's **Policies** tab (admin-owned) → OAuth Policies → **Permitted Users = "Admin approved
users are pre-authorized"**, then assign the app to a **Permission Set** that the integration user
holds. Optional hardening: add a **login-IP restriction** here if your pod egress IPs are stable.

### 4. Licences / permissions for the integration user

- **Shield Event Monitoring** add-on (most RTEM streaming channels) and **Threat Detection** (anomaly
  channels such as `ApiAnomalyEvent`).
- **View Real-Time Event Monitoring Data** — subscribe to RTEM streams / query stored event objects
  (Phase 1 & 2).
- **View Event Log Files** — the EventLogFile path (Phase 3). ELF retention is 1 day without Shield,
  30 days (up to 365) with the Event Monitoring add-on.
- **API Enabled** — for the Pub/Sub and REST APIs.

### 5. Point the service at your org

Set `salesforce.environment` to `production` or `sandbox` (this derives the login URL), or set
`salesforce.login_url` explicitly to a custom My Domain URL (it takes precedence over `environment`).
Then set `salesforce.client_id` to the Consumer Key, `salesforce.username` to the integration user
(jwt_bearer only), and the private key via `salesforce.private_key_file`.

> Topic availability depends on your Shield/Threat-Detection entitlements. Topic inclusion/exclusion
> is operator config (`sources.pubsub.topics` + `include`/`exclude` globs); defaults stay
> conservative.

### Alternative: Client Credentials flow

Simpler than JWT bearer — no keypair, certificate, or user pre-authorisation. Set
`salesforce.auth_mode: client_credentials` and supply a consumer **secret** instead of a private key.

1. **External Client App** — created as above, but under **Flow Enablement** tick **Enable Client
   Credentials Flow** (instead of JWT Bearer Flow). No certificate upload is needed.
2. **Run As user** — on the app's **Policies** tab → OAuth Policies, set the **Run As** user to your
   integration user; its permissions (step 4) determine what is collected. (Client credentials has no
   `sub` claim and no per-user pre-authorisation — the Run-As user replaces both.)
3. **Scope** — `Manage user data via APIs (api)`; `refresh_token` is **not** required for this flow.
4. **Config** — `salesforce.auth_mode: client_credentials`, `salesforce.client_id` = Consumer Key, and
   the Consumer Secret via `salesforce.client_secret_file` (or `client_secret`). `username` and the
   private key are unused in this mode.

> Trade-off: client credentials transmits a shared secret (symmetric); JWT bearer never sends a secret
> over the wire (asymmetric key). Both yield an access token that works identically for the Pub/Sub,
> REST/SOQL, and EventLogFile paths.

## Configuration

Config loads from a YAML file and/or environment (`SF2LOKI_*` with `__` nesting; env overrides YAML
overrides defaults). Secrets are injected from `*_file` paths or `${ENV}` interpolation; a missing or
unreadable secret is fatal at startup. See [`config.example.yaml`](config.example.yaml) for the full
annotated schema.

```bash
# run locally against a config file
uv run python -m sf2loki --config config.example.yaml

# validate config + wiring (secrets, label allowlist, source-overlap guard) without
# any network calls — exits 0 (ok) or 1 (invalid)
uv run python -m sf2loki --config config.example.yaml --check
```

### Source recipes

See [`docs/configuring-sources.md`](docs/configuring-sources.md) for recipes — polling arbitrary
custom objects, ingesting login history / setup audit trail, and the either/or-per-category rule.

### Metrics (OTLP)

All metrics push via OTLP/HTTP. Set `service.telemetry.enabled: true` and `service.telemetry.endpoint`
(a Grafana Cloud OTLP gateway, e.g. `https://otlp-gateway-<zone>.grafana.net/otlp/v1/metrics`, or a
local Alloy `http://alloy:4318/v1/metrics`). Basic auth defaults to the Loki sink's
`tenant_id`/`auth_token` (Grafana Cloud uses one stack credential for Loki and OTLP); use
`service.telemetry.auth: none` for an unauthenticated in-cluster Alloy. Enable Salesforce org-limit
metrics (API usage, storage, streaming events, …) with `salesforce.limits.enabled: true`. A ready-made
Grafana dashboard lives in [`deploy/grafana/`](deploy/grafana/).

## Run with Docker / docker-compose

The container is the primary run target (alongside ECS). The image is slim, runs as a non-root user,
and exposes `:8080` (`/healthz`, `/readyz`). Metrics are pushed via OTLP, so there is no scrape port.

```bash
# build
docker build -t sf2loki:dev .

# run via compose — non-secret values from .env.dev, secrets mounted from ./secrets
docker compose --env-file .env.dev up --build
```

[`docker-compose.yml`](docker-compose.yml) mounts [`config.docker.yaml`](config.docker.yaml) (no
secrets — env-driven via `${VAR}` + `*_file`) at `/etc/sf2loki/config.yaml` and `./secrets` (the
private key + Loki token) read-only at `/etc/sf2loki/secrets`. Create `.env.dev` from the values in
the config (Salesforce login URL / consumer key / username, Loki URL + tenant); `.env*`, `*.key`,
`*.crt`, and `secrets/` are gitignored.

**Health check target — use `/readyz`, not `/healthz`.** `/healthz` is *liveness* (200 whenever the
process is up, even mid-startup before Salesforce auth); `/readyz` is *readiness* (200 only once auth
resolved and the pipeline is running). Docker/ECS collapse container health into a single signal, so
they should probe `/readyz` — the Dockerfile `HEALTHCHECK` already does. For **ECS**, set the task
definition `healthCheck` to `CMD-SHELL curl -f http://localhost:8080/readyz || exit 1` with a
`startPeriod` (~20s) covering normal startup, and mark the container `essential: true` so a fast-fail
(e.g. bad Salesforce credentials → process exits) restarts the task.

## Kubernetes deployment

Manifests live in [`deploy/k8s/`](deploy/k8s/): `Deployment` (replicas 1 / `Recreate`), `Service`,
`PodDisruptionBudget`, RBAC + `ServiceAccount`, `ConfigMap` (config + state), and `Secret`. The default
uses the **ConfigMap checkpoint store** (no PVC; survives reschedules); switch to `state.store: file`
with a PVC if you prefer. (Metrics push via OTLP, so there is no `ServiceMonitor`.)

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
