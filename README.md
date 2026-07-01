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
- **Resumable**: per-topic `replay_id` / per-object watermark checkpointing to a local JSON file;
  at-least-once delivery, structural backpressure (no silent drops).
- **Self-observable (OTel-native)**: all metrics — connector self-observability **and** Salesforce org
  limits (API usage, storage, streaming events) — push via **OTLP/HTTP** (Grafana Cloud or a local
  Alloy `otelcol.receiver.otlp`); plus `/healthz`, `/readyz`, structured logs, graceful shutdown.
- **Operable from day one**: `sf2loki doctor` runs a live end-to-end preflight (auth, permissions,
  entitlements, Pub/Sub reachability, a Loki test write); `sf2loki backfill` loads historical
  EventLogFile data; a generated Grafana dashboard **and alert-rule pack** ship in
  [`deploy/grafana/`](deploy/grafana/).
- **Compliance & cost controls**: declarative PII transforms (hash / mask / drop field / drop row /
  regex), deterministic per-type sampling, sink rate caps, and a daily byte budget with a lossless
  pause mode — all opt-in.

## Documentation

- [DESIGN.md](DESIGN.md) — full architecture, frozen seams, label strategy, phase plan, HA model.
- [docs/configuring-sources.md](docs/configuring-sources.md) — source/config reference: custom
  object polling, login history / setup audit trail recipes, the overlap rule, cardinality controls,
  PII redaction & sampling, cost controls.
- [docs/alerts.md](docs/alerts.md) — the shipped Grafana alert-rule pack: what each alert means and
  the first response step.
- [deploy/grafana/README.md](deploy/grafana/README.md) — the bundled Grafana dashboard.
- [docs/generate-activity.md](docs/generate-activity.md) — synthetic activity generator for
  exercising a dev org's Event Monitoring pipeline.

## Salesforce setup (OAuth)

The service authenticates server-to-server, no interactive login. Pick one flow via
`salesforce.auth_mode`:

- **`jwt_bearer`** (default) — a private key signs a short-lived JWT assertion, Salesforce returns an
  access token. Most secure: no shared secret ever leaves your secret store. Setup is steps 1–5 below.
- **`client_credentials`** — a consumer key + secret; simplest to set up (no keypair, certificate, or
  user pre-authorisation). See [Client Credentials flow](#alternative-client-credentials-flow).

Both reuse the same External Client App shell and the same integration-user permissions (step 4); the
JWT bearer walkthrough follows. See [DESIGN.md §5](DESIGN.md#5-salesforce-auth--oauth-jwt-bearer-or-client-credentials)
for the protocol-level detail (JWT claims, token endpoint, why each scope/toggle is set the way it is).

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
| `server.key` | **yes** | mounted read-only (e.g. `./secrets`) → `salesforce.private_key_file` (or `salesforce.private_key`). Never upload it. |
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
  at any time (refresh_token, offline_access)`**. Leave **everything else** in *Available*: `Access the
  identity URL service (id…)`, `web`, `Full access (full)`, `chatter_api`, `visualforce`, and all Data
  Cloud / platform scopes (`cdp_segment_api`, `cdp_identityresolution_api`,
  `cdp_calculated_insight_api`, `sfap_api`, `interaction_api`, `cdp_api`). `refresh_token` is required
  even though this flow never issues or uses a refresh token — see
  [DESIGN.md §5](DESIGN.md#5-salesforce-auth--oauth-jwt-bearer-or-client-credentials) for why. `openid`
  is needed only if you leave `salesforce.org_id` unset; set `org_id` in config to avoid needing it.
- **Introspect all Tokens** — ❌ leave unticked (authorises introspecting *every* token in the org; the
  app can already introspect its own).
- **Configure ID token** — ❌ leave unticked (only relevant when `openid` is requested and an ID token
  is consumed; the connector does neither).

**Flow Enablement** — tick exactly one:
- **Enable JWT Bearer Flow** — **✅ tick**.
- **Enable Client Credentials Flow** — ❌. **Enable Authorization Code and Credentials Flow** — ❌.
  **Enable Device Flow** — ❌. **Enable Token Exchange Flow** — ❌.
  (Each disabled flow is one fewer way to mint a token from this app — least privilege.)

**Security** — leave every default as-is (`Require secret for Web Server Flow` / `…Refresh Token Flow`
/ `Require PKCE` ticked; refresh-token rotation/idle-TTL/IP-allowlist and named-user JWT access tokens
unticked). These toggles govern flows and refresh-token behaviour this connector doesn't use — see
[DESIGN.md §5](DESIGN.md#5-salesforce-auth--oauth-jwt-bearer-or-client-credentials) if you want the
reasoning for each.

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

> **No Event Monitoring add-on? Expect a short ELF menu.** Without the Shield/Event Monitoring
> add-on, orgs get only the free EventLogFile subset: Login, Logout, API Total Usage, Apex
> Unexpected Exception, and the CORS/CSP-violation + hostname-redirect types — **Daily interval
> only, 1-day retention**. An `event_types: ["*"]` wildcard on a free org silently yields just
> those (that's discovery working correctly, not a bug), and `interval: Hourly` needs the add-on's
> hourly opt-in. The full ~70-type catalogue and RTEM streaming channels require the add-on.

### 5. Point the service at your org

Set `salesforce.environment` to `production` or `sandbox` (this derives the login URL), or set
`salesforce.login_url` explicitly to a custom My Domain URL (it takes precedence over `environment`).
Then set `salesforce.client_id` to the Consumer Key, `salesforce.username` to the integration user
(jwt_bearer only), and the private key via `salesforce.private_key_file`.

> **Prefer your My Domain URL over the generic hosts.** For `client_credentials` it's mandatory
> (see below). For `jwt_bearer` the generic `login.salesforce.com` still works as the JWT `aud`,
> but Salesforce is tightening this: Spring '26 removed legacy hostname redirects, and External
> Client Apps reject `test.salesforce.com` as an `aud` — so setting `salesforce.login_url` to
> `https://yourorg.my.salesforce.com` (or `...sandbox.my.salesforce.com`) is the future-proof
> choice for both flows.

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
5. **`salesforce.login_url` MUST be your My Domain URL** (e.g.
   `https://yourorg.my.salesforce.com`) — Salesforce rejects the client_credentials grant at the
   generic `login.salesforce.com` / `test.salesforce.com` endpoints, so the `environment`-derived
   default cannot work for this flow. sf2loki fails fast at config load if you leave it generic.

> Trade-off: client credentials transmits a shared secret (symmetric); JWT bearer never sends a secret
> over the wire (asymmetric key). Both yield an access token that works identically for the Pub/Sub,
> REST/SOQL, and EventLogFile paths. See [DESIGN.md §5](DESIGN.md#5-salesforce-auth--oauth-jwt-bearer-or-client-credentials)
> for the protocol-level detail.

### Token lifetime and reconnect churn (ops note)

Neither flow returns `expires_in` or a refresh token — the access token's real lifetime is your
org's **session timeout** (Setup → Session Settings), which can be as short as 15 minutes. sf2loki
handles expiry reactively (re-mints on 401/UNAUTHENTICATED and resubscribes from the stored
replay_id — no data loss), so a short timeout shows up as periodic reconnect churn, not an outage:
a sawtooth on `sf2loki_pubsub_reconnects` and `sf2loki_auth_refreshes` roughly every
session-timeout interval is your org's timeout at work, not a fault. To reduce the churn, raise the
integration user's session timeout (a profile-level Session Settings override works) and set
`salesforce.token_ttl` to match so proactive re-mints land before Salesforce kills the session.

## Configuration

Config loads from a YAML file and/or environment (`SF2LOKI_*` with `__` nesting; env overrides YAML
overrides defaults). Secrets are injected from `*_file` paths or `${ENV}` interpolation; a missing or
unreadable secret is fatal at startup. See [`config.example.yaml`](config.example.yaml) for a runnable
annotated example, and [`docs/config-reference.md`](docs/config-reference.md) for the complete
generated reference (every key, type, default, and description).

Both `config.example.yaml` and `docs/config-reference.md` are **generated from the Pydantic config
schema** — do not hand-edit them. Regenerate after changing config models with `just gen-config`
(or `sf2loki config example` / `sf2loki config reference` directly). CI fails the build if either
file drifts from the schema.

```bash
# run locally against a config file
uv run python -m sf2loki --config config.example.yaml

# validate config + wiring (secrets, label allowlist, source-overlap guard) without
# any network calls — exits 0 (ok) or 1 (invalid)
uv run python -m sf2loki --config config.example.yaml --check
```

> Note: `config.example.yaml` is a **template** — it references `${ENV}` placeholders (e.g.
> `${SF_CLIENT_ID}`, `${GC_LOKI}`) and `*_file` secret paths that must exist before even `--check`
> passes. Export the referenced env vars and put real files at the secret paths (or point the
> `*_file` keys at your own), then run `--check`.

### Grafana Cloud credentials

What sf2loki needs from your Grafana Cloud stack, and where to find it:

- **Token** — create an **Access Policy** token (Cloud Portal → Access Policies) with the
  `logs:write` scope (add `metrics:write` if you enable OTLP telemetry). One token can serve both
  Loki push and OTLP push. This goes in `sink.loki.auth_token_file` (and is reused for telemetry by
  default).
- **Loki push URL + tenant id** — Cloud Portal → your stack → Loki → **Details**: the push URL is
  `https://logs-prod-<zone>.grafana.net/loki/api/v1/push` (→ `sink.loki.url`) and the numeric
  **User / tenant id** shown there (→ `sink.loki.tenant_id`).
- **OTLP endpoint + instance id** — Cloud Portal → your stack → OpenTelemetry → **Details**: the
  gateway is `https://otlp-gateway-<zone>.grafana.net/otlp` (sf2loki needs the full signal path,
  `.../otlp/v1/metrics` → `service.telemetry.endpoint`) with its own numeric **instance id** (→
  `service.telemetry.basic_auth_user`).
- **Trap:** the OTLP **instance id is NOT the Loki tenant id** — they are different numbers on the
  same stack. `service.telemetry.basic_auth_user` defaults to the Loki `tenant_id` when unset,
  which 401s on Grafana Cloud; set it explicitly (see
  [`config.docker.yaml`](config.docker.yaml)'s `GC_OTLP_USER` vs `GC_LOKI_USER`).

### Verify your setup

`sf2loki --check` only validates configuration offline (secrets resolve, labels are legal, sources
don't overlap) — it never touches the network. Once you have real credentials in place, run the live
preflight instead:

```console
$ sf2loki doctor --config config.yaml
name                            status  detail
config                          PASS    configuration and wiring valid
auth                            PASS    flow=jwt_bearer instance_url=https://myorg.my.salesforce.com org_id=00D5g000000ABCDEAU
permissions                     PASS    EventLogFile describe OK (View Event Log Files granted)
pubsub:/event/LoginEventStream  PASS    topic reachable
entitlement                     WARN    org produces 42 Hourly EventType(s) total; no Hourly files found for ReportExport — check Event Monitoring entitlement or the type name
loki                            PASS    pushed 1 test line in 87ms (the only write this command performs)
state                           PASS    state directory /var/lib/sf2loki is writable and lockable
limits                          PASS    DailyApiRequests 14312/15000 remaining

7 passed, 1 warnings, 0 failed, 0 skipped
```

It authenticates for real, checks the integration user's permissions, probes Pub/Sub topic
reachability, reports which configured EventLogFile types the org has actually produced files for,
and pushes exactly **one** test log line to Loki (labelled `source=sf2loki-doctor`) to confirm the
write path end-to-end — that Loki push is the only write `doctor` ever performs. Exits `0` if
nothing FAILed (WARNs are fine — e.g. a configured EventType the org hasn't produced yet), `1` if
anything FAILed. Add `--json` for machine-readable output in CI.

Checks run in order, and later checks that depend on an earlier one SKIP automatically instead of
failing confusingly — e.g. if `auth` FAILs, `permissions`/`pubsub`/`entitlement`/`limits` all report
SKIP (they need a token), but `loki` and `state` still run since they don't depend on Salesforce
auth at all.

### Source recipes

See [`docs/configuring-sources.md`](docs/configuring-sources.md) for recipes — polling arbitrary
custom objects, ingesting login history / setup audit trail, the either/or-per-category rule,
PII redaction & sampling, and cost controls (rate caps + the daily byte budget).

### Presets

[`examples/presets/`](examples/presets/) has ready-to-merge config fragments for common setups (merge
the relevant keys into your `config.yaml` alongside sink/state/service — see
[`config.example.yaml`](config.example.yaml) for the full schema):

- [`custom-object-polling.yaml`](examples/presets/custom-object-polling.yaml) — SOQL-poll an
  arbitrary custom object (e.g. `MyAudit__c`) via `sources.eventlog_objects`.
- [`login-history.yaml`](examples/presets/login-history.yaml) — ingest login activity via
  `LoginHistory` polling, with the `LoginEvent`/`/event/LoginEventStream`/ELF `Login` alternatives
  shown as commented blocks (pick exactly one — they're the same overlap-guard category).
- [`setup-audit-trail.yaml`](examples/presets/setup-audit-trail.yaml) — ingest admin/config change
  history via `SetupAuditTrail` polling.

### Metrics (OTLP)

All metrics push via OTLP/HTTP. Set `service.telemetry.enabled: true` and `service.telemetry.endpoint`
(a Grafana Cloud OTLP gateway, e.g. `https://otlp-gateway-<zone>.grafana.net/otlp/v1/metrics`, or a
local Alloy `http://alloy:4318/v1/metrics`). Basic auth defaults to the Loki sink's
`tenant_id`/`auth_token` (Grafana Cloud uses one stack credential for Loki and OTLP); use
`service.telemetry.auth: none` for an unauthenticated in-cluster Alloy. Enable Salesforce org-limit
metrics (API usage, storage, streaming events, …) with `salesforce.limits.enabled: true`. A ready-made
Grafana dashboard lives in [`deploy/grafana/`](deploy/grafana/), alongside a generated
**alert-rule pack** ([`deploy/grafana/alerts.yaml`](deploy/grafana/alerts.yaml)) covering every
data-loss and degradation signal — see [docs/alerts.md](docs/alerts.md) for what each alert means
and how to provision it.

## Backfilling history

`sf2loki backfill` is a one-shot CLI for pushing historical EventLogFile data into Loki — useful
when you enable ingestion on an org that already has weeks of ELF history you want in Grafana, or
to refill a gap after an outage. It reads the same config file as the daemon but keeps its own
checkpoint file (a `-backfill` sibling of `state.file.path`), so it's safe to run alongside the
running service and is resumable if interrupted.

```bash
sf2loki backfill --config config.yaml --since 2026-05-01 --until 2026-06-01 \
  --event-types Login,API --interval Daily
```

`--since`/`--until` are `YYYY-MM-DD` (UTC); `--until` defaults to now. `--event-types` defaults to
the types configured under `sources.eventlogfile.event_types` (or discovers all types the org
produces if only `["*"]` is configured). `--concurrency` (default 2) bounds concurrent file
downloads. Configured `sources.eventlogfile.transforms` (PII redaction) apply to backfilled rows
too; sampling does not.

Loki rejects samples with timestamps older than its out-of-order window
(`reject_old_samples_max_age`, default 168h/7d), so there are two strategies for pushing history:

**Default (label) strategy** — pushes the true event timestamps and tags every backfilled stream
with `backfill="true"`, giving backfilled data its own set of Loki streams so old timestamps don't
collide with the daemon's live streams. Raise Loki's `reject_old_samples_max_age` (or your Grafana
Cloud stack's out-of-order window) to cover the `--since` date, or use `--ingest-timestamps`
instead. The extra cardinality is one bit per stream — avoid stacking more per-run labels on top.

**`--ingest-timestamps`** — pushes at ingest time (now) instead, so it always lands inside Loki's
out-of-order window with no config changes needed. The true event time is preserved in structured
metadata (`event_time`, ISO-8601) so it's still queryable, and no `backfill` label is added. Use
this when you can't or don't want to touch Loki's ingestion limits.

Resume semantics: progress checkpoints after every fully-pushed file, keyed per
`(interval, event_type)`. Re-running the same command after an interruption picks up from the last
successfully pushed file — already-pushed files are not re-downloaded. If a run does re-push a file
(e.g. a retry landed some but not all of its batches), duplicate rows are harmless in the default
label strategy (byte-identical lines dedup in Loki); in `--ingest-timestamps` mode a rare
retry-related duplicate won't dedup — acceptable for a one-shot backfill tool.

The command prints a summary on exit (files processed, rows pushed/dropped, bytes pushed, API calls
used, elapsed time) and exits `1` only if Loki pushes fail persistently (10 consecutive failures) —
a transient Salesforce or Loki hiccup is retried automatically.

## Run with Docker / docker-compose

The container is the primary run target (alongside ECS). The image is slim, runs as a non-root user,
and exposes `:8080` (`/healthz`, `/readyz`). Metrics are pushed via OTLP, so there is no scrape port.

Every push to `main` publishes a multi-arch image to GHCR (`ghcr.io/rknightion/sf2loki:main`, plus
`:main-<sha>`); releases add semver + `:latest`. The compose file pulls that image by default, so a
deploy is just pull + up:

```bash
# run the published :main edge image — non-secret values from .env.dev, secrets from ./secrets
docker compose --env-file .env.dev pull
docker compose --env-file .env.dev up -d

# …or build from local source instead (dev iteration):
docker compose -f docker-compose.yml -f docker-compose.build.yml up --build
```

[`docker-compose.yml`](docker-compose.yml) mounts [`config.docker.yaml`](config.docker.yaml) (no
secrets — env-driven via `${VAR}` + `*_file`) at `/etc/sf2loki/config.yaml` and `./secrets` (the
private key + Loki token) read-only at `/etc/sf2loki/secrets`. The env file is named **`.env.dev`**
(that exact filename is what the documented `--env-file .env.dev` commands expect) — create it from
the values the config interpolates (Salesforce login URL / consumer key / username, Loki URL +
tenant); `.env*`, `*.key`, `*.crt`, and `secrets/` are gitignored **and** `.dockerignore`d so they
can never be baked into a locally built image. Checkpoint state persists to `./state` (bind-mounted
at `/var/lib/sf2loki`) so resume survives container recreation — the container runs as uid 10001,
so make it writable first: `mkdir -p state && chmod 777 state`.

**Secret file permissions — same uid-10001 rule.** The files in `./secrets` must be *readable* by
uid 10001 or the service crash-loops at startup with an actionable "permission denied" error. A
root-owned `chmod 0600` key file (the natural way to store one) is exactly the trap: use
`chmod 640` plus a group the container user can read, e.g.

```bash
chmod 640 secrets/*        # or chown the files to uid 10001
```

**Health check target — use `/readyz`, not `/healthz`.** `/healthz` is *liveness* (200 whenever the
process is up, even mid-startup before Salesforce auth); `/readyz` is *readiness* (200 only once auth
resolved and the pipeline is running). `/readyz` also degrades to 503 (with a reason in the body)
when Loki pushes have been failing continuously for longer than
`service.unready_after_sink_failing` (default 15m; 0 disables) — data is checkpointed and retried,
so this signals "degraded, surface me", not "restart me"; `/healthz` deliberately stays 200 through
a Loki outage. Docker/ECS collapse container health into a single signal, so
they should probe `/readyz` — the Dockerfile `HEALTHCHECK` already does. For **ECS**, set the task
definition `healthCheck` to `CMD-SHELL curl -f http://localhost:8080/readyz || exit 1` with a
`startPeriod` (~20s) covering normal startup, and mark the container `essential: true` so a fast-fail
(e.g. bad Salesforce credentials → process exits) restarts the task.

**Run exactly one replica** (stop-then-start rollout, not overlapping) — the Pub/Sub API delivers
events independently per subscriber connection, so a second instance double-delivers. See
[DESIGN.md §13](DESIGN.md#13-resilience-lifecycle--ha) for the full HA/replica model and the
`Coordinator` seam that will allow active-passive failover later.

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

See [CONTRIBUTING.md](CONTRIBUTING.md) for the contribution workflow and
[SECURITY.md](SECURITY.md) for reporting vulnerabilities (privately, please).

## License

[AGPL-3.0-only](LICENSE) — free to use, modify, and self-host; if you run a modified version
as a network service, the AGPL requires you to offer its source to those users.
