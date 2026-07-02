---
title: Getting Started
description: Zero to first log lines in Loki in about 5 minutes
---

# Getting Started

This walks through the fastest path to seeing Salesforce events in Loki: one Salesforce org,
one Pub/Sub topic, `client_credentials` auth (no keypair to generate), running the published
container. See [Installation](installation.md) for other install methods and
[Configuration](configuration/index.md) for everything this skips.

## Prerequisites

- A Salesforce External Client App with the **Client Credentials Flow** enabled, a Consumer Key
  + Secret, and a **Run As** user with **View Real-Time Event Monitoring Data** + **API Enabled**
  permissions. See the [README's Salesforce setup walkthrough](https://github.com/rknightion/sf2loki/blob/main/README.md#salesforce-setup-oauth)
  if you haven't created one yet.
- A Grafana Cloud stack (or any Loki-compatible endpoint) and its push URL, tenant ID, and an
  access-policy token with the `logs:write` scope.
- Docker and Docker Compose.

## 1. Write a minimal config

```yaml title="config.yaml"
salesforce:
  # client_credentials needs your My Domain URL explicitly — the generic
  # login.salesforce.com / test.salesforce.com hosts reject this flow.
  login_url: https://yourorg.my.salesforce.com
  auth_mode: client_credentials
  client_id: ${SF_CLIENT_ID}
  client_secret_file: /etc/sf2loki/secrets/client-secret

sources:
  pubsub:
    topics: ["/event/LoginEventStream"]

sink:
  loki:
    url: ${GC_LOKI}
    tenant_id: ${GC_TENANT_ID}
    auth_token_file: /etc/sf2loki/secrets/loki-token
```

Everything else (batching, structured metadata, HA, telemetry) has a working default — see the
[full config reference](config-reference.md) for every key.

!!! note "`${VAR}` placeholders"
    `${SF_CLIENT_ID}`, `${GC_LOKI}`, and `${GC_TENANT_ID}` are interpolated from the environment
    at load time; a missing variable is fatal at startup, not a silent blank.

## 2. Provide secrets and run it

```bash title="docker-compose.yml"
services:
  sf2loki:
    image: ghcr.io/rknightion/sf2loki:latest
    environment:
      SF_CLIENT_ID: "your-consumer-key"
      GC_LOKI: "https://logs-prod-<zone>.grafana.net/loki/api/v1/push"
      GC_TENANT_ID: "your-loki-tenant-id"
    volumes:
      - ./config.yaml:/etc/sf2loki/config.yaml:ro
      - ./secrets:/etc/sf2loki/secrets:ro
      - ./state:/var/lib/sf2loki
    ports:
      - "127.0.0.1:8080:8080"
    command: ["--config", "/etc/sf2loki/config.yaml"]
```

```bash
mkdir -p secrets state
echo -n "your-consumer-secret" > secrets/client-secret
echo -n "your-grafana-cloud-token" > secrets/loki-token
chmod 640 secrets/*          # the container runs as uid 10001; root-owned 0600 files crash-loop it
mkdir -p state && chmod 770 state && chown 10001 state

docker compose up -d
```

!!! tip "Validate before you run it"
    `docker compose run --rm sf2loki --config /etc/sf2loki/config.yaml --check` validates secrets,
    the label allowlist, and source-overlap rules without touching the network — a fast way to
    catch a typo before the container starts polling Salesforce.

## 3. Confirm ingestion

Every stream sf2loki writes carries `job="sf2loki"` and `service_name="sf2loki"`. In Grafana
Explore (or `logcli`), run:

```logql
{service_name="sf2loki"} | json
```

You should see `LoginEventStream` rows arriving within a few seconds of a login on the org. If
nothing shows up, check `docker compose logs -f sf2loki` and run `doctor` for a live preflight
(auth, topic reachability, a one-line Loki test write):

```bash
docker compose run --rm sf2loki doctor --config /etc/sf2loki/config.yaml
```

## Next steps

- [Configuration](configuration/index.md) — every setting, config loading precedence, and secret
  sourcing.
- [Sources](sources/index.md) — add SOQL-polled objects, EventLogFile, ApexLog, or your own
  custom platform events / Change Data Capture channels.
- [Deployment](deployment/index.md) — production Docker/Kubernetes deployment, HA, and
  checkpoint stores.
