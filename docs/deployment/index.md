# Deployment & Operations

sf2loki is a long-running process — one container, one process, no worker pool. The
container image is the primary run target; there is no Helm chart or Terraform module
yet, so Kubernetes and ECS both run from the plain manifests / task-definition JSON
described on this page and in [Kubernetes](kubernetes.md).

## Container image

Every push to `main` publishes a multi-arch image to GHCR
(`ghcr.io/rknightion/sf2loki:main`, plus `:main-<sha>`); releases add semver tags and
`:latest`. `:latest` tracks releases, not the edge build — set `SF2LOKI_TAG=main` only
for dev/staging, since it can carry unreleased or breaking changes.

## Docker / docker-compose

[`docker-compose.yml`](https://github.com/rknightion/sf2loki/blob/main/docker-compose.yml)
is the baseline for a standalone deployment:

```bash
docker compose --env-file .env.dev pull
docker compose --env-file .env.dev up -d
```

It mounts three things into the container (uid `10001`, non-root):

- [`config.docker.yaml`](https://github.com/rknightion/sf2loki/blob/main/config.docker.yaml)
  at `/etc/sf2loki/config.yaml` — no secrets, `${VAR}` interpolation only.
- `./secrets` at `/etc/sf2loki/secrets`, read-only — the Salesforce private key and Loki
  token. These files must be *readable* by uid `10001` or the service crash-loops at
  startup: `chmod 640 secrets/*` (a root-owned `chmod 0600` key is the trap).
- `./state` at `/var/lib/sf2loki` — durable checkpoint state, so a recreated container
  resumes instead of re-ingesting. Must be writable by uid `10001`:
  `mkdir -p state && chmod 770 state && chown 10001 state`.

Non-secret config values (login URL, consumer key, Loki URL/tenant) are interpolated
from an env file named exactly `.env.dev` — that's the filename the documented
`--env-file .env.dev` commands expect.

## ECS / Fargate

The container runs on ECS the same way: mount the config and secrets the same way
(EFS or a secrets provider in place of bind mounts), and set the task definition's
`healthCheck` to poll `/readyz`:

```
CMD-SHELL curl -f http://localhost:8080/readyz || exit 1
```

with a `startPeriod` of ~20s covering normal startup, and the container marked
`essential: true` so a fast-fail (e.g. bad Salesforce credentials) restarts the task.
Set `stopTimeout` to at least 35s (`service.shutdown_grace` default 25s + the app's own
~5s closer budget + margin) — ECS's own default (30s) is borderline.

!!! danger "Never point an ECS/Docker task-level health check at `/readyz` on an HA replica"
    `/readyz` on a standby in an active-passive pair returns `503 standby` **forever, by
    design** — see [High Availability](high-availability.md). A task-level `healthCheck`
    (which triggers ECS to kill and replace an "unhealthy" task) or a Docker
    `HEALTHCHECK` pointed at `/readyz` restart-loops the standby forever and defeats
    failover. `/readyz` is only safe as a **target-group** health check (controls
    traffic routing, not task lifecycle); for the task-level check use `/healthz`
    instead. Standalone (single-instance, `coordinate.type: noop`) deployments don't hit
    this trap — `/readyz` and `/healthz` agree once the pipeline is up.

## Health endpoints

The container exposes `:8080` with two unauthenticated endpoints (loopback-only by
default in `docker-compose.yml` — don't expose them to the network without a reason):

- **`/healthz`** — liveness. `200` whenever the process is up, even mid-startup before
  Salesforce auth resolves, and stays `200` through a Loki outage. This is what a
  restart/liveness check should target.
- **`/readyz`** — readiness. `200` only once auth has resolved and the pipeline is
  running; degrades to `503` (with a reason in the body) when Loki pushes have been
  failing continuously for longer than `service.unready_after_sink_failing` (default
  15m; `0` disables). Data is checkpointed and retried during that window, so a `503`
  here means "degraded, surface me", not "restart me". This is what a load-balancer /
  target-group check should target.

On an active-passive HA pair the standby reports `503 standby` on `/readyz` forever
while staying `200` on `/healthz` — that's correct (it keeps traffic off the standby)
but the two endpoints must never be pointed at the same kind of check. See
[High Availability](high-availability.md) for the full model and the coordinator
options.

## Known gaps

- **No Helm chart.** Kubernetes deployments use the plain example manifests in
  [`deploy/k8s/`](https://github.com/rknightion/sf2loki/tree/main/deploy/k8s) — see
  [Kubernetes](kubernetes.md).
- **No Terraform module.** ECS/Fargate task definitions and any supporting
  infrastructure (EFS, secrets provider, log groups) are yours to author; nothing is
  published for either.

## See also

- [Kubernetes](kubernetes.md) — example manifests for the HA pair.
- [High Availability](high-availability.md) — the active-passive model and coordinators.
- [State & Checkpoints](state.md) — checkpoint backends and the `sf2loki state` CLI.
- [Configuration](../configuration/index.md) and the
  [full config reference](../config-reference.md).
