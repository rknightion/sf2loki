---
title: Installation
description: Docker image, docker-compose, uv/pipx, and config loading rules
---

# Installation

sf2loki requires **Python 3.14+** (it uses 3.14 language features). `pipx`/`uvx` provision a
matching interpreter automatically; the container needs no Python on the host at all.

| Use case | Install | Notes |
| --- | --- | --- |
| **Run the daemon** (recommended) | `docker pull ghcr.io/rknightion/sf2loki:latest` | The long-running service. Multi-arch image, non-root, slim. |
| **CLI / setup tooling** | `uvx sf2loki --help` | Zero-install run of `--check`, `doctor`, `backfill`, `config` — handy during Salesforce app setup before any infra exists. |
| **CLI, persistent** | `pipx install sf2loki` | Same CLI on a VM or air-gapped host where a container isn't wanted. |
| **As a library / from source** | `uv sync` (repo checkout) or `pip install sf2loki` | Optional `sf2loki[s3]` / `sf2loki[gcs]` / `sf2loki[k8s]` extras for the non-default checkpoint stores and Kubernetes-Lease coordinator. |

```bash
uvx sf2loki --version
uvx sf2loki --check --config config.yaml    # validate config + wiring, no network calls
uvx sf2loki doctor --config config.yaml     # live preflight (auth, entitlements, Loki write)
```

The container is the right target for the always-on ingestion daemon; `pipx`/`uvx` are for the
one-shot CLI surfaces (`doctor`, `--check`, `backfill`) you run by hand around setup and
troubleshooting.

## Docker / docker-compose

Every push to `main` publishes a multi-arch image to GHCR (`ghcr.io/rknightion/sf2loki:main`,
plus `:main-<sha>`); releases add semver tags and `:latest`. The container is slim, runs as a
non-root user (uid 10001), and exposes `:8080` for `/healthz` and `/readyz` — metrics push over
OTLP, so there is no scrape port.

```bash
# run the published :latest release image — non-secret values from .env.dev, secrets from ./secrets
docker compose --env-file .env.dev pull
docker compose --env-file .env.dev up -d
```

Set `SF2LOKI_TAG=main` (e.g. in `.env.dev`) to track the rolling edge build instead of a release
— it can carry unreleased and breaking changes, so it's opt-in, never the default.
`SF2LOKI_TAG=main-<sha>` pins a specific edge build.

!!! warning "Check the changelog before bumping `:latest`"
    Releases are semver'd by [release-please](https://github.com/googleapis/release-please) from
    conventional commits. Check the repository's changelog for a `feat!:` / `BREAKING CHANGE:`
    entry between your current and target version before upgrading, the same way you'd check any
    other dependency's major-version notes.

### Volumes and permissions

The container mounts a config file, a read-only secrets directory, and a writable state
directory:

- **Config** — mounted read-only at `/etc/sf2loki/config.yaml`.
- **Secrets** (`*_file` paths, e.g. the private key or Loki token) — mounted read-only, e.g. at
  `/etc/sf2loki/secrets`. They must be **readable by uid 10001** or the container crash-loops
  with an actionable "permission denied" error at startup. A root-owned `chmod 0600` key file is
  exactly the trap: use `chmod 640` plus a group the container user can read (or `chown` the
  files to uid 10001).
- **Checkpoint state** — bind-mount a volume at `/var/lib/sf2loki` so resume survives container
  recreation. The container runs as uid 10001, so the host directory must be writable by it:
  `mkdir -p state && chmod 770 state && chown 10001 state` (770 + chown, not a permissive 777 —
  `sf2loki doctor`'s own failure hint recommends the same).

!!! danger "Health check target — use `/readyz`, not `/healthz`, for lifecycle checks"
    `/healthz` is *liveness* (200 whenever the process is up, even mid-startup); `/readyz` is
    *readiness* (200 only once auth has resolved and the pipeline is running, and it degrades to
    503 if Loki pushes have been failing continuously). For a **standalone** instance, point a
    Docker `HEALTHCHECK` or an ECS task `healthCheck` at `/readyz` — the shipped Dockerfile
    already does. On an **active-passive HA replica**, the standby's `/readyz` is 503 forever by
    design (it never becomes ready), so a task-level `healthCheck` restart-loops it and defeats
    failover — use `/healthz` there instead. See [High Availability](deployment/high-availability.md)
    for the full readiness-vs-liveness split.

Run **exactly one active replica** outside of the HA pair described in
[High Availability](deployment/high-availability.md) — the Pub/Sub API delivers events
independently per subscriber connection, so a second concurrently-active instance double-delivers.

## From source (uv)

```bash
git clone https://github.com/rknightion/sf2loki.git
cd sf2loki
just setup         # uv sync — create the venv from the lockfile
just gate          # ruff + mypy --strict + pytest — the green bar
uv run python -m sf2loki --config config.example.yaml --check
```

`just setup` is a thin wrapper over `uv sync`; use `uv sync` directly if you don't have `just`
installed. See [Development](development/contributing.md) for the full contribution workflow.

## Configuration loading

Config loads from a YAML file and/or environment variables, in this precedence order (highest
first):

1. **Environment variables** — `SF2LOKI_*`, with `__` (double underscore) as the nesting
   delimiter, e.g. `SF2LOKI_SALESFORCE__CLIENT_ID` sets `salesforce.client_id`.
2. **YAML file** — passed via `--config`.
3. **Built-in defaults.**

Secrets are never inlined as plain config values by default. Two mechanisms inject them:

- **`*_file` paths** — e.g. `salesforce.private_key_file`, `sink.loki.auth_token_file` — point at
  a mounted secret file; sf2loki reads its contents at startup.
- **`${ENV}` interpolation** — any config value can reference an environment variable, e.g.
  `client_id: ${SF_CLIENT_ID}`, resolved when the file is loaded.

A missing or unreadable secret (an unset `${ENV}` reference, or a `*_file` path that doesn't
exist or isn't readable) is **fatal at startup** — sf2loki never falls back to a silent blank.

Validate the whole thing offline with `--check` (secrets resolve, Loki labels are legal, sources
don't overlap — no network calls):

```bash
uv run python -m sf2loki --config config.yaml --check
```

See [Configuration](configuration/index.md) for the full settings guide and
[config-reference.md](config-reference.md) for the generated reference of every key, type,
default, and description.
