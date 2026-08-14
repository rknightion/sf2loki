---
id: SFL-0068
title: >-
  deploy: docker-compose runs unhardened (no read_only / cap_drop /
  no-new-privileges) and the image chowns /app to the runtime uid
status: To Do
assignee: []
created_date: '2026-08-14 16:58'
labels:
  - followup
  - phase-2
milestone: m-2
dependencies: []
references:
  - 'https://github.com/rknightion/sf2loki/issues/152'
ordinal: 68000
---

## Description

<!-- SECTION:DESCRIPTION:BEGIN -->
## What

`docker-compose.yml` is the documented standalone deploy target (`docker-compose.yml:2-3` — "This is the deploy target (e.g. jules)"; `docs/deployment/index.md:17-18` — "the baseline for a standalone deployment"). Its service block hardens resources and network exposure but not runtime privileges:

- `docker-compose.yml:20-57` sets `image`, `pull_policy`, `env_file`, three bind mounts (`:24-34`), `command`, loopback-only `ports` (`:40`), `stop_grace_period: 35s` (`:45`), `mem_limit`/`mem_reservation` (`:52-53`) and `restart: unless-stopped` (`:57`).
- It sets **no** `read_only: true`, **no** `cap_drop: [ALL]`, **no** `security_opt: ["no-new-privileges:true"]`, and **no** `tmpfs` for `/tmp`. Those four tokens appear nowhere in the file (the only occurrences in the repo are the Helm chart's, below).

The image compounds it. `Dockerfile:43` is `COPY --from=builder --chown=sf2loki:sf2loki /app /app` and `Dockerfile:48` is `USER sf2loki` (uid/gid 10001, created at `Dockerfile:39-40`). uid 10001 therefore **owns** `/app`, `/app/.venv` (the interpreter on `PATH`, `Dockerfile:45`) and the source tree, so with a writable root filesystem the service can rewrite the code it executes on the next start. Nothing in the image requires that ownership: `UV_COMPILE_BYTECODE=1` (`Dockerfile:15`) pre-compiles installed packages at build time, and the runtime only ever *reads* `/app`.

The Helm chart already runs the same image fully hardened, which proves the posture is viable:

- `deploy/helm/values.yaml:169-174` — `readOnlyRootFilesystem: true`, `allowPrivilegeEscalation: false`, `capabilities.drop: [ALL]`.
- `deploy/helm/values.yaml:163-168` — `runAsNonRoot`, `runAsUser`/`runAsGroup`/`fsGroup: 10001`, `seccompProfile: RuntimeDefault`.
- `deploy/helm/templates/deployment.yaml:159-164` — only two writable paths: the state dir and a `/tmp` emptyDir.
- `docs/deployment/kubernetes.md:143-144` documents that posture, but only for Kubernetes.

Two constraints any fix must respect:

1. **`/tmp` must stay writable.** `src/sf2loki/salesforce/eventlogfile_client.py:310` spools each EventLogFile body into a `SpooledTemporaryFile(max_size=_SPOOL_MAX_MEMORY_BYTES)`; `_SPOOL_MAX_MEMORY_BYTES = 8 * 1024 * 1024` (`:37`). Above 8 MiB the spool spills to `tempfile.gettempdir()`, so `read_only: true` without a writable `/tmp` breaks ELF ingestion of any non-trivial file. This is why the chart mounts a `/tmp` emptyDir.
2. **The three existing bind mounts are unaffected by `read_only`** — Docker exempts volume/bind targets, so `/etc/sf2loki/config.yaml` (ro), `/etc/sf2loki/secrets` (ro) and `/var/lib/sf2loki` (rw, `docker-compose.yml:25-34`, matching `config.docker.yaml:79`) keep working.

Nothing enforces or documents this. `.github/workflows/docker-security.yml` delegates to the shared reusable image-CVE scan; no test references `docker-compose.yml`; `docs/security.md` covers secret handling and the unauthenticated health endpoints but not container runtime privileges; `docs/deployment/index.md:86-90` "Known gaps" lists only the missing Terraform module.

## Why it matters

Given any code-execution primitive inside the container (a dependency vulnerability on the Avro decode, CSV parse, protobuf, or HTTP paths), the writable-and-owned `/app` tree converts a transient compromise into a persistent one: the attacker overwrites a module under `/app/.venv/lib/python3.14/site-packages/` (or the entrypoint package), and because `restart: unless-stopped` (`docker-compose.yml:57`) restarts the **same** container with the same writable layer, the implant is re-executed on every crash-restart until an operator explicitly recreates the container. The process holds live Salesforce credentials and a Grafana Cloud push token, so persistence means indefinite credential access and the ability to tamper with the audit-event stream being forwarded to Loki.

Missing `security_opt: ["no-new-privileges:true"]` additionally leaves the setuid-root binaries that ship in `python:3.14-slim` (`su`, `mount`, `passwd`, `gpasswd`, `chsh`, …) usable as an escalation path from uid 10001. `cap_drop: [ALL]` is defence in depth on top: the process already has an empty effective capability set because it runs as a non-root uid with no file or ambient capabilities, but dropping the bounding set removes what a successful escalation could inherit.

An identical compromise on the Helm deployment cannot modify code or persist (`deploy/helm/values.yaml:169-174`). The compose and ECS paths are the ones left exposed, and they are the paths the docs point standalone operators at.

## Proposed approach

1. **`docker-compose.yml`** — add to the `sf2loki` service, with a comment explaining the `/tmp` requirement and pointing at `eventlogfile_client.py`'s spool:

   ```yaml
       read_only: true
       tmpfs:
         # EventLogFile bodies spool to /tmp above 8 MiB
         # (salesforce/eventlogfile_client.py:37,310); read_only forbids any
         # other writable path, so /tmp must be an explicit tmpfs.
         - /tmp
       cap_drop:
         - ALL
       security_opt:
         - "no-new-privileges:true"
   ```

   Size the tmpfs if the host needs a bound (`- /tmp:size=64m`), keeping headroom above the largest expected EventLogFile blob.

2. **`Dockerfile:43`** — drop `--chown=sf2loki:sf2loki` so the runtime tree is root-owned and world-readable, making code immutable to uid 10001 even where `read_only` is forgotten (a hand-rolled `docker run`, the ECS path, an operator-modified compose file). Keep `USER sf2loki` (`:48`). Note: if the root project is installed editable (uv's default for the workspace root), CPython can no longer write `__pycache__` under `/app/src`; that write failure is silently ignored, and the only cost is per-start bytecode recompilation of the project's own modules. If that cost is measurable, either set `tool.uv.package`/`--no-editable` so the project lands in `site-packages` (already byte-compiled by `UV_COMPILE_BYTECODE=1`, `Dockerfile:15`) or pre-create the `__pycache__` dirs in the builder stage — do **not** restore the chown.

3. **`docker-compose.build.yml`** — confirm the local-build override still starts under the same flags (it only layers `build:`, so no change is expected).

4. **Docs** — add the hardening flags and the `/tmp` requirement to `docs/deployment/index.md` (Docker section, `:15-38`) and a "container runtime privileges" subsection to `docs/security.md` stating that the shipped compose file and the Helm chart both run non-root, read-only-rootfs, all-capabilities-dropped, and that removing any of them is an operator decision. Extend the ECS guidance (`docs/deployment/index.md:40-53`) with the equivalent task-definition settings (`readonlyRootFilesystem: true`, a `/tmp` tmpfs via `linuxParameters.tmpfs`, `linuxParameters.capabilities.drop: ["ALL"]`).

5. **Regression gate** — add `tests/test_deploy_compose_hardening.py` (pure `yaml.safe_load` + text assertions, no Docker required, same style as `tests/test_config_artifacts_drift.py`) so the flags cannot silently regress, and assert the runtime `COPY` in `Dockerfile` carries no `--chown`.

6. **Live check before closing** — run `docker compose up -d` against a dev config and confirm the service reaches `/readyz` 200 and that an EventLogFile larger than 8 MiB is ingested (the spool-spill path), rather than asserting it from the config alone.

---

Imported from GitHub issue #152 on 2026-08-14, when this repo migrated from GitHub Issues to Backlog.md. The original issue has been deleted; its verbatim body, labels and comments are preserved in `archive/issues-dump.json` (`jq '.[] | select(.number == 152)' archive/issues-dump.json`).

<sub>Filed from the 2026-07-30 full-repo audit (11 finder lanes + adversarial verification per finding).</sub>
<!-- SECTION:DESCRIPTION:END -->

## Acceptance Criteria
<!-- AC:BEGIN -->
- [ ] #1 `docker-compose.yml` sets `read_only: true`, `tmpfs: [/tmp]`, `cap_drop: [ALL]`, and `security_opt: ["no-new-privileges:true"]` on the `sf2loki` service, with a comment naming the `/tmp` spool dependency.
- [ ] #2 `Dockerfile:43` no longer passes `--chown=sf2loki:sf2loki`; `USER sf2loki` (`:48`) is unchanged and the container still starts.
- [ ] #3 `tests/test_deploy_compose_hardening.py` parses `docker-compose.yml` and asserts all four hardening keys on the `sf2loki` service (`read_only is True`, `ALL` in `cap_drop`, `no-new-privileges:true` in `security_opt`, a `/tmp` entry in `tmpfs`).
- [ ] #4 The same test asserts the three existing mounts survive: `/etc/sf2loki/config.yaml` and `/etc/sf2loki/secrets` read-only, `/var/lib/sf2loki` writable.
- [ ] #5 The same test asserts the runtime-stage `COPY --from=builder ... /app /app` line in `Dockerfile` contains no `--chown`.
- [ ] #6 `docs/deployment/index.md` documents the four flags and why `/tmp` must be writable; `docs/security.md` gains a container-runtime-privileges subsection covering both compose and Helm.
- [ ] #7 `docs/deployment/index.md` ECS section lists the equivalent task-definition settings (`readonlyRootFilesystem`, `linuxParameters.tmpfs` for `/tmp`, `linuxParameters.capabilities.drop`).
- [ ] #8 Live verification recorded on the issue: `docker compose up -d` reaches `/readyz` 200 with the flags applied, and an EventLogFile body exceeding 8 MiB is ingested end to end (exercises the `/tmp` spill).
- [ ] #9 `just gate` green.
<!-- AC:END -->

## Definition of Done
<!-- DOD:BEGIN -->
- [ ] #1 just gate is green (ruff check + ruff format --check + mypy src + pytest) — run it, don't assert it
- [ ] #2 just gen-config run and its output committed, if config.py changed (CI drift gate fails otherwise)
- [ ] #3 committed straight to main with a conventional-commit message, and pushed
<!-- DOD:END -->
