---
id: SFL-0064
title: 'state: Azure Blob checkpoint store backend'
status: To Do
assignee: []
created_date: '2026-08-14 16:58'
labels:
  - followup
  - phase-3
  - roadmap
milestone: m-4
dependencies: []
references:
  - 'https://github.com/rknightion/sf2loki/issues/148'
ordinal: 64000
---

## Description

<!-- SECTION:DESCRIPTION:BEGIN -->
## What

`build_store` (`src/sf2loki/state/__init__.py:13-46`) supports exactly three backends: `s3` (`:25-32`), `gcs` (`:34-44`), and a `file` fallback (`:46`). The config Literal is `Literal["file", "s3", "gcs"]` (`src/sf2loki/config.py:1050-1058`), with sub-config fields only for those three (`:1059-1067`) and a bucket-required validator covering only s3/gcs (`:1069-1075`). Extras are `s3`/`gcs`/`k8s` (`pyproject.toml:28-31`). There is no Azure Blob Storage backend anywhere in the tree — a repo-wide grep for `azure` matches only the `azure/setup-helm` action at `.github/workflows/ci.yml:132`.

The two remote backends already implement the same primitive under different vendor names, and both raise the same error type:

- `S3CheckpointStore` (`src/sf2loki/state/s3_store.py:150`) — whole-document `PutObject` with `IfMatch` on the current ETag, `IfNoneMatch: *` for the first write (`:265-310`), CAS failure classified to `StateStoreConflictError` (`:45`).
- `GcsCheckpointStore` (`src/sf2loki/state/gcs_store.py:105`) — same document, `ifGenerationMatch` precondition (`"0"` for first write), same `StateStoreConflictError` imported from the S3 module (`gcs_store.py:28`).

Documented at `docs/deployment/state.md:31-35`. Azure Blob Storage supports the identical primitive natively — `Put Blob` with `If-Match` on the blob ETag, returning `412 Precondition Failed` when another writer won the race, and `If-None-Match: *` for create-only (see Azure's own optimistic-concurrency guidance) — reachable from asyncio via `azure.storage.blob.aio.BlobServiceClient` with `azure.identity.aio.DefaultAzureCredential`.

## Why it matters

The published Helm chart makes an object-store state backend mandatory for HA and names only s3/gcs:

- `deploy/helm/values.yaml:14-15` — "expects `config.coordinate.type: k8s_lease` AND a SHARED state store (`config.state.store: s3 | gcs` — the local `file` store is per-pod and INVALID"
- `deploy/helm/values.yaml:179`, `:216`, `:620`
- render guard at `deploy/helm/templates/deployment.yaml:9`, comment at `:178`
- `deploy/helm/templates/networkpolicy.yaml:156-163` (state-store egress), `deploy/helm/templates/rbac.yaml:30`

AKS is already a first-class HA target because the `k8s_lease` coordinator exists (`src/sf2loki/coordinate/k8s_lease.py`, `config.py:1123+`). So an AKS or Azure Container Apps operator gets a native coordinator but has no native stateless checkpoint store: Azure Blob exposes no S3-compatible endpoint, so the options are (a) an RWX Azure Files / PVC mount with the `file` store — which re-introduces the shared-volume dependency the s3/gcs backends exist to remove, and interacts badly with the file store's `flock`-based exclusivity (`src/sf2loki/state/file_store.py`), (b) a third-party S3-gateway sidecar, or (c) cross-cloud egress to S3/GCS. Every one of those is infrastructure an equivalent EKS/GKE deployment does not need, for a CAS pattern this codebase has already implemented twice.

## Proposed approach

Port the s3/gcs shape rather than inventing a new one.

1. **Config** — add `AzureStateConfig` next to `GcsStateConfig` (`src/sf2loki/config.py:1023-1047`):
   - `account_url: str = ""` (e.g. `https://<account>.blob.core.windows.net`)
   - `container: str = ""` (required when `state.store == "azure"`)
   - `blob_name: str = "sf2loki/state.json"`
   - `connection_string_file: Path | None = None` — read the connection string from a file (secret-mount friendly, mirrors how other secrets are handled); when unset, auth is `DefaultAzureCredential` (workload identity on AKS)
   Extend the Literal at `config.py:1050` to `Literal["file", "s3", "gcs", "azure"]`, add the `azure:` field alongside `:1062-1067`, and extend `_require_bucket_for_remote` (`:1069-1075`) to require `container` (and `account_url` unless a connection string is configured) when `store == "azure"`.

2. **Store** — `src/sf2loki/state/azure_store.py`, `AzureCheckpointStore`, method-for-method with `S3CheckpointStore`: `load` (`s3_store.py:256`), `commit` (`:262`), `commit_many` (`:265`), `delete` (`:310`), `set_fence` (`:184`), `reset` (`:193`), `close` (`:361`), plus the lazily-cached client (`_get_client`, `:217`) and cached document (`_ensure_loaded`, `:224`). Reuse `StateStoreConflictError` / `StateObjectCorruptError` and the `_is_transient` / `_retry_transient` retry discipline (`s3_store.py:45`, `:56`, `:112-148`) exactly as `gcs_store.py:28` does — do not fork a second retry policy.
   - Update: `upload_blob(payload, overwrite=True, etag=<current>, match_condition=MatchConditions.IfNotModified)`.
   - First write: `match_condition=MatchConditions.IfMissing` (`If-None-Match: *`).
   - Map `412`/`409` (`azure.core.exceptions.ResourceModifiedError`, `ResourceExistsError`) to `StateStoreConflictError` and **never** retry it; retry only transient 5xx / connection errors.
   - **Do not implement `set_epoch`.** It is file-store-only (`src/sf2loki/state/file_store.py:80`) and the app installs it via `getattr` (`src/sf2loki/app.py:966-968`); neither remote store has it.
   - **No top-level import of `azure.*`.** Follow the documented reason at `gcs_store.py:1-16` and `_default_client_factory` (`gcs_store.py:159`): build the client lazily inside a factory so the module stays importable, unit-testable with an injected fake client, and `mypy --strict`-clean without the extra installed (which is why no `[[tool.mypy.overrides]]` entry exists for `aiobotocore`/`gcloud` at `pyproject.toml:95-97` — keep it that way for `azure`).

3. **Factory** — add an `azure` branch to `build_store` (`state/__init__.py`) with the same explicit `importlib.util.find_spec` guard and actionable `ConfigError` the s3/gcs branches use (`:26-32`, `:36-44`). Probe the top-level package name (`azure.storage.blob` is a namespace package — verify which bare name `find_spec` resolves cleanly, per the `gcloud` note at `:35-38`, and guard on that).

4. **Extra** — `azure = ["azure-storage-blob>=12.24", "azure-identity>=1.19"]` in `pyproject.toml:28-31`; refresh `uv.lock`.

5. **Doctor** — extend the state probe: `_probe_state_config` (`src/sf2loki/doctor.py:531-548`) needs an `azure` branch producing a probe-suffixed `blob_name`, and `_state_object_target` (`:550-554`) an `azure://<container>/<blob_name>` target string. `_check_state` (`:487`) already routes every non-`file` store through `_check_state_object` (`:557`), so no dispatch change is needed there.

6. **Docs / generated artifacts** — run `just gen-config` to regenerate `config.example.yaml` and `docs/config-reference.md` (drift-gated by `tests/test_config_artifacts_drift.py`); add an `azure` row to the backend table at `docs/deployment/state.md:31-35` and to the `see also` line at `:105`; update `docs/installation.md:16`, `README.md:37`/`:449-459`, `deploy/helm/values.yaml:14-15`/`:179`/`:216` (and the regenerated `values.yaml` config block plus its Helm drift gate), `deploy/helm/templates/deployment.yaml:178`, `deploy/helm/templates/networkpolicy.yaml:156-157`, `deploy/helm/templates/rbac.yaml:30`. `sf2loki state` needs no change — `src/sf2loki/statecmd.py` goes through `build_store`.

---

Imported from GitHub issue #148 on 2026-08-14, when this repo migrated from GitHub Issues to Backlog.md. The original issue has been deleted; its verbatim body, labels and comments are preserved in `archive/issues-dump.json` (`jq '.[] | select(.number == 148)' archive/issues-dump.json`).

<sub>Filed from the 2026-07-30 full-repo audit (11 finder lanes + adversarial verification per finding).</sub>
<!-- SECTION:DESCRIPTION:END -->

## Acceptance Criteria
<!-- AC:BEGIN -->
- [ ] #1 `state.store: azure` selectable; `AzureStateConfig` added with `account_url` / `container` / `blob_name` / `connection_string_file`, and the `Literal` at `src/sf2loki/config.py:1050` extended.
- [ ] #2 `AzureCheckpointStore` implements `load` / `commit` / `commit_many` / `delete` / `set_fence` / `reset` / `close`, with no `set_epoch`.
- [ ] #3 `src/sf2loki/state/azure_store.py` imports no `azure.*` symbol at module scope; `just gate` is green with the `azure` extra **not** installed.
- [ ] #4 `pyproject.toml` declares the `azure` extra; `uv.lock` refreshed.
- [ ] #5 Doctor probes the Azure backend against a probe-suffixed blob, never the real checkpoint blob.
- [ ] #6 `just gen-config` re-run; config/docs/Helm drift gates green.
- [ ] #7 Tests, mirroring `tests/state/test_gcs_store.py` with an injected fake client (no live Azure): - [ ] first write uses the create-only precondition (`If-None-Match: *` / `MatchConditions.IfMissing`); a subsequent write uses `If-Match` with the ETag returned by the previous upload - [ ] a 412 on update raises `StateStoreConflictError` and is **not** retried (assert exactly one upload attempt) - [ ] a transient 503 on upload is retried and then succeeds; a transient 503 on download is retried on `load` - [ ] non-JSON / truncated blob content raises `StateObjectCorruptError` - [ ] `commit_many` writes one blob containing all keys (single upload call), and `load` returns each; `delete` removes a key and preserves the rest - [ ] `set_fence` refuses a commit once the fence is lost, matching the s3/gcs behaviour asserted in `tests/state/test_s3_store.py` - [ ] `reset` drops the cached document so the next `load` re-downloads - [ ] `close` is awaited and closes the client (regression guard for the leak fixed in #52) - [ ] `tests/state/test_build_store.py` gains an azure case asserting the missing-extra `ConfigError` names `pip install 'sf2loki[azure]'` (mirroring `:29`) - [ ] config validation: `state.store: azure` with an empty `container` fails with an actionable message
<!-- AC:END -->

## Definition of Done
<!-- DOD:BEGIN -->
- [ ] #1 just gate is green (ruff check + ruff format --check + mypy src + pytest) — run it, don't assert it
- [ ] #2 just gen-config run and its output committed, if config.py changed (CI drift gate fails otherwise)
- [ ] #3 committed straight to main with a conventional-commit message, and pushed
<!-- DOD:END -->
