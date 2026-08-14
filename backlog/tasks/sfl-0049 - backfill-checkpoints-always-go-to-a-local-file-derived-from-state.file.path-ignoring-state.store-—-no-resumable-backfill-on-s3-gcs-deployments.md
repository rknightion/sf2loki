---
id: SFL-0049
title: >-
  backfill: checkpoints always go to a local file derived from state.file.path,
  ignoring state.store — no resumable backfill on s3/gcs deployments
status: To Do
assignee: []
created_date: '2026-08-14 16:58'
labels:
  - followup
  - phase-5
milestone: m-0
dependencies: []
references:
  - 'https://github.com/rknightion/sf2loki/issues/133'
ordinal: 49000
---

## Description

<!-- SECTION:DESCRIPTION:BEGIN -->
## What

`run_backfill` builds its checkpoint store unconditionally from the file backend, regardless of `state.store`:

```python
# src/sf2loki/backfill.py:742-744
state_path = cfg.state.file.path
backfill_state_path = state_path.with_name(f"{state_path.stem}-backfill{state_path.suffix}")
store = FileCheckpointStore(backfill_state_path)
```

There is no branch on `cfg.state.store` anywhere in `backfill.py` — that store is threaded straight into `_process_event_type` (`backfill.py:575`, `backfill.py:581`, `backfill.py:455`, closed at `backfill.py:784`).

The state is reachable with a fully valid config. `StateConfig` (`src/sf2loki/config.py:1049-1075`) always materialises `file: FileStateConfig` from its `default_factory`, and `FileStateConfig.path` defaults to `/var/lib/sf2loki/state.json` (`src/sf2loki/config.py:987-991`). Its only validator, `_require_bucket_for_remote` (`src/sf2loki/config.py:1069-1075`), checks `state.s3.bucket` / `state.gcs.bucket` and never inspects `state.file`. So `state.store: s3` with an untouched default `state.file.path` is a normal, valid, expected config — and `sf2loki backfill` against it writes resume state to `/var/lib/sf2loki/state-backfill.json` on local disk.

Every other consumer of checkpoint state already dispatches on the configured backend through the shared factory `build_store` (`src/sf2loki/state/__init__.py:12-47`): the daemon, and `sf2loki state show/set/delete` (`src/sf2loki/statecmd.py:110`). The doctor was fixed for precisely this class of mismatch under issue #59 — `_check_state` now dispatches on `cfg.state.store` (`src/sf2loki/doctor.py:486-495`, whose comment names the "doctor validates a state dir the deployment doesn't use" gap). `backfill.py:742` is the remaining place that assumes the file backend.

Two concrete consequences on an object-store deployment:

1. **Lost resume.** The Helm chart mounts an `emptyDir` at `stateDir` even for `s3`/`gcs` (`deploy/helm/values.yaml:177-180`, `deploy/helm/templates/deployment.yaml:177-182`) because `readOnlyRootFilesystem: true` (`deploy/helm/values.yaml:170`) forbids unlisted writable paths. An `emptyDir` is per-pod and dies with the pod, so backfill progress does not survive a restarted/rescheduled Job pod.
2. **Unhandled crash when the path is not writable.** The first `store.load()` (`backfill.py:575`) reaches `_acquire_instance_lock` (`src/sf2loki/state/file_store.py:117-136`), which does `self._path.parent.mkdir(parents=True, exist_ok=True)` then `os.open(lock_path, O_RDWR|O_CREAT)`. On a pod (or a laptop run as a non-root user) where `/var/lib/sf2loki` is neither present nor creatable, that `OSError` propagates through `run_backfill`'s `try/finally` and out of `cli.py:230-241` — the surrounding `try/except (ConfigError, ValueError)` at `cli.py:205-219` wraps only config loading, so the operator gets a raw traceback instead of an actionable message.

Docs assert the file-sibling behaviour unconditionally and would need updating with the fix: `docs/deployment/state.md:59-63` and `docs/reference/cli.md:73-74`.

## Why it matters

The documented property of `sf2loki backfill` is that it is resumable (`docs/reference/cli.md:73-74`). On `file` state that holds. On `s3`/`gcs` — the backends that exist specifically for stateless deployments (#30, #37) — it does not: an operator running `sf2loki backfill --since 2026-05-01 --until 2026-06-01` in a Kubernetes Job either

- crashes with a traceback before pushing anything, when the pod has no writable `/var/lib/sf2loki` (a plain `kubectl run` of the image, or any pod spec that does not replicate the chart's emptyDir mount), or
- writes resume state to the pod's ephemeral disk, so a restarted Job re-lists and re-pushes the entire window from the start. Ingestion is at-least-once and Loki dedupes identical lines only inside its per-stream reject window, which historical backfill data sits outside of, so the re-push is real duplicate volume plus a repeated Salesforce API-call spend against the daily limit, not a free retry.

## Proposed approach

Route backfill checkpoints through `build_store`, but against a **derived** `StateConfig` whose backend target is suffixed — not the daemon's own key.

A bare `build_store(cfg.state)` is NOT correct. Object-store commits rewrite the whole document under a conditional write (`src/sf2loki/state/s3_store.py:276-308`, `IfMatch` ETag / `IfNoneMatch: *`), and a 412 raises `StateStoreConflictError` (`src/sf2loki/state/s3_store.py:45-54`), which is deliberately excluded from `_is_transient` and never retried. GCS uses the same shape via `ifGenerationMatch`. Backfill commits once per processed file (`backfill.py:455`), so sharing one object with a live daemon would abort the run on the first lost CAS race. The `backfill:` key namespace does not help — the CAS is per object, not per key.

1. Add a helper (e.g. `backfill_state_config(state: StateConfig) -> StateConfig` in `src/sf2loki/state/__init__.py`) that returns a `model_copy` of the state config pointed at a `-backfill`-suffixed target per backend, mirroring the existing suffix-copy pattern in `_probe_state_config` (`src/sf2loki/doctor.py:533-549`):
   - `file`: `path.with_name(f"{stem}-backfill{suffix}")` — byte-identical to today's derivation, so existing deployments resume from the same file.
   - `s3`: `s3.key` + `-backfill` (same bucket, credentials, endpoint).
   - `gcs`: `gcs.object_name` + `-backfill`.
2. In `run_backfill` (`backfill.py:742-744`), replace the hardcoded `FileCheckpointStore(...)` with `build_store(backfill_state_config(cfg.state))`. Keep the existing `store.close()` in the `finally` (`backfill.py:784`) — note `close()` is sync on the file store but async on the object stores (issue #52), so the teardown must `await` when the store's `close` is a coroutine, or the session leaks on every backfill run.
3. Wrap store construction so a missing extra surfaces cleanly: `build_store` raises `ConfigError` when `aiobotocore`/`gcloud` is absent (`src/sf2loki/state/__init__.py:19-45`). Build the store inside the CLI's existing `try/except (ConfigError, ValueError)` (`cli.py:205-219`), or catch it in `run_backfill` and print + return `_CONFIG_ERROR_EXIT_CODE`, so `sf2loki backfill` reports "install the extra" rather than a traceback.
4. Also catch `OSError` from the file backend's lock/mkdir path and report it as a config-shaped error naming the directory and the uid (mirroring the message at `src/sf2loki/doctor.py:514-517`), so a non-writable state dir fails fast and legibly.
5. Update `docs/deployment/state.md:59-63` and `docs/reference/cli.md:73-74` to state the target per backend (`-backfill` file sibling / `-backfill`-suffixed s3 key / gcs object) instead of "a `-backfill` sibling of the daemon's state file".

---

Imported from GitHub issue #133 on 2026-08-14, when this repo migrated from GitHub Issues to Backlog.md. The original issue has been deleted; its verbatim body, labels and comments are preserved in `archive/issues-dump.json` (`jq '.[] | select(.number == 133)' archive/issues-dump.json`).

## Additional evidence (parallel review lanes)

Related closed work that did not cover this: #40 namespaced the backfill checkpoint *key* per org (src/sf2loki/backfill.py:766-767) but left the store hardwired; #30 added the s3/gcs backends for the daemon path only; #59 taught doctor to probe the configured backend — its `_probe_state_config` model_copy pattern (src/sf2loki/doctor.py:533-549) is the template for resolving the store here; #63's `state` subcommand reaches the main state document only. Also note `_print_summary` (src/sf2loki/backfill.py:786) sits outside the `finally`, so the unhandled-OSError route additionally loses the run summary after rows were already pushed.

<sub>Filed from the 2026-07-30 full-repo audit (11 finder lanes + adversarial verification per finding).</sub>
<!-- SECTION:DESCRIPTION:END -->

## Acceptance Criteria
<!-- AC:BEGIN -->
- [ ] #1 `run_backfill` obtains its checkpoint store via `build_store` against a `-backfill`-suffixed copy of `cfg.state`; no reference to `cfg.state.file.path` remains in `backfill.py` outside the `file` branch of the new helper.
- [ ] #2 Test: `state.store="s3"` — `run_backfill` builds an S3-backed store whose key is `state.s3.key` + `-backfill` (assert on a monkeypatched `build_store`/client factory), and never creates or opens anything under `cfg.state.file.path.parent`.
- [ ] #3 Test: `state.store="gcs"` — same assertion against `state.gcs.object_name` + `-backfill`.
- [ ] #4 Test: the derived object key/name is never equal to the daemon's configured key, so a backfill run cannot CAS-race the live daemon's state object.
- [ ] #5 Regression test: `state.store="file"` still checkpoints to `<stem>-backfill<suffix>` and still resumes from a pre-existing file at that exact path (extends the existing coverage at `tests/test_backfill.py:479-481` and the legacy-key fallback at `tests/test_backfill.py:993-1027`).
- [ ] #6 Test: `state.store="s3"` with the extra absent → `sf2loki backfill` exits with the config-error exit code and prints the "install the extra: pip install 'sf2loki[s3]'" message, no traceback.
- [ ] #7 Test: file backend whose state dir cannot be created → a single actionable stderr line naming the directory and a non-zero exit, no traceback.
- [ ] #8 Test: an object-store backfill run awaits the store's async `close()` on both the success and the abort path (no un-awaited-coroutine warning, no leaked session — same failure shape as issue #52).
- [ ] #9 `docs/deployment/state.md` and `docs/reference/cli.md` describe the backfill checkpoint target per backend.
- [ ] #10 `just gate` green.
<!-- AC:END -->

## Definition of Done
<!-- DOD:BEGIN -->
- [ ] #1 just gate is green (ruff check + ruff format --check + mypy src + pytest) — run it, don't assert it
- [ ] #2 just gen-config run and its output committed, if config.py changed (CI drift gate fails otherwise)
- [ ] #3 committed straight to main with a conventional-commit message, and pushed
<!-- DOD:END -->
