---
id: SFL-0059
title: >-
  cli: state export/import for migrating checkpoints between backends (file ->
  s3/gcs)
status: To Do
assignee: []
created_date: '2026-08-14 16:58'
labels:
  - followup
  - phase-1
  - roadmap
milestone: m-4
dependencies: []
references:
  - 'https://github.com/rknightion/sf2loki/issues/143'
ordinal: 59000
---

## Description

<!-- SECTION:DESCRIPTION:BEGIN -->
# cli: state export/import for migrating checkpoints between backends

## What

The `sf2loki state` verb surface is exactly three single-key operations:

| verb | entrypoint | wiring |
| --- | --- | --- |
| `show` | `src/sf2loki/statecmd.py:145` | `src/sf2loki/cli.py:151-165` |
| `set` | `src/sf2loki/statecmd.py:173` | `src/sf2loki/cli.py:167-177` |
| `delete` | `src/sf2loki/statecmd.py:192` | `src/sf2loki/cli.py:179-189` |

Dispatch is `src/sf2loki/cli.py:243-249`. There is no verb that moves a whole checkpoint document from one configured store to another, and no documented procedure for doing so:

- `state show` is a human-readable dump, not machine output: it prints `key\tvalue` per key (`src/sf2loki/statecmd.py:166-167`) followed by a blank line and a `N checkpoint(s)` summary (`src/sf2loki/statecmd.py:168`), so anything consuming it must strip trailing lines and split on tabs.
- `state set` commits one key per process invocation (`src/sf2loki/statecmd.py:184-186`).
- `docs/deployment/state.md:18-26` documents only those three verbs; there is no "migrating state backends" section anywhere under `docs/`.

Meanwhile a backend switch is the documented growth path. `docs/deployment/high-availability.md:84-88` requires the checkpoint store to be **shared** between replicas, and for the `k8s_lease` topology specifically requires `state.store: s3` or `gcs` (no shared volume). `docs/deployment/state.md:104-106` repeats the requirement. A single-instance deployment therefore starts on `state.store: file` and must move its live checkpoints to `s3`/`gcs` at the moment it becomes HA.

The plumbing for a bulk write already exists and is unused by the CLI: `commit_many` is implemented on all three stores (`src/sf2loki/state/file_store.py:210`, `src/sf2loki/state/s3_store.py:265`, `src/sf2loki/state/gcs_store.py:222`) and consumed duck-typed by the flush path at `src/sf2loki/app.py:508-513`. `statecmd._whole_document` (`src/sf2loki/statecmd.py:138`) already enumerates the entire document via the stores' `_cache`, and `_RESERVED_KEYS` (`src/sf2loki/statecmd.py:63`) already filters the file store's internal `__fence_epoch__` bookkeeping key.

## Why it matters

Without a supported migration path an operator promoting a working `file` deployment to active-passive HA has three options, all bad:

1. **Start the new backend empty.** Every SOQL-polled source re-lists its `lookback_hours` window (a bounded duplicate window), and every Pub/Sub subscription starts a *fresh* subscribe from "now" — a real ingestion **gap**, not a duplicate, for the interval between the last commit and the cutover (`docs/deployment/state.md:78-83` documents this consequence for `state delete`; a fresh backend is the same thing for every key at once).
2. **Hand-transcribe key by key.** Parse `state show` output around its summary line, then one `sf2loki state set` invocation per key against the new config, copying base64 `replay_id`s and multi-KB JSON carried-id windows by hand. One mangled value stalls or gaps that source. Key counts scale with sources x orgs (multi-org keys are additionally prefixed `org=<name>:`, `docs/deployment/state.md:50-56`).
3. **Copy the raw document out of band.** Both backends persist the same flat `{str: str}` JSON object (`src/sf2loki/state/file_store.py:170-173` dumps `_cache`; `src/sf2loki/state/s3_store.py:258-280` loads/PUTs the same shape), so `aws s3 cp state.json s3://bucket/key` does in fact work today. This is undocumented, requires direct bucket credentials the operator may not have outside the service account, has no equivalent one-liner for `s3` -> `gcs`, and copies `__fence_epoch__` across — inert on `s3`/`gcs`, but on a copy back into a `file` store a carried-forward high epoch is compared against the live leader's epoch at `src/sf2loki/state/file_store.py:274-296` and can reject commits with `StateFenceError` ("stale leader ... rejected").

Two verbs turn this into a two-command, reviewable, backend-agnostic operation that reuses the service's own configured credentials for both ends.

## Proposed approach

Add two subcommands over the existing `statecmd` plumbing. Note that `--config` is a **top-level** flag (`src/sf2loki/cli.py:176-180`), so it must precede the subcommand: `sf2loki --config old.yaml state export`. (The examples at `docs/deployment/state.md:20-22` place it after the subcommand, which argparse rejects with `unrecognized arguments: --config`; fix those lines while adding the new section.)

**`state export`** — `run_state_export(config_path, *, force=False)` in `src/sf2loki/statecmd.py`:

- Reuse `_run` (`src/sf2loki/statecmd.py:95`) for config load, `build_store`, error-to-exit-code mapping and `_close_store`, and `_whole_document` (`src/sf2loki/statecmd.py:138`) to read the document.
- Filter `_RESERVED_KEYS` (`src/sf2loki/statecmd.py:63`) so `__fence_epoch__` is never carried across backends.
- Write `json.dumps(doc, indent=2, sort_keys=True)` to stdout and **nothing else** — no summary line, no progress text (diagnostics go to stderr). Deterministic ordering makes a dump diffable and reviewable before import.
- Keep `--force` with the same semantics/help text as the existing verbs (the file store's flock is acquired even for reads, `src/sf2loki/statecmd.py:117-126`).

**`state import`** — `run_state_import(config_path, *, stream=sys.stdin, if_empty=False, force=False)`:

- Parse stdin as a JSON object; reject a non-object payload, a non-string value, or any reserved key with the config-error exit code `2` (`src/sf2loki/statecmd.py:57`) and a message naming the offending key.
- With `--if-empty`, call `_whole_document` first and refuse (exit `1`, `_OPERATION_ERROR_EXIT_CODE`) when the target already holds any non-reserved key, naming the count. Without it, imported keys merge over existing ones.
- Commit with a single `commit_many` call, mirroring `app.py:508-513`'s duck-typed `getattr(store, "commit_many", None)` fallback to per-key `commit` (`commit_many` is not on the frozen `CheckpointStore` Protocol in `src/sf2loki/state/base.py`).
- Print `sf2loki: imported N checkpoint(s)` to stdout on success. Existing `StateFileLockError` / `StateStoreConflictError` handling comes free from `_run`.

**Docs** — a "Migrating state backends" section in `docs/deployment/state.md` (after "Command surface"), giving the ordered procedure: stop the daemon, `sf2loki --config old.yaml state export > state-dump.json`, point a copy of the config at the new backend, `sf2loki --config new.yaml state import --if-empty < state-dump.json`, verify with `state show`, start the daemon. State explicitly that the dump is plaintext checkpoints (not secret, per `src/sf2loki/statecmd.py:20`), that `__fence_epoch__` is deliberately not carried, that multi-org `org=<name>:` prefixes travel verbatim so org **names** must not change in the same step, and that backfill state lives in a separate file (`docs/deployment/state.md:58-63`) needing its own export if a backfill run is mid-flight. Cross-link from `docs/deployment/high-availability.md`'s shared-store section (lines 84-88).

---

Imported from GitHub issue #143 on 2026-08-14, when this repo migrated from GitHub Issues to Backlog.md. The original issue has been deleted; its verbatim body, labels and comments are preserved in `archive/issues-dump.json` (`jq '.[] | select(.number == 143)' archive/issues-dump.json`).

<sub>Filed from the 2026-07-30 full-repo audit (11 finder lanes + adversarial verification per finding).</sub>
<!-- SECTION:DESCRIPTION:END -->

## Acceptance Criteria
<!-- AC:BEGIN -->
- [ ] #1 `sf2loki --config c.yaml state export` prints a single JSON object of all non-reserved keys to stdout, with no summary or decorative output.
- [ ] #2 `sf2loki --config c.yaml state import < dump.json` commits every key via one `commit_many` call and reports the count.
- [ ] #3 `state import --if-empty` exits non-zero and writes nothing when the target store already holds a non-reserved key.
- [ ] #4 Both verbs work against `file`, `s3` and `gcs` (built through `build_store`, as the existing verbs are) and honour `--force` with the existing help text.
- [ ] #5 `__fence_epoch__` is excluded from `export` output and rejected on `import`.
- [ ] #6 `docs/deployment/state.md` gains a "Migrating state backends" section with the end-to-end procedure, and its `--config` placement in the command-surface block (lines 20-22) is corrected to precede the subcommand.
- [ ] #7 Tests in `tests/test_statecmd.py`: - [ ] `test_run_state_export_emits_parseable_json_only` — capsys stdout parses with `json.loads` and equals the seeded document. - [ ] `test_run_state_export_omits_reserved_epoch_key` — seed `__fence_epoch__` alongside real keys, assert it is absent (mirrors the existing `test_run_state_show_hides_reserved_epoch_key`). - [ ] `test_run_state_import_commits_all_keys_in_one_commit_many` — fake/spy store asserting a single `commit_many` call with the full mapping. - [ ] `test_run_state_import_falls_back_to_per_key_commit_without_commit_many` — store lacking `commit_many` still imports every key. - [ ] `test_run_state_import_if_empty_refuses_non_empty_target` — non-zero exit and target document unchanged. - [ ] `test_run_state_import_rejects_non_object_payload_and_reserved_key` — exit code 2, nothing written. - [ ] `test_export_then_import_round_trips_file_to_s3` — export from a `file` store, import into the stubbed s3 store used by the existing s3 tests, assert the resulting document matches (the migration path end to end). - [ ] A CLI-level test in `tests/test_cli.py` that `state export` / `state import` parse and dispatch to the new entrypoints.
- [ ] #8 `just gate` green.
<!-- AC:END -->

## Definition of Done
<!-- DOD:BEGIN -->
- [ ] #1 just gate is green (ruff check + ruff format --check + mypy src + pytest) — run it, don't assert it
- [ ] #2 just gen-config run and its output committed, if config.py changed (CI drift gate fails otherwise)
- [ ] #3 committed straight to main with a conventional-commit message, and pushed
<!-- DOD:END -->
