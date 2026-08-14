---
id: SFL-0060
title: >-
  cli: doctor only ever checks one org — add `--all-orgs` for a whole-deployment
  preflight
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
  - 'https://github.com/rknightion/sf2loki/issues/144'
ordinal: 60000
---

## Description

<!-- SECTION:DESCRIPTION:BEGIN -->
## What

`sf2loki doctor` scopes every per-org check to a single org, so a multi-org deployment has no single-command preflight.

`run_doctor` (`src/sf2loki/doctor.py:850`) resolves one org and collapses the config to a single-org view before any live check runs:

- `org, note = select_org(cfg, org_name)` — `src/sf2loki/doctor.py:872`
- `cfg = as_single_org_view(cfg, org)` — `src/sf2loki/doctor.py:880` (implementation: `src/sf2loki/config.py:1466`, swaps in `org.salesforce` / `org.sources`)
- `sf = org.salesforce` — `src/sf2loki/doctor.py:881`

Everything after that sees exactly one org: `auth`/`permissions`/`pubsub`/`entitlement`/`traceflags` (`src/sf2loki/doctor.py:891-899`), `transforms` (`src/sf2loki/doctor.py:901`), and `limits` (`src/sf2loki/doctor.py:909`). The docstring states the limitation explicitly (`src/sf2loki/doctor.py:856-861`). `select_org` returns a note rendered as a WARN row — "multiple orgs configured [...]; this command operates on org '<name>' only" (`src/sf2loki/config.py:1455-1461`, emitted at `src/sf2loki/doctor.py:876-878`).

The CLI exposes no way to widen the scope: the doctor subparser has `--json` and `--org NAME` only (`src/sf2loki/cli.py:80-97`), passed through at `src/sf2loki/cli.py:200`. `docs/reference/cli.md:59-62` documents those two flags and nothing else.

`CheckResult` carries `name`, `status`, `detail` and no org identity (`src/sf2loki/doctor.py:90-96`); the `--json` payload is `{"checks": [asdict(r) ...], "exit_code": N}` (`src/sf2loki/doctor.py:820-823`). So N invocations emit N payloads whose check names are byte-identical, with nothing to attribute a row to an org.

This is a doctor-only restriction. Multi-org ingestion is first class (`OrgConfig` `src/sf2loki/config.py:1298-1306`, `Config.resolved_orgs` `src/sf2loki/config.py:1420`, issue #31), and the daemon already probes every org at startup with an order-preserving `asyncio.gather` (`src/sf2loki/app.py:1240-1265`). `sf2loki --check` also validates the whole multi-org config, as does doctor's own `config` check, which runs `App.build(cfg)` on the unscoped config (`src/sf2loki/doctor.py:105-120`).

## Why it matters

A six-org deployment cannot answer "is this deployment ready?" in one command. The operator runs `sf2loki doctor --org <name>` six times, reads six tables (or six JSON blobs with indistinguishable check names), and derives the overall verdict by hand. CI wiring needs a shell loop plus exit-code aggregation.

The failure mode is silence: an org missing from the hand-written loop gets no preflight at all, and nothing in the output of the runs that *did* happen indicates coverage was incomplete. A broken integration user, a revoked connected app, or an unreachable Pub/Sub topic in that org first surfaces at runtime — where multi-org semantics are deliberately non-fatal (some-orgs-fail keeps the healthy orgs streaming, `src/sf2loki/app.py:1241-1247`), so it degrades quietly rather than failing fast.

Six runs also repeat the deployment-wide checks six times, including six `source=sf2loki-doctor` test writes to Loki (`src/sf2loki/doctor.py:358-364`), six OTLP probes, six state-store probe objects and six coordinator lease probes — none of which vary by org.

## Proposed approach

Add `doctor --all-orgs`: run the per-org check set once per configured org, run the deployment-wide checks exactly once, and aggregate into one table / one JSON payload with one exit code.

**CLI** (`src/sf2loki/cli.py:86-97`)

- Add `--all-orgs` (`action="store_true"`, `dest="all_orgs"`) in an `add_mutually_exclusive_group()` with `--org`, so `--org X --all-orgs` is an argparse usage error rather than an ambiguous scope.
- Pass `all_orgs=args.all_orgs` through `run_doctor(...)` at `src/sf2loki/cli.py:200`.

**Check partition** — the split is not the one the surface naming suggests:

- Per-org (run once per org): `auth`, `permissions`, `pubsub:<topic>`, `entitlement`, `traceflags`, `transforms`, `limits`. `transforms` belongs here despite reading like a global: transform rules and `transform_salt` live on `SourcesConfig` (`src/sf2loki/config.py:791`, `src/sf2loki/config.py:826`), which `as_single_org_view` replaces per org (`src/sf2loki/config.py:1466-1472`), and secret resolution is per-org (`src/sf2loki/config.py:1538-1540`).
- Deployment-wide (run exactly once, after the org loop): `loki` (`src/sf2loki/doctor.py:900`), `telemetry` (`src/sf2loki/doctor.py:902`), `state` (`src/sf2loki/doctor.py:903`), `coordinator` (`src/sf2loki/doctor.py:904`) — all read `cfg.sink` / `cfg.state` / `cfg.coordinate` / `cfg.service`, which `OrgConfig` explicitly leaves deployment-wide (`src/sf2loki/config.py:1304-1305`). Exactly one Loki test write regardless of org count.
- `config` stays a single leading row (it already validates the whole multi-org config).

**Row naming and identity**

- Namespace per-org rows `org=<name>:<check>`, mirroring the existing `pubsub:<topic>` compound-row pattern (`src/sf2loki/doctor.py:280-283`) and the `org=<name>:` checkpoint-key prefix convention (`src/sf2loki/config.py:1301-1303`). A Pub/Sub row becomes `org=emea:pubsub:/event/LoginEventStream`.
- Add `org: str = ""` to `CheckResult` (`src/sf2loki/doctor.py:90-96`). Deployment-wide rows and every row in the existing single-org path keep `""`, so the JSON change is purely additive.
- SKIP rows synthesised by `_skip_remaining` (`src/sf2loki/doctor.py:829-832`, name list `_CHECKS_AFTER_CONFIG` `src/sf2loki/doctor.py:835-848`) must be namespaced and org-tagged the same way, and the auth-failure short-circuit (`src/sf2loki/doctor.py:892-894`, `src/sf2loki/doctor.py:906-909`) must skip only the failing org's dependents — one org's dead auth must not suppress another org's checks.
- Suppress the `select_org` multi-org WARN note in `--all-orgs` mode: "this command operates on org '<name>' only" is false there.

**Execution and scope edge cases**

- One `TokenProvider` per org (each org has its own `SalesforceConfig`). The single shared `httpx.AsyncClient` (`src/sf2loki/doctor.py:884`) is safe to reuse across orgs: ELF clock skew is per-client state, not an httpx event hook (`src/sf2loki/salesforce/eventlogfile_client.py:118-124`).
- Run orgs sequentially. Deterministic table order, and it avoids issuing N orgs' worth of `permissions`/`entitlement`/`limits` API calls simultaneously. If concurrency is added later, preserve input order the way `App._probe_orgs` does (`src/sf2loki/app.py:1265`).
- A single-org config resolves to one empty-name org (`src/sf2loki/config.py:1428-1435`), so `--all-orgs` there must be byte-identical to the default path: no `org=` prefixes, no extra rows.
- Exit-code contract unchanged: `2` when the config itself cannot be loaded or selected (`_CONFIG_ERROR_EXIT_CODE`, `src/sf2loki/doctor.py:815`), `1` if any row anywhere FAILs, `0` otherwise (`_finish`, `src/sf2loki/doctor.py:818-826`).

**Docs**

- Add the flag to the doctor table in `docs/reference/cli.md:59-62`, noting that per-org API-call cost scales with org count and that the Loki test write still happens exactly once.
- Cross-reference from the multi-org section of `docs/configuration/index.md` (around line 64).

---

Imported from GitHub issue #144 on 2026-08-14, when this repo migrated from GitHub Issues to Backlog.md. The original issue has been deleted; its verbatim body, labels and comments are preserved in `archive/issues-dump.json` (`jq '.[] | select(.number == 144)' archive/issues-dump.json`).

<sub>Filed from the 2026-07-30 full-repo audit (11 finder lanes + adversarial verification per finding).</sub>
<!-- SECTION:DESCRIPTION:END -->

## Acceptance Criteria
<!-- AC:BEGIN -->
- [ ] #1 `doctor --all-orgs` runs `auth`/`permissions`/`pubsub`/`entitlement`/`traceflags`/`transforms`/`limits` once per org in `cfg.resolved_orgs()`, with rows named `org=<name>:<check>`.
- [ ] #2 `loki`, `telemetry`, `state` and `coordinator` each produce exactly one row for the whole run, and exactly one `source=sf2loki-doctor` line is pushed regardless of org count.
- [ ] #3 `CheckResult` gains `org: str = ""`; the `--json` payload carries `org` per check; deployment-wide and single-org rows keep `""`.
- [ ] #4 `--org` and `--all-orgs` together exit as an argparse usage error.
- [ ] #5 Exit code is `1` when any org's check FAILs, `0` when every row is PASS/WARN/SKIP, `2` on a config load/selection error.
- [ ] #6 The `select_org` "operates on org X only" WARN row is absent in `--all-orgs` mode and still present for a multi-org config without it.
- [ ] #7 `docs/reference/cli.md` documents `--all-orgs`; `just gate` is green (ruff, `mypy --strict`, pytest).
- [ ] #8 Test `tests/test_doctor.py::test_all_orgs_runs_per_org_checks_for_every_org` — two-org config, faked Salesforce/Pub/Sub/Loki layers; asserts `org=a:auth` and `org=b:auth` rows both present and that `loki`/`state`/`telemetry`/`coordinator` appear exactly once each.
- [ ] #9 Test `tests/test_doctor.py::test_all_orgs_one_org_auth_failure_does_not_skip_the_other` — org A auth FAILs, org B auth PASSes; asserts A's dependents are SKIP-with-org-prefix while B's still execute, and the exit code is `1`.
- [ ] #10 Test `tests/test_doctor.py::test_all_orgs_json_payload_carries_org_field` — asserts every per-org check dict has the right `org`, and deployment-wide checks have `""`.
- [ ] #11 Test `tests/test_doctor.py::test_all_orgs_on_single_org_config_matches_default_output` — asserts identical row names/statuses with and without `--all-orgs` for a single-org config (no `org=` prefixes).
- [ ] #12 Test `tests/test_doctor.py::test_all_orgs_pushes_exactly_one_loki_test_line` — counts sink pushes across a three-org run.
- [ ] #13 Test `tests/test_cli.py` (or the existing CLI arg test module) — asserts `doctor --org x --all-orgs` exits non-zero as a usage error.
<!-- AC:END -->

## Definition of Done
<!-- DOD:BEGIN -->
- [ ] #1 just gate is green (ruff check + ruff format --check + mypy src + pytest) — run it, don't assert it
- [ ] #2 just gen-config run and its output committed, if config.py changed (CI drift gate fails otherwise)
- [ ] #3 committed straight to main with a conventional-commit message, and pushed
<!-- DOD:END -->
