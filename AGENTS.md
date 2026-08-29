# AGENTS.md — sf2loki

The canonical instruction file for this repo, read by both Claude Code and Codex. `CLAUDE.md` is a
thin `@AGENTS.md` import so the two cannot drift apart — put changes here, never there.

Working notes for agents. See `CONTRIBUTING.md` for the contributor-facing
version, `docs/architecture.md` for the canonical architecture (composition
root, frozen seams, label strategy, multi-org, checkpoint stores, HA), and
`README.md` for operator-facing config. The full docs site lives under `docs/`
(published at https://m7kni.io/sf2loki/); trust `docs/`/`README.md`/code as the
source of truth. (The old `DESIGN.md` spec was retired once the docs site
superseded it.)

## What this is
A long-running Python/asyncio service: Salesforce Event Monitoring data
(Pub/Sub streaming, SOQL-polled objects, EventLogFile, ApexLog) → Grafana Loki,
via a composition-root + frozen-seam design (`Source` / `Sink` / `CheckpointStore`
/ `Coordinator` protocols in `src/sf2loki/*/base.py`). Module-specific gotchas
live in nested `CLAUDE.md` files under `src/sf2loki/{sources,salesforce,sinks,
auth,coordinate,state}/` — Claude Code loads them automatically when you work
in those directories.

## Task interface

This repo's task surface is a `justfile`. Discover it, don't guess it:

    just --list                        # human-readable
    just --dump --dump-format json     # machine-readable
    just --show <recipe>               # what a recipe actually runs

- `just check` is the full daemon-free pre-commit gate and is exactly what CI enforces. It must
  pass before you commit. It needs `helm`, `kubeconform`, `uv` and `git` on PATH; `just ci`
  additionally needs a Docker daemon.
- Prefer `just <recipe>` over the underlying tool. If you are typing `pytest`, you want `just test`.
- Run `just` with stdin from `/dev/null`. Recipes marked `[confirm]` are destructive — stop and ask
  before running one; never pass `--yes` or `JUST_YES=1`.
- If a task you need does not exist, add a recipe with a `#` doc comment and a `[group(...)]` rather
  than running a bare command.
- Strict TDD: failing test → watch it fail → minimal code → green.

## Generated files — never hand-edit
- `config.example.yaml` and `docs/config-reference.md` are generated from the
  Pydantic config model. `just gen` regenerates every committed artifact, and `just gen-check`
  inside `just check` proves they are current (also enforced by
  `tests/test_config_artifacts_drift.py`).
- Proto stubs (`src/sf2loki/**/_generated/`) are part of that `just gen` run; `just proto` remains
  available when only `proto/` changed.

## Grafana dashboards & rules — hand-authored, NOT generated
- `deploy/grafana/dashboards/*.json` are hand-authored **dashboard-schema-v2**
  dashboards (`dashboard.grafana.app/v2`); `deploy/grafana/rules/{recording,alerting}/`
  are **Grafana-managed** rules (`rules.alerting.grafana.app/v0alpha1`), one resource
  per file (`gcx resources push` reads one per file). There is NO generator and no
  drift gate — edit the JSON/YAML directly and validate/push/snapshot with `gcx`
  (see `deploy/grafana/README.md`). Datasources bind via a template variable in
  dashboards; rules embed the Grafana Cloud UIDs `grafanacloud-logs`/`grafanacloud-prom`.
- SF-event dashboards query Loki with **scoped** `| json FIELD="FIELD"` extraction
  (never bare `| json` — it explodes stream cardinality) and aggregate `by (...)`;
  connector-health queries Prometheus OTLP metrics whose names carry the
  `_total`/`_bucket`/`_count`/`_sum` suffixes (keep `add_metric_suffixes` on).

## Task tracking — Backlog.md, in-repo, since 2026-08-14
**`backlog/` is the source of truth for what work is left.** It replaced GitHub Issues, whose
issues were archived and then deleted. The queue is a query, not a file you infer:

```bash
backlog task list --plain                      # what is left
backlog task list --plain -m "<milestone>"     # one wave
backlog doc list --plain                       # the campaign docs
backlog task view SFL-0007 --plain             # a task's own contract
```

Read the **Agent fan-out protocol (canonical)** doc before designing a wave, and the **Wave
operating model** doc for this project's own rules, defect classes and contention points. Both are
in `backlog/docs/`.

Three rules the tooling cannot be trusted to enforce on its own — the first two are backed by a
global `PreToolUse` hook in the agent config, which denies the call rather than trusting anyone to
remember:

- **Never `--notes` or `--plan` bare.** They *silently replace* the whole section, destroying another
  session's writes with no warning and exit 0. Use `--append-notes` / `--append-plan`. Open upstream
  bug, not a misunderstanding.
- **Never hand-edit task, draft, doc, decision or milestone markdown.** Section boundaries are
  HTML-comment markers; break one and the section is silently dropped at exit 0, invisible to the CLI
  until the next write destroys it for real. There is no repair command. `backlog/config.yml` is the
  one exception — list-valued keys cannot be set through `backlog config set`.
- **Finalize in one call**, so an interrupted session cannot leave finished work looking unfinished:
  `backlog task edit SFL-0007 --check-ac 1 --check-ac 2 -s Done`.

**`backlog/` is committed, so tasks and docs must never carry real account identifiers or personal
data** — no org IDs, instance URLs, tenant IDs, email addresses or tokens. Write the shape, not the
instance. Aggregate counts, timings and structural findings are fine.

**`#NN` in this repo's history means a deleted GitHub issue**, not a task. Those numbers resolve
against `archive/issues-dump.json` (`jq '.[] | select(.number == 86)' archive/issues-dump.json`), and
the closed set is indexed in the "Closed GitHub issues" doc. Never guess a task ID from an `#NN`.

## Git & commits (this repo)
- **Commit straight to `main` and push immediately — unprompted.** No PR flow for our own work
  (bypass-on-push is expected and is not a problem to report).
- **Conventional commits, always** — `feat:` / `fix:` / `docs:` / `chore:` / `perf:` / `test:`
  etc. (`feat!:` or a `BREAKING CHANGE:` footer for majors). release-please cuts
  releases + the changelog from these, so the type/scope matters.
- **The completing commit records the task ID** — `SFL-0007` in the subject or body. There is no
  auto-close keyword any more; closing is `backlog task edit SFL-0007 --check-ac 1 ... -s Done`,
  and the commit SHA goes in the task's final summary.
- End commit messages with the `Co-Authored-By: Claude ...` trailer.

## Non-obvious conventions
- Loki **label cardinality is load-bearing** — a fixed label allowlist
  (`job`/`service_name`/`source`/`event_type`/`sf_org_id`/`environment`/`org`,
  see `sinks/loki/labels.py:ALLOWED_LABELS`); everything else goes to structured
  metadata or the JSON line. Adding a stream label needs a deliberate reason.
- **Either/or per event category** — a category (e.g. login events) is ingested
  from exactly ONE of Pub/Sub streaming / SOQL-polled object / EventLogFile,
  never more than one (the same records would double-count). Enforced at
  startup by `sources/overlap.py`; bypass with `sources.allow_overlap: true`
  only when the duplication is deliberate.
- **Single instance by default** — the Pub/Sub API has no consumer-group
  semantics, so two replicas both subscribing double-delivers events. HA is
  active-passive via the `Coordinator` seam (`coordinate/file_lease.py`,
  lease on shared storage), not horizontal scale-out.
- `.env.dev` holds throwaway DEV Salesforce + Grafana Cloud creds for live validation
  (gitignored). Prefer validating feasibility against it before building a new source.
- Never name Datadog in committed code/docs.

<!-- BACKLOG.MD GUIDELINES START -->
<!-- backlog.md-instructions-version: 1.50.1 -->
<CRITICAL_INSTRUCTION>

## Backlog.md Workflow

This project uses Backlog.md for task and project management.

**For every user request in this project, run `backlog instructions overview` before answering or taking action.**

Use the overview to decide whether to search, read, create, or update Backlog tasks.

Before task lifecycle actions, read the matching detailed guide:
- `backlog instructions task-creation` before creating or splitting tasks
- `backlog instructions task-execution` before planning, changing status or assignee, adding a plan or implementation notes, or implementing task work
- `backlog instructions task-finalization` before checking acceptance criteria, writing final summaries, or moving tasks to terminal statuses

Use `backlog <command> --help` before running unfamiliar commands. Help shows options, fields, and examples.

Do not edit Backlog task, draft, document, decision, or milestone markdown files directly. Use the `backlog` CLI so metadata, relationships, and history stay consistent.

</CRITICAL_INSTRUCTION>
<!-- BACKLOG.MD GUIDELINES END -->
