# CLAUDE.md — sf2loki

Working notes for Claude. See `CONTRIBUTING.md` for the contributor-facing
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

## Quick commands
```bash
just setup       # uv sync — create the venv from the lockfile
just gate        # ruff + mypy --strict + pytest — the green bar, must be green before commit
just test        # pytest only
just lint        # ruff check + format check
just proto       # regen gRPC/protobuf stubs (only when proto/ changes)
just gen-config  # regen config.example.yaml + docs/config-reference.md (only when config.py changes)
just run config=config.yaml
```

## The green bar
- `just gate` (= `ruff check` + `ruff format --check` + `mypy src` + `pytest`) must
  be green before any commit — run it, don't assert it. CI runs the same.
- Strict TDD: failing test → watch it fail → minimal code → green.

## Generated files — never hand-edit
- `config.example.yaml` and `docs/config-reference.md` are generated from the
  Pydantic config model: run `just gen-config` after any `config.py` change (a CI
  drift gate fails otherwise, enforced via `tests/test_config_artifacts_drift.py`).
- proto stubs (`src/sf2loki/**/_generated/`) come from `just proto` (only when
  `proto/` changes).

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

## Git & commits (this repo)
- **GitHub issues are the source of truth for what work has happened on this repo.**
  Every substantial piece of work should have a tracking issue, and the commit that
  completes it closes it (see the closing-keyword rule below). If you do a chunk of work
  that has no issue yet, **file one describing it** (`gh issue create`) so the tracker
  stays a complete record — then close it from the completing commit.
- **Commit straight to `main` and push immediately — unprompted.** No PR flow for our own work;
  the push is what closes the tracking issue (bypass-on-push is expected).
- **Conventional commits, always** — `feat:` / `fix:` / `docs:` / `chore:` / `perf:`
  etc. (`feat!:` or a `BREAKING CHANGE:` footer for majors). release-please cuts
  releases + the changelog from these, so the type/scope matters.
- **When the work targets a GitHub issue, reference it with a closing keyword in the
  commit that completes it** — `Closes #NN` (or `Fixes #NN`) in the commit body/footer.
  GitHub auto-closes the issue on push to `main` from that keyword. release-please does
  NOT close issues, and a bare `#NN` mention doesn't either — only the keyword does. If
  the closing commit is already pushed without it, close the issue manually with
  `gh issue close NN` + a summary comment (never rewrite published `main` history to add it).
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
