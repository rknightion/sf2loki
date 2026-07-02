# CLAUDE.md — sf2loki

Working notes for Claude. See `CONTRIBUTING.md` for the human-facing version and
`DESIGN.md` for architecture.

## The green bar
- `just gate` (= `ruff check` + `ruff format --check` + `mypy src` + `pytest`) must
  be green before any commit — run it, don't assert it. CI runs the same.
- Strict TDD: failing test → watch it fail → minimal code → green.

## Generated files — never hand-edit
- `config.example.yaml` and `docs/config-reference.md` are generated from the
  Pydantic config model: run `just gen-config` after any `config.py` change (a CI
  drift gate fails otherwise).
- proto stubs come from `just proto` (only when `proto/` changes).

## Git & commits (this repo)
- **Commit straight to `main`; push only when asked.** No PR flow for our own work.
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
  (`job`/`source`/`event_type`/`sf_org_id`/`environment`/`org`); everything else goes
  to structured metadata or the JSON line. Adding a stream label needs a deliberate reason.
- `.env.dev` holds throwaway DEV Salesforce + Grafana Cloud creds for live validation
  (gitignored). Prefer validating feasibility against it before building a new source.
- Never name Datadog in committed code/docs.
