# Contributing to sf2loki

Thanks for contributing! This is a small project with a strict green bar — the
workflow below keeps it that way.

## Dev setup

Requirements: Python 3.14, [uv](https://docs.astral.sh/uv/), and
[just](https://github.com/casey/just).

```bash
just setup   # uv sync — create the venv from the lockfile
just gate    # ruff + mypy --strict + pytest (the green bar; CI runs the same)
```

Useful extras:

```bash
just test        # pytest only
just lint        # ruff check + format check
just proto       # regenerate gRPC/protobuf stubs (only when proto/ changes)
just gen-config  # regenerate config.example.yaml + docs/config-reference.md
```

## Ground rules

- **Tests first.** Bug fixes and features come with tests; the repo is built
  test-driven and CI fails on any red.
- **Generated files are generated.** `config.example.yaml`,
  `docs/config-reference.md`, and the proto stubs are produced from source
  (`just gen-config` / `just proto`) — never hand-edit them; CI has drift gates.
- **Conventional commits.** Releases are cut by
  [release-please](https://github.com/googleapis/release-please) from commit
  messages, so use `feat:`, `fix:`, `docs:`, `chore:` etc. (`feat!:`/
  `BREAKING CHANGE:` for majors). PR titles should follow the same convention
  (squash merges use them).
- **No DCO / CLA.** No sign-off dance — by contributing you agree your work is
  licensed under the project license (AGPL-3.0-only).
- **Label discipline is a feature.** Anything that adds Loki stream labels or
  otherwise touches cardinality gets extra scrutiny — see docs/architecture.md.

## Reporting issues

Use GitHub issues for bugs/features. For security problems use the private
process in [SECURITY.md](SECURITY.md) instead.
