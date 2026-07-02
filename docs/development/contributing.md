# Contributing

sf2loki is a small project with a strict green bar — the workflow below keeps it that way.

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

- **Tests first.** Bug fixes and features come with tests, strict TDD: write a failing test, watch
  it fail for the right reason, write the minimal code to pass, then refactor. The repo is
  test-driven and CI fails on any red.
- **`just gate` must be green before a commit** — `ruff check`, `ruff format --check`,
  `mypy src` (strict), and `pytest`. Run it, don't assert it; CI runs the identical gate.
- **Generated files are generated.** `config.example.yaml`, `docs/config-reference.md`, and the
  proto stubs under `src/sf2loki/**/_generated/` are produced from source (`just gen-config` /
  `just proto`) — never hand-edit them. CI has drift gates that fail the build if a generated file
  is out of sync with its source (the Pydantic config model, or `proto/`).
- **Conventional commits.** Releases are cut by
  [release-please](https://github.com/googleapis/release-please) from commit messages, so use
  `feat:`, `fix:`, `docs:`, `chore:`, `perf:`, etc. (`feat!:` or a `BREAKING CHANGE:` footer for
  majors). PR titles should follow the same convention — squash merges use them. See
  [Release Process](release-process.md) for how these map to changelog sections and version bumps.
- **No DCO / CLA.** No sign-off dance — by contributing you agree your work is licensed under the
  project license (AGPL-3.0-only).
- **Label discipline is a feature.** Anything that adds Loki stream labels or otherwise touches
  cardinality gets extra review scrutiny — see [DESIGN.md](https://github.com/rknightion/sf2loki/blob/main/DESIGN.md).

## Reporting issues

Use [GitHub issues](https://github.com/rknightion/sf2loki/issues) for bugs and feature requests.
For security problems, use the private process in [Security](../security.md) instead — never a
public issue.
