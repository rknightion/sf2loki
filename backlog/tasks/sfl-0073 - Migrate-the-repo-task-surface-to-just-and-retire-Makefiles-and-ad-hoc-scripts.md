---
id: SFL-0073
title: Migrate the repo task surface to just and retire Makefiles and ad-hoc scripts
status: To Do
assignee: []
created_date: '2026-08-28 19:33'
updated_date: '2026-08-29 11:03'
labels:
  - 'wave:0-pilot'
dependencies: []
priority: medium
type: chore
ordinal: 73000
---

## Description

<!-- SECTION:DESCRIPTION:BEGIN -->
Conform this repo's existing `justfile` to the frozen fleet `just` standard, absorb
`scripts/gen_proto.sh`, and move every shell-logic CI step onto a `just` recipe.

**This repo has no Makefile.** `find` over the tree (excluding `.venv/`, `.cache/`) returns nothing
named `Makefile` or `GNUmakefile`, and `git ls-files` confirms none is tracked. Section 3 below is
therefore a no-op — do not create one, do not go looking for one.

**A justfile already exists** at `justfile` (55 lines, 12 recipes). This is a conformance task, not
greenfield. The complete replacement file is in section 2 and has been validated against
`just 1.58.0`: `just --fmt --check` exits 0, `just --list` exits 0, `just --dump --dump-format json`
exits 0.

## 1. Outcome

`justfile` carries the seven mandatory recipes (`default`, `setup`, `fmt`, `fmt-check`, `lint`,
`test`, `check`) plus this repo's real extras, every public recipe has a `#` doc comment and a
`[group(...)]`, and the header is `set shell := ["bash", "-euo", "pipefail", "-c"]`. `just check` is
the complete gate — ruff format check, ruff lint, mypy strict, pytest, the generated-artifact drift
gate, the Helm chart lint/render/kubeconform/render-guard suite, and the sdist/wheel content check —
and it is exactly what `.github/workflows/ci.yml` enforces. `scripts/gen_proto.sh` is deleted, its
body living in the `proto` recipe. `scripts/check_dist.py`, `scripts/gen_helm_values.py`,
`scripts/generate_activity.py` and `scripts/cloud-environment-setup.sh` all survive as files, each
reachable through a recipe. `ci.yml`'s `gate`, `package` and `helm-chart` jobs each collapse to
`just <recipe>` calls behind a pinned `extractions/setup-just` step; the `docker-build-test` job
keeps its `uses: docker/build-push-action` and calls `just smoke sf2loki:test`. `ci-success`, every
`permissions:` block, every `concurrency:` group, every SHA pin, `persist-credentials: false` and
every `uses: rknightion/.github/...` reusable call are untouched. `AGENTS.md`, `CONTRIBUTING.md`,
`README.md`, `docs/installation.md`, `docs/development/contributing.md`,
`deploy/helm/values.yaml`'s generated-block banner and `backlog/config.yml`'s `definition_of_done`
all name `just check` instead of `just gate`.

## 2. The complete justfile

Drop this in whole, replacing the current `justfile`. Attribute lines are in **alphabetical order**
(`[confirm]` < `[group]` < `[no-exit-message]` < `[script]`) because `just --fmt` sorts them and
`fmt-check` would otherwise fail — see section 9.

```just
set shell := ["bash", "-euo", "pipefail", "-c"]
set positional-arguments

# Committed generated artifacts. `gen-check` hashes exactly these paths.
generated := "src/sf2loki/salesforce/_generated src/sf2loki/sinks/loki/_generated config.example.yaml docs/config-reference.md deploy/helm/values.yaml"
sf_gen := "src/sf2loki/salesforce/_generated"
loki_gen := "src/sf2loki/sinks/loki/_generated"

# show the task surface
default:
    @just --list

# create/refresh .venv from the lockfile (idempotent; fails if uv.lock is stale)
setup:
    uv sync --locked

# format Python sources and this justfile in place
[group('check')]
fmt:
    uv run ruff format .
    uv run ruff check --fix .
    just --fmt

# verify formatting of Python sources and this justfile; never mutates
[group('check')]
[no-exit-message]
fmt-check:
    uv run ruff format --check .
    just --fmt --check

# ruff lint over the repo, no autofix
[group('check')]
[no-exit-message]
lint:
    uv run ruff check --no-fix .

# mypy --strict over src/
[group('check')]
[no-exit-message]
typecheck:
    uv run mypy src

# run the test suite; `just test <expr>` narrows with pytest -k
[group('check')]
[no-exit-message]
test filter="":
    uv run pytest -q {{ if filter == "" { "" } else { "-k " + quote(filter) } }}

# THE GATE — everything CI's shell steps enforce. Must be green before any commit.
[group('check')]
check: fmt-check lint typecheck test gen-check helm-lint dist-check

# CI superset: the gate plus the container image build and its smoke assertions
[group('check')]
ci: check image smoke

# regenerate the committed gRPC/protobuf stubs from proto/
[group('gen')]
proto:
    mkdir -p {{ sf_gen }} {{ loki_gen }}
    touch {{ sf_gen }}/__init__.py {{ loki_gen }}/__init__.py
    uv run python -m grpc_tools.protoc -Iproto --python_out={{ sf_gen }} --grpc_python_out={{ sf_gen }} proto/pubsub_api.proto
    uv run python -m grpc_tools.protoc -Iproto --python_out={{ loki_gen }} proto/loki_push.proto
    sed -i.bak 's/^import pubsub_api_pb2/from . import pubsub_api_pb2/' {{ sf_gen }}/pubsub_api_pb2_grpc.py
    rm -f {{ sf_gen }}/*.bak

# regenerate config.example.yaml, docs/config-reference.md and the Helm values block
[group('gen')]
gen-config:
    uv run python -m sf2loki config example > config.example.yaml
    uv run python -m sf2loki config reference > docs/config-reference.md
    uv run python scripts/gen_helm_values.py

# regenerate ONLY the Helm chart's generated config block (subset of gen-config)
[group('gen')]
gen-helm-values:
    uv run python scripts/gen_helm_values.py

# regenerate every committed generated artifact (idempotent)
[group('gen')]
gen: proto gen-config

# THE DRIFT GATE — fail if any committed generated artifact is stale
[group('gen')]
[script('bash')]
gen-check:
    set -euo pipefail
    before="$(git ls-files -z -- {{ generated }} | xargs -0 git hash-object)"
    just gen
    after="$(git ls-files -z -- {{ generated }} | xargs -0 git hash-object)"
    if [[ "$before" != "$after" ]]; then
        echo "generated artifacts are stale - run 'just gen' and commit the result" >&2
        git status --short -- {{ generated }} >&2
        exit 1
    fi
    echo "generated artifacts are up to date"

# lint the Helm chart, kubeconform-validate every permutation, assert the single-instance guard
[group('infra')]
[script('bash')]
helm-lint:
    set -euo pipefail
    helm lint deploy/helm
    validate() {
        echo "helm template $*"
        helm template sf2loki deploy/helm "$@" \
            | kubeconform -strict -summary -ignore-missing-schemas \
                -kubernetes-version 1.29.0 -schema-location default
    }
    validate
    validate --set ha.enabled=true --set replicaCount=2 --set networkPolicy.enabled=true
    validate --set externalSecrets.enabled=true --set externalSecrets.aws.region=eu-west-1 --set serviceAccount.esoName=sf2loki-eso --set secrets.create=true
    if helm template sf2loki deploy/helm --set replicaCount=2 >/dev/null 2>&1; then
        echo "render guard did not fire: replicaCount>1 without ha.enabled must fail" >&2
        exit 1
    fi
    echo "render guard fired as expected"

# build the sdist and wheel into dist/
[group('build')]
build:
    uv build --out-dir dist

# build the distribution and assert its contents (stubs present, tests/docs/scratch absent)
[group('build')]
dist-check: build
    uv run --no-project python scripts/check_dist.py dist

# build the container image locally
[group('build')]
image tag="sf2loki:dev":
    docker build -t {{ tag }} .

# assert a built image's CLI responds and it runs as the non-root sf2loki user
[group('check')]
smoke tag="sf2loki:dev":
    docker run --rm {{ tag }} --help
    docker run --rm --entrypoint whoami {{ tag }} | grep -qx sf2loki

# run the service in the foreground until interrupted (long-running; needs a config file)
[group('dev')]
run config="config.yaml":
    uv run python -m sf2loki --config {{ config }}

# run the saturated-pipeline hot-path benchmark (prints timings; not in the gate)
[group('dev')]
bench:
    uv run python benchmarks/bench_pipeline_hotpath.py

# drive synthetic activity through a live Salesforce DEV org (long-running; needs .env.dev)
[confirm('This writes to (and with --cleanup deletes from) a live Salesforce org. Continue?')]
[group('dev')]
activity *args="--env-file .env.dev":
    uv run python scripts/generate_activity.py "$@"

# delete build output and tool caches (all reproducible by `just setup` / `just build`)
[confirm('Delete dist/, .venv/ and the ruff/mypy/pytest caches?')]
[group('build')]
clean:
    rm -rf dist .venv .ruff_cache .mypy_cache .pytest_cache

alias gate := check
```

### Recipe-by-recipe diff against the current `justfile`

| Current | Action | New |
|---|---|---|
| `set shell := ["bash", "-uc"]` (`justfile:1`) | **replace** — no `-e`, no `pipefail`, so a failing mid-recipe command green-lights the gate | `set shell := ["bash", "-euo", "pipefail", "-c"]` |
| — | **add** | `set positional-arguments` (so `activity`'s variadic args reach the script as `"$@"` with quoting intact) |
| `default` (`justfile:4`) | keep, retitle doc comment | `default` — `# show the task surface` |
| `setup: uv sync` (`justfile:8`) | **change body** | `uv sync --locked` — matches `scripts/cloud-environment-setup.sh:57` and fails loudly on a stale `uv.lock` |
| — | **add** | `fmt` — no in-place formatter exists today |
| `lint` (`justfile:16`, runs `ruff check .` + `ruff format --check .`) | **split** | `lint` = `ruff check --no-fix .`; `fmt-check` = `ruff format --check .` + `just --fmt --check` |
| `type` (`justfile:21`) | **rename** | `typecheck` (mandatory-adjacent vocabulary; `type` is not a fleet name) |
| `test` (`justfile:25`) | **add param** | `test filter=""` → `pytest -q -k <filter>` |
| `gate: lint type test` (`justfile:29`) | **rename + widen** | `check: fmt-check lint typecheck test gen-check helm-lint dist-check`, with `alias gate := check` so `just gate` keeps working |
| `proto` (`justfile:12`, `bash scripts/gen_proto.sh`) | **absorb the script** | `proto` body inlines the whole of `scripts/gen_proto.sh` |
| `gen-config` (`justfile:32`) | keep, add `[group('gen')]` | unchanged body |
| `gen-helm-values` (`justfile:38`) | keep, add `[group('gen')]` | unchanged body |
| — | **add** | `gen: proto gen-config` (aggregate) |
| — | **add** | `gen-check` — the drift gate, `[script('bash')]` |
| `helm-lint` (`justfile:42`) | **widen** to match CI, add `[script('bash')]` + `[group('infra')]`, and put it inside `check` | now also runs kubeconform over all three permutations and asserts the single-instance render guard |
| — | **add** | `build`, `dist-check` — CI's `package` job had no recipe at all |
| — | **add** | `smoke` — CI's image smoke assertions had no recipe |
| `run config="config.yaml"` (`justfile:50`) | keep; **fix doc comment** to say it blocks, and fix `{{config}}` → `{{ config }}` | `# run the service in the foreground until interrupted (long-running; needs a config file)` |
| `image tag="sf2loki:dev"` (`justfile:54`) | keep; fix `{{tag}}` → `{{ tag }}` | unchanged body |
| — | **add** | `ci: check image smoke`, `bench`, `activity`, `clean` |

Group assignment is orthogonal to gate membership: `helm-lint` sits in `[group('infra')]` and
`gen-check` in `[group('gen')]`, yet both are dependencies of `check`. `just --show check` is the
authoritative list of what the gate runs.

`check` deliberately excludes the container image build. CI builds the image through a SHA-pinned
`uses: docker/build-push-action` with GHA layer caching, and the standard forbids converting a
`uses:` into `run: just`. `ci: check image smoke` is the local equivalent of the whole workflow.

## 3. Makefile disposition

**None exists.** `find . -name Makefile -o -name GNUmakefile` (excluding `.venv/`, `.cache/`)
returns nothing, and no Makefile is tracked. Nothing to `git rm`. Do not add one.

## 4. Script disposition

| Script | Verdict | Recipe | Notes |
|---|---|---|---|
| `scripts/gen_proto.sh` (29 lines) | **ABSORB — `git rm`** | `proto` | Pure "make some dirs and run a tool with flags"; no loops, functions, traps or argument parsing. Its own docstring already says "Run via `just proto`". Exact recipe lines below. |
| `scripts/gen_helm_values.py` (55 lines) | **KEEP** | `gen-helm-values`, and inside `gen-config` | A real program: it parses `deploy/helm/values.yaml`, locates the `configdoc._HELM_VALUES_BEGIN/_END` sentinels and splices a region in place. |
| `scripts/check_dist.py` (94 lines) | **KEEP** | `dist-check` | A real program: opens the built wheel/sdist as zip/tar archives and asserts member paths against `REQUIRED_IN_WHEEL` / `FORBIDDEN_SUBSTRINGS`. |
| `scripts/generate_activity.py` (1345 lines) | **KEEP** | `activity` | A real program with its own argparse CLI, capability discovery and `--cleanup` path; documented at `docs/development/generate-activity.md`. |
| `scripts/cloud-environment-setup.sh` (66 lines) | **KEEP** | none — deliberately | Executes on a Codex/Claude cloud runner that has no `just` yet; it is the script that *installs* `just` (`scripts/cloud-environment-setup.sh:41-45`). It has a `version_is()` function, conditionals, and `~/.bashrc` mutation. Wrapping it in a recipe would be circular. Leave the invocation `bash scripts/cloud-environment-setup.sh` exactly as documented in `CONTRIBUTING.md:32`. |

### ABSORB detail — `scripts/gen_proto.sh` → the `proto` recipe

Original body (`scripts/gen_proto.sh:7-29`), and the recipe lines that replace it:

- `cd "$(dirname "$0")/.."` → **dropped.** `just` runs recipes with cwd set to the justfile's
  directory, which is the repo root. Do not add `[no-cd]`.
- `SF=…` / `LK=…` → the justfile variables `sf_gen` / `loki_gen`. Shell variables cannot be used
  because each recipe line runs in its own shell.
- `mkdir -p "$SF" "$LK"` → `mkdir -p {{ sf_gen }} {{ loki_gen }}`
- `touch "$SF/__init__.py" "$LK/__init__.py"` → `touch {{ sf_gen }}/__init__.py {{ loki_gen }}/__init__.py`
- both `grpc_tools.protoc` invocations → one line each, backslash continuations flattened
- `sed -i.bak 's/^import pubsub_api_pb2/from . import pubsub_api_pb2/' …` → verbatim. **Keep the
  `.bak` suffix** — it is what makes the `-i` flag portable between GNU and BSD sed, and this repo is
  developed on macOS and CI'd on Linux.
- `rm -f "$SF"/*.bak` → `rm -f {{ sf_gen }}/*.bak`
- The final `echo "generated stubs in …"` → dropped; `just` echoes each command line to stderr
  already.
- `set -euo pipefail` → dropped; the file-level `set shell` supplies it.

Delete the file (`git rm scripts/gen_proto.sh`) only after `.github/workflows/ci.yml:54` no longer
references it.

## 5. CI changes

### `.github/workflows/ci.yml`

Insert this step into the `gate`, `package` and `helm-chart` jobs, immediately after the
`actions/checkout` step (and after `astral-sh/setup-uv` is fine too; order relative to the toolchain
setup does not matter). Resolve the action SHA at implementation time and record the version in the
trailing comment, matching the SHA-pin convention every other action in this repo already uses:

```yaml
      - name: Set up just
        uses: extractions/setup-just@<resolve-sha-at-implementation-time> # v4
        with:
          just-version: '1.58.0'
```

`1.58.0` is the version this repo already pins for cloud agents
(`scripts/cloud-environment-setup.sh:9`), and is the version the justfile in section 2 was validated
against. Pin it exactly — `just --fmt` output carries no backwards-compatibility guarantee, so an
unpinned bump can turn `fmt-check` red with no repo change.

**Job `gate`** (`ci.yml:17-56`). Keep `name: lint · type · test · proto-drift` or retitle it to
`lint · fmt · type · test · drift`; keep `runs-on`, the `harden-runner` step, the `checkout` step
with `persist-credentials: false`, and the `astral-sh/setup-uv` step exactly as they are. Then:

| Current step | Lines | Becomes |
|---|---|---|
| `Sync (frozen)` — `run: uv sync --frozen` | `ci.yml:37-38` | `run: just setup` (note: `--frozen` → `--locked`; see section 9) |
| `Ruff lint` — `run: uv run ruff check .` | `ci.yml:40-41` | `run: just lint` |
| `Ruff format check` — `run: uv run ruff format --check .` | `ci.yml:43-44` | `run: just fmt-check` (this also newly enforces `just --fmt --check`) |
| `Mypy (strict)` — `run: uv run mypy src` | `ci.yml:46-47` | `run: just typecheck` |
| `Tests` — `run: uv run pytest -q` | `ci.yml:49-50` | `run: just test` |
| `Proto stub drift` — the `run: \|` block calling `bash scripts/gen_proto.sh` then `git diff --exit-code` | `ci.yml:52-56` | `run: just gen-check` — one line; the recipe now covers the config artifacts and the Helm values block as well as the proto stubs |

**Job `docker-build-test`** (`ci.yml:58-90`). Do **not** touch `docker/setup-buildx-action`
(`ci.yml:71-72`) or `docker/build-push-action` (`ci.yml:74-83`) — they are `uses:` steps with GHA
cache wiring and the standard forbids converting a `uses:` into `run: just`. Only the final step
changes:

| Current step | Lines | Becomes |
|---|---|---|
| `Smoke-test image` — two-line `run: \|` with `docker run --rm sf2loki:test --help` and the `whoami \| grep -qx sf2loki` assertion | `ci.yml:85-90` | `run: just smoke sf2loki:test` |

Add the `setup-just` step to this job too, before the smoke step.

**Job `package`** (`ci.yml:92-116`). Keep `harden-runner`, `checkout`, `setup-uv`.

| Current step | Lines | Becomes |
|---|---|---|
| `Build sdist + wheel` — `run: uv build --out-dir dist` | `ci.yml:112-113` | delete — `dist-check` depends on `build` |
| `Check dist contents …` — `run: uv run --no-project python scripts/check_dist.py dist` | `ci.yml:115-116` | `run: just dist-check` |

**Job `helm-chart`** (`ci.yml:118-174`). Keep `harden-runner`, `checkout`, `azure/setup-helm`
(`ci.yml:131-132`) and the whole `Install kubeconform` step including its `KUBECONFORM_SHA256`
checksum verification (`ci.yml:134-143`) — that is toolchain provisioning, not task logic.

| Current step | Lines | Becomes |
|---|---|---|
| `Helm lint` — `run: helm lint deploy/helm` | `ci.yml:145-146` | delete — folded into `just helm-lint` |
| `Helm template + kubeconform validate` — the `run: \|` block with the `validate()` bash function and three invocations | `ci.yml:151-165` | delete — folded into `just helm-lint` |
| `Assert single-instance render guard fires` — the `run: \|` block with the `if helm template … then error` guard | `ci.yml:168-174` | delete — folded into `just helm-lint` |
| — | — | one new step: `run: just helm-lint` |

The `echo "::group::"` / `echo "::endgroup::"` wrappers at `ci.yml:156,160` are dropped in the move
(they are GitHub-log folding, not logic); the recipe echoes `helm template $*` instead. That is an
accepted cosmetic regression in the CI log.

**Job `ci-success`** (`ci.yml:176-191`). **Do not touch it.** Its `name: ci-success`, its
`if: always()`, and its `needs: [gate, docker-build-test, package, helm-chart]` list are what the
branch ruleset and Renovate automerge gate on. The four job ids must keep their current names.

Also unchanged in `ci.yml`: the workflow-level `permissions: contents: read` (`ci.yml:9-10`), the
`concurrency` block (`ci.yml:12-14`), every `step-security/harden-runner` step, every
`persist-credentials: false`, and every action SHA pin.

### `.github/workflows/release-please.yml`

One step qualifies: `Verify dist contents before publish` at `release-please.yml:103-104`
(`run: uv run --no-project python scripts/check_dist.py dist`). Leave it alone. The preceding step
`Build sdist + wheel` (`release-please.yml:100-101`) deliberately runs with
`enable-cache: false` (`release-please.yml:98`) for cache-poisoning resistance, and the publish path
is a release lane, not a task surface. Converting it would add a `just` install to the release
critical path for no gain. **Out of scope — do not edit this file.**

### Every other workflow

`actionlint.yml`, `arm-automerge.yml`, `auto-rc.yml`, `codeql.yml`, `dependency-review.yml`,
`docker-security.yml`, `ghcr-cleanup.yml`, `publish.yml`, `scorecard.yml`,
`trigger-docs-sync.yml`, `zizmor.yml` — every one of them is either a thin caller over a
`uses: rknightion/.github/...` reusable or a GitHub-native security/release workflow. **None of them
gets a `just` step. Do not edit any of them.**

## 6. Docs and agent-contract changes

| File | Line(s) | Current | Change to |
|---|---|---|---|
| `AGENTS.md` | 24-32 | the `## Quick commands` block naming `just gate`, `just test`, `just lint`, `just proto`, `just gen-config`, `just run` | **Delete the whole block** and replace the `## Quick commands` + `## The green bar` sections with the Task interface section below. Do not paste a recipe list — it rots. |
| `AGENTS.md` | 34-37 | `## The green bar` — "`just gate` (= `ruff check` + …) must be green before any commit" | folded into the Task interface section; the gate is `just check` |
| `AGENTS.md` | 40-44 | `## Generated files — never hand-edit`: "run `just gen-config` after any `config.py` change"; "proto stubs … come from `just proto`" | keep the section; add that `just gen` regenerates everything and `just gen-check` (inside `just check`) is the drift gate |
| `CLAUDE.md` | — | thin `@AGENTS.md` import | **no change** |
| `CONTRIBUTING.md` | 12-13 | `just setup` / `just gate    # ruff + mypy --strict + pytest (the green bar; CI runs the same)` | `just setup` / `just check   # the full gate; CI enforces exactly this` |
| `CONTRIBUTING.md` | 18-22 | `just test` / `just lint` / `just proto` / `just gen-config` | keep, but change the `just lint` comment to "ruff lint only (`just fmt-check` covers formatting)" and add `just fmt   # format in place` |
| `CONTRIBUTING.md` | 32 | `bash scripts/cloud-environment-setup.sh` | **no change** — KEEP script, invoked by the cloud provider before `just` exists |
| `CONTRIBUTING.md` | 48-49 | "(`just gen-config` / `just proto`)" | "(`just gen`)" |
| `README.md` | 263 | "Regenerate after changing config models with `just gen-config`" | unchanged wording is still correct; optionally mention `just gen` |
| `README.md` | 720-724 | the Development block: `uv sync` / `just gate` / `just proto` / `just image` | `just setup` / `just check   # the full gate (ruff, mypy --strict, pytest, drift, helm, dist)` / `just gen` / `just image` |
| `README.md` | 726-727 | "Tooling: `uv`, `ruff`, `mypy --strict`, `pytest` + `pytest-asyncio`, `just`." | add `helm`, `kubeconform` and `docker` to the list, since `just check` now needs helm + kubeconform |
| `docs/installation.md` | 86-87 | `just setup` / `just gate          # ruff + mypy --strict + pytest — the green bar` | `just setup` / `just check         # the full gate` |
| `docs/installation.md` | 91 | "`just setup` is a thin wrapper over `uv sync`" | "`just setup` is a thin wrapper over `uv sync --locked`" |
| `docs/development/contributing.md` | 11-12 | `just setup` / `just gate` | `just setup` / `just check` |
| `docs/development/contributing.md` | 18-21 | `just test` / `just lint` / `just proto` / `just gen-config` | same treatment as `CONTRIBUTING.md:18-22` |
| `docs/development/contributing.md` | 29-30 | "**`just gate` must be green before a commit**" | "**`just check` must be green before a commit**" |
| `docs/development/contributing.md` | 32-33 | "(`just gen-config` / `just proto`)" | "(`just gen`)" |
| `docs/development/generate-activity.md` | 71, 74, 77 | `python scripts/generate_activity.py --env-file .env.dev …` | rewrite as `just activity --env-file .env.dev …`, and note the recipe is `[confirm]`-guarded because it writes to a live org |
| `deploy/helm/values.yaml` | 254, 256 | "run `just gen-helm-values`" inside the generated banner | **do not hand-edit.** This text is emitted by `src/sf2loki/configdoc.py` — if the wording changes, change it there (`configdoc._HELM_VALUES_BEGIN` and the surrounding banner) and run `just gen`. `just gen-helm-values` still exists, so no change is actually required. |
| `src/sf2loki/configdoc.py` | 405, 433 | docstring references to `scripts/gen_helm_values.py` | **no change** — the script is a KEEP |
| `tests/test_config_artifacts_drift.py` | 35 | comment "run `just gen-helm-values`" | still accurate; no change required |

### AGENTS.md Task interface section

Replace `AGENTS.md:23-37` (`## Quick commands` + `## The green bar`) with:

```markdown
## Task interface

This repo's task surface is a `justfile`. Discover it, don't guess it:

    just --list                        # human-readable
    just --dump --dump-format json     # machine-readable
    just --show <recipe>               # what a recipe actually runs

- `just check` is the full gate and is exactly what CI enforces. It must pass before you commit.
  `just gate` is an alias for it. It needs `helm`, `kubeconform`, `uv` and `git` on PATH.
- Prefer `just <recipe>` over the underlying tool. If you are typing `pytest`, you want `just test`.
- Run `just` with stdin from /dev/null. Recipes marked `[confirm]` are destructive — stop and ask
  before running one; never pass `--yes` or `JUST_YES=1`.
- If a task you need does not exist, add a recipe with a `#` doc comment and a `[group(...)]`
  rather than running a bare command.
- Strict TDD: failing test → watch it fail → minimal code → green.
```

## 7. backlog/config.yml

`backlog/config.yml:4-7` currently reads:

```yaml
definition_of_done:
  - "just gate is green (ruff check + ruff format --check + mypy src + pytest) — run it, don't assert it"
  - "just gen-config run and its output committed, if config.py changed (CI drift gate fails otherwise)"
  - "committed straight to main with a conventional-commit message, and pushed"
```

Replace with:

```yaml
definition_of_done:
  - "just check is green (fmt-check + lint + typecheck + test + gen-check + helm-lint + dist-check) — run it, don't assert it"
  - "just gen run and its output committed, if config.py or proto/ changed (just gen-check inside the gate fails otherwise)"
  - "committed straight to main with a conventional-commit message, and pushed"
```

`backlog/config.yml` is the one file under `backlog/` that is edited by hand — list-valued keys
cannot be set through `backlog config set` (`AGENTS.md:83-84`). Do not hand-edit anything else under
`backlog/`.

## 8. Order of work

1. Write the new `justfile` from section 2. Do not delete anything yet.
2. Prove it locally, in this order, and paste the real output — do not assert it:
   `just --fmt --check` (must exit 0) → `just --list` (must exit 0 and show a doc comment and group
   for every recipe) → `just setup` → `just fmt-check` → `just lint` → `just typecheck` →
   `just test` → `just gen-check` → `just helm-lint` → `just dist-check` → `just check`.
3. Run `just check` a second time. It must still be green and must leave `git status` clean — this
   is what proves `gen` is idempotent and `check` is re-runnable.
4. Run `just image` then `just smoke`, or `just ci`, once. Docker is needed only for this step.
5. Edit `.github/workflows/ci.yml`: add the `setup-just` step to all four jobs, then collapse the
   `run:` bodies per section 5. Leave `ci-success` alone.
6. Update `AGENTS.md`, `CONTRIBUTING.md`, `README.md`, `docs/installation.md`,
   `docs/development/contributing.md`, `docs/development/generate-activity.md` per section 6.
7. Update `backlog/config.yml`'s `definition_of_done` per section 7.
8. Verify nothing still points at the absorbed script:
   `git grep -n 'gen_proto\|just gate\|just type\b' -- ':!CHANGELOG.md' ':!archive/'` must return
   nothing except an intentional `alias gate := check` line and historical changelog text.
9. **Only now** `git rm scripts/gen_proto.sh`.
10. Run `just check` once more, then commit and push. Conventional-commit type: `chore:`, subject
    carries the task id.

## 9. Traps specific to this repo

- **`just --fmt` sorts recipe attributes alphabetically.** `[confirm]` < `[group]` <
  `[no-exit-message]` < `[script]`. Writing `[group('dev')]` above `[confirm(...)]` makes
  `just --fmt --check` exit 1 with a diff, which fails `fmt-check`, which fails `check`. The file in
  section 2 is already in the right order — preserve it.
- **The current `justfile` already fails `just --fmt --check`.** Verified: `justfile:51` has
  `{{config}}` and `justfile:55` has `{{tag}}`; the formatter wants `{{ config }}` and `{{ tag }}`.
  So `fmt-check` goes red the moment it is added, until those two lines are fixed. Section 2 fixes
  them.
- **`set shell := ["bash", "-uc"]` at `justfile:1` has no `-e` and no `pipefail`.** Every existing
  multi-line recipe — `lint`, `gen-config`, `helm-lint` — currently continues past a failing command
  and exits 0 on the last one. `just gate` has therefore been under-reporting: a `ruff check` failure
  followed by a passing `ruff format --check` exits 0 today. Expect the widened gate to surface real
  pre-existing failures on the first run. Fix them; do not weaken the gate.
- **`gen-check` must not diff against `HEAD`.** CI's current proto-drift step uses
  `git diff --exit-code` (`ci.yml:55`), which is correct only on a clean checkout. Locally an agent
  runs `just check` with uncommitted work in the tree, and a HEAD-diff would fail on every
  legitimately-changed generated file. The recipe in section 2 therefore hashes the generated paths
  with `git hash-object` before and after running `gen` and compares the two — it detects staleness
  without caring what is committed. Do not "simplify" it back to `git diff --exit-code`.
- **`gen-check` calls `just gen` inside its body rather than declaring it as a dependency.** That is
  deliberate and is the one sanctioned exception to "aggregates are dependency lists": the recipe
  needs the pre-`gen` hashes, and a dependency would run `gen` before the body could take them.
- **`gen-check` only sees tracked files.** `git ls-files` is the input to the hash, so a newly
  generated *untracked* file would be invisible. That is acceptable here — `proto` and `gen-config`
  only rewrite files that already exist and are committed. If a future change makes `gen` emit a new
  file, `git add` it before trusting the gate.
- **`scripts/gen_proto.sh` does `cd "$(dirname "$0")/.."` (line 7).** `just` already runs recipes
  from the justfile's directory, so the `cd` is dropped, not translated. Do not add `[no-cd]`, which
  would break it.
- **Keep `sed -i.bak` (`gen_proto.sh:25`).** The `.bak` suffix plus the follow-up `rm -f …/*.bak` is
  what makes in-place sed work identically on macOS (BSD sed) and Linux (GNU sed). A bare `sed -i`
  fails on macOS. Both platforms matter: development is on macOS, CI is `ubuntu-latest`.
- **Each recipe line is its own shell.** `helm-lint` needs a bash function (`validate()`) and an
  `if`, and `gen-check` needs variables that survive across lines — both are `[script('bash')]` for
  that reason. `[script(...)]` bypasses `set shell`, so each of those bodies restates
  `set -euo pipefail` on its first line. Do not remove it.
- **`just check` now requires `helm` and `kubeconform` on PATH.** Neither is installed by
  `just setup` or by `scripts/cloud-environment-setup.sh` (which installs only `uv`, `just` and
  `backlog`). A cloud agent running `just check` will fail at `helm-lint` with "command not found".
  If that becomes painful, the fix is to add helm + kubeconform to
  `scripts/cloud-environment-setup.sh` — not to drop `helm-lint` out of `check`.
- **`setup` moves from `uv sync` to `uv sync --locked`, and CI's step moves from `--frozen` to
  `--locked`.** `--locked` additionally asserts `uv.lock` is up to date with `pyproject.toml`; this
  is deliberate (it matches `scripts/cloud-environment-setup.sh:57`) and makes a stale lockfile fail
  loudly instead of silently. If a Renovate PR ever trips on it, the lockfile really was stale.
- **`kubeconform -kubernetes-version 1.29.0`** is hard-coded in `ci.yml:158` and carried verbatim
  into the recipe. Keep the pin; bumping it is a separate decision.
- **`smoke`'s second line pipes into `grep -qx`.** With `pipefail` set, a `docker run` failure now
  propagates where it previously could not. That is the intended behaviour change.
- **`run` blocks forever.** `justfile:50` currently reads `# run the service locally (needs a config
  file)`, which does not say so. The service is a long-running daemon; an agent that invokes it
  synchronously will hang. The new doc comment says "in the foreground until interrupted".
- **`activity` is `[confirm]`-guarded** because `scripts/generate_activity.py` writes records into a
  live Salesforce DEV org and its `--cleanup` mode deletes them. With stdin closed, `[confirm]` fails
  closed at exit 1 — that is correct, not a bug.
- **`pyproject.toml:87` has a per-file-ignore for `deploy/grafana/gen_dashboard.py`, which does not
  exist** (no such tracked file; the Grafana dashboards are hand-authored, per `AGENTS.md:46-53`).
  It is a harmless stale entry. **Out of scope — do not remove it in this task.**
- **No unstable features.** The file in section 2 uses only stable constructs, verified against
  `just 1.58.0`: `[group]`, `[confirm]`, `[no-exit-message]`, `[script('bash')]`,
  `set positional-arguments`, `quote()`, `require()`, `alias`, and `if/else` in an interpolation. Do
  not add `set lists`, list literals, `set dotenv-command`, `[cache]` or user-defined functions —
  one unstable feature makes `just --list` and `just --dump` exit 1 for the whole file.
- **No `set dotenv-load`.** `.env.dev` exists but is gitignored, holds live DEV Salesforce and
  Grafana Cloud credentials, and is consumed explicitly by `scripts/generate_activity.py --env-file`.
  Auto-loading it into every recipe's environment would leak those credentials into `just test` and
  `just check`. Do not add it.

## 10. Out of scope

Do not touch any of the following in this task:

- **KEEP scripts — none of these is deleted, rewritten or inlined:**
  `scripts/check_dist.py`, `scripts/gen_helm_values.py`, `scripts/generate_activity.py`,
  `scripts/cloud-environment-setup.sh`.
- **GitHub-native workflows — no `just` step, no edits at all:** `.github/workflows/actionlint.yml`,
  `arm-automerge.yml`, `auto-rc.yml`, `codeql.yml`, `dependency-review.yml`, `docker-security.yml`,
  `ghcr-cleanup.yml`, `publish.yml`, `release-please.yml`, `scorecard.yml`,
  `trigger-docs-sync.yml`, `zizmor.yml`.
- The `ci-success` job in `ci.yml` — its name, its `if: always()`, and its `needs:` list.
- Any `permissions:` block, `concurrency:` group, `persist-credentials: false`, action SHA pin,
  `harden-runner` step, `workflow_call` input, or `uses: rknightion/.github/...` reusable call.
- The `KUBECONFORM_SHA256` checksum step (`ci.yml:134-143`) and the `azure/setup-helm` step.
- `renovate.json`, `release-please-config.json`, `.release-please-manifest.json`, `docs.toml`,
  `Dockerfile`, `docker-compose*.yml`, `deploy/helm/**` templates and values, `deploy/grafana/**`.
- Anything under `backlog/` other than `config.yml`'s `definition_of_done`, and that by hand only
  because list-valued keys have no `backlog config set` form.
- The stale `deploy/grafana/gen_dashboard.py` ruff per-file-ignore at `pyproject.toml:87`.
- Any behaviour change to `src/sf2loki/**` or `tests/**`. This task changes the task surface only;
  the only source-tree writes are the regenerated artifacts `just gen` produces, and there should be
  none if the committed ones are current.
<!-- SECTION:DESCRIPTION:END -->

## Acceptance Criteria
<!-- AC:BEGIN -->
- [ ] #1 Top-level justfile defines the seven mandatory recipes (default, setup, fmt, fmt-check, lint, test, check) plus typecheck, gen, gen-check, ci, and alias gate := check, with the header set shell := ["bash", "-euo", "pipefail", "-c"]
- [ ] #2 just check runs fmt-check, lint, typecheck, test, gen-check, helm-lint and dist-check; it is green on a clean checkout and a second consecutive run is still green and leaves git status clean
- [ ] #3 just --fmt --check exits 0 (the {{config}} and {{tag}} interpolation spacing at the current justfile:51 and justfile:55 is fixed) and fmt-check includes just --fmt --check
- [ ] #4 just --list exits 0 and shows a # doc comment and one of the six fleet groups (check/build/dev/gen/infra/release) for every public recipe; just --dump --dump-format json exits 0, proving no unstable feature is used
- [ ] #5 No Makefile or GNUmakefile exists or is created; the repo has none today and none is added
- [ ] #6 scripts/gen_proto.sh is deleted and its body (including the portable sed -i.bak rewrite and the .bak cleanup) lives in the proto recipe; nothing in the tree references gen_proto.sh outside CHANGELOG.md and archive/
- [ ] #7 The KEEP scripts survive as files and are each reachable through a recipe: scripts/check_dist.py via just dist-check, scripts/gen_helm_values.py via just gen-helm-values and just gen-config, scripts/generate_activity.py via just activity; scripts/cloud-environment-setup.sh is left completely untouched
- [ ] #8 In .github/workflows/ci.yml the gate, package, helm-chart and docker-build-test jobs each carry a SHA-pinned extractions/setup-just step with just-version: '1.58.0' and their shell steps collapse to just setup / just lint / just fmt-check / just typecheck / just test / just gen-check / just dist-check / just helm-lint / just smoke sf2loki:test; the docker/build-push-action and azure/setup-helm and kubeconform-checksum steps stay as they are
- [ ] #9 ci-success keeps its exact name, its if: always() and its needs: [gate, docker-build-test, package, helm-chart] list, and no workflow file other than ci.yml is edited
- [ ] #10 AGENTS.md carries a Task interface section naming just check as the gate with no pasted recipe list, and CONTRIBUTING.md, README.md, docs/installation.md, docs/development/contributing.md and docs/development/generate-activity.md no longer tell anyone to run just gate or python scripts/generate_activity.py
- [ ] #11 backlog/config.yml definition_of_done names just check and just gen instead of just gate and just gen-config
<!-- AC:END -->

## Definition of Done
<!-- DOD:BEGIN -->
- [ ] #1 just gate is green (ruff check + ruff format --check + mypy src + pytest) — run it, don't assert it
- [ ] #2 just gen-config run and its output committed, if config.py changed (CI drift gate fails otherwise)
- [ ] #3 committed straight to main with a conventional-commit message, and pushed
<!-- DOD:END -->

## Comments

<!-- COMMENTS:BEGIN -->
author: campaign-ordering
created: 2026-08-29 09:18
---
## Fleet ordering — WAVE 0, the pilot. Do this repo FIRST.

Nothing else in the 42-repo campaign starts until this one lands. It is the pilot because it already carries the fleet's only pre-existing `justfile`, so it is the smallest delta while still exercising the whole standard: `just --fmt --check`, `[group(...)]` on every public recipe, the `gen` / `gen-check` drift pair, and CI wiring.

**Your extra obligation as the pilot:** the other 41 tasks were written against a frozen seam. Anything you find wrong, missing or ambiguous in the standard is cheap to fix here and 41x more expensive later. When you finish, report every deviation you had to make and why, so the standard is amended before Wave 1 begins. Do not silently work around a defect in the standard.

**Provisioning `just` in CI.** Which mechanism depends on the runner, and the two must not be mixed:

| Runner | Mechanism |
| --- | --- |
| `arc-arm64` (m7kni self-hosted) | `just` is **baked into the runner image** by `m7kni/ci-tools` (`runner-image/Dockerfile`, `ARG JUST_VERSION`). Do **not** add `extractions/setup-just`, and delete the step if this repo already has one — it installs a second `just` earlier on `PATH` and turns the image pin into a lie. |
| GitHub-hosted (all `rknightion` repos) | `extractions/setup-just`, SHA-pinned, with an explicit `just-version:`. |

Both sides currently sit on **1.58.0** and are Renovate-managed. `ci-tools`' `Tool version drift` workflow fails if the Dockerfile `ARG` and the published image ever disagree, and lists any repo still carrying a second pin.

**While you are in the workflow files, check the hub pin.** On 2026-08-29 Renovate was unfrozen for `rknightion/.github` in `m7kni/renovate-config` — it had been `enabled: false` on the mistaken belief that callers tracked `@main`, which froze the fleet across 19 different hub SHAs (v1.3.1 June → v1.9.7 August) so that no hub fix ever propagated. Bumps now arrive as one grouped, CI-gated, automerged PR per repo. **A `uses:` whose comment is not a real `# vX.Y.Z` still cannot be bumped** (it resolves to a digest-only update, which the fleet rules disable) — if you find one, repair the comment as part of this task.
---

author: campaign-ordering
created: 2026-08-29 10:43
---
## Standard amendment — `ci` is the sanctioned superset of `check` (RATIFIED)

This supersedes the frozen wording *"`check` is the complete local gate and reproduces every CI job that can run off a GitHub runner"*, which several lanes could not honour without making the pre-commit gate depend on a Docker daemon.

**The definitions now are:**

- **`check`** — everything that runs with **only the language toolchain installed**. This is the pre-commit gate. A leg that runs on a bare toolchain belongs here *however long it takes*.
- **`ci`** — `check` plus the legs CI gates that need a **Docker daemon, a service container, or cross-compilation**, and nothing else. Written as `ci: check <heavy legs>`.

**Every leg you put in `ci` must carry a comment naming which of those three it needs.** That comment is the guard: without it `ci` becomes the bin for anything slow or awkward, `check` quietly stops meaning much, and the fleet is back to a per-repo gate.

Eleven of the 42 lanes arrived at this shape independently before it was ratified, which is why it won.

**If this repo has no such legs, it has no `ci` recipe at all** and `check` is the whole gate. Do not add an empty one.
---

author: campaign-ordering
created: 2026-08-29 11:03
---
## Correction to the WAVE 0 comment above

That comment says this repo carries **"the fleet's only pre-existing `justfile`"**. That is **wrong**. Six repos already have one: `backlog.md-iOS`, `dmarc-reporties`, `sf2loki`, `tailscale2otel`, `brewmdm-control-plane` and `brewmdm-macos-agent`.

The pilot choice still stands — this repo's justfile is the closest to the frozen standard and it exercises `--fmt --check`, groups and the `gen`/`gen-check` drift pair — but do **not** assume the other 41 repos are all greenfield. Six are partial migrations, and `tailscale2otel` and `brewmdm-control-plane` already run `extractions/setup-just` in CI.

Check for an existing `justfile` before planning a repo's migration; several tasks were written on the assumption there wasn't one.
---
<!-- COMMENTS:END -->
