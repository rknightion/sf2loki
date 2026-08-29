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

# THE GATE — everything that needs no Docker daemon. Must be green before any commit.
[group('check')]
check: fmt-check lint typecheck test gen-check helm-lint dist-check

# CI superset: check plus the image and smoke legs, which require a Docker daemon.
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

# assert the Helm chart rejects replicaCount>1 unless active-passive HA is enabled
[group('infra')]
[script('bash')]
helm-render-guard:
    set -euo pipefail
    if helm template sf2loki deploy/helm --set replicaCount=2 >/dev/null 2>&1; then
        echo "render guard did not fire: replicaCount>1 without ha.enabled must fail" >&2
        exit 1
    fi
    echo "render guard fired as expected"

# lint the Helm chart and kubeconform-validate every CI permutation with CRD schemas
[group('infra')]
[script('bash')]
helm-lint: helm-render-guard
    set -euo pipefail
    open_brace='{'
    close_brace='}'
    crd_schema="https://raw.githubusercontent.com/datreeio/CRDs-catalog/main/${open_brace}${open_brace}.Group${close_brace}${close_brace}/${open_brace}${open_brace}.ResourceKind${close_brace}${close_brace}_${open_brace}${open_brace}.ResourceAPIVersion${close_brace}${close_brace}.json"
    helm lint deploy/helm
    validate() {
        echo "helm template $*"
        helm template sf2loki deploy/helm "$@" \
            | kubeconform -strict -summary \
                -kubernetes-version 1.29.0 \
                -schema-location default \
                -schema-location "$crd_schema"
    }
    validate
    validate --set ha.enabled=true --set replicaCount=2 --set networkPolicy.enabled=true
    validate --set externalSecrets.enabled=true --set externalSecrets.aws.region=eu-west-1 --set serviceAccount.esoName=sf2loki-eso --set secrets.create=true

# build the sdist and wheel into dist/
[group('build')]
build:
    uv build --out-dir dist

# build the distribution and assert its contents (stubs present, tests/docs/scratch absent)
[group('build')]
dist-check: build
    uv run --no-project python scripts/check_dist.py dist

# build the container image locally (requires a Docker daemon)
[group('build')]
image tag="sf2loki:dev":
    docker build -t {{ tag }} .

# assert a built image's CLI responds and it runs as the non-root user (requires a Docker daemon)
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
