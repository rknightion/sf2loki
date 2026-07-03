set shell := ["bash", "-uc"]

# list recipes
default:
    @just --list

# install deps into the project venv
setup:
    uv sync

# (re)generate gRPC/protobuf stubs from proto/
proto:
    bash scripts/gen_proto.sh

# lint + format check
lint:
    uv run ruff check .
    uv run ruff format --check .

# static type check
type:
    uv run mypy src

# unit tests
test:
    uv run pytest -q

# the green bar: lint + type + test
gate: lint type test

# regenerate the committed config docs from the schema
gen-config:
    uv run python -m sf2loki config example > config.example.yaml
    uv run python -m sf2loki config reference > docs/config-reference.md
    uv run python scripts/gen_helm_values.py

# regenerate ONLY the Helm chart's generated config block (subset of gen-config)
gen-helm-values:
    uv run python scripts/gen_helm_values.py

# lint + render the Helm chart (default values + the HA and extras permutations)
helm-lint:
    helm lint deploy/helm
    helm template sf2loki deploy/helm > /dev/null
    helm template sf2loki deploy/helm --set ha.enabled=true --set replicaCount=2 \
        --set networkPolicy.enabled=true --set externalSecrets.enabled=true \
        --set secrets.create=true > /dev/null

# run the service locally (needs a config file)
run config="config.yaml":
    uv run python -m sf2loki --config {{config}}

# build the container image
image tag="sf2loki:dev":
    docker build -t {{tag}} .
