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

# run the service locally (needs a config file)
run config="config.yaml":
    uv run python -m sf2loki --config {{config}}

# build the container image
image tag="sf2loki:dev":
    docker build -t {{tag}} .
