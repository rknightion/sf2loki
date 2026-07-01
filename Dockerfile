# syntax=docker/dockerfile:1

# --- Builder: resolve deps + install the project with uv ----------------------
FROM python:3.14-slim AS builder

COPY --from=ghcr.io/astral-sh/uv:latest /uv /uvx /bin/

ENV UV_COMPILE_BYTECODE=1 \
    UV_LINK_MODE=copy \
    UV_PYTHON_DOWNLOADS=never

WORKDIR /app

# Install dependencies first (cached layer keyed on the lockfile only).
COPY pyproject.toml uv.lock ./
RUN --mount=type=cache,target=/root/.cache/uv \
    uv sync --frozen --no-install-project --no-dev

# Then the project itself (committed proto stubs ship in the tree; no codegen).
COPY . .
RUN --mount=type=cache,target=/root/.cache/uv \
    uv sync --frozen --no-dev

# --- Runtime: slim, non-root --------------------------------------------------
FROM python:3.14-slim AS runtime

# Injected by the shared container-publish pipeline (release tag or short SHA).
ARG VERSION=dev
LABEL org.opencontainers.image.version="${VERSION}"

RUN groupadd --system --gid 10001 sf2loki \
    && useradd --system --uid 10001 --gid sf2loki --no-create-home sf2loki

WORKDIR /app
COPY --from=builder --chown=sf2loki:sf2loki /app /app

ENV PATH="/app/.venv/bin:$PATH" \
    PYTHONUNBUFFERED=1

USER sf2loki

# metrics (9090) and health (8080)
EXPOSE 9090 8080

# Use /readyz (readiness), not /healthz (liveness): Docker/ECS collapse health
# into a single signal, and for that signal we want "actually serving" — i.e.
# Salesforce auth resolved and the pipeline running. start-period covers the
# normal startup window during which /readyz is legitimately 503.
HEALTHCHECK --interval=15s --timeout=3s --start-period=20s --retries=3 \
    CMD ["python", "-c", "import urllib.request,sys; sys.exit(0 if urllib.request.urlopen('http://localhost:8080/readyz', timeout=2).status==200 else 1)"]

ENTRYPOINT ["python", "-m", "sf2loki"]
CMD ["--config", "/etc/sf2loki/config.yaml"]
