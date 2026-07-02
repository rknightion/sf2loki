# syntax=docker/dockerfile:1

# --- Builder: resolve deps + install the project with uv ----------------------
# Digest-pinned (tag kept alongside for readability): reproducible builds +
# Renovate proposes bumps to the digest as new 3.14-slim images publish
# (":latest" or a bare tag defeats both).
# renovate: datasource=docker depName=python versioning=docker
FROM python:3.14-slim@sha256:b877e50bd90de10af8d82c57a022fc2e0dc731c5320d762a27986facfc3355c1 AS builder

# Pinned minor tag + digest: reproducible builds + Renovate can propose bumps
# (":latest" defeats both).
# renovate: datasource=docker depName=ghcr.io/astral-sh/uv versioning=docker
COPY --from=ghcr.io/astral-sh/uv:0.11@sha256:3d868e555f8f1dbc324afa005066cd11e1053fc4743b9808ca8025283e65efa5 /uv /uvx /bin/

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
# renovate: datasource=docker depName=python versioning=docker
FROM python:3.14-slim@sha256:b877e50bd90de10af8d82c57a022fc2e0dc731c5320d762a27986facfc3355c1 AS runtime

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

# health endpoints (/healthz, /readyz); metrics are OTLP push — no scrape port
EXPOSE 8080

# Use /readyz (readiness), not /healthz (liveness): Docker/ECS collapse health
# into a single signal, and for that signal we want "actually serving" — i.e.
# Salesforce auth resolved and the pipeline running. start-period covers the
# normal startup window during which /readyz is legitimately 503.
HEALTHCHECK --interval=15s --timeout=3s --start-period=20s --retries=3 \
    CMD ["python", "-c", "import urllib.request,sys; sys.exit(0 if urllib.request.urlopen('http://localhost:8080/readyz', timeout=2).status==200 else 1)"]

ENTRYPOINT ["python", "-m", "sf2loki"]
CMD ["--config", "/etc/sf2loki/config.yaml"]
