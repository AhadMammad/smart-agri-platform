# =============================================================================
# smart-agri-etl — the image every Airflow DockerOperator task runs.
#
# Deliberately JVM-free: the lake is reached over WebHDFS, not libhdfs, which
# keeps this image small enough to start once per task without penalty.
#
# Build context is the repository root (see `make build-etl`).
# =============================================================================

# ---- builder ----------------------------------------------------------------
FROM python:3.12-slim-bookworm AS builder

COPY --from=ghcr.io/astral-sh/uv:0.5.11 /uv /usr/local/bin/uv

ENV UV_COMPILE_BYTECODE=1 \
    UV_LINK_MODE=copy \
    UV_PYTHON_DOWNLOADS=never \
    UV_PROJECT_ENVIRONMENT=/opt/venv

WORKDIR /build

# Dependency layer first: it rebuilds only when the lockfile actually changes.
COPY etl/pyproject.toml etl/uv.lock ./
RUN uv sync --frozen --no-dev --no-install-project

# Then the source, then the project itself.
#
# `--no-editable` is essential: the default editable install leaves a .pth file
# in the venv pointing at /build/src, which does not exist in the runtime stage,
# so the copied venv would import nothing.
COPY etl/src ./src
COPY etl/README.md ./README.md
RUN uv sync --frozen --no-dev --no-editable

# ---- test -------------------------------------------------------------------
# Integration tests have to run on the platform network: a WebHDFS write is
# redirected to `datanode:9864`, a hostname that only resolves inside it. This
# stage carries the dev dependencies and the test suite so `make test-integration`
# can run them from a container rather than from the host.
FROM builder AS test

RUN uv sync --frozen
COPY etl/tests ./tests

ENV PATH="/opt/venv/bin:${PATH}" \
    PYTHONUNBUFFERED=1

WORKDIR /build
ENTRYPOINT ["pytest"]
CMD ["-m", "integration"]

# ---- runtime ----------------------------------------------------------------
FROM python:3.12-slim-bookworm AS runtime

# ca-certificates is required for the Open-Meteo HTTPS calls in Phase 4.
RUN apt-get update \
 && apt-get install -y --no-install-recommends curl ca-certificates \
 && apt-get clean \
 && rm -rf /var/lib/apt/lists/*

# Tasks must never run as root.
RUN groupadd --gid 1000 etl \
 && useradd --uid 1000 --gid etl --create-home etl

COPY --from=builder /opt/venv /opt/venv

ENV PATH="/opt/venv/bin:${PATH}" \
    PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1

USER etl
WORKDIR /home/etl

ENTRYPOINT ["smart-agri"]
CMD ["--help"]
