# Single image shared by all four services; docker-compose sets each `command`.
FROM ghcr.io/astral-sh/uv:python3.14-bookworm-slim

# git: the pubsub-py dependency is a git source. OpenSSL creates a test-only
# certificate used by the isolated TLS startup probe.
RUN apt-get update \
    && apt-get install -y --no-install-recommends git openssl \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app
RUN mkdir -p /app/certs \
    && openssl req -x509 -newkey rsa:2048 -sha256 -nodes -days 3650 \
       -keyout /app/certs/server.key \
       -out /app/certs/server.crt \
       -subj "/CN=localhost" \
       -addext "subjectAltName=DNS:localhost,IP:127.0.0.1"
ENV UV_COMPILE_BYTECODE=1 UV_LINK_MODE=copy

# Deps first (cached layer). The root project has no build backend and is run as
# a plain package from the workdir, so it is never installed — only its deps.
COPY pyproject.toml uv.lock ./
RUN uv sync --frozen --group testclient --no-install-project

COPY testclient/ ./testclient/

# Put the venv on PATH so `python`/`uvicorn` resolve without `uv run`.
ENV PATH="/app/.venv/bin:$PATH"

CMD ["python", "-m", "testclient.controller"]
