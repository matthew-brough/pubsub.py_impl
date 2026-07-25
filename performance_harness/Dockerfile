# Single image shared by all four services; docker-compose sets each `command`.
FROM ghcr.io/astral-sh/uv:python3.14-bookworm-slim

# git: the pubsub-py dependency is a git source.
RUN apt-get update \
    && apt-get install -y --no-install-recommends git \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app
ENV UV_COMPILE_BYTECODE=1 UV_LINK_MODE=copy

# Deps first (cached layer). The root project has no build backend and is run as
# a plain package from the workdir, so it is never installed — only its deps.
COPY pyproject.toml uv.lock ./
RUN uv sync --frozen --group testclient --no-install-project

COPY testclient/ ./testclient/

# Put the venv on PATH so `python`/`uvicorn` resolve without `uv run`.
ENV PATH="/app/.venv/bin:$PATH"

CMD ["python", "-m", "testclient.controller"]
