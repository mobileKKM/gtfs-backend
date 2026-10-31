# Two images from one build: `--target api` (gtfs-api) and `--target worker` (gtfs-worker, the default).
# Each gets only its own dependencies (pyproject extras). Migrations run from the worker image (`alembic upgrade head`).

FROM ghcr.io/astral-sh/uv:python3.13-bookworm-slim AS build-base
ENV UV_COMPILE_BYTECODE=1 UV_LINK_MODE=copy UV_PYTHON_DOWNLOADS=never
WORKDIR /app
COPY pyproject.toml uv.lock ./

FROM build-base AS build-api
RUN --mount=type=cache,target=/root/.cache/uv uv sync --frozen --no-dev --no-install-project --extra api
COPY README.md ./
COPY src ./src
RUN --mount=type=cache,target=/root/.cache/uv uv sync --frozen --no-dev --extra api

FROM build-base AS build-worker
RUN --mount=type=cache,target=/root/.cache/uv uv sync --frozen --no-dev --no-install-project --extra worker
COPY README.md alembic.ini ./
COPY src ./src
COPY migrations ./migrations
RUN --mount=type=cache,target=/root/.cache/uv uv sync --frozen --no-dev --extra worker

FROM python:3.13-slim-bookworm AS runtime
RUN useradd --create-home --uid 1000 app
WORKDIR /app
ENV PATH="/app/.venv/bin:$PATH" PYTHONUNBUFFERED=1

FROM runtime AS api
COPY --from=build-api --chown=app:app /app /app
COPY --chown=app:app feeds.yml ./
USER app
EXPOSE 8000
CMD ["gtfs-api", "--host", "0.0.0.0", "--port", "8000"]

FROM runtime AS worker
COPY --from=build-worker --chown=app:app /app /app
COPY --chown=app:app feeds.yml ./
ENV ARCHIVE_DIR=/data/archive
RUN mkdir -p /data/archive && chown -R app:app /data
USER app
VOLUME ["/data"]
CMD ["gtfs-worker", "run"]
