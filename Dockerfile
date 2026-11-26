# One image for both processes of the demo:
#   web UI (default): gunicorn serving ui.app on $PORT (8080)
#   worker:           python worker.py
FROM python:3.13-slim

COPY --from=ghcr.io/astral-sh/uv:0.8 /uv /bin/uv

ENV UV_COMPILE_BYTECODE=1 \
    UV_LINK_MODE=copy \
    UV_PYTHON_DOWNLOADS=never \
    PYTHONUNBUFFERED=1 \
    PATH="/app/.venv/bin:$PATH" \
    PORT=8080

WORKDIR /app

COPY pyproject.toml uv.lock .python-version ./
RUN uv sync --locked --no-dev --no-install-project

COPY . .

# The activities write job files under demodata/ and the idempotency file in /app.
RUN useradd --create-home --uid 10001 app && chown -R app /app
USER 10001

EXPOSE 8080
CMD ["sh", "-c", "exec gunicorn --bind 0.0.0.0:${PORT} --threads 8 ui.app:app"]
