FROM python:3.12-slim

COPY --from=ghcr.io/astral-sh/uv:0.7.20 /uv /bin/uv

WORKDIR /app
ENV UV_COMPILE_BYTECODE=1 UV_LINK_MODE=copy

COPY pyproject.toml uv.lock .python-version ./
RUN uv sync --frozen --no-dev

COPY receiver ./receiver

RUN useradd --system --no-create-home receiver
USER receiver

EXPOSE 8000
CMD ["/app/.venv/bin/uvicorn", "--factory", "receiver.app:create_app", "--host", "0.0.0.0", "--port", "8000"]
