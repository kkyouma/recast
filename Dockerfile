# ── Builder ────────────────────────────────────────────────────────────────
# Uses the official uv image with Python 3.13 for fast dependency resolution
FROM ghcr.io/astral-sh/uv:0.6-python3.13-bookworm-slim AS builder

WORKDIR /app

ENV UV_COMPILE_BYTECODE=1 \
  UV_LINK_MODE=copy

# 1) Copy only dependency metadata first — this layer is cached
#    as long as no dependency changes.
COPY pyproject.toml uv.lock ./

# Install third-party deps only (cached if pyproject.toml is unchanged)
RUN uv sync --frozen --no-dev --no-install-project

# 2) Copy full source and install project packages
COPY src/ src/
RUN uv sync --frozen --no-dev


# ── Runtime ────────────────────────────────────────────────────────────────
# Minimal image — no uv, no build tools, just Python + the venv
FROM python:3.13-slim-bookworm

WORKDIR /app

# Copy the ready-to-use virtual environment and source code
COPY --from=builder /app/.venv /app/.venv
COPY --from=builder /app/src  /app/src

# Set PATH to use the virtualenv and PYTHONPATH so internal imports work seamlessly
ENV PATH="/app/.venv/bin:$PATH" \
  PYTHONPATH="/app/src/ingest"

# Default: run the CEN collector.
# Override CMD at Cloud Run Job level if needed for other collectors.
CMD ["python", "-m", "cen_collector"]
