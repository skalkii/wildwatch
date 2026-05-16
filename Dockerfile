# syntax=docker/dockerfile:1.7
# Multi-stage build: deps in one layer, slim runtime in another.

# ──── stage 1: build deps ────────────────────────────────────────────────
FROM python:3.12-slim AS builder

WORKDIR /build

# System deps for videodb git install + scientific Python wheels
RUN apt-get update && apt-get install -y --no-install-recommends \
        git \
        build-essential \
    && rm -rf /var/lib/apt/lists/*

COPY pyproject.toml ./
COPY wildwatch/__init__.py ./wildwatch/__init__.py

# Pre-install deps (large) — pyproject pins everything
RUN pip install --no-cache-dir --upgrade pip setuptools wheel \
 && pip install --no-cache-dir -e .

# ──── stage 2: runtime ──────────────────────────────────────────────────
FROM python:3.12-slim AS runtime

WORKDIR /app

# Copy installed site-packages from builder (much smaller than reinstalling)
COPY --from=builder /usr/local/lib/python3.12/site-packages /usr/local/lib/python3.12/site-packages
COPY --from=builder /usr/local/bin /usr/local/bin

# Copy app source (gets refreshed on every build — keep below deps for caching)
COPY pyproject.toml ./
COPY wildwatch ./wildwatch
COPY config.py ./
COPY prompts ./prompts
COPY scripts ./scripts

# Re-install in editable mode so changes mounted at runtime propagate
RUN pip install --no-cache-dir -e .

# Pre-create runtime dirs the app expects
RUN mkdir -p /app/data /app/logs

EXPOSE 8000

HEALTHCHECK --interval=15s --timeout=3s --start-period=10s --retries=3 \
    CMD python -c "import urllib.request; urllib.request.urlopen('http://localhost:8000/health', timeout=2).read()" || exit 1

CMD ["uvicorn", "wildwatch.webhooks:app", "--host", "0.0.0.0", "--port", "8000"]
