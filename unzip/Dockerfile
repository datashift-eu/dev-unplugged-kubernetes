# ─────────────────────────────────────────────────────────────────────────────
# Stage 1 – build/install dependencies in an isolated layer
# ─────────────────────────────────────────────────────────────────────────────
FROM python:3.12-slim AS builder

WORKDIR /build

# Install build tools (needed by some boto3 deps)
RUN apt-get update && apt-get install -y --no-install-recommends gcc && \
    rm -rf /var/lib/apt/lists/*

COPY requirements.txt .
RUN pip install --upgrade pip && \
    pip install --prefix=/install --no-cache-dir -r requirements.txt


# ─────────────────────────────────────────────────────────────────────────────
# Stage 2 – lean runtime image
# ─────────────────────────────────────────────────────────────────────────────
FROM python:3.12-slim AS runtime

# Security: run as non-root
RUN addgroup --system appgroup && adduser --system --ingroup appgroup appuser

WORKDIR /app

# Copy installed packages from builder
COPY --from=builder /install /usr/local

# Copy application
COPY unzip_to_minio.py .

# Argo mounts an emptyDir at /tmp; make sure it's writable
RUN mkdir -p /tmp && chown appuser:appgroup /tmp

USER appuser

# Default entry point (all config via env vars; Argo can override args)
ENTRYPOINT ["python", "/app/unzip_to_minio.py"]