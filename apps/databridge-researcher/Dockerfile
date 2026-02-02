# ============================================
# DataBridge Researcher - Analytics Engine
# Multi-stage Dockerfile for production deployment
# ============================================

# Build arguments
ARG PYTHON_VERSION=3.10
ARG VERSION=dev
ARG BUILD_DATE=unknown
ARG VCS_REF=unknown

# ============================================
# Stage 1: Builder
# ============================================
FROM python:${PYTHON_VERSION}-slim as builder

WORKDIR /app

# Install build dependencies
RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential \
    gcc \
    libffi-dev \
    libpq-dev \
    && rm -rf /var/lib/apt/lists/*

# Create virtual environment
RUN python -m venv /opt/venv
ENV PATH="/opt/venv/bin:$PATH"

# Install Python dependencies
COPY requirements.txt .
RUN pip install --no-cache-dir --upgrade pip wheel setuptools && \
    pip install --no-cache-dir -r requirements.txt

# Install connector dependencies if available
COPY requirements-connectors.txt* ./
RUN if [ -f requirements-connectors.txt ]; then \
        pip install --no-cache-dir -r requirements-connectors.txt || true; \
    fi

# Install dev dependencies for testing in CI
COPY requirements-dev.txt* ./
RUN if [ -f requirements-dev.txt ]; then \
        pip install --no-cache-dir -r requirements-dev.txt || true; \
    fi

# ============================================
# Stage 2: Runtime
# ============================================
FROM python:${PYTHON_VERSION}-slim as runtime

# Re-declare build args for this stage
ARG VERSION
ARG BUILD_DATE
ARG VCS_REF

# Labels following OCI image spec
LABEL org.opencontainers.image.title="DataBridge Researcher - Analytics Engine" \
      org.opencontainers.image.description="Headless FP&A analytics platform with NL-to-SQL, insights, and workflow automation" \
      org.opencontainers.image.version="${VERSION}" \
      org.opencontainers.image.created="${BUILD_DATE}" \
      org.opencontainers.image.revision="${VCS_REF}" \
      org.opencontainers.image.vendor="DataBridge AI" \
      org.opencontainers.image.source="https://github.com/databridge-ai/databridge" \
      org.opencontainers.image.licenses="MIT"

# Install runtime dependencies
RUN apt-get update && apt-get install -y --no-install-recommends \
    libffi8 \
    libpq5 \
    ca-certificates \
    curl \
    && rm -rf /var/lib/apt/lists/* \
    && apt-get clean

# Create non-root user for security
RUN groupadd --gid 1000 databridge && \
    useradd --uid 1000 --gid databridge --shell /bin/bash --create-home databridge

# Set working directory
WORKDIR /app

# Copy virtual environment from builder
COPY --from=builder /opt/venv /opt/venv
ENV PATH="/opt/venv/bin:$PATH"

# Copy application code
COPY --chown=databridge:databridge src/ ./src/
COPY --chown=databridge:databridge skills/ ./skills/ 2>/dev/null || true
COPY --chown=databridge:databridge templates/ ./templates/ 2>/dev/null || true

# Create data and settings directories
RUN mkdir -p /app/data /app/settings /app/logs && \
    chown -R databridge:databridge /app/data /app/settings /app/logs

# Environment variables
ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    PYTHONPATH=/app \
    DATABRIDGE_DATA_DIR=/app/data \
    DATABRIDGE_SETTINGS_DIR=/app/settings \
    DATABRIDGE_VERSION=${VERSION}

# Switch to non-root user
USER databridge

# Health check
HEALTHCHECK --interval=30s --timeout=10s --start-period=5s --retries=3 \
    CMD python -c "import sys; sys.exit(0)" || exit 1

# Expose MCP server port
EXPOSE 8001

# Default command - run MCP server
CMD ["python", "-m", "src.mcp.server"]
