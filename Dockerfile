# ---------------------------------------------------------------------------
# Stage 1 — build the React frontend
# ---------------------------------------------------------------------------
FROM node:20-slim AS frontend-build

WORKDIR /build/frontend

COPY frontend/package.json frontend/package-lock.json ./
RUN npm ci --ignore-scripts

COPY frontend/ ./
RUN npm run build

# ---------------------------------------------------------------------------
# Stage 2 — Python runtime with the backend + built frontend
# ---------------------------------------------------------------------------
FROM python:3.10-slim AS runtime

# Install uv (pinned for reproducibility; update as needed)
COPY --from=ghcr.io/astral-sh/uv:latest /uv /usr/local/bin/uv

WORKDIR /app

# Copy dependency manifests first so this layer is cached when code changes
COPY backend/pyproject.toml backend/uv.lock ./backend/

# Install Python dependencies into the project venv (no editable install)
RUN uv sync --directory backend --no-dev --frozen

# Copy the backend source
COPY backend/ ./backend/

# Copy the built frontend so the backend can serve it as static files
COPY --from=frontend-build /build/frontend/dist ./frontend/dist

# Persistent data lives in a volume so it survives container restarts
VOLUME ["/app/backend/data"]

EXPOSE 8000

# Run as a non-root user
RUN useradd --no-create-home --shell /bin/false adsb
USER adsb

CMD ["uv", "run", "--directory", "backend", "uvicorn", "main:app", "--host", "0.0.0.0", "--port", "8000"]
