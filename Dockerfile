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

# Copy the backend source and seed scripts
COPY backend/ ./backend/
COPY tools/ ./tools/

# Copy the built frontend so the backend can serve it as static files
COPY --from=frontend-build /build/frontend/dist ./frontend/dist

# Fetch static data files (airports + coastline) and bake them into the image.
# Must run before VOLUME is declared so the files are preserved in the image layer
# and copied into the named volume on first container initialisation.
RUN python3 tools/fetch_airports.py && python3 tools/fetch_coastline.py

# Create the data directory before declaring the VOLUME so Docker initialises
# the named volume with the correct ownership (not root).
RUN useradd --create-home --shell /bin/false adsb \
    && chown -R adsb:adsb /app

# Persistent data lives in a volume so it survives container restarts
VOLUME ["/app/backend/data"]

EXPOSE 8000

USER adsb

# Run from backend/ so relative paths (../frontend/dist, data/) resolve correctly.
# HOST_PORT controls the port uvicorn binds to (default 8000); this allows the
# port to be changed via environment variable when using network_mode: host.
WORKDIR /app/backend
CMD ["/bin/sh", "-c", ".venv/bin/uvicorn main:app --host 0.0.0.0 --port ${HOST_PORT:-8000}"]
