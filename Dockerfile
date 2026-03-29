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
FROM python:3.12-slim AS runtime

# Install uv (pinned for reproducibility; update as needed)
COPY --from=ghcr.io/astral-sh/uv:latest /uv /usr/local/bin/uv

WORKDIR /app

# Keep uv aligned with the image interpreter; avoid auto-selecting newer
# managed Python versions (e.g. 3.14) that can break Cython extension builds.
ENV UV_PYTHON=3.12

# Copy dependency manifests first so this layer is cached when code changes
COPY backend/pyproject.toml backend/uv.lock ./backend/

# Install Python dependencies into the project venv (no editable install)
RUN uv sync --directory backend --no-dev --frozen --python 3.12

# Copy the full backend source and seed scripts
COPY backend/ ./backend/
COPY tools/ ./tools/

# Build native extensions:
#   1. pyModeS Cython extension (c_common.pyx) — speeds up hot decode path
#   2. libdecode.so — readsb-derived C decode library used by decode_cffi.py
# build-essential is purged afterwards to keep the image slim.
RUN apt-get update -qq \
    && apt-get install -y --no-install-recommends build-essential \
    && bash backend/build_pymodes_cython.sh \
    && make -C backend/native \
    && make -C backend/native install \
    && make -C backend/native clean \
    && rm -rf /var/lib/apt/lists/*

# Copy the built frontend so the backend can serve it as static files
COPY --from=frontend-build /build/frontend/dist ./frontend/dist

# Fetch airports and coastline data into /app/static_data/ (outside the volume).
# The entrypoint copies them into /app/backend/data/ at startup if missing,
# so they are available on both fresh volumes and existing volumes after an update.
RUN python3 tools/fetch_airports.py && python3 tools/fetch_coastline.py \
    && mkdir -p /app/static_data \
    && mv /app/backend/data/airports.json /app/static_data/ \
    && mv /app/backend/data/coastline.json /app/static_data/

COPY docker-entrypoint.sh /app/docker-entrypoint.sh

RUN useradd --create-home --shell /bin/false adsb \
    && mkdir -p /app/backend/data \
    && chmod +x /app/docker-entrypoint.sh \
    && chown -R adsb:adsb /app

# Persistent data lives in a volume so it survives container restarts
VOLUME ["/app/backend/data"]

EXPOSE 8000

USER adsb

WORKDIR /app/backend
ENTRYPOINT ["/app/docker-entrypoint.sh"]
