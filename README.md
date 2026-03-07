# ADS-B Dashboard

A real-time web dashboard for monitoring ADS-B receiver performance. Connects to a Mode-S Beast TCP stream (from readsb or dump1090-fa), decodes aircraft messages, and presents live and historical data through a React frontend.

## Features

- **Live tab** — real-time aircraft table with signal strength, altitude, speed, heading, callsign, and message rates
- **History tab** — traffic heatmaps by hour of day, calendar heatmap, message type analysis, notable sightings (military, rare, unique)
- **Receiver tab** — polar coverage chart, azimuth/elevation scatter, range percentiles, signal statistics
- **Fleet tab** — aircraft registry analysis: types, operators, countries, manufacture years, most-seen individual aircraft

## Prerequisites

- A working ADS-B receiver running **readsb** or **dump1090-fa** with Beast TCP output (default port 30005)
- **Python 3.10+** with [uv](https://docs.astral.sh/uv/getting-started/installation/) (`pip install uv`)
- **Node.js 18+** with npm

## Quick Start

```bash
git clone https://github.com/caiusseverus/adsb-dashboard
cd adsb-dashboard

# 1. Configure
cp backend/.env.example backend/.env
# Edit backend/.env — set RECEIVER_LAT, RECEIVER_LON and HOME_COUNTRY at minimum

# 2. Build the frontend
cd frontend && npm install && npm run build && cd ..

# 3. Run
uv run --directory backend uvicorn main:app --host 0.0.0.0 --port 8000
```

Open **http://localhost:8000** in your browser. `uv` automatically creates a virtual environment and installs all Python dependencies on first run.

## Configuration

All settings are in `backend/.env` (copy from `backend/.env.example`).

| Variable | Default | Description |
|---|---|---|
| `BEAST_HOST` | `localhost` | Hostname/IP of your receiver |
| `BEAST_PORT` | `30005` | Beast TCP port |
| `RECEIVER_LAT` | — | Receiver latitude (enables range/bearing/polar chart) |
| `RECEIVER_LON` | — | Receiver longitude |
| `HOME_COUNTRY` | — | Your country — aircraft from other countries are flagged "foreign military" |
| `AIRCRAFT_TIMEOUT` | `60` | Seconds before removing an aircraft from the live table |
| `RARE_THRESHOLD` | `5` | Sighting count below which an aircraft type is flagged "rare" |
| `GHOST_FILTER_MSGS` | `5` | Min messages before writing an aircraft to the registry (filters CRC errors) |
| `MINUTE_STATS_RETENTION_DAYS` | `30` | Days of per-minute data to keep (daily rollups kept forever) |
| `DB_PATH` | `data/adsb.db` | SQLite database path (relative to `backend/`) |
| `DEBUG_ENRICHMENT` | — | Set `true` for verbose hexdb/ADSBExchange lookup logging |

`RECEIVER_LAT` and `RECEIVER_LON` are optional but strongly recommended — without them the polar coverage chart, range statistics, and azimuth/elevation plot are unavailable.

## Development

Run the backend and frontend separately for hot reload on both sides:

```bash
# Terminal 1 — backend (auto-reloads on code changes)
uv run --directory backend uvicorn main:app --reload

# Terminal 2 — frontend dev server (proxies /ws and /api to backend)
cd frontend && npm run dev
```

Frontend dev server runs on **http://localhost:5173**.

## Architecture

```
Beast TCP stream → beast_client.py (binary frame decode)
  → aircraft_state.py (pyModeS decode, in-memory state)
    → main.py (FastAPI WebSocket /ws, broadcasts JSON every 1s)
      → React frontend (App.jsx → tabs → components)
```

- **Backend** — FastAPI + SQLite (WAL mode). Aircraft enrichment from ADSBExchange (bulk, cached) and hexdb.io + tar1090-db (on-demand, cached). All DB calls run via `asyncio.to_thread`.
- **Frontend** — React + Recharts, CSS Modules, no Tailwind. Vite proxy handles WebSocket and API routing in dev.
- **Database** — SQLite at `backend/data/adsb.db`. Per-minute stats, per-aircraft registry, coverage samples, DF message counts. Created automatically on first run.

## Production Deployment

The backend serves the built frontend as static files. A simple systemd unit:

```ini
[Unit]
Description=ADS-B Dashboard
After=network.target

[Service]
Type=simple
WorkingDirectory=/path/to/adsb-dashboard
ExecStart=uv run --directory backend uvicorn main:app --host 0.0.0.0 --port 8000
Restart=on-failure
RestartSec=5

[Install]
WantedBy=multi-user.target
```

Rebuild the frontend after pulling updates:

```bash
cd frontend && npm run build
```
