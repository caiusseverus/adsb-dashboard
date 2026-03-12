"""
Coverage API router.

GET /api/coverage/polar?days=30       — scatter points for polar plot
GET /api/coverage/max_range?days=30   — max range per bearing sector
GET /api/coverage/coastline           — coastlines + borders projected to bearing/range
"""

import asyncio
import json
import math
from pathlib import Path

from fastapi import APIRouter, HTTPException, Query
from db import stats_db

# ── Coastline helpers ────────────────────────────────────────────────────────

_COASTLINE_PATH = Path(__file__).parent / "data" / "coastline.json"
_coastline_cache: list | None = None


def _load_coastline() -> list:
    global _coastline_cache
    if _coastline_cache is None:
        if _COASTLINE_PATH.exists():
            with open(_COASTLINE_PATH) as f:
                _coastline_cache = json.load(f)
        else:
            _coastline_cache = []
    return _coastline_cache


def _haversine_nm(lat1: float, lon1: float, lat2: float, lon2: float) -> float:
    R = 3440.065  # Earth radius in nautical miles
    phi1, phi2 = math.radians(lat1), math.radians(lat2)
    dphi = math.radians(lat2 - lat1)
    dlam = math.radians(lon2 - lon1)
    a = math.sin(dphi / 2) ** 2 + math.cos(phi1) * math.cos(phi2) * math.sin(dlam / 2) ** 2
    return 2 * R * math.asin(math.sqrt(min(1.0, a)))


def _bearing_deg(lat1: float, lon1: float, lat2: float, lon2: float) -> float:
    phi1, phi2 = math.radians(lat1), math.radians(lat2)
    dlam = math.radians(lon2 - lon1)
    x = math.sin(dlam) * math.cos(phi2)
    y = math.cos(phi1) * math.sin(phi2) - math.sin(phi1) * math.cos(phi2) * math.cos(dlam)
    return (math.degrees(math.atan2(x, y)) + 360) % 360


def _project_coastline(range_nm: float) -> dict:
    import config
    if config.RECEIVER_LAT is None or config.RECEIVER_LON is None:
        return {"segments": []}
    rlat, rlon = config.RECEIVER_LAT, config.RECEIVER_LON
    segments = []
    for line in _load_coastline():
        projected = [
            (_bearing_deg(rlat, rlon, lat, lon), _haversine_nm(rlat, rlon, lat, lon))
            for lat, lon in line
        ]
        for i in range(len(projected) - 1):
            b1, r1 = projected[i]
            b2, r2 = projected[i + 1]
            # Keep segment if at least one endpoint is within range
            if r1 > range_nm and r2 > range_nm:
                continue
            segments.append([round(b1, 1), round(r1, 1), round(b2, 1), round(r2, 1)])
    return {"segments": segments}


# ── Airport helpers ──────────────────────────────────────────────────────────

_AIRPORTS_PATH = Path(__file__).parent / "data" / "airports.json"
_airports_cache: list | None = None


def _load_airports() -> list:
    global _airports_cache
    if _airports_cache is None:
        if _AIRPORTS_PATH.exists():
            with open(_AIRPORTS_PATH) as f:
                _airports_cache = json.load(f)
        else:
            _airports_cache = []
    return _airports_cache


def _project_airports(range_nm: float, types: str) -> dict:
    import config
    if config.RECEIVER_LAT is None or config.RECEIVER_LON is None:
        return {"airports": []}
    rlat, rlon = config.RECEIVER_LAT, config.RECEIVER_LON
    allowed = set(t.strip() for t in types.split(","))
    result = []
    for ap in _load_airports():
        if ap["type"] not in allowed:
            continue
        r = _haversine_nm(rlat, rlon, ap["lat"], ap["lon"])
        if r > range_nm:
            continue
        b = _bearing_deg(rlat, rlon, ap["lat"], ap["lon"])
        result.append({
            "name":     ap["name"],
            "iata":     ap["iata"],
            "icao":     ap["icao"],
            "bearing":  round(b, 1),
            "range_nm": round(r, 1),
            "type":     ap["type"],
        })
    return {"airports": result}


router = APIRouter(prefix="/api/coverage")


@router.get("/polar")
async def coverage_polar(days: int = Query(default=30, ge=1, le=90)) -> list[dict]:
    return await asyncio.to_thread(stats_db.query_polar, days)


@router.get("/polar_bins")
async def coverage_polar_bins(
    days: int = Query(default=30, ge=1, le=90),
    sectors: int = Query(default=32, ge=8, le=360),
) -> dict:
    return await asyncio.to_thread(stats_db.query_polar_bins, days, sectors)



@router.get("/max_range")
async def coverage_max_range(days: int = Query(default=30, ge=1, le=90)) -> list[dict]:
    return await asyncio.to_thread(stats_db.query_max_range_by_bearing, days)


@router.get("/range_percentiles")
async def coverage_range_percentiles(days: int = Query(default=30, ge=1, le=90)) -> list[dict]:
    return await asyncio.to_thread(stats_db.query_range_percentiles, days)


@router.get("/azimuth_elevation")
async def coverage_azimuth_elevation(days: int = Query(default=30, ge=1, le=90)) -> list[dict]:
    return await asyncio.to_thread(stats_db.query_azimuth_elevation, days)


@router.get("/range_trend")
async def coverage_range_trend(days: int = Query(default=90, ge=7, le=365)) -> list[dict]:
    """Daily max and mean range, for the receiver page trend chart."""
    return await asyncio.to_thread(stats_db.query_coverage_range_trend, days)


@router.get("/flow")
async def coverage_flow(
    days:     int   = Query(default=7,    ge=1,   le=30),
    grid_deg: float = Query(default=0.05, ge=0.01, le=0.5),
) -> dict:
    """Grid-binned traffic density map.
    Reconstructs lat/lon from bearing_deg + range_nm + receiver coords.
    Returns {cells: [[lat, lon, count], ...], max_count, grid_deg,
             receiver_lat, receiver_lon}"""
    return await asyncio.to_thread(stats_db.query_coverage_flow, days, grid_deg)


@router.get("/points")
async def coverage_points(
    days:       int = Query(default=30,     ge=1,    le=365),
    max_points: int = Query(default=100000, ge=10000, le=500000),
) -> dict:
    """Downsampled coverage points for the 3-D coverage view.
    Returns flat list-of-lists [bearing_deg, range_nm, altitude_ft, military, interesting]
    to minimise payload size."""
    return await asyncio.to_thread(stats_db.query_coverage_points, days, max_points)


@router.get("/coastline")
async def coverage_coastline(
    range_nm: float = Query(default=400, ge=100, le=1000),
) -> dict:
    """Coastline + country borders projected to bearing/range from receiver.
    Returns {segments: [[bearing1, range1, bearing2, range2], ...]}
    Returns empty segments if receiver location is not configured or data file is missing."""
    return await asyncio.to_thread(_project_coastline, range_nm)


@router.get("/airports")
async def coverage_airports(
    range_nm: float = Query(default=400, ge=50,  le=1000),
    types:    str   = Query(default="large_airport"),
) -> dict:
    """Airports projected to bearing/range from receiver.
    types: comma-separated — large_airport and/or medium_airport"""
    return await asyncio.to_thread(_project_airports, range_nm, types)


@router.get("/timelapse_hires")
async def coverage_timelapse_hires(
    start_ts: int = Query(..., description="Unix timestamp — window start"),
    end_ts:   int = Query(..., description="Unix timestamp — window end"),
) -> dict:
    """High-resolution timelapse from in-memory buffer (10-second samples, up to 24 hours).
    Falls back gracefully to sparse data if the server recently restarted.
    Same response format as /timelapse."""
    import hires_buffer
    if end_ts - start_ts > 90_000:
        raise HTTPException(status_code=400, detail="Window exceeds 25 hours")
    if end_ts - start_ts < 300:
        raise HTTPException(status_code=400, detail="Window too short (min 5 min)")
    return await asyncio.to_thread(hires_buffer.query_tracks, start_ts, end_ts)


@router.get("/timelapse")
async def coverage_timelapse(
    start_ts: int = Query(..., description="Unix timestamp — window start"),
    end_ts:   int = Query(..., description="Unix timestamp — window end"),
) -> dict:
    """Per-aircraft position tracks for the timelapse player.
    Maximum window: 25 hours.  Minimum: 5 minutes."""
    if end_ts - start_ts > 90_000:
        raise HTTPException(status_code=400, detail="Window exceeds 25 hours")
    if end_ts - start_ts < 300:
        raise HTTPException(status_code=400, detail="Window too short (min 5 min)")
    return await asyncio.to_thread(stats_db.query_timelapse_tracks, start_ts, end_ts)
