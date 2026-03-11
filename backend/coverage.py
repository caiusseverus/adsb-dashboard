"""
Coverage API router.

GET /api/coverage/polar?days=30       — scatter points for polar plot
GET /api/coverage/max_range?days=30   — max range per bearing sector
"""

import asyncio
from fastapi import APIRouter, Query
from db import stats_db

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
    days:       int = Query(default=30,     ge=1,    le=90),
    max_points: int = Query(default=100000, ge=10000, le=500000),
) -> dict:
    """Downsampled coverage points for the 3-D coverage view.
    Returns flat list-of-lists [bearing_deg, range_nm, altitude_ft, military, interesting]
    to minimise payload size."""
    return await asyncio.to_thread(stats_db.query_coverage_points, days, max_points)
