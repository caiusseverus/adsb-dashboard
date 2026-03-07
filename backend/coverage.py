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
async def coverage_polar_bins(days: int = Query(default=30, ge=1, le=90)) -> dict:
    return await asyncio.to_thread(stats_db.query_polar_bins, days)


@router.get("/max_range")
async def coverage_max_range(days: int = Query(default=30, ge=1, le=90)) -> list[dict]:
    return await asyncio.to_thread(stats_db.query_max_range_by_bearing, days)


@router.get("/range_percentiles")
async def coverage_range_percentiles(days: int = Query(default=30, ge=1, le=90)) -> list[dict]:
    return await asyncio.to_thread(stats_db.query_range_percentiles, days)


@router.get("/azimuth_elevation")
async def coverage_azimuth_elevation(days: int = Query(default=30, ge=1, le=90)) -> list[dict]:
    return await asyncio.to_thread(stats_db.query_azimuth_elevation, days)
