"""
Fleet analysis API router.
All endpoints query aircraft_registry for aggregate statistics over all observed aircraft.
"""

import asyncio
from datetime import datetime, timedelta, timezone
from typing import Optional
from fastapi import APIRouter, Query

from db import stats_db
import enrichment
from utils import format_operator

router = APIRouter(prefix="/api/fleet")


def _since_ts(since_days: int | None) -> int | None:
    if since_days is None:
        return None
    return int((datetime.now(timezone.utc) - timedelta(days=since_days)).timestamp())


@router.get("/summary")
async def fleet_summary(since: Optional[int] = Query(None, ge=1)) -> dict:
    return await asyncio.to_thread(stats_db.query_fleet_summary, _since_ts(since))


@router.get("/types")
async def fleet_types(
    limit: int = Query(20, ge=1, le=100),
    military: Optional[int] = Query(None, ge=0, le=1),
    since: Optional[int] = Query(None, ge=1),
) -> list[dict]:
    rows = await asyncio.to_thread(stats_db.query_fleet_types, limit, military, _since_ts(since))
    for row in rows:
        tc = row["type_code"]
        ti = enrichment.db.get_type_info(tc)
        row["type_name"]     = ti.get("name") if ti else None
        row["type_category"] = ti.get("desc") if ti else None
        row["wtc"]           = ti.get("wtc")  if ti else None
    return rows


@router.get("/operators")
async def fleet_operators(
    limit: int = Query(20, ge=1, le=100),
    since: Optional[int] = Query(None, ge=1),
    military: Optional[int] = Query(None, ge=0, le=1),
) -> list[dict]:
    rows = await asyncio.to_thread(stats_db.query_fleet_operators, limit, _since_ts(since), military)
    for row in rows:
        row["operator_display"] = format_operator(row["operator"])
    return rows


@router.get("/countries")
async def fleet_countries(
    limit: int = Query(25, ge=1, le=100),
    military: Optional[int] = Query(None, ge=0, le=1),
    since: Optional[int] = Query(None, ge=1),
) -> list[dict]:
    return await asyncio.to_thread(stats_db.query_fleet_countries, limit, military, _since_ts(since))


@router.get("/categories")
async def fleet_categories(
    military: Optional[int] = Query(None, ge=0, le=1),
    since: Optional[int] = Query(None, ge=1),
) -> list[dict]:
    return await asyncio.to_thread(stats_db.query_fleet_categories, military, _since_ts(since))


@router.get("/ages")
async def fleet_ages(since: Optional[int] = Query(None, ge=1)) -> list[dict]:
    return await asyncio.to_thread(stats_db.query_fleet_ages, _since_ts(since))


@router.get("/top_aircraft")
async def fleet_top_aircraft(
    limit: int = Query(20, ge=1, le=100),
    since: Optional[int] = Query(None, ge=1),
    military: Optional[int] = Query(None, ge=0, le=1),
) -> list[dict]:
    rows = await asyncio.to_thread(stats_db.query_top_aircraft, limit, _since_ts(since), military)
    for row in rows:
        row["operator_display"] = format_operator(row["operator"])
    return rows
