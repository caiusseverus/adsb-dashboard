"""
Emergency squawk events router.

GET /api/squawks/events?days=30  — recent 7700/7600/7500 events
"""
import asyncio
from fastapi import APIRouter, Query
from db import stats_db

router = APIRouter(prefix="/api/squawks")


@router.get("/events")
async def squawk_events(
    days:  int = Query(default=30, ge=1, le=90),
    limit: int = Query(default=500, ge=1, le=2000),
) -> list[dict]:
    return await asyncio.to_thread(stats_db.query_squawk_events, days, limit)
