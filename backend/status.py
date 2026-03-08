"""
Status API — database size, table row counts, retention info.
"""

import asyncio
from fastapi import APIRouter

from db import stats_db

router = APIRouter(prefix="/api/status")


@router.get("")
async def get_status() -> dict:
    return await asyncio.to_thread(stats_db.query_status)
