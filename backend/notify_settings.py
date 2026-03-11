"""
Notification settings API.

GET  /api/notify/prefs              — all prefs
POST /api/notify/prefs              — set one pref {key, value}
GET  /api/notify/watchlist          — full watchlist
GET  /api/notify/watchlist/{icao}   — is this ICAO watched?
POST /api/notify/watchlist/{icao}   — add to watchlist {label?, max_range_nm?}
DELETE /api/notify/watchlist/{icao} — remove from watchlist
POST /api/notify/backup             — trigger manual backup now
"""
import asyncio
from fastapi import APIRouter, HTTPException
from pydantic import BaseModel
from db import stats_db
import config

router = APIRouter(prefix="/api/notify")


@router.get("/prefs")
async def get_prefs() -> dict:
    return await asyncio.to_thread(stats_db.get_notify_prefs)


class PrefUpdate(BaseModel):
    key: str
    value: str

@router.post("/prefs")
async def set_pref(body: PrefUpdate) -> dict:
    allowed = {
        "notify_emergency", "notify_acas", "notify_military", "notify_interesting",
        "military_max_range_nm", "interesting_max_range_nm", "acas_max_range_nm",
        "backup_path", "backup_retain",
    }
    if body.key not in allowed:
        raise HTTPException(400, f"Unknown pref key: {body.key}")
    await asyncio.to_thread(stats_db.set_notify_pref, body.key, body.value)
    return {"ok": True}


@router.get("/watchlist")
async def get_watchlist() -> list[dict]:
    return await asyncio.to_thread(stats_db.get_notify_watchlist)


@router.get("/watchlist/{icao}")
async def get_watched(icao: str) -> dict:
    icao = icao.upper()
    watched = await asyncio.to_thread(stats_db.is_watched, icao)
    return {"icao": icao, "watched": watched}


class WatchBody(BaseModel):
    label: str | None = None
    max_range_nm: float | None = None

@router.post("/watchlist/{icao}")
async def add_watch(icao: str, body: WatchBody = WatchBody()) -> dict:
    icao = icao.upper()
    await asyncio.to_thread(stats_db.add_to_watchlist, icao, body.label, body.max_range_nm)
    return {"ok": True, "icao": icao}


@router.delete("/watchlist/{icao}")
async def remove_watch(icao: str) -> dict:
    icao = icao.upper()
    await asyncio.to_thread(stats_db.remove_from_watchlist, icao)
    return {"ok": True, "icao": icao}


@router.post("/backup")
async def trigger_backup() -> dict:
    backup_path, _ = await asyncio.to_thread(stats_db.get_effective_backup_config)
    if not backup_path:
        raise HTTPException(400, "Backup path not configured")
    path = await asyncio.to_thread(stats_db.backup, backup_path)
    return {"ok": True, "path": str(path)}
