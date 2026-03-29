"""
Cast API router — config, rules, device scan, and display image endpoint.

All endpoints are under /api/cast/.
The display endpoint /api/cast/display/{token} serves a JPEG to the Chromecast;
tokens are single-use and expire after 90 s (see cast.py).
"""

import asyncio
import logging
from typing import Optional

from fastapi import APIRouter, HTTPException
from fastapi.responses import Response
from pydantic import BaseModel

from db import stats_db
import cast

log = logging.getLogger(__name__)
router = APIRouter(prefix="/api/cast")


# ---------------------------------------------------------------------------
# Pydantic models
# ---------------------------------------------------------------------------

class CastConfigPayload(BaseModel):
    device_name:        str
    lan_url:            str
    display_seconds:    int = 30
    cooldown_minutes:   int = 30
    active_hours_start: str = ""   # "HH:MM" or empty = no restriction
    active_hours_end:   str = ""


class CastRulePayload(BaseModel):
    match_type:      str
    match_value:     Optional[str]   = None
    max_range_nm:    Optional[float] = None
    max_altitude_ft: Optional[int]   = None
    enabled:         bool            = True


# ---------------------------------------------------------------------------
# Config endpoints
# ---------------------------------------------------------------------------

@router.get("/config")
async def get_cast_config():
    raw = await asyncio.to_thread(stats_db.get_cast_config)
    return {
        "device_name":        raw.get("device_name", ""),
        "lan_url":            raw.get("lan_url", ""),
        "display_seconds":    int(raw.get("display_seconds", 30)),
        "cooldown_minutes":   int(raw.get("cooldown_minutes", 30)),
        "active_hours_start": raw.get("active_hours_start", ""),
        "active_hours_end":   raw.get("active_hours_end", ""),
    }


@router.put("/config")
async def put_cast_config(payload: CastConfigPayload):
    _validate_active_hours(payload.active_hours_start, payload.active_hours_end)

    fields = {
        "device_name":        payload.device_name.strip(),
        "lan_url":            payload.lan_url.strip(),
        "display_seconds":    str(max(5, payload.display_seconds)),
        "cooldown_minutes":   str(max(1, payload.cooldown_minutes)),
        "active_hours_start": payload.active_hours_start.strip(),
        "active_hours_end":   payload.active_hours_end.strip(),
    }

    def _write():
        for key, value in fields.items():
            stats_db.set_cast_config(key, value)

    await asyncio.to_thread(_write)
    cast.reset_config_cache()
    return {"ok": True}


# ---------------------------------------------------------------------------
# Rules endpoints
# ---------------------------------------------------------------------------

_VALID_MATCH_TYPES = {"icao", "military", "watchlist", "interesting", "emergency", "any"}


@router.get("/rules")
async def get_cast_rules():
    rules = await asyncio.to_thread(stats_db.get_cast_rules)
    return {"rules": rules}


@router.post("/rules")
async def add_cast_rule(payload: CastRulePayload):
    _validate_rule(payload)
    rule_id = await asyncio.to_thread(
        stats_db.add_cast_rule,
        payload.match_type,
        _normalise_value(payload.match_type, payload.match_value),
        payload.max_range_nm,
        payload.max_altitude_ft,
    )
    return {"id": rule_id}


@router.put("/rules/{rule_id}")
async def update_cast_rule(rule_id: int, payload: CastRulePayload):
    _validate_rule(payload)
    await asyncio.to_thread(
        stats_db.update_cast_rule,
        rule_id,
        payload.match_type,
        _normalise_value(payload.match_type, payload.match_value),
        payload.max_range_nm,
        payload.max_altitude_ft,
        payload.enabled,
    )
    return {"ok": True}


@router.delete("/rules/{rule_id}")
async def delete_cast_rule(rule_id: int):
    await asyncio.to_thread(stats_db.delete_cast_rule, rule_id)
    return {"ok": True}


# ---------------------------------------------------------------------------
# Device scan
# ---------------------------------------------------------------------------

@router.get("/devices")
async def scan_devices():
    """Discover Chromecast devices on the LAN. Takes up to ~8 s."""
    devices = await asyncio.to_thread(cast.discover_devices)
    return {"devices": devices}


# ---------------------------------------------------------------------------
# Display image endpoint (fetched by Chromecast)
# ---------------------------------------------------------------------------

@router.get("/display/{token}")
async def cast_display(token: str):
    """
    Single-use endpoint: consumes the token, generates and returns the JPEG.
    Returns 404 if token is unknown or expired.
    """
    aircraft = cast.consume_token(token)
    if aircraft is None:
        raise HTTPException(status_code=404, detail="Token expired or unknown")

    jpeg_bytes = await asyncio.to_thread(cast.render_display_image, aircraft)
    return Response(content=jpeg_bytes, media_type="image/jpeg")


# ---------------------------------------------------------------------------
# Validation helpers
# ---------------------------------------------------------------------------

def _validate_rule(payload: CastRulePayload) -> None:
    if payload.match_type not in _VALID_MATCH_TYPES:
        raise HTTPException(
            status_code=422,
            detail=f"match_type must be one of: {', '.join(sorted(_VALID_MATCH_TYPES))}",
        )
    if payload.match_type == "icao" and not (payload.match_value or "").strip():
        raise HTTPException(status_code=422, detail="match_value required for match_type 'icao'")
    if payload.max_range_nm is not None and payload.max_range_nm <= 0:
        raise HTTPException(status_code=422, detail="max_range_nm must be positive")
    if payload.max_altitude_ft is not None and payload.max_altitude_ft <= 0:
        raise HTTPException(status_code=422, detail="max_altitude_ft must be positive")


def _normalise_value(match_type: str, value: Optional[str]) -> Optional[str]:
    """ICAO values are stored uppercase; other types ignore match_value."""
    if match_type == "icao" and value:
        return value.strip().upper()
    return None


def _validate_active_hours(start: str, end: str) -> None:
    """Both must be empty, or both must be valid HH:MM strings."""
    if not start and not end:
        return
    if bool(start) != bool(end):
        raise HTTPException(
            status_code=422,
            detail="active_hours_start and active_hours_end must both be set or both empty",
        )
    from datetime import datetime
    for val in (start, end):
        try:
            datetime.strptime(val.strip(), "%H:%M")
        except ValueError:
            raise HTTPException(
                status_code=422,
                detail=f"Invalid time format {val!r} — expected HH:MM",
            )
