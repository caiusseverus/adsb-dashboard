"""
Debug API router — aircraft data source comparison and field override.

GET  /api/debug/aircraft/{icao}          — query all enrichment sources
POST /api/debug/aircraft/{icao}/override — override a field in aircraft_registry
"""

import asyncio
import statistics

from fastapi import APIRouter, HTTPException
from pydantic import BaseModel

from db import stats_db
import enrichment as enrichment_module
import aircraft_state as _state_module

router = APIRouter(prefix="/api/debug")

OVERRIDEABLE_FIELDS = {
    "country", "registration", "type_code", "operator",
    "military", "manufacturer", "year",
}


@router.get("/perf")
async def get_perf() -> dict:
    """Return performance timing statistics for message decode and push-updates."""
    msg_t = sorted(_state_module.msg_timings)  # copy of deque as sorted list
    push_t = list(_state_module.push_timings)

    def percentiles(data: list[float], scale: float = 1_000_000) -> dict:
        n = len(data)
        if not n:
            return {"samples": 0, "p50": 0, "p95": 0, "p99": 0, "max": 0, "mean": 0}
        return {
            "samples": n,
            "p50":  round(data[int(n * 0.50)] * scale, 1),
            "p95":  round(data[int(n * 0.95)] * scale, 1),
            "p99":  round(data[int(n * 0.99)] * scale, 1),
            "max":  round(data[-1] * scale, 1),
            "mean": round(statistics.mean(data) * scale, 1),
        }

    def push_avg(key: str) -> float:
        if not push_t:
            return 0.0
        return round(sum(p[key] for p in push_t) / len(push_t), 2)

    return {
        # Per-message decode time in microseconds
        "msg_decode_us": percentiles(msg_t, scale=1_000_000),
        # Per-_push_updates invocation in milliseconds
        "push_updates_ms": {
            "samples":       len(push_t),
            "loop_avg":      push_avg("loop_ms"),       # notify + track recording
            "broadcast_avg": push_avg("broadcast_ms"),  # websocket send
            "total_avg":     push_avg("total_ms"),
            "ac_count_avg":  push_avg("ac_count"),
        },
    }


@router.get("/aircraft/{icao}")
async def debug_aircraft(icao: str) -> dict:
    """Return data from every enrichment source for a given ICAO address."""
    icao_upper = icao.upper().strip()
    if len(icao_upper) != 6 or not all(c in "0123456789ABCDEF" for c in icao_upper):
        raise HTTPException(400, "ICAO must be a 6-character hex address")

    db = enrichment_module.db

    # Run blocking I/O in threads
    hexdb_data, tar1090_data, registry = await asyncio.gather(
        asyncio.to_thread(db.force_lookup_hexdb, icao_upper),
        asyncio.to_thread(db.get_tar1090, icao_upper),
        asyncio.to_thread(stats_db.get_aircraft_registry_entry, icao_upper),
    )

    adsbx = db.get_adsbx(icao_upper)
    country_from_icao = db.get_country_by_icao(icao_upper)

    return {
        "icao": icao_upper,
        "icao_block": {
            "country": country_from_icao,
        },
        "adsbexchange": adsbx or {},
        "hexdb": hexdb_data or {},
        "tar1090": tar1090_data or {},
        "registry": registry or {},
    }


class OverrideBody(BaseModel):
    field: str
    value: str | int | None


@router.post("/aircraft/{icao}/override")
async def override_aircraft_field(icao: str, body: OverrideBody) -> dict:
    """Write a manual override for one field into aircraft_registry."""
    icao_upper = icao.upper().strip()
    if body.field not in OVERRIDEABLE_FIELDS:
        raise HTTPException(400, f"Field '{body.field}' cannot be overridden. "
                            f"Allowed: {sorted(OVERRIDEABLE_FIELDS)}")
    # Coerce military to int (0/1)
    value = body.value
    if body.field == "military":
        value = 1 if value else 0
    await asyncio.to_thread(stats_db.update_aircraft_field, icao_upper, body.field, value)
    return {"ok": True, "icao": icao_upper, "field": body.field, "value": value}
