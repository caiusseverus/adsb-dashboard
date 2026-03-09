"""
Debug API router — aircraft data source comparison and field override.

GET  /api/debug/aircraft/{icao}          — query all enrichment sources
POST /api/debug/aircraft/{icao}/override — override a field in aircraft_registry
"""

import asyncio

from fastapi import APIRouter, HTTPException
from pydantic import BaseModel

from db import stats_db
import enrichment as enrichment_module

router = APIRouter(prefix="/api/debug")

OVERRIDEABLE_FIELDS = {
    "country", "registration", "type_code", "operator",
    "military", "manufacturer", "year",
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
