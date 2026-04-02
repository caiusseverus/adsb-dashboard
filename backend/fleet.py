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
from coverage import _load_airports
from utils import format_operator

router = APIRouter(prefix="/api/fleet")

# ---------------------------------------------------------------------------
# Manufacturer normalisation
# ---------------------------------------------------------------------------

# Raw DB manufacturer strings → canonical display name
_MFR_NAME_MAP: dict[str, str] = {
    "AIRBUS": "Airbus",
    "AIRBUS SAS": "Airbus",
    "AIRBUS INDUSTRIE": "Airbus",
    "BOEING": "Boeing",
    "BOEING CO": "Boeing",
    "THE BOEING COMPANY": "Boeing",
    "EMBRAER": "Embraer",
    "EMBRAER S.A.": "Embraer",
    "EMBRAER SA": "Embraer",
    "BOMBARDIER": "Bombardier",
    "BOMBARDIER INC": "Bombardier",
    "BOMBARDIER AEROSPACE": "Bombardier",
    "LEARJET": "Bombardier",
    "CESSNA": "Cessna",
    "CESSNA AIRCRAFT CO": "Cessna",
    "TEXTRON AVIATION": "Cessna",
    "PIPER": "Piper",
    "PIPER AIRCRAFT": "Piper",
    "PIPER AIRCRAFT INC": "Piper",
    "DASSAULT": "Dassault",
    "DASSAULT AVIATION": "Dassault",
    "DIAMOND": "Diamond",
    "DIAMOND AIRCRAFT": "Diamond",
    "DIAMOND AIRCRAFT INDUSTRIES": "Diamond",
    "PILATUS": "Pilatus",
    "PILATUS AIRCRAFT": "Pilatus",
    "PILATUS AIRCRAFT LTD": "Pilatus",
    "BEECHCRAFT": "Beechcraft",
    "BEECH": "Beechcraft",
    "HAWKER BEECHCRAFT": "Hawker Beechcraft",
    "GULFSTREAM": "Gulfstream",
    "GULFSTREAM AEROSPACE": "Gulfstream",
    "CIRRUS": "Cirrus",
    "CIRRUS DESIGN": "Cirrus",
    "CIRRUS DESIGN CORP": "Cirrus",
    "ATR": "ATR",
    "AEROSPATIALE/ATR": "ATR",
    "AEROSPATIALE": "ATR",
    "LEONARDO": "Leonardo",
    "ALENIA AERMACCHI": "Leonardo",
    "FINMECCANICA": "Leonardo",
    "DE HAVILLAND": "De Havilland",
    "VIKING AIR": "De Havilland",
    "SAAB": "Saab",
    "SAAB AB": "Saab",
    "FOKKER": "Fokker",
    "MOONEY": "Mooney",
    "MOONEY AIRCRAFT CORP": "Mooney",
}

# Type-code overrides — used when DB manufacturer is absent or mismatched
_TYPE_MFR_OVERRIDES: dict[str, str] = {
    # Boeing
    "B38M": "Boeing", "B39M": "Boeing",
    "B736": "Boeing", "B737": "Boeing", "B738": "Boeing", "B739": "Boeing",
    "B741": "Boeing", "B742": "Boeing", "B743": "Boeing", "B744": "Boeing", "B748": "Boeing",
    "B752": "Boeing", "B753": "Boeing",
    "B762": "Boeing", "B763": "Boeing", "B764": "Boeing",
    "B772": "Boeing", "B773": "Boeing", "B77L": "Boeing", "B77W": "Boeing",
    "B788": "Boeing", "B789": "Boeing", "B78X": "Boeing",
    "C17":  "Boeing",
    # Airbus
    "A318": "Airbus", "A319": "Airbus", "A320": "Airbus", "A321": "Airbus",
    "A19N": "Airbus", "A20N": "Airbus", "A21N": "Airbus",
    "A306": "Airbus", "A310": "Airbus",
    "A332": "Airbus", "A333": "Airbus", "A338": "Airbus", "A339": "Airbus",
    "A342": "Airbus", "A343": "Airbus", "A345": "Airbus", "A346": "Airbus",
    "A359": "Airbus", "A35K": "Airbus",
    "A380": "Airbus", "A388": "Airbus",
    "BCS1": "Airbus", "BCS3": "Airbus",  # A220-100/300
    # Embraer
    "E170": "Embraer", "E175": "Embraer",
    "E190": "Embraer", "E195": "Embraer",
    "E290": "Embraer", "E295": "Embraer",
    "E35L": "Embraer", "E545": "Embraer", "E550": "Embraer",
    # Bombardier
    "CL30": "Bombardier", "CL35": "Bombardier", "CL60": "Bombardier",
    "CRJ2": "Bombardier", "CRJ7": "Bombardier", "CRJ9": "Bombardier", "CRJX": "Bombardier",
    "GL5T": "Bombardier", "GLEX": "Bombardier",
    # Cessna
    "C172": "Cessna", "C182": "Cessna", "C208": "Cessna",
    "C510": "Cessna", "C525": "Cessna", "C550": "Cessna", "C56X": "Cessna",
    # Piper
    "P28A": "Piper", "P28B": "Piper", "P28R": "Piper", "PA44": "Piper",
    # Dassault
    "F900": "Dassault", "F2TH": "Dassault", "F7X": "Dassault", "F8EX": "Dassault",
    # Diamond
    "DA40": "Diamond", "DA42": "Diamond", "DA62": "Diamond",
    # Pilatus
    "PC12": "Pilatus", "PC24": "Pilatus",
    # Hawker / Beechcraft
    "H25B": "Hawker Beechcraft", "H25C": "Hawker Beechcraft",
    "BE20": "Beechcraft", "BE40": "Beechcraft", "BE58": "Beechcraft",
    # Not a manufacturer
    "GRND": "Ground Vehicle",
}


def _canonical_manufacturer(
    type_code: str,
    db_manufacturer: str | None,
    user_overrides: dict[str, str] | None = None,
) -> str:
    """Return a normalised manufacturer name for grouping purposes."""
    # User DB overrides take highest priority
    if user_overrides and type_code in user_overrides:
        return user_overrides[type_code]
    # Hardcoded type_code overrides second
    if type_code in _TYPE_MFR_OVERRIDES:
        return _TYPE_MFR_OVERRIDES[type_code]
    # Normalise DB manufacturer string
    if db_manufacturer:
        key = db_manufacturer.strip().upper()
        if key in _MFR_NAME_MAP:
            return _MFR_NAME_MAP[key]
        # Partial prefix match (e.g. "BOEING COMMERCIAL AIRPLANES")
        for raw, canonical in _MFR_NAME_MAP.items():
            if key.startswith(raw):
                return canonical
        # Unknown but non-empty: title-case it
        return db_manufacturer.strip().title()
    # No data: fall back to type_code so it groups alone
    return type_code


# ---------------------------------------------------------------------------
# Lazy ICAO → airport name lookup built from the shared airports.json
_icao_name_map: dict[str, str] | None = None


def _airport_name(icao: str) -> str | None:
    global _icao_name_map
    if _icao_name_map is None:
        _icao_name_map = {ap["icao"]: ap["name"] for ap in _load_airports() if ap.get("icao")}
    return _icao_name_map.get(icao)


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


@router.get("/top_routes")
async def fleet_top_routes(
    limit: int = Query(20, ge=1, le=100),
    since: Optional[int] = Query(None, ge=1),
) -> list[dict]:
    rows = await asyncio.to_thread(stats_db.query_fleet_top_routes, limit, _since_ts(since))
    for row in rows:
        row["origin_name"] = _airport_name(row["origin"])
        row["dest_name"]   = _airport_name(row["dest"])
    return rows


@router.get("/top_airports")
async def fleet_top_airports(
    limit: int = Query(20, ge=1, le=100),
    since: Optional[int] = Query(None, ge=1),
    direction: str = Query("origin"),
) -> list[dict]:
    if direction not in ("origin", "dest"):
        direction = "origin"
    rows = await asyncio.to_thread(stats_db.query_fleet_top_airports, limit, _since_ts(since), direction)
    for row in rows:
        row["name"] = _airport_name(row["airport"])
    return rows


@router.get("/types_table")
async def fleet_types_table(
    limit: int = Query(50, ge=1, le=200),
    offset: int = Query(0, ge=0),
    since: Optional[int] = Query(None, ge=1),
    military: Optional[int] = Query(None, ge=0, le=1),
    search: Optional[str] = Query(None, max_length=50),
    sort_col: str = Query("airframes"),
    sort_dir: str = Query("desc"),
    group_by: Optional[str] = Query(None),
) -> dict:
    ts = _since_ts(since)

    if group_by == "manufacturer":
        # Fetch all type_codes and group in Python so we can normalise manufacturer names
        raw, user_overrides = await asyncio.gather(
            asyncio.to_thread(stats_db.query_fleet_all_type_codes, ts, military, search),
            asyncio.to_thread(stats_db.get_all_type_manufacturer_overrides),
        )
        # Aggregate by canonical manufacturer
        groups: dict[str, dict] = {}
        for row in raw:
            mfr = _canonical_manufacturer(row["type_code"], row["manufacturer"], user_overrides)
            if mfr not in groups:
                groups[mfr] = {"group_key": mfr, "type_codes_set": [], "airframes": 0, "flights": 0, "last_seen": 0}
            g = groups[mfr]
            g["type_codes_set"].append(row["type_code"])
            g["airframes"] += row["airframes"]
            g["flights"]   += row["flights"]
            if row["last_seen"] and row["last_seen"] > g["last_seen"]:
                g["last_seen"] = row["last_seen"]

        all_items = []
        for g in groups.values():
            g["type_codes"] = ",".join(sorted(g["type_codes_set"]))
            del g["type_codes_set"]
            all_items.append(g)

        # Sort
        reverse = sort_dir != "asc"
        key_fn = {
            "type_code": lambda r: r["group_key"].lower(),
            "airframes":  lambda r: r["airframes"],
            "flights":    lambda r: r["flights"],
            "last_seen":  lambda r: r["last_seen"] or 0,
        }.get(sort_col, lambda r: r["airframes"])
        all_items.sort(key=key_fn, reverse=reverse)

        total = len(all_items)
        items = all_items[offset: offset + limit]

        # Enrich: WTC from first type code in group
        for row in items:
            tc = row["type_codes"].split(",")[0].strip() if row["type_codes"] else None
            ti = enrichment.db.get_type_info(tc) if tc else None
            row["type_name"] = None  # no single type name for a manufacturer group
            row["wtc"]       = ti.get("wtc") if ti else None
    else:
        items, total = await asyncio.to_thread(
            stats_db.query_fleet_types_table,
            limit, offset, ts, military, search, sort_col, sort_dir, None,
        )
        for row in items:
            tc = row["type_codes"].strip() if row["type_codes"] else None
            ti = enrichment.db.get_type_info(tc) if tc else None
            row["type_name"] = ti.get("name") if ti else None
            row["wtc"]       = ti.get("wtc")  if ti else None

    return {"total": total, "items": items}


@router.get("/type_airframes")
async def fleet_type_airframes(
    type_codes: str = Query(..., max_length=2000),
    since: Optional[int] = Query(None, ge=1),
    military: Optional[int] = Query(None, ge=0, le=1),
) -> list[dict]:
    codes = [c.strip() for c in type_codes.split(",") if c.strip()]
    rows = await asyncio.to_thread(
        stats_db.query_fleet_type_airframes, codes, _since_ts(since), military
    )
    for row in rows:
        row["operator_display"] = format_operator(row["operator"])
    return rows
