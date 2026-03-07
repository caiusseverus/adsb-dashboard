"""
Aircraft detail API router.

GET /api/aircraft/{icao}      — merged registry + live state + enrichment snapshot
GET /api/aircraft/{icao}/route — proxied hexdb.io route lookup with in-memory cache
"""

import asyncio
import json
import logging
import time
import urllib.request
import urllib.error

from fastapi import APIRouter, HTTPException, Query

import enrichment
from db import stats_db
from utils import country_from_registration

log = logging.getLogger(__name__)

router = APIRouter(prefix="/api/aircraft")

# In-memory route cache: callsign → (fetched_ts, result_dict | None)
_route_cache: dict[str, tuple[float, dict | None]] = {}
_ROUTE_CACHE_TTL = 3600  # 1 hour


# ---------------------------------------------------------------------------
# Aircraft detail
# ---------------------------------------------------------------------------

@router.get("/{icao}")
async def aircraft_detail(icao: str) -> dict:
    icao = icao.upper()

    # Registry row
    row = await asyncio.to_thread(stats_db.get_aircraft, icao)

    # Live state injected by caller (main.py sets state on the router)
    live = None
    if hasattr(router, "_state"):
        snap = router._state.get_aircraft_live(icao)
        if snap:
            live = snap

    # Enrichment data
    adsbx = enrichment.db.get_adsbx(icao)
    hexdb = enrichment.db.get_hexdb_cached(icao)
    country = enrichment.db.get_country_by_icao(icao)
    military = enrichment.db.is_military(icao)

    if row is None and live is None and adsbx is None and hexdb is None:
        raise HTTPException(404, "Aircraft not found")

    # Build response — merge all sources, preferring live > registry > enrichment
    result: dict = {"icao": icao}

    # --- Registration ---
    result["registration"] = (
        (live and live.get("registration"))
        or (row and row.get("registration"))
        or (adsbx and adsbx.get("reg"))
        or (hexdb and hexdb.get("Registration"))
        or None
    )

    # --- Type ---
    type_code = (
        (live and live.get("type_code"))
        or (row and row.get("type_code"))
        or (adsbx and adsbx.get("icaotype"))
        or (hexdb and hexdb.get("ICAOTypeCode"))
        or None
    )
    result["type_code"] = type_code

    type_info = enrichment.db.get_type_info(type_code) if type_code else None
    result["type_full_name"] = (live and live.get("type_full_name")) or (type_info and type_info.get("name")) or None
    result["type_category"] = (
        (live and live.get("type_category"))
        or (row and row.get("type_category"))
        or (type_info and type_info.get("desc"))
        or None
    )
    result["wtc"] = (live and live.get("wtc")) or (type_info and type_info.get("wtc")) or None

    # --- Manufacturer + model (type_desc) ---
    mfr   = (adsbx and adsbx.get("manufacturer")) or (hexdb and hexdb.get("Manufacturer")) or ""
    model = (adsbx and adsbx.get("model")) or (hexdb and hexdb.get("Type")) or ""
    result["type_desc"] = (live and live.get("type_desc")) or (f"{mfr} {model}".strip()) or None

    # --- Operator / owner ---
    result["operator"] = (
        (live and live.get("operator"))
        or (adsbx and adsbx.get("ownop"))
        or (hexdb and hexdb.get("RegisteredOwners"))
        or None
    )

    # --- Country + military ---
    # Prefer registration-derived country (most accurate for GA), fall back to ICAO block
    result["country"] = (
        country_from_registration(result.get("registration"))
        or (live and live.get("country"))
        or (row and row.get("country"))
        or country
        or None
    )
    result["military"] = bool(
        (live and live.get("military"))
        or (row and row.get("military"))
        or military
    )

    # --- Year ---
    result["year"] = (live and live.get("year")) or (adsbx and adsbx.get("year")) or None

    # --- Registry history ---
    if row:
        result["history"] = {
            "first_seen":      row["first_seen"],
            "last_seen":       row["last_seen"],
            "sighting_count":  row["sighting_count"],
            "foreign_military": bool(row["foreign_military"]),
            "interesting":     bool(row["interesting"]),
            "rare":            bool(row["rare"]),
            "first_seen_flag": bool(row["first_seen_flag"]),
        }
    else:
        result["history"] = None

    # --- Live state ---
    if live:
        result["live"] = {
            "callsign":          live.get("callsign"),
            "altitude":          live.get("altitude"),
            "squawk":            live.get("squawk"),
            "signal":            live.get("signal"),
            "msg_count":         live.get("msg_count"),
            "age":               live.get("age"),
            "lat":               live.get("lat"),
            "lon":               live.get("lon"),
            "range_nm":          live.get("range_nm"),
            "airspeed_kts":      live.get("airspeed_kts"),
            "airspeed_type":     live.get("airspeed_type"),
            "heading_deg":       live.get("heading_deg"),
            "vertical_rate_fpm": live.get("vertical_rate_fpm"),
            "mach":              live.get("mach"),
            "selected_alt":      live.get("selected_alt"),
        }
    else:
        result["live"] = None

    return result


# ---------------------------------------------------------------------------
# Route lookup (proxied hexdb.io)
# ---------------------------------------------------------------------------

def _get_json(url: str) -> dict | None:
    """Synchronous JSON fetch — run via asyncio.to_thread."""
    try:
        req = urllib.request.Request(url, headers={"User-Agent": "adsb-dashboard/1.0"})
        with urllib.request.urlopen(req, timeout=5) as resp:
            if resp.status == 200:
                return json.loads(resp.read().decode())
    except Exception as exc:
        log.debug("GET %s failed: %s", url, exc)
    return None


def _fetch_route_blocking(callsign: str) -> dict | None:
    # hexdb response: {"flight": "AIC117", "route": "VIAR-EGBB", "updatetime": ...}
    data = _get_json(f"https://hexdb.io/api/v1/route/icao/{callsign}")
    if not data or not data.get("route"):
        return None

    parts = data["route"].split("-", 1)
    if len(parts) != 2:
        return None
    origin_icao, dest_icao = parts[0].strip(), parts[1].strip()

    origin_info = _get_json(f"https://hexdb.io/api/v1/airport/icao/{origin_icao}") if origin_icao else None
    dest_info   = _get_json(f"https://hexdb.io/api/v1/airport/icao/{dest_icao}")   if dest_icao   else None

    return {
        "flight":      data.get("flight") or callsign,
        "origin":      {"icao": origin_icao, "info": origin_info},
        "destination": {"icao": dest_icao,   "info": dest_info},
    }


@router.post("/{icao}/refresh")
async def aircraft_refresh(icao: str) -> dict:
    """Force a fresh hexdb lookup, merge with ADSBex, write all resolved fields to
    the registry (hexdb takes priority; ADSBex fills gaps), return updated record."""
    icao = icao.upper()

    # Always make a fresh hexdb HTTP request — updates the persistent cache too
    hexdb = await asyncio.to_thread(enrichment.db.force_lookup_hexdb, icao)
    # Always try tar1090-db as supplementary source — hexdb may have registration but no type code
    tar1090 = await asyncio.to_thread(enrichment.db.get_tar1090, icao)
    adsbx = enrichment.db.get_adsbx(icao)

    if hexdb or tar1090 or adsbx:
        # Priority: hexdb > tar1090 > adsbx
        # Registration: hexdb is the live register; tar1090 then adsbx as fallbacks
        registration = (
            (hexdb and (hexdb.get("Registration") or "").strip() or None)
            or (tar1090 and (tar1090.get("Registration") or "").strip() or None)
            or (adsbx and adsbx.get("reg"))
            or None
        )

        # Type code: hexdb ICAOTypeCode preferred, tar1090 then adsbx as fallbacks
        type_code = (
            (hexdb and (hexdb.get("ICAOTypeCode") or "").strip() or None)
            or (tar1090 and (tar1090.get("ICAOTypeCode") or "").strip() or None)
            or (adsbx and adsbx.get("icaotype"))
            or None
        )
        type_category = None
        if type_code:
            ti = enrichment.db.get_type_info(type_code)
            if ti:
                type_category = ti.get("desc") or None

        # Manufacturer/year: adsbx tends to be more complete for these
        mfr  = (hexdb and (hexdb.get("Manufacturer") or "").strip() or None) or (adsbx and adsbx.get("manufacturer")) or None
        year = (adsbx and adsbx.get("year")) or None

        # Operator: hexdb OperatorFlagCode → clean airline name, then RegisteredOwners,
        # then tar1090 owner, then adsbx ownop as final fallback
        operator = None
        if hexdb:
            flag_code = (hexdb.get("OperatorFlagCode") or "").strip()
            if flag_code:
                op = enrichment.db.get_operator(flag_code)
                if op:
                    operator = op.get("n")
            if not operator:
                operator = (hexdb.get("RegisteredOwners") or "").strip() or None
        if not operator:
            operator = (tar1090 and (tar1090.get("RegisteredOwners") or "").strip() or None)
        if not operator:
            operator = (adsbx and adsbx.get("ownop")) or None

        country = country_from_registration(registration) or enrichment.db.get_country_by_icao(icao)

        await asyncio.to_thread(
            stats_db.force_update_aircraft_enrichment,
            icao, registration, type_code, type_category,
            operator, mfr, year, country,
        )

    return await aircraft_detail(icao)


@router.get("/{icao}/route")
async def aircraft_route(icao: str, callsign: str = Query(...)) -> dict | None:
    # Strip ADS-B padding underscores (callsigns are padded to 8 chars with _)
    callsign = callsign.upper().strip().rstrip('_')

    # Check cache
    cached = _route_cache.get(callsign)
    if cached:
        ts, result = cached
        if time.time() - ts < _ROUTE_CACHE_TTL:
            return result

    result = await asyncio.to_thread(_fetch_route_blocking, callsign)
    _route_cache[callsign] = (time.time(), result)
    return result
