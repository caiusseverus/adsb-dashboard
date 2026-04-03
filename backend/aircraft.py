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

import config
import enrichment
from db import stats_db
from utils import country_from_registration

log = logging.getLogger(__name__)

router = APIRouter(prefix="/api/aircraft")

# In-memory route cache: callsign → (fetched_ts, result_dict | None)
_route_cache: dict[str, tuple[float, dict | None]] = {}
_ROUTE_CACHE_TTL  = 3600   # 1 hour
_ROUTE_CACHE_MAX  = 2000   # prevent unbounded growth

# Airport info cache: ICAO code → info dict (or None if not found)
# Airport names are stable; no TTL needed, evict if too large.
_airport_cache: dict[str, dict | None] = {}
_AIRPORT_CACHE_MAX = 500


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
    # Prefer live/row (already resolved via apply_adsbx/apply_hexdb) over raw sources.
    result["operator"] = (
        (live and live.get("operator"))
        or (row and row.get("operator"))
        or (adsbx and adsbx.get("ownop"))
        or (hexdb and hexdb.get("RegisteredOwners"))
        or None
    )

    # --- Country + military ---
    is_military = bool(
        (live and live.get("military"))
        or (row and row.get("military"))
        or military
    )
    result["military"] = is_military

    if is_military:
        # Military serials don't follow civil registration prefix conventions
        # (e.g. RAF ZK341 starts with 'ZK' but is NOT New Zealand).
        # Prefer the registry value — it may have been manually corrected.
        result["country"] = (
            (row and row.get("country"))
            or (live and live.get("country"))
            or country
            or None
        )
    else:
        result["country"] = (
            country_from_registration(result.get("registration"))
            or (live and live.get("country"))
            or (row and row.get("country"))
            or country
            or None
        )

    # --- Year ---
    result["year"] = (live and live.get("year")) or (row and row.get("year")) or (adsbx and adsbx.get("year")) or None

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
            "bearing_deg":       live.get("bearing_deg"),
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


def _get_airport_info_blocking(ap_icao: str) -> dict | None:
    """Fetch airport info from hexdb, using module-level cache."""
    if ap_icao in _airport_cache:
        return _airport_cache[ap_icao]
    info = _get_json(f"https://hexdb.io/api/v1/airport/icao/{ap_icao}")
    if len(_airport_cache) >= _AIRPORT_CACHE_MAX:
        # Evict one entry (oldest key)
        _airport_cache.pop(next(iter(_airport_cache)), None)
    _airport_cache[ap_icao] = info
    return info


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


def _resolve_enrichment_fields(
    icao: str,
    hexdb: dict | None,
    tar1090: dict | None,
    adsbx: dict | None,
) -> dict | None:
    """Merge hexdb / tar1090 / adsbx into a single resolved enrichment dict.

    Priority order: hexdb > tar1090 > adsbx.
    Returns None if all sources are empty.
    """
    if not (hexdb or tar1090 or adsbx):
        return None

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

    return dict(
        registration=registration, type_code=type_code, type_category=type_category,
        operator=operator, manufacturer=mfr, year=year, country=country,
    )


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

    resolved = _resolve_enrichment_fields(icao, hexdb, tar1090, adsbx)
    if resolved:
        await asyncio.to_thread(
            stats_db.force_update_aircraft_enrichment,
            icao,
            resolved["registration"], resolved["type_code"], resolved["type_category"],
            resolved["operator"], resolved["manufacturer"], resolved["year"], resolved["country"],
        )

    return await aircraft_detail(icao)


@router.get("/{icao}/visits")
async def aircraft_visits(icao: str, limit: int = 30) -> list:
    icao = icao.upper()
    visits = await asyncio.to_thread(stats_db.query_visits, icao, limit)

    # Resolve airport names for unique origin/dest ICAOs (concurrent, cached)
    ap_icaos = {v[k] for v in visits for k in ("origin_icao", "dest_icao") if v.get(k)}
    if ap_icaos:
        infos = await asyncio.gather(
            *[asyncio.to_thread(_get_airport_info_blocking, ap) for ap in ap_icaos],
            return_exceptions=True,
        )
        airport_info = {ap: info for ap, info in zip(ap_icaos, infos)
                        if not isinstance(info, Exception)}
        for v in visits:
            for src_key, dst_key in (("origin_icao", "origin_info"), ("dest_icao", "dest_info")):
                ap = v.get(src_key)
                if ap and ap in airport_info:
                    v[dst_key] = airport_info[ap]

    return visits


@router.get("/{icao}/visits/{visit_id}/track")
async def aircraft_visit_track(icao: str, visit_id: int) -> list:
    icao = icao.upper()
    if config.RECEIVER_LAT is None or config.RECEIVER_LON is None:
        return []
    # Look up the visit's timestamps
    visits = await asyncio.to_thread(stats_db.query_visits, icao, 200)
    visit = next((v for v in visits if v["id"] == visit_id), None)
    if not visit:
        return []
    return await asyncio.to_thread(
        stats_db.query_visit_track,
        icao, visit["start_ts"], visit["end_ts"],
        config.RECEIVER_LAT, config.RECEIVER_LON,
    )


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
    if result is None and cached:
        # Fetch failed — serve the previous valid result rather than downgrading
        # to None. Reset timestamp so we retry after another TTL window.
        _, old_result = cached
        if old_result is not None:
            result = old_result
    _route_cache[callsign] = (time.time(), result)
    if len(_route_cache) > _ROUTE_CACHE_MAX:
        # Evict all expired entries; if still over limit remove the oldest.
        now = time.time()
        expired = [k for k, (ts, _) in _route_cache.items() if now - ts >= _ROUTE_CACHE_TTL]
        for k in expired:
            del _route_cache[k]
        if len(_route_cache) > _ROUTE_CACHE_MAX:
            oldest = min(_route_cache, key=lambda k: _route_cache[k][0])
            del _route_cache[oldest]
    return result
