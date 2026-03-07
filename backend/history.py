"""
History API router — serves aggregated historical stats from SQLite.
All DB calls are dispatched via asyncio.to_thread() to avoid blocking the event loop.
"""

import asyncio
from typing import Optional
from fastapi import APIRouter, HTTPException, Query

from db import stats_db
import enrichment

router = APIRouter(prefix="/api/history")

# Whitelists prevent SQL injection via dynamic column names
_HEATMAP_METRICS = {
    "ac_total":    "ac_total",
    "ac_civil":    "ac_civil",
    "ac_military": "ac_military",
}
_CALENDAR_METRICS = {
    "ac_peak":           "ac_peak",
    "ac_civil_peak":     "ac_civil_peak",
    "ac_military_peak":  "ac_military_peak",
    "msg_total":         "msg_total",
    "msg_max":           "msg_max",
    "unique_aircraft":   "unique_aircraft",
}
_VALID_FLAGS = {"all", "foreign_military", "interesting", "rare", "first_seen_flag", "unique_sighting"}


_VALID_BUCKETS = {15, 60}


@router.get("/heatmap")
async def heatmap(
    metric: str = Query("ac_total"),
    days: int = Query(30, ge=7, le=30),
    bucket: int = Query(15),
) -> list[dict]:
    if metric not in _HEATMAP_METRICS:
        raise HTTPException(400, f"Unknown metric. Valid: {sorted(_HEATMAP_METRICS)}")
    if bucket not in _VALID_BUCKETS:
        raise HTTPException(400, f"Invalid bucket. Valid: {sorted(_VALID_BUCKETS)}")
    col = _HEATMAP_METRICS[metric]
    return await asyncio.to_thread(stats_db.query_heatmap, col, days, bucket)


@router.get("/heatmap/group")
async def heatmap_group(
    types: Optional[str] = Query(None),
    category: Optional[str] = Query(None),
    days: int = Query(30, ge=7, le=30),
    bucket: int = Query(15),
) -> list[dict]:
    if bucket not in _VALID_BUCKETS:
        raise HTTPException(400, f"Invalid bucket. Valid: {sorted(_VALID_BUCKETS)}")
    type_codes = [t.strip().upper() for t in types.split(',') if t.strip()] if types else None
    cat_prefix = category.strip().upper() if category else None
    return await asyncio.to_thread(
        stats_db.query_heatmap_group, type_codes, cat_prefix, days, bucket
    )


@router.get("/heatmap/type")
async def heatmap_type(
    type_code: str = Query(...),
    days: int = Query(30, ge=7, le=30),
    bucket: int = Query(15),
) -> list[dict]:
    if bucket not in _VALID_BUCKETS:
        raise HTTPException(400, f"Invalid bucket. Valid: {sorted(_VALID_BUCKETS)}")
    return await asyncio.to_thread(
        stats_db.query_heatmap_type, type_code.upper(), days, bucket
    )


@router.get("/heatmap/operator")
async def heatmap_operator(
    operator: str = Query(...),
    days: int = Query(30, ge=7, le=30),
    bucket: int = Query(15),
) -> list[dict]:
    if bucket not in _VALID_BUCKETS:
        raise HTTPException(400, f"Invalid bucket. Valid: {sorted(_VALID_BUCKETS)}")
    return await asyncio.to_thread(
        stats_db.query_heatmap_operator, operator, days, bucket
    )


@router.get("/calendar")
async def calendar(
    metric: str = Query("ac_peak"),
    months: int = Query(12, ge=1, le=24),
) -> list[dict]:
    if metric not in _CALENDAR_METRICS:
        raise HTTPException(400, f"Unknown metric. Valid: {sorted(_CALENDAR_METRICS)}")
    col = _CALENDAR_METRICS[metric]
    return await asyncio.to_thread(stats_db.query_calendar, col, months)


@router.get("/trend")
async def trend(
    days: int = Query(90, ge=7, le=365),
) -> list[dict]:
    return await asyncio.to_thread(stats_db.query_trend, days)


@router.get("/heatmap/options")
async def heatmap_options() -> dict:
    return await asyncio.to_thread(stats_db.query_heatmap_options)


@router.get("/heatmap/df")
async def heatmap_df(
    df: Optional[int] = Query(None),
    days: int = Query(30, ge=7, le=30),
    bucket: int = Query(60),
) -> list[dict]:
    if bucket not in _VALID_BUCKETS:
        raise HTTPException(400, f"Invalid bucket. Valid: {sorted(_VALID_BUCKETS)}")
    return await asyncio.to_thread(stats_db.query_heatmap_df, df, days, bucket)


@router.get("/calendar/new_aircraft")
async def calendar_new_aircraft(
    months: int = Query(12, ge=1, le=24),
) -> list[dict]:
    return await asyncio.to_thread(stats_db.query_new_aircraft_per_day, months)


@router.get("/calendar/military_aircraft")
async def calendar_military_aircraft(
    months: int = Query(12, ge=1, le=24),
) -> list[dict]:
    return await asyncio.to_thread(stats_db.query_military_aircraft_per_day, months)


@router.get("/calendar/notable_sightings")
async def calendar_notable_sightings(
    months: int = Query(12, ge=1, le=24),
) -> list[dict]:
    return await asyncio.to_thread(stats_db.query_notable_sightings_per_day, months)


@router.get("/calendar/group")
async def calendar_group(
    months: int = Query(12, ge=1, le=24),
    types: Optional[str] = Query(None),
    category: Optional[str] = Query(None),
) -> list[dict]:
    type_codes = [t.strip().upper() for t in types.split(',') if t.strip()] if types else None
    cat_prefix = category.strip().upper() if category else None
    return await asyncio.to_thread(
        stats_db.query_calendar_group, months, type_codes, cat_prefix
    )


@router.get("/receiver/scatter")
async def receiver_scatter(days: int = Query(7, ge=1, le=30)) -> list[dict]:
    return await asyncio.to_thread(stats_db.query_receiver_scatter, days)


@router.get("/receiver/signal")
async def receiver_signal(days: int = Query(14, ge=1, le=90)) -> list[dict]:
    return await asyncio.to_thread(stats_db.query_signal_percentiles, days)


@router.get("/receiver/df")
async def receiver_df(days: int = Query(30, ge=1, le=90)) -> list[dict]:
    return await asyncio.to_thread(stats_db.query_df_breakdown, days)


@router.get("/receiver/baseline")
async def receiver_baseline() -> list[dict]:
    return await asyncio.to_thread(stats_db.query_receiver_baseline)


@router.get("/receiver/distributions")
async def receiver_distributions() -> dict:
    return await asyncio.to_thread(stats_db.query_distributions)


@router.get("/receiver/unique_aircraft")
async def receiver_unique_aircraft(days: int = Query(90, ge=7, le=365)) -> list[dict]:
    return await asyncio.to_thread(stats_db.query_unique_aircraft_per_day, days)


@router.get("/receiver/completeness")
async def receiver_completeness(days: int = Query(90, ge=7, le=365)) -> list[dict]:
    return await asyncio.to_thread(stats_db.query_completeness, days)


@router.get("/receiver/position_decode_rate")
async def receiver_position_decode_rate(days: int = Query(90, ge=7, le=365)) -> list[dict]:
    return await asyncio.to_thread(stats_db.query_position_decode_rate, days)


@router.get("/notable")
async def notable(
    limit: int = Query(50, ge=1, le=200),
    flag: str = Query("all"),
    days: Optional[int] = Query(None, ge=1, le=365),
) -> list[dict]:
    if flag not in _VALID_FLAGS:
        raise HTTPException(400, f"Unknown flag. Valid: {sorted(_VALID_FLAGS)}")
    if flag == "unique_sighting":
        rows = await asyncio.to_thread(stats_db.query_unique_sightings, limit, days)
    else:
        rows = await asyncio.to_thread(stats_db.query_notable, limit, flag, days)
    # Overlay ADSBExchange + hexdb in-memory data; fall back to registry-stored fields
    for row in rows:
        icao = row["icao"]
        adsbx = enrichment.db.get_adsbx(icao)
        hexdb = enrichment.db.get_hexdb_cached(icao)
        row["operator"] = (
            (adsbx and adsbx.get("ownop"))
            or (hexdb and hexdb.get("RegisteredOwners"))
            or row.get("operator")
            or None
        )
        row["year"] = (adsbx and adsbx.get("year")) or row.get("year") or None
        mfr   = (adsbx and adsbx.get("manufacturer")) or (hexdb and hexdb.get("Manufacturer")) or ""
        model = (adsbx and adsbx.get("model")) or (hexdb and hexdb.get("Type")) or ""
        type_desc = (f"{mfr} {model}".strip()) or None
        row["type_desc"] = type_desc or row.get("manufacturer") or None
    return rows
