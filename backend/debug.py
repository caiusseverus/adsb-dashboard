"""
Debug API router — aircraft data source comparison, field override, and benchmark.

GET  /api/debug/perf                     — live runtime timing stats
GET  /api/debug/benchmark                — run (or return cached) pipeline benchmark
GET  /api/debug/aircraft/{icao}          — query all enrichment sources
POST /api/debug/aircraft/{icao}/override — override a field in aircraft_registry
"""

import asyncio
import logging
import os

from fastapi import APIRouter, HTTPException
from pydantic import BaseModel

# On Pi hardware 20k iterations pins all cores for ~20s and can push junction
# temperature above the 80°C soft-throttle threshold, corrupting the results.
_IS_PI    = os.path.exists("/sys/firmware/devicetree/base/model")
_BENCH_MAX = 2_000 if _IS_PI else 20_000

from db import stats_db
import enrichment as enrichment_module
import aircraft_state as _state_module
import benchmark as _benchmark_module
from benchmark import DecoderPaused

log = logging.getLogger(__name__)
router = APIRouter(prefix="/api/debug")

OVERRIDEABLE_FIELDS = {
    "country", "registration", "type_code", "operator",
    "military", "manufacturer", "year",
}


# ---------------------------------------------------------------------------
# Perf stats (live runtime)
# ---------------------------------------------------------------------------

@router.get("/perf")
async def get_perf() -> dict:
    """Return performance timing statistics for message decode and push-updates."""
    from main import _msg_queue
    msg_t  = sorted(_state_module.msg_timings)
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
            "mean": round((sum(data) / n) * scale, 1),
        }

    def push_avg(key: str) -> float:
        if not push_t:
            return 0.0
        return round(sum(p[key] for p in push_t) / len(push_t), 2)

    return {
        "msg_decode_us":  percentiles(msg_t, scale=1_000_000),
        "msg_queue_depth": _msg_queue.qsize(),
        "push_updates_ms": {
            "samples":          len(push_t),
            "sync_avg":         push_avg("sync_ms"),
            "gather_avg":       push_avg("gather_ms"),
            "notify_tasks_avg": push_avg("notify_tasks"),
            "broadcast_avg":    push_avg("broadcast_ms"),
            "total_avg":        push_avg("total_ms"),
            "ac_count_avg":     push_avg("ac_count"),
        },
    }


# ---------------------------------------------------------------------------
# Benchmark endpoint
# ---------------------------------------------------------------------------

_bench_running = False   # guard against concurrent runs


def _run_with_pause(n_msgs: int) -> dict:
    """
    Blocking function executed in a thread (via asyncio.to_thread).

    Pauses the live decoder for the full duration of the benchmark so that
    GIL contention from real message decoding cannot inflate timings.
    """
    with DecoderPaused(drain_timeout=2.0):
        return _benchmark_module.run_benchmark(n_msgs=n_msgs, paused=True)


@router.get("/benchmark")
async def run_benchmark(fresh: bool = False, n: int = 5000) -> dict:
    """
    Run the pipeline micro-benchmark and return results.

    The live Beast decoder is paused for the duration of the run so that
    GIL contention does not inflate the timings.  The Beast TCP connection
    remains open; buffered frames are processed once the decoder resumes.

    Query params
    ------------
    fresh : bool  — force a new run even if a cached result exists
    n     : int   — iterations per stage (default 5000; clamped 100–20000)
    """
    global _bench_running

    if not fresh:
        cached = _benchmark_module.get_last_result()
        if cached:
            return cached

    if _bench_running:
        raise HTTPException(
            status_code=409,
            detail="Benchmark already running — try again in a few seconds",
        )

    n = max(100, min(n, _BENCH_MAX))

    _bench_running = True
    try:
        log.info("benchmark: starting (%d iterations, decoder will be paused)", n)
        result = await asyncio.to_thread(_run_with_pause, n)
        log.info("benchmark: complete in %.1fs — %s", result["total_bench_time_s"], result["verdict"])
    finally:
        _bench_running = False

    return result


# ---------------------------------------------------------------------------
# Benchmark status (polled by UI to show running state)
# ---------------------------------------------------------------------------

@router.get("/benchmark/status")
async def benchmark_status() -> dict:
    """Returns whether a benchmark run is currently in progress."""
    cached = _benchmark_module.get_last_result()
    return {
        "running":    _bench_running,
        "has_result": bool(cached),
        "timestamp":  cached.get("timestamp") if cached else None,
        "verdict":    cached.get("verdict")   if cached else None,
    }


# ---------------------------------------------------------------------------
# Aircraft debug / override
# ---------------------------------------------------------------------------

@router.get("/aircraft/{icao}")
async def debug_aircraft(icao: str) -> dict:
    icao = icao.upper()
    adsbx    = enrichment_module.db.get_adsbx(icao)
    hexdb    = enrichment_module.db.get_hexdb_cached(icao)
    tar1090  = enrichment_module.db.get_tar1090_cached(icao)
    country  = enrichment_module.db.get_country_by_icao(icao)
    military = enrichment_module.db.is_military(icao)
    registry = await asyncio.to_thread(stats_db.get_aircraft, icao)
    return {
        "icao":        icao,
        "icao_block":  {"country": country, "military": military},
        "adsbexchange": adsbx,
        "hexdb":       hexdb,
        "tar1090":     tar1090,
        "registry":    dict(registry) if registry else None,
    }


class OverrideRequest(BaseModel):
    field: str
    value: str | bool | None


@router.post("/aircraft/{icao}/override")
async def override_aircraft_field(icao: str, req: OverrideRequest) -> dict:
    icao = icao.upper()
    if req.field not in OVERRIDEABLE_FIELDS:
        raise HTTPException(
            status_code=400,
            detail=f"Field '{req.field}' is not overrideable. Allowed: {sorted(OVERRIDEABLE_FIELDS)}",
        )
    row = await asyncio.to_thread(stats_db.get_aircraft, icao)
    if row is None:
        raise HTTPException(status_code=404, detail=f"Aircraft {icao} not in registry")

    await asyncio.to_thread(
        stats_db.force_update_aircraft_enrichment,
        icao,
        req.value if req.field == "registration" else None,
        req.value if req.field == "type_code"     else None,
        None,
        req.value if req.field == "operator"      else None,
        req.value if req.field == "manufacturer"  else None,
        req.value if req.field == "year"          else None,
        req.value if req.field == "country"       else None,
    )

    if req.field == "military" and req.value is not None:
        await asyncio.to_thread(stats_db.set_military_flag, icao, bool(req.value))

    return {"status": "ok", "icao": icao, "field": req.field, "value": req.value}
