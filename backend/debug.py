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

import aircraft_state as _state_module
import beast_client as _beast_client_module
import benchmark as _benchmark_module
import enrichment as enrichment_module
import radar.api as _radar_api_module
import radar.sweep as _radar_sweep_module
from fastapi import APIRouter, HTTPException
from pydantic import BaseModel

from benchmark import DecoderPaused
from db import stats_db

# On Pi hardware 20k iterations pins all cores for ~20s and can push junction
# temperature above the 80°C soft-throttle threshold, corrupting the results.
_IS_PI    = os.path.exists("/sys/firmware/devicetree/base/model")
_BENCH_MAX = 2_000 if _IS_PI else 20_000

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
    import main as _main_module
    _fm_run_timings = getattr(_main_module, "_fm_run_timings", [])
    _msg_drops = getattr(_main_module, "_msg_drops", 0)
    _msg_queue = getattr(_main_module, "_msg_queue")
    _queue_depth_samples = getattr(_main_module, "_queue_depth_samples", [])
    _radar_drops = getattr(_main_module, "_radar_drops", 0)
    _radar_loop_timings = getattr(_main_module, "_radar_loop_timings", [])
    _radar_queue = getattr(_main_module, "_radar_queue")
    _radar_queue_depth_samples = getattr(_main_module, "_radar_queue_depth_samples", [])
    _radar_ws_timings = getattr(_main_module, "_radar_ws_timings", [])
    _radar_worker_timings = getattr(_main_module, "_radar_worker_timings", [])
    _timing_ws_timings = getattr(_main_module, "_timing_ws_timings", [])
    msg_t        = sorted(_state_module.msg_timings)
    predecode_t  = sorted(_state_module.predecode_timings)
    lock_wait_t  = sorted(_state_module.lock_wait_timings)
    decode_t     = sorted(_state_module.decode_timings)
    decoder_phase_t = list(getattr(_state_module, "decoder_phase_timings", []))
    push_t       = list(_state_module.push_timings)
    qdepth       = sorted(_queue_depth_samples)
    radar_qdepth = sorted(_radar_queue_depth_samples)
    radar_df11_t = sorted(_radar_sweep_module.df11_event_timings)
    radar_builder_t = sorted(_radar_sweep_module.df11_builder_timings)
    radar_worker_phase_t = list(getattr(_radar_sweep_module, "df11_batch_phase_timings", []))
    radar_rotation_t = list(_radar_sweep_module.rotation_update_timings)
    radar_loop_t = list(_radar_loop_timings)
    radar_worker_t = list(_radar_worker_timings)
    radar_ws_t = list(_radar_ws_timings)
    timing_ws_t = list(_timing_ws_timings)
    fm_t = list(_fm_run_timings)
    decoder_batch_t = list(_benchmark_module.decoder_batch_timings)
    beast_chunk_t = list(_beast_client_module.chunk_timings)
    radar_api_t = list(_radar_api_module.api_timings)

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

    def int_percentiles(data: list[int]) -> dict:
        n = len(data)
        if not n:
            return {"samples": 0, "p50": 0, "p95": 0, "max": 0}
        return {
            "samples": n,
            "p50": data[int(n * 0.50)],
            "p95": data[int(n * 0.95)],
            "max": data[-1],
        }

    def push_avg(key: str) -> float:
        if not push_t:
            return 0.0
        return round(sum(p.get(key, 0) for p in push_t) / len(push_t), 2)

    def sample_avg(samples: list[dict], key: str) -> float:
        if not samples:
            return 0.0
        return round(sum(float(sample.get(key, 0.0)) for sample in samples) / len(samples), 2)

    def endpoint_summary(samples: list[dict]) -> dict:
        by_name: dict[str, list[float]] = {}
        for sample in samples:
            name = str(sample.get("endpoint", "unknown"))
            by_name.setdefault(name, []).append(float(sample.get("elapsed_ms", 0.0)))
        return {
            name: {
                "samples": len(values),
                "avg_ms": round(sum(values) / len(values), 2) if values else 0.0,
                "max_ms": round(max(values), 2) if values else 0.0,
            }
            for name, values in sorted(by_name.items())
        }

    return {
        "msg_decode_us":   percentiles(msg_t, scale=1_000_000),
        "predecode_us":    percentiles(predecode_t, scale=1_000_000),
        "lock_wait_us":    percentiles(lock_wait_t, scale=1_000_000),
        "pure_decode_us":  percentiles(decode_t, scale=1_000_000),
        "msg_queue_depth": _msg_queue.qsize(),
        "msg_queue_stats": int_percentiles(qdepth),
        "msg_drops_total": _msg_drops,
        "radar_queue_depth": _radar_queue.qsize(),
        "radar_queue_stats": int_percentiles(radar_qdepth),
        "radar_drops_total": _radar_drops,
        "push_updates_ms": {
            "samples":                len(push_t),
            "sync_avg":               push_avg("sync_ms"),
            "snapshot_avg":           push_avg("snapshot_ms"),
            "serialize_avg":          push_avg("serialize_ms"),
            "gather_avg":             push_avg("gather_ms"),
            "notify_tasks_avg":       push_avg("notify_tasks"),
            "broadcast_avg":          push_avg("broadcast_ms"),
            "total_avg":              push_avg("total_ms"),
            "ac_count_avg":           push_avg("ac_count"),
            "ws_client_count_avg":    push_avg("ws_client_count"),
            "ws_clients_dropped_total": 0,
            "ws_send_max_avg":        push_avg("ws_send_max_ms"),
        },
        "radar_decoder_ms": {
            "df11_event": percentiles(radar_df11_t, scale=1_000),
            "df11_builder": percentiles(radar_builder_t, scale=1_000),
        },
        "radar_rotation_ms": {
            "samples": len(radar_rotation_t),
            "total_avg": sample_avg(radar_rotation_t, "total_ms"),
            "snapshot_avg": sample_avg(radar_rotation_t, "snapshot_ms"),
            "tracker_refresh_avg": sample_avg(radar_rotation_t, "tracker_refresh_ms"),
            "history_fetch_avg": sample_avg(radar_rotation_t, "history_fetch_ms"),
            "cache_build_avg": sample_avg(radar_rotation_t, "cache_build_ms"),
            "sweep_build_avg": sample_avg(radar_rotation_t, "sweep_build_ms"),
            "analyse_swap_avg": sample_avg(radar_rotation_t, "analyse_swap_ms"),
            "iid_count_avg": sample_avg(radar_rotation_t, "iid_count"),
            "event_count_avg": sample_avg(radar_rotation_t, "event_count"),
            "deferred_iid_count_avg": sample_avg(radar_rotation_t, "deferred_iid_count"),
        },
        "radar_loop_ms": {
            "samples": len(radar_loop_t),
            "update_avg": sample_avg(radar_loop_t, "update_ms"),
            "flush_avg": sample_avg(radar_loop_t, "flush_ms"),
            "msg_queue_depth_avg": sample_avg(radar_loop_t, "msg_queue_depth"),
            "skipped_for_backlog_count": sum(1 for sample in radar_loop_t if sample.get("skipped_for_backlog")),
            "models_flushed_avg": sample_avg(radar_loop_t, "models_flushed"),
        },
        "radar_worker_ms": {
            "samples": len(radar_worker_t),
            "queue_wait_avg": sample_avg(radar_worker_t, "queue_wait_ms"),
            "batch_fill_avg": sample_avg(radar_worker_t, "batch_fill_ms"),
            "process_wall_avg": sample_avg(radar_worker_t, "process_wall_ms"),
            "process_cpu_avg": sample_avg(radar_worker_t, "process_cpu_ms"),
            "process_offcpu_avg": sample_avg(radar_worker_t, "process_offcpu_ms"),
            "batch_size_avg": sample_avg(radar_worker_t, "batch_size"),
            "batch_target_avg": sample_avg(radar_worker_t, "batch_target"),
        },
        "radar_worker_phase_ms": {
            "samples": len(radar_worker_phase_t),
            "input_count_avg": sample_avg(radar_worker_phase_t, "input_count"),
            "processed_count_avg": sample_avg(radar_worker_phase_t, "processed_count"),
            "active_iid_count_avg": sample_avg(radar_worker_phase_t, "active_iid_count"),
            "fired_burst_count_avg": sample_avg(radar_worker_phase_t, "fired_burst_count"),
            "prepare_avg": sample_avg(radar_worker_phase_t, "prepare_ms"),
            "append_avg": sample_avg(radar_worker_phase_t, "append_ms"),
            "group_avg": sample_avg(radar_worker_phase_t, "group_ms"),
            "builder_wall_avg": sample_avg(radar_worker_phase_t, "builder_wall_ms"),
            "builder_cpu_avg": sample_avg(radar_worker_phase_t, "builder_cpu_ms"),
            "builder_offcpu_avg": sample_avg(radar_worker_phase_t, "builder_offcpu_ms"),
            "native_burst_avg": sample_avg(radar_worker_phase_t, "native_burst_ms"),
            "process_burst_avg": sample_avg(radar_worker_phase_t, "process_burst_ms"),
            "total_wall_avg": sample_avg(radar_worker_phase_t, "total_wall_ms"),
            "total_cpu_avg": sample_avg(radar_worker_phase_t, "total_cpu_ms"),
            "total_offcpu_avg": sample_avg(radar_worker_phase_t, "total_offcpu_ms"),
        },
        "radar_fired_burst_phase_ms": {
            "samples": len(radar_worker_phase_t),
            "fired_burst_count_avg": sample_avg(radar_worker_phase_t, "fired_burst_count"),
            "reference_select_count_avg": sample_avg(radar_worker_phase_t, "reference_select_count"),
            "reference_reuse_count_avg": sample_avg(radar_worker_phase_t, "reference_reuse_count"),
            "reference_rescore_count_avg": sample_avg(radar_worker_phase_t, "reference_rescore_count"),
            "position_lookup_count_avg": sample_avg(radar_worker_phase_t, "position_lookup_count"),
            "dominant_check_count_avg": sample_avg(radar_worker_phase_t, "dominant_check_count"),
            "phase_check_count_avg": sample_avg(radar_worker_phase_t, "phase_check_count"),
            "frame_start_count_avg": sample_avg(radar_worker_phase_t, "frame_start_count"),
            "observation_count_avg": sample_avg(radar_worker_phase_t, "observation_count"),
            "frame_finalized_count_avg": sample_avg(radar_worker_phase_t, "frame_finalized_count"),
            "fm_callback_count_avg": sample_avg(radar_worker_phase_t, "fm_callback_count"),
            "setup_avg": sample_avg(radar_worker_phase_t, "setup_ms"),
            "centroid_avg": sample_avg(radar_worker_phase_t, "centroid_ms"),
            "finalize_avg": sample_avg(radar_worker_phase_t, "finalize_ms"),
            "reference_select_avg": sample_avg(radar_worker_phase_t, "reference_select_ms"),
            "position_lookup_avg": sample_avg(radar_worker_phase_t, "position_lookup_ms"),
            "dominant_check_avg": sample_avg(radar_worker_phase_t, "dominant_check_ms"),
            "suppression_avg": sample_avg(radar_worker_phase_t, "suppression_ms"),
            "phase_check_avg": sample_avg(radar_worker_phase_t, "phase_check_ms"),
            "frame_mutation_avg": sample_avg(radar_worker_phase_t, "frame_mutation_ms"),
            "fm_callback_avg": sample_avg(radar_worker_phase_t, "fm_callback_ms"),
        },
        "fm_run_ms": {
            "samples": len(fm_t),
            "elapsed_avg": sample_avg(fm_t, "elapsed_ms"),
            "frame_count_avg": sample_avg(fm_t, "frame_count"),
            "stored_avg": sample_avg(fm_t, "stored"),
            "success_avg": sample_avg(fm_t, "ok"),
        },
        "radar_iid_ws_ms": {
            "samples": len(radar_ws_t),
            "elapsed_avg": sample_avg(radar_ws_t, "elapsed_ms"),
            "full_send_count": sum(1 for sample in radar_ws_t if sample.get("sent_kind") == "full"),
            "heartbeat_count": sum(1 for sample in radar_ws_t if sample.get("sent_kind") == "heartbeat"),
            "rebuild_count": sum(1 for sample in radar_ws_t if sample.get("rebuilt")),
        },
        "timing_ws_ms": {
            "samples": len(timing_ws_t),
            "elapsed_avg": sample_avg(timing_ws_t, "elapsed_ms"),
            "event_count_avg": sample_avg(timing_ws_t, "event_count"),
            "raw_event_count_avg": sample_avg(timing_ws_t, "raw_event_count"),
            "send_count": sum(1 for sample in timing_ws_t if sample.get("sent")),
        },
        "decoder_thread_ms": {
            "samples": len(decoder_batch_t),
            "queue_wait_avg": sample_avg(decoder_batch_t, "queue_wait_ms"),
            "batch_fill_avg": sample_avg(decoder_batch_t, "batch_fill_ms"),
            "process_wall_avg": sample_avg(decoder_batch_t, "process_wall_ms"),
            "process_cpu_avg": sample_avg(decoder_batch_t, "process_cpu_ms"),
            "process_offcpu_avg": sample_avg(decoder_batch_t, "process_offcpu_ms"),
            "batch_size_avg": sample_avg(decoder_batch_t, "batch_size"),
            "batch_target_avg": sample_avg(decoder_batch_t, "batch_target"),
        },
        "decoder_batch_phase_ms": {
            "samples": len(decoder_phase_t),
            "batch_size_avg": sample_avg(decoder_phase_t, "batch_size"),
            "predecode_wall_avg": sample_avg(decoder_phase_t, "predecode_wall_ms"),
            "predecode_cpu_avg": sample_avg(decoder_phase_t, "predecode_cpu_ms"),
            "predecode_offcpu_avg": sample_avg(decoder_phase_t, "predecode_offcpu_ms"),
            "lock_wait_avg": sample_avg(decoder_phase_t, "lock_wait_ms"),
            "apply_wall_avg": sample_avg(decoder_phase_t, "apply_wall_ms"),
            "apply_cpu_avg": sample_avg(decoder_phase_t, "apply_cpu_ms"),
            "apply_offcpu_avg": sample_avg(decoder_phase_t, "apply_offcpu_ms"),
            "total_wall_avg": sample_avg(decoder_phase_t, "total_wall_ms"),
            "total_cpu_avg": sample_avg(decoder_phase_t, "total_cpu_ms"),
            "total_offcpu_avg": sample_avg(decoder_phase_t, "total_offcpu_ms"),
        },
        "radar_api_ms": {
            "samples": len(radar_api_t),
            "elapsed_avg": sample_avg(radar_api_t, "elapsed_ms"),
            "by_endpoint": endpoint_summary(radar_api_t),
        },
        "beast_ingest_ms": {
            "samples": len(beast_chunk_t),
            "chunk_bytes_avg": sample_avg(beast_chunk_t, "chunk_bytes"),
            "frames_avg": sample_avg(beast_chunk_t, "frames"),
            "parse_wall_avg": sample_avg(beast_chunk_t, "parse_wall_ms"),
            "parse_cpu_avg": sample_avg(beast_chunk_t, "parse_cpu_ms"),
            "parse_offcpu_avg": sample_avg(beast_chunk_t, "parse_offcpu_ms"),
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

@router.get("/aircraft/{icao}/live")
async def debug_aircraft_live(icao: str) -> dict:
    """Return live in-memory state for a single aircraft (position QA fields)."""
    from main import state
    icao = icao.upper()
    with state._lock:
        ac = state._aircraft.get(icao)
        if ac is None:
            raise HTTPException(status_code=404, detail="Aircraft not in live state")
        return {
            "icao":               ac.icao,
            "pos_source":         ac._pos_source,
            "pos_rejected_count": ac.pos_rejected_count,
            "pos_reliable_odd":   round(ac.pos_reliable_odd, 3),
            "pos_reliable_even":  round(ac.pos_reliable_even, 3),
            "pos_global":         ac.pos_global,
            "pos_confident":      _state_module._pos_reliable(ac),
            "lat":                ac.lat,
            "lon":                ac.lon,
            "last_pos_age":       round(__import__('time').time() - ac.last_pos_ts, 1) if ac.last_pos_ts > 0 else None,
            "mlat":               ac.mlat,
            "mlat_source":        ac.mlat_source,
        }


@router.get("/aircraft/{icao}")
async def debug_aircraft(icao: str) -> dict:
    icao = icao.upper()
    adsbx = enrichment_module.db.get_adsbx(icao)
    country = enrichment_module.db.get_country_by_icao(icao)
    military = enrichment_module.db.is_military(icao)
    cache = {
        "adsbx": enrichment_module.db.get_adsbx_cached(icao) is not None,
        "tar1090": enrichment_module.db.get_tar1090_cached(icao) is not None,
        "hexdb": enrichment_module.db.get_hexdb_cached(icao) is not None,
    }
    hexdb, tar1090, registry = await asyncio.gather(
        asyncio.to_thread(enrichment_module.db.force_lookup_hexdb, icao),
        asyncio.to_thread(enrichment_module.db.get_tar1090, icao),
        asyncio.to_thread(stats_db.get_aircraft, icao),
    )
    enrichment_module.log_enrichment_trace(
        icao,
        "manual_debug",
        cache=cache,
        adsbx=adsbx,
        tar1090=tar1090,
        hexdb=hexdb,
        final=dict(registry) if registry else None,
        pending=False,
    )
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

    if req.field == "military":
        if req.value is not None:
            await asyncio.to_thread(stats_db.set_military_flag, icao, bool(req.value))
    else:
        # Build a sparse update: only the requested field is non-None; the rest
        # pass through COALESCE in force_update_aircraft_enrichment unchanged.
        kwargs: dict = dict(registration=None, type_code=None, type_category=None,
                            operator=None, manufacturer=None, year=None, country=None)
        kwargs[req.field] = req.value
        await asyncio.to_thread(stats_db.force_update_aircraft_enrichment, icao, **kwargs)

    return {"status": "ok", "icao": icao, "field": req.field, "value": req.value}


# ---------------------------------------------------------------------------
# Type code manufacturer override
# ---------------------------------------------------------------------------

class ManufacturerRequest(BaseModel):
    manufacturer: str


@router.get("/type/{type_code}")
async def debug_type(type_code: str) -> dict:
    tc = type_code.strip().upper()
    ti = enrichment_module.db.get_type_info(tc)
    airframe_count, db_manufacturer = await asyncio.to_thread(_type_stats, tc)
    overrides = await asyncio.to_thread(stats_db.get_all_type_manufacturer_overrides)
    override = overrides.get(tc)
    return {
        "type_code":      tc,
        "type_name":      ti.get("name") if ti else None,
        "wtc":            ti.get("wtc")  if ti else None,
        "airframe_count": airframe_count,
        "db_manufacturer": db_manufacturer,
        "override":       override,
    }


def _type_stats(type_code: str) -> tuple[int, str | None]:
    return stats_db.get_type_code_stats(type_code)


@router.post("/type/{type_code}/manufacturer")
async def set_type_manufacturer(type_code: str, req: ManufacturerRequest) -> dict:
    tc = type_code.strip().upper()
    mfr = req.manufacturer.strip()
    if not mfr:
        from fastapi import HTTPException
        raise HTTPException(status_code=400, detail="manufacturer must not be empty")
    await asyncio.to_thread(stats_db.set_type_manufacturer_override, tc, mfr)
    return {"status": "ok", "type_code": tc, "manufacturer": mfr}


@router.delete("/type/{type_code}/manufacturer")
async def delete_type_manufacturer(type_code: str) -> dict:
    tc = type_code.strip().upper()
    deleted = await asyncio.to_thread(stats_db.delete_type_manufacturer_override, tc)
    if not deleted:
        from fastapi import HTTPException
        raise HTTPException(status_code=404, detail=f"No override for {tc}")
    return {"status": "ok", "type_code": tc}
