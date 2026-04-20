import asyncio
import json
import time
from collections import deque
try:
    import orjson as _orjson
    def _json_dumps(obj: dict) -> str:
        return _orjson.dumps(obj).decode("utf-8")
except ImportError:
    _orjson = None  # type: ignore[assignment]
    def _json_dumps(obj: dict) -> str:  # type: ignore[misc]
        return json.dumps(obj)
import logging
import queue
import threading
from contextlib import asynccontextmanager
from datetime import date, datetime, timedelta

from fastapi import FastAPI, WebSocket, WebSocketDisconnect
from fastapi.middleware.cors import CORSMiddleware
from fastapi.staticfiles import StaticFiles

import config
import enrichment
from beast_client import BeastClient
from aircraft_state import (
    AircraftState,
    get_latency_waveforms as _get_latency_waveforms,
    push_timings as _push_timings_store,
    record_push_perf_sample as _record_push_perf_sample,
)
import readsb_ingest
import readsb_stats
from collections import deque as _deque
from db import stats_db
from track_store import TrackStore
from history import router as history_router
from aircraft import router as aircraft_router
from fleet import router as fleet_router
from coverage import router as coverage_router
from terrain import router as terrain_router, prewarm_cache as terrain_prewarm
from acas import router as acas_router
from squawks import router as squawks_router
import notifications
import hires_buffer
import memory_policy
from status import router as status_router, register_runtime_stats
from debug import router as debug_router
from notify_settings import router as notify_settings_router
import cast
from cast_api import router as cast_router
import health as health_module
from health import router as health_router
import tracks as tracks_module
from tracks import router as tracks_router
import mlat as mlat_module
from mlat import router as mlat_router
import position_quality as position_quality_module
from position_quality import router as position_quality_router, PositionQualityChecker, run_position_quality_checker

from benchmark import make_pause_aware_decoder
from interrogators import router as interrogators_router
from timing import router as timing_router
from radar.sweep import RadarState
from radar import api as radar_api
from radar import aircraft_api as radar_aircraft_api
from radar.aircraft_localiser import AircraftLocaliser
from radar_core.client import RadarCoreClient
from radar_core.worker import RadarCoreWorker



logging.basicConfig(
    level=logging.DEBUG if config.DEBUG_LOG else logging.WARNING,
    format="%(asctime)s  %(levelname)-8s  %(name)s  %(message)s",
)
log = logging.getLogger(__name__)
log.setLevel(logging.INFO)  # main module always logs at INFO regardless of DEBUG_LOG

# ---------------------------------------------------------------------------
# Shared state
# ---------------------------------------------------------------------------
state = AircraftState(aircraft_timeout=config.AIRCRAFT_TIMEOUT)
track_store = TrackStore()
radar_state = RadarState(
    aircraft_state=state,
    track_store=track_store,
    receiver_lat=config.RECEIVER_LAT,
    receiver_lon=config.RECEIVER_LON,
)
aircraft_localiser = AircraftLocaliser(
    radar_state=radar_state,
    aircraft_state=state,
    receiver_lat=config.RECEIVER_LAT,
    receiver_lon=config.RECEIVER_LON,
    min_calibration_samples=config.STAGE3_MIN_CALIBRATION_SAMPLES,
    min_radars_for_fix=config.STAGE3_MIN_RADARS_FOR_FIX,
    max_cep_m=config.STAGE3_MAX_CEP_M,
    stable_calibration_samples=config.STAGE3_STABLE_CALIBRATION_SAMPLES,
)
# Wire live-path config into localiser
aircraft_localiser._ray_retention_s = config.STAGE3_RAY_RETENTION_S

# radar-core IPC client.
# Sends DF11 events and position updates to the radar-core process.
# When RADAR_CORE_FRAMES_ENABLED, FRAME_READY messages are routed to the FM
# mailbox and Python frame-building is suppressed.
_radar_core_client: RadarCoreClient | None = None
_radar_core_worker: RadarCoreWorker | None = None
if config.RADAR_CORE_ENABLED:
    _rc_on_frame_ready = None
    if config.RADAR_CORE_FRAMES_ENABLED and not config.RADAR_CORE_FM_ENABLED:
        _rc_on_frame_ready = radar_state.inject_frame_from_go
        radar_state.enable_radar_core_frames(True)
    _radar_core_worker = RadarCoreWorker(
        binary_path=config.RADAR_CORE_BINARY,
        socket_path=config.RADAR_CORE_SOCKET,
        managed=config.RADAR_CORE_MANAGED,
        startup_timeout_s=config.RADAR_CORE_STARTUP_TIMEOUT_S,
        debug_logging=(config.DEBUG_LOG or config.RADAR_CORE_DEBUG_LOG),
        env_debug_flag=(config.DEBUG_LOG or config.RADAR_CORE_DEBUG_LOG),
    )
    _radar_core_client = RadarCoreClient(
        config.RADAR_CORE_SOCKET,
        on_frame_ready=_rc_on_frame_ready,
        on_fm_state=radar_state.update_forward_model_from_go if config.RADAR_CORE_FM_ENABLED else None,
        connect_timeout_s=config.RADAR_CORE_CONNECT_TIMEOUT_S,
        reconnect_delay_s=config.RADAR_CORE_RECONNECT_DELAY_S,
    )
    radar_state.radar_core_event_sink = _radar_core_client.send_radar_event

# FM solve runs on a dedicated worker thread that drains RadarState's per-IID
# mailbox.  Keeping FM work off the radar worker thread prevents burst
# processing from stalling when a solve is slow.  Latest-wins: a newer frame
# for the same IID supersedes any unsolved older frame before the worker picks
# it up, and at most one solve is in flight per IID at a time.
_fm_worker_thread: threading.Thread | None = None
_fm_worker_stop = threading.Event()


def _start_fm_worker() -> threading.Thread | None:
    if config.RADAR_CORE_ENABLED and config.RADAR_CORE_FM_ENABLED:
        log.info("ForwardModel: Python per-frame FM worker disabled; radar-core FM is authoritative")
        return None
    try:
        from radar.api import _get_fm
    except Exception:
        log.exception("ForwardModel: worker failed to import _get_fm")
        return None

    def _run() -> None:
        while not _fm_worker_stop.is_set():
            # Block until there is work, with a short timeout so shutdown is responsive.
            radar_state.wait_for_pending_fm_frames(timeout=0.5)
            if _fm_worker_stop.is_set():
                return
            pending = radar_state.claim_pending_fm_frames()
            if not pending:
                continue
            # Single-threaded worker: there is at most one solve in flight per
            # IID because we process the claimed batch sequentially before
            # re-draining.  Any frames arriving mid-solve overwrite the
            # mailbox entry for that IID (latest wins) and are picked up on
            # the next drain.
            for iid, frame, period_s in pending:
                try:
                    _t0_wall = time.perf_counter()
                    _t0_cpu = time.thread_time()
                    _get_fm().on_new_frame(
                        iid, frame, period_s,
                        getattr(config, "RECEIVER_LAT", None),
                        getattr(config, "RECEIVER_LON", None),
                    )
                    _wall_ms = (time.perf_counter() - _t0_wall) * 1000
                    _cpu_ms = (time.thread_time() - _t0_cpu) * 1000
                    _fm_worker_timings.append({
                        "iid": iid,
                        "wall_ms": round(_wall_ms, 2),
                        "cpu_ms": round(_cpu_ms, 2),
                        "offcpu_ms": round(_wall_ms - _cpu_ms, 2),
                    })
                except Exception:
                    log.exception("fm-worker: solve failed for IID %d", iid)

    t = threading.Thread(target=_run, daemon=True, name="fm-worker")
    t.start()
    return t


_fm_worker_thread = _start_fm_worker()

# Each connected WebSocket gets its own bounded send queue.
# _push_updates enqueues the serialised payload and returns immediately;
# a per-client sender coroutine drains the queue asynchronously.
# maxsize=2: one frame in-flight + one queued.  If the client falls behind
# a second full cycle, the new frame is dropped (QueueFull) — the client
# will receive the next cycle's snapshot instead.
_clients: dict[WebSocket, asyncio.Queue] = {}

# Emergency squawk tracking: {icao: {squawk, db_id}} for ongoing events
EMERGENCY_SQUAWKS = frozenset({"7700", "7600", "7500"})
_active_squawks: dict[str, dict] = {}

# Watchlist cache — refreshed from DB every 30s to avoid per-aircraft DB reads
_watchlist_cache: dict[str, float | None] = {}   # icao → max_range_nm

# Route lookup queue: (visit_id, callsign) pairs awaiting adsbdb.com resolution
_route_queue: deque[tuple[int, str]] = deque(maxlen=2000)
_route_queue_drops: int = 0   # count of visits evicted before enrichment
_watchlist_cache_ts: float = 0.0
_cast_enabled_cache: bool = False
_cast_enabled_cache_ts: float = 0.0

# Message decode queue — Beast/MLAT runners push raw messages here; a single
# background thread drains the queue calling state.process_message().  This
# keeps all pyModeS decode work off the asyncio event loop so TCP reads and
# WebSocket broadcasts are never starved.
# Bounded at 5000: if the decoder falls behind, drop new arrivals rather than
# accumulating stale messages that would be decoded minutes late.
_MSG_QUEUE_MAX = 5000
_msg_queue: queue.Queue = queue.Queue(maxsize=_MSG_QUEUE_MAX)
_DECODE_SENTINEL = object()   # placed on queue to signal the decoder thread to exit
_decoder_thread: threading.Thread | None = None
# Cumulative count of Beast/MLAT messages dropped due to full queue
_msg_drops: int = 0
# Queue depth sampled once per push cycle (maxlen matches push_timings window)
_queue_depth_samples: _deque[int] = _deque(maxlen=120)
_RADAR_QUEUE_MAX = 5000
_radar_queue: queue.Queue = queue.Queue(maxsize=_RADAR_QUEUE_MAX)
_RADAR_SENTINEL = object()
_radar_thread: threading.Thread | None = None
_radar_drops: int = 0
_radar_queue_depth_samples: _deque[int] = _deque(maxlen=120)
# Live delivery observability — counts since process start.
_ws_main_frame_drops: int = 0   # per-client send-queue full (main aircraft WS)
_ws_sync_rebuilds: int = 0      # sync snapshot rebuilds across all /sync sessions
_ws_sync_emissions: int = 0     # sync snapshots actually sent (sequence changed)
_ws_live_emissions: int = 0     # radar-live state payloads sent (change-driven)
_radar_worker_timings: _deque[dict] = _deque(maxlen=400)
_TIMING_WS_BATCH_LIMIT = 5_000
_TIMING_PAGE_BATCH_LIMIT = 60_000
_AGGREGATE_REBUILD_INTERVAL_S = 0.25
_RADAR_IID_REBUILD_INTERVAL_S = max(1.0, config.RADAR_IID_WS_REBUILD_INTERVAL_S)
_radar_loop_timings: _deque[dict] = _deque(maxlen=240)
_fm_run_timings: _deque[dict] = _deque(maxlen=240)
_fm_worker_timings: _deque[dict] = _deque(maxlen=400)
_radar_ws_timings: _deque[dict] = _deque(maxlen=400)
_RADAR_CORE_POSITION_INTERVAL_S = 2.0
_RADAR_CORE_POSITION_REFRESH_S = 10.0
_RADAR_CORE_POSITION_MIN_MOVE_DEG = 0.00005  # ~5 m latitude; cheap flood guard.
_RADAR_CORE_POSITION_ALT_DELTA_FT = 100
_radar_core_position_last_sent: dict[str, tuple[float, float, int | None, float]] = {}
_radar_core_position_sent: int = 0
_radar_core_position_skipped: int = 0
_timing_ws_timings: _deque[dict] = _deque(maxlen=400)
_BACKGROUND_QUEUE_BACKLOG_SKIP = 100
_FM_MAX_IIDS_PER_CYCLE = 1
_CI_MAX_IIDS_PER_CYCLE = 1
_RADAR_BATCH_SIZE = 16
_RADAR_BATCH_SIZE_MAX = 128
_RADAR_BATCH_BACKLOG_THRESHOLD = 32

# Shared snapshot cache — _notify_cast_loop and _broadcast_loop run on the same
# asyncio thread, so there is no race.  Both loops reuse a snapshot built within
# the same cycle, avoiding a second get_snapshot() call per second.
_snapshot_cache: dict | None = None
_snapshot_cache_ts: float = 0.0
_snapshot_cache_mode: str = ""
_SNAPSHOT_CACHE_TTL_S: float = 0.5   # half a cycle — enough to cover both loops

# Unattended trail accumulation — housekeeping records track points at this
# interval so connecting clients see recent trails even after a no-client period.
# 10 s = 2× TrackStore.SAMPLE_INTERVAL_S; when clients ARE connected the
# TrackStore rate limiter makes these calls no-ops, so there is no double work.
_TRAIL_HOUSEKEEPING_INTERVAL_S: float = 10.0
_trail_housekeeping_last_ts: float = 0.0
_VISIT_MERGE_INTERVAL_S: float = 86400.0  # once per day
_visit_merge_last_ts: float = 0.0


def _radar_core_runtime_stats(radar_state_stats: dict | None = None) -> dict:
    enabled = bool(config.RADAR_CORE_ENABLED)
    frames_enabled = bool(enabled and config.RADAR_CORE_FRAMES_ENABLED)
    managed_mode = bool(enabled and config.RADAR_CORE_MANAGED)

    client_stats = _radar_core_client.stats() if _radar_core_client is not None else {}
    worker_stats = _radar_core_worker.stats() if _radar_core_worker is not None else {}
    latest_health = client_stats.get("latest_health") or {}
    latest_snapshot = client_stats.get("latest_snapshot") or {}
    go_frames_emitted = latest_snapshot.get("frames_emitted", latest_health.get("fe"))

    memory_stats = radar_state_stats if radar_state_stats is not None else radar_state.get_memory_stats()
    python_legacy_total = int(memory_stats.get("python_frames_finalized_total", 0))
    go_injected_total = int(memory_stats.get("radar_core_frames_injected", 0))

    return {
        "enabled": enabled,
        "frames_enabled": frames_enabled,
        "backend_managed_autostart": managed_mode,
        "mode": (
            "disabled"
            if not enabled else
            "backend_managed" if managed_mode else
            "external"
        ),
        "socket_path": config.RADAR_CORE_SOCKET,
        "binary_path": config.RADAR_CORE_BINARY,
        "client_connected": bool(client_stats.get("connected", False)),
        "events_sent_to_worker": int(client_stats.get("events_sent", 0)),
        "position_updates_sent_to_worker": int(client_stats.get("position_updates_sent", 0)),
        "events_dropped_before_send": int(client_stats.get("events_dropped", 0)),
        "go_frames_emitted": go_frames_emitted,
        "go_frames_received_by_python": int(client_stats.get("frames_received", 0)),
        "go_frames_injected_into_python": go_injected_total,
        "go_frame_inject_errors": int(memory_stats.get("radar_core_frame_inject_errors", 0)),
        "python_frame_builder_active": not frames_enabled,
        "python_frame_builder_status": (
            "suppressed_by_go_frames" if frames_enabled else "active"
        ),
        "python_frame_builder_frames_legacy_total": python_legacy_total,
        "position_updates_queued": _radar_core_position_sent,
        "position_updates_skipped": _radar_core_position_skipped,
        "position_cache_entries": len(_radar_core_position_last_sent),
        "position_interval_s": _RADAR_CORE_POSITION_INTERVAL_S,
        "position_refresh_s": _RADAR_CORE_POSITION_REFRESH_S,
        "client": client_stats,
        "worker": worker_stats,
    }


def _start_msg_processor() -> threading.Thread:
    """Start the daemon thread that decodes Beast messages from _msg_queue.
    Uses make_pause_aware_decoder so the benchmark can pause it cleanly."""
    _run = make_pause_aware_decoder(_msg_queue, state, _DECODE_SENTINEL, radar_event_sink=_enqueue_radar_event)
    t = threading.Thread(target=_run, daemon=True, name="beast-decoder")
    t.start()
    return t


def _enqueue_radar_event(radar_event: tuple[int, int, str, float | None]) -> None:
    global _radar_drops
    try:
        _radar_queue.put_nowait(radar_event)
    except queue.Full:
        _radar_drops += 1


def _start_radar_processor() -> threading.Thread:
    def _target_radar_batch_size(current_qsize: int) -> int:
        if current_qsize < _RADAR_BATCH_BACKLOG_THRESHOLD:
            return _RADAR_BATCH_SIZE
        return min(_RADAR_BATCH_SIZE_MAX, max(_RADAR_BATCH_SIZE, current_qsize + 1))

    def _run() -> None:
        while True:
            t_wait_start = time.perf_counter()
            item = _radar_queue.get()
            queue_wait_s = time.perf_counter() - t_wait_start
            if item is _RADAR_SENTINEL:
                return
            try:
                batch = [item]
                target_batch_size = _target_radar_batch_size(_radar_queue.qsize())
                t_batch_fill_start = time.perf_counter()
                for _ in range(target_batch_size - 1):
                    try:
                        next_item = _radar_queue.get_nowait()
                        if next_item is _RADAR_SENTINEL:
                            break
                        batch.append(next_item)
                    except queue.Empty:
                        break

                batch_fill_s = time.perf_counter() - t_batch_fill_start
                t_process_start = time.perf_counter()
                t_process_cpu_start = time.thread_time()
                radar_state.on_df11_batch(batch)
                process_cpu_s = time.thread_time() - t_process_cpu_start
                process_wall_s = time.perf_counter() - t_process_start
                _radar_worker_timings.append({
                    "ts_s": time.time(),
                    "queue_wait_ms": round(queue_wait_s * 1000, 2),
                    "batch_fill_ms": round(batch_fill_s * 1000, 2),
                    "process_wall_ms": round(process_wall_s * 1000, 2),
                    "process_cpu_ms": round(process_cpu_s * 1000, 2),
                    "process_offcpu_ms": round(max(0.0, process_wall_s - process_cpu_s) * 1000, 2),
                    "batch_size": len(batch),
                    "batch_target": target_batch_size,
                })
            except Exception:
                log.exception("radar-worker: unhandled error")

    t = threading.Thread(target=_run, daemon=True, name="radar-worker")
    t.start()
    return t


def _get_cycle_snapshot(mode: str = "full") -> dict:
    """Return a snapshot, reusing the cached one if it was built in this cycle.

    Both async loops run on the same event thread so the cache is never written
    concurrently.  The TTL is half a push interval — short enough that stale data
    is never served, long enough to cover both loops landing in the same cycle.
    """
    global _snapshot_cache, _snapshot_cache_ts, _snapshot_cache_mode
    now = time.time()
    if (
        _snapshot_cache is not None
        and _snapshot_cache_mode == mode
        and now - _snapshot_cache_ts < _SNAPSHOT_CACHE_TTL_S
    ):
        return _snapshot_cache
    snap = state.get_snapshot(mode=mode)
    _snapshot_cache = snap
    _snapshot_cache_ts = now
    _snapshot_cache_mode = mode
    return snap


# ---------------------------------------------------------------------------
# Background tasks
# ---------------------------------------------------------------------------

async def _beast_runner() -> None:
    def on_message(msg: dict) -> None:
        global _msg_drops
        try:
            _msg_queue.put_nowait((msg, None))
        except queue.Full:
            _msg_drops += 1  # decoder is behind; count the loss

    client = BeastClient(config.BEAST_HOST, config.BEAST_PORT, on_message)
    await client.run()


async def _mlat_runner(name: str, host: str, port: int) -> None:
    def on_mlat_message(msg: dict) -> None:
        global _msg_drops
        try:
            _msg_queue.put_nowait((msg, name))
        except queue.Full:
            _msg_drops += 1  # decoder is behind; count the loss

    client = BeastClient(host, port, on_mlat_message)
    log.info("MLAT runner starting: %s (%s:%s)", name, host, port)
    await client.run()


async def _db_update_checker() -> None:
    await asyncio.sleep(10)
    mirror_completed = await asyncio.to_thread(enrichment.db.check_for_updates)
    if mirror_completed:
        # Cold-start: tar1090 mirror just finished.  Re-enqueue every live aircraft
        # so _hexdb_task gives them a tar1090 lookup now that the shards are on disk.
        state.seed_hexdb_queue(list(state.get_icaos()))
        log.info("tar1090 mirror cold-start complete — re-enqueued %d aircraft", len(state.get_icaos()))
    while True:
        await asyncio.sleep(86400)
        await asyncio.to_thread(enrichment.db.check_for_updates)


async def _hexdb_cache_flusher() -> None:
    """Flush the hexdb cache to SD at most every 5 minutes, only when dirty.
    Batches all lookups since the last flush into a single gzip write."""
    while True:
        await asyncio.sleep(300)
        await asyncio.to_thread(enrichment.db.flush_hexdb_cache_if_dirty)


async def _backup_runner() -> None:
    """Nightly backup at local midnight. Uses DB-configured path (falls back to env var)."""
    while True:
        now = datetime.now()
        next_midnight = datetime(now.year, now.month, now.day) + timedelta(days=1)
        await asyncio.sleep((next_midnight - now).total_seconds())
        try:
            backup_path, _ = await asyncio.to_thread(stats_db.get_effective_backup_config)
            if backup_path:
                backup_path.mkdir(parents=True, exist_ok=True)
                await asyncio.to_thread(stats_db.backup, backup_path)
        except Exception:
            log.exception("Nightly backup failed")


async def _adsbx_task() -> None:
    """Drain the ADSBx enrichment queue for newly-seen aircraft.

    Runs every 0.5s, up to 20 ICAOs per cycle. All SQLite lookups for the batch
    run in a single asyncio.to_thread call — one thread per cycle instead of N —
    to minimise GIL contention with the decoder thread.
    """
    await asyncio.sleep(2)  # brief startup delay — let the decoder warm up first
    while True:
        batch = state.pop_adsbx_queue(max_n=20)
        if batch:
            def _lookup_batch(icaos: set) -> dict:
                return {icao: enrichment.db.get_adsbx(icao) for icao in icaos}
            results = await asyncio.to_thread(_lookup_batch, batch)
            for icao, adsbx in results.items():
                state.apply_adsbx(icao, adsbx)
        await asyncio.sleep(0.5)


def _pick_first(*values) -> str | None:
    """Return the first non-empty string value; strips whitespace from str values."""
    for value in values:
        text = (value or "").strip() if isinstance(value, str) else value
        if text:
            return text
    return None


async def _enrich_live_icao(icao: str) -> None:
    """Fetch enrichment for one queued ICAO (adsbx → tar1090 → hexdb) and apply it."""
    cache = {
        "adsbx":   enrichment.db.get_adsbx_cached(icao) is not None,
        "tar1090": enrichment.db.get_tar1090_cached(icao) is not None,
        "hexdb":   enrichment.db.get_hexdb_cached(icao) is not None,
    }
    adsbx   = await asyncio.to_thread(enrichment.db.get_adsbx, icao)
    tar1090 = await asyncio.to_thread(enrichment.db.get_tar1090, icao)

    registration = _pick_first(
        adsbx.get("reg") if adsbx else None,
        tar1090.get("Registration") if tar1090 else None,
    )
    type_code = _pick_first(
        adsbx.get("icaotype") if adsbx else None,
        tar1090.get("ICAOTypeCode") if tar1090 else None,
    )
    operator = _pick_first(
        adsbx.get("ownop") if adsbx else None,
        tar1090.get("RegisteredOwners") if tar1090 else None,
    )
    manufacturer = _pick_first(
        adsbx.get("manufacturer") if adsbx else None,
        tar1090.get("Manufacturer") if tar1090 else None,
    )

    hexdb = None
    if not registration or not type_code or not operator or not manufacturer:
        hexdb = await asyncio.to_thread(enrichment.db.lookup_hexdb, icao)

    if tar1090:
        state.apply_hexdb(icao, tar1090)
    if hexdb:
        state.apply_hexdb(icao, hexdb)

    # Re-resolve with hexdb so offline aircraft registry also gets updated.
    registration  = _pick_first(registration, hexdb.get("Registration") if hexdb else None)
    type_code     = _pick_first(type_code, hexdb.get("ICAOTypeCode") if hexdb else None)
    manufacturer  = _pick_first(manufacturer, hexdb.get("Manufacturer") if hexdb else None)
    type_category = None
    if type_code:
        ti = enrichment.db.get_type_info(type_code)
        if ti:
            type_category = ti.get("desc") or None

    if not operator and hexdb:
        flag_code = (hexdb.get("OperatorFlagCode") or "").strip()
        if flag_code:
            op = enrichment.db.get_operator(flag_code)
            if op:
                operator = op.get("n")
        if not operator:
            operator = (hexdb.get("RegisteredOwners") or "").strip() or None

    pending = not all([registration, type_code, operator, manufacturer])
    resolved_sources = {
        "registration": (
            "adsbx" if adsbx and registration == _pick_first(adsbx.get("reg")) else
            "tar1090" if tar1090 and registration == _pick_first(tar1090.get("Registration")) else
            "hexdb" if hexdb and registration == _pick_first(hexdb.get("Registration")) else "—"
        ),
        "type_code": (
            "adsbx" if adsbx and type_code == _pick_first(adsbx.get("icaotype")) else
            "tar1090" if tar1090 and type_code == _pick_first(tar1090.get("ICAOTypeCode")) else
            "hexdb" if hexdb and type_code == _pick_first(hexdb.get("ICAOTypeCode")) else "—"
        ),
        "operator": (
            "adsbx" if adsbx and operator == _pick_first(adsbx.get("ownop")) else
            "tar1090" if tar1090 and operator == _pick_first(tar1090.get("RegisteredOwners")) else
            "hexdb" if hexdb and operator == _pick_first(hexdb.get("RegisteredOwners")) else
            "hexdb_flag" if hexdb and operator else "—"
        ),
        "manufacturer": (
            "adsbx" if adsbx and manufacturer == _pick_first(adsbx.get("manufacturer")) else
            "tar1090" if tar1090 and manufacturer == _pick_first(tar1090.get("Manufacturer")) else
            "hexdb" if hexdb and manufacturer == _pick_first(hexdb.get("Manufacturer")) else "—"
        ),
        "year":    "adsbx" if adsbx and _pick_first(adsbx.get("year")) else "—",
        "military": "adsbx" if adsbx else "—",
    }
    enrichment.log_enrichment_trace(
        icao, "live:queued",
        cache=cache, adsbx=adsbx, tar1090=tar1090, hexdb=hexdb,
        final={
            "registration": registration, "type_code": type_code,
            "operator": operator, "manufacturer": manufacturer,
            "year": _pick_first(adsbx.get("year") if adsbx else None),
            "military": "Y" if bool(adsbx and adsbx.get("mil")) else "N",
            "type_category": type_category,
        },
        resolved_sources=resolved_sources,
        pending=pending,
    )

    if registration or type_code or operator or manufacturer:
        await asyncio.to_thread(
            stats_db.update_aircraft_enrichment,
            icao, registration, type_code, type_category, operator, manufacturer,
        )


async def _hexdb_task() -> None:
    """Process deferred live enrichment in order: ADSBx, tar1090, then hexdb for gaps."""
    await asyncio.sleep(5)  # brief startup delay
    while True:
        batch = state.pop_hexdb_queue(max_n=10)
        for icao in batch:
            await _enrich_live_icao(icao)
            await asyncio.sleep(1)  # 1 req/sec rate limit
        await asyncio.sleep(5)


_last_wal_checkpoint: float = 0.0
_WAL_CHECKPOINT_INTERVAL = 3600.0  # 1 hour


def _wal_checkpoint_passive() -> None:
    """Run a PASSIVE WAL checkpoint — returns immediately, doesn't block readers."""
    try:
        with stats_db._connect() as conn:
            conn.execute("PRAGMA wal_checkpoint(PASSIVE)")
        log.debug("Periodic WAL checkpoint complete")
    except Exception:
        log.warning("Periodic WAL checkpoint failed", exc_info=True)


def _is_credible(icao: str, msg_count: int, mlat: bool) -> bool:
    """Core ghost-filter logic shared by both Aircraft-object and dict callers."""
    if config.GHOST_FILTER_MSGS <= 0:
        return True
    if msg_count >= config.GHOST_FILTER_MSGS:
        return True
    # MLAT-confirmed aircraft are real (multilaterated by multiple receivers)
    if mlat:
        return True
    if enrichment.db.get_adsbx(icao):
        return True
    if enrichment.db.get_hexdb_cached(icao):
        return True
    if enrichment.db.get_tar1090_cached(icao):
        return True
    return False


def _credible_aircraft(ac) -> bool:
    """Ghost filter for Aircraft dataclass objects (used at visit close time)."""
    return _is_credible(ac.icao, ac.msg_count, bool(ac.mlat))


def _ghost_credible(ac: dict) -> bool:
    """Return True if this aircraft (dict snapshot) is likely real and should be persisted."""
    return _is_credible(ac["icao"], ac.get("msg_count", 0), bool(ac.get("mlat")))


async def _process_emergency_squawks(snapshot: dict, now_ts: int) -> None:
    """Update the in-memory emergency-squawk state machine and write confirmed events to DB.

    A squawk must be observed ≥5 times across snapshots AND sustained for ≥120 s before
    it is written to DB and a notification is fired — this dual gate filters both
    transient code scrolling (clears in seconds) and noisy-receiver bursts (many obs,
    short-lived).  Cleared squawks are finalized in place.
    """
    squawking_icaos: set[str] = set()
    for ac in snapshot["aircraft"]:
        sq = ac.get("squawk") or ""
        if sq not in EMERGENCY_SQUAWKS:
            continue
        icao = ac["icao"]
        squawking_icaos.add(icao)
        if icao in _active_squawks and _active_squawks[icao]["squawk"] == sq:
            entry = _active_squawks[icao]
            entry["obs_count"] += 1
            if entry["db_id"] is None:
                if entry["obs_count"] >= 5 and now_ts - entry["first_seen"] >= 120:
                    db_id = await asyncio.to_thread(
                        stats_db.write_squawk_event,
                        icao, sq, ac.get("callsign"), ac.get("altitude"),
                        entry["first_seen"], now_ts,
                    )
                    entry["db_id"] = db_id
                    entry["last_update"] = now_ts
                    log.info("Emergency squawk %s from %s confirmed", sq, icao)
                    await asyncio.to_thread(
                        notifications.notify_emergency_squawk,
                        icao, sq, ac.get("callsign"), ac.get("altitude"), ac.get("operator"),
                    )
            else:
                # Ongoing confirmed event — update ts_last every 30s
                if now_ts - entry["last_update"] >= 30:
                    await asyncio.to_thread(
                        stats_db.update_squawk_event_last,
                        entry["db_id"], now_ts, ac.get("altitude"),
                    )
                    entry["last_update"] = now_ts
        else:
            # New event or squawk code changed — finalize any existing confirmed
            # event before opening a fresh one so the DB record isn't left open.
            existing = _active_squawks.get(icao)
            if existing is not None and existing["db_id"] is not None:
                await asyncio.to_thread(
                    stats_db.update_squawk_event_last,
                    existing["db_id"], now_ts, None,
                )
            _active_squawks[icao] = {
                "squawk": sq, "db_id": None,
                "first_seen": now_ts, "last_update": now_ts,
                "obs_count": 1,
            }
    # Close out events for aircraft no longer squawking emergency
    for icao in list(_active_squawks.keys()):
        if icao not in squawking_icaos:
            entry = _active_squawks.pop(icao)
            if entry["db_id"] is not None:
                await asyncio.to_thread(
                    stats_db.update_squawk_event_last,
                    entry["db_id"], now_ts, None,
                )


async def _db_writer() -> None:
    """Write completed minute stats to SQLite every minute; roll up at day boundary."""
    import time
    last_day = date.today().isoformat()
    while True:
        await asyncio.sleep(60)
        today = date.today().isoformat()
        if today != last_day:
            await asyncio.to_thread(stats_db.rollup_yesterday)
            await asyncio.to_thread(stats_db.prune)
            notifications.reset_daily()
            last_day = today
        snapshot = state.get_snapshot()
        # Filter out ghost aircraft (bogus CRC decodes) before persisting
        if config.GHOST_FILTER_MSGS > 0:
            credible = [ac for ac in snapshot.get("aircraft", []) if _ghost_credible(ac)]
            snapshot = {**snapshot, "aircraft": credible}
        await asyncio.to_thread(stats_db.write_minute, snapshot)

        # Periodic WAL checkpoint — keeps the WAL file small between restarts.
        # PASSIVE mode doesn't block readers or writers.
        import time as _time
        global _last_wal_checkpoint
        if _time.monotonic() - _last_wal_checkpoint >= _WAL_CHECKPOINT_INTERVAL:
            await asyncio.to_thread(_wal_checkpoint_passive)
            _last_wal_checkpoint = _time.monotonic()

        # Refresh in-memory sighting counts from DB so NEW badge stays accurate
        current_icaos = [ac["icao"] for ac in snapshot.get("aircraft", [])]
        if current_icaos:
            fresh_counts = await asyncio.to_thread(
                stats_db.query_sighting_counts_for_icaos, current_icaos
            )
            state.update_sighting_counts(fresh_counts)

        # Write coverage samples for aircraft that have a position
        if config.RECEIVER_LAT is not None and config.RECEIVER_LON is not None:
            now_ts = int(time.time())
            samples = (
                (now_ts, ac["icao"], ac["bearing_deg"], ac["range_nm"],
                 (ac.get("altitude")
                  if ((ac.get("last_pos_age") is not None and ac.get("last_pos_age") <= config.POS_FRESH_S)
                      and (ac.get("last_alt_age") is not None and ac.get("last_alt_age") <= config.ALT_FRESH_S))
                  else None),
                 ac.get("signal_raw"),
                 1 if ac.get("mlat") else 0)
                for ac in snapshot.get("aircraft", [])
                if (ac.get("bearing_deg") is not None and ac.get("range_nm") is not None
                        and ac.get("last_pos_age") is not None
                        and ac.get("last_pos_age") <= config.POS_FRESH_S)
            )
            await asyncio.to_thread(stats_db.write_coverage_tuples, samples)

        # Drain pending ACAS events to DB and fire notifications
        acas_evts = state.pop_acas_events()
        if acas_evts:
            await asyncio.to_thread(stats_db.write_acas_events, acas_evts)
            for evt in acas_evts:
                await asyncio.to_thread(
                    notifications.notify_acas,
                    evt["icao"], evt["ra_description"], bool(evt.get("ra_corrective")),
                    None, None, evt.get("altitude"),   # reg/operator resolved later by DB join
                )

        # Detect emergency squawk start/continuation/end
        await _process_emergency_squawks(snapshot, int(time.time()))


async def _route_enricher() -> None:
    """Resolve origin/destination airports for completed visits via adsbdb.com."""
    import urllib.request
    import urllib.error
    while True:
        if not _route_queue:
            await asyncio.sleep(10)
            continue
        visit_id, callsign = _route_queue.popleft()

        def _lookup(cs: str) -> tuple[str | None, str | None]:
            url = f"https://api.adsbdb.com/v0/callsign/{cs}"
            try:
                with urllib.request.urlopen(url, timeout=15) as resp:
                    data = json.loads(resp.read())
                route = data.get("response", {}).get("flightroute") or {}
                origin = (route.get("origin") or {}).get("icao_code") or None
                dest   = (route.get("destination") or {}).get("icao_code") or None
                return origin, dest
            except Exception:
                return None, None

        origin, dest = await asyncio.to_thread(_lookup, callsign)
        if origin or dest:
            await asyncio.to_thread(stats_db.update_visit_route, visit_id, origin, dest)
        await asyncio.sleep(0.5)  # max 2 req/s — adsbdb is a free service


def _cast_enabled() -> bool:
    """Return True when cast has enough config to trigger on live traffic."""
    global _cast_enabled_cache, _cast_enabled_cache_ts
    now = time.monotonic()
    if now - _cast_enabled_cache_ts < 10.0:
        return _cast_enabled_cache
    try:
        cfg = stats_db.get_cast_config()
        rules = stats_db.get_cast_rules()
        _cast_enabled_cache = bool(
            (cfg.get("device_name") or "").strip()
            and (cfg.get("lan_url") or "").strip()
            and any(bool(rule.get("enabled", 1)) for rule in rules)
        )
    except Exception:
        log.debug("cast: config probe failed", exc_info=True)
        _cast_enabled_cache = False
    _cast_enabled_cache_ts = now
    return _cast_enabled_cache


async def _close_expired_visits(expired: list) -> None:
    """Persist timed-out visits and queue route enrichment when needed."""
    global _route_queue_drops
    if not expired:
        return
    credible = [ac for ac in expired if _credible_aircraft(ac)]
    if not credible:
        return
    tuples = [
        (ac.icao, int(ac.first_seen), int(ac.last_seen),
         ac.callsign, ac.squawk, ac.max_altitude, ac.msg_count)
        for ac in credible
    ]
    visit_ids = await asyncio.to_thread(stats_db.write_visits, tuples)
    for ac, vid in zip(credible, visit_ids):
        if ac.callsign:
            if len(_route_queue) == _route_queue.maxlen:
                _route_queue_drops += 1
                log.warning("route enrichment queue full — drop #%d (visit %d %s)",
                            _route_queue_drops, vid, ac.callsign)
            _route_queue.append((vid, ac.callsign))


def _record_track_point(ac: dict, now: float) -> None:
    """Record a track point if the aircraft has a reliable position."""
    if (
        ac.get("bearing_deg") is not None
        and ac.get("range_nm") is not None
        and ac.get("lat") is not None
        and ac.get("pos_confident")
    ):
        track_store.record(
            icao=ac["icao"],
            bearing_deg=ac["bearing_deg"],
            range_nm=ac["range_nm"],
            altitude_ft=ac.get("altitude"),
            lat=ac["lat"],
            lon=ac["lon"],
            military=bool(ac.get("military")),
            mlat=bool(ac.get("mlat")),
            interesting=bool(ac.get("interesting")),
            acas_ra_active=bool(ac.get("acas_ra_active")),
            mlat_source=ac.get("mlat_source"),
            now=now,
        )


async def _housekeeping_loop() -> None:
    """Keep unattended collection healthy without building broadcast snapshots."""
    global _trail_housekeeping_last_ts, _visit_merge_last_ts
    while True:
        await asyncio.sleep(config.PUSH_INTERVAL_S)
        try:
            expired = state.expire_aircraft()
            await _close_expired_visits(expired)
            # Prune tracks for aircraft that have left the live set.  Must run
            # after expire_aircraft() so the active set reflects post-expiry state.
            track_store.expire(state.get_icaos())

            # Low-frequency unattended trail accumulation.  Keeps recent trail
            # history available for clients that connect after a no-client period.
            # When clients ARE connected, _broadcast_loop records every cycle so
            # TrackStore's per-aircraft rate limiter makes these calls no-ops.
            now_ts = time.time()
            if now_ts - _trail_housekeeping_last_ts >= _TRAIL_HOUSEKEEPING_INTERVAL_S:
                _trail_housekeeping_last_ts = now_ts
                _trail_snap = _get_cycle_snapshot("full")
                for _ac in _trail_snap["aircraft"]:
                    _record_track_point(_ac, now_ts)

            if now_ts - _visit_merge_last_ts >= _VISIT_MERGE_INTERVAL_S:
                _visit_merge_last_ts = now_ts
                merged = await asyncio.to_thread(stats_db.merge_short_visits)
                if merged:
                    log.info("Daily visit merge: consolidated %d split visit(s)", merged)

            # Sample queue depth (Beast mode only — queue unused in readsb/hybrid).
            if config.INGEST_MODE == "beast":
                _current_queue_depth = _msg_queue.qsize()
                _queue_depth_samples.append(_current_queue_depth)
                if config.MEMORY_POLICY_ENABLED:
                    memory_policy.report_queue_depth(_current_queue_depth)
        except Exception:
            log.exception("_housekeeping_loop: unhandled error — continuing")


async def _notify_cast_loop() -> None:
    """Run live notifications/cast without tying them to WebSocket clients."""
    import time
    while True:
        await asyncio.sleep(config.PUSH_INTERVAL_S)
        try:
            _notify_enabled = notifications.any_channel()
            _cast_on = _cast_enabled()
            if not _notify_enabled and not _cast_on:
                continue

            # Use the same snapshot mode as _broadcast_loop so the cycle cache is shared.
            if config.SNAPSHOT_MODE_OVERRIDE:
                _notify_snap_mode = config.SNAPSHOT_MODE_OVERRIDE
            elif config.MEMORY_POLICY_ENABLED:
                _notify_snap_mode = memory_policy.get_policy()["snapshot_mode"]
            else:
                _notify_snap_mode = "full"
            snapshot = _get_cycle_snapshot(_notify_snap_mode)
            now = time.time()

            # Refresh watchlist cache from DB every 30s
            global _watchlist_cache, _watchlist_cache_ts
            if _notify_enabled and now - _watchlist_cache_ts > 30:
                rows = await asyncio.to_thread(stats_db.get_notify_watchlist)
                _watchlist_cache = {r["icao"]: r["max_range_nm"] for r in rows}
                _watchlist_cache_ts = now

            notify_tasks = []
            # Check per-trigger prefs once per cycle (uses cached prefs, <1µs each).
            # Avoids dispatching any threads for triggers the user has turned off.
            _mil_on  = _notify_enabled and notifications.trigger_enabled("notify_military")
            _int_on  = _notify_enabled and notifications.trigger_enabled("notify_interesting")
            # Collect matching aircraft first; dispatch at most 3 threads per cycle
            # regardless of how many aircraft match, replacing the previous O(N) dispatch.
            _mil_batch: list[dict] = []
            _int_batch: list[dict] = []
            _wl_batch:  list[dict] = []
            for ac in snapshot["aircraft"]:
                if not _notify_enabled:
                    break  # cast-only cycle: no notifications to collect
                icao = ac["icao"]
                if icao in _watchlist_cache and not notifications.already_notified(f"watchlist:{icao}"):
                    _wl_batch.append({
                        "icao": icao, "callsign": ac.get("callsign"),
                        "registration": ac.get("registration"), "operator": ac.get("operator"),
                        "altitude": ac.get("altitude"), "range_nm": ac.get("range_nm"),
                        "max_range_nm": _watchlist_cache[icao],
                    })
                if _mil_on and ac.get("military") and not notifications.already_notified(f"military:{icao}"):
                    _mil_batch.append({
                        "icao": icao, "callsign": ac.get("callsign"),
                        "operator": ac.get("operator"), "country": ac.get("country"),
                        "altitude": ac.get("altitude"), "range_nm": ac.get("range_nm"),
                    })
                if _int_on and ac.get("interesting") and not notifications.already_notified(f"interesting:{icao}"):
                    _int_batch.append({
                        "icao": icao, "callsign": ac.get("callsign"),
                        "type_code": ac.get("type_code"), "operator": ac.get("operator"),
                        "altitude": ac.get("altitude"), "range_nm": ac.get("range_nm"),
                    })
            if _mil_batch:
                notify_tasks.append(asyncio.to_thread(notifications.notify_military_batch, _mil_batch))
            if _int_batch:
                notify_tasks.append(asyncio.to_thread(notifications.notify_interesting_batch, _int_batch))
            if _wl_batch:
                notify_tasks.append(asyncio.to_thread(notifications.notify_watchlist_batch, _wl_batch))

            if notify_tasks:
                await asyncio.gather(*notify_tasks)

            if _cast_on:
                # Cast check — non-blocking; dispatches I/O to thread internally
                try:
                    cast.check(snapshot["aircraft"])
                except Exception:
                    log.exception("cast: unhandled error in check()")
        except Exception:
            log.exception("_notify_cast_loop: unhandled error — continuing")

 
async def _broadcast_loop() -> None:
    """Broadcast a state snapshot to every connected WebSocket client."""
    import time
    while True:
        await asyncio.sleep(config.PUSH_INTERVAL_S)
        if not _clients:
            continue
        try:
            if config.SNAPSHOT_MODE_OVERRIDE:
                _snap_mode = config.SNAPSHOT_MODE_OVERRIDE
            elif config.MEMORY_POLICY_ENABLED:
                _snap_mode = memory_policy.get_policy()["snapshot_mode"]
            else:
                _snap_mode = "full"
            t_snap = time.perf_counter()
            snapshot = _get_cycle_snapshot(_snap_mode)
            snapshot_ms = (time.perf_counter() - t_snap) * 1000

            # Record track points (rate-limited to 1/5s per aircraft inside TrackStore)
            # pos_confident = _pos_reliable() in snapshot; requires a global CPR decode
            # or MLAT fix before a point is accepted.
            now = time.time()
            t_loop_start = time.perf_counter()
            for ac in snapshot["aircraft"]:
                _record_track_point(ac, now)
            t_sync_end = time.perf_counter()

            t_ser = time.perf_counter()
            payload = _json_dumps(snapshot)
            serialize_ms = (time.perf_counter() - t_ser) * 1000

            # Enqueue to each client's send queue — non-blocking, O(1) per client.
            # Clients that fall behind (QueueFull) get this frame dropped; they
            # receive the next snapshot instead.  Actual WS sends happen in the
            # per-client sender coroutine started by websocket_endpoint.
            ws_frames_dropped = 0
            for q in list(_clients.values()):
                try:
                    q.put_nowait(payload)
                except asyncio.QueueFull:
                    ws_frames_dropped += 1
            global _ws_main_frame_drops
            _ws_main_frame_drops += ws_frames_dropped

            t_done = time.perf_counter()
            push_sample = {
                "ts_s":              now,
                "sync_ms":           round((t_sync_end - t_loop_start) * 1000, 2),
                "snapshot_ms":       round(snapshot_ms, 2),
                "gather_ms":         0.0,
                "notify_tasks":      0,
                "broadcast_ms":      round((t_done - t_sync_end) * 1000, 2),
                "serialize_ms":      round(serialize_ms, 2),
                "total_ms":          round((t_done - t_loop_start) * 1000, 2),
                "ac_count":          len(snapshot["aircraft"]),
                "payload_bytes":     len(payload),
                "ws_client_count":   len(_clients),
                "ws_clients_dropped": ws_frames_dropped,
                "ws_send_max_ms":    0.0,
            }
            _push_timings_store.append(push_sample)
            _record_push_perf_sample(push_sample)
            _radar_queue_depth_samples.append(_radar_queue.qsize())
        except Exception:
            log.exception("_broadcast_loop: unhandled error in broadcast cycle — continuing")


def _should_forward_radar_core_position(ac: dict, now: float) -> bool:
    """Return True when a live position is fresh, confident, and worth sending."""
    global _radar_core_position_skipped
    if ac.get("lat") is None or ac.get("lon") is None:
        _radar_core_position_skipped += 1
        return False
    if not ac.get("pos_confident"):
        _radar_core_position_skipped += 1
        return False
    last_pos_ts = ac.get("last_pos_ts")
    if not last_pos_ts or now - float(last_pos_ts) > config.POS_FRESH_S:
        _radar_core_position_skipped += 1
        return False

    icao = ac.get("icao")
    if not icao:
        _radar_core_position_skipped += 1
        return False
    lat = float(ac["lat"])
    lon = float(ac["lon"])
    alt = ac.get("altitude")
    alt_ft = int(alt) if alt is not None else None
    previous = _radar_core_position_last_sent.get(icao)
    if previous is None:
        return True
    prev_lat, prev_lon, prev_alt, prev_sent_ts = previous
    if now - prev_sent_ts >= _RADAR_CORE_POSITION_REFRESH_S:
        return True
    if abs(lat - prev_lat) >= _RADAR_CORE_POSITION_MIN_MOVE_DEG:
        return True
    if abs(lon - prev_lon) >= _RADAR_CORE_POSITION_MIN_MOVE_DEG:
        return True
    if alt_ft is not None and (prev_alt is None or abs(alt_ft - prev_alt) >= _RADAR_CORE_POSITION_ALT_DELTA_FT):
        return True
    _radar_core_position_skipped += 1
    return False


async def _radar_core_position_loop() -> None:
    """Forward fresh confident ADS-B positions to radar-core at a bounded live cadence."""
    global _radar_core_position_sent
    while True:
        await asyncio.sleep(_RADAR_CORE_POSITION_INTERVAL_S)
        if _radar_core_client is None:
            continue
        try:
            now = time.time()
            positions = state.get_positions_snapshot()
            active_icaos = {str(ac.get("icao")) for ac in positions if ac.get("icao")}
            for stale_icao in list(_radar_core_position_last_sent):
                if stale_icao not in active_icaos:
                    _radar_core_position_last_sent.pop(stale_icao, None)

            for ac in positions:
                if not _should_forward_radar_core_position(ac, now):
                    continue
                try:
                    icao = str(ac["icao"])
                    icao_int = int(icao, 16)
                    lat = float(ac["lat"])
                    lon = float(ac["lon"])
                    alt = ac.get("altitude")
                    alt_ft = int(alt) if alt is not None else None
                    ts = float(ac.get("last_pos_ts") or now)
                except (TypeError, ValueError):
                    continue

                _radar_core_client.send_position_update(icao_int, lat, lon, alt_ft, ts)
                _radar_core_position_last_sent[icao] = (lat, lon, alt_ft, now)
                _radar_core_position_sent += 1
        except Exception:
            log.exception("_radar_core_position_loop: unhandled error — continuing")


async def _hires_writer() -> None:
    """Record aircraft positions to the in-memory hires buffer every 10 seconds.
    Applies the same ghost filter and value guards as the DB coverage writer."""
    import time as _time
    while True:
        await asyncio.sleep(hires_buffer.HIRES_INTERVAL_S)
        if config.RECEIVER_LAT is None or config.RECEIVER_LON is None:
            continue
        snapshot = state.get_snapshot()
        now_ts = int(_time.time())
        samples = [
            (now_ts, ac["icao"],
             ac["bearing_deg"], ac["range_nm"],
             # Same freshness gate as _db_writer — suppress stale altitude
             (ac.get("altitude")
              if ((ac.get("last_pos_age") is not None and ac.get("last_pos_age") <= config.POS_FRESH_S)
                  and (ac.get("last_alt_age") is not None and ac.get("last_alt_age") <= config.ALT_FRESH_S))
              else None),
             ac.get("military", False), ac.get("interesting", False),
             ac.get("type_code"), ac.get("type_category"), ac.get("operator"),
             bool(ac.get("mlat")))
            for ac in snapshot.get("aircraft", [])
            if (ac.get("bearing_deg") is not None
                and ac.get("range_nm") is not None
                and (ac.get("range_nm") or 0) > 0
                and _ghost_credible(ac))
        ]
        hires_buffer.record(samples)


def _apply_memory_policy(level: str) -> None:
    policy = memory_policy.get_policy()
    hires_buffer.set_policy(policy["hires_max_age_s"], policy["hires_interval_s"])
    hires_buffer.prune_now()
    new_timeout = config.AIRCRAFT_TIMEOUT // 2 if policy["halve_timeout"] else config.AIRCRAFT_TIMEOUT
    state.set_timeout(new_timeout)
    log.info(
        "memory_policy applied: level=%s hires_age=%ds hires_interval=%ds "
        "snapshot=%s aircraft_timeout=%ds",
        level, policy["hires_max_age_s"], policy["hires_interval_s"],
        policy["snapshot_mode"], new_timeout,
    )


async def _memory_guard() -> None:
    """Poll memory pressure every 10 s and apply policy to consumers.

    On level escalation (worsening): immediately shrinks hires_buffer
    retention and forces a prune, halves aircraft timeout if High/Critical.
    On de-escalation (improving): restores defaults after sustained readings
    (handled inside memory_policy.check() via hysteresis counter).
    """
    prev_level = memory_policy.check()
    _apply_memory_policy(prev_level)
    while True:
        await asyncio.sleep(10)
        new_level = memory_policy.check()
        if new_level == prev_level:
            continue
        prev_level = new_level
        _apply_memory_policy(new_level)


# ---------------------------------------------------------------------------
# App lifespan
# ---------------------------------------------------------------------------

async def _seed_startup_state() -> None:
    """Run DB rollups and seed in-memory state from the database at startup."""
    await asyncio.to_thread(enrichment.db.load_or_download)
    await asyncio.to_thread(stats_db.rollup_missed_days)
    await asyncio.to_thread(stats_db.prune)
    await asyncio.to_thread(stats_db.backfill_daily_coverage)
    await asyncio.to_thread(stats_db.backfill_us_mil_years)

    # Merge visits split by the previous restart before serving any data.
    # Reset the housekeeping timer so the daily task doesn't duplicate this run.
    global _visit_merge_last_ts
    merged = await asyncio.to_thread(stats_db.merge_short_visits)
    _visit_merge_last_ts = time.time()
    if merged:
        log.info("Startup visit merge: consolidated %d split visit(s)", merged)

    # Seed today's unique-aircraft sets from DB so counts survive restarts
    today = date.today().isoformat()
    today_data = await asyncio.to_thread(stats_db.query_today_icaos, today)
    state.init_today(today_data["all"], today_data["military"])

    # Seed sighting counts so live aircraft correctly reflect DB state.
    # Only load aircraft seen in the last 90 days — avoids pulling the full
    # registry into RAM on mature installs with years of history.
    sighting_counts = await asyncio.to_thread(stats_db.query_sighting_counts_recent, 90)
    state.seed_sighting_counts(sighting_counts)
    log.info("Seeded sighting counts for %d aircraft (last 90 days)", len(sighting_counts))

    # Queue aircraft with missing enrichment fields for hexdb re-lookup
    needs_enrichment = await asyncio.to_thread(stats_db.query_needs_enrichment, 500)
    state.seed_hexdb_queue(needs_enrichment)
    log.info("Queued %d aircraft for re-enrichment", len(needs_enrichment))

    # Purge ghost aircraft accumulated before the filter was in place
    if config.GHOST_FILTER_MSGS > 0:
        await asyncio.to_thread(stats_db.purge_ghost_aircraft)

    # Load Stage 3 bearing calibrations from DB
    if config.STAGE3_ENABLED:
        cal_rows = await asyncio.to_thread(stats_db.load_radar_bearing_calibrations)
        aircraft_localiser.load_calibrations(cal_rows)

    # Correct country/foreign_military for all military aircraft using ICAO block.
    # Repairs entries written before the registration-prefix bug was fixed.
    mil_icaos = await asyncio.to_thread(stats_db.query_military_icaos)
    corrections = {
        icao: enrichment.db.get_country_by_icao(icao)
        for icao in mil_icaos
        if enrichment.db.get_country_by_icao(icao)
    }
    await asyncio.to_thread(stats_db.fix_military_countries, corrections, config.HOME_COUNTRY)


async def _graceful_shutdown(bg_tasks: list) -> None:
    """Cancel background tasks and flush all state to disk."""
    log.info("Shutdown: cancelling background tasks…")
    for t in bg_tasks:
        t.cancel()
    await asyncio.gather(*bg_tasks, return_exceptions=True)

    # Stop the decoder thread before the final DB write so it can't be
    # holding _lock when we call get_snapshot() below.
    log.info("Shutdown: stopping decoder thread…")
    _msg_queue.put(_DECODE_SENTINEL)
    if _decoder_thread is not None:
        _decoder_thread.join(timeout=2.0)
    log.info("Shutdown: stopping radar thread…")
    _radar_queue.put(_RADAR_SENTINEL)
    if _radar_thread is not None:
        _radar_thread.join(timeout=2.0)

    log.info("Shutdown: stopping FM worker…")
    _fm_worker_stop.set()
    try:
        radar_state._fm_mailbox_event.set()  # wake the worker immediately
    except Exception:
        pass
    if _fm_worker_thread is not None:
        _fm_worker_thread.join(timeout=2.0)

    log.info("Shutdown: closing in-progress visits…")
    try:
        await _close_expired_visits(state.drain_all())
    except Exception:
        log.exception("Shutdown: visit close-out failed")

    log.info("Shutdown: flushing state to disk…")
    try:
        final_snapshot = state.get_snapshot()
        await asyncio.to_thread(stats_db.write_minute, final_snapshot)
        # write_minute() may not flush the registry buffer if the interval hasn't
        # elapsed — force a final flush so no aircraft data is lost on clean shutdown.
        await asyncio.to_thread(stats_db.flush_registry_now)
    except Exception:
        log.exception("Shutdown: final DB write failed")

    try:
        enrichment.db.flush_hexdb_cache_if_dirty()
    except Exception:
        log.exception("Shutdown: hexdb cache flush failed")

    # Checkpoint the WAL so next startup opens a clean DB without recovery.
    try:
        with stats_db._connect() as conn:
            conn.execute("PRAGMA wal_checkpoint(TRUNCATE)")
        log.info("Shutdown: WAL checkpoint complete")
    except Exception:
        log.exception("Shutdown: WAL checkpoint failed")


async def _radar_loop() -> None:
    """Background task: update radar rotation models every 30s, flush DB every 60s."""
    # Load persisted models on startup
    try:
        db_models = await asyncio.to_thread(stats_db.load_radar_iids)
        radar_state.load_from_db(db_models)
    except Exception:
        log.exception("Radar: failed to load persisted IID models")

    iteration = 0
    while True:
        await asyncio.sleep(30)
        update_ms = 0.0
        flush_ms = 0.0
        pair_generate_ms = 0.0
        pair_flush_ms = 0.0
        models_flushed = 0
        pairs_generated = 0
        pairs_flushed = 0
        queue_backlog = _msg_queue.qsize()
        if queue_backlog >= _BACKGROUND_QUEUE_BACKLOG_SKIP:
            _radar_loop_timings.append({
                "ts_s": time.time(),
                "update_ms": 0.0,
                "flush_ms": 0.0,
                "pair_generate_ms": 0.0,
                "pair_flush_ms": 0.0,
                "models_flushed": 0,
                "pairs_generated": 0,
                "pairs_flushed": 0,
                "msg_queue_depth": queue_backlog,
                "skipped_for_backlog": True,
            })
            continue
        try:
            t_update = time.perf_counter()
            await asyncio.to_thread(
                radar_state.update_rotation_models,
                config.RADAR_UPDATE_MAX_IIDS,
                config.RADAR_UPDATE_BUDGET_MS,
            )
            update_ms = (time.perf_counter() - t_update) * 1000
        except Exception:
            log.exception("Radar: update_rotation_models failed")

        try:
            t_pairs = time.perf_counter()
            pairs_generated_now = await asyncio.to_thread(radar_state.update_calibration_pairs)
            pair_generate_ms = (time.perf_counter() - t_pairs) * 1000
            pairs_generated = len(pairs_generated_now)
            if pairs_generated_now:
                pending_pairs = await asyncio.to_thread(radar_state.pop_pending_pairs)
                try:
                    t_pair_flush = time.perf_counter()
                    await asyncio.to_thread(stats_db.insert_calibration_pairs, pending_pairs)
                    pair_flush_ms = (time.perf_counter() - t_pair_flush) * 1000
                    pairs_flushed = len(pending_pairs)
                except Exception:
                    await asyncio.to_thread(radar_state.requeue_pending_pairs, pending_pairs)
                    raise
        except Exception:
            log.exception("Radar: calibration pair update failed")

        iteration += 1
        if iteration % 2 == 0 and _msg_queue.qsize() < _BACKGROUND_QUEUE_BACKLOG_SKIP:  # every 60s: flush to DB
            try:
                t_flush = time.perf_counter()
                models = radar_state.get_all_rotation_models()
                await asyncio.to_thread(stats_db.upsert_radar_iids, list(models.values()))
                models_flushed = len(models)
                flush_ms = (time.perf_counter() - t_flush) * 1000
            except Exception:
                log.exception("Radar: DB flush failed")
        _radar_loop_timings.append({
            "ts_s": time.time(),
            "update_ms": round(update_ms, 2),
            "flush_ms": round(flush_ms, 2),
            "pair_generate_ms": round(pair_generate_ms, 2),
            "pair_flush_ms": round(pair_flush_ms, 2),
            "models_flushed": models_flushed,
            "pairs_generated": pairs_generated,
            "pairs_flushed": pairs_flushed,
            "msg_queue_depth": _msg_queue.qsize(),
            "skipped_for_backlog": False,
        })


async def _fm_loop() -> None:
    """Background task: commit per-frame position accumulation for eligible IIDs.

    Wakes every 30 s and runs run_full_pipeline for each auto-mode IID that has
    accumulated at least one new frame since the last run.  When ≥20 per-frame
    estimates are in the buffer, the pipeline takes the fast path (weighted
    centroid only — no expensive intersection solve).  The intersection solver
    only runs as a bootstrap when the buffer is still too thin.
    """
    from radar.api import _get_fm

    _last_frame_count: dict[int, int] = {}   # iid → frame count at last run
    _iid_locks: dict[int, asyncio.Lock] = {}  # iid → lock

    while True:
        await asyncio.sleep(30)
        if config.RADAR_CORE_ENABLED and config.RADAR_CORE_FM_ENABLED:
            continue
        if radar_state.is_update_active() or _msg_queue.qsize() >= _BACKGROUND_QUEUE_BACKLOG_SKIP:
            continue
        try:
            models = radar_state.get_all_rotation_models()
        except Exception:
            log.exception("FM loop: get_all_rotation_models failed")
            continue

        candidates: list[tuple[int, int]] = []
        for iid, model in models.items():
            if model.resolution_mode == "locked_unresolvable":
                continue
            if model.period_s is None:
                continue  # rotation model not ready

            frames = radar_state.get_sweep_frames(iid)
            completed = [f for f in frames if f.quality in ("good", "marginal")]
            n_frames = len(completed)

            if n_frames == 0:
                continue
            if n_frames == _last_frame_count.get(iid, 0):
                continue  # no new frames since last run
            candidates.append((iid, n_frames))

        candidates.sort(key=lambda item: item[1], reverse=True)
        for iid, n_frames in candidates[:_FM_MAX_IIDS_PER_CYCLE]:
            model = models.get(iid)
            if model is None or model.period_s is None or model.resolution_mode != "auto":
                continue

            lock = _iid_locks.setdefault(iid, asyncio.Lock())
            if lock.locked():
                continue  # previous run still in progress — skip this cycle

            async def _run_fm(iid=iid, lock=lock, n_frames=n_frames) -> None:
                async with lock:
                    try:
                        t0 = time.perf_counter()
                        result = await asyncio.to_thread(_get_fm().run_full_pipeline, iid, radar_state)
                        elapsed_ms = (time.perf_counter() - t0) * 1000
                        await asyncio.to_thread(radar_state.record_forward_model_attempt, iid, result, elapsed_ms)
                    except Exception:
                        log.exception("FM loop: pipeline failed for IID %d", iid)
                        try:
                            await asyncio.to_thread(
                                radar_state.record_forward_model_attempt,
                                iid,
                                {"error": "pipeline exception", "stage": "exception"},
                                0.0,
                            )
                        except Exception:
                            pass
                        return

                    if result is None or "error" in result:
                        # Update frame count even on failure so we don't retry
                        # until enough new frames have accumulated.
                        _last_frame_count[iid] = n_frames
                        _fm_run_timings.append({
                            "ts_s": time.time(),
                            "iid": iid,
                            "elapsed_ms": round(elapsed_ms, 2),
                            "frame_count": n_frames,
                            "stored": False,
                            "ok": False,
                        })
                        log.info("FM loop: IID %d — failed in %.0f ms: %s", iid, elapsed_ms,
                                 (result or {}).get("error", "unknown"))
                        return

                    _last_frame_count[iid] = n_frames
                    stored = result.get("stored", True)
                    _fm_run_timings.append({
                        "ts_s": time.time(),
                        "iid": iid,
                        "elapsed_ms": round(elapsed_ms, 2),
                        "frame_count": n_frames,
                        "stored": bool(stored),
                        "ok": True,
                    })
                    log.info(
                        "FM loop: IID %d — (%.4f, %.4f) cep=%.0f m from %d frames in %.0f ms%s",
                        iid, result["lat"], result["lon"], result.get("cep_m", 0),
                        n_frames, elapsed_ms, "" if stored else " (not stored — regression guard)",
                    )

            if _msg_queue.qsize() >= _BACKGROUND_QUEUE_BACKLOG_SKIP:
                break
            await _run_fm()


async def _coincident_loop() -> None:
    """Background task: run coincident-illumination localisation on eligible IIDs."""
    from radar.localiser import RadarLocaliser
    from radar.models import CalibrationPair

    _iid_locks: dict[int, asyncio.Lock] = {}

    def _load_pairs(iid: int) -> list[CalibrationPair]:
        rows = stats_db.load_calibration_pairs(iid)
        return [
            CalibrationPair(
                iid=row["iid"],
                ts=row["ts"],
                icao_a=row["icao_a"],
                icao_b=row["icao_b"],
                lat_a=row["lat_a"],
                lon_a=row["lon_a"],
                lat_b=row["lat_b"],
                lon_b=row["lon_b"],
                tdoa_us=row["tdoa_us"],
                receiver_lat=row["receiver_lat"],
                receiver_lon=row["receiver_lon"],
            )
            for row in rows
        ]

    _last_attempted: dict[int, float] = {}  # iid → last attempt time (success or fail)

    while True:
        await asyncio.sleep(90)
        if radar_state.is_update_active() or _msg_queue.qsize() >= _BACKGROUND_QUEUE_BACKLOG_SKIP:
            continue
        try:
            models = radar_state.get_all_rotation_models()
        except Exception:
            log.exception("CI loop: get_all_rotation_models failed")
            continue

        candidates: list[tuple[int, float]] = []
        for iid, model in models.items():
            if model.resolution_mode != "auto":
                continue
            # Schedule by last attempt, not last success — prevents a perpetually
            # failing IID from starving all others by keeping ci_last_updated = 0.
            candidates.append((iid, _last_attempted.get(iid, 0.0)))

        candidates.sort(key=lambda item: item[1])
        for iid, _last_ts in candidates[:_CI_MAX_IIDS_PER_CYCLE]:
            lock = _iid_locks.setdefault(iid, asyncio.Lock())
            if lock.locked():
                continue

            async def _run_ci(iid=iid, lock=lock) -> None:
                async with lock:
                    try:
                        pairs = await asyncio.to_thread(_load_pairs, iid)
                        if not pairs:
                            log.debug("CI loop IID %d: no pairs loaded", iid)
                            return
                        log.debug("CI loop IID %d: solving with %d pairs", iid, len(pairs))
                        # CI requires an FM seed — without it the beam-line
                        # intersections can cluster around a coherent but wrong
                        # position.  FM is the primary solver; CI only refines.
                        fm_model = radar_state.get_rotation_model(iid)
                        seed_lat = fm_model.fm_lat if fm_model is not None else None
                        seed_lon = fm_model.fm_lon if fm_model is not None else None
                        if seed_lat is None or seed_lon is None:
                            log.debug("CI loop IID %d: skipping — no FM solution to seed from", iid)
                            return
                        lat, lon, cep_m, n_pairs = await asyncio.to_thread(
                            RadarLocaliser().solve_coincident,
                            pairs,
                            seed_lat,
                            seed_lon,
                        )
                    except ValueError as exc:
                        log.info("CI loop IID %d: solver rejected — %s", iid, exc)
                        return
                    except Exception:
                        log.exception("CI loop IID %d: unexpected solver error", iid)
                        return

                    radar_state.update_coincident_location(
                        iid=iid,
                        lat=lat,
                        lon=lon,
                        cep_m=cep_m,
                        n_pairs=n_pairs,
                    )
                    try:
                        model = radar_state.get_rotation_model(iid)
                        if model is not None:
                            await asyncio.to_thread(stats_db.upsert_radar_iid, model)
                    except Exception:
                        log.exception("CI loop: failed to persist IID %d", iid)

            qsize = _msg_queue.qsize()
            if qsize >= _BACKGROUND_QUEUE_BACKLOG_SKIP:
                log.info("CI loop: skipping IID %d — queue backlog %d", iid, qsize)
                break
            _last_attempted[iid] = time.time()
            await _run_ci()


async def _aircraft_bearing_calibration_loop() -> None:
    """Stage 3: continuously fit per-radar bearing calibration from truth aircraft."""
    await asyncio.sleep(30)  # let the radar loop seed first
    while True:
        await asyncio.sleep(config.STAGE3_CALIBRATION_INTERVAL_S)
        try:
            if radar_state.is_update_active():
                continue
            if _msg_queue.qsize() >= _BACKGROUND_QUEUE_BACKLOG_SKIP:
                aircraft_localiser._backlog_skips += 1
                log.debug("Stage3 cal loop: skipped — queue backlog %d", _msg_queue.qsize())
                continue
            updated = await asyncio.to_thread(
                aircraft_localiser.run_calibration_cycle,
                config.STAGE3_MAX_TARGETS_PER_CYCLE,
            )
            if updated:
                await asyncio.to_thread(stats_db.upsert_radar_bearing_calibrations, updated)
                log.info("Stage3: calibrated %d radar(s)", len(updated))
        except asyncio.CancelledError:
            raise
        except Exception:
            log.exception("Stage3 calibration loop error")


async def _aircraft_localisation_loop() -> None:
    """Stage 3: continuously solve aircraft positions from live bearing observations."""
    await asyncio.sleep(60)  # let calibration run first
    while True:
        await asyncio.sleep(config.STAGE3_LOCALISATION_INTERVAL_S)
        try:
            if radar_state.is_update_active():
                continue
            if _msg_queue.qsize() >= _BACKGROUND_QUEUE_BACKLOG_SKIP:
                aircraft_localiser._backlog_skips += 1
                log.debug("Stage3 loc loop: skipped — queue backlog %d", _msg_queue.qsize())
                continue

            target_icaos = radar_aircraft_api._get_active_icaos()

            if not target_icaos:
                continue

            fixes = await asyncio.to_thread(
                aircraft_localiser.run_localisation_cycle,
                target_icaos,
                config.STAGE3_MAX_TARGETS_PER_CYCLE,
            )
            if fixes:
                log.debug("Stage3: produced %d fix(es) for %d target(s)", len(fixes), len(target_icaos))
        except asyncio.CancelledError:
            raise
        except Exception:
            log.exception("Stage3 localisation loop error")


@asynccontextmanager
async def lifespan(app: FastAPI):
    await _seed_startup_state()

    _bg_tasks: list[asyncio.Task] = []
    def _bg(coro):
        t = asyncio.create_task(coro)
        _bg_tasks.append(t)
        return t

    if _radar_core_worker is not None:
        worker_ready = _radar_core_worker.start()
        if not worker_ready:
            worker_error = (_radar_core_worker.stats().get("last_error") or "unknown startup failure")
            raise RuntimeError(f"radar-core startup failed: {worker_error}")
    if _radar_core_client is not None:
        _radar_core_client.start()
        if not _radar_core_client.wait_until_connected(config.RADAR_CORE_STARTUP_TIMEOUT_S):
            if _radar_core_worker is not None:
                _radar_core_worker.stop()
            _radar_core_client.stop()
            raise RuntimeError(
                f"radar-core client failed to connect to {config.RADAR_CORE_SOCKET!r} "
                f"within {config.RADAR_CORE_STARTUP_TIMEOUT_S:.1f}s"
            )
        if config.RECEIVER_LAT is not None:
            _radar_core_client.send_config_update("RECEIVER_LAT", config.RECEIVER_LAT)
        if config.RECEIVER_LON is not None:
            _radar_core_client.send_config_update("RECEIVER_LON", config.RECEIVER_LON)

    global _decoder_thread, _radar_thread
    if config.INGEST_MODE == "beast":
        # Default: decode raw Beast TCP stream in Python
        _radar_thread = _start_radar_processor()
        _decoder_thread = _start_msg_processor()
        _bg(_beast_runner())
        for name, host, port in config.MLAT_SERVERS:
            _bg(_mlat_runner(name, host, port))
    elif config.INGEST_MODE == "readsb":
        # Pure readsb mode: no Beast connection, no decode thread
        log.info("Ingest mode: readsb JSON (no Beast TCP)")
        _bg(readsb_ingest.readsb_poller(state))
        _bg(readsb_stats.readsb_stats_poller())
    elif config.INGEST_MODE == "hybrid":
        # Hybrid: readsb JSON for positions/EHS + Beast TCP for ACAS, DF counts,
        # and per-source MLAT attribution.  Beast CPR position updates are suppressed
        # in aircraft_state so readsb JSON remains authoritative for position.
        log.info("Ingest mode: hybrid (readsb JSON + Beast TCP for ACAS/MLAT)")
        _bg(readsb_ingest.readsb_poller(state))
        _bg(readsb_stats.readsb_stats_poller())
        _radar_thread = _start_radar_processor()
        _decoder_thread = _start_msg_processor()
        _bg(_beast_runner())
        for name, host, port in config.MLAT_SERVERS:
            _bg(_mlat_runner(name, host, port))
    else:
        raise ValueError(f"Unknown INGEST_MODE: {config.INGEST_MODE!r} (expected beast/readsb/hybrid)")
    _bg(_housekeeping_loop())
    _bg(_notify_cast_loop())
    _bg(_broadcast_loop())
    _bg(_db_writer())
    _bg(_db_update_checker())
    _bg(_hexdb_cache_flusher())
    _bg(_adsbx_task())
    _bg(_hexdb_task())
    _bg(_backup_runner())  # runs nightly; path resolved from DB/env at runtime
    _bg(_hires_writer())
    if config.MEMORY_POLICY_ENABLED:
        _bg(_memory_guard())
    _bg(_route_enricher())
    if config.RECEIVER_LAT is not None and config.RECEIVER_LON is not None:
        async def _terrain_prewarm():
            await asyncio.to_thread(terrain_prewarm, config.RECEIVER_LAT, config.RECEIVER_LON)
        _bg(_terrain_prewarm())
    _bg(run_position_quality_checker(position_quality_module._checker))
    _bg(health_module.loop_lag_sampler())
    _bg(_radar_loop())
    if _radar_core_client is not None:
        _bg(_radar_core_position_loop())
    if config.RADAR_COINCIDENT_BACKGROUND_ENABLED:
        _bg(_coincident_loop())
    _bg(_fm_loop())
    if config.STAGE3_ENABLED:
        _bg(_aircraft_bearing_calibration_loop())
        _bg(_aircraft_localisation_loop())
    health_module.register_context(_msg_queue, _clients)

    def _runtime_stats_payload() -> dict:
        radar_state_stats = radar_state.get_memory_stats()
        return {
            "ws_clients":        len(_clients),
            # route_enrichment_queue: async HTTP lookup queue for adsbdb.com route data.
            # This is NOT the live radar/WebSocket delivery path — drops here mean some
            # visit records won't get origin/dest airports, not that live UI is degraded.
            "route_enrichment_queue_size":  len(_route_queue),
            "route_enrichment_queue_drops": _route_queue_drops,
            # Live WebSocket delivery metrics — use these to judge coalescing/delivery health.
            "ws_main_frame_drops":   _ws_main_frame_drops,
            "ws_sync_rebuilds":      _ws_sync_rebuilds,
            "ws_sync_emissions":     _ws_sync_emissions,
            "ws_live_emissions":     _ws_live_emissions,
            "ws_client_queue_depths": [q.qsize() for q in _clients.values()],
            "aircraft_state":    state.get_aux_dict_sizes(),
            "cast":              cast.get_cache_stats(),
            "track_store":       track_store.stats(),
            "radar_state":       radar_state_stats,
            "radar_core":        _radar_core_runtime_stats(radar_state_stats),
        }

    register_runtime_stats(_runtime_stats_payload)
    log.info("ADS-B Dashboard backend started  (Beast: %s:%s)",
             config.BEAST_HOST, config.BEAST_PORT)

    yield

    # --- Graceful shutdown ---
    if _radar_core_client is not None:
        _radar_core_client.stop()
    if _radar_core_worker is not None:
        _radar_core_worker.stop()
    # Cancel background tasks first; the explicit final DB write inside
    # _graceful_shutdown handles persistence — no need for _db_writer to finish.
    await _graceful_shutdown(_bg_tasks)


app = FastAPI(title="ADS-B Dashboard", lifespan=lifespan)
tracks_module._track_store = track_store
app.include_router(tracks_router)
app.include_router(history_router)
aircraft_router._state = state  # type: ignore[attr-defined]
app.include_router(aircraft_router)
mlat_module._state = state
app.include_router(mlat_router)
app.include_router(fleet_router)
app.include_router(coverage_router)
app.include_router(terrain_router)
app.include_router(acas_router)
app.include_router(squawks_router)
app.include_router(status_router)
app.include_router(notify_settings_router)
app.include_router(cast_router)
app.include_router(debug_router)
position_quality_module._state = state
position_quality_module._checker = PositionQualityChecker(state)
app.include_router(position_quality_router)
app.include_router(health_router)
interrogators_router._state = state  # type: ignore[attr-defined]
app.include_router(interrogators_router)
timing_router._state = state  # type: ignore[attr-defined]
app.include_router(timing_router)
radar_api._state = radar_state  # type: ignore[attr-defined]
radar_api.register_radar_core_stats_provider(
    lambda: _radar_core_runtime_stats()
)
app.include_router(radar_api.router)
radar_aircraft_api._localiser = aircraft_localiser  # type: ignore[attr-defined]
radar_aircraft_api._msg_queue = _msg_queue  # type: ignore[attr-defined]
app.include_router(radar_aircraft_api.router)
if config.DEBUG_ENRICHMENT:
    log.info("Debug router mounted (DEBUG_ENRICHMENT=%s)", config.DEBUG_ENRICHMENT)

# Security note: no authentication is enforced on any endpoint.
# This is intentional for a LAN-only deployment (behind a home router/firewall).
# If this service is ever exposed to the internet, add token auth or an API key
# before all destructive endpoints (debug overrides, cast rules, history cleanup).
app.add_middleware(
    CORSMiddleware,
    allow_origins=config.CORS_ORIGINS,
    allow_methods=["GET", "POST", "PUT", "DELETE"],
    allow_headers=["*"],
)


# ---------------------------------------------------------------------------
# Routes
# ---------------------------------------------------------------------------

@app.websocket("/ws")
async def websocket_endpoint(ws: WebSocket) -> None:
    await ws.accept()
    send_queue: asyncio.Queue = asyncio.Queue(maxsize=2)
    _clients[ws] = send_queue
    log.info("WebSocket client connected  (total: %d)", len(_clients))

    async def _sender() -> None:
        """Drain the per-client send queue; runs independently of _push_updates."""
        while True:
            payload = await send_queue.get()
            if payload is None:
                return
            await asyncio.wait_for(
                ws.send_text(payload), timeout=config.WS_SEND_TIMEOUT_S
            )

    sender_task = asyncio.create_task(_sender())
    receive_task: asyncio.Task | None = None
    try:
        # Send the current snapshot immediately on connect
        await ws.send_text(_json_dumps(state.get_snapshot()))
        # Receive loop — keeps the connection alive; client messages are ignored
        while True:
            if receive_task is None:
                receive_task = asyncio.create_task(ws.receive_text())
            done, _ = await asyncio.wait(
                {sender_task, receive_task},
                return_when=asyncio.FIRST_COMPLETED,
            )
            if sender_task in done:
                exc = sender_task.exception()
                if exc is not None:
                    raise exc
                break
            if receive_task in done:
                await receive_task
                receive_task = None
    except WebSocketDisconnect:
        pass
    except Exception as exc:
        log.debug("WebSocket error: %s", exc)
    finally:
        _clients.pop(ws, None)
        if receive_task is not None:
            receive_task.cancel()
            try:
                await receive_task
            except (asyncio.CancelledError, Exception):
                pass
        sender_task.cancel()
        try:
            await sender_task
        except (asyncio.CancelledError, Exception):
            pass
        log.info("WebSocket client disconnected (total: %d)", len(_clients))


_HF_DF_FAMILIES = {
    "ADS-B": frozenset({17}),
    "TIS-B": frozenset({18}),
    "All-Call": frozenset({11}),
    "Comm-B": frozenset({20, 21}),
    "Surveillance": frozenset({4, 5}),
    "ACAS": frozenset({0, 16}),
}


def _bucket_counts(now_us: int, window_us: int, bin_ms: int, events: list[tuple[int, int, int, float | None, int, str, float | None, float | None, int | None]]) -> list[int]:
    bin_us = max(1_000, int(bin_ms * 1000))
    bucket_count = max(1, (window_us + bin_us - 1) // bin_us)
    counts = [0] * bucket_count
    cutoff_us = max(0, now_us - window_us)
    for arrival_us, _df, _msg_len, _signal_dbfs, _source_class, _icao, _bearing_deg, _range_nm, _iid in events:
        idx = (arrival_us - cutoff_us) // bin_us
        if 0 <= idx < bucket_count:
            counts[idx] += 1
    return counts


def _build_df_family_bins(now_us: int, window_us: int, bin_ms: int, events: list[tuple[int, int, int, float | None, int, str, float | None, float | None, int | None]]) -> dict[str, list[int]]:
    bin_us = max(1_000, int(bin_ms * 1000))
    bucket_count = max(1, (window_us + bin_us - 1) // bin_us)
    cutoff_us = max(0, now_us - window_us)
    lanes = {name: [0] * bucket_count for name in _HF_DF_FAMILIES}
    other = [0] * bucket_count
    for arrival_us, df, _msg_len, _signal_dbfs, _source_class, _icao, _bearing_deg, _range_nm, _iid in events:
        idx = (arrival_us - cutoff_us) // bin_us
        if idx < 0 or idx >= bucket_count:
            continue
        family_name = next((name for name, dfs in _HF_DF_FAMILIES.items() if df in dfs), None)
        if family_name is None:
            other[idx] += 1
        else:
            lanes[family_name][idx] += 1
    if any(other):
        lanes["Other"] = other
    return lanes


def _build_highfreq_payload(window_s: float, burst_bin_ms: int, cadence_bin_ms: int, latency_window_s: float) -> dict:
    window_us = max(1_000_000, int(window_s * 1_000_000))
    now_us, events = state.get_recent_timing_window(window_us)
    return {
        "now_us": now_us,
        "window_s": window_s,
        "burst": {
            "bin_ms": burst_bin_ms,
            "counts": _bucket_counts(now_us, window_us, burst_bin_ms, events),
        },
        "cadence": {
            "bin_ms": cadence_bin_ms,
            "families": _build_df_family_bins(now_us, window_us, cadence_bin_ms, events),
        },
        "latency": _get_latency_waveforms(window_s=latency_window_s, bin_ms=250),
    }


def _build_interrogator_payload(window_s: float) -> dict:
    now_us, timeline = state.get_iid_timeline(window_s)
    lanes = sorted(
        [{"iid": iid, "arrivals_us": entry["arrivals_us"], "latest_icao": entry.get("latest_icao", "")}
         for iid, entry in timeline.items()],
        key=lambda x: x["iid"],
    )
    return {"now_us": now_us, "window_s": window_s, "lanes": lanes}


@app.websocket("/ws/timing")
async def timing_websocket_endpoint(ws: WebSocket) -> None:
    """Dedicated timing-event stream for the receiver timing plot."""
    await ws.accept()
    try:
        timing_iid_filter = int(ws.query_params["iid"]) if "iid" in ws.query_params else None
    except (TypeError, ValueError):
        timing_iid_filter = None
    timing_df11_only = str(ws.query_params.get("df11_only", "")).lower() in {"1", "true", "yes"}
    since_seq = 0
    last_heartbeat = 0.0
    try:
        while True:
            now = time.time()
            loop_t0 = time.perf_counter()
            events_raw = state.get_timing_events(since_seq, limit=_TIMING_WS_BATCH_LIMIT)
            raw_event_count = len(events_raw)
            if events_raw:
                since_seq = events_raw[-1][0]
                if timing_df11_only or timing_iid_filter is not None:
                    events_raw = [
                        event for event in events_raw
                        if (
                            (not timing_df11_only or event[2] == 11)
                            and (timing_iid_filter is None or event[9] == timing_iid_filter)
                        )
                    ]
            if events_raw:
                await ws.send_text(_json_dumps({
                    "now_us": state.get_timing_now_us(),
                    "events": [
                        [seq, arrival_us, df, msg_len, signal_dbfs, source_class, icao, bearing_deg, range_nm, iid]
                        for seq, arrival_us, df, msg_len, signal_dbfs, source_class, icao, bearing_deg, range_nm, iid in events_raw
                    ],
                }))
                last_heartbeat = now
            elif now - last_heartbeat >= 1.0:
                await ws.send_text(_json_dumps({"now_us": state.get_timing_now_us(), "events": []}))
                last_heartbeat = now
            _timing_ws_timings.append({
                "ts_s": now,
                "elapsed_ms": round((time.perf_counter() - loop_t0) * 1000, 2),
                "event_count": len(events_raw),
                "raw_event_count": raw_event_count,
                "sent": bool(events_raw),
            })
            await asyncio.sleep(0.1)
    except WebSocketDisconnect:
        pass
    except Exception as exc:
        log.debug("Timing WebSocket error: %s", exc)


@app.websocket("/ws/highfreq")
async def highfreq_websocket_endpoint(ws: WebSocket) -> None:
    """Shared aggregate feed for high-frequency Timing-page panels."""
    await ws.accept()
    window_s = 5.0
    burst_bin_ms = 20
    cadence_bin_ms = 50
    latency_window_s = 30.0
    last_payload = None
    last_heartbeat = 0.0
    last_rebuild = 0.0
    try:
        while True:
            try:
                msg = await asyncio.wait_for(ws.receive_text(), timeout=0.2)
                try:
                    payload = json.loads(msg)
                    req_window_s = float(payload.get("window_s", window_s))
                    req_burst_bin_ms = int(payload.get("burst_bin_ms", burst_bin_ms))
                    req_cadence_bin_ms = int(payload.get("cadence_bin_ms", cadence_bin_ms))
                    req_latency_window_s = float(payload.get("latency_window_s", latency_window_s))
                    if 2 <= req_window_s <= 10:
                        window_s = req_window_s
                    if req_burst_bin_ms in (10, 20, 50):
                        burst_bin_ms = req_burst_bin_ms
                    if req_cadence_bin_ms in (20, 50):
                        cadence_bin_ms = req_cadence_bin_ms
                    if 10 <= req_latency_window_s <= 60:
                        latency_window_s = req_latency_window_s
                except Exception:
                    pass
            except asyncio.TimeoutError:
                pass

            now = time.time()
            payload = last_payload
            should_rebuild = payload is None or (now - last_rebuild) >= _AGGREGATE_REBUILD_INTERVAL_S
            if should_rebuild:
                payload = _build_highfreq_payload(window_s, burst_bin_ms, cadence_bin_ms, latency_window_s)
                last_rebuild = now
            if payload != last_payload:
                await ws.send_text(_json_dumps(payload))
                last_payload = payload
                last_heartbeat = now
            elif now - last_heartbeat >= 1.0:
                await ws.send_text(_json_dumps(payload))
                last_heartbeat = now
            await asyncio.sleep(0.1)
    except WebSocketDisconnect:
        pass
    except Exception as exc:
        log.debug("High-frequency WebSocket error: %s", exc)


@app.websocket("/ws/interrogators")
async def interrogator_timing_websocket_endpoint(ws: WebSocket) -> None:
    """Dedicated interrogator timing stream for the high-frequency timing page."""
    await ws.accept()
    window_s = 10.0
    last_payload = None
    last_heartbeat = 0.0
    last_rebuild = 0.0
    try:
        while True:
            try:
                msg = await asyncio.wait_for(ws.receive_text(), timeout=0.2)
                try:
                    payload = json.loads(msg)
                    req_window = float(payload.get("window_s", window_s))
                    if 2 <= req_window <= 60:
                        window_s = req_window
                except Exception:
                    pass
            except asyncio.TimeoutError:
                pass

            now = time.time()
            payload = last_payload
            should_rebuild = payload is None or (now - last_rebuild) >= _AGGREGATE_REBUILD_INTERVAL_S
            if should_rebuild:
                payload = _build_interrogator_payload(window_s)
                last_rebuild = now
            if payload != last_payload:
                await ws.send_text(_json_dumps(payload))
                last_payload = payload
                last_heartbeat = now
            elif now - last_heartbeat >= 1.0:
                await ws.send_text(_json_dumps(payload))
                last_heartbeat = now
            await asyncio.sleep(0.1)
    except WebSocketDisconnect:
        pass
    except Exception as exc:
        log.debug("Interrogator WebSocket error: %s", exc)


@app.websocket("/ws/timing-page")
async def timing_page_websocket_endpoint(ws: WebSocket) -> None:
    """Multiplexed Timing-page stream carrying message, interrogator, and aggregate panels."""
    await ws.accept()
    timing_window_s = 5.0
    interrogator_window_s = 10.0
    burst_bin_ms = 20
    cadence_bin_ms = 50
    latency_window_s = 30.0
    since_seq = 0
    last_interrogators = None
    last_aggregates = None
    last_heartbeat = 0.0
    last_aggregate_rebuild = 0.0
    last_interrogator_rebuild = 0.0
    try:
        while True:
            try:
                msg = await asyncio.wait_for(ws.receive_text(), timeout=0.2)
                try:
                    payload = json.loads(msg)
                    req_timing_window_s = float(payload.get("timing_window_s", timing_window_s))
                    req_interrogator_window_s = float(payload.get("interrogator_window_s", interrogator_window_s))
                    req_burst_bin_ms = int(payload.get("burst_bin_ms", burst_bin_ms))
                    req_cadence_bin_ms = int(payload.get("cadence_bin_ms", cadence_bin_ms))
                    req_latency_window_s = float(payload.get("latency_window_s", latency_window_s))
                    if 2 <= req_timing_window_s <= 10:
                        timing_window_s = req_timing_window_s
                    if 2 <= req_interrogator_window_s <= 60:
                        interrogator_window_s = req_interrogator_window_s
                    if req_burst_bin_ms in (10, 20, 50):
                        burst_bin_ms = req_burst_bin_ms
                    if req_cadence_bin_ms in (20, 50):
                        cadence_bin_ms = req_cadence_bin_ms
                    if 10 <= req_latency_window_s <= 60:
                        latency_window_s = req_latency_window_s
                except Exception:
                    pass
            except asyncio.TimeoutError:
                pass

            now = time.time()
            events_raw = state.get_timing_events(since_seq, limit=_TIMING_PAGE_BATCH_LIMIT)
            if events_raw:
                since_seq = events_raw[-1][0]

            timing_payload = {
                "now_us": state.get_timing_now_us(),
                "window_s": timing_window_s,
                "events": [
                    [seq, arrival_us, df, msg_len, signal_dbfs, source_class, icao, bearing_deg, range_nm, iid]
                    for seq, arrival_us, df, msg_len, signal_dbfs, source_class, icao, bearing_deg, range_nm, iid in events_raw
                ],
            }
            interrogator_payload = last_interrogators
            if interrogator_payload is None or (now - last_interrogator_rebuild) >= _AGGREGATE_REBUILD_INTERVAL_S:
                interrogator_payload = _build_interrogator_payload(interrogator_window_s)
                last_interrogator_rebuild = now

            aggregate_payload = last_aggregates
            if aggregate_payload is None or (now - last_aggregate_rebuild) >= _AGGREGATE_REBUILD_INTERVAL_S:
                aggregate_payload = _build_highfreq_payload(timing_window_s, burst_bin_ms, cadence_bin_ms, latency_window_s)
                last_aggregate_rebuild = now

            has_interrogator_change = interrogator_payload != last_interrogators
            has_aggregate_change = aggregate_payload != last_aggregates
            has_timing_events = bool(events_raw)

            if has_timing_events or has_interrogator_change or has_aggregate_change or now - last_heartbeat >= 1.0:
                await ws.send_text(_json_dumps({
                    "type": "timing_page",
                    "timing": timing_payload,
                    "interrogators": interrogator_payload,
                    "aggregates": aggregate_payload,
                }))
                last_interrogators = interrogator_payload
                last_aggregates = aggregate_payload
                last_heartbeat = now
            await asyncio.sleep(0.1)
    except WebSocketDisconnect:
        pass
    except Exception as exc:
        log.debug("Timing page WebSocket error: %s", exc)


@app.websocket("/ws/radar/iids/{iid}")
async def radar_iid_websocket_endpoint(ws: WebSocket, iid: int) -> None:
    """Selected-IID Stage 1 stream carrying live alignment timeline plus rotation summary."""
    await ws.accept()
    window_s = 90.0
    last_payload = None
    last_heartbeat = 0.0
    last_rebuild = 0.0
    last_signature = None
    try:
        while True:
            try:
                msg = await asyncio.wait_for(ws.receive_text(), timeout=0.2)
                try:
                    payload = json.loads(msg)
                    req_window_s = float(payload.get("window_s", window_s))
                    if 10 <= req_window_s <= 300:
                        window_s = req_window_s
                except Exception:
                    pass
            except asyncio.TimeoutError:
                pass

            now = time.time()
            loop_t0 = time.perf_counter()
            payload = last_payload
            rebuilt = False
            model = radar_state.get_rotation_model(iid)
            current_signature = (
                radar_state.get_iid_latest_arrival_us(iid),
                model.last_updated if model is not None else None,
                model.period_s if model is not None else None,
                model.status if model is not None else None,
                model.reference_aircraft.ref_icao if model is not None and model.reference_aircraft else None,
                window_s,
            )
            if (
                payload is None
                or (
                    current_signature != last_signature
                    and (now - last_rebuild) >= _RADAR_IID_REBUILD_INTERVAL_S
                )
            ):
                payload = {
                    "type": "radar_iid",
                    "timeline": radar_api.build_iid_timeline_payload(radar_state, iid, window_s),
                    "rotation": radar_api.build_iid_rotation_payload(radar_state, iid),
                }
                last_rebuild = now
                last_signature = current_signature
                rebuilt = True
            if payload != last_payload:
                await ws.send_text(_json_dumps(payload))
                last_payload = payload
                last_heartbeat = now
                sent_kind = "full"
            elif now - last_heartbeat >= 1.0:
                await ws.send_text(_json_dumps({"type": "radar_iid_heartbeat", "iid": iid}))
                last_heartbeat = now
                sent_kind = "heartbeat"
            else:
                sent_kind = "none"
            _radar_ws_timings.append({
                "ts_s": now,
                "elapsed_ms": round((time.perf_counter() - loop_t0) * 1000, 2),
                "iid": iid,
                "sent_kind": sent_kind,
                "rebuilt": rebuilt,
            })
            await asyncio.sleep(0.1)
    except WebSocketDisconnect:
        pass
    except Exception as exc:
        log.debug("Radar IID WebSocket error: %s", exc)


@app.websocket("/ws/radar/iids/{iid}/sync")
async def radar_iid_sync_websocket_endpoint(ws: WebSocket, iid: int) -> None:
    """Selected-IID pushed sync snapshot feed for RadarPage default diagnostics."""
    await ws.accept()
    window_s = 90.0
    debug_limit = 120
    last_sequence = None
    last_heartbeat = 0.0
    last_rebuild = 0.0
    try:
        while True:
            try:
                msg = await asyncio.wait_for(ws.receive_text(), timeout=0.2)
                try:
                    request = json.loads(msg)
                    req_window_s = float(request.get("window_s", window_s))
                    req_debug_limit = int(request.get("debug_limit", debug_limit))
                    if 10 <= req_window_s <= 300:
                        window_s = req_window_s
                    if 1 <= req_debug_limit <= 300:
                        debug_limit = req_debug_limit
                except Exception:
                    pass
            except asyncio.TimeoutError:
                pass

            now = time.time()
            sent_kind = "none"
            if now - last_rebuild >= 0.25:
                global _ws_sync_rebuilds, _ws_sync_emissions
                _ws_sync_rebuilds += 1
                payload = radar_api.build_iid_sync_snapshot_payload(
                    radar_state,
                    iid,
                    window_s=window_s,
                    debug_limit=debug_limit,
                )
                last_rebuild = now
                sequence = payload.get("sequence")
                if sequence != last_sequence:
                    await ws.send_text(_json_dumps(payload))
                    last_sequence = sequence
                    last_heartbeat = now
                    sent_kind = "snapshot"
                    _ws_sync_emissions += 1
            if sent_kind == "none" and now - last_heartbeat >= 1.0:
                await ws.send_text(_json_dumps({
                    "type": "radar_sync_heartbeat",
                    "iid": iid,
                    "server_ts": now,
                    "sequence": last_sequence,
                }))
                last_heartbeat = now
            await asyncio.sleep(0.1)
    except WebSocketDisconnect:
        pass
    except Exception as exc:
        log.debug("Radar IID sync WebSocket error: %s", exc)


@app.websocket("/ws/radar/live")
async def radar_live_websocket_endpoint(ws: WebSocket) -> None:
    """Lean change-driven multi-IID live-state stream.

    Emits a payload per changed IID whenever sync or localiser state changes
    (rate-limited to 500ms per emission). A heartbeat fires every 5s regardless
    of change so the frontend can detect a stale connection.
    """
    await ws.accept()
    last_signature: tuple = ()
    last_heartbeat: float = 0.0
    last_emission: float = 0.0
    _RATE_LIMIT_S = 0.5
    _HEARTBEAT_S = 5.0
    try:
        # Send current state immediately on connect
        if radar_state is not None:
            payload = radar_api.build_radar_live_state_payload(radar_state)
            await ws.send_text(_json_dumps(payload))
            last_signature = radar_api._radar_live_signature(radar_state)
            last_heartbeat = time.time()
            last_emission = last_heartbeat
        while True:
            try:
                await asyncio.wait_for(ws.receive_text(), timeout=0.2)
            except asyncio.TimeoutError:
                pass
            now = time.time()
            sig = radar_api._radar_live_signature(radar_state)
            if sig != last_signature and now - last_emission >= _RATE_LIMIT_S:
                global _ws_live_emissions
                _ws_live_emissions += 1
                payload = radar_api.build_radar_live_state_payload(radar_state)
                await ws.send_text(_json_dumps(payload))
                last_signature = sig
                last_emission = now
                last_heartbeat = now
            elif now - last_heartbeat >= _HEARTBEAT_S:
                await ws.send_text(_json_dumps({
                    "type": "radar_live_heartbeat",
                    "server_ts": now,
                }))
                last_heartbeat = now
    except WebSocketDisconnect:
        pass
    except Exception as exc:
        log.debug("Radar live WebSocket error: %s", exc)


@app.get("/api/stats")
async def get_stats() -> dict:
    """HTTP fallback – returns the same snapshot the WebSocket streams."""
    return state.get_snapshot()


# ---------------------------------------------------------------------------
# Serve the built frontend (production)
# ---------------------------------------------------------------------------
try:
    app.mount("/", StaticFiles(directory="../frontend/dist", html=True), name="static")
except RuntimeError:
    pass  # frontend not built yet – dev mode uses the Vite dev server
