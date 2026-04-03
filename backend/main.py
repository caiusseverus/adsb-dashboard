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
from aircraft_state import AircraftState, push_timings as _push_timings_store
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


def _start_msg_processor() -> threading.Thread:
    """Start the daemon thread that decodes Beast messages from _msg_queue.
    Uses make_pause_aware_decoder so the benchmark can pause it cleanly."""
    _run = make_pause_aware_decoder(_msg_queue, state, _DECODE_SENTINEL)
    t = threading.Thread(target=_run, daemon=True, name="beast-decoder")
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
                 ac.get("signal"),
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

            t_done = time.perf_counter()
            _push_timings_store.append({
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
            })
        except Exception:
            log.exception("_broadcast_loop: unhandled error in broadcast cycle — continuing")


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


@asynccontextmanager
async def lifespan(app: FastAPI):
    await _seed_startup_state()

    _bg_tasks: list[asyncio.Task] = []
    def _bg(coro):
        t = asyncio.create_task(coro)
        _bg_tasks.append(t)
        return t

    global _decoder_thread
    if config.INGEST_MODE == "beast":
        # Default: decode raw Beast TCP stream in Python
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
    health_module.register_context(_msg_queue, _clients)
    register_runtime_stats(lambda: {
        "ws_clients":       len(_clients),
        "route_queue_size": len(_route_queue),
        "route_queue_drops": _route_queue_drops,
    })
    log.info("ADS-B Dashboard backend started  (Beast: %s:%s)",
             config.BEAST_HOST, config.BEAST_PORT)

    yield

    # --- Graceful shutdown ---
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
