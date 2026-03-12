import asyncio
import json
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
import warnings
from contextlib import asynccontextmanager
from datetime import date, datetime, timedelta

# websockets ≥ 12 deprecates the two-argument ws_handler signature used
# internally by starlette/uvicorn — suppress until uvicorn catches up.
warnings.filterwarnings('ignore', category=DeprecationWarning, module='websockets')

from fastapi import FastAPI, WebSocket, WebSocketDisconnect
from fastapi.middleware.cors import CORSMiddleware
from fastapi.staticfiles import StaticFiles

import config
import enrichment
from beast_client import BeastClient
from aircraft_state import AircraftState, push_timings as _push_timings_store
from db import stats_db
from track_store import TrackStore
from history import router as history_router
from aircraft import router as aircraft_router
from fleet import router as fleet_router
from coverage import router as coverage_router
from acas import router as acas_router
from squawks import router as squawks_router
import notifications
import hires_buffer
from status import router as status_router
from debug import router as debug_router
from notify_settings import router as notify_settings_router
import tracks as tracks_module
from tracks import router as tracks_router

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
_clients: list[WebSocket] = []

# Emergency squawk tracking: {icao: {squawk, db_id}} for ongoing events
EMERGENCY_SQUAWKS = frozenset({"7700", "7600", "7500"})
_active_squawks: dict[str, dict] = {}

# Watchlist cache — refreshed from DB every 30s to avoid per-aircraft DB reads
_watchlist_cache: dict[str, float | None] = {}   # icao → max_range_nm

# Route lookup queue: (visit_id, callsign) pairs awaiting adsbdb.com resolution
_route_queue: deque[tuple[int, str]] = deque(maxlen=2000)
_watchlist_cache_ts: float = 0.0

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


def _start_msg_processor() -> threading.Thread:
    """Start the daemon thread that decodes Beast messages from _msg_queue.
    Uses make_pause_aware_decoder so the benchmark can pause it cleanly."""
    _run = make_pause_aware_decoder(_msg_queue, state, _DECODE_SENTINEL)
    t = threading.Thread(target=_run, daemon=True, name="beast-decoder")
    t.start()
    return t


# ---------------------------------------------------------------------------
# Background tasks
# ---------------------------------------------------------------------------

async def _beast_runner() -> None:
    def on_message(msg: dict) -> None:
        try:
            _msg_queue.put_nowait((msg, None))
        except queue.Full:
            pass  # drop oldest-equivalent: decoder is behind, discard this arrival

    client = BeastClient(config.BEAST_HOST, config.BEAST_PORT, on_message)
    await client.run()


async def _mlat_runner(name: str, host: str, port: int) -> None:
    def on_mlat_message(msg: dict) -> None:
        try:
            _msg_queue.put_nowait((msg, name))
        except queue.Full:
            pass

    client = BeastClient(host, port, on_mlat_message)
    log.info("MLAT runner starting: %s (%s:%s)", name, host, port)
    await client.run()


async def _db_update_checker() -> None:
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


async def _hexdb_task() -> None:
    """Process hexdb.io fallback lookups at a polite rate (1 req/sec).
    Checks the queue every 5 seconds; new aircraft are enriched within ~5s of first contact."""
    await asyncio.sleep(5)  # brief startup delay
    while True:
        batch = state.pop_hexdb_queue(max_n=10)
        for icao in batch:
            data = await asyncio.to_thread(enrichment.db.lookup_hexdb, icao)
            data_source = "hexdb"
            if not data:
                # tar1090-db shard as final fallback (downloads shard on first access)
                data = await asyncio.to_thread(enrichment.db.get_tar1090, icao)
                data_source = "tar1090"
            if data:
                state.apply_hexdb(icao, data)
                # Also persist enrichment to DB so offline (historical) aircraft get updated.
                # Mirrors the field resolution in _apply_hexdb_data().
                registration  = (data.get("Registration")   or "").strip() or None
                type_code     = (data.get("ICAOTypeCode")   or "").strip() or None
                type_category = None
                if type_code:
                    ti = enrichment.db.get_type_info(type_code)
                    if ti:
                        type_category = ti.get("desc") or None
                manufacturer = (data.get("Manufacturer") or "").strip() or None
                # Operator: prefer OperatorFlagCode lookup, fall back to RegisteredOwners
                operator = None
                flag_code = (data.get("OperatorFlagCode") or "").strip()
                if flag_code:
                    op = enrichment.db.get_operator(flag_code)
                    if op:
                        operator = op.get("n")
                if not operator:
                    operator = (data.get("RegisteredOwners") or "").strip() or None
                if config.DEBUG_ENRICHMENT == 1:
                    log.info(
                        "[enrich] %-8s  %-8s  reg=%-9s type=%-6s op=%s",
                        icao, data_source,
                        registration or "—",
                        type_code or "—",
                        repr(operator) if operator else "—",
                    )
                await asyncio.to_thread(
                    stats_db.update_aircraft_enrichment,
                    icao, registration, type_code, type_category, operator, manufacturer,
                )
            else:
                if config.DEBUG_ENRICHMENT == 1:
                    log.info("[enrich] %-8s  miss", icao)
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


def _credible_aircraft(ac) -> bool:
    """Ghost filter for Aircraft dataclass objects (used at visit close time)."""
    if config.GHOST_FILTER_MSGS <= 0:
        return True
    if ac.msg_count >= config.GHOST_FILTER_MSGS:
        return True
    if ac.mlat:
        return True
    if enrichment.db.get_adsbx(ac.icao):
        return True
    if enrichment.db.get_hexdb_cached(ac.icao):
        return True
    if enrichment.db.get_tar1090_cached(ac.icao):
        return True
    return False


def _ghost_credible(ac: dict) -> bool:
    """Return True if this aircraft is likely real and should be persisted."""
    if config.GHOST_FILTER_MSGS <= 0:
        return True
    if ac.get("msg_count", 0) >= config.GHOST_FILTER_MSGS:
        return True
    # MLAT-confirmed aircraft are real (multilaterated by multiple receivers)
    if ac.get("mlat"):
        return True
    icao = ac["icao"]
    if enrichment.db.get_adsbx(icao):
        return True
    if enrichment.db.get_hexdb_cached(icao):
        return True
    if enrichment.db.get_tar1090_cached(icao):
        return True
    return False


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
            samples = [
                (now_ts, ac["icao"], ac["bearing_deg"], ac["range_nm"],
                 ac.get("altitude"), ac.get("signal"))
                for ac in snapshot.get("aircraft", [])
                if ac.get("bearing_deg") is not None and ac.get("range_nm") is not None
            ]
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
        now_ts = int(time.time())
        squawking_icaos: set[str] = set()
        for ac in snapshot["aircraft"]:
            sq = ac.get("squawk") or ""
            if sq not in EMERGENCY_SQUAWKS:
                continue
            icao = ac["icao"]
            squawking_icaos.add(icao)
            if icao in _active_squawks and _active_squawks[icao]["squawk"] == sq:
                # Ongoing — update ts_last every 30s to avoid hammering DB every second
                if now_ts - _active_squawks[icao]["last_update"] >= 30:
                    await asyncio.to_thread(
                        stats_db.update_squawk_event_last,
                        _active_squawks[icao]["db_id"], now_ts, ac.get("altitude"),
                    )
                    _active_squawks[icao]["last_update"] = now_ts
            else:
                # New event (or squawk code changed)
                db_id = await asyncio.to_thread(
                    stats_db.write_squawk_event,
                    icao, sq, ac.get("callsign"), ac.get("altitude"), now_ts,
                )
                _active_squawks[icao] = {"squawk": sq, "db_id": db_id, "last_update": now_ts}
                log.info("Emergency squawk %s from %s", sq, icao)
                await asyncio.to_thread(
                    notifications.notify_emergency_squawk,
                    icao, sq, ac.get("callsign"), ac.get("altitude"), ac.get("operator"),
                )
        # Close out events for aircraft no longer squawking emergency
        for icao in list(_active_squawks.keys()):
            if icao not in squawking_icaos:
                del _active_squawks[icao]


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


async def _push_updates() -> None:
    """Broadcast a state snapshot to every connected WebSocket client every second."""
    import time
    global _watchlist_cache, _watchlist_cache_ts
    while True:
        await asyncio.sleep(1)
        expired = state.expire_aircraft()

        # Close visit records for aircraft that just timed out
        if expired:
            credible = [ac for ac in expired if _credible_aircraft(ac)]
            if credible:
                tuples = [
                    (ac.icao, int(ac.first_seen), int(ac.last_seen),
                     ac.callsign, ac.squawk, ac.max_altitude, ac.msg_count)
                    for ac in credible
                ]
                visit_ids = await asyncio.to_thread(stats_db.write_visits, tuples)
                for ac, vid in zip(credible, visit_ids):
                    if ac.callsign:
                        _route_queue.append((vid, ac.callsign))

        snapshot = state.get_snapshot()

        # Record track points (rate-limited to 1/5s per aircraft inside TrackStore)
        now = time.time()

        # Refresh watchlist cache from DB every 30s
        if now - _watchlist_cache_ts > 30:
            rows = await asyncio.to_thread(stats_db.get_notify_watchlist)
            _watchlist_cache = {r["icao"]: r["max_range_nm"] for r in rows}
            _watchlist_cache_ts = now

        # All live ICAOs — used to expire tracks for aircraft that have left the set.
        # Kept separate from the recording condition so a temporary loss of position
        # doesn't prematurely prune an existing trail.
        active_icaos: set[str] = {ac["icao"] for ac in snapshot["aircraft"]}
        # Build notification tasks and track positions in a single pass.
        # Skip thread dispatch entirely when no notification channel is configured —
        # avoids O(N × thread_overhead) per second even for no-op calls.
        notify_tasks = []
        _notify_enabled = notifications.any_channel()
        # Check per-trigger prefs once per cycle (uses cached prefs, <1µs each).
        # Avoids dispatching any threads for triggers the user has turned off.
        _mil_on  = _notify_enabled and notifications.trigger_enabled("notify_military")
        _int_on  = _notify_enabled and notifications.trigger_enabled("notify_interesting")
        # Collect matching aircraft first; dispatch at most 3 threads per cycle
        # regardless of how many aircraft match, replacing the previous O(N) dispatch.
        _mil_batch: list[dict] = []
        _int_batch: list[dict] = []
        _wl_batch:  list[dict] = []
        t_loop_start = time.perf_counter()
        for ac in snapshot["aircraft"]:
            icao = ac["icao"]
            if _notify_enabled:
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

            # Only record once the position is confirmed reliable:
            # - pos_global=True: a global CPR decode (even+odd pair) has succeeded,
            #   guaranteeing the position is not from the potentially-wrong local
            #   decode fallback that runs before the first pair is available.
            # - mlat=True: position established by multilateration, also reliable.
            if (ac.get("bearing_deg") is not None and ac.get("range_nm") is not None
                    and ac.get("lat") is not None
                    and (ac.get("pos_global") or ac.get("mlat"))):
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
                    now=now,
                )
        # Prune tracks for aircraft that have left the live set
        track_store.expire(active_icaos)
        t_sync_end = time.perf_counter()

        if notify_tasks:
            await asyncio.gather(*notify_tasks)
        t_gather_end = time.perf_counter()

        if not _clients:
            _push_timings_store.append({
                "sync_ms":     round((t_sync_end - t_loop_start) * 1000, 2),
                "gather_ms":   round((t_gather_end - t_sync_end) * 1000, 2),
                "notify_tasks": len(notify_tasks),
                "broadcast_ms": 0.0,
                "total_ms":    round((t_gather_end - t_loop_start) * 1000, 2),
                "ac_count":    len(snapshot["aircraft"]),
            })
            continue

        payload = _json_dumps(snapshot)
        dead: list[WebSocket] = []

        for ws in list(_clients):
            try:
                await ws.send_text(payload)
            except Exception:
                dead.append(ws)

        for ws in dead:
            try:
                _clients.remove(ws)
            except ValueError:
                pass

        t_done = time.perf_counter()
        _push_timings_store.append({
            "sync_ms":      round((t_sync_end - t_loop_start) * 1000, 2),
            "gather_ms":    round((t_gather_end - t_sync_end) * 1000, 2),
            "notify_tasks": len(notify_tasks),
            "broadcast_ms": round((t_done - t_gather_end) * 1000, 2),
            "total_ms":     round((t_done - t_loop_start) * 1000, 2),
            "ac_count":     len(snapshot["aircraft"]),
        })


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
             ac["bearing_deg"], ac["range_nm"], ac.get("altitude"),
             ac.get("military", False), ac.get("interesting", False),
             ac.get("type_code"), ac.get("type_category"))
            for ac in snapshot.get("aircraft", [])
            if (ac.get("bearing_deg") is not None
                and ac.get("range_nm") is not None
                and (ac.get("range_nm") or 0) > 0
                and (ac.get("altitude") or 0) > 0
                and _ghost_credible(ac))
        ]
        hires_buffer.record(samples)


# ---------------------------------------------------------------------------
# App lifespan
# ---------------------------------------------------------------------------

@asynccontextmanager
async def lifespan(app: FastAPI):
    await asyncio.to_thread(enrichment.db.load_or_download)
    await asyncio.to_thread(stats_db.rollup_missed_days)
    await asyncio.to_thread(stats_db.prune)
    await asyncio.to_thread(stats_db.backfill_daily_coverage)
    await asyncio.to_thread(stats_db.backfill_us_mil_years)

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

    global _decoder_thread
    _decoder_thread = _start_msg_processor()
    asyncio.create_task(_beast_runner())
    for name, host, port in config.MLAT_SERVERS:
        asyncio.create_task(_mlat_runner(name, host, port))
    asyncio.create_task(_push_updates())
    asyncio.create_task(_db_writer())
    asyncio.create_task(_db_update_checker())
    asyncio.create_task(_hexdb_cache_flusher())
    asyncio.create_task(_adsbx_task())
    asyncio.create_task(_hexdb_task())
    asyncio.create_task(_backup_runner())  # runs nightly; path resolved from DB/env at runtime
    asyncio.create_task(_hires_writer())
    asyncio.create_task(_route_enricher())
    log.info("ADS-B Dashboard backend started  (Beast: %s:%s)",
             config.BEAST_HOST, config.BEAST_PORT)

    yield

    # --- Graceful shutdown ---
    # Stop the decoder thread before the final DB write so it can't be
    # holding _lock when we call get_snapshot() below.
    log.info("Shutdown: stopping decoder thread…")
    _msg_queue.put(_DECODE_SENTINEL)
    if _decoder_thread is not None:
        _decoder_thread.join(timeout=2.0)

    log.info("Shutdown: flushing state to disk…")
    try:
        final_snapshot = state.get_snapshot()
        await asyncio.to_thread(stats_db.write_minute, final_snapshot)
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


app = FastAPI(title="ADS-B Dashboard", lifespan=lifespan)
tracks_module._track_store = track_store
app.include_router(tracks_router)
app.include_router(history_router)
aircraft_router._state = state  # type: ignore[attr-defined]
app.include_router(aircraft_router)
app.include_router(fleet_router)
app.include_router(coverage_router)
app.include_router(acas_router)
app.include_router(squawks_router)
app.include_router(status_router)
app.include_router(notify_settings_router)
app.include_router(debug_router)
if config.DEBUG_ENRICHMENT:
    log.info("Debug router mounted (DEBUG_ENRICHMENT=%s)", config.DEBUG_ENRICHMENT)

app.add_middleware(
    CORSMiddleware,
    allow_origins=config.CORS_ORIGINS,
    allow_methods=["GET", "POST", "DELETE"],
    allow_headers=["*"],
)


# ---------------------------------------------------------------------------
# Routes
# ---------------------------------------------------------------------------

@app.websocket("/ws")
async def websocket_endpoint(ws: WebSocket) -> None:
    await ws.accept()
    _clients.append(ws)
    log.info("WebSocket client connected  (total: %d)", len(_clients))

    try:
        # Send the current snapshot immediately on connect
        await ws.send_text(_json_dumps(state.get_snapshot()))
        # Keep the connection open; the push loop handles subsequent updates
        while True:
            await ws.receive_text()
    except WebSocketDisconnect:
        pass
    except Exception as exc:
        log.debug("WebSocket error: %s", exc)
    finally:
        try:
            _clients.remove(ws)
        except ValueError:
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
