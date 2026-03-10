import asyncio
import json
import logging
from contextlib import asynccontextmanager
from datetime import date

from fastapi import FastAPI, WebSocket, WebSocketDisconnect
from fastapi.middleware.cors import CORSMiddleware
from fastapi.staticfiles import StaticFiles

import config
import enrichment
from beast_client import BeastClient
from aircraft_state import AircraftState
from db import stats_db
from history import router as history_router
from aircraft import router as aircraft_router
from fleet import router as fleet_router
from coverage import router as coverage_router
from acas import router as acas_router
from status import router as status_router
from debug import router as debug_router

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-8s  %(name)s  %(message)s",
)
log = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Shared state
# ---------------------------------------------------------------------------
state = AircraftState(aircraft_timeout=config.AIRCRAFT_TIMEOUT)
_clients: list[WebSocket] = []


# ---------------------------------------------------------------------------
# Background tasks
# ---------------------------------------------------------------------------

async def _beast_runner() -> None:
    def on_message(msg: dict) -> None:
        state.process_message(msg)

    client = BeastClient(config.BEAST_HOST, config.BEAST_PORT, on_message)
    await client.run()


async def _mlat_runner(name: str, host: str, port: int) -> None:
    def on_mlat_message(msg: dict) -> None:
        state.process_message(msg, mlat_source=name)

    client = BeastClient(host, port, on_mlat_message)
    log.info("MLAT runner starting: %s (%s:%s)", name, host, port)
    await client.run()


async def _db_update_checker() -> None:
    while True:
        await asyncio.sleep(86400)
        await asyncio.to_thread(enrichment.db.check_for_updates)


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
            last_day = today
        snapshot = state.get_snapshot()
        # Filter out ghost aircraft (bogus CRC decodes) before persisting
        if config.GHOST_FILTER_MSGS > 0:
            credible = [ac for ac in snapshot.get("aircraft", []) if _ghost_credible(ac)]
            snapshot = {**snapshot, "aircraft": credible}
        await asyncio.to_thread(stats_db.write_minute, snapshot)

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
                {
                    "ts":          now_ts,
                    "icao":        ac["icao"],
                    "bearing_deg": ac["bearing_deg"],
                    "range_nm":    ac["range_nm"],
                    "altitude":    ac.get("altitude"),
                    "signal":      ac.get("signal"),
                }
                for ac in snapshot.get("aircraft", [])
                if ac.get("bearing_deg") is not None and ac.get("range_nm") is not None
            ]
            await asyncio.to_thread(stats_db.write_coverage, samples)

        # Drain pending ACAS events to DB
        acas_evts = state.pop_acas_events()
        if acas_evts:
            await asyncio.to_thread(stats_db.write_acas_events, acas_evts)


async def _push_updates() -> None:
    """Broadcast a state snapshot to every connected WebSocket client every second."""
    while True:
        await asyncio.sleep(1)
        state.expire_aircraft()

        if not _clients:
            continue

        payload = json.dumps(state.get_snapshot())
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


# ---------------------------------------------------------------------------
# App lifespan
# ---------------------------------------------------------------------------

@asynccontextmanager
async def lifespan(app: FastAPI):
    enrichment.db.load_or_download()
    await asyncio.to_thread(stats_db.rollup_missed_days)
    await asyncio.to_thread(stats_db.prune)
    await asyncio.to_thread(stats_db.backfill_daily_coverage)
    await asyncio.to_thread(stats_db.backfill_us_mil_years)

    # Seed today's unique-aircraft sets from DB so counts survive restarts
    today = date.today().isoformat()
    today_data = await asyncio.to_thread(stats_db.query_today_icaos, today)
    state.init_today(today_data["all"], today_data["military"])

    # Seed sighting counts so live aircraft correctly reflect DB state
    sighting_counts = await asyncio.to_thread(stats_db.query_all_sighting_counts)
    state.seed_sighting_counts(sighting_counts)
    log.info("Seeded sighting counts for %d aircraft", len(sighting_counts))

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

    asyncio.create_task(_beast_runner())
    for name, host, port in config.MLAT_SERVERS:
        asyncio.create_task(_mlat_runner(name, host, port))
    asyncio.create_task(_push_updates())
    asyncio.create_task(_db_writer())
    asyncio.create_task(_db_update_checker())
    asyncio.create_task(_hexdb_task())
    log.info("ADS-B Dashboard backend started  (Beast: %s:%s)",
             config.BEAST_HOST, config.BEAST_PORT)
    yield


app = FastAPI(title="ADS-B Dashboard", lifespan=lifespan)
app.include_router(history_router)
aircraft_router._state = state  # type: ignore[attr-defined]
app.include_router(aircraft_router)
app.include_router(fleet_router)
app.include_router(coverage_router)
app.include_router(acas_router)
app.include_router(status_router)
app.include_router(debug_router)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
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
        await ws.send_text(json.dumps(state.get_snapshot()))
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
