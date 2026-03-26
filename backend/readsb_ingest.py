"""
readsb JSON ingest — polls /run/readsb/aircraft.json every READSB_POLL_INTERVAL_S
and calls state.update_from_json().

Used when INGEST_MODE=readsb or INGEST_MODE=hybrid.  In hybrid mode this module
handles position/EHS data; Beast TCP handles raw frames for MLAT/ACAS.
"""

import asyncio
import logging
import os
import time
from pathlib import Path
from typing import TYPE_CHECKING

try:
    import orjson as _json_lib
    def _json_loads(data: bytes) -> dict:
        return _json_lib.loads(data)
except ImportError:
    import json as _json_lib  # type: ignore[no-redef]
    def _json_loads(data: bytes) -> dict:  # type: ignore[misc]
        return _json_lib.loads(data)

import config
import readsb_stats

if TYPE_CHECKING:
    from aircraft_state import AircraftState

log = logging.getLogger(__name__)

# File-age thresholds (seconds)
_STALE_WARN_S  = 5.0
_STALE_ERROR_S = 30.0


def _read_json_file(path: str) -> dict | None:
    """Read and parse a JSON file; return None on any error."""
    try:
        with open(path, "rb") as f:
            return _json_loads(f.read())
    except FileNotFoundError:
        return None
    except Exception as exc:
        log.warning("readsb_ingest: failed to read %s: %s", path, exc)
        return None


def _load_receiver_position() -> None:
    """Read receiver.json and set RECEIVER_LAT/LON in config if not already set.

    readsb writes this file infrequently; called once at startup.
    Only overrides config if the user has not set the env vars.
    """
    if config.RECEIVER_LAT is not None and config.RECEIVER_LON is not None:
        return  # user-configured values take precedence

    path = os.path.join(config.READSB_JSON_DIR, "receiver.json")
    data = _read_json_file(path)
    if not data:
        return

    lat = data.get("lat")
    lon = data.get("lon")
    if lat is not None and lon is not None:
        config.RECEIVER_LAT = float(lat)
        config.RECEIVER_LON = float(lon)
        log.info(
            "readsb_ingest: receiver position from receiver.json: %.4f, %.4f",
            config.RECEIVER_LAT, config.RECEIVER_LON,
        )


async def readsb_poller(state: "AircraftState") -> None:
    """Async background task: poll aircraft.json and feed state.update_from_json().

    Runs forever; caller should wrap in asyncio.create_task() and cancel on shutdown.
    """
    aircraft_path = os.path.join(config.READSB_JSON_DIR, "aircraft.json")
    _stale_warned = False

    # Auto-populate receiver position from readsb's receiver.json
    await asyncio.to_thread(_load_receiver_position)

    log.info("readsb_ingest: polling %s every %.1fs", aircraft_path, config.READSB_POLL_INTERVAL_S)

    while True:
        t0 = time.monotonic()

        try:
            data = await asyncio.to_thread(_read_json_file, aircraft_path)

            if data is None:
                # File missing — log once, keep trying
                if not _stale_warned:
                    log.warning("readsb_ingest: %s not found", aircraft_path)
                    _stale_warned = True
            else:
                _stale_warned = False

                # File-age watchdog using the 'now' field readsb writes
                file_now = data.get("now")
                wall_now  = time.time()
                if file_now is not None:
                    staleness = wall_now - file_now
                    if staleness > _STALE_ERROR_S:
                        log.error(
                            "readsb_ingest: aircraft.json is %.0f s stale — readsb may be down",
                            staleness,
                        )
                    elif staleness > _STALE_WARN_S:
                        log.warning(
                            "readsb_ingest: aircraft.json is %.0f s stale", staleness
                        )

                aircraft_list  = data.get("aircraft", [])
                total_messages = data.get("messages", 0)
                now            = file_now if file_now is not None else wall_now

                # Pass airspy df_counts if available for live DF type breakdown
                latest_stats = readsb_stats.get_latest()
                df_counts = latest_stats.get("airspy_df_counts") or None

                await asyncio.to_thread(
                    state.update_from_json, aircraft_list, now, total_messages, df_counts
                )

        except asyncio.CancelledError:
            raise
        except Exception as exc:
            log.exception("readsb_ingest: unexpected error: %s", exc)

        # Sleep for the remainder of the poll interval
        elapsed = time.monotonic() - t0
        sleep_s = max(0.0, config.READSB_POLL_INTERVAL_S - elapsed)
        await asyncio.sleep(sleep_s)
