"""
readsb stats supplementation — reads /run/readsb/stats.json (and optionally
/run/airspy_adsb/stats.json) once per minute to collect SDR health, CPR
quality, and signal metrics that are not available from the aircraft snapshot.

Used when INGEST_MODE=readsb or INGEST_MODE=hybrid.

The parsed stats are stored in _latest and exposed via get_latest() for the
status and debug endpoints.  No DB schema changes are required for Phase 1;
the data is in-memory only.
"""

import asyncio
import logging
import os
import time
from typing import Any

try:
    import orjson as _json_lib
    def _json_loads(data: bytes) -> dict:
        return _json_lib.loads(data)
except ImportError:
    import json as _json_lib  # type: ignore[no-redef]
    def _json_loads(data: bytes) -> dict:  # type: ignore[misc]
        return _json_lib.loads(data)

import config
from db import stats_db

log = logging.getLogger(__name__)

# Module-level cache — last successfully parsed stats snapshot
_latest: dict[str, Any] = {}
_latest_ts: float = 0.0


def get_latest() -> dict[str, Any]:
    """Return the most recently parsed readsb stats (empty dict if never read)."""
    return _latest


def _read_json_file(path: str) -> dict | None:
    try:
        with open(path, "rb") as f:
            return _json_loads(f.read())
    except FileNotFoundError:
        return None
    except Exception as exc:
        log.warning("readsb_stats: failed to read %s: %s", path, exc)
        return None


def _parse_readsb_stats(data: dict) -> dict:
    """Extract the fields we care about from stats.json."""
    last1 = data.get("last1min", {})
    local = last1.get("local", {})
    cpr   = last1.get("cpr", {})
    tracks = last1.get("tracks", {})
    cpu   = last1.get("cpu", {})

    accepted = local.get("accepted", [])
    total_accepted = sum(accepted) if isinstance(accepted, list) else 0

    return {
        # Signal quality
        "signal_avg_dbfs":   local.get("signal"),
        "signal_peak_dbfs":  local.get("peak_signal"),
        "noise_dbfs":        local.get("noise"),
        "strong_signals":    local.get("strong_signals"),
        # Message counts
        "total_accepted":    total_accepted,
        "mode_s_total":      local.get("modes"),
        "bad_preambles":     local.get("bad"),
        # SDR health
        "blocks_processed":  local.get("blocks_processed"),
        "blocks_dropped":    local.get("blocks_dropped"),
        # CPR quality
        "cpr_global_ok":     cpr.get("global_ok"),
        "cpr_global_bad":    cpr.get("global_bad"),
        "cpr_global_speed":  cpr.get("global_speed"),
        "cpr_local_ok":      cpr.get("local_ok"),
        "cpr_filtered":      cpr.get("filtered"),
        # Track counts
        "tracks_all":        tracks.get("all"),
        "tracks_single_msg": tracks.get("single_message"),
        # readsb CPU (milliseconds per period)
        "cpu_demod_ms":      cpu.get("demod"),
        "cpu_reader_ms":     cpu.get("reader"),
        "cpu_background_ms": cpu.get("background"),
    }


def _normalise_df_counts(raw: Any) -> dict[int, int] | None:
    """Convert df_counts to {df: count} regardless of whether airspy gave a list or dict."""
    if raw is None:
        return None
    if isinstance(raw, dict):
        return {int(k): int(v) for k, v in raw.items() if v}
    if isinstance(raw, list):
        # list indexed by DF number: position = DF type, value = count
        return {i: int(v) for i, v in enumerate(raw) if v}
    return None


def _parse_airspy_stats(data: dict) -> dict:
    """Extract fields from airspy_adsb stats.json."""
    def _quartile(obj: Any) -> dict | None:
        if not isinstance(obj, dict):
            return None
        return {k: obj.get(k) for k in ("min", "p5", "q1", "median", "q3", "p95", "max")}

    return {
        "airspy_rssi":           _quartile(data.get("rssi")),
        "airspy_snr":            _quartile(data.get("snr")),
        "airspy_noise":          _quartile(data.get("noise")),
        "airspy_gain":           data.get("gain"),
        "airspy_lost_buffers":   data.get("lost_buffers"),
        "airspy_max_aircraft":   data.get("max_aircraft_count"),
        # df_counts may be a list (indexed by DF number) or a dict — normalise to dict
        "airspy_df_counts":      _normalise_df_counts(data.get("df_counts")),
        "airspy_samplerate":     data.get("samplerate"),
        "airspy_preamble_filter":data.get("preamble_filter"),
    }


async def readsb_stats_poller() -> None:
    """Async background task: read readsb stats.json every 60 seconds.

    Populates the module-level _latest dict for consumption by status/debug endpoints.
    """
    global _latest, _latest_ts

    stats_path  = os.path.join(config.READSB_JSON_DIR, "stats.json")
    airspy_path = config.AIRSPY_STATS_PATH

    log.info("readsb_stats: polling %s every 60s", stats_path)

    while True:
        try:
            stats_data = await asyncio.to_thread(_read_json_file, stats_path)
            result: dict[str, Any] = {"ts": time.time()}

            if stats_data:
                result.update(_parse_readsb_stats(stats_data))
            else:
                log.debug("readsb_stats: stats.json not available")

            # Optional airspy_adsb supplementation
            airspy_data = await asyncio.to_thread(_read_json_file, airspy_path)
            if airspy_data:
                result.update(_parse_airspy_stats(airspy_data))

            _latest    = result
            _latest_ts = result["ts"]

            # Persist SDR/CPR health into the most recently completed minute row.
            # last1min data aligns with the previous 60-second boundary.
            if stats_data:
                minute_ts = (int(_latest_ts) // 60) * 60 - 60
                await asyncio.to_thread(stats_db.write_readsb_stats, minute_ts, result)

        except asyncio.CancelledError:
            raise
        except Exception as exc:
            log.exception("readsb_stats: unexpected error: %s", exc)

        await asyncio.sleep(60)
