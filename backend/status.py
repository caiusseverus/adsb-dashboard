"""
Status API — database size, table row counts, retention info, Pi health.

Two endpoints:
  GET /api/status        — fast: config, Pi health, notification prefs (no table scans)
  GET /api/status/tables — slow: per-table row counts, sizes, backup info
"""

import asyncio
import pathlib
import subprocess
from collections.abc import Callable
from fastapi import APIRouter

import config as _config
import hires_buffer
import memory_policy
from db import stats_db

# Callable registered by main.py at startup to avoid a circular import.
_runtime_stats_fn: Callable[[], dict] | None = None


def _runtime_git_sha() -> str | None:
    try:
        return (
            subprocess.check_output(
                ["git", "rev-parse", "--short=8", "HEAD"],
                cwd=pathlib.Path(__file__).resolve().parent.parent,
                text=True,
                stderr=subprocess.DEVNULL,
            )
            .strip()
            or None
        )
    except Exception:
        return None


RUNTIME_GIT_SHA = _runtime_git_sha()


def register_runtime_stats(fn: Callable[[], dict]) -> None:
    """Called once from main.py lifespan to wire in live queue/client metrics."""
    global _runtime_stats_fn
    _runtime_stats_fn = fn

router = APIRouter(prefix="/api/status")


def _pi_health() -> dict:
    """Read Raspberry Pi thermal and throttle state from sysfs.
    Returns an empty dict on non-Pi hardware (paths simply won't exist)."""
    result: dict = {}

    temp_path = pathlib.Path("/sys/class/thermal/thermal_zone0/temp")
    if temp_path.exists():
        try:
            result["cpu_temp_c"] = round(int(temp_path.read_text()) / 1000, 1)
        except Exception:
            pass

    # BCM2711 throttle flags — bit meanings:
    #   0x1  under-voltage detected      0x10000  under-voltage has occurred
    #   0x2  currently throttled         0x20000  throttling has occurred
    #   0x4  ARM freq capped             0x40000  ARM freq capping has occurred
    #   0x8  soft temp limit active      0x80000  soft temp limit has occurred
    throttle_path = pathlib.Path("/sys/devices/platform/soc/soc:firmware/get_throttled")
    if throttle_path.exists():
        try:
            flags = int(throttle_path.read_text().strip(), 16)
            result["throttled"]        = bool(flags & 0x2)
            result["under_voltage"]    = bool(flags & 0x1)
            result["throttle_occurred"]= bool(flags & 0x20000)
            result["throttle_flags"]   = hex(flags)
        except Exception:
            pass

    meminfo = pathlib.Path("/proc/meminfo")
    if meminfo.exists():
        try:
            for line in meminfo.read_text().splitlines():
                if line.startswith("MemAvailable:"):
                    result["mem_available_mb"] = round(int(line.split()[1]) / 1024, 1)
                    break
        except Exception:
            pass

    return result


@router.get("")
async def get_status() -> dict:
    """Fast — config, Pi health, notification prefs, memory pressure. No table scans."""
    pi_health, notifications, mem_status, hires_stats = await asyncio.gather(
        asyncio.to_thread(_pi_health),
        asyncio.to_thread(stats_db.query_status_notifications),
        asyncio.to_thread(memory_policy.get_status),
        asyncio.to_thread(hires_buffer.stats),
    )
    runtime = await asyncio.to_thread(_runtime_stats_fn) if _runtime_stats_fn else {}
    return {
        "config": {
            "runtime_git_sha":            RUNTIME_GIT_SHA,
            "RADAR_SYNC_CONTAMINATION_DETECTION_ENABLED": _config.RADAR_SYNC_CONTAMINATION_DETECTION_ENABLED,
            "RADAR_SYNC_GO_REFINER_OPERATIONAL": _config.RADAR_SYNC_GO_REFINER_OPERATIONAL,
            "RADAR_CORE_ENABLED": _config.RADAR_CORE_ENABLED,
            "RADAR_CORE_MANAGED": _config.RADAR_CORE_MANAGED,
            "minute_stats_retention_days": _config.MINUTE_STATS_RETENTION_DAYS,
            "coverage_retention_days":     90,
            "acas_retention_days":         90,
            "ghost_filter_msgs":           _config.GHOST_FILTER_MSGS,
            "rare_threshold":              _config.RARE_THRESHOLD,
            "receiver_lat":                _config.RECEIVER_LAT,
            "receiver_lon":                _config.RECEIVER_LON,
            "debug":                       _config.DEBUG_LOG,
            "terrain_enabled":             _config.TERRAIN_ENABLED,
            "radar_core_enabled":          _config.RADAR_CORE_ENABLED,
            "radar_core_frames_enabled":   _config.RADAR_CORE_FRAMES_ENABLED,
            "radar_core_managed":          _config.RADAR_CORE_MANAGED,
            "radar_core_socket":           _config.RADAR_CORE_SOCKET,
            "radar_core_binary":           _config.RADAR_CORE_BINARY,
        },
        "pi_health":       pi_health,
        "notifications":   notifications,
        "memory_pressure": mem_status,
        "hires_buffer":    hires_stats,
        "ws_clients":      runtime.get("ws_clients", 0),
        # Route enrichment queue: async HTTP lookups for origin/dest airports.
        # Not the live radar delivery path — drops here only affect visit route data.
        "route_enrichment_queue": {
            "size":  runtime.get("route_enrichment_queue_size",  0),
            "drops": runtime.get("route_enrichment_queue_drops", 0),
        },
        # Live WebSocket delivery — use these to judge coalescing/push health.
        "ws_live_delivery": {
            "main_frame_drops":    runtime.get("ws_main_frame_drops",    0),
            "sync_rebuilds":       runtime.get("ws_sync_rebuilds",       0),
            "sync_emissions":      runtime.get("ws_sync_emissions",       0),
            "selected_state_rebuilds": runtime.get("ws_selected_state_rebuilds", 0),
            "selected_state_emissions": runtime.get("ws_selected_state_emissions", 0),
            "live_emissions":      runtime.get("ws_live_emissions",       0),
            "client_queue_depths": runtime.get("ws_client_queue_depths", []),
        },
        "radar_core": runtime.get("radar_core", {}),
        "runtime": {
            "radar_core_pid": runtime.get("radar_core", {}).get("radar_core_pid"),
            "radar_core_running": runtime.get("radar_core", {}).get("radar_core_running"),
            "radar_core_defunct": runtime.get("radar_core", {}).get("radar_core_defunct"),
            "radar_core_exit_code": runtime.get("radar_core", {}).get("radar_core_exit_code"),
            "radar_core_socket_path": runtime.get("radar_core", {}).get("socket_path") or _config.RADAR_CORE_SOCKET,
            "radar_core_connected": runtime.get("radar_core", {}).get("radar_core_connected"),
            "radar_core_last_error": runtime.get("radar_core", {}).get("radar_core_last_error"),
        },
        "in_memory_state": {
            "aircraft_state": runtime.get("aircraft_state", {}),
            "cast":           runtime.get("cast", {}),
            "track_store":    runtime.get("track_store", {}),
            "radar_state":    runtime.get("radar_state", {}),
        },
    }


@router.get("/tables")
async def get_status_tables() -> dict:
    """Slow — per-table row counts, sizes, backup info. Runs dbstat scans."""
    return await asyncio.to_thread(stats_db.query_status_tables)
