"""
Health / hardware telemetry API — Pi 4 operational metrics.

GET  /api/health   — CPU temp, CPU%, throttle flags, event-loop lag,
                     decode queue depth, WebSocket client count
"""

import asyncio
import logging
import subprocess
import time
import traceback
from pathlib import Path

from fastapi import APIRouter

log = logging.getLogger(__name__)
router = APIRouter(prefix="/api/health")

# ---------------------------------------------------------------------------
# CPU% state (delta between consecutive /proc/stat reads)
# ---------------------------------------------------------------------------
_cpu_prev_total: int = 0
_cpu_prev_idle: int = 0


def read_cpu_percent() -> float | None:
    """Return current CPU utilisation % from /proc/stat (Linux/Pi only)."""
    global _cpu_prev_total, _cpu_prev_idle
    try:
        line = Path("/proc/stat").read_text().split("\n")[0]
        vals = list(map(int, line.split()[1:]))
        idle = vals[3] + vals[4]          # idle + iowait
        total = sum(vals)
        dt = total - _cpu_prev_total
        didle = idle - _cpu_prev_idle
        _cpu_prev_total, _cpu_prev_idle = total, idle
        return 0.0 if dt <= 0 else round((1 - didle / dt) * 100, 1)
    except Exception:
        return None


def read_pi_temp() -> float | None:
    """Return CPU junction temperature in °C from the sysfs thermal zone."""
    try:
        return float(Path("/sys/class/thermal/thermal_zone0/temp").read_text()) / 1000.0
    except Exception:
        return None


def read_throttle_flags() -> dict | None:
    """Return ARM throttle/under-voltage flags from vcgencmd (Pi only)."""
    try:
        result = subprocess.run(
            ["vcgencmd", "get_throttled"],
            capture_output=True, text=True, timeout=1,
        )
        val = int(result.stdout.strip().split("=")[1], 16)
        return {
            "raw":                 hex(val),
            "currently_throttled": bool(val & 0x04),
            "arm_freq_capped":     bool(val & 0x02),
            "under_voltage":       bool(val & 0x01),
            "throttling_occurred": bool(val & 0x40000),
        }
    except Exception:
        return None


# ---------------------------------------------------------------------------
# Event-loop lag sampler — started as a background task by main.py
# ---------------------------------------------------------------------------
_loop_lag_ms: float = 0.0
_loop_heartbeat_ts: float = 0.0


async def loop_lag_sampler() -> None:
    """Measure asyncio event-loop lag by comparing intended vs actual sleep duration."""
    global _loop_lag_ms, _loop_heartbeat_ts
    while True:
        t0 = time.monotonic()
        await asyncio.sleep(1.0)
        elapsed = time.monotonic() - t0
        _loop_lag_ms = round((elapsed - 1.0) * 1000, 1)
        _loop_heartbeat_ts = time.time()


def get_loop_heartbeat_ts() -> float:
    return float(_loop_heartbeat_ts)


# ---------------------------------------------------------------------------
# Context registration — main.py calls this at startup with its queue / clients
# ---------------------------------------------------------------------------
_context: dict = {}


def register_context(msg_queue, clients: list, radar_state=None) -> None:
    """Register main.py objects so the health endpoint can read live values."""
    _context["msg_queue"] = msg_queue
    _context["clients"] = clients
    _context["radar_state"] = radar_state


# ---------------------------------------------------------------------------
# Endpoint
# ---------------------------------------------------------------------------

@router.get("")
async def get_health() -> dict:
    radar_lock_runtime = None
    radar_lock_type = None
    radar_lock_snapshot_error = None
    radar_state = _context.get("radar_state")
    if radar_state is not None:
        try:
            radar_lock = getattr(radar_state, "_lock", None)
            radar_lock_type = type(radar_lock).__name__ if radar_lock is not None else None
            if radar_lock is not None and hasattr(radar_lock, "snapshot"):
                radar_lock_runtime = radar_lock.snapshot()
        except Exception:
            radar_lock_snapshot_error = traceback.format_exc(limit=1)
            radar_lock_runtime = None
    return {
        "cpu_temp_c":    read_pi_temp(),
        "cpu_percent":   read_cpu_percent(),
        "loop_lag_ms":   _loop_lag_ms,
        "throttle":      read_throttle_flags(),
        "queue_depth":   _context["msg_queue"].qsize() if "msg_queue" in _context else None,
        "ws_clients":    len(_context["clients"])       if "clients"   in _context else None,
        "radar_lock_runtime": radar_lock_runtime,
        "radar_state_present": "radar_state" in _context and _context.get("radar_state") is not None,
        "context_keys": sorted(_context.keys()),
        "radar_lock_type": radar_lock_type,
        "radar_lock_snapshot_error": radar_lock_snapshot_error,
    }
