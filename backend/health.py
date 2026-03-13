"""
Health / hardware telemetry API — Pi 4 operational metrics.

GET  /api/health   — CPU temp, CPU%, throttle flags, event-loop lag,
                     decode queue depth, WebSocket client count
"""

import asyncio
import logging
import subprocess
import time
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


async def loop_lag_sampler() -> None:
    """Measure asyncio event-loop lag by comparing intended vs actual sleep duration."""
    global _loop_lag_ms
    while True:
        t0 = time.monotonic()
        await asyncio.sleep(1.0)
        elapsed = time.monotonic() - t0
        _loop_lag_ms = round((elapsed - 1.0) * 1000, 1)


# ---------------------------------------------------------------------------
# Context registration — main.py calls this at startup with its queue / clients
# ---------------------------------------------------------------------------
_context: dict = {}


def register_context(msg_queue, clients: list) -> None:
    """Register main.py objects so the health endpoint can read live values."""
    _context["msg_queue"] = msg_queue
    _context["clients"] = clients


# ---------------------------------------------------------------------------
# Endpoint
# ---------------------------------------------------------------------------

@router.get("")
async def get_health() -> dict:
    return {
        "cpu_temp_c":    read_pi_temp(),
        "cpu_percent":   read_cpu_percent(),
        "loop_lag_ms":   _loop_lag_ms,
        "throttle":      read_throttle_flags(),
        "queue_depth":   _context["msg_queue"].qsize() if "msg_queue" in _context else None,
        "ws_clients":    len(_context["clients"])       if "clients"   in _context else None,
    }
