"""
Status API — database size, table row counts, retention info, Pi health.
"""

import asyncio
import pathlib
from fastapi import APIRouter

from db import stats_db

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
    status = await asyncio.to_thread(stats_db.query_status)
    status["pi_health"] = await asyncio.to_thread(_pi_health)
    return status
