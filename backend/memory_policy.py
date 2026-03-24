"""
Memory pressure policy for resource-constrained deployments (Raspberry Pi 4+).

Reads /proc/meminfo every poll cycle and computes a pressure level based on
the percentage of total RAM that is currently available.  Exposes policy
values (hires retention, snapshot mode, aircraft timeout) that other modules
apply when the level changes.

Pressure levels (% of MemTotal as MemAvailable):
  normal   >= 25%  — full retention, no changes
  elevated  18-25% — reduce hires to 6 h / 20 s interval, drop mlat_sources
  high      12-18% — hires 2 h / 30 s, halve aircraft timeout
  critical  < 12%  — hires 30 min, thin snapshot

Hysteresis:
  Escalation (worsening) is applied immediately on first reading.
  De-escalation requires DEESCALATE_READINGS consecutive readings at a
  lower level (default 3 × 10 s = 30 s sustained improvement).

Non-Linux fallback:
  If /proc/meminfo does not exist the module locks to "normal" and logs
  once.  All get_* functions remain safe to call.
"""

import logging
import pathlib
import threading

log = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Pressure level definitions
# ---------------------------------------------------------------------------

# Each level: (min_pct_free, hires_max_age_s, hires_interval_s, snapshot_mode, timeout_halved)
_LEVELS: list[tuple[str, float, int, int, str, bool]] = [
    # name        min_%  hires_age   interval  snap_mode   halve_timeout
    ("normal",    25.0,  43200,      10,       "full",     False),
    ("elevated",  18.0,  21600,      20,       "reduced",  False),
    ("high",      12.0,   7200,      30,       "reduced",  True),
    ("critical",   0.0,   1800,      30,       "thin",     True),
]

# How many consecutive lower-pressure readings before de-escalating
DEESCALATE_READINGS = 3

# ---------------------------------------------------------------------------
# Module state
# ---------------------------------------------------------------------------

_lock = threading.Lock()
_mem_total_kb: int = 0          # read once; 0 means /proc/meminfo unavailable
_available: bool = False         # True once MemTotal was read successfully
_current_level_idx: int = 0     # index into _LEVELS (0 = normal)
_deescalate_count: int = 0      # consecutive readings at a better level
_pending_level_idx: int = 0     # candidate for de-escalation


def _read_meminfo() -> dict[str, int]:
    """Parse key fields from /proc/meminfo.  Returns {} on any error."""
    p = pathlib.Path("/proc/meminfo")
    if not p.exists():
        return {}
    result: dict[str, int] = {}
    try:
        for line in p.read_text().splitlines():
            if line.startswith(("MemTotal:", "MemAvailable:")):
                parts = line.split()
                result[parts[0].rstrip(":")] = int(parts[1])
            if len(result) == 2:
                break
    except Exception:
        pass
    return result


def _level_idx_for_pct(pct: float) -> int:
    """Return the index of the most-severe level whose min_pct_free <= pct."""
    # Walk from most severe (last) to least severe (first); first match wins
    for i in range(len(_LEVELS) - 1, -1, -1):
        if pct >= _LEVELS[i][1]:
            return i
    return len(_LEVELS) - 1  # critical — shouldn't be reached


# ---------------------------------------------------------------------------
# Initialisation — read MemTotal once at import time
# ---------------------------------------------------------------------------

def _init() -> None:
    global _mem_total_kb, _available
    info = _read_meminfo()
    if "MemTotal" in info:
        _mem_total_kb = info["MemTotal"]
        _available = True
        log.info("memory_policy: MemTotal=%d MB", _mem_total_kb // 1024)
    else:
        log.info("memory_policy: /proc/meminfo not available — locked to 'normal'")


_init()

# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------

def check() -> str:
    """Re-evaluate memory pressure and return the current level name.

    Applies hysteresis: escalation is immediate; de-escalation requires
    DEESCALATE_READINGS consecutive readings at a better level.
    """
    global _current_level_idx, _deescalate_count, _pending_level_idx

    if not _available or _mem_total_kb == 0:
        return "normal"

    info = _read_meminfo()
    mem_avail = info.get("MemAvailable", 0)
    pct_free = (mem_avail / _mem_total_kb) * 100.0

    new_idx = _level_idx_for_pct(pct_free)

    with _lock:
        if new_idx > _current_level_idx:
            # Escalation (worsening) — apply immediately
            old = _LEVELS[_current_level_idx][0]
            _current_level_idx = new_idx
            _deescalate_count = 0
            _pending_level_idx = new_idx
            log.info(
                "memory_policy: %s → %s (%.1f%% free, %d MB available)",
                old, _LEVELS[_current_level_idx][0], pct_free, mem_avail // 1024,
            )
        elif new_idx < _current_level_idx:
            # De-escalation candidate — require sustained improvement
            if new_idx == _pending_level_idx:
                _deescalate_count += 1
            else:
                _pending_level_idx = new_idx
                _deescalate_count = 1

            if _deescalate_count >= DEESCALATE_READINGS:
                old = _LEVELS[_current_level_idx][0]
                _current_level_idx = new_idx
                _deescalate_count = 0
                _pending_level_idx = new_idx
                log.info(
                    "memory_policy: %s → %s (%.1f%% free, %d MB available)",
                    old, _LEVELS[_current_level_idx][0], pct_free, mem_avail // 1024,
                )
        else:
            # Same level — reset de-escalation counter
            _deescalate_count = 0
            _pending_level_idx = new_idx

        return _LEVELS[_current_level_idx][0]


def get_level() -> str:
    """Return current pressure level name without re-evaluating."""
    with _lock:
        return _LEVELS[_current_level_idx][0]


def get_policy() -> dict:
    """Return active policy values for consumers."""
    with _lock:
        _, _, age_s, interval_s, snap_mode, halve_timeout = _LEVELS[_current_level_idx]
        return {
            "snapshot_mode":    snap_mode,
            "hires_max_age_s":  age_s,
            "hires_interval_s": interval_s,
            "halve_timeout":    halve_timeout,
        }


def get_status() -> dict:
    """Return current state for /api/status observability."""
    with _lock:
        idx = _current_level_idx
        level_name, min_pct, age_s, interval_s, snap_mode, halve_timeout = _LEVELS[idx]

    info = _read_meminfo()
    mem_avail_kb = info.get("MemAvailable", 0)
    pct_free = round((mem_avail_kb / _mem_total_kb) * 100.0, 1) if _mem_total_kb else None

    return {
        "level":            level_name,
        "mem_available_mb": round(mem_avail_kb / 1024, 1) if mem_avail_kb else None,
        "mem_total_mb":     round(_mem_total_kb / 1024, 1) if _mem_total_kb else None,
        "pct_free":         pct_free,
        "available":        _available,
        "policy": {
            "snapshot_mode":    snap_mode,
            "hires_max_age_s":  age_s,
            "hires_interval_s": interval_s,
            "halve_timeout":    halve_timeout,
        },
    }
