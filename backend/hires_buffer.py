"""
High-resolution in-memory coverage track buffer.

Stores aircraft position samples at HIRES_INTERVAL_S resolution for up to
HIRES_MAX_AGE_S.  Not persistent — data exists only while the process is
running.  Intended to replace the 1-minute coverage_samples DB query for the
timelapse player when data is available.

Thread-safe: a single Lock guards all state.

Memory controls (both configurable via .env):
  HIRES_MAX_AGE_S   — retention window (default 12 h)
  HIRES_MAX_POINTS  — global hard cap on total stored points (default 500 k)
"""

import logging
import time
import threading
from collections import deque

import config
from db import StatsDB   # for _get_type_group_idx (staticmethod, no DB I/O)

log = logging.getLogger(__name__)

HIRES_INTERVAL_S = 10                        # minimum seconds between samples for the same ICAO
HIRES_MAX_AGE_S  = config.HIRES_MAX_AGE_S   # retention window
HIRES_MAX_POINTS = config.HIRES_MAX_POINTS  # global point cap

_lock         = threading.Lock()
_tracks:      dict[str, deque] = {}   # icao → deque of (ts, bearing, range, alt)
_meta:        dict[str, dict]  = {}   # icao → {military, interesting, type_code, type_category, operator, mlat}
_last_ts:     dict[str, int]   = {}   # icao → last recorded ts (rate-limiter)
_total_points: int = 0                # running total across all deques
_cap_logged:   bool = False           # warn only once per cap episode


def record(samples: list[tuple]) -> None:
    """Append a batch of position samples to the buffer.

    Each element of `samples`:
        (ts, icao, bearing_deg, range_nm, alt_ft,
         military, interesting, type_code, type_category, operator, mlat)

    Silently ignores samples where the same ICAO was recorded fewer than
    HIRES_INTERVAL_S seconds ago.  Prunes entries older than HIRES_MAX_AGE_S
    from each affected deque.  Drops new samples when the global point cap
    is reached (oldest retained data stays until it ages out naturally).
    """
    global _total_points, _cap_logged

    if not samples:
        return
    cutoff = int(time.time()) - HIRES_MAX_AGE_S
    with _lock:
        # When at the point cap, sweep ALL tracks for aged-out entries before
        # processing new samples.  Without this, data from inactive aircraft
        # (landed / out of range) is never pruned — they never appear in a new
        # samples batch so their per-deque pruning never runs — causing the cap
        # to stay permanently hit and new data to be silently dropped.
        if _total_points >= HIRES_MAX_POINTS:
            for dq in _tracks.values():
                while dq and dq[0][0] < cutoff:
                    dq.popleft()
                    _total_points -= 1

        for ts, icao, bearing, range_nm, alt, military, interesting, tc, tcat, operator, mlat in samples:
            if ts - _last_ts.get(icao, 0) < HIRES_INTERVAL_S:
                continue
            _last_ts[icao] = ts

            dq = _tracks.get(icao)
            if dq is None:
                dq = deque()
                _tracks[icao] = dq

            # Prune aged-out entries for this aircraft so the cap check sees
            # the true count after any recent window shrinkage.
            while dq and dq[0][0] < cutoff:
                dq.popleft()
                _total_points -= 1

            # Hard cap: drop only if still at limit after the global sweep above
            if _total_points >= HIRES_MAX_POINTS:
                if not _cap_logged:
                    log.warning(
                        "hires_buffer: global cap of %d points reached — "
                        "new samples dropped until old data ages out",
                        HIRES_MAX_POINTS,
                    )
                    _cap_logged = True
                continue

            _cap_logged = False  # reset warning once we're below the cap again
            dq.append((ts, bearing, range_nm, alt))
            _total_points += 1
            _meta[icao] = {
                "military":      bool(military),
                "interesting":   bool(interesting),
                "type_code":     tc,
                "type_category": tcat,
                "operator":      operator,
                "mlat":          bool(mlat),
            }

        # Remove ICAOs whose deques have been fully pruned
        empty = [icao for icao, dq in _tracks.items() if not dq]
        for icao in empty:
            del _tracks[icao]
            _meta.pop(icao, None)
            _last_ts.pop(icao, None)


def set_policy(max_age_s: int, interval_s: int) -> None:
    """Update retention window and sampling interval (called by memory_guard).

    Takes effect on the next record() call.  Call prune_now() immediately
    after to free memory without waiting for the next recording cycle.
    """
    global HIRES_MAX_AGE_S, HIRES_INTERVAL_S
    with _lock:
        HIRES_MAX_AGE_S = max_age_s
        HIRES_INTERVAL_S = interval_s


def prune_now() -> int:
    """Force an immediate age-based prune using the current HIRES_MAX_AGE_S.

    Returns the number of points removed.  Called by memory_guard on
    escalation so memory is freed without waiting for the next record() cycle.
    """
    global _total_points, _cap_logged
    removed = 0
    cutoff = int(time.time()) - HIRES_MAX_AGE_S
    with _lock:
        for dq in _tracks.values():
            while dq and dq[0][0] < cutoff:
                dq.popleft()
                _total_points -= 1
                removed += 1
        # Clean up fully-emptied deques
        empty = [icao for icao, dq in _tracks.items() if not dq]
        for icao in empty:
            del _tracks[icao]
            _meta.pop(icao, None)
            _last_ts.pop(icao, None)
        if removed:
            _cap_logged = False  # allow cap warning to fire again if needed
    return removed


def stats() -> dict:
    """Return current buffer statistics for observability."""
    with _lock:
        return {
            "icao_count":   len(_tracks),
            "total_points": _total_points,
            "max_points":   HIRES_MAX_POINTS,
            "max_age_s":    HIRES_MAX_AGE_S,
            "interval_s":   HIRES_INTERVAL_S,
        }


def query_tracks(start_ts: int, end_ts: int) -> dict:
    """Return position tracks for the requested time window.

    Response format is identical to db.StatsDB.query_timelapse_tracks so the
    frontend needs no changes:
        {start_ts, end_ts, tracks: [{icao, military, interesting, tg_idx, operator,
                                      points: [[dt_s, bearing, range, alt], ...]}, ...]}
    Only tracks with >= 2 points in the window are included.
    """
    with _lock:
        # Snapshot under lock; deques are copied as lists so we release quickly
        snapshot = {icao: list(dq) for icao, dq in _tracks.items()}
        meta_snap = dict(_meta)

    tracks = []
    for icao, pts in snapshot.items():
        window = [
            [ts - start_ts, round(b, 1), round(r, 1), int(a)]
            for ts, b, r, a in pts
            if start_ts <= ts <= end_ts and a is not None and r and a > 0
        ]
        if len(window) < 2:
            continue
        m = meta_snap.get(icao, {})
        tg_idx = StatsDB._get_type_group_idx(m.get("type_code"), m.get("type_category"))
        tracks.append({
            "icao":        icao,
            "military":    m.get("military",    False),
            "interesting": m.get("interesting", False),
            "tg_idx":      tg_idx,
            "type_code":   m.get("type_code"),
            "operator":    m.get("operator"),
            "mlat":        m.get("mlat",        False),
            "points":      window,
        })

    return {"start_ts": start_ts, "end_ts": end_ts, "tracks": tracks}
