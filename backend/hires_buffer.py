"""
High-resolution in-memory coverage track buffer.

Stores aircraft position samples at HIRES_INTERVAL_S resolution for up to
HIRES_MAX_AGE_S (24 hours).  Not persistent — data exists only while the
process is running.  Intended to replace the 1-minute coverage_samples DB
query for the timelapse player when data is available.

Thread-safe: a single RLock guards all state.
"""

import time
import threading
from collections import deque

from db import StatsDB   # for _get_type_group_idx (staticmethod, no DB I/O)

HIRES_INTERVAL_S = 10    # minimum seconds between samples for the same ICAO
HIRES_MAX_AGE_S  = 86_400  # 24 hours

_lock      = threading.Lock()
_tracks:    dict[str, deque]  = {}   # icao → deque of (ts, bearing, range, alt)
_meta:      dict[str, dict]   = {}   # icao → {military, interesting, type_code, type_category, operator}
_last_ts:   dict[str, int]    = {}   # icao → last recorded ts (rate-limiter)


def record(samples: list[tuple]) -> None:
    """Append a batch of position samples to the buffer.

    Each element of `samples`:
        (ts, icao, bearing_deg, range_nm, alt_ft,
         military, interesting, type_code, type_category, operator)

    Silently ignores samples where the same ICAO was recorded fewer than
    HIRES_INTERVAL_S seconds ago.  Prunes entries older than HIRES_MAX_AGE_S
    from each affected deque.
    """
    if not samples:
        return
    cutoff = int(time.time()) - HIRES_MAX_AGE_S
    with _lock:
        for ts, icao, bearing, range_nm, alt, military, interesting, tc, tcat, operator in samples:
            if ts - _last_ts.get(icao, 0) < HIRES_INTERVAL_S:
                continue
            _last_ts[icao] = ts

            dq = _tracks.get(icao)
            if dq is None:
                dq = deque()
                _tracks[icao] = dq
            dq.append((ts, bearing, range_nm, alt))
            _meta[icao] = {
                "military":      bool(military),
                "interesting":   bool(interesting),
                "type_code":     tc,
                "type_category": tcat,
                "operator":      operator,
            }
            # Prune the tail of this deque (oldest entries first)
            while dq and dq[0][0] < cutoff:
                dq.popleft()

        # Remove ICAOs whose deques have been fully pruned
        empty = [icao for icao, dq in _tracks.items() if not dq]
        for icao in empty:
            del _tracks[icao]
            _meta.pop(icao, None)
            _last_ts.pop(icao, None)


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
            "operator":    m.get("operator"),
            "points":      window,
        })

    return {"start_ts": start_ts, "end_ts": end_ts, "tracks": tracks}
