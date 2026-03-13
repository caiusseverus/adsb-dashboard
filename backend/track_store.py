"""
In-memory rolling track store for live aircraft positions.

Points are sampled at most every SAMPLE_INTERVAL_S seconds per aircraft and
kept for WINDOW_S seconds (30 minutes).  Data is intentionally ephemeral —
lost on restart — which is fine because the SkyView and 3-D view only show
currently-visible aircraft.

Each track point is a plain tuple for minimal memory overhead:
  (ts, bearing_deg, range_nm, altitude_ft, lat, lon,
   military, mlat, interesting, acas_ra_active, mlat_source)

Schema version 2 added mlat_source (index 10) as a trailing field.
Consumers reading only indices 0–9 are unaffected.
"""

# Increment when tuple schema changes (trailing fields only — safe for existing consumers).
TRACK_SCHEMA_VERSION = 2

import threading
import time
from collections import deque
from typing import Optional

SAMPLE_INTERVAL_S = 5
WINDOW_S = 1800  # 30 minutes
_MAX_POINTS = WINDOW_S // SAMPLE_INTERVAL_S  # 360 points per aircraft

# Tuple field indices (avoid dataclass overhead for hot path)
_TS          = 0
_BEARING     = 1
_RANGE       = 2
_ALT         = 3
_LAT         = 4
_LON         = 5
_MIL         = 6
_MLAT        = 7
_INTERESTING = 8
_ACAS        = 9
_MLAT_SRC    = 10  # added in schema v2 — str | None


class TrackStore:
    def __init__(self) -> None:
        self._lock = threading.Lock()
        # icao → deque of track-point tuples
        self._tracks: dict[str, deque] = {}
        # icao → timestamp of last recorded point (rate limiter)
        self._last_ts: dict[str, float] = {}

    # ------------------------------------------------------------------
    # Write path
    # ------------------------------------------------------------------

    def record(
        self,
        icao: str,
        bearing_deg: float,
        range_nm: float,
        altitude_ft: Optional[int],
        lat: float,
        lon: float,
        military: bool,
        mlat: bool,
        interesting: bool,
        acas_ra_active: bool,
        mlat_source: Optional[str] = None,
        now: Optional[float] = None,
    ) -> None:
        """Append a track point if the per-aircraft rate limit allows it."""
        if now is None:
            now = time.time()
        with self._lock:
            if now - self._last_ts.get(icao, 0) < SAMPLE_INTERVAL_S:
                return
            self._last_ts[icao] = now
            if icao not in self._tracks:
                self._tracks[icao] = deque(maxlen=_MAX_POINTS)
            self._tracks[icao].append((
                now, bearing_deg, range_nm, altitude_ft, lat, lon,
                military, mlat, interesting, acas_ra_active, mlat_source,
            ))

    def expire(self, active_icaos: set[str]) -> None:
        """Remove tracks for aircraft no longer in the live set."""
        with self._lock:
            stale = [icao for icao in self._tracks if icao not in active_icaos]
            for icao in stale:
                del self._tracks[icao]
                self._last_ts.pop(icao, None)

    # ------------------------------------------------------------------
    # Read path
    # ------------------------------------------------------------------

    def get_tracks(self, icaos: Optional[set[str]] = None) -> dict:
        """
        Return serialisable track data as:
          { icao: [ {ts, bearing_deg, range_nm, altitude_ft, lat, lon,
                     military, mlat, interesting, acas_ra_active}, ... ] }

        If *icaos* is provided only those aircraft are returned.
        Points older than WINDOW_S are filtered out.
        """
        cutoff = time.time() - WINDOW_S
        with self._lock:
            keys = icaos if icaos is not None else set(self._tracks.keys())
            result: dict[str, list] = {}
            for icao in keys:
                track = self._tracks.get(icao)
                if not track:
                    continue
                points = [
                    {
                        "ts":            p[_TS],
                        "bearing_deg":   p[_BEARING],
                        "range_nm":      p[_RANGE],
                        "altitude_ft":   p[_ALT],
                        "lat":           p[_LAT],
                        "lon":           p[_LON],
                        "military":      p[_MIL],
                        "mlat":          p[_MLAT],
                        "interesting":   p[_INTERESTING],
                        "acas_ra_active": p[_ACAS],
                        "mlat_source":   p[_MLAT_SRC] if len(p) > _MLAT_SRC else None,
                    }
                    for p in track
                    if p[_TS] >= cutoff
                ]
                if points:
                    result[icao] = points
        return result
