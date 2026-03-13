"""
MLAT diagnostic API endpoints.

GET /api/mlat/fixes/{icao}     — per-source raw fix positions for one aircraft
GET /api/mlat/residuals        — all aircraft with positions coloured by cross-source residual
"""

import statistics
from fastapi import APIRouter, HTTPException

router = APIRouter()

# Injected by main.py
_state = None


@router.get("/api/mlat/fixes")
def get_all_mlat_fixes() -> dict:
    """
    Return per-source fix buffers for all MLAT aircraft with position data.
    Polled by the map MLAT-sources overlay.
    Returns: { icao: { source: [[lat, lon], ...], ... }, ... }
    """
    if _state is None:
        return {}

    out = {}
    with _state._lock:
        for icao, ac in _state._aircraft.items():
            if not ac.mlat or not ac.mlat_fixes:
                continue
            srcs = {
                src: [[round(f.lat, 6), round(f.lon, 6)] for f in buf]
                for src, buf in ac.mlat_fixes.items()
                if buf
            }
            if srcs:
                out[icao] = srcs
    return out


@router.get("/api/mlat/fixes/{icao}")
def get_mlat_fixes(icao: str) -> dict:
    """
    Return the rolling per-source fix buffer for a single aircraft.
    Used by the Map page spaghetti-track overlay.
    Returns: { source: [[lat, lon], ...], ... }
    """
    if _state is None:
        raise HTTPException(503, "State not available")

    icao = icao.lower()
    with _state._lock:
        ac = _state._aircraft.get(icao)
        if ac is None:
            return {"_debug": "aircraft_not_in_live_state"}

        result = {}
        for src, buf in ac.mlat_fixes.items():
            result[src] = [[round(f.lat, 6), round(f.lon, 6)] for f in buf]

    return result


@router.get("/api/mlat/residuals")
def get_mlat_residuals() -> list:
    """
    Return one entry per MLAT-tracked aircraft that has a known position and
    cross-source residuals, for the geographic residual overlay on the Map page.
    Returns: [ { icao, lat, lon, sources, avg_residual_nm }, ... ]
    """
    if _state is None:
        return []

    out = []
    with _state._lock:
        for icao, ac in _state._aircraft.items():
            if not ac.mlat or ac.lat is None or ac.lon is None:
                continue
            if not ac.mlat_residuals:
                continue

            # Median-of-medians across all source residual streams
            all_vals = []
            for buf in ac.mlat_residuals.values():
                if len(buf) >= 3:
                    all_vals.append(statistics.median(buf))
            if not all_vals:
                continue

            out.append({
                "icao":             icao,
                "lat":              round(ac.lat, 5),
                "lon":              round(ac.lon, 5),
                "sources":          list(ac.mlat_residuals.keys()),
                "avg_residual_nm":  round(sum(all_vals) / len(all_vals), 3),
            })

    return out
