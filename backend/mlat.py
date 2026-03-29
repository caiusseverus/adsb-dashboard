"""
MLAT diagnostic API endpoints.

GET /api/mlat/fixes/{icao}     — per-source raw fix positions for one aircraft
GET /api/mlat/residuals        — all aircraft with positions coloured by cross-source residual
"""

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
    return _state.get_mlat_fixes_all()


@router.get("/api/mlat/fixes/{icao}")
def get_mlat_fixes(icao: str) -> dict:
    """
    Return the rolling per-source fix buffer for a single aircraft.
    Used by the Map page spaghetti-track overlay.
    Returns: { source: [[lat, lon], ...], ... }
    """
    if _state is None:
        raise HTTPException(503, "State not available")
    result = _state.get_mlat_fixes_for(icao.upper())
    if result is None:
        return {"_debug": "aircraft_not_in_live_state"}
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
    return _state.get_mlat_residuals()
