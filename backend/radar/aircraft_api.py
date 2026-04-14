"""
Stage 3 API: aircraft localisation endpoints.

Router: /api/radar/aircraft

All mutating operations (calibration/run, tracks/run, tracks/reset) are
triggered synchronously in the request; background loops provide the
continuous updates.
"""

from __future__ import annotations

import asyncio
import logging
import time
from typing import TYPE_CHECKING

from fastapi import APIRouter, HTTPException
from pydantic import BaseModel

import config

if TYPE_CHECKING:
    from .aircraft_localiser import AircraftLocaliser

log = logging.getLogger(__name__)

router = APIRouter(prefix="/api/radar/aircraft", tags=["stage3"])

# Injected by main.py after router creation
_localiser: "AircraftLocaliser | None" = None
# Reference to the message queue for backlog check (injected by main.py)
_msg_queue = None


# ---------------------------------------------------------------------------
# Status / health
# ---------------------------------------------------------------------------

@router.get("/status")
async def get_status():
    """Global Stage 3 health and counters."""
    enabled = config.STAGE3_ENABLED
    if not enabled or _localiser is None:
        return {
            "enabled": False,
            "n_calibrated_radars": 0,
            "n_active_tracks": 0,
            "last_calibration_ts": None,
            "last_localisation_ts": None,
            "queue_backlog": False,
        }

    status = _localiser.get_status()
    backlog = _msg_queue.qsize() >= 100 if _msg_queue is not None else False
    return {
        "enabled": True,
        "n_calibrated_radars": status["n_calibrated_radars"],
        "n_active_tracks": status["n_active_tracks"],
        "last_calibration_ts": status["last_calibration_ts"],
        "last_localisation_ts": status["last_localisation_ts"],
        "queue_backlog": backlog,
        "n_calibration_cycles": status["n_calibration_cycles"],
        "n_localisation_cycles": status["n_localisation_cycles"],
        "backlog_skips": status["backlog_skips"],
    }


# ---------------------------------------------------------------------------
# Radar readiness
# ---------------------------------------------------------------------------

@router.get("/radars")
async def get_radars():
    """Per-radar Stage 3 readiness and calibration state."""
    if not config.STAGE3_ENABLED or _localiser is None:
        return {"enabled": False, "radars": []}

    radars = await asyncio.to_thread(_localiser.get_radar_readiness)
    return {"enabled": True, "radars": radars}


# ---------------------------------------------------------------------------
# Tracks
# ---------------------------------------------------------------------------

@router.get("/tracks")
async def get_tracks():
    """All current Stage 3 runtime tracks."""
    if not config.STAGE3_ENABLED or _localiser is None:
        return {"enabled": False, "tracks": []}

    tracks = _localiser.get_tracks()
    return {"enabled": True, "tracks": tracks}


@router.get("/tracks/{track_id}")
async def get_track(track_id: str):
    """Full state for one Stage 3 track."""
    if not config.STAGE3_ENABLED or _localiser is None:
        raise HTTPException(status_code=503, detail="Stage 3 not enabled")

    track = _localiser.get_track(track_id)
    if track is None:
        raise HTTPException(status_code=404, detail=f"Track {track_id!r} not found")
    return track


# ---------------------------------------------------------------------------
# Evidence layers
# ---------------------------------------------------------------------------

class EvidenceRequest(BaseModel):
    iid_filter: list[int] | None = None


@router.get("/tracks/{track_id}/evidence")
async def get_track_evidence(track_id: str, iids: str | None = None):
    """Evidence layers for one aircraft: radar origins, bearing rays, seeds, fix, CEP."""
    if not config.STAGE3_ENABLED or _localiser is None:
        raise HTTPException(status_code=503, detail="Stage 3 not enabled")

    iid_subset: set[int] | None = None
    if iids:
        try:
            iid_subset = {int(i) for i in iids.split(",") if i.strip()}
        except ValueError:
            raise HTTPException(status_code=400, detail="iids must be comma-separated integers")

    evidence = await asyncio.to_thread(_localiser.build_evidence, track_id, iid_subset)
    return evidence


# ---------------------------------------------------------------------------
# Manual triggers
# ---------------------------------------------------------------------------

@router.post("/calibration/run")
async def run_calibration():
    """Manually trigger one bearing calibration cycle."""
    if not config.STAGE3_ENABLED or _localiser is None:
        raise HTTPException(status_code=503, detail="Stage 3 not enabled")

    t0 = time.time()
    updated = await asyncio.to_thread(_localiser.run_calibration_cycle)

    # Persist to DB inline
    if updated:
        from db import stats_db
        await asyncio.to_thread(stats_db.upsert_radar_bearing_calibrations, updated)

    return {
        "ok": True,
        "updated": len(updated),
        "elapsed_s": round(time.time() - t0, 3),
        "calibrations": [
            {
                "iid": c.iid,
                "bearing_offset_deg": round(c.bearing_offset_deg, 3),
                "bearing_sigma_deg": round(c.bearing_sigma_deg, 3),
                "n_samples": c.n_samples,
                "quality": c.quality,
            }
            for c in updated
        ],
    }


@router.post("/tracks/run")
async def run_localisation(icaos: str | None = None):
    """Manually trigger one localisation cycle for specific ICAOs (comma-separated) or all active."""
    if not config.STAGE3_ENABLED or _localiser is None:
        raise HTTPException(status_code=503, detail="Stage 3 not enabled")

    from aircraft_state import AircraftState

    # Resolve target ICAOs
    if icaos:
        target_list = [i.strip().upper() for i in icaos.split(",") if i.strip()]
    else:
        # Use currently active aircraft from radar frames
        target_list = _get_active_icaos()

    t0 = time.time()
    fixes = await asyncio.to_thread(
        _localiser.run_localisation_cycle,
        target_list,
        config._AIRCRAFT_LOC_MAX_TARGETS if hasattr(config, "_AIRCRAFT_LOC_MAX_TARGETS") else 20,
    )

    return {
        "ok": True,
        "n_targets": len(target_list),
        "n_fixes": len(fixes),
        "elapsed_s": round(time.time() - t0, 3),
        "fixes": [
            {
                "track_id": f.track_id,
                "lat": f.lat,
                "lon": f.lon,
                "cep_m": f.cep_m,
                "geometry_score": round(f.geometry_score, 3),
                "n_radars": f.n_radars,
                "solver_status": f.solver_status,
            }
            for f in fixes
        ],
    }


@router.post("/tracks/reset")
async def reset_tracks():
    """Clear all runtime Stage 3 tracks (does not delete calibration rows)."""
    if not config.STAGE3_ENABLED or _localiser is None:
        raise HTTPException(status_code=503, detail="Stage 3 not enabled")

    _localiser.reset_tracks()
    return {"ok": True, "message": "Runtime tracks cleared"}


@router.post("/calibration/reset")
async def reset_calibrations():
    """Clear runtime calibrations AND delete all DB rows."""
    if not config.STAGE3_ENABLED or _localiser is None:
        raise HTTPException(status_code=503, detail="Stage 3 not enabled")

    _localiser.reset_calibrations()
    from db import stats_db
    n = await asyncio.to_thread(stats_db.clear_radar_bearing_calibrations)
    return {"ok": True, "db_rows_deleted": n}


# ---------------------------------------------------------------------------
# Helper: active ICAOs from radar frames
# ---------------------------------------------------------------------------

def _get_active_icaos() -> list[str]:
    """Collect ICAOs recently observed in any radar sweep frame."""
    if _localiser is None:
        return []
    icaos: set[str] = set()
    for iid, _auth, _model in _localiser.get_eligible_iids():
        frames = _localiser._radar_state.get_sweep_frames(iid)
        for frame in frames[-5:]:   # last 5 frames per radar
            if frame.ref_icao:
                icaos.add(frame.ref_icao)
            for obs in frame.observations:
                if obs.icao:
                    icaos.add(obs.icao)
    return sorted(icaos)
