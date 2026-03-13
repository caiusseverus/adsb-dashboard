"""FastAPI router exposing the in-memory track store."""

from fastapi import APIRouter
from track_store import TRACK_SCHEMA_VERSION

router = APIRouter()

# Injected by main.py after instantiation
_track_store = None


@router.get("/api/tracks")
def get_tracks() -> dict:
    """
    Return rolling 30-minute track history for all currently-tracked aircraft.
    Polled by the SkyView and 3-D view components every 5 seconds.
    """
    if _track_store is None:
        return {"schema_version": TRACK_SCHEMA_VERSION, "tracks": {}}
    return {
        "schema_version": TRACK_SCHEMA_VERSION,
        "tracks": _track_store.get_tracks(),
    }
