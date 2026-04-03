"""
DF11 interrogator identifier (IID) API.

GET /api/interrogators?window_s=600
  Returns active IID codes and their message counts over the requested window.
  Only DF11 replies are counted (the IID field is the lower 7 bits of the CRC
  syndrome for DF11, indicating which SSR interrogator triggered the reply).
"""

import time

from fastapi import APIRouter, Query

router = APIRouter(prefix="/api/interrogators")


@router.get("")
async def get_interrogators(window_s: float = Query(600, ge=60, le=3600)) -> dict:
    """Return DF11 IID counts for the last window_s seconds.

    Returns:
        window_s: the requested window
        total: total DF11 messages in the window
        codes: list of {iid, count} sorted by count descending
    """
    counts = router._state.get_iid_counts(window_s)
    total = sum(counts.values())
    codes = sorted(
        [{"iid": iid, "count": cnt} for iid, cnt in counts.items()],
        key=lambda x: x["count"],
        reverse=True,
    )
    return {"window_s": window_s, "total": total, "codes": codes}


@router.get("/timeline")
async def get_timeline(window_s: float = Query(10, ge=2, le=60)) -> dict:
    """Return per-IID DF11 message timestamps for the last window_s seconds.

    Used to render timing-lane visualisations showing SSR interrogator rotation
    periods.  Returns:
        now:     current server time (Unix seconds, float)
        window_s: the requested window
        lanes:   list of {iid, timestamps: [float]} sorted by count descending
    """
    now = time.time()
    timeline = router._state.get_iid_timeline(window_s)
    lanes = sorted(
        [{"iid": iid, "timestamps": entry["timestamps"], "latest_icao": entry.get("latest_icao", "")}
         for iid, entry in timeline.items()],
        key=lambda x: len(x["timestamps"]),
        reverse=True,
    )
    return {"now": now, "window_s": window_s, "lanes": lanes}
