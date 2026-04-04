"""
DF11 interrogator identifier (IID) API.

GET /api/interrogators?window_s=600
  Returns active IID codes and their message counts over the requested window.
  Only DF11 replies are counted (the IID field is the lower 7 bits of the CRC
  syndrome for DF11, indicating which SSR interrogator triggered the reply).
"""

from fastapi import APIRouter, Query

router = APIRouter(prefix="/api/interrogators")


@router.get("")
async def get_interrogators(window_s: float = Query(600, ge=60, le=3600)) -> dict:
    """Return DF11 IID counts for the last window_s seconds.

    Returns:
        window_s: the requested window
        total: total DF11 messages in the window
        codes: list of {iid, count, last_seen, latest_icao} sorted by count descending
    """
    activity = router._state.get_iid_activity(window_s)
    total = sum(entry["count"] for entry in activity.values())
    codes = sorted(
        [{
            "iid": iid,
            "count": entry["count"],
            "last_seen": round(entry["last_seen"], 3),
            "latest_icao": entry.get("latest_icao", ""),
        } for iid, entry in activity.items()],
        key=lambda x: (x["count"], x["last_seen"]),
        reverse=True,
    )
    return {"window_s": window_s, "total": total, "codes": codes}


@router.get("/timeline")
async def get_timeline(window_s: float = Query(10, ge=2, le=60)) -> dict:
    """Return per-IID DF11 message timing for the last window_s seconds.

    Used to render timing-lane visualisations showing SSR interrogator rotation
    periods.  Returns:
        now_us:  current Beast-relative time estimate
        window_s: the requested window
        lanes:   list of {iid, arrivals_us: [int]} sorted by IID ascending
    """
    now_us, timeline = router._state.get_iid_timeline(window_s)
    lanes = sorted(
        [{"iid": iid, "arrivals_us": entry["arrivals_us"], "latest_icao": entry.get("latest_icao", "")}
         for iid, entry in timeline.items()],
        key=lambda x: x["iid"],
    )
    return {"now_us": now_us, "window_s": window_s, "lanes": lanes}
