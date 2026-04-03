"""
Real-time message timing API.

GET /api/timing/events?since_ts=<float>
  Returns all decoded DF messages with ts > since_ts (up to 5s worth).
  Designed for polling at 200 ms intervals; each response is compact.

Response:
  {
    "now":    <float>,   # current server time
    "events": [[ts, df], ...]  # sorted ascending
  }
"""

import time

from fastapi import APIRouter, Query

router = APIRouter(prefix="/api/timing")

_MAX_EVENTS = 5000   # hard cap per response (~2.5s at 2000 msg/s)


@router.get("/events")
async def timing_events(
    since_ts: float = Query(0.0, description="Return events with ts > since_ts"),
) -> dict:
    events_raw = router._state.get_timing_events(since_ts)
    # Cap to avoid accidentally huge responses on first poll
    if len(events_raw) > _MAX_EVENTS:
        events_raw = events_raw[-_MAX_EVENTS:]
    return {
        "now": time.time(),
        "events": [[round(ts, 4), df] for ts, df in events_raw],
    }
