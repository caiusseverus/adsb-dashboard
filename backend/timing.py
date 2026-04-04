"""
Real-time message timing API.

GET /api/timing/events?since_seq=<int>
  Returns all decoded DF messages with seq > since_seq (up to 5s worth).
  Designed for polling at 200 ms intervals; each response is compact.

Response:
  {
    "now_us": <int>,   # current Beast-relative time estimate
    "events": [[seq, arrival_us, df, msg_len], ...]  # sorted ascending
  }
"""

from fastapi import APIRouter, Query

router = APIRouter(prefix="/api/timing")

_MAX_EVENTS = 5000   # hard cap per response (~2.5s at 2000 msg/s)


@router.get("/events")
async def timing_events(
    since_seq: int = Query(0, ge=0, description="Return events with seq > since_seq"),
) -> dict:
    events_raw = router._state.get_timing_events(since_seq)
    # Cap to avoid accidentally huge responses on first poll
    if len(events_raw) > _MAX_EVENTS:
        events_raw = events_raw[-_MAX_EVENTS:]
    return {
        "now_us": router._state.get_timing_now_us(),
        "events": [[seq, arrival_us, df, msg_len] for seq, arrival_us, df, msg_len in events_raw],
    }
