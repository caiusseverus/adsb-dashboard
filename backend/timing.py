"""
Real-time message timing API.

GET /api/timing/events?since_seq=<int>
  Returns all decoded DF messages with seq > since_seq (up to 5s worth).
  Designed for polling at 200 ms intervals; each response is compact.

Response:
  {
    "now_us": <int>,   # current Beast-relative time estimate
    "events": [[seq, arrival_us, df, msg_len, signal_dbfs, source_class, icao, bearing_deg, range_nm, iid], ...]  # sorted ascending
  }
"""

from fastapi import APIRouter, Query

router = APIRouter(prefix="/api/timing")

_MAX_EVENTS = 5000   # hard cap per response (~2.5s at 2000 msg/s)


@router.get("/events")
async def timing_events(
    since_seq: int = Query(0, ge=0, description="Return events with seq > since_seq"),
    iid: int | None = Query(None, description="Optional IID filter"),
    df11_only: bool = Query(False, description="Return only DF11 events"),
) -> dict:
    iid_filter = iid if isinstance(iid, int) else None
    df11_filter = df11_only if isinstance(df11_only, bool) else False
    events_raw = router._state.get_timing_events(since_seq)
    # Cap to avoid accidentally huge responses on first poll
    if len(events_raw) > _MAX_EVENTS:
        events_raw = events_raw[-_MAX_EVENTS:]
    if df11_filter or iid_filter is not None:
        events_raw = [
            event for event in events_raw
            if (not df11_filter or event[2] == 11) and (iid_filter is None or event[9] == iid_filter)
        ]
    return {
        "now_us": router._state.get_timing_now_us(),
        "events": [
            [seq, arrival_us, df, msg_len, signal_dbfs, source_class, icao, bearing_deg, range_nm, iid]
            for seq, arrival_us, df, msg_len, signal_dbfs, source_class, icao, bearing_deg, range_nm, iid in events_raw
        ],
    }
