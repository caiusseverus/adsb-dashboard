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
