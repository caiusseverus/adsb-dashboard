from __future__ import annotations

from typing import TYPE_CHECKING

from .angular import _circular_delta_deg

if TYPE_CHECKING:
    from .sync_models import AlignedBurstSyncObs


_MOTION_RATE_MIN_DT_S = 1.0
_MOTION_RATE_MAX_DT_S = 60.0
_MOTION_RATE_MAX_POS_AGE_S = 8.0
_MOTION_RATE_MAX_ABS_DEG_S = 20.0


def _estimate_aircraft_bearing_rate(
    *,
    icao: str,
    bearing_deg: float,
    burst_centroid_us: float,
    pos_age_s: float,
    history: list["AlignedBurstSyncObs"],
) -> dict:
    """Estimate aircraft angular motion relative to the radar."""
    if not icao:
        return {
            "bearing_rate_deg_s": None,
            "motion_comp_block_reason": "missing_icao",
        }
    if pos_age_s is None or pos_age_s > _MOTION_RATE_MAX_POS_AGE_S:
        return {
            "bearing_rate_deg_s": None,
            "motion_comp_block_reason": "stale_position",
        }

    previous = None
    for candidate in reversed(history):
        if getattr(candidate, "icao", None) != icao:
            continue
        previous = candidate
        break

    if previous is None:
        return {
            "bearing_rate_deg_s": None,
            "motion_comp_block_reason": "insufficient_history",
        }
    if getattr(previous, "pos_age_s", None) is None or previous.pos_age_s > _MOTION_RATE_MAX_POS_AGE_S:
        return {
            "bearing_rate_deg_s": None,
            "motion_comp_block_reason": "previous_position_stale",
        }

    dt_s = (burst_centroid_us - previous.burst_centroid_us) / 1_000_000.0
    if dt_s < _MOTION_RATE_MIN_DT_S:
        return {
            "bearing_rate_deg_s": None,
            "motion_comp_block_reason": "time_delta_too_small",
        }
    if dt_s > _MOTION_RATE_MAX_DT_S:
        return {
            "bearing_rate_deg_s": None,
            "motion_comp_block_reason": "time_delta_too_large",
        }

    delta_deg = _circular_delta_deg(bearing_deg, previous.bearing_deg)
    if delta_deg is None:
        return {
            "bearing_rate_deg_s": None,
            "motion_comp_block_reason": "bearing_delta_unavailable",
        }
    bearing_rate_deg_s = delta_deg / dt_s
    if abs(bearing_rate_deg_s) > _MOTION_RATE_MAX_ABS_DEG_S:
        return {
            "bearing_rate_deg_s": bearing_rate_deg_s,
            "motion_comp_block_reason": "bearing_rate_absurd",
        }

    return {
        "bearing_rate_deg_s": bearing_rate_deg_s,
        "motion_comp_block_reason": None,
    }
