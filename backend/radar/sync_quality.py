from __future__ import annotations

from typing import Any

from .sync_models import IcaoSyncQuality


def _sync_quality_from_model(model: Any) -> float:
    """Derive a 0-1 sync quality score from the rotation model status."""
    if model is None or getattr(model, "period_s", None) is None:
        return 0.0
    status = getattr(model, "status", "UNKNOWN")
    if status == "SINGLE_RADAR":
        return 1.0
    if status == "LIKELY_SINGLE":
        return 0.8
    if status == "CHECK_MULTI":
        return 0.5
    if status == "MULTI_RADAR":
        return 0.3
    return 0.0


def _icao_quality_memory_score(entry: IcaoSyncQuality | None) -> float:
    """Return a 0-1 quality multiplier derived from residual spread memory."""
    if entry is None:
        return 1.0
    return max(0.0, min(1.0, 1.0 - entry.residual_mad_deg / 25.0))


def _icao_quality_reject_reason(entry: IcaoSyncQuality | None) -> str | None:
    """Return the hard-reject reason for poor ICAO quality memory, if any."""
    if entry is None or entry.n_recent < 6:
        return None
    if entry.residual_mad_deg > 25.0 or abs(entry.residual_median_deg) > 45.0:
        return "poor_icao_quality"
    return None


def _icao_quality_anchor_warning(entry: IcaoSyncQuality | None) -> str | None:
    """Return the anchor-selection warning for degraded ICAO quality memory, if any."""
    if entry is None or entry.n_recent < 6:
        return None
    if entry.residual_mad_deg > 25.0 or abs(entry.residual_median_deg) > 60.0:
        return "poor_icao_quality_memory"
    return None


def _update_icao_sync_quality_memory(
    quality_dict: dict[str, IcaoSyncQuality],
    scored: list[dict],
) -> None:
    """Update per-ICAO residual quality memory from scored sync observations."""
    quality_alpha = 0.1
    for entry in scored:
        residual_c = entry["residual"]
        weight = entry["weight"]
        status = entry["status"]
        icao = entry["icao"]
        ts_obs = entry["obs_ts"]
        if weight <= 0 or status == "rejected":
            continue
        q_entry = quality_dict.get(icao)
        if q_entry is None:
            quality_dict[icao] = IcaoSyncQuality(
                residual_median_deg=residual_c,
                residual_mad_deg=abs(residual_c),
                n_recent=1,
                last_ts=ts_obs,
            )
            continue
        q_entry.residual_median_deg = (
            (1.0 - quality_alpha) * q_entry.residual_median_deg
            + quality_alpha * residual_c
        )
        dev = abs(residual_c - q_entry.residual_median_deg)
        q_entry.residual_mad_deg = (
            (1.0 - quality_alpha) * q_entry.residual_mad_deg
            + quality_alpha * dev
        )
        q_entry.n_recent += 1
        q_entry.last_ts = ts_obs
