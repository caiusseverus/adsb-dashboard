from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from .sync_models import LiveSyncState


@dataclass
class SyncPrediction:
    """Authoritative live sync prediction for one timestamp.

    Sign convention: residuals are always observed_bearing - predicted_bearing.
    Positive residual slope over effective Beast time means the model is rotating
    too slowly, so period refinement must decrease period_s.
    """

    raw_arrival_us: float
    effective_arrival_us: float
    prop_corrected_beast_us: float
    motion_corrected_beast_us: float
    propagation_correction_us: float
    motion_comp_dt_us: float | None
    bearing_rate_deg_s: float | None
    motion_comp_enabled: bool
    motion_comp_applied: bool
    motion_comp_block_reason: str | None
    phase_in_rot_deg: float
    predicted_bearing_raw_deg: float
    predicted_bearing_deg: float
    predictor_version: str = "authoritative_sync_v3_motion"
    phase_status: str | None = None
    phase_basis: str = "sweep_epoch_only"


def predict_sync_observation(
    sync: "LiveSyncState",
    arrival_us: float,
    *,
    range_nm: float | None = None,
    apply_propagation: bool | None = None,
    apply_motion: bool | None = None,
    bearing_rate_deg_s: float | None = None,
    motion_comp_dt_us: float | None = None,
    motion_comp_block_reason: str | None = None,
) -> SyncPrediction:
    """Predict bearing from arrival time using the live sync model.

    Update order:
      1. compute propagation-corrected effective time,
      2. subtract aircraft-motion timing shift when enabled and valid,
      3. compute phase with the current period.
    """
    period_us = sync.period_s * 1e6
    phase_basis = _derive_prediction_phase_basis(sync)
    if period_us <= 0:
        predicted = sync.phase_offset_deg % 360.0
        return SyncPrediction(
            raw_arrival_us=arrival_us,
            effective_arrival_us=arrival_us,
            prop_corrected_beast_us=arrival_us,
            motion_corrected_beast_us=arrival_us,
            propagation_correction_us=0.0,
            motion_comp_dt_us=None,
            bearing_rate_deg_s=bearing_rate_deg_s,
            motion_comp_enabled=False,
            motion_comp_applied=False,
            motion_comp_block_reason="invalid_period",
            phase_in_rot_deg=0.0,
            predicted_bearing_raw_deg=predicted,
            predicted_bearing_deg=predicted,
            phase_basis=phase_basis,
        )
    effective_us = arrival_us
    prop_delay_us = 0.0
    prop_enabled = sync.prop_delay_enabled if apply_propagation is None else bool(apply_propagation)
    if prop_enabled:
        prop_delay_us = _compute_propagation_delay_us(range_nm)
        effective_us = arrival_us - prop_delay_us
    prop_corrected_us = effective_us

    motion_enabled = sync.motion_comp_phase_enabled if apply_motion is None else bool(apply_motion)
    motion_dt = motion_comp_dt_us
    if motion_dt is None:
        motion_dt = _compute_motion_comp_dt_us(sync.period_s, bearing_rate_deg_s)
    motion_applied = False
    motion_block = motion_comp_block_reason
    if not motion_enabled:
        motion_block = "disabled"
    elif motion_block is not None:
        motion_applied = False
    elif motion_dt is None:
        motion_block = motion_block or "bearing_rate_unavailable"
    else:
        effective_us = prop_corrected_us - motion_dt
        motion_applied = True
        motion_block = None

    phase_in_rot = ((effective_us - sync.phase_epoch_us) / period_us * 360.0) % 360.0
    predicted_raw = (phase_in_rot + sync.phase_offset_deg) % 360.0
    return SyncPrediction(
        raw_arrival_us=arrival_us,
        effective_arrival_us=effective_us,
        prop_corrected_beast_us=prop_corrected_us,
        motion_corrected_beast_us=effective_us,
        propagation_correction_us=prop_delay_us,
        motion_comp_dt_us=motion_dt,
        bearing_rate_deg_s=bearing_rate_deg_s,
        motion_comp_enabled=motion_enabled,
        motion_comp_applied=motion_applied,
        motion_comp_block_reason=motion_block,
        phase_in_rot_deg=phase_in_rot,
        predicted_bearing_raw_deg=predicted_raw,
        predicted_bearing_deg=predicted_raw,
        phase_status=getattr(sync, "phase_status", "untrusted"),
        phase_basis=phase_basis,
    )


def _predict_bearing_from_sync(
    sync: "LiveSyncState",
    arrival_us: float,
    *,
    range_nm: float | None = None,
) -> tuple[float, float]:
    """Compatibility wrapper around the authoritative predictor."""
    prediction = predict_sync_observation(sync, arrival_us, range_nm=range_nm)
    return prediction.predicted_bearing_deg, prediction.phase_in_rot_deg


def _derive_prediction_phase_basis(sync: "LiveSyncState") -> str:
    """Derive the phase basis for a prediction from the live sync state.

    Mirrors the logic in _live_sync_state_to_dict() so consumers can
    distinguish sweep_epoch_only, anchor_relative, and geographic predictions.
    """
    typed_basis = getattr(sync, "phase_basis", None)
    if typed_basis in {"sweep_epoch_only", "anchor_relative", "geographic"}:
        return typed_basis
    phase_anchor_icao = getattr(sync, "phase_anchor_icao", None)
    phase_anchor_status = str(getattr(sync, "phase_anchor_status", "") or "")
    if phase_anchor_icao and phase_anchor_status in {"selected", "anchor_only"}:
        return "anchor_relative"
    return "sweep_epoch_only"


_US_PER_NM_LIGHT = 1852.0 / 299792458.0 * 1e6


def _compute_propagation_delay_us(range_nm: float | None) -> float:
    """One-way aircraft→receiver propagation delay for the given range.

    Negative or missing ranges produce 0 so the observation passes through
    unchanged.
    """
    if range_nm is None or range_nm <= 0:
        return 0.0
    return float(range_nm) * _US_PER_NM_LIGHT


def _compute_motion_comp_dt_us(period_s: float, bearing_rate_deg_s: float | None) -> float | None:
    """First-order beam-crossing timing shift from aircraft bearing rate.

    If aircraft bearing changes by Δθ during one sweep, the apparent crossing
    time shifts by approximately (Δθ / 360) * T. With a bearing rate θdot this
    is (θdot / 360) * T². The returned value is subtracted from the
    propagation-corrected Beast timestamp to put the observation on the raw
    sweep-period basis used by phase/period fitting.
    """
    if bearing_rate_deg_s is None or period_s <= 0:
        return None
    return (bearing_rate_deg_s / 360.0) * (period_s ** 2) * 1_000_000.0
