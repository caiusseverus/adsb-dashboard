from __future__ import annotations

from dataclasses import dataclass, field


@dataclass
class IcaoSyncQuality:
    """Per-IID per-ICAO residual quality memory for fit downweighting."""
    residual_median_deg: float = 0.0
    residual_mad_deg: float = 5.0
    n_recent: int = 0
    last_ts: float = 0.0


@dataclass
class LiveSyncState:
    """Per-IID live synchronisation state for Stage 3 bearing computation."""

    iid: int
    period_s: float
    phase_epoch_us: float
    phase_offset_deg: float
    sync_quality: float
    sync_jitter_deg: float
    last_sync_update_ts: float
    source: str
    usable: bool
    residual_ema_deg: float = 5.0
    n_sync_frames: int = 0
    n_rejected_frames: int = 0
    last_residual_deg: float = 0.0
    holdover: bool = False
    n_burst_obs_inliers: int = 0
    n_burst_obs_rejected: int = 0
    contributing_icao_count: int = 0
    period_base_s: float = 0.0
    residual_slope_deg_per_s: float = 0.0
    period_correction_ppm: float = 0.0
    period_refine_enabled: bool = False
    period_update_term: float = 0.0
    period_update_direction: str = "none"
    period_update_applied: float = 0.0
    period_update_gain: float = 0.0
    period_refine_block_reason: str | None = None
    period_update_proposed_s: float = 0.0
    period_update_proposed_us: float = 0.0
    period_update_applied_s: float = 0.0
    period_update_applied_us: float = 0.0
    period_update_ppm_unclamped: float = 0.0
    period_update_ppm_applied: float = 0.0
    period_update_block_reason: str | None = None
    period_update_clamp_reason: str | None = None
    period_update_allowed: bool = False
    period_update_fit_support: int = 0
    period_update_fit_span_s: float = 0.0
    period_correction_status: str = "unknown"
    period_update_safety_ppm_per_update: float = 0.0
    period_update_safety_ppm_from_base: float = 0.0
    period_update_ppm_from_base: float = 0.0
    period_update_ppm_step: float = 0.0
    period_update_ppm_limit_from_base: float = 0.0
    period_update_ppm_limit_step: float = 0.0
    phase_status: str | None = None
    phase_status_reason: str | None = None
    fit_time_basis: str = "effective_beast_time_s"
    fit_residual_basis: str = "observed_minus_authoritative_prediction_deg"
    fit_total_observations: int = 0
    fit_eligible_observations: int = 0
    fit_rejected_observations: int = 0
    fit_reject_reasons: dict[str, int] = field(default_factory=dict)
    fit_contributing_icao_count: int = 0
    fit_span_s: float = 0.0
    prop_delay_enabled: bool = False
    motion_comp_phase_enabled: bool = False
    motion_comp_fit_enabled: bool = False
    motion_comp_applied_count: int = 0
    motion_comp_blocked_count: int = 0
    motion_comp_mean_dt_us: float = 0.0
    motion_comp_mean_residual_improvement_deg: float = 0.0
    motion_comp_high_rate_mean_residual_improvement_deg: float | None = None
    phase_anchor_icao: str | None = None
    phase_anchor_score: float = 0.0
    phase_anchor_obs_count: int = 0
    phase_anchor_offset_raw_deg: float | None = None
    phase_anchor_offset_smoothed_deg: float | None = None
    phase_anchor_spread_deg: float | None = None
    phase_anchor_status: str = "unavailable"
    phase_anchor_since_ts: float | None = None
    phase_anchor_replacement_reason: str | None = None
    phase_anchor_candidate_count: int = 0
    phase_anchor_no_candidate_reason: str | None = None
    phase_validation_contributors: int = 0
    phase_validation_reject_count: int = 0
    phase_validation_median_error_deg: float | None = None
    phase_validation_status: str = "unavailable"
    phase_anchor_candidates: list[dict] = field(default_factory=list)
    period_authoritative_source: str = "refined"
    dominant_period_s: float | None = None
    dominant_prior_period_s: float | None = None
    trusted_refined_period_s: float | None = None
    bootstrap_period_s: float | None = None
    active_family_prior_s: float | None = None
    active_family_prior_source: str | None = None
    dominant_prior_active: bool = False
    dominant_period_delta_s: float | None = None
    dominant_period_delta_ppm: float | None = None
    compact_period_s: float | None = None
    compact_period_delta_to_dominant_s: float | None = None
    compact_period_delta_to_dominant_ppm: float | None = None
    compact_sync_unreliable: bool = False


@dataclass
class AlignedBurstSyncObs:
    """One burst-centre bearing observation for multi-aircraft sync maintenance."""

    burst_centroid_us: float
    icao: str
    bearing_deg: float
    n_replies: int
    signal_dbfs: float | None
    pos_age_s: float
    range_nm: float
    ts: float
    sync_update_eligible: bool = True
    raw_arrival_us: float = 0.0
    prop_delay_aircraft_to_receiver_us: float = 0.0
    prop_delay_radar_to_aircraft_us: float | None = None
    effective_arrival_us: float = 0.0
    bearing_rate_deg_s: float | None = None
    motion_comp_dt_us: float | None = None
    motion_corrected_beast_us: float | None = None
    motion_comp_applied: bool = False
    motion_comp_block_reason: str | None = None
    burst_center_simple_us: float | None = None
    burst_center_weighted_us: float | None = None
    burst_center_delta_us: float | None = None
    burst_center_method: str = "centroid"
    burst_ts_first_reply_beast_us: float | None = None
    burst_ts_strongest_reply_beast_us: float | None = None
    burst_ts_simple_centroid_beast_us: float | None = None
    burst_ts_weighted_centroid_beast_us: float | None = None
    burst_ts_mid_strong_window_beast_us: float | None = None
    burst_ts_last_reply_beast_us: float | None = None
    burst_span_us: float | None = None
    peak_amplitude: float | None = None
    position_interpolated: bool = False
    position_extrapolated: bool = False
    position_source_age_s: float | None = None
    truth_position_ts_beast_us: float | None = None
