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
    """Python simple sync state for prediction and Stage 3 bearing derivation.

    period_s is the authoritative rotation period that all downstream code
    (frame-building, display helpers, prediction) should use.  When period
    refinement is accepted period_s holds the bounded corrected value; when
    refinement is blocked or unavailable period_s is set to period_base_s so
    callers never need to choose between the two.

    period_base_s is retained as the coarse DF/model period used as the
    refinement anchor and for diagnostic comparison only.
    """

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
    phase_status: str | None = None
    fit_total_observations: int = 0
    fit_eligible_observations: int = 0
    fit_span_s: float = 0.0
    prop_delay_enabled: bool = False
    motion_comp_phase_enabled: bool = False
    motion_comp_fit_enabled: bool = False
    phase_anchor_icao: str | None = None
    phase_anchor_score: float = 0.0
    phase_anchor_obs_count: int = 0
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
    phase_basis: str | None = None
    phase_anchor_age_s: float | None = None
    phase_is_absolute: bool = False
    phase_absolute_available: bool = False
    phase_trust_reason: str | None = None
    phase_offset_geographic_deg: float | None = None
    base_period_s: float | None = None
    period_delta_s: float | None = None
    effective_period_s: float | None = None
    period_authority: str | None = None
    period_refinement_status: str | None = None
    sync_authority: str | None = None
    phase_authority: str | None = None
    handoff_state: str | None = None
    handoff_reason: str | None = None
    last_handoff_transition_ts: float | None = None
    handoff_gate_failures: dict = field(default_factory=dict)
    fit_icao_count: int = 0
    fit_observations_per_icao_min: int = 0
    fit_observations_per_icao_median: float | None = None
    fit_observations_per_icao_max: int = 0
    fit_retention_window_s: float | None = None
    fit_global_cap_hit: bool = False
    fit_last_eviction_reason: str | None = None
    fit_inlier_ratio: float | None = None
    suspicious_icao_count: int = 0
    suspicious_icao_last_reason: str | None = None
    slope_ema_deg_per_s: float | None = None
    slope_std_deg_per_s: float | None = None
    proposed_delta_s: float | None = None
    applied_delta_s: float | None = None
    last_slew_limited: bool | None = None
    last_hard_bound: bool | None = None
    hard_bound_reason: str | None = None
    hard_bound_limit_s: float | None = None
    hard_bound_limit_ppm: float | None = None
    requested_delta_s: float | None = None
    requested_delta_ppm: float | None = None
    current_delta_s: float | None = None
    current_delta_ppm: float | None = None
    delta_to_base_s: float | None = None
    delta_to_base_ppm: float | None = None
    df_base_period_s: float | None = None
    period_disagreement_s: float | None = None
    period_disagreement_ppm: float | None = None
    compact_period_agrees_with_df: bool | None = None
    compact_period_diagnostic_reason: str | None = None
    compact_period_disagreement_s: float | None = None
    compact_period_disagreement_ppm: float | None = None
    fit_epoch_id: int = 0
    fit_epoch_started_ts: float | None = None
    fit_epoch_reset_reason: str | None = None
    fit_epoch_observation_count: int = 0
    fit_epoch_span_s: float | None = None
    fit_dropped_on_epoch_reset: int = 0
    fit_segment_count: int = 0
    slope_sign_convention: str | None = None
    holdover_reason: str | None = None
    holdover_quality_gate_failed: int = 0
    holdover_missing_df_base_period: int = 0
    holdover_hard_residual_reject: int = 0
    holdover_no_reference: int = 0
    holdover_stale_reference_position: int = 0
    holdover_period_disagreement: int = 0
    holdover_insufficient_aircraft: int = 0
    holdover_no_dominant_family: int = 0
    holdover_sync_state_missing: int = 0
    update_epoch_attempts: int = 0
    update_epoch_accepts: int = 0
    update_epoch_rejects: int = 0
    last_update_epoch_reject_reason: str | None = None
    last_update_epoch_n_aircraft: int | None = None
    last_update_epoch_ref_pos_age_s: float | None = None
    last_update_epoch_ref_icao: str | None = None
    update_epoch_reject_quality_gate: int = 0
    update_epoch_reject_missing_base: int = 0
    update_epoch_reject_hard_residual: int = 0
    update_epoch_reject_no_reference: int = 0
    update_epoch_reject_stale_ref_pos: int = 0
    update_epoch_reject_insufficient_aircraft: int = 0
    update_epoch_last_strict_gate_pass: bool | None = None
    last_update_epoch_residual_deg: float | None = None
    last_update_epoch_predicted_deg: float | None = None
    last_update_epoch_observed_deg: float | None = None
    consecutive_hard_residual_rejects: int = 0
    sync_epoch_age_s: float | None = None
    current_phase_epoch_us: float | None = None
    candidate_epoch_us: float | None = None
    sync_reacquired_provisional: bool = False
    population_validation_state: str | None = None  # "pass" | "fail" | "insufficient_data" | "disabled"
    population_validation_reason: str | None = None  # compact reason string
    contamination_state: str | None = None  # "single_family" | "contaminated" | "insufficient_data" | "disabled"
    contamination_reason: str | None = None  # compact decision explanation
    contamination_total_observations: int = 0
    contamination_distinct_icaos: int = 0
    contamination_primary_observations: int = 0
    contamination_secondary_observations: int = 0
    contamination_secondary_icaos: int = 0
    contamination_family_separation_deg: float | None = None
    contamination_secondary_support_ratio: float | None = None
    phase_blocking_gate: str | None = None
    phase_blocking_reason: str | None = None
    blocking_gate: str | None = None
    go_operational_enabled: bool = False
    go_operational_active: bool = False
    previous_phase_anchor_icao: str | None = None
    previous_phase_anchor_age_s: float | None = None
    previous_phase_status: str | None = None
    phase_anchor_clear_reason: str | None = None
    phase_basis_override_reason: str | None = None
    last_phase_anchor_clear_ts: float | None = None
    go_sync_unusable_reason_at_clear: str | None = None
    phase_anchor_retention_reason: str | None = None


@dataclass
class AlignedBurstSyncObs:
    """One burst-centre bearing observation for multi-aircraft sync maintenance."""

    burst_centroid_us: float
    icao: str
    bearing_deg: float | None
    n_replies: int
    signal_dbfs: float | None
    pos_age_s: float
    range_nm: float | None
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
