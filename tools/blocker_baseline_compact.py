#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import pathlib
import time
import urllib.request
from collections import Counter, defaultdict
from datetime import datetime, timezone
from typing import Any


def _http_json(url: str, timeout_s: float = 8.0) -> dict[str, Any]:
    with urllib.request.urlopen(url, timeout=timeout_s) as resp:
        return json.loads(resp.read().decode("utf-8"))


def _pick(*vals: Any) -> Any:
    for v in vals:
        if v is not None:
            return v
    return None


def _pick_exact(*vals: Any) -> Any:
    """Like _pick, but named to emphasize that 0/False/'' are preserved."""
    return _pick(*vals)


def extract_compact_row(payload: dict[str, Any], iid: int, ts: float) -> dict[str, Any]:
    ss = payload.get("sync_state") or {}

    go_unusable = _pick(
        ss.get("go_sync_unusable_reason"),
        ss.get("go_diagnostic_go_sync_unusable_reason"),
    )

    tq_count = _pick(
        ss.get("transition_quarantine_count"),
        ss.get("go_diagnostic_transition_quarantine_count"),
        payload.get("transition_quarantine_count"),
        payload.get("go_diagnostic_transition_quarantine_count"),
    )
    tq_fit_excl = _pick(
        ss.get("transition_quarantine_fit_excluded_count"),
        ss.get("go_diagnostic_transition_quarantine_fit_excluded_count"),
        payload.get("transition_quarantine_fit_excluded_count"),
        payload.get("go_diagnostic_transition_quarantine_fit_excluded_count"),
    )
    tq_hard_supp = _pick(
        ss.get("transition_quarantine_hard_reject_suppressed_count"),
        ss.get("go_diagnostic_transition_quarantine_hard_reject_suppressed_count"),
        payload.get("transition_quarantine_hard_reject_suppressed_count"),
        payload.get("go_diagnostic_transition_quarantine_hard_reject_suppressed_count"),
    )

    stale_go_evidence_raw = _pick(
        ss.get("stale_go_evidence"),
        ss.get("anchor_retained_without_current_evidence"),
        payload.get("stale_go_evidence"),
    )
    handoff_reason = _pick(ss.get("handoff_reason"), payload.get("handoff_reason"))
    go_operational_blocking_gate = _pick(ss.get("go_operational_blocking_gate"), payload.get("go_operational_blocking_gate"))
    go_evidence_fresh = _pick(
        ss.get("go_evidence_fresh"),
        payload.get("go_evidence_fresh"),
    )
    stale_go_evidence_reason = _pick(
        ss.get("stale_go_evidence_reason"),
        payload.get("stale_go_evidence_reason"),
    )
    if stale_go_evidence_reason is None:
        if handoff_reason == "stale_go_evidence":
            stale_go_evidence_reason = "handoff_reason_stale_go_evidence"
        elif go_operational_blocking_gate == "go_readiness.go_evidence_fresh":
            stale_go_evidence_reason = "go_blocking_gate_evidence_fresh"

    if stale_go_evidence_raw is None:
        stale_go_evidence = (
            handoff_reason == "stale_go_evidence"
            or go_operational_blocking_gate == "go_readiness.go_evidence_fresh"
        )
    else:
        stale_go_evidence = (
            bool(stale_go_evidence_raw)
            or handoff_reason == "stale_go_evidence"
            or go_operational_blocking_gate == "go_readiness.go_evidence_fresh"
        )

    last_update_epoch_residual_deg = _pick(
        ss.get("last_update_epoch_residual_deg"),
        ss.get("go_diagnostic_last_update_epoch_residual_deg"),
        payload.get("last_update_epoch_residual_deg"),
    )

    return {
        "iid": iid,
        "timestamp": ts,
        "sync_state_present": bool(payload.get("sync_state") is not None),
        "go_operational_enabled": _pick(ss.get("go_operational_enabled"), payload.get("go_operational_enabled")),
        "go_operational_active": _pick(ss.get("go_operational_active"), payload.get("go_operational_active")),
        "go_operational_ready_streak": _pick(ss.get("go_operational_ready_streak"), payload.get("go_operational_ready_streak")),
        "go_operational_promotion_threshold": _pick(ss.get("go_operational_promotion_threshold"), payload.get("go_operational_promotion_threshold")),
        "go_operational_ready_streak_age_s": _pick(ss.get("go_operational_ready_streak_age_s"), payload.get("go_operational_ready_streak_age_s")),
        "go_operational_last_ready_ts": _pick(ss.get("go_operational_last_ready_ts"), payload.get("go_operational_last_ready_ts")),
        "go_operational_last_not_ready_ts": _pick(ss.get("go_operational_last_not_ready_ts"), payload.get("go_operational_last_not_ready_ts")),
        "go_operational_streak_reset_reason": _pick(ss.get("go_operational_streak_reset_reason"), payload.get("go_operational_streak_reset_reason")),
        "go_operational_streak_reset_gate": _pick(ss.get("go_operational_streak_reset_gate"), payload.get("go_operational_streak_reset_gate")),
        "go_operational_streak_reset_handoff_reason": _pick(ss.get("go_operational_streak_reset_handoff_reason"), payload.get("go_operational_streak_reset_handoff_reason")),
        "go_operational_soft_failure_active": _pick(ss.get("go_operational_soft_failure_active"), payload.get("go_operational_soft_failure_active")),
        "go_operational_soft_failure_until_ts": _pick(ss.get("go_operational_soft_failure_until_ts"), payload.get("go_operational_soft_failure_until_ts")),
        "go_operational_soft_failure_remaining_s": _pick(ss.get("go_operational_soft_failure_remaining_s"), payload.get("go_operational_soft_failure_remaining_s")),
        "go_operational_soft_failure_reason": _pick(ss.get("go_operational_soft_failure_reason"), payload.get("go_operational_soft_failure_reason")),
        "go_operational_hysteresis_decision": _pick(ss.get("go_operational_hysteresis_decision"), payload.get("go_operational_hysteresis_decision")),
        "go_operational_blocking_gate": go_operational_blocking_gate,
        "readiness_state": _pick(ss.get("readiness_state"), payload.get("readiness_state")),
        "readiness_reason": _pick(ss.get("readiness_reason"), payload.get("readiness_reason")),
        "readiness_blocking_gate": _pick(ss.get("readiness_blocking_gate"), payload.get("readiness_blocking_gate")),
        "blocking_gate": _pick(ss.get("blocking_gate"), payload.get("blocking_gate")),
        "handoff_state": _pick(ss.get("handoff_state"), payload.get("handoff_state")),
        "handoff_reason": handoff_reason,
        "go_sync_unusable_reason": go_unusable,
        "go_diagnostic_go_sync_unusable_reason": _pick(ss.get("go_diagnostic_go_sync_unusable_reason"), payload.get("go_diagnostic_go_sync_unusable_reason")),
        "holdover": _pick(ss.get("holdover"), payload.get("holdover")),
        "holdover_reason": _pick(ss.get("holdover_reason"), ss.get("go_diagnostic_holdover_reason"), payload.get("holdover_reason")),
        "strict_gate_primary_fail_reason": _pick(ss.get("strict_gate_primary_fail_reason"), ss.get("strict_gate_fail_reason"), payload.get("strict_gate_primary_fail_reason"), payload.get("strict_gate_fail_reason")),
        "quality_gate_fail_reason": _pick(ss.get("quality_gate_fail_reason"), payload.get("quality_gate_fail_reason")),
        "sync_quality": _pick(ss.get("sync_quality"), ss.get("go_diagnostic_sync_quality"), payload.get("sync_quality")),
        "status_at_quality_eval": _pick(ss.get("status_at_quality_eval"), ss.get("go_diagnostic_status_at_quality_eval"), payload.get("status_at_quality_eval")),
        "quality_effective_value": _pick(ss.get("go_diagnostic_go_sync_usable_quality_value"), payload.get("quality_effective_value")),
        "transition_quarantine_count": tq_count,
        "transition_quarantine_fit_excluded_count": tq_fit_excl,
        "transition_quarantine_hard_reject_suppressed_count": tq_hard_supp,
        "transition_quarantine_last_reason": _pick(
            ss.get("transition_quarantine_last_reason"),
            ss.get("go_diagnostic_transition_quarantine_last_reason"),
            payload.get("transition_quarantine_last_reason"),
        ),
        "stale_go_evidence": stale_go_evidence,
        "stale_go_evidence_reason": stale_go_evidence_reason,
        "stale_go_evidence_raw": _pick(ss.get("stale_go_evidence_raw"), payload.get("stale_go_evidence_raw")),
        "stale_go_evidence_effective": _pick(ss.get("stale_go_evidence_effective"), payload.get("stale_go_evidence_effective")),
        "stale_go_evidence_computed_from": _pick(ss.get("stale_go_evidence_computed_from"), payload.get("stale_go_evidence_computed_from")),
        "stale_go_evidence_suppressed_by_fresh_source": _pick(
            ss.get("stale_go_evidence_suppressed_by_fresh_source"),
            payload.get("stale_go_evidence_suppressed_by_fresh_source"),
        ),
        "stale_go_evidence_suppressed_reason": _pick(
            ss.get("stale_go_evidence_suppressed_reason"),
            payload.get("stale_go_evidence_suppressed_reason"),
        ),
        "go_evidence_fresh": go_evidence_fresh,
        "phase_status_display": _pick(ss.get("phase_status_display"), payload.get("phase_status_display")),
        "phase_evidence_fresh": _pick(ss.get("phase_evidence_fresh"), payload.get("phase_evidence_fresh")),
        "phase_evidence_age_s": _pick(ss.get("phase_evidence_age_s"), payload.get("phase_evidence_age_s")),
        "phase_evidence_age_source": _pick(ss.get("phase_evidence_age_source"), payload.get("phase_evidence_age_source")),
        "sync_driving_evidence_age_source": _pick(ss.get("sync_driving_evidence_age_source"), payload.get("sync_driving_evidence_age_source")),
        "last_go_evidence_event_age_s": _pick(ss.get("last_go_evidence_event_age_s"), payload.get("last_go_evidence_event_age_s")),
        "last_eligible_burst_observation_age_s": _pick(ss.get("last_eligible_burst_observation_age_s"), payload.get("last_eligible_burst_observation_age_s")),
        "last_sync_driving_fit_observation_age_s": _pick(ss.get("last_sync_driving_fit_observation_age_s"), payload.get("last_sync_driving_fit_observation_age_s")),
        "fit_epoch_last_observation_age_s": _pick(ss.get("fit_epoch_last_observation_age_s"), ss.get("go_diagnostic_fit_epoch_last_observation_age_s"), payload.get("fit_epoch_last_observation_age_s")),
        "burst_rows_absence_reason": _pick(ss.get("burst_rows_absence_reason"), ss.get("go_diagnostic_burst_rows_absence_reason"), payload.get("burst_rows_absence_reason")),
        "fit_counters_source": _pick(ss.get("fit_counters_source"), ss.get("go_diagnostic_fit_counters_source"), payload.get("fit_counters_source")),
        "slope_ema": _pick(ss.get("slope_ema_deg_per_s"), ss.get("go_diagnostic_slope_ema_deg_per_s"), payload.get("slope_ema")),
        "slope_std": _pick(ss.get("slope_std_deg_per_s"), ss.get("go_diagnostic_slope_std_deg_per_s"), payload.get("slope_std"), payload.get("slope_sigma")),
        "slope_converged": _pick(ss.get("slope_converged"), payload.get("slope_converged")),
        "slope_not_converged_reason": _pick(ss.get("slope_trend_state"), payload.get("slope_not_converged_reason")),
        "slope_not_converged_subreason": _pick(ss.get("slope_not_converged_subreason"), payload.get("slope_not_converged_subreason")),
        "slope_near_zero_window_pass": _pick(ss.get("slope_near_zero_window_pass"), payload.get("slope_near_zero_window_pass")),
        "slope_near_zero_window_duration_s": _pick(ss.get("slope_near_zero_window_duration_s"), payload.get("slope_near_zero_window_duration_s")),
        "slope_near_zero_required_duration_s": _pick(ss.get("slope_near_zero_required_duration_s"), payload.get("slope_near_zero_required_duration_s")),
        "slope_near_zero_threshold_deg_s": _pick(ss.get("slope_near_zero_threshold_deg_s"), payload.get("slope_near_zero_threshold_deg_s")),
        "slope_near_zero_max_abs_slope_deg_s": _pick(ss.get("slope_near_zero_max_abs_slope_deg_s"), payload.get("slope_near_zero_max_abs_slope_deg_s")),
        "slope_near_zero_sample_count": _pick(ss.get("slope_near_zero_sample_count"), payload.get("slope_near_zero_sample_count")),
        "slope_near_zero_fail_reason": _pick(ss.get("slope_near_zero_fail_reason"), payload.get("slope_near_zero_fail_reason")),
        "slope_regression_pass": _pick(ss.get("slope_regression_pass"), payload.get("slope_regression_pass")),
        "slope_regression_window_s": _pick(ss.get("slope_regression_window_s"), payload.get("slope_regression_window_s")),
        "slope_regression_r2": _pick(ss.get("slope_regression_r2"), payload.get("slope_regression_r2")),
        "slope_regression_r2_min": _pick(ss.get("slope_regression_r2_min"), payload.get("slope_regression_r2_min")),
        "slope_regression_trend_deg_s2": _pick(ss.get("slope_regression_trend_deg_s2"), payload.get("slope_regression_trend_deg_s2")),
        "slope_regression_trend_direction": _pick(ss.get("slope_regression_trend_direction"), payload.get("slope_regression_trend_direction")),
        "slope_regression_sample_count": _pick(ss.get("slope_regression_sample_count"), payload.get("slope_regression_sample_count")),
        "slope_regression_fail_reason": _pick(ss.get("slope_regression_fail_reason"), payload.get("slope_regression_fail_reason")),
        "retained_go_delta_rate_s_per_s_10s": _pick(ss.get("retained_go_delta_rate_s_per_s_10s"), payload.get("retained_go_delta_rate_s_per_s_10s")),
        "retained_go_delta_rate_s_per_s_30s": _pick(ss.get("retained_go_delta_rate_s_per_s_30s"), payload.get("retained_go_delta_rate_s_per_s_30s")),
        "retained_go_delta_rate_s_per_s_60s": _pick(ss.get("retained_go_delta_rate_s_per_s_60s"), payload.get("retained_go_delta_rate_s_per_s_60s")),
        "proposed_delta_rate_s_per_s_10s": _pick(ss.get("proposed_delta_rate_s_per_s_10s"), payload.get("proposed_delta_rate_s_per_s_10s")),
        "proposed_delta_rate_s_per_s_30s": _pick(ss.get("proposed_delta_rate_s_per_s_30s"), payload.get("proposed_delta_rate_s_per_s_30s")),
        "proposed_delta_rate_s_per_s_60s": _pick(ss.get("proposed_delta_rate_s_per_s_60s"), payload.get("proposed_delta_rate_s_per_s_60s")),
        "applied_delta_rate_s_per_s_10s": _pick(ss.get("applied_delta_rate_s_per_s_10s"), payload.get("applied_delta_rate_s_per_s_10s")),
        "applied_delta_rate_s_per_s_30s": _pick(ss.get("applied_delta_rate_s_per_s_30s"), payload.get("applied_delta_rate_s_per_s_30s")),
        "applied_delta_rate_s_per_s_60s": _pick(ss.get("applied_delta_rate_s_per_s_60s"), payload.get("applied_delta_rate_s_per_s_60s")),
        "retained_go_delta_abs_movement_s_10s": _pick(ss.get("retained_go_delta_abs_movement_s_10s"), payload.get("retained_go_delta_abs_movement_s_10s")),
        "retained_go_delta_abs_movement_s_30s": _pick(ss.get("retained_go_delta_abs_movement_s_30s"), payload.get("retained_go_delta_abs_movement_s_30s")),
        "retained_go_delta_abs_movement_s_60s": _pick(ss.get("retained_go_delta_abs_movement_s_60s"), payload.get("retained_go_delta_abs_movement_s_60s")),
        "proposed_delta_abs_movement_s_10s": _pick(ss.get("proposed_delta_abs_movement_s_10s"), payload.get("proposed_delta_abs_movement_s_10s")),
        "proposed_delta_abs_movement_s_30s": _pick(ss.get("proposed_delta_abs_movement_s_30s"), payload.get("proposed_delta_abs_movement_s_30s")),
        "proposed_delta_abs_movement_s_60s": _pick(ss.get("proposed_delta_abs_movement_s_60s"), payload.get("proposed_delta_abs_movement_s_60s")),
        "applied_delta_abs_movement_s_10s": _pick(ss.get("applied_delta_abs_movement_s_10s"), payload.get("applied_delta_abs_movement_s_10s")),
        "applied_delta_abs_movement_s_30s": _pick(ss.get("applied_delta_abs_movement_s_30s"), payload.get("applied_delta_abs_movement_s_30s")),
        "applied_delta_abs_movement_s_60s": _pick(ss.get("applied_delta_abs_movement_s_60s"), payload.get("applied_delta_abs_movement_s_60s")),
        "slope_target_moving_10s": _pick(ss.get("slope_target_moving_10s"), payload.get("slope_target_moving_10s")),
        "slope_target_moving_30s": _pick(ss.get("slope_target_moving_30s"), payload.get("slope_target_moving_30s")),
        "slope_target_moving_60s": _pick(ss.get("slope_target_moving_60s"), payload.get("slope_target_moving_60s")),
        "slope_target_movement_reason": _pick(ss.get("slope_target_movement_reason"), payload.get("slope_target_movement_reason")),
        "shadow_delta_freeze_variant_a_would_pass": _pick(ss.get("shadow_delta_freeze_variant_a_would_pass"), payload.get("shadow_delta_freeze_variant_a_would_pass")),
        "shadow_delta_freeze_variant_b_would_pass": _pick(ss.get("shadow_delta_freeze_variant_b_would_pass"), payload.get("shadow_delta_freeze_variant_b_would_pass")),
        "shadow_delta_stability_variant_c_would_pass": _pick(ss.get("shadow_delta_stability_variant_c_would_pass"), payload.get("shadow_delta_stability_variant_c_would_pass")),
        "shadow_delta_freeze_would_promote": _pick(ss.get("shadow_delta_freeze_would_promote"), payload.get("shadow_delta_freeze_would_promote")),
        "shadow_delta_freeze_blocker_if_not": _pick(ss.get("shadow_delta_freeze_blocker_if_not"), payload.get("shadow_delta_freeze_blocker_if_not")),
        "shadow_delta_freeze_safety_class": _pick(ss.get("shadow_delta_freeze_safety_class"), payload.get("shadow_delta_freeze_safety_class")),
        "shadow_delta_freeze_safety_flags": _pick(ss.get("shadow_delta_freeze_safety_flags"), payload.get("shadow_delta_freeze_safety_flags")),
        "shadow_delta_freeze_residual_abs_p95": _pick(ss.get("shadow_delta_freeze_residual_abs_p95"), payload.get("shadow_delta_freeze_residual_abs_p95")),
        "shadow_delta_freeze_recent_holdover": _pick(ss.get("shadow_delta_freeze_recent_holdover"), payload.get("shadow_delta_freeze_recent_holdover")),
        "shadow_delta_freeze_recent_hard_reject": _pick(ss.get("shadow_delta_freeze_recent_hard_reject"), payload.get("shadow_delta_freeze_recent_hard_reject")),
        "shadow_delta_freeze_recent_transition": _pick(ss.get("shadow_delta_freeze_recent_transition"), payload.get("shadow_delta_freeze_recent_transition")),
        "shadow_delta_freeze_support_ok": _pick(ss.get("shadow_delta_freeze_support_ok"), payload.get("shadow_delta_freeze_support_ok")),
        "shadow_delta_freeze_strict_ok": _pick(ss.get("shadow_delta_freeze_strict_ok"), payload.get("shadow_delta_freeze_strict_ok")),
        "shadow_freeze_candidate_count_10m": _pick(ss.get("shadow_freeze_candidate_count_10m"), payload.get("shadow_freeze_candidate_count_10m")),
        "shadow_freeze_safe_candidate_count_10m": _pick(ss.get("shadow_freeze_safe_candidate_count_10m"), payload.get("shadow_freeze_safe_candidate_count_10m")),
        "shadow_freeze_unsafe_candidate_count_10m": _pick(ss.get("shadow_freeze_unsafe_candidate_count_10m"), payload.get("shadow_freeze_unsafe_candidate_count_10m")),
        "shadow_freeze_transition_contaminated_count_10m": _pick(ss.get("shadow_freeze_transition_contaminated_count_10m"), payload.get("shadow_freeze_transition_contaminated_count_10m")),
        "shadow_freeze_promote_opportunity_count_10m": _pick(ss.get("shadow_freeze_promote_opportunity_count_10m"), payload.get("shadow_freeze_promote_opportunity_count_10m")),
        "shadow_freeze_false_positive_risk_count_10m": _pick(ss.get("shadow_freeze_false_positive_risk_count_10m"), payload.get("shadow_freeze_false_positive_risk_count_10m")),
        "shadow_freeze_candidate_count_30m": _pick(ss.get("shadow_freeze_candidate_count_30m"), payload.get("shadow_freeze_candidate_count_30m")),
        "shadow_freeze_safe_candidate_count_30m": _pick(ss.get("shadow_freeze_safe_candidate_count_30m"), payload.get("shadow_freeze_safe_candidate_count_30m")),
        "shadow_freeze_unsafe_candidate_count_30m": _pick(ss.get("shadow_freeze_unsafe_candidate_count_30m"), payload.get("shadow_freeze_unsafe_candidate_count_30m")),
        "shadow_freeze_transition_contaminated_count_30m": _pick(ss.get("shadow_freeze_transition_contaminated_count_30m"), payload.get("shadow_freeze_transition_contaminated_count_30m")),
        "shadow_freeze_promote_opportunity_count_30m": _pick(ss.get("shadow_freeze_promote_opportunity_count_30m"), payload.get("shadow_freeze_promote_opportunity_count_30m")),
        "shadow_freeze_false_positive_risk_count_30m": _pick(ss.get("shadow_freeze_false_positive_risk_count_30m"), payload.get("shadow_freeze_false_positive_risk_count_30m")),
        "proposed_period_delta_s": _pick(ss.get("proposed_delta_s"), ss.get("go_diagnostic_proposed_delta_s"), payload.get("proposed_period_delta_s")),
        "applied_period_delta_s": _pick(ss.get("applied_delta_s"), ss.get("go_diagnostic_applied_delta_s"), payload.get("applied_period_delta_s")),
        "current_period_delta_s": _pick(ss.get("current_delta_s"), ss.get("go_diagnostic_current_delta_s"), payload.get("current_period_delta_s")),
        "period_delta_stdev_s": _pick(ss.get("period_delta_stdev_s"), payload.get("period_delta_stdev_s")),
        "refinement_status": _pick(ss.get("go_diagnostic_refinement_status"), ss.get("period_refinement_status"), payload.get("refinement_status")),
        "refinement_history_obs": _pick(ss.get("go_diagnostic_fit_observation_count"), ss.get("fit_observation_count"), payload.get("refinement_history_obs")),
        "refinement_history_icaos": _pick(ss.get("go_diagnostic_fit_icao_count"), ss.get("fit_icao_count"), payload.get("refinement_history_icaos")),
        "refinement_history_span_s": _pick(ss.get("go_diagnostic_fit_span_s"), ss.get("fit_span_s"), payload.get("refinement_history_span_s")),
        "fit_observation_count": _pick(ss.get("fit_observation_count"), ss.get("go_diagnostic_fit_observation_count"), payload.get("fit_observation_count")),
        "fit_icao_count": _pick(ss.get("fit_icao_count"), ss.get("go_diagnostic_fit_icao_count"), payload.get("fit_icao_count")),
        "fit_span_s": _pick(ss.get("fit_span_s"), ss.get("go_diagnostic_fit_span_s"), payload.get("fit_span_s")),
        "current_fit_epoch_age_s": _pick(ss.get("current_fit_epoch_age_s"), ss.get("go_diagnostic_current_fit_epoch_age_s"), payload.get("current_fit_epoch_age_s")),
        "strict_gate_pass": _pick(ss.get("strict_gate_pass"), payload.get("strict_gate_pass")),
        "strict_epoch_required_min_aircraft": _pick(ss.get("strict_epoch_required_min_aircraft"), ss.get("strict_gate_min_fit_obs"), payload.get("strict_epoch_required_min_aircraft")),
        "strict_epoch_required_ref_age_s": _pick(ss.get("strict_epoch_required_ref_age_s"), payload.get("strict_epoch_required_ref_age_s")),
        "strict_epoch_actual_n_aircraft": _pick(ss.get("strict_gate_n_aircraft"), payload.get("strict_epoch_actual_n_aircraft")),
        "strict_epoch_actual_ref_age_s": _pick(ss.get("strict_gate_ref_pos_age_s"), payload.get("strict_epoch_actual_ref_age_s")),
        "last_update_epoch_n_aircraft": _pick(ss.get("last_update_epoch_n_aircraft"), payload.get("last_update_epoch_n_aircraft")),
        "last_update_epoch_ref_icao": _pick(ss.get("last_update_epoch_ref_icao"), payload.get("last_update_epoch_ref_icao")),
        "last_update_epoch_ref_pos_age_s": _pick(ss.get("last_update_epoch_ref_pos_age_s"), payload.get("last_update_epoch_ref_pos_age_s")),
        "last_update_epoch_ref_range_nm": _pick(ss.get("strict_gate_ref_range_nm"), payload.get("last_update_epoch_ref_range_nm")),
        "last_update_epoch_residual_deg": last_update_epoch_residual_deg,
        "last_update_epoch_abs_residual_deg": abs(last_update_epoch_residual_deg) if isinstance(last_update_epoch_residual_deg, (int, float)) else None,
        "last_update_epoch_outcome": _pick(ss.get("go_diagnostic_last_update_epoch_outcome"), ss.get("strict_gate_last_update_outcome"), payload.get("last_update_epoch_outcome")),
        "reacquire_support_obs_count": _pick(ss.get("go_diagnostic_reacquire_support_obs_count"), payload.get("reacquire_support_obs_count")),
        "reacquire_support_icao_count": _pick(ss.get("go_diagnostic_reacquire_support_icao_count"), payload.get("reacquire_support_icao_count")),
        "consecutive_hard_residual_rejects": _pick(ss.get("go_diagnostic_consecutive_hard_residual_rejects"), ss.get("consecutive_hard_residual_rejects"), payload.get("consecutive_hard_residual_rejects")),
        "phase_offset_discontinuity_count": _pick(ss.get("phase_offset_discontinuity_count"), payload.get("phase_offset_discontinuity_count")),
        "phase_offset_discontinuity_rebased_gradual_drift_count": _pick(ss.get("phase_offset_discontinuity_rebased_gradual_drift_count"), payload.get("phase_offset_discontinuity_rebased_gradual_drift_count")),
        "fit_epoch_reset_reason": _pick(ss.get("fit_epoch_reset_reason"), ss.get("go_diagnostic_fit_epoch_reset_reason"), payload.get("fit_epoch_reset_reason")),
        "fit_epoch_id": _pick(ss.get("fit_epoch_id"), ss.get("go_diagnostic_fit_epoch_id"), payload.get("fit_epoch_id")),
        "hard_bound_reject_count": _pick(ss.get("hard_bound_reject_count"), ss.get("go_diagnostic_consecutive_hard_bound_rejects"), payload.get("hard_bound_reject_count")),
        "slew_limited_count": _pick(ss.get("slew_limited_count"), payload.get("slew_limited_count")),
        "holdover_entered_ts": _pick_exact(ss.get("holdover_entered_ts"), payload.get("holdover_entered_ts")),
        "holdover_age_s": _pick_exact(ss.get("holdover_age_s"), payload.get("holdover_age_s")),
        "holdover_last_transition_ts": _pick_exact(ss.get("holdover_last_transition_ts"), payload.get("holdover_last_transition_ts")),
        "holdover_exit_attempted": _pick_exact(ss.get("holdover_exit_attempted"), payload.get("holdover_exit_attempted")),
        "holdover_exit_allowed": _pick_exact(ss.get("holdover_exit_allowed"), payload.get("holdover_exit_allowed")),
        "holdover_exit_block_reason": _pick_exact(ss.get("holdover_exit_block_reason"), payload.get("holdover_exit_block_reason")),
        "holdover_exit_first_failed_gate": _pick_exact(ss.get("holdover_exit_first_failed_gate"), payload.get("holdover_exit_first_failed_gate")),
        "reacquire_valid_period": _pick_exact(ss.get("reacquire_valid_period"), payload.get("reacquire_valid_period")),
        "reacquire_valid_ref_icao": _pick_exact(ss.get("reacquire_valid_ref_icao"), payload.get("reacquire_valid_ref_icao")),
        "reacquire_ref_position_present": _pick_exact(ss.get("reacquire_ref_position_present"), payload.get("reacquire_ref_position_present")),
        "reacquire_ref_pos_age_s": _pick_exact(ss.get("reacquire_ref_pos_age_s"), payload.get("reacquire_ref_pos_age_s")),
        "reacquire_ref_pos_age_limit_s": _pick_exact(ss.get("reacquire_ref_pos_age_limit_s"), payload.get("reacquire_ref_pos_age_limit_s")),
        "reacquire_ref_pos_fresh": _pick_exact(ss.get("reacquire_ref_pos_fresh"), payload.get("reacquire_ref_pos_fresh")),
        "reacquire_n_aircraft": _pick_exact(ss.get("reacquire_n_aircraft"), payload.get("reacquire_n_aircraft")),
        "reacquire_min_aircraft": _pick_exact(ss.get("reacquire_min_aircraft"), payload.get("reacquire_min_aircraft")),
        "reacquire_aircraft_count_ok": _pick_exact(ss.get("reacquire_aircraft_count_ok"), payload.get("reacquire_aircraft_count_ok")),
        "reacquire_support_obs_count": _pick_exact(
            ss.get("reacquire_support_obs_count"),
            ss.get("go_diagnostic_reacquire_support_obs_count"),
            payload.get("reacquire_support_obs_count"),
        ),
        "reacquire_support_obs_min": _pick_exact(ss.get("reacquire_support_obs_min"), payload.get("reacquire_support_obs_min")),
        "reacquire_support_obs_ok": _pick_exact(ss.get("reacquire_support_obs_ok"), payload.get("reacquire_support_obs_ok")),
        "reacquire_support_icao_count": _pick_exact(
            ss.get("reacquire_support_icao_count"),
            ss.get("go_diagnostic_reacquire_support_icao_count"),
            payload.get("reacquire_support_icao_count"),
        ),
        "reacquire_support_icao_min": _pick_exact(ss.get("reacquire_support_icao_min"), payload.get("reacquire_support_icao_min")),
        "reacquire_support_icao_ok": _pick_exact(ss.get("reacquire_support_icao_ok"), payload.get("reacquire_support_icao_ok")),
        "reacquire_effective_obs_count": _pick_exact(ss.get("reacquire_effective_obs_count"), payload.get("reacquire_effective_obs_count")),
        "reacquire_effective_icao_count": _pick_exact(ss.get("reacquire_effective_icao_count"), payload.get("reacquire_effective_icao_count")),
        "reacquire_fit_support_ok": _pick_exact(ss.get("reacquire_fit_support_ok"), payload.get("reacquire_fit_support_ok")),
        "reacquire_hard_reject_streak": _pick_exact(ss.get("reacquire_hard_reject_streak"), payload.get("reacquire_hard_reject_streak")),
        "reacquire_min_hard_reject_streak": _pick_exact(ss.get("reacquire_min_hard_reject_streak"), payload.get("reacquire_min_hard_reject_streak")),
        "reacquire_hard_reject_streak_ok": _pick_exact(ss.get("reacquire_hard_reject_streak_ok"), payload.get("reacquire_hard_reject_streak_ok")),
        "reacquire_fallback_hard_rejects_min": _pick_exact(ss.get("reacquire_fallback_hard_rejects_min"), payload.get("reacquire_fallback_hard_rejects_min")),
        "reacquire_last_accepted_epoch_age_s": _pick_exact(ss.get("reacquire_last_accepted_epoch_age_s"), payload.get("reacquire_last_accepted_epoch_age_s")),
        "reacquire_fallback_age_limit_s": _pick_exact(ss.get("reacquire_fallback_age_limit_s"), payload.get("reacquire_fallback_age_limit_s")),
        "reacquire_last_accepted_age_ok": _pick_exact(ss.get("reacquire_last_accepted_age_ok"), payload.get("reacquire_last_accepted_age_ok")),
        "reacquire_fallback_allowed": _pick_exact(ss.get("reacquire_fallback_allowed"), payload.get("reacquire_fallback_allowed")),
        "reacquire_hard_reject_condition_ok": _pick_exact(ss.get("reacquire_hard_reject_condition_ok"), payload.get("reacquire_hard_reject_condition_ok")),
        "reacquire_current_hard_reject": _pick_exact(ss.get("reacquire_current_hard_reject"), payload.get("reacquire_current_hard_reject")),
        "reacquire_exit_trigger_seen": _pick_exact(ss.get("reacquire_exit_trigger_seen"), payload.get("reacquire_exit_trigger_seen")),
        "reacquire_can_reacquire_base": _pick_exact(ss.get("reacquire_can_reacquire_base"), payload.get("reacquire_can_reacquire_base")),
        "reacquire_hidden_gate_name": _pick_exact(ss.get("reacquire_hidden_gate_name"), payload.get("reacquire_hidden_gate_name")),
        "reacquire_hidden_gate_pass": _pick_exact(ss.get("reacquire_hidden_gate_pass"), payload.get("reacquire_hidden_gate_pass")),
        "reacquire_temporal_pending": _pick_exact(ss.get("reacquire_temporal_pending"), payload.get("reacquire_temporal_pending")),
        "reacquire_exit_code_path": _pick_exact(ss.get("reacquire_exit_code_path"), payload.get("reacquire_exit_code_path")),
        "reacquire_exit_decision": _pick_exact(ss.get("reacquire_exit_decision"), payload.get("reacquire_exit_decision")),
        "reacquire_exit_decision_reason": _pick_exact(ss.get("reacquire_exit_decision_reason"), payload.get("reacquire_exit_decision_reason")),
        "reacquire_all_gates_pass": _pick_exact(ss.get("reacquire_all_gates_pass"), payload.get("reacquire_all_gates_pass")),
        "reacquire_ref_selection_source": _pick_exact(ss.get("reacquire_ref_selection_source"), payload.get("reacquire_ref_selection_source")),
        "reacquire_ref_selection_reason": _pick_exact(ss.get("reacquire_ref_selection_reason"), payload.get("reacquire_ref_selection_reason")),
        "reacquire_ref_icao_current_epoch_present": _pick_exact(ss.get("reacquire_ref_icao_current_epoch_present"), payload.get("reacquire_ref_icao_current_epoch_present")),
        "reacquire_ref_icao_fit_pool_present": _pick_exact(ss.get("reacquire_ref_icao_fit_pool_present"), payload.get("reacquire_ref_icao_fit_pool_present")),
        "reacquire_ref_icao_support_pool_present": _pick_exact(ss.get("reacquire_ref_icao_support_pool_present"), payload.get("reacquire_ref_icao_support_pool_present")),
        "reacquire_better_ref_candidate_available": _pick_exact(ss.get("reacquire_better_ref_candidate_available"), payload.get("reacquire_better_ref_candidate_available")),
        "reacquire_better_ref_candidate_icao": _pick_exact(ss.get("reacquire_better_ref_candidate_icao"), payload.get("reacquire_better_ref_candidate_icao")),
        "reacquire_better_ref_candidate_pos_age_s": _pick_exact(ss.get("reacquire_better_ref_candidate_pos_age_s"), payload.get("reacquire_better_ref_candidate_pos_age_s")),
        "reacquire_better_ref_candidate_obs_count": _pick_exact(ss.get("reacquire_better_ref_candidate_obs_count"), payload.get("reacquire_better_ref_candidate_obs_count")),
        "reacquire_better_ref_candidate_fit_eligible": _pick_exact(ss.get("reacquire_better_ref_candidate_fit_eligible"), payload.get("reacquire_better_ref_candidate_fit_eligible")),
        "reacquire_ref_replacement_suppressed_reason": _pick_exact(ss.get("reacquire_ref_replacement_suppressed_reason"), payload.get("reacquire_ref_replacement_suppressed_reason")),
        "shadow_reacquire_with_best_fresh_ref": _pick_exact(ss.get("shadow_reacquire_with_best_fresh_ref"), payload.get("shadow_reacquire_with_best_fresh_ref")),
        "shadow_ref_candidate_icao": _pick_exact(ss.get("shadow_ref_candidate_icao"), payload.get("shadow_ref_candidate_icao")),
        "shadow_ref_candidate_age_s": _pick_exact(ss.get("shadow_ref_candidate_age_s"), payload.get("shadow_ref_candidate_age_s")),
        "shadow_ref_candidate_residual_deg": _pick_exact(ss.get("shadow_ref_candidate_residual_deg"), payload.get("shadow_ref_candidate_residual_deg")),
        "shadow_ref_candidate_support_obs": _pick_exact(ss.get("shadow_ref_candidate_support_obs"), payload.get("shadow_ref_candidate_support_obs")),
        "shadow_ref_candidate_support_icaos": _pick_exact(ss.get("shadow_ref_candidate_support_icaos"), payload.get("shadow_ref_candidate_support_icaos")),
        "shadow_all_reacquire_gates_pass_with_candidate": _pick_exact(ss.get("shadow_all_reacquire_gates_pass_with_candidate"), payload.get("shadow_all_reacquire_gates_pass_with_candidate")),
        "shadow_safety_flags": _pick_exact(ss.get("shadow_safety_flags"), payload.get("shadow_safety_flags")),
    }


def side_by_side(raw_payload: dict[str, Any], compact: dict[str, Any]) -> dict[str, Any]:
    ss = raw_payload.get("sync_state") or {}
    return {
        "raw_sync_state.go_operational_active": ss.get("go_operational_active"),
        "compact.go_operational_active": compact.get("go_operational_active"),
        "raw_sync_state.go_operational_ready_streak": ss.get("go_operational_ready_streak"),
        "compact.go_operational_ready_streak": compact.get("go_operational_ready_streak"),
        "raw_sync_state.go_operational_promotion_threshold": ss.get("go_operational_promotion_threshold"),
        "compact.go_operational_promotion_threshold": compact.get("go_operational_promotion_threshold"),
        "raw_sync_state.go_operational_hysteresis_decision": ss.get("go_operational_hysteresis_decision"),
        "compact.go_operational_hysteresis_decision": compact.get("go_operational_hysteresis_decision"),
        "raw_sync_state.go_operational_soft_failure_active": ss.get("go_operational_soft_failure_active"),
        "compact.go_operational_soft_failure_active": compact.get("go_operational_soft_failure_active"),
        "raw_sync_state.handoff_state": ss.get("handoff_state"),
        "compact.handoff_state": compact.get("handoff_state"),
        "raw_sync_state.handoff_reason": ss.get("handoff_reason"),
        "compact.handoff_reason": compact.get("handoff_reason"),
        "raw_sync_state.go_operational_blocking_gate": ss.get("go_operational_blocking_gate"),
        "compact.go_operational_blocking_gate": compact.get("go_operational_blocking_gate"),
        "raw_sync_state.blocking_gate": ss.get("blocking_gate"),
        "compact.blocking_gate": compact.get("blocking_gate"),
        "raw_sync_state.go_sync_unusable_reason": ss.get("go_sync_unusable_reason"),
        "compact.go_sync_unusable_reason": compact.get("go_sync_unusable_reason"),
        "raw_sync_state.holdover_reason": ss.get("holdover_reason"),
        "compact.holdover_reason": compact.get("holdover_reason"),
        "raw_sync_state.strict_gate_primary_fail_reason": ss.get("strict_gate_primary_fail_reason"),
        "compact.strict_gate_primary_fail_reason": compact.get("strict_gate_primary_fail_reason"),
        "raw_sync_state.transition_quarantine_count": ss.get("transition_quarantine_count"),
        "compact.transition_quarantine_count": compact.get("transition_quarantine_count"),
        "raw_sync_state.transition_quarantine_fit_excluded_count": ss.get("transition_quarantine_fit_excluded_count"),
        "compact.transition_quarantine_fit_excluded_count": compact.get("transition_quarantine_fit_excluded_count"),
        "raw_sync_state.transition_quarantine_hard_reject_suppressed_count": ss.get("transition_quarantine_hard_reject_suppressed_count"),
        "compact.transition_quarantine_hard_reject_suppressed_count": compact.get("transition_quarantine_hard_reject_suppressed_count"),
    }


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--base-url", default="http://127.0.0.1:8000")
    ap.add_argument("--duration-s", type=int, default=600)
    ap.add_argument("--interval-s", type=float, default=5.0)
    ap.add_argument("--top-n", type=int, default=25)
    ap.add_argument("--out-dir", default="tasks/radar_sync_baseline")
    ap.add_argument("--label", default="")
    args = ap.parse_args()

    out_dir = pathlib.Path(args.out_dir)
    out_dir.mkdir(parents=True, exist_ok=True)
    stamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    lbl = f"_{args.label}" if args.label else ""
    rows_path = out_dir / f"blocker_baseline_compact{lbl}_{stamp}_rows.jsonl"
    summary_path = out_dir / f"blocker_baseline_compact{lbl}_{stamp}_summary.json"
    sbs_path = out_dir / f"blocker_baseline_compact{lbl}_{stamp}_raw_vs_compact.json"

    iids_payload = _http_json(f"{args.base_url.rstrip('/')}/api/radar/iids")
    top_rows = sorted(iids_payload.get("iids", []), key=lambda r: r.get("count", 0), reverse=True)[: args.top_n]
    iids = [int(r["iid"]) for r in top_rows if isinstance(r.get("iid"), int)]

    start = time.time()
    deadline = start + args.duration_s
    rows = []
    fails = []
    side = []

    while time.time() < deadline:
        tick = time.time()
        for iid in iids:
            url = f"{args.base_url.rstrip('/')}/api/radar/iids/{iid}/sync-snapshot?window_s=90&compact=1"
            try:
                payload = _http_json(url)
                compact = extract_compact_row(payload, iid=iid, ts=tick)
                rows.append(compact)
                if len(side) < 5:
                    side.append({"iid": iid, "timestamp": tick, **side_by_side(payload, compact)})
            except Exception as exc:
                fails.append({"iid": iid, "timestamp": tick, "error": str(exc)})
        sleep_s = args.interval_s - (time.time() - tick)
        if sleep_s > 0:
            time.sleep(sleep_s)

    with rows_path.open("w", encoding="utf-8") as f:
        for r in rows:
            f.write(json.dumps(r, separators=(",", ":")) + "\n")
    with sbs_path.open("w", encoding="utf-8") as f:
        json.dump(side, f, indent=2)

    def count(field: str) -> dict[str, int]:
        c = Counter(str(r.get(field)) for r in rows)
        return dict(c.most_common())

    by_iid = defaultdict(list)
    for r in rows:
        by_iid[r["iid"]].append(r)

    summary = {
        "capture": {
            "rows": len(rows),
            "failures": len(fails),
            "duration_s": args.duration_s,
            "interval_s": args.interval_s,
            "selected_iids": iids,
            "start_ts": start,
            "end_ts": time.time(),
            "side_by_side_file": str(sbs_path),
        },
        "go_operational_active_dwell_by_iid": {str(iid): sum(1 for r in rr if r.get("go_operational_active") is True) for iid, rr in by_iid.items()},
        "handoff_state_dwell_subset": {k: count("handoff_state").get(k, 0) for k in ["GO_REFINED_READY", "GO_REFINING", "HOLDOVER", "BOOTSTRAPPING_PY"]},
        "go_operational_blocking_gate_counts": count("go_operational_blocking_gate"),
        "readiness_state_counts": count("readiness_state"),
        "readiness_reason_counts": count("readiness_reason"),
        "handoff_reason_counts": count("handoff_reason"),
        "go_sync_unusable_reason_counts": count("go_sync_unusable_reason"),
        "holdover_reason_counts": count("holdover_reason"),
        "strict_gate_primary_fail_reason_counts": count("strict_gate_primary_fail_reason"),
        "transition_quarantine_counters": {
            "max_count": max((r.get("transition_quarantine_count") for r in rows if r.get("transition_quarantine_count") is not None), default=None),
            "max_fit_excluded": max((r.get("transition_quarantine_fit_excluded_count") for r in rows if r.get("transition_quarantine_fit_excluded_count") is not None), default=None),
            "max_hard_reject_suppressed": max((r.get("transition_quarantine_hard_reject_suppressed_count") for r in rows if r.get("transition_quarantine_hard_reject_suppressed_count") is not None), default=None),
        },
        "stale_go_evidence_counts": count("stale_go_evidence"),
        "stale_anchor_trust_violations": sum(1 for r in rows if r.get("phase_status_display") == "anchor_trusted" and (r.get("phase_evidence_fresh") is False or (isinstance(r.get("phase_evidence_age_s"), (int, float)) and r.get("phase_evidence_age_s") > 120))),
        "go_state_unclassified_or_missing_blocker_rows": sum(1 for r in rows if r.get("sync_state_present") and not r.get("go_operational_active") and r.get("go_operational_blocking_gate") is None),
        "raw_vs_compact_examples": side,
    }

    with summary_path.open("w", encoding="utf-8") as f:
        json.dump(summary, f, indent=2)

    print(json.dumps({"rows_file": str(rows_path), "summary_file": str(summary_path), "side_by_side_file": str(sbs_path), "rows": len(rows), "fails": len(fails)}))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
