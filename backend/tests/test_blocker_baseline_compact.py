from __future__ import annotations

import importlib.util
from pathlib import Path


def _load_module():
    root = Path(__file__).resolve().parents[2]
    mod_path = root / "tools" / "blocker_baseline_compact.py"
    spec = importlib.util.spec_from_file_location("blocker_baseline_compact", mod_path)
    assert spec is not None and spec.loader is not None
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod


def test_extract_compact_fields_and_preserve_zero_false_values():
    mod = _load_module()

    payload = {
        "sync_state": {
            "go_operational_enabled": True,
            "go_operational_active": False,
            "go_operational_ready_streak": 0,
            "go_operational_promotion_threshold": 3,
            "go_operational_ready_streak_age_s": 0.0,
            "go_operational_last_ready_ts": 0.0,
            "go_operational_last_not_ready_ts": 0.0,
            "go_operational_streak_reset_reason": "",
            "go_operational_streak_reset_gate": "",
            "go_operational_streak_reset_handoff_reason": "",
            "go_operational_soft_failure_active": False,
            "go_operational_soft_failure_until_ts": 0.0,
            "go_operational_soft_failure_remaining_s": 0.0,
            "go_operational_soft_failure_reason": "",
            "go_operational_hysteresis_decision": "pending",
            "go_operational_blocking_gate": "go_readiness.go_sync_state_usable",
            "blocking_gate": "go_sync_state_usable",
            "handoff_state": "GO_REFINING",
            "handoff_reason": "strict_gate_failed",
            "go_sync_unusable_reason": "strict_gate_failed",
            "go_diagnostic_go_sync_unusable_reason": "strict_gate_failed",
            "holdover": False,
            "holdover_reason": "",
            "strict_gate_primary_fail_reason": "strict_gate_failed",
            "quality_gate_fail_reason": "quality_below_threshold",
            "sync_quality": 0.0,
            "status_at_quality_eval": "",
            "go_diagnostic_go_sync_usable_quality_value": 0.0,
            "transition_quarantine_count": 0,
            "transition_quarantine_fit_excluded_count": 11,
            "transition_quarantine_hard_reject_suppressed_count": 0,
            "stale_go_evidence": False,
            "phase_status_display": "anchor_untrusted",
            "phase_evidence_fresh": False,
            "phase_evidence_age_s": 0.0,
            "slope_ema_deg_per_s": 0.0,
            "slope_std_deg_per_s": 0.0,
            "proposed_delta_s": 0.0,
            "applied_delta_s": 0.0,
            "current_delta_s": 0.0,
            "go_diagnostic_fit_observation_count": 0,
            "go_diagnostic_fit_icao_count": 0,
            "go_diagnostic_fit_span_s": 0.0,
            "go_diagnostic_fit_epoch_last_observation_age_s": 0.0,
            "go_diagnostic_current_fit_epoch_age_s": 0.0,
            "go_diagnostic_last_update_epoch_outcome": "",
            "go_diagnostic_consecutive_hard_residual_rejects": 0,
            "go_diagnostic_reacquire_support_obs_count": 0,
            "go_diagnostic_reacquire_support_icao_count": 0,
            "holdover_entered_ts": 0.0,
            "holdover_age_s": 0.0,
            "holdover_last_transition_ts": 0.0,
            "holdover_exit_attempted": False,
            "holdover_exit_allowed": False,
            "holdover_exit_block_reason": "",
            "holdover_exit_first_failed_gate": "reacquire_support_obs",
            "reacquire_valid_period": True,
            "reacquire_valid_ref_icao": False,
            "reacquire_ref_position_present": False,
            "reacquire_ref_pos_age_s": 0.0,
            "reacquire_ref_pos_age_limit_s": 8.0,
            "reacquire_ref_pos_fresh": False,
            "reacquire_n_aircraft": 0,
            "reacquire_min_aircraft": 3,
            "reacquire_aircraft_count_ok": False,
            "reacquire_support_obs_count": 0,
            "reacquire_support_obs_min": 3,
            "reacquire_support_obs_ok": False,
            "reacquire_support_icao_count": 0,
            "reacquire_support_icao_min": 2,
            "reacquire_support_icao_ok": False,
            "reacquire_hard_reject_streak": 0,
            "reacquire_min_hard_reject_streak": 0,
            "reacquire_hard_reject_streak_ok": True,
            "reacquire_last_accepted_epoch_age_s": 0.0,
            "reacquire_fallback_age_limit_s": 0.0,
            "reacquire_fallback_allowed": False,
            "reacquire_all_gates_pass": False,
        }
    }

    row = mod.extract_compact_row(payload, iid=1, ts=123.0)

    assert row["go_operational_enabled"] is True
    assert row["go_operational_active"] is False
    assert row["go_operational_ready_streak"] == 0
    assert row["go_operational_promotion_threshold"] == 3
    assert row["go_operational_ready_streak_age_s"] == 0.0
    assert row["go_operational_last_ready_ts"] == 0.0
    assert row["go_operational_last_not_ready_ts"] == 0.0
    assert row["go_operational_streak_reset_reason"] == ""
    assert row["go_operational_streak_reset_gate"] == ""
    assert row["go_operational_streak_reset_handoff_reason"] == ""
    assert row["go_operational_soft_failure_active"] is False
    assert row["go_operational_soft_failure_until_ts"] == 0.0
    assert row["go_operational_soft_failure_remaining_s"] == 0.0
    assert row["go_operational_soft_failure_reason"] == ""
    assert row["go_operational_hysteresis_decision"] == "pending"
    assert row["go_operational_blocking_gate"] == "go_readiness.go_sync_state_usable"
    assert row["blocking_gate"] == "go_sync_state_usable"
    assert row["handoff_state"] == "GO_REFINING"
    assert row["handoff_reason"] == "strict_gate_failed"
    assert row["go_sync_unusable_reason"] == "strict_gate_failed"
    assert row["go_diagnostic_go_sync_unusable_reason"] == "strict_gate_failed"
    assert row["holdover"] is False
    assert row["holdover_reason"] == ""
    assert row["strict_gate_primary_fail_reason"] == "strict_gate_failed"
    assert row["quality_gate_fail_reason"] == "quality_below_threshold"
    assert row["sync_quality"] == 0.0
    assert row["status_at_quality_eval"] == ""
    assert row["quality_effective_value"] == 0.0
    assert row["transition_quarantine_count"] == 0
    assert row["transition_quarantine_fit_excluded_count"] == 11
    assert row["transition_quarantine_hard_reject_suppressed_count"] == 0
    assert row["stale_go_evidence"] is False
    assert row["phase_status_display"] == "anchor_untrusted"
    assert row["phase_evidence_fresh"] is False
    assert row["phase_evidence_age_s"] == 0.0
    assert row["slope_ema"] == 0.0
    assert row["slope_std"] == 0.0
    assert row["proposed_period_delta_s"] == 0.0
    assert row["applied_period_delta_s"] == 0.0
    assert row["current_period_delta_s"] == 0.0
    assert row["fit_observation_count"] == 0
    assert row["fit_icao_count"] == 0
    assert row["fit_span_s"] == 0.0
    assert row["fit_epoch_last_observation_age_s"] == 0.0
    assert row["current_fit_epoch_age_s"] == 0.0
    assert row["last_update_epoch_outcome"] == ""
    assert row["consecutive_hard_residual_rejects"] == 0
    assert row["reacquire_support_obs_count"] == 0
    assert row["reacquire_support_icao_count"] == 0
    assert row["holdover_entered_ts"] == 0.0
    assert row["holdover_age_s"] == 0.0
    assert row["holdover_last_transition_ts"] == 0.0
    assert row["holdover_exit_attempted"] is False
    assert row["holdover_exit_allowed"] is False
    assert row["holdover_exit_block_reason"] == ""
    assert row["holdover_exit_first_failed_gate"] == "reacquire_support_obs"
    assert row["reacquire_n_aircraft"] == 0
    assert row["reacquire_support_obs_count"] == 0
    assert row["reacquire_support_icao_count"] == 0
    assert row["reacquire_support_obs_ok"] is False
    assert row["reacquire_support_icao_ok"] is False


def test_extract_compact_fallbacks_from_diagnostic_aliases_and_top_level():
    mod = _load_module()

    payload = {
        "go_operational_active": True,
        "sync_state": {
            "handoff_state": "GO_REFINED_READY",
            "handoff_reason": "go_runtime_operational",
            "go_diagnostic_go_sync_unusable_reason": "",
            "go_diagnostic_holdover_reason": "hard_residual_reject_holdover",
            "go_diagnostic_transition_quarantine_count": 15,
            "go_diagnostic_transition_quarantine_fit_excluded_count": 14,
            "go_diagnostic_transition_quarantine_hard_reject_suppressed_count": 1,
            "anchor_retained_without_current_evidence": True,
            "phase_status_display": "anchor_retained_stale",
            "phase_evidence_fresh": True,
            "phase_evidence_age_s": 1.5,
            "strict_gate_fail_reason": "quality_below_threshold",
            "slope_trend_state": "slope_not_converged",
            "go_diagnostic_fit_observation_count": 54,
            "go_diagnostic_fit_icao_count": 3,
            "go_diagnostic_fit_span_s": 115.0,
            "go_diagnostic_fit_epoch_last_observation_age_s": 0.2,
            "go_diagnostic_current_fit_epoch_age_s": 10.0,
            "last_go_evidence_event_age_s": 0.2,
            "last_eligible_burst_observation_age_s": 0.2,
            "last_sync_driving_fit_observation_age_s": 0.2,
            "phase_evidence_age_source": "phase_state_ts",
            "sync_driving_evidence_age_source": "fit_epoch_last_observation",
            "burst_rows_absence_reason": "no_display_burst_sync_rows",
            "fit_counters_source": "live_current",
        },
    }

    row = mod.extract_compact_row(payload, iid=9, ts=999.0)

    assert row["go_operational_active"] is True
    assert row["handoff_state"] == "GO_REFINED_READY"
    assert row["handoff_reason"] == "go_runtime_operational"
    assert row["go_sync_unusable_reason"] == ""
    assert row["holdover_reason"] == "hard_residual_reject_holdover"
    assert row["strict_gate_primary_fail_reason"] == "quality_below_threshold"
    assert row["transition_quarantine_count"] == 15
    assert row["transition_quarantine_fit_excluded_count"] == 14
    assert row["transition_quarantine_hard_reject_suppressed_count"] == 1
    assert row["stale_go_evidence"] is True
    assert row["phase_status_display"] == "anchor_retained_stale"
    assert row["phase_evidence_fresh"] is True
    assert row["phase_evidence_age_s"] == 1.5
    assert row["slope_not_converged_reason"] == "slope_not_converged"
    assert row["fit_observation_count"] == 54
    assert row["fit_icao_count"] == 3
    assert row["fit_span_s"] == 115.0
    assert row["fit_epoch_last_observation_age_s"] == 0.2
    assert row["current_fit_epoch_age_s"] == 10.0
    assert row["phase_evidence_age_source"] == "phase_state_ts"
    assert row["sync_driving_evidence_age_source"] == "fit_epoch_last_observation"
    assert row["burst_rows_absence_reason"] == "no_display_burst_sync_rows"
    assert row["fit_counters_source"] == "live_current"


def test_extract_compact_hysteresis_promoted_reset_and_soft_hold_rows():
    mod = _load_module()
    pending_payload = {
        "sync_state": {
            "go_operational_active": False,
            "go_operational_ready_streak": 2,
            "go_operational_promotion_threshold": 3,
            "go_operational_hysteresis_decision": "pending",
            "go_operational_blocking_gate": "go_readiness_hysteresis",
            "handoff_reason": "go_ready_pending_hysteresis",
        }
    }
    promoted_payload = {
        "sync_state": {
            "go_operational_active": True,
            "go_operational_ready_streak": 3,
            "go_operational_promotion_threshold": 3,
            "go_operational_hysteresis_decision": "promoted",
            "handoff_reason": "go_runtime_operational",
        }
    }
    reset_payload = {
        "sync_state": {
            "go_operational_active": False,
            "go_operational_ready_streak": 0,
            "go_operational_hysteresis_decision": "reset",
            "go_operational_streak_reset_reason": "strict_gate_failed",
            "go_operational_streak_reset_gate": "go_sync_state_usable",
            "go_operational_streak_reset_handoff_reason": "strict_gate_failed",
        }
    }
    soft_hold_payload = {
        "sync_state": {
            "go_operational_active": True,
            "go_operational_hysteresis_decision": "soft_hold",
            "go_operational_soft_failure_active": True,
            "go_operational_soft_failure_until_ts": 1234.5,
            "go_operational_soft_failure_remaining_s": 1.2,
            "go_operational_soft_failure_reason": "slope_not_converged",
            "handoff_reason": "go_soft_failure_holdover:slope_not_converged",
        }
    }

    pending = mod.extract_compact_row(pending_payload, iid=1, ts=1.0)
    promoted = mod.extract_compact_row(promoted_payload, iid=2, ts=2.0)
    reset = mod.extract_compact_row(reset_payload, iid=3, ts=3.0)
    soft_hold = mod.extract_compact_row(soft_hold_payload, iid=4, ts=4.0)

    assert pending["go_operational_hysteresis_decision"] == "pending"
    assert pending["go_operational_ready_streak"] == 2
    assert pending["go_operational_promotion_threshold"] == 3
    assert promoted["go_operational_hysteresis_decision"] == "promoted"
    assert promoted["go_operational_ready_streak"] >= promoted["go_operational_promotion_threshold"]
    assert reset["go_operational_hysteresis_decision"] == "reset"
    assert reset["go_operational_streak_reset_reason"] == "strict_gate_failed"
    assert reset["go_operational_streak_reset_gate"] == "go_sync_state_usable"
    assert soft_hold["go_operational_hysteresis_decision"] == "soft_hold"
    assert soft_hold["go_operational_soft_failure_active"] is True
    assert soft_hold["go_operational_soft_failure_until_ts"] == 1234.5
    assert soft_hold["go_operational_soft_failure_remaining_s"] == 1.2
    assert soft_hold["go_operational_soft_failure_reason"] == "slope_not_converged"


def test_stale_go_evidence_equivalent_freshness_blocker_is_derived():
    mod = _load_module()
    payload = {
        "sync_state": {
            "stale_go_evidence": False,
            "handoff_reason": "stale_go_evidence",
            "go_operational_blocking_gate": "go_readiness.go_evidence_fresh",
        }
    }
    row = mod.extract_compact_row(payload, iid=5, ts=1.0)
    assert row["stale_go_evidence"] is True
    assert row["stale_go_evidence_reason"] == "handoff_reason_stale_go_evidence"


def test_strict_gate_failed_strict_epoch_and_holdover_shapes():
    mod = _load_module()
    strict_payload = {
        "sync_state": {
            "handoff_reason": "strict_gate_failed",
            "go_operational_blocking_gate": "go_readiness.go_sync_state_usable",
            "strict_gate_pass": False,
            "strict_gate_primary_fail_reason": "strict_gate_failed_strict_epoch",
            "strict_gate_n_aircraft": 1,
            "strict_gate_ref_pos_age_s": 12.3,
            "last_update_epoch_n_aircraft": 1,
            "last_update_epoch_ref_icao": "ABC123",
            "last_update_epoch_ref_pos_age_s": 12.3,
            "strict_gate_ref_range_nm": 45.6,
            "last_update_epoch_residual_deg": -27.5,
            "go_diagnostic_last_update_epoch_outcome": "rejected",
            "holdover": False,
        }
    }
    holdover_payload = {
        "sync_state": {
            "handoff_reason": "go_holdover",
            "go_operational_blocking_gate": "go_readiness.go_not_holdover",
            "holdover": True,
            "holdover_reason": "insufficient_aircraft",
            "go_diagnostic_reacquire_support_obs_count": 0,
            "go_diagnostic_reacquire_support_icao_count": 0,
        }
    }

    strict_row = mod.extract_compact_row(strict_payload, iid=36, ts=2.0)
    hold_row = mod.extract_compact_row(holdover_payload, iid=76, ts=3.0)

    assert strict_row["strict_gate_pass"] is False
    assert strict_row["strict_gate_primary_fail_reason"] == "strict_gate_failed_strict_epoch"
    assert strict_row["strict_epoch_actual_n_aircraft"] == 1
    assert strict_row["strict_epoch_actual_ref_age_s"] == 12.3
    assert strict_row["last_update_epoch_n_aircraft"] == 1
    assert strict_row["last_update_epoch_ref_icao"] == "ABC123"
    assert strict_row["last_update_epoch_ref_pos_age_s"] == 12.3
    assert strict_row["last_update_epoch_ref_range_nm"] == 45.6
    assert strict_row["last_update_epoch_residual_deg"] == -27.5
    assert strict_row["last_update_epoch_abs_residual_deg"] == 27.5
    assert strict_row["last_update_epoch_outcome"] == "rejected"
    assert strict_row["holdover"] is False

    assert hold_row["go_operational_blocking_gate"] == "go_readiness.go_not_holdover"
    assert hold_row["holdover"] is True
    assert hold_row["holdover_reason"] == "insufficient_aircraft"
    assert hold_row["reacquire_support_obs_count"] == 0
    assert hold_row["reacquire_support_icao_count"] == 0
