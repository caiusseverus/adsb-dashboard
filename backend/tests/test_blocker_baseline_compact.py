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
        }
    }

    row = mod.extract_compact_row(payload, iid=1, ts=123.0)

    assert row["go_operational_enabled"] is True
    assert row["go_operational_active"] is False
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
