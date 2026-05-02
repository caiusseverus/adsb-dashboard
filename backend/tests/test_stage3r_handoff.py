"""Stage 3R: Python-to-Go authority handoff — gate and state machine tests."""
from __future__ import annotations

import sys
import os
import time
from collections import deque

import pytest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

import radar.sweep as sweep
from radar.sweep import RadarState
from radar.models import RadarIID
from radar.sync_models import LiveSyncState


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _go_sync_for_gates(iid: int = 100, period_s: float = 4.0) -> dict:
    """Minimal Go sync dict with all readiness gates passing except slope trend."""
    return {
        "i": iid, "sp": True, "su": True, "sps": period_s, "sep": 1000.0, "sod": 10.0,
        "sq": 0.9, "sj": 1.0, "snf": 10, "sh": False, "lu": 2000.0, "rv": 1,
        "bps": period_s, "eps": period_s, "pag": True,
    }


def _make_state_with_python_model(iid: int, period_s: float = 4.0) -> RadarState:
    state = RadarState()
    state._models[iid] = RadarIID(iid=iid, status="SINGLE_RADAR", period_s=period_s, primary_support_count=8)
    return state


def _inject_stable_slope_history(state: RadarState, iid: int, near_zero: bool = True) -> None:
    """Inject slope history so the slope trend gate evaluates rather than returning None."""
    now = time.time()
    if near_zero:
        # 13 entries spanning 12 s, all |slope| < 0.5
        entries = [
            {"ts": now - (12 - i), "residual_slope_deg_per_s": 0.05 * (i % 3 - 1),
             "raw_slope_deg_per_s": 0.0, "slope_source": "per_aircraft_consensus"}
            for i in range(13)
        ]
    else:
        # 30 entries with flat high-error slope (no convergence)
        entries = [
            {"ts": now - (30 - i), "residual_slope_deg_per_s": 1.5 + 0.05 * (i % 3 - 1),
             "raw_slope_deg_per_s": 1.5, "slope_source": "per_aircraft_consensus"}
            for i in range(30)
        ]
    state._live_slope_history[iid] = deque(entries, maxlen=80)


# ===========================================================================
# 1. sync_authority=go_runtime requires Go-readiness gates
# ===========================================================================

def test_go_runtime_requires_go_readiness_gates(monkeypatch):
    """sync_authority=go_runtime must be impossible unless Go-readiness gates pass."""
    import config as _cfg
    monkeypatch.setattr(_cfg, "RADAR_SYNC_GO_REFINER_OPERATIONAL", True)
    state = _make_state_with_python_model(1001)
    # Go period disagrees → Go-readiness fails
    state.update_go_iid_state({
        "i": 1001, "sp": True, "su": True, "sps": 4.8, "sep": 1000.0, "sod": 10.0,
        "sq": 0.9, "sj": 1.0, "snf": 10, "sh": False, "lu": 2000.0, "rv": 1,
        "bps": 4.8, "eps": 4.8, "pag": True,
    })
    payload = sweep._live_sync_state_to_dict(state.get_live_sync_state(1001))
    assert payload["sync_authority"] != "go_runtime"
    assert payload["period_authority"] != "go_refined"


def test_go_runtime_becomes_authority_when_all_gates_pass(monkeypatch):
    """When all gates pass and flag is enabled, sync_authority becomes go_runtime."""
    import config as _cfg
    monkeypatch.setattr(_cfg, "RADAR_SYNC_GO_REFINER_OPERATIONAL", True)
    state = _make_state_with_python_model(1002)
    state.update_go_iid_state(_go_sync_for_gates(1002, 4.0))
    _inject_stable_slope_history(state, 1002, near_zero=True)
    payload = sweep._live_sync_state_to_dict(state.get_live_sync_state(1002))
    assert payload["period_authority"] == "go_refined"
    assert payload["sync_authority"] == "go_runtime"
    assert payload["handoff_state"] == "GO_REFINED_READY"


# ===========================================================================
# 2. Period authority independent of phase authority
# ===========================================================================

def test_go_period_authority_without_phase_authority(monkeypatch):
    """Go period authority can become active while phase authority is unavailable."""
    import config as _cfg
    monkeypatch.setattr(_cfg, "RADAR_SYNC_GO_REFINER_OPERATIONAL", True)
    state = _make_state_with_python_model(1010)
    state.update_go_iid_state(_go_sync_for_gates(1010, 4.0))
    _inject_stable_slope_history(state, 1010, near_zero=True)
    payload = sweep._live_sync_state_to_dict(state.get_live_sync_state(1010))
    # Period authority should be Go, but no phase anchor → phase authority not go_runtime.
    assert payload["period_authority"] == "go_refined"
    assert payload["phase_authority"] != "go_runtime"


# ===========================================================================
# 3. Phase authority requires fresh typed phase anchor
# ===========================================================================

def _make_go_sync_with_phase(iid: int, period_s: float, anchor_age_s, anchor_status: str,
                              pop_state: str, phase_status: str = "trusted") -> LiveSyncState:
    """Build a LiveSyncState with configured phase fields for gate testing."""
    return LiveSyncState(
        iid=iid,
        period_s=period_s, period_base_s=period_s,
        phase_epoch_us=1000.0, phase_offset_deg=10.0,
        sync_quality=0.9, sync_jitter_deg=1.0,
        last_sync_update_ts=time.time(),
        source="go_frame_sync", usable=True,
        phase_basis="anchor_relative",
        phase_anchor_icao="ABCDEF",
        phase_anchor_status=anchor_status,
        phase_anchor_age_s=anchor_age_s,
        phase_status=phase_status,
        population_validation_state=pop_state,
    )


def _setup_go_state_for_phase_test(state: RadarState, iid: int, period_s: float,
                                    sync: LiveSyncState) -> None:
    state.update_go_iid_state(_go_sync_for_gates(iid, period_s))
    _inject_stable_slope_history(state, iid, near_zero=True)
    with state._lock:
        state._live_sync_states[iid] = sync
    state._apply_go_handoff_state_locked(iid)


def test_phase_authority_blocked_by_stale_anchor_age(monkeypatch):
    """phase_anchor_age_s >= 8 blocks phase authority."""
    import config as _cfg
    monkeypatch.setattr(_cfg, "RADAR_SYNC_GO_REFINER_OPERATIONAL", True)
    state = _make_state_with_python_model(1020)
    sync = _make_go_sync_with_phase(1020, 4.0, anchor_age_s=15.0,
                                     anchor_status="selected", pop_state="pass")
    _setup_go_state_for_phase_test(state, 1020, 4.0, sync)
    updated = state.get_live_sync_state(1020)
    assert updated.phase_authority != "go_runtime"


def test_phase_authority_blocked_by_missing_anchor_age(monkeypatch):
    """Missing phase_anchor_age_s (None) blocks phase authority."""
    import config as _cfg
    monkeypatch.setattr(_cfg, "RADAR_SYNC_GO_REFINER_OPERATIONAL", True)
    state = _make_state_with_python_model(1021)
    sync = _make_go_sync_with_phase(1021, 4.0, anchor_age_s=None,
                                     anchor_status="selected", pop_state="pass")
    _setup_go_state_for_phase_test(state, 1021, 4.0, sync)
    updated = state.get_live_sync_state(1021)
    assert updated.phase_authority != "go_runtime"


def test_phase_authority_blocked_by_population_fail(monkeypatch):
    """population_validation_state=fail blocks phase authority."""
    import config as _cfg
    monkeypatch.setattr(_cfg, "RADAR_SYNC_GO_REFINER_OPERATIONAL", True)
    state = _make_state_with_python_model(1022)
    sync = _make_go_sync_with_phase(1022, 4.0, anchor_age_s=2.0,
                                     anchor_status="selected", pop_state="fail")
    _setup_go_state_for_phase_test(state, 1022, 4.0, sync)
    updated = state.get_live_sync_state(1022)
    assert updated.phase_authority != "go_runtime"


def test_phase_authority_blocked_by_population_demoted_anchor(monkeypatch):
    """phase_anchor_status=population_demoted blocks phase authority."""
    import config as _cfg
    monkeypatch.setattr(_cfg, "RADAR_SYNC_GO_REFINER_OPERATIONAL", True)
    state = _make_state_with_python_model(1023)
    sync = _make_go_sync_with_phase(1023, 4.0, anchor_age_s=2.0,
                                     anchor_status="population_demoted", pop_state="pass")
    _setup_go_state_for_phase_test(state, 1023, 4.0, sync)
    updated = state.get_live_sync_state(1023)
    assert updated.phase_authority != "go_runtime"


def test_phase_authority_blocked_by_stale_anchor_status(monkeypatch):
    """phase_anchor_status=stale_anchor blocks phase authority."""
    import config as _cfg
    monkeypatch.setattr(_cfg, "RADAR_SYNC_GO_REFINER_OPERATIONAL", True)
    state = _make_state_with_python_model(1024)
    sync = _make_go_sync_with_phase(1024, 4.0, anchor_age_s=2.0,
                                     anchor_status="stale_anchor", pop_state="pass")
    _setup_go_state_for_phase_test(state, 1024, 4.0, sync)
    updated = state.get_live_sync_state(1024)
    assert updated.phase_authority != "go_runtime"


def test_phase_authority_blocked_by_no_anchor_status(monkeypatch):
    """phase_anchor_status=no_anchor blocks phase authority."""
    import config as _cfg
    monkeypatch.setattr(_cfg, "RADAR_SYNC_GO_REFINER_OPERATIONAL", True)
    state = _make_state_with_python_model(1025)
    sync = _make_go_sync_with_phase(1025, 4.0, anchor_age_s=2.0,
                                     anchor_status="no_anchor", pop_state="pass")
    _setup_go_state_for_phase_test(state, 1025, 4.0, sync)
    updated = state.get_live_sync_state(1025)
    assert updated.phase_authority != "go_runtime"


# ===========================================================================
# 4. Period stability gate
# ===========================================================================

def test_period_stability_gate_passes_with_stable_history():
    """Period stability gate passes when stdev < 1% of mean period."""
    state = RadarState()
    iid = 2001
    base_p = 4.0
    # 10 samples with tiny jitter (0.1% of period, well within 1% threshold)
    stable_values = [base_p + 0.0001 * (i % 3 - 1) for i in range(10)]
    state._live_period_history[iid] = deque(
        [{"ts": float(i), "period_base_s": v, "period_s": v, "period_correction_ppm": 0.0}
         for i, v in enumerate(stable_values)],
        maxlen=80,
    )
    result = state._evaluate_period_stability_gate_locked(iid)
    assert result["passed"] is True


def test_period_stability_gate_fails_with_high_variance():
    """Period stability gate fails when stdev >= 1% of mean period."""
    state = RadarState()
    iid = 2002
    # Large spread: alternating 3.0 and 5.0 — stdev >> 1% of mean 4.0
    noisy_values = [3.0, 5.0, 3.0, 5.0, 3.0]
    state._live_period_history[iid] = deque(
        [{"ts": float(i), "period_base_s": v, "period_s": v, "period_correction_ppm": 0.0}
         for i, v in enumerate(noisy_values)],
        maxlen=80,
    )
    result = state._evaluate_period_stability_gate_locked(iid)
    assert result["passed"] is False
    assert result["reason"] == "stdev_too_high"


def test_period_stability_gate_emits_insufficient_history_not_not_evaluated():
    """With fewer than 3 samples, gate returns insufficient_history, not not_evaluated."""
    state = RadarState()
    iid = 2003
    state._live_period_history[iid] = deque(
        [{"ts": 0.0, "period_base_s": 4.0, "period_s": 4.0, "period_correction_ppm": 0.0}],
        maxlen=80,
    )
    result = state._evaluate_period_stability_gate_locked(iid)
    assert result["reason"] != "not_evaluated"
    assert result["reason"] == "insufficient_history"
    assert result["passed"] is not False  # non-blocking


def test_period_stability_gate_insufficient_when_no_history():
    """Empty history → insufficient_history (non-blocking)."""
    state = RadarState()
    result = state._evaluate_period_stability_gate_locked(9999)
    assert result["reason"] == "insufficient_history"
    assert result["passed"] is not False


# ===========================================================================
# 5. Pre-Stage-8 contamination stub
# ===========================================================================

def test_contamination_stub_not_not_evaluated():
    """Pre-Stage-8 stub must emit single_family or insufficient_data, not not_evaluated."""
    state = _make_state_with_python_model(3001)
    state.update_go_iid_state(_go_sync_for_gates(3001, 4.0))
    go_gates = state._evaluate_go_readiness_gates_locked(3001, 4.0)
    contamination_gate = go_gates["gates"].get("go_contamination_state", {})
    assert contamination_gate.get("reason") != "not_evaluated"
    assert contamination_gate.get("reason") in ("single_family", "insufficient_data")


def test_contamination_stub_nonblocking():
    """Pre-Stage-8 contamination stub must not block Go readiness (passed is not False)."""
    state = _make_state_with_python_model(3002)
    state.update_go_iid_state(_go_sync_for_gates(3002, 4.0))
    go_gates = state._evaluate_go_readiness_gates_locked(3002, 4.0)
    contamination_gate = go_gates["gates"].get("go_contamination_state", {})
    assert contamination_gate.get("passed") is not False


def test_contamination_stub_emits_insufficient_data_when_go_absent():
    """When Go sync absent, contamination stub emits insufficient_data."""
    state = RadarState()
    go_gates = state._evaluate_go_readiness_gates_locked(3003, 4.0)
    contamination_gate = go_gates["gates"].get("go_contamination_state", {})
    assert contamination_gate.get("reason") == "insufficient_data"


# ===========================================================================
# 6. Slope-EMA trend gate
# ===========================================================================

def test_slope_ema_near_zero_sustained_passes():
    """abs(slope_ema) < 0.5 for >= 10 s window → passes with near_zero_sustained reason."""
    state = RadarState()
    iid = 4001
    now = time.time()
    # 11 entries from now-9.7 to now-0.7 (step ~0.9 s), all near-zero.
    # Oldest entry is at now-9.7, which is clearly inside the 10 s window and
    # satisfies the >= 9.5 s span check regardless of sub-millisecond timing drift.
    entries = [
        {"ts": now - 9.7 + i * 0.9, "residual_slope_deg_per_s": 0.05 * (i % 3 - 1),
         "raw_slope_deg_per_s": 0.0, "slope_source": "per_aircraft_consensus"}
        for i in range(11)
    ]
    state._live_slope_history[iid] = deque(entries, maxlen=80)
    result = state._evaluate_slope_trend_gate_locked(iid)
    assert result["passed"] is True
    assert "near_zero" in (result["reason"] or "")


def test_slope_ema_decreasing_trend_passes():
    """Decreasing abs(slope_ema) over 30 s with R² >= 0.5 passes."""
    state = RadarState()
    iid = 4002
    now = time.time()
    # Perfect linear decrease: 2.0 → 0.2 over 30 entries
    entries = [
        {"ts": now - (30 - i),
         "residual_slope_deg_per_s": 2.0 - (1.8 * i / 29),
         "raw_slope_deg_per_s": 0.0, "slope_source": "per_aircraft_consensus"}
        for i in range(30)
    ]
    state._live_slope_history[iid] = deque(entries, maxlen=80)
    result = state._evaluate_slope_trend_gate_locked(iid)
    assert result["passed"] is True
    assert result["reason"] == "slope_magnitude_decreasing"


def test_slope_ema_flat_high_error_fails():
    """Flat high-error slope (no convergence) fails the trend gate."""
    state = RadarState()
    iid = 4003
    now = time.time()
    entries = [
        {"ts": now - (30 - i), "residual_slope_deg_per_s": 1.5 + 0.05 * (i % 3 - 1),
         "raw_slope_deg_per_s": 1.5, "slope_source": "per_aircraft_consensus"}
        for i in range(30)
    ]
    state._live_slope_history[iid] = deque(entries, maxlen=80)
    result = state._evaluate_slope_trend_gate_locked(iid)
    assert result["passed"] is False
    assert result["reason"] == "slope_not_converged"


def test_slope_trend_gate_nonblocking_when_no_history():
    """Empty slope history returns non-blocking None (not False)."""
    state = RadarState()
    result = state._evaluate_slope_trend_gate_locked(9999)
    assert result["passed"] is not False
    assert result["reason"] == "insufficient_slope_history"


# ===========================================================================
# 7. Handoff state coverage
# ===========================================================================

def test_handoff_state_bootstrapping_py_no_python_base(monkeypatch):
    """BOOTSTRAPPING_PY when no Python base period exists."""
    import config as _cfg
    monkeypatch.setattr(_cfg, "RADAR_SYNC_GO_REFINER_OPERATIONAL", True)
    state = RadarState()
    state.update_go_iid_state(_go_sync_for_gates(5001, 4.0))
    payload = sweep._live_sync_state_to_dict(state.get_live_sync_state(5001))
    assert payload["handoff_state"] == "BOOTSTRAPPING_PY"
    assert payload["period_authority"] == "py_bootstrap"


def test_handoff_state_base_period_ready_when_go_absent(monkeypatch):
    """BASE_PERIOD_READY when Python base valid but Go sync not yet present."""
    import config as _cfg
    monkeypatch.setattr(_cfg, "RADAR_SYNC_GO_REFINER_OPERATIONAL", True)
    state = _make_state_with_python_model(5002)
    # Manually set a Python-sourced sync state (Go sync not provided)
    from radar.sync_models import LiveSyncState
    state._live_sync_states[5002] = LiveSyncState(
        iid=5002, period_s=4.0, period_base_s=4.0,
        phase_epoch_us=1000.0, phase_offset_deg=10.0,
        sync_quality=0.9, sync_jitter_deg=1.0,
        last_sync_update_ts=time.time(),
        source="go_frame_sync", usable=False,
    )
    state._apply_go_handoff_state_locked(5002)
    sync = state.get_live_sync_state(5002)
    assert sync.handoff_state == "BASE_PERIOD_READY"
    assert sync.handoff_reason == "go_sync_absent"


def test_handoff_state_holdover_when_go_in_holdover(monkeypatch):
    """HOLDOVER emitted when Python base valid but Go sync has holdover=True."""
    import config as _cfg
    monkeypatch.setattr(_cfg, "RADAR_SYNC_GO_REFINER_OPERATIONAL", True)
    state = _make_state_with_python_model(5003)
    state.update_go_iid_state({
        "i": 5003, "sp": True, "su": True, "sps": 4.0, "sep": 1000.0, "sod": 10.0,
        "sq": 0.9, "sj": 1.0, "snf": 10,
        "sh": True,  # holdover
        "lu": 2000.0, "rv": 1, "bps": 4.0, "eps": 4.0, "pag": True,
    })
    payload = sweep._live_sync_state_to_dict(state.get_live_sync_state(5003))
    assert payload["handoff_state"] == "HOLDOVER"
    assert payload["period_authority"] == "holdover"
    assert payload["sync_authority"] == "holdover"


def test_handoff_state_untrusted_when_base_disagrees(monkeypatch):
    """UNTRUSTED when Go has a valid base period that disagrees with Python."""
    import config as _cfg
    monkeypatch.setattr(_cfg, "RADAR_SYNC_GO_REFINER_OPERATIONAL", True)
    state = _make_state_with_python_model(5004)
    # Go base = 4.8, Python base = 4.0 → disagrees by 0.8s > tolerance 0.15s
    state.update_go_iid_state({
        "i": 5004, "sp": True, "su": True, "sps": 4.8, "sep": 1000.0, "sod": 10.0,
        "sq": 0.9, "sj": 1.0, "snf": 10, "sh": False, "lu": 2000.0, "rv": 1,
        "bps": 4.8, "eps": 4.8, "pag": True,
    })
    payload = sweep._live_sync_state_to_dict(state.get_live_sync_state(5004))
    assert payload["handoff_state"] == "UNTRUSTED"
    assert payload["sync_authority"] != "go_runtime"
    assert payload["period_authority"] != "go_refined"


def test_handoff_state_go_refining_when_base_accepted_but_frames_low(monkeypatch):
    """GO_REFINING when Go base agrees with Python but n_sync_frames < 3."""
    import config as _cfg
    monkeypatch.setattr(_cfg, "RADAR_SYNC_GO_REFINER_OPERATIONAL", True)
    state = _make_state_with_python_model(5005)
    state.update_go_iid_state({
        "i": 5005, "sp": True, "su": True, "sps": 4.0, "sep": 1000.0, "sod": 10.0,
        "sq": 0.9, "sj": 1.0,
        "snf": 2,  # only 2 frames, < 3 required
        "sh": False, "lu": 2000.0, "rv": 1,
        "bps": 4.0, "eps": 4.0, "pag": True,
    })
    payload = sweep._live_sync_state_to_dict(state.get_live_sync_state(5005))
    assert payload["handoff_state"] == "GO_REFINING"
    assert payload["period_authority"] == "py_base"
    assert payload["sync_authority"] != "go_runtime"


def test_handoff_state_go_refined_ready_flag_disabled(monkeypatch):
    """GO_REFINED_READY (diagnostic) when all gates pass but flag is disabled."""
    import config as _cfg
    monkeypatch.setattr(_cfg, "RADAR_SYNC_GO_REFINER_OPERATIONAL", False)
    state = _make_state_with_python_model(5006)
    state.update_go_iid_state(_go_sync_for_gates(5006, 4.0))
    payload = sweep._live_sync_state_to_dict(state.get_live_sync_state(5006))
    assert payload["handoff_state"] == "GO_REFINED_READY"
    assert payload["handoff_reason"] == "go_ready_flag_disabled"
    assert payload["period_authority"] == "py_base"


def test_handoff_state_go_refined_ready_operational(monkeypatch):
    """GO_REFINED_READY with period_authority=go_refined when flag enabled and gates pass."""
    import config as _cfg
    monkeypatch.setattr(_cfg, "RADAR_SYNC_GO_REFINER_OPERATIONAL", True)
    state = _make_state_with_python_model(5007)
    state.update_go_iid_state(_go_sync_for_gates(5007, 4.0))
    _inject_stable_slope_history(state, 5007, near_zero=True)
    payload = sweep._live_sync_state_to_dict(state.get_live_sync_state(5007))
    assert payload["handoff_state"] == "GO_REFINED_READY"
    assert payload["period_authority"] == "go_refined"
    assert payload["sync_authority"] == "go_runtime"
    assert payload["handoff_reason"] == "go_ready"


# ===========================================================================
# 8. Snapshot diagnostic fields
# ===========================================================================

def test_snapshot_exposes_period_stability_state(monkeypatch):
    """Snapshot must include period_stability_state field."""
    import config as _cfg
    monkeypatch.setattr(_cfg, "RADAR_SYNC_GO_REFINER_OPERATIONAL", False)
    state = _make_state_with_python_model(6001)
    state.update_go_iid_state(_go_sync_for_gates(6001, 4.0))
    payload = sweep._live_sync_state_to_dict(state.get_live_sync_state(6001))
    assert "period_stability_state" in payload


def test_snapshot_exposes_contamination_state(monkeypatch):
    """Snapshot must include contamination_state field, not not_evaluated."""
    import config as _cfg
    monkeypatch.setattr(_cfg, "RADAR_SYNC_GO_REFINER_OPERATIONAL", False)
    state = _make_state_with_python_model(6002)
    state.update_go_iid_state(_go_sync_for_gates(6002, 4.0))
    payload = sweep._live_sync_state_to_dict(state.get_live_sync_state(6002))
    assert "contamination_state" in payload
    assert payload["contamination_state"] != "not_evaluated"
    assert payload["contamination_state"] in ("single_family", "insufficient_data")


def test_snapshot_exposes_slope_trend_state(monkeypatch):
    """Snapshot must include slope_trend_state field."""
    import config as _cfg
    monkeypatch.setattr(_cfg, "RADAR_SYNC_GO_REFINER_OPERATIONAL", False)
    state = _make_state_with_python_model(6003)
    state.update_go_iid_state(_go_sync_for_gates(6003, 4.0))
    payload = sweep._live_sync_state_to_dict(state.get_live_sync_state(6003))
    assert "slope_trend_state" in payload


def test_snapshot_exposes_population_validation_state(monkeypatch):
    """population_validation_state and reason must appear in the snapshot payload."""
    import config as _cfg
    monkeypatch.setattr(_cfg, "RADAR_SYNC_GO_REFINER_OPERATIONAL", False)
    sync = LiveSyncState(
        iid=6004,
        period_s=4.0, period_base_s=4.0,
        phase_epoch_us=1000.0, phase_offset_deg=10.0,
        sync_quality=0.9, sync_jitter_deg=1.0,
        last_sync_update_ts=time.time(),
        source="multi_aircraft_burst", usable=True,
        population_validation_state="pass",
        population_validation_reason="anchor_consistent_with_population",
    )
    payload = sweep._live_sync_state_to_dict(sync)
    assert payload.get("population_validation_state") == "pass"
    assert payload.get("population_validation_reason") == "anchor_consistent_with_population"


# ===========================================================================
# 9. Stage 6R / 7R regression guard
# ===========================================================================

def test_stage6r_typed_phase_fields_still_in_snapshot(monkeypatch):
    """Stage 6R typed phase fields must remain present after Stage 3R changes."""
    import config as _cfg
    monkeypatch.setattr(_cfg, "RADAR_SYNC_GO_REFINER_OPERATIONAL", False)
    state = _make_state_with_python_model(7001)
    state.update_go_iid_state(_go_sync_for_gates(7001, 4.0))
    payload = sweep._live_sync_state_to_dict(state.get_live_sync_state(7001))
    for field in ("phase_basis", "phase_is_absolute", "phase_absolute_available",
                  "phase_trust_reason", "phase_anchor_age_s"):
        assert field in payload, f"Stage 6R field missing: {field}"


def test_stage7r_population_validation_fields_present(monkeypatch):
    """Stage 7R population_validation_state and reason must flow through snapshot."""
    import config as _cfg
    monkeypatch.setattr(_cfg, "RADAR_SYNC_GO_REFINER_OPERATIONAL", False)
    sync = LiveSyncState(
        iid=7002,
        period_s=4.0, period_base_s=4.0,
        phase_epoch_us=1000.0, phase_offset_deg=10.0,
        sync_quality=0.9, sync_jitter_deg=1.0,
        last_sync_update_ts=time.time(),
        source="multi_aircraft_burst", usable=True,
        population_validation_state="fail",
        population_validation_reason="population_disagrees",
    )
    payload = sweep._live_sync_state_to_dict(sync)
    assert payload.get("population_validation_state") == "fail"
    assert payload.get("population_validation_reason") == "population_disagrees"


# ===========================================================================
# 10. RADAR_SYNC_GO_REFINER_OPERATIONAL stays default-off
# ===========================================================================

def test_go_refiner_operational_default_off():
    """RADAR_SYNC_GO_REFINER_OPERATIONAL must default to False."""
    import config as _cfg
    assert getattr(_cfg, "RADAR_SYNC_GO_REFINER_OPERATIONAL", False) is False
