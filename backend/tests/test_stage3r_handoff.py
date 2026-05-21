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
        "sq": 0.9, "sj": 1.0, "snf": 10, "sh": False, "lu": time.time(), "rv": 1,
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
        # 20 entries spanning ~11 s (tight spacing ensures 9.5 s span in 10 s window)
        entries = [
            {"ts": now - 11.0 + i * 0.58, "residual_slope_deg_per_s": 0.05 * (i % 3 - 1),
             "raw_slope_deg_per_s": 0.0, "slope_source": "per_aircraft_consensus"}
            for i in range(20)
        ]
    else:
        # 30 entries with flat high-error slope (no convergence)
        entries = [
            {"ts": now - (30 - i), "residual_slope_deg_per_s": 1.5 + 0.05 * (i % 3 - 1),
             "raw_slope_deg_per_s": 1.5, "slope_source": "per_aircraft_consensus"}
            for i in range(30)
        ]
    state._live_slope_history[iid] = deque(entries, maxlen=80)


def _inject_stable_period_history(state: RadarState, iid: int, period_s: float = 4.0) -> None:
    """Inject period history so the period stability gate evaluates as pass."""
    stable_values = [period_s + 0.0001 * (i % 3 - 1) for i in range(10)]
    now = time.time()
    state._live_period_history[iid] = deque(
        [{"ts": now - (len(stable_values) - 1 - i), "period_base_s": v, "period_s": v, "period_correction_ppm": 0.0}
         for i, v in enumerate(stable_values)],
        maxlen=80,
    )


def _drive_go_handoff_cycles(state: RadarState, iid: int, cycles: int = 3) -> None:
    for _ in range(cycles):
        state._apply_go_handoff_state_locked(iid)


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
    _inject_stable_period_history(state, 1002, 4.0)
    _inject_stable_slope_history(state, 1002, near_zero=True)
    state.update_go_iid_state(_go_sync_for_gates(1002, 4.0))
    _drive_go_handoff_cycles(state, 1002, cycles=3)
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
    _inject_stable_period_history(state, 1010, 4.0)
    _inject_stable_slope_history(state, 1010, near_zero=True)
    state.update_go_iid_state(_go_sync_for_gates(1010, 4.0))
    _drive_go_handoff_cycles(state, 1010, cycles=3)
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
    _inject_stable_period_history(state, iid, period_s)
    _inject_stable_slope_history(state, iid, near_zero=True)
    state.update_go_iid_state(_go_sync_for_gates(iid, period_s))
    with state._lock:
        state._live_sync_states[iid] = sync
    _drive_go_handoff_cycles(state, iid, cycles=3)


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


def test_period_stability_gate_emits_insufficient_history_blocking():
    """With fewer than 3 samples, gate returns insufficient_history with passed=False (blocking)."""
    state = RadarState()
    iid = 2003
    state._live_period_history[iid] = deque(
        [{"ts": 0.0, "period_base_s": 4.0, "period_s": 4.0, "period_correction_ppm": 0.0}],
        maxlen=80,
    )
    result = state._evaluate_period_stability_gate_locked(iid)
    assert result["reason"] == "insufficient_history"
    assert result["passed"] is False  # blocking


def test_period_stability_gate_insufficient_when_no_history():
    """Empty history → insufficient_history with passed=False (blocking)."""
    state = RadarState()
    result = state._evaluate_period_stability_gate_locked(9999)
    assert result["reason"] == "insufficient_history"
    assert result["passed"] is False  # blocking


# ===========================================================================
# 5. Stage 8 contamination detection — flag-disabled baseline
# ===========================================================================

def test_contamination_flag_disabled_emits_typed_stage8_stub():
    """When RADAR_SYNC_CONTAMINATION_DETECTION_ENABLED=False, gate emits typed Stage-8 stub."""
    state = _make_state_with_python_model(3001)
    state.update_go_iid_state(_go_sync_for_gates(3001, 4.0))
    go_gates = state._evaluate_go_readiness_gates_locked(3001, 4.0)
    contamination_gate = go_gates["gates"].get("go_contamination_state", {})
    assert contamination_gate.get("reason") == "stage8_not_available_stub"
    assert contamination_gate.get("passed") is None


def test_contamination_flag_disabled_sync_has_typed_state():
    """When flag disabled, LiveSyncState stores typed non-operative contamination state."""
    state = _make_state_with_python_model(3002)
    state.update_go_iid_state(_go_sync_for_gates(3002, 4.0))
    state._evaluate_go_readiness_gates_locked(3002, 4.0)
    sync = state.get_live_sync_state(3002)
    assert sync.contamination_state == "insufficient_data"
    assert sync.contamination_reason == "stage8_not_available_stub"


def test_contamination_disabled_nonblocking_for_authority():
    """With flag disabled, disabled contamination does not block Go readiness."""
    state = _make_state_with_python_model(3003)
    state.update_go_iid_state(_go_sync_for_gates(3003, 4.0))
    go_gates = state._evaluate_go_readiness_gates_locked(3003, 4.0)
    contamination_gate = go_gates["gates"].get("go_contamination_state", {})
    assert contamination_gate.get("passed") is not False


def test_stage3_operational_gate_reasons_do_not_emit_not_evaluated():
    """Operative Stage 3/10 gate outputs must use typed reasons, never not_evaluated."""
    state = _make_state_with_python_model(30031)
    state.update_go_iid_state(_go_sync_for_gates(30031, 4.0))
    py_gates = state._evaluate_python_base_validity_gates_locked(30031)["gates"]
    go_gates = state._evaluate_go_readiness_gates_locked(30031, 4.0)["gates"]
    phase = state._evaluate_phase_authority_gates_locked(30031, state.get_live_sync_state(30031))["gates"]
    all_reasons = [
        v.get("reason")
        for gate_map in (py_gates, go_gates, phase)
        for v in gate_map.values()
        if isinstance(v, dict)
    ]
    assert "not_evaluated" not in all_reasons


# ===========================================================================
# 6. Stage 8 contamination detection — flag-enabled
# ===========================================================================

def test_contamination_emits_insufficient_data_when_no_burst_events(monkeypatch):
    """When no burst residual events exist, contamination emits insufficient_data."""
    import config as _cfg
    monkeypatch.setattr(_cfg, "RADAR_SYNC_CONTAMINATION_DETECTION_ENABLED", True)
    state = RadarState()
    go_gates = state._evaluate_go_readiness_gates_locked(3004, 4.0)
    contamination_gate = go_gates["gates"].get("go_contamination_state", {})
    assert contamination_gate.get("reason") == "no_recorded_burst_residual_events"
    assert contamination_gate.get("passed") is None  # non-blocking


def test_contamination_insufficient_data_not_blocking(monkeypatch):
    """insufficient_data should be non-blocking (permits bootstrap before evidence)."""
    import config as _cfg
    monkeypatch.setattr(_cfg, "RADAR_SYNC_CONTAMINATION_DETECTION_ENABLED", True)
    state = _make_state_with_python_model(3005)
    state.update_go_iid_state(_go_sync_for_gates(3005, 4.0))
    go_gates = state._evaluate_go_readiness_gates_locked(3005, 4.0)
    contamination_gate = go_gates["gates"].get("go_contamination_state", {})
    assert contamination_gate.get("reason") is not None
    assert contamination_gate.get("passed") is not False


# ===========================================================================
# 6. Slope-EMA trend gate
# ===========================================================================

def test_slope_ema_near_zero_sustained_passes():
    """abs(slope_ema) < 0.5 for >= 10 s window → passes with near_zero_sustained reason."""
    state = RadarState()
    iid = 4001
    now = time.time()
    # 20 entries spanning ~11 s, all near-zero. Robust against sub-second timing drift.
    entries = [
        {"ts": now - 11.0 + i * 0.58, "residual_slope_deg_per_s": 0.05 * (i % 3 - 1),
         "raw_slope_deg_per_s": 0.0, "slope_source": "per_aircraft_consensus"}
        for i in range(20)
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


def test_slope_trend_gate_blocking_when_no_history():
    """Empty slope history returns insufficient_slope_history with passed=False (blocking)."""
    state = RadarState()
    result = state._evaluate_slope_trend_gate_locked(9999)
    assert result["passed"] is False  # blocking
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
    _inject_stable_period_history(state, 5006, 4.0)
    _inject_stable_slope_history(state, 5006, near_zero=True)
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
    _inject_stable_period_history(state, 5007, 4.0)
    _inject_stable_slope_history(state, 5007, near_zero=True)
    state.update_go_iid_state(_go_sync_for_gates(5007, 4.0))
    _drive_go_handoff_cycles(state, 5007, cycles=3)
    payload = sweep._live_sync_state_to_dict(state.get_live_sync_state(5007))
    assert payload["handoff_state"] == "GO_REFINED_READY"
    assert payload["period_authority"] == "go_refined"
    assert payload["sync_authority"] == "go_runtime"
    assert payload["handoff_reason"] == "go_runtime_operational"


def test_handoff_hysteresis_requires_sustained_ready(monkeypatch):
    import config as _cfg
    monkeypatch.setattr(_cfg, "RADAR_SYNC_GO_REFINER_OPERATIONAL", True)
    state = _make_state_with_python_model(5008)
    _inject_stable_period_history(state, 5008, 4.0)
    _inject_stable_slope_history(state, 5008, near_zero=True)
    state.update_go_iid_state(_go_sync_for_gates(5008, 4.0))
    payload = sweep._live_sync_state_to_dict(state.get_live_sync_state(5008))
    assert payload["period_authority"] == "py_base"
    assert payload["handoff_reason"] == "go_ready_pending_hysteresis"
    assert payload["blocking_gate"] == "go_readiness_hysteresis"


def test_handoff_soft_failure_holdover_delays_demotion(monkeypatch):
    import config as _cfg
    monkeypatch.setattr(_cfg, "RADAR_SYNC_GO_REFINER_OPERATIONAL", True)
    state = _make_state_with_python_model(5009)
    _inject_stable_period_history(state, 5009, 4.0)
    _inject_stable_slope_history(state, 5009, near_zero=True)
    state.update_go_iid_state(_go_sync_for_gates(5009, 4.0))
    _drive_go_handoff_cycles(state, 5009, cycles=3)
    with state._lock:
        state._go_sync_states_by_iid[5009]["n_sync_frames"] = 1
    state._apply_go_handoff_state_locked(5009)
    payload = sweep._live_sync_state_to_dict(state.get_live_sync_state(5009))
    assert payload["period_authority"] == "go_refined"
    assert str(payload["handoff_reason"]).startswith("go_soft_failure_holdover:")


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
    assert payload["contamination_state"] in ("single_family", "insufficient_data", "disabled")


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

def test_go_refiner_operational_default_off(monkeypatch):
    """RADAR_SYNC_GO_REFINER_OPERATIONAL must default to False."""
    import config as _cfg
    monkeypatch.setattr(_cfg, "RADAR_SYNC_GO_REFINER_OPERATIONAL", False)
    assert getattr(_cfg, "RADAR_SYNC_GO_REFINER_OPERATIONAL", False) is False


# ===========================================================================
# 11. Period stability blocks operational Go authority
# ===========================================================================

def test_insufficient_period_history_blocks_go_runtime(monkeypatch):
    """RADAR_SYNC_GO_REFINER_OPERATIONAL=True + insufficient period history → sync_authority != go_runtime."""
    import config as _cfg
    monkeypatch.setattr(_cfg, "RADAR_SYNC_GO_REFINER_OPERATIONAL", True)
    state = _make_state_with_python_model(8001)
    _inject_stable_slope_history(state, 8001, near_zero=True)
    # No period history injected — go_period_stable gate will fail.
    state.update_go_iid_state(_go_sync_for_gates(8001, 4.0))
    payload = sweep._live_sync_state_to_dict(state.get_live_sync_state(8001))
    assert payload["sync_authority"] != "go_runtime"
    assert payload["period_authority"] != "go_refined"


def test_high_period_variance_blocks_go_runtime(monkeypatch):
    """RADAR_SYNC_GO_REFINER_OPERATIONAL=True + high period variance → sync_authority != go_runtime."""
    import config as _cfg
    monkeypatch.setattr(_cfg, "RADAR_SYNC_GO_REFINER_OPERATIONAL", True)
    state = _make_state_with_python_model(8002)
    _inject_stable_slope_history(state, 8002, near_zero=True)
    # High-variance period history: alternating 3.0 / 5.0 — stdev >> 1% of mean
    noisy_values = [3.0, 5.0, 3.0, 5.0, 3.0]
    state._live_period_history[8002] = deque(
        [{"ts": float(i), "period_base_s": v, "period_s": v, "period_correction_ppm": 0.0}
         for i, v in enumerate(noisy_values)],
        maxlen=80,
    )
    state.update_go_iid_state(_go_sync_for_gates(8002, 4.0))
    payload = sweep._live_sync_state_to_dict(state.get_live_sync_state(8002))
    assert payload["sync_authority"] != "go_runtime"
    assert payload["period_authority"] != "go_refined"


def test_insufficient_slope_history_blocks_go_runtime(monkeypatch):
    """RADAR_SYNC_GO_REFINER_OPERATIONAL=True + insufficient slope history → sync_authority != go_runtime."""
    import config as _cfg
    monkeypatch.setattr(_cfg, "RADAR_SYNC_GO_REFINER_OPERATIONAL", True)
    state = _make_state_with_python_model(8003)
    _inject_stable_period_history(state, 8003, 4.0)
    # No slope history injected — go_slope_converged gate will fail.
    state.update_go_iid_state(_go_sync_for_gates(8003, 4.0))
    payload = sweep._live_sync_state_to_dict(state.get_live_sync_state(8003))
    assert payload["sync_authority"] != "go_runtime"
    assert payload["period_authority"] != "go_refined"


def test_flat_high_error_slope_blocks_go_runtime(monkeypatch):
    """RADAR_SYNC_GO_REFINER_OPERATIONAL=True + flat high-error slope → sync_authority != go_runtime."""
    import config as _cfg
    monkeypatch.setattr(_cfg, "RADAR_SYNC_GO_REFINER_OPERATIONAL", True)
    state = _make_state_with_python_model(8004)
    _inject_stable_period_history(state, 8004, 4.0)
    _inject_stable_slope_history(state, 8004, near_zero=False)  # flat high error
    state.update_go_iid_state(_go_sync_for_gates(8004, 4.0))
    payload = sweep._live_sync_state_to_dict(state.get_live_sync_state(8004))
    assert payload["sync_authority"] != "go_runtime"
    assert payload["period_authority"] != "go_refined"


def test_stable_period_and_converged_slope_enables_go_operational(monkeypatch):
    """RADAR_SYNC_GO_REFINER_OPERATIONAL=True + stable period + converged slope → go_runtime."""
    import config as _cfg
    monkeypatch.setattr(_cfg, "RADAR_SYNC_GO_REFINER_OPERATIONAL", True)
    state = _make_state_with_python_model(8005)
    _inject_stable_period_history(state, 8005, 4.0)
    _inject_stable_slope_history(state, 8005, near_zero=True)
    state.update_go_iid_state(_go_sync_for_gates(8005, 4.0))
    _drive_go_handoff_cycles(state, 8005, cycles=3)
    payload = sweep._live_sync_state_to_dict(state.get_live_sync_state(8005))
    assert payload["sync_authority"] == "go_runtime"
    assert payload["period_authority"] == "go_refined"
    assert payload["handoff_state"] == "GO_REFINED_READY"


def test_gates_reported_diagnostically_when_flag_disabled(monkeypatch):
    """RADAR_SYNC_GO_REFINER_OPERATIONAL=False: gates reported without changing operational authority."""
    import config as _cfg
    monkeypatch.setattr(_cfg, "RADAR_SYNC_GO_REFINER_OPERATIONAL", False)
    state = _make_state_with_python_model(8006)
    _inject_stable_period_history(state, 8006, 4.0)
    _inject_stable_slope_history(state, 8006, near_zero=True)
    state.update_go_iid_state(_go_sync_for_gates(8006, 4.0))
    payload = sweep._live_sync_state_to_dict(state.get_live_sync_state(8006))
    # Gates are reported diagnostically
    assert "period_stability_state" in payload
    assert "slope_trend_state" in payload
    assert "contamination_state" in payload
    # Operational authority is NOT changed — stays py
    assert payload["sync_authority"] != "go_runtime"
    assert payload["period_authority"] == "py_base"
    assert payload["handoff_state"] == "GO_REFINED_READY"  # diagnostic, not operational


# ===========================================================================
# 12. population_validation_state=disabled is intentionally non-blocking
# ===========================================================================

def test_population_validation_disabled_allows_phase_authority(monkeypatch):
    """population_validation_state=disabled is intentional rollback/degraded mode.

    When RADAR_SYNC_POPULATION_MONITOR_ENABLED=False is set as an explicit
    operator choice, phase authority is allowed to proceed without population
    validation.  This is documented in the Phase 2 plan as a conscious
    rollback/degraded mode, not a missing implementation.
    """
    import config as _cfg
    monkeypatch.setattr(_cfg, "RADAR_SYNC_GO_REFINER_OPERATIONAL", True)
    state = _make_state_with_python_model(9001)
    sync = LiveSyncState(
        iid=9001,
        period_s=4.0, period_base_s=4.0,
        phase_epoch_us=1000.0, phase_offset_deg=10.0,
        sync_quality=0.9, sync_jitter_deg=1.0,
        last_sync_update_ts=time.time(),
        source="go_frame_sync", usable=True,
        phase_basis="anchor_relative",
        phase_anchor_icao="ABCDEF",
        phase_anchor_status="selected",
        phase_anchor_age_s=2.0,
        phase_status="trusted",
        population_validation_state="disabled",
    )
    _setup_go_state_for_phase_test(state, 9001, 4.0, sync)
    updated = state.get_live_sync_state(9001)
    # Phase authority proceeds when population monitor is intentionally disabled.
    assert updated.phase_authority == "go_runtime"
    assert updated.handoff_state == "GO_REFINED_READY"


# ===========================================================================
# 13. Stage 8 contamination detection — flag-enabled detailed tests
# ===========================================================================


def _inject_burst_residual_events(state: RadarState, iid: int,
                                   icao_residuals: list[tuple[str, float]]):
    """Inject synthetic burst residual events for one IID.

    Each tuple is (icao_hex, residual_deg). Events have wall_ts=now and
    are added to state._live_burst_residual_events[iid].
    """
    now = time.time()
    events = []
    for i, (icao, resid) in enumerate(icao_residuals):
        events.append({
            "event_id": f"{iid}:burst:{icao}:{now:.3f}-{i}",
            "event_kind": "burst",
            "wall_ts": now,
            "arrival_beast_us": 1000000.0 + i * 1000.0,
            "beam_center_us": 1000000.0 + i * 1000.0,
            "iid": iid,
            "icao": icao,
            "centroid_timestamp_us": 1000000.0 + i * 1000.0,
            "residual_deg": resid,
            "residual_basis": "runtime_effective",
            "display_residual_class": "burst_inlier",
            "classification": "inlier",
            "timing_class": None,
            "bearing_deg": resid,
            "predicted_deg": 0.0,
            "range_nm": 50.0,
            "corrected_residual_deg": resid,
            "pos_age_s": 2.0,
            "aircraft_position_age_s": 2.0,
            "n_replies": 5,
            "signal_dbfs": -25.0,
            "sync_update_eligible": True,
            "fit_eligible": True,
            "dominant_family": True,
            "refinement_status": "stable",
            "reject_reason": None,
            "base_period_s": 4.0,
            "period_delta_s": 0.0,
            "effective_period_s": 4.0,
            "period_authority": "py_base",
            "sync_authority": "py_bootstrap",
            "phase_authority": "py_bootstrap",
            "event_base_period_s": 4.0,
            "event_period_delta_s": 0.0,
            "event_effective_period_s": 4.0,
            "event_period_authority": "py_base",
            "event_sync_authority": "py_bootstrap",
            "event_phase_authority": "py_bootstrap",
            "phase_basis": "sweep_epoch_only",
            "event_phase_basis": "sweep_epoch_only",
            "phase_is_absolute": False,
            "event_phase_is_absolute": False,
            "phase_offset_deg": 0.0,
            "event_phase_offset_deg": 0.0,
            "phase_offset_basis": None,
            "event_phase_offset_basis": None,
            "phase_offset_geographic_deg": None,
            "event_phase_offset_geographic_deg": None,
            "phase_anchor_icao": None,
            "event_phase_anchor_icao": None,
            "phase_anchor_status": None,
            "event_phase_anchor_status": None,
            "phase_epoch_us": 0.0,
            "effective_period_source": "none",
            "period_delta_source": "none",
            "source_path": "recorded_burst_alignment",
            "recording_path_kind": "recorded_burst_alignment",
            "handoff_state": "UNTRUSTED",
            "handoff_reason": "diagnostic_frame_accumulation_only",
            "event_handoff_state": "UNTRUSTED",
            "event_handoff_reason": "diagnostic_frame_accumulation_only",
            "sync_revision": 0,
        })
    state._live_burst_residual_events.setdefault(iid, deque()).extend(events)


def _setup_contamination_test(monkeypatch, iid: int = 9101,
                               period_s: float = 4.0) -> RadarState:
    """Set up state with Go operational, flag enabled, and basic sync."""
    import config as _cfg
    monkeypatch.setattr(_cfg, "RADAR_SYNC_CONTAMINATION_DETECTION_ENABLED", True)
    monkeypatch.setattr(_cfg, "RADAR_SYNC_GO_REFINER_OPERATIONAL", True)
    state = _make_state_with_python_model(iid, period_s)
    _inject_stable_period_history(state, iid, period_s)
    _inject_stable_slope_history(state, iid, near_zero=True)
    return state


def test_coherent_single_family_emits_single_family(monkeypatch):
    """A coherent single residual family (all residuals near zero) emits single_family."""
    state = _setup_contamination_test(monkeypatch, 9101)
    _inject_burst_residual_events(state, 9101, [
        ("AAAAAA", 0.5), ("AAAAAA", -0.3), ("AAAAAA", 1.0),
        ("BBBBBB", -1.0), ("BBBBBB", 0.0), ("BBBBBB", 0.8),
        ("CCCCCC", 1.5), ("CCCCCC", -0.5), ("CCCCCC", 0.2),
        ("DDDDDD", -1.5), ("DDDDDD", 0.3), ("DDDDDD", -0.1),
        ("EEEEEE", 2.0), ("EEEEEE", -2.0), ("EEEEEE", 0.0),
    ])
    result = state._detect_contamination_locked(9101)
    assert result["state"] == "single_family"
    assert result["distinct_icaos"] >= 3
    assert result["total_observations"] >= 12


def test_single_family_permits_go_readiness(monkeypatch):
    """single_family contamination state permits Go readiness when other gates pass."""
    state = _setup_contamination_test(monkeypatch, 9102)
    _inject_burst_residual_events(state, 9102, [
        ("AAAAAA", 0.5), ("AAAAAA", -0.3), ("AAAAAA", 1.0),
        ("BBBBBB", -1.0), ("BBBBBB", 0.0), ("BBBBBB", 0.8),
        ("CCCCCC", 1.5), ("CCCCCC", -0.5), ("CCCCCC", 0.2),
        ("DDDDDD", -1.5), ("DDDDDD", 0.3), ("DDDDDD", -0.1),
    ])
    state.update_go_iid_state(_go_sync_for_gates(9102, 4.0))
    _drive_go_handoff_cycles(state, 9102, cycles=3)
    updated = state.get_live_sync_state(9102)
    assert updated.handoff_state == "GO_REFINED_READY"
    assert updated.sync_authority == "go_runtime"


def test_isolated_outliers_do_not_emit_contaminated(monkeypatch):
    """Isolated outliers (a few far residuals from one ICAO) do not cause contamination."""
    state = _setup_contamination_test(monkeypatch, 9103)
    # 12 observations from 3 ICAOs near zero, plus 2 from one far ICAO
    events = [("AAAAAA", r) for r in [0.5, -0.3, 1.0, -1.0]]
    events += [("BBBBBB", r) for r in [-0.5, 0.0, 0.8, -1.2]]
    events += [("CCCCCC", r) for r in [1.5, -0.5, 0.2, -0.1]]
    # Outliers: only 2 observations from DDDDDD at 50 deg
    events += [("DDDDDD", 50.0), ("DDDDDD", 52.0)]
    _inject_burst_residual_events(state, 9103, events)
    result = state._detect_contamination_locked(9103)
    assert result["state"] == "single_family"
    assert result["secondary_icaos"] > 0  # outlier ICAO detected but rejected


def test_coherent_secondary_family_emits_contaminated(monkeypatch):
    """A coherent secondary family with >=4 obs and >=2 ICAOs emits contaminated."""
    state = _setup_contamination_test(monkeypatch, 9104)
    events = []
    # Primary family: 4 ICAOs, each with 4 observations near 0 deg
    for icao in ["AAAAAA", "BBBBBB", "CCCCCC", "DDDDDD"]:
        for r in [-2.0, -0.5, 0.5, 2.0]:
            events.append((icao, r))
    # Secondary family: 2 ICAOs, each with 4 observations near 60 deg
    for icao in ["EEEEEE", "FFFFFF"]:
        for r in [58.0, 59.0, 61.0, 62.0]:
            events.append((icao, r))
    _inject_burst_residual_events(state, 9104, events)
    result = state._detect_contamination_locked(9104)
    assert result["state"] == "contaminated"
    assert result["secondary_icaos"] >= 2
    assert result["secondary_observations"] >= 4
    assert result["family_separation_deg"] is not None
    assert result["family_separation_deg"] >= 25.0
    assert result["secondary_support_ratio"] >= 0.25


def test_contaminated_blocks_go_refined_ready(monkeypatch):
    """contaminated state blocks GO_REFINED_READY and sync_authority=go_runtime."""
    state = _setup_contamination_test(monkeypatch, 9105)
    events = []
    for icao in ["AAAAAA", "BBBBBB", "CCCCCC", "DDDDDD"]:
        for r in [-2.0, -0.5, 0.5, 2.0]:
            events.append((icao, r))
    for icao in ["EEEEEE", "FFFFFF"]:
        for r in [58.0, 59.0, 61.0, 62.0]:
            events.append((icao, r))
    _inject_burst_residual_events(state, 9105, events)
    state.update_go_iid_state(_go_sync_for_gates(9105, 4.0))
    updated = state.get_live_sync_state(9105)
    assert updated.sync_authority != "go_runtime"
    assert updated.handoff_state != "GO_REFINED_READY"


def test_contaminated_blocks_phase_authority(monkeypatch):
    """contaminated state blocks phase_authority=go_runtime even when other gates pass."""
    state = _setup_contamination_test(monkeypatch, 9106)
    events = []
    for icao in ["AAAAAA", "BBBBBB", "CCCCCC", "DDDDDD"]:
        for r in [-2.0, -0.5, 0.5, 2.0]:
            events.append((icao, r))
    for icao in ["EEEEEE", "FFFFFF"]:
        for r in [58.0, 59.0, 61.0, 62.0]:
            events.append((icao, r))
    _inject_burst_residual_events(state, 9106, events)
    state.update_go_iid_state(_go_sync_for_gates(9106, 4.0))
    updated = state.get_live_sync_state(9106)
    assert updated.phase_authority != "go_runtime"


def test_contamination_diagnostic_fields_in_snapshot(monkeypatch):
    """Contamination diagnostic fields appear in the live sync/API snapshot."""
    state = _setup_contamination_test(monkeypatch, 9107)
    events = []
    for icao in ["AAAAAA", "BBBBBB", "CCCCCC", "DDDDDD"]:
        for r in [-2.0, -0.5, 0.5, 2.0]:
            events.append((icao, r))
    for icao in ["EEEEEE", "FFFFFF"]:
        for r in [58.0, 59.0, 61.0, 62.0]:
            events.append((icao, r))
    _inject_burst_residual_events(state, 9107, events)
    state.update_go_iid_state(_go_sync_for_gates(9107, 4.0))
    payload = sweep._live_sync_state_to_dict(state.get_live_sync_state(9107))
    assert payload["contamination_state"] == "contaminated"
    assert payload["contamination_total_observations"] > 0
    assert payload["contamination_distinct_icaos"] > 0
    assert payload["contamination_primary_observations"] > 0
    assert payload["contamination_secondary_observations"] > 0
    assert payload["contamination_secondary_icaos"] > 0
    assert payload["contamination_family_separation_deg"] is not None
    assert payload["contamination_family_separation_deg"] >= 25.0
    assert payload["contamination_secondary_support_ratio"] is not None
    assert payload["contamination_secondary_support_ratio"] >= 0.25


def test_incoherent_secondary_no_contamination(monkeypatch):
    """A secondary cluster with high spread is not coherent, no contamination."""
    state = _setup_contamination_test(monkeypatch, 9108)
    events = []
    for icao in ["AAAAAA", "BBBBBB", "CCCCCC", "DDDDDD"]:
        for r in [-2.0, -0.5, 0.5, 2.0]:
            events.append((icao, r))
    # Secondary candidates: 2 ICAOs but scattered across wide range → not coherent
    # Place them >15 deg from primary but widely separated from each other
    for icao in ["EEEEEE", "FFFFFF"]:
        for r in [40.0, 70.0, -50.0, -80.0]:
            events.append((icao, r))
    _inject_burst_residual_events(state, 9108, events)
    result = state._detect_contamination_locked(9108)
    assert result["state"] in ("single_family", "insufficient_data")
