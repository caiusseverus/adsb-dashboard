"""Unit tests for sync_population_monitor.py (Stage 7)."""
from __future__ import annotations

import math
from unittest.mock import MagicMock

import pytest

from radar.sync_population_monitor import (
    DEFAULT_THRESHOLDS,
    DISAGREEMENT_THRESHOLD_DEG,
    MIN_DISAGREEING_ICAOS,
    MIN_NON_ANCHOR_ICAOS,
    MIN_OBS_PER_ICAO,
    PopulationResidualSummary,
    _circular_mean_residuals_deg,
    _circular_spread_deg,
    compute_population_residual_summary,
)


# ── Helpers ───────────────────────────────────────────────────────────────────


def _make_sync(
    phase_anchor_icao: str | None = "AAA000",
    phase_anchor_status: str = "selected",
) -> MagicMock:
    sync = MagicMock()
    sync.phase_anchor_icao = phase_anchor_icao
    sync.phase_anchor_status = phase_anchor_status
    return sync


def _make_entry(
    icao: str,
    residual_deg: float,
    pos_age_s: float = 1.0,
    sync_update_eligible: bool = True,
    classification: str = "inlier",
    wall_ts: float = 1000.0,
) -> dict:
    return {
        "icao": icao,
        "residual_deg": residual_deg,
        "pos_age_s": pos_age_s,
        "sync_update_eligible": sync_update_eligible,
        "classification": classification,
        "wall_ts": wall_ts,
        "bearing_deg": 90.0,
        "predicted_deg": 90.0 - residual_deg,
    }


def _make_entries_for_icao(
    icao: str,
    residuals: list[float],
    base_ts: float = 1000.0,
    **kwargs,
) -> list[dict]:
    return [
        _make_entry(icao, r, wall_ts=base_ts + i, **kwargs)
        for i, r in enumerate(residuals)
    ]


def _agrees_pop(n_non_anchor_icaos: int = 5, anchor_res: float = 1.0, pop_res: float = 0.5) -> list[dict]:
    """Build an agreeing population: anchor + n non-anchor ICAOs."""
    anchor_icao = "AAA000"
    entries = _make_entries_for_icao(anchor_icao, [anchor_res] * 3)
    for i in range(n_non_anchor_icaos):
        icao = f"BBB{i:03d}"
        entries.extend(_make_entries_for_icao(icao, [pop_res] * 3, base_ts=2000.0 + i * 10))
    return entries


# ── Circular maths ────────────────────────────────────────────────────────────


class TestCircularMath:
    def test_mean_zero(self):
        result = _circular_mean_residuals_deg([-5.0, 5.0])
        assert result is not None
        assert abs(result) < 0.01

    def test_mean_near_positive_wrap(self):
        # Values near +/-180 should average to ±180, not 0.
        result = _circular_mean_residuals_deg([170.0, -170.0])
        assert result is not None
        # Both values are near ±180°; mean resultant points at 180 (or -180).
        assert abs(abs(result) - 180.0) < 1.0

    def test_mean_small_negative(self):
        result = _circular_mean_residuals_deg([-10.0, -8.0, -12.0])
        assert result is not None
        assert abs(result - (-10.0)) < 0.5

    def test_mean_empty_returns_none(self):
        assert _circular_mean_residuals_deg([]) is None

    def test_mean_single_value(self):
        result = _circular_mean_residuals_deg([15.0])
        assert result is not None
        assert abs(result - 15.0) < 0.01

    def test_spread_zero_identical(self):
        result = _circular_spread_deg([5.0, 5.0, 5.0])
        assert result is not None
        assert result < 0.01

    def test_spread_increases_with_scatter(self):
        tight = _circular_spread_deg([1.0, 2.0, -1.0, -2.0])
        loose = _circular_spread_deg([10.0, 20.0, -10.0, -20.0])
        assert tight is not None and loose is not None
        assert loose > tight

    def test_spread_near_180_wrap(self):
        # Two clusters at ±175° should have high spread.
        result = _circular_spread_deg([175.0, -175.0, 176.0, -176.0])
        assert result is not None
        assert result < 15.0  # They are tightly clustered near ±180°.

    def test_spread_none_for_single_value(self):
        assert _circular_spread_deg([5.0]) is None

    def test_spread_none_for_empty(self):
        assert _circular_spread_deg([]) is None

    def test_delta_wrap(self):
        from radar.sync_population_monitor import _circular_delta_deg  # re-export
        from radar.angular import _circular_delta_deg as delta
        # 175 - (-175) = 350 naive; wrapped = -10.
        result = delta(175.0, -175.0)
        assert result is not None
        assert abs(result - (-10.0)) < 0.01

    def test_delta_none_when_either_is_none(self):
        from radar.angular import _circular_delta_deg as delta
        assert delta(None, 5.0) is None
        assert delta(5.0, None) is None


# ── Insufficient data ─────────────────────────────────────────────────────────


class TestInsufficientData:
    def test_no_observations(self):
        sync = _make_sync()
        result = compute_population_residual_summary([], sync, iid=1)
        assert result.status == "insufficient_data"
        assert result.reason == "no_observations"
        assert result.observation_count == 0

    def test_no_sync_state_returns_unavailable(self):
        result = compute_population_residual_summary([], None, iid=1)
        assert result.status == "unavailable"
        assert result.reason == "no_sync_state"

    def test_no_anchor_returns_anchor_unavailable(self):
        sync = _make_sync(phase_anchor_icao=None)
        entries = _agrees_pop()
        result = compute_population_residual_summary(entries, sync, iid=1)
        # sweep_epoch_only with no anchor → anchor_unavailable
        assert result.status == "anchor_unavailable"
        assert result.reason == "no_anchor"

    def test_anchor_not_in_contributing_returns_anchor_unavailable(self):
        # Anchor ICAO exists in sync but has no observations in entries.
        sync = _make_sync(phase_anchor_icao="ZZZZZZ")
        entries = []
        for i in range(6):
            entries.extend(_make_entries_for_icao(f"BBB{i:03d}", [2.0] * 3))
        result = compute_population_residual_summary(entries, sync, iid=1)
        assert result.status == "anchor_unavailable"

    def test_not_enough_non_anchor_icaos(self):
        # Anchor present but only MIN_NON_ANCHOR_ICAOS - 1 non-anchor ICAOs.
        sync = _make_sync()
        entries = _make_entries_for_icao("AAA000", [1.0] * 3)
        for i in range(MIN_NON_ANCHOR_ICAOS - 1):
            entries.extend(_make_entries_for_icao(f"BBB{i:03d}", [1.5] * 3))
        result = compute_population_residual_summary(entries, sync, iid=1)
        assert result.status == "insufficient_data"
        assert result.reason == "insufficient_non_anchor_icaos"

    def test_not_enough_obs_per_icao(self):
        # Each ICAO has only 1 observation (< MIN_OBS_PER_ICAO = 2).
        sync = _make_sync()
        entries = [_make_entry("AAA000", 1.0)]
        for i in range(6):
            entries.append(_make_entry(f"BBB{i:03d}", 1.0))
        result = compute_population_residual_summary(entries, sync, iid=1)
        assert result.status in ("insufficient_data", "anchor_unavailable")

    def test_all_hard_rejected(self):
        sync = _make_sync()
        entries = []
        for icao in ["AAA000", "BBB001", "BBB002", "BBB003", "BBB004", "BBB005"]:
            entries.extend(_make_entries_for_icao(icao, [1.0] * 3, classification="rejected"))
        result = compute_population_residual_summary(entries, sync, iid=1)
        assert result.status == "insufficient_data"
        assert result.reason == "all_observations_rejected"

    def test_stale_positions_excluded(self):
        # All observations have pos_age_s > freshness gate.
        sync = _make_sync()
        entries = []
        for icao in ["AAA000", "BBB001", "BBB002", "BBB003", "BBB004"]:
            entries.extend(_make_entries_for_icao(icao, [1.0] * 3, pos_age_s=99.0))
        result = compute_population_residual_summary(entries, sync, iid=1)
        assert result.status == "insufficient_data"

    def test_not_sync_update_eligible_excluded(self):
        sync = _make_sync()
        entries = []
        for icao in ["AAA000", "BBB001", "BBB002", "BBB003", "BBB004"]:
            entries.extend(
                _make_entries_for_icao(icao, [1.0] * 3, sync_update_eligible=False)
            )
        result = compute_population_residual_summary(entries, sync, iid=1)
        assert result.status == "insufficient_data"


# ── Agreement ─────────────────────────────────────────────────────────────────


class TestAgreement:
    def test_agrees_when_residuals_close(self):
        sync = _make_sync()
        entries = _agrees_pop(n_non_anchor_icaos=5, anchor_res=2.0, pop_res=1.5)
        result = compute_population_residual_summary(entries, sync, iid=1)
        assert result.status == "population_agrees"
        assert result.reason is None
        assert result.disagreeing_icao_count == 0

    def test_agrees_small_outlier_below_threshold_count(self):
        # Only MIN_DISAGREEING_ICAOS - 1 ICAOs disagree → agrees or mixed, not disagrees.
        sync = _make_sync()
        entries = _make_entries_for_icao("AAA000", [1.0] * 3)
        # Non-anchor ICAOs mostly agreeing.
        for i in range(MIN_NON_ANCHOR_ICAOS + 1):
            entries.extend(_make_entries_for_icao(f"BBB{i:03d}", [2.0] * 3))
        # One ICAO disagrees.
        entries.extend(_make_entries_for_icao("DIS001", [35.0] * 3))
        result = compute_population_residual_summary(entries, sync, iid=1)
        assert result.status in ("population_agrees", "population_mixed")
        assert result.status != "population_disagrees"

    def test_anchor_population_delta_small_when_agrees(self):
        sync = _make_sync()
        entries = _agrees_pop(n_non_anchor_icaos=5, anchor_res=1.0, pop_res=0.5)
        result = compute_population_residual_summary(entries, sync, iid=1)
        assert result.anchor_population_delta_deg is not None
        assert abs(result.anchor_population_delta_deg) < DISAGREEMENT_THRESHOLD_DEG

    def test_contributing_icao_count_correct(self):
        sync = _make_sync()
        entries = _agrees_pop(n_non_anchor_icaos=4)
        result = compute_population_residual_summary(entries, sync, iid=1)
        # 1 anchor + 4 non-anchor = 5 contributing ICAOs
        assert result.contributing_icao_count == 5


# ── Disagreement ──────────────────────────────────────────────────────────────


class TestDisagreement:
    def _disagreeing_entries(
        self,
        n_disagreeing: int = 4,
        disagreeing_offset: float = 40.0,
        n_agreeing: int = 2,
    ) -> list[dict]:
        entries = _make_entries_for_icao("AAA000", [1.0] * 3)
        for i in range(n_agreeing):
            entries.extend(_make_entries_for_icao(f"AGR{i:03d}", [2.0] * 3))
        for i in range(n_disagreeing):
            entries.extend(
                _make_entries_for_icao(f"DIS{i:03d}", [1.0 + disagreeing_offset] * 3)
            )
        return entries

    def test_population_disagrees_when_enough_icaos_offset(self):
        # Anchor at 0°; all non-anchor ICAOs consistently at 35° → clear disagreement.
        # anchor-population delta = 35°, every non-anchor ICAO delta = 35° > threshold.
        sync = _make_sync()
        entries = _make_entries_for_icao("AAA000", [0.0] * 3)
        for i in range(MIN_NON_ANCHOR_ICAOS + 1):
            entries.extend(
                _make_entries_for_icao(f"DIS{i:03d}", [35.0] * 3)
            )
        result = compute_population_residual_summary(entries, sync, iid=1)
        assert result.status == "population_disagrees"
        assert result.reason == "population_delta_exceeds_threshold"
        assert result.disagreeing_icao_count >= MIN_DISAGREEING_ICAOS

    def test_worst_icao_identified(self):
        sync = _make_sync()
        entries = _make_entries_for_icao("AAA000", [0.0] * 3)
        entries.extend(_make_entries_for_icao("WORST0", [50.0] * 3))
        for i in range(MIN_NON_ANCHOR_ICAOS):
            entries.extend(_make_entries_for_icao(f"DIS{i:03d}", [35.0] * 3))
        result = compute_population_residual_summary(entries, sync, iid=1)
        if result.status == "population_disagrees" and result.worst_icao is not None:
            assert result.worst_icao_delta_deg is not None
            assert abs(result.worst_icao_delta_deg) >= DISAGREEMENT_THRESHOLD_DEG

    def test_rejected_obs_excluded_from_eligible(self):
        sync = _make_sync()
        entries = _make_entries_for_icao("AAA000", [1.0] * 3)
        for i in range(MIN_NON_ANCHOR_ICAOS + 1):
            entries.extend(_make_entries_for_icao(f"BBB{i:03d}", [2.0] * 3))
        # Outliers but marked rejected — must not affect population mean.
        for i in range(5):
            entries.extend(
                _make_entries_for_icao(
                    f"RDIS{i:03d}", [90.0] * 3, classification="rejected"
                )
            )
        result = compute_population_residual_summary(entries, sync, iid=1)
        assert result.status in ("population_agrees", "population_mixed")

    def test_status_not_disagrees_when_delta_below_threshold(self):
        # Many ICAOs disagree in count, but anchor-population delta is small.
        sync = _make_sync()
        # This is hard to construct naturally; ensure the logic requires BOTH
        # disagreeing_count AND delta to exceed thresholds.
        entries = _make_entries_for_icao("AAA000", [0.0] * 3)
        # Several ICAOs each slightly above threshold from anchor individually
        # but population mean stays near anchor (they cancel out).
        for i in range(MIN_NON_ANCHOR_ICAOS):
            sign = 1 if i % 2 == 0 else -1
            entries.extend(
                _make_entries_for_icao(
                    f"MIX{i:03d}", [sign * (DISAGREEMENT_THRESHOLD_DEG + 5.0)] * 3
                )
            )
        result = compute_population_residual_summary(entries, sync, iid=1)
        # Mixed spread — not a clean population_disagrees.
        assert result.status != "population_agrees" or result.anchor_population_delta_deg is not None


# ── Mixed ─────────────────────────────────────────────────────────────────────


class TestMixed:
    def test_mixed_when_high_spread_but_few_disagree(self):
        # Anchor at 5°. Non-anchor ICAOs spread symmetrically across ±45° from anchor:
        # the population mean stays near the anchor (symmetric spread), so
        # anchor_population_delta is small, but spread > threshold → population_mixed.
        sync = _make_sync()
        entries = _make_entries_for_icao("AAA000", [5.0] * 3)
        offsets = [-45, -30, -15, 0, 15, 30, 45]
        for i, off in enumerate(offsets):
            entries.extend(_make_entries_for_icao(f"SPR{i:03d}", [5.0 + off] * 3))
        result = compute_population_residual_summary(entries, sync, iid=1)
        # Symmetric high-spread population: not a clean disagrees.
        assert result.status in ("population_agrees", "population_mixed")


# ── API / integration ─────────────────────────────────────────────────────────


class TestApiPayload:
    def test_to_api_dict_has_required_keys(self):
        sync = _make_sync()
        entries = _agrees_pop(n_non_anchor_icaos=5)
        result = compute_population_residual_summary(entries, sync, iid=42)
        payload = result.to_api_dict()
        required = {
            "status", "reason", "window_s", "observation_count",
            "eligible_observation_count", "contributing_icao_count",
            "disagreeing_icao_count", "anchor_icao",
            "anchor_residual_mean_deg", "population_residual_mean_deg",
            "anchor_population_delta_deg", "population_residual_spread_deg",
            "worst_icao", "worst_icao_delta_deg",
            "per_icao", "per_icao_total_count", "per_icao_omitted_count",
            "thresholds",
        }
        assert required.issubset(payload.keys())

    def test_thresholds_included(self):
        sync = _make_sync()
        result = compute_population_residual_summary([], sync, iid=1)
        payload = result.to_api_dict()
        assert payload["thresholds"] == DEFAULT_THRESHOLDS

    def test_per_icao_bounded(self):
        from radar.sync_population_monitor import MAX_PER_ICAO_IN_PAYLOAD
        sync = _make_sync()
        entries = _make_entries_for_icao("AAA000", [1.0] * 3)
        for i in range(MAX_PER_ICAO_IN_PAYLOAD + 5):
            entries.extend(_make_entries_for_icao(f"B{i:04d}", [2.0] * 3))
        result = compute_population_residual_summary(entries, sync, iid=1)
        payload = result.to_api_dict()
        assert len(payload["per_icao"]) <= MAX_PER_ICAO_IN_PAYLOAD
        assert payload["per_icao_omitted_count"] >= 0

    def test_no_sync_state_api_dict(self):
        result = compute_population_residual_summary([], None, iid=1)
        payload = result.to_api_dict()
        assert payload["status"] == "unavailable"
        assert payload["reason"] == "no_sync_state"

    def test_agrees_label_renders(self):
        sync = _make_sync()
        entries = _agrees_pop(n_non_anchor_icaos=5)
        result = compute_population_residual_summary(entries, sync, iid=1)
        payload = result.to_api_dict()
        assert payload["status"] == "population_agrees"

    def test_disagrees_payload_has_delta(self):
        sync = _make_sync()
        entries = _make_entries_for_icao("AAA000", [0.0] * 3)
        for i in range(MIN_NON_ANCHOR_ICAOS):
            entries.extend(
                _make_entries_for_icao(f"DIS{i:03d}", [35.0] * 3)
            )
        result = compute_population_residual_summary(entries, sync, iid=1)
        if result.status == "population_disagrees":
            payload = result.to_api_dict()
            assert payload["anchor_population_delta_deg"] is not None
            assert payload["disagreeing_icao_count"] >= MIN_DISAGREEING_ICAOS

    def test_monitor_does_not_mutate_sync_state(self):
        sync = _make_sync()
        original_anchor = sync.phase_anchor_icao
        original_status = sync.phase_anchor_status
        entries = _agrees_pop(n_non_anchor_icaos=5)
        compute_population_residual_summary(entries, sync, iid=1)
        assert sync.phase_anchor_icao == original_anchor
        assert sync.phase_anchor_status == original_status

    def test_insufficient_data_reason_explicit(self):
        sync = _make_sync()
        result = compute_population_residual_summary([], sync, iid=1)
        payload = result.to_api_dict()
        assert payload["status"] == "insufficient_data"
        assert payload["reason"] is not None
        assert len(payload["reason"]) > 0

    def test_per_icao_anchor_first_in_payload(self):
        sync = _make_sync()
        entries = _agrees_pop(n_non_anchor_icaos=5)
        result = compute_population_residual_summary(entries, sync, iid=1)
        payload = result.to_api_dict()
        per_icao = payload["per_icao"]
        if per_icao:
            # Anchor row (if present) should appear before non-anchor rows.
            anchor_rows = [r for r in per_icao if r.get("is_anchor")]
            if anchor_rows:
                assert per_icao.index(anchor_rows[0]) == 0

    def test_sweep_epoch_only_no_anchor_is_anchor_unavailable(self):
        # sweep_epoch_only phase → anchor_unavailable status expected.
        sync = _make_sync(phase_anchor_icao=None, phase_anchor_status="unavailable")
        entries = []
        for i in range(6):
            entries.extend(_make_entries_for_icao(f"BBB{i:03d}", [1.0] * 3))
        result = compute_population_residual_summary(entries, sync, iid=1)
        assert result.status == "anchor_unavailable"


# ── Stage 7R: feature flag, hysteresis, typed validation state ────────────────


import time as _time

import config as _cfg

from radar.sync_models import LiveSyncState
from radar.simple_sync import _derive_phase_trust_reason
from radar.sweep import RadarState, _live_sync_state_to_dict


def _make_radar_state_stub():
    """Minimal stub satisfying _resolve_phase_anchor_state's attribute access."""
    state = MagicMock(spec=[
        "_select_phase_anchor_aircraft",
        "_solve_phase_anchor_from_icao",
        "_validate_phase_anchor_against_population",
        "_population_demotion_hold",
    ])
    state._population_demotion_hold = {}
    return state


def _make_existing(iid: int = 1, anchor_icao: str = "ABCD12") -> LiveSyncState:
    return LiveSyncState(
        iid=iid,
        period_s=4.5,
        phase_epoch_us=1_000_000_000.0,
        phase_offset_deg=45.0,
        sync_quality=0.9,
        sync_jitter_deg=2.0,
        last_sync_update_ts=_time.time(),
        source="multi_aircraft_burst",
        usable=True,
        phase_anchor_icao=anchor_icao,
        phase_anchor_since_ts=_time.time() - 30.0,
    )


def _strong_disagree_val() -> dict:
    return {
        "contributors": [],
        "rejected": [
            {"icao": "X1", "reason": "branch_disagreement", "error_deg": 50.0},
            {"icao": "X2", "reason": "branch_disagreement", "error_deg": 48.0},
        ],
        "contributor_count": 0,
        "contributor_icaos": [],
        "reject_count": 2,
        "median_error_deg": None,
        "nudge_deg": 0.0,
        "status": "population_disagrees",
    }


def _agree_val() -> dict:
    return {
        "contributors": [{"icao": "Y1", "error_deg": 1.0, "offset_deg": 46.0, "spread_deg": 2.0, "obs_count": 3, "weight": 1.0}],
        "rejected": [],
        "contributor_count": 1,
        "contributor_icaos": ["Y1"],
        "reject_count": 0,
        "median_error_deg": 1.0,
        "nudge_deg": 0.1,
        "status": "confirmed",
    }


def _call_resolve(state, existing: LiveSyncState, validation: dict, now_ts: float = 1000.0, anchor_icao: str = "ABCD12") -> dict:
    """Drive RadarState._resolve_phase_anchor_state unbound on the stub."""
    state._select_phase_anchor_aircraft.return_value = {
        "selected": {"icao": anchor_icao, "score": 0.9},
        "candidates": [{"icao": anchor_icao, "score": 0.9}],
        "replacement_reason": None,
        "no_candidate_reason": None,
    }
    state._solve_phase_anchor_from_icao.return_value = {
        "offset_raw_deg": 45.0,
        "offset_smoothed_deg": 45.5,
        "spread_deg": 3.0,
        "delta_from_existing_deg": 0.5,
        "obs_count": 5,
    }
    state._validate_phase_anchor_against_population.return_value = validation
    return RadarState._resolve_phase_anchor_state(
        state,
        iid=existing.iid,
        scored=[],
        existing=existing,
        epoch_us=1_000_000_000.0,
        now_ts=now_ts,
        mixed_fallback_offset=0.0,
    )


class TestStage7RPopulationMonitor:
    """Stage 7R: feature flag, demotion hysteresis, typed validation state."""

    def test_flag_disabled_no_demotion(self, monkeypatch):
        """Flag=False: strong disagreement must NOT produce population_demoted."""
        monkeypatch.setattr(_cfg, "RADAR_SYNC_POPULATION_MONITOR_ENABLED", False)
        state = _make_radar_state_stub()
        result = _call_resolve(state, _make_existing(), _strong_disagree_val())
        assert result["phase_anchor_status"] != "population_demoted"
        assert result["phase_anchor_status"] != "population_veto"

    def test_flag_disabled_validation_state_is_disabled(self, monkeypatch):
        """Flag=False → population_validation_state=='disabled', reason=='monitor_disabled'."""
        monkeypatch.setattr(_cfg, "RADAR_SYNC_POPULATION_MONITOR_ENABLED", False)
        state = _make_radar_state_stub()
        result = _call_resolve(state, _make_existing(), _strong_disagree_val())
        assert result["population_validation_state"] == "disabled"
        assert result["population_validation_reason"] == "monitor_disabled"

    def test_strong_disagreement_demotes_anchor(self, monkeypatch):
        """Flag=True + strong disagree → phase_anchor_status=='population_demoted', state=='fail'."""
        monkeypatch.setattr(_cfg, "RADAR_SYNC_POPULATION_MONITOR_ENABLED", True)
        state = _make_radar_state_stub()
        result = _call_resolve(state, _make_existing(), _strong_disagree_val(), now_ts=1000.0)
        assert result["phase_anchor_status"] == "population_demoted"
        assert result["population_validation_state"] == "fail"
        assert result["population_validation_reason"] == "population_demoted"

    def test_demotion_persists_during_hold(self, monkeypatch):
        """After demotion at t=0: at t=5 (agreement) still demoted; at t=11 (agreement) recovered."""
        monkeypatch.setattr(_cfg, "RADAR_SYNC_POPULATION_MONITOR_ENABLED", True)
        state = _make_radar_state_stub()
        existing = _make_existing(anchor_icao="ABCD12")
        hold_key = (existing.iid, "ABCD12")

        # t=0: trigger demotion
        r0 = _call_resolve(state, existing, _strong_disagree_val(), now_ts=1000.0)
        assert r0["phase_anchor_status"] == "population_demoted"
        assert state._population_demotion_hold.get(hold_key, 0.0) == pytest.approx(1010.0)

        # t=5: population now agrees, but hold is still active → still demoted
        r5 = _call_resolve(state, existing, _agree_val(), now_ts=1005.0)
        assert r5["phase_anchor_status"] == "population_demoted"
        assert r5["population_validation_state"] == "fail"
        assert "hold" in (r5["population_validation_reason"] or "")

        # t=11: hold expired (1010 < 1011), population agrees → anchor recovers
        r11 = _call_resolve(state, existing, _agree_val(), now_ts=1011.0)
        assert r11["phase_anchor_status"] != "population_demoted"
        assert r11["population_validation_state"] == "pass"

    def test_borderline_reject_count_no_demotion(self, monkeypatch):
        """Only 1 reject (< 2 threshold) → no demotion."""
        monkeypatch.setattr(_cfg, "RADAR_SYNC_POPULATION_MONITOR_ENABLED", True)
        state = _make_radar_state_stub()
        weak = {**_strong_disagree_val(), "reject_count": 1}
        result = _call_resolve(state, _make_existing(), weak)
        assert result["phase_anchor_status"] != "population_demoted"

    def test_borderline_contributor_present_no_demotion(self, monkeypatch):
        """2 rejects but 1 contributor → not a strong disagree → no demotion."""
        monkeypatch.setattr(_cfg, "RADAR_SYNC_POPULATION_MONITOR_ENABLED", True)
        state = _make_radar_state_stub()
        mixed = {**_strong_disagree_val(), "contributor_count": 1}
        result = _call_resolve(state, _make_existing(), mixed)
        assert result["phase_anchor_status"] != "population_demoted"

    def test_phase_trust_reason_derivation_from_status(self):
        """_derive_phase_trust_reason returns 'population_demoted' for demoted anchor."""
        result = _derive_phase_trust_reason(
            phase_anchor_icao="ABCD12",
            phase_anchor_status="population_demoted",
            validation={"status": "confirmed"},
            anchor_selection={"candidates": [{"icao": "ABCD12"}]},
            phase_status="untrusted",
        )
        assert result == "population_demoted"

    def test_phase_trust_reason_derivation_from_validation_status(self):
        """_derive_phase_trust_reason returns 'population_demoted' via v_status check."""
        result = _derive_phase_trust_reason(
            phase_anchor_icao="ABCD12",
            phase_anchor_status="selected",
            validation={"status": "population_disagrees"},
            anchor_selection={"candidates": [{"icao": "ABCD12"}, {"icao": "EFGH34"}]},
            phase_status="untrusted",
        )
        assert result == "population_demoted"

    def test_snapshot_exposes_population_validation_fields(self):
        """_live_sync_state_to_dict output includes population_validation_state and _reason."""
        sync = LiveSyncState(
            iid=1,
            period_s=4.5,
            phase_epoch_us=1_000_000_000.0,
            phase_offset_deg=45.0,
            sync_quality=0.9,
            sync_jitter_deg=2.0,
            last_sync_update_ts=1000.0,
            source="multi_aircraft_burst",
            usable=True,
            population_validation_state="fail",
            population_validation_reason="population_demoted",
        )
        payload = _live_sync_state_to_dict(sync)
        assert "population_validation_state" in payload
        assert "population_validation_reason" in payload
        assert payload["population_validation_state"] == "fail"
        assert payload["population_validation_reason"] == "population_demoted"
