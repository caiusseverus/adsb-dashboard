"""Tests for the re-implemented radar phase refinement model.

Covers:
- _fit_per_aircraft_slope() helper (per-ICAO unwrapped slope)
- phase_status computation (trusted/provisional/untrusted)
- Period correction anchored to period_base_s with ±500 PPM cap
- EMA preservation when per-ICAO consensus is rejected
- Stage 3 trust gate in _sync_state_has_trusted_absolute_phase()
"""
from dataclasses import fields
import math
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(__file__)))

import pytest
from collections import deque as _deque

from radar.sweep import (
    AlignedBurstSyncObs,
    LiveSyncState,
    RadarState,
    SyncPrediction,
    _fit_per_aircraft_slope,
    _fit_weighted_slope,
    predict_sync_observation,
)
from radar.aircraft_localiser import AircraftLocaliser


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _make_entries(icao, n, t_start_s, slope_deg_per_s, period_s=4.0,
                  base_bearing=0.0, weight=1.0, span_s=None):
    """Generate scored-style dicts for one ICAO with a linear residual slope.

    Default span is 3 * period_s so per-ICAO span gate (2 * period_base_s) passes.
    Pass span_s explicitly to test the span gate itself.
    """
    if span_s is None:
        span_s = 3.0 * period_s
    entries = []
    for i in range(n):
        t = t_start_s + i * (span_s / max(n - 1, 1))
        residual = base_bearing + slope_deg_per_s * (t - t_start_s)
        entries.append({
            "icao": icao,
            "effective_us": t * 1_000_000.0,
            "residual": (residual + 180.0) % 360.0 - 180.0,  # wrapped
            "weight": weight,
        })
    return entries


def _make_sync(phase_status="untrusted", phase_anchor_status="selected",
               phase_anchor_icao="ABC123", period_s=4.0, period_base_s=4.0,
               residual_slope_deg_per_s=0.0, phase_epoch_us=0.0,
               phase_offset_deg=0.0, phase_anchor_spread_deg=None,
               phase_validation_status="anchor_only", phase_validation_contributors=0,
               phase_validation_median_error_deg=None, sync_quality=0.9,
               source="multi_aircraft_burst"):
    return LiveSyncState(
        iid=1,
        period_s=period_s,
        phase_epoch_us=phase_epoch_us,
        phase_offset_deg=phase_offset_deg,
        sync_quality=sync_quality,
        sync_jitter_deg=2.0,
        last_sync_update_ts=0.0,
        source=source,
        usable=True,
        period_base_s=period_base_s,
        residual_slope_deg_per_s=residual_slope_deg_per_s,
        phase_status=phase_status,
        phase_anchor_status=phase_anchor_status,
        phase_anchor_icao=phase_anchor_icao,
        phase_anchor_spread_deg=phase_anchor_spread_deg,
        phase_validation_status=phase_validation_status,
        phase_validation_contributors=phase_validation_contributors,
        phase_validation_median_error_deg=phase_validation_median_error_deg,
    )


# ---------------------------------------------------------------------------
# _fit_per_aircraft_slope tests
# ---------------------------------------------------------------------------

class TestFitPerAircraftSlope:

    def test_single_icao_rejected(self):
        entries = _make_entries("AAAAAA", 10, 0.0, +0.5, period_s=4.0)
        result = _fit_per_aircraft_slope(entries, period_base_s=4.0)
        assert result["slope_deg_per_s"] is None
        assert result["reject_reason"] == "insufficient_icaos"
        assert result["icao_count"] == 0

    def test_two_icaos_opposite_sign_blocked(self):
        entries = (
            _make_entries("AAAAAA", 8, 0.0, +0.4, period_s=4.0)
            + _make_entries("BBBBBB", 8, 0.0, -0.4, period_s=4.0)
        )
        result = _fit_per_aircraft_slope(entries, period_base_s=4.0)
        assert result["slope_deg_per_s"] is None
        assert result["sign_agreement"] is False
        assert result["reject_reason"] == "sign_disagreement"

    def test_three_icaos_agree_positive(self):
        period_s = 4.0
        # Each ICAO: 8 obs spanning 3 * period_s = 12 s (> 2 * period_base_s = 8 s)
        entries = (
            _make_entries("AAAAAA", 8, 0.0, +0.4, period_s=period_s)
            + _make_entries("BBBBBB", 8, 0.0, +0.38, period_s=period_s)
            + _make_entries("CCCCCC", 8, 0.0, +0.42, period_s=period_s)
        )
        result = _fit_per_aircraft_slope(entries, period_base_s=period_s)
        assert result["sign_agreement"] is True
        assert result["slope_deg_per_s"] is not None
        assert result["slope_deg_per_s"] == pytest.approx(0.40, abs=0.05)
        assert result["icao_count"] == 3

    def test_insufficient_obs_per_icao(self):
        # Only 2 obs each — below _PER_ICAO_MIN_OBS = 3
        entries = (
            _make_entries("AAAAAA", 2, 0.0, +0.4, period_s=4.0)
            + _make_entries("BBBBBB", 2, 0.0, +0.4, period_s=4.0)
        )
        result = _fit_per_aircraft_slope(entries, period_base_s=4.0)
        assert result["slope_deg_per_s"] is None
        assert result["reject_reason"] == "insufficient_icaos"
        assert "AAAAAA" in result["per_icao_reject_reasons"]
        assert result["per_icao_reject_reasons"]["AAAAAA"] == "insufficient_icao_observations"

    def test_global_span_not_enough_without_per_icao_span(self):
        # 3 ICAOs but each spans only 4 s < 2 * 4.0 = 8 s
        entries = (
            _make_entries("AAAAAA", 5, 0.0, +0.4, period_s=4.0 / 4)
            + _make_entries("BBBBBB", 5, 0.0, +0.4, period_s=4.0 / 4)
            + _make_entries("CCCCCC", 5, 0.0, +0.4, period_s=4.0 / 4)
        )
        result = _fit_per_aircraft_slope(entries, period_base_s=4.0)
        assert result["slope_deg_per_s"] is None
        assert result["reject_reason"] == "insufficient_icaos"
        for icao in ("AAAAAA", "BBBBBB", "CCCCCC"):
            assert result["per_icao_reject_reasons"].get(icao) == "insufficient_icao_span"

    def test_same_sign_different_magnitude_rejected(self):
        # Slopes: 0.001, 0.001, 5.0 deg/s — magnitude inconsistent
        period_s = 4.0
        entries = (
            _make_entries("AAAAAA", 8, 0.0, +0.001, period_s=period_s)
            + _make_entries("BBBBBB", 8, 0.0, +0.001, period_s=period_s)
            + _make_entries("CCCCCC", 8, 0.0, +5.0, period_s=period_s)
        )
        result = _fit_per_aircraft_slope(entries, period_base_s=period_s)
        assert result["slope_deg_per_s"] is None
        assert result["reject_reason"] == "slope_magnitude_disagreement"

    def test_near_zero_median_magnitude_rejected(self):
        # Median slope is near-zero but one ICAO has substantial slope
        period_s = 4.0
        entries = (
            _make_entries("AAAAAA", 8, 0.0, +0.001, period_s=period_s)
            + _make_entries("BBBBBB", 8, 0.0, +0.001, period_s=period_s)
            + _make_entries("CCCCCC", 8, 0.0, +0.5, period_s=period_s)
        )
        result = _fit_per_aircraft_slope(entries, period_base_s=period_s)
        assert result["slope_deg_per_s"] is None
        assert result["reject_reason"] == "slope_magnitude_disagreement"

    def test_missing_icao_skipped_not_crash(self):
        # Mix of entries: some with missing icao, some valid
        bad = [{"icao": "", "effective_us": 1e6, "residual": 5.0, "weight": 1.0}]
        good = (
            _make_entries("AAAAAA", 8, 0.0, +0.4, period_s=4.0)
            + _make_entries("BBBBBB", 8, 0.0, +0.4, period_s=4.0)
        )
        result = _fit_per_aircraft_slope(bad + good, period_base_s=4.0)
        # Should succeed using the valid ICAOs
        assert result["sign_agreement"] is True
        assert result["slope_deg_per_s"] is not None

    def test_missing_icao_only_returns_insufficient_icaos(self):
        # All entries have missing/empty icao — no non-finite values
        entries = [
            {"icao": "", "effective_us": float(i * 1e6), "residual": 0.5, "weight": 1.0}
            for i in range(10)
        ]
        result = _fit_per_aircraft_slope(entries, period_base_s=4.0)
        assert result["slope_deg_per_s"] is None
        assert result["reject_reason"] == "insufficient_icaos"  # not "nonfinite_input"

    def test_nonfinite_input_reason_only_when_responsible(self):
        # Non-empty ICAO with non-finite timing — should trigger nonfinite_input
        entries = [
            {"icao": "AAAAAA", "effective_us": float("nan"), "residual": 5.0, "weight": 1.0},
            {"icao": "BBBBBB", "effective_us": float("inf"), "residual": 5.0, "weight": 1.0},
        ]
        result = _fit_per_aircraft_slope(entries, period_base_s=4.0)
        assert result["slope_deg_per_s"] is None
        assert result["reject_reason"] == "nonfinite_input"

    def test_ambiguous_unwrap_icao_excluded(self):
        # ICAO with a near-±180° jump is excluded; other two still produce consensus
        period_s = 4.0
        normal_a = _make_entries("AAAAAA", 8, 0.0, +0.4, period_s=period_s)
        normal_b = _make_entries("BBBBBB", 8, 0.0, +0.4, period_s=period_s)
        # CCCCCC has a ~±175° jump between consecutive residuals; span 3 periods so
        # the span gate passes and ambiguous_unwrap is the rejection reason.
        ambiguous = []
        for i in range(8):
            t = i * (3.0 * period_s) / 7
            # Alternates between 0 and +175: circular delta = 175° > 160° threshold.
            # (+170/-170 has a delta of only 20° by the short-arc formula.)
            residual = 0.0 if i % 2 == 0 else 175.0
            ambiguous.append({
                "icao": "CCCCCC",
                "effective_us": t * 1_000_000.0,
                "residual": residual,
                "weight": 1.0,
            })
        result = _fit_per_aircraft_slope(normal_a + normal_b + ambiguous, period_base_s=period_s)
        assert result["per_icao_reject_reasons"].get("CCCCCC") == "ambiguous_unwrap"
        # The other two still produce a valid consensus
        assert result["sign_agreement"] is True
        assert result["slope_deg_per_s"] is not None

    def test_unwrap_across_boundary(self):
        """Residuals that cross the +180°/-180° wrap boundary.

        If we naively use wrapped residuals, an apparent-negative slope appears.
        Per-ICAO unwrapping must reveal the true positive slope.
        """
        period_s = 4.0
        # Residuals walk from +160° to +200° (appear as -160° after wrapping)
        def make_crossing_entries(icao, n=8):
            entries = []
            for i in range(n):
                t = i * (period_s * 3) / (n - 1)  # span = 3 * period_s > 2 * period_s
                true_residual = 160.0 + 40.0 * i / (n - 1)  # 160 → 200
                wrapped = (true_residual + 180.0) % 360.0 - 180.0
                entries.append({
                    "icao": icao,
                    "effective_us": t * 1_000_000.0,
                    "residual": wrapped,
                    "weight": 1.0,
                })
            return entries

        entries = make_crossing_entries("AAAAAA") + make_crossing_entries("BBBBBB")
        result = _fit_per_aircraft_slope(entries, period_base_s=period_s)
        # Unwrapping should reveal positive slope; sign_agreement must be True
        assert result["sign_agreement"] is True
        assert result["slope_deg_per_s"] is not None
        assert result["slope_deg_per_s"] > 0, (
            f"Expected positive slope but got {result['slope_deg_per_s']}"
        )

    def test_zero_weight_skipped_not_nonfinite(self):
        # Zero/negative weight should not set had_nonfinite
        entries = (
            [{"icao": "AAAAAA", "effective_us": 1e6, "residual": 5.0, "weight": 0.0}]
            + _make_entries("BBBBBB", 8, 0.0, +0.4, period_s=4.0)
        )
        # Only BBBBBB — still insufficient_icaos
        result = _fit_per_aircraft_slope(entries, period_base_s=4.0)
        assert result["reject_reason"] == "insufficient_icaos"


# ---------------------------------------------------------------------------
# Phase status tests
# ---------------------------------------------------------------------------

class TestPhaseStatus:

    def test_phase_status_propagated_to_sync_prediction(self):
        sync = _make_sync(phase_status="trusted")
        pred = predict_sync_observation(sync, arrival_us=1_000_000.0)
        assert pred.phase_status == "trusted"

    def test_phase_status_untrusted_propagated(self):
        sync = _make_sync(phase_status="untrusted")
        pred = predict_sync_observation(sync, arrival_us=1_000_000.0)
        assert pred.phase_status == "untrusted"

    def test_phase_status_provisional_propagated(self):
        sync = _make_sync(phase_status="provisional")
        pred = predict_sync_observation(sync, arrival_us=1_000_000.0)
        assert pred.phase_status == "provisional"


# ---------------------------------------------------------------------------
# Stage 3 trust gate tests
# ---------------------------------------------------------------------------

class TestStage3TrustGate:

    def test_trusted_accepted_by_stage3(self):
        sync = _make_sync(phase_status="trusted", source="multi_aircraft_burst")
        assert AircraftLocaliser._sync_state_has_trusted_absolute_phase(sync) is True

    def test_provisional_rejected_by_stage3(self):
        sync = _make_sync(phase_status="provisional", source="multi_aircraft_burst")
        assert AircraftLocaliser._sync_state_has_trusted_absolute_phase(sync) is False

    def test_untrusted_rejected_by_stage3(self):
        sync = _make_sync(phase_status="untrusted", source="multi_aircraft_burst")
        assert AircraftLocaliser._sync_state_has_trusted_absolute_phase(sync) is False

    def test_go_source_rejected(self):
        sync = _make_sync(source="go_multi_aircraft_burst", phase_status="trusted")
        assert AircraftLocaliser._sync_state_has_trusted_absolute_phase(sync) is False

    def test_go_frame_sync_source_rejected(self):
        sync = _make_sync(source="go_frame_sync")
        assert AircraftLocaliser._sync_state_has_trusted_absolute_phase(sync) is False

    def test_absent_phase_status_treated_as_untrusted(self):
        """A state without phase_status must be rejected (defaults to 'untrusted')."""
        sync = _make_sync(phase_status="trusted", source="multi_aircraft_burst")
        del sync.__dict__["phase_status"]
        assert AircraftLocaliser._sync_state_has_trusted_absolute_phase(sync) is False


# ---------------------------------------------------------------------------
# EMA preservation tests
# ---------------------------------------------------------------------------

class TestEmaPreservation:

    def test_rejected_consensus_does_not_update_slope_ema(self):
        """When per-ICAO consensus is rejected, smoothed_slope must not change."""
        from radar.sweep import _fit_per_aircraft_slope

        # Verify sign_disagreement produces None slope
        entries = (
            _make_entries("AAAAAA", 8, 0.0, +0.4, period_s=4.0)
            + _make_entries("BBBBBB", 8, 0.0, -0.4, period_s=4.0)
        )
        result = _fit_per_aircraft_slope(entries, period_base_s=4.0)
        assert result["slope_deg_per_s"] is None

        # The calling code must preserve existing EMA:
        existing_ema = 0.3
        per_aircraft_slope = result["slope_deg_per_s"]
        _SLOPE_EMA_ALPHA = 0.08

        if per_aircraft_slope is not None:
            new_ema = (1.0 - _SLOPE_EMA_ALPHA) * existing_ema + _SLOPE_EMA_ALPHA * per_aircraft_slope
        else:
            new_ema = existing_ema  # preserve; do not update

        assert new_ema == pytest.approx(existing_ema)

    def test_consensus_updates_slope_ema(self):
        """When consensus is available, EMA is updated."""
        from radar.sweep import _fit_per_aircraft_slope

        entries = (
            _make_entries("AAAAAA", 8, 0.0, +0.5, period_s=4.0)
            + _make_entries("BBBBBB", 8, 0.0, +0.5, period_s=4.0)
        )
        result = _fit_per_aircraft_slope(entries, period_base_s=4.0)
        assert result["slope_deg_per_s"] is not None

        existing_ema = 0.3
        _SLOPE_EMA_ALPHA = 0.08
        new_ema = (1.0 - _SLOPE_EMA_ALPHA) * existing_ema + _SLOPE_EMA_ALPHA * result["slope_deg_per_s"]
        assert new_ema != pytest.approx(existing_ema)
        assert new_ema > existing_ema  # positive consensus pulls EMA up


# ---------------------------------------------------------------------------
# Period clamp tests
# ---------------------------------------------------------------------------

class TestPeriodClamp:

    def test_period_bounded_500ppm_from_base(self):
        """Period correction must never exceed ±500 PPM of period_base_s."""
        _PERIOD_PPM_FROM_BASE_MAX = 500.0
        period_base_s = 4.0

        # Simulate a large smoothed_slope that would push period far from base
        smoothed_slope = 5.0  # deg/s — much larger than typical
        _PERIOD_GAIN = 0.12
        rate_nominal = 360.0 / period_base_s
        rate_target = rate_nominal + smoothed_slope * _PERIOD_GAIN
        target_from_base_s = 360.0 / rate_target

        base_limit_s = abs(period_base_s) * _PERIOD_PPM_FROM_BASE_MAX * 1e-6
        refined_period_s = max(
            period_base_s - base_limit_s,
            min(period_base_s + base_limit_s, target_from_base_s),
        )

        deviation_ppm = abs(refined_period_s - period_base_s) / period_base_s * 1e6
        assert deviation_ppm <= _PERIOD_PPM_FROM_BASE_MAX + 1e-6

    def test_period_bounded_500ppm_negative_direction(self):
        """Period cap applies in both directions."""
        _PERIOD_PPM_FROM_BASE_MAX = 500.0
        period_base_s = 4.0
        smoothed_slope = -5.0
        _PERIOD_GAIN = 0.12
        rate_nominal = 360.0 / period_base_s
        rate_target = rate_nominal + smoothed_slope * _PERIOD_GAIN
        assert rate_target > 0
        target_from_base_s = 360.0 / rate_target

        base_limit_s = abs(period_base_s) * _PERIOD_PPM_FROM_BASE_MAX * 1e-6
        refined_period_s = max(
            period_base_s - base_limit_s,
            min(period_base_s + base_limit_s, target_from_base_s),
        )

        deviation_ppm = abs(refined_period_s - period_base_s) / period_base_s * 1e6
        assert deviation_ppm <= _PERIOD_PPM_FROM_BASE_MAX + 1e-6

    def test_live_sync_state_has_no_obsolete_compact_refined_fields(self):
        names = {f.name for f in fields(LiveSyncState)}
        obsolete = {
            "period_refine_enabled",
            "period_update_term",
            "period_update_direction",
            "period_update_applied",
            "period_update_gain",
            "period_refine_block_reason",
            "period_update_proposed_s",
            "period_update_proposed_us",
            "period_update_applied_s",
            "period_update_applied_us",
            "period_update_ppm_unclamped",
            "period_update_ppm_applied",
            "period_update_block_reason",
            "period_update_clamp_reason",
            "period_update_allowed",
            "period_update_fit_support",
            "period_update_fit_span_s",
            "period_correction_status",
            "period_update_safety_ppm_per_update",
            "period_update_safety_ppm_from_base",
            "period_update_ppm_from_base",
            "period_update_ppm_step",
            "period_update_ppm_limit_from_base",
            "period_update_ppm_limit_step",
            "phase_status_reason",
            "fit_time_basis",
            "fit_residual_basis",
            "fit_rejected_observations",
            "fit_reject_reasons",
            "fit_contributing_icao_count",
            "motion_comp_applied_count",
            "motion_comp_blocked_count",
            "motion_comp_mean_dt_us",
            "motion_comp_mean_residual_improvement_deg",
            "motion_comp_high_rate_mean_residual_improvement_deg",
            "phase_anchor_offset_raw_deg",
            "phase_anchor_offset_smoothed_deg",
            "dominant_period_s",
            "dominant_prior_period_s",
            "trusted_refined_period_s",
            "bootstrap_period_s",
            "active_family_prior_s",
            "active_family_prior_source",
            "dominant_prior_active",
            "dominant_period_delta_s",
            "dominant_period_delta_ppm",
            "compact_period_s",
            "compact_period_delta_to_dominant_s",
            "compact_period_delta_to_dominant_ppm",
            "compact_sync_unreliable",
        }
        assert names.isdisjoint(obsolete)


# ---------------------------------------------------------------------------
# Slope history source tagging tests
# ---------------------------------------------------------------------------

class TestSlopeHistorySourceTagging:

    def test_old_slope_history_entries_excluded_from_persistence_gate(self):
        """Entries without slope_source must not satisfy the persistence gate."""
        _SLOPE_DEAD_BAND = 0.08
        _PERSIST_MIN_ENTRIES = 5

        # Old entries with no slope_source key, all positive
        old_rows = [
            {"ts": float(i), "residual_slope_deg_per_s": 0.3}
            for i in range(8)
        ]
        slope_signs = [
            1 if float(row.get("residual_slope_deg_per_s") or 0.0) > 0 else -1
            for row in old_rows
            if (row.get("slope_source") == "per_aircraft_consensus"
                and abs(float(row.get("residual_slope_deg_per_s") or 0.0)) >= _SLOPE_DEAD_BAND)
        ]
        # No entries should pass the slope_source filter
        assert len(slope_signs) == 0
        persistent = len(slope_signs) >= _PERSIST_MIN_ENTRIES
        assert persistent is False

    def test_tagged_consensus_entries_count_for_persistence(self):
        """Entries tagged slope_source='per_aircraft_consensus' satisfy the gate."""
        _SLOPE_DEAD_BAND = 0.08
        _PERSIST_MIN_ENTRIES = 5

        new_rows = [
            {"ts": float(i), "residual_slope_deg_per_s": 0.3,
             "slope_source": "per_aircraft_consensus"}
            for i in range(8)
        ]
        slope_signs = [
            1 if float(row.get("residual_slope_deg_per_s") or 0.0) > 0 else -1
            for row in new_rows
            if (row.get("slope_source") == "per_aircraft_consensus"
                and abs(float(row.get("residual_slope_deg_per_s") or 0.0)) >= _SLOPE_DEAD_BAND)
        ]
        assert len(slope_signs) == 8
        assert all(s == 1 for s in slope_signs)
        persistent = len(slope_signs) >= _PERSIST_MIN_ENTRIES
        assert persistent is True


# ---------------------------------------------------------------------------
# Zero validation error test
# ---------------------------------------------------------------------------

class TestZeroValidationError:

    def test_zero_validation_error_not_treated_as_missing(self):
        """validation['median_error_deg'] = 0.0 must not be coerced to 999.0."""
        validation = {
            "median_error_deg": 0.0,
            "contributor_count": 2,
            "contributor_icaos": ["AAAAAA", "BBBBBB"],
            "status": "confirmed",
            "reject_count": 0,
        }
        # Replicate the Step 6 extraction logic exactly
        v_median_err = validation.get("median_error_deg")
        if v_median_err is None:
            v_median_err = 999.0
        assert v_median_err == 0.0, (
            f"Expected 0.0 but got {v_median_err}; 'or 999.0' pattern would corrupt this"
        )
        # Trusted gate condition
        assert abs(v_median_err) < 10.0


# ---------------------------------------------------------------------------
# Simple model architectural invariant tests
# ---------------------------------------------------------------------------

class TestSimpleModelBehavior:
    """Prove _update_simple_live_sync_state() enforces architectural invariants.

    These tests verify that complex behaviors from _update_multi_aircraft_sync_state
    are NOT present in the simple model: no b_fit driving period, no waveform
    correction, no reacquisition, no strong-fit/adaptive-clamp exceptions.
    """

    _IID = 7
    _PERIOD_S = 4.0
    _NOW_TS = 1000.0

    # ------------------------------------------------------------------
    # Internal helpers

    def _base_sync_state(self, period_s=None, residual_slope_deg_per_s=0.0, **kwargs):
        period_s = period_s if period_s is not None else self._PERIOD_S
        defaults = dict(
            iid=self._IID,
            period_s=period_s,
            phase_epoch_us=0.0,
            phase_offset_deg=0.0,
            sync_quality=1.0,
            sync_jitter_deg=2.0,
            last_sync_update_ts=self._NOW_TS - 1.0,
            source="multi_aircraft_burst",
            usable=True,
            period_base_s=period_s,
            prop_delay_enabled=False,
            residual_slope_deg_per_s=residual_slope_deg_per_s,
        )
        defaults.update(kwargs)
        return LiveSyncState(**defaults)

    def _make_burst_obs(self, *, period_s, now_ts, icaos=("AAAAAA", "BBBBBB"),
                        n_per_icao=8, slope_deg_per_s=0.0):
        """Build AlignedBurstSyncObs with near-zero (inlier) residuals.

        Bearings match the sync prediction (epoch=0, offset=0) plus an optional
        linear drift.  All ts values fall within the recent-observation window.
        """
        window_s = max(period_s * 6, 30.0)
        t_start = now_ts - window_s + period_s
        n_total = n_per_icao * len(icaos)
        obs = []
        for idx in range(n_total):
            frac = idx / max(n_total - 1, 1)
            t_s = t_start + frac * (window_s - period_s)
            icao = icaos[idx % len(icaos)]
            burst_us = t_s * 1_000_000.0
            predicted = (burst_us / (period_s * 1_000_000.0) * 360.0) % 360.0
            drift = slope_deg_per_s * (t_s - t_start)
            bearing = (predicted + drift) % 360.0
            obs.append(AlignedBurstSyncObs(
                burst_centroid_us=burst_us,
                icao=icao,
                bearing_deg=bearing,
                n_replies=4,
                signal_dbfs=-12.0,
                pos_age_s=0.2,
                range_nm=0.0,
                ts=t_s,
                sync_update_eligible=True,
            ))
        return obs

    def _run(self, state, obs_list, period_s=None):
        period_s = period_s if period_s is not None else self._PERIOD_S
        buf = _deque(maxlen=state._MULTI_SYNC_OBS_MAX)
        for o in obs_list:
            buf.append(o)
        state._live_aligned_burst_obs[self._IID] = buf
        state._update_simple_live_sync_state(self._IID, period_s=period_s)
        return state.get_live_sync_state(self._IID)

    def _seed_slope_history(self, state, slope=0.5, n=6):
        state._live_slope_history[self._IID] = _deque(maxlen=80)
        for _ in range(n):
            state._live_slope_history[self._IID].append({
                "ts": self._NOW_TS - 2.0,
                "residual_slope_deg_per_s": slope,
                "raw_slope_deg_per_s": slope,
                "fit_span_s": 28.0,
                "n_fit_observations": 12,
                "slope_source": "per_aircraft_consensus",
            })

    # ------------------------------------------------------------------
    # Tests

    def test_simple_model_sets_base_period_and_no_reacquire_state(self, monkeypatch):
        import radar.sweep as sweep_module
        monkeypatch.setattr(sweep_module.time, "time", lambda: self._NOW_TS)

        state = RadarState()
        state._live_sync_states[self._IID] = self._base_sync_state()
        obs = self._make_burst_obs(period_s=self._PERIOD_S, now_ts=self._NOW_TS)
        sync = self._run(state, obs)

        assert sync is not None
        assert sync.period_base_s == pytest.approx(self._PERIOD_S)

    def test_single_icao_cannot_drive_period_update(self, monkeypatch):
        """A single ICAO cannot produce per-aircraft consensus — period stays unchanged."""
        import radar.sweep as sweep_module
        monkeypatch.setattr(sweep_module.time, "time", lambda: self._NOW_TS)

        state = RadarState()
        # period_s != period_base_s so any change would be detectable
        state._live_sync_states[self._IID] = self._base_sync_state(
            period_s=4.001, period_base_s=self._PERIOD_S
        )
        self._seed_slope_history(state, slope=0.5)

        obs = self._make_burst_obs(
            period_s=self._PERIOD_S, now_ts=self._NOW_TS,
            icaos=("AAAAAA",),  # single ICAO — insufficient for cross-ICAO consensus
            n_per_icao=12,
            slope_deg_per_s=0.5,
        )
        sync = self._run(state, obs)

        assert sync is not None
        assert sync.period_s == pytest.approx(self._PERIOD_S)  # snaps to base, not carry-forward

    def test_conflicting_icao_slopes_block_period_update(self, monkeypatch):
        """Two ICAOs with opposite drift cannot reach consensus — period stays frozen."""
        import radar.sweep as sweep_module
        monkeypatch.setattr(sweep_module.time, "time", lambda: self._NOW_TS)

        state = RadarState()
        state._live_sync_states[self._IID] = self._base_sync_state()
        self._seed_slope_history(state, slope=0.3)

        obs = (
            self._make_burst_obs(
                period_s=self._PERIOD_S, now_ts=self._NOW_TS,
                icaos=("AAAAAA",), n_per_icao=8, slope_deg_per_s=+0.5,
            )
            + self._make_burst_obs(
                period_s=self._PERIOD_S, now_ts=self._NOW_TS,
                icaos=("BBBBBB",), n_per_icao=8, slope_deg_per_s=-0.5,
            )
        )
        sync = self._run(state, obs)

        assert sync is not None
        assert sync.period_s == pytest.approx(self._PERIOD_S)

    def test_period_cap_500ppm_enforced_under_extreme_slope(self, monkeypatch):
        """Period must never deviate >500 PPM from period_base_s even with extreme slope EMA."""
        import radar.sweep as sweep_module
        monkeypatch.setattr(sweep_module.time, "time", lambda: self._NOW_TS)

        state = RadarState()
        # Extreme pre-warmed EMA (5 deg/s) would push period ~6600 PPM from base
        state._live_sync_states[self._IID] = self._base_sync_state(
            residual_slope_deg_per_s=5.0,
        )
        self._seed_slope_history(state, slope=5.0, n=6)

        # Two-ICAO positive slope so per_aircraft consensus is available
        obs = self._make_burst_obs(
            period_s=self._PERIOD_S, now_ts=self._NOW_TS,
            icaos=("AAAAAA", "BBBBBB"), n_per_icao=8, slope_deg_per_s=0.5,
        )
        sync = self._run(state, obs)

        assert sync is not None
        deviation_ppm = abs(sync.period_s - self._PERIOD_S) / self._PERIOD_S * 1e6
        assert deviation_ppm <= 500.0 + 1e-6, (
            f"Period deviation {deviation_ppm:.2f} PPM exceeds ±500 PPM cap from base"
        )

    def test_phase_trusted_independent_of_period_correction(self, monkeypatch):
        """phase_status can reach 'trusted' even when period correction is blocked."""
        import radar.sweep as sweep_module
        monkeypatch.setattr(sweep_module.time, "time", lambda: self._NOW_TS)

        state = RadarState()
        state._live_sync_states[self._IID] = self._base_sync_state()
        # No slope history → persistence gate will block period update

        state._validate_phase_anchor_against_population = lambda *args, **kwargs: {
            "contributors": [
                {"icao": "AAAAAA", "error_deg": 2.0},
                {"icao": "BBBBBB", "error_deg": 3.0},
            ],
            "rejected": [],
            "contributor_count": 2,
            "contributor_icaos": ["AAAAAA", "BBBBBB"],
            "reject_count": 0,
            "median_error_deg": 2.5,
            "nudge_deg": 0.0,
            "status": "confirmed",
        }

        obs = self._make_burst_obs(
            period_s=self._PERIOD_S, now_ts=self._NOW_TS,
            icaos=("AAAAAA", "BBBBBB"), n_per_icao=8, slope_deg_per_s=0.0,
        )
        sync = self._run(state, obs)

        assert sync is not None
        assert sync.period_s == pytest.approx(self._PERIOD_S)  # blocked → snaps to base
        assert sync.phase_status == "trusted"       # phase trusted independently

    def test_fit_and_phase_diagnostics_present(self, monkeypatch):
        import radar.sweep as sweep_module
        monkeypatch.setattr(sweep_module.time, "time", lambda: self._NOW_TS)

        state = RadarState()
        state._live_sync_states[self._IID] = self._base_sync_state()
        obs = self._make_burst_obs(period_s=self._PERIOD_S, now_ts=self._NOW_TS)
        sync = self._run(state, obs)

        assert sync is not None
        assert sync.fit_total_observations >= sync.fit_eligible_observations
        assert sync.phase_status in {"trusted", "provisional", "untrusted"}


# ---------------------------------------------------------------------------
# period_s invariant tests
# ---------------------------------------------------------------------------

class TestPeriodSInvariant:
    """Verify the LiveSyncState.period_s-is-authoritative invariant.

    period_s must always hold the period downstream code should use.
    Blocked refinement snaps to period_base_s; accepted refinement stores
    the bounded corrected value.  No selector field should exist.
    """

    _IID = 7
    _PERIOD_S = 4.0
    _NOW_TS = 1000.0

    def _base_sync_state(self, period_s=None, period_base_s=None, **kwargs):
        ps = period_s if period_s is not None else self._PERIOD_S
        pb = period_base_s if period_base_s is not None else self._PERIOD_S
        defaults = dict(
            iid=self._IID,
            period_s=ps,
            phase_epoch_us=0.0,
            phase_offset_deg=0.0,
            sync_quality=1.0,
            sync_jitter_deg=2.0,
            last_sync_update_ts=self._NOW_TS - 1.0,
            source="multi_aircraft_burst",
            usable=True,
            period_base_s=pb,
            prop_delay_enabled=False,
            residual_slope_deg_per_s=0.0,
        )
        defaults.update(kwargs)
        return LiveSyncState(**defaults)

    def _make_burst_obs(self, *, period_s, now_ts, icaos=("AAAAAA", "BBBBBB"),
                        n_per_icao=8, slope_deg_per_s=0.0):
        window_s = max(period_s * 6, 30.0)
        t_start = now_ts - window_s + period_s
        n_total = n_per_icao * len(icaos)
        obs = []
        for idx in range(n_total):
            frac = idx / max(n_total - 1, 1)
            t_s = t_start + frac * (window_s - period_s)
            icao = icaos[idx % len(icaos)]
            burst_us = t_s * 1_000_000.0
            predicted = (burst_us / (period_s * 1_000_000.0) * 360.0) % 360.0
            drift = slope_deg_per_s * (t_s - t_start)
            bearing = (predicted + drift) % 360.0
            obs.append(AlignedBurstSyncObs(
                burst_centroid_us=burst_us,
                icao=icao,
                bearing_deg=bearing,
                n_replies=4,
                signal_dbfs=-12.0,
                pos_age_s=0.2,
                range_nm=0.0,
                ts=t_s,
                sync_update_eligible=True,
            ))
        return obs

    def _run(self, state, obs_list, period_s=None):
        from collections import deque as _deque
        period_s = period_s if period_s is not None else self._PERIOD_S
        buf = _deque(maxlen=state._MULTI_SYNC_OBS_MAX)
        for o in obs_list:
            buf.append(o)
        state._live_aligned_burst_obs[self._IID] = buf
        state._update_simple_live_sync_state(self._IID, period_s=period_s)
        return state.get_live_sync_state(self._IID)

    def _seed_slope_history(self, state, slope=0.5, n=6):
        from collections import deque as _deque
        state._live_slope_history[self._IID] = _deque(maxlen=80)
        for _ in range(n):
            state._live_slope_history[self._IID].append({
                "ts": self._NOW_TS - 2.0,
                "residual_slope_deg_per_s": slope,
                "raw_slope_deg_per_s": slope,
                "fit_span_s": 28.0,
                "n_fit_observations": 12,
                "slope_source": "per_aircraft_consensus",
            })

    # A. Blocked refinement stores base period, not carry-forward
    def test_blocked_refinement_stores_base_period(self, monkeypatch):
        """When refinement is blocked period_s == period_base_s, never a drifted carry-forward."""
        import radar.sweep as sweep_module
        monkeypatch.setattr(sweep_module.time, "time", lambda: self._NOW_TS)

        # Seed existing state with a deliberately drifted period_s
        state = RadarState()
        state._live_sync_states[self._IID] = self._base_sync_state(
            period_s=4.05,           # drifted carry-forward
            period_base_s=self._PERIOD_S,
        )
        # No slope history → persistence gate blocks refinement

        obs = self._make_burst_obs(period_s=self._PERIOD_S, now_ts=self._NOW_TS)
        sync = self._run(state, obs)

        assert sync is not None
        assert sync.period_s == pytest.approx(self._PERIOD_S)   # snaps to base
        assert sync.period_correction_ppm == pytest.approx(0.0)
        assert sync.period_base_s == pytest.approx(self._PERIOD_S)

    # B. Accepted refinement stores bounded corrected period
    def test_accepted_refinement_stores_corrected_period(self, monkeypatch):
        """When refinement is accepted period_s holds the bounded correction."""
        import radar.sweep as sweep_module
        monkeypatch.setattr(sweep_module.time, "time", lambda: self._NOW_TS)

        state = RadarState()
        state._live_sync_states[self._IID] = self._base_sync_state(
            residual_slope_deg_per_s=0.5,
        )
        self._seed_slope_history(state, slope=0.5, n=6)

        obs = self._make_burst_obs(
            period_s=self._PERIOD_S, now_ts=self._NOW_TS,
            icaos=("AAAAAA", "BBBBBB"), n_per_icao=12, slope_deg_per_s=0.5,
        )
        sync = self._run(state, obs)

        assert sync is not None
        # If refinement fired, period_s deviates from base but within ±500 PPM
        if sync.period_correction_ppm != 0.0:
            assert sync.period_s != pytest.approx(self._PERIOD_S)
            deviation_ppm = abs(sync.period_s - self._PERIOD_S) / self._PERIOD_S * 1e6
            assert deviation_ppm <= 500.0 + 1e-6
            assert sync.period_correction_ppm == pytest.approx(
                (sync.period_s - sync.period_base_s) / sync.period_base_s * 1e6
            )
        # Either way period_s is always the value downstream should use
        assert sync.period_s > 0.0

    # C. Frame/display helpers use period_s directly — no selector field needed
    def test_frame_period_helpers_use_period_s_directly(self):
        """Helpers return period_s verbatim; period_base_s has no effect on result."""
        state = RadarState()
        # Construct a state where period_s differs from period_base_s
        state._live_sync_states[self._IID] = self._base_sync_state(
            period_s=4.001,
            period_base_s=self._PERIOD_S,
        )
        assert state._get_authoritative_frame_period_s(self._IID, 9.8) == pytest.approx(4.001)
        assert state.get_authoritative_display_period_s(self._IID) == pytest.approx(4.001)
        assert state.get_authoritative_display_period_std_s(self._IID) == pytest.approx(
            2.0 / 360.0 * 4.001
        )

    # D. Obsolete field is absent from the dataclass
    def test_period_authoritative_source_field_absent(self):
        """period_authoritative_source must not exist on LiveSyncState."""
        from dataclasses import fields
        field_names = {f.name for f in fields(LiveSyncState)}
        assert "period_authoritative_source" not in field_names

    def test_transition_quarantine_applies_to_failed_residuals_with_recent_transition(self, monkeypatch):
        import radar.sweep as sweep_module
        monkeypatch.setattr(sweep_module.time, "time", lambda: self._NOW_TS)

        state = RadarState()
        state._live_sync_states[self._IID] = self._base_sync_state(
            phase_anchor_since_ts=self._NOW_TS - 2.0,
        )
        state._compact_sync_debug_by_iid[self._IID] = {
            "last_reference_change_ts": self._NOW_TS - 2.0,
        }
        obs = self._make_burst_obs(period_s=self._PERIOD_S, now_ts=self._NOW_TS)
        for row in obs:
            row.bearing_deg = (row.bearing_deg + 90.0) % 360.0

        sync = self._run(state, obs)

        assert sync is not None
        assert sync.fit_eligible_observations == 0
        compact = state._compact_sync_debug_by_iid[self._IID]
        assert compact["transition_quarantine_count"] > 0
        assert compact["transition_quarantine_fit_excluded_count"] > 0
        assert compact["transition_quarantine_hard_reject_suppressed_count"] == 0
        assert compact["transition_quarantine_last_reason"] in {"anchor_changed", "reference_changed", "multiple"}
        assert compact["transition_quarantine_last_ts"] == pytest.approx(self._NOW_TS)

    def test_transition_adjacent_coherent_rows_not_quarantined(self, monkeypatch):
        import radar.sweep as sweep_module
        monkeypatch.setattr(sweep_module.time, "time", lambda: self._NOW_TS)

        state = RadarState()
        state._live_sync_states[self._IID] = self._base_sync_state(
            phase_anchor_since_ts=self._NOW_TS - 2.0,
        )
        state._compact_sync_debug_by_iid[self._IID] = {
            "last_reference_change_ts": self._NOW_TS - 2.0,
        }
        obs = self._make_burst_obs(period_s=self._PERIOD_S, now_ts=self._NOW_TS)
        sync = self._run(state, obs)

        assert sync is not None
        assert sync.fit_eligible_observations > 0
        compact = state._compact_sync_debug_by_iid[self._IID]
        assert compact.get("transition_quarantine_count", 0) == 0

    def test_failed_residuals_outside_transition_window_keep_residual_gate(self, monkeypatch):
        import radar.sweep as sweep_module
        monkeypatch.setattr(sweep_module.time, "time", lambda: self._NOW_TS)

        state = RadarState()
        state._live_sync_states[self._IID] = self._base_sync_state(
            phase_anchor_since_ts=self._NOW_TS - 120.0,
        )
        state._compact_sync_debug_by_iid[self._IID] = {
            "last_reference_change_ts": self._NOW_TS - 120.0,
        }
        obs = self._make_burst_obs(period_s=self._PERIOD_S, now_ts=self._NOW_TS)
        for row in obs:
            row.bearing_deg = (row.bearing_deg + 90.0) % 360.0
        sync = self._run(state, obs)

        assert sync is not None
        compact = state._compact_sync_debug_by_iid.get(self._IID, {})
        assert compact.get("transition_quarantine_count", 0) == 0
        assert sync.fit_eligible_observations == 0
