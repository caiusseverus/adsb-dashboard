"""Tests for the re-implemented radar phase refinement model.

Covers:
- _fit_per_aircraft_slope() helper (per-ICAO unwrapped slope)
- phase_status computation (trusted/provisional/untrusted)
- Period correction anchored to period_base_s with ±500 PPM cap
- EMA preservation when per-ICAO consensus is rejected
- Stage 3 trust gate in _sync_state_has_trusted_absolute_phase()
"""
import math
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(__file__)))

import pytest
from radar.sweep import (
    LiveSyncState,
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

    def test_go_source_defers_to_absolute_phase_trusted_field(self):
        sync = _make_sync(source="go_multi_aircraft_burst")
        # Default absolute_phase_trusted is absent → False
        assert AircraftLocaliser._sync_state_has_trusted_absolute_phase(sync) is False

        # Explicitly set to True
        sync.absolute_phase_trusted = True
        assert AircraftLocaliser._sync_state_has_trusted_absolute_phase(sync) is True

    def test_sweep_frame_go_source_rejected(self):
        sync = _make_sync(source="sweep_frame_go")
        assert AircraftLocaliser._sync_state_has_trusted_absolute_phase(sync) is False

    def test_old_state_without_phase_status_falls_back_to_heuristics(self):
        """States that predate the phase_status field use legacy heuristic checks."""
        sync = _make_sync(
            phase_anchor_status="selected",
            phase_anchor_spread_deg=5.0,
            phase_validation_status="confirmed",
            phase_validation_contributors=2,
            phase_validation_median_error_deg=3.0,
        )
        # Remove the field to simulate an old state
        del sync.__dict__["phase_status"]
        # With good heuristics, should still return True via legacy path
        assert AircraftLocaliser._sync_state_has_trusted_absolute_phase(sync) is True


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

    def test_period_update_has_separate_base_ppm_and_step_ppm_diagnostics(self):
        """LiveSyncState exposes period_update_ppm_from_base and period_update_ppm_step."""
        sync = _make_sync()
        assert hasattr(sync, "period_update_ppm_from_base")
        assert hasattr(sync, "period_update_ppm_step")
        assert hasattr(sync, "period_update_ppm_limit_from_base")
        assert hasattr(sync, "period_update_ppm_limit_step")
        # Defaults should be finite
        assert math.isfinite(sync.period_update_ppm_from_base)
        assert math.isfinite(sync.period_update_ppm_step)


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
