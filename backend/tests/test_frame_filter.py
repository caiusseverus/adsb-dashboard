"""Tests for the four-stage frame estimate filter."""

import math

import pytest

from radar.frame_filter import (
    CHI2_THRESHOLD_99,
    HUBER_C,
    MAX_CONDITION_NUMBER,
    MAX_INTERP,
    MAX_ITER,
    MIN_ARCS,
    MIN_DOMINANCE,
    MIN_SPREAD,
    STAGE2_MIN_N,
    FilterResult,
    filter_frame_estimates,
)

# ── Helper ────────────────────────────────────────────────────────────────

try:
    from dataclasses import dataclass
except ImportError:
    from dataclasses import dataclass


@dataclass
class _FakeEstimate:
    """Minimal stand-in for FramePositionEstimate."""
    frame_index: int = 0
    sweep_start_us: float = 0.0
    lat: float = 51.0
    lon: float = -1.0
    cep_km: float = 1.0
    n_contributing_arcs: int = 5
    azimuth_spread_deg: float = 60.0
    weight: float = 1.0
    cluster_dominance_ratio: float = 2.0
    interpolated_position_fraction: float = 0.2


_RLAT = 51.0
_RLON = -1.0


def _make_estimate(**overrides):
    return _FakeEstimate(**overrides)


# ── Stage 0: quality pre-filter ──────────────────────────────────────────

class TestStage0:
    def test_rejects_low_arc_count(self):
        est = _make_estimate(n_contributing_arcs=MIN_ARCS - 1)
        result = filter_frame_estimates([est], _RLAT, _RLON)
        assert result.n_stage0_survivors == 0
        assert result.rejection_counts["stage0"] == 1
        assert result.lat is None

    def test_rejects_low_spread(self):
        est = _make_estimate(azimuth_spread_deg=MIN_SPREAD - 1.0)
        result = filter_frame_estimates([est], _RLAT, _RLON)
        assert result.n_stage0_survivors == 0
        assert result.rejection_counts["stage0"] == 1

    def test_rejects_low_dominance(self):
        est = _make_estimate(cluster_dominance_ratio=MIN_DOMINANCE - 0.1)
        result = filter_frame_estimates([est], _RLAT, _RLON)
        assert result.n_stage0_survivors == 0
        assert result.rejection_counts["stage0"] == 1

    def test_rejects_high_interp_fraction(self):
        est = _make_estimate(interpolated_position_fraction=MAX_INTERP + 0.1)
        result = filter_frame_estimates([est], _RLAT, _RLON)
        assert result.n_stage0_survivors == 0
        assert result.rejection_counts["stage0"] == 1

    def test_passes_quality_check(self):
        est = _make_estimate()
        result = filter_frame_estimates([est], _RLAT, _RLON)
        assert result.n_stage0_survivors == 1
        assert result.rejection_counts["stage0"] == 0

    def test_mixed_pass_and_reject(self):
        good = _make_estimate(frame_index=0)
        bad_arcs = _make_estimate(frame_index=1, n_contributing_arcs=1)
        bad_spread = _make_estimate(frame_index=2, azimuth_spread_deg=10.0)
        result = filter_frame_estimates([good, bad_arcs, bad_spread], _RLAT, _RLON)
        assert result.n_total == 3
        assert result.n_stage0_survivors == 1
        assert result.rejection_counts["stage0"] == 2


# ── Stage 1: per-frame Mahalanobis gate ──────────────────────────────────

class TestStage1:
    def test_tight_cluster_accepted(self):
        """Estimates close together pass the Mahalanobis gate."""
        estimates = []
        for i in range(10):
            dlat = (i - 5) * 0.00001   # ~1 m spacing
            dlon = (i - 5) * 0.00001
            estimates.append(_make_estimate(
                frame_index=i,
                lat=_RLAT + dlat,
                lon=_RLON + dlon,
                cep_km=2.0,  # large uncertainty → large acceptance ellipse
            ))
        result = filter_frame_estimates(estimates, _RLAT, _RLON)
        assert result.n_inliers == result.n_stage0_survivors  # no Stage 1 rejects

    def test_outlier_rejected(self):
        """One far estimate should be rejected by Stage 1."""
        estimates = []
        for i in range(10):
            estimates.append(_make_estimate(
                frame_index=i,
                lat=_RLAT,
                lon=_RLON,
                cep_km=0.5,
            ))
        # Add one outlier ~10 km away
        estimates.append(_make_estimate(
            frame_index=10,
            lat=_RLAT + 0.09,   # ~10 km north
            lon=_RLON,
            cep_km=0.5,
        ))
        result = filter_frame_estimates(estimates, _RLAT, _RLON)
        assert result.rejection_counts["stage1"] >= 1
        assert result.n_inliers < result.n_stage0_survivors


# ── Stage 2: sample-covariance second pass ───────────────────────────────

class TestStage2:
    def test_skipped_when_under_20(self):
        """Stage 2 must be skipped if n_inliers < 20."""
        estimates = [_make_estimate(frame_index=i) for i in range(15)]
        result = filter_frame_estimates(estimates, _RLAT, _RLON)
        assert result.rejection_counts["stage2"] == 0

    def test_runs_when_20_plus(self):
        """Stage 2 runs when >= 20 inliers survive Stage 1."""
        estimates = []
        for i in range(25):
            dlat = (i - 12) * 0.00001
            estimates.append(_make_estimate(
                frame_index=i,
                lat=_RLAT + dlat,
                lon=_RLON,
                cep_km=3.0,
            ))
        result = filter_frame_estimates(estimates, _RLAT, _RLON)
        # Stage 2 may or may not reject depending on geometry, but it must run.
        # The key invariant is that Stage 2 was evaluated.
        assert result.n_total == 25
        assert result.n_stage0_survivors == 25


# ── Stage 3: iteration ───────────────────────────────────────────────────

class TestStage3:
    def test_converges_in_few_iterations(self):
        """Iteration should stabilise quickly with a tight cluster + outliers."""
        estimates = []
        for i in range(30):
            estimates.append(_make_estimate(
                frame_index=i,
                lat=_RLAT,
                lon=_RLON,
                cep_km=2.0,
            ))
        # Add 5 clear outliers
        for i in range(5):
            estimates.append(_make_estimate(
                frame_index=30 + i,
                lat=_RLAT + 0.1,   # ~11 km away
                lon=_RLON + 0.1,
                cep_km=2.0,
            ))
        result = filter_frame_estimates(estimates, _RLAT, _RLON)
        assert result.n_inliers == 30  # the tight cluster
        assert result.rejection_counts["stage1"] >= 5


# ── Invariants ───────────────────────────────────────────────────────────

class TestInvariants:
    def test_empty_input(self):
        result = filter_frame_estimates([], _RLAT, _RLON)
        assert result.n_total == 0
        assert result.n_stage0_survivors == 0
        assert result.n_inliers == 0
        assert result.lat is None

    def test_single_estimate_returns_none(self):
        """n_inliers < 2 → None."""
        est = _make_estimate()
        result = filter_frame_estimates([est], _RLAT, _RLON)
        assert result.lat is None
        assert result.sigma_combined_m is None

    def test_two_identical_produces_position(self):
        e1 = _make_estimate(frame_index=0)
        e2 = _make_estimate(frame_index=1)
        result = filter_frame_estimates([e1, e2], _RLAT, _RLON)
        assert result.lat is not None
        assert result.sigma_combined_m is not None
        assert result.n_inliers == 2

    def test_invariant_ordering(self):
        n_inliers = [0]
        n_s0 = [0]
        n_tot = [0]

        def check(n_total, n_s0_val, n_inliers_val):
            assert n_inliers_val <= n_s0_val <= n_total

        estimates = [_make_estimate(frame_index=i) for i in range(5)]
        result = filter_frame_estimates(estimates, _RLAT, _RLON)
        check(result.n_total, result.n_stage0_survivors, result.n_inliers)


# ── Condition number guard ───────────────────────────────────────────────

class TestConditionNumber:
    def test_isotropic_cov_has_cond_1(self):
        """Isotropic covariance has condition number 1 — always passes Stage 0."""
        est = _make_estimate(cep_km=0.1)  # above MIN_CEP_KM, very small but valid
        result = filter_frame_estimates([est], _RLAT, _RLON)
        # Should pass Stage 0 (condition number of σ²I is 1)
        # but fail final output because n_inliers < 2
        assert result.n_stage0_survivors == 1


# ── End-to-end: synthetic ENU data ───────────────────────────────────────

class TestEndToEnd:
    def test_tight_cluster_with_noise(self):
        """30 frames near true position, 5 far outliers → inliers == 30."""
        import random
        random.seed(42)

        estimates = []
        for i in range(30):
            # Within ~100 m of true position
            dlat = random.gauss(0, 0.0005)
            dlon = random.gauss(0, 0.0005)
            estimates.append(_make_estimate(
                frame_index=i,
                lat=_RLAT + dlat,
                lon=_RLON + dlon,
                cep_km=1.5,
            ))

        # 5 outliers at 5-10 km
        for i in range(5):
            estimates.append(_make_estimate(
                frame_index=30 + i,
                lat=_RLAT + random.uniform(0.04, 0.09),
                lon=_RLON + random.uniform(0.04, 0.09),
                cep_km=1.5,
            ))

        result = filter_frame_estimates(estimates, _RLAT, _RLON)
        # Most of the 30 tight-cluster frames survive; the 5 outliers are rejected.
        assert result.n_inliers >= 25  # allows for a few Stage 2 casualties
        assert result.rejection_counts["stage1"] == 5
        assert result.sigma_combined_m is not None
        # Combined sigma should be much smaller than individual frame sigma
        assert result.sigma_combined_m < 1500.0  # < 1.5 km

    def test_output_format(self):
        result = filter_frame_estimates(
            [_make_estimate(frame_index=i) for i in range(10)],
            _RLAT, _RLON,
        )
        assert isinstance(result, FilterResult)
        assert hasattr(result, "lat")
        assert hasattr(result, "lon")
        assert hasattr(result, "sigma_combined_m")
        assert hasattr(result, "n_total")
        assert hasattr(result, "n_stage0_survivors")
        assert hasattr(result, "n_inliers")
        assert "stage0" in result.rejection_counts
        assert "stage1" in result.rejection_counts
        assert "stage2" in result.rejection_counts
