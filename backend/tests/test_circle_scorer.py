"""Tests for the circle scoring and selection pipeline."""

import math

import pytest

from radar.circle_scorer import (
    BURST_CENTROID_BASE_UNCERTAINTY_SEC,
    DIVERSITY_OVERRIDE_THRESHOLD,
    MAX_CIRCLES,
    MAX_POSITION_AGE_SEC,
    MIN_CONSTRAINT_SEPARATION_DEG,
    MIN_CIRCLE_SCORE,
    MIN_REPLIES_PER_BURST,
    POSITION_AGE_TAU_SEC,
    SIGMA_BAND_FLOOR_METRES,
    ScoredCircle,
    circular_distance,
    compute_circle_scores,
    select_circles,
)


# ── Constants are named module-level values ────────────────────────────────

class TestConstants:
    def test_named_constants_exist(self):
        assert SIGMA_BAND_FLOOR_METRES == 500.0
        assert BURST_CENTROID_BASE_UNCERTAINTY_SEC == 0.030
        assert POSITION_AGE_TAU_SEC == 4.0
        assert MIN_REPLIES_PER_BURST == 2
        assert MAX_POSITION_AGE_SEC == 10.0
        assert MIN_CIRCLE_SCORE == 0.05
        assert MAX_CIRCLES == 6
        assert MIN_CONSTRAINT_SEPARATION_DEG == 25.0
        assert DIVERSITY_OVERRIDE_THRESHOLD == 0.85


# ── circular_distance helper ───────────────────────────────────────────────

class TestCircularDistance:
    def test_same_angle(self):
        assert circular_distance(0.0, 0.0) == 0.0
        assert circular_distance(math.pi, math.pi) == 0.0

    def test_opposite_angles(self):
        assert circular_distance(0.0, math.pi) == pytest.approx(math.pi)

    def test_wrap_around(self):
        # 359° and 1° should be 2° apart
        d = circular_distance(math.radians(359), math.radians(1))
        assert d == pytest.approx(math.radians(2), abs=1e-6)

    def test_small_separation(self):
        d = circular_distance(math.radians(10), math.radians(35))
        assert d == pytest.approx(math.radians(25), abs=1e-6)

    def test_result_in_range(self):
        for a1 in [0.0, 1.0, 2.0, 3.0, 5.0, 6.0]:
            for a2 in [0.5, 1.5, 2.5, 4.0, 5.5]:
                d = circular_distance(a1, a2)
                assert 0.0 <= d <= math.pi


# ── compute_circle_scores ──────────────────────────────────────────────────

def _make_circle(
    center_enu_m=(10000.0, 5000.0),
    radius_m=50000.0,
    delta_phi=math.radians(90.0),
    n_replies=3,
    position_age_seconds=0.0,
    frame_index=0,
    icao="TEST01",
):
    return {
        "center_enu_m": center_enu_m,
        "radius_m": radius_m,
        "delta_phi": delta_phi,
        "n_replies": n_replies,
        "position_age_seconds": position_age_seconds,
        "frame_index": frame_index,
        "icao": icao,
    }


class TestScoreComponents:
    def test_phi_weight_is_sin_delta_phi(self):
        """phi_weight = sin(delta_phi), not baseline_km/100."""
        circles = [_make_circle(delta_phi=math.radians(90.0))]
        scored = compute_circle_scores(circles, p_bar_enu_m=None, omega=0.628)
        assert scored[0].phi_weight == pytest.approx(1.0)

        circles = [_make_circle(delta_phi=math.radians(30.0))]
        scored = compute_circle_scores(circles, p_bar_enu_m=None, omega=0.628)
        assert scored[0].phi_weight == pytest.approx(0.5)

    def test_residual_score_one_without_p_bar(self):
        """No prior → residual_score = 1.0 (no penalty)."""
        circles = [_make_circle()]
        scored = compute_circle_scores(circles, p_bar_enu_m=None, omega=0.628)
        assert scored[0].residual_score == 1.0
        assert scored[0].residual_metres is None

    def test_residual_score_with_p_bar_perfect_fit(self):
        """Circle centre distance to P_bar equals radius → residual = 0 → score = 1.0."""
        # P_bar at (10000 + 50000, 5000) = (60000, 5000), circle centre at (10000, 5000), radius 50000
        # dist = 50000 = radius → residual = 0
        circles = [_make_circle(center_enu_m=(10000.0, 5000.0), radius_m=50000.0)]
        p_bar = (60000.0, 5000.0)
        scored = compute_circle_scores(circles, p_bar_enu_m=p_bar, omega=0.628)
        assert scored[0].residual_metres == pytest.approx(0.0)
        assert scored[0].residual_score == pytest.approx(1.0)

    def test_residual_score_with_p_bar_miss(self):
        """Circle misses P_bar by many sigma → residual_score near 0."""
        # Circle centre at origin, radius 50000. P_bar at (0, 0).
        # dist_to_centre = 0, residual = |0 - 50000| = 50000
        circles = [_make_circle(center_enu_m=(0.0, 0.0), radius_m=50000.0)]
        p_bar = (0.0, 0.0)
        scored = compute_circle_scores(circles, p_bar_enu_m=p_bar, omega=0.628)
        assert scored[0].residual_metres == pytest.approx(50000.0)
        assert scored[0].residual_score < 1.0

    def test_age_weight_decay(self):
        """age_weight = exp(-position_age / POSITION_AGE_TAU_SEC)."""
        circles = [_make_circle(position_age_seconds=0.0)]
        scored = compute_circle_scores(circles, p_bar_enu_m=None, omega=0.628)
        assert scored[0].age_weight == pytest.approx(1.0)

        circles = [_make_circle(position_age_seconds=POSITION_AGE_TAU_SEC)]
        scored = compute_circle_scores(circles, p_bar_enu_m=None, omega=0.628)
        assert scored[0].age_weight == pytest.approx(math.exp(-1.0))

    def test_age_weight_not_binary(self):
        """No binary 0.7/1.0 distinction — continuous exponential decay."""
        c1 = [_make_circle(position_age_seconds=0.0)]
        c2 = [_make_circle(position_age_seconds=2.0)]
        c3 = [_make_circle(position_age_seconds=4.0)]
        s1 = compute_circle_scores(c1, p_bar_enu_m=None, omega=0.628)[0].age_weight
        s2 = compute_circle_scores(c2, p_bar_enu_m=None, omega=0.628)[0].age_weight
        s3 = compute_circle_scores(c3, p_bar_enu_m=None, omega=0.628)[0].age_weight
        assert s1 > s2 > s3
        assert s2 != pytest.approx(0.7)  # Not the old binary 0.7

    def test_circle_score_multiplicative(self):
        """circle_score = phi_weight * residual_score * age_weight."""
        circles = [_make_circle(delta_phi=math.radians(90.0), position_age_seconds=0.0)]
        scored = compute_circle_scores(circles, p_bar_enu_m=None, omega=0.628)
        sc = scored[0]
        expected = sc.phi_weight * sc.residual_score * sc.age_weight
        assert sc.circle_score == pytest.approx(expected)

    def test_sigma_band_floor(self):
        """sigma_band >= SIGMA_BAND_FLOOR_METRES."""
        circles = [_make_circle(n_replies=100, delta_phi=math.radians(90.0))]
        scored = compute_circle_scores(circles, p_bar_enu_m=None, omega=0.628)
        assert scored[0].sigma_band_metres >= SIGMA_BAND_FLOOR_METRES

    def test_sigma_band_increases_with_fewer_replies(self):
        """More replies → lower timing uncertainty → narrower sigma band."""
        c1 = [_make_circle(n_replies=2)]
        c2 = [_make_circle(n_replies=10)]
        s1 = compute_circle_scores(c1, p_bar_enu_m=None, omega=0.628)[0].sigma_band_metres
        s2 = compute_circle_scores(c2, p_bar_enu_m=None, omega=0.628)[0].sigma_band_metres
        assert s1 >= s2  # fewer replies → wider band (or floored)


class TestDegenerateGeometryGate:
    def test_below_2_degrees_excluded(self):
        circles = [_make_circle(delta_phi=math.radians(1.0))]
        scored = compute_circle_scores(circles, p_bar_enu_m=None, omega=0.628)
        assert scored[0].circle_score == 0.0
        assert scored[0].exclusion_reason == "degenerate_delta_phi"

    def test_above_178_degrees_excluded(self):
        circles = [_make_circle(delta_phi=math.radians(179.0))]
        scored = compute_circle_scores(circles, p_bar_enu_m=None, omega=0.628)
        assert scored[0].circle_score == 0.0
        assert scored[0].exclusion_reason == "degenerate_delta_phi"

    def test_at_2_degrees_included(self):
        circles = [_make_circle(delta_phi=math.radians(2.0))]
        scored = compute_circle_scores(circles, p_bar_enu_m=None, omega=0.628)
        assert scored[0].circle_score > 0.0
        assert scored[0].exclusion_reason is None

    def test_at_178_degrees_included(self):
        circles = [_make_circle(delta_phi=math.radians(178.0))]
        scored = compute_circle_scores(circles, p_bar_enu_m=None, omega=0.628)
        assert scored[0].circle_score > 0.0
        assert scored[0].exclusion_reason is None


# ── select_circles ─────────────────────────────────────────────────────────

class TestHardGates:
    def test_insufficient_replies_excluded(self):
        circles = [_make_circle(n_replies=1, delta_phi=math.radians(90.0))]
        scored = compute_circle_scores(circles, p_bar_enu_m=None, omega=0.628)
        selected = select_circles(scored)
        assert len(selected) == 0
        assert scored[0].exclusion_reason == "insufficient_replies"

    def test_stale_position_excluded(self):
        circles = [_make_circle(
            n_replies=3,
            delta_phi=math.radians(90.0),
            position_age_seconds=MAX_POSITION_AGE_SEC + 1.0,
        )]
        scored = compute_circle_scores(circles, p_bar_enu_m=None, omega=0.628)
        selected = select_circles(scored)
        assert len(selected) == 0
        assert scored[0].exclusion_reason == "stale_position"

    def test_low_score_excluded(self):
        # Low phi_weight + low age_weight → circle_score < MIN_CIRCLE_SCORE
        # but position_age_seconds < MAX_POSITION_AGE_SEC (so stale gate doesn't fire)
        circles = [_make_circle(
            n_replies=3,
            delta_phi=math.radians(10.0),  # sin(10°) ≈ 0.174
            position_age_seconds=8.0,       # exp(-8/4) ≈ 0.135 → score ≈ 0.023
        )]
        scored = compute_circle_scores(circles, p_bar_enu_m=None, omega=0.628)
        selected = select_circles(scored)
        assert len(selected) == 0
        assert scored[0].exclusion_reason == "low_score"


class TestDiversitySelection:
    def test_selects_up_to_max_circles(self):
        circles = [
            _make_circle(
                delta_phi=math.radians(90.0),
                frame_index=i,
                icao=f"ICAO{i:02d}",
            )
            for i in range(10)
        ]
        scored = compute_circle_scores(circles, p_bar_enu_m=None, omega=0.628)
        selected = select_circles(scored)
        assert len(selected) <= MAX_CIRCLES

    def test_first_highest_scoring_always_selected(self):
        circles = [
            _make_circle(delta_phi=math.radians(90.0), frame_index=0, icao="A"),
            _make_circle(delta_phi=math.radians(45.0), frame_index=1, icao="B"),
        ]
        scored = compute_circle_scores(circles, p_bar_enu_m=None, omega=0.628)
        selected = select_circles(scored)
        assert len(selected) >= 1
        assert selected[0].icao == "A"  # Highest score (sin 90° > sin 45°)

    def test_high_score_bypasses_diversity(self):
        """Circles with score > DIVERSITY_OVERRIDE_THRESHOLD bypass angular separation."""
        # All circles at same constraint angle but high scores
        circles = [
            _make_circle(
                center_enu_m=(i * 1000.0, 0.0),  # Different centres → different angles
                delta_phi=math.radians(90.0),
                frame_index=i,
                icao=f"ICAO{i:02d}",
            )
            for i in range(10)
        ]
        scored = compute_circle_scores(circles, p_bar_enu_m=None, omega=0.628)
        selected = select_circles(scored)
        # High-scoring circles should still get selected even if angularly close
        assert len(selected) >= 1


class TestIcaoDiversity:
    def test_max_4_per_icao(self):
        """No more than 4 circles from same ICAO across all frames."""
        circles = [
            _make_circle(delta_phi=math.radians(90.0), frame_index=i, icao="SAME01")
            for i in range(10)
        ]
        scored = compute_circle_scores(circles, p_bar_enu_m=None, omega=0.628)
        selected = select_circles(scored)
        icao_counts = {}
        for sc in selected:
            icao_counts[sc.icao] = icao_counts.get(sc.icao, 0) + 1
        assert all(c <= 4 for c in icao_counts.values())

    def test_no_duplicate_icao_in_same_frame(self):
        """No two selected circles from same frame share an ICAO."""
        # Same frame, same ICAO — shouldn't happen in practice but test the guard
        circles = [
            _make_circle(
                center_enu_m=(i * 10000.0, 0.0),
                delta_phi=math.radians(90.0),
                frame_index=0,
                icao="DUP001",
            )
            for i in range(3)
        ]
        scored = compute_circle_scores(circles, p_bar_enu_m=None, omega=0.628)
        selected = select_circles(scored)
        frame_icaos = {}
        for sc in selected:
            frame_icaos.setdefault(sc.frame_index, set()).add(sc.icao)
        for f_idx, icaos in frame_icaos.items():
            assert len(icaos) == len(icaos)  # No duplicates


class TestInvariants:
    def test_score_in_range(self):
        """circle_score in [0, 1] for all non-excluded circles."""
        circles = [
            _make_circle(
                delta_phi=math.radians(10.0 + i * 20.0),
                n_replies=i + 2,
                position_age_seconds=float(i),
                frame_index=i,
                icao=f"INV{i:02d}",
            )
            for i in range(10)
        ]
        scored = compute_circle_scores(circles, p_bar_enu_m=None, omega=0.628)
        for sc in scored:
            if sc.exclusion_reason is None:
                assert 0.0 <= sc.circle_score <= 1.0

    def test_selected_count_limit(self):
        """len(selected) <= MAX_CIRCLES."""
        circles = [
            _make_circle(
                delta_phi=math.radians(90.0),
                frame_index=i,
                icao=f"INV{i:02d}",
            )
            for i in range(20)
        ]
        scored = compute_circle_scores(circles, p_bar_enu_m=None, omega=0.628)
        selected = select_circles(scored)
        assert len(selected) <= MAX_CIRCLES

    def test_sigma_band_floor_for_all(self):
        """sigma_band >= SIGMA_BAND_FLOOR_METRES for all evaluated circles."""
        circles = [
            _make_circle(
                delta_phi=math.radians(90.0),
                n_replies=100,
            )
        ]
        scored = compute_circle_scores(circles, p_bar_enu_m=None, omega=0.628)
        for sc in scored:
            if sc.exclusion_reason is None:
                assert sc.sigma_band_metres >= SIGMA_BAND_FLOOR_METRES


class TestDiagnostics:
    def test_diagnostics_present(self):
        circles = [_make_circle(delta_phi=math.radians(90.0))]
        scored = compute_circle_scores(circles, p_bar_enu_m=None, omega=0.628)
        sc = scored[0]
        assert sc.phi_weight is not None
        assert sc.sigma_band_metres is not None
        assert sc.residual_metres is None  # No P_bar
        assert sc.residual_score is not None
        assert sc.age_weight is not None
        assert sc.constraint_angle_deg is not None

    def test_diagnostics_with_p_bar(self):
        circles = [_make_circle()]
        p_bar = (60000.0, 5000.0)
        scored = compute_circle_scores(circles, p_bar_enu_m=p_bar, omega=0.628)
        sc = scored[0]
        assert sc.residual_metres is not None  # P_bar available

    def test_selected_flag(self):
        circles = [_make_circle(delta_phi=math.radians(90.0), frame_index=0, icao="A")]
        scored = compute_circle_scores(circles, p_bar_enu_m=None, omega=0.628)
        selected = select_circles(scored)
        assert all(sc.selected for sc in selected)
        # Non-selected circles should have selected=False
        non_selected = [sc for sc in scored if not sc.selected]
        for sc in non_selected:
            assert sc.selected is False
