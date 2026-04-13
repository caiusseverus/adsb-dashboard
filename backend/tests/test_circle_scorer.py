"""Tests for the pair-derived circle scoring and admission pipeline."""

import math

import pytest

from radar.circle_scorer import (
    BURST_CENTROID_BASE_UNCERTAINTY_SEC,
    MAX_CIRCLES_PER_FRAME,
    MAX_PAIRS_PER_AIRCRAFT_PER_FRAME,
    MAX_POSITION_AGE_SEC,
    MIN_CIRCLE_SCORE,
    MIN_REPLIES_PER_BURST,
    POSITION_AGE_TAU_SEC,
    SIGMA_BAND_FLOOR_METRES,
    SIGMA_BAND_REFERENCE_METRES,
    circular_distance,
    compute_circle_scores,
    select_circles,
)


class TestConstants:
    def test_named_constants_exist(self):
        assert SIGMA_BAND_FLOOR_METRES == 500.0
        assert SIGMA_BAND_REFERENCE_METRES == 3_000.0
        assert BURST_CENTROID_BASE_UNCERTAINTY_SEC == 0.030
        assert POSITION_AGE_TAU_SEC == 4.0
        assert MIN_REPLIES_PER_BURST == 2
        assert MAX_POSITION_AGE_SEC == 10.0
        assert MIN_CIRCLE_SCORE == 0.05
        assert MAX_PAIRS_PER_AIRCRAFT_PER_FRAME == 8
        assert MAX_CIRCLES_PER_FRAME == 48


class TestCircularDistance:
    def test_same_angle(self):
        assert circular_distance(0.0, 0.0) == 0.0
        assert circular_distance(math.pi, math.pi) == 0.0

    def test_opposite_angles(self):
        assert circular_distance(0.0, math.pi) == pytest.approx(math.pi)

    def test_wrap_around(self):
        d = circular_distance(math.radians(359), math.radians(1))
        assert d == pytest.approx(math.radians(2), abs=1e-6)


def _make_circle(
    *,
    circle_index=0,
    center_enu_m=(10000.0, 5000.0),
    radius_m=50000.0,
    delta_phi=math.radians(90.0),
    icao_a="AAAAAA",
    icao_b="BBBBBB",
    pair_baseline_m=50000.0,
    interpolated_a=False,
    interpolated_b=False,
    n_replies_a=3,
    n_replies_b=3,
    position_age_a_seconds=0.0,
    position_age_b_seconds=0.0,
    frame_index=0,
):
    return {
        "circle_index": circle_index,
        "center_enu_m": center_enu_m,
        "radius_m": radius_m,
        "delta_phi": delta_phi,
        "icao_a": icao_a,
        "icao_b": icao_b,
        "pair_baseline_m": pair_baseline_m,
        "interpolated_a": interpolated_a,
        "interpolated_b": interpolated_b,
        "n_replies_a": n_replies_a,
        "n_replies_b": n_replies_b,
        "position_age_a_seconds": position_age_a_seconds,
        "position_age_b_seconds": position_age_b_seconds,
        "frame_index": frame_index,
    }


class TestScoreComponents:
    def test_phi_weight_is_folded_angle_strength(self):
        scored = compute_circle_scores([_make_circle(delta_phi=math.radians(90.0))], None, 0.628)
        assert scored[0].phi_weight == pytest.approx(1.0)

        scored = compute_circle_scores([_make_circle(delta_phi=math.radians(30.0))], None, 0.628)
        assert scored[0].phi_weight == pytest.approx(math.sin(math.radians(30.0)) ** 1.5)

    def test_pair_identity_is_carried(self):
        scored = compute_circle_scores([
            _make_circle(circle_index=42, icao_a="ABC123", icao_b="DEF456", frame_index=7)
        ], None, 0.628)
        assert scored[0].circle_index == 42
        assert scored[0].icao_a == "ABC123"
        assert scored[0].icao_b == "DEF456"
        assert scored[0].frame_index == 7

    def test_residual_score_one_without_p_bar(self):
        scored = compute_circle_scores([_make_circle()], None, 0.628)
        assert scored[0].residual_score == 1.0
        assert scored[0].prior_weight == 1.0
        assert scored[0].residual_metres is None

    def test_prior_is_weak_when_p_bar_misses(self):
        scored = compute_circle_scores([
            _make_circle(center_enu_m=(0.0, 0.0), radius_m=50000.0)
        ], (0.0, 0.0), 0.628)
        assert scored[0].residual_metres == pytest.approx(50000.0)
        assert scored[0].residual_score < 1.0
        assert 0.7 <= scored[0].prior_weight <= 1.0

    def test_age_weight_uses_worse_aircraft_age(self):
        scored = compute_circle_scores([
            _make_circle(position_age_a_seconds=0.0, position_age_b_seconds=POSITION_AGE_TAU_SEC)
        ], None, 0.628)
        assert scored[0].age_weight == pytest.approx(math.exp(-1.0))

    def test_reply_weight_uses_weaker_reply_count(self):
        scored = compute_circle_scores([
            _make_circle(n_replies_a=16, n_replies_b=2)
        ], None, 0.628)
        assert scored[0].reply_weight == pytest.approx(math.sqrt(2.0 / 4.0))

    def test_uncertainty_penalty_downweights_broad_bands(self):
        tight = compute_circle_scores([
            _make_circle(delta_phi=math.radians(90.0), pair_baseline_m=20000.0, n_replies_a=16, n_replies_b=16)
        ], None, 0.628)[0]
        broad = compute_circle_scores([
            _make_circle(delta_phi=math.radians(5.0), pair_baseline_m=200000.0, n_replies_a=2, n_replies_b=2)
        ], None, 0.628)[0]
        assert broad.sigma_band_metres > tight.sigma_band_metres
        assert broad.uncertainty_weight < tight.uncertainty_weight

    def test_circle_score_multiplicative(self):
        sc = compute_circle_scores([_make_circle()], None, 0.628)[0]
        assert sc.intrinsic_weight == pytest.approx(
            sc.phi_weight * sc.age_weight * sc.reply_weight * sc.uncertainty_weight
        )
        assert sc.circle_score == pytest.approx(sc.intrinsic_weight * sc.prior_weight)

    def test_sigma_band_floor(self):
        scored = compute_circle_scores([
            _make_circle(n_replies_a=100, n_replies_b=100, delta_phi=math.radians(90.0))
        ], None, 0.628)
        assert scored[0].sigma_band_metres >= SIGMA_BAND_FLOOR_METRES

    def test_sigma_band_increases_with_fewer_replies(self):
        s1 = compute_circle_scores([_make_circle(n_replies_a=2, n_replies_b=2)], None, 0.628)[0]
        s2 = compute_circle_scores([_make_circle(n_replies_a=10, n_replies_b=10)], None, 0.628)[0]
        assert s1.sigma_band_metres >= s2.sigma_band_metres


class TestHardGates:
    def test_below_2_degrees_excluded(self):
        scored = compute_circle_scores([_make_circle(delta_phi=math.radians(1.0))], None, 0.628)
        assert scored[0].circle_score == 0.0
        assert scored[0].exclusion_reason == "degenerate_delta_phi"

    def test_above_178_degrees_excluded(self):
        scored = compute_circle_scores([_make_circle(delta_phi=math.radians(179.0))], None, 0.628)
        assert scored[0].circle_score == 0.0
        assert scored[0].exclusion_reason == "degenerate_delta_phi"

    def test_insufficient_replies_excluded(self):
        scored = compute_circle_scores([_make_circle(n_replies_a=3, n_replies_b=1)], None, 0.628)
        selected = select_circles(scored)
        assert selected == []
        assert scored[0].exclusion_reason == "insufficient_replies"

    def test_stale_position_excluded(self):
        scored = compute_circle_scores([
            _make_circle(position_age_a_seconds=0.0, position_age_b_seconds=MAX_POSITION_AGE_SEC + 1.0)
        ], None, 0.628)
        selected = select_circles(scored)
        assert selected == []
        assert scored[0].exclusion_reason == "stale_position"

    def test_low_score_excluded(self):
        scored = compute_circle_scores([
            _make_circle(delta_phi=math.radians(10.0), position_age_a_seconds=8.0, position_age_b_seconds=8.0)
        ], None, 0.628)
        selected = select_circles(scored)
        assert selected == []
        assert scored[0].exclusion_reason == "low_score"


class TestPairAdmission:
    def test_admits_more_than_six_high_quality_pairs(self):
        circles = [
            _make_circle(circle_index=i, frame_index=i, icao_a=f"A{i:02d}", icao_b=f"B{i:02d}")
            for i in range(12)
        ]
        scored = compute_circle_scores(circles, None, 0.628)
        selected = select_circles(scored)
        assert len(selected) == 12

    def test_caps_total_circles_per_frame(self):
        circles = [
            _make_circle(circle_index=i, frame_index=0, icao_a=f"A{i:02d}", icao_b=f"B{i:02d}")
            for i in range(MAX_CIRCLES_PER_FRAME + 5)
        ]
        scored = compute_circle_scores(circles, None, 0.628)
        selected = select_circles(scored)
        assert len(selected) == MAX_CIRCLES_PER_FRAME
        assert sum(1 for sc in scored if sc.exclusion_reason == "frame_cap") == 5

    def test_caps_aircraft_participation_per_frame(self):
        circles = [
            _make_circle(circle_index=i, frame_index=0, icao_a="HOT001", icao_b=f"B{i:02d}")
            for i in range(MAX_PAIRS_PER_AIRCRAFT_PER_FRAME + 4)
        ]
        scored = compute_circle_scores(circles, None, 0.628)
        selected = select_circles(scored)
        assert len(selected) == MAX_PAIRS_PER_AIRCRAFT_PER_FRAME
        assert sum(1 for sc in scored if sc.exclusion_reason == "aircraft_cap") == 4

    def test_selected_flag(self):
        scored = compute_circle_scores([_make_circle()], None, 0.628)
        selected = select_circles(scored)
        assert all(sc.selected for sc in selected)
        assert all(sc.selected is False for sc in scored if sc not in selected)
