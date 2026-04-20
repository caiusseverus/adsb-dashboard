"""Tests for the forward model phase-difference scoring and SweepFrame logic."""

import math
import os
import sys
from types import SimpleNamespace

import config
import pytest

sys.path.insert(0, os.path.dirname(os.path.dirname(__file__)))

from radar.forward_model import (
    ForwardModel,
    _IntersectionCandidate,
    _IntersectionCluster,
    _build_residual_replay_diagnostics,
    _build_selected_lookup,
    _build_intersection_clusters,
    _cluster_same_lobe_metrics,
    _evaluate_cluster_ambiguity,
    _prepare_frames_for_solving,
    _observation_quality_weight,
    _preprocess_scoring_frames,
    _resolve_intersection_candidates,
    _select_intersection_observations,
    _select_intersection_observations_with_diagnostics,
    _detect_directional_signal,
    validate_coincident_alignment,
    score_candidate_position,
)
from radar.models import SweepFrame, SweepFrameObservation, ReferenceAircraftInfo


# ── Helpers ──────────────────────────────────────────────────────────────────

def _make_frame(ref_icao, ref_lat, ref_lon, ref_arrival_us, observations,
                period_s=10.0, quality="good", frame_index=0):
    """Convenience factory for SweepFrame test data."""
    obs_list = [
        SweepFrameObservation(
            icao=o["icao"], lat=o["lat"], lon=o["lon"],
            arrival_us=o["arrival_us"],
            signal_dbfs=o.get("signal_dbfs"),
            interpolated=o.get("interpolated", False),
        )
        for o in observations
    ]
    return SweepFrame(
        frame_index=frame_index,
        sweep_start_us=ref_arrival_us,
        ref_icao=ref_icao,
        ref_lat=ref_lat,
        ref_lon=ref_lon,
        ref_arrival_us=ref_arrival_us,
        observations=obs_list,
        quality=quality,
        period_s=period_s,
    )


def _bearing_deg(lat1, lon1, lat2, lon2):
    """Forward azimuth from point 1 to point 2."""
    from radar.localiser import _bearing_deg as bd
    return bd(lat1, lon1, lat2, lon2)


class _FakeRadarState:
    def __init__(self, model, frames):
        self._model = model
        self._frames = frames
        self.updated = None

    def get_rotation_model(self, iid):
        return self._model

    def build_sweep_frames(self, iid):
        return self._frames

    def update_forward_model_location(self, **kwargs):
        self.updated = kwargs


# ── Task 7.5: Circular residual subtraction ─────────────────────────────────

def test_circular_residual_wrap_around():
    """+179° vs -179° should give -2°, not 358°."""
    obs = 179.0
    pred = -179.0
    residual = (obs - pred + 540.0) % 360.0 - 180.0
    assert residual == pytest.approx(-2.0)

    obs2 = -179.0
    pred2 = 179.0
    residual2 = (obs2 - pred2 + 540.0) % 360.0 - 180.0
    assert residual2 == pytest.approx(2.0)


def test_circular_residual_small_positive():
    """Observed 10°, predicted 8° → residual = 2°."""
    obs = 10.0
    pred = 8.0
    residual = (obs - pred + 540.0) % 360.0 - 180.0
    assert residual == pytest.approx(2.0)


def test_circular_residual_small_negative():
    """Observed 8°, predicted 10° → residual = -2°."""
    obs = 8.0
    pred = 10.0
    residual = (obs - pred + 540.0) % 360.0 - 180.0
    assert residual == pytest.approx(-2.0)


def test_circular_residual_zero():
    """Exact match → residual = 0°."""
    obs = 45.0
    pred = 45.0
    residual = (obs - pred + 540.0) % 360.0 - 180.0
    assert residual == pytest.approx(0.0)


# ── Task 7.4: Phase-difference invariance to absolute timestamp offset ──────

def test_score_invariant_to_timestamp_offset():
    """Same geometry, different absolute t_ref → same score."""
    period_s = 10.0

    frame1 = _make_frame(
        ref_icao="AAAAAA", ref_lat=51.0, ref_lon=-1.0,
        ref_arrival_us=1_000_000,
        observations=[
            {"icao": "BBBBBB", "lat": 51.5, "lon": -0.5, "arrival_us": 1_300_000},
            {"icao": "CCCCCC", "lat": 50.5, "lon": -1.5, "arrival_us": 1_700_000},
        ],
        period_s=period_s,
    )

    frame2 = _make_frame(
        ref_icao="AAAAAA", ref_lat=51.0, ref_lon=-1.0,
        ref_arrival_us=999_000_000,
        observations=[
            {"icao": "BBBBBB", "lat": 51.5, "lon": -0.5, "arrival_us": 999_300_000},
            {"icao": "CCCCCC", "lat": 50.5, "lon": -1.5, "arrival_us": 999_700_000},
        ],
        period_s=period_s,
    )

    candidate_lat, candidate_lon = 51.2, -1.2

    score1, _, _, _ = score_candidate_position(
        candidate_lat, candidate_lon, [frame1], period_s,
    )
    score2, _, _, _ = score_candidate_position(
        candidate_lat, candidate_lon, [frame2], period_s,
    )

    assert score1 == pytest.approx(score2)


# ── Task 7.3: score_candidate_position returns lower score for correct position

def test_score_lower_for_correct_position():
    """Synthetic geometry: place radar at a known point, verify scoring picks it."""
    period_s = 10.0
    true_radar_lat = 51.0
    true_radar_lon = -1.0

    ac1_lat, ac1_lon = 51.5, -0.5
    ac2_lat, ac2_lon = 50.5, -1.5

    bearing_1 = _bearing_deg(true_radar_lat, true_radar_lon, ac1_lat, ac1_lon)
    bearing_2 = _bearing_deg(true_radar_lat, true_radar_lon, ac2_lat, ac2_lon)

    phase_diff_1 = (bearing_1 - 0) % 360.0
    phase_diff_2 = (bearing_2 - 0) % 360.0

    dt_1 = (phase_diff_1 / 360.0) * period_s * 1_000_000
    dt_2 = (phase_diff_2 / 360.0) * period_s * 1_000_000

    frame = _make_frame(
        ref_icao="AAAAAA", ref_lat=ac1_lat, ref_lon=ac1_lon,
        ref_arrival_us=1_000_000,
        observations=[
            {"icao": "BBBBBB", "lat": ac2_lat, "lon": ac2_lon, "arrival_us": 1_000_000 + int(dt_2 - dt_1)},
        ],
        period_s=period_s,
    )

    true_score, _, _, _ = score_candidate_position(
        true_radar_lat, true_radar_lon, [frame], period_s,
    )
    wrong_score, _, _, _ = score_candidate_position(
        52.0, -2.0, [frame], period_s,
    )

    assert true_score < wrong_score or abs(true_score - wrong_score) < 1.0


# ── Task 7.1: SweepFrame building produces correct ref aircraft and phases ───

def test_sweep_frame_basic_structure():
    """SweepFrame stores reference and observations correctly."""
    frame = _make_frame(
        ref_icao="AAAAAA", ref_lat=51.0, ref_lon=-1.0,
        ref_arrival_us=1_000_000,
        observations=[
            {"icao": "BBBBBB", "lat": 51.5, "lon": -0.5, "arrival_us": 1_300_000},
            {"icao": "CCCCCC", "lat": 50.5, "lon": -1.5, "arrival_us": 1_700_000},
        ],
        period_s=10.0,
        quality="good",
        frame_index=5,
    )

    assert frame.ref_icao == "AAAAAA"
    assert frame.ref_lat == 51.0
    assert frame.ref_lon == -1.0
    assert frame.ref_arrival_us == 1_000_000
    assert frame.quality == "good"
    assert frame.frame_index == 5
    assert len(frame.observations) == 2
    assert frame.observations[0].icao == "BBBBBB"
    assert frame.observations[1].icao == "CCCCCC"


# ── Task 7.6: Frame quality flag classification ─────────────────────────────

def test_frame_quality_good_with_four_aircraft():
    """4+ aircraft (ref + 3 obs) = good."""
    frame = _make_frame(
        ref_icao="AAAAAA", ref_lat=51.0, ref_lon=-1.0,
        ref_arrival_us=1_000_000,
        observations=[
            {"icao": "BBBBBB", "lat": 51.5, "lon": -0.5, "arrival_us": 1_300_000},
            {"icao": "CCCCCC", "lat": 50.5, "lon": -1.5, "arrival_us": 1_700_000},
            {"icao": "DDDDDD", "lat": 51.2, "lon": -0.8, "arrival_us": 1_500_000},
        ],
        quality="good",
    )
    assert frame.quality == "good"


def test_frame_quality_marginal_with_three_aircraft():
    """Exactly 3 aircraft (ref + 2 obs) = marginal."""
    frame = _make_frame(
        ref_icao="AAAAAA", ref_lat=51.0, ref_lon=-1.0,
        ref_arrival_us=1_000_000,
        observations=[
            {"icao": "BBBBBB", "lat": 51.5, "lon": -0.5, "arrival_us": 1_300_000},
            {"icao": "CCCCCC", "lat": 50.5, "lon": -1.5, "arrival_us": 1_700_000},
        ],
        quality="marginal",
    )
    assert frame.quality == "marginal"


# ── Task 7.2: Reference aircraft selection with hysteresis ───────────────────

def test_reference_selection_picks_best_score():
    """Best-scoring ICAO (closest to aggregate period, most bursts) is selected."""
    from radar.sweep import RadarState
    from radar.models import RadarIID

    state = RadarState()
    model = RadarIID(iid=1, period_s=10.0)
    state._models[1] = model

    # Simulate live burst data: each aircraft has multiple reply timestamps
    # AAAAAA: perfect 10s intervals
    # BBBBBB: slightly worse
    # CCCCCC: erratic
    state._live_burst_centroids[1] = {
        "AAAAAA": [i * 10_000_000 for i in range(6)],
        "BBBBBB": [int((i * 10.3 + 0.3 * (i % 2)) * 1_000_000) for i in range(6)],
        "CCCCCC": [i * 9_000_000 + i * 500_000 * (i % 3) for i in range(6)],
    }

    ref_icao = state._select_reference_from_live_bursts(iid=1, period_s=10.0)

    assert ref_icao == "AAAAAA"
    assert model.reference_aircraft is not None
    assert model.reference_aircraft.ref_icao == "AAAAAA"


def test_reference_selection_hysteresis():
    """Reference stays current when already selected."""
    from radar.sweep import RadarState
    from radar.models import RadarIID

    state = RadarState()
    model = RadarIID(iid=1, period_s=10.0)
    state._models[1] = model

    model.reference_aircraft = ReferenceAircraftInfo(
        ref_icao="AAAAAA", ref_score=0.10, ref_since_sweep=0, hysteresis_margin=0.25,
    )

    state._live_burst_centroids[1] = {
        "AAAAAA": [i * 10_000_000 for i in range(6)],
        "BBBBBB": [i * 10_000_000 for i in range(6)],
    }

    ref_icao = state._select_reference_from_live_bursts(iid=1, period_s=10.0)
    assert ref_icao == "AAAAAA"


def test_reference_selection_switches_on_clear_winner():
    """Reference switches when a clearly better candidate appears."""
    from radar.sweep import RadarState
    from radar.models import RadarIID

    state = RadarState()
    model = RadarIID(iid=1, period_s=10.0)
    state._models[1] = model

    model.reference_aircraft = ReferenceAircraftInfo(
        ref_icao="CCCCCC", ref_score=0.50, ref_since_sweep=0, hysteresis_margin=0.25,
    )

    state._live_burst_centroids[1] = {
        "AAAAAA": [i * 10_000_000 for i in range(6)],
        "CCCCCC": [i * 9_000_000 + i * 500_000 * (i % 3) for i in range(6)],
    }

    ref_icao = state._select_reference_from_live_bursts(iid=1, period_s=10.0)
    assert ref_icao == "AAAAAA"


# ── Task 7.7: Observation weighting ─────────────────────────────────────────

def test_observation_quality_direct_good_baseline():
    """Direct position with strong baseline → weight = 1.0."""
    w = _observation_quality_weight(interpolated=False, baseline_km=100.0)
    assert w == pytest.approx(1.0)


def test_observation_quality_interpolated_penalty():
    """Interpolated position → 0.7 penalty applied."""
    w = _observation_quality_weight(interpolated=True, baseline_km=100.0)
    assert w == pytest.approx(0.7)


def test_observation_quality_short_baseline():
    """Short baseline → leverage weight capped at 0.2."""
    w = _observation_quality_weight(interpolated=False, baseline_km=5.0)
    assert w == pytest.approx(0.2)


def test_observation_quality_prefers_direct_long_baseline():
    strong = _observation_quality_weight(interpolated=False, baseline_km=120.0)
    weak = _observation_quality_weight(interpolated=True, baseline_km=15.0)
    assert strong > weak


def test_preprocess_scoring_frames_assigns_higher_quality_to_direct_strong_observation():
    frame = _make_frame(
        ref_icao="AAAAAA", ref_lat=51.0, ref_lon=-1.0,
        ref_arrival_us=1_000_000,
        observations=[
            {
                "icao": "BBBBBB",
                "lat": 51.6,
                "lon": -0.2,
                "arrival_us": 3_500_000,
                "signal_dbfs": -12.0,
                "interpolated": False,
            },
            {
                "icao": "CCCCCC",
                "lat": 51.05,
                "lon": -0.98,
                "arrival_us": 1_300_000,
                "signal_dbfs": -58.0,
                "interpolated": True,
            },
        ],
        period_s=10.0,
    )

    scored = _preprocess_scoring_frames([frame], period_s=10.0)

    assert len(scored) == 1
    assert len(scored[0].observations) == 2
    assert scored[0].observations[0].quality_weight > scored[0].observations[1].quality_weight


def test_select_intersection_observations_caps_per_frame():
    observations = []
    for idx in range(8):
        observations.append({
            "icao": f"B{idx:05d}",
            "lat": 51.0 + (idx * 0.05),
            "lon": -1.0 + (idx * 0.08),
            "arrival_us": 1_500_000 + idx * 200_000,
            "signal_dbfs": -12.0,
            "interpolated": False,
        })

    frame = _make_frame(
        ref_icao="AAAAAA", ref_lat=51.0, ref_lon=-1.0,
        ref_arrival_us=1_000_000,
        observations=observations,
        period_s=10.0,
    )

    scored = _preprocess_scoring_frames([frame], period_s=10.0)
    selected = _select_intersection_observations(scored, min_sin_phi=0.15)

    assert len(selected) == 6


def test_select_intersection_observations_caps_per_icao_across_frames():
    frames = []
    for frame_index in range(6):
        frames.append(_make_frame(
            ref_icao="AAAAAA", ref_lat=51.0, ref_lon=-1.0,
            ref_arrival_us=1_000_000 + frame_index * 10_000_000,
            observations=[
                {
                    "icao": "BBBBBB",
                    "lat": 51.4,
                    "lon": -0.4,
                    "arrival_us": 3_000_000 + frame_index * 10_000_000,
                    "signal_dbfs": -10.0,
                    "interpolated": False,
                },
                {
                    "icao": f"C{frame_index:05d}",
                    "lat": 50.6,
                    "lon": -1.3,
                    "arrival_us": 5_000_000 + frame_index * 10_000_000,
                    "signal_dbfs": -12.0,
                    "interpolated": False,
                },
            ],
            period_s=10.0,
            frame_index=frame_index,
        ))

    scored = _preprocess_scoring_frames(frames, period_s=10.0)
    selected = _select_intersection_observations(scored, min_sin_phi=0.15)

    repeated_count = sum(1 for obs in selected if obs.icao == "BBBBBB")
    assert repeated_count == 4


def test_select_intersection_observations_prunes_weak_angle_candidates():
    frame = _make_frame(
        ref_icao="AAAAAA", ref_lat=51.0, ref_lon=-1.0,
        ref_arrival_us=1_000_000,
        observations=[
            {
                "icao": "BBBBBB",
                "lat": 51.4,
                "lon": -0.4,
                "arrival_us": 1_050_000,
                "signal_dbfs": -10.0,
                "interpolated": False,
            },
            {
                "icao": "CCCCCC",
                "lat": 50.7,
                "lon": -1.3,
                "arrival_us": 3_500_000,
                "signal_dbfs": -10.0,
                "interpolated": False,
            },
        ],
        period_s=10.0,
    )

    scored = _preprocess_scoring_frames([frame], period_s=10.0)
    selected = _select_intersection_observations(scored, min_sin_phi=0.15)

    assert {obs.icao for obs in selected} == {"CCCCCC"}


def test_select_intersection_observations_reports_pruning_diagnostics():
    # WEAK00 has arrival_us very close to ref (50 ms into a 10 s period → sin_phi ≈ 0.03 < 0.15)
    # so it is dropped for weak angle.  LOWQ00 and GOOD01 both pass quality and angle gates.
    frame = _make_frame(
        ref_icao="AAAAAA", ref_lat=51.0, ref_lon=-1.0,
        ref_arrival_us=1_000_000,
        observations=[
            {
                "icao": "LOWQ00",
                "lat": 51.02,
                "lon": -0.99,
                "arrival_us": 1_300_000,
                "signal_dbfs": -70.0,
                "interpolated": True,
            },
            {
                "icao": "WEAK00",
                "lat": 51.3,
                "lon": -0.7,
                "arrival_us": 1_050_000,
                "signal_dbfs": -10.0,
                "interpolated": False,
            },
            {
                "icao": "GOOD01",
                "lat": 51.4,
                "lon": -0.4,
                "arrival_us": 3_000_000,
                "signal_dbfs": -10.0,
                "interpolated": False,
            },
        ],
        period_s=10.0,
    )

    scored = _preprocess_scoring_frames([frame], period_s=10.0)
    selected, diagnostics = _select_intersection_observations_with_diagnostics(scored, min_sin_phi=0.15)

    assert {obs.icao for obs in selected} == {"LOWQ00", "GOOD01"}
    assert diagnostics["total_scored_observations"] == 3
    assert diagnostics["dropped_low_quality"] == 0
    assert diagnostics["dropped_weak_angle"] == 1
    assert diagnostics["selected_observations"] == 2


def test_resolve_intersection_candidates_prefers_stronger_cluster():
    candidates = [
        _IntersectionCandidate(arc_i=0, arc_j=1, x_km=0.0, y_km=0.0, weight=4.0),
        _IntersectionCandidate(arc_i=0, arc_j=1, x_km=3.0, y_km=2.0, weight=3.5),
        _IntersectionCandidate(arc_i=0, arc_j=1, x_km=-2.0, y_km=1.0, weight=3.0),
        _IntersectionCandidate(arc_i=0, arc_j=1, x_km=120.0, y_km=120.0, weight=0.4),
        _IntersectionCandidate(arc_i=0, arc_j=1, x_km=125.0, y_km=118.0, weight=0.3),
    ]

    resolved = _resolve_intersection_candidates(candidates, cluster_radius_km=20.0)

    assert resolved is not None
    x_km = resolved.mean_x_km
    y_km = resolved.mean_y_km
    rms_km = resolved.rms_km
    assert abs(x_km) < 5.0
    assert abs(y_km) < 5.0
    assert rms_km < 5.0


def test_resolve_intersection_candidates_rejects_bimodal_ambiguous_cloud():
    candidates = [
        _IntersectionCandidate(arc_i=0, arc_j=1, x_km=0.0, y_km=0.0, weight=3.0),
        _IntersectionCandidate(arc_i=0, arc_j=1, x_km=3.0, y_km=2.0, weight=3.0),
        _IntersectionCandidate(arc_i=0, arc_j=1, x_km=100.0, y_km=100.0, weight=3.0),
        _IntersectionCandidate(arc_i=0, arc_j=1, x_km=103.0, y_km=99.0, weight=3.0),
    ]

    resolved = _resolve_intersection_candidates(
        candidates,
        cluster_radius_km=15.0,
        dominance_ratio=1.25,
    )

    assert resolved is None


def test_resolve_intersection_candidates_ignores_far_outliers():
    candidates = [
        _IntersectionCandidate(arc_i=0, arc_j=1, x_km=10.0, y_km=10.0, weight=2.0),
        _IntersectionCandidate(arc_i=0, arc_j=1, x_km=12.0, y_km=9.0, weight=2.5),
        _IntersectionCandidate(arc_i=0, arc_j=1, x_km=11.0, y_km=12.0, weight=2.0),
        _IntersectionCandidate(arc_i=0, arc_j=1, x_km=250.0, y_km=-220.0, weight=0.2),
        _IntersectionCandidate(arc_i=0, arc_j=1, x_km=-180.0, y_km=210.0, weight=0.2),
    ]

    resolved = _resolve_intersection_candidates(candidates, cluster_radius_km=20.0)

    assert resolved is not None
    x_km = resolved.mean_x_km
    y_km = resolved.mean_y_km
    rms_km = resolved.rms_km
    assert 8.0 <= x_km <= 14.0
    assert 8.0 <= y_km <= 14.0
    assert rms_km < 3.0


def test_build_intersection_clusters_merges_near_duplicate_seed_neighborhoods():
    candidates = [
        _IntersectionCandidate(arc_i=0, arc_j=100, x_km=-11.0, y_km=0.0, weight=0.5),
        _IntersectionCandidate(arc_i=1, arc_j=101, x_km=-4.0, y_km=0.3, weight=3.0),
        _IntersectionCandidate(arc_i=2, arc_j=102, x_km=-3.0, y_km=-0.3, weight=3.0),
        _IntersectionCandidate(arc_i=3, arc_j=103, x_km=3.0, y_km=0.2, weight=3.0),
        _IntersectionCandidate(arc_i=4, arc_j=104, x_km=4.0, y_km=-0.4, weight=3.0),
        _IntersectionCandidate(arc_i=5, arc_j=105, x_km=11.0, y_km=0.0, weight=0.5),
    ]

    clusters, cluster_diag = _build_intersection_clusters(candidates, cluster_radius_km=14.0)
    merge_events = cluster_diag["merge_events"]

    assert len(clusters) == 1
    assert cluster_diag["initial_neighborhood_count"] == 6
    assert cluster_diag["post_initial_prune_count"] == 6
    assert cluster_diag["post_merge_count"] == 1
    assert cluster_diag["initial_pruning_enabled"] is False
    assert cluster_diag["initial_prune_events"] == []
    assert len(merge_events) >= 1
    assert merge_events[0]["merge_reason"] == "final_centroid_plus_support_overlap"
    assert len(clusters[0].merged_cluster_indices) >= 2


def test_cluster_same_lobe_metrics_distinguishes_close_overlap_from_distinct_lobes():
    close_a = _IntersectionCluster(
        mean_x_km=0.0,
        mean_y_km=0.0,
        rms_km=3.0,
        total_weight=10.0,
        member_count=24,
        center_x_km=-8.0,
        center_y_km=0.5,
        contributing_arc_indices=frozenset({1, 2, 3, 4, 5, 6}),
        merged_cluster_indices=(0,),
    )
    close_b = _IntersectionCluster(
        mean_x_km=2.4,
        mean_y_km=1.2,
        rms_km=2.8,
        total_weight=9.7,
        member_count=22,
        center_x_km=8.0,
        center_y_km=-0.2,
        contributing_arc_indices=frozenset({2, 3, 4, 6, 7, 8}),
        merged_cluster_indices=(1,),
    )
    far_c = _IntersectionCluster(
        mean_x_km=52.0,
        mean_y_km=50.0,
        rms_km=3.2,
        total_weight=9.5,
        member_count=20,
        center_x_km=52.0,
        center_y_km=49.0,
        contributing_arc_indices=frozenset({30, 31, 32, 33, 34}),
        merged_cluster_indices=(2,),
    )

    close_metrics = _cluster_same_lobe_metrics(close_a, close_b, cluster_radius_km=20.0)
    far_metrics = _cluster_same_lobe_metrics(close_a, far_c, cluster_radius_km=20.0)

    assert close_metrics["same_lobe"] is True
    assert close_metrics["merge_like"] is True
    assert far_metrics["same_lobe"] is False
    assert far_metrics["merge_like"] is False


def test_evaluate_cluster_ambiguity_bypasses_same_lobe_near_duplicates():
    best = _IntersectionCluster(
        mean_x_km=0.0,
        mean_y_km=0.0,
        rms_km=3.0,
        total_weight=12.0,
        member_count=10,
        center_x_km=-7.0,
        center_y_km=0.0,
        contributing_arc_indices=frozenset({1, 2, 3, 4, 5, 6, 7, 8}),
        merged_cluster_indices=(0,),
    )
    second = _IntersectionCluster(
        mean_x_km=2.0,
        mean_y_km=1.5,
        rms_km=2.8,
        total_weight=10.5,
        member_count=9,
        center_x_km=8.0,
        center_y_km=-0.5,
        contributing_arc_indices=frozenset({2, 3, 4, 5, 6, 8, 9}),
        merged_cluster_indices=(1,),
    )
    verdict = _evaluate_cluster_ambiguity(
        best_cluster=best,
        second_cluster=second,
        best_quality_score=1.25,
        second_quality_score=1.23,
        best_support_score=2.1,
        second_support_score=2.0,
        raw_inlier_count=8,
        cluster_radius_km=20.0,
    )

    assert verdict["quality_ratio"] < verdict["effective_threshold"]
    assert verdict["same_lobe_bypass"] is True
    assert verdict["is_ambiguous"] is False


def test_evaluate_cluster_ambiguity_rejects_distinct_near_equal_clusters():
    best = _IntersectionCluster(
        mean_x_km=0.0,
        mean_y_km=0.0,
        rms_km=2.5,
        total_weight=12.0,
        member_count=10,
        center_x_km=0.0,
        center_y_km=0.0,
        contributing_arc_indices=frozenset({1, 2, 3, 4, 5}),
        merged_cluster_indices=(0,),
    )
    second = _IntersectionCluster(
        mean_x_km=38.0,
        mean_y_km=34.0,
        rms_km=2.7,
        total_weight=10.0,
        member_count=9,
        center_x_km=38.0,
        center_y_km=34.0,
        contributing_arc_indices=frozenset({40, 41, 42, 43, 44}),
        merged_cluster_indices=(1,),
    )
    verdict = _evaluate_cluster_ambiguity(
        best_cluster=best,
        second_cluster=second,
        best_quality_score=1.24,
        second_quality_score=1.22,
        best_support_score=2.1,
        second_support_score=2.0,
        raw_inlier_count=8,
        cluster_radius_km=20.0,
    )

    assert verdict["quality_ratio"] < verdict["effective_threshold"]
    assert verdict["same_lobe_bypass"] is False
    assert verdict["is_ambiguous"] is True


def _make_frame_estimate(frame_index, lat, lon, cep_km=2.0, n_arcs=8):
    """Convenience factory for FramePositionEstimate instances."""
    from radar.forward_model import FramePositionEstimate
    weight = n_arcs / (cep_km + 0.5) ** 2
    return FramePositionEstimate(
        frame_index=frame_index,
        sweep_start_us=float(frame_index) * 10_000_000,
        lat=lat,
        lon=lon,
        cep_km=cep_km,
        n_contributing_arcs=n_arcs,
        azimuth_spread_deg=180.0,
        weight=weight,
        cluster_dominance_ratio=2.0,
        interpolated_position_fraction=0.2,
    )


# ── run_full_pipeline: centroid fast-path ────────────────────────────────────

def test_classify_frame_for_accumulation_accepts_reduced_arc_high_quality():
    rejection, tier = ForwardModel._classify_frame_for_accumulation({
        "centroid_uncertainty_km": 6.0,
        "n_inlier_pair_circles": 5,
        "best_cluster_support_score": 1.2,
        "support_dominance_ratio": 1.4,
        "pairwise_weighted_rms_deg": 20.0,
    })
    assert rejection is None
    assert tier == "accepted_reduced_arc_high_quality"


def test_on_new_frame_rejection_updates_pipeline_stats(monkeypatch):
    fm = ForwardModel()
    called = []
    estimate = SimpleNamespace(frame_index=17, weight=1.0)
    solve_result = {
        "result": {
            "centroid_uncertainty_km": 6.0,
            "n_inlier_pair_circles": 5,
            "best_cluster_support_score": 0.5,
            "support_dominance_ratio": 1.0,
            "pairwise_weighted_rms_deg": 20.0,
        }
    }
    monkeypatch.setattr(fm, "solve_single_frame_with_result", lambda *args, **kwargs: (estimate, solve_result))
    monkeypatch.setattr(fm, "_add_frame_position", lambda iid, est: called.append((iid, est)))

    fm.on_new_frame(
        iid=77,
        frame=SimpleNamespace(frame_index=17, quality="good"),
        period_s=4.0,
        receiver_lat=51.0,
        receiver_lon=-1.0,
    )

    assert called == []
    stats = fm.get_frame_pipeline_stats(77)
    assert stats["frames_reaching_solver"] == 1
    assert stats["solver_success"] == 1
    assert stats["candidate_positions"] == 1
    assert stats["accumulation_rejected"] == 1
    assert stats["accumulation_rejection_reasons"] == {"poor_support_score": 1}


def test_on_new_frame_acceptance_updates_pipeline_stats_and_buffer(monkeypatch):
    fm = ForwardModel()
    iid = 78
    fm._frame_positions_loaded.add(iid)
    estimate = SimpleNamespace(
        frame_index=18,
        sweep_start_us=1_000_000.0,
        lat=51.5,
        lon=-1.2,
        cep_km=6.0,
        n_contributing_arcs=5,
        azimuth_spread_deg=110.0,
        weight=1.0,
        cluster_dominance_ratio=2.0,
        interpolated_position_fraction=0.1,
        admission_tier="accepted_high_confidence",
    )
    solve_result = {
        "result": {
            "centroid_uncertainty_km": 6.0,
            "n_inlier_pair_circles": 5,
            "best_cluster_support_score": 1.2,
            "support_dominance_ratio": 1.4,
            "pairwise_weighted_rms_deg": 20.0,
        }
    }
    monkeypatch.setattr(fm, "solve_single_frame_with_result", lambda *args, **kwargs: (estimate, solve_result))
    from db import stats_db
    monkeypatch.setattr(stats_db, "insert_frame_position", lambda *args, **kwargs: None)

    fm.on_new_frame(
        iid=iid,
        frame=SimpleNamespace(frame_index=18, quality="good"),
        period_s=4.0,
        receiver_lat=51.0,
        receiver_lon=-1.0,
    )

    stats = fm.get_frame_pipeline_stats(iid)
    assert stats["accumulation_accepted"] == 1
    assert stats["accumulation_written"] == 1
    assert stats["accumulation_acceptance_tiers"] == {"accepted_reduced_arc_high_quality": 1}
    assert stats["accumulated_frame_positions"] == 1
    assert stats["centroid_available"] is False

_TEST_IID = 99901   # high IID unlikely to exist in any real DB

def _isolated_fm(iid=_TEST_IID):
    """Return a ForwardModel with DB loading bypassed for the given IID."""
    fm = ForwardModel()
    fm._frame_positions_loaded.add(iid)
    return fm


def test_run_full_pipeline_returns_error_when_too_few_frames(monkeypatch):
    """Pipeline returns accumulating error when fewer than 20 frame estimates exist."""
    monkeypatch.setattr(config, "RECEIVER_LAT", 51.0)
    monkeypatch.setattr(config, "RECEIVER_LON", -1.0)
    model = SimpleNamespace(period_s=10.0, status="SINGLE_RADAR", fm_cep_m=None,
                            manual_lat=None, manual_lon=None)
    state = _FakeRadarState(model, [])
    fm = _isolated_fm()

    result = fm.run_full_pipeline(_TEST_IID, state)

    assert result is not None
    assert "error" in result
    assert result["stage"] == "accumulating"
    assert result["n_estimates"] == 0


def test_run_full_pipeline_stores_centroid_when_enough_frames(monkeypatch):
    """Pipeline stores the weighted centroid once ≥ 20 frame estimates exist."""
    monkeypatch.setattr(config, "RECEIVER_LAT", 51.0)
    monkeypatch.setattr(config, "RECEIVER_LON", -1.0)
    model = SimpleNamespace(period_s=10.0, status="SINGLE_RADAR", fm_cep_m=None,
                            manual_lat=None, manual_lon=None)
    state = _FakeRadarState(model, [])
    fm = _isolated_fm()

    for i in range(25):
        fm._add_frame_position(_TEST_IID, _make_frame_estimate(i, lat=51.5 + i * 0.001, lon=-1.0))

    result = fm.run_full_pipeline(_TEST_IID, state)

    assert result is not None
    assert "error" not in result
    assert result["source"] == "frame_accumulation"
    assert result["stored"] is True
    assert abs(result["lat"] - 51.512) < 0.05
    assert state.updated is not None
    assert state.updated["source"] == "frame_accumulation"


def test_run_full_pipeline_regression_guard_blocks_large_cep_increase(monkeypatch):
    """A new centroid with CEP >> existing CEP is not stored (regression guard)."""
    monkeypatch.setattr(config, "RECEIVER_LAT", 51.0)
    monkeypatch.setattr(config, "RECEIVER_LON", -1.0)
    model = SimpleNamespace(period_s=10.0, status="SINGLE_RADAR", fm_cep_m=500.0,
                            manual_lat=None, manual_lon=None)
    state = _FakeRadarState(model, [])
    fm = _isolated_fm()

    # Add estimates with very high scatter → large cep_km
    # Use per-frame cep_km that passes Stage 0 (< 20 km) but scatter
    # across lat/lon so the combined centroid cep is huge, firing the regression guard.
    for i in range(25):
        fm._add_frame_position(_TEST_IID, _make_frame_estimate(i, lat=51.0 + i * 0.5, lon=-1.0, cep_km=10.0, n_arcs=4))

    result = fm.run_full_pipeline(_TEST_IID, state)

    assert result is not None
    assert "error" not in result
    # cep_m would be >> 500 * 3.0 so regression guard fires
    assert result["stored"] is False


def test_run_full_pipeline_regression_guard_allows_poor_existing(monkeypatch):
    """When existing CEP is >= 20 km (poor), any new result is accepted."""
    monkeypatch.setattr(config, "RECEIVER_LAT", 51.0)
    monkeypatch.setattr(config, "RECEIVER_LON", -1.0)
    model = SimpleNamespace(period_s=10.0, status="SINGLE_RADAR", fm_cep_m=25_000.0,
                            manual_lat=None, manual_lon=None)
    state = _FakeRadarState(model, [])
    fm = _isolated_fm()

    for i in range(25):
        fm._add_frame_position(_TEST_IID, _make_frame_estimate(i, lat=51.5 + i * 0.001, lon=-1.0, cep_km=10.0))

    result = fm.run_full_pipeline(_TEST_IID, state)

    assert result is not None
    assert "error" not in result
    assert result["stored"] is True  # existing was poor, so accepted


def test_run_full_pipeline_missing_receiver_config_returns_error(monkeypatch):
    """Pipeline returns error when receiver coordinates are not configured."""
    monkeypatch.setattr(config, "RECEIVER_LAT", None)
    monkeypatch.setattr(config, "RECEIVER_LON", None)
    model = SimpleNamespace(period_s=10.0, status="SINGLE_RADAR", fm_cep_m=None,
                            manual_lat=None, manual_lon=None)
    state = _FakeRadarState(model, [])
    fm = _isolated_fm()

    result = fm.run_full_pipeline(_TEST_IID, state)

    assert result is not None
    assert "error" in result
    assert result["stage"] == "config"


def test_run_full_pipeline_no_model_returns_error(monkeypatch):
    """Pipeline returns error when no rotation model exists for the IID."""
    monkeypatch.setattr(config, "RECEIVER_LAT", 51.0)
    monkeypatch.setattr(config, "RECEIVER_LON", -1.0)

    class _NoModelState:
        def get_rotation_model(self, iid):
            return None
        def build_sweep_frames(self, iid):
            return []
        def update_forward_model_location(self, **kw):
            pass

    fm = _isolated_fm(99902)
    result = fm.run_full_pipeline(99902, _NoModelState())

    assert result is not None
    assert "error" in result
    assert result["stage"] == "rotation_model"


# ── Legacy intersection tests removed 2026-04-12 ───────────────────────────
# run_full_pipeline no longer calls the intersection solver directly.
# The solver is still exercised via solve_single_frame (called per-frame from
# the sweep callback), and tested end-to-end by test_forward_model_integration.py.


# ── Directional signal detection ─────────────────────────────────────────────

def test_detect_directional_signal_with_sinusoidal_pattern():
    """Sinusoidal residuals should be detected."""
    azimuths = list(range(0, 360, 10))
    residuals = [10.0 * math.sin(math.radians(az - 90)) for az in azimuths]

    result = _detect_directional_signal(residuals, azimuths)

    assert result is not None
    assert result["amplitude_deg"] > 5.0


def test_no_directional_signal_random():
    """Random residuals should not produce a directional signal."""
    import random
    random.seed(42)
    azimuths = list(range(0, 360, 10))
    residuals = [random.gauss(0, 1) for _ in azimuths]

    result = _detect_directional_signal(residuals, azimuths)

    if result is not None:
        assert result["amplitude_deg"] < 10.0
