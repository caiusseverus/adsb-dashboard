"""Integration test: verify the forward model pipeline converges to a known radar position
using synthetic SweepFrames with perfect geometry."""

import math
import os
import sys

import pytest

sys.path.insert(0, os.path.dirname(os.path.dirname(__file__)))

from radar.forward_model import ForwardModel, score_candidate_position
from radar.models import SweepFrame, SweepFrameObservation


def _bearing_deg(lat1, lon1, lat2, lon2):
    """Forward azimuth from point 1 to point 2, degrees [0, 360)."""
    from radar.localiser import _bearing_deg as bd
    return bd(lat1, lon1, lat2, lon2)


def _make_synthetic_frame(
    radar_lat, radar_lon,
    ref_icao, ref_lat, ref_lon,
    other_aircraft,  # list of (icao, lat, lon)
    period_s=10.0,
    sweep_start_us=1_000_000,
    frame_index=0,
    sweep_direction=1,  # +1=clockwise, -1=counterclockwise
    noise_deg=0.0,
):
    """Build a SweepFrame with phase angles that are consistent with the given radar position.

    observed_phase for aircraft j = (bearing(R->j) - bearing(R->ref)) * sweep_direction
    arrival_us = ref_arrival_us + (observed_phase / 360) * period_s * 1_000_000
    """
    import random

    ref_arrival_us = sweep_start_us
    observations = []

    for icao, lat, lon in other_aircraft:
        bearing_obs = _bearing_deg(radar_lat, radar_lon, lat, lon)
        bearing_ref = _bearing_deg(radar_lat, radar_lon, ref_lat, ref_lon)

        # True phase difference (what the radar beam would see)
        true_phase = ((bearing_obs - bearing_ref) * sweep_direction) % 360.0

        # Add noise
        if noise_deg > 0:
            true_phase += random.gauss(0, noise_deg)
            true_phase = true_phase % 360.0

        # Convert phase to arrival time offset
        phase_fraction = true_phase / 360.0
        arrival_us = int(ref_arrival_us + phase_fraction * period_s * 1_000_000)

        observations.append(SweepFrameObservation(
            icao=icao,
            lat=lat,
            lon=lon,
            arrival_us=arrival_us,
        ))

    return SweepFrame(
        frame_index=frame_index,
        sweep_start_us=ref_arrival_us,
        ref_icao=ref_icao,
        ref_lat=ref_lat,
        ref_lon=ref_lon,
        ref_arrival_us=ref_arrival_us,
        observations=observations,
        quality="good" if len(observations) >= 3 else "marginal",
        period_s=period_s,
    )


# ── Test: score is lower at the true position ────────────────────────────────

def test_score_lower_at_true_position():
    """The score at the true radar position should be lower than at any wrong position."""
    # True radar position
    true_lat, true_lon = 51.0, -1.0

    # Aircraft positions
    ref_icao, ref_lat, ref_lon = "AAAAAA", 51.5, -0.5
    others = [
        ("BBBBBB", 50.8, -1.3),
        ("CCCCCC", 51.2, -0.7),
        ("DDDDDD", 50.6, -0.4),
        ("EEEEEE", 51.3, -1.4),
    ]

    frame = _make_synthetic_frame(
        true_lat, true_lon, ref_icao, ref_lat, ref_lon, others,
        period_s=10.0,
    )

    # Score at true position should be near zero
    true_score, _, _, _ = score_candidate_position(true_lat, true_lon, [frame], 10.0)

    # Score at wrong positions should be much higher
    wrong_scores = []
    for offset_lat, offset_lon in [
        (true_lat + 0.5, true_lon),
        (true_lat - 0.5, true_lon),
        (true_lat, true_lon + 0.5),
        (true_lat + 0.3, true_lon + 0.3),
        (true_lat - 0.3, true_lon - 0.3),
    ]:
        wrong_score, _, _, _ = score_candidate_position(offset_lat, offset_lon, [frame], 10.0)
        wrong_scores.append(wrong_score)

    assert true_score < min(wrong_scores), (
        f"True position score {true_score} should be lower than "
        f"all wrong position scores {wrong_scores}"
    )


# ── Test: optimizer converges to true position (single frame, many aircraft) ─

def test_optimizer_converges_from_single_frame():
    """A single frame with enough aircraft should allow the optimizer to find the radar."""
    true_lat, true_lon = 51.0, -1.0
    period_s = 10.0

    ref_icao, ref_lat, ref_lon = "AAAAAA", 51.4, -0.6
    # Spread aircraft around the radar for good geometry
    others = [
        ("B00001", 51.5, -0.8),
        ("B00002", 51.3, -1.3),
        ("B00003", 50.7, -1.2),
        ("B00004", 50.6, -0.5),
        ("B00005", 50.8, -0.2),
        ("B00006", 51.1, -0.1),
        ("B00007", 51.4, -0.3),
        ("B00008", 51.5, -1.0),
        ("B00009", 51.2, -1.5),
        ("B00010", 50.5, -1.0),
    ]

    frame = _make_synthetic_frame(
        true_lat, true_lon, ref_icao, ref_lat, ref_lon, others,
        period_s=period_s,
    )

    fm = ForwardModel()
    result = fm.run_2d_optimisation(
        iid=99,
        period_s=period_s,
        initial_guess=(51.2, -0.8),  # Start from a different point
        sweep_frames=[frame],
    )

    assert result is not None, "Optimisation should converge"
    lat, lon, cep_m, n_obs = result
    distance_m = _haversine_distance(true_lat, true_lon, lat, lon)

    assert distance_m < 5000, (
        f"Optimizer landed {distance_m:.0f}m from true position "
        f"(expected <5km). Got ({lat:.4f}, {lon:.4f}) vs true ({true_lat:.4f}, {true_lon:.4f})"
    )


# ── Test: optimizer converges with noisy data ────────────────────────────────

def test_optimizer_converges_with_noise():
    """Multiple frames with realistic noise should still converge."""
    true_lat, true_lon = 51.0, -1.0
    period_s = 10.0

    # Aircraft pool
    aircraft_pool = [
        ("A00001", 51.5, -0.8),
        ("A00002", 51.3, -1.3),
        ("A00003", 50.7, -1.2),
        ("A00004", 50.6, -0.5),
        ("A00005", 50.8, -0.2),
        ("A00006", 51.1, -0.1),
        ("A00007", 51.4, -0.3),
        ("A00008", 51.5, -1.0),
        ("A00009", 51.2, -1.5),
        ("A00010", 50.5, -1.0),
    ]

    # Build 10 frames, each with a random subset of aircraft
    import random
    random.seed(42)

    frames = []
    for i in range(10):
        n_aircraft = random.randint(4, 8)
        subset = random.sample(aircraft_pool, n_aircraft)
        ref = random.choice(subset)
        others = [a for a in subset if a[0] != ref[0]]

        if len(others) < 2:
            continue

        frame = _make_synthetic_frame(
            true_lat, true_lon,
            ref_icao=ref[0], ref_lat=ref[1], ref_lon=ref[2],
            other_aircraft=others,
            period_s=period_s,
            frame_index=i,
            noise_deg=2.0,  # 2° noise on phase
            sweep_start_us=1_000_000 + i * 10_000_000,
        )
        frames.append(frame)

    fm = ForwardModel()
    result = fm.run_2d_optimisation(
        iid=99,
        period_s=period_s,
        initial_guess=(51.3, -0.7),
        sweep_frames=frames,
    )

    assert result is not None, "Optimisation should converge"
    lat, lon, cep_m, n_obs = result
    distance_m = _haversine_distance(true_lat, true_lon, lat, lon)

    assert distance_m < 10000, (
        f"With 2° noise, optimizer should be within 10km. "
        f"Got {distance_m:.0f}m at ({lat:.4f}, {lon:.4f})"
    )


# ── Test: counterclockwise sweep direction ───────────────────────────────────

def test_optimizer_converges_counterclockwise():
    """Optimizer should find the true position for counterclockwise sweeps."""
    true_lat, true_lon = 51.0, -1.0
    period_s = 10.0

    ref_icao, ref_lat, ref_lon = "AAAAAA", 51.4, -0.6
    others = [
        ("B00001", 51.5, -0.8),
        ("B00002", 51.3, -1.3),
        ("B00003", 50.7, -1.2),
        ("B00004", 50.6, -0.5),
        ("B00005", 50.8, -0.2),
        ("B00006", 51.1, -0.1),
        ("B00007", 51.4, -0.3),
        ("B00008", 51.5, -1.0),
    ]

    frame = _make_synthetic_frame(
        true_lat, true_lon, ref_icao, ref_lat, ref_lon, others,
        period_s=period_s,
        sweep_direction=-1,  # counterclockwise
    )

    fm = ForwardModel()
    result = fm.run_2d_optimisation(
        iid=99,
        period_s=period_s,
        initial_guess=(51.2, -0.8),
        sweep_frames=[frame],
    )

    assert result is not None, "Optimisation should converge for counterclockwise"
    lat, lon, cep_m, n_obs = result
    distance_m = _haversine_distance(true_lat, true_lon, lat, lon)

    assert distance_m < 5000, (
        f"Counterclockwise: optimizer landed {distance_m:.0f}m from true. "
        f"Got ({lat:.4f}, {lon:.4f}) vs true ({true_lat:.4f}, {true_lon:.4f})"
    )


# ── Test: direction detection picks correct sweep direction ──────────────────

def test_direction_detection_picks_correct_direction():
    """When scoring, the optimizer should pick the direction that gives lower score."""
    true_lat, true_lon = 51.0, -1.0
    period_s = 10.0

    ref_icao, ref_lat, ref_lon = "AAAAAA", 51.4, -0.6
    others = [
        ("B00001", 51.5, -0.8),
        ("B00002", 51.3, -1.3),
        ("B00003", 50.7, -1.2),
        ("B00004", 50.6, -0.5),
        ("B00005", 50.8, -0.2),
        ("B00006", 51.1, -0.1),
    ]

    # Build frame with counterclockwise sweep
    frame = _make_synthetic_frame(
        true_lat, true_lon, ref_icao, ref_lat, ref_lon, others,
        period_s=period_s,
        sweep_direction=-1,
    )

    # Score at true position with both directions
    score_cw, _, _, _ = score_candidate_position(
        true_lat, true_lon, [frame], period_s, sweep_direction=1,
    )
    score_ccw, _, _, _ = score_candidate_position(
        true_lat, true_lon, [frame], period_s, sweep_direction=-1,
    )

    assert score_ccw < score_cw, (
        f"Counterclockwise frame should score lower with ccw direction. "
        f"CCW={score_ccw:.1f}, CW={score_cw:.1f}"
    )


# ── Test: position errors from stale positions ───────────────────────────────

def test_stale_position_causes_high_residual():
    """If an aircraft's position is wrong in the frame (stale data),
    the score at the true radar position should be high because the predicted
    bearing won't match the observed phase."""
    true_lat, true_lon = 51.0, -1.0
    period_s = 10.0

    # Build a frame with CORRECT positions and phases
    ref_icao, ref_lat, ref_lon = "AAAAAA", 51.5, -0.5
    others = [
        ("BBBBBB", 50.8, -1.3),
        ("CCCCCC", 51.2, -0.7),  # True position
        ("DDDDDD", 50.6, -0.4),
        ("EEEEEE", 51.3, -1.4),
    ]
    frame_correct = _make_synthetic_frame(
        true_lat, true_lon, ref_icao, ref_lat, ref_lon, others,
        period_s=period_s,
    )

    # Now mutate one aircraft's position to be wrong (simulating stale data)
    # The observed phase is still based on the true position, but the predicted
    # phase will use the wrong stored position — causing a mismatch
    frame_stale = _make_synthetic_frame(
        true_lat, true_lon, ref_icao, ref_lat, ref_lon, others,
        period_s=period_s,
    )
    # Mutate CCCCCC's stored position to be far away
    for obs in frame_stale.observations:
        if obs.icao == "CCCCCC":
            obs.lat = 52.0
            obs.lon = 0.5
            break

    score_correct, _, _, _ = score_candidate_position(
        true_lat, true_lon, [frame_correct], period_s,
    )
    score_stale, _, _, _ = score_candidate_position(
        true_lat, true_lon, [frame_stale], period_s,
    )

    assert score_stale > score_correct * 100, (
        f"Frame with stale position should have much higher score. "
        f"Stale={score_stale:.1f}, Correct={score_correct:.1f}"
    )


# ── Helper ───────────────────────────────────────────────────────────────────

def _haversine_distance(lat1, lon1, lat2, lon2):
    """Great-circle distance in metres."""
    R = 6_371_000
    phi1, phi2 = math.radians(lat1), math.radians(lat2)
    dphi = math.radians(lat2 - lat1)
    dlam = math.radians(lon2 - lon1)
    a = math.sin(dphi / 2) ** 2 + math.cos(phi1) * math.cos(phi2) * math.sin(dlam / 2) ** 2
    return 2 * R * math.asin(math.sqrt(a))
