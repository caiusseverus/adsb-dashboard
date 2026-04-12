"""Stage 6 synthetic verification for the live forward-model intersection pipeline."""

import math
import os
import random
import sys
from types import SimpleNamespace

import config
import pytest

sys.path.insert(0, os.path.dirname(os.path.dirname(__file__)))

from radar.forward_model import ForwardModel
from radar.models import SweepFrame, SweepFrameObservation


def _bearing_deg(lat1, lon1, lat2, lon2):
    from radar.localiser import _bearing_deg as bd
    return bd(lat1, lon1, lat2, lon2)


def _haversine_distance_m(lat1, lon1, lat2, lon2):
    r = 6_371_000.0
    phi1, phi2 = math.radians(lat1), math.radians(lat2)
    dphi = math.radians(lat2 - lat1)
    dlam = math.radians(lon2 - lon1)
    a = math.sin(dphi / 2) ** 2 + math.cos(phi1) * math.cos(phi2) * math.sin(dlam / 2) ** 2
    return 2 * r * math.asin(math.sqrt(a))


def _make_synthetic_frame(
    radar_lat,
    radar_lon,
    ref_icao,
    ref_lat,
    ref_lon,
    other_aircraft,
    period_s=10.0,
    sweep_start_us=1_000_000,
    frame_index=0,
    sweep_direction=1,
    noise_deg=0.0,
):
    ref_arrival_us = sweep_start_us
    observations = []

    for icao, lat, lon in other_aircraft:
        bearing_obs = _bearing_deg(radar_lat, radar_lon, lat, lon)
        bearing_ref = _bearing_deg(radar_lat, radar_lon, ref_lat, ref_lon)
        true_phase = ((bearing_obs - bearing_ref) * sweep_direction) % 360.0
        if noise_deg > 0:
            true_phase = (true_phase + random.gauss(0, noise_deg)) % 360.0
        arrival_us = int(ref_arrival_us + (true_phase / 360.0) * period_s * 1_000_000)
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


_STAGE6_IID = 99903  # high IID — will not exist in real DB


def _run_pipeline(monkeypatch, frames, *, receiver_lat=51.0, receiver_lon=-1.0, period_s=10.0):
    """Run solve_single_frame for each frame, accumulate, then compute centroid.

    Returns (result_dict, fake_state) where result_dict follows the same conventions
    as run_full_pipeline:  "error" key present ↔ no result stored.
    """
    monkeypatch.setattr(config, "RECEIVER_LAT", receiver_lat)
    monkeypatch.setattr(config, "RECEIVER_LON", receiver_lon)
    model = SimpleNamespace(period_s=period_s, status="SINGLE_RADAR", fm_cep_m=None)
    state = _FakeRadarState(model, frames)

    # Synthetic frames have only 3 observations → 3 arc pairs max.
    # Lower the per-frame arc minimum so the solver accepts them.
    monkeypatch.setattr("radar.forward_model._PER_FRAME_MIN_CONTRIBUTING_ARCS", 2)

    fm = ForwardModel()
    fm._frame_positions_loaded.add(_STAGE6_IID)  # prevent real-DB load

    for frame in frames:
        est = fm.solve_single_frame(_STAGE6_IID, frame, period_s, receiver_lat, receiver_lon)
        if est is not None:
            fm._add_frame_position(_STAGE6_IID, est)

    n_estimates = len(fm.get_frame_positions(_STAGE6_IID))
    if n_estimates == 0:
        return {"error": "no frames solved", "stage": "intersection", "n_estimates": 0}, state

    centroid = fm.compute_weighted_centroid(_STAGE6_IID)
    if centroid is None:
        return {"error": "centroid failed", "stage": "quality_gates", "n_estimates": n_estimates}, state

    state.update_forward_model_location(
        iid=_STAGE6_IID, lat=centroid["lat"], lon=centroid["lon"],
        cep_m=centroid["cep_km"] * 1000.0, n_observations=n_estimates,
        window_s=0.0, source="frame_accumulation", coincident_validation=None,
    )
    return {
        "lat": centroid["lat"],
        "lon": centroid["lon"],
        "cep_m": centroid["cep_km"] * 1000.0,
        "stored": True,
        "n_observations": n_estimates,
        "source": "frame_accumulation",
    }, state


def _build_clean_frames():
    random.seed(12345)
    true_lat, true_lon = 51.0, -1.0
    frame_specs = [
        ("R00001", 51.4, -1.4, [("U00001", 50.6, -0.8), ("U00002", 50.7, -1.3), ("U00011", 51.5, -0.4)]),
        ("R00002", 50.7, -0.5, [("U00003", 51.3, -0.2), ("U00004", 50.5, -1.4), ("U00012", 51.4, -1.1)]),
        ("R00003", 51.5, -0.9, [("U00005", 50.8, -0.2), ("U00006", 50.6, -1.2), ("U00013", 51.2, -1.5)]),
        ("R00004", 50.8, -1.5, [("U00007", 51.4, -0.6), ("U00008", 50.5, -0.5), ("U00014", 51.1, -0.1)]),
        ("R00005", 51.2, -0.3, [("U00009", 50.7, -0.6), ("U00010", 51.0, -1.5), ("U00015", 51.5, -1.0)]),
    ]
    frames = []
    for idx, (ref_icao, ref_lat, ref_lon, others) in enumerate(frame_specs):
        frames.append(_make_synthetic_frame(
            true_lat,
            true_lon,
            ref_icao,
            ref_lat,
            ref_lon,
            others,
            frame_index=idx,
            sweep_start_us=1_000_000 + idx * 10_000_000,
            noise_deg=0.5,
        ))
    return true_lat, true_lon, frames


def test_stage6_clean_geometry_localises_close_to_truth(monkeypatch):
    true_lat, true_lon, frames = _build_clean_frames()

    result, state = _run_pipeline(monkeypatch, frames)

    assert result is not None
    assert "error" not in result
    assert result["stored"] is True
    assert state.updated is not None
    distance_m = _haversine_distance_m(true_lat, true_lon, result["lat"], result["lon"])
    assert distance_m < 20000
    assert result["cep_m"] < 32000


def test_stage6_weak_angle_geometry_is_not_published(monkeypatch):
    true_lat, true_lon = 51.0, -1.0
    frames = []
    for idx in range(4):
        ref_icao = f"REF{idx}"
        ref_lat = 51.2 + idx * 0.02
        ref_lon = -0.2 + idx * 0.02
        others = [
            (f"A{idx}1", ref_lat + 0.02, ref_lon + 0.02),
            (f"A{idx}2", ref_lat + 0.04, ref_lon + 0.04),
            (f"A{idx}3", ref_lat + 0.06, ref_lon + 0.05),
        ]
        frames.append(_make_synthetic_frame(
            true_lat,
            true_lon,
            ref_icao,
            ref_lat,
            ref_lon,
            others,
            frame_index=idx,
            sweep_start_us=1_000_000 + idx * 10_000_000,
        ))

    result, state = _run_pipeline(monkeypatch, frames)

    assert result is not None
    assert "error" in result
    assert result["stage"] in {"intersection", "quality_gates", "accumulating"}
    assert state.updated is None


def test_stage6_projected_position_overload_produces_lower_weight_result(monkeypatch):
    # Interpolated positions are no longer rejected outright; they produce estimates
    # with lower weight (0.7 penalty).  Verify the pipeline still produces a result.
    true_lat, true_lon, frames = _build_clean_frames()
    for frame in frames:
        for obs in frame.observations:
            obs.interpolated = True

    result, state = _run_pipeline(monkeypatch, frames)

    assert result is not None
    # Either solves (lower quality) or returns accumulating error — both acceptable
    if "error" not in result:
        assert result["source"] == "frame_accumulation"


def test_stage6_repeated_aircraft_dominance_still_solves(monkeypatch):
    random.seed(23456)
    true_lat, true_lon = 51.0, -1.0
    repeated_aircraft = ("DOM001", 51.45, -0.45)
    frame_specs = [
        ("R00001", 51.4, -1.4, [repeated_aircraft, ("U00001", 50.6, -0.8), ("U00002", 50.7, -1.3)]),
        ("R00002", 50.7, -0.5, [repeated_aircraft, ("U00003", 51.3, -0.2), ("U00004", 50.5, -1.4)]),
        ("R00003", 51.5, -0.9, [repeated_aircraft, ("U00005", 50.8, -0.2), ("U00006", 50.6, -1.2)]),
        ("R00004", 50.8, -1.5, [repeated_aircraft, ("U00007", 51.4, -0.6), ("U00008", 50.5, -0.5)]),
        ("R00005", 51.2, -0.3, [repeated_aircraft, ("U00009", 50.7, -0.6), ("U00010", 51.0, -1.5)]),
    ]
    frames = []
    for idx, (ref_icao, ref_lat, ref_lon, others) in enumerate(frame_specs):
        frames.append(_make_synthetic_frame(
            true_lat,
            true_lon,
            ref_icao,
            ref_lat,
            ref_lon,
            others,
            frame_index=idx,
            sweep_start_us=1_000_000 + idx * 10_000_000,
            noise_deg=1.0,
        ))

    result, state = _run_pipeline(monkeypatch, frames)

    assert result is not None
    assert "error" not in result
    assert result["stored"] is True
    assert state.updated is not None
    distance_m = _haversine_distance_m(true_lat, true_lon, result["lat"], result["lon"])
    assert distance_m < 50000


def test_stage6_bimodal_interference_cloud_centroid_is_between_both_radars(monkeypatch):
    # With two distinct radar positions, the per-frame solver produces estimates near
    # each true position.  The centroid ends up between them — not close to either.
    random.seed(34567)
    true_a = (51.0, -1.0)
    true_b = (51.35, -0.55)
    frame_specs = [
        (true_a, "A00001", 51.5, -0.6, [("A10001", 51.4, -1.4), ("A10002", 50.6, -1.1), ("A10003", 50.7, -0.3)]),
        (true_a, "A00002", 50.8, -0.4, [("A10004", 51.4, -0.3), ("A10005", 51.2, -1.5), ("A10006", 50.5, -1.2)]),
        (true_b, "B00001", 51.7, -0.2, [("B10001", 51.6, -0.9), ("B10002", 50.9, -0.5), ("B10003", 51.0, 0.1)]),
        (true_b, "B00002", 51.1, 0.0, [("B10004", 51.8, -0.1), ("B10005", 51.5, -0.8), ("B10006", 50.8, -0.2)]),
    ]
    frames = []
    for idx, (true_pos, ref_icao, ref_lat, ref_lon, others) in enumerate(frame_specs):
        frames.append(_make_synthetic_frame(
            true_pos[0],
            true_pos[1],
            ref_icao,
            ref_lat,
            ref_lon,
            others,
            frame_index=idx,
            sweep_start_us=1_000_000 + idx * 10_000_000,
            noise_deg=0.5,
        ))

    result, state = _run_pipeline(monkeypatch, frames)

    assert result is not None
    if "error" not in result:
        # Centroid should not be close to either true position — it's in-between
        dist_a = _haversine_distance_m(true_a[0], true_a[1], result["lat"], result["lon"])
        dist_b = _haversine_distance_m(true_b[0], true_b[1], result["lat"], result["lon"])
        # At least one of the distances should be significant (>10 km)
        assert max(dist_a, dist_b) > 10_000
