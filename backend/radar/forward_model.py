"""
radar/forward_model.py — Forward model radar localisation via SSR beam phase fitting.

Uses per-sweep phase-difference observations (not co-sweep TDOA) to determine
radar position. Each SweepFrame provides one reference aircraft (phase=0) and
N other aircraft with measured phase offsets. The radar position is the unique
point where predicted bearing differences match observed phase differences.

Method:
  1. Build SweepFrames from burst centroids (done in sweep.py)
  2. For each candidate airport, score by comparing bearing differences to phase differences
  3. Run 2D optimisation from best airport
  4. Store result in RadarIID.fm_lat/fm_lon/fm_cep_m
"""

from __future__ import annotations

import dataclasses
from collections import deque
from dataclasses import dataclass
import json
import logging
import math
import time
from pathlib import Path
from typing import TYPE_CHECKING, Optional

import numpy as np

if TYPE_CHECKING:
    from .sweep import RadarState

log = logging.getLogger(__name__)

# ── Constants ────────────────────────────────────────────────────────────────

_R_EARTH = 6_371_000.0
_C_MUS = 299.792458
_NM_TO_M = 1852.0

from .localiser import _haversine_m, _bearing_deg  # noqa: E402,F401

_AIRPORT_TIER1_NM = 200.0
_AIRPORT_TIER2_NM = 400.0
_AIRPORT_TIER3_NM = 600.0

_MIN_FRAMES = 1  # Need at least one valid SweepFrame

_OPTIM_MAX_ITER = 500
_OPTIM_TIMEOUT_S = 10   # max wall-clock seconds per direction before callback interrupts
_HYPOTHESIS_MAX_FRAMES = 100  # frames used for airport scoring (single pass, not iterated)
_OPTIM_MAX_FRAMES = 30        # frames used for Nelder-Mead (called thousands of times)
_GRID_SPACING_DEG = 0.25  # coarser grid to reduce computation
_GRID_RADIUS_KM = 300.0
_GRID_MAX_POINTS = 5000  # hard cap on grid points to prevent runaway computation

_AZ_BINS = 36
_DIRECTIONAL_THRESHOLD = 5.0  # degrees amplitude threshold
_MIN_OBSERVATION_QUALITY = 0.05
_INTERSECTION_MIN_SIN_PHI = math.sin(math.radians(5.0))  # exclude phase < 5° (near-line arcs)
_INTERSECTION_MAX_OBS_PER_FRAME = 6
_INTERSECTION_MAX_OBS_PER_ICAO = 4
_PER_FRAME_MAX_OBS = 50           # relaxed cap for per-frame solving (each ICAO appears once per sweep)
_PER_FRAME_MIN_CONTRIBUTING_ARCS = 4
_PER_FRAME_MAX_CEP_KM = 100.0
_PER_FRAME_BUFFER_MAX = 5000
_PER_FRAME_TRIM_FRACTION = 0.05   # drop outermost 5% by distance before centroid
_INTERSECTION_CLUSTER_RADIUS_KM = 40.0       # bootstrap default; tightened adaptively once converged
_ADAPTIVE_CLUSTER_RADIUS_K = 2.5             # cluster radius = K × prior_rms when converged
_ADAPTIVE_CLUSTER_RADIUS_FLOOR_KM = 5.0
_INTERSECTION_CLUSTER_DOMINANCE_RATIO = 1.25
_MIN_INTERSECTION_AZ_SPREAD_DEG = 60.0
_MIN_HIGH_QUALITY_FRAMES = 2
_MAX_INTERPOLATED_FRACTION = 0.6
_MIN_CONTRIBUTING_ARCS = 8                   # replaces fixed 50-point cluster member gate
_MIN_INTERSECTION_PAIR_COUNT = 12
_MAX_INTERSECTION_RECEIVER_DISTANCE_NM = 300.0
_MAX_CCW_INTERSECTION_RECEIVER_DISTANCE_NM = 120.0
_MIN_STANDALONE_CCW_PAIR_COUNT = 24
_MIN_STANDALONE_CCW_HIGH_QUALITY_FRAMES = 3
_MIN_STANDALONE_CCW_CONTRIBUTING_ARCS = 14   # stricter CCW standalone arc gate
_MIN_STANDALONE_CCW_DOMINANCE_RATIO = 1.75
_INTERSECTION_FIT_DOMINANCE_RATIO = 1.15
_INTERSECTION_SECONDARY_WEIGHT_FRACTION = 0.75
_FRAME_REFERENCE_MAX_CANDIDATES = 4
_FRAME_REFERENCE_REPAIR_MIN_IMPROVEMENT_RATIO = 1.15
_GOOD_FRAME_REFERENCE_SIGMA_DEG = 70.0
_MAX_FRAME_REFERENCE_SIGMA_DEG = 105.0
_MARGINAL_FRAME_WEIGHT_SCALE = 0.6
_REFINE_MAX_MOVE_KM = 80.0
_REFINE_MIN_IMPROVEMENT_RATIO = 1.05
_MAX_FINAL_RESIDUAL_SIGMA_DEG = 35.0
_CI_VALIDATION_MAX_FRAMES = 12
_CI_VALIDATION_MAX_AIRCRAFT_PER_FRAME = 8
_CI_VALIDATION_PREDICTED_BEARING_TOL_DEG = 5.0
_CI_VALIDATION_OBSERVED_DT_TOL_MS = 60.0
_CI_VALIDATION_UNEXPECTED_DT_TOL_MS = 30.0
_CI_VALIDATION_MIN_PREDICTED_PAIRS = 4
_CI_VALIDATION_MIN_SCORE = 0.55

_AIRPORTS_PATH = Path(__file__).parent.parent / "data" / "airports.json"


# ── Data structures (imported from models) ──────────────────────────────────
# SweepFrame, SweepFrameObservation → from .models


@dataclass
class _ScoredObservation:
    icao: str
    lat: float
    lon: float
    observed_phase_deg: float
    quality_weight: float
    phase_strength: float
    baseline_km: float
    azimuth_from_ref_deg: float
    interpolated: bool


@dataclass
class _ScoredFrame:
    frame_index: int
    ref_icao: str
    ref_lat: float
    ref_lon: float
    observations: list[_ScoredObservation]


@dataclass
class _IntersectionObservation:
    frame_index: int
    icao: str
    ref_lat: float
    ref_lon: float
    obs_lat: float
    obs_lon: float
    observed_phase_deg: float
    quality_weight: float
    azimuth_from_ref_deg: float
    interpolated: bool


@dataclass
class _IntersectionCandidate:
    x_km: float
    y_km: float
    weight: float
    arc_i: int
    arc_j: int


@dataclass
class _IntersectionResolution:
    mean_x_km: float
    mean_y_km: float
    rms_km: float
    best_weight: float
    second_weight: float
    member_count: int


@dataclass
class _IntersectionCluster:
    mean_x_km: float
    mean_y_km: float
    rms_km: float
    total_weight: float
    member_count: int
    center_x_km: float
    center_y_km: float
    contributing_arc_indices: frozenset


@dataclass
class FramePositionEstimate:
    """Per-sweep-frame position estimate from the inscribed-angle solver."""
    frame_index: int
    sweep_start_us: float
    lat: float
    lon: float
    cep_km: float
    n_contributing_arcs: int
    azimuth_spread_deg: float
    weight: float   # n_contributing_arcs / (cep_km + 0.5)**2
    cluster_dominance_ratio: float = 0.0
    interpolated_position_fraction: float = 0.0


@dataclass
class _ResidualFit:
    score: float
    mean_residual_deg: float
    residual_sigma_deg: float
    n_residuals: int


def _circular_spread_deg(angles_deg: list[float]) -> float:
    if len(angles_deg) < 2:
        return 0.0
    azimuths = sorted(set(angle % 360.0 for angle in angles_deg))
    if len(azimuths) < 2:
        return 0.0
    gaps = [azimuths[i + 1] - azimuths[i] for i in range(len(azimuths) - 1)]
    gaps.append(360.0 - azimuths[-1] + azimuths[0])
    return 360.0 - max(gaps)


def _phase_deg_to_dt_ms(phase_deg: float, period_s: float) -> float:
    return (phase_deg / 360.0) * period_s * 1000.0


# ── Airport loading ──────────────────────────────────────────────────────────

class _AirportCandidate:
    def __init__(self, icao: str, name: str, lat: float, lon: float):
        self.icao = icao
        self.name = name
        self.lat = lat
        self.lon = lon


def _load_airports(airports_path: Path, receiver_lat: float, receiver_lon: float,
                   max_range_nm: float) -> list[_AirportCandidate]:
    """Load airports from JSON, filtered by max range from receiver."""
    if not airports_path.exists():
        log.warning("ForwardModel: airports.json not found at %s", airports_path)
        return []

    try:
        with open(airports_path) as f:
            raw = json.load(f)
    except (json.JSONDecodeError, OSError) as e:
        log.warning("ForwardModel: failed to load airports.json: %s", e)
        return []

    max_range_m = max_range_nm * _NM_TO_M
    candidates = []
    for ap in raw:
        lat = ap.get("lat")
        lon = ap.get("lon")
        if lat is None or lon is None:
            continue
        if _haversine_m(receiver_lat, receiver_lon, lat, lon) <= max_range_m:
            candidates.append(_AirportCandidate(
                icao=ap.get("icao", ""),
                name=ap.get("name", ""),
                lat=lat,
                lon=lon,
            ))

    log.info("ForwardModel: loaded %d airports within %.0f NM", len(candidates), max_range_nm)
    return candidates


# ── Phase-difference scoring ────────────────────────────────────────────────

def _baseline_leverage_weight(baseline_km: float) -> float:
    """Return a bounded leverage weight from aircraft-reference separation."""
    if baseline_km <= 0:
        return 0.0
    return min(1.0, max(0.2, baseline_km / 100.0))


def _observation_quality_weight(interpolated: bool, baseline_km: float) -> float:
    """Weight by position reliability and baseline leverage only.

    Signal strength is not used: a correctly decoded Mode-S message carries
    a valid position regardless of RSSI.  Phase strength (sin φ) is handled
    at the pair level via the angular-separation weight in the intersection
    solver, where it has a direct geometric meaning.
    """
    w_pos = 0.7 if interpolated else 1.0
    return w_pos * _baseline_leverage_weight(baseline_km)


def _preprocess_scoring_frames(
    sweep_frames: list,
    period_s: float,
    sweep_direction: int = 1,
) -> list[_ScoredFrame]:
    """Precompute observed phases and quality weights for one sweep direction."""
    scored_frames: list[_ScoredFrame] = []

    for frame in sweep_frames:
        frame_weight_scale = 1.0 if getattr(frame, "quality", "good") == "good" else _MARGINAL_FRAME_WEIGHT_SCALE
        scored_observations: list[_ScoredObservation] = []
        for obs in frame.observations:
            dt_s = (obs.arrival_us - frame.ref_arrival_us) / 1_000_000.0
            observed_phase_deg = ((dt_s / period_s) * 360.0 * sweep_direction) % 360.0
            phase_strength = abs(math.sin(math.radians(observed_phase_deg)))
            baseline_km = _haversine_m(frame.ref_lat, frame.ref_lon, obs.lat, obs.lon) / 1000.0
            azimuth_from_ref_deg = _bearing_deg(frame.ref_lat, frame.ref_lon, obs.lat, obs.lon)
            quality_weight = _observation_quality_weight(
                getattr(obs, "interpolated", False),
                baseline_km,
            ) * frame_weight_scale
            scored_observations.append(_ScoredObservation(
                icao=obs.icao,
                lat=obs.lat,
                lon=obs.lon,
                observed_phase_deg=observed_phase_deg,
                quality_weight=quality_weight,
                phase_strength=phase_strength,
                baseline_km=baseline_km,
                azimuth_from_ref_deg=azimuth_from_ref_deg,
                interpolated=getattr(obs, "interpolated", False),
            ))

        scored_frames.append(_ScoredFrame(
            frame_index=getattr(frame, "frame_index", len(scored_frames)),
            ref_icao=getattr(frame, "ref_icao", ""),
            ref_lat=frame.ref_lat,
            ref_lon=frame.ref_lon,
            observations=scored_observations,
        ))

    return scored_frames


def _score_candidate_position_preprocessed(
    r_lat: float,
    r_lon: float,
    scored_frames: list[_ScoredFrame],
) -> tuple[float, float, list[float], list[float]]:
    """Score a candidate radar position against preprocessed observations."""
    all_residuals = []
    all_weights = []
    all_azimuths = []

    for frame in scored_frames:
        bearing_ref = _bearing_deg(r_lat, r_lon, frame.ref_lat, frame.ref_lon)

        for obs in frame.observations:
            if obs.quality_weight <= 0:
                continue

            bearing_obs = _bearing_deg(r_lat, r_lon, obs.lat, obs.lon)
            predicted_phase = (bearing_obs - bearing_ref) % 360.0
            residual = (obs.observed_phase_deg - predicted_phase + 540.0) % 360.0 - 180.0

            all_residuals.append(residual)
            all_weights.append(obs.quality_weight)
            all_azimuths.append(bearing_obs)

    if not all_residuals:
        return (float("inf"), 0.0, [], [])

    total_w = sum(all_weights)
    mean_residual = sum(r * w for r, w in zip(all_residuals, all_weights)) / total_w
    score = sum(w * r * r for r, w in zip(all_residuals, all_weights))
    return (score, mean_residual, all_residuals, all_azimuths)


def _summarize_residual_fit(
    r_lat: float,
    r_lon: float,
    scored_frames: list[_ScoredFrame],
) -> _ResidualFit:
    score, mean_residual, residuals, _azimuths = _score_candidate_position_preprocessed(
        r_lat,
        r_lon,
        scored_frames,
    )
    if not residuals or not math.isfinite(score):
        return _ResidualFit(
            score=float("inf"),
            mean_residual_deg=0.0,
            residual_sigma_deg=float("inf"),
            n_residuals=0,
        )
    residual_sigma_deg = math.sqrt(sum(r * r for r in residuals) / len(residuals))
    return _ResidualFit(
        score=score,
        mean_residual_deg=mean_residual,
        residual_sigma_deg=residual_sigma_deg,
        n_residuals=len(residuals),
    )


def _angular_separation_deg(a: float, b: float) -> float:
    return abs((a - b + 180.0) % 360.0 - 180.0)


def _coincident_validation_status(score: float | None, predicted_pairs: int) -> str:
    if predicted_pairs < _CI_VALIDATION_MIN_PREDICTED_PAIRS:
        return "insufficient_geometry"
    if score is None:
        return "unavailable"
    if score >= 0.8:
        return "supporting"
    if score >= _CI_VALIDATION_MIN_SCORE:
        return "mixed"
    return "contradicting"


def validate_coincident_alignment(
    radar_lat: float,
    radar_lon: float,
    sweep_frames: list,
    period_s: float,
    sweep_direction: int = 1,
) -> dict:
    """Validate an FM position with bounded predicted-vs-observed coincidences."""
    if period_s <= 0:
        return {"available": False, "status": "unavailable", "reason": "invalid period"}

    frames = [f for f in sweep_frames if getattr(f, "quality", None) in ("good", "marginal")]
    frames = frames[-_CI_VALIDATION_MAX_FRAMES:]
    if not frames:
        return {"available": False, "status": "unavailable", "reason": "no sweep frames"}

    predicted_pairs = 0
    observed_supporting_pairs = 0
    missed_predicted_pairs = 0
    unexpected_observed_pairs = 0
    examples: list[dict] = []

    for frame in frames:
        aircraft = [{
            "icao": frame.ref_icao,
            "lat": frame.ref_lat,
            "lon": frame.ref_lon,
            "arrival_us": frame.ref_arrival_us,
            "role": "reference",
        }]
        aircraft.extend({
            "icao": obs.icao,
            "lat": obs.lat,
            "lon": obs.lon,
            "arrival_us": obs.arrival_us,
            "role": "observation",
        } for obs in frame.observations)

        aircraft = aircraft[:_CI_VALIDATION_MAX_AIRCRAFT_PER_FRAME]
        if len(aircraft) < 2:
            continue

        bearings = [
            _bearing_deg(radar_lat, radar_lon, ac["lat"], ac["lon"])
            for ac in aircraft
        ]

        for i in range(len(aircraft)):
            for j in range(i + 1, len(aircraft)):
                bearing_delta = _angular_separation_deg(bearings[i], bearings[j])
                observed_dt_ms = abs(aircraft[i]["arrival_us"] - aircraft[j]["arrival_us"]) / 1000.0
                should_coincide = bearing_delta <= _CI_VALIDATION_PREDICTED_BEARING_TOL_DEG
                did_coincide = observed_dt_ms <= _CI_VALIDATION_OBSERVED_DT_TOL_MS
                if should_coincide:
                    predicted_pairs += 1
                    if did_coincide:
                        observed_supporting_pairs += 1
                        kind = "support"
                    else:
                        missed_predicted_pairs += 1
                        kind = "missed"
                elif observed_dt_ms <= _CI_VALIDATION_UNEXPECTED_DT_TOL_MS:
                    unexpected_observed_pairs += 1
                    kind = "unexpected"
                else:
                    continue

                if len(examples) < 12:
                    examples.append({
                        "frame_index": getattr(frame, "frame_index", None),
                        "icao_a": aircraft[i]["icao"],
                        "icao_b": aircraft[j]["icao"],
                        "bearing_delta_deg": round(bearing_delta, 2),
                        "observed_dt_ms": round(observed_dt_ms, 2),
                        "classification": kind,
                    })

    if predicted_pairs > 0:
        support_rate = observed_supporting_pairs / predicted_pairs
        miss_rate = missed_predicted_pairs / predicted_pairs
        unexpected_penalty = min(0.25, unexpected_observed_pairs / max(predicted_pairs, 1) * 0.1)
        score = max(0.0, min(1.0, support_rate - unexpected_penalty))
    else:
        support_rate = 0.0
        miss_rate = 0.0
        score = None

    status = _coincident_validation_status(score, predicted_pairs)
    return {
        "available": predicted_pairs >= _CI_VALIDATION_MIN_PREDICTED_PAIRS,
        "status": status,
        "score": round(score, 3) if score is not None else None,
        "support_rate": round(support_rate, 3),
        "miss_rate": round(miss_rate, 3),
        "predicted_pairs": predicted_pairs,
        "observed_supporting_pairs": observed_supporting_pairs,
        "missed_predicted_pairs": missed_predicted_pairs,
        "unexpected_observed_pairs": unexpected_observed_pairs,
        "frames_used": len(frames),
        "sweep_direction": "CW" if sweep_direction == 1 else "CCW",
        "thresholds": {
            "predicted_bearing_tol_deg": _CI_VALIDATION_PREDICTED_BEARING_TOL_DEG,
            "observed_dt_tol_ms": _CI_VALIDATION_OBSERVED_DT_TOL_MS,
            "unexpected_dt_tol_ms": _CI_VALIDATION_UNEXPECTED_DT_TOL_MS,
            "min_predicted_pairs": _CI_VALIDATION_MIN_PREDICTED_PAIRS,
            "min_score": _CI_VALIDATION_MIN_SCORE,
        },
        "examples": examples,
    }


def _build_selected_lookup(
    selected_observations: list[_IntersectionObservation],
) -> set[tuple[int, str]]:
    return {(obs.frame_index, obs.icao) for obs in selected_observations}


def _reanchor_sweep_frame(frame, candidate_ref_icao: str):
    from .models import SweepFrame, SweepFrameObservation

    if frame.ref_icao == candidate_ref_icao:
        return frame

    new_ref = next((obs for obs in frame.observations if obs.icao == candidate_ref_icao), None)
    if new_ref is None:
        return frame

    new_observations = [
        SweepFrameObservation(
            icao=frame.ref_icao,
            lat=frame.ref_lat,
            lon=frame.ref_lon,
            arrival_us=frame.ref_arrival_us,
            signal_dbfs=None,
            interpolated=False,
        )
    ]
    for obs in frame.observations:
        if obs.icao == candidate_ref_icao:
            continue
        new_observations.append(obs)

    return SweepFrame(
        frame_index=frame.frame_index,
        sweep_start_us=new_ref.arrival_us,
        ref_icao=new_ref.icao,
        ref_lat=new_ref.lat,
        ref_lon=new_ref.lon,
        ref_arrival_us=new_ref.arrival_us,
        observations=new_observations,
        quality=frame.quality,
        period_s=frame.period_s,
    )


def _set_sweep_frame_quality(frame, quality: str):
    from .models import SweepFrame

    if getattr(frame, "quality", None) == quality:
        return frame

    return SweepFrame(
        frame_index=frame.frame_index,
        sweep_start_us=frame.sweep_start_us,
        ref_icao=frame.ref_icao,
        ref_lat=frame.ref_lat,
        ref_lon=frame.ref_lon,
        ref_arrival_us=frame.ref_arrival_us,
        observations=list(frame.observations),
        quality=quality,
        period_s=frame.period_s,
    )


def _score_frame_reference_candidates(
    frame,
    period_s: float,
    anchor_lat: float,
    anchor_lon: float,
) -> list[dict]:
    candidates = [{"icao": frame.ref_icao, "frame": frame}]
    for obs in frame.observations[:_FRAME_REFERENCE_MAX_CANDIDATES]:
        candidates.append({"icao": obs.icao, "frame": _reanchor_sweep_frame(frame, obs.icao)})

    scored = []
    seen = set()
    for candidate in candidates:
        candidate_icao = candidate["icao"]
        if candidate_icao in seen:
            continue
        seen.add(candidate_icao)
        cand_frame = candidate["frame"]
        best_sigma = float("inf")
        best_selected = 0
        best_weight = 0.0
        for direction in (1, -1):
            scored_frames = _preprocess_scoring_frames([cand_frame], period_s, sweep_direction=direction)
            selected, _diag = _select_intersection_observations_with_diagnostics(
                scored_frames,
                min_sin_phi=_INTERSECTION_MIN_SIN_PHI,
            )
            fit = _summarize_residual_fit(anchor_lat, anchor_lon, scored_frames)
            total_selected_weight = sum(obs.quality_weight for obs in selected)
            if fit.residual_sigma_deg < best_sigma:
                best_sigma = fit.residual_sigma_deg
                best_selected = len(selected)
                best_weight = total_selected_weight
        scored.append({
            "candidate_ref_icao": candidate_icao,
            "frame": cand_frame,
            "residual_sigma_deg": best_sigma,
            "selected_observations": best_selected,
            "selected_weight": best_weight,
            "is_actual_reference": candidate_icao == frame.ref_icao,
        })

    scored.sort(key=lambda row: (row["residual_sigma_deg"], -row["selected_observations"], -row["selected_weight"]))
    return scored


def _prepare_frames_for_solving(
    valid_frames: list,
    period_s: float,
    anchor_lat: float,
    anchor_lon: float,
) -> tuple[list, dict]:
    prepared = []
    diagnostics = {
        "frames_considered": len(valid_frames),
        "frames_good": 0,
        "frames_marginal": 0,
        "frames_reanchored": 0,
        "frames_downgraded_to_marginal": 0,
        "frames_dropped_reference_sigma": 0,
        "frame_reference_repairs": [],
    }

    for frame in valid_frames:
        scored = _score_frame_reference_candidates(frame, period_s, anchor_lat, anchor_lon)
        if not scored:
            diagnostics["frames_dropped_reference_sigma"] += 1
            continue

        best = scored[0]
        actual = next((row for row in scored if row["is_actual_reference"]), best)
        if best["residual_sigma_deg"] > _MAX_FRAME_REFERENCE_SIGMA_DEG:
            diagnostics["frames_dropped_reference_sigma"] += 1
            diagnostics["frame_reference_repairs"].append({
                "frame_index": frame.frame_index,
                "actual_ref_icao": frame.ref_icao,
                "best_ref_icao": best["candidate_ref_icao"],
                "actual_sigma_deg": round(actual["residual_sigma_deg"], 2),
                "best_sigma_deg": round(best["residual_sigma_deg"], 2),
                "action": "dropped",
            })
            continue

        target_quality = "good" if best["residual_sigma_deg"] <= _GOOD_FRAME_REFERENCE_SIGMA_DEG else "marginal"

        if (
            best["candidate_ref_icao"] != frame.ref_icao
            and actual["residual_sigma_deg"] > 0
            and actual["residual_sigma_deg"] >= best["residual_sigma_deg"] * _FRAME_REFERENCE_REPAIR_MIN_IMPROVEMENT_RATIO
        ):
            prepared_frame = _set_sweep_frame_quality(best["frame"], target_quality)
            prepared.append(prepared_frame)
            diagnostics["frames_reanchored"] += 1
            if target_quality == "marginal" and getattr(frame, "quality", "good") != "marginal":
                diagnostics["frames_downgraded_to_marginal"] += 1
            diagnostics["frame_reference_repairs"].append({
                "frame_index": frame.frame_index,
                "actual_ref_icao": frame.ref_icao,
                "best_ref_icao": best["candidate_ref_icao"],
                "actual_sigma_deg": round(actual["residual_sigma_deg"], 2),
                "best_sigma_deg": round(best["residual_sigma_deg"], 2),
                "result_quality": target_quality,
                "action": "reanchored",
            })
        else:
            prepared_frame = frame if target_quality == getattr(frame, "quality", "good") else _set_sweep_frame_quality(frame, target_quality)
            prepared.append(prepared_frame)
            if target_quality == "marginal" and getattr(frame, "quality", "good") != "marginal":
                diagnostics["frames_downgraded_to_marginal"] += 1
                diagnostics["frame_reference_repairs"].append({
                    "frame_index": frame.frame_index,
                    "actual_ref_icao": frame.ref_icao,
                    "best_ref_icao": best["candidate_ref_icao"],
                    "actual_sigma_deg": round(actual["residual_sigma_deg"], 2),
                    "best_sigma_deg": round(best["residual_sigma_deg"], 2),
                    "result_quality": target_quality,
                    "action": "downgraded",
                })

        if target_quality == "good":
            diagnostics["frames_good"] += 1
        else:
            diagnostics["frames_marginal"] += 1

    diagnostics["frames_kept"] = len(prepared)
    return prepared, diagnostics


def _run_best_intersection_attempt(
    fm: "ForwardModel",
    frames: list,
    period_s: float,
    receiver_lat: float,
    receiver_lon: float,
    prior_rms_km: Optional[float] = None,
) -> tuple[Optional[dict], int, Optional[dict]]:
    best_intersection: Optional[dict] = None
    best_rms = float("inf")
    best_direction = 1
    best_attempt_detail: Optional[dict] = None
    successful_attempts: dict[int, dict] = {}

    for direction in (1, -1):
        attempt = fm._solve_by_intersection_attempt(
            frames,
            period_s,
            receiver_lat,
            receiver_lon,
            direction=direction,
            prior_rms_km=prior_rms_km,
        )
        attempt_detail = {
            **attempt.get("detail", {}),
            "intersection_direction": "CW" if direction == 1 else "CCW",
            "intersection_reason": attempt.get("reason"),
        }
        if best_attempt_detail is None:
            best_attempt_detail = attempt_detail
        else:
            best_score = (
                best_attempt_detail.get("plausible_candidate_count", 0),
                best_attempt_detail.get("raw_candidate_count", 0),
                best_attempt_detail.get("n_pairs", 0),
                best_attempt_detail.get("selected_observations", 0),
            )
            attempt_score = (
                attempt_detail.get("plausible_candidate_count", 0),
                attempt_detail.get("raw_candidate_count", 0),
                attempt_detail.get("n_pairs", 0),
                attempt_detail.get("selected_observations", 0),
            )
            if attempt_score > best_score:
                best_attempt_detail = attempt_detail

        if attempt["success"]:
            res = attempt["result"]
            successful_attempts[direction] = res
            rms_km = res["rms_km"]
            if rms_km < best_rms:
                best_rms = rms_km
                best_intersection = res
                best_direction = direction

    if 1 in successful_attempts and -1 in successful_attempts:
        cw_res = successful_attempts[1]
        best_intersection = cw_res
        best_direction = 1
    elif best_intersection is not None and best_direction == -1:
        ccw_gate_failures = []
        if best_intersection.get("n_pairs", 0) < _MIN_STANDALONE_CCW_PAIR_COUNT:
            ccw_gate_failures.append("pair_count")
        if best_intersection.get("high_quality_frames", 0) < _MIN_STANDALONE_CCW_HIGH_QUALITY_FRAMES:
            ccw_gate_failures.append("high_quality_frames")
        if best_intersection.get("n_contributing_arcs", 0) < _MIN_STANDALONE_CCW_CONTRIBUTING_ARCS:
            ccw_gate_failures.append("n_contributing_arcs")
        if best_intersection.get("dominance_ratio", 0.0) < _MIN_STANDALONE_CCW_DOMINANCE_RATIO:
            ccw_gate_failures.append("cluster_dominance")
        if ccw_gate_failures:
            best_attempt_detail = {
                **(best_attempt_detail or {}),
                "intersection_direction": "CCW",
                "intersection_reason": f"standalone CCW evidence insufficient ({', '.join(ccw_gate_failures)})",
                "ccw_gate_failures": ccw_gate_failures,
            }
            return None, -1, best_attempt_detail

    return best_intersection, best_direction, best_attempt_detail


def _alternative_reference_diagnostics(
    r_lat: float,
    r_lon: float,
    frame: _ScoredFrame,
    period_s: float,
    selected_lookup: set[tuple[int, str]],
    max_candidates: int = 4,
) -> list[dict]:
    candidates: list[tuple[str, float, float, float, bool]] = []
    frame_selected = {(fi, icao) for fi, icao in selected_lookup if fi == frame.frame_index}

    def _score_reference(ref_label: str, ref_lat: float, ref_lon: float, include_selected: bool) -> None:
        bearing_ref = _bearing_deg(r_lat, r_lon, ref_lat, ref_lon)
        residuals: list[float] = []
        weighted_sq = 0.0
        total_weight = 0.0
        n_selected = 0
        n_total = 0

        for obs in frame.observations:
            selected = (frame.frame_index, obs.icao) in frame_selected
            if include_selected and not selected:
                continue
            if not include_selected and obs.quality_weight <= 0:
                continue

            bearing_obs = _bearing_deg(r_lat, r_lon, obs.lat, obs.lon)
            predicted_phase_deg = (bearing_obs - bearing_ref) % 360.0
            residual_deg = (obs.observed_phase_deg - predicted_phase_deg + 540.0) % 360.0 - 180.0
            residuals.append(residual_deg)
            weighted_sq += obs.quality_weight * (residual_deg ** 2)
            total_weight += obs.quality_weight
            n_total += 1
            if selected:
                n_selected += 1

        if not residuals or total_weight <= 0:
            return

        sigma = math.sqrt(sum(r * r for r in residuals) / len(residuals))
        weighted_rms = math.sqrt(weighted_sq / total_weight)
        mean_residual = sum(residuals) / len(residuals)
        candidates.append((
            ref_label,
            weighted_rms,
            sigma,
            mean_residual,
            ref_label == frame.ref_icao,
        ))

    _score_reference(frame.ref_icao, frame.ref_lat, frame.ref_lon, include_selected=False)
    for obs in frame.observations:
        if (frame.frame_index, obs.icao) not in frame_selected:
            continue
        _score_reference(obs.icao, obs.lat, obs.lon, include_selected=True)

    candidates.sort(key=lambda row: (row[1], row[2], row[0]))
    diagnostics = []
    for ref_label, weighted_rms, sigma, mean_residual, is_actual in candidates[:max_candidates]:
        diagnostics.append({
            "candidate_ref_icao": ref_label,
            "is_actual_reference": is_actual,
            "weighted_rms_deg": round(weighted_rms, 2),
            "residual_sigma_deg": round(sigma, 2),
            "mean_residual_deg": round(mean_residual, 2),
        })
    return diagnostics


def _build_residual_replay_diagnostics(
    r_lat: float,
    r_lon: float,
    scored_frames: list[_ScoredFrame],
    period_s: float,
    selected_lookup: set[tuple[int, str]],
    max_frames: int = 3,
    max_observations_per_frame: int = 8,
) -> dict:
    frame_diagnostics = []
    all_abs_residuals: list[float] = []

    for frame in scored_frames:
        bearing_ref = _bearing_deg(r_lat, r_lon, frame.ref_lat, frame.ref_lon)
        frame_residuals: list[float] = []
        frame_weighted_residual_sq = 0.0
        frame_total_weight = 0.0
        observation_rows = []

        for obs in frame.observations:
            if obs.quality_weight <= 0:
                continue

            bearing_obs = _bearing_deg(r_lat, r_lon, obs.lat, obs.lon)
            predicted_phase_deg = (bearing_obs - bearing_ref) % 360.0
            residual_deg = (obs.observed_phase_deg - predicted_phase_deg + 540.0) % 360.0 - 180.0
            selected = (frame.frame_index, obs.icao) in selected_lookup

            frame_residuals.append(residual_deg)
            frame_weighted_residual_sq += obs.quality_weight * (residual_deg ** 2)
            frame_total_weight += obs.quality_weight
            all_abs_residuals.append(abs(residual_deg))
            observation_rows.append({
                "icao": obs.icao,
                "selected_for_intersection": selected,
                "interpolated": obs.interpolated,
                "quality_weight": round(obs.quality_weight, 4),
                "phase_strength": round(obs.phase_strength, 4),
                "baseline_km": round(obs.baseline_km, 2),
                "azimuth_from_ref_deg": round(obs.azimuth_from_ref_deg, 2),
                "bearing_from_candidate_deg": round(bearing_obs, 2),
                "observed_phase_deg": round(obs.observed_phase_deg, 2),
                "predicted_phase_deg": round(predicted_phase_deg, 2),
                "residual_deg": round(residual_deg, 2),
                "observed_dt_ms": round(_phase_deg_to_dt_ms(obs.observed_phase_deg, period_s), 3),
                "predicted_dt_ms": round(_phase_deg_to_dt_ms(predicted_phase_deg, period_s), 3),
                "residual_dt_ms": round(_phase_deg_to_dt_ms(residual_deg, period_s), 3),
            })

        if not observation_rows:
            continue

        observation_rows.sort(
            key=lambda row: (
                0 if row["selected_for_intersection"] else 1,
                -abs(row["residual_deg"]),
                -row["quality_weight"],
            )
        )
        mean_residual_deg = sum(frame_residuals) / len(frame_residuals)
        residual_sigma_deg = math.sqrt(sum(r * r for r in frame_residuals) / len(frame_residuals))
        weighted_rms_deg = (
            math.sqrt(frame_weighted_residual_sq / frame_total_weight)
            if frame_total_weight > 0
            else float("inf")
        )
        frame_diagnostics.append({
            "frame_index": frame.frame_index,
            "ref_icao": frame.ref_icao,
            "ref_lat": round(frame.ref_lat, 6),
            "ref_lon": round(frame.ref_lon, 6),
            "bearing_ref_deg": round(bearing_ref, 2),
            "n_observations": len(observation_rows),
            "n_selected_observations": sum(1 for row in observation_rows if row["selected_for_intersection"]),
            "mean_residual_deg": round(mean_residual_deg, 2),
            "residual_sigma_deg": round(residual_sigma_deg, 2),
            "weighted_rms_deg": round(weighted_rms_deg, 2),
            "observations": observation_rows[:max_observations_per_frame],
            "alternative_references": _alternative_reference_diagnostics(
                r_lat,
                r_lon,
                frame,
                period_s,
                selected_lookup,
            ),
        })

    frame_diagnostics.sort(
        key=lambda row: (
            -row["weighted_rms_deg"],
            -row["n_selected_observations"],
            row["frame_index"],
        )
    )

    p90_abs_residual_deg = None
    if all_abs_residuals:
        ordered = sorted(all_abs_residuals)
        idx = min(len(ordered) - 1, max(0, math.ceil(len(ordered) * 0.9) - 1))
        p90_abs_residual_deg = ordered[idx]

    summary = {
        "n_frames_scored": len(frame_diagnostics),
        "n_residuals": sum(row["n_observations"] for row in frame_diagnostics),
        "max_abs_residual_deg": round(max(all_abs_residuals), 2) if all_abs_residuals else None,
        "p90_abs_residual_deg": round(p90_abs_residual_deg, 2) if p90_abs_residual_deg is not None else None,
        "worst_frame_index": frame_diagnostics[0]["frame_index"] if frame_diagnostics else None,
        "worst_frame_ref_icao": frame_diagnostics[0]["ref_icao"] if frame_diagnostics else None,
        "worst_frame_weighted_rms_deg": frame_diagnostics[0]["weighted_rms_deg"] if frame_diagnostics else None,
    }
    return {
        "summary": summary,
        "example_frame": frame_diagnostics[0] if frame_diagnostics else None,
        "top_frames": frame_diagnostics[:max_frames],
    }


def _select_intersection_observations(
    scored_frames: list[_ScoredFrame],
    min_sin_phi: float,
    max_per_frame: int = _INTERSECTION_MAX_OBS_PER_FRAME,
    max_per_icao: int = _INTERSECTION_MAX_OBS_PER_ICAO,
) -> list[_IntersectionObservation]:
    selected, _ = _select_intersection_observations_with_diagnostics(
        scored_frames,
        min_sin_phi=min_sin_phi,
        max_per_frame=max_per_frame,
        max_per_icao=max_per_icao,
    )
    return selected


def _select_intersection_observations_with_diagnostics(
    scored_frames: list[_ScoredFrame],
    min_sin_phi: float,
    max_per_frame: int = _INTERSECTION_MAX_OBS_PER_FRAME,
    max_per_icao: int = _INTERSECTION_MAX_OBS_PER_ICAO,
) -> tuple[list[_IntersectionObservation], dict]:
    """Select observations for circle construction.

    Azimuth-bin deduplication is removed: geometric diversity is now enforced
    at the pair level by weighting each intersection by sin(Δaz), the angular
    separation between the two aircraft as seen from the receiver.  This is
    more accurate and does not require assuming a beam width or bin width.
    Per-ICAO and per-frame caps remain to prevent single-aircraft domination.
    """
    selected: list[_IntersectionObservation] = []
    selected_per_icao: dict[str, int] = {}
    diagnostics = {
        "total_scored_observations": 0,
        "dropped_low_quality": 0,
        "dropped_weak_angle": 0,
        "eligible_observations": 0,
        "dropped_per_icao_cap": 0,
        "dropped_duplicate_icao_in_frame": 0,
        "dropped_per_frame_cap": 0,
    }

    for frame_index, frame in enumerate(scored_frames):
        candidates: list[_IntersectionObservation] = []
        for obs in frame.observations:
            diagnostics["total_scored_observations"] += 1
            if obs.quality_weight < _MIN_OBSERVATION_QUALITY:
                diagnostics["dropped_low_quality"] += 1
                continue
            if obs.phase_strength < min_sin_phi:
                diagnostics["dropped_weak_angle"] += 1
                continue
            diagnostics["eligible_observations"] += 1
            candidates.append(_IntersectionObservation(
                frame_index=frame_index,
                icao=obs.icao,
                ref_lat=frame.ref_lat,
                ref_lon=frame.ref_lon,
                obs_lat=obs.lat,
                obs_lon=obs.lon,
                observed_phase_deg=obs.observed_phase_deg,
                quality_weight=obs.quality_weight,
                azimuth_from_ref_deg=obs.azimuth_from_ref_deg,
                interpolated=obs.interpolated,
            ))

        candidates.sort(key=lambda obs: obs.quality_weight, reverse=True)
        frame_selected: list[_IntersectionObservation] = []

        for candidate in candidates:
            if len(frame_selected) >= max_per_frame:
                diagnostics["dropped_per_frame_cap"] += 1
                break
            if selected_per_icao.get(candidate.icao, 0) >= max_per_icao:
                diagnostics["dropped_per_icao_cap"] += 1
                continue
            if any(existing.icao == candidate.icao for existing in frame_selected):
                diagnostics["dropped_duplicate_icao_in_frame"] += 1
                continue
            frame_selected.append(candidate)
            selected_per_icao[candidate.icao] = selected_per_icao.get(candidate.icao, 0) + 1

        selected.extend(frame_selected)

    diagnostics["selected_observations"] = len(selected)
    return selected, diagnostics


def _resolve_intersection_candidates(
    candidates: list[_IntersectionCandidate],
    cluster_radius_km: float = _INTERSECTION_CLUSTER_RADIUS_KM,
    dominance_ratio: float = _INTERSECTION_CLUSTER_DOMINANCE_RATIO,
) -> Optional[_IntersectionResolution]:
    """Pick the dominant weighted cluster and return its weighted center and scatter."""
    if not candidates:
        return None

    neighborhoods: list[tuple[float, int, list[_IntersectionCandidate]]] = []
    for idx, center in enumerate(candidates):
        members = [
            candidate
            for candidate in candidates
            if math.hypot(candidate.x_km - center.x_km, candidate.y_km - center.y_km) <= cluster_radius_km
        ]
        total_weight = sum(candidate.weight for candidate in members)
        neighborhoods.append((total_weight, idx, members))

    neighborhoods.sort(key=lambda item: item[0], reverse=True)
    best_weight, best_idx, best_members = neighborhoods[0]
    best_center = candidates[best_idx]

    second_weight = 0.0
    for weight, idx, _members in neighborhoods[1:]:
        center = candidates[idx]
        if math.hypot(center.x_km - best_center.x_km, center.y_km - best_center.y_km) > cluster_radius_km:
            second_weight = weight
            break

    if second_weight > 0.0 and best_weight < second_weight * dominance_ratio:
        return None

    total_weight = sum(candidate.weight for candidate in best_members)
    if total_weight <= 0:
        return None

    mean_x = sum(candidate.x_km * candidate.weight for candidate in best_members) / total_weight
    mean_y = sum(candidate.y_km * candidate.weight for candidate in best_members) / total_weight
    rms_km = math.sqrt(
        sum(
            candidate.weight * ((candidate.x_km - mean_x) ** 2 + (candidate.y_km - mean_y) ** 2)
            for candidate in best_members
        ) / total_weight
    )
    return _IntersectionResolution(
        mean_x_km=mean_x,
        mean_y_km=mean_y,
        rms_km=rms_km,
        best_weight=best_weight,
        second_weight=second_weight,
        member_count=len(best_members),
    )


def _build_intersection_clusters(
    candidates: list[_IntersectionCandidate],
    cluster_radius_km: float = _INTERSECTION_CLUSTER_RADIUS_KM,
) -> list[_IntersectionCluster]:
    """Build distinct weighted neighborhoods from candidate intersections."""
    if not candidates:
        return []

    def _build_neighborhood(center: _IntersectionCandidate) -> _IntersectionCluster | None:
        members = [
            c for c in candidates
            if math.hypot(c.x_km - center.x_km, c.y_km - center.y_km) <= cluster_radius_km
        ]
        total_weight = sum(c.weight for c in members)
        if total_weight <= 0:
            return None
        mean_x = sum(c.x_km * c.weight for c in members) / total_weight
        mean_y = sum(c.y_km * c.weight for c in members) / total_weight
        rms_km = math.sqrt(
            sum(c.weight * ((c.x_km - mean_x) ** 2 + (c.y_km - mean_y) ** 2) for c in members)
            / total_weight
        )
        return _IntersectionCluster(
            mean_x_km=mean_x,
            mean_y_km=mean_y,
            rms_km=rms_km,
            total_weight=total_weight,
            member_count=len(members),
            center_x_km=center.x_km,
            center_y_km=center.y_km,
            contributing_arc_indices=frozenset(),  # deferred; filled in for distinct clusters only
        )

    neighborhoods = [nb for c in candidates if (nb := _build_neighborhood(c)) is not None]
    neighborhoods.sort(
        key=lambda cluster: (cluster.total_weight, cluster.member_count, -cluster.rms_km),
        reverse=True,
    )

    distinct: list[_IntersectionCluster] = []
    for cluster in neighborhoods:
        if any(
            math.hypot(
                cluster.center_x_km - kept.center_x_km,
                cluster.center_y_km - kept.center_y_km,
            ) <= cluster_radius_km
            for kept in distinct
        ):
            continue
        # Compute contributing arcs only for the clusters that survive deduplication.
        members = [
            c for c in candidates
            if math.hypot(c.x_km - cluster.center_x_km, c.y_km - cluster.center_y_km) <= cluster_radius_km
        ]
        contributing = frozenset(arc for c in members for arc in (c.arc_i, c.arc_j))
        distinct.append(dataclasses.replace(cluster, contributing_arc_indices=contributing))

    return distinct


def _summarize_selected_observations(
    selected_observations: list[_IntersectionObservation],
    origin_lat: float,
    origin_lon: float,
) -> dict:
    if not selected_observations:
        return {
            "n_selected_observations": 0,
            "interpolated_fraction": 1.0,
            "azimuth_spread_deg": 0.0,
            "high_quality_frames": 0,
            "total_selected_weight": 0.0,
        }

    frame_buckets: dict[int, list[_IntersectionObservation]] = {}
    azimuths_deg: list[float] = []
    interpolated_count = 0
    total_selected_weight = 0.0
    for obs in selected_observations:
        frame_buckets.setdefault(obs.frame_index, []).append(obs)
        azimuths_deg.append(_bearing_deg(origin_lat, origin_lon, obs.obs_lat, obs.obs_lon))
        total_selected_weight += obs.quality_weight
        if obs.interpolated:
            interpolated_count += 1

    high_quality_frames = 0
    for frame_observations in frame_buckets.values():
        frame_weight = sum(obs.quality_weight for obs in frame_observations)
        if len(frame_observations) >= 2 and frame_weight >= 0.5:
            high_quality_frames += 1

    return {
        "n_selected_observations": len(selected_observations),
        "interpolated_fraction": interpolated_count / len(selected_observations),
        "azimuth_spread_deg": _circular_spread_deg(azimuths_deg),
        "high_quality_frames": high_quality_frames,
        "total_selected_weight": total_selected_weight,
    }


def _passes_intersection_quality_gates(metrics: dict) -> tuple[bool, Optional[str]]:
    if metrics.get("azimuth_spread_deg", 0.0) < _MIN_INTERSECTION_AZ_SPREAD_DEG:
        return False, "azimuth_spread"
    if metrics.get("high_quality_frames", 0) < _MIN_HIGH_QUALITY_FRAMES:
        return False, "high_quality_frames"
    if metrics.get("n_pairs", 0) < _MIN_INTERSECTION_PAIR_COUNT:
        return False, "pair_count"
    if metrics.get("interpolated_fraction", 1.0) > _MAX_INTERPOLATED_FRACTION:
        return False, "interpolated_fraction"
    if metrics.get("receiver_distance_m", 0.0) > (_MAX_INTERSECTION_RECEIVER_DISTANCE_NM * _NM_TO_M):
        return False, "receiver_distance"
    if (
        metrics.get("intersection_direction") == "CCW"
        and metrics.get("receiver_distance_m", 0.0) > (_MAX_CCW_INTERSECTION_RECEIVER_DISTANCE_NM * _NM_TO_M)
    ):
        return False, "ccw_receiver_distance"
    if metrics.get("n_contributing_arcs", 0) < _MIN_CONTRIBUTING_ARCS:
        return False, "n_contributing_arcs"
    if metrics.get("dominance_ratio", 0.0) < _INTERSECTION_CLUSTER_DOMINANCE_RATIO:
        return False, "cluster_dominance"
    return True, None


def score_candidate_position(
    r_lat: float,
    r_lon: float,
    sweep_frames: list,
    period_s: float,
    sweep_direction: int = 1,  # +1 = clockwise, -1 = counterclockwise
) -> tuple[float, float, list[float], list[float]]:
    """Score a candidate radar position against all SweepFrames.

    For each aircraft in each frame:
      observed_phase  = (t_obs - t_ref) / T_sweep * 360  (degrees, modulo 360)
      predicted_phase = (bearing(R->obs) - bearing(R->ref)) * sweep_direction  (degrees, modulo 360)
      residual        = circular_difference(observed, predicted)  (degrees, [-180, 180])

    The sweep_direction parameter allows the optimiser to try both directions.
    For clockwise: predicted = bearing_obs - bearing_ref
    For counterclockwise: predicted = bearing_ref - bearing_obs

    Args:
        r_lat: Candidate radar latitude
        r_lon: Candidate radar longitude
        sweep_frames: List of SweepFrame objects
        period_s: Rotation period in seconds
        sweep_direction: +1 for clockwise, -1 for counterclockwise

    Returns:
        (score, mean_residual_deg, all_residuals_deg, all_azimuths_deg)
        score: weighted sum of squared residuals (degrees^2)
    """
    scored_frames = _preprocess_scoring_frames(
        sweep_frames,
        period_s,
        sweep_direction=sweep_direction,
    )
    return _score_candidate_position_preprocessed(r_lat, r_lon, scored_frames)


# ── Directional signal detection ────────────────────────────────────────────

def _detect_directional_signal(
    residuals_deg: list[float],
    azimuths_deg: list[float],
) -> Optional[dict]:
    """Detect sinusoidal pattern in residuals vs azimuth.

    Bins residuals by azimuth, fits a sinusoid, returns amplitude/direction/displacement.
    """
    if len(residuals_deg) < _AZ_BINS:
        return None

    bins_sum: list[float] = [0.0] * _AZ_BINS
    bins_count: list[int] = [0] * _AZ_BINS

    for r, az in zip(residuals_deg, azimuths_deg):
        bin_idx = int(az / 10.0) % _AZ_BINS
        bins_sum[bin_idx] += r
        bins_count[bin_idx] += 1

    bin_means = []
    bin_centers = []
    for i in range(_AZ_BINS):
        if bins_count[i] > 0:
            bin_means.append(bins_sum[i] / bins_count[i])
            bin_centers.append(math.radians(i * 10 + 5))

    if len(bin_means) < 4:
        return None

    n = len(bin_means)
    mean_r = sum(bin_means) / n
    centered_r = [r - mean_r for r in bin_means]

    sum_cr_sin = sum(r * math.sin(theta) for r, theta in zip(centered_r, bin_centers))
    sum_cr_cos = sum(r * math.cos(theta) for r, theta in zip(centered_r, bin_centers))

    a = 2.0 / n * sum_cr_sin
    b = 2.0 / n * sum_cr_cos

    amplitude_deg = math.sqrt(a ** 2 + b ** 2)

    if amplitude_deg < _DIRECTIONAL_THRESHOLD:
        return None

    theta0_rad = math.pi / 2 - math.atan2(b, a)
    direction_deg = (math.degrees(theta0_rad) + 360) % 360

    typical_range_km = 150.0
    displacement_km = amplitude_deg * typical_range_km / 360.0 * 2 * math.pi

    return {
        "amplitude_deg": round(amplitude_deg, 2),
        "direction_deg": round(direction_deg, 1),
        "displacement_km": round(displacement_km, 1),
    }


# ── Hypothesis result ───────────────────────────────────────────────────────

class HypothesisResult:
    def __init__(self, airport_icao, airport_name, airport_lat, airport_lon,
                 score, residual_sigma_deg, n_aircraft, n_observations,
                 n_frames, directional_signal=None):
        self.airport_icao = airport_icao
        self.airport_name = airport_name
        self.airport_lat = airport_lat
        self.airport_lon = airport_lon
        self.score = score
        self.residual_sigma_deg = residual_sigma_deg
        self.n_aircraft = n_aircraft
        self.n_observations = n_observations
        self.n_frames = n_frames
        self.directional_signal = directional_signal


# ── ForwardModel class ───────────────────────────────────────────────────────

class ForwardModel:
    """Forward model radar localisation via SSR beam phase fitting.

    Uses per-sweep phase-difference observations (SweepFrames) to determine
    radar position, bootstrapping from known airport locations.
    """

    def __init__(
        self,
        max_range_nm: float = _AIRPORT_TIER3_NM,
        airports_path: Path = _AIRPORTS_PATH,
    ) -> None:
        import threading
        self._max_range_nm = max_range_nm
        self._airports_path = airports_path
        self._airports: list[_AirportCandidate] = []
        self._airports_loaded = False
        self._receiver_lat: Optional[float] = None
        self._receiver_lon: Optional[float] = None
        self._frame_positions: dict[int, deque] = {}
        self._frame_positions_lock = threading.Lock()
        self._frame_positions_loaded: set[int] = set()  # IIDs whose DB rows have been loaded

    def _ensure_airports(self, receiver_lat: float, receiver_lon: float) -> list[_AirportCandidate]:
        if self._airports_loaded:
            return self._airports
        self._receiver_lat = receiver_lat
        self._receiver_lon = receiver_lon
        self._airports = _load_airports(self._airports_path, receiver_lat, receiver_lon, self._max_range_nm)
        self._airports_loaded = True
        return self._airports

    # ── Per-frame position accumulation ─────────────────────────────────

    def on_new_frame(
        self,
        iid: int,
        frame,
        period_s: float,
        receiver_lat: Optional[float],
        receiver_lon: Optional[float],
    ) -> None:
        """Called for each new completed sweep frame. Solve and accumulate."""
        if receiver_lat is None or receiver_lon is None:
            return
        est = self.solve_single_frame(iid, frame, period_s, receiver_lat, receiver_lon)
        if est is not None:
            self._add_frame_position(iid, est)

    def solve_single_frame(
        self,
        iid: int,
        frame,
        period_s: float,
        receiver_lat: float,
        receiver_lon: float,
    ) -> Optional["FramePositionEstimate"]:
        """Solve a single sweep frame's position using the inscribed-angle method.

        Tries both rotation directions; returns the tighter result or None if
        the frame geometry is insufficient to produce a reliable estimate.
        """
        if getattr(frame, "quality", "insufficient") == "insufficient":
            return None

        best: Optional[dict] = None
        for direction in (1, -1):
            result = ForwardModel._solve_by_intersection_attempt(
                [frame],
                period_s,
                receiver_lat,
                receiver_lon,
                direction=direction,
                max_per_frame=_PER_FRAME_MAX_OBS,
                max_per_icao=_PER_FRAME_MAX_OBS,
            )
            if not result.get("success"):
                continue
            r = result["result"]
            if best is None or r["rms_km"] < best["rms_km"]:
                best = r

        if best is None:
            return None

        n_arcs = best.get("n_contributing_arcs", 0)
        cep_km = best.get("centroid_uncertainty_km", float("inf"))
        if n_arcs < _PER_FRAME_MIN_CONTRIBUTING_ARCS or cep_km >= _PER_FRAME_MAX_CEP_KM:
            return None

        # Azimuth spread: bearings from the estimated position to each selected observation.
        selected = best.get("n_selected_observations", 0)
        azimuth_spread = best.get("azimuth_spread_deg", 0.0)

        weight = n_arcs / (cep_km + 0.5) ** 2
        return FramePositionEstimate(
            frame_index=getattr(frame, "frame_index", 0),
            sweep_start_us=getattr(frame, "ref_arrival_us", 0.0),
            lat=best["lat"],
            lon=best["lon"],
            cep_km=cep_km,
            n_contributing_arcs=n_arcs,
            azimuth_spread_deg=azimuth_spread,
            weight=weight,
            cluster_dominance_ratio=best.get("dominance_ratio", 0.0),
            interpolated_position_fraction=best.get("interpolated_fraction", 0.0),
        )

    # ── Per-frame buffer helpers ─────────────────────────────────────────

    def _load_db_frame_positions(self, iid: int) -> None:
        """Populate the in-memory buffer from DB. Must be called under the lock."""
        self._frame_positions_loaded.add(iid)
        try:
            from db import stats_db
            rows = stats_db.load_frame_positions(iid, limit=_PER_FRAME_BUFFER_MAX)
            if not rows:
                return
            buf: deque = deque(maxlen=_PER_FRAME_BUFFER_MAX)
            for r in rows:
                buf.append(FramePositionEstimate(
                    frame_index=r["frame_index"],
                    sweep_start_us=r["sweep_start_us"],
                    lat=r["lat"],
                    lon=r["lon"],
                    cep_km=r["cep_km"],
                    n_contributing_arcs=r["n_contributing_arcs"],
                    azimuth_spread_deg=r["azimuth_spread_deg"],
                    weight=r["weight"],
                    # Legacy rows lack quality columns; use pass-through defaults
                    # so Stage 1 Mahalanobis gate does the actual quality work.
                    cluster_dominance_ratio=r.get("cluster_dominance_ratio") or 2.0,
                    interpolated_position_fraction=r.get("interpolated_position_fraction") or 0.0,
                ))
            self._frame_positions[iid] = buf
            log.info("ForwardModel: IID %d — loaded %d frame positions from DB", iid, len(buf))
        except Exception:
            log.exception("ForwardModel: IID %d — failed to load frame positions from DB", iid)

    def _add_frame_position(self, iid: int, estimate: "FramePositionEstimate") -> None:
        with self._frame_positions_lock:
            if iid not in self._frame_positions_loaded:
                self._load_db_frame_positions(iid)
            if iid not in self._frame_positions:
                self._frame_positions[iid] = deque(maxlen=_PER_FRAME_BUFFER_MAX)
            self._frame_positions[iid].append(estimate)
        try:
            from db import stats_db
            stats_db.insert_frame_position(iid, estimate)
        except Exception:
            log.exception("ForwardModel: IID %d — failed to persist frame position", iid)

    def get_frame_positions(self, iid: int) -> list["FramePositionEstimate"]:
        with self._frame_positions_lock:
            if iid not in self._frame_positions_loaded:
                self._load_db_frame_positions(iid)
            return list(self._frame_positions.get(iid, []))

    def clear_frame_positions(self, iid: int) -> None:
        with self._frame_positions_lock:
            self._frame_positions.pop(iid, None)
            self._frame_positions_loaded.discard(iid)
        try:
            from db import stats_db
            stats_db.clear_frame_positions(iid)
        except Exception:
            log.exception("ForwardModel: IID %d — failed to clear frame positions from DB", iid)

    def remove_frame_position(self, iid: int, frame_index: int) -> bool:
        """Remove a single per-frame estimate by frame_index. Returns True if found."""
        removed_sweep_start_us: Optional[float] = None
        with self._frame_positions_lock:
            buf = self._frame_positions.get(iid)
            if not buf:
                return False
            for e in buf:
                if e.frame_index == frame_index:
                    removed_sweep_start_us = e.sweep_start_us
                    break
            new_buf: deque = deque(
                (e for e in buf if e.frame_index != frame_index),
                maxlen=_PER_FRAME_BUFFER_MAX,
            )
            if len(new_buf) == len(buf):
                return False
            self._frame_positions[iid] = new_buf
        if removed_sweep_start_us is not None:
            try:
                from db import stats_db
                stats_db.delete_frame_position(iid, removed_sweep_start_us)
            except Exception:
                log.exception("ForwardModel: IID %d — failed to delete frame position from DB", iid)
        return True

    def remove_frame_positions_bulk(self, iid: int, sweep_start_us_list: list[float]) -> int:
        """Remove multiple per-frame estimates by sweep_start_us. Returns count removed."""
        if not sweep_start_us_list:
            return 0
        to_remove = frozenset(sweep_start_us_list)
        removed = 0
        with self._frame_positions_lock:
            buf = self._frame_positions.get(iid)
            if not buf:
                return 0
            new_buf: deque = deque(
                (e for e in buf if e.sweep_start_us not in to_remove),
                maxlen=_PER_FRAME_BUFFER_MAX,
            )
            removed = len(buf) - len(new_buf)
            if removed > 0:
                self._frame_positions[iid] = new_buf
        if removed > 0:
            try:
                from db import stats_db
                stats_db.delete_frame_positions_bulk(iid, sweep_start_us_list)
            except Exception:
                log.exception("ForwardModel: IID %d — failed to bulk delete frame positions", iid)
        return removed

    def compute_weighted_centroid(
        self,
        iid: int,
        receiver_lat: Optional[float] = None,
        receiver_lon: Optional[float] = None,
    ) -> Optional[dict]:
        """Weighted centroid of all per-frame position estimates for an IID.

        Uses the four-stage frame filter (quality pre-filter, per-frame Mahalanobis
        gate, Huberized sample-covariance second pass, iterative refinement) to
        reject outlier frames before computing the inverse-variance weighted mean.

        Args:
            iid: Interrogator ID.
            receiver_lat: Receiver latitude.  Falls back to config if not given.
            receiver_lon: Receiver longitude.  Falls back to config if not given.

        Returns:
            dict with lat, lon, cep_km, n_estimates, total_weight,
            plus filter diagnostics (n_total, n_stage0_survivors, n_inliers,
            rejection_counts) when available.  None if no estimates or < 2 inliers.
        """
        estimates = self.get_frame_positions(iid)
        if not estimates:
            return None

        if receiver_lat is None or receiver_lon is None:
            import config as _cfg
            receiver_lat = getattr(_cfg, "RECEIVER_LAT", None)
            receiver_lon = getattr(_cfg, "RECEIVER_LON", None)

        if receiver_lat is None or receiver_lon is None:
            return None

        from .frame_filter import filter_frame_estimates
        result = filter_frame_estimates(estimates, receiver_lat, receiver_lon)

        if result.lat is None:
            return None

        return {
            "lat": result.lat,
            "lon": result.lon,
            "cep_km": result.sigma_combined_m / 1000.0,
            "n_estimates": result.n_inliers,
            "total_weight": 0.0,  # weight sum not needed by callers
            "n_total": result.n_total,
            "n_stage0_survivors": result.n_stage0_survivors,
            "n_inliers": result.n_inliers,
            "rejection_counts": result.rejection_counts,
        }

    # ── Airport hypothesis test ──────────────────────────────────────────

    def run_airport_hypothesis(
        self,
        iid: int,
        period_s: float,
        sweep_frames: list,
        receiver_lat: float,
        receiver_lon: float,
    ) -> list[HypothesisResult]:
        """Score all candidate airports as potential radar positions.

        Args:
            iid: Interrogator ID
            period_s: Rotation period in seconds
            sweep_frames: List of SweepFrame objects
            receiver_lat: Receiver latitude
            receiver_lon: Receiver longitude

        Returns:
            List of HypothesisResult sorted by score (lowest first).
        """
        n_frames = len([f for f in sweep_frames if f.quality in ("good", "marginal")])
        if n_frames < _MIN_FRAMES:
            return []

        # Use most recent frames for scoring — oldest frames (before reference selection
        # stabilised) are noisier and more data should improve estimates.
        valid_frames = [f for f in sweep_frames if f.quality in ("good", "marginal")]
        frames_to_score = valid_frames[-_HYPOTHESIS_MAX_FRAMES:]

        airports = self._ensure_airports(receiver_lat, receiver_lon)
        if not airports:
            return []

        # Count unique aircraft across scored frames
        all_icaos = set()
        n_observations = 0
        for frame in frames_to_score:
            all_icaos.add(frame.ref_icao)
            n_observations += 1 + len(frame.observations)
            for obs in frame.observations:
                all_icaos.add(obs.icao)

        results = []
        scored_frames = _preprocess_scoring_frames(frames_to_score, period_s, sweep_direction=1)
        for ap in airports:
            score, mean_res, residuals, azimuths = _score_candidate_position_preprocessed(
                ap.lat, ap.lon, scored_frames,
            )

            if score == float("inf"):
                continue

            residual_sigma = (
                math.sqrt(sum(r * r for r in residuals) / len(residuals))
                if residuals else 0.0
            )

            directional = _detect_directional_signal(residuals, azimuths)

            results.append(HypothesisResult(
                airport_icao=ap.icao,
                airport_name=ap.name,
                airport_lat=ap.lat,
                airport_lon=ap.lon,
                score=round(score, 2),
                residual_sigma_deg=round(residual_sigma, 2),
                n_aircraft=len(all_icaos),
                n_observations=n_observations,
                n_frames=n_frames,
                directional_signal=directional,
            ))

        results.sort(key=lambda r: r.score)
        log.info(
            "ForwardModel: hypothesis test for IID %d — %d airports scored, "
            "best=%s (score=%.0f), %d frames, %d observations",
            iid, len(results),
            results[0].airport_icao if results else "none",
            results[0].score if results else 0,
            n_frames, n_observations,
        )
        return results

    # ── 2D optimisation ──────────────────────────────────────────────────

    def run_2d_optimisation(
        self,
        iid: int,
        period_s: float,
        initial_guess: tuple[float, float],
        sweep_frames: list,
    ) -> Optional[tuple[float, float, float, int]]:
        """Run 2D Nelder-Mead optimisation from a starting position.

        Args:
            iid: Interrogator ID
            period_s: Rotation period in seconds
            initial_guess: (lat, lon) starting point
            sweep_frames: List of SweepFrame objects

        Returns:
            (lat, lon, cep_m, n_observations) or None on failure.
        """
        try:
            from scipy.optimize import minimize
        except ImportError:
            log.error("ForwardModel: scipy not available for optimisation")
            return None

        if len(sweep_frames) < _MIN_FRAMES:
            return None

        # Use most recent quality frames — same set as the hypothesis test uses.
        valid_frames = [f for f in sweep_frames if f.quality in ("good", "marginal")]
        frames_to_use = valid_frames[-_OPTIM_MAX_FRAMES:]
        if len(frames_to_use) < len(valid_frames):
            log.info(
                "ForwardModel: IID %d — using %d of %d frames (capped at %d)",
                iid, len(frames_to_use), len(sweep_frames), _OPTIM_MAX_FRAMES,
            )

        start_lat, start_lon = initial_guess

        # Try both sweep directions and pick the best
        import time as _time
        best_result = None
        best_direction = None
        best_score = float("inf")
        preprocessed_by_direction = {
            direction: _preprocess_scoring_frames(frames_to_use, period_s, sweep_direction=direction)
            for direction in (1, -1)
        }

        for direction in (1, -1):
            direction_label = "clockwise" if direction == 1 else "counterclockwise"
            scored_frames = preprocessed_by_direction[direction]

            def objective(x: list[float], d=direction) -> float:
                score, _, _, _ = _score_candidate_position_preprocessed(
                    x[0], x[1], scored_frames,
                )
                return score

            dir_start = _time.monotonic()
            _timed_out = False

            def _timeout_callback(x) -> bool:
                nonlocal _timed_out
                if _time.monotonic() - dir_start > _OPTIM_TIMEOUT_S:
                    _timed_out = True
                    return True  # returning True stops Nelder-Mead immediately
                return False

            try:
                result = minimize(
                    fun=objective,
                    x0=[start_lat, start_lon],
                    method="Nelder-Mead",
                    callback=_timeout_callback,
                    options={
                        "maxiter": _OPTIM_MAX_ITER,
                        "xatol": 0.0001,
                        "fatol": 1e-6,
                        "adaptive": True,
                        "disp": False,
                    },
                )

                elapsed = _time.monotonic() - dir_start
                if _timed_out:
                    log.warning(
                        "ForwardModel: direction %s timed out after %.1fs — skipping",
                        direction_label, elapsed,
                    )
                    continue
                if result.fun < best_score:
                    lat, lon = float(result.x[0]), float(result.x[1])
                    # Sanity-check: must be a valid geographic coordinate and
                    # within 700 NM of the receiver (SSR range + generous margin).
                    import config as _cfg
                    recv_lat = getattr(_cfg, "RECEIVER_LAT", None)
                    recv_lon = getattr(_cfg, "RECEIVER_LON", None)
                    plausible = (
                        -90.0 <= lat <= 90.0
                        and -180.0 <= lon <= 180.0
                        and (
                            recv_lat is None
                            or _haversine_m(recv_lat, recv_lon, lat, lon) <= 700 * _NM_TO_M
                        )
                    )
                    if not plausible:
                        log.warning(
                            "ForwardModel: direction %s — result (%.4f, %.4f) outside "
                            "plausible bounds — discarding",
                            direction_label, lat, lon,
                        )
                        continue
                    dist_m = _haversine_m(start_lat, start_lon, lat, lon)
                    best_result = result
                    best_direction = direction
                    best_score = result.fun
                    log.info(
                        "ForwardModel: direction %s — score=%.0f at (%.4f, %.4f) "
                        "(%.0f km from seed) in %.1fs",
                        direction_label, result.fun, lat, lon, dist_m / 1000, elapsed,
                    )
                else:
                    log.debug(
                        "ForwardModel: direction %s — score=%.0f worse than best=%.0f (%.1fs)",
                        direction_label, result.fun, best_score, elapsed,
                    )
            except Exception as e:
                log.warning("ForwardModel: optimisation failed for direction %s: %s", direction_label, e)

        if best_result is None:
            log.warning("ForwardModel: optimisation did not converge for IID %d (any direction)", iid)
            return None

        result = best_result
        lat, lon = float(result.x[0]), float(result.x[1])

        log.info(
            "ForwardModel: IID %d — best direction: %s (score=%.0f)",
            iid, "clockwise" if best_direction == 1 else "counterclockwise", best_score,
        )

        # Estimate CEP from final score
        score = float(result.fun)
        total_weight = sum(
            obs.quality_weight
            for frame in preprocessed_by_direction[best_direction]
            for obs in frame.observations
        )
        rms_deg = math.sqrt(score / total_weight) if total_weight > 0 else 0.0

        # Convert degrees RMS to metres: at range d, 1° ≈ d * π/180
        # For typical range 150km: 1° ≈ 2.6km → rough conversion
        typical_range_m = 150_000.0
        cep_m = typical_range_m * math.radians(rms_deg)

        if cep_m > 500_000:
            log.debug("ForwardModel: CEP implausibly large (%.0f m) for IID %d — discarding", cep_m, iid)
            return None
        if cep_m > 50_000:
            log.info("ForwardModel: large CEP (%.0f m) for IID %d — weak signal", cep_m, iid)

        n_obs = sum(1 + len(f.observations) for f in sweep_frames)
        log.info(
            "ForwardModel: optimised position for IID %d: (%.4f, %.4f) "
            "cep=%.0f m from %d observations (%d frames)",
            iid, lat, lon, cep_m, n_obs, len(sweep_frames),
        )
        return (lat, lon, cep_m, n_obs)

    def _refine_intersection_seed(
        self,
        seed_lat: float,
        seed_lon: float,
        scored_frames: list[_ScoredFrame],
        receiver_lat: float,
        receiver_lon: float,
    ) -> Optional[tuple[float, float, _ResidualFit]]:
        """Bounded local refinement from an intersection seed using full residual scoring."""
        try:
            from scipy.optimize import minimize
        except ImportError:
            return None

        seed_fit = _summarize_residual_fit(seed_lat, seed_lon, scored_frames)
        if not math.isfinite(seed_fit.score):
            return None

        max_move_m = _REFINE_MAX_MOVE_KM * 1000.0

        def objective(x: list[float]) -> float:
            lat, lon = float(x[0]), float(x[1])
            if _haversine_m(seed_lat, seed_lon, lat, lon) > max_move_m:
                return seed_fit.score * 10.0
            fit = _summarize_residual_fit(lat, lon, scored_frames)
            if not math.isfinite(fit.score):
                return seed_fit.score * 10.0
            return fit.score

        try:
            result = minimize(
                fun=objective,
                x0=[seed_lat, seed_lon],
                method="Nelder-Mead",
                options={
                    "maxiter": 120,
                    "xatol": 0.00005,
                    "fatol": 1e-4,
                    "adaptive": True,
                    "disp": False,
                },
            )
        except Exception:
            return None

        cand_lat, cand_lon = float(result.x[0]), float(result.x[1])
        if _haversine_m(seed_lat, seed_lon, cand_lat, cand_lon) > max_move_m:
            return None
        if _haversine_m(receiver_lat, receiver_lon, cand_lat, cand_lon) > 700 * _NM_TO_M:
            return None

        refined_fit = _summarize_residual_fit(cand_lat, cand_lon, scored_frames)
        if not math.isfinite(refined_fit.score):
            return None
        return (cand_lat, cand_lon, refined_fit)

    # ── Grid search fallback ─────────────────────────────────────────────

    def run_grid_search(
        self,
        iid: int,
        period_s: float,
        sweep_frames: list,
        receiver_lat: float,
        receiver_lon: float,
    ) -> Optional[tuple[float, float, float, int]]:
        """Coarse grid search over a wide area, then local optimisation."""
        grid_radius_km = _GRID_RADIUS_KM
        grid_spacing_deg = _GRID_SPACING_DEG

        radius_lat_deg = grid_radius_km / 111.0
        radius_lon_deg = grid_radius_km / (111.0 * math.cos(math.radians(receiver_lat)))

        best_score = float("inf")
        best_lat = receiver_lat
        best_lon = receiver_lon
        scored_frames = _preprocess_scoring_frames(sweep_frames, period_s, sweep_direction=1)

        lat_start = receiver_lat - radius_lat_deg
        lat_end = receiver_lat + radius_lat_deg
        lon_start = receiver_lon - radius_lon_deg
        lon_end = receiver_lon + radius_lon_deg

        n_points = 0
        lat_val = lat_start
        while lat_val <= lat_end and n_points < _GRID_MAX_POINTS:
            lon_val = lon_start
            while lon_val <= lon_end and n_points < _GRID_MAX_POINTS:
                dist_m = _haversine_m(receiver_lat, receiver_lon, lat_val, lon_val)
                if dist_m <= grid_radius_km * 1000:
                    score, _, _, _ = _score_candidate_position_preprocessed(
                        lat_val, lon_val, scored_frames,
                    )
                    if score < best_score:
                        best_score = score
                        best_lat = lat_val
                        best_lon = lon_val
                    n_points += 1
                lon_val += grid_spacing_deg
            lat_val += grid_spacing_deg

        if n_points == 0 or best_score == float("inf"):
            log.warning("ForwardModel: grid search found no candidates for IID %d", iid)
            return None

        log.info(
            "ForwardModel: grid search for IID %d — %d points, best at (%.4f, %.4f) "
            "score=%.0f",
            iid, n_points, best_lat, best_lon, best_score,
        )

        return self.run_2d_optimisation(iid, period_s, (best_lat, best_lon), sweep_frames)

    # ── Inscribed-angle circle intersection solver ────────────────────────

    @staticmethod
    def solve_by_intersection(
        sweep_frames: list,
        period_s: float,
        origin_lat: float,
        origin_lon: float,
        direction: int = 1,
        min_sin_phi: float = 0.15,
    ) -> Optional[dict]:
        """Direct geometric solver using inscribed-angle circles.

        For each (reference, observation) pair in each frame the locus of valid
        radar positions is a circular arc — the inscribed angle theorem applied
        to the CW bearing difference.  Circle-circle intersections from different
        pairs cluster at the radar position.

        Returns solve metadata dict or None if insufficient data.

        Args:
            sweep_frames:  list of SweepFrame objects
            period_s:      radar rotation period
            origin_lat/lon: local Cartesian origin (typically receiver position)
            direction:     +1 clockwise, -1 counterclockwise
            min_sin_phi:   skip pairs whose |sin(Δφ)| is below this (weak constraint;
                           ~8.6° from 0° or 180°)
        """
        attempt = ForwardModel._solve_by_intersection_attempt(
            sweep_frames,
            period_s,
            origin_lat,
            origin_lon,
            direction=direction,
            min_sin_phi=min_sin_phi,
        )
        if not attempt["success"]:
            return None
        return attempt["result"]

    @staticmethod
    def _solve_by_intersection_attempt(
        sweep_frames: list,
        period_s: float,
        origin_lat: float,
        origin_lon: float,
        direction: int = 1,
        min_sin_phi: float = _INTERSECTION_MIN_SIN_PHI,
        prior_rms_km: Optional[float] = None,
        max_per_frame: int = _INTERSECTION_MAX_OBS_PER_FRAME,
        max_per_icao: int = _INTERSECTION_MAX_OBS_PER_ICAO,
    ) -> dict:
        """Run one intersection attempt and return result or detailed failure diagnostics."""
        R_KM = _R_EARTH / 1000.0
        cos_orig = math.cos(math.radians(origin_lat))
        cluster_radius_km = (
            max(_ADAPTIVE_CLUSTER_RADIUS_K * prior_rms_km, _ADAPTIVE_CLUSTER_RADIUS_FLOOR_KM)
            if prior_rms_km is not None
            else _INTERSECTION_CLUSTER_RADIUS_KM
        )

        def to_xy(lat: float, lon: float) -> tuple[float, float]:
            x = (lon - origin_lon) * math.radians(1) * R_KM * cos_orig
            y = (lat - origin_lat) * math.radians(1) * R_KM
            return x, y

        def from_xy(x: float, y: float) -> tuple[float, float]:
            lat = origin_lat + math.degrees(y / R_KM)
            lon = origin_lon + math.degrees(x / (R_KM * cos_orig))
            return lat, lon

        # (cx, cy, R, w, rx_bearing_deg) — rx_bearing is from receiver to obs aircraft
        circles: list[tuple[float, float, float, float, float]] = []
        valid_frames = [f for f in sweep_frames if f.quality in ("good", "marginal")]
        frames_to_use = valid_frames[-_OPTIM_MAX_FRAMES:]
        scored_frames = _preprocess_scoring_frames(frames_to_use, period_s, sweep_direction=direction)
        selected_observations, selection_diagnostics = _select_intersection_observations_with_diagnostics(
            scored_frames,
            min_sin_phi=min_sin_phi,
            max_per_frame=max_per_frame,
            max_per_icao=max_per_icao,
        )
        selection_summary = _summarize_selected_observations(
            selected_observations,
            origin_lat,
            origin_lon,
        )
        detail = {
            **selection_diagnostics,
            **selection_summary,
            "n_frames_considered": len(frames_to_use),
            "n_valid_frames": len(valid_frames),
            "n_scored_frames": len(scored_frames),
            "n_pairs": 0,
            "degenerate_baseline_pairs": 0,
            "raw_candidate_count": 0,
            "plausible_candidate_count": 0,
            "receiver_distance_m": None,
        }

        for obs in selected_observations:
            ax, ay = to_xy(obs.ref_lat, obs.ref_lon)
            bx, by = to_xy(obs.obs_lat, obs.obs_lon)

            phi_rad = math.radians(obs.observed_phase_deg)
            sin_phi = math.sin(phi_rad)

            dx, dy = bx - ax, by - ay
            d = math.hypot(dx, dy)
            if d < 0.1:
                detail["degenerate_baseline_pairs"] += 1
                continue

            R = d / (2.0 * abs(sin_phi))
            px, py = -(dy / d), dx / d
            h = -(d / 2.0) * (math.cos(phi_rad) / sin_phi)
            cx = (ax + bx) / 2.0 + h * px
            cy = (ay + by) / 2.0 + h * py

            rx_bearing = _bearing_deg(origin_lat, origin_lon, obs.obs_lat, obs.obs_lon)
            circles.append((cx, cy, R, obs.quality_weight, rx_bearing))
            detail["n_pairs"] += 1

        if len(circles) < 2:
            return {
                "success": False,
                "reason": "too few qualifying pairs",
                "detail": detail,
            }

        # Vectorised circle-circle intersection using numpy.
        # NumPy's C-level operations release the Python GIL, so the Beast
        # decoder thread runs freely during this computation.
        arr = np.array(circles, dtype=np.float64)  # (n, 5): cx, cy, R, w, rx_bearing_deg
        cx_a = arr[:, 0]; cy_a = arr[:, 1]; R_a = arr[:, 2]; w_a = arr[:, 3]; brx_a = arr[:, 4]
        n_c = len(arr)
        ii, jj = np.triu_indices(n_c, k=1)

        ddx = cx_a[jj] - cx_a[ii]
        ddy = cy_a[jj] - cy_a[ii]
        dist = np.hypot(ddx, ddy)
        R1 = R_a[ii]; R2 = R_a[jj]
        valid = (dist >= 1e-6) & (dist <= R1 + R2 + 1e-6) & (dist >= np.abs(R1 - R2) - 1e-6)

        valid_ii = ii[valid]; valid_jj = jj[valid]
        ddx = ddx[valid]; ddy = ddy[valid]; dist = dist[valid]
        R1 = R1[valid]; R2 = R2[valid]

        # Pair weight = w_i × w_j × sin(Δaz): down-weights pairs where both aircraft
        # are at similar azimuth from the receiver, since their arcs are nearly parallel
        # and intersect at a shallow angle (poor localisation).
        # _angular_separation_deg vectorised: abs((a-b+180)%360-180) → [0, 180]
        delta_az = np.abs((brx_a[valid_ii] - brx_a[valid_jj] + 180.0) % 360.0 - 180.0)
        delta_az = np.minimum(delta_az, 90.0)  # sin is symmetric around 90°
        az_weight = np.sin(np.radians(delta_az))
        cw = w_a[valid_ii] * w_a[valid_jj] * az_weight

        mx0 = cx_a[valid_ii]; my0 = cy_a[valid_ii]

        a = (R1 * R1 - R2 * R2 + dist * dist) / (2.0 * dist)
        h_val = np.sqrt(np.maximum(R1 * R1 - a * a, 0.0))
        mx = mx0 + a * ddx / dist
        my = my0 + a * ddy / dist

        x1 = mx + h_val * ddy / dist
        y1 = my - h_val * ddx / dist
        x2 = mx - h_val * ddy / dist
        y2 = my + h_val * ddx / dist
        has_two = h_val > 1e-6

        cand_x = np.concatenate([x1, x2[has_two]])
        cand_y = np.concatenate([y1, y2[has_two]])
        cand_w = np.concatenate([cw, cw[has_two]])
        cand_arc_i = np.concatenate([valid_ii, valid_ii[has_two]])
        cand_arc_j = np.concatenate([valid_jj, valid_jj[has_two]])

        candidates = [
            _IntersectionCandidate(x_km=float(x), y_km=float(y), weight=float(w), arc_i=int(ai), arc_j=int(aj))
            for x, y, w, ai, aj in zip(cand_x, cand_y, cand_w, cand_arc_i, cand_arc_j)
        ]

        detail["raw_candidate_count"] = len(candidates)
        if not candidates:
            return {
                "success": False,
                "reason": "no circle intersections",
                "detail": detail,
            }

        max_km = 700 * _NM_TO_M / 1000.0
        plausible = [
            candidate
            for candidate in candidates
            if math.hypot(candidate.x_km, candidate.y_km) <= max_km
        ]
        detail["plausible_candidate_count"] = len(plausible)
        if not plausible:
            return {
                "success": False,
                "reason": "all intersection candidates were implausibly distant",
                "detail": detail,
            }

        clusters = _build_intersection_clusters(plausible, cluster_radius_km=cluster_radius_km)
        detail["cluster_count"] = len(clusters)
        if not clusters:
            return {
                "success": False,
                "reason": "candidate cloud was ambiguous",
                "detail": detail,
            }
        top_clusters = clusters[: min(3, len(clusters))]
        ranked_clusters: list[tuple[float, _IntersectionCluster]] = []
        for cluster in top_clusters:
            cand_lat, cand_lon = from_xy(cluster.mean_x_km, cluster.mean_y_km)
            fit_score, _, _, _ = _score_candidate_position_preprocessed(cand_lat, cand_lon, scored_frames)
            ranked_clusters.append((fit_score, cluster))

        ranked_clusters.sort(key=lambda item: item[0])
        best_fit_score, best_cluster = ranked_clusters[0]
        if not math.isfinite(best_fit_score):
            return {
                "success": False,
                "reason": "candidate cloud was ambiguous",
                "detail": detail,
            }

        second_fit_score = float("inf")
        second_cluster: Optional[_IntersectionCluster] = None
        if len(ranked_clusters) > 1:
            second_fit_score, second_cluster = ranked_clusters[1]
            fit_ratio = second_fit_score / best_fit_score if best_fit_score > 0 else float("inf")
            if (
                second_cluster is not None
                and fit_ratio < _INTERSECTION_FIT_DOMINANCE_RATIO
                and second_cluster.total_weight >= best_cluster.total_weight * _INTERSECTION_SECONDARY_WEIGHT_FRACTION
            ):
                return {
                    "success": False,
                    "reason": "candidate cloud was ambiguous",
                    "detail": {
                        **detail,
                        "fit_dominance_ratio": fit_ratio,
                        "cluster_count": len(clusters),
                    },
                }

        lat, lon = from_xy(best_cluster.mean_x_km, best_cluster.mean_y_km)
        receiver_distance_m = _haversine_m(origin_lat, origin_lon, lat, lon)
        detail["receiver_distance_m"] = receiver_distance_m
        dominance_ratio = (
            second_fit_score / best_fit_score
            if best_fit_score > 0.0 and math.isfinite(second_fit_score)
            else float("inf")
        )
        n_contributing_arcs = len(best_cluster.contributing_arc_indices)
        # σ_centroid ≈ RMS_scatter / √N_arcs: corrects for the correlation between
        # intersection points that share a common arc (not independent observations).
        centroid_uncertainty_km = best_cluster.rms_km / math.sqrt(max(1, n_contributing_arcs))
        return {
            "success": True,
            "result": {
                "lat": lat,
                "lon": lon,
                "rms_km": best_cluster.rms_km,
                "centroid_uncertainty_km": centroid_uncertainty_km,
                "n_contributing_arcs": n_contributing_arcs,
                "n_pairs": len(circles),
                "n_selected_observations": selection_summary["n_selected_observations"],
                "interpolated_fraction": selection_summary["interpolated_fraction"],
                "azimuth_spread_deg": selection_summary["azimuth_spread_deg"],
                "high_quality_frames": selection_summary["high_quality_frames"],
                "total_selected_weight": selection_summary["total_selected_weight"],
                "cluster_member_count": best_cluster.member_count,
                "cluster_best_weight": best_cluster.total_weight,
                "cluster_second_weight": second_cluster.total_weight if second_cluster is not None else 0.0,
                "dominance_ratio": dominance_ratio,
                "receiver_distance_m": receiver_distance_m,
                "selection_diagnostics": selection_diagnostics,
                "raw_candidate_count": detail["raw_candidate_count"],
                "plausible_candidate_count": detail["plausible_candidate_count"],
                "degenerate_baseline_pairs": detail["degenerate_baseline_pairs"],
                "cluster_count": len(clusters),
                "fit_score": best_fit_score,
                "cluster_radius_km": cluster_radius_km,
            },
            "detail": detail,
        }

    # ── Full pipeline ────────────────────────────────────────────────────

    def run_full_pipeline(
        self,
        iid: int,
        radar_state: "RadarState",
    ) -> Optional[dict]:
        """Run the complete forward model localisation pipeline.

        Primary solver: inscribed-angle circle intersection.  For each
        (reference, observation) pair the locus of valid radar positions is a
        circular arc.  Circle-circle intersections cluster at the radar position.
        The MAD-trimmed mean of all surviving candidates is the estimate.

        This runs in pure Python in a few milliseconds — no scipy, no GIL stall,
        no initial-guess dependency.  The position improves naturally as more
        frames accumulate.

        run_2d_optimisation (Nelder-Mead) is still available via the manual
        /api/radar/iids/{iid}/fm-run endpoint for precision refinement on demand.

        Returns a result dict on success, or a failure dict with "error" key.
        """
        import config as _config
        receiver_lat = getattr(_config, "RECEIVER_LAT", None)
        receiver_lon = getattr(_config, "RECEIVER_LON", None)

        if receiver_lat is None or receiver_lon is None:
            return {"error": "Receiver coordinates not configured", "stage": "config"}

        model = radar_state.get_rotation_model(iid)
        if model is None or model.period_s is None:
            reason = "no rotation model" if model is None else f"rotation model has no period (status={model.status})"
            log.warning("ForwardModel: no rotation model for IID %d (%s)", iid, reason)
            return {"error": reason, "stage": "rotation_model"}

        period_s = model.period_s

        # Fast path: if enough per-frame estimates have accumulated, skip the
        # expensive intersection solver entirely.  The centroid integrates many
        # independent frames and is always more accurate than a single pool solve.
        n_raw = len(self.get_frame_positions(iid))
        filter_ran = n_raw >= 20
        centroid = self.compute_weighted_centroid(iid) if filter_ran else None
        if centroid is not None:
            lat = centroid["lat"]
            lon = centroid["lon"]
            cep_m = centroid["cep_km"] * 1000.0
            source = "frame_accumulation"
            n_obs = centroid["n_estimates"]
            existing_cep = model.fm_cep_m
            _REGRESS_FACTOR = 3.0
            _POOR_CEP_M = 20_000.0
            if (
                existing_cep is None
                or cep_m <= existing_cep * _REGRESS_FACTOR
                or existing_cep >= _POOR_CEP_M
            ):
                radar_state.update_forward_model_location(
                    iid=iid,
                    lat=lat,
                    lon=lon,
                    cep_m=cep_m,
                    n_observations=n_obs,
                    window_s=0.0,
                    source=source,
                    coincident_validation=None,
                )
                stored = True
            else:
                stored = False
            log.info(
                "ForwardModel: IID %d — frame_accumulation centroid from %d estimates "
                "(%.4f, %.4f) cep=%.0f m%s",
                iid, n_obs, lat, lon, cep_m,
                "" if stored else " (not stored — regression guard)",
            )
            return {
                "iid": iid,
                "lat": lat,
                "lon": lon,
                "cep_m": cep_m,
                "stored": stored,
                "n_observations": n_obs,
                "source": source,
                "period_s": period_s,
            }

        if filter_ran:
            reason = f"frame filter found no stable cluster ({n_raw} frames)"
            log.debug("ForwardModel: IID %d — %s", iid, reason)
            return {"error": reason, "stage": "no_cluster", "n_estimates": n_raw}
        reason = f"accumulating frame estimates ({n_raw}/20 needed)"
        log.debug("ForwardModel: IID %d — %s", iid, reason)
        return {"error": reason, "stage": "accumulating", "n_estimates": n_raw}
