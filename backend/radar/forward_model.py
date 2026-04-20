"""
radar/forward_model.py — Forward model radar localisation via SSR beam phase fitting.

Uses per-sweep phase-difference observations (not co-sweep TDOA) to determine
radar position. The all-pairs intersection path uses every unordered aircraft
pair within each SweepFrame as a bearing-difference constraint. The radar
position is the point where predicted pair bearing differences match observed
pair phase differences.

Method:
  1. Build SweepFrames from burst centroids (done in sweep.py)
  2. For each candidate airport, score by comparing bearing differences to phase differences
  3. Run 2D optimisation from best airport
  4. Store result in RadarIID.fm_lat/fm_lon/fm_cep_m
"""

from __future__ import annotations

import dataclasses
from collections import defaultdict, deque
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
ACCUM_MAX_FRAME_CEP_KM = 20.0
"""Maximum single-frame CEP admitted to long-term accumulation.

This is deliberately much lower than the diagnostic per-frame display cap:
high-CEP frame solves are useful to inspect, but should not dilute the
long-term centroid merely because they claim large uncertainty.
"""
ACCUM_MIN_INLIER_PAIR_CIRCLES = 6
ACCUM_MIN_INLIER_PAIR_CIRCLES_RELAXED = 4
ACCUM_RELAXED_MIN_SUPPORT_SCORE = 1.1
ACCUM_RELAXED_MAX_PAIRWISE_WEIGHTED_RMS_DEG = 25.0
ACCUM_MIN_SUPPORT_SCORE = 0.8
ACCUM_MIN_SUPPORT_DOMINANCE_RATIO = 1.25
ACCUM_MAX_PAIRWISE_WEIGHTED_RMS_DEG = 35.0
# Geometry-dominant admission tier: strong member/weight dominance can bypass
# pairwise RMS and support-dominance gates, but at reduced accumulation weight.
ACCUM_GEOM_DOMINANT_MIN_MEMBER_COUNT = 6      # best cluster must have at least this many members
ACCUM_GEOM_DOMINANT_MIN_MEMBER_RATIO = 2.0    # best_members / second_members threshold
ACCUM_GEOM_DOMINANT_MIN_WEIGHT_RATIO = 1.5    # best_weight / second_weight threshold
ACCUM_GEOM_DOMINANT_WEIGHT_SCALE = 0.5        # accumulation weight multiplier for geometry-dominant frames
_PER_FRAME_BUFFER_MAX = 5000
_PER_FRAME_TRIM_FRACTION = 0.05   # drop outermost 5% by distance before centroid
_INTERSECTION_CLUSTER_RADIUS_KM = 40.0       # bootstrap default; tightened adaptively once converged
_ADAPTIVE_CLUSTER_RADIUS_K = 2.5             # cluster radius = K × prior_rms when converged
_ADAPTIVE_CLUSTER_RADIUS_FLOOR_KM = 5.0
_ENDPOINT_INTERSECTION_REJECT_KM = 1.0       # remove circle intersections at source aircraft endpoints
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
_INTERSECTION_SUPPORT_KEEP_FRACTION = 0.25
_FRAME_REFERENCE_MAX_CANDIDATES = 4
_FRAME_REFERENCE_REPAIR_MIN_IMPROVEMENT_RATIO = 1.15
_GOOD_FRAME_REFERENCE_SIGMA_DEG = 70.0
_MAX_FRAME_REFERENCE_SIGMA_DEG = 105.0
_MARGINAL_FRAME_WEIGHT_SCALE = 0.6
_QUALITY_AMBIGUITY_THRESHOLD = 1.35
_QUALITY_SUPPORT_SCALE = 1.0
_QUALITY_COMPACTNESS_SCALE_KM = 15.0
_QUALITY_COMPACTNESS_EXPONENT = 1.5
_QUALITY_CONDITIONING_SCALE = 0.5
_QUALITY_CONDITIONING_FLOOR = 0.01
_QUALITY_DIVERSITY_SCALE = 0.3
_QUALITY_MAX_REFINED_SUBSET = 30
_QUALITY_GREEDY_MIN_EIGEN_GAIN = 0.005
_QUALITY_NORMALIZED_RESIDUAL_INLIER = 2.5
_QUALITY_DUPLICATE_BEARING_TOL_DEG = 3.0
_QUALITY_PER_AIRCRAFT_CAP = 6
_CLUSTER_MERGE_DISTANCE_FRACTION = 0.45
_CLUSTER_MERGE_RMS_DISTANCE_MULT = 1.6
_CLUSTER_MERGE_MAX_DISTANCE_KM = 18.0
_CLUSTER_MERGE_MIN_ARC_JACCARD = 0.40
_CLUSTER_MERGE_MIN_ARC_OVERLAP = 0.60
_SAME_LOBE_DISTANCE_RMS_MULT = 2.0
_SAME_LOBE_MAX_DISTANCE_KM = 24.0
_SAME_LOBE_MIN_ARC_JACCARD = 0.25
_SAME_LOBE_MIN_ARC_OVERLAP = 0.50
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
    n_replies: int = 1
    position_age_seconds: float = 0.0


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
    n_replies: int = 1
    position_age_seconds: float = 0.0


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
    merged_cluster_indices: tuple[int, ...] = ()


def _cluster_arc_overlap_metrics(
    a: _IntersectionCluster,
    b: _IntersectionCluster,
) -> tuple[float, float]:
    """Return (jaccard, min-overlap-ratio) for contributing arc sets."""
    if not a.contributing_arc_indices or not b.contributing_arc_indices:
        return 0.0, 0.0
    intersection = len(a.contributing_arc_indices & b.contributing_arc_indices)
    union = len(a.contributing_arc_indices | b.contributing_arc_indices)
    if union <= 0:
        return 0.0, 0.0
    min_size = min(len(a.contributing_arc_indices), len(b.contributing_arc_indices))
    if min_size <= 0:
        return 0.0, 0.0
    return intersection / union, intersection / min_size


def _cluster_same_lobe_metrics(
    a: _IntersectionCluster,
    b: _IntersectionCluster,
    cluster_radius_km: float,
) -> dict:
    """Compute same-lobe proximity/overlap metrics from final cluster estimates."""
    centroid_separation_km = math.hypot(a.mean_x_km - b.mean_x_km, a.mean_y_km - b.mean_y_km)
    jaccard, overlap_min = _cluster_arc_overlap_metrics(a, b)
    rms_scale_km = max(a.rms_km, b.rms_km, 1e-3)
    merge_distance_limit_km = min(
        _CLUSTER_MERGE_MAX_DISTANCE_KM,
        max(
            _CLUSTER_MERGE_DISTANCE_FRACTION * cluster_radius_km,
            _CLUSTER_MERGE_RMS_DISTANCE_MULT * rms_scale_km,
        ),
    )
    same_lobe_distance_limit_km = min(
        _SAME_LOBE_MAX_DISTANCE_KM,
        max(
            0.6 * cluster_radius_km,
            _SAME_LOBE_DISTANCE_RMS_MULT * rms_scale_km,
        ),
    )
    merge_like = (
        centroid_separation_km <= merge_distance_limit_km
        and (jaccard >= _CLUSTER_MERGE_MIN_ARC_JACCARD or overlap_min >= _CLUSTER_MERGE_MIN_ARC_OVERLAP)
    )
    very_close = centroid_separation_km <= max(1.0, min(a.rms_km, b.rms_km))
    same_lobe = (
        centroid_separation_km <= same_lobe_distance_limit_km
        and (
            jaccard >= _SAME_LOBE_MIN_ARC_JACCARD
            or overlap_min >= _SAME_LOBE_MIN_ARC_OVERLAP
            or very_close
        )
    )
    return {
        "centroid_separation_km": centroid_separation_km,
        "arc_jaccard": jaccard,
        "arc_overlap_min_ratio": overlap_min,
        "merge_distance_limit_km": merge_distance_limit_km,
        "same_lobe_distance_limit_km": same_lobe_distance_limit_km,
        "merge_like": merge_like,
        "same_lobe": same_lobe,
    }


def _dedupe_near_duplicate_clusters(
    clusters: list[_IntersectionCluster],
    cluster_radius_km: float,
) -> tuple[list[_IntersectionCluster], list[dict]]:
    """Merge near-duplicate clusters that represent the same candidate lobe."""
    if not clusters:
        return [], []

    merged: list[_IntersectionCluster] = []
    merge_events: list[dict] = []

    for idx, cluster in enumerate(clusters):
        merged_into: Optional[int] = None
        merge_reason: Optional[dict] = None
        for kept_idx, kept in enumerate(merged):
            metrics = _cluster_same_lobe_metrics(cluster, kept, cluster_radius_km)
            if not metrics["merge_like"]:
                continue
            merged_into = kept_idx
            merge_reason = metrics
            break
        if merged_into is None:
            merged.append(cluster)
            continue

        kept = merged[merged_into]
        # Keep the dominant representative (weight, then members), but preserve support provenance.
        keep_current = (
            cluster.total_weight > kept.total_weight
            or (
                math.isclose(cluster.total_weight, kept.total_weight, rel_tol=1e-9, abs_tol=1e-9)
                and cluster.member_count > kept.member_count
            )
        )
        dominant = cluster if keep_current else kept
        other = kept if keep_current else cluster
        dominant_sources = set(dominant.merged_cluster_indices or ())
        dominant_sources.update(other.merged_cluster_indices or ())
        if not dominant_sources:
            dominant_sources = {idx, merged_into}
        merged[merged_into] = dataclasses.replace(
            dominant,
            contributing_arc_indices=frozenset(
                dominant.contributing_arc_indices | other.contributing_arc_indices
            ),
            merged_cluster_indices=tuple(sorted(dominant_sources)),
        )
        merge_events.append({
            "source_cluster_index": idx,
            "target_cluster_index": merged_into,
            "kept_cluster_source_indices": list(merged[merged_into].merged_cluster_indices),
            "centroid_separation_km": round(float(merge_reason["centroid_separation_km"]), 3) if merge_reason else None,
            "arc_jaccard": round(float(merge_reason["arc_jaccard"]), 3) if merge_reason else None,
            "arc_overlap_min_ratio": round(float(merge_reason["arc_overlap_min_ratio"]), 3) if merge_reason else None,
            "merge_distance_limit_km": round(float(merge_reason["merge_distance_limit_km"]), 3) if merge_reason else None,
            "merge_reason": "final_centroid_plus_support_overlap",
        })

    return merged, merge_events


def _evaluate_cluster_ambiguity(
    best_cluster: _IntersectionCluster,
    second_cluster: Optional[_IntersectionCluster],
    best_quality_score: float,
    second_quality_score: float,
    best_support_score: float,
    second_support_score: float,
    raw_inlier_count: int,
    cluster_radius_km: float,
) -> dict:
    """Evaluate ambiguity with same-lobe bypass on final cluster estimates."""
    quality_ratio = (
        best_quality_score / second_quality_score
        if second_quality_score > 0.0
        else float("inf")
    )
    support_ratio = (
        best_support_score / second_support_score
        if second_support_score > 0.0
        else float("inf")
    )

    if raw_inlier_count >= 8:
        effective_threshold = _QUALITY_AMBIGUITY_THRESHOLD
    elif raw_inlier_count >= 4:
        t = (raw_inlier_count - 4) / 4.0
        effective_threshold = 1.15 + t * (_QUALITY_AMBIGUITY_THRESHOLD - 1.15)
    else:
        effective_threshold = 1.15
    if best_quality_score >= 2.0 and quality_ratio >= 1.1:
        effective_threshold = 1.05

    second_member_count = second_cluster.member_count if second_cluster is not None else 0
    second_weight = second_cluster.total_weight if second_cluster is not None else 0.0
    member_ratio = (
        best_cluster.member_count / second_member_count
        if second_member_count > 0 else float("inf")
    )
    weight_ratio = (
        best_cluster.total_weight / second_weight
        if second_weight > 0.0 else float("inf")
    )
    is_geometry_dominant = (
        best_cluster.member_count >= ACCUM_GEOM_DOMINANT_MIN_MEMBER_COUNT
        and (
            not math.isfinite(member_ratio)
            or member_ratio >= ACCUM_GEOM_DOMINANT_MIN_MEMBER_RATIO
        )
        and (
            not math.isfinite(weight_ratio)
            or weight_ratio >= ACCUM_GEOM_DOMINANT_MIN_WEIGHT_RATIO
        )
    )

    same_lobe_metrics: dict = {}
    same_lobe_bypass = False
    if second_cluster is not None:
        same_lobe_metrics = _cluster_same_lobe_metrics(best_cluster, second_cluster, cluster_radius_km)
        same_lobe_bypass = bool(same_lobe_metrics.get("same_lobe", False))

    is_ambiguous = False
    ambiguity_reason = ""
    if not is_geometry_dominant and quality_ratio < effective_threshold:
        if second_cluster is not None:
            second_is_competitive = (
                second_cluster.total_weight
                >= best_cluster.total_weight * _INTERSECTION_SECONDARY_WEIGHT_FRACTION
            )
            if second_is_competitive and not same_lobe_bypass:
                is_ambiguous = True
                ambiguity_reason = (
                    f"quality ratio {quality_ratio:.2f} below threshold {effective_threshold:.2f}"
                )

    return {
        "quality_ratio": quality_ratio,
        "support_ratio": support_ratio,
        "effective_threshold": effective_threshold,
        "member_ratio": member_ratio,
        "weight_ratio": weight_ratio,
        "second_member_count": second_member_count,
        "is_geometry_dominant_cluster": is_geometry_dominant,
        "same_lobe_metrics": same_lobe_metrics,
        "same_lobe_bypass": same_lobe_bypass,
        "is_ambiguous": is_ambiguous,
        "ambiguity_reason": ambiguity_reason,
    }


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
    best_cluster_support_score: float = 0.0
    support_dominance_ratio: float = 0.0
    pairwise_weighted_rms_deg: float = float("inf")
    cluster_member_count: int = 0
    second_cluster_member_count: int = 0
    member_dominance_ratio: float = 0.0
    weight_dominance_ratio: float = 0.0
    admission_tier: str = "accepted_high_confidence"


@dataclass
class _ResidualFit:
    score: float
    mean_residual_deg: float
    residual_sigma_deg: float
    n_residuals: int


@dataclass
class _PairwiseResidualFit:
    score: float
    mean_residual_deg: float
    residual_sigma_deg: float
    weighted_rms_deg: float
    n_residuals: int
    residuals_deg: list[float]


def _cluster_inlier_circles(
    cluster: _IntersectionCluster,
    admitted_by_arr_index: list,
    selected_rows: list[tuple],
    cluster_radius_km: float,
    max_normalized_residual: float = _QUALITY_NORMALIZED_RESIDUAL_INLIER,
) -> tuple[list[int], list[float]]:
    """Return indices of admitted circles consistent with a candidate cluster.

    A circle is an inlier if:
    - its normalized residual to the cluster centre is within threshold
    - at least one of its intersection candidates lies within the cluster radius

    Returns:
        inlier_indices: admitted-array indices of inlier circles
        all_residuals: normalized residuals for ALL admitted circles (indexed by admitted-array index)
    """
    inlier_indices: list[int] = []
    residuals: list[float] = []

    for idx, (sc, raw) in enumerate(zip(admitted_by_arr_index, selected_rows)):
        cx_km, cy_km, R_km, _w, _brg, sigma_km, _idx = raw
        sigma_km = max(float(sigma_km), 1e-6)
        distance_to_centre = math.hypot(cluster.mean_x_km - cx_km, cluster.mean_y_km - cy_km)
        normalized = abs(distance_to_centre - R_km) / sigma_km
        residuals.append(normalized)
        if normalized <= max_normalized_residual:
            inlier_indices.append(idx)

    return inlier_indices, residuals


def _cluster_local_information_matrix(
    candidate_x_km: float,
    candidate_y_km: float,
    inlier_indices: list[int],
    admitted_by_arr_index: list,
    selected_rows: list[tuple],
    normalized_residuals: list[float],
) -> tuple[np.ndarray, float, float, list[float]]:
    """Build the 2x2 local information matrix from inlier circle normals.

    For each inlier circle with centre C and candidate point P, the constraint
    normal is the unit vector from C to P.  We accumulate weighted outer products:
        J = sum_i w_i * n_i * n_i^T

    Returns:
        J: 2x2 numpy array
        lambda_min: smallest eigenvalue
        lambda_max: largest eigenvalue
        weights: list of per-inlier weights
    """
    J = np.zeros((2, 2), dtype=np.float64)
    weights: list[float] = []

    for idx in inlier_indices:
        sc = admitted_by_arr_index[idx]
        raw = selected_rows[idx]
        cx_km, cy_km = float(raw[0]), float(raw[1])
        circle_score = float(sc.circle_score)

        # Normal direction: from circle centre toward candidate point
        dx = candidate_x_km - cx_km
        dy = candidate_y_km - cy_km
        norm = math.hypot(dx, dy)
        if norm < 1e-9:
            continue
        nx, ny = dx / norm, dy / norm

        # Weight combines circle score with residual consistency
        residual = normalized_residuals[idx] if idx < len(normalized_residuals) else 3.0
        residual_weight = math.exp(-0.5 * min(residual, 3.0) ** 2)
        w = circle_score * residual_weight

        weights.append(w)
        # Accumulate outer product: w * n * n^T
        J[0, 0] += w * nx * nx
        J[0, 1] += w * nx * ny
        J[1, 0] += w * ny * nx
        J[1, 1] += w * ny * ny

    # Eigenvalue analysis
    if np.all(J == 0.0):
        return J, 0.0, 0.0, weights

    eigvals = np.linalg.eigvalsh(J)
    lambda_min = float(eigvals[0])
    lambda_max = float(eigvals[1])

    return J, lambda_min, lambda_max, weights


def _select_refined_cluster_subset(
    candidate_x_km: float,
    candidate_y_km: float,
    inlier_indices: list[int],
    admitted_by_arr_index: list,
    selected_rows: list[tuple],
    normalized_residuals: list[float],
    max_subset_size: int = _QUALITY_MAX_REFINED_SUBSET,
    min_eigen_gain: float = _QUALITY_GREEDY_MIN_EIGEN_GAIN,
    per_aircraft_cap: int = _QUALITY_PER_AIRCRAFT_CAP,
    duplicate_bearing_tol: float = _QUALITY_DUPLICATE_BEARING_TOL_DEG,
) -> dict:
    """Greedy marginal information gain selection for the best-supporting subset.

    Start from the strongest inlier, then iteratively add the pair that maximizes
    increase in lambda_min of the information matrix, subject to:
    - normalized residual within threshold
    - not a near-duplicate of already selected geometry
    - per-aircraft redundancy control

    Returns a dict with:
        selected_indices: list of admitted indices in the refined subset
        excluded_indices: list of inlier indices not selected
        exclusion_reasons: dict mapping excluded index to reason
        J_final: final 2x2 information matrix
        lambda_min_final: final smallest eigenvalue
        lambda_max_final: final largest eigenvalue
    """
    if not inlier_indices:
        return {
            "selected_indices": [],
            "excluded_indices": [],
            "exclusion_reasons": {},
            "J_final": np.zeros((2, 2)),
            "lambda_min_final": 0.0,
            "lambda_max_final": 0.0,
        }

    # Sort inliers by circle score (descending) to seed from strongest
    scored_inliers = sorted(
        inlier_indices,
        key=lambda idx: float(admitted_by_arr_index[idx].circle_score),
        reverse=True,
    )

    selected_indices: list[int] = []
    excluded_indices: list[int] = []
    exclusion_reasons: dict[int, str] = {}
    aircraft_counts: dict[str, int] = {}
    selected_bearings: list[float] = []

    # Track current information matrix incrementally
    J = np.zeros((2, 2), dtype=np.float64)
    lambda_min_current = 0.0

    def _add_to_matrix(idx: int) -> None:
        nonlocal J, lambda_min_current
        sc = admitted_by_arr_index[idx]
        raw = selected_rows[idx]
        cx_km, cy_km = float(raw[0]), float(raw[1])

        dx = candidate_x_km - cx_km
        dy = candidate_y_km - cy_km
        norm = math.hypot(dx, dy)
        if norm < 1e-9:
            return

        nx, ny = dx / norm, dy / norm
        residual = normalized_residuals[idx] if idx < len(normalized_residuals) else 3.0
        residual_weight = math.exp(-0.5 * min(residual, 3.0) ** 2)
        w = float(sc.circle_score) * residual_weight

        J[0, 0] += w * nx * nx
        J[0, 1] += w * nx * ny
        J[1, 0] += w * ny * nx
        J[1, 1] += w * ny * ny

        eigvals = np.linalg.eigvalsh(J)
        lambda_min_current = float(eigvals[0])

    def _try_add(idx: int) -> tuple[bool, str]:
        """Test whether adding idx would improve conditioning. Returns (ok, reason)."""
        sc = admitted_by_arr_index[idx]
        raw = selected_rows[idx]
        cx_km, cy_km = float(raw[0]), float(raw[1])
        residual = normalized_residuals[idx] if idx < len(normalized_residuals) else 3.0

        # Check residual threshold
        if residual > _QUALITY_NORMALIZED_RESIDUAL_INLIER:
            return False, "high_residual"

        # Check per-aircraft cap
        icao_a = sc.icao_a
        icao_b = sc.icao_b
        if aircraft_counts.get(icao_a, 0) >= per_aircraft_cap:
            return False, f"aircraft_cap_{icao_a}"
        if aircraft_counts.get(icao_b, 0) >= per_aircraft_cap:
            return False, f"aircraft_cap_{icao_b}"

        # Check bearing diversity (not a near-duplicate)
        midpoint_bearing = float(raw[4]) if len(raw) > 4 else 0.0
        for sel_bearing in selected_bearings:
            sep = abs((midpoint_bearing - sel_bearing + 180.0) % 360.0 - 180.0)
            if sep < duplicate_bearing_tol:
                return False, "duplicate_bearing"

        # Check eigenvalue improvement
        # Tentatively add and check gain
        dx = candidate_x_km - cx_km
        dy = candidate_y_km - cy_km
        norm = math.hypot(dx, dy)
        if norm < 1e-9:
            return False, "degenerate_geometry"

        nx, ny = dx / norm, dy / norm
        residual_weight = math.exp(-0.5 * min(residual, 3.0) ** 2)
        w = float(sc.circle_score) * residual_weight

        J_test = J.copy()
        J_test[0, 0] += w * nx * nx
        J_test[0, 1] += w * nx * ny
        J_test[1, 0] += w * ny * nx
        J_test[1, 1] += w * ny * ny

        eigvals = np.linalg.eigvalsh(J_test)
        lambda_min_test = float(eigvals[0])
        eigen_gain = lambda_min_test - lambda_min_current

        if eigen_gain < min_eigen_gain and len(selected_indices) >= 2:
            return False, "marginal_eigenvalue_gain"

        return True, ""

    def _commit_add(idx: int) -> None:
        sc = admitted_by_arr_index[idx]
        raw = selected_rows[idx]
        aircraft_counts[sc.icao_a] = aircraft_counts.get(sc.icao_a, 0) + 1
        aircraft_counts[sc.icao_b] = aircraft_counts.get(sc.icao_b, 0) + 1
        midpoint_bearing = float(raw[4]) if len(raw) > 4 else 0.0
        selected_bearings.append(midpoint_bearing)
        selected_indices.append(idx)
        _add_to_matrix(idx)

    # Seed: start from the strongest inlier
    if scored_inliers:
        seed_idx = scored_inliers[0]
        _commit_add(seed_idx)

    # Greedy iteration
    for candidate_idx in scored_inliers[1:]:
        if len(selected_indices) >= max_subset_size:
            # Remaining become excluded
            remaining = [ci for ci in scored_inliers[scored_inliers.index(candidate_idx):] if ci not in {s for s in selected_indices}]
            for ri in remaining:
                excluded_indices.append(ri)
                exclusion_reasons[ri] = "max_subset_size"
            break

        if candidate_idx in {s for s in selected_indices}:
            continue

        ok, reason = _try_add(candidate_idx)
        if ok:
            _commit_add(candidate_idx)
        else:
            excluded_indices.append(candidate_idx)
            exclusion_reasons[candidate_idx] = reason

    eigvals = np.linalg.eigvalsh(J)

    return {
        "selected_indices": selected_indices,
        "excluded_indices": excluded_indices,
        "exclusion_reasons": exclusion_reasons,
        "J_final": J,
        "lambda_min_final": float(eigvals[0]),
        "lambda_max_final": float(eigvals[1]),
    }


def _score_cluster_quality(
    candidate_x_km: float,
    candidate_y_km: float,
    inlier_indices: list[int],
    normalized_residuals: list[float],
    admitted_by_arr_index: list,
    selected_rows: list[tuple],
    plausible_candidates: list[_IntersectionCandidate],
    cluster: _IntersectionCluster,
) -> dict:
    """Compute a combined cluster-quality score combining support, compactness,
    conditioning, and diversity/independence.

    Returns a dict with:
        quality_score: combined quality score (higher is better)
        support_score: circle support strength component
        compactness_km: weighted RMS distance to cluster centre
        conditioning_min_eigenvalue: lambda_min of information matrix
        conditioning_max_eigenvalue: lambda_max
        condition_number: lambda_max / lambda_min (lower is better)
        inverse_condition_number: lambda_min / lambda_max (higher is better)
        diversity_score: circular spread and independence measure
        effective_aircraft_count: distinct aircraft in refined subset
        effective_frame_count: distinct frames in refined subset
        raw_inlier_count: number of raw inlier circles
        refined_subset_count: number of refined subset circles
        refined_subset: dict from _select_refined_cluster_subset
    """
    # A. Support: aggregate circle scores of inliers
    support_total = 0.0
    for idx in inlier_indices:
        sc = admitted_by_arr_index[idx]
        residual = normalized_residuals[idx] if idx < len(normalized_residuals) else 3.0
        support = math.exp(-0.5 * min(residual, 3.0) ** 2)
        support_total += float(sc.circle_score) * support

    support_score = support_total

    # B. Compactness: weighted RMS of intersection candidates to cluster centre
    inlier_set = set(inlier_indices)
    inlier_candidates = [
        c for c in plausible_candidates
        if c.arc_i in inlier_set and c.arc_j in inlier_set
        and math.hypot(c.x_km - cluster.mean_x_km, c.y_km - cluster.mean_y_km) <= _INTERSECTION_CLUSTER_RADIUS_KM
    ]

    compactness_km = cluster.rms_km
    if inlier_candidates:
        total_w = sum(max(c.weight, 1e-9) for c in inlier_candidates)
        weighted_sq = sum(
            c.weight * ((c.x_km - cluster.mean_x_km) ** 2 + (c.y_km - cluster.mean_y_km) ** 2)
            for c in inlier_candidates
        )
        compactness_km = math.sqrt(weighted_sq / total_w) if total_w > 0 else cluster.rms_km

    # Compactness penalty: exp(-compactness / scale)^exponent
    compactness_penalty = 1.0 - math.exp(
        -(compactness_km / _QUALITY_COMPACTNESS_SCALE_KM) ** _QUALITY_COMPACTNESS_EXPONENT
    )

    # D. Diversity / independence: build refined subset first (needed for conditioning too)
    refined = _select_refined_cluster_subset(
        candidate_x_km, candidate_y_km,
        inlier_indices, admitted_by_arr_index, selected_rows, normalized_residuals,
    )

    # C. Conditioning: from information matrix of the REFINED subset normals,
    # not all raw inliers.  The spec requires the quality score to be based
    # primarily on the refined supporting subset.
    refined_indices = refined["selected_indices"]
    if refined_indices:
        _J_mat, lambda_min, lambda_max, _weights = _cluster_local_information_matrix(
            candidate_x_km, candidate_y_km,
            refined_indices, admitted_by_arr_index, selected_rows, normalized_residuals,
        )
    else:
        # Fallback: no refined subset → use all inliers (degenerate case)
        _J_mat, lambda_min, lambda_max, _weights = _cluster_local_information_matrix(
            candidate_x_km, candidate_y_km,
            inlier_indices, admitted_by_arr_index, selected_rows, normalized_residuals,
        )

    condition_number = lambda_max / lambda_min if lambda_min > _QUALITY_CONDITIONING_FLOOR else float("inf")
    inverse_condition_number = lambda_min / lambda_max if lambda_max > 0 else 0.0

    # Conditioning bonus: strong in both axes
    conditioning_score = math.sqrt(max(0.0, lambda_min)) * (1.0 - 1.0 / (1.0 + inverse_condition_number))
    conditioning_bonus = _QUALITY_CONDITIONING_SCALE * conditioning_score

    # Diversity metrics from the refined subset
    selected_idx_set = set(refined["selected_indices"])
    distinct_aircraft: set[str] = set()
    distinct_frames: set[int] = set()
    bearings: list[float] = []

    for idx in refined["selected_indices"]:
        sc = admitted_by_arr_index[idx]
        distinct_aircraft.add(sc.icao_a)
        distinct_aircraft.add(sc.icao_b)
        distinct_frames.add(int(sc.frame_index))
        raw = selected_rows[idx]
        if len(raw) > 4:
            bearings.append(float(raw[4]))

    effective_aircraft = len(distinct_aircraft)
    effective_frames = len(distinct_frames)

    # Diversity score: circular spread of bearings, normalized
    bearing_spread = _circular_spread_deg(bearings) if bearings else 0.0
    bearing_diversity = bearing_spread / 360.0

    # Aircraft diversity bonus: more distinct aircraft = better
    aircraft_diversity = min(1.0, effective_aircraft / 10.0)

    diversity_score = 0.5 * bearing_diversity + 0.5 * aircraft_diversity
    diversity_bonus = _QUALITY_DIVERSITY_SCALE * diversity_score

    # E. Redundancy penalty: if refined subset is much smaller than raw inliers,
    # it means many inliers were redundant
    raw_inlier_count = len(inlier_indices)
    refined_count = len(refined["selected_indices"])
    redundancy_ratio = refined_count / raw_inlier_count if raw_inlier_count > 0 else 1.0

    # Combined quality score
    support_term = _QUALITY_SUPPORT_SCALE * (1.0 - math.exp(-support_score / 3.0))
    compactness_term = 1.0 - compactness_penalty
    quality_score = (
        support_term
        + compactness_term
        + conditioning_bonus
        + diversity_bonus
    )

    # Normalize to [0, ~4] range (each component contributes ~1 max)
    return {
        "quality_score": quality_score,
        "support_score": support_score,
        "compactness_km": compactness_km,
        "compactness_penalty": compactness_penalty,
        "conditioning_min_eigenvalue": lambda_min,
        "conditioning_max_eigenvalue": lambda_max,
        "condition_number": condition_number,
        "inverse_condition_number": inverse_condition_number,
        "conditioning_bonus": conditioning_bonus,
        "diversity_score": diversity_score,
        "bearing_spread_deg": bearing_spread,
        "effective_aircraft_count": effective_aircraft,
        "effective_frame_count": effective_frames,
        "raw_inlier_count": raw_inlier_count,
        "refined_subset_count": refined_count,
        "refined_subset": refined,
        "redundancy_ratio": redundancy_ratio,
        "support_term": support_term,
        "compactness_term": compactness_term,
        "diversity_bonus": diversity_bonus,
    }


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


# ── Phase-difference scoring (legacy: used by reference aircraft selection only)
#     The main circle scoring pipeline has been replaced by circle_scorer.py.
#     These functions remain for _select_intersection_observations_with_diagnostics
#     which is used by _score_frame_reference_candidates().
# ─────────────────────────────────────────────────────────────────────────────

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
                n_replies=getattr(obs, "n_replies", 1),
                position_age_seconds=getattr(obs, "position_age_seconds", 0.0),
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


def _score_candidate_position_pairwise(
    r_lat: float,
    r_lon: float,
    scored_circles: list,
    raw_by_index: dict[int, dict],
    period_s: float,
    direction: int,
) -> _PairwiseResidualFit:
    """Score a candidate radar position against unordered aircraft-pair phases."""
    residuals: list[float] = []
    weights: list[float] = []
    weighted_sum = 0.0
    weighted_sq = 0.0

    for sc in scored_circles:
        raw = raw_by_index.get(int(sc.circle_index))
        if raw is None:
            continue

        weight = float(getattr(sc, "intrinsic_weight", 0.0))
        if weight <= 0.0:
            continue

        bearing_a = _bearing_deg(r_lat, r_lon, raw["lat_a"], raw["lon_a"])
        bearing_b = _bearing_deg(r_lat, r_lon, raw["lat_b"], raw["lon_b"])
        predicted_phase_deg = (bearing_b - bearing_a) % 360.0
        dt_s = (raw["arrival_us_b"] - raw["arrival_us_a"]) / 1_000_000.0
        observed_phase_deg = ((dt_s / period_s) * 360.0 * direction) % 360.0
        residual = (observed_phase_deg - predicted_phase_deg + 540.0) % 360.0 - 180.0

        residuals.append(residual)
        weights.append(weight)
        weighted_sum += residual * weight
        weighted_sq += residual * residual * weight

    if not residuals:
        return _PairwiseResidualFit(
            score=float("inf"),
            mean_residual_deg=0.0,
            residual_sigma_deg=float("inf"),
            weighted_rms_deg=float("inf"),
            n_residuals=0,
            residuals_deg=[],
        )

    total_weight = sum(weights)
    if total_weight <= 0.0:
        return _PairwiseResidualFit(
            score=float("inf"),
            mean_residual_deg=0.0,
            residual_sigma_deg=float("inf"),
            weighted_rms_deg=float("inf"),
            n_residuals=0,
            residuals_deg=residuals,
        )

    return _PairwiseResidualFit(
        score=weighted_sq,
        mean_residual_deg=weighted_sum / total_weight,
        residual_sigma_deg=math.sqrt(sum(r * r for r in residuals) / len(residuals)),
        weighted_rms_deg=math.sqrt(weighted_sq / total_weight),
        n_residuals=len(residuals),
        residuals_deg=residuals,
    )


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
    best_rank: Optional[tuple] = None
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
            rank = (
                res.get("best_cluster_quality_score", 0.0),
                res.get("n_inlier_pair_circles", res.get("n_contributing_arcs", 0)),
                -res.get("pairwise_weighted_rms_deg", float("inf")),
                -res.get("rms_km", float("inf")),
            )
            if best_rank is None or rank > best_rank:
                best_rank = rank
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
                n_replies=getattr(obs, "n_replies", 1),
                position_age_seconds=getattr(obs, "position_age_seconds", 0.0),
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
) -> tuple[list[_IntersectionCluster], list[dict]]:
    """Build distinct weighted neighborhoods from candidate intersections."""
    if not candidates:
        return [], []

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
            merged_cluster_indices=(),
        )

    neighborhoods = [nb for c in candidates if (nb := _build_neighborhood(c)) is not None]
    neighborhoods.sort(
        key=lambda cluster: (cluster.total_weight, cluster.member_count, -cluster.rms_km),
        reverse=True,
    )

    distinct: list[_IntersectionCluster] = []
    for idx, cluster in enumerate(neighborhoods):
        if any(
            math.hypot(
                cluster.mean_x_km - kept.mean_x_km,
                cluster.mean_y_km - kept.mean_y_km,
            ) <= max(1.0, 0.25 * cluster_radius_km)
            for kept in distinct
        ):
            continue
        # Compute contributing arcs from the final weighted cluster estimate.
        members = [
            c for c in candidates
            if math.hypot(c.x_km - cluster.mean_x_km, c.y_km - cluster.mean_y_km) <= cluster_radius_km
        ]
        contributing = frozenset(arc for c in members for arc in (c.arc_i, c.arc_j))
        distinct.append(
            dataclasses.replace(
                cluster,
                contributing_arc_indices=contributing,
                merged_cluster_indices=(idx,),
            )
        )

    merged, merge_events = _dedupe_near_duplicate_clusters(distinct, cluster_radius_km=cluster_radius_km)
    return merged, merge_events


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
        self._frame_pipeline_stats_lock = threading.Lock()
        self._frame_pipeline_stats: dict[int, dict] = defaultdict(self._new_frame_pipeline_stats)

    @staticmethod
    def _new_frame_pipeline_stats() -> dict:
        return {
            "frames_reaching_solver": 0,
            "solver_success": 0,
            "candidate_positions": 0,
            "solver_no_candidate": 0,
            "accumulation_rejected": 0,
            "accumulation_rejection_reasons": {},
            "accumulation_accepted": 0,
            "accumulation_acceptance_tiers": {},
            "accumulation_written": 0,
            "storage_errors": 0,
            "last_rejection_reason": None,
            "last_admission_tier": None,
        }

    def _record_frame_pipeline_event(
        self,
        iid: int,
        *,
        rejection_reason: Optional[str] = None,
        admission_tier: Optional[str] = None,
        storage_error: bool = False,
        **increments: int,
    ) -> None:
        with self._frame_pipeline_stats_lock:
            stats = self._frame_pipeline_stats[iid]
            for key, delta in increments.items():
                if delta:
                    stats[key] = int(stats.get(key, 0)) + int(delta)
            if rejection_reason is not None:
                reasons = stats.setdefault("accumulation_rejection_reasons", {})
                reasons[rejection_reason] = int(reasons.get(rejection_reason, 0)) + 1
                stats["last_rejection_reason"] = rejection_reason
            if admission_tier is not None:
                tiers = stats.setdefault("accumulation_acceptance_tiers", {})
                tiers[admission_tier] = int(tiers.get(admission_tier, 0)) + 1
                stats["last_admission_tier"] = admission_tier
            if storage_error:
                stats["storage_errors"] = int(stats.get("storage_errors", 0)) + 1

    def get_frame_pipeline_stats(self, iid: int) -> dict:
        with self._frame_pipeline_stats_lock:
            raw = self._frame_pipeline_stats.get(iid)
            if raw is None:
                raw = self._new_frame_pipeline_stats()
            stats = {
                "frames_reaching_solver": int(raw.get("frames_reaching_solver", 0)),
                "solver_success": int(raw.get("solver_success", 0)),
                "candidate_positions": int(raw.get("candidate_positions", 0)),
                "solver_no_candidate": int(raw.get("solver_no_candidate", 0)),
                "accumulation_rejected": int(raw.get("accumulation_rejected", 0)),
                "accumulation_rejection_reasons": dict(raw.get("accumulation_rejection_reasons", {})),
                "accumulation_accepted": int(raw.get("accumulation_accepted", 0)),
                "accumulation_acceptance_tiers": dict(raw.get("accumulation_acceptance_tiers", {})),
                "accumulation_written": int(raw.get("accumulation_written", 0)),
                "storage_errors": int(raw.get("storage_errors", 0)),
                "last_rejection_reason": raw.get("last_rejection_reason"),
                "last_admission_tier": raw.get("last_admission_tier"),
            }
        n_accumulated = len(self.get_frame_positions(iid))
        centroid = self.compute_weighted_centroid(iid) if n_accumulated > 0 else None
        stats["accumulated_frame_positions"] = n_accumulated
        stats["centroid_available"] = centroid is not None
        return stats

    @staticmethod
    def _classify_frame_for_accumulation(result: dict) -> tuple[Optional[str], str]:
        """Classify a per-frame solve result for accumulation.

        Three-tier classification:
          - ``accepted_high_confidence``: all quality gates pass
          - ``accepted_geometry_dominant``: pairwise RMS or support-dominance is weak
            but the best cluster is clearly dominant by member count and weight
          - ``rejected``: hard gates failed; do not accumulate

        Returns:
            (rejection_reason, tier) where rejection_reason is None when accepted.
        """
        cep_km = float(result.get("centroid_uncertainty_km", float("inf")))
        n_inliers = int(result.get("n_inlier_pair_circles", result.get("n_contributing_arcs", 0)))
        support_score = float(result.get("best_cluster_support_score", 0.0))
        support_dominance = float(result.get("support_dominance_ratio", result.get("dominance_ratio", 0.0)))
        pairwise_rms = float(result.get("pairwise_weighted_rms_deg", float("inf")))
        member_count = int(result.get("cluster_member_count", 0))
        second_member_count = int(result.get("second_cluster_member_count", 0))
        member_ratio = float(result.get("member_dominance_ratio", 0.0))
        weight_ratio = float(result.get("weight_dominance_ratio", 0.0))

        # Hard gates that cannot be bypassed by any tier
        if n_inliers < ACCUM_MIN_INLIER_PAIR_CIRCLES_RELAXED:
            return "too_few_inlier_pair_circles", "rejected"
        if not math.isfinite(cep_km) or cep_km >= ACCUM_MAX_FRAME_CEP_KM:
            return "excessive_frame_cep", "rejected"
        if support_score < ACCUM_MIN_SUPPORT_SCORE:
            return "poor_support_score", "rejected"

        # Tier 1: full quality gates pass — high confidence
        support_dom_ok = support_dominance >= ACCUM_MIN_SUPPORT_DOMINANCE_RATIO
        rms_ok = math.isfinite(pairwise_rms) and pairwise_rms <= ACCUM_MAX_PAIRWISE_WEIGHTED_RMS_DEG
        if n_inliers >= ACCUM_MIN_INLIER_PAIR_CIRCLES and support_dom_ok and rms_ok:
            return None, "accepted_high_confidence"
        if (
            n_inliers < ACCUM_MIN_INLIER_PAIR_CIRCLES
            and support_score >= ACCUM_RELAXED_MIN_SUPPORT_SCORE
            and support_dom_ok
            and math.isfinite(pairwise_rms)
            and pairwise_rms <= ACCUM_RELAXED_MAX_PAIRWISE_WEIGHTED_RMS_DEG
        ):
            return None, "accepted_reduced_arc_high_quality"

        # Tier 2: geometry-dominant bypass — strong member/weight dominance can override
        # noisy pairwise RMS and borderline support-dominance.
        no_second = second_member_count == 0
        member_dom_ok = member_count >= ACCUM_GEOM_DOMINANT_MIN_MEMBER_COUNT and (
            no_second or member_ratio >= ACCUM_GEOM_DOMINANT_MIN_MEMBER_RATIO
        )
        weight_dom_ok = no_second or not math.isfinite(weight_ratio) or weight_ratio >= ACCUM_GEOM_DOMINANT_MIN_WEIGHT_RATIO
        if member_dom_ok and weight_dom_ok:
            return None, "accepted_geometry_dominant"

        # Rejected: report the primary failing gate
        if not support_dom_ok:
            return "poor_support_dominance", "rejected"
        return "poor_pairwise_residual_rms", "rejected"

    @staticmethod
    def _accumulation_rejection_reason(result: dict) -> Optional[str]:
        """Return why a per-frame solve should not enter long-term accumulation.

        Backward-compatible wrapper around ``_classify_frame_for_accumulation``.
        Returns None when accepted (any tier), or a reason string when rejected.
        """
        rejection, _tier = ForwardModel._classify_frame_for_accumulation(result)
        return rejection

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
        """Called for each new completed sweep frame. Solve and accumulate.

        The per-frame estimate is always returned for diagnostics, but the
        stricter accumulation gates are enforced before writing to the
        long-term buffer so that weak frame solves do not dilute the centroid.
        """
        if receiver_lat is None or receiver_lon is None:
            return
        self._record_frame_pipeline_event(iid, frames_reaching_solver=1)

        est, solve_result = self.solve_single_frame_with_result(
            iid, frame, period_s, receiver_lat, receiver_lon,
        )
        if est is None:
            self._record_frame_pipeline_event(iid, solver_no_candidate=1)
            if solve_result is not None:
                n_arcs = solve_result.get("result", {}).get("n_contributing_arcs", 0)
                cep_km = solve_result.get("result", {}).get("centroid_uncertainty_km", float("inf"))
                self._record_frame_pipeline_event(iid, solver_success=1)
                log.info(
                    "ForwardModel: IID %d — frame %d solve succeeded but est is None "
                    "(n_contributing_arcs=%d < %d, cep_km=%.1f >= %.1f)",
                    iid,
                    getattr(frame, "frame_index", "?"),
                    n_arcs,
                    _PER_FRAME_MIN_CONTRIBUTING_ARCS,
                    cep_km,
                    _PER_FRAME_MAX_CEP_KM,
                )
            return
        self._record_frame_pipeline_event(iid, solver_success=1, candidate_positions=1)

        # Stricter accumulation admission gate — prevents weak frame solves
        # from entering the long-term buffer while still returning the estimate
        # for per-frame diagnostics and UI inspection.
        # solve_result is the full outer response dict; extract the inner "result"
        # sub-dict which carries the quality metrics used for gating.
        if solve_result is not None:
            inner = solve_result.get("result", {})
            rejection, tier = self._classify_frame_for_accumulation(inner)
            if rejection is not None:
                self._record_frame_pipeline_event(
                    iid,
                    accumulation_rejected=1,
                    rejection_reason=rejection,
                )
                n_inliers = int(inner.get("n_inlier_pair_circles", inner.get("n_contributing_arcs", 0)))
                cep_km = float(inner.get("centroid_uncertainty_km", float("inf")))
                support = float(inner.get("best_cluster_support_score", 0.0))
                support_dom = float(inner.get("support_dominance_ratio", inner.get("dominance_ratio", 0.0)))
                member_dom = float(inner.get("member_dominance_ratio", 0.0))
                rms = float(inner.get("pairwise_weighted_rms_deg", float("inf")))
                log.info(
                    "ForwardModel: IID %d — frame %d solved but excluded from accumulation: %s "
                    "(cep=%.1f km, inliers=%d, support=%.2f, support_dom=%.2f, "
                    "member_dom=%.2f, rms=%.1f°)",
                    iid, est.frame_index, rejection, cep_km, n_inliers, support,
                    support_dom, member_dom, rms,
                )
                return
            est.admission_tier = tier
            self._record_frame_pipeline_event(
                iid,
                accumulation_accepted=1,
                admission_tier=tier,
            )
            if tier == "accepted_geometry_dominant":
                est.weight *= ACCUM_GEOM_DOMINANT_WEIGHT_SCALE
                log.info(
                    "ForwardModel: IID %d — frame %d admitted as geometry_dominant "
                    "(weight scaled to %.3f, member_dom=%.2f, rms=%.1f°)",
                    iid, est.frame_index, est.weight,
                    float(inner.get("member_dominance_ratio", 0.0)),
                    float(inner.get("pairwise_weighted_rms_deg", float("inf"))),
                )

        self._add_frame_position(iid, est)

    def solve_single_frame_with_result(
        self,
        iid: int,
        frame,
        period_s: float,
        receiver_lat: float,
        receiver_lon: float,
    ) -> tuple[Optional["FramePositionEstimate"], Optional[dict]]:
        """Solve a single sweep frame and return both the estimate and the raw solve result.

        The raw result dict is needed for accumulation-admission gating via
        ``_accumulation_rejection_reason``.  The estimate alone is sufficient
        for per-frame diagnostics, but the raw result carries the full quality
        metadata (support scores, dominance ratios, pairwise RMS) that the
        stricter accumulation gates require.

        Returns:
            (estimate, raw_result) — either may be None independently.
        """
        if getattr(frame, "quality", "insufficient") == "insufficient":
            return None, None

        best: Optional[dict] = None
        best_result: Optional[dict] = None
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
            rank = (
                r.get("best_cluster_quality_score", 0.0),
                r.get("n_inlier_pair_circles", r.get("n_contributing_arcs", 0)),
                -r.get("pairwise_weighted_rms_deg", float("inf")),
                -r.get("rms_km", float("inf")),
            )
            best_rank = (
                best.get("best_cluster_quality_score", 0.0),
                best.get("n_inlier_pair_circles", best.get("n_contributing_arcs", 0)),
                -best.get("pairwise_weighted_rms_deg", float("inf")),
                -best.get("rms_km", float("inf")),
            ) if best is not None else None
            if best is None or best_rank is None or rank > best_rank:
                best = r
                best_result = result

        if best is None:
            return None, None

        n_arcs = best.get("n_contributing_arcs", 0)
        cep_km = best.get("centroid_uncertainty_km", float("inf"))
        if n_arcs < _PER_FRAME_MIN_CONTRIBUTING_ARCS or cep_km >= _PER_FRAME_MAX_CEP_KM:
            return None, best_result

        # Azimuth spread: bearings from the estimated position to each selected observation.
        azimuth_spread = best.get("azimuth_spread_deg", 0.0)
        weight = n_arcs / (cep_km + 0.5) ** 2
        est = FramePositionEstimate(
            frame_index=getattr(frame, "frame_index", 0),
            sweep_start_us=getattr(frame, "ref_arrival_us", 0.0),
            lat=best["lat"],
            lon=best["lon"],
            cep_km=cep_km,
            n_contributing_arcs=n_arcs,
            azimuth_spread_deg=azimuth_spread,
            weight=weight,
            cluster_dominance_ratio=best.get("support_dominance_ratio", best.get("dominance_ratio", 0.0)),
            interpolated_position_fraction=best.get("interpolated_fraction", 0.0),
            best_cluster_support_score=best.get("best_cluster_support_score", 0.0),
            support_dominance_ratio=best.get("support_dominance_ratio", 0.0),
            pairwise_weighted_rms_deg=best.get("pairwise_weighted_rms_deg", float("inf")),
            cluster_member_count=int(best.get("cluster_member_count", 0)),
            second_cluster_member_count=int(best.get("second_cluster_member_count", 0)),
            member_dominance_ratio=float(best.get("member_dominance_ratio", 0.0)),
            weight_dominance_ratio=float(best.get("weight_dominance_ratio", 0.0)),
        )
        return est, best_result

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
        est, _ = self.solve_single_frame_with_result(
            iid, frame, period_s, receiver_lat, receiver_lon,
        )
        return est

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
            n_accumulated = len(self._frame_positions[iid])
        self._record_frame_pipeline_event(iid, accumulation_written=1)
        log.debug(
            "ForwardModel: IID %d — accumulated frame %d (%.4f, %.4f) cep=%.1f km [%d total in buffer]",
            iid, estimate.frame_index, estimate.lat, estimate.lon, estimate.cep_km, n_accumulated,
        )
        try:
            from db import stats_db
            stats_db.insert_frame_position(iid, estimate)
        except Exception:
            self._record_frame_pipeline_event(iid, storage_error=True)
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
            "stage1_cluster_sigma_m": getattr(result, "stage1_cluster_sigma_m", None),
            "stage1_abs_distance_cap_m": getattr(result, "stage1_abs_distance_cap_m", None),
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
        p_bar_enu_m: Optional[tuple[float, float]] = None,
        manual_selected_circle_indices: Optional[set[int]] = None,
        manually_forced_preview: bool = False,
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

        valid_frames = [f for f in sweep_frames if f.quality in ("good", "marginal")]
        frames_to_use = valid_frames[-_OPTIM_MAX_FRAMES:]

        # Build all unordered pair-derived circles per frame. The scorer and
        # selector decide which pair constraints are strong enough to admit.
        raw_circles: list[dict] = []
        detail = {
            "n_frames_considered": len(frames_to_use),
            "n_valid_frames": len(valid_frames),
            "n_scored_frames": len(frames_to_use),
            "n_pairs": 0,
            "degenerate_baseline_pairs": 0,
            "endpoint_candidate_rejections": 0,
            "raw_candidate_count": 0,
            "plausible_candidate_count": 0,
            "receiver_distance_m": None,
            "selection_diagnostics": {},
            "high_quality_frames": 0,
            "total_selected_weight": 0.0,
            "azimuth_spread_deg": 0.0,
            "interpolated_fraction": 0.0,
        }

        for frame in frames_to_use:
            ref_n_replies = max(2, max((getattr(obs, "n_replies", 1) for obs in frame.observations), default=2))
            aircraft = [{
                "icao": getattr(frame, "ref_icao", ""),
                "lat": frame.ref_lat,
                "lon": frame.ref_lon,
                "arrival_us": frame.ref_arrival_us,
                "interpolated": bool(getattr(frame, "ref_interpolated", False)),
                "n_replies": int(getattr(frame, "ref_n_replies", ref_n_replies)),
                "position_age_seconds": float(getattr(frame, "ref_position_age_seconds", 0.0)),
            }]
            for obs in frame.observations:
                aircraft.append({
                    "icao": obs.icao,
                    "lat": obs.lat,
                    "lon": obs.lon,
                    "arrival_us": obs.arrival_us,
                    "interpolated": bool(getattr(obs, "interpolated", False)),
                    "n_replies": int(getattr(obs, "n_replies", 1)),
                    "position_age_seconds": float(getattr(obs, "position_age_seconds", 0.0)),
                })

            frame_index = int(getattr(frame, "frame_index", len(raw_circles)))
            for i in range(len(aircraft)):
                for j in range(i + 1, len(aircraft)):
                    a_rec = aircraft[i]
                    b_rec = aircraft[j]
                    ax, ay = to_xy(a_rec["lat"], a_rec["lon"])
                    bx, by = to_xy(b_rec["lat"], b_rec["lon"])

                    dt_s = (b_rec["arrival_us"] - a_rec["arrival_us"]) / 1_000_000.0
                    phase_deg = ((dt_s / period_s) * 360.0 * direction) % 360.0
                    delta_phi_deg = min(phase_deg, 360.0 - phase_deg)
                    delta_phi = math.radians(delta_phi_deg)
                    signed_phase_deg = phase_deg if phase_deg <= 180.0 else phase_deg - 360.0
                    signed_phi = math.radians(signed_phase_deg)

                    dx, dy = bx - ax, by - ay
                    d = math.hypot(dx, dy)
                    if d < 0.1:
                        detail["degenerate_baseline_pairs"] += 1
                        continue

                    sin_phi = math.sin(signed_phi)
                    R = d / (2.0 * abs(sin_phi)) if abs(sin_phi) > 1e-12 else float("inf")
                    px, py = -(dy / d), dx / d
                    cos_phi = math.cos(signed_phi)
                    h = -(d / 2.0) * (cos_phi / sin_phi) if abs(sin_phi) > 1e-12 else 0.0
                    cx = (ax + bx) / 2.0 + h * px
                    cy = (ay + by) / 2.0 + h * py

                    mid_x = (ax + bx) / 2.0
                    mid_y = (ay + by) / 2.0
                    midpoint_lat, midpoint_lon = from_xy(mid_x, mid_y)
                    rx_bearing = _bearing_deg(origin_lat, origin_lon, midpoint_lat, midpoint_lon)

                    circle_index = len(raw_circles)
                    raw_circles.append({
                        "circle_index": circle_index,
                        "center_enu_m": (cx * 1000.0, cy * 1000.0),
                        "radius_m": R * 1000.0,
                        "delta_phi": delta_phi,
                        "icao_a": a_rec["icao"],
                        "icao_b": b_rec["icao"],
                        "lat_a": a_rec["lat"],
                        "lon_a": a_rec["lon"],
                        "lat_b": b_rec["lat"],
                        "lon_b": b_rec["lon"],
                        "arrival_us_a": a_rec["arrival_us"],
                        "arrival_us_b": b_rec["arrival_us"],
                        "pair_baseline_m": d * 1000.0,
                        "delta_phi_deg": delta_phi_deg,
                        "interpolated_a": a_rec["interpolated"],
                        "interpolated_b": b_rec["interpolated"],
                        "n_replies_a": a_rec["n_replies"],
                        "n_replies_b": b_rec["n_replies"],
                        "position_age_a_seconds": a_rec["position_age_seconds"],
                        "position_age_b_seconds": b_rec["position_age_seconds"],
                        "frame_index": frame_index,
                        "cx_km": cx,
                        "cy_km": cy,
                        "R_km": R,
                        "rx_bearing": rx_bearing,
                        "endpoints_xy_km": ((ax, ay), (bx, by)),
                    })
                    detail["n_pairs"] += 1
                    detail["raw_candidate_count"] += 1

        # Score all circles using the new scoring pipeline
        from .circle_scorer import compute_circle_scores, select_circles
        omega = 2.0 * math.pi / period_s  # radar rotation rate in rad/s
        scored = compute_circle_scores(raw_circles, p_bar_enu_m, omega)
        selected = select_circles(scored)
        detail["initial_admitted_pair_count"] = len(selected)
        if manual_selected_circle_indices is not None:
            requested = {int(idx) for idx in manual_selected_circle_indices}
            selected = [sc for sc in selected if int(sc.circle_index) in requested]
        raw_by_index = {int(c["circle_index"]): c for c in raw_circles}
        gate_counts: dict[str, int] = {}
        for sc in scored:
            if sc.exclusion_reason:
                gate_counts[sc.exclusion_reason] = gate_counts.get(sc.exclusion_reason, 0) + 1

        frame_ref_by_index = {
            int(getattr(frame, "frame_index", -1)): getattr(frame, "ref_icao", "")
            for frame in frames_to_use
        }

        def contains_reference(sc) -> bool:
            frame_ref = frame_ref_by_index.get(int(sc.frame_index), "")
            return bool(frame_ref and (sc.icao_a == frame_ref or sc.icao_b == frame_ref))

        # Populate selection diagnostics
        detail["selection_diagnostics"] = {
            "total_raw_pair_circles": len(raw_circles),
            "total_scored": len(scored),
            "total_admitted": len(selected),
            "total_selected": len(selected),
            "total_raw_pairs_generated": len(raw_circles),
            "total_scored_pair_circles": len(scored),
            "total_admitted_pair_circles": len(selected),
            "admitted_pairs_containing_reference": sum(1 for sc in selected if contains_reference(sc)),
            "admitted_pairs_not_containing_reference": sum(1 for sc in selected if not contains_reference(sc)),
            "rejected_by_gate": gate_counts,
            "per_aircraft_cap_rejections": gate_counts.get("aircraft_cap", 0),
            "per_frame_cap_rejections": gate_counts.get("frame_cap", 0),
            "scores": [s.circle_score for s in selected],
            "manual_selected_circle_indices": sorted(manual_selected_circle_indices) if manual_selected_circle_indices is not None else None,
            "manually_forced_preview": bool(manually_forced_preview),
        }

        def pairwise_residual_for_raw(raw: dict, lat: float, lon: float) -> float:
            bearing_a = _bearing_deg(lat, lon, raw["lat_a"], raw["lon_a"])
            bearing_b = _bearing_deg(lat, lon, raw["lat_b"], raw["lon_b"])
            predicted_phase_deg = (bearing_b - bearing_a) % 360.0
            dt_s = (raw["arrival_us_b"] - raw["arrival_us_a"]) / 1_000_000.0
            observed_phase_deg = ((dt_s / period_s) * 360.0 * direction) % 360.0
            return (observed_phase_deg - predicted_phase_deg + 540.0) % 360.0 - 180.0

        def serialize_pair_circle(
            sc,
            *,
            admitted_idx: Optional[int] = None,
            inlier: bool = False,
            normalized_residual: Optional[float] = None,
            chosen_lat: Optional[float] = None,
            chosen_lon: Optional[float] = None,
        ) -> dict:
            raw = raw_by_index.get(int(sc.circle_index), {})
            pair_residual_deg = (
                pairwise_residual_for_raw(raw, chosen_lat, chosen_lon)
                if raw and chosen_lat is not None and chosen_lon is not None
                else None
            )
            return {
                "circle_index": int(sc.circle_index),
                "admitted_index": admitted_idx,
                "frame_index": int(sc.frame_index),
                "icao_a": sc.icao_a,
                "icao_b": sc.icao_b,
                "circle_score": round(float(sc.circle_score), 6),
                "intrinsic_weight": round(float(sc.intrinsic_weight), 6),
                "prior_weight": round(float(sc.prior_weight), 6),
                "phi_weight": round(float(sc.phi_weight), 6),
                "uncertainty_weight": round(float(sc.uncertainty_weight), 6),
                "reply_weight": round(float(sc.reply_weight), 6),
                "age_weight": round(float(sc.age_weight), 6),
                "sigma_band_metres": round(float(sc.sigma_band_metres), 3),
                "pair_baseline_m": round(float(sc.pair_baseline_m), 3),
                "delta_phi_deg": round(math.degrees(float(sc.delta_phi)), 3),
                "interpolated_a": bool(sc.interpolated_a),
                "interpolated_b": bool(sc.interpolated_b),
                "n_replies_a": int(sc.n_replies_a),
                "n_replies_b": int(sc.n_replies_b),
                "position_age_a_seconds": round(float(sc.position_age_a_seconds), 3),
                "position_age_b_seconds": round(float(sc.position_age_b_seconds), 3),
                "pair_midpoint_bearing_deg": round(float(raw.get("rx_bearing", 0.0)), 3) if raw else None,
                "selected": bool(sc.selected and (manual_selected_circle_indices is None or int(sc.circle_index) in manual_selected_circle_indices)),
                "inlier": bool(inlier),
                "normalized_residual": round(float(normalized_residual), 6) if normalized_residual is not None else None,
                "pair_residual_deg": round(float(pair_residual_deg), 3) if pair_residual_deg is not None else None,
                "exclusion_reason": sc.exclusion_reason,
                "cx_km": round(float(raw["cx_km"]), 6) if raw else None,
                "cy_km": round(float(raw["cy_km"]), 6) if raw else None,
                "R_km": round(float(raw["R_km"]), 6) if raw else None,
            }

        def pair_circle_summary(inlier_count: int = 0) -> dict:
            return {
                "total_raw_pair_circles": len(raw_circles),
                "total_scored_pair_circles": len(scored),
                "total_admitted_pair_circles": len(selected),
                "total_inlier_pair_circles": inlier_count,
                "admitted_pairs_containing_reference": detail["selection_diagnostics"]["admitted_pairs_containing_reference"],
                "admitted_pairs_not_containing_reference": detail["selection_diagnostics"]["admitted_pairs_not_containing_reference"],
            }

        def rejected_payload(status: str, reason: str, extra_detail: Optional[dict] = None) -> dict:
            merged_detail = {**detail, **(extra_detail or {})}
            return {
                "success": False,
                "available": True,
                "solve_status": status,
                "solve_reason": reason,
                "reason": reason,
                "admitted_pair_circles": [
                    serialize_pair_circle(sc, admitted_idx=idx)
                    for idx, sc in enumerate(selected)
                ],
                "scored_pair_circles": [
                    serialize_pair_circle(sc)
                    for sc in scored
                ],
                "candidate_intersections": merged_detail.get("candidate_intersections", []),
                "candidate_clusters": merged_detail.get("candidate_clusters", []),
                "pair_circle_summary": pair_circle_summary(),
                "selection_diagnostics": detail["selection_diagnostics"],
                "detail": merged_detail,
            }

        # Now that scoring and helpers are ready, check for early-rejection cases.
        if len(selected) < 2:
            return rejected_payload(
                "rejected_too_few_admitted",
                "too few circles after scoring and selection",
            )

        # Build intersection input from selected circles
        selected_rows = []
        admitted_by_arr_index = []
        admitted_endpoints_by_arr_index = []
        for sc in selected:
            raw = raw_by_index.get(sc.circle_index)
            if raw is None:
                continue
            selected_rows.append((
                raw["cx_km"],
                raw["cy_km"],
                raw["R_km"],
                sc.circle_score,
                raw["rx_bearing"],
                sc.sigma_band_metres / 1000.0,
                sc.circle_index,
            ))
            admitted_by_arr_index.append(sc)
            admitted_endpoints_by_arr_index.append(raw["endpoints_xy_km"])
        arr = np.array(
            selected_rows,
            dtype=np.float64,
        )  # (n, 7): cx, cy, R, w, pair_midpoint_bearing_deg, sigma_km, circle_index
        if len(arr) < 2:
            return rejected_payload(
                "rejected_too_few_admitted",
                "too few circles after scoring and selection",
            )
        n_c = len(arr)
        cx_a = arr[:, 0]; cy_a = arr[:, 1]; R_a = arr[:, 2]; w_a = arr[:, 3]; brx_a = arr[:, 4]
        ii, jj = np.triu_indices(n_c, k=1)

        ddx = cx_a[jj] - cx_a[ii]
        ddy = cy_a[jj] - cy_a[ii]
        dist = np.hypot(ddx, ddy)
        R1 = R_a[ii]; R2 = R_a[jj]
        valid = (dist >= 1e-6) & (dist <= R1 + R2 + 1e-6) & (dist >= np.abs(R1 - R2) - 1e-6)

        valid_ii = ii[valid]; valid_jj = jj[valid]
        ddx = ddx[valid]; ddy = ddy[valid]; dist = dist[valid]
        R1 = R1[valid]; R2 = R2[valid]

        # Pair weight = w_i x w_j x sin(Δaz): down-weights intersections whose
        # pair midpoint bearings are similar and therefore geometrically shallow.
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
            return rejected_payload("rejected_no_intersections", "no circle intersections")

        max_km = 700 * _NM_TO_M / 1000.0
        plausible = []
        endpoint_rejections = 0
        for candidate in candidates:
            if math.hypot(candidate.x_km, candidate.y_km) > max_km:
                continue
            relevant_endpoints = (
                admitted_endpoints_by_arr_index[candidate.arc_i]
                + admitted_endpoints_by_arr_index[candidate.arc_j]
            )
            if any(
                math.hypot(candidate.x_km - ax_km, candidate.y_km - ay_km) <= _ENDPOINT_INTERSECTION_REJECT_KM
                for ax_km, ay_km in relevant_endpoints
            ):
                endpoint_rejections += 1
                continue
            plausible.append(candidate)
        detail["endpoint_candidate_rejections"] = endpoint_rejections
        detail["plausible_candidate_count"] = len(plausible)
        detail["candidate_intersections"] = [
            {
                "x_km": round(candidate.x_km, 6),
                "y_km": round(candidate.y_km, 6),
                "weight": round(candidate.weight, 6),
                "circle_index_a": int(selected_rows[candidate.arc_i][6]),
                "circle_index_b": int(selected_rows[candidate.arc_j][6]),
            }
            for candidate in plausible[:250]
        ]
        if not plausible:
            return rejected_payload(
                "rejected_all_candidates_implausible",
                "all intersection candidates were implausibly distant",
            )

        raw_clusters, cluster_merge_events = _build_intersection_clusters(
            plausible,
            cluster_radius_km=cluster_radius_km,
        )
        clusters = raw_clusters
        detail["cluster_count_pre_merge"] = len(raw_clusters) + len(cluster_merge_events)
        detail["cluster_count_post_merge"] = len(clusters)
        detail["cluster_count"] = len(clusters)
        detail["cluster_merge_events"] = cluster_merge_events[:20]
        if not clusters:
            return rejected_payload("rejected_ambiguous", "candidate cloud was ambiguous")

        def circle_support_at(x_km: float, y_km: float) -> tuple[float, list[float]]:
            support_total = 0.0
            residuals: list[float] = []
            for sc, raw in zip(admitted_by_arr_index, selected_rows):
                cx_km, cy_km, R_km, _w, _brg, sigma_km, _idx = raw
                sigma_km = max(float(sigma_km), 1e-6)
                normalized = abs(math.hypot(x_km - cx_km, y_km - cy_km) - R_km) / sigma_km
                support = math.exp(-0.5 * min(normalized, 3.0) ** 2)
                support_total += sc.circle_score * support
                residuals.append(normalized)
            return support_total, residuals

        top_clusters = clusters[: min(8, len(clusters))]

        # ── New cluster-quality scoring ─────────────────────────────────
        # Rank clusters by the combined quality score (support + compactness
        # + conditioning + diversity) rather than raw support alone.
        quality_ranked: list[tuple[float, float, _PairwiseResidualFit, _IntersectionCluster, dict]] = []

        for cluster in top_clusters:
            candidate_x = cluster.mean_x_km
            candidate_y = cluster.mean_y_km

            # 1. Raw inlier circles for this cluster
            inlier_idx, inlier_residuals = _cluster_inlier_circles(
                cluster, admitted_by_arr_index, selected_rows, cluster_radius_km,
            )

            if not inlier_idx:
                continue

            # 2. Support score (existing metric, kept as a component)
            candidate_support, _ = circle_support_at(candidate_x, candidate_y)

            # 3. Pairwise residual fit
            cand_lat, cand_lon = from_xy(candidate_x, candidate_y)
            pairwise_fit = _score_candidate_position_pairwise(
                cand_lat, cand_lon, selected, raw_by_index, period_s, direction,
            )

            # 4. Combined quality score
            quality = _score_cluster_quality(
                candidate_x, candidate_y,
                inlier_idx, inlier_residuals,
                admitted_by_arr_index, selected_rows,
                plausible, cluster,
            )

            quality_ranked.append((
                quality["quality_score"],
                pairwise_fit.weighted_rms_deg,
                pairwise_fit,
                cluster,
                quality,
            ))

        if not quality_ranked:
            return rejected_payload("rejected_no_viable_clusters", "no clusters passed quality gates")

        # Sort by member count (descending) first — dominant cluster identification is
        # member-first.  Quality score, compactness, and conditioning are secondary
        # tiebreakers.  Pairwise RMS is NOT used for cluster selection; it is a
        # confidence/refinement metric applied after the dominant cluster is chosen.
        def _cluster_sort_key(item: tuple) -> tuple:
            _q_score, _pairwise_rms, _pairwise_fit, cluster, quality = item
            compactness = quality.get("compactness_km", float("inf"))
            cond = quality.get("condition_number", float("inf"))
            cond_sort = cond if math.isfinite(cond) else 1e9
            return (
                -cluster.member_count,   # primary: most intersection members first
                -cluster.total_weight,   # secondary: highest aggregate weight first
                cluster.rms_km,          # tertiary: raw cluster spread (tighter = more genuine)
                compactness,             # quaternary: refined inlier compactness
                cond_sort,               # quinary: best-conditioned first
                -_q_score,               # senary: quality-score tiebreaker
            )
        quality_ranked.sort(key=_cluster_sort_key)

        # ── Cluster serialization with full diagnostics ─────────────────
        def serialize_cluster_v2(
            rank: int,
            item: tuple[float, float, _PairwiseResidualFit, _IntersectionCluster, dict],
        ) -> dict:
            quality_score, _pairwise_rms, pairwise_fit, cluster, quality = item
            # Use rank+1 for dominance comparisons (compare against the NEXT cluster, not self)
            next_rank = rank + 1
            next_quality = quality_ranked[next_rank][0] if next_rank < len(quality_ranked) else 0.0
            next_support = quality_ranked[next_rank][4]["support_score"] if next_rank < len(quality_ranked) else 0.0
            next_member_count = quality_ranked[next_rank][3].member_count if next_rank < len(quality_ranked) else 0

            contributing_admitted_indices = sorted(cluster.contributing_arc_indices)
            contributing_circle_indices = [
                int(selected_rows[idx][6])
                for idx in contributing_admitted_indices
                if 0 <= idx < len(selected_rows)
            ]
            cluster_lat, cluster_lon = from_xy(cluster.mean_x_km, cluster.mean_y_km)

            # Refined subset circle indices
            refined_indices = quality.get("refined_subset", {}).get("selected_indices", [])
            refined_circle_indices = [
                int(selected_rows[idx][6])
                for idx in refined_indices
                if 0 <= idx < len(selected_rows)
            ]

            # Excluded from refined subset with reasons
            excluded_indices = quality.get("refined_subset", {}).get("excluded_indices", [])
            exclusion_reasons = quality.get("refined_subset", {}).get("exclusion_reasons", {})
            excluded_circle_details = []
            for ex_idx in excluded_indices[:20]:  # cap for payload size
                reason = exclusion_reasons.get(ex_idx, "unknown")
                circle_idx = int(selected_rows[ex_idx][6]) if 0 <= ex_idx < len(selected_rows) else None
                excluded_circle_details.append({"circle_index": circle_idx, "reason": reason})

            return {
                "cluster_rank": rank + 1,
                "merged_cluster_indices": list(cluster.merged_cluster_indices),
                "lat": round(cluster_lat, 6),
                "lon": round(cluster_lon, 6),
                "cluster_quality_score": round(quality_score, 6),
                "cluster_support_score": round(quality["support_score"], 6),
                "cluster_compactness_km": round(quality["compactness_km"], 3),
                "cluster_conditioning_min_eigenvalue": round(quality["conditioning_min_eigenvalue"], 6),
                "cluster_conditioning_max_eigenvalue": round(quality["conditioning_max_eigenvalue"], 6),
                "cluster_condition_number": (
                    round(quality["condition_number"], 3)
                    if math.isfinite(quality["condition_number"]) else None
                ),
                "cluster_inverse_condition_number": round(quality["inverse_condition_number"], 6),
                "cluster_diversity_score": round(quality["diversity_score"], 6),
                "cluster_effective_aircraft_count": quality["effective_aircraft_count"],
                "cluster_effective_frame_count": quality["effective_frame_count"],
                "cluster_raw_inlier_count": quality["raw_inlier_count"],
                "cluster_refined_subset_count": quality["refined_subset_count"],
                "pairwise_fit_score": round(float(pairwise_fit.score), 6) if math.isfinite(pairwise_fit.score) else None,
                "pairwise_weighted_rms_deg": round(float(pairwise_fit.weighted_rms_deg), 3) if math.isfinite(pairwise_fit.weighted_rms_deg) else None,
                "quality_dominance_ratio": (
                    round(quality_score / next_quality, 3)
                    if next_quality > 0.0 else None
                ),
                "support_dominance_ratio": (
                    round(quality["support_score"] / next_support, 3)
                    if next_support > 0.0 else None
                ),
                "member_count": int(cluster.member_count),
                "next_cluster_member_count": int(next_member_count),
                "member_dominance_ratio": (
                    round(cluster.member_count / next_member_count, 3)
                    if next_member_count > 0 else None
                ),
                "cluster_total_weight": round(float(cluster.total_weight), 6),
                "rms_km": round(float(cluster.rms_km), 3),
                "contributing_pair_indices": contributing_circle_indices,
                "contributing_circle_indices": contributing_circle_indices,
                "refined_subset_circle_indices": refined_circle_indices,
                "excluded_from_refined_subset": excluded_circle_details,
                "inlier_pair_indices": [],
                "is_selected_best": rank == 0,
            }

        detail["candidate_clusters"] = [
            serialize_cluster_v2(rank, item)
            for rank, item in enumerate(quality_ranked[:8])
        ]

        # ── Best cluster selection ──────────────────────────────────────
        best_quality_score, _best_pairwise_rms_deg, best_pairwise_fit, best_cluster, best_quality = quality_ranked[0]

        if not math.isfinite(best_pairwise_fit.score):
            return rejected_payload("rejected_ambiguous", "best cluster has non-finite pairwise fit")

        # ── Ambiguity decision using quality score ──────────────────────
        second_quality_score = 0.0
        second_cluster: Optional[_IntersectionCluster] = None
        second_quality: Optional[dict] = None
        if len(quality_ranked) > 1:
            second_quality_score, _second_rms, _second_fit, second_cluster, second_quality = quality_ranked[1]
        ambiguity_eval = _evaluate_cluster_ambiguity(
            best_cluster=best_cluster,
            second_cluster=second_cluster,
            best_quality_score=best_quality_score,
            second_quality_score=second_quality_score,
            best_support_score=best_quality["support_score"],
            second_support_score=second_quality["support_score"] if second_quality else 0.0,
            raw_inlier_count=best_quality["raw_inlier_count"],
            cluster_radius_km=cluster_radius_km,
        )
        quality_ratio = ambiguity_eval["quality_ratio"]
        support_ratio = ambiguity_eval["support_ratio"]
        effective_threshold = ambiguity_eval["effective_threshold"]
        _is_geometry_dominant_cluster = ambiguity_eval["is_geometry_dominant_cluster"]
        _second_member_count_check = ambiguity_eval["second_member_count"]
        _member_ratio_for_dom = ambiguity_eval["member_ratio"]
        _weight_ratio_for_dom = ambiguity_eval["weight_ratio"]
        same_lobe_metrics = ambiguity_eval["same_lobe_metrics"]
        same_lobe_bypass = ambiguity_eval["same_lobe_bypass"]
        is_ambiguous = ambiguity_eval["is_ambiguous"]
        ambiguity_reason = ambiguity_eval["ambiguity_reason"]

        if is_ambiguous and not manually_forced_preview:
            return rejected_payload(
                "rejected_ambiguous",
                f"candidate cloud was ambiguous: {ambiguity_reason}",
                {
                    "quality_ratio": round(quality_ratio, 3),
                    "support_ratio": round(support_ratio, 3),
                    "best_quality_score": round(best_quality_score, 6),
                    "second_quality_score": round(second_quality_score, 6),
                    "cluster_count": len(clusters),
                    "cluster_count_pre_merge": detail.get("cluster_count_pre_merge"),
                    "cluster_count_post_merge": detail.get("cluster_count_post_merge"),
                    "cluster_merge_events": detail.get("cluster_merge_events", []),
                    "top_centroid_separation_km": (
                        round(float(same_lobe_metrics["centroid_separation_km"]), 3)
                        if same_lobe_metrics else None
                    ),
                    "top_arc_jaccard": (
                        round(float(same_lobe_metrics["arc_jaccard"]), 3)
                        if same_lobe_metrics else None
                    ),
                    "top_arc_overlap_min_ratio": (
                        round(float(same_lobe_metrics["arc_overlap_min_ratio"]), 3)
                        if same_lobe_metrics else None
                    ),
                    "ambiguity_same_lobe_bypass": same_lobe_bypass,
                    "is_geometry_dominant_cluster": _is_geometry_dominant_cluster,
                    "best_cluster_member_count": best_cluster.member_count,
                    "second_cluster_member_count": _second_member_count_check,
                    "member_dominance_ratio": round(_member_ratio_for_dom, 3) if math.isfinite(_member_ratio_for_dom) else None,
                    "weight_dominance_ratio": round(_weight_ratio_for_dom, 3) if math.isfinite(_weight_ratio_for_dom) else None,
                },
            )

        lat, lon = from_xy(best_cluster.mean_x_km, best_cluster.mean_y_km)
        receiver_distance_m = _haversine_m(origin_lat, origin_lon, lat, lon)
        detail["receiver_distance_m"] = receiver_distance_m
        dominance_ratio = (
            best_quality_score / second_quality_score
            if second_quality_score > 0.0
            else float("inf")
        )
        _best_support_check, normalized_residuals = circle_support_at(best_cluster.mean_x_km, best_cluster.mean_y_km)
        inlier_indices_set = {
            idx for idx, residual in enumerate(normalized_residuals)
            if residual <= 2.5
        }
        inlier_circles = [admitted_by_arr_index[idx] for idx in sorted(inlier_indices_set)]
        inlier_candidates = [
            candidate
            for candidate in plausible
            if candidate.arc_i in inlier_indices_set and candidate.arc_j in inlier_indices_set
            and math.hypot(candidate.x_km - best_cluster.mean_x_km, candidate.y_km - best_cluster.mean_y_km) <= cluster_radius_km
        ]
        n_contributing_arcs = len(inlier_circles)
        if inlier_candidates:
            inlier_weight = sum(max(candidate.weight, 0.0) for candidate in inlier_candidates)
            if inlier_weight > 0:
                inlier_rms_km = math.sqrt(
                    sum(
                        candidate.weight * (
                            (candidate.x_km - best_cluster.mean_x_km) ** 2
                            + (candidate.y_km - best_cluster.mean_y_km) ** 2
                        )
                        for candidate in inlier_candidates
                    ) / inlier_weight
                )
            else:
                inlier_rms_km = best_cluster.rms_km
        else:
            inlier_rms_km = best_cluster.rms_km
        centroid_uncertainty_km = inlier_rms_km / math.sqrt(max(1, n_contributing_arcs))

        # Compute summary stats from inlier admitted circles.
        n_selected = len(selected)
        total_weight = sum(s.circle_score for s in selected)
        inlier_bearings = [float(selected_rows[idx][4]) for idx in inlier_indices_set]
        az_spread = _circular_spread_deg(inlier_bearings)
        interp_frac = (
            sum(1 for s in inlier_circles if s.interpolated_a or s.interpolated_b) / len(inlier_circles)
            if inlier_circles else 1.0
        )
        inlier_residual_summary = {
            "min": min((normalized_residuals[idx] for idx in inlier_indices_set), default=None),
            "mean": (
                sum(normalized_residuals[idx] for idx in inlier_indices_set) / len(inlier_indices_set)
                if inlier_indices_set else None
            ),
            "max": max((normalized_residuals[idx] for idx in inlier_indices_set), default=None),
        }
        inlier_circle_indices = [
            int(selected_rows[idx][6])
            for idx in sorted(inlier_indices_set)
            if 0 <= idx < len(selected_rows)
        ]
        for cluster_row in detail["candidate_clusters"]:
            cluster_row["inlier_pair_indices"] = [
                idx for idx in cluster_row["contributing_circle_indices"]
                if idx in inlier_circle_indices
            ]

        admitted_pair_circles = [
            serialize_pair_circle(
                sc,
                admitted_idx=idx,
                inlier=idx in inlier_indices_set,
                normalized_residual=normalized_residuals[idx] if idx < len(normalized_residuals) else None,
                chosen_lat=lat,
                chosen_lon=lon,
            )
            for idx, sc in enumerate(admitted_by_arr_index)
        ]
        inlier_pair_circles = [
            row for row in admitted_pair_circles if row["inlier"]
        ]

        # Refined subset details from quality scoring
        refined_subset = best_quality.get("refined_subset", {})
        refined_circle_indices = [
            int(selected_rows[idx][6])
            for idx in refined_subset.get("selected_indices", [])
            if 0 <= idx < len(selected_rows)
        ]

        # ── Post-cluster pruning diagnostics ─────────────────────────────────
        # Record which admitted circles are inconsistent with the dominant cluster.
        # This is SUBTRACTIVE: we identify outliers *after* cluster discovery, not
        # pre-emptively.  The inlier set is already computed (inlier_indices_set).
        post_cluster_pruned_pairs: list[dict] = []
        for _idx in range(len(admitted_by_arr_index)):
            if _idx not in inlier_indices_set:
                _sc = admitted_by_arr_index[_idx]
                _residual = normalized_residuals[_idx] if _idx < len(normalized_residuals) else float("inf")
                _raw_row = selected_rows[_idx]
                post_cluster_pruned_pairs.append({
                    "circle_index": int(_raw_row[6]),
                    "admitted_index": _idx,
                    "frame_index": int(_sc.frame_index),
                    "icao_a": _sc.icao_a,
                    "icao_b": _sc.icao_b,
                    "normalized_residual": round(_residual, 3) if math.isfinite(_residual) else None,
                    "removal_reason": "cluster_outlier",
                })

        # Refined pairwise fit using only cluster-inlier circles (not all admitted circles).
        # This gives a pairwise RMS that reflects the cluster quality rather than being
        # polluted by outlier circles outside the dominant cluster.
        if inlier_circles:
            refined_pairwise_fit = _score_candidate_position_pairwise(
                lat, lon, inlier_circles, raw_by_index, period_s, direction,
            )
        else:
            refined_pairwise_fit = best_pairwise_fit

        detail["selection_diagnostics"].update({
            "best_cluster_quality_score": round(best_quality_score, 6),
            "best_cluster_support_score": round(best_quality["support_score"], 6),
            "cluster_count_pre_merge": detail.get("cluster_count_pre_merge"),
            "cluster_count_post_merge": detail.get("cluster_count_post_merge"),
            "cluster_merge_events": detail.get("cluster_merge_events", []),
            "top_centroid_separation_km": (
                round(float(same_lobe_metrics["centroid_separation_km"]), 3)
                if same_lobe_metrics else None
            ),
            "top_arc_jaccard": (
                round(float(same_lobe_metrics["arc_jaccard"]), 3)
                if same_lobe_metrics else None
            ),
            "top_arc_overlap_min_ratio": (
                round(float(same_lobe_metrics["arc_overlap_min_ratio"]), 3)
                if same_lobe_metrics else None
            ),
            "ambiguity_same_lobe_bypass": same_lobe_bypass,
            "inlier_circle_count": n_contributing_arcs,
            "total_inlier_pair_circles": n_contributing_arcs,
            "refined_subset_count": len(refined_circle_indices),
            "refined_subset_circle_indices": refined_circle_indices,
            "inlier_residual_summary": inlier_residual_summary,
            "pairwise_fit_score": best_pairwise_fit.score,
            "pairwise_weighted_rms_deg": best_pairwise_fit.weighted_rms_deg,
            "pairwise_residual_sigma_deg": best_pairwise_fit.residual_sigma_deg,
            "refined_pairwise_weighted_rms_deg": refined_pairwise_fit.weighted_rms_deg,
            "initial_admitted_pair_count": detail.get("initial_admitted_pair_count", len(selected)),
            "post_cluster_pruned_pair_count": len(post_cluster_pruned_pairs),
            "is_geometry_dominant_cluster": _is_geometry_dominant_cluster,
        })

        # Determine what the automatic solver would have done with this result.
        # Three statuses: accepted, accepted_geometry_dominant, would_be_rejected_ambiguous,
        # would_be_rejected_other.
        # Pairwise RMS (even refined) should reduce confidence but not automatically
        # kill a geometry-dominant frame.
        _auto_status = "accepted"
        if manually_forced_preview:
            if quality_ratio < effective_threshold and not _is_geometry_dominant_cluster:
                _auto_status = "would_be_rejected_ambiguous"
            elif (
                not math.isfinite(centroid_uncertainty_km)
                or centroid_uncertainty_km < 0.05
                or n_contributing_arcs < _PER_FRAME_MIN_CONTRIBUTING_ARCS
                or centroid_uncertainty_km >= _PER_FRAME_MAX_CEP_KM
            ):
                _auto_status = "would_be_rejected_other"
            elif (
                not math.isfinite(refined_pairwise_fit.weighted_rms_deg)
                or refined_pairwise_fit.weighted_rms_deg > _MAX_FINAL_RESIDUAL_SIGMA_DEG
            ):
                # High pairwise RMS even after pruning — geometry-dominant frames
                # survive with reduced weight; non-dominant frames are rejected.
                if _is_geometry_dominant_cluster:
                    _auto_status = "accepted_geometry_dominant"
                else:
                    _auto_status = "would_be_rejected_other"

        return {
            "success": True,
            "available": True,
            "solve_status": "success",
            "solve_reason": "success",
            "manually_forced_preview": bool(manually_forced_preview),
            "automatic_acceptance_status": _auto_status,
            "result": {
                "lat": lat,
                "lon": lon,
                "rms_km": best_cluster.rms_km,
                "centroid_uncertainty_km": centroid_uncertainty_km,
                "n_contributing_arcs": n_contributing_arcs,
                "n_inlier_pair_circles": n_contributing_arcs,
                "n_pairs": len(arr),
                "n_selected_observations": n_selected,
                "n_admitted_pair_circles": n_selected,
                "initial_admitted_pair_count": detail.get("initial_admitted_pair_count", n_selected),
                "interpolated_fraction": interp_frac,
                "azimuth_spread_deg": az_spread,
                "high_quality_frames": sum(
                    1 for f in frames_to_use if getattr(f, "quality", "") == "good"
                ),
                "total_selected_weight": total_weight,
                "cluster_member_count": best_cluster.member_count,
                "second_cluster_member_count": second_cluster.member_count if second_cluster is not None else 0,
                "member_dominance_ratio": (
                    best_cluster.member_count / second_cluster.member_count
                    if second_cluster is not None and second_cluster.member_count > 0
                    else float("inf")
                ),
                "cluster_best_weight": best_cluster.total_weight,
                "cluster_second_weight": second_cluster.total_weight if second_cluster is not None else 0.0,
                "weight_dominance_ratio": (
                    best_cluster.total_weight / second_cluster.total_weight
                    if second_cluster is not None and second_cluster.total_weight > 0.0
                    else float("inf")
                ),
                "dominance_ratio": dominance_ratio,
                "support_dominance_ratio": support_ratio,
                "quality_ratio": quality_ratio,
                "cluster_count_pre_merge": detail.get("cluster_count_pre_merge"),
                "cluster_count_post_merge": detail.get("cluster_count_post_merge"),
                "cluster_merge_events": detail.get("cluster_merge_events", []),
                "top_centroid_separation_km": (
                    round(float(same_lobe_metrics["centroid_separation_km"]), 3)
                    if same_lobe_metrics else None
                ),
                "top_arc_jaccard": (
                    round(float(same_lobe_metrics["arc_jaccard"]), 3)
                    if same_lobe_metrics else None
                ),
                "top_arc_overlap_min_ratio": (
                    round(float(same_lobe_metrics["arc_overlap_min_ratio"]), 3)
                    if same_lobe_metrics else None
                ),
                "ambiguity_same_lobe_bypass": same_lobe_bypass,
                "is_geometry_dominant_cluster": _is_geometry_dominant_cluster,
                "receiver_distance_m": receiver_distance_m,
                "selection_diagnostics": detail["selection_diagnostics"],
                "admitted_pair_circles": admitted_pair_circles,
                "inlier_pair_circles": inlier_pair_circles,
                "post_cluster_pruned_pairs": post_cluster_pruned_pairs[:50],
                "post_cluster_pruned_pair_count": len(post_cluster_pruned_pairs),
                "refined_subset_circle_indices": refined_circle_indices,
                "scored_pair_circles": [serialize_pair_circle(sc) for sc in scored],
                "candidate_intersections": detail.get("candidate_intersections", []),
                "candidate_clusters": detail.get("candidate_clusters", []),
                "manually_forced_preview": bool(manually_forced_preview),
                "automatic_acceptance_status": _auto_status,
                "pair_circle_summary": pair_circle_summary(len(inlier_pair_circles)),
                "raw_candidate_count": detail["raw_candidate_count"],
                "plausible_candidate_count": detail["plausible_candidate_count"],
                "degenerate_baseline_pairs": detail["degenerate_baseline_pairs"],
                "endpoint_candidate_rejections": detail["endpoint_candidate_rejections"],
                "cluster_count": len(clusters),
                "fit_score": best_pairwise_fit.score,
                "pairwise_fit_score": best_pairwise_fit.score,
                "pairwise_mean_residual_deg": best_pairwise_fit.mean_residual_deg,
                "pairwise_residual_sigma_deg": best_pairwise_fit.residual_sigma_deg,
                "pairwise_weighted_rms_deg": best_pairwise_fit.weighted_rms_deg,
                "refined_pairwise_weighted_rms_deg": (
                    round(float(refined_pairwise_fit.weighted_rms_deg), 3)
                    if math.isfinite(refined_pairwise_fit.weighted_rms_deg) else None
                ),
                "best_cluster_quality_score": round(best_quality_score, 6),
                "best_cluster_support_score": round(best_quality["support_score"], 6),
                "best_cluster_compactness_km": round(best_quality["compactness_km"], 3),
                "best_cluster_conditioning_min_eigenvalue": round(best_quality["conditioning_min_eigenvalue"], 6),
                "best_cluster_conditioning_max_eigenvalue": round(best_quality["conditioning_max_eigenvalue"], 6),
                "best_cluster_condition_number": (
                    round(best_quality["condition_number"], 3)
                    if math.isfinite(best_quality["condition_number"]) else None
                ),
                "best_cluster_inverse_condition_number": round(best_quality["inverse_condition_number"], 6),
                "best_cluster_diversity_score": round(best_quality["diversity_score"], 6),
                "best_cluster_effective_aircraft_count": best_quality["effective_aircraft_count"],
                "best_cluster_effective_frame_count": best_quality["effective_frame_count"],
                "best_cluster_raw_inlier_count": best_quality["raw_inlier_count"],
                "best_cluster_refined_subset_count": best_quality["refined_subset_count"],
                "second_cluster_quality_score": round(second_quality_score, 6) if second_quality else 0.0,
                "second_cluster_support_score": round(second_quality["support_score"], 6) if second_quality else 0.0,
                "inlier_residual_summary": inlier_residual_summary,
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
