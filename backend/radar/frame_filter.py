"""Four-stage filter for frame position estimates before weighted-mean accumulation.

Inputs
------
estimates : list[FramePositionEstimate]
    Per-frame position estimates in lat/lon, each carrying a scalar cep_km
    that serves as the 1-sigma uncertainty (isotropic assumption).
receiver_lat, receiver_lon : float
    Fixed receiver location used as the ENU origin.

Outputs
-------
FilterResult (dataclass) with:
    lat, lon          – combined position (or None if n_inliers < 2)
    sigma_combined_m  – scalar 1-sigma uncertainty in metres
    n_total           – frames entering Stage 0
    n_stage0_survivors– frames passing Stage 0
    n_inliers         – frames in final inlier set
    rejection_counts  – dict{'stage0', 'stage1', 'stage2'} → int

Which stage feeds what
----------------------
Stage 0 → quality pre-filter (runs once, before any position comparison)
Stage 1 → per-frame Mahalanobis gate using each frame's own predicted covariance
Stage 2 → sample-covariance second pass (Huberized, only when n_inliers >= 20)
Stage 3 → iteration of Stages 1–2 up to MAX_ITER times
"""

from __future__ import annotations

import math
from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Optional

import numpy as np

if TYPE_CHECKING:
    from .forward_model import FramePositionEstimate


# ── Named constants (no magic numbers) ──────────────────────────────────

MIN_ARCS: int = 3
MIN_SPREAD: float = 30.0          # degrees
MIN_DOMINANCE: float = 1.25
MAX_INTERP: float = 0.60

CHI2_THRESHOLD_99: float = 9.2103  # chi-squared, 2 DOF, p=0.99

HUBER_C: float = 1.5               # tuning constant in units of MAD
MAX_ITER: int = 3
STAGE2_MIN_N: int = 20

MAX_CONDITION_NUMBER: float = 1e6  # guard against near-singular covariance

# Minimum plausible single-frame CEP (km).  The solver can produce cep_km = 0
# when all intersection candidates coincide exactly (e.g. near-identical circles
# from collinear observations).  These frames are solver artifacts; 50 m is well
# below anything achievable from TOA timing resolution so no real estimate is lost.
MIN_CEP_KM: float = 0.05

# Physical minimum 1-sigma used in the weighted centroid and Mahalanobis metric.
# Frames claiming sub-500 m accuracy would dominate the centroid by orders of
# magnitude over normal ~3-5 km frames, biasing P_bar and collapsing Stage-1
# thresholds.  500 m is a conservative lower bound for single-frame TOA accuracy.
MIN_SIGMA_M: float = 500.0


# ── Result type ─────────────────────────────────────────────────────────

@dataclass
class FilterResult:
    """Output of the four-stage frame estimate filter."""
    lat: Optional[float]
    lon: Optional[float]
    sigma_combined_m: Optional[float]
    n_total: int
    n_stage0_survivors: int
    n_inliers: int
    rejection_counts: dict[str, int]
    inlier_sweep_start_us: frozenset = field(default_factory=frozenset)


# ── ENU conversion helpers ─────────────────────────────────────────────

_R_EARTH: float = 6_371_000.0


def _latlon_to_enu(lat: float, lon: float,
                   origin_lat: float, origin_lon: float) -> tuple[float, float]:
    """Convert lat/lon (degrees) to ENU metres relative to origin."""
    dlat = math.radians(lat - origin_lat)
    dlon = math.radians(lon - origin_lon)
    east = _R_EARTH * dlon * math.cos(math.radians(origin_lat))
    north = _R_EARTH * dlat
    return east, north


def _enu_to_latlon(east: float, north: float,
                   origin_lat: float, origin_lon: float) -> tuple[float, float]:
    """Convert ENU metres back to lat/lon (degrees)."""
    lat = origin_lat + math.degrees(north / _R_EARTH)
    lon = origin_lon + math.degrees(east / (_R_EARTH * math.cos(math.radians(origin_lat))))
    return lat, lon


# ── Main filter function ────────────────────────────────────────────────

def filter_frame_estimates(
    estimates: list[FramePositionEstimate],
    receiver_lat: float,
    receiver_lon: float,
) -> FilterResult:
    """Run the four-stage filter on frame estimates and return combined position.

    Stages
    ------
    Stage 0 : quality-score pre-filter (arc count, spread, dominance, interp fraction,
              condition number).  Applied once only.
    Stage 1 : per-frame predicted-covariance Mahalanobis gate.
    Stage 2 : sample-covariance second pass with Huber weights (n_inliers >= 20 only).
    Stage 3 : iterate Stages 1–2 up to MAX_ITER times or until convergence.

    Parameters
    ----------
    estimates : list[FramePositionEstimate]
        Raw per-frame estimates from the intersection solver / Nelder-Mead.
    receiver_lat, receiver_lon : float
        Fixed receiver location serving as the ENU origin.

    Returns
    -------
    FilterResult
        Combined position, uncertainty, and diagnostic counts.
    """
    n_total = len(estimates)
    rejection_counts = {"stage0": 0, "stage1": 0, "stage2": 0}

    if n_total == 0:
        return FilterResult(
            lat=None, lon=None, sigma_combined_m=None,
            n_total=0, n_stage0_survivors=0, n_inliers=0,
            rejection_counts=rejection_counts,
        )

    # Convert all estimates to ENU and pre-compute per-frame covariance.
    class _FrameData:
        __slots__ = ("est", "enu", "sigma2", "cov")
        def __init__(self, est: FramePositionEstimate,
                     enu: np.ndarray, sigma2: float, cov: np.ndarray):
            self.est = est
            self.enu = enu
            self.sigma2 = sigma2
            self.cov = cov

    frames: list[_FrameData] = []
    for est in estimates:
        e, n = _latlon_to_enu(est.lat, est.lon, receiver_lat, receiver_lon)
        enu = np.array([e, n], dtype=np.float64)
        # Isotropic covariance: Σᵢ = σ² × I₂,  σᵢ = cep_km × 1000
        # Clamp to MIN_SIGMA_M so no frame claims tighter accuracy than
        # the physical system can achieve; prevents small-cep frames from
        # dominating the centroid even after the Stage-0 CEP floor.
        sigma = max(est.cep_km * 1000.0, MIN_SIGMA_M)
        sigma2 = sigma * sigma  # trace(Σᵢ)/2 = σ² for isotropic
        cov = sigma2 * np.eye(2, dtype=np.float64)
        frames.append(_FrameData(est, enu, sigma2, cov))

    # ── Stage 0: quality-score pre-filter ────────────────────────────────
    stage0_survivors: list[_FrameData] = []
    for fd in frames:
        est = fd.est
        # Reject solver artifacts: cep_km = 0 (or near-zero) means intersection
        # candidates perfectly coincided — this is a degenerate geometry, not a
        # valid precision estimate.
        if est.cep_km < MIN_CEP_KM:
            rejection_counts["stage0"] += 1
            continue
        if est.n_contributing_arcs < MIN_ARCS:
            rejection_counts["stage0"] += 1
            continue
        if est.azimuth_spread_deg < MIN_SPREAD:
            rejection_counts["stage0"] += 1
            continue
        if est.cluster_dominance_ratio < MIN_DOMINANCE:
            rejection_counts["stage0"] += 1
            continue
        if est.interpolated_position_fraction > MAX_INTERP:
            rejection_counts["stage0"] += 1
            continue
        # Condition number guard on per-frame covariance
        if fd.sigma2 > 0.0:
            # For isotropic 2×2 matrix σ²I, cond = 1.0. But guard explicitly.
            if np.linalg.cond(fd.cov) > MAX_CONDITION_NUMBER:
                rejection_counts["stage0"] += 1
                continue
        else:
            rejection_counts["stage0"] += 1
            continue
        stage0_survivors.append(fd)

    n_stage0_survivors = len(stage0_survivors)
    inliers = stage0_survivors  # start iteration from Stage-0 survivors

    # ── Stages 1–3: iterative Mahalanobis + sample-covariance gates ──────
    # On the first pass use the coordinate-wise median rather than the
    # inverse-variance weighted mean.  The weighted mean is sensitive to
    # any frame with σᵢ << typical σ (even after MIN_SIGMA_M clamping it
    # can still dominate when there are only a few such frames).  The median
    # is unaffected by weight imbalance and gives a robust seed for the gate.
    # Subsequent iterations switch to the weighted mean, which is now stable
    # because the small-σ outliers have been trimmed.
    use_median_seed = True

    for _iteration in range(MAX_ITER):
        prev_inlier_count = len(inliers)

        # Compute centroid: median on first pass, weighted mean thereafter.
        if use_median_seed and len(inliers) >= 2:
            positions = np.array([fd.enu for fd in inliers], dtype=np.float64)
            p_bar = np.median(positions, axis=0)
            use_median_seed = False
        else:
            p_bar = _weighted_centroid(inliers)
        if p_bar is None:
            # All inliers have zero weight — degenerate case.
            inliers = []
            break

        # Stage 1: per-frame predicted-covariance Mahalanobis gate.
        #   d²ᵢ = (Pᵢ − P̄)ᵀ · inv(Σᵢ) · (Pᵢ − P̄)
        #   Reject if d²ᵢ > CHI2_THRESHOLD_99
        stage1_survivors: list[_FrameData] = []
        for fd in inliers:
            residual = fd.enu - p_bar
            # For isotropic Σᵢ = σ²I: d² = ||residual||² / σ²
            d2 = float(np.dot(residual, residual) / fd.sigma2)
            if d2 <= CHI2_THRESHOLD_99:
                stage1_survivors.append(fd)
            else:
                rejection_counts["stage1"] += 1

        # Stage 2: sample-covariance second pass (only when n >= STAGE2_MIN_N).
        #   Compute Huberized covariance of inlier set, then Mahalanobis gate.
        if len(stage1_survivors) >= STAGE2_MIN_N:
            stage2_survivors = _stage2_filter(stage1_survivors, p_bar)
            rejected_stage2 = len(stage1_survivors) - len(stage2_survivors)
            rejection_counts["stage2"] += rejected_stage2
            inliers = stage2_survivors
        else:
            inliers = stage1_survivors

        # Convergence check: inlier set unchanged.
        if len(inliers) == prev_inlier_count:
            break

    # ── Final estimate computation ───────────────────────────────────────
    n_inliers = len(inliers)

    # Invariant: n_inliers <= n_stage0_survivors <= n_total
    assert n_inliers <= n_stage0_survivors <= n_total, (
        f"Invariant violated: {n_inliers=} > {n_stage0_survivors=} or "
        f"{n_stage0_survivors} > {n_total}"
    )

    inlier_sus = frozenset(fd.est.sweep_start_us for fd in inliers)

    if n_inliers < 2:
        return FilterResult(
            lat=None, lon=None, sigma_combined_m=None,
            n_total=n_total, n_stage0_survivors=n_stage0_survivors,
            n_inliers=n_inliers, rejection_counts=rejection_counts,
            inlier_sweep_start_us=inlier_sus,
        )

    p_combined, sigma_combined = _final_weighted_mean(inliers)

    lat, lon = _enu_to_latlon(p_combined[0], p_combined[1], receiver_lat, receiver_lon)

    return FilterResult(
        lat=lat, lon=lon, sigma_combined_m=sigma_combined,
        n_total=n_total, n_stage0_survivors=n_stage0_survivors,
        n_inliers=n_inliers, rejection_counts=rejection_counts,
        inlier_sweep_start_us=inlier_sus,
    )


# ── Internal helpers ─────────────────────────────────────────────────────

def _weighted_centroid(frames: list[_FrameData]) -> Optional[np.ndarray]:
    """Inverse-variance weighted mean of ENU positions.

    P̄ = Σ(Pᵢ / σ²ᵢ) / Σ(1 / σ²ᵢ)
    """
    total_w = 0.0
    weighted_sum = np.zeros(2, dtype=np.float64)
    for fd in frames:
        w = 1.0 / fd.sigma2 if fd.sigma2 > 0.0 else 0.0
        weighted_sum += fd.enu * w
        total_w += w
    if total_w <= 0.0:
        return None
    return weighted_sum / total_w


def _final_weighted_mean(frames: list[_FrameData]) -> tuple[np.ndarray, float]:
    """Compute combined position and scalar uncertainty from inlier set.

    P_combined = Σ(Pᵢ / σ²ᵢ) / Σ(1 / σ²ᵢ)
    σ_combined = 1 / sqrt(Σ(1 / σ²ᵢ))

    σ²ᵢ = trace(Σᵢ) / 2 = σ² for isotropic case.
    """
    total_w = 0.0
    weighted_sum = np.zeros(2, dtype=np.float64)
    for fd in frames:
        w = 1.0 / fd.sigma2 if fd.sigma2 > 0.0 else 0.0
        weighted_sum += fd.enu * w
        total_w += w
    p_combined = weighted_sum / total_w
    sigma_combined = 1.0 / math.sqrt(total_w)
    return p_combined, sigma_combined


def _stage2_filter(inliers: list[_FrameData],
                   p_bar: np.ndarray) -> list[_FrameData]:
    """Stage 2: Huberized sample-covariance Mahalanobis gate.

    1. Compute scalar distances from P̄ for each inlier.
    2. Compute MAD (median absolute deviation) of those distances.
    3. Compute Huber weights: wᵢ = min(1, c × MAD / |rᵢ|) with c = HUBER_C.
    4. Compute weighted sample covariance Σ_sample.
    5. Reject frame i if (Pᵢ − P̄)ᵀ · inv(Σ_sample) · (Pᵢ − P̄) > CHI2_THRESHOLD_99.
    """
    residuals = [fd.enu - p_bar for fd in inliers]
    distances = np.array([np.linalg.norm(r) for r in residuals])

    # MAD of distances
    median_dist = float(np.median(distances))
    abs_devs = np.abs(distances - median_dist)
    mad = float(np.median(abs_devs))

    # Huber weights: wᵢ = min(1, c * MAD / |rᵢ|)
    # Use c * MAD as the scale; protect against zero.
    scale = HUBER_C * mad if mad > 0.0 else 1e-6
    huber_weights = np.minimum(1.0, scale / np.maximum(distances, 1e-12))

    # Weighted sample covariance
    # Σ_sample = Σ(wᵢ · rᵢ · rᵢᵀ) / Σ(wᵢ)
    total_w = float(np.sum(huber_weights))
    if total_w <= 0.0:
        return inliers  # degenerate, pass through

    sample_cov = np.zeros((2, 2), dtype=np.float64)
    for r, w in zip(residuals, huber_weights):
        sample_cov += w * np.outer(r, r)
    sample_cov /= total_w

    # Gate against sample covariance
    survivors: list[_FrameData] = []
    try:
        inv_sample_cov = np.linalg.inv(sample_cov)
    except np.linalg.LinAlgError:
        return inliers  # singular, can't gate

    for fd, r in zip(inliers, residuals):
        d2 = float(r @ inv_sample_cov @ r)
        if d2 <= CHI2_THRESHOLD_99:
            survivors.append(fd)

    return survivors
