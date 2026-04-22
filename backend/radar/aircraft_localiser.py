"""
Stage 3: Aircraft localisation from known radar bearing observations.

AircraftLocaliser is a consumer of resolved radar state (Stage 1/2 outputs).
It must not store state inside RadarState or RadarIID.

Pipeline:
  3A  Bearing calibration per radar (offset + sigma fitted from truth aircraft)
  3B  Bearing observation construction per target per radar
  3C  Seed generation via pairwise ray intersections
  3D  Robust nonlinear snapshot solve (scipy, Huber loss)
  3E  Short-horizon runtime track filter
"""

from __future__ import annotations

import logging
import math
import time
import threading
from collections import deque
from dataclasses import dataclass
from typing import TYPE_CHECKING

import numpy as np
from scipy.optimize import least_squares

from .aircraft_models import (
    AircraftFix,
    AircraftTrackState,
    RadarBearingCalibration,
    RadarBearingObservation,
    Stage3LiveRay,
)
from .sweep import _get_authoritative_radar_position, LiveSyncState, predict_sync_observation

if TYPE_CHECKING:
    from .sweep import RadarState

log = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------
_R_EARTH_M = 6_371_000.0

# Calibration quality thresholds (samples)
_QUALITY_PROVISIONAL = 10
_QUALITY_STABLE_SAMPLES = 50
_QUALITY_STABLE_SIGMA_DEG = 5.0     # sigma must also be below this for "stable"

# Minimum angular separation to keep a pairwise intersection (deg)
_MIN_INTERSECTION_ANGLE_DEG = 10.0

# Maximum position age of ADS-B observation to use for calibration (seconds)
_MAX_POSITION_AGE_S = 8.0

# Maximum phase deviation to accept as same-sweep (degrees)
_MAX_PHASE_RANGE_DEG = 170.0

# Track timeout
_TRACK_TIMEOUT_S = 120.0

# Minimum observations to attempt a solve
_MIN_OBS_FOR_SOLVE = 2

# Freshness gating for live observations used as authoritative per-radar rays.
# A detection older than either gate is considered historical, not operational.
# Wall-clock gate catches long stalls; period-multiple gate catches radars with
# slow rotations where even one missed sweep is already operationally wrong.
_MAX_OBS_AGE_S = 5.0
_MAX_OBS_AGE_PERIODS = 1.5

# Minimum angular separation for seeds already covered by _MIN_INTERSECTION_ANGLE_DEG.

# Structured rejection reasons emitted by the selection / solve pipeline.
REASON_NO_SYNC = "no_usable_sync"
REASON_NO_CALIBRATION = "no_calibration"
REASON_SYNC_QUALITY_LOW = "sync_quality_below_threshold"
REASON_NO_DETECTIONS = "no_detections_for_icao"
REASON_STALE_OBSERVATION = "stale_observation"
REASON_PHASE_OUT_OF_RANGE = "phase_out_of_range"
REASON_OBS_BUILD_FAILED = "observation_build_failed"
REASON_RADAR_NOT_ELIGIBLE = "radar_not_eligible"
REASON_INSUFFICIENT_RADARS = "insufficient_distinct_radars"
REASON_NO_FORWARD_INTERSECTIONS = "no_valid_forward_intersections"
REASON_SOLVE_FAILED = "solve_failed"
REASON_EXCESSIVE_UNCERTAINTY = "excessive_uncertainty"
REASON_POOR_GEOMETRY = "poor_geometry"
REASON_NO_ELIGIBLE_RADARS = "no_eligible_radars"
REASON_ABSOLUTE_PHASE_UNTRUSTED = "absolute_phase_untrusted"
REASON_BEARING_TRUTH_MISMATCH = "bearing_truth_mismatch"

# Weight floor (prevents single observation from dominating)
_MIN_OBS_WEIGHT = 0.01

# Observation weight scale for quality
_READINESS_WEIGHT = {"stable": 1.0, "provisional": 0.5, "none": 0.1}


# ---------------------------------------------------------------------------
# Geometry helpers (local – avoids importing from api.py)
# ---------------------------------------------------------------------------

def _haversine_m(lat1: float, lon1: float, lat2: float, lon2: float) -> float:
    phi1, phi2 = math.radians(lat1), math.radians(lat2)
    dphi = math.radians(lat2 - lat1)
    dlam = math.radians(lon2 - lon1)
    a = math.sin(dphi / 2) ** 2 + math.cos(phi1) * math.cos(phi2) * math.sin(dlam / 2) ** 2
    return 2 * _R_EARTH_M * math.asin(math.sqrt(max(0.0, a)))


def _bearing_deg(lat1: float, lon1: float, lat2: float, lon2: float) -> float:
    """Forward azimuth from point 1 to point 2 [0, 360)."""
    phi1, phi2 = math.radians(lat1), math.radians(lat2)
    dlam = math.radians(lon2 - lon1)
    x = math.cos(phi1) * math.sin(phi2) - math.sin(phi1) * math.cos(phi2) * math.cos(dlam)
    y = math.sin(dlam) * math.cos(phi2)
    return (math.degrees(math.atan2(y, x)) + 360) % 360


def _wrap_deg(d: float) -> float:
    """Wrap angle to (-180, 180]."""
    d = d % 360.0
    if d > 180.0:
        d -= 360.0
    return d


def _latlon_to_enu(lat: float, lon: float, origin_lat: float, origin_lon: float) -> tuple[float, float]:
    """Lat/lon → local East/North in metres."""
    dlat = math.radians(lat - origin_lat)
    dlon = math.radians(lon - origin_lon)
    x = _R_EARTH_M * dlon * math.cos(math.radians(origin_lat))
    y = _R_EARTH_M * dlat
    return x, y


def _enu_to_latlon(x: float, y: float, origin_lat: float, origin_lon: float) -> tuple[float, float]:
    lat = origin_lat + math.degrees(y / _R_EARTH_M)
    lon = origin_lon + math.degrees(x / (_R_EARTH_M * math.cos(math.radians(origin_lat))))
    return lat, lon


# ---------------------------------------------------------------------------
# Authoritative radar position — delegate to the shared helper in sweep.py
# so Stage 2 and Stage 3 always agree on radar location.
# ---------------------------------------------------------------------------

def _authoritative_position_for_model(model) -> dict:
    """Return the best-available radar position for a RadarIID model."""
    return _get_authoritative_radar_position(model)


# ---------------------------------------------------------------------------
# Stage 3A: Bearing calibration fitter
# ---------------------------------------------------------------------------

def _calibration_quality(n: int, sigma: float, stable_samples: int) -> str:
    if n < _QUALITY_PROVISIONAL:
        return "none"
    if n >= stable_samples and sigma <= _QUALITY_STABLE_SIGMA_DEG:
        return "stable"
    return "provisional"


def _fit_calibration(iid: int, residuals_deg: list[float], stable_samples: int) -> RadarBearingCalibration | None:
    """Fit offset + sigma from a list of (theta_obs - theta_true) residuals."""
    if len(residuals_deg) < 3:
        return None

    arr = np.array(residuals_deg, dtype=float)

    # Iterative mean with wrap-around awareness
    # Start with unwrapped mean, then refine
    mean_offset = float(np.mean(arr))
    for _ in range(3):
        centred = np.array([_wrap_deg(r - mean_offset) for r in arr])
        mean_offset = _wrap_deg(mean_offset + float(np.mean(centred)))

    residuals_centred = np.array([_wrap_deg(r - mean_offset) for r in arr])
    sigma = float(np.std(residuals_centred))

    n = len(arr)
    quality = _calibration_quality(n, sigma, stable_samples)

    return RadarBearingCalibration(
        iid=iid,
        bearing_offset_deg=mean_offset,
        effective_delay_us=0.0,   # not separately fitted in v1
        bearing_sigma_deg=sigma,
        n_samples=n,
        quality=quality,
        last_calibrated_ts=time.time(),
    )


# ---------------------------------------------------------------------------
# Stage 3B: Bearing observation builder
# ---------------------------------------------------------------------------

def _build_observations_for_icao(
    iid: int,
    radar_lat: float,
    radar_lon: float,
    calibration: RadarBearingCalibration,
    frames: list,
    target_icao: str,
    receiver_lat: float,
    receiver_lon: float,
) -> list[RadarBearingObservation]:
    """Extract bearing observations for target_icao from recent sweep frames of one radar."""
    obs_out: list[RadarBearingObservation] = []

    for frame in frames:
        period_s = frame.period_s
        if period_s is None or period_s <= 0:
            continue
        if frame.ref_lat is None or frame.ref_lon is None:
            continue

        period_us = period_s * 1e6
        ref_bearing = _bearing_deg(radar_lat, radar_lon, frame.ref_lat, frame.ref_lon)

        for obs in frame.observations:
            if obs.icao != target_icao:
                continue
            if obs.position_age_seconds > _MAX_POSITION_AGE_S:
                continue

            phase_deg = (obs.arrival_us - frame.ref_arrival_us) / period_us * 360.0
            if abs(phase_deg) > _MAX_PHASE_RANGE_DEG:
                continue

            bearing_obs = _wrap_deg(ref_bearing + phase_deg + calibration.bearing_offset_deg)

            sigma = calibration.bearing_sigma_deg
            # Scale sigma by association confidence proxy (position age)
            age_factor = 1.0 + obs.position_age_seconds / _MAX_POSITION_AGE_S
            sigma = sigma * age_factor
            sigma = max(sigma, 0.5)  # floor at 0.5°

            obs_out.append(RadarBearingObservation(
                iid=iid,
                icao=target_icao,
                arrival_us=obs.arrival_us,
                phase_deg=phase_deg,
                bearing_obs_deg=bearing_obs,
                bearing_sigma_deg=sigma,
                radar_lat=radar_lat,
                radar_lon=radar_lon,
                receiver_lat=receiver_lat,
                receiver_lon=receiver_lon,
                association_confidence=max(0.0, 1.0 - obs.position_age_seconds / _MAX_POSITION_AGE_S),
                burst_signal_dbfs=obs.signal_dbfs,
                altitude_ft=None,
            ))

    return obs_out


# ---------------------------------------------------------------------------
# Stage 3C: Seed generation from pairwise ray intersections
# ---------------------------------------------------------------------------

def _ray_intersection_enu(
    x1: float, y1: float, theta1_deg: float,
    x2: float, y2: float, theta2_deg: float,
) -> tuple[float, float] | None:
    """Intersect two forward rays in ENU space.

    Each ray starts at its radar origin and extends in the direction of the
    observed bearing.  Intersections behind either radar origin are rejected
    so the seed pool is built from physically plausible crossings only.
    """
    t1 = math.radians(90.0 - theta1_deg)  # bearing → trig angle
    t2 = math.radians(90.0 - theta2_deg)
    dx1, dy1 = math.cos(t1), math.sin(t1)
    dx2, dy2 = math.cos(t2), math.sin(t2)

    # Solve: (x1 + t*dx1, y1 + t*dy1) = (x2 + s*dx2, y2 + s*dy2)
    denom = dx1 * dy2 - dy1 * dx2
    if abs(denom) < 1e-9:
        return None

    t = ((x2 - x1) * dy2 - (y2 - y1) * dx2) / denom
    s = ((x2 - x1) * dy1 - (y2 - y1) * dx1) / denom
    # Forward-ray constraint: both parameters must be non-negative.
    if t < 0.0 or s < 0.0:
        return None
    xi = x1 + t * dx1
    yi = y1 + t * dy1
    return xi, yi


def _angular_separation(a_deg: float, b_deg: float) -> float:
    """Absolute angular separation in [0, 90] degrees."""
    diff = abs(_wrap_deg(a_deg - b_deg))
    return min(diff, 180.0 - diff)


def _geometric_median_2d(points: np.ndarray, tol: float = 1e-6, max_iter: int = 50) -> np.ndarray:
    """Weiszfeld's algorithm for 2D geometric median."""
    if len(points) == 1:
        return points[0].copy()
    pt = points.mean(axis=0)
    for _ in range(max_iter):
        dists = np.linalg.norm(points - pt, axis=1)
        dists = np.maximum(dists, tol)
        weights = 1.0 / dists
        pt_new = (points * weights[:, None]).sum(axis=0) / weights.sum()
        if np.linalg.norm(pt_new - pt) < tol:
            break
        pt = pt_new
    return pt


def generate_seeds(
    observations: list[RadarBearingObservation],
    max_pairs: int = 50,
) -> list[tuple[float, float]]:
    """Generate candidate seed positions via pairwise ray intersections.

    Returns a list of (lat, lon) candidates, best first.
    """
    if len(observations) < 2:
        return []

    # Use the first observation's radar as origin
    origin_lat = observations[0].radar_lat
    origin_lon = observations[0].radar_lon

    # Build ENU positions of each radar and its bearing
    radar_enu: list[tuple[float, float, float]] = []  # (x, y, bearing_deg)
    for obs in observations:
        rx, ry = _latlon_to_enu(obs.radar_lat, obs.radar_lon, origin_lat, origin_lon)
        radar_enu.append((rx, ry, obs.bearing_obs_deg))

    intersections: list[np.ndarray] = []
    n = len(radar_enu)
    pairs_checked = 0

    for i in range(n):
        for j in range(i + 1, n):
            if pairs_checked >= max_pairs:
                break
            x1, y1, t1 = radar_enu[i]
            x2, y2, t2 = radar_enu[j]

            # Skip near-parallel
            if _angular_separation(t1, t2) < _MIN_INTERSECTION_ANGLE_DEG:
                continue

            pt = _ray_intersection_enu(x1, y1, t1, x2, y2, t2)
            if pt is not None:
                intersections.append(np.array(pt))
            pairs_checked += 1

    if not intersections:
        return []

    pts = np.array(intersections)

    # Filter to densest cluster (within 200 km of median)
    med = _geometric_median_2d(pts)
    dists = np.linalg.norm(pts - med, axis=1)
    keep = pts[dists < 200_000.0]
    if len(keep) < 1:
        keep = pts

    seed_enu = _geometric_median_2d(keep)
    seed_lat, seed_lon = _enu_to_latlon(seed_enu[0], seed_enu[1], origin_lat, origin_lon)
    return [(seed_lat, seed_lon)]


# ---------------------------------------------------------------------------
# Stage 3D: Nonlinear snapshot solve
# ---------------------------------------------------------------------------

def _obs_weight(obs: RadarBearingObservation, cal: RadarBearingCalibration) -> float:
    """Composite weight for one bearing observation."""
    sigma_weight = 1.0 / max(obs.bearing_sigma_deg, 0.5)
    qual_weight = _READINESS_WEIGHT.get(cal.quality, 0.1)
    conf_weight = max(obs.association_confidence, 0.1)
    w = sigma_weight * qual_weight * conf_weight
    return max(w, _MIN_OBS_WEIGHT)


def _geometry_score(
    observations: list[RadarBearingObservation],
    sol_lat: float,
    sol_lon: float,
) -> float:
    """Score 0–1 based on angular spread and number of radars."""
    n = len(observations)
    if n < 2:
        return 0.0

    bearings = [
        _bearing_deg(obs.radar_lat, obs.radar_lon, sol_lat, sol_lon)
        for obs in observations
    ]
    # Angular spread: mean absolute deviation from uniform
    bearings_rad = np.array(sorted(bearings))
    gaps = np.diff(np.append(bearings_rad, bearings_rad[0] + 360.0))
    # Best possible spread: equal gaps = 360/n
    ideal_gap = 360.0 / n
    spread_score = float(1.0 - np.std(gaps) / max(ideal_gap, 1.0))
    spread_score = max(0.0, min(1.0, spread_score))

    count_score = min(1.0, n / 4.0)
    return 0.5 * spread_score + 0.5 * count_score


def solve_snapshot(
    observations: list[RadarBearingObservation],
    calibrations: dict[int, RadarBearingCalibration],
    seed_lat: float,
    seed_lon: float,
    max_iters: int = 10,
) -> AircraftFix | None:
    """Robust nonlinear bearing-only solve.

    Caller is expected to pass an already-selected authoritative observation
    set (one per radar).  A trust-but-verify deduplication step is retained
    so older callers that pass multi-candidate lists do not silently mix
    observations; when duplicates exist the newest arrival_us wins.  This
    keeps the solver input identical to the display set produced upstream.
    """
    by_iid: dict[int, RadarBearingObservation] = {}
    for obs in observations:
        existing = by_iid.get(obs.iid)
        if existing is None or obs.arrival_us > existing.arrival_us:
            by_iid[obs.iid] = obs

    obs_list = [o for o in by_iid.values() if o.iid in calibrations]
    n_radars = len(obs_list)

    if n_radars < _MIN_OBS_FOR_SOLVE:
        return None

    origin_lat, origin_lon = seed_lat, seed_lon

    # Build per-obs data
    weights = np.array([_obs_weight(o, calibrations[o.iid]) for o in obs_list])
    sigmas = np.array([o.bearing_sigma_deg for o in obs_list])
    radar_xy = np.array([
        _latlon_to_enu(o.radar_lat, o.radar_lon, origin_lat, origin_lon)
        for o in obs_list
    ])  # shape (n, 2)
    bearing_obs = np.array([o.bearing_obs_deg for o in obs_list])

    def residuals(xy: np.ndarray) -> np.ndarray:
        px, py = xy
        plat, plon = _enu_to_latlon(px, py, origin_lat, origin_lon)
        r = np.empty(len(obs_list))
        for i, obs in enumerate(obs_list):
            theta_pred = _bearing_deg(obs.radar_lat, obs.radar_lon, plat, plon)
            r[i] = _wrap_deg(obs.bearing_obs_deg - theta_pred) / sigmas[i] * math.sqrt(weights[i])
        return r

    x0 = np.array(_latlon_to_enu(seed_lat, seed_lon, origin_lat, origin_lon))

    try:
        result = least_squares(
            residuals,
            x0,
            method="trf",
            loss="huber",
            f_scale=1.0,
            max_nfev=max_iters * 10,
            ftol=1e-6,
            xtol=1.0,   # 1 metre convergence
            gtol=1e-6,
        )
        converged = result.status in (1, 2, 3, 4)
    except Exception as exc:
        log.debug("Stage 3 solve exception: %s", exc)
        return None

    if not converged and result.cost > 1e6:
        return None

    sol_x, sol_y = result.x
    sol_lat, sol_lon = _enu_to_latlon(sol_x, sol_y, origin_lat, origin_lon)

    # Estimate CEP from the bearing geometry at the solved position.
    cep_m = _estimate_cep_from_geometry(obs_list, sol_lat, sol_lon)

    geo_score = _geometry_score(obs_list, sol_lat, sol_lon)

    track_id = obs_list[0].icao or f"uk_{int(time.time())}"

    return AircraftFix(
        track_id=track_id,
        lat=sol_lat,
        lon=sol_lon,
        alt_ft=None,
        cep_m=cep_m,
        geometry_score=geo_score,
        n_radars=n_radars,
        n_observations=len(obs_list),
        solver_status="ok" if converged else "non_convergent",
        solver_detail={
            "cost": float(result.cost),
            "nfev": int(result.nfev),
            "status": int(result.status),
            "seed_lat": seed_lat,
            "seed_lon": seed_lon,
        },
        ts=time.time(),
    )


def _estimate_cep_from_geometry(
    obs_list: list[RadarBearingObservation],
    sol_lat: float,
    sol_lon: float,
) -> float:
    """CEP50 estimate in metres from a physically-grounded bearing-only Fisher model.

    For each radar the bearing gradient with respect to the target ENU position
    is H_i = (-sin(az_i), cos(az_i)) / r_i, with units [1/m].  The Fisher
    information in rad^-2·m^-2 is F = sum(H_i H_i^T / sigma_rad_i^2), so the
    position covariance is C = F^-1 [m^2].  CEP50 ≈ 0.59*(sigma_major + sigma_minor)
    (Grubbs' approximation for a bivariate normal).
    """
    if len(obs_list) < 2:
        return float("inf")
    try:
        F = np.zeros((2, 2), dtype=float)
        for o in obs_list:
            r_m = _haversine_m(o.radar_lat, o.radar_lon, sol_lat, sol_lon)
            if r_m < 1.0:
                continue
            az = _bearing_deg(o.radar_lat, o.radar_lon, sol_lat, sol_lon)
            az_rad = math.radians(az)
            # Gradient of bearing (rad) with respect to ENU (east, north).
            h = np.array([-math.sin(az_rad), math.cos(az_rad)]) / r_m
            sigma_rad = math.radians(max(o.bearing_sigma_deg, 0.1))
            F += np.outer(h, h) / (sigma_rad ** 2)
        if np.linalg.matrix_rank(F) < 2:
            return float("inf")
        cov = np.linalg.inv(F)
        eigvals = np.linalg.eigvalsh(cov)
        eigvals = np.maximum(eigvals, 0.0)
        sigma_major = math.sqrt(float(eigvals[-1]))
        sigma_minor = math.sqrt(float(eigvals[0]))
        return float(0.59 * (sigma_major + sigma_minor))
    except Exception:
        return float("inf")


# ---------------------------------------------------------------------------
# Stage 3B/3C helpers: per-IID candidate collapsing and display selection
# ---------------------------------------------------------------------------

def _score_live_bearing_candidate(obs: RadarBearingObservation) -> float:
    """Ranking score for live bearing candidates.  Higher is better.

    Recency (arrival_us) is the primary axis so the displayed ray always uses
    the latest burst centre.  Signal quality, association confidence, and bearing
    sigma act as secondary factors — a stronger, well-confirmed recent burst is
    preferred over an equally recent but weaker one.
    """
    # Signal quality: stronger signals produce better amplitude-weighted burst centres.
    if obs.burst_signal_dbfs is not None:
        # Map [-50 dBFS, -10 dBFS] → [0.5, 1.5]; stronger gets a larger bonus.
        sig_bonus = max(0.5, min(1.5, (obs.burst_signal_dbfs + 50.0) / 40.0 + 0.5))
    else:
        sig_bonus = 1.0  # neutral when signal is unavailable

    sigma_score = 1.0 / max(obs.bearing_sigma_deg, 0.5)
    return (
        obs.arrival_us
        + obs.association_confidence * 1e6 * sig_bonus
        + sigma_score * 1e5
    )


def _collapse_candidates_by_iid(
    observations: list[RadarBearingObservation],
) -> list[RadarBearingObservation]:
    """Return one observation per radar IID, keeping the best-scored candidate.

    Collapsing before seed generation prevents a single radar that has retained
    several recent detections from dominating pairwise intersection counts and
    producing misleading seed clusters.
    """
    best: dict[int, RadarBearingObservation] = {}
    for obs in observations:
        if obs.iid not in best or _score_live_bearing_candidate(obs) > _score_live_bearing_candidate(best[obs.iid]):
            best[obs.iid] = obs
    return list(best.values())


def _score_live_ray_for_display(ray: Stage3LiveRay) -> float:
    """Ranking score for one Stage3LiveRay for display selection.  Higher is better.

    Recency (arrival_us) is the primary axis.  Association confidence acts as a
    secondary factor so a confirmed, recent ray beats an unconfirmed one of the
    same burst-centre timestamp.
    """
    return ray.arrival_us + ray.association_confidence * 1e6


def _select_best_live_ray_per_radar(rays: list[Stage3LiveRay]) -> list[Stage3LiveRay]:
    """Return one Stage3LiveRay per radar IID, preferring the best-scored burst-centre ray.

    Used by build_evidence() to collapse the display layer so exactly one
    bearing ray is rendered per radar regardless of how many solve cycles have
    contributed rays to the buffer for a given aircraft.

    Selection uses _score_live_ray_for_display() so the chosen ray reflects the
    most recent well-confirmed burst centre, not just the most recent timestamp.
    """
    best: dict[int, Stage3LiveRay] = {}
    for ray in rays:
        if ray.iid not in best:
            best[ray.iid] = ray
        else:
            if _score_live_ray_for_display(ray) > _score_live_ray_for_display(best[ray.iid]):
                best[ray.iid] = ray
    return list(best.values())


# ---------------------------------------------------------------------------
# Stage 3E: Short-horizon runtime track filter
# ---------------------------------------------------------------------------

_TRACK_HISTORY_LEN = 20
_VELOCITY_ALPHA = 0.3   # EMA smoothing for velocity estimate


def _update_track(
    track: AircraftTrackState,
    fix: AircraftFix,
    now: float,
) -> AircraftTrackState:
    """Blend a new fix into the track state."""
    dt = now - track.last_update_ts
    if dt <= 0 or dt > _TRACK_TIMEOUT_S:
        # Reset velocity if too long since last fix
        vx, vy = 0.0, 0.0
    else:
        fix_x, fix_y = _latlon_to_enu(fix.lat, fix.lon, track.lat, track.lon)
        vx_raw = fix_x / dt
        vy_raw = fix_y / dt
        # EMA smoothing
        vx = (1 - _VELOCITY_ALPHA) * track.vx_mps + _VELOCITY_ALPHA * vx_raw
        vy = (1 - _VELOCITY_ALPHA) * track.vy_mps + _VELOCITY_ALPHA * vy_raw

    history = list(track.history)[-(_TRACK_HISTORY_LEN - 1):] + [fix]

    return AircraftTrackState(
        track_id=track.track_id,
        lat=fix.lat,
        lon=fix.lon,
        vx_mps=vx,
        vy_mps=vy,
        alt_ft=fix.alt_ft,
        position_covariance=[fix.cep_m ** 2, 0.0, 0.0, fix.cep_m ** 2],
        last_update_ts=now,
        source="stage3",
        history=history,
    )


def _new_track(fix: AircraftFix, now: float) -> AircraftTrackState:
    return AircraftTrackState(
        track_id=fix.track_id,
        lat=fix.lat,
        lon=fix.lon,
        vx_mps=0.0,
        vy_mps=0.0,
        alt_ft=fix.alt_ft,
        position_covariance=[fix.cep_m ** 2, 0.0, 0.0, fix.cep_m ** 2],
        last_update_ts=now,
        source="stage3",
        history=[fix],
    )


def predict_localiser_live_path_bearing(
    sync_state: "LiveSyncState",
    arrival_beast_us: float,
    *,
    range_nm: float | None = None,
    waveform_bins: list | None = None,
    bearing_rate_deg_s: float | None = None,
    motion_comp_dt_us: float | None = None,
    motion_comp_block_reason: str | None = None,
) -> float:
    """Backend-localiser live predictor before calibration offset.

    This helper exists for sync-debug path comparison.  The operational
    localiser calls the same authoritative predictor inside
    _bearing_from_live_detection(); wall-clock timestamps are not accepted here.
    """
    prediction = predict_sync_observation(
        sync_state,
        arrival_beast_us,
        range_nm=range_nm,
        waveform_bins=waveform_bins,
        bearing_rate_deg_s=bearing_rate_deg_s,
        motion_comp_dt_us=motion_comp_dt_us,
        motion_comp_block_reason=motion_comp_block_reason,
    )
    return prediction.predicted_bearing_deg


# ---------------------------------------------------------------------------
# Main AircraftLocaliser class
# ---------------------------------------------------------------------------

class AircraftLocaliser:
    """Stage 3 runtime: calibration, observation, solve, and track filter."""

    def __init__(
        self,
        radar_state: "RadarState",
        aircraft_state,
        receiver_lat: float | None,
        receiver_lon: float | None,
        min_calibration_samples: int = _QUALITY_PROVISIONAL,
        min_radars_for_fix: int = 2,
        max_cep_m: float = 50_000.0,
        stable_calibration_samples: int = _QUALITY_STABLE_SAMPLES,
    ):
        self._radar_state = radar_state
        self._aircraft_state = aircraft_state
        self._receiver_lat = receiver_lat or 0.0
        self._receiver_lon = receiver_lon or 0.0
        self._min_calibration_samples = min_calibration_samples
        self._min_radars_for_fix = min_radars_for_fix
        self._max_cep_m = max_cep_m
        self._stable_calibration_samples = stable_calibration_samples

        self._lock = threading.Lock()
        self._calibrations: dict[int, RadarBearingCalibration] = {}
        self._tracks: dict[str, AircraftTrackState] = {}
        self._timing_history: deque = deque(maxlen=240)
        self._n_calibration_cycles = 0
        self._n_localisation_cycles = 0
        self._last_calibration_ts = 0.0
        self._last_localisation_ts = 0.0
        self._backlog_skips = 0

        # Evidence buffers, keyed by ICAO.  Populated during _solve_for_icao;
        # read by build_evidence().
        #
        #   _current_rays_by_icao
        #     The authoritative live bearing ray set from the most recent
        #     solve cycle — exactly one per contributing radar, the identical
        #     set used as solver input.  This is what the primary
        #     "Bearing Rays" display layer renders.
        #
        #   _current_rejected_rays_by_icao
        #     Rays from the most recent solve cycle that were built but
        #     rejected (stale, no sync, no calibration, etc.), each with a
        #     populated rejection_reason.
        #
        #   _historical_rays_by_icao
        #     Rolling deque of accepted rays from prior solve cycles, used
        #     only for the separate "Recent Ray History" evidence layer.
        #     Never used as solver input.
        #
        #   _recent_seed_points_by_icao
        #     Seed points generated from forward-ray intersections.
        #
        #   _last_rejection_reasons_by_icao
        #     Structured rejection state from the most recent solve: per-radar
        #     reasons and an optional global failure reason.
        _RAY_BUF = 200
        self._current_rays_by_icao: dict[str, list[Stage3LiveRay]] = {}
        self._current_rejected_rays_by_icao: dict[str, list[Stage3LiveRay]] = {}
        self._historical_rays_by_icao: dict[str, deque] = {}
        self._recent_seed_points_by_icao: dict[str, deque] = {}
        self._last_rejection_reasons_by_icao: dict[str, dict] = {}
        self._RAY_BUFFER_MAX = _RAY_BUF
        self._ray_retention_s: float = 30.0   # overridden by config in main.py

    # ------------------------------------------------------------------
    # DB round-trip helpers
    # ------------------------------------------------------------------

    def load_calibrations(self, rows: list[dict]) -> None:
        """Load persisted calibration rows from DB at startup."""
        with self._lock:
            for row in rows:
                cal = RadarBearingCalibration(
                    iid=int(row["iid"]),
                    bearing_offset_deg=float(row["bearing_offset_deg"]),
                    effective_delay_us=float(row["effective_delay_us"]),
                    bearing_sigma_deg=float(row["bearing_sigma_deg"]),
                    n_samples=int(row["n_samples"]),
                    quality=str(row["quality"]),
                    last_calibrated_ts=float(row["last_calibrated_ts"]),
                )
                self._calibrations[cal.iid] = cal
        log.info("Stage3: loaded %d bearing calibration rows from DB", len(rows))

    # ------------------------------------------------------------------
    # Eligible radar selection
    # ------------------------------------------------------------------

    def get_eligible_iids(self) -> list[tuple[int, dict, object]]:
        """Return list of (iid, auth_pos, model) for Stage 3-eligible radars.

        get_all_rotation_models() returns dict[int, RadarIID] — check period_s on RadarIID.
        """
        eligible = []
        models = self._radar_state.get_all_rotation_models()  # dict[int, RadarIID]
        for iid, model in models.items():
            if model.period_s is None:
                continue
            if model.resolution_mode == "locked_unresolvable":
                continue
            auth = _authoritative_position_for_model(model)
            if auth["lat"] is None or auth["lon"] is None:
                continue
            eligible.append((iid, auth, model))
        return eligible

    # ------------------------------------------------------------------
    # Stage 3A: Calibration
    # ------------------------------------------------------------------

    def run_calibration_cycle(
        self,
        max_iids: int = 10,
    ) -> list[RadarBearingCalibration]:
        """Fit per-radar bearing calibration from truth aircraft sweep frames.

        Returns updated calibrations to be upserted to DB.
        """
        t0 = time.time()
        updated: list[RadarBearingCalibration] = []

        eligible = self.get_eligible_iids()[:max_iids]

        for iid, auth, _model in eligible:
            radar_lat = auth["lat"]
            radar_lon = auth["lon"]

            frames = self._radar_state.get_sweep_frames(iid)
            # Use only completed frames with a period
            frames = [f for f in frames if f.period_s is not None and f.period_s > 0]
            if not frames:
                continue

            residuals: list[float] = []

            for frame in frames:
                period_us = frame.period_s * 1e6
                if frame.ref_lat is None or frame.ref_lon is None:
                    continue

                ref_bearing = _bearing_deg(radar_lat, radar_lon, frame.ref_lat, frame.ref_lon)

                for obs in frame.observations:
                    # Only use observations with fresh ADS-B positions (truth)
                    if obs.position_age_seconds > _MAX_POSITION_AGE_S:
                        continue
                    if obs.lat is None or obs.lon is None:
                        continue

                    phase_deg = (obs.arrival_us - frame.ref_arrival_us) / period_us * 360.0
                    if abs(phase_deg) > _MAX_PHASE_RANGE_DEG:
                        continue

                    theta_obs = ref_bearing + phase_deg
                    theta_true = _bearing_deg(radar_lat, radar_lon, obs.lat, obs.lon)
                    residuals.append(_wrap_deg(theta_obs - theta_true))

            if len(residuals) < 3:
                continue

            cal = _fit_calibration(iid, residuals, self._stable_calibration_samples)
            if cal is None:
                continue

            with self._lock:
                self._calibrations[iid] = cal
            updated.append(cal)
            log.debug(
                "Stage3 calibration IID %d: offset=%.2f° sigma=%.2f° n=%d quality=%s",
                iid, cal.bearing_offset_deg, cal.bearing_sigma_deg, cal.n_samples, cal.quality,
            )

        elapsed = time.time() - t0
        self._last_calibration_ts = time.time()
        self._n_calibration_cycles += 1
        self._timing_history.append({"type": "calibration", "elapsed_s": elapsed, "ts": t0})
        return updated

    # ------------------------------------------------------------------
    # Stage 3B: Bearing observation construction
    # ------------------------------------------------------------------

    def build_bearing_observations(
        self,
        icao: str,
        iid_subset: set[int] | None = None,
        max_obs_per_radar: int = 3,
    ) -> list[RadarBearingObservation]:
        """Build bearing observations for a target aircraft across all calibrated radars."""
        with self._lock:
            cals = dict(self._calibrations)

        all_obs: list[RadarBearingObservation] = []

        for iid, auth, _ in self.get_eligible_iids():
            if iid_subset is not None and iid not in iid_subset:
                continue
            if iid not in cals:
                continue
            cal = cals[iid]
            if cal.quality == "none":
                continue

            radar_lat = auth["lat"]
            radar_lon = auth["lon"]

            frames = self._radar_state.get_sweep_frames(iid)
            frames = [f for f in frames if f.period_s is not None and f.period_s > 0]

            obs = _build_observations_for_icao(
                iid=iid,
                radar_lat=radar_lat,
                radar_lon=radar_lon,
                calibration=cal,
                frames=frames,
                target_icao=icao,
                receiver_lat=self._receiver_lat,
                receiver_lon=self._receiver_lon,
            )
            # Keep most recent per radar
            obs.sort(key=lambda o: o.arrival_us, reverse=True)
            all_obs.extend(obs[:max_obs_per_radar])

        return all_obs

    # ------------------------------------------------------------------
    # Stage 3B (live): Bearing observation from live detection + sync state
    # ------------------------------------------------------------------

    def _bearing_from_live_detection(
        self,
        detection,
        sync_state: "LiveSyncState",
        radar_lat: float,
        radar_lon: float,
        calibration: RadarBearingCalibration,
        waveform_bins: list | None = None,
        effective_sync_jitter_deg: float | None = None,
    ) -> "RadarBearingObservation | None":
        """Convert one live detection to a bearing observation.

        Uses the authoritative backend sync predictor so live localisation,
        burst-sync diagnostics, and period fitting share propagation/waveform
        timing semantics.  Calibration offset is the only localiser-specific
        addition after prediction.
        """
        if sync_state.period_s <= 0:
            return None

        range_nm = None
        if detection.truth_lat is not None and detection.truth_lon is not None:
            range_nm = _haversine_m(
                radar_lat, radar_lon, detection.truth_lat, detection.truth_lon,
            ) / 1852.0

        prediction = predict_sync_observation(
            sync_state,
            detection.arrival_us,
            range_nm=range_nm,
            waveform_bins=waveform_bins,
            bearing_rate_deg_s=getattr(detection, "bearing_rate_deg_s", None),
            motion_comp_dt_us=getattr(detection, "motion_comp_dt_us", None),
            motion_comp_block_reason=getattr(detection, "motion_comp_block_reason", None),
        )
        phase_deg = prediction.phase_in_rot_deg
        bearing_raw = prediction.predicted_bearing_deg
        bearing_obs = _wrap_deg(bearing_raw + calibration.bearing_offset_deg)
        bearing_obs = (bearing_obs + 360.0) % 360.0

        # Observation sigma: calibration residual + sync jitter + association penalty.
        # The effective sync jitter may be overridden locally (e.g. tightened by
        # a well-fitted calibration sigma) without mutating shared sync state.
        sigma = max(calibration.bearing_sigma_deg, 0.5)
        jitter = (
            effective_sync_jitter_deg
            if effective_sync_jitter_deg is not None
            else sync_state.sync_jitter_deg
        )
        sigma = math.sqrt(sigma ** 2 + jitter ** 2)
        if detection.position_age_seconds is not None:
            age_factor = 1.0 + detection.position_age_seconds / _MAX_POSITION_AGE_S
            sigma = sigma * age_factor
        # Association confidence penalty: unconfirmed targets get higher sigma
        if detection.association_confidence < 1.0:
            sigma = sigma / max(detection.association_confidence, 0.1)
        sigma = max(sigma, 0.5)

        assoc_conf = detection.association_confidence
        if detection.position_age_seconds is not None:
            assoc_conf = min(assoc_conf, max(0.0, 1.0 - detection.position_age_seconds / _MAX_POSITION_AGE_S))

        return RadarBearingObservation(
            iid=detection.iid,
            icao=detection.icao,
            arrival_us=detection.arrival_us,
            phase_deg=phase_deg,
            bearing_obs_deg=bearing_obs,
            bearing_sigma_deg=sigma,
            radar_lat=radar_lat,
            radar_lon=radar_lon,
            receiver_lat=detection.receiver_lat,
            receiver_lon=detection.receiver_lon,
            association_confidence=assoc_conf,
            burst_signal_dbfs=detection.signal_dbfs,
            altitude_ft=None,
        )

    def _build_live_observations_for_icao(
        self,
        icao: str,
        iid: int,
        sync_state: "LiveSyncState",
        radar_lat: float,
        radar_lon: float,
        calibration: RadarBearingCalibration,
        max_obs: int = 6,
        detection_retention_s: float = 30.0,
    ) -> list["RadarBearingObservation"]:
        """Build bearing observations from live detections for one ICAO and IID."""
        if not sync_state.usable:
            return []
        if calibration.quality == "none":
            return []

        detections = self._radar_state.get_recent_live_detections_for_icao(
            icao, max_age_s=detection_retention_s
        )
        detections = [d for d in detections if d.iid == iid]
        if not detections:
            return []

        obs_out: list[RadarBearingObservation] = []
        waveform_bins = self._radar_state.get_stage3_live_waveform_bins(iid)
        for det in detections:
            ob = self._bearing_from_live_detection(
                det, sync_state, radar_lat, radar_lon, calibration, waveform_bins,
            )
            if ob is not None:
                obs_out.append(ob)

        # Most recent first
        obs_out.sort(key=lambda o: o.arrival_us, reverse=True)
        return obs_out[:max_obs]

    def build_live_bearing_observations(
        self,
        icao: str,
        iid_subset: set[int] | None = None,
        max_obs_per_radar: int = 6,
        detection_retention_s: float = 30.0,
    ) -> list["RadarBearingObservation"]:
        """Build live bearing observations for a target across all calibrated radars.

        Legacy helper retained for callers that want the multi-candidate pool
        (not used by the operational solve path, which uses
        select_authoritative_observations()).  No shared sync state is mutated.
        """
        with self._lock:
            cals = dict(self._calibrations)

        all_obs: list[RadarBearingObservation] = []
        sync_states = self._radar_state.get_all_stage3_live_sync_states()

        for iid, auth, _ in self.get_eligible_iids():
            if iid_subset is not None and iid not in iid_subset:
                continue
            if iid not in cals:
                continue
            cal = cals[iid]
            if cal.quality == "none":
                continue
            sync_state = sync_states.get(iid)
            if sync_state is None or not sync_state.usable:
                continue

            obs = self._build_live_observations_for_icao(
                icao=icao,
                iid=iid,
                sync_state=sync_state,
                radar_lat=auth["lat"],
                radar_lon=auth["lon"],
                calibration=cal,
                max_obs=max_obs_per_radar,
                detection_retention_s=detection_retention_s,
            )
            all_obs.extend(obs)

        return all_obs

    # ------------------------------------------------------------------
    # Phase-trust and bearing-sanity helpers
    # ------------------------------------------------------------------

    @staticmethod
    def _sync_state_has_trusted_absolute_phase(sync_state) -> bool:
        """Return True iff sync_state carries a trustworthy absolute phase anchor."""
        if sync_state.source != "multi_aircraft_burst":
            return False
        if sync_state.phase_anchor_status not in {"selected", "anchor_only"}:
            return False
        if sync_state.phase_anchor_icao is None:
            return False
        if sync_state.phase_validation_status == "population_disagrees":
            return False
        if (sync_state.phase_anchor_spread_deg is not None
                and sync_state.phase_anchor_spread_deg > 25.0):
            return False
        if (sync_state.phase_validation_contributors > 0
                and sync_state.phase_validation_median_error_deg is not None
                and abs(sync_state.phase_validation_median_error_deg) > 20.0):
            return False
        return True

    @staticmethod
    def _bearing_matches_current_truth(
        obs: "RadarBearingObservation",
        detection,
        radar_lat: float,
        radar_lon: float,
    ) -> bool:
        """Return True iff the observed bearing is within 45° of the truth bearing.

        Only applied when the detection carries a fresh (≤3 s) ADS-B position.
        Returns True (pass) when truth is unavailable or stale so the gate is
        strictly fail-closed on confirmed bad bearings, not on unknowns.
        """
        if (detection.truth_lat is None or detection.truth_lon is None
                or detection.position_age_seconds is None
                or detection.position_age_seconds > 3.0):
            return True
        true_bearing = _bearing_deg(radar_lat, radar_lon, detection.truth_lat, detection.truth_lon)
        err = abs(_wrap_deg(obs.bearing_obs_deg - true_bearing))
        return err <= 45.0

    @staticmethod
    def _derive_global_rejection_reason(per_radar_reasons: dict) -> str:
        """Choose the most informative global rejection reason from per-radar reasons."""
        if not per_radar_reasons:
            return REASON_NO_ELIGIBLE_RADARS
        reasons = set(per_radar_reasons.values())
        if len(reasons) == 1:
            return next(iter(reasons))
        priority = [
            REASON_BEARING_TRUTH_MISMATCH,
            REASON_ABSOLUTE_PHASE_UNTRUSTED,
            REASON_STALE_OBSERVATION,
            REASON_SYNC_QUALITY_LOW,
            REASON_NO_SYNC,
            REASON_NO_CALIBRATION,
            REASON_NO_DETECTIONS,
            REASON_OBS_BUILD_FAILED,
        ]
        for reason in priority:
            if reason in reasons:
                return reason
        return REASON_NO_ELIGIBLE_RADARS

    # ------------------------------------------------------------------
    # Authoritative per-radar observation selection (new operational path)
    # ------------------------------------------------------------------

    def select_authoritative_observations(
        self,
        icao: str,
        iid_subset: set[int] | None,
        now_ts: float,
    ) -> dict:
        """Select exactly one live bearing observation per eligible radar.

        Rules:
          * A contributing radar must have an authoritative position, a usable
            live sync state, and a bearing calibration.
          * The chosen detection is the newest one whose wall-clock age does
            not exceed _MAX_OBS_AGE_S AND whose age does not exceed
            _MAX_OBS_AGE_PERIODS * period_s.
          * A stale-but-present detection is emitted as a rejected ray with
            rejection_reason set to REASON_STALE_OBSERVATION.
          * Radars that fail any gate are recorded in per_radar_reasons but
            contribute nothing to the accepted set.

        Returns:
            {
              "accepted":   list[RadarBearingObservation],   # one per radar
              "rejected_rays": list[Stage3LiveRay],          # with reason
              "per_radar_reasons": dict[int, str],
            }
        """
        with self._lock:
            cals = dict(self._calibrations)

        sync_states = self._radar_state.get_all_stage3_live_sync_states()
        all_sync_states = self._radar_state.get_all_live_sync_states()
        eligible = self.get_eligible_iids()

        accepted: list[RadarBearingObservation] = []
        rejected_rays: list[Stage3LiveRay] = []
        per_radar_reasons: dict[int, str] = {}

        for iid, auth, _model in eligible:
            if iid_subset is not None and iid not in iid_subset:
                continue

            cal = cals.get(iid)
            if cal is None or cal.quality == "none":
                per_radar_reasons[iid] = REASON_NO_CALIBRATION
                continue

            sync_state = sync_states.get(iid)
            if sync_state is None:
                raw_sync_state = all_sync_states.get(iid)
                if raw_sync_state is None or raw_sync_state.period_s <= 0:
                    per_radar_reasons[iid] = REASON_NO_SYNC
                elif not raw_sync_state.usable:
                    per_radar_reasons[iid] = REASON_SYNC_QUALITY_LOW
                else:
                    per_radar_reasons[iid] = REASON_ABSOLUTE_PHASE_UNTRUSTED
                continue
            if not sync_state.usable:
                per_radar_reasons[iid] = REASON_SYNC_QUALITY_LOW
                continue
            if sync_state.period_s <= 0:
                per_radar_reasons[iid] = REASON_NO_SYNC
                continue

            if not self._sync_state_has_trusted_absolute_phase(sync_state):
                per_radar_reasons[iid] = REASON_ABSOLUTE_PHASE_UNTRUSTED
                continue

            # Pull all recent detections for this ICAO from the shared buffer.
            detections = self._radar_state.get_recent_live_detections_for_icao(
                icao, max_age_s=self._ray_retention_s,
            )
            detections = [d for d in detections if d.iid == iid]
            if not detections:
                per_radar_reasons[iid] = REASON_NO_DETECTIONS
                continue

            # detections are returned newest-first — select the newest that passes
            # both freshness gates (wall-clock and period-multiple).
            period_s = sync_state.period_s
            max_age_periods_s = _MAX_OBS_AGE_PERIODS * period_s
            effective_gate = min(_MAX_OBS_AGE_S, max_age_periods_s)

            chosen = None
            for det in detections:
                age_s = now_ts - det.wall_ts
                if age_s <= effective_gate:
                    chosen = det
                    break

            # The observation sigma is computed inside _bearing_from_live_detection
            # as quadrature(calibration_sigma, sync_jitter).  We do not override
            # the jitter here: clamping it down to the calibration sigma would
            # systematically under-estimate uncertainty when sync jitter is the
            # dominant term.  Shared sync state is never mutated.
            waveform_bins = self._radar_state.get_stage3_live_waveform_bins(iid)

            if chosen is None:
                per_radar_reasons[iid] = REASON_STALE_OBSERVATION
                # Emit a rejection ray from the newest stale detection so the
                # evidence layer can show operators which radars fell behind.
                stale = detections[0]
                stale_obs = self._bearing_from_live_detection(
                    stale, sync_state, auth["lat"], auth["lon"], cal, waveform_bins,
                )
                if stale_obs is not None:
                    rejected_rays.append(self._obs_to_live_ray(
                        stale_obs, icao, now_ts,
                        accepted=False, rejection_reason=REASON_STALE_OBSERVATION,
                    ))
                continue

            obs = self._bearing_from_live_detection(
                chosen, sync_state, auth["lat"], auth["lon"], cal, waveform_bins,
            )
            if obs is None:
                per_radar_reasons[iid] = REASON_OBS_BUILD_FAILED
                continue

            if not self._bearing_matches_current_truth(obs, chosen, auth["lat"], auth["lon"]):
                per_radar_reasons[iid] = REASON_BEARING_TRUTH_MISMATCH
                rejected_rays.append(self._obs_to_live_ray(
                    obs, icao, now_ts,
                    accepted=False, rejection_reason=REASON_BEARING_TRUTH_MISMATCH,
                ))
                continue

            accepted.append(obs)

        return {
            "accepted": accepted,
            "rejected_rays": rejected_rays,
            "per_radar_reasons": per_radar_reasons,
        }

    @staticmethod
    def _obs_to_live_ray(
        obs: "RadarBearingObservation",
        icao: str,
        ts: float,
        *,
        accepted: bool,
        rejection_reason: str | None = None,
    ) -> Stage3LiveRay:
        """Wrap one observation as a display-layer Stage3LiveRay."""
        return Stage3LiveRay(
            track_id=icao,
            icao=icao,
            iid=obs.iid,
            ts=ts,
            radar_lat=obs.radar_lat,
            radar_lon=obs.radar_lon,
            bearing_deg=obs.bearing_obs_deg,
            bearing_sigma_deg=obs.bearing_sigma_deg,
            accepted=accepted,
            rejection_reason=rejection_reason,
            arrival_us=obs.arrival_us,
            association_confidence=obs.association_confidence,
        )

    # ------------------------------------------------------------------
    # Stage 3: Combined solve for one target
    # ------------------------------------------------------------------

    def _solve_for_icao(
        self,
        icao: str,
        iid_subset: set[int] | None = None,
        max_cep_m: float | None = None,
    ) -> AircraftFix | None:
        """Run the full per-aircraft localisation pipeline.

        Selects exactly one authoritative live bearing observation per
        contributing radar, publishes that identical set as both the
        "Bearing Rays" display layer and the solver input, and records a
        structured rejection reason if any stage fails.
        """
        max_cep_m = max_cep_m or self._max_cep_m
        now_ts = time.time()

        selection = self.select_authoritative_observations(icao, iid_subset, now_ts)
        accepted_obs: list[RadarBearingObservation] = selection["accepted"]
        rejected_rays: list[Stage3LiveRay] = selection["rejected_rays"]
        per_radar_reasons: dict[int, str] = selection["per_radar_reasons"]

        # Build the authoritative display rays.  The accepted set is also the
        # solver input; the two paths cannot diverge by construction.
        accepted_rays = [
            self._obs_to_live_ray(o, icao, now_ts, accepted=True)
            for o in accepted_obs
        ]

        reasons = {
            "ts": now_ts,
            "per_radar": per_radar_reasons,
            "global": None,
        }

        # Publish current-cycle evidence buffers under the same lock used by
        # build_evidence() so readers never see torn state mid-update.
        with self._lock:
            self._current_rays_by_icao[icao] = accepted_rays
            self._current_rejected_rays_by_icao[icao] = rejected_rays
            self._last_rejection_reasons_by_icao[icao] = reasons
            if accepted_rays:
                hist_buf = self._historical_rays_by_icao.setdefault(
                    icao, deque(maxlen=self._RAY_BUFFER_MAX),
                )
                hist_buf.extend(accepted_rays)

        if not accepted_obs:
            reasons["global"] = self._derive_global_rejection_reason(per_radar_reasons)
            return None

        if len({o.iid for o in accepted_obs}) < self._min_radars_for_fix:
            reasons["global"] = REASON_INSUFFICIENT_RADARS
            return None

        seeds = generate_seeds(accepted_obs)
        if not seeds:
            reasons["global"] = REASON_NO_FORWARD_INTERSECTIONS
            return None

        seed_lat, seed_lon = seeds[0]
        with self._lock:
            seed_buf = self._recent_seed_points_by_icao.setdefault(
                icao, deque(maxlen=20),
            )
            seed_buf.append({"lat": seed_lat, "lon": seed_lon, "ts": now_ts})
            cals = dict(self._calibrations)

        fix = solve_snapshot(accepted_obs, cals, seed_lat, seed_lon)
        if fix is None:
            reasons["global"] = REASON_SOLVE_FAILED
            return None

        if fix.cep_m > max_cep_m:
            reasons["global"] = REASON_EXCESSIVE_UNCERTAINTY
            log.debug(
                "Stage3: fix for %s rejected — CEP %.0f m > limit %.0f m",
                icao, fix.cep_m, max_cep_m,
            )
            return None
        if fix.geometry_score < 0.05:
            reasons["global"] = REASON_POOR_GEOMETRY
            return None

        return fix

    # ------------------------------------------------------------------
    # Stage 3E: Track filter
    # ------------------------------------------------------------------

    def _expire_old_tracks(self, now: float) -> None:
        expired = [tid for tid, t in self._tracks.items()
                   if now - t.last_update_ts > _TRACK_TIMEOUT_S]
        for tid in expired:
            del self._tracks[tid]

    # ------------------------------------------------------------------
    # Main localisation cycle
    # ------------------------------------------------------------------

    def run_localisation_cycle(
        self,
        target_icaos: list[str],
        max_targets: int = 20,
        iid_subset: set[int] | None = None,
    ) -> list[AircraftFix]:
        """Solve aircraft positions for a list of ICAOs.

        Returns the list of publishable fixes.
        """
        t0 = time.time()
        fixes: list[AircraftFix] = []
        now = time.time()

        with self._lock:
            self._expire_old_tracks(now)

        for icao in target_icaos[:max_targets]:
            fix = self._solve_for_icao(icao, iid_subset)
            if fix is None:
                continue

            fixes.append(fix)

            with self._lock:
                if icao in self._tracks:
                    self._tracks[icao] = _update_track(self._tracks[icao], fix, now)
                else:
                    self._tracks[icao] = _new_track(fix, now)

        elapsed = time.time() - t0
        self._last_localisation_ts = time.time()
        self._n_localisation_cycles += 1
        self._timing_history.append({
            "type": "localisation",
            "elapsed_s": elapsed,
            "n_targets": len(target_icaos),
            "n_fixes": len(fixes),
            "ts": t0,
        })
        return fixes

    # ------------------------------------------------------------------
    # State accessors
    # ------------------------------------------------------------------

    def get_status(self) -> dict:
        with self._lock:
            cals = dict(self._calibrations)
            tracks = dict(self._tracks)
        n_calibrated = sum(1 for c in cals.values() if c.quality != "none")
        return {
            "n_calibrated_radars": n_calibrated,
            "n_calibrations_total": len(cals),
            "n_active_tracks": len(tracks),
            "n_calibration_cycles": self._n_calibration_cycles,
            "n_localisation_cycles": self._n_localisation_cycles,
            "last_calibration_ts": self._last_calibration_ts or None,
            "last_localisation_ts": self._last_localisation_ts or None,
            "backlog_skips": self._backlog_skips,
        }

    def get_radar_readiness(self) -> list[dict]:
        with self._lock:
            cals = dict(self._calibrations)
        out = []
        for iid, auth, iid_model in self.get_eligible_iids():
            cal = cals.get(iid)
            out.append({
                "iid": iid,
                "auth_source": auth["source"],
                "radar_lat": auth["lat"],
                "radar_lon": auth["lon"],
                "radar_cep_m": auth.get("cep_m"),
                "period_s": iid_model.period_s,
                "calibration_quality": cal.quality if cal else "none",
                "bearing_offset_deg": cal.bearing_offset_deg if cal else None,
                "bearing_sigma_deg": cal.bearing_sigma_deg if cal else None,
                "n_calibration_samples": cal.n_samples if cal else 0,
                "last_calibrated_ts": cal.last_calibrated_ts if cal else None,
                "stage3_eligible": cal is not None and cal.quality != "none",
            })
        return out

    def get_tracks(self) -> list[dict]:
        with self._lock:
            tracks = list(self._tracks.values())
        return [self._track_to_dict(t) for t in tracks]

    def get_track(self, track_id: str) -> dict | None:
        with self._lock:
            t = self._tracks.get(track_id)
        return self._track_to_dict(t) if t else None

    def reset_tracks(self) -> None:
        with self._lock:
            self._tracks.clear()
        log.info("Stage3: runtime tracks cleared")

    def reset_calibrations(self) -> None:
        with self._lock:
            self._calibrations.clear()
        log.info("Stage3: runtime calibrations cleared")

    @staticmethod
    def _track_to_dict(t: AircraftTrackState) -> dict:
        latest = t.history[-1] if t.history else None
        return {
            "track_id": t.track_id,
            "lat": t.lat,
            "lon": t.lon,
            "vx_mps": t.vx_mps,
            "vy_mps": t.vy_mps,
            "alt_ft": t.alt_ft,
            "last_update_ts": t.last_update_ts,
            "source": t.source,
            "cep_m": latest.cep_m if latest else None,
            "geometry_score": latest.geometry_score if latest else None,
            "n_radars": latest.n_radars if latest else None,
            "solver_status": latest.solver_status if latest else None,
            "n_history": len(t.history),
        }

    # ------------------------------------------------------------------
    # Evidence layer builder
    # ------------------------------------------------------------------

    def build_evidence(
        self,
        icao: str,
        iid_subset: set[int] | None = None,
    ) -> dict:
        """Build evidence layers for the Stage 3 map page.

        Reads from live ray/seed buffers (populated by _solve_for_icao), not
        from sweep frames. Returns the same envelope structure as the radar
        evidence API: {available, reason, layers}.
        Each layer: {method, label, geometry_type, source_count, active_estimate, features}
        """
        with self._lock:
            cals = dict(self._calibrations)
            track = self._tracks.get(icao)
            # Snapshot evidence buffers under the same lock for consistency.
            current_rays: list[Stage3LiveRay] = list(
                self._current_rays_by_icao.get(icao, [])
            )
            current_rejected: list[Stage3LiveRay] = list(
                self._current_rejected_rays_by_icao.get(icao, [])
            )
            historical_rays: list[Stage3LiveRay] = list(
                self._historical_rays_by_icao.get(icao, [])
            )
            recent_seeds: list[dict] = list(self._recent_seed_points_by_icao.get(icao, []))
            rejection_reasons: dict = dict(
                self._last_rejection_reasons_by_icao.get(icao, {})
            )

        # Filter history + seeds to retention window (current-cycle buffers
        # are already from the latest solve cycle, so no time filter).
        cutoff = time.time() - self._ray_retention_s
        historical_rays = [r for r in historical_rays if r.ts >= cutoff]
        recent_seeds = [s for s in recent_seeds if s["ts"] >= cutoff]

        # Apply IID filter.
        if iid_subset:
            current_rays = [r for r in current_rays if r.iid in iid_subset]
            current_rejected = [r for r in current_rejected if r.iid in iid_subset]
            historical_rays = [r for r in historical_rays if r.iid in iid_subset]

        unique_iids = (
            {r.iid for r in current_rays}
            | {r.iid for r in current_rejected}
        )

        if not current_rays and not current_rejected and track is None:
            return {
                "available": False,
                "reason": "no_observations",
                "layers": [],
                "rejection_reasons": rejection_reasons,
            }

        layers = []

        # --- Layer: Radar Origins ---
        radar_features = []
        for iid, auth, _ in self.get_eligible_iids():
            if iid_subset and iid not in iid_subset:
                continue
            cal = cals.get(iid)
            sync = self._radar_state.get_live_sync_state(iid)
            radar_features.append({
                "type": "Feature",
                "geometry": {"type": "Point", "coordinates": [auth["lon"], auth["lat"]]},
                "properties": {
                    "iid": iid,
                    "label": f"IID {iid}",
                    "role": "radar_origin",
                    "quality": cal.quality if cal else "none",
                    "has_obs": iid in unique_iids,
                    "sync_quality": sync.sync_quality if sync else None,
                    "sync_usable": sync.usable if sync else False,
                    "sync_source": sync.source if sync else None,
                    # Residual-tracking diagnostics
                    "sync_jitter_deg": sync.sync_jitter_deg if sync else None,
                    "residual_ema_deg": sync.residual_ema_deg if sync else None,
                    "last_residual_deg": sync.last_residual_deg if sync else None,
                    "n_sync_frames": sync.n_sync_frames if sync else None,
                    "n_rejected_frames": sync.n_rejected_frames if sync else None,
                    "sync_holdover": sync.holdover if sync else None,
                    # Multi-aircraft burst-sync diagnostics (populated when source == "multi_aircraft_burst")
                    "n_burst_obs_inliers": getattr(sync, "n_burst_obs_inliers", None) if sync else None,
                    "n_burst_obs_rejected": getattr(sync, "n_burst_obs_rejected", None) if sync else None,
                    "contributing_icao_count": getattr(sync, "contributing_icao_count", None) if sync else None,
                },
            })
        layers.append({
            "method": "radar_origins",
            "label": "Radar Origins",
            "geometry_type": "point",
            "source_count": len(radar_features),
            "active_estimate": None,
            "features": radar_features,
        })

        # --- Layer: Bearing Rays (accepted, from live buffer) ---
        def _ray_to_feature(ray: Stage3LiveRay, role: str) -> dict:
            dist_m = 400_000.0
            bearing_rad = math.radians(ray.bearing_deg)
            dlon_scale = math.cos(math.radians(ray.radar_lat))
            end_lat = ray.radar_lat + math.degrees(dist_m * math.cos(bearing_rad) / _R_EARTH_M)
            end_lon = ray.radar_lon + math.degrees(dist_m * math.sin(bearing_rad) / (_R_EARTH_M * max(dlon_scale, 1e-6)))
            return {
                "type": "Feature",
                "geometry": {
                    "type": "LineString",
                    "coordinates": [[ray.radar_lon, ray.radar_lat], [end_lon, end_lat]],
                },
                "properties": {
                    "iid": ray.iid,
                    "bearing_deg": ray.bearing_deg,
                    "bearing_sigma_deg": ray.bearing_sigma_deg,
                    "role": role,
                    "ts": ray.ts,
                },
            }

        # Primary "Bearing Rays" layer: exactly the authoritative live rays
        # from the most recent solve cycle, one per contributing radar.  By
        # construction this matches the solver's input set.
        ray_features = [_ray_to_feature(r, "bearing_ray") for r in current_rays]
        layers.append({
            "method": "bearing_rays",
            "label": "Bearing Rays",
            "geometry_type": "line",
            "source_count": len(ray_features),
            "active_estimate": None,
            "features": ray_features,
        })

        # Rejected rays (current cycle only).  Each ray carries its rejection
        # reason so the map can colour-code why each one failed the gates.
        rejected_features = []
        for r in current_rejected:
            feat = _ray_to_feature(r, "rejected_ray")
            feat["properties"]["rejection_reason"] = r.rejection_reason
            rejected_features.append(feat)
        layers.append({
            "method": "rejected_rays",
            "label": "Rejected Rays",
            "geometry_type": "line",
            "source_count": len(rejected_features),
            "active_estimate": None,
            "features": rejected_features,
        })

        # Separate historical ray layer (older accepted rays) — clearly named
        # so operators never confuse it with the current live ray set.
        current_ids = {(r.iid, r.arrival_us) for r in current_rays}
        historical_only = [
            r for r in historical_rays
            if (r.iid, r.arrival_us) not in current_ids
        ]
        historical_features = [
            _ray_to_feature(r, "historical_ray") for r in historical_only
        ]
        layers.append({
            "method": "historical_bearing_rays",
            "label": "Historical Bearing Rays",
            "geometry_type": "line",
            "source_count": len(historical_features),
            "active_estimate": None,
            "features": historical_features,
        })

        # --- Layer: Seed Intersections (from live buffer) ---
        seed_features = [
            {
                "type": "Feature",
                "geometry": {"type": "Point", "coordinates": [s["lon"], s["lat"]]},
                "properties": {"role": "seed_intersection", "label": "Seed", "ts": s["ts"]},
            }
            for s in recent_seeds
        ]
        layers.append({
            "method": "seed_intersections",
            "label": "Seed Intersections",
            "geometry_type": "point",
            "source_count": len(seed_features),
            "active_estimate": None,
            "features": seed_features,
        })

        # --- Layer: Selected Aircraft Fix ---
        fix_features = []
        if track:
            latest_fix = track.history[-1] if track.history else None
            fix_features.append({
                "type": "Feature",
                "geometry": {"type": "Point", "coordinates": [track.lon, track.lat]},
                "properties": {
                    "role": "selected_fix",
                    "label": icao,
                    "cep_m": latest_fix.cep_m if latest_fix else None,
                    "geometry_score": latest_fix.geometry_score if latest_fix else None,
                    "n_radars": latest_fix.n_radars if latest_fix else None,
                    "solver_status": latest_fix.solver_status if latest_fix else None,
                },
            })
        layers.append({
            "method": "selected_fix",
            "label": "Selected Aircraft Fix",
            "geometry_type": "point",
            "source_count": len(fix_features),
            "active_estimate": {"lat": track.lat, "lon": track.lon} if track else None,
            "features": fix_features,
        })

        # --- Layer: Fix History ---
        # Expose recent localiser fixes from the track history as both a
        # LineString and a set of points so the map can render either.
        history_features: list[dict] = []
        if track and track.history:
            coords = [[f.lon, f.lat] for f in track.history]
            if len(coords) >= 2:
                history_features.append({
                    "type": "Feature",
                    "geometry": {"type": "LineString", "coordinates": coords},
                    "properties": {
                        "role": "fix_history_track",
                        "n_points": len(coords),
                    },
                })
            for f in track.history[-10:]:
                history_features.append({
                    "type": "Feature",
                    "geometry": {"type": "Point", "coordinates": [f.lon, f.lat]},
                    "properties": {
                        "role": "fix_history_point",
                        "ts": f.ts,
                        "cep_m": f.cep_m,
                        "geometry_score": f.geometry_score,
                        "n_radars": f.n_radars,
                    },
                })
        layers.append({
            "method": "fix_history",
            "label": "Fix History",
            "geometry_type": "mixed",
            "source_count": len(history_features),
            "active_estimate": None,
            "features": history_features,
        })

        # --- Layer: Fix Uncertainty (CEP circle) ---
        uncertainty_features = []
        if track and track.history:
            latest_fix = track.history[-1]
            cep_m = latest_fix.cep_m
            if math.isfinite(cep_m):
                cep_km = cep_m / 1000.0
                uncertainty_features.append({
                    "type": "Feature",
                    "geometry": {"type": "Point", "coordinates": [track.lon, track.lat]},
                    "properties": {
                        "role": "cep_circle",
                        "radius_km": cep_km,
                        "label": f"CEP {cep_m:.0f} m",
                    },
                })
        layers.append({
            "method": "fix_uncertainty",
            "label": "Fix Uncertainty",
            "geometry_type": "circle",
            "source_count": len(uncertainty_features),
            "active_estimate": None,
            "features": uncertainty_features,
        })

        # Prefer n_radars from the latest fix if available; fall back to unique IIDs in evidence.
        latest_fix = (track.history[-1] if track and track.history else None)
        n_radars_out = latest_fix.n_radars if latest_fix is not None else len(unique_iids)

        # Build per-IID sync summary for the evidence envelope.
        sync_summary = {}
        for iid_key in unique_iids:
            sync = self._radar_state.get_live_sync_state(iid_key)
            if sync is not None:
                sync_summary[str(iid_key)] = {
                    "sync_source": sync.source,
                    "sync_quality": sync.sync_quality,
                    "sync_usable": sync.usable,
                    "sync_jitter_deg": sync.sync_jitter_deg,
                    "holdover": sync.holdover,
                    "n_sync_frames": sync.n_sync_frames,
                    "n_rejected_frames": sync.n_rejected_frames,
                    "contributing_icao_count": getattr(sync, "contributing_icao_count", 0),
                    "n_burst_obs_inliers": getattr(sync, "n_burst_obs_inliers", 0),
                    "n_burst_obs_rejected": getattr(sync, "n_burst_obs_rejected", 0),
                }

        return {
            "available": True,
            "reason": "ok",
            "layers": layers,
            "icao": icao,
            "n_observations": len(current_rays),
            "n_radars": n_radars_out,
            "sync_diagnostics": sync_summary,
            "rejection_reasons": rejection_reasons,
        }

    def get_sync_diagnostics(self, iid_subset: set[int] | None = None) -> dict:
        """Return per-IID multi-aircraft sync diagnostics for the verification endpoint.

        Provides the information needed to tell whether poor sync is due to bad
        burst-centre accuracy, weak aircraft positions, unstable period family
        membership, or true sync drift.  Intended for the alignment / verification
        visualisation path.
        """
        result: dict[int, dict] = {}
        for iid, auth, _ in self.get_eligible_iids():
            if iid_subset and iid not in iid_subset:
                continue
            sync = self._radar_state.get_live_sync_state(iid)
            if sync is None:
                result[iid] = {
                    "iid": iid,
                    "sync_available": False,
                    "sync_source": None,
                }
                continue
            result[iid] = {
                "iid": iid,
                "sync_available": True,
                "sync_source": sync.source,
                "period_s": sync.period_s,
                "phase_epoch_us": sync.phase_epoch_us,
                "phase_offset_deg": sync.phase_offset_deg,
                "sync_quality": sync.sync_quality,
                "sync_usable": sync.usable,
                "sync_jitter_deg": sync.sync_jitter_deg,
                "residual_ema_deg": sync.residual_ema_deg,
                "last_residual_deg": sync.last_residual_deg,
                "n_sync_frames": sync.n_sync_frames,
                "n_rejected_frames": sync.n_rejected_frames,
                "holdover": sync.holdover,
                "last_sync_update_ts": sync.last_sync_update_ts,
                # Multi-aircraft burst-sync diagnostics
                "contributing_icao_count": getattr(sync, "contributing_icao_count", 0),
                "n_burst_obs_inliers": getattr(sync, "n_burst_obs_inliers", 0),
                "n_burst_obs_rejected": getattr(sync, "n_burst_obs_rejected", 0),
                "period_base_s": getattr(sync, "period_base_s", None),
                "residual_slope_deg_per_s": getattr(sync, "residual_slope_deg_per_s", None),
                "period_correction_ppm": getattr(sync, "period_correction_ppm", None),
                "period_update_term": getattr(sync, "period_update_term", None),
                "period_update_direction": getattr(sync, "period_update_direction", None),
                "period_update_applied": getattr(sync, "period_update_applied", None),
                "period_update_gain": getattr(sync, "period_update_gain", None),
                "period_refine_block_reason": getattr(sync, "period_refine_block_reason", None),
                "fit_time_basis": getattr(sync, "fit_time_basis", None),
                "fit_residual_basis": getattr(sync, "fit_residual_basis", None),
                "fit_total_observations": getattr(sync, "fit_total_observations", None),
                "fit_eligible_observations": getattr(sync, "fit_eligible_observations", None),
                "fit_rejected_observations": getattr(sync, "fit_rejected_observations", None),
                "fit_reject_reasons": getattr(sync, "fit_reject_reasons", None),
                "fit_contributing_icao_count": getattr(sync, "fit_contributing_icao_count", None),
                "fit_span_s": getattr(sync, "fit_span_s", None),
                "predictor_consistency": getattr(sync, "predictor_consistency", None),
                "waveform_enabled": getattr(sync, "waveform_enabled", None),
                "waveform_applied": getattr(sync, "waveform_applied", None),
                "waveform_learning_enabled": getattr(sync, "waveform_learning_enabled", None),
                "waveform_update_block_reason": getattr(sync, "waveform_update_block_reason", None),
                "waveform_learning_residual_basis": getattr(sync, "waveform_learning_residual_basis", None),
            }
        return result
