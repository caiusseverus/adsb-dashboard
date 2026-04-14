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
)

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
# Authoritative radar position (mirrors radar/api.py logic without FastAPI deps)
# ---------------------------------------------------------------------------

def _authoritative_position_for_model(model) -> dict:
    """Return the best-available radar position for a RadarIID model."""
    if model is None:
        return {"source": "none", "lat": None, "lon": None, "cep_m": None}

    if model.resolution_mode == "locked_unresolvable":
        return {"source": "none", "lat": None, "lon": None, "cep_m": None}

    if (
        model.resolution_mode == "locked_position"
        and model.manual_lat is not None
        and model.manual_lon is not None
    ):
        return {
            "source": "manual",
            "lat": model.manual_lat,
            "lon": model.manual_lon,
            "cep_m": None,
        }

    # Auto: rank CI, FM, TDOA by CEP; prefer lowest CEP
    candidates: list[dict] = []
    if model.ci_lat is not None and model.ci_lon is not None:
        candidates.append({"source": "ci", "lat": model.ci_lat, "lon": model.ci_lon, "cep_m": model.ci_cep_m})
    if model.fm_lat is not None and model.fm_lon is not None:
        candidates.append({"source": "fm", "lat": model.fm_lat, "lon": model.fm_lon, "cep_m": model.fm_cep_m})
    if model.lat is not None and model.lon is not None and not model.multi_radar_flag:
        candidates.append({"source": "tdoa", "lat": model.lat, "lon": model.lon, "cep_m": model.cep_m})

    if not candidates:
        return {"source": "none", "lat": None, "lon": None, "cep_m": None}

    candidates.sort(key=lambda c: (c.get("cep_m") is None, c.get("cep_m") or float("inf")))
    return candidates[0]


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
    """Intersect two rays in ENU space. Returns (x, y) or None if near-parallel."""
    t1 = math.radians(90.0 - theta1_deg)  # bearing → trig angle
    t2 = math.radians(90.0 - theta2_deg)
    dx1, dy1 = math.cos(t1), math.sin(t1)
    dx2, dy2 = math.cos(t2), math.sin(t2)

    # Solve: (x1 + t*dx1, y1 + t*dy1) = (x2 + s*dx2, y2 + s*dy2)
    denom = dx1 * dy2 - dy1 * dx2
    if abs(denom) < 1e-9:
        return None

    t = ((x2 - x1) * dy2 - (y2 - y1) * dx2) / denom
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

    Returns AircraftFix on success, None if insufficient observations.
    """
    # Deduplicate: one observation per radar (most recent / best)
    by_iid: dict[int, RadarBearingObservation] = {}
    for obs in observations:
        if obs.iid not in by_iid:
            by_iid[obs.iid] = obs
        else:
            existing = by_iid[obs.iid]
            if obs.association_confidence > existing.association_confidence:
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

    # Estimate CEP from Jacobian
    cep_m = _estimate_cep(result.jac, len(obs_list))

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


def _estimate_cep(jac: np.ndarray | None, n_obs: int) -> float:
    """Estimate CEP50 in metres from the least-squares Jacobian."""
    if jac is None or n_obs < 3:
        return float("inf")
    try:
        J = np.array(jac)
        if J.shape[0] < 2 or J.shape[1] < 2:
            return float("inf")
        JTJ = J.T @ J
        cov = np.linalg.inv(JTJ)
        eigvals = np.linalg.eigvalsh(cov)
        eigvals = np.maximum(eigvals, 0.0)
        # CEP50 for 2D Gaussian ~ 1.1774 * sqrt(mean eigenvalue)
        return float(1.1774 * math.sqrt(float(np.mean(eigvals))))
    except Exception:
        return float("inf")


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
        covariance=[fix.cep_m ** 2, 0.0, 0.0, fix.cep_m ** 2],
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
        covariance=[fix.cep_m ** 2, 0.0, 0.0, fix.cep_m ** 2],
        last_update_ts=now,
        source="stage3",
        history=[fix],
    )


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
    # Stage 3: Combined solve for one target
    # ------------------------------------------------------------------

    def _solve_for_icao(
        self,
        icao: str,
        iid_subset: set[int] | None = None,
        max_cep_m: float | None = None,
    ) -> AircraftFix | None:
        """Full pipeline for one aircraft: build obs → seed → solve."""
        max_cep_m = max_cep_m or self._max_cep_m
        observations = self.build_bearing_observations(icao, iid_subset)

        with self._lock:
            cals = dict(self._calibrations)

        # Need at least 2 different radars
        unique_iids = {o.iid for o in observations}
        if len(unique_iids) < self._min_radars_for_fix:
            return None

        seeds = generate_seeds(observations)
        if not seeds:
            return None

        seed_lat, seed_lon = seeds[0]
        fix = solve_snapshot(observations, cals, seed_lat, seed_lon)
        if fix is None:
            return None

        # Quality gate
        if fix.solver_status not in ("ok",) and fix.cep_m > max_cep_m:
            return None
        if fix.cep_m > max_cep_m:
            log.debug("Stage3: fix for %s rejected — CEP %.0f m > limit %.0f m", icao, fix.cep_m, max_cep_m)
            return None
        if fix.geometry_score < 0.05:
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

        Returns the same envelope structure as the existing radar evidence API:
          {available, reason, layers}
        Each layer: {method, label, geometry_type, source_count, active_estimate, features}
        """
        with self._lock:
            cals = dict(self._calibrations)
            track = self._tracks.get(icao)

        observations = self.build_bearing_observations(icao, iid_subset)
        unique_iids = {o.iid for o in observations}

        if not observations:
            return {"available": False, "reason": "no_observations", "layers": []}

        layers = []

        # --- Layer: Radar Origins ---
        radar_features = []
        for iid, auth, _ in self.get_eligible_iids():
            if iid_subset and iid not in iid_subset:
                continue
            cal = cals.get(iid)
            radar_features.append({
                "type": "Feature",
                "geometry": {"type": "Point", "coordinates": [auth["lon"], auth["lat"]]},
                "properties": {
                    "iid": iid,
                    "label": f"IID {iid}",
                    "role": "radar_origin",
                    "quality": cal.quality if cal else "none",
                    "has_obs": iid in unique_iids,
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

        # --- Layer: Bearing Rays ---
        ray_features = []
        rejected_features = []
        for obs in observations:
            # Project ray 400 km
            bearing_rad = math.radians(obs.bearing_obs_deg)
            dist_m = 400_000.0
            dlat = math.degrees(dist_m / _R_EARTH_M * math.cos(math.radians(obs.bearing_obs_deg)))
            dlon_scale = math.cos(math.radians(obs.radar_lat))
            end_lat = obs.radar_lat + math.degrees(dist_m * math.cos(math.radians(obs.bearing_obs_deg)) / _R_EARTH_M)
            end_lon = obs.radar_lon + math.degrees(dist_m * math.sin(math.radians(obs.bearing_obs_deg)) / (_R_EARTH_M * dlon_scale))

            feature = {
                "type": "Feature",
                "geometry": {
                    "type": "LineString",
                    "coordinates": [
                        [obs.radar_lon, obs.radar_lat],
                        [end_lon, end_lat],
                    ],
                },
                "properties": {
                    "iid": obs.iid,
                    "bearing_obs_deg": obs.bearing_obs_deg,
                    "bearing_sigma_deg": obs.bearing_sigma_deg,
                    "association_confidence": obs.association_confidence,
                    "role": "bearing_ray",
                },
            }
            ray_features.append(feature)

        layers.append({
            "method": "bearing_rays",
            "label": "Bearing Rays",
            "geometry_type": "line",
            "source_count": len(ray_features),
            "active_estimate": None,
            "features": ray_features,
        })

        # --- Layer: Rejected Rays (placeholder) ---
        layers.append({
            "method": "rejected_rays",
            "label": "Rejected Rays",
            "geometry_type": "line",
            "source_count": 0,
            "active_estimate": None,
            "features": rejected_features,
        })

        # --- Layer: Seed Intersections ---
        seeds = generate_seeds(observations)
        seed_features = []
        for slat, slon in seeds:
            seed_features.append({
                "type": "Feature",
                "geometry": {"type": "Point", "coordinates": [slon, slat]},
                "properties": {"role": "seed_intersection", "label": "Seed"},
            })
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
            fix_features.append({
                "type": "Feature",
                "geometry": {"type": "Point", "coordinates": [track.lon, track.lat]},
                "properties": {
                    "role": "selected_fix",
                    "label": icao,
                    "cep_m": track.history[-1].cep_m if track.history else None,
                    "geometry_score": track.history[-1].geometry_score if track.history else None,
                    "n_radars": track.history[-1].n_radars if track.history else None,
                    "solver_status": track.history[-1].solver_status if track.history else None,
                },
            })
        layers.append({
            "method": "selected_fix",
            "label": "Selected Aircraft Fix",
            "geometry_type": "point",
            "source_count": len(fix_features),
            "active_estimate": track.lat if track else None,
            "features": fix_features,
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

        return {
            "available": True,
            "reason": "ok",
            "layers": layers,
            "icao": icao,
            "n_observations": len(observations),
            "n_radars": len(unique_iids),
        }
