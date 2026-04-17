"""
Stage 3 dataclasses: bearing calibration, observations, fixes, tracks.

These are consumed by aircraft_localiser.py and aircraft_api.py.
They must not be stored inside RadarIID or RadarState.
"""

from __future__ import annotations

from dataclasses import dataclass, field


@dataclass
class Stage3LiveDetection:
    """One live DF11 detection event usable by Stage 3 localisation."""
    iid: int
    icao: str | None
    arrival_us: float               # Beast-monotonic timestamp (microseconds)
    wall_ts: float                  # Wall-clock time for age filtering
    df: int                         # Downlink format (11 for squitter)
    signal_dbfs: float | None
    receiver_lat: float
    receiver_lon: float
    truth_lat: float | None = None
    truth_lon: float | None = None
    position_age_seconds: float | None = None
    association_confidence: float = 1.0
    bearing_rate_deg_s: float | None = None
    motion_comp_dt_us: float | None = None
    motion_comp_block_reason: str | None = None


@dataclass
class Stage3LiveRay:
    """One bearing ray emitted from a live detection."""
    track_id: str | None
    icao: str | None
    iid: int
    ts: float                       # Wall-clock time ray was emitted
    radar_lat: float
    radar_lon: float
    bearing_deg: float
    bearing_sigma_deg: float
    accepted: bool
    rejection_reason: str | None = None
    arrival_us: float = 0.0         # Beast-monotonic burst-centre timestamp (microseconds)
    association_confidence: float = 1.0  # Copied from the source observation for ranking


@dataclass
class RadarBearingCalibration:
    """Per-radar Stage 3 bearing calibration record, persisted in radar_bearing_calibration."""
    iid: int
    bearing_offset_deg: float       # mean angular offset (obs − true)
    effective_delay_us: float       # absorbs transponder turnaround + beam-centre bias
    bearing_sigma_deg: float        # residual spread (1-sigma)
    n_samples: int
    quality: str                    # "none" | "provisional" | "stable"
    last_calibrated_ts: float       # epoch seconds


@dataclass
class RadarBearingObservation:
    """One bearing-like measurement from a single radar for a single target aircraft."""
    iid: int
    icao: str | None
    arrival_us: float               # Beast-monotonic burst centre (microseconds)
    phase_deg: float                # (arrival_us - ref_arrival_us) / period_us * 360
    bearing_obs_deg: float          # calibrated bearing: phase_deg + bearing_offset_deg
    bearing_sigma_deg: float        # per-observation uncertainty
    radar_lat: float
    radar_lon: float
    receiver_lat: float
    receiver_lon: float
    association_confidence: float
    burst_signal_dbfs: float | None = None
    altitude_ft: float | None = None


@dataclass
class AircraftFix:
    """One snapshot position estimate produced by the Stage 3 solver."""
    track_id: str
    lat: float
    lon: float
    alt_ft: float | None
    cep_m: float                    # circular error probable (50th percentile estimate)
    geometry_score: float           # 0–1, higher = better radar geometry
    n_radars: int
    n_observations: int
    solver_status: str              # "ok" | failure reason code
    solver_detail: dict
    ts: float                       # epoch seconds when the fix was produced


@dataclass
class AircraftTrackState:
    """Runtime-only short-horizon track state for one aircraft target."""
    track_id: str
    lat: float
    lon: float
    vx_mps: float                   # east velocity (m/s)
    vy_mps: float                   # north velocity (m/s)
    alt_ft: float | None
    position_covariance: list       # 2×2 flattened position covariance [var_x, cov_xy, cov_xy, var_y]
    last_update_ts: float
    source: str                     # "stage3" or "adsb_seed"
    history: list[AircraftFix] = field(default_factory=list)
