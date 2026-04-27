"""Data models for the passive radar positioning module."""

from dataclasses import dataclass, field
from typing import Optional


@dataclass
class SweepFrameObservation:
    """One non-reference aircraft observation within a SweepFrame."""
    icao: str
    lat: float
    lon: float
    arrival_us: float        # Beast-relative burst centroid timestamp (microseconds, fractional allowed)
    signal_dbfs: Optional[float] = None
    interpolated: bool = False
    n_replies: int = 1       # Number of Mode S replies in the burst centroid
    position_age_seconds: float = 0.0  # Seconds since the ADS-B position fix was received


@dataclass
class SweepFrame:
    """One radar sweep's worth of phase-difference observations.

    The reference aircraft defines phase = 0 for this sweep.
    All other aircraft have an observed phase relative to the reference,
    computed as (arrival_us - ref_arrival_us) / T_sweep * 360 degrees.
    """
    frame_index: int                        # Sequential frame number within the accumulation window
    sweep_start_us: float                   # Reference aircraft burst timestamp
    ref_icao: str
    ref_lat: float
    ref_lon: float
    ref_arrival_us: float
    observations: list[SweepFrameObservation] = field(default_factory=list)
    quality: str = "insufficient"           # "good" (>=4), "marginal" (3), "insufficient" (<3)
    period_s: Optional[float] = None        # Sweep period used for phase computation


@dataclass
class ReferenceAircraftInfo:
    """Current reference aircraft selection state for an IID."""
    ref_icao: Optional[str] = None
    ref_score: Optional[float] = None       # Combined score (period adherence + tightness + count)
    ref_since_sweep: int = 0                # Frame index when this reference was selected
    hysteresis_margin: float = 0.25         # 25% — challenger must beat by this margin
    # Challenger rankings from latest sweep: [{icao, score, ratio_to_ref, status}]
    challengers: list[dict] = field(default_factory=list)


@dataclass
class RotationModel:
    """Per-IID rotation model derived from burst-interval analysis."""
    dominant_period_s: Optional[float] = None
    secondary_period_s: Optional[float] = None
    primary_direct_count: int = 0
    secondary_direct_count: int = 0
    period_std_s: float = 0.0
    # SINGLE_RADAR, LIKELY_SINGLE, MULTI_RADAR, CHECK_MULTI, INSUFFICIENT_DATA
    status: str = "INSUFFICIENT_DATA"
    n_qualifying: int = 0
    n_harmonic: int = 0
    n_residual: int = 0
    rpm: Optional[float] = None
    # {icao: {raw_period_s, multiplier, folded_period_s, detection_rate, method, snap_info}}
    folded: dict = field(default_factory=dict)
    # Secondary family peeled from the residual ICAOs, if one exists.
    secondary_folded: dict = field(default_factory=dict)
    # {icao: float} — periods that did not fold into the dominant
    residual: dict = field(default_factory=dict)
    last_updated: Optional[float] = None


@dataclass
class LiveFrameState:
    """State for a SweepFrame being built in real time as DF11s arrive."""
    ref_icao: str
    ref_lat: float
    ref_lon: float
    ref_arrival_us: float  # t=0 for this frame
    observations: list = field(default_factory=list)  # list of SweepFrameObservation
    seen_icaos: set[str] = field(default_factory=set)
    n_aircraft_seen: int = 1  # reference counts
    ref_pos_age_s: float = 0.0  # position age of ref aircraft at frame start (for sync gate)


@dataclass
class RadarIID:
    """Persistent model for one interrogator identifier (IID)."""
    iid: int
    # Localisation control state: auto search, operator-locked position, or
    # operator-locked unresolvable.
    resolution_mode: str = "auto"
    # Reinforced long-term classification: UNKNOWN, INSUFFICIENT_DATA, SINGLE_RADAR,
    # LIKELY_SINGLE, MULTI_RADAR, CHECK_MULTI
    status: str = "UNKNOWN"
    period_s: Optional[float] = None
    secondary_period_s: Optional[float] = None
    period_std_s: Optional[float] = None
    rpm: Optional[float] = None
    # Stage 2 localisation results (TDOA)
    lat: Optional[float] = None
    lon: Optional[float] = None
    cep_m: Optional[float] = None
    n_pairs: int = 0
    last_updated: float = 0.0
    multi_radar_flag: bool = False
    primary_support_count: int = 0
    secondary_support_count: int = 0
    rotation_model: Optional[RotationModel] = None
    # [{ts, lat, lon, cep_m}] — TDOA convergence history (runtime only)
    convergence_history: list = field(default_factory=list)
    # Runtime-only last manual TDOA run result/diagnostics.
    tdoa_last_run: Optional[dict] = None

    # Forward model localisation results (parallel to TDOA, stored separately)
    fm_lat: Optional[float] = None
    fm_lon: Optional[float] = None
    fm_cep_m: Optional[float] = None
    fm_source: Optional[str] = None  # "airport_prior" or "grid_search"
    # Coincident-illumination localisation results
    ci_lat: Optional[float] = None
    ci_lon: Optional[float] = None
    ci_cep_m: Optional[float] = None
    ci_source: Optional[str] = None
    ci_n_pairs: int = 0
    ci_last_updated: Optional[float] = None
    # Runtime-only (not persisted to DB):
    fm_n_observations: int = 0
    fm_window_s: float = 0.0
    # [{airport_icao, name, lat, lon, score, residual_sigma_ms, n_aircraft,
    #   directional_signal, ts}] — regenerated on demand
    airport_hypothesis: list = field(default_factory=list)
    # [{ts, lat, lon, cep_m, n_observations, source}] — runtime only
    fm_convergence_history: list = field(default_factory=list)
    fm_coincident_validation: Optional[dict] = None
    fm_last_run: Optional[dict] = None

    # Reference aircraft selection state (runtime only, not persisted to DB)
    reference_aircraft: Optional["ReferenceAircraftInfo"] = None

    # Manual reference aircraft override (runtime only, not persisted to DB).
    # When set, the next FM run uses this ICAO as reference instead of auto-scoring.
    # Cleared after each FM run so subsequent runs re-evaluate unless re-set.
    reference_aircraft_override: Optional[str] = None

    # Operator-supplied authoritative location and notes.
    manual_lat: Optional[float] = None
    manual_lon: Optional[float] = None
    manual_note: Optional[str] = None
    manual_updated_ts: Optional[float] = None

    # Operator-supplied reason for suppressing localisation attempts.
    unresolvable_reason: Optional[str] = None
    unresolvable_updated_ts: Optional[float] = None

    # All accumulated SweepFrames for this IID (runtime only, rebuilt each FM pass)
    sweep_frames: list["SweepFrame"] = field(default_factory=list)


@dataclass
class BurstRecord:
    """Compact operational record emitted when a burst fires.

    Core fields are always populated immediately when the burst centroid is computed.
    Enrichment fields are optional and layered on after ADS-B position lookup.
    Retained per-IID for the operational window needed by rotation analysis,
    sync fitting, and TDOA pair generation — not as a visual/debug history.
    """
    # Core — set when burst fires
    iid: int
    icao: str
    centroid_us: float       # Weighted centroid of reply timestamps (µs, Beast monotonic)
    n_replies: int
    signal_dbfs: Optional[float] = None

    # Enrichment — set after ADS-B position lookup, if available
    lat: Optional[float] = None
    lon: Optional[float] = None
    bearing_deg: Optional[float] = None
    range_nm: Optional[float] = None
    pos_age_s: Optional[float] = None
    dominant_family: bool = False    # True if ICAO belongs to the dominant period family
    sync_eligible: bool = False      # True if this Python burst qualified for the live sync fit


@dataclass
class CalibrationPair:
    """One co-sweep TDOA observation between two ADS-B aircraft."""
    iid: int
    ts: float           # epoch (wall time of burst centroids)
    icao_a: str
    icao_b: str
    lat_a: float
    lon_a: float
    lat_b: float
    lon_b: float
    tdoa_us: float      # signed TDOA in microseconds (positive if A arrived first)
    receiver_lat: float
    receiver_lon: float
