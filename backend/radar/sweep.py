"""
radar/sweep.py — DF11 IID event accumulation and rotation model analysis.

Consumes raw Beast frames, extracts DF11 IID events, and runs periodic
burst/period analysis to build per-IID rotation models.

Ported from tools/radar_iid_probe.py with adaptations for continuous
in-process operation.
"""

from __future__ import annotations

import logging
import statistics
import threading
import time
from collections import defaultdict, deque
from dataclasses import dataclass as _dataclass, field as _field
from typing import TYPE_CHECKING, Optional, Any

try:
    import decode_cffi as _decode_cffi
except Exception:
    _decode_cffi = None

from .models import RadarIID, RotationModel, CalibrationPair, BurstRecord
from .aircraft_models import Stage3LiveDetection

try:
    from config import (
        RADAR_SYNC_PERIOD_REFINE_ENABLED,
        RADAR_SYNC_WAVEFORM_ENABLED,
        RADAR_SYNC_PROP_DELAY_ENABLED,
        RADAR_SYNC_MOTION_COMP_PHASE_ENABLED,
        RADAR_SYNC_MOTION_COMP_FIT_ENABLED,
        RADAR_SYNC_WAVEFORM_BIN_COUNT,
        RADAR_DIAGNOSTICS,
    )
except Exception:  # pragma: no cover — config not importable in some test harnesses
    RADAR_SYNC_PERIOD_REFINE_ENABLED = True
    RADAR_SYNC_WAVEFORM_ENABLED = True
    RADAR_SYNC_PROP_DELAY_ENABLED = True
    RADAR_SYNC_MOTION_COMP_PHASE_ENABLED = True
    RADAR_SYNC_MOTION_COMP_FIT_ENABLED = True
    RADAR_SYNC_WAVEFORM_BIN_COUNT = 24
    RADAR_DIAGNOSTICS = False

if TYPE_CHECKING:
    from aircraft_state import AircraftState

log = logging.getLogger(__name__)

_perf_lock = threading.Lock()
df11_event_timings: deque[float] = deque(maxlen=4000)
df11_builder_timings: deque[float] = deque(maxlen=4000)
df11_batch_phase_timings: deque[dict] = deque(maxlen=400)
rotation_update_timings: deque[dict] = deque(maxlen=240)

_FIRED_BURST_PHASE_KEYS = (
    "fired_burst_count",
    "reference_select_count",
    "reference_reuse_count",
    "reference_rescore_count",
    "position_lookup_count",
    "dominant_check_count",
    "phase_check_count",
    "frame_start_count",
    "observation_count",
    "frame_finalized_count",
    "fm_callback_count",
    "setup_ms",
    "centroid_ms",
    "finalize_ms",
    "reference_select_ms",
    "position_lookup_ms",
    "dominant_check_ms",
    "suppression_ms",
    "phase_check_ms",
    "frame_mutation_ms",
    "fm_callback_ms",
)


def record_df11_event_timing(total_s: float, builder_s: float) -> None:
    with _perf_lock:
        df11_event_timings.append(total_s)
        df11_builder_timings.append(builder_s)


def record_rotation_update_timing(sample: dict) -> None:
    with _perf_lock:
        rotation_update_timings.append(sample)


def record_df11_batch_phase_timing(sample: dict) -> None:
    with _perf_lock:
        df11_batch_phase_timings.append(sample)


def _new_fired_burst_phase_metrics() -> dict:
    return {key: 0.0 for key in _FIRED_BURST_PHASE_KEYS}


def _add_fired_burst_phase_metrics(target: dict, source: dict) -> None:
    for key in _FIRED_BURST_PHASE_KEYS:
        target[key] = target.get(key, 0.0) + source.get(key, 0.0)

# Beast 12 MHz counter — 12 ticks per microsecond
BEAST_TICKS_PER_US = 12
BEAST_TS_MODULUS = 1 << 48

# Burst detection: replies within 200ms of each other belong to one burst
BURST_GAP_US = 200_000

# Minimum bursts for a reliable period estimate per ICAO
MIN_BURSTS = 4

# IID event max age for in-memory accumulation (seconds)
IID_EVENT_MAX_AGE_S = 60  # 60s — sufficient for DF11 residual diagnostics; rotation uses _burst_records
ROTATION_ANALYSIS_MAX_AGE_S = 120.0   # 2 min ≈ 24–40 rotations — sufficient for period detection
# BurstRecord retention: matches the rotation analysis window so _burst_records
# always covers the full window used by _analyse_burst_records.
BURST_RECORD_MAX_AGE_S = ROTATION_ANALYSIS_MAX_AGE_S
# Density-aware burst retention policy (bounded):
# retained_records ~= active_aircraft * target_sweeps_per_aircraft * headroom.
#
# The lower bound avoids sparse-history starvation at moderate load; the upper
# bound keeps RAM safe on Pi-class systems during extreme traffic bursts.
_BURST_RECORDS_TARGET_SWEEPS_PER_AIRCRAFT = 16.0
_BURST_RECORDS_HEADROOM = 1.4
_BURST_RECORDS_MIN_PER_IID = 800
_BURST_RECORDS_MAX_PER_IID = 8_000
# Active-aircraft estimate lookback for dynamic retention scaling.
_ACTIVE_AIRCRAFT_LOOKBACK_S = 180.0
# Density-aware analysis cap (bounded independently from storage cap).
_ROTATION_ANALYSIS_TARGET_SWEEPS_PER_AIRCRAFT = 20.0
_ROTATION_ANALYSIS_HEADROOM = 1.5
_ROTATION_ANALYSIS_MIN_EVENTS_PER_IID = 1_200
_ROTATION_ANALYSIS_MAX_EVENTS_PER_IID = 12_000
# Hard cap on the global _iid_events deque. Age-based pruning (IID_EVENT_MAX_AGE_S) is the
# primary bound; this maxlen prevents runaway growth during high-rate DF11 bursts between
# pruning cycles.
_IID_EVENTS_MAX = 50_000
STABLE_REANALYZE_INTERVAL_S = 300.0

# Co-sweep window: two bursts within 70ms are considered one radar sweep
CO_SWEEP_WINDOW_US = 70_000
MAX_PAIR_TDOA_US = 5_000
PAIR_GENERATION_WINDOW_S = 120.0

# Position freshness gates for calibration pairs
POS_AGE_FRESH_S = 5.0       # use raw position directly
POS_AGE_INTERP_S = 15.0     # extrapolate with velocity if older than fresh but within this
POS_ADSB_DIRECT_S = 1.0
POS_VECTOR_EXTRAP_S = 3.0

# Minimum qualifying ICAOs for a reliable rotation model
MIN_QUALIFYING_ICAOS = 4
PERIOD_MATCH_TOLERANCE = 0.15
SUPPORT_CAP = 12
PRIMARY_CONFIDENCE_TARGET = 12
SECONDARY_CONFIDENCE_TARGET = 8
SECONDARY_DIRECT_COUNT_MIN = 3
SECONDARY_SUPPORT_RATIO_MIN = 0.6
SERIES_CLUSTER_TOLERANCE = 0.12

# Max age for ADS-B position to be considered "current" (seconds)
_ADSB_POSITION_MAX_AGE_S = 30.0

# On-time window for DF11 residual classification on the burst-sync residual chart.
# Must match the BURST_SYNC_DF11_ON_TIME_THRESHOLD_DEG constant in RadarPage.jsx so
# the backend-computed timing_class agrees with the frontend colour mapping.
_DF11_RESIDUAL_ON_TIME_THRESHOLD_DEG = 6.0
_MIN_FRAME_START_SEPARATION_FRACTION = 0.5
_PHASE_FAMILY_HISTORY_MIN = 2
_PHASE_FAMILY_TOLERANCE_FRACTION = 0.15
_MOTION_RATE_MIN_DT_S = 1.0
_MOTION_RATE_MAX_DT_S = 60.0
_MOTION_RATE_MAX_POS_AGE_S = 8.0
_MOTION_RATE_MAX_ABS_DEG_S = 20.0

_BURST_TIMESTAMP_METHODS = (
    ("first_reply", "burst_ts_first_reply_beast_us", "resid_first_reply_deg"),
    ("strongest_reply", "burst_ts_strongest_reply_beast_us", "resid_strongest_reply_deg"),
    ("simple_centroid", "burst_ts_simple_centroid_beast_us", "resid_simple_centroid_deg"),
    ("weighted_centroid", "burst_ts_weighted_centroid_beast_us", "resid_weighted_centroid_deg"),
    ("mid_strong_window", "burst_ts_mid_strong_window_beast_us", "resid_mid_strong_window_deg"),
    ("last_reply", "burst_ts_last_reply_beast_us", "resid_last_reply_deg"),
)


import math as _math


def _bearing_deg_simple(lat1: float, lon1: float, lat2: float, lon2: float) -> float:
    """Forward azimuth from point 1 to point 2 in [0, 360)."""
    phi1 = _math.radians(lat1)
    phi2 = _math.radians(lat2)
    dlam = _math.radians(lon2 - lon1)
    x = _math.cos(phi1) * _math.sin(phi2) - _math.sin(phi1) * _math.cos(phi2) * _math.cos(dlam)
    y = _math.sin(dlam) * _math.cos(phi2)
    return (_math.degrees(_math.atan2(y, x)) + 360) % 360


def _haversine_nm_simple(lat1: float, lon1: float, lat2: float, lon2: float) -> float:
    """Great-circle distance between two lat/lon points in nautical miles."""
    r_m = 6_371_000.0
    phi1 = _math.radians(lat1)
    phi2 = _math.radians(lat2)
    dphi = _math.radians(lat2 - lat1)
    dlam = _math.radians(lon2 - lon1)
    a = _math.sin(dphi / 2.0) ** 2 + _math.cos(phi1) * _math.cos(phi2) * _math.sin(dlam / 2.0) ** 2
    c = 2.0 * _math.asin(_math.sqrt(max(0.0, min(1.0, a))))
    return (r_m * c) / 1852.0


def _get_authoritative_radar_position(model: RadarIID) -> dict:
    """Return the best-available radar position for a RadarIID model.

    Shared helper used by both radar/api.py and aircraft_localiser.py so that
    Stage 2 and Stage 3 always agree on where a given radar is.
    Priority: manual > CI > FM > TDOA (by lowest CEP).
    Returns dict with keys: source, lat, lon, cep_m.
    """
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


def _sync_quality_from_model(model: RadarIID) -> float:
    """Derive a 0-1 sync quality score from the rotation model status."""
    if model is None or model.period_s is None:
        return 0.0
    status = getattr(model, "status", "UNKNOWN")
    if status == "SINGLE_RADAR":
        return 1.0
    if status == "LIKELY_SINGLE":
        return 0.8
    if status == "CHECK_MULTI":
        return 0.5
    if status == "MULTI_RADAR":
        return 0.3
    return 0.0


def _live_sync_state_to_dict(sync: "LiveSyncState") -> dict:
    """Serialise a LiveSyncState to a plain dict for API/verification payloads."""
    import dataclasses
    return dataclasses.asdict(sync)


def _sync_source_has_rich_python_diagnostics(sync: "LiveSyncState | None") -> bool:
    """Return True when the current sync source carries Python-only rich diagnostics."""
    return bool(sync is not None and getattr(sync, "source", None) == "multi_aircraft_burst")


def _sync_source_is_go_compact(sync: "LiveSyncState | None") -> bool:
    return bool(sync is not None and getattr(sync, "source", None) == "sweep_frame_go")


def _serialise_waveform_bins(bins: list) -> list[dict]:
    """Serialise a list of WaveformBin objects to JSON-safe dicts."""
    n = len(bins)
    if not n:
        return []
    bin_width = 360.0 / n
    return [
        {
            "phase_center_deg": (i + 0.5) * bin_width,
            "correction_deg": b.correction_deg,
            "weight": b.weight,
            "n": b.n,
        }
        for i, b in enumerate(bins)
    ]


def _classify_period_correction_status(
    allowed: bool,
    block_reason: str | None,
    clamp_reason: str | None,
    proposed_delta_s: float | None,
    applied_delta_s: float | None,
    residual_slope_deg_per_s: float | None,
    fit_support: int,
    fit_total: int,
    fit_span_s: float,
) -> str:
    """Summarise the live period correction loop for compact operator display."""
    slope_abs = abs(float(residual_slope_deg_per_s or 0.0))
    proposed_abs = abs(float(proposed_delta_s or 0.0))
    applied_abs = abs(float(applied_delta_s or 0.0))
    if not allowed:
        if block_reason in {"insufficient_fit_observations", "insufficient_fit_icaos", "insufficient_fit_span"}:
            return "weak_fit"
        return "blocked"
    if clamp_reason:
        return "clamp_limited"
    if proposed_abs > 0.0 and applied_abs < proposed_abs * 0.25:
        return "clamp_limited"
    if slope_abs >= 0.05 and applied_abs > 0.0:
        return "converging"
    if fit_total > 0 and fit_support <= 0:
        return "weak_fit"
    if fit_span_s <= 0.0:
        return "weak_fit"
    return "unknown"


def _fit_weighted_slope(xs: list[float], ys: list[float], ws: list[float]) -> tuple[float, float]:
    """Weighted least-squares linear fit y = a + b*x.

    Returns (a, b).  Falls back to (weighted mean, 0.0) when the fit is
    underdetermined (fewer than 2 samples with positive weight, or zero
    x-variance).  All three input lists must be equal length.
    """
    n = len(xs)
    if n == 0 or n != len(ys) or n != len(ws):
        return 0.0, 0.0
    total_w = 0.0
    sum_wx = 0.0
    sum_wy = 0.0
    for x, y, w in zip(xs, ys, ws):
        if w <= 0:
            continue
        total_w += w
        sum_wx += w * x
        sum_wy += w * y
    if total_w <= 0:
        return 0.0, 0.0
    mx = sum_wx / total_w
    my = sum_wy / total_w
    num = 0.0
    den = 0.0
    for x, y, w in zip(xs, ys, ws):
        if w <= 0:
            continue
        dx = x - mx
        num += w * dx * (y - my)
        den += w * dx * dx
    if den <= 0:
        return my, 0.0
    b = num / den
    a = my - b * mx
    return a, b


def _median_float(values: list[float]) -> float | None:
    if not values:
        return None
    clean = sorted(values)
    mid = len(clean) // 2
    if len(clean) % 2:
        return clean[mid]
    return (clean[mid - 1] + clean[mid]) / 2.0


def _residual_stats(values: list[float | None]) -> dict:
    clean = [float(v) for v in values if v is not None and _math.isfinite(float(v))]
    abs_clean = [abs(v) for v in clean]
    median = _median_float(clean)
    deviations = [abs(v - median) for v in clean] if median is not None else []
    return {
        "count": len(clean),
        "mean_abs_residual_deg": (sum(abs_clean) / len(abs_clean)) if abs_clean else None,
        "median_abs_residual_deg": _median_float(abs_clean),
        "robust_spread_mad_deg": _median_float(deviations),
        "mean_residual_deg": (sum(clean) / len(clean)) if clean else None,
        "median_residual_deg": median,
    }


def _clamp_float(value: float, lo: float, hi: float) -> float:
    return max(lo, min(hi, value))


def _compute_folded_phase_shape(rows: list[dict], n_bins: int = 24) -> dict:
    """Summarise diagnostic-only detrended residual shape folded by rotation phase."""
    if n_bins <= 0:
        n_bins = 24
    bins: list[list[float]] = [[] for _ in range(n_bins)]
    cycle_bins: dict[int, dict[int, list[float]]] = defaultdict(lambda: defaultdict(list))
    for row in rows:
        phase = row.get("phase_deg")
        resid = row.get("residual_detrended_deg")
        cycle_index = row.get("cycle_index")
        if phase is None or resid is None:
            continue
        phase_f = float(phase) % 360.0
        resid_f = float(resid)
        if not _math.isfinite(phase_f) or not _math.isfinite(resid_f):
            continue
        idx = _waveform_bin_index(phase_f, n_bins)
        bins[idx].append(resid_f)
        if cycle_index is not None:
            cycle_bins[int(cycle_index)][idx].append(resid_f)

    bin_width = 360.0 / n_bins
    phase_bins = []
    bin_medians: list[float] = []
    bin_spreads: list[float] = []
    for idx, values in enumerate(bins):
        stats = _residual_stats(values)
        median = stats["median_residual_deg"]
        spread = stats["robust_spread_mad_deg"]
        if median is not None:
            bin_medians.append(median)
        if spread is not None:
            bin_spreads.append(spread)
        phase_bins.append({
            "phase_start_deg": idx * bin_width,
            "phase_center_deg": (idx + 0.5) * bin_width,
            "phase_end_deg": (idx + 1) * bin_width,
            "count": stats["count"],
            "median_residual_detrended_deg": median,
            "spread_mad_deg": spread,
            "median_abs_residual_detrended_deg": stats["median_abs_residual_deg"],
        })

    cycle_summaries = []
    cycle_shape_errors: list[float] = []
    for cycle_index, per_bin in cycle_bins.items():
        cycle_points = 0
        cycle_errors = []
        cycle_values = []
        for idx, values in per_bin.items():
            bin_median = phase_bins[idx]["median_residual_detrended_deg"]
            if bin_median is None:
                continue
            cycle_median = _median_float(values)
            if cycle_median is None:
                continue
            cycle_points += len(values)
            cycle_values.extend(values)
            cycle_errors.append(abs(cycle_median - bin_median))
        error = _median_float(cycle_errors)
        if error is not None:
            cycle_shape_errors.append(error)
        stats = _residual_stats(cycle_values)
        cycle_summaries.append({
            "cycle_index": cycle_index,
            "count": cycle_points,
            "median_shape_error_deg": error,
            "median_residual_detrended_deg": stats["median_residual_deg"],
            "spread_mad_deg": stats["robust_spread_mad_deg"],
        })
    cycle_summaries.sort(key=lambda row: row["cycle_index"])

    phase_shape_strength = _residual_stats(bin_medians)["median_abs_residual_deg"]
    within_bin_spread = _median_float(bin_spreads)
    cycle_shape_error = _median_float(cycle_shape_errors)
    if phase_shape_strength is None:
        repeatability_score = None
    else:
        noise = cycle_shape_error if cycle_shape_error is not None else within_bin_spread
        if noise is None:
            repeatability_score = None
        else:
            repeatability_score = _clamp_float(
                phase_shape_strength / max(phase_shape_strength + noise, 1.0),
                0.0,
                1.0,
            )

    return {
        "phase_bin_count": n_bins,
        "phase_bins": phase_bins,
        "cycles": cycle_summaries,
        "phase_shape_strength_deg": phase_shape_strength,
        "within_bin_spread_mad_deg": within_bin_spread,
        "cycle_shape_error_median_deg": cycle_shape_error,
        "cycle_to_cycle_repeatability": repeatability_score,
        "cycle_count": len(cycle_summaries),
    }


def _classify_sync_error_mode(
    *,
    slope_deg_per_s: float | None,
    time_span_s: float,
    raw_mad_deg: float | None,
    detrended_mad_deg: float | None,
    phase_shape_strength_deg: float | None,
    repeatability: float | None,
) -> dict:
    """Compact rule-based interpretation for operator diagnostics only."""
    slope = abs(float(slope_deg_per_s or 0.0))
    drift_deg = slope * max(0.0, time_span_s)
    period_drift_strength = _clamp_float(drift_deg / 20.0, 0.0, 1.0)
    if raw_mad_deg is not None and detrended_mad_deg is not None and raw_mad_deg > 0:
        drift_improvement = _clamp_float((raw_mad_deg - detrended_mad_deg) / raw_mad_deg, 0.0, 1.0)
        period_drift_strength = max(period_drift_strength, drift_improvement)

    phase_shape_strength = _clamp_float(float(phase_shape_strength_deg or 0.0) / 10.0, 0.0, 1.0)
    repeat = repeatability if repeatability is not None else 0.0
    unstable_strength = _clamp_float((1.0 - repeat) * (float(detrended_mad_deg or 0.0) / 12.0), 0.0, 1.0)

    period_strong = period_drift_strength >= 0.35
    shape_strong = phase_shape_strength >= 0.35 and repeat >= 0.45
    unstable = unstable_strength >= 0.35 and repeat < 0.45
    if period_strong and shape_strong:
        mode = "mixed"
    elif period_strong and not shape_strong:
        mode = "period_drift"
    elif shape_strong:
        mode = "repeatable_phase_shape"
    elif unstable:
        mode = "unstable_cycle_shape"
    else:
        mode = "mixed" if period_drift_strength >= 0.2 or phase_shape_strength >= 0.2 else "unstable_cycle_shape"

    return {
        "dominant_error_mode": mode,
        "period_drift_strength": period_drift_strength,
        "period_drift_window_deg": drift_deg,
        "phase_shape_strength": phase_shape_strength,
        "phase_shape_strength_deg": phase_shape_strength_deg,
        "cycle_to_cycle_repeatability": repeatability,
        "unstable_cycle_strength": unstable_strength,
    }


def _waveform_bin_index(phase_deg: float, n_bins: int) -> int:
    """Phase (0–360) → bin index in [0, n_bins).  Wraps negative/large phases."""
    if n_bins <= 0:
        return 0
    phase = phase_deg % 360.0
    if phase < 0:
        phase += 360.0
    idx = int(phase / (360.0 / n_bins))
    if idx >= n_bins:
        idx = n_bins - 1
    return idx


def _assess_period_failure_mode(
    *,
    recent_obs_count: int,
    n_rejected: int,
    recent_icao_count: int,
    fit_support_count: int,
    fit_contributing_icao_count: int,
    period_update_block_reason: str | None,
    phase_correction_deg: float | None,
    phase_anchor_status: str | None,
    phase_validation_status: str | None,
    phase_validation_contributors: int,
    fit_reject_reasons: dict[str, int] | None,
    scored: list[dict],
    fit_origin_s: float,
    fit_intercept_deg: float,
    fit_slope_deg_per_s: float,
    fit_span_s: float,
    period_s: float,
) -> dict:
    """Assess whether the refined live period likely converged to a wrong family."""
    rows = []
    for entry in scored:
        effective_us = float(entry.get("effective_us") or 0.0)
        x_s = effective_us / 1_000_000.0 - fit_origin_s
        detrended_residual = entry.get("residual")
        if detrended_residual is not None:
            detrended_residual = float(detrended_residual) - (
                fit_intercept_deg + fit_slope_deg_per_s * x_s
            )
        cycle_index = int(x_s / period_s) if period_s > 0 else None
        rows.append({
            "phase_deg": entry.get("phase_in_rot"),
            "residual_detrended_deg": detrended_residual,
            "cycle_index": cycle_index,
        })

    raw_stats = _residual_stats([entry.get("residual") for entry in scored])
    detrended_stats = _residual_stats([row.get("residual_detrended_deg") for row in rows])
    raw_median_abs = raw_stats["median_abs_residual_deg"]
    detrended_median_abs = detrended_stats["median_abs_residual_deg"]
    detrended_improvement_ratio = None
    if raw_median_abs is not None and detrended_median_abs is not None and raw_median_abs > 0:
        detrended_improvement_ratio = (raw_median_abs - detrended_median_abs) / raw_median_abs

    phase_shape = _compute_folded_phase_shape(rows)
    error_mode = _classify_sync_error_mode(
        slope_deg_per_s=fit_slope_deg_per_s,
        time_span_s=fit_span_s,
        raw_mad_deg=raw_stats["robust_spread_mad_deg"],
        detrended_mad_deg=detrended_stats["robust_spread_mad_deg"],
        phase_shape_strength_deg=phase_shape["phase_shape_strength_deg"],
        repeatability=phase_shape["cycle_to_cycle_repeatability"],
    )

    primary_conditions: list[str] = []
    secondary_conditions: list[str] = []
    if recent_obs_count > 0 and n_rejected >= max(4, recent_obs_count * 0.4):
        primary_conditions.append("high_rejected_fraction")
    if fit_support_count < 6:
        primary_conditions.append("low_fit_support")
    if fit_contributing_icao_count < 2:
        primary_conditions.append("single_icao_fit")
    if fit_span_s < max(2.0 * period_s, 0.0):
        primary_conditions.append("short_fit_span")
    if period_update_block_reason == "slope_not_persistent":
        primary_conditions.append("slope_not_persistent")
    if (
        raw_median_abs is not None
        and raw_median_abs > 12.0
        and detrended_improvement_ratio is not None
        and detrended_improvement_ratio < 0.2
    ):
        primary_conditions.append("weak_detrending_improvement")
    if error_mode["dominant_error_mode"] == "mixed":
        primary_conditions.append(f"error_mode_{error_mode['dominant_error_mode']}")
    elif (
        error_mode["dominant_error_mode"] == "unstable_cycle_shape"
        and recent_icao_count >= 2
        and (
            "weak_detrending_improvement" in primary_conditions
            or (recent_obs_count > 0 and n_rejected >= max(3, recent_obs_count * 0.25))
            or fit_support_count < 6
        )
    ):
        primary_conditions.append(f"error_mode_{error_mode['dominant_error_mode']}")
    if phase_validation_status == "population_disagrees":
        secondary_conditions.append("population_disagrees")
    if str(phase_anchor_status or "").startswith("fallback_"):
        secondary_conditions.append("fallback_anchor")
    if abs(float(phase_correction_deg or 0.0)) > 20.0:
        secondary_conditions.append("large_phase_correction")

    dominant_fit_reject_reason = None
    if fit_reject_reasons:
        dominant_fit_reject_reason = max(
            fit_reject_reasons.items(),
            key=lambda item: item[1],
        )[0]
        if dominant_fit_reject_reason in {
            "residual_gate",
            "zero_weight",
            "poor_icao_quality",
            "near_wrap_residual",
        }:
            primary_conditions.append(f"dominant_fit_reject_{dominant_fit_reject_reason}")

    # A single-aircraft anchor-only correction path is primarily an absolute
    # branch problem, not evidence that the period family is wrong.
    if (
        recent_icao_count < 2
        and phase_validation_status == "anchor_only"
        and phase_anchor_status in {"anchor_only", "selected"}
        and error_mode["dominant_error_mode"] == "unstable_cycle_shape"
    ):
        primary_conditions = [
            condition for condition in primary_conditions
            if condition not in {
                "high_rejected_fraction",
                "low_fit_support",
                "single_icao_fit",
                "short_fit_span",
            }
        ]

    primary_condition_count = len(set(primary_conditions))
    secondary_condition_count = len(set(secondary_conditions))
    severe_primary_failure = (
        recent_obs_count > 0
        and (n_rejected / recent_obs_count) > 0.6
        and (fit_contributing_icao_count < 2 or fit_support_count < 4)
        and error_mode["dominant_error_mode"] != "unstable_cycle_shape"
    )
    severe_combined_failure = (
        severe_primary_failure
        and phase_validation_status == "population_disagrees"
        and phase_validation_contributors <= 0
    )
    immediate_reacquire = severe_primary_failure or severe_combined_failure
    wrong_period_suspect = (
        primary_condition_count >= 2
        or (primary_condition_count >= 1 and secondary_condition_count >= 1)
        or immediate_reacquire
    )

    failure_primary_class = "clean"
    if primary_condition_count >= 1:
        failure_primary_class = "period_family"
        if (
            secondary_condition_count >= 1
            and phase_validation_status == "population_disagrees"
            and phase_validation_contributors <= 0
        ):
            failure_primary_class = "combined"
    elif secondary_condition_count >= 1:
        failure_primary_class = "anchor_branch"

    failure_score = float(primary_condition_count)
    failure_score += 0.35 * secondary_condition_count
    if immediate_reacquire:
        failure_score += 2.0
    if dominant_fit_reject_reason == "residual_gate":
        failure_score += 0.5
    if failure_primary_class == "combined":
        failure_reason = ",".join((list(dict.fromkeys(primary_conditions + secondary_conditions)))[:3])
    elif failure_primary_class == "period_family":
        failure_reason = ",".join((list(dict.fromkeys(primary_conditions)))[:3])
    elif failure_primary_class == "anchor_branch":
        failure_reason = ",".join((list(dict.fromkeys(secondary_conditions)))[:3])
    else:
        failure_reason = "clean"

    return {
        "wrong_period_suspect": wrong_period_suspect,
        "immediate_reacquire": immediate_reacquire,
        "failure_score": failure_score,
        "failure_reason": failure_reason,
        "failure_primary_class": failure_primary_class,
        "primary_conditions": list(dict.fromkeys(primary_conditions)),
        "secondary_conditions": list(dict.fromkeys(secondary_conditions)),
        "raw_residual_stats": raw_stats,
        "detrended_residual_stats": detrended_stats,
        "raw_median_abs_residual_deg": raw_median_abs,
        "detrended_median_abs_residual_deg": detrended_median_abs,
        "detrended_improvement_ratio": detrended_improvement_ratio,
        "dominant_error_mode": error_mode["dominant_error_mode"],
        "dominant_fit_reject_reason": dominant_fit_reject_reason,
        "fit_reject_reasons": dict(fit_reject_reasons or {}),
        "phase_shape_diagnostics": phase_shape,
        "error_mode": error_mode,
    }


def _assess_anchor_failure_mode(
    *,
    phase_anchor_status: str | None,
    phase_validation_status: str | None,
    phase_validation_contributors: int,
    phase_validation_reject_count: int,
    phase_correction_deg: float | None,
) -> dict:
    """Assess whether the current issue is primarily absolute branch/anchor related."""
    conditions: list[str] = []
    if str(phase_anchor_status or "").startswith("fallback_"):
        conditions.append("fallback_anchor")
    if phase_anchor_status == "population_veto":
        conditions.append("population_veto")
    if phase_validation_status == "population_disagrees":
        conditions.append("population_disagrees")
    if phase_validation_status == "population_disagrees" and phase_validation_contributors <= 0:
        conditions.append("no_validation_support")
    if phase_validation_reject_count >= 2:
        conditions.append("multiple_validation_rejects")
    if abs(float(phase_correction_deg or 0.0)) > 20.0:
        conditions.append("large_phase_correction")

    return {
        "anchor_branch_suspect": bool(conditions),
        "anchor_failure_reason": ",".join(conditions[:3]) if conditions else "clean",
        "anchor_conditions": conditions,
        "severe_anchor_failure": (
            phase_validation_status == "population_disagrees"
            and phase_validation_contributors <= 0
            and phase_validation_reject_count >= 2
        ),
    }


def _apply_phase_waveform_correction(
    bins: list[WaveformBin] | None,
    phase_deg: float,
    *,
    applied: bool,
) -> float:
    """Return the empirical waveform correction in degrees for this phase.

    Returns 0.0 when the waveform is not yet applied or no bins exist.
    """
    if not applied or not bins:
        return 0.0
    n_bins = len(bins)
    idx = _waveform_bin_index(phase_deg, n_bins)
    # Simple linear interpolation between bin centres for smoothness.
    width = 360.0 / n_bins
    centre = (idx + 0.5) * width
    delta = (phase_deg % 360.0) - centre
    if delta > width:
        delta -= 360.0
    elif delta < -width:
        delta += 360.0
    if delta >= 0:
        other = (idx + 1) % n_bins
        frac = delta / width
    else:
        other = (idx - 1) % n_bins
        frac = -delta / width
    a = bins[idx].correction_deg
    b = bins[other].correction_deg
    return (1.0 - frac) * a + frac * b


@_dataclass
class SyncPrediction:
    """Authoritative live sync prediction for one timestamp.

    Sign convention: residuals are always observed_bearing - predicted_bearing.
    Positive residual slope over effective Beast time means the model is rotating
    too slowly, so period refinement must decrease period_s.
    """
    raw_arrival_us: float
    effective_arrival_us: float
    prop_corrected_beast_us: float
    motion_corrected_beast_us: float
    propagation_correction_us: float
    motion_comp_dt_us: float | None
    bearing_rate_deg_s: float | None
    motion_comp_enabled: bool
    motion_comp_applied: bool
    motion_comp_block_reason: str | None
    phase_in_rot_deg: float
    predicted_bearing_raw_deg: float
    predicted_bearing_deg: float
    waveform_correction_deg: float
    waveform_applied: bool
    predictor_version: str = "authoritative_sync_v3_motion"


def predict_sync_observation(
    sync: "LiveSyncState",
    arrival_us: float,
    *,
    range_nm: float | None = None,
    waveform_bins: list[WaveformBin] | None = None,
    apply_propagation: bool | None = None,
    apply_waveform: bool | None = None,
    apply_motion: bool | None = None,
    bearing_rate_deg_s: float | None = None,
    motion_comp_dt_us: float | None = None,
    motion_comp_block_reason: str | None = None,
) -> SyncPrediction:
    """Predict bearing from arrival time using the refined sync model.

    This is the single authoritative predictor consumed by burst-sync
    residual generation, live period fitting, diagnostics, and Stage 3 live
    bearing observation construction.  Update order is explicit:
      1. compute propagation-corrected effective time,
      2. subtract aircraft-motion timing shift when enabled and valid,
      3. compute phase with the current refined period,
      4. apply the current waveform correction to the predicted bearing.
    """
    period_us = sync.period_s * 1e6
    if period_us <= 0:
        predicted = sync.phase_offset_deg % 360.0
        return SyncPrediction(
            raw_arrival_us=arrival_us,
            effective_arrival_us=arrival_us,
            prop_corrected_beast_us=arrival_us,
            motion_corrected_beast_us=arrival_us,
            propagation_correction_us=0.0,
            motion_comp_dt_us=None,
            bearing_rate_deg_s=bearing_rate_deg_s,
            motion_comp_enabled=False,
            motion_comp_applied=False,
            motion_comp_block_reason="invalid_period",
            phase_in_rot_deg=0.0,
            predicted_bearing_raw_deg=predicted,
            predicted_bearing_deg=predicted,
            waveform_correction_deg=0.0,
            waveform_applied=False,
        )
    effective_us = arrival_us
    prop_delay_us = 0.0
    prop_enabled = sync.prop_delay_enabled if apply_propagation is None else bool(apply_propagation)
    if prop_enabled:
        prop_delay_us = _compute_propagation_delay_us(range_nm)
        effective_us = arrival_us - prop_delay_us
    prop_corrected_us = effective_us

    motion_enabled = sync.motion_comp_phase_enabled if apply_motion is None else bool(apply_motion)
    motion_dt = motion_comp_dt_us
    if motion_dt is None:
        motion_dt = _compute_motion_comp_dt_us(sync.period_s, bearing_rate_deg_s)
    motion_applied = False
    motion_block = motion_comp_block_reason
    if not motion_enabled:
        motion_block = "disabled"
    elif motion_block is not None:
        motion_applied = False
    elif motion_dt is None:
        motion_block = motion_block or "bearing_rate_unavailable"
    else:
        effective_us = prop_corrected_us - motion_dt
        motion_applied = True
        motion_block = None

    phase_in_rot = ((effective_us - sync.phase_epoch_us) / period_us * 360.0) % 360.0
    predicted_raw = (phase_in_rot + sync.phase_offset_deg) % 360.0
    predicted = predicted_raw
    waveform_correction = 0.0
    waveform_enabled = (sync.waveform_enabled and sync.waveform_applied) if apply_waveform is None else bool(apply_waveform)
    waveform_applied = bool(waveform_enabled)
    if waveform_enabled:
        waveform_correction = _apply_phase_waveform_correction(
            waveform_bins, phase_in_rot, applied=True,
        )
        predicted = (predicted - waveform_correction) % 360.0
    return SyncPrediction(
        raw_arrival_us=arrival_us,
        effective_arrival_us=effective_us,
        prop_corrected_beast_us=prop_corrected_us,
        motion_corrected_beast_us=effective_us,
        propagation_correction_us=prop_delay_us,
        motion_comp_dt_us=motion_dt,
        bearing_rate_deg_s=bearing_rate_deg_s,
        motion_comp_enabled=motion_enabled,
        motion_comp_applied=motion_applied,
        motion_comp_block_reason=motion_block,
        phase_in_rot_deg=phase_in_rot,
        predicted_bearing_raw_deg=predicted_raw,
        predicted_bearing_deg=predicted,
        waveform_correction_deg=waveform_correction,
        waveform_applied=waveform_applied,
    )


def _predict_bearing_from_sync(
    sync: "LiveSyncState",
    arrival_us: float,
    *,
    range_nm: float | None = None,
    waveform_bins: list[WaveformBin] | None = None,
) -> tuple[float, float]:
    """Compatibility wrapper around the authoritative predictor."""
    prediction = predict_sync_observation(
        sync,
        arrival_us,
        range_nm=range_nm,
        waveform_bins=waveform_bins,
    )
    return prediction.predicted_bearing_deg, prediction.phase_in_rot_deg


def _compute_sync_residual_deg(
    existing: "LiveSyncState",
    new_epoch_us: float,
    new_offset_deg: float,
    period_us: float,
) -> float:
    """Circular residual between a new frame's bearing and the existing sync prediction.

    Converts the existing sync state forward to new_epoch_us, then returns the
    signed angular difference in (-180, 180].  A residual near 0 means the new
    frame agrees well with the current sync anchor.
    """
    existing_at_new_epoch = (
        (new_epoch_us - existing.phase_epoch_us) / period_us * 360.0 + existing.phase_offset_deg
    ) % 360.0
    return (new_offset_deg - existing_at_new_epoch + 540.0) % 360.0 - 180.0


def _circular_delta_deg(a_deg: float | None, b_deg: float | None) -> float | None:
    """Signed circular delta a-b in degrees, or None when either side is absent."""
    if a_deg is None or b_deg is None:
        return None
    return (a_deg - b_deg + 540.0) % 360.0 - 180.0


def _circular_weighted_mean_deg(values: list[float], weights: list[float] | None = None) -> float | None:
    """Weighted circular mean in [0, 360), or None when no finite values exist."""
    if weights is None:
        weights = [1.0] * len(values)
    sin_sum = 0.0
    cos_sum = 0.0
    total_w = 0.0
    for value, weight in zip(values, weights):
        if not _math.isfinite(float(value)) or not _math.isfinite(float(weight)) or weight <= 0:
            continue
        rad = _math.radians(float(value))
        sin_sum += _math.sin(rad) * float(weight)
        cos_sum += _math.cos(rad) * float(weight)
        total_w += float(weight)
    if total_w <= 0.0 or (abs(sin_sum) < 1e-12 and abs(cos_sum) < 1e-12):
        return None
    return (_math.degrees(_math.atan2(sin_sum, cos_sum)) + 360.0) % 360.0


def _circular_mad_deg(values: list[float], centre_deg: float | None = None) -> float | None:
    """Median absolute circular deviation from centre_deg."""
    clean = [float(v) for v in values if _math.isfinite(float(v))]
    if not clean:
        return None
    centre = centre_deg if centre_deg is not None else _circular_weighted_mean_deg(clean)
    if centre is None:
        return None
    return _median_float([abs(_circular_delta_deg(v, centre) or 0.0) for v in clean])


def _estimate_aircraft_bearing_rate(
    *,
    icao: str,
    bearing_deg: float,
    burst_centroid_us: float,
    pos_age_s: float,
    history: list["AlignedBurstSyncObs"],
) -> dict:
    """Estimate aircraft angular motion relative to the radar.

    The relevant sync term is bearing rate as seen from the radar, not linear
    speed or radial speed.  This first implementation uses finite differences
    over recent trusted burst-position observations for the same ICAO.  Bearing
    wrap is handled with a signed circular delta.
    """
    if not icao:
        return {
            "bearing_rate_deg_s": None,
            "motion_comp_block_reason": "missing_icao",
        }
    if pos_age_s is None or pos_age_s > _MOTION_RATE_MAX_POS_AGE_S:
        return {
            "bearing_rate_deg_s": None,
            "motion_comp_block_reason": "stale_position",
        }

    previous = None
    for candidate in reversed(history):
        if getattr(candidate, "icao", None) != icao:
            continue
        previous = candidate
        break

    if previous is None:
        return {
            "bearing_rate_deg_s": None,
            "motion_comp_block_reason": "insufficient_history",
        }
    if getattr(previous, "pos_age_s", None) is None or previous.pos_age_s > _MOTION_RATE_MAX_POS_AGE_S:
        return {
            "bearing_rate_deg_s": None,
            "motion_comp_block_reason": "previous_position_stale",
        }

    dt_s = (burst_centroid_us - previous.burst_centroid_us) / 1_000_000.0
    if dt_s < _MOTION_RATE_MIN_DT_S:
        return {
            "bearing_rate_deg_s": None,
            "motion_comp_block_reason": "time_delta_too_small",
        }
    if dt_s > _MOTION_RATE_MAX_DT_S:
        return {
            "bearing_rate_deg_s": None,
            "motion_comp_block_reason": "time_delta_too_large",
        }

    delta_deg = _circular_delta_deg(bearing_deg, previous.bearing_deg)
    if delta_deg is None:
        return {
            "bearing_rate_deg_s": None,
            "motion_comp_block_reason": "bearing_delta_unavailable",
        }
    bearing_rate_deg_s = delta_deg / dt_s
    if abs(bearing_rate_deg_s) > _MOTION_RATE_MAX_ABS_DEG_S:
        return {
            "bearing_rate_deg_s": bearing_rate_deg_s,
            "motion_comp_block_reason": "bearing_rate_absurd",
        }

    return {
        "bearing_rate_deg_s": bearing_rate_deg_s,
        "motion_comp_block_reason": None,
    }


# One-way speed-of-light delay per nautical mile, microseconds.
# c = 299792458 m/s, 1 NM = 1852 m → 1 NM ≈ 6.18 µs.
_US_PER_NM_LIGHT = 1852.0 / 299792458.0 * 1e6


def _compute_propagation_delay_us(range_nm: float | None) -> float:
    """One-way aircraft→receiver propagation delay for the given range.

    Negative or missing ranges produce 0 so the observation passes through
    unchanged.
    """
    if range_nm is None or range_nm <= 0:
        return 0.0
    return float(range_nm) * _US_PER_NM_LIGHT


def _compute_motion_comp_dt_us(period_s: float, bearing_rate_deg_s: float | None) -> float | None:
    """First-order beam-crossing timing shift from aircraft bearing rate.

    If aircraft bearing changes by Δθ during one sweep, the apparent crossing
    time shifts by approximately (Δθ / 360) * T.  With a bearing rate θdot this
    is (θdot / 360) * T².  The returned value is subtracted from the
    propagation-corrected Beast timestamp to put the observation on the raw
    sweep-period basis used by phase/period fitting.
    """
    if bearing_rate_deg_s is None or period_s <= 0:
        return None
    return (bearing_rate_deg_s / 360.0) * (period_s ** 2) * 1_000_000.0


@_dataclass
class WaveformBin:
    """One circular bin in the empirical phase-in-rotation waveform model."""
    correction_deg: float = 0.0   # EMA of residual at this phase
    weight: float = 0.0           # summed observation weight
    n: int = 0                    # raw observation count


@_dataclass
class IcaoSyncQuality:
    """Per-IID per-ICAO residual quality memory for fit downweighting."""
    residual_median_deg: float = 0.0   # circular-mean EMA of signed residual
    residual_mad_deg: float = 5.0      # EMA of |residual − median|
    n_recent: int = 0
    last_ts: float = 0.0


def _icao_quality_memory_score(entry: IcaoSyncQuality | None) -> float:
    """Return a 0-1 quality multiplier derived from residual spread memory."""
    if entry is None:
        return 1.0
    return max(0.0, min(1.0, 1.0 - entry.residual_mad_deg / 25.0))


def _icao_quality_reject_reason(entry: IcaoSyncQuality | None) -> str | None:
    """Return the hard-reject reason for poor ICAO quality memory, if any."""
    if entry is None or entry.n_recent < 6:
        return None
    if entry.residual_mad_deg > 25.0 or abs(entry.residual_median_deg) > 45.0:
        return "poor_icao_quality"
    return None


def _icao_quality_anchor_warning(entry: IcaoSyncQuality | None) -> str | None:
    """Return the anchor-selection warning for degraded ICAO quality memory, if any."""
    if entry is None or entry.n_recent < 6:
        return None
    if entry.residual_mad_deg > 25.0 or abs(entry.residual_median_deg) > 60.0:
        return "poor_icao_quality_memory"
    return None


def _update_icao_sync_quality_memory(
    quality_dict: dict[str, IcaoSyncQuality],
    scored: list[dict],
) -> None:
    """Update per-ICAO residual quality memory from scored sync observations."""
    quality_alpha = 0.1
    for entry in scored:
        residual_c = entry["residual"]
        weight = entry["weight"]
        status = entry["status"]
        icao = entry["icao"]
        ts_obs = entry["obs_ts"]
        if weight <= 0 or status == "rejected":
            continue
        q_entry = quality_dict.get(icao)
        if q_entry is None:
            quality_dict[icao] = IcaoSyncQuality(
                residual_median_deg=residual_c,
                residual_mad_deg=abs(residual_c),
                n_recent=1,
                last_ts=ts_obs,
            )
            continue
        q_entry.residual_median_deg = (
            (1.0 - quality_alpha) * q_entry.residual_median_deg
            + quality_alpha * residual_c
        )
        dev = abs(residual_c - q_entry.residual_median_deg)
        q_entry.residual_mad_deg = (
            (1.0 - quality_alpha) * q_entry.residual_mad_deg
            + quality_alpha * dev
        )
        q_entry.n_recent += 1
        q_entry.last_ts = ts_obs


@_dataclass
class LiveSyncState:
    """Per-IID live synchronisation state for Stage 3 bearing computation.

    Updated each time a sweep frame is completed. The localiser converts
    burst-centre arrival times to bearings using:
        bearing = (arrival_us - phase_epoch_us) / period_us * 360 % 360 + phase_offset_deg.

    The phase epoch is advanced each accepted frame so long-baseline period
    errors do not accumulate.  sync_jitter_deg is derived from the residual EMA
    rather than a fixed constant.
    """
    iid: int
    period_s: float
    phase_epoch_us: float       # burst-centre timestamp of last accepted frame
    phase_offset_deg: float     # bearing from radar to ref aircraft at phase_epoch_us
    sync_quality: float         # 0.0–1.0; derived from rotation model status
    sync_jitter_deg: float      # residual-EMA-derived bearing jitter (1-sigma estimate)
    last_sync_update_ts: float  # wall-clock time of last accepted update
    source: str                 # "sweep_frame"
    usable: bool                # sync_quality is above the minimum threshold
    # Residual tracking for robust sync (all fields have defaults for backwards compat)
    residual_ema_deg: float = 5.0        # EMA of |residual| over accepted frames
    n_sync_frames: int = 0               # count of accepted sync frame updates
    n_rejected_frames: int = 0           # count of rejected frame updates (diagnostics)
    last_residual_deg: float = 0.0       # most recent circular residual in degrees
    holdover: bool = False               # True when the last update was rejected or too weak
    # Multi-aircraft sync diagnostics (populated by _update_multi_aircraft_sync_state)
    n_burst_obs_inliers: int = 0         # inlier burst observations in last multi-aircraft update
    n_burst_obs_rejected: int = 0        # rejected burst observations in last update
    contributing_icao_count: int = 0     # distinct ICAOs contributing to current sync estimate
    # Live period refinement diagnostics.
    # period_base_s is the aggregate estimator's coarse period at the time this
    # state was seeded; period_s above is the live-refined period that may drift
    # slightly from period_base_s as the residual slope is corrected.
    period_base_s: float = 0.0
    residual_slope_deg_per_s: float = 0.0  # fitted residual-vs-time slope last update
    period_correction_ppm: float = 0.0     # (period_s - period_base_s) / period_base_s * 1e6
    period_refine_enabled: bool = False    # True if period refinement is active this session
    period_update_term: float = 0.0         # unclamped candidate period delta in seconds
    period_update_direction: str = "none"   # increase/decrease/none; positive residual slope decreases period
    period_update_applied: float = 0.0      # actual applied period delta in seconds after clamps
    period_update_gain: float = 0.0         # gain applied to residual_slope_deg_per_s
    period_refine_block_reason: str | None = None
    period_update_proposed_s: float = 0.0
    period_update_proposed_us: float = 0.0
    period_update_applied_s: float = 0.0
    period_update_applied_us: float = 0.0
    period_update_ppm_unclamped: float = 0.0
    period_update_ppm_applied: float = 0.0
    period_update_block_reason: str | None = None
    period_update_clamp_reason: str | None = None
    period_update_allowed: bool = False
    period_update_fit_support: int = 0
    period_update_fit_span_s: float = 0.0
    period_correction_status: str = "unknown"
    period_update_safety_ppm_per_update: float = 0.0
    period_update_safety_ppm_from_base: float = 0.0
    fit_time_basis: str = "effective_beast_time_s"
    fit_residual_basis: str = "observed_minus_authoritative_prediction_after_waveform_deg"
    fit_total_observations: int = 0
    fit_eligible_observations: int = 0
    fit_rejected_observations: int = 0
    fit_reject_reasons: dict[str, int] = _field(default_factory=dict)
    fit_contributing_icao_count: int = 0
    fit_span_s: float = 0.0
    predictor_consistency: dict[str, bool] = _field(default_factory=dict)
    # Phase-in-rotation waveform correction diagnostics.
    waveform_enabled: bool = False         # config flag state
    waveform_applied: bool = False         # True if waveform passed coverage threshold and is applied
    waveform_bin_count: int = 0            # configured bin count
    waveform_residual_reduction_deg: float = 0.0  # rolling |resid_raw| - |resid_corrected|
    waveform_learning_enabled: bool = False
    waveform_update_block_reason: str | None = None
    waveform_learning_residual_basis: str = "residual_after_waveform_detrended_deg"
    # Propagation delay correction diagnostics.
    prop_delay_enabled: bool = False       # config flag state
    # Aircraft angular-motion timing correction diagnostics.
    motion_comp_phase_enabled: bool = False
    motion_comp_fit_enabled: bool = False
    motion_comp_applied_count: int = 0
    motion_comp_blocked_count: int = 0
    motion_comp_mean_dt_us: float = 0.0
    motion_comp_mean_residual_improvement_deg: float = 0.0
    motion_comp_high_rate_mean_residual_improvement_deg: float | None = None
    # Absolute phase anchoring.  Period refinement and absolute phase are
    # separate estimation problems: period_s is fitted from the multi-aircraft
    # residual slope, while these fields describe the selected aircraft that
    # currently defines the absolute sweep branch.
    phase_anchor_icao: str | None = None
    phase_anchor_score: float = 0.0
    phase_anchor_obs_count: int = 0
    phase_anchor_offset_raw_deg: float | None = None
    phase_anchor_offset_smoothed_deg: float | None = None
    phase_anchor_spread_deg: float | None = None
    phase_anchor_status: str = "unavailable"
    phase_anchor_since_ts: float | None = None
    phase_anchor_replacement_reason: str | None = None
    phase_anchor_candidate_count: int = 0
    phase_anchor_no_candidate_reason: str | None = None
    phase_validation_contributors: int = 0
    phase_validation_reject_count: int = 0
    phase_validation_median_error_deg: float | None = None
    phase_validation_status: str = "unavailable"
    phase_anchor_candidates: list[dict] = _field(default_factory=list)
    period_failure_score: float = 0.0
    period_failure_streak: int = 0
    period_reacquire_active: bool = False
    period_reacquire_reason: str | None = None
    period_reacquire_started_ts: float | None = None
    period_refine_mode: str = "normal"
    period_authoritative_source: str = "refined"
    period_failure_primary_class: str = "clean"
    period_reacquire_trigger: str | None = None
    period_recovery_clean_update: bool = False
    period_recovery_clean_streak: int = 0
    dominant_period_s: float | None = None
    dominant_prior_period_s: float | None = None
    trusted_refined_period_s: float | None = None
    bootstrap_period_s: float | None = None
    active_family_prior_s: float | None = None
    active_family_prior_source: str | None = None
    dominant_prior_active: bool = False
    dominant_period_delta_s: float | None = None
    dominant_period_delta_ppm: float | None = None
    compact_period_s: float | None = None
    compact_period_delta_to_dominant_s: float | None = None
    compact_period_delta_to_dominant_ppm: float | None = None
    compact_sync_unreliable: bool = False
    recovery_mode_active: bool = False
    recovery_trigger_reasons: list[str] = _field(default_factory=list)
    compact_gating_bypassed: bool = False
    recovery_relaxed_admitted_observations: int = 0
    active_authority_mode: str | None = None
    authority_switch_count: int = 0
    last_authority_switch_ts: float | None = None
    last_authority_switch_reason: str | None = None
    authority_enter_streak: int = 0
    authority_exit_streak: int = 0
    anchor_switch_count: int = 0
    last_anchor_switch_ts: float | None = None
    last_anchor_switch_reason: str | None = None
    anchor_hold_updates: int = 0


@_dataclass
class AlignedBurstSyncObs:
    """One burst-centre bearing observation for multi-aircraft sync maintenance.

    Recorded for each dominant-family burst that has an ADS-B position.
    The rolling buffer of these observations is used by
    _update_multi_aircraft_sync_state() to fit a robust phase correction
    without depending on any single reference aircraft.
    """
    burst_centroid_us: float    # Beast-monotonic burst-centre timestamp
    icao: str                   # Aircraft ICAO
    bearing_deg: float          # Geometric bearing from radar to aircraft (pre-computed)
    n_replies: int              # Burst reply count (quality factor for burst-centre accuracy)
    signal_dbfs: float | None   # Average signal strength (dBFS, negative; None if unknown)
    pos_age_s: float            # ADS-B position age at burst time (seconds)
    range_nm: float             # Geometric range from radar to aircraft (nautical miles)
    ts: float                   # Wall-clock time for rolling-window age filtering
    sync_update_eligible: bool = True  # True if this burst was eligible to steer sync
    # Propagation-corrected timing.  burst_centroid_us above is preserved as the
    # raw Beast-monotonic timestamp so downstream consumers are unaffected.
    # raw_arrival_us mirrors burst_centroid_us for API clarity; effective_arrival_us
    # is the propagation-corrected time used by the sync model when the
    # RADAR_SYNC_PROP_DELAY_ENABLED flag is on.
    raw_arrival_us: float = 0.0
    prop_delay_aircraft_to_receiver_us: float = 0.0
    prop_delay_radar_to_aircraft_us: float | None = None
    effective_arrival_us: float = 0.0
    # Aircraft-motion compensation diagnostics.  bearing_rate_deg_s is the
    # angular motion of this aircraft as seen from the radar; radial speed is
    # not the term that changes apparent sweep recurrence.
    bearing_rate_deg_s: float | None = None
    motion_comp_dt_us: float | None = None
    motion_corrected_beast_us: float | None = None
    motion_comp_applied: bool = False
    motion_comp_block_reason: str | None = None
    # Burst-centre estimator diagnostics.
    burst_center_simple_us: float | None = None
    burst_center_weighted_us: float | None = None
    burst_center_delta_us: float | None = None
    burst_center_method: str = "centroid"
    burst_ts_first_reply_beast_us: float | None = None
    burst_ts_strongest_reply_beast_us: float | None = None
    burst_ts_simple_centroid_beast_us: float | None = None
    burst_ts_weighted_centroid_beast_us: float | None = None
    burst_ts_mid_strong_window_beast_us: float | None = None
    burst_ts_last_reply_beast_us: float | None = None
    burst_span_us: float | None = None
    peak_amplitude: float | None = None
    position_interpolated: bool = False
    position_extrapolated: bool = False
    position_source_age_s: float | None = None
    truth_position_ts_beast_us: float | None = None


class AircraftPositionTracker:
    """Lightweight tracker of latest ADS-B position + velocity per ICAO.

    Updated in real-time from DF17/18 messages. Provides instantaneous
    position lookup with optional velocity-based projection for DF11 messages
    that arrive between ADS-B position updates.

    This is separate from AircraftState — it exists solely to provide
    precise position-at-DF11-time for radar localisation.
    """
    __slots__ = ("_positions", "_lock")

    def __init__(self) -> None:
        import threading
        self._lock = threading.Lock()
        # {icao: {"lat", "lon", "groundspeed_kts", "track_deg", "ts"}}
        self._positions: dict[str, dict] = {}

    def prune_stale(self, max_age_s: float = _ADSB_POSITION_MAX_AGE_S * 2) -> int:
        """Remove entries older than max_age_s. Returns count removed."""
        cutoff = time.time() - max_age_s
        with self._lock:
            stale = [icao for icao, entry in self._positions.items()
                     if entry["ts"] < cutoff]
            for icao in stale:
                del self._positions[icao]
        return len(stale)

    def size(self) -> int:
        with self._lock:
            return len(self._positions)

    def update(self, icao: str, lat: float, lon: float,
               groundspeed_kts: float | None = None,
               track_deg: float | None = None,
               ts: float | None = None) -> None:
        """Update the latest position for an ICAO from an ADS-B message."""
        if ts is None:
            ts = time.time()
        with self._lock:
            self._positions[icao] = {
                "lat": lat,
                "lon": lon,
                "groundspeed_kts": groundspeed_kts,
                "track_deg": track_deg,
                "ts": ts,
            }

    def get_position_at(self, icao: str, wall_ts: float) -> dict | None:
        """Get aircraft position at a specific wall-clock time.

        Uses the latest ADS-B position and projects forward/backward using
        velocity vector if available. Falls back to raw position if no
        velocity data is available.
        """
        with self._lock:
            entry = self._positions.get(icao)
        if entry is None:
            return None

        age = wall_ts - entry["ts"]
        if abs(age) > _ADSB_POSITION_MAX_AGE_S:
            return None  # Too stale

        lat = entry["lat"]
        lon = entry["lon"]
        groundspeed_kts = entry.get("groundspeed_kts")
        track_deg = entry.get("track_deg")

        # If within 1 second, use position directly — direct (non-interpolated)
        # positions get position_age_seconds = 0.0 per spec.
        if abs(age) <= 1.0:
            return {
                "lat": lat,
                "lon": lon,
                "interpolated": False,
                "extrapolated": False,
                "position_age_seconds": 0.0,
                "source_age_seconds": age,
                "source_wall_ts": entry["ts"],
            }

        # Try to project using velocity vector — this is interpolation, so report age
        if groundspeed_kts is not None and track_deg is not None and groundspeed_kts > 0:
            import math
            v_ms = groundspeed_kts * 0.514444
            dlat = (v_ms * age * math.cos(math.radians(track_deg))) / 111_320
            dlon = (v_ms * age * math.sin(math.radians(track_deg))) / (
                111_320 * max(math.cos(math.radians(lat)), 1e-6)
            )
            return {
                "lat": lat + dlat,
                "lon": lon + dlon,
                "interpolated": True,
                "extrapolated": True,
                "groundspeed_kts": groundspeed_kts,
                "track_deg": track_deg,
                "position_age_seconds": abs(age),
                "source_age_seconds": age,
                "source_wall_ts": entry["ts"],
            }

        # No velocity data — use position directly if reasonably fresh
        # Most aircraft don't transmit gs/track, so this is the common case
        # 5 seconds at 200m/s = ~1km position error, acceptable for radar localisation
        # This is a direct (non-interpolated) use of the position — age = 0.0 per spec.
        if abs(age) <= 5.0:
            return {
                "lat": lat,
                "lon": lon,
                "interpolated": False,
                "extrapolated": False,
                "position_age_seconds": 0.0,
                "source_age_seconds": age,
                "source_wall_ts": entry["ts"],
            }

        return None  # Stale with no velocity vector


def _signal_weight(signal_dbfs: float | None) -> float | None:
    """Convert canonical dBFS into a positive relative weight."""
    if signal_dbfs is None:
        return None
    return 10 ** (signal_dbfs / 20.0)


def refine_burst_center(reply_samples: list[tuple[float, float | None]]) -> dict:
    """Estimate a better beam-centre timestamp from per-reply timing and amplitude.

    The raw centroid remains the fallback. When multiple replies in a burst carry
    usable signal strength, use an amplitude-weighted centre so stronger replies
    pull the timestamp toward the beam centre rather than the burst edges.
    """
    arrivals_us = [arrival_us for arrival_us, _signal_dbfs in reply_samples]
    raw_centroid_us = sum(arrivals_us) / len(arrivals_us)

    weighted_samples: list[tuple[float, float]] = []
    for arrival_us, signal_dbfs in reply_samples:
        weight = _signal_weight(signal_dbfs)
        if weight is not None and weight > 0:
            weighted_samples.append((arrival_us, weight))

    if len(weighted_samples) < 2:
        return {
            "beam_center_us": raw_centroid_us,
            "beam_center_method": "centroid",
            "beam_center_simple_us": raw_centroid_us,
            "beam_center_weighted_us": None,
            "beam_center_delta_us": 0.0,
        }

    weight_sum = sum(weight for _arrival_us, weight in weighted_samples)
    if weight_sum <= 0:
        return {
            "beam_center_us": raw_centroid_us,
            "beam_center_method": "centroid",
            "beam_center_simple_us": raw_centroid_us,
            "beam_center_weighted_us": None,
            "beam_center_delta_us": 0.0,
        }

    weighted_center = (
        sum(arrival_us * weight for arrival_us, weight in weighted_samples) / weight_sum
    )
    return {
        "beam_center_us": weighted_center,
        "beam_center_method": "amplitude_weighted",
        "beam_center_simple_us": raw_centroid_us,
        "beam_center_weighted_us": weighted_center,
        "beam_center_delta_us": weighted_center - raw_centroid_us,
    }


def _compute_burst_timestamp_candidates(reply_samples: list[tuple[float, float | None]]) -> dict:
    """Return diagnostic timestamp definitions for one burst in Beast microseconds."""
    if not reply_samples:
        return {
            "burst_ts_first_reply_beast_us": None,
            "burst_ts_strongest_reply_beast_us": None,
            "burst_ts_simple_centroid_beast_us": None,
            "burst_ts_weighted_centroid_beast_us": None,
            "burst_ts_mid_strong_window_beast_us": None,
            "burst_ts_last_reply_beast_us": None,
            "burst_span_us": None,
            "peak_amplitude": None,
        }

    samples = sorted(reply_samples, key=lambda item: item[0])
    arrivals_us = [float(arrival_us) for arrival_us, _signal_dbfs in samples]
    simple_centroid = sum(arrivals_us) / len(arrivals_us)
    weighted_samples: list[tuple[float, float]] = []
    strongest_ts = None
    strongest_signal = None
    for arrival_us, signal_dbfs in samples:
        if signal_dbfs is None:
            continue
        if strongest_signal is None or signal_dbfs > strongest_signal:
            strongest_signal = signal_dbfs
            strongest_ts = float(arrival_us)
        weight = _signal_weight(signal_dbfs)
        if weight is not None and weight > 0:
            weighted_samples.append((float(arrival_us), weight))

    weighted_centroid = None
    if len(weighted_samples) >= 2:
        weight_sum = sum(weight for _arrival_us, weight in weighted_samples)
        if weight_sum > 0:
            weighted_centroid = sum(arrival_us * weight for arrival_us, weight in weighted_samples) / weight_sum

    mid_strong_window = None
    if strongest_signal is not None:
        strong_arrivals = [
            float(arrival_us)
            for arrival_us, signal_dbfs in samples
            if signal_dbfs is not None and signal_dbfs >= strongest_signal - 6.0
        ]
        if strong_arrivals:
            mid_strong_window = (min(strong_arrivals) + max(strong_arrivals)) / 2.0

    return {
        "burst_ts_first_reply_beast_us": arrivals_us[0],
        "burst_ts_strongest_reply_beast_us": strongest_ts,
        "burst_ts_simple_centroid_beast_us": simple_centroid,
        "burst_ts_weighted_centroid_beast_us": weighted_centroid,
        "burst_ts_mid_strong_window_beast_us": mid_strong_window,
        "burst_ts_last_reply_beast_us": arrivals_us[-1],
        "burst_span_us": arrivals_us[-1] - arrivals_us[0] if len(arrivals_us) > 1 else 0.0,
        "peak_amplitude": strongest_signal,
    }


def detect_bursts(arrival_us_list: list[float]) -> list[dict]:
    """Group sorted arrival times into bursts separated by BURST_GAP_US.

    Returns list of dicts: {centroid_us, n_replies, span_us, arrivals_us}
    """
    if not arrival_us_list:
        return []

    sorted_ts = (
        arrival_us_list
        if all(
            arrival_us_list[i] <= arrival_us_list[i + 1]
            for i in range(len(arrival_us_list) - 1)
        )
        else sorted(arrival_us_list)
    )
    groups: list[list[float]] = []
    current = [sorted_ts[0]]

    for ts in sorted_ts[1:]:
        if ts - current[-1] > BURST_GAP_US:
            groups.append(current)
            current = [ts]
        else:
            current.append(ts)
    groups.append(current)

    return [
        {
            "centroid_us": sum(b) // len(b),
            "n_replies": len(b),
            "span_us": b[-1] - b[0] if len(b) > 1 else 0,
            "arrivals_us": b,
        }
        for b in groups
    ]


def detect_bursts_with_signals(reply_samples: list[tuple[float, float | None]]) -> list[dict]:
    """Group sorted `(arrival_us, signal_dbfs)` samples into bursts.

    Returns list of dicts:
    `{centroid_us, n_replies, span_us, arrivals_us, replies, signal_dbfs}`
    where `signal_dbfs` is the average of the replies in that burst.
    """
    if not reply_samples:
        return []

    sorted_samples = sorted(reply_samples, key=lambda item: item[0])
    groups: list[list[tuple[float, float | None]]] = []
    current = [sorted_samples[0]]

    for sample in sorted_samples[1:]:
        if sample[0] - current[-1][0] > BURST_GAP_US:
            groups.append(current)
            current = [sample]
        else:
            current.append(sample)
    groups.append(current)

    bursts = []
    for group in groups:
        arrivals_us = [arrival_us for arrival_us, _signal_dbfs in group]
        signals = [signal_dbfs for _arrival_us, signal_dbfs in group if signal_dbfs is not None]
        replies = [
            {"arrival_us": arrival_us, "signal_dbfs": signal_dbfs}
            for arrival_us, signal_dbfs in group
        ]
        refinement = refine_burst_center(group)
        timestamp_candidates = _compute_burst_timestamp_candidates(group)
        bursts.append(
            {
                "centroid_us": sum(arrivals_us) / len(arrivals_us),
                "n_replies": len(arrivals_us),
                "span_us": arrivals_us[-1] - arrivals_us[0] if len(arrivals_us) > 1 else 0,
                "arrivals_us": arrivals_us,
                "replies": replies,
                "signal_dbfs": round(sum(signals) / len(signals), 2) if signals else None,
                "beam_center_us": refinement["beam_center_us"],
                "beam_center_method": refinement["beam_center_method"],
                "beam_center_simple_us": refinement.get("beam_center_simple_us"),
                "beam_center_weighted_us": refinement.get("beam_center_weighted_us"),
                "beam_center_delta_us": refinement.get("beam_center_delta_us"),
                **timestamp_candidates,
            }
        )

    return bursts


def analyse_icao(bursts: list[dict]) -> dict | None:
    """Compute rotation period from inter-burst intervals for one ICAO.

    Returns None if insufficient bursts.
    """
    if len(bursts) < MIN_BURSTS:
        return None

    centroids = [b["centroid_us"] for b in bursts]
    intervals_us = [centroids[i + 1] - centroids[i] for i in range(len(centroids) - 1)]
    intervals_s = [iv / 1_000_000 for iv in intervals_us]

    # Filter obviously wrong intervals (< 1s or > 30s)
    valid = [iv for iv in intervals_s if 1.0 < iv < 30.0]
    if len(valid) < 2:
        return None

    def _cluster_repeated_intervals(intervals_s: list[float]) -> list[dict]:
        clusters: list[list[float]] = []
        for interval_s in sorted(intervals_s):
            placed = False
            for cluster in clusters:
                cluster_med = statistics.median(cluster)
                if cluster_med > 0 and abs(interval_s - cluster_med) / cluster_med <= SERIES_CLUSTER_TOLERANCE:
                    cluster.append(interval_s)
                    placed = True
                    break
            if not placed:
                clusters.append([interval_s])

        series_candidates: list[dict] = []
        for cluster in clusters:
            if len(cluster) < 2:
                continue
            cluster_med = statistics.median(cluster)
            series_candidates.append(
                {
                    "period_s": cluster_med,
                    "n_intervals": len(cluster),
                    "std_s": statistics.stdev(cluster) if len(cluster) > 1 else 0.0,
                }
            )

        series_candidates.sort(
            key=lambda item: (
                item["n_intervals"],
                -item["std_s"],
                -item["period_s"],
            ),
            reverse=True,
        )
        return series_candidates

    series_candidates = _cluster_repeated_intervals(valid)
    if not series_candidates:
        return None

    strongest = series_candidates[0]
    strongest_period = strongest["period_s"]
    filtered = [iv for iv in valid if abs(iv - strongest_period) / strongest_period <= 0.5]
    if len(filtered) < 2:
        filtered = [strongest_period] * strongest["n_intervals"]

    return {
        "n_bursts": len(bursts),
        "n_intervals": strongest["n_intervals"],
        "centroids_us": centroids,
        "median_period_s": strongest_period,
        "mean_period_s": statistics.mean(filtered),
        "std_s": strongest["std_s"],
        "all_intervals_s": list(intervals_s),
        "series_candidates": series_candidates,
        "avg_replies_per_burst": round(
            statistics.mean(b["n_replies"] for b in bursts), 1
        ),
    }


def _snap_intervals(intervals_s: list[float], base_period: float,
                    tolerance: float = 0.08) -> dict:
    """Check how well individual intervals snap to integer multiples of base_period."""
    mult_counts: dict[int, int] = defaultdict(int)
    non_snapped = []

    for iv in intervals_s:
        if iv <= 0:
            continue
        ratio = iv / base_period
        nearest = round(ratio)
        if nearest < 1:
            nearest = 1
        if abs(ratio - nearest) / nearest < tolerance:
            mult_counts[nearest] += 1
        else:
            non_snapped.append(iv)

    total = len(intervals_s)
    n_snapped = sum(mult_counts.values())
    snap_rate = n_snapped / total if total > 0 else 0.0

    total_sweeps = sum(n * c for n, c in mult_counts.items())
    implied_detect = n_snapped / total_sweeps if total_sweeps > 0 else 0.0

    return {
        "snap_rate": snap_rate,
        "mult_counts": dict(sorted(mult_counts.items())),
        "non_snapped": non_snapped,
        "implied_detect": implied_detect,
    }


def _evaluate_base_candidate(
    candidate: float,
    icao_results: dict[str, dict],
    tolerance: float = 0.05,
) -> dict:
    """Score one base-period candidate against all ICAO burst series.

    A shorter false period can often explain a true dominant family only as ×2/×3
    harmonics. To avoid collapsing to that artefact, candidates are ranked by a
    weighted support score that prefers direct ×1 alignments over harmonic-only fits.
    """
    folded: dict[str, dict] = {}
    residual: dict[str, float] = {}
    support_weight = 0.0
    direct_count = 0
    folded_count = 0

    def _pick_centroid_sequence(centroids_us: list[int]) -> tuple[list[int], float]:
        if candidate <= 0 or len(centroids_us) < 3:
            return [], 0.0

        best_sequence: list[int] = []
        best_error = float("inf")
        for anchor_idx, anchor_us in enumerate(centroids_us):
            matched = [anchor_us]
            error_sum = 0.0
            for centroid_us in centroids_us[anchor_idx + 1:]:
                delta_s = (centroid_us - anchor_us) / 1_000_000.0
                nearest = round(delta_s / candidate)
                if nearest < 1:
                    continue
                frac_error = abs(delta_s - (nearest * candidate)) / candidate
                if frac_error <= 0.12:
                    matched.append(centroid_us)
                    error_sum += frac_error

            if len(matched) > len(best_sequence) or (len(matched) == len(best_sequence) and error_sum < best_error):
                best_sequence = matched
                best_error = error_sum

        coverage = len(best_sequence) / len(centroids_us) if centroids_us else 0.0
        return best_sequence, coverage

    for icao, result in icao_results.items():
        period = result["median_period_s"]
        series_candidates = result.get("series_candidates") or [
            {"period_s": period, "n_intervals": result.get("n_intervals", 0), "std_s": result.get("std_s", 0.0)}
        ]
        best_match = None
        for series in series_candidates:
            series_period = series["period_s"]
            ratio = series_period / candidate
            nearest_int = round(ratio)
            if nearest_int < 1:
                nearest_int = 1
            relative_err = abs(ratio - nearest_int) / nearest_int
            if relative_err >= tolerance:
                continue
            match = {
                "period_s": series_period,
                "n_intervals": series.get("n_intervals", 0),
                "nearest_int": nearest_int,
                "relative_err": relative_err,
                "std_s": series.get("std_s", 0.0),
            }
            if best_match is None or (
                match["nearest_int"] == 1,
                match["n_intervals"],
                -match["relative_err"],
                -match["std_s"],
            ) > (
                best_match["nearest_int"] == 1,
                best_match["n_intervals"],
                -best_match["relative_err"],
                -best_match["std_s"],
            ):
                best_match = match

        if best_match is not None:
            nearest_int = best_match["nearest_int"]
            detection_rate = 1.0 / nearest_int
            folded[icao] = {
                "raw_period_s": best_match["period_s"],
                "multiplier": nearest_int,
                "folded_period_s": best_match["period_s"] / nearest_int,
                "detection_rate": detection_rate,
                "method": "median",
                "snap_info": None,
            }
            folded_count += 1
            support_weight += best_match["n_intervals"] * detection_rate
            if nearest_int == 1:
                direct_count += 1
        else:
            residual[icao] = period

    # Pass 2: interval-level snap check for residuals
    snap_threshold = 0.80
    still_residual: dict[str, float] = {}

    for icao in list(residual.keys()):
        all_intervals = icao_results[icao].get("all_intervals_s", [])
        valid_intervals = [iv for iv in all_intervals if 1.0 < iv < 30.0]
        if len(valid_intervals) < 3:
            snap = None
        else:
            snap = _snap_intervals(valid_intervals, candidate)

        if snap is not None and snap["snap_rate"] >= snap_threshold:
            mult_counts = snap["mult_counts"]
            dom_mult = max(mult_counts, key=mult_counts.get) if mult_counts else 1
            folded[icao] = {
                "raw_period_s": residual[icao],
                "multiplier": dom_mult,
                "folded_period_s": candidate,
                "detection_rate": snap["implied_detect"],
                "method": "interval",
                "snap_info": snap,
            }
            folded_count += 1
            support_weight += icao_results[icao].get("n_intervals", 0) * snap["implied_detect"]
            if dom_mult == 1:
                direct_count += 1
            continue

        centroids_us = icao_results[icao].get("centroids_us", [])
        sequence, coverage = _pick_centroid_sequence(centroids_us)
        if len(sequence) >= 3 and coverage >= 0.6:
            observed_span_s = (sequence[-1] - sequence[0]) / 1_000_000.0 if len(sequence) > 1 else 0.0
            implied_sweeps = max(1, round(observed_span_s / candidate))
            detection_rate = ((len(sequence) - 1) / implied_sweeps) if implied_sweeps > 0 else 0.0
            folded[icao] = {
                "raw_period_s": residual[icao],
                "multiplier": 1,
                "folded_period_s": candidate,
                "detection_rate": detection_rate,
                "method": "centroid",
                "snap_info": {
                    "matched_centroids": sequence,
                    "coverage": coverage,
                },
            }
            folded_count += 1
            support_weight += max(len(sequence) - 1, 1) * max(detection_rate, coverage)
            direct_count += 1
            continue

        still_residual[icao] = residual[icao]

    return {
        "dominant_period_s": candidate,
        "folded": folded,
        "residual": still_residual,
        "support_weight": support_weight,
        "direct_count": direct_count,
        "folded_count": folded_count,
    }


def _fold_harmonics(icao_results: dict[str, dict], tolerance: float = 0.05) -> dict:
    """Detect and fold missed-sweep harmonics into the dominant period.

    Candidate base periods are scored across all ICAOs. The chosen dominant period
    should match the strongest visible family, rather than a shorter artefactual
    period that only explains the true family as harmonics.
    """
    if not icao_results:
        return {"dominant_period_s": None, "folded": {}, "residual": {}}

    candidate_values = sorted([
        series["period_s"]
        for result in icao_results.values()
        for series in (result.get("series_candidates") or [])
        if series.get("period_s") is not None
    ] + [
        result["median_period_s"]
        for result in icao_results.values()
        if result.get("median_period_s") is not None
    ])
    candidates: list[float] = []
    for value in candidate_values:
        if not candidates or abs(value - candidates[-1]) > 1e-9:
            candidates.append(value)
    if not candidates:
        return {"dominant_period_s": None, "folded": {}, "residual": {}}

    evaluations = [
        _evaluate_base_candidate(candidate, icao_results, tolerance=tolerance)
        for candidate in candidates
    ]
    evaluations.sort(
        key=lambda item: (
            item["direct_count"],
            item["support_weight"],
            item["folded_count"],
            -len(item["residual"]),
            -item["dominant_period_s"],
        ),
        reverse=True,
    )
    best = evaluations[0]
    return {
        "dominant_period_s": best["dominant_period_s"],
        "folded": best["folded"],
        "residual": best["residual"],
        "support_weight": best["support_weight"],
        "direct_count": best["direct_count"],
        "folded_count": best["folded_count"],
    }


def _analyse_iid_events(
    events_for_iid: list[tuple[int, int, str, float | None]]
) -> RotationModel:
    """Run full rotation analysis for one IID.

    events_for_iid: [(arrival_us, iid, icao, signal_dbfs), ...] filtered to one IID.
    Returns a RotationModel.
    """
    # Group by ICAO
    icao_arrivals: dict[str, list[int]] = defaultdict(list)
    for arrival_us, _iid, icao, _sig in events_for_iid:
        if icao:
            icao_arrivals[icao].append(arrival_us)

    icao_results: dict[str, dict] = {}
    for icao, arrivals in icao_arrivals.items():
        bursts = detect_bursts(arrivals)
        result = analyse_icao(bursts)
        if result is not None:
            icao_results[icao] = result

    if not icao_results:
        return RotationModel(
            status="INSUFFICIENT_DATA",
            n_qualifying=0,
            last_updated=time.time(),
        )

    harmonics = _fold_harmonics(icao_results)
    dominant = harmonics["dominant_period_s"]
    final_residual = dict(harmonics["residual"])
    n_residual = len(final_residual)

    folded_periods = [
        f["folded_period_s"] for f in harmonics["folded"].values()
    ]
    if not folded_periods:
        folded_periods = [r["median_period_s"] for r in icao_results.values()]

    spread = max(folded_periods) - min(folded_periods)
    overall_std = statistics.stdev(folded_periods) if len(folded_periods) > 1 else 0.0

    if n_residual == 0 and spread < 0.1:
        verdict = "SINGLE_RADAR"
    elif n_residual == 0 and spread < 0.5:
        verdict = "LIKELY_SINGLE"
    elif n_residual > 0:
        verdict = "CHECK_MULTI"
    else:
        verdict = "CHECK_MULTI"

    n_harmonic = sum(
        1 for f in harmonics["folded"].values() if f["multiplier"] > 1
    )

    rpm = 60.0 / dominant if dominant and dominant > 0 else None

    return RotationModel(
        dominant_period_s=dominant,
        secondary_period_s=None,
        primary_direct_count=harmonics.get("direct_count", 0),
        secondary_direct_count=0,
        period_std_s=round(overall_std, 6),
        status=verdict,
        n_qualifying=len(icao_results),
        n_harmonic=n_harmonic,
        n_residual=n_residual,
        rpm=round(rpm, 3) if rpm is not None else None,
        folded=harmonics["folded"],
        secondary_folded={},
        residual=final_residual,
        last_updated=time.time(),
    )


def _analyse_burst_records(records: list[BurstRecord]) -> RotationModel:
    """Run full rotation analysis for one IID from BurstRecord objects.

    Equivalent to _analyse_iid_events but consumes already-computed burst
    centroids rather than re-running burst detection on raw arrival timestamps.
    records: BurstRecord list filtered to one IID, within the analysis window.
    """
    icao_bursts: dict[str, list[dict]] = defaultdict(list)
    for r in records:
        if r.icao:
            icao_bursts[r.icao].append({"centroid_us": r.centroid_us, "n_replies": r.n_replies})
    for bursts in icao_bursts.values():
        bursts.sort(key=lambda b: b["centroid_us"])

    icao_results: dict[str, dict] = {}
    for icao, bursts in icao_bursts.items():
        result = analyse_icao(bursts)
        if result is not None:
            icao_results[icao] = result

    if not icao_results:
        return RotationModel(
            status="INSUFFICIENT_DATA",
            n_qualifying=0,
            last_updated=time.time(),
        )

    harmonics = _fold_harmonics(icao_results)
    dominant = harmonics["dominant_period_s"]
    final_residual = dict(harmonics["residual"])
    n_residual = len(final_residual)

    folded_periods = [f["folded_period_s"] for f in harmonics["folded"].values()]
    if not folded_periods:
        folded_periods = [r["median_period_s"] for r in icao_results.values()]

    spread = max(folded_periods) - min(folded_periods)
    overall_std = statistics.stdev(folded_periods) if len(folded_periods) > 1 else 0.0

    if n_residual == 0 and spread < 0.1:
        verdict = "SINGLE_RADAR"
    elif n_residual == 0 and spread < 0.5:
        verdict = "LIKELY_SINGLE"
    else:
        verdict = "CHECK_MULTI"

    n_harmonic = sum(1 for f in harmonics["folded"].values() if f["multiplier"] > 1)
    rpm = 60.0 / dominant if dominant and dominant > 0 else None

    return RotationModel(
        dominant_period_s=dominant,
        secondary_period_s=None,
        primary_direct_count=harmonics.get("direct_count", 0),
        secondary_direct_count=0,
        period_std_s=round(overall_std, 6),
        status=verdict,
        n_qualifying=len(icao_results),
        n_harmonic=n_harmonic,
        n_residual=n_residual,
        rpm=round(rpm, 3) if rpm is not None else None,
        folded=harmonics["folded"],
        secondary_folded={},
        residual=final_residual,
        last_updated=time.time(),
    )


def _periods_match(period_a: float | None, period_b: float | None, tolerance: float = PERIOD_MATCH_TOLERANCE) -> bool:
    if period_a is None or period_b is None or period_a <= 0 or period_b <= 0:
        return False
    return abs(period_a - period_b) / max(period_a, period_b) <= tolerance


def _blend_period(existing_period: float | None, support_count: int, observed_period: float) -> tuple[float, int]:
    if existing_period is None or existing_period <= 0:
        return observed_period, 1
    weight = max(1, min(support_count, SUPPORT_CAP))
    blended = ((existing_period * weight) + observed_period) / (weight + 1)
    return blended, min(weight + 1, SUPPORT_CAP)


def _confidence_from_support(support_count: int, target: int) -> float:
    if target <= 0:
        return 0.0
    return round(min(support_count / target, 1.0), 3)


def _reinforce_period_slot(existing_period: float | None, support_count: int, observed_period: float | None) -> tuple[float | None, int]:
    if observed_period is None:
        return existing_period, support_count
    if existing_period is None:
        return observed_period, 1
    if _periods_match(existing_period, observed_period):
        return _blend_period(existing_period, support_count, observed_period)
    return existing_period, support_count


def _reinforce_radar_characteristics(radar_iid: RadarIID, model: RotationModel) -> None:
    """Maintain a long-term reinforced primary/secondary characteristic per IID."""
    observed_periods = [period for period in (model.dominant_period_s,) if period is not None]

    primary_period = radar_iid.period_s
    primary_support = radar_iid.primary_support_count

    for observed_period in observed_periods:
        support_gain = 0
        if model.dominant_period_s is not None and _periods_match(model.dominant_period_s, observed_period):
            support_gain = max(support_gain, model.primary_direct_count)

        if primary_period is not None and _periods_match(primary_period, observed_period):
            if support_gain > 0:
                primary_period, blended_support = _blend_period(primary_period, primary_support, observed_period)
                primary_support = min(blended_support + support_gain - 1, SUPPORT_CAP)
            continue
        if primary_period is None:
            if support_gain > 0:
                primary_period, primary_support = observed_period, support_gain
            continue

        if support_gain > 0:
            primary_support = max(primary_support - 1, 0)
            replacement_threshold = max(MIN_QUALIFYING_ICAOS, primary_support + 1)
            if support_gain >= replacement_threshold:
                primary_period = observed_period
                primary_support = min(support_gain, SUPPORT_CAP)
            continue

    radar_iid.period_s = primary_period
    radar_iid.secondary_period_s = None
    radar_iid.primary_support_count = primary_support
    radar_iid.secondary_support_count = 0
    radar_iid.period_std_s = model.period_std_s
    radar_iid.rpm = round(60.0 / primary_period, 3) if primary_period else None

    primary_confidence = _confidence_from_support(primary_support, PRIMARY_CONFIDENCE_TARGET)

    if primary_period is not None and primary_confidence >= 0.75:
        radar_iid.status = "SINGLE_RADAR"
        radar_iid.multi_radar_flag = False
    elif primary_period is not None and primary_confidence >= 0.35:
        radar_iid.status = "LIKELY_SINGLE"
        radar_iid.multi_radar_flag = False
    else:
        radar_iid.status = model.status
        radar_iid.multi_radar_flag = model.status in ("MULTI_RADAR", "CHECK_MULTI")


class RadarState:
    """In-process IID event accumulator and rotation model store.

    Consumes raw Beast frames via on_frame(), accumulates DF11 events,
    and builds per-IID rotation models on a background schedule.
    """

    def __init__(
        self,
        aircraft_state: Optional["AircraftState"] = None,
        track_store: Optional[Any] = None,
        receiver_lat: Optional[float] = None,
        receiver_lon: Optional[float] = None,
    ) -> None:
        self._aircraft_state = aircraft_state
        self._track_store = track_store
        self._receiver_lat: float = receiver_lat or 0.0
        self._receiver_lon: float = receiver_lon or 0.0

        # Diagnostics flag: controls whether expensive debug structures are populated.
        # Read once at startup from config so the hot path avoids attribute lookup cost.
        try:
            import config as _cfg
            self._diagnostics_enabled: bool = bool(getattr(_cfg, "RADAR_DIAGNOSTICS", False))
        except Exception:
            self._diagnostics_enabled = False

        # When True, FRAME_READY messages from radar-core are routed to the FM
        # mailbox and Python's own frame-finalization step is suppressed.
        # Set via enable_radar_core_frames() from main.py after startup.
        self._radar_core_frames_enabled: bool = False
        self._radar_core_frames_injected: int = 0
        self._radar_core_frame_inject_errors: int = 0
        self._radar_core_frames_injected_by_iid: dict[int, int] = {}
        self._python_frames_finalized_total: int = 0
        self._python_frames_finalized_by_iid: dict[int, int] = {}
        self._go_fm_states: dict[int, dict] = {}
        self._go_fm_pipeline_stats: dict[int, dict] = {}
        self._GO_FRAME_POSITIONS_MAX = 5000
        self._go_frame_positions: dict[int, deque] = {}
        self._go_frame_positions_revision: dict[int, int] = {}
        self._GO_TRACK_OBSERVATIONS_MAX = 8_000
        self._GO_EVIDENCE_EVENTS_MAX = 12_000
        self._GO_SWEEP_FRAMES_MAX = 256
        self._go_sync_states_by_iid: dict[int, dict] = {}
        self._go_multi_sync_states_by_iid: dict[int, dict] = {}
        self._go_multi_sync_admission_by_iid: dict[int, dict] = {}
        self._compact_sync_debug_by_iid: dict[int, dict] = {}
        self._go_iid_state_revision: dict[int, int] = {}
        self._go_track_observations: deque = deque(maxlen=self._GO_TRACK_OBSERVATIONS_MAX)
        self._go_evidence_events: deque = deque(maxlen=self._GO_EVIDENCE_EVENTS_MAX)
        self._go_sweep_frames_by_iid: dict[int, deque] = {}
        self._go_sweep_frames_revision: dict[int, int] = {}
        self._go_reference_aircraft_by_iid: dict[int, dict] = {}
        self._go_snapshot_payload: dict | None = None

        # Lightweight ADS-B position tracker for real-time position capture
        self._adsb_tracker = AircraftPositionTracker()

        # Long-lived DF11 event deque: (arrival_us, iid, icao, signal_dbfs)
        # Pruned by age in update_rotation_models(); maxlen caps worst-case growth
        # between pruning cycles at high DF11 rates (oldest events evicted first).
        self._iid_events: deque[tuple[float, int, str, float | None]] = deque(maxlen=_IID_EVENTS_MAX)
        self._iid_latest_arrival_us: dict[int, float] = {}

        # Per-IID compact burst records: {iid: deque[BurstRecord]}
        # One record per fired burst; replaces _iid_events as the source for
        # rotation analysis. Pruned to BURST_RECORD_MAX_AGE_S plus a bounded
        # density-aware per-IID count cap in _append_burst_record().
        self._burst_records: dict[int, deque] = {}

        # Wrap-around unwrapping state (mirrors IIDAccumulator in probe tool)
        self._base_ticks: int | None = None
        self._last_raw: int = 0
        self._wrap_offset: int = 0
        self._timing_epoch: int = 0

        # Per-IID rotation models {iid: RadarIID}
        self._models: dict[int, RadarIID] = {}
        self._dirty_iids: set[int] = set()

        # Per-IID burst lists for the sweep waterfall endpoint
        # {iid: deque[(centroid_us, n_aircraft, [{icao, signal_dbfs}])]}
        self._sweep_history: dict[int, deque] = {}
        self._SWEEP_HISTORY_MAX = 200  # keep last 200 sweeps per IID

        # Pending calibration pairs not yet flushed to DB
        self._pending_pairs: list[CalibrationPair] = []
        self._seen_pair_keys: set[tuple] = set()

        # ------------------------------------------------------------------
        # Per-IID live SweepFrame builder — processes each DF11 as it arrives.
        # ------------------------------------------------------------------
        # Per-IID per-aircraft pending burst replies: {iid: {icao: [(arrival_us, signal), ...]}}
        self._live_bursts: dict[int, dict[str, list[tuple[float, float | None]]]] = {}
        # Diagnostic-only mirror of pending burst replies for the native burst path.
        # The operational native processor still owns burst firing and centroid
        # selection; this mirror only preserves reply timing so sync-debug can
        # compare alternate timestamp definitions on the same fired bursts.
        self._live_burst_diagnostic_replies: dict[int, dict[str, list[tuple[float, float | None]]]] = {}
        # Per-IID last arrival time per aircraft (for gap detection): {iid: {icao: arrival_us}}
        self._live_last_arrival: dict[int, dict[str, float]] = {}
        # Per-IID per-aircraft completed burst centroid history: {iid: {icao: [centroid_us, ...]}}
        # Used by reference selection — inter-centroid intervals are radar-period-sized.
        self._live_burst_centroids: dict[int, dict[str, list[float]]] = {}
        # Per-IID current frame being built: {iid: LiveFrameState | None}
        self._live_frames: dict[int, "LiveFrameState | None"] = {}
        # Per-IID last accepted frame-start timestamp to suppress near-duplicate openings.
        self._live_last_frame_start_us: dict[int, float] = {}
        # Per-IID completed frames: {iid: deque of SweepFrame} — bounded to FM working window.
        # FM scorer uses last 100 frames; 120 gives 20 frames headroom for quality filtering.
        self._LIVE_FRAMES_MAX = 120
        self._live_completed_frames: dict[int, deque] = {}
        # Monotonically increasing frame counter per IID — never resets when the deque wraps,
        # so frame_index stays unique even after the ring buffer fills.
        self._live_frame_counters: dict[int, int] = {}
        self._native_burst_processors: dict[int, Any] = {}
        # Diagnostic-only completed burst replies for dwell-profile lookup after
        # sweep history is rebuilt from compact BurstRecord summaries.
        self._dwell_profiles: dict[int, deque] = {}
        self._DWELL_PROFILE_MAX = 1_000

        # Real-time DF11 flash events for the sweep diagram.
        # Written from the decoder thread; read from the async event loop.
        # Sequence counter lets the frontend poll for new events since last fetch.
        # Bounded deque prevents unbounded growth; 2000 events ≈ a few seconds at high rate.
        self._flash_events: deque[tuple[int, int, str, int]] = deque(maxlen=2000)
        self._flash_seq: int = 0

        # ------------------------------------------------------------------
        # Stage 3 live state — sync states and detection buffer.
        # These are updated from the decoder thread and read by the localiser.
        # ------------------------------------------------------------------
        # Per-IID live sync state; updated each time a sweep frame is completed.
        self._live_sync_states: dict[int, LiveSyncState] = {}
        # Per-IID rolling buffer of aligned burst observations for multi-aircraft sync.
        # Each entry is one dominant-family burst with a known ADS-B position and
        # pre-computed geometric bearing.  Used by _update_multi_aircraft_sync_state().
        # Retention is time-window-first: keep enough depth to satisfy the largest
        # supported sync window requests under normal burst rates, then bound memory
        # with a high count cap.
        # API/websocket window_s allows up to 300s; keep 6 minutes in-memory so
        # 300s windows survive normal burst-rate variation and reconnect jitter.
        self._LIVE_SYNC_OBS_RETENTION_S = 360.0
        # Hard caps remain as safety bounds.  Sized for ~8–15 burst observations/s
        # with retention headroom while avoiding unbounded growth.
        self._MULTI_SYNC_OBS_MAX = 6_000
        self._live_aligned_burst_obs: dict[int, deque] = {}  # {iid: deque[AlignedBurstSyncObs]}
        # Per-IID burst observation buffer for UI timeline rendering. This is broader
        # than _live_aligned_burst_obs: it includes all burst-centre observations that
        # can be compared against the maintained sync model, even when they are not
        # eligible to steer sync updates.
        # Keep a higher cap than sync-maintenance so 60–300s timeline/debug windows
        # remain populated at real traffic rates.
        self._BURST_SYNC_TIMELINE_OBS_MAX = 8_000
        self._live_burst_timeline_obs: dict[int, deque] = {}  # {iid: deque[AlignedBurstSyncObs]}
        # Bounded buffer of recent Stage 3-usable live detections.
        # One entry per fired burst; Stage 3 solver queries with max_age_s=30.
        # At Pi density (~50 aircraft, 15 bursts/min each = ~12/s × 30s ≈ 375 entries).
        self._LIVE_DETECTION_BUFFER_MAX = 2_000
        self._live_detection_buffer: deque[Stage3LiveDetection] = deque(maxlen=self._LIVE_DETECTION_BUFFER_MAX)

        import threading
        self._lock = threading.Lock()
        self._update_active = threading.Event()

        # Injected at startup; called outside _lock for each completed good/marginal frame.
        # Signature: (iid: int, frame: SweepFrame, period_s: float) -> None
        # Kept for backwards-compat / tests, but the hot path now uses the per-IID
        # mailbox below rather than invoking this callback inline.
        self.per_frame_solve_callback = None

        # Shadow tap for radar-core Stage 1.
        # If set, called with (arrival_us: float, iid: int, icao: int, signal_dbfs: float|None)
        # for each unwrapped DF11 event, outside any lock.
        # The tap must be non-blocking; failures are silently swallowed.
        self.radar_core_event_sink = None

        # Per-IID latest-frame mailbox: a background FM worker consumes frames
        # from here so the radar worker thread is never blocked by FM solves.
        # "Latest wins": a newer frame for the same IID overwrites an unsolved
        # older frame — we only care about the most recent estimate.
        self._fm_mailbox_lock = threading.Lock()
        self._fm_mailbox: dict[int, tuple] = {}  # {iid: (frame, period_s)}
        self._fm_mailbox_event = threading.Event()

        # Per-IID throttle timestamps for _update_multi_aircraft_sync_state.
        # Rolling-fit work is skipped if the last update fired recently; the
        # observation buffer keeps growing in the meantime so the next update
        # sees the full set.
        self._MULTI_SYNC_UPDATE_MIN_INTERVAL_S = 0.25
        self._last_multi_sync_update_ts: dict[int, float] = {}

        # Per-IID empirical phase-in-rotation waveform model (circular bins).
        # Learned slowly from residuals of the refined sync model; subtracted
        # from predicted bearings when coverage is sufficient.
        self._live_waveform_bins: dict[int, list[WaveformBin]] = {}
        # Per-IID per-ICAO residual quality memory, used to downweight repeatedly
        # noisy aircraft in slope fitting and waveform learning.
        self._live_icao_sync_quality: dict[int, dict[str, IcaoSyncQuality]] = {}
        # Per-IID short histories for convergence diagnostics.
        self._live_period_update_history: dict[int, deque] = {}
        self._live_slope_history: dict[int, deque] = {}
        self._live_period_history: dict[int, deque] = {}
        self._live_period_clean_update_streak: dict[int, int] = {}
        self._live_sync_snapshot_cache: dict[int, tuple[tuple, dict]] = {}
        self._live_sync_snapshot_seq: dict[int, int] = {}
        self._live_sync_snapshot_last_cache_hit: dict[int, bool] = {}
        self._live_sweep_frame_summary_cache: dict[int, dict] = {}
        self._live_sweep_frame_summary_signature: dict[int, tuple] = {}
        self._live_sweep_frame_summary_revision: dict[int, int] = {}
        self._live_sweep_frame_summary_last_cache_hit: dict[int, bool] = {}

        # Per-IID rotation-analysis gating: (last event count, last run ts).
        # update_rotation_models() uses these to skip _analyse_iid_events for
        # IIDs whose event stream has not meaningfully grown since last run.
        self._rotation_analysis_meta: dict[int, tuple[int, float]] = {}
        # Dynamic retention observability for burst-record and analysis caps.
        self._burst_record_dynamic_cap_by_iid: dict[int, int] = {}
        self._burst_record_cap_hits_by_iid: dict[int, int] = {}
        self._burst_record_last_active_aircraft_by_iid: dict[int, int] = {}
        self._rotation_analysis_dynamic_cap_by_iid: dict[int, int] = {}
        self._rotation_analysis_cap_hits_by_iid: dict[int, int] = {}
        self._rotation_analysis_last_active_aircraft_by_iid: dict[int, int] = {}
        # Minimum new-event delta required to re-run analysis for an already
        # established IID.  Lower deltas are deferred until the next cycle.
        self._ROTATION_ANALYSIS_MIN_DELTA = 40

    # ------------------------------------------------------------------
    # Frame ingestion
    # ------------------------------------------------------------------

    def _unwrap(self, raw_ticks: int) -> int:
        """Unwrap Beast 48-bit counter to a monotonic tick count."""
        raw_ticks &= BEAST_TS_MODULUS - 1
        if self._base_ticks is None:
            self._base_ticks = raw_ticks
            self._last_raw = raw_ticks
            return 0

        if raw_ticks < self._last_raw:
            backward = self._last_raw - raw_ticks
            if backward > BEAST_TS_MODULUS // 2:
                self._wrap_offset += BEAST_TS_MODULUS
                self._timing_epoch += 1
                # Reset base to avoid huge monotonic values after wrap
                self._base_ticks = raw_ticks + self._wrap_offset

        self._last_raw = raw_ticks
        return (raw_ticks + self._wrap_offset - self._base_ticks + BEAST_TS_MODULUS) % BEAST_TS_MODULUS

    def on_frame(self, frame: dict) -> None:
        """Process one raw Beast frame dict.

        Only DF11 frames are processed; all others return immediately.
        ADS-B positions are fetched in bulk via get_position_histories_bulk()
        inside update_rotation_models() (every 30s), matched to each burst
        by Beast timestamp for sub-second precision.
        """
        try:
            import decode_cffi  # imported lazily — avoids top-level import issues
            raw = frame.get("raw")
            if raw is None:
                return

            timestamp = frame.get("timestamp", 0)
            signal = frame.get("signal", 0)

            nd = decode_cffi.decode_message(raw, signal=signal)
            if nd is None:
                return

            df = nd.get("df")
            icao = nd.get("addr")
            if not icao:
                return
            icao_hex = f"{icao:06X}"

            # Capture DF11 IID events
            if df != 11:
                return

            iid = int(nd.get("iid", 0))
            if iid == 0:
                return

            signal_dbfs = -(signal / 2.0) if signal else None

            ticks = self._unwrap(timestamp)
            arrival_us = ticks / BEAST_TICKS_PER_US

            with self._lock:
                self._iid_events.append((arrival_us, iid, icao_hex, signal_dbfs))
                self._iid_latest_arrival_us[iid] = arrival_us
                self._dirty_iids.add(iid)

            if self._diagnostics_enabled:
                self._flash_seq += 1
                self._flash_events.append((self._flash_seq, iid, icao_hex, int(arrival_us)))

            # Live frame building — process each DF11 as it arrives.
            self._on_df11_frame_builder(iid, icao_hex, arrival_us, signal_dbfs)

        except Exception:
            pass  # never let radar errors affect the main decode path

    def on_df11_event(self, timestamp: int, iid: int, icao_hex: str, signal_dbfs: float | None) -> None:
        """Process a pre-decoded DF11 event from the main decoder path.

        This avoids decoding the same Beast frame twice on the decoder thread.
        """
        self.on_df11_batch([(timestamp, iid, icao_hex, signal_dbfs)])

    def _ensure_live_builder_state(self, iid: int) -> None:
        if iid not in self._live_bursts:
            self._live_bursts[iid] = {}
            self._live_last_arrival[iid] = {}
            self._live_burst_centroids[iid] = {}
            self._live_frames[iid] = None
            self._live_completed_frames[iid] = deque(maxlen=self._LIVE_FRAMES_MAX)
            self._live_frame_counters[iid] = 0
        if iid not in self._live_burst_diagnostic_replies:
            self._live_burst_diagnostic_replies[iid] = {}
        if iid not in self._live_aligned_burst_obs:
            self._live_aligned_burst_obs[iid] = deque(maxlen=self._MULTI_SYNC_OBS_MAX)
        if iid not in self._live_burst_timeline_obs:
            self._live_burst_timeline_obs[iid] = deque(maxlen=self._BURST_SYNC_TIMELINE_OBS_MAX)

    @staticmethod
    def _density_scaled_cap(
        active_aircraft: int,
        *,
        target_sweeps_per_aircraft: float,
        headroom: float,
        min_cap: int,
        max_cap: int,
    ) -> int:
        active = max(int(active_aircraft), 1)
        scaled = int(round(active * target_sweeps_per_aircraft * headroom))
        return max(min_cap, min(max_cap, scaled))

    def _estimate_active_aircraft_for_iid(self, iid: int, now_us: float | None = None) -> int:
        """Estimate currently active aircraft for one IID from recent burst activity."""
        last_arrival = self._live_last_arrival.get(iid, {})
        pending = self._live_bursts.get(iid, {})
        try:
            last_arrivals = list(last_arrival.values())
        except RuntimeError:
            last_arrivals = []
        active = len(last_arrivals)
        if now_us is not None and now_us > 0 and last_arrivals:
            cutoff_us = now_us - (_ACTIVE_AIRCRAFT_LOOKBACK_S * 1_000_000.0)
            active = sum(1 for ts in last_arrivals if ts >= cutoff_us)
        active = max(active, len(pending))
        if active <= 0:
            br_deque = self._burst_records.get(iid)
            if br_deque:
                try:
                    active = len({rec.icao for rec in list(br_deque)})
                except RuntimeError:
                    active = 1
        return max(active, 1)

    def _burst_record_dynamic_cap(self, iid: int, *, now_us: float | None = None) -> int:
        active = self._estimate_active_aircraft_for_iid(iid, now_us=now_us)
        cap = self._density_scaled_cap(
            active,
            target_sweeps_per_aircraft=_BURST_RECORDS_TARGET_SWEEPS_PER_AIRCRAFT,
            headroom=_BURST_RECORDS_HEADROOM,
            min_cap=_BURST_RECORDS_MIN_PER_IID,
            max_cap=_BURST_RECORDS_MAX_PER_IID,
        )
        self._burst_record_dynamic_cap_by_iid[iid] = cap
        self._burst_record_last_active_aircraft_by_iid[iid] = active
        return cap

    def _rotation_analysis_dynamic_cap(self, iid: int, *, now_us: float | None = None) -> int:
        active = self._estimate_active_aircraft_for_iid(iid, now_us=now_us)
        cap = self._density_scaled_cap(
            active,
            target_sweeps_per_aircraft=_ROTATION_ANALYSIS_TARGET_SWEEPS_PER_AIRCRAFT,
            headroom=_ROTATION_ANALYSIS_HEADROOM,
            min_cap=_ROTATION_ANALYSIS_MIN_EVENTS_PER_IID,
            max_cap=_ROTATION_ANALYSIS_MAX_EVENTS_PER_IID,
        )
        self._rotation_analysis_dynamic_cap_by_iid[iid] = cap
        self._rotation_analysis_last_active_aircraft_by_iid[iid] = active
        return cap

    def _append_burst_record(self, iid: int, record: BurstRecord) -> None:
        """Append one BurstRecord and enforce age + density-aware bounded retention."""
        records = self._burst_records.setdefault(iid, deque())
        records.append(record)

        cutoff_us = record.centroid_us - (BURST_RECORD_MAX_AGE_S * 1_000_000.0)
        while records and records[0].centroid_us < cutoff_us:
            records.popleft()

        cap = self._burst_record_dynamic_cap(iid, now_us=record.centroid_us)
        if len(records) > cap:
            dropped = len(records) - cap
            for _ in range(dropped):
                records.popleft()
            self._burst_record_cap_hits_by_iid[iid] = (
                self._burst_record_cap_hits_by_iid.get(iid, 0) + dropped
            )

    _ICAO_STATE_PRUNE_INTERVAL_S = 120.0  # prune at most once every 2 minutes
    _icao_state_last_prune_ts: float = 0.0

    def _prune_live_icao_state(self, max_icao_age_s: float = 300.0) -> None:
        """Prune stale per-ICAO entries from per-IID-per-ICAO dicts.

        Called from update_rotation_models() to prevent indefinite growth as
        aircraft churn through the tracked set.  max_icao_age_s defaults to
        5 minutes — a generous window that keeps data for any aircraft still
        transmitting at ~1 Hz.
        """
        now = time.time()
        if now - self._icao_state_last_prune_ts < self._ICAO_STATE_PRUNE_INTERVAL_S:
            return
        self._icao_state_last_prune_ts = now
        icao_quality_cutoff = now - max_icao_age_s

        with self._lock:
            # _live_icao_sync_quality: {iid: {icao: IcaoSyncQuality}}
            # IcaoSyncQuality has a last_ts field set each time it is updated.
            for iid_quality in self._live_icao_sync_quality.values():
                stale = [icao for icao, q in iid_quality.items()
                         if q.last_ts < icao_quality_cutoff]
                for icao in stale:
                    del iid_quality[icao]

            # _live_last_arrival: {iid: {icao: arrival_us}} (Beast µs, not wall time).
            # Prune ICAOs absent from _live_icao_sync_quality (already confirmed stale).
            for iid, last_arr in self._live_last_arrival.items():
                quality_dict = self._live_icao_sync_quality.get(iid, {})
                stale = [icao for icao in last_arr if icao not in quality_dict]
                for icao in stale:
                    del last_arr[icao]

            # _live_burst_diagnostic_replies: {iid: {icao: [...]}}
            for iid, burst_map in self._live_burst_diagnostic_replies.items():
                quality_dict = self._live_icao_sync_quality.get(iid, {})
                stale = [icao for icao in burst_map if icao not in quality_dict]
                for icao in stale:
                    del burst_map[icao]

            # _seen_pair_keys size cap: keys are position+tdoa tuples that grow
            # monotonically with ICAO churn.  Reset when the set grows large;
            # within-call dedup via the local copy in generate_calibration_pairs()
            # still prevents duplicates within a single generation cycle.
            _SEEN_PAIR_KEYS_MAX = 20_000
            if len(self._seen_pair_keys) > _SEEN_PAIR_KEYS_MAX:
                self._seen_pair_keys = set()

        # _adsb_tracker: prune positions older than 2× _ADSB_POSITION_MAX_AGE_S
        self._adsb_tracker.prune_stale()

    def _prune_live_sync_observation_buffer(self, obs_buf: deque, *, now_ts: float) -> None:
        """Apply time-first retention to one live sync observation buffer."""
        cutoff_ts = now_ts - self._LIVE_SYNC_OBS_RETENTION_S
        while obs_buf and getattr(obs_buf[0], "ts", now_ts) < cutoff_ts:
            obs_buf.popleft()
        # Safety bound for deques that may have been reconstructed without maxlen.
        max_entries = obs_buf.maxlen
        if max_entries is not None and max_entries > 0:
            while len(obs_buf) > max_entries:
                obs_buf.popleft()

    @staticmethod
    def _summarise_live_sync_observation_buffer(obs_snapshot: list[AlignedBurstSyncObs], max_entries: int) -> dict:
        """Return count/span diagnostics for one retained observation deque snapshot."""
        count = len(obs_snapshot)
        if count <= 0:
            return {
                "count": 0,
                "max_entries": max_entries,
                "cap_hit": False,
                "oldest_burst_centroid_us": None,
                "newest_burst_centroid_us": None,
                "retained_duration_s": 0.0,
            }
        oldest_us = float(getattr(obs_snapshot[0], "burst_centroid_us", 0.0))
        newest_us = float(getattr(obs_snapshot[-1], "burst_centroid_us", oldest_us))
        retained_duration_s = max(0.0, (newest_us - oldest_us) / 1_000_000.0)
        return {
            "count": count,
            "max_entries": max_entries,
            "cap_hit": bool(max_entries and count >= max_entries),
            "oldest_burst_centroid_us": oldest_us,
            "newest_burst_centroid_us": newest_us,
            "retained_duration_s": retained_duration_s,
        }

    def _build_live_sync_retention_diagnostics(
        self,
        iid: int,
        aligned_snapshot: list[AlignedBurstSyncObs],
        timeline_snapshot: list[AlignedBurstSyncObs],
    ) -> dict:
        """Per-IID retention diagnostics for live sync/alignment observation buffers."""
        return {
            "iid": iid,
            "retention_target_s": self._LIVE_SYNC_OBS_RETENTION_S,
            "aligned": self._summarise_live_sync_observation_buffer(
                aligned_snapshot,
                self._MULTI_SYNC_OBS_MAX,
            ),
            "timeline": self._summarise_live_sync_observation_buffer(
                timeline_snapshot,
                self._BURST_SYNC_TIMELINE_OBS_MAX,
            ),
        }

    def get_memory_stats(self) -> dict:
        """Return current sizes of key in-memory structures for observability."""
        with self._lock:
            n_icao_quality = sum(len(v) for v in self._live_icao_sync_quality.values())
            n_last_arrival = sum(len(v) for v in self._live_last_arrival.values())
            n_live_bursts  = sum(len(v) for v in self._live_bursts.values())
            burst_record_counts = [len(v) for v in self._burst_records.values()]
            non_empty_burst_counts = [count for count in burst_record_counts if count > 0]
            n_burst_records = sum(burst_record_counts)
            dynamic_caps = list(self._burst_record_dynamic_cap_by_iid.values())
            return {
                "models":              len(self._models),
                "iid_events":          len(self._iid_events),
                "iid_events_max":      _IID_EVENTS_MAX,
                "burst_records_total": n_burst_records,
                "burst_records_iids":  len(non_empty_burst_counts),
                "burst_records_max_per_iid": max(burst_record_counts, default=0),
                "burst_records_cap_per_iid": _BURST_RECORDS_MAX_PER_IID,
                "burst_records_dynamic_cap_min": min(dynamic_caps, default=_BURST_RECORDS_MIN_PER_IID),
                "burst_records_dynamic_cap_max": max(dynamic_caps, default=_BURST_RECORDS_MIN_PER_IID),
                "burst_records_cap_hits_total": sum(self._burst_record_cap_hits_by_iid.values()),
                "burst_records_avg_per_iid": (
                    round(n_burst_records / len(non_empty_burst_counts), 1)
                    if non_empty_burst_counts else 0.0
                ),
                "seen_pair_keys":      len(self._seen_pair_keys),
                "pending_pairs":       len(self._pending_pairs),
                "adsb_tracker_positions": self._adsb_tracker.size(),
                "live_icao_sync_quality_total": n_icao_quality,
                "live_last_arrival_total":      n_last_arrival,
                "live_bursts_total":            n_live_bursts,
                "live_iids":           len(self._live_bursts),
                "sweep_history_iids":  len(self._sweep_history),
                "radar_core_frames_enabled": self._radar_core_frames_enabled,
                "radar_core_frames_injected": self._radar_core_frames_injected,
                "radar_core_frame_inject_errors": self._radar_core_frame_inject_errors,
                "radar_core_frames_injected_iids": len(self._radar_core_frames_injected_by_iid),
                "python_frames_finalized_total": self._python_frames_finalized_total,
                "python_frames_finalized_iids": len(self._python_frames_finalized_by_iid),
            }

    def _process_fired_bursts(self, iid: int, fired_bursts: list[dict]) -> dict:
        metrics = _new_fired_burst_phase_metrics()
        t_setup = time.perf_counter()
        model = self._models.get(iid)
        # Compute authoritative period if a coarse model exists; may be None during
        # bootstrap before the first rotation model is established.
        period_s: float | None = None
        if model is not None and model.period_s is not None:
            period_s = self._get_authoritative_frame_period_s(iid, model.period_s)
        period_us = period_s * 1_000_000.0 if period_s is not None else None
        # native_processor and dominant-family checks require a valid period.
        native_processor = self._native_burst_processors.get(iid) if period_s is not None else None

        def _matches_dominant(icao: str) -> bool:
            rotation_model = getattr(model, "rotation_model", None)
            if rotation_model is not None:
                if icao in getattr(rotation_model, "folded", {}):
                    return True
                if icao in getattr(rotation_model, "residual", {}):
                    return False
                if icao in getattr(rotation_model, "secondary_folded", {}):
                    return False
            if native_processor is not None:
                return native_processor.matches_dominant_period(
                    icao,
                    period_s,
                    MIN_BURSTS,
                    PERIOD_MATCH_TOLERANCE,
                )
            return self._burst_matches_dominant_period_family(iid, icao, period_s)

        self._ensure_live_builder_state(iid)
        # Pre-compute radar position once for the batch (used by sync observation recording).
        # During bootstrap when no model exists yet, default to no-op position.
        _radar_pos_cache = _get_authoritative_radar_position(model) if model is not None else {"lat": None, "lon": None}
        metrics["setup_ms"] += (time.perf_counter() - t_setup) * 1000
        batch_ref_icao: str | None = None

        for fired_burst in fired_bursts:
            metrics["fired_burst_count"] += 1
            fired_icao = fired_burst["icao"]
            burst_centroid_us = fired_burst["burst_centroid_us"]
            burst_signal = fired_burst["burst_signal"]
            n_replies = fired_burst.get("n_replies", 1)
            trigger_arrival_us = fired_burst.get("trigger_arrival_us", burst_centroid_us)

            self._append_burst_record(
                iid,
                BurstRecord(
                    iid=iid,
                    icao=fired_icao,
                    centroid_us=burst_centroid_us,
                    n_replies=n_replies,
                    signal_dbfs=burst_signal,
                ),
            )
            self._record_dwell_profile(
                iid=iid,
                icao=fired_icao,
                beam_center_us=burst_centroid_us,
                replies=fired_burst.get("replies"),
            )

            t_centroid = time.perf_counter()
            centroid_hist = self._live_burst_centroids[iid].setdefault(fired_icao, [])
            centroid_hist.append(burst_centroid_us)
            if len(centroid_hist) > 30:
                centroid_hist.pop(0)
            metrics["centroid_ms"] += (time.perf_counter() - t_centroid) * 1000

            # Position lookup runs unconditionally: needed for sync obs and live
            # detections regardless of whether Python or Go builds frames.
            lat = lon = None
            interpolated = False
            position_age_seconds = 0.0
            position_extrapolated = False
            position_source_age_s = None
            pos: dict | None = None
            metrics["position_lookup_count"] += 1
            t_position_lookup = time.perf_counter()
            try:
                wall_ts = self._estimate_wall_time_from_arrival_us(burst_centroid_us, trigger_arrival_us)
                if wall_ts is not None:
                    pos = self._adsb_tracker.get_position_at(fired_icao, wall_ts)
                    if pos:
                        lat = pos.get("lat")
                        lon = pos.get("lon")
                        interpolated = pos.get("interpolated", False)
                        position_age_seconds = pos.get("position_age_seconds", 0.0)
                        position_extrapolated = bool(pos.get("extrapolated", False))
                        position_source_age_s = pos.get("source_age_seconds")
            except Exception:
                pass
            finally:
                metrics["position_lookup_ms"] += (time.perf_counter() - t_position_lookup) * 1000

            # Record one burst-centre Stage3LiveDetection per fired burst.
            # This replaces the per-message detection recording in on_df11_batch()
            # so that bearing observations are built from burst-centre timestamps.
            self._record_live_burst_detection(iid, fired_icao, burst_centroid_us, burst_signal, pos)

            # Sync obs and frame building require a usable period; skip during bootstrap.
            if period_us is None:
                continue

            # Record burst-centre observations for sync timeline plotting.
            # This is broader than sync maintenance: include all bursts that have
            # usable geometry so the UI is not limited to only sync-driving updates.
            if (
                lat is not None
                and lon is not None
                and _radar_pos_cache["lat"] is not None
                and _radar_pos_cache["lon"] is not None
            ):
                matches_dominant_for_sync = _matches_dominant(fired_icao)
                if self.radar_core_event_sink is None:
                    self._record_burst_sync_timeline_obs(
                        iid=iid,
                        icao=fired_icao,
                        burst_centroid_us=burst_centroid_us,
                        radar_lat=_radar_pos_cache["lat"],
                        radar_lon=_radar_pos_cache["lon"],
                        aircraft_lat=lat,
                        aircraft_lon=lon,
                        n_replies=n_replies,
                        signal_dbfs=burst_signal,
                        pos_age_s=position_age_seconds,
                        sync_update_eligible=matches_dominant_for_sync,
                        burst_center_method=fired_burst.get("burst_center_method", "centroid"),
                        burst_center_simple_us=fired_burst.get("burst_center_simple_us"),
                        burst_center_weighted_us=fired_burst.get("burst_center_weighted_us"),
                        burst_center_delta_us=fired_burst.get("burst_center_delta_us"),
                        burst_ts_first_reply_beast_us=fired_burst.get("burst_ts_first_reply_beast_us"),
                        burst_ts_strongest_reply_beast_us=fired_burst.get("burst_ts_strongest_reply_beast_us"),
                        burst_ts_simple_centroid_beast_us=fired_burst.get("burst_ts_simple_centroid_beast_us"),
                        burst_ts_weighted_centroid_beast_us=fired_burst.get("burst_ts_weighted_centroid_beast_us"),
                        burst_ts_mid_strong_window_beast_us=fired_burst.get("burst_ts_mid_strong_window_beast_us"),
                        burst_ts_last_reply_beast_us=fired_burst.get("burst_ts_last_reply_beast_us"),
                        burst_span_us=fired_burst.get("burst_span_us"),
                        peak_amplitude=fired_burst.get("peak_amplitude"),
                        position_interpolated=interpolated,
                        position_extrapolated=position_extrapolated,
                        position_source_age_s=position_source_age_s,
                        truth_position_ts_beast_us=(
                            burst_centroid_us - position_source_age_s * 1_000_000.0
                            if position_source_age_s is not None else None
                        ),
                    )
                if matches_dominant_for_sync and self.radar_core_event_sink is None:
                    self._record_aligned_burst_sync_obs(
                        iid=iid,
                        icao=fired_icao,
                        burst_centroid_us=burst_centroid_us,
                        radar_lat=_radar_pos_cache["lat"],
                        radar_lon=_radar_pos_cache["lon"],
                        aircraft_lat=lat,
                        aircraft_lon=lon,
                        n_replies=n_replies,
                        signal_dbfs=burst_signal,
                        pos_age_s=position_age_seconds,
                        period_s=period_s,
                    )

            # Frame building: skipped when radar-core is the primary frame source.
            # Sync obs and live detections (above) still run in all modes.
            if self._radar_core_frames_enabled:
                continue

            from .models import SweepFrameObservation, LiveFrameState

            current_frame = self._live_frames.get(iid)
            if current_frame is not None and burst_centroid_us >= current_frame.ref_arrival_us + period_us:
                t_finalize = time.perf_counter()
                finalize_metrics = self._finalize_live_frame(iid, period_s)
                metrics["finalize_ms"] += (time.perf_counter() - t_finalize) * 1000
                _add_fired_burst_phase_metrics(metrics, finalize_metrics)
                current_frame = None

            if current_frame is not None:
                ref_icao = current_frame.ref_icao
            else:
                t_reference_select = time.perf_counter()
                try:
                    override = model.reference_aircraft_override
                    prev_ref_icao = model.reference_aircraft.ref_icao if model.reference_aircraft else None
                    if override is not None:
                        metrics["reference_select_count"] += 1
                        ref_icao = override
                        if prev_ref_icao != override:
                            from .models import ReferenceAircraftInfo
                            model.reference_aircraft = ReferenceAircraftInfo(
                                ref_icao=override,
                                ref_score=None,
                                ref_since_sweep=0,
                                hysteresis_margin=0.0,
                            )
                    else:
                        current_ref_icao = model.reference_aircraft.ref_icao if model.reference_aircraft else None
                        if batch_ref_icao is not None:
                            metrics["reference_select_count"] += 1
                            metrics["reference_reuse_count"] += 1
                            ref_icao = batch_ref_icao
                        elif native_processor is not None:
                            metrics["reference_select_count"] += 1
                            metrics["reference_rescore_count"] += 1
                            ref_icao = native_processor.select_reference(
                                period_s=period_s,
                                now_us=burst_centroid_us,
                                min_bursts_for_ref=4,
                                recency_periods=5.0,
                                hysteresis=0.25,
                                current_ref_icao=current_ref_icao,
                            )
                            if ref_icao is not None:
                                from .models import ReferenceAircraftInfo
                                model.reference_aircraft = ReferenceAircraftInfo(
                                    ref_icao=ref_icao,
                                    ref_score=None,
                                    ref_since_sweep=0,
                                    hysteresis_margin=0.25,
                                )
                                batch_ref_icao = ref_icao
                        else:
                            metrics["reference_select_count"] += 1
                            metrics["reference_rescore_count"] += 1
                            ref_icao = self._select_reference_from_live_bursts(
                                iid, period_s, now_us=burst_centroid_us
                            )
                            if ref_icao is not None:
                                batch_ref_icao = ref_icao
                        if ref_icao is None:
                            continue
                finally:
                    metrics["reference_select_ms"] += (time.perf_counter() - t_reference_select) * 1000

            if fired_icao == ref_icao:
                if current_frame is not None:
                    continue
                metrics["dominant_check_count"] += 1
                t_dominant = time.perf_counter()
                matches_dominant = _matches_dominant(fired_icao)
                metrics["dominant_check_ms"] += (time.perf_counter() - t_dominant) * 1000
                if not matches_dominant:
                    continue
                t_suppression = time.perf_counter()
                suppress_frame_start = self._should_suppress_frame_start(iid, burst_centroid_us, period_s)
                metrics["suppression_ms"] += (time.perf_counter() - t_suppression) * 1000
                if suppress_frame_start:
                    continue
                if lat is None or lon is None:
                    self._live_frames[iid] = None
                    continue
                t_frame_mutation = time.perf_counter()
                self._live_frames[iid] = LiveFrameState(
                    ref_icao=ref_icao,
                    ref_lat=lat,
                    ref_lon=lon,
                    ref_arrival_us=burst_centroid_us,
                    seen_icaos={ref_icao},
                    ref_pos_age_s=position_age_seconds,
                )
                self._live_last_frame_start_us[iid] = burst_centroid_us
                metrics["frame_start_count"] += 1
                metrics["frame_mutation_ms"] += (time.perf_counter() - t_frame_mutation) * 1000
            else:
                current_frame = self._live_frames.get(iid)
                if current_frame is None:
                    continue
                if burst_centroid_us < current_frame.ref_arrival_us:
                    continue
                if burst_centroid_us >= current_frame.ref_arrival_us + period_us:
                    continue
                if lat is None or lon is None:
                    continue
                metrics["dominant_check_count"] += 1
                t_dominant = time.perf_counter()
                matches_dominant = _matches_dominant(fired_icao)
                metrics["dominant_check_ms"] += (time.perf_counter() - t_dominant) * 1000
                if not matches_dominant:
                    continue
                metrics["phase_check_count"] += 1
                t_phase_check = time.perf_counter()
                if native_processor is not None:
                    phase_tolerance_us = max(
                        CO_SWEEP_WINDOW_US,
                        period_us * _PHASE_FAMILY_TOLERANCE_FRACTION,
                    )
                    matches_phase = native_processor.matches_phase_family(
                        current_frame.ref_icao,
                        current_frame.ref_arrival_us,
                        fired_icao,
                        burst_centroid_us,
                        period_s,
                        _PHASE_FAMILY_HISTORY_MIN,
                        phase_tolerance_us,
                    )
                else:
                    matches_phase = self._burst_matches_reference_phase_family(
                        iid,
                        current_frame.ref_icao,
                        current_frame.ref_arrival_us,
                        fired_icao,
                        burst_centroid_us,
                        period_s,
                    )
                metrics["phase_check_ms"] += (time.perf_counter() - t_phase_check) * 1000
                if not matches_phase:
                    continue
                if fired_icao in current_frame.seen_icaos:
                    continue
                t_frame_mutation = time.perf_counter()
                current_frame.observations.append(SweepFrameObservation(
                    icao=fired_icao,
                    lat=lat,
                    lon=lon,
                    arrival_us=burst_centroid_us,
                    signal_dbfs=burst_signal,
                    interpolated=interpolated,
                    n_replies=n_replies,
                    position_age_seconds=position_age_seconds,
                ))
                current_frame.seen_icaos.add(fired_icao)
                current_frame.n_aircraft_seen += 1
                metrics["observation_count"] += 1
                metrics["frame_mutation_ms"] += (time.perf_counter() - t_frame_mutation) * 1000
        return metrics

    def _finalize_diagnostic_burst(
        self,
        iid: int,
        icao: str,
        trigger_arrival_us: float,
    ) -> dict | None:
        """Finalize the diagnostic-only reply mirror for one native-fired burst."""
        pending_by_icao = self._live_burst_diagnostic_replies.get(iid)
        if pending_by_icao is None:
            return None
        replies = pending_by_icao.pop(icao, [])
        if not replies:
            return None

        refinement = refine_burst_center(replies)
        timestamp_candidates = _compute_burst_timestamp_candidates(replies)
        burst_signal = max((s for _, s in replies if s is not None), default=None)
        has_signal = burst_signal is not None
        fired = {
            "icao": icao,
            "trigger_arrival_us": trigger_arrival_us,
            "n_replies": len(replies),
            "burst_signal": burst_signal,
            "burst_center_method": "diagnostic_weighted" if has_signal else "centroid",
            "burst_center_simple_us": refinement.get("beam_center_simple_us"),
            "burst_center_weighted_us": refinement.get("beam_center_weighted_us"),
            "burst_center_delta_us": refinement.get("beam_center_delta_us"),
            **timestamp_candidates,
        }
        if self._diagnostics_enabled:
            fired["replies"] = [
                {"arrival_us": arrival_us, "signal_dbfs": signal_dbfs}
                for arrival_us, signal_dbfs in replies
            ]
        return fired

    def _collect_native_burst_diagnostics(
        self,
        iid: int,
        iid_events: list[tuple[float, str, float | None]],
    ) -> list[dict]:
        """Mirror native burst grouping so diagnostics retain reply-level timestamps.

        The native burst processor intentionally remains authoritative for which
        bursts fire and what operational `burst_centroid_us` is used.  This
        mirror follows the same gap rule and only attaches candidate timestamps
        to the fired burst dictionaries for sync-debug comparison.
        """
        pending_by_icao = self._live_burst_diagnostic_replies.setdefault(iid, {})
        fired: list[dict] = []
        for arrival_us, icao, signal_dbfs in iid_events:
            replies = pending_by_icao.get(icao)
            if replies and arrival_us - replies[-1][0] > BURST_GAP_US:
                diagnostic = self._finalize_diagnostic_burst(iid, icao, trigger_arrival_us=arrival_us)
                if diagnostic is not None:
                    fired.append(diagnostic)
                replies = []
                pending_by_icao[icao] = replies
            pending_by_icao.setdefault(icao, replies or []).append((arrival_us, signal_dbfs))
        fired.sort(key=lambda burst: (
            burst.get("burst_ts_weighted_centroid_beast_us")
            or burst.get("burst_ts_simple_centroid_beast_us")
            or 0.0
        ))
        return fired

    @staticmethod
    def _enrich_native_fired_bursts_with_diagnostics(
        fired_bursts: list[dict],
        diagnostic_bursts: list[dict],
    ) -> list[dict]:
        """Attach diagnostic candidate timestamps to native fired-burst records."""
        by_key: dict[tuple[str, int, int], list[dict]] = defaultdict(list)
        for diagnostic in diagnostic_bursts:
            trigger = diagnostic.get("trigger_arrival_us")
            if trigger is None:
                continue
            key = (
                diagnostic.get("icao"),
                int(round(float(trigger))),
                int(diagnostic.get("n_replies") or 0),
            )
            by_key[key].append(diagnostic)

        enriched: list[dict] = []
        for fired in fired_bursts:
            row = dict(fired)
            trigger = row.get("trigger_arrival_us")
            key = (
                row.get("icao"),
                int(round(float(trigger))) if trigger is not None else 0,
                int(row.get("n_replies") or 0),
            )
            matches = by_key.get(key) or []
            diagnostic = matches.pop(0) if matches else None
            if diagnostic is None and int(row.get("n_replies") or 0) == 1:
                # A one-reply native burst has enough information in the native
                # output itself: the operational centroid is the reply timestamp.
                # This fallback is diagnostic-only and does not alter the solver.
                ts = row.get("burst_centroid_us")
                diagnostic = {
                    "burst_center_method": "centroid",
                    "burst_center_simple_us": ts,
                    "burst_center_weighted_us": ts if row.get("burst_signal") is not None else None,
                    "burst_center_delta_us": 0.0,
                    "burst_ts_first_reply_beast_us": ts,
                    "burst_ts_strongest_reply_beast_us": ts if row.get("burst_signal") is not None else None,
                    "burst_ts_simple_centroid_beast_us": ts,
                    "burst_ts_weighted_centroid_beast_us": ts if row.get("burst_signal") is not None else None,
                    "burst_ts_mid_strong_window_beast_us": ts if row.get("burst_signal") is not None else None,
                    "burst_ts_last_reply_beast_us": ts,
                    "burst_span_us": 0.0,
                    "peak_amplitude": row.get("burst_signal"),
                }
            if diagnostic is not None:
                for key_name, value in diagnostic.items():
                    if key_name in {"icao", "trigger_arrival_us", "n_replies", "burst_signal"}:
                        continue
                    row.setdefault(key_name, value)
            enriched.append(row)
        return enriched

    def on_df11_batch(self, events: list[tuple[int, int, str, float | None]]) -> None:
        """Process a batch of pre-decoded DF11 events from the radar worker."""
        if not events:
            return
        t0 = time.perf_counter()
        t_cpu0 = time.thread_time()
        builder_s = 0.0
        prepare_s = 0.0
        append_s = 0.0
        group_s = 0.0
        native_burst_s = 0.0
        process_burst_s = 0.0
        builder_cpu_s = 0.0
        processed_count = 0
        fired_burst_count = 0
        active_iid_count = 0
        fired_phase_metrics = _new_fired_burst_phase_metrics()
        try:
            t_prepare = time.perf_counter()
            prepared: list[tuple[int, str, float | None, float]] = []
            for timestamp, iid, icao_hex, signal_dbfs in events:
                if iid == 0:
                    continue
                ticks = self._unwrap(timestamp)
                arrival_us = ticks / BEAST_TICKS_PER_US
                prepared.append((iid, icao_hex, signal_dbfs, arrival_us))
            prepare_s = time.perf_counter() - t_prepare
            if not prepared:
                return

            # Shadow tap: forward unwrapped events to radar-core (non-blocking).
            sink = self.radar_core_event_sink
            if sink is not None:
                try:
                    for iid, icao_hex, signal_dbfs, arrival_us in prepared:
                        sink(arrival_us, iid, int(icao_hex, 16), signal_dbfs)
                except Exception:
                    pass

            t_append = time.perf_counter()
            with self._lock:
                for iid, icao_hex, signal_dbfs, arrival_us in prepared:
                    self._iid_events.append((arrival_us, iid, icao_hex, signal_dbfs))
                    self._iid_latest_arrival_us[iid] = arrival_us
                    self._dirty_iids.add(iid)
            append_s = time.perf_counter() - t_append

            # In radar-core mode Go owns DF11 burst detection; skip Python's
            # accumulator entirely.  _iid_events and _dirty_iids (above) are
            # still populated so update_rotation_models() continues to run via
            # its raw-event bootstrap path.
            if self.radar_core_event_sink is None:
                t_group = time.perf_counter()
                grouped_events: dict[int, list[tuple[float, str, float | None]]] = defaultdict(list)
                for iid, icao_hex, signal_dbfs, arrival_us in prepared:
                    if self._diagnostics_enabled:
                        self._flash_seq += 1
                        self._flash_events.append((self._flash_seq, iid, icao_hex, int(arrival_us)))
                    grouped_events[iid].append((arrival_us, icao_hex, signal_dbfs))
                group_s = time.perf_counter() - t_group

                t_builder = time.perf_counter()
                t_builder_cpu = time.thread_time()
                native_available = _decode_cffi is not None and hasattr(_decode_cffi, "RadarBurstProcessor")
                for iid, iid_events in grouped_events.items():
                    model = self._models.get(iid)
                    # active_iid_count reflects IIDs with a working period (for metrics).
                    # Burst accumulation proceeds even during bootstrap (no model/period yet).
                    if model is not None and model.period_s is not None:
                        active_iid_count += 1
                    self._ensure_live_builder_state(iid)
                    iid_events.sort(key=lambda event: event[0])
                    if native_available:
                        processor = self._native_burst_processors.get(iid)
                        if processor is None:
                            processor = _decode_cffi.RadarBurstProcessor()
                            self._native_burst_processors[iid] = processor
                        diagnostic_bursts = self._collect_native_burst_diagnostics(iid, iid_events)
                        t_native_burst = time.perf_counter()
                        fired_bursts = processor.process_batch(iid_events, BURST_GAP_US)
                        fired_bursts = self._enrich_native_fired_bursts_with_diagnostics(
                            fired_bursts,
                            diagnostic_bursts,
                        )
                        native_burst_s += time.perf_counter() - t_native_burst
                        fired_burst_count += len(fired_bursts)
                        t_process_burst = time.perf_counter()
                        iid_fired_phase_metrics = self._process_fired_bursts(iid, fired_bursts)
                        process_burst_s += time.perf_counter() - t_process_burst
                        _add_fired_burst_phase_metrics(fired_phase_metrics, iid_fired_phase_metrics)
                    else:
                        t_process_burst = time.perf_counter()
                        for arrival_us, icao_hex, signal_dbfs in iid_events:
                            self._on_df11_frame_builder(iid, icao_hex, arrival_us, signal_dbfs)
                        process_burst_s += time.perf_counter() - t_process_burst
                    processed_count += len(iid_events)
                builder_s = time.perf_counter() - t_builder
                builder_cpu_s = time.thread_time() - t_builder_cpu

            # Stage 3 live detections are now recorded at burst-fire time inside
            # _process_fired_bursts() (native path) and _on_df11_frame_builder()
            # (Python fallback).  Each fired burst produces exactly one
            # Stage3LiveDetection with arrival_us = burst_centroid_us so that
            # bearing observations are built from burst-centre timestamps rather
            # than individual message arrivals.
        except Exception:
            pass
        finally:
            if processed_count <= 0:
                processed_count = max(1, len(events))
            total_s = time.perf_counter() - t0
            total_cpu_s = time.thread_time() - t_cpu0
            per_total_s = total_s / processed_count
            per_builder_s = builder_s / processed_count if builder_s > 0 else 0.0
            for _ in range(processed_count):
                record_df11_event_timing(per_total_s, per_builder_s)
            sample = {
                "ts_s": time.time(),
                "input_count": len(events),
                "processed_count": processed_count,
                "active_iid_count": active_iid_count,
                "fired_burst_count": fired_burst_count,
                "prepare_ms": round(prepare_s * 1000, 2),
                "append_ms": round(append_s * 1000, 2),
                "group_ms": round(group_s * 1000, 2),
                "builder_wall_ms": round(builder_s * 1000, 2),
                "builder_cpu_ms": round(builder_cpu_s * 1000, 2),
                "builder_offcpu_ms": round(max(0.0, builder_s - builder_cpu_s) * 1000, 2),
                "native_burst_ms": round(native_burst_s * 1000, 2),
                "process_burst_ms": round(process_burst_s * 1000, 2),
                "total_wall_ms": round(total_s * 1000, 2),
                "total_cpu_ms": round(total_cpu_s * 1000, 2),
                "total_offcpu_ms": round(max(0.0, total_s - total_cpu_s) * 1000, 2),
            }
            for key in _FIRED_BURST_PHASE_KEYS:
                value = fired_phase_metrics.get(key, 0.0)
                sample[key] = round(value, 2) if key.endswith("_ms") else value
            record_df11_batch_phase_timing(sample)

    # ------------------------------------------------------------------
    # Live SweepFrame builder — called for every DF11 as it arrives.
    # ------------------------------------------------------------------

    def _finalize_live_frame(self, iid: int, period_s: float) -> dict:
        metrics = _new_fired_burst_phase_metrics()
        current_frame = self._live_frames.get(iid)
        if current_frame is None:
            return metrics

        n_obs = len(current_frame.observations)
        n_aircraft = n_obs + 1
        if n_aircraft >= 3:
            metrics["frame_finalized_count"] += 1
            quality = "good" if n_aircraft >= 4 else "marginal"
            from .models import SweepFrame
            next_index = self._live_frame_counters.get(iid, 0)
            self._live_frame_counters[iid] = next_index + 1
            frame = SweepFrame(
                frame_index=next_index,
                sweep_start_us=current_frame.ref_arrival_us,
                ref_icao=current_frame.ref_icao,
                ref_lat=current_frame.ref_lat,
                ref_lon=current_frame.ref_lon,
                ref_arrival_us=current_frame.ref_arrival_us,
                observations=list(current_frame.observations),
                quality=quality,
                period_s=period_s,
            )
            self._live_completed_frames[iid].append(frame)
            self._python_frames_finalized_total += 1
            self._python_frames_finalized_by_iid[iid] = (
                self._python_frames_finalized_by_iid.get(iid, 0) + 1
            )
            if quality in ("good", "marginal") and not self._radar_core_frames_enabled:
                metrics["fm_callback_count"] += 1
                t_fm_callback = time.perf_counter()
                try:
                    # Non-blocking hand-off to the FM worker — latest frame wins.
                    with self._fm_mailbox_lock:
                        self._fm_mailbox[iid] = (frame, period_s)
                    self._fm_mailbox_event.set()
                finally:
                    metrics["fm_callback_ms"] += (time.perf_counter() - t_fm_callback) * 1000

        # Update live sync state for Stage 3 when a usable frame is completed.
        # Bootstrap only: seed the first sync state from the reference aircraft bearing so
        # the multi-aircraft estimator has an initial model to compute residuals against.
        # Once bootstrapped, sync is driven entirely by _record_aligned_burst_sync_obs →
        # _update_multi_aircraft_sync_state, which fires on every aligned burst arrival.
        if n_aircraft >= 3:
            self._seed_live_sync_from_frame(
                iid, frame, period_s, ref_pos_age_s=current_frame.ref_pos_age_s
            )

        self._live_frames[iid] = None
        return metrics

    def claim_pending_fm_frames(self) -> list[tuple]:
        """Atomically drain the per-IID FM mailbox.

        Returns a list of (iid, frame, period_s) for each IID with a pending
        frame at call time.  "Latest wins": each IID appears at most once, with
        its most recent frame.  Clears the wake event.
        """
        with self._fm_mailbox_lock:
            if not self._fm_mailbox:
                self._fm_mailbox_event.clear()
                return []
            items = [(iid, frame, period_s) for iid, (frame, period_s) in self._fm_mailbox.items()]
            self._fm_mailbox.clear()
            self._fm_mailbox_event.clear()
        return items

    def wait_for_pending_fm_frames(self, timeout: float | None = None) -> bool:
        """Block until the mailbox has pending frames, or timeout."""
        return self._fm_mailbox_event.wait(timeout=timeout)

    def enable_radar_core_frames(self, enabled: bool = True) -> None:
        """Switch the FM frame source between Python (False) and radar-core (True).

        When enabled, _finalize_live_frame() skips the FM mailbox injection so
        only FRAME_READY messages from radar-core reach the FM worker.
        Call once at startup after the RadarCoreClient is connected.
        """
        self._radar_core_frames_enabled = enabled

    def inject_frame_from_go(self, frame_dict: dict) -> None:
        """Convert a FRAME_READY dict from radar-core into a SweepFrame and
        inject it into the FM worker mailbox.  Called from the RadarCoreClient
        receiver thread when RADAR_CORE_FRAMES_ENABLED is True.
        """
        from .models import SweepFrame, SweepFrameObservation
        try:
            iid = int(frame_dict["i"])
            period_s = float(frame_dict["p"])
            ref_icao = f"{int(frame_dict['rc']):06X}"
            ref_arrival_us = float(frame_dict["ra"])
            quality = str(frame_dict.get("q", "marginal"))

            obs_list = []
            for obs in (frame_dict.get("obs") or []):
                obs_list.append(SweepFrameObservation(
                    icao=f"{int(obs['c']):06X}",
                    lat=float(obs["la"]),
                    lon=float(obs["lo"]),
                    arrival_us=float(obs["a"]),
                    n_replies=int(obs.get("n", 1)),
                    position_age_seconds=float(obs.get("pa", 0.0)),
                ))

            frame = SweepFrame(
                frame_index=int(frame_dict.get("fi", 0)),
                sweep_start_us=ref_arrival_us,
                ref_icao=ref_icao,
                ref_lat=float(frame_dict["rla"]),
                ref_lon=float(frame_dict["rlo"]),
                ref_arrival_us=ref_arrival_us,
                observations=obs_list,
                quality=quality,
                period_s=period_s,
            )

            with self._fm_mailbox_lock:
                self._fm_mailbox[iid] = (frame, period_s)
            self._fm_mailbox_event.set()
            # Store in the completed-frames buffer so _fm_loop() can count Go
            # frames and trigger run_full_pipeline() without the Python builder.
            buf = self._live_completed_frames.setdefault(
                iid, deque(maxlen=self._LIVE_FRAMES_MAX)
            )
            buf.append(frame)
            self._radar_core_frames_injected += 1
            self._radar_core_frames_injected_by_iid[iid] = (
                self._radar_core_frames_injected_by_iid.get(iid, 0) + 1
            )

        except Exception:
            self._radar_core_frame_inject_errors += 1
            log.debug("RadarState: malformed radar-core FRAME_READY ignored", exc_info=True)

    def update_forward_model_from_go(self, state_dict: dict) -> None:
        """Project radar-core FM_STATE into Python's compatibility model fields.

        Go is authoritative for operational FM when this callback is wired.
        Python keeps the existing RadarIID.fm_* fields populated so APIs and
        coincident-illumination seeding do not need a parallel lookup path.
        """
        try:
            iid = int(state_dict["i"])
            centroid_available = bool(state_dict.get("ca"))
            lat = state_dict.get("la")
            lon = state_dict.get("lo")
            cep_m = state_dict.get("cep")
            updated_ts = float(state_dict.get("ts") or time.time())

            pipeline = {
                "frames_reaching_solver": int(state_dict.get("fr", 0)),
                "solver_success": int(state_dict.get("sc", 0)),
                "candidate_positions": int(state_dict.get("cp", 0)),
                "solver_no_candidate": int(state_dict.get("nc", 0)),
                "accumulation_rejected": int(state_dict.get("ar", 0)),
                "accumulation_rejection_reasons": dict(state_dict.get("rr") or {}),
                "accumulation_accepted": int(state_dict.get("aa", 0)),
                "accumulation_acceptance_tiers": dict(state_dict.get("at") or {}),
                "accumulation_written": int(state_dict.get("aw", 0)),
                "storage_errors": 0,
                "last_rejection_reason": state_dict.get("lr") or None,
                "last_admission_tier": state_dict.get("lt") or None,
                "accumulated_frame_positions": int(state_dict.get("af", 0)),
                "centroid_available": centroid_available,
                "centroid_rejection_counts": dict(state_dict.get("cr") or {}),
                "centroid_inliers": int(state_dict.get("ci", 0)),
                "centroid_stage0_survivors": int(state_dict.get("cs", 0)),
                "source": "go_radar_core",
            }

            with self._lock:
                if iid not in self._models:
                    self._models[iid] = RadarIID(iid=iid)
                model = self._models[iid]
                if centroid_available and lat is not None and lon is not None:
                    model.fm_lat = float(lat)
                    model.fm_lon = float(lon)
                    model.fm_cep_m = float(cep_m) if cep_m is not None else None
                    model.fm_source = "go_frame_accumulation"
                    model.fm_n_observations = int(state_dict.get("ci", 0))
                    model.fm_window_s = 0.0
                    model.last_updated = updated_ts
                    entry = {
                        "ts": updated_ts,
                        "lat": model.fm_lat,
                        "lon": model.fm_lon,
                        "cep_m": model.fm_cep_m,
                        "n_observations": model.fm_n_observations,
                        "source": model.fm_source,
                        "authoritative_engine": "go_radar_core",
                    }
                    model.fm_convergence_history.append(entry)
                    if len(model.fm_convergence_history) > 50:
                        model.fm_convergence_history = model.fm_convergence_history[-50:]
                model.fm_last_run = {
                    "ts": updated_ts,
                    "success": bool(state_dict.get("fs")),
                    "stage": state_dict.get("lss") or "go_frame_solve",
                    "reason": state_dict.get("frr") or state_dict.get("lsr"),
                    "source": "go_radar_core",
                    "authoritative_engine": "go_radar_core",
                    "lat": model.fm_lat,
                    "lon": model.fm_lon,
                    "cep_m": model.fm_cep_m,
                    "detail": {
                        "last_frame_index": state_dict.get("fi"),
                        "last_candidate_position": bool(state_dict.get("fh")),
                        "last_ambiguity_same_lobe_bypass": bool(state_dict.get("lsl")),
                        "last_cluster_member_count": state_dict.get("lcm"),
                        "last_second_cluster_member_count": state_dict.get("lsm"),
                        "last_support_dominance_ratio": state_dict.get("lsd"),
                        "last_pairwise_rms_deg": state_dict.get("lpr"),
                    },
                }
                self._go_fm_states[iid] = dict(state_dict)
                self._go_fm_pipeline_stats[iid] = pipeline
        except Exception:
            log.debug("RadarState: malformed radar-core FM_STATE ignored", exc_info=True)

    def get_go_fm_pipeline_stats(self, iid: int) -> dict | None:
        with self._lock:
            stats = self._go_fm_pipeline_stats.get(iid)
            return dict(stats) if stats is not None else None

    @staticmethod
    def _normalise_go_frame_position(entry: dict) -> dict | None:
        try:
            lat = entry.get("lat")
            lon = entry.get("lon")
            if lat is None or lon is None:
                return None
            return {
                "frame_index": int(entry["frame_index"]),
                "sweep_start_us": float(entry["sweep_start_us"]),
                "lat": float(lat),
                "lon": float(lon),
                "cep_km": float(entry.get("cep_km") or 0.0),
                "n_contributing_arcs": int(entry.get("n_contributing_arcs") or 0),
                "azimuth_spread_deg": float(entry.get("azimuth_spread_deg") or 0.0),
                "weight": float(entry.get("weight") or 0.0),
            }
        except Exception:
            return None

    @staticmethod
    def _go_frame_position_signature(entry: dict) -> tuple:
        return (
            entry.get("frame_index"),
            entry.get("sweep_start_us"),
            entry.get("lat"),
            entry.get("lon"),
            entry.get("cep_km"),
            entry.get("n_contributing_arcs"),
            entry.get("azimuth_spread_deg"),
            entry.get("weight"),
        )

    def _set_go_frame_positions_locked(self, iid: int, entries: list[dict]) -> bool:
        existing = list(self._go_frame_positions.get(iid, ()))
        existing_sig = tuple(self._go_frame_position_signature(entry) for entry in existing)
        next_sig = tuple(self._go_frame_position_signature(entry) for entry in entries)
        if existing_sig == next_sig:
            return False
        self._go_frame_positions[iid] = deque(entries, maxlen=self._GO_FRAME_POSITIONS_MAX)
        self._go_frame_positions_revision[iid] = self._go_frame_positions_revision.get(iid, 0) + 1
        return True

    def update_go_frame_position_result(self, result_dict: dict) -> None:
        try:
            iid = int(result_dict["i"])
        except Exception:
            log.debug("RadarState: malformed radar-core FM_FRAME_RESULT ignored", exc_info=True)
            return

        if not bool(result_dict.get("aa")):
            return

        entry = self._normalise_go_frame_position({
            "frame_index": result_dict.get("fi"),
            "sweep_start_us": result_dict.get("su"),
            "lat": result_dict.get("la"),
            "lon": result_dict.get("lo"),
            "cep_km": (
                (float(result_dict["cep"]) / 1000.0)
                if result_dict.get("cep") is not None else None
            ),
            "n_contributing_arcs": result_dict.get("na"),
            "azimuth_spread_deg": result_dict.get("az"),
            "weight": result_dict.get("w"),
        })
        if entry is None:
            return

        with self._lock:
            buf = self._go_frame_positions.setdefault(
                iid,
                deque(maxlen=self._GO_FRAME_POSITIONS_MAX),
            )
            for idx, existing in enumerate(buf):
                if existing.get("frame_index") != entry["frame_index"]:
                    continue
                if self._go_frame_position_signature(existing) == self._go_frame_position_signature(entry):
                    return
                buf[idx] = entry
                self._go_frame_positions_revision[iid] = self._go_frame_positions_revision.get(iid, 0) + 1
                return
            buf.append(entry)
            self._go_frame_positions_revision[iid] = self._go_frame_positions_revision.get(iid, 0) + 1

    def update_go_snapshot(self, snapshot: dict) -> None:
        iids_payload = snapshot.get("iids")
        if not isinstance(iids_payload, dict):
            iids_payload = {}

        with self._lock:
            self._go_snapshot_payload = dict(snapshot)
            seen_go_sync_iids: set[int] = set()
            for iid_key, iid_payload in iids_payload.items():
                if not isinstance(iid_payload, dict):
                    continue
                try:
                    iid = int(iid_payload.get("iid", iid_key))
                except Exception:
                    continue
                ref_icao = iid_payload.get("reference_icao")
                has_ref = bool(iid_payload.get("has_reference_icao")) and ref_icao is not None
                if has_ref:
                    ref_text = f"{int(ref_icao):06X}" if not isinstance(ref_icao, str) else ref_icao.upper()
                    self._go_reference_aircraft_by_iid[iid] = {
                        "status": "SELECTED",
                        "ref_icao": ref_text,
                        "ref_score": None,
                        "ref_since_sweep": None,
                        "hysteresis_margin": None,
                        "challengers": [],
                        "source": "go_snapshot",
                    }
                elif iid in self._go_reference_aircraft_by_iid:
                    self._go_reference_aircraft_by_iid.pop(iid, None)
                    ref_text = None
                else:
                    ref_text = None

                go_sync = self._normalise_go_sync_state(iid_payload)
                if go_sync is not None:
                    self._go_sync_states_by_iid[iid] = go_sync
                    self._record_compact_sync_transition_locked(
                        iid,
                        ref_icao=ref_text,
                        sync_present=True,
                        phase_epoch_us=go_sync.get("phase_epoch_us"),
                        holdover=go_sync.get("holdover"),
                        event_ts=go_sync.get("last_updated"),
                        source="sweep_frame_go",
                    )
                    self._adopt_go_frame_sync_locked(iid, go_sync)
                    seen_go_sync_iids.add(iid)
                elif iid in self._go_sync_states_by_iid:
                    self._go_sync_states_by_iid.pop(iid, None)
                    self._record_compact_sync_transition_locked(
                        iid,
                        ref_icao=ref_text,
                        sync_present=False,
                        event_ts=time.time(),
                        source="sweep_frame_go",
                    )

                admission = self._normalise_go_multi_sync_admission(iid_payload.get("multi_sync_admission"))
                if admission is not None:
                    self._go_multi_sync_admission_by_iid[iid] = admission
                elif iid in self._go_multi_sync_admission_by_iid:
                    self._go_multi_sync_admission_by_iid.pop(iid, None)

                if "frame_positions" in iid_payload:
                    entries = []
                    for raw_entry in iid_payload.get("frame_positions") or []:
                        if not isinstance(raw_entry, dict):
                            continue
                        entry = self._normalise_go_frame_position(raw_entry)
                        if entry is not None:
                            entries.append(entry)
                    self._set_go_frame_positions_locked(iid, entries)
                elif (
                    iid in self._go_frame_positions
                    and (
                        "frame_position_pipeline" in iid_payload
                        or "forward_model" in iid_payload
                    )
                ):
                    self._set_go_frame_positions_locked(iid, [])
            track_entries = []
            for raw_entry in snapshot.get("track_observations") or []:
                entry = self._normalise_go_track_observation(raw_entry)
                if entry is not None:
                    track_entries.append(entry)
            if "track_observations" in snapshot:
                self._go_track_observations = deque(
                    track_entries[-self._GO_TRACK_OBSERVATIONS_MAX:],
                    maxlen=self._GO_TRACK_OBSERVATIONS_MAX,
                )
            evidence_entries = []
            for raw_entry in snapshot.get("evidence_events") or []:
                entry = self._normalise_go_evidence_event(raw_entry)
                if entry is not None:
                    evidence_entries.append(entry)
            if "evidence_events" in snapshot:
                self._go_evidence_events = deque(
                    evidence_entries[-self._GO_EVIDENCE_EVENTS_MAX:],
                    maxlen=self._GO_EVIDENCE_EVENTS_MAX,
                )
            sweep_frames_payload = snapshot.get("sweep_frames")
            if sweep_frames_payload is None and isinstance(snapshot.get("radar_snapshot"), dict):
                sweep_frames_payload = snapshot["radar_snapshot"].get("sweep_frames")
            if sweep_frames_payload is not None:
                frames_by_iid: dict[int, list] = defaultdict(list)
                for raw_entry in sweep_frames_payload or []:
                    frame = self._normalise_go_sweep_frame(raw_entry)
                    if frame is not None:
                        frames_by_iid[frame.iid].append(frame.frame)
                seen_iids = set(self._go_sweep_frames_by_iid.keys()) | set(frames_by_iid.keys())
                for iid in seen_iids:
                    next_frames = frames_by_iid.get(iid, [])
                    next_sig = self._go_sweep_frame_signature(next_frames)
                    existing = list(self._go_sweep_frames_by_iid.get(iid, ()))
                    existing_sig = self._go_sweep_frame_signature(existing)
                    if existing_sig == next_sig:
                        continue
                    self._go_sweep_frames_by_iid[iid] = deque(
                        next_frames[-self._GO_SWEEP_FRAMES_MAX:],
                        maxlen=self._GO_SWEEP_FRAMES_MAX,
                    )
                    self._go_sweep_frames_revision[iid] = self._go_sweep_frames_revision.get(iid, 0) + 1
            stale_go_sync_iids = set(self._go_sync_states_by_iid.keys()) - seen_go_sync_iids
            for iid in stale_go_sync_iids:
                self._go_sync_states_by_iid.pop(iid, None)

    @staticmethod
    def _normalise_go_sync_state(entry: dict) -> dict | None:
        if not isinstance(entry, dict) or not bool(entry.get("sync_state_present")):
            return None
        try:
            return {
                "period_s": (
                    float(entry["sync_period_s"])
                    if entry.get("sync_period_s") is not None else None
                ),
                "phase_epoch_us": (
                    float(entry["sync_phase_epoch_us"])
                    if entry.get("sync_phase_epoch_us") is not None else None
                ),
                "phase_offset_deg": (
                    float(entry["sync_phase_offset_deg"])
                    if entry.get("sync_phase_offset_deg") is not None else None
                ),
                "sync_quality": float(entry.get("sync_quality") or 0.0),
                "usable": bool(entry.get("sync_state_usable", False)),
                "sync_jitter_deg": (
                    float(entry["sync_jitter_deg"])
                    if entry.get("sync_jitter_deg") is not None else None
                ),
                "residual_ema_deg": (
                    float(entry["sync_residual_ema_deg"])
                    if entry.get("sync_residual_ema_deg") is not None else None
                ),
                "last_residual_deg": (
                    float(entry["sync_last_residual_deg"])
                    if entry.get("sync_last_residual_deg") is not None else None
                ),
                "n_sync_frames": int(entry.get("sync_n_frames") or 0),
                "n_rejected_frames": int(entry.get("sync_n_rejected_frames") or 0),
                "holdover": bool(entry.get("sync_holdover", False)),
                "last_updated": (
                    float(entry["sync_last_updated"])
                    if entry.get("sync_last_updated") is not None else None
                ),
            }
        except Exception:
            return None

    @staticmethod
    def _normalise_go_multi_sync_admission(entry: dict) -> dict | None:
        if not isinstance(entry, dict):
            return None
        counts_raw = entry.get("counts") or {}
        if not isinstance(counts_raw, dict):
            counts_raw = {}
        try:
            counts = {str(k): int(v) for k, v in counts_raw.items()}
        except Exception:
            counts = {}
        last_icao = entry.get("last_icao")
        if last_icao is not None:
            try:
                last_icao = f"{int(last_icao):06X}"
            except Exception:
                last_icao = None
        return {
            "last_reason": entry.get("last_reason"),
            "last_icao": last_icao,
            "last_ts": float(entry.get("last_ts") or 0.0),
            "counts": counts,
        }

    @staticmethod
    def _normalise_go_anchor_candidates(entries: list | None) -> list[dict]:
        if not isinstance(entries, list):
            return []
        results: list[dict] = []
        for raw in entries:
            if not isinstance(raw, dict):
                continue
            icao = raw.get("i")
            try:
                icao_text = f"{int(icao):06X}" if icao is not None else None
            except Exception:
                icao_text = None
            if not icao_text:
                continue
            reject_reasons = raw.get("rr") or []
            if not isinstance(reject_reasons, list):
                reject_reasons = []
            results.append({
                "icao": icao_text,
                "score": float(raw.get("s") or 0.0),
                "spread_deg": float(raw.get("sp") or 0.0),
                "obs_count": int(raw.get("o") or 0),
                "fit_eligible_count": int(raw.get("f") or 0),
                "fit_eligible_fraction": float(raw.get("ff") or 0.0),
                "status": raw.get("st") or "unknown",
                "reject_reasons": [str(reason) for reason in reject_reasons if reason],
            })
        return results

    def _record_compact_sync_transition_locked(
        self,
        iid: int,
        *,
        ref_icao: str | None = None,
        sync_present: bool | None = None,
        phase_epoch_us: float | None = None,
        holdover: bool | None = None,
        event_ts: float | None = None,
        source: str | None = None,
    ) -> dict:
        now_ts = float(event_ts or time.time())
        entry = dict(self._compact_sync_debug_by_iid.get(iid) or {})
        entry.setdefault("current_reference_icao", None)
        entry.setdefault("last_reference_icao", None)
        entry.setdefault("reference_change_count", 0)
        entry.setdefault("last_reference_change_ts", None)
        entry.setdefault("reference_changed_recently", False)
        entry.setdefault("sync_reset_count", 0)
        entry.setdefault("last_sync_reset_ts", None)
        entry.setdefault("last_sync_reset_reason", None)
        entry.setdefault("sync_present", None)
        entry.setdefault("current_phase_epoch_us", None)
        entry.setdefault("last_phase_epoch_us", None)
        entry.setdefault("last_phase_epoch_change_ts", None)
        entry.setdefault("phase_epoch_changed_recently", False)
        entry.setdefault("holdover", None)
        entry.setdefault("last_holdover_transition_ts", None)
        entry.setdefault("last_holdover_transition", None)
        entry.setdefault("source", None)

        if source:
            entry["source"] = source
        if ref_icao is not None and ref_icao != entry.get("current_reference_icao"):
            entry["last_reference_icao"] = entry.get("current_reference_icao")
            entry["current_reference_icao"] = ref_icao
            entry["reference_change_count"] = int(entry.get("reference_change_count") or 0) + 1
            entry["last_reference_change_ts"] = now_ts
            entry["reference_changed_recently"] = True
        elif entry.get("last_reference_change_ts") is not None:
            entry["reference_changed_recently"] = (now_ts - float(entry["last_reference_change_ts"])) <= 30.0

        if phase_epoch_us is not None:
            prev_epoch = entry.get("current_phase_epoch_us")
            if prev_epoch is not None and abs(float(phase_epoch_us) - float(prev_epoch)) > 1e-6:
                entry["last_phase_epoch_us"] = prev_epoch
                entry["last_phase_epoch_change_ts"] = now_ts
                entry["phase_epoch_changed_recently"] = True
            elif entry.get("last_phase_epoch_change_ts") is not None:
                entry["phase_epoch_changed_recently"] = (now_ts - float(entry["last_phase_epoch_change_ts"])) <= 30.0
            entry["current_phase_epoch_us"] = float(phase_epoch_us)

        if sync_present is False and entry.get("sync_present") is not False:
            entry["sync_reset_count"] = int(entry.get("sync_reset_count") or 0) + 1
            entry["last_sync_reset_ts"] = now_ts
            entry["last_sync_reset_reason"] = "sync_state_missing"
        if sync_present is not None:
            entry["sync_present"] = bool(sync_present)
        if holdover is not None and holdover != entry.get("holdover"):
            entry["last_holdover_transition_ts"] = now_ts
            entry["last_holdover_transition"] = "entered_holdover" if holdover else "exited_holdover"
            entry["holdover"] = bool(holdover)

        self._compact_sync_debug_by_iid[iid] = entry
        return dict(entry)

    @staticmethod
    def _normalise_go_sync_eligibility(entry: dict) -> tuple[bool, bool, bool, bool]:
        compact_value = entry.get("compact_sync_eligible")
        refined_present_value = entry.get("refined_sync_present")
        refined_usable_value = entry.get("refined_sync_usable")
        sync_value = entry.get("sync_eligible")

        legacy_compact_only = (
            compact_value is None
            and refined_present_value is None
            and refined_usable_value is None
        )

        compact_sync_eligible = bool(
            compact_value if compact_value is not None else (
                sync_value if legacy_compact_only else False
            )
        )
        refined_sync_present = bool(refined_present_value)
        refined_sync_usable = bool(
            refined_usable_value if refined_usable_value is not None else (
                sync_value if not legacy_compact_only else False
            )
        )
        sync_eligible = bool(
            refined_usable_value if refined_usable_value is not None else (
                sync_value if sync_value is not None else refined_sync_usable
            )
        )
        return (
            compact_sync_eligible,
            refined_sync_present,
            refined_sync_usable,
            sync_eligible,
        )

    @staticmethod
    def _normalise_go_track_observation(entry: dict) -> dict | None:
        if not isinstance(entry, dict):
            return None
        try:
            (
                compact_sync_eligible,
                refined_sync_present,
                refined_sync_usable,
                sync_eligible,
            ) = RadarState._normalise_go_sync_eligibility(entry)
            return {
                "iid": int(entry["iid"]),
                "icao": f"{int(entry['icao']):06X}",
                "arrival_us": float(entry["arrival_us"]),
                "wall_ts": float(entry["wall_ts"]),
                "signal_dbfs": (
                    float(entry["signal_dbfs"])
                    if entry.get("signal_dbfs") is not None else None
                ),
                "truth_lat": (
                    float(entry["truth_lat"])
                    if entry.get("truth_lat") is not None else None
                ),
                "truth_lon": (
                    float(entry["truth_lon"])
                    if entry.get("truth_lon") is not None else None
                ),
                "position_age_seconds": (
                    float(entry["position_age_s"])
                    if entry.get("position_age_s") is not None else None
                ),
                "association_confidence": float(entry.get("association_confidence") or 0.0),
                "dominant_family": bool(entry.get("dominant_family")),
                "compact_sync_eligible": compact_sync_eligible,
                "refined_sync_present": refined_sync_present,
                "refined_sync_usable": refined_sync_usable,
                "sync_eligible": sync_eligible,
            }
        except Exception:
            return None

    @_dataclass
    class _GoSweepFrame:
        iid: int
        frame: Any

    @staticmethod
    def _normalise_go_sweep_frame(entry: dict) -> "RadarState._GoSweepFrame | None":
        if not isinstance(entry, dict):
            return None
        try:
            from .models import SweepFrame, SweepFrameObservation

            iid = int(entry["iid"])
            observations = []
            for obs in entry.get("observations") or []:
                observations.append(SweepFrameObservation(
                    icao=f"{int(obs['icao']):06X}" if not isinstance(obs.get("icao"), str) else str(obs.get("icao")).upper(),
                    lat=float(obs["lat"]),
                    lon=float(obs["lon"]),
                    arrival_us=float(obs["arrival_us"]),
                    n_replies=int(obs.get("n_replies") or 1),
                    position_age_seconds=float(obs.get("position_age_s") or 0.0),
                ))
            frame = SweepFrame(
                frame_index=int(entry["frame_index"]),
                sweep_start_us=float(entry["ref_arrival_us"]),
                ref_icao=f"{int(entry['ref_icao']):06X}" if not isinstance(entry.get("ref_icao"), str) else str(entry.get("ref_icao")).upper(),
                ref_lat=float(entry["ref_lat"]),
                ref_lon=float(entry["ref_lon"]),
                ref_arrival_us=float(entry["ref_arrival_us"]),
                observations=observations,
                quality=str(entry.get("quality") or "marginal"),
                period_s=float(entry["period_s"]) if entry.get("period_s") is not None else None,
            )
            return RadarState._GoSweepFrame(iid=iid, frame=frame)
        except Exception:
            return None

    @staticmethod
    def _go_sweep_frame_signature(frames: list) -> tuple:
        return tuple(
            (
                int(frame.frame_index),
                str(frame.ref_icao),
                round(float(frame.ref_arrival_us), 3),
                str(frame.quality),
                round(float(frame.period_s or 0.0), 9),
                tuple(
                    (
                        str(obs.icao),
                        round(float(obs.arrival_us), 3),
                        round(float(obs.lat), 6),
                        round(float(obs.lon), 6),
                        int(getattr(obs, "n_replies", 1)),
                        round(float(getattr(obs, "position_age_seconds", 0.0)), 3),
                    )
                    for obs in frame.observations
                ),
            )
            for frame in frames
        )

    def _store_go_sweep_frame_locked(self, iid: int, frame) -> None:
        buf = self._go_sweep_frames_by_iid.setdefault(
            iid,
            deque(maxlen=self._GO_SWEEP_FRAMES_MAX),
        )
        for idx, existing in enumerate(buf):
            if int(existing.frame_index) != int(frame.frame_index):
                continue
            if self._go_sweep_frame_signature([existing]) == self._go_sweep_frame_signature([frame]):
                return
            buf[idx] = frame
            self._go_sweep_frames_revision[iid] = self._go_sweep_frames_revision.get(iid, 0) + 1
            return
        buf.append(frame)
        self._go_sweep_frames_revision[iid] = self._go_sweep_frames_revision.get(iid, 0) + 1

    def update_go_frame_ready(self, frame_dict: dict) -> None:
        normalised = self._normalise_go_sweep_frame({
            "iid": frame_dict.get("i"),
            "frame_index": frame_dict.get("fi"),
            "period_s": frame_dict.get("p"),
            "ref_icao": frame_dict.get("rc"),
            "ref_lat": frame_dict.get("rla"),
            "ref_lon": frame_dict.get("rlo"),
            "ref_arrival_us": frame_dict.get("ra"),
            "quality": frame_dict.get("q"),
            "observations": [
                {
                    "icao": obs.get("c"),
                    "lat": obs.get("la"),
                    "lon": obs.get("lo"),
                    "arrival_us": obs.get("a"),
                    "n_replies": obs.get("n"),
                    "position_age_s": obs.get("pa"),
                }
                for obs in (frame_dict.get("obs") or [])
            ],
        })
        if normalised is None:
            return
        with self._lock:
            self._store_go_sweep_frame_locked(normalised.iid, normalised.frame)
            self._go_reference_aircraft_by_iid[normalised.iid] = {
                "status": "SELECTED",
                "ref_icao": normalised.frame.ref_icao,
                "ref_score": None,
                "ref_since_sweep": normalised.frame.frame_index,
                "hysteresis_margin": None,
                "challengers": [],
                "source": "go_frame_ready",
            }

    @staticmethod
    def _normalise_go_evidence_event(entry: dict) -> dict | None:
        if not isinstance(entry, dict):
            return None
        try:
            (
                compact_sync_eligible,
                refined_sync_present,
                refined_sync_usable,
                sync_eligible,
            ) = RadarState._normalise_go_sync_eligibility(entry)
            return {
                "kind": str(entry.get("kind") or "burst_fired"),
                "iid": int(entry["iid"]),
                "icao": f"{int(entry['icao']):06X}",
                "arrival_us": float(entry["arrival_us"]),
                "simple_centroid_us": (
                    float(entry["simple_centroid_us"])
                    if entry.get("simple_centroid_us") is not None else None
                ),
                "weighted_centroid_us": (
                    float(entry["weighted_centroid_us"])
                    if entry.get("weighted_centroid_us") is not None else None
                ),
                "centroid_delta_us": (
                    float(entry["centroid_delta_us"])
                    if entry.get("centroid_delta_us") is not None else None
                ),
                "first_reply_us": (
                    float(entry["first_reply_us"])
                    if entry.get("first_reply_us") is not None else None
                ),
                "strongest_reply_us": (
                    float(entry["strongest_reply_us"])
                    if entry.get("strongest_reply_us") is not None else None
                ),
                "mid_strong_window_us": (
                    float(entry["mid_strong_window_us"])
                    if entry.get("mid_strong_window_us") is not None else None
                ),
                "last_reply_us": (
                    float(entry["last_reply_us"])
                    if entry.get("last_reply_us") is not None else None
                ),
                "span_us": (
                    float(entry["span_us"])
                    if entry.get("span_us") is not None else None
                ),
                "peak_amplitude": (
                    float(entry["peak_amplitude"])
                    if entry.get("peak_amplitude") is not None else None
                ),
                "wall_ts": float(entry["wall_ts"]),
                "n_replies": int(entry.get("n_replies") or 0),
                "signal_dbfs": (
                    float(entry["signal_dbfs"])
                    if entry.get("signal_dbfs") is not None else None
                ),
                "truth_lat": (
                    float(entry["truth_lat"])
                    if entry.get("truth_lat") is not None else None
                ),
                "truth_lon": (
                    float(entry["truth_lon"])
                    if entry.get("truth_lon") is not None else None
                ),
                "position_age_seconds": (
                    float(entry["position_age_s"])
                    if entry.get("position_age_s") is not None else None
                ),
                "association_confidence": float(entry.get("association_confidence") or 0.0),
                "dominant_family": bool(entry.get("dominant_family")),
                "compact_sync_eligible": compact_sync_eligible,
                "refined_sync_present": refined_sync_present,
                "refined_sync_usable": refined_sync_usable,
                "sync_eligible": sync_eligible,
            }
        except Exception:
            return None

    def update_go_burst_fired(self, burst_dict: dict) -> None:
        compact_sync_eligible = burst_dict.get("ce")
        refined_sync_present = burst_dict.get("rp")
        refined_sync_usable = burst_dict.get("ru")
        sync_eligible = burst_dict.get("ru", burst_dict.get("se"))
        entry = self._normalise_go_track_observation({
            "iid": burst_dict.get("i"),
            "icao": burst_dict.get("c"),
            "arrival_us": burst_dict.get("cu"),
            "wall_ts": time.time(),
            "signal_dbfs": burst_dict.get("s"),
            "truth_lat": burst_dict.get("la"),
            "truth_lon": burst_dict.get("lo"),
            "position_age_s": burst_dict.get("pa"),
            "association_confidence": 1.0 if burst_dict.get("la") is not None and burst_dict.get("lo") is not None else 0.0,
            "dominant_family": burst_dict.get("df"),
            "compact_sync_eligible": compact_sync_eligible,
            "refined_sync_present": refined_sync_present,
            "refined_sync_usable": refined_sync_usable,
            "sync_eligible": sync_eligible,
        })
        if entry is None:
            return
        evidence = self._normalise_go_evidence_event({
            "kind": "burst_fired",
            "iid": burst_dict.get("i"),
            "icao": burst_dict.get("c"),
            "arrival_us": burst_dict.get("cu"),
            "simple_centroid_us": burst_dict.get("cs"),
            "weighted_centroid_us": burst_dict.get("cw"),
            "centroid_delta_us": burst_dict.get("cd"),
            "first_reply_us": burst_dict.get("cf"),
            "strongest_reply_us": burst_dict.get("ct"),
            "mid_strong_window_us": burst_dict.get("cm"),
            "last_reply_us": burst_dict.get("cl"),
            "span_us": burst_dict.get("cp"),
            "peak_amplitude": burst_dict.get("pk"),
            "wall_ts": entry["wall_ts"],
            "n_replies": burst_dict.get("n"),
            "signal_dbfs": burst_dict.get("s"),
            "truth_lat": burst_dict.get("la"),
            "truth_lon": burst_dict.get("lo"),
            "position_age_s": burst_dict.get("pa"),
            "association_confidence": entry["association_confidence"],
            "dominant_family": burst_dict.get("df"),
            "compact_sync_eligible": compact_sync_eligible,
            "refined_sync_present": refined_sync_present,
            "refined_sync_usable": refined_sync_usable,
            "sync_eligible": sync_eligible,
        })
        sync_update_iid = None
        sync_update_period_s = None
        with self._lock:
            self._go_track_observations.append(entry)
            if evidence is not None:
                self._go_evidence_events.append(evidence)
                if evidence.get("compact_sync_eligible"):
                    iid = int(evidence["iid"])
                    sync = self._live_sync_states.get(iid)
                    model = self._models.get(iid)
                    period_s = None
                    if sync is not None and sync.period_s > 0:
                        period_s = float(sync.period_s)
                    elif model is not None and model.period_s:
                        period_s = float(model.period_s)
                    if period_s and period_s > 0:
                        sync_update_iid = iid
                        sync_update_period_s = period_s
        if sync_update_iid is not None and sync_update_period_s is not None:
            # Skip Python solver when Go has established authoritative multi-sync
            # state for this IID.  Go's MultiSyncSolver takes over once it has
            # produced its first valid fit; Python resumes if radar-core resets.
            if not self._go_multi_sync_states_by_iid.get(sync_update_iid):
                now_mono = time.monotonic()
                last = self._last_multi_sync_update_ts.get(sync_update_iid, 0.0)
                if (now_mono - last) >= self._MULTI_SYNC_UPDATE_MIN_INTERVAL_S:
                    self._last_multi_sync_update_ts[sync_update_iid] = now_mono
                    self._update_multi_aircraft_sync_state(
                        iid=sync_update_iid,
                        period_s=sync_update_period_s,
                    )

    def _go_track_observation_snapshot(self) -> list[dict]:
        with self._lock:
            return list(self._go_track_observations)

    def _go_evidence_event_snapshot(self, iid: int | None = None) -> list[dict]:
        with self._lock:
            if iid is None:
                return list(self._go_evidence_events)
            return [entry for entry in self._go_evidence_events if int(entry.get("iid", -1)) == iid]

    def _go_multi_sync_admission_snapshot(self, iid: int) -> dict | None:
        with self._lock:
            payload = self._go_multi_sync_admission_by_iid.get(iid)
            return dict(payload) if payload is not None else None

    def _adopt_go_frame_sync_locked(self, iid: int, go_sync: dict) -> None:
        existing = self._live_sync_states.get(iid)
        if existing is not None and existing.source in {"multi_aircraft_burst", "go_multi_aircraft_burst"}:
            return
        if go_sync.get("period_s") is None:
            return
        phase_epoch_us = go_sync.get("phase_epoch_us")
        phase_offset_deg = go_sync.get("phase_offset_deg")
        if phase_epoch_us is None or phase_offset_deg is None:
            return
        last_updated = float(go_sync.get("last_updated") or time.time())
        self._live_sync_states[iid] = LiveSyncState(
            iid=iid,
            period_s=float(go_sync["period_s"]),
            phase_epoch_us=float(phase_epoch_us),
            phase_offset_deg=float(phase_offset_deg),
            sync_quality=float(go_sync.get("sync_quality") or 0.0),
            sync_jitter_deg=float(go_sync.get("sync_jitter_deg") or 5.0),
            last_sync_update_ts=last_updated,
            source="sweep_frame_go",
            usable=bool(go_sync.get("usable", False)),
            residual_ema_deg=float(go_sync.get("residual_ema_deg") or 5.0),
            n_sync_frames=int(go_sync.get("n_sync_frames") or 0),
            n_rejected_frames=int(go_sync.get("n_rejected_frames") or 0),
            last_residual_deg=float(go_sync.get("last_residual_deg") or 0.0),
            holdover=bool(go_sync.get("holdover", False)),
            period_base_s=float(go_sync["period_s"]),
            period_authoritative_source="base",
        )

    def update_go_iid_state(self, iid_state: dict) -> None:
        try:
            iid = int(iid_state["i"])
        except Exception:
            log.debug("RadarState: malformed radar-core IID_STATE ignored", exc_info=True)
            return
        revision = int(iid_state.get("rv") or 0)
        with self._lock:
            if revision and revision < self._go_iid_state_revision.get(iid, 0):
                return
            if revision:
                self._go_iid_state_revision[iid] = revision
            go_sync = self._normalise_go_sync_state({
                "sync_state_present": iid_state.get("sp"),
                "sync_state_usable": iid_state.get("su"),
                "sync_period_s": iid_state.get("sps"),
                "sync_phase_epoch_us": iid_state.get("sep"),
                "sync_phase_offset_deg": iid_state.get("sod"),
                "sync_quality": iid_state.get("sq"),
                "sync_jitter_deg": iid_state.get("sj"),
                "sync_residual_ema_deg": iid_state.get("sre"),
                "sync_last_residual_deg": iid_state.get("slr"),
                "sync_n_frames": iid_state.get("snf"),
                "sync_n_rejected_frames": iid_state.get("snr"),
                "sync_holdover": iid_state.get("sh"),
                "sync_last_updated": iid_state.get("lu"),
            })
            if go_sync is not None:
                self._go_sync_states_by_iid[iid] = go_sync
                existing_ref = (self._go_reference_aircraft_by_iid.get(iid) or {}).get("ref_icao")
                self._record_compact_sync_transition_locked(
                    iid,
                    ref_icao=existing_ref,
                    sync_present=True,
                    phase_epoch_us=go_sync.get("phase_epoch_us"),
                    holdover=go_sync.get("holdover"),
                    event_ts=go_sync.get("last_updated"),
                    source="sweep_frame_go",
                )
                self._adopt_go_frame_sync_locked(iid, go_sync)
            else:
                self._go_sync_states_by_iid.pop(iid, None)
                existing_ref = (self._go_reference_aircraft_by_iid.get(iid) or {}).get("ref_icao")
                self._record_compact_sync_transition_locked(
                    iid,
                    ref_icao=existing_ref,
                    sync_present=False,
                    event_ts=float(iid_state.get("lu") or time.time()),
                    source="sweep_frame_go",
                )

    def update_go_multi_sync_state(self, msg: dict) -> None:
        """Mirror a Go MULTI_SYNC_STATE into a Python LiveSyncState.

        Go's multi-aircraft sync solver runs the same weighted phase fit as the
        Python _update_multi_aircraft_sync_state() path.  When Go is authoritative,
        Python adopts the Go-produced sync state as
        LiveSyncState(source="go_multi_aircraft_burst") so Stage 3 can use it
        without running the Python solver.
        """
        try:
            iid = int(msg["i"])
        except Exception:
            log.debug("RadarState: malformed MULTI_SYNC_STATE ignored", exc_info=True)
            return
        if not bool(msg.get("pr")):
            return  # solver ran but produced no state yet

        period_s = msg.get("p")
        phase_epoch_us = msg.get("pe")
        phase_offset_deg = msg.get("po")
        if period_s is None or phase_epoch_us is None or phase_offset_deg is None:
            return

        period_base_s = float(msg.get("pb") or period_s)
        jitter_deg = float(msg.get("jd") or 5.0)
        residual_ema_deg = float(msg.get("re") or 5.0)
        usable = bool(msg.get("us", False))
        holdover = bool(msg.get("ho", False))
        n_sync_updates = int(msg.get("nu") or 0)
        reacquire_active = bool(msg.get("ra", False))
        last_updated = float(msg.get("ts") or time.time())
        anchor_icao_raw = msg.get("ai")
        anchor_icao = None
        if anchor_icao_raw is not None:
            try:
                anchor_icao = f"{int(anchor_icao_raw):06X}"
            except Exception:
                anchor_icao = None
        anchor_candidates = self._normalise_go_anchor_candidates(msg.get("acs"))
        anchor_row = next((row for row in anchor_candidates if row.get("icao") == anchor_icao), None)
        fit_reject_reasons_raw = msg.get("frr") or {}
        if not isinstance(fit_reject_reasons_raw, dict):
            fit_reject_reasons_raw = {}
        fit_reject_reasons = {
            str(key): int(value)
            for key, value in fit_reject_reasons_raw.items()
            if key is not None
        }
        recovery_trigger_reasons = [
            str(reason) for reason in (msg.get("rtr") or []) if reason is not None
        ]
        active_authority_mode = msg.get("aam")
        period_authoritative_source = (
            "refined" if active_authority_mode == "refined_authoritative" else "base"
        )

        with self._lock:
            self._go_multi_sync_states_by_iid[iid] = dict(msg)
            self._live_sync_states[iid] = LiveSyncState(
                iid=iid,
                period_s=float(period_s),
                period_base_s=period_base_s,
                phase_epoch_us=float(phase_epoch_us),
                phase_offset_deg=float(phase_offset_deg),
                sync_quality=0.8 if usable else 0.4,
                sync_jitter_deg=jitter_deg,
                last_sync_update_ts=last_updated,
                source="go_multi_aircraft_burst",
                usable=usable,
                residual_ema_deg=residual_ema_deg,
                n_sync_frames=n_sync_updates,
                n_rejected_frames=0,
                last_residual_deg=0.0,
                holdover=holdover,
                fit_total_observations=int(msg.get("ft") or 0),
                fit_eligible_observations=int(msg.get("fe") or 0),
                fit_rejected_observations=int(msg.get("fr") or 0),
                fit_reject_reasons=fit_reject_reasons,
                fit_contributing_icao_count=int(msg.get("fc") or 0),
                period_reacquire_active=reacquire_active,
                period_reacquire_reason=msg.get("rr"),
                recovery_mode_active=bool(msg.get("rma", reacquire_active)),
                recovery_trigger_reasons=recovery_trigger_reasons,
                phase_anchor_icao=anchor_icao,
                phase_anchor_score=float(msg.get("as") or 0.0),
                phase_anchor_obs_count=int((anchor_row or {}).get("obs_count") or 0),
                phase_anchor_spread_deg=((anchor_row or {}).get("spread_deg")),
                phase_anchor_status=("selected" if anchor_icao else "unavailable"),
                phase_anchor_candidate_count=int(msg.get("ac") or 0),
                phase_anchor_no_candidate_reason=msg.get("anr"),
                phase_anchor_candidates=anchor_candidates,
                dominant_period_s=(float(msg["dp"]) if msg.get("dp") not in (None, 0) else None),
                dominant_prior_period_s=(float(msg["dp"]) if msg.get("dp") not in (None, 0) else None),
                trusted_refined_period_s=(float(msg["trp"]) if msg.get("trp") not in (None, 0) else None),
                bootstrap_period_s=(float(msg["bp"]) if msg.get("bp") not in (None, 0) else None),
                active_family_prior_s=(float(msg["afp"]) if msg.get("afp") not in (None, 0) else None),
                active_family_prior_source=msg.get("afs"),
                dominant_prior_active=bool(msg.get("dpa", False)),
                dominant_period_delta_s=(float(msg["pds"]) if msg.get("pds") is not None else None),
                dominant_period_delta_ppm=(float(msg["pdp"]) if msg.get("pdp") is not None else None),
                compact_period_s=(float(msg["cp"]) if msg.get("cp") not in (None, 0) else None),
                compact_period_delta_to_dominant_s=(float(msg["cds"]) if msg.get("cds") is not None else None),
                compact_period_delta_to_dominant_ppm=(float(msg["cdp"]) if msg.get("cdp") is not None else None),
                compact_sync_unreliable=bool(msg.get("cu", False)),
                compact_gating_bypassed=bool(msg.get("cgb", False)),
                recovery_relaxed_admitted_observations=int(msg.get("rla") or 0),
                active_authority_mode=active_authority_mode,
                authority_switch_count=int(msg.get("asc") or 0),
                last_authority_switch_ts=(float(msg["ast"]) if msg.get("ast") not in (None, 0) else None),
                last_authority_switch_reason=msg.get("asr"),
                authority_enter_streak=int(msg.get("aes") or 0),
                authority_exit_streak=int(msg.get("axs") or 0),
                anchor_switch_count=int(msg.get("anc") or 0),
                last_anchor_switch_ts=(float(msg["ant"]) if msg.get("ant") not in (None, 0) else None),
                last_anchor_switch_reason=msg.get("ahr"),
                anchor_hold_updates=int(msg.get("ahu") or 0),
                period_authoritative_source=period_authoritative_source,
            )

    def _stage3_detection_from_go_observation(self, entry: dict) -> Stage3LiveDetection:
        return Stage3LiveDetection(
            iid=int(entry["iid"]),
            icao=entry.get("icao"),
            arrival_us=float(entry["arrival_us"]),
            wall_ts=float(entry["wall_ts"]),
            df=11,
            signal_dbfs=entry.get("signal_dbfs"),
            receiver_lat=self._receiver_lat,
            receiver_lon=self._receiver_lon,
            truth_lat=entry.get("truth_lat"),
            truth_lon=entry.get("truth_lon"),
            position_age_seconds=entry.get("position_age_seconds"),
            association_confidence=float(entry.get("association_confidence") or 0.0),
        )

    def get_go_frame_positions(self, iid: int) -> list[dict]:
        with self._lock:
            return [dict(entry) for entry in self._go_frame_positions.get(iid, ())]

    def get_go_frame_positions_revision(self, iid: int) -> int:
        with self._lock:
            return int(self._go_frame_positions_revision.get(iid, 0))

    def clear_go_frame_positions(self, iid: int) -> None:
        with self._lock:
            existing = self._go_frame_positions.get(iid)
            if existing:
                self._go_frame_positions.pop(iid, None)
                self._go_frame_positions_revision[iid] = self._go_frame_positions_revision.get(iid, 0) + 1

    def _seed_live_sync_from_frame(
        self,
        iid: int,
        frame: "SweepFrame",
        period_s: float,
        ref_pos_age_s: float | None = None,
    ) -> bool:
        """Seed the live sync state from a completed SweepFrame.

        Shared bootstrap logic used by _finalize_live_frame, inject_frame_from_go,
        and _bootstrap_live_sync_from_recent_frame_if_possible.  No-op if sync
        already exists, radar position is unavailable, ref position is absent, or
        the frame has fewer than 3 aircraft.  Returns True if sync was seeded.
        """
        if self._live_sync_states.get(iid) is not None:
            return False
        model = self._models.get(iid)
        if model is None:
            return False
        radar_pos = _get_authoritative_radar_position(model)
        if radar_pos["lat"] is None or frame.ref_lat is None or frame.ref_lon is None:
            return False
        n_aircraft = 1 + len(frame.observations)
        if n_aircraft < 3:
            return False
        sync_quality = _sync_quality_from_model(model)
        ref_bearing = _bearing_deg_simple(
            radar_pos["lat"], radar_pos["lon"],
            frame.ref_lat, frame.ref_lon,
        )
        self._update_live_sync_state_filtered(
            iid=iid,
            period_s=period_s,
            new_epoch_us=frame.ref_arrival_us,
            new_offset_deg=ref_bearing,
            sync_quality=sync_quality,
            n_aircraft=n_aircraft,
            ref_pos_age_s=ref_pos_age_s,
        )
        return self._live_sync_states.get(iid) is not None

    def _bootstrap_live_sync_from_recent_frame_if_possible(self, iid: int) -> bool:
        """Bootstrap live sync immediately from an existing completed frame.

        Called when a radar position becomes available for an IID that has no
        current sync state.  Without this, refined sync would be absent until
        the next frame completes — even though FM/CI may already have localised
        the radar and recent frames already exist in the completed-frames buffer.

        Scans _live_completed_frames[iid] from newest to oldest and seeds sync
        from the first frame that satisfies:
          - ref lat/lon present
          - at least 3 aircraft total (ref + observations)
          - usable period (frame.period_s, falling back to model.period_s)

        Returns True if sync was bootstrapped.
        """
        if self._live_sync_states.get(iid) is not None:
            return False
        model = self._models.get(iid)
        if model is None:
            return False
        radar_pos = _get_authoritative_radar_position(model)
        if radar_pos["lat"] is None:
            return False
        frames = self._live_completed_frames.get(iid)
        if not frames:
            return False
        for frame in reversed(frames):
            if frame.ref_lat is None or frame.ref_lon is None:
                continue
            if 1 + len(frame.observations) < 3:
                continue
            period_s = frame.period_s if frame.period_s is not None else model.period_s
            if period_s is None:
                continue
            # Age is unknown for frames in the completed-frames buffer (SweepFrame
            # does not carry ref_pos_age_s).  Treat as unknown rather than fresh so
            # the 3-aircraft shortcut does not fire spuriously.
            return self._seed_live_sync_from_frame(iid, frame, period_s, ref_pos_age_s=None)
        return False

    def _update_live_sync_state_filtered(
        self,
        iid: int,
        period_s: float,
        new_epoch_us: float,
        new_offset_deg: float,
        sync_quality: float,
        n_aircraft: int,
        ref_pos_age_s: float | None,
    ) -> None:
        """Update the live sync state with residual-aware smoothing.

        For the initial sync, accept unconditionally.  For subsequent frames,
        compute the circular residual between the new observation and the
        current prediction and apply a weighted update:
          - residual <= 20°: standard EMA update (alpha = 1 / inertia)
          - 20° < residual <= 50°: soft/damped update (alpha = 0.1)
          - residual > 50°: reject — do not move the phase anchor

        The phase epoch is always advanced to the new frame's epoch on accept,
        preventing accumulated error from large (arrival_us - epoch) distances.

        sync_jitter_deg is derived from the residual EMA so bearing uncertainty
        reflects actual sync scatter rather than a fixed constant.

        Quality gate: frames with fewer than 4 aircraft and a stale reference
        position are too weak to steer the sync anchor.
        """
        # Sync-update quality gate — stricter than the localisation permit gate.
        # Marginal frames (n_aircraft == 3) may still trigger the FM callback and
        # produce live bearing observations; they just do not steer the sync anchor
        # unless the reference position is very fresh.
        # A None ref_pos_age_s means the age is unknown; it must not qualify the
        # freshness shortcut (treat as stale for gate purposes).
        sync_eligible = (
            n_aircraft >= 4
            or (n_aircraft >= 3 and ref_pos_age_s is not None and ref_pos_age_s <= 2.0)
        )

        now_ts = time.time()
        existing = self._live_sync_states.get(iid)

        if not sync_eligible:
            # Frame is too weak to steer sync; enter holdover if state exists.
            if existing is not None:
                existing.holdover = True
            return

        if existing is None:
            # First sync for this IID — accept unconditionally.
            self._live_sync_states[iid] = LiveSyncState(
                iid=iid,
                period_s=period_s,
                phase_epoch_us=new_epoch_us,
                phase_offset_deg=new_offset_deg,
                sync_quality=sync_quality,
                sync_jitter_deg=5.0,
                last_sync_update_ts=now_ts,
                source="sweep_frame",
                usable=sync_quality >= 0.3,
                residual_ema_deg=5.0,
                n_sync_frames=1,
                n_rejected_frames=0,
                last_residual_deg=0.0,
                holdover=False,
                period_base_s=period_s,
                residual_slope_deg_per_s=0.0,
                period_correction_ppm=0.0,
                period_refine_enabled=bool(RADAR_SYNC_PERIOD_REFINE_ENABLED),
                predictor_consistency={
                    "burst_sync": True,
                    "period_fit": True,
                    "localiser_live": True,
                    "position_verification": True,
                },
                waveform_enabled=bool(RADAR_SYNC_WAVEFORM_ENABLED),
                waveform_applied=False,
                waveform_bin_count=int(RADAR_SYNC_WAVEFORM_BIN_COUNT),
                waveform_residual_reduction_deg=0.0,
                prop_delay_enabled=bool(RADAR_SYNC_PROP_DELAY_ENABLED),
                motion_comp_phase_enabled=bool(RADAR_SYNC_MOTION_COMP_PHASE_ENABLED),
                motion_comp_fit_enabled=bool(RADAR_SYNC_MOTION_COMP_FIT_ENABLED),
                period_failure_score=0.0,
                period_failure_streak=0,
                period_reacquire_active=False,
                period_reacquire_reason=None,
                period_reacquire_started_ts=None,
                period_refine_mode="normal",
                period_authoritative_source="refined",
                period_failure_primary_class="clean",
                period_reacquire_trigger=None,
                period_recovery_clean_update=False,
                period_recovery_clean_streak=0,
            )
            return

        period_us = period_s * 1e6
        residual_deg = _compute_sync_residual_deg(existing, new_epoch_us, new_offset_deg, period_us)
        abs_residual = abs(residual_deg)

        # Rolling EMA of |residual| — always updated for diagnostics, even on reject.
        _RESIDUAL_EMA_ALPHA = 0.2
        new_residual_ema = (
            (1.0 - _RESIDUAL_EMA_ALPHA) * existing.residual_ema_deg
            + _RESIDUAL_EMA_ALPHA * abs_residual
        )

        # Hard-reject threshold: frame is too inconsistent to trust.
        _RESIDUAL_REJECT_DEG = 50.0
        if abs_residual > _RESIDUAL_REJECT_DEG:
            existing.n_rejected_frames += 1
            existing.last_residual_deg = residual_deg
            existing.residual_ema_deg = new_residual_ema
            existing.sync_jitter_deg = min(max(new_residual_ema, 2.0), 20.0)
            existing.holdover = True
            return

        # Soft-accept threshold: large but survivable residual.
        # Heavily damped alpha prevents one bad frame from jumping the anchor.
        _RESIDUAL_SOFT_DEG = 20.0
        inertia = min(existing.n_sync_frames + 1, 30)
        if abs_residual > _RESIDUAL_SOFT_DEG:
            alpha = 0.1
        else:
            alpha = 1.0 / max(inertia, 2)

        # Express the existing state at the new epoch, then blend the bearing.
        existing_at_new_epoch = (
            (new_epoch_us - existing.phase_epoch_us) / period_us * 360.0
            + existing.phase_offset_deg
        ) % 360.0
        blended_offset = (existing_at_new_epoch + alpha * residual_deg) % 360.0

        new_jitter = min(max(new_residual_ema, 1.5), 15.0)

        self._live_sync_states[iid] = LiveSyncState(
            iid=iid,
            period_s=period_s,
            phase_epoch_us=new_epoch_us,   # advance epoch to keep phase reference fresh
            phase_offset_deg=blended_offset,
            sync_quality=sync_quality,
            sync_jitter_deg=new_jitter,
            last_sync_update_ts=now_ts,
            source="sweep_frame",
            usable=sync_quality >= 0.3,
            residual_ema_deg=new_residual_ema,
            n_sync_frames=existing.n_sync_frames + 1,
            n_rejected_frames=existing.n_rejected_frames,
            last_residual_deg=residual_deg,
            holdover=False,
            # Carry forward refinement/waveform/prop diagnostics from prior state.
            period_base_s=existing.period_base_s or period_s,
            residual_slope_deg_per_s=existing.residual_slope_deg_per_s,
            period_correction_ppm=existing.period_correction_ppm,
            period_refine_enabled=existing.period_refine_enabled,
            period_update_term=existing.period_update_term,
            period_update_direction=existing.period_update_direction,
            period_update_applied=existing.period_update_applied,
            period_update_gain=existing.period_update_gain,
            period_refine_block_reason=existing.period_refine_block_reason,
            period_update_proposed_s=existing.period_update_proposed_s,
            period_update_proposed_us=existing.period_update_proposed_us,
            period_update_applied_s=existing.period_update_applied_s,
            period_update_applied_us=existing.period_update_applied_us,
            period_update_ppm_unclamped=existing.period_update_ppm_unclamped,
            period_update_ppm_applied=existing.period_update_ppm_applied,
            period_update_block_reason=existing.period_update_block_reason,
            period_update_clamp_reason=existing.period_update_clamp_reason,
            period_update_allowed=existing.period_update_allowed,
            period_update_fit_support=existing.period_update_fit_support,
            period_update_fit_span_s=existing.period_update_fit_span_s,
            period_correction_status=existing.period_correction_status,
            period_update_safety_ppm_per_update=existing.period_update_safety_ppm_per_update,
            period_update_safety_ppm_from_base=existing.period_update_safety_ppm_from_base,
            fit_time_basis=existing.fit_time_basis,
            fit_residual_basis=existing.fit_residual_basis,
            fit_total_observations=existing.fit_total_observations,
            fit_eligible_observations=existing.fit_eligible_observations,
            fit_rejected_observations=existing.fit_rejected_observations,
            fit_reject_reasons=dict(existing.fit_reject_reasons),
            fit_contributing_icao_count=existing.fit_contributing_icao_count,
            fit_span_s=existing.fit_span_s,
            predictor_consistency=dict(existing.predictor_consistency),
            waveform_enabled=existing.waveform_enabled,
            waveform_applied=existing.waveform_applied,
            waveform_bin_count=existing.waveform_bin_count,
            waveform_residual_reduction_deg=existing.waveform_residual_reduction_deg,
            waveform_learning_enabled=existing.waveform_learning_enabled,
            waveform_update_block_reason=existing.waveform_update_block_reason,
            waveform_learning_residual_basis=existing.waveform_learning_residual_basis,
            prop_delay_enabled=existing.prop_delay_enabled,
            motion_comp_phase_enabled=existing.motion_comp_phase_enabled,
            motion_comp_fit_enabled=existing.motion_comp_fit_enabled,
            motion_comp_applied_count=existing.motion_comp_applied_count,
            motion_comp_blocked_count=existing.motion_comp_blocked_count,
            motion_comp_mean_dt_us=existing.motion_comp_mean_dt_us,
            motion_comp_mean_residual_improvement_deg=existing.motion_comp_mean_residual_improvement_deg,
            motion_comp_high_rate_mean_residual_improvement_deg=existing.motion_comp_high_rate_mean_residual_improvement_deg,
            phase_anchor_icao=existing.phase_anchor_icao,
            phase_anchor_score=existing.phase_anchor_score,
            phase_anchor_obs_count=existing.phase_anchor_obs_count,
            phase_anchor_offset_raw_deg=existing.phase_anchor_offset_raw_deg,
            phase_anchor_offset_smoothed_deg=existing.phase_anchor_offset_smoothed_deg,
            phase_anchor_spread_deg=existing.phase_anchor_spread_deg,
            phase_anchor_status=existing.phase_anchor_status,
            phase_anchor_since_ts=existing.phase_anchor_since_ts,
            phase_anchor_replacement_reason=existing.phase_anchor_replacement_reason,
            phase_anchor_candidate_count=existing.phase_anchor_candidate_count,
            phase_validation_contributors=existing.phase_validation_contributors,
            phase_validation_reject_count=existing.phase_validation_reject_count,
            phase_validation_median_error_deg=existing.phase_validation_median_error_deg,
            phase_validation_status=existing.phase_validation_status,
            phase_anchor_candidates=list(existing.phase_anchor_candidates),
            period_failure_score=existing.period_failure_score,
            period_failure_streak=existing.period_failure_streak,
            period_reacquire_active=existing.period_reacquire_active,
            period_reacquire_reason=existing.period_reacquire_reason,
            period_reacquire_started_ts=existing.period_reacquire_started_ts,
            period_refine_mode=existing.period_refine_mode,
            period_authoritative_source=existing.period_authoritative_source,
            period_failure_primary_class=existing.period_failure_primary_class,
            period_reacquire_trigger=existing.period_reacquire_trigger,
            period_recovery_clean_update=existing.period_recovery_clean_update,
            period_recovery_clean_streak=existing.period_recovery_clean_streak,
        )

    def _record_live_burst_detection(
        self,
        iid: int,
        icao: str,
        burst_centroid_us: float,
        signal_dbfs: float | None,
        pos: dict | None,
    ) -> None:
        """Record one burst-centre Stage3LiveDetection into the live detection buffer.

        Called once per fired burst so the bearing observation path receives
        burst-centre timestamps rather than individual message arrivals.  Only
        buffers detections for IIDs that have a usable sync state.
        """
        sync_state = self._live_sync_states.get(iid)
        if sync_state is None or not sync_state.usable:
            return

        truth_lat = pos.get("lat") if pos else None
        truth_lon = pos.get("lon") if pos else None
        pos_age = pos.get("position_age_seconds") if pos else None
        assoc_conf = 1.0 if pos is not None else 0.0
        bearing_rate_deg_s = None
        motion_comp_dt_us = None
        motion_comp_block_reason = None
        if truth_lat is not None and truth_lon is not None:
            model = self._models.get(iid)
            radar_pos = _get_authoritative_radar_position(model)
            if radar_pos.get("lat") is not None and radar_pos.get("lon") is not None:
                bearing_deg = _bearing_deg_simple(radar_pos["lat"], radar_pos["lon"], truth_lat, truth_lon)
                history = list(self._live_burst_timeline_obs.get(iid) or self._live_aligned_burst_obs.get(iid) or [])
                motion_estimate = _estimate_aircraft_bearing_rate(
                    icao=icao,
                    bearing_deg=bearing_deg,
                    burst_centroid_us=burst_centroid_us,
                    pos_age_s=pos_age or 0.0,
                    history=history,
                )
                bearing_rate_deg_s = motion_estimate.get("bearing_rate_deg_s")
                motion_comp_block_reason = motion_estimate.get("motion_comp_block_reason")
                motion_comp_dt_us = _compute_motion_comp_dt_us(sync_state.period_s, bearing_rate_deg_s)

        det = Stage3LiveDetection(
            iid=iid,
            icao=icao,
            arrival_us=burst_centroid_us,   # burst-centre timestamp, not raw arrival
            wall_ts=time.time(),
            df=11,
            signal_dbfs=signal_dbfs,
            receiver_lat=self._receiver_lat,
            receiver_lon=self._receiver_lon,
            truth_lat=truth_lat,
            truth_lon=truth_lon,
            position_age_seconds=pos_age,
            association_confidence=assoc_conf,
            bearing_rate_deg_s=bearing_rate_deg_s,
            motion_comp_dt_us=motion_comp_dt_us,
            motion_comp_block_reason=motion_comp_block_reason,
        )
        # deque.append is GIL-safe; no lock needed for single-threaded DF11 path.
        self._live_detection_buffer.append(det)

    def _record_aligned_burst_sync_obs(
        self,
        iid: int,
        icao: str,
        burst_centroid_us: float,
        radar_lat: float,
        radar_lon: float,
        aircraft_lat: float,
        aircraft_lon: float,
        n_replies: int,
        signal_dbfs: float | None,
        pos_age_s: float,
        period_s: float = 0.0,
    ) -> None:
        """Record one burst-centre bearing observation for multi-aircraft sync maintenance.

        Called for every dominant-family burst that has an ADS-B position, from
        both burst processing paths.  The pre-computed geometric bearing is stored
        so _update_multi_aircraft_sync_state() can compute residuals without
        re-fetching positions.

        After inserting the observation, immediately drives _update_multi_aircraft_sync_state
        so sync evolves continuously as bursts arrive rather than waiting for frame
        completion.  period_s must be non-zero for the sync update to fire.
        """
        bearing_deg = _bearing_deg_simple(radar_lat, radar_lon, aircraft_lat, aircraft_lon)
        range_nm = _haversine_nm_simple(radar_lat, radar_lon, aircraft_lat, aircraft_lon)
        prop_delay_us = _compute_propagation_delay_us(range_nm)
        prop_corrected_us = (burst_centroid_us - prop_delay_us) if RADAR_SYNC_PROP_DELAY_ENABLED else burst_centroid_us
        obs_buf = self._live_aligned_burst_obs.setdefault(
            iid, deque(maxlen=self._MULTI_SYNC_OBS_MAX)
        )
        motion_estimate = _estimate_aircraft_bearing_rate(
            icao=icao,
            bearing_deg=bearing_deg,
            burst_centroid_us=burst_centroid_us,
            pos_age_s=pos_age_s,
            history=list(obs_buf),
        )
        bearing_rate_deg_s = motion_estimate.get("bearing_rate_deg_s")
        motion_comp_dt_us = _compute_motion_comp_dt_us(period_s, bearing_rate_deg_s) if period_s > 0 else None
        motion_block_reason = motion_estimate.get("motion_comp_block_reason")
        motion_applied = bool(
            RADAR_SYNC_MOTION_COMP_PHASE_ENABLED
            and motion_comp_dt_us is not None
            and motion_block_reason is None
        )
        if not RADAR_SYNC_MOTION_COMP_PHASE_ENABLED:
            motion_block_reason = "disabled"
        elif motion_comp_dt_us is None and motion_block_reason is None:
            motion_block_reason = "bearing_rate_unavailable"
        effective_us = prop_corrected_us - motion_comp_dt_us if motion_applied else prop_corrected_us
        obs = AlignedBurstSyncObs(
            burst_centroid_us=burst_centroid_us,
            icao=icao,
            bearing_deg=bearing_deg,
            n_replies=n_replies,
            signal_dbfs=signal_dbfs,
            pos_age_s=pos_age_s,
            range_nm=range_nm,
            ts=time.time(),
            sync_update_eligible=True,
            raw_arrival_us=burst_centroid_us,
            prop_delay_aircraft_to_receiver_us=prop_delay_us,
            prop_delay_radar_to_aircraft_us=None,
            effective_arrival_us=effective_us,
            bearing_rate_deg_s=bearing_rate_deg_s,
            motion_comp_dt_us=motion_comp_dt_us,
            motion_corrected_beast_us=effective_us,
            motion_comp_applied=motion_applied,
            motion_comp_block_reason=motion_block_reason,
        )
        obs_buf.append(obs)
        self._prune_live_sync_observation_buffer(obs_buf, now_ts=obs.ts)

        # Drive sync update — throttled per IID so repeated bursts do not trigger
        # a rolling fit on every single arrival.  Observations keep accumulating
        # in obs_buf, so the next update sees the full recent window.
        if period_s > 0.0:
            now_mono = time.monotonic()
            last = self._last_multi_sync_update_ts.get(iid, 0.0)
            if (now_mono - last) >= self._MULTI_SYNC_UPDATE_MIN_INTERVAL_S:
                self._last_multi_sync_update_ts[iid] = now_mono
                self._update_multi_aircraft_sync_state(iid=iid, period_s=period_s)

    def _record_burst_sync_timeline_obs(
        self,
        iid: int,
        icao: str,
        burst_centroid_us: float,
        radar_lat: float,
        radar_lon: float,
        aircraft_lat: float,
        aircraft_lon: float,
        n_replies: int,
        signal_dbfs: float | None,
        pos_age_s: float,
        *,
        sync_update_eligible: bool,
        burst_center_method: str = "centroid",
        burst_center_simple_us: float | None = None,
        burst_center_weighted_us: float | None = None,
        burst_center_delta_us: float | None = None,
        burst_ts_first_reply_beast_us: float | None = None,
        burst_ts_strongest_reply_beast_us: float | None = None,
        burst_ts_simple_centroid_beast_us: float | None = None,
        burst_ts_weighted_centroid_beast_us: float | None = None,
        burst_ts_mid_strong_window_beast_us: float | None = None,
        burst_ts_last_reply_beast_us: float | None = None,
        burst_span_us: float | None = None,
        peak_amplitude: float | None = None,
        position_interpolated: bool = False,
        position_extrapolated: bool = False,
        position_source_age_s: float | None = None,
        truth_position_ts_beast_us: float | None = None,
    ) -> None:
        """Record one burst-centre observation for sync timeline visualisation.

        This buffer is intentionally broader than sync-maintenance updates: it
        includes non-dominant/non-steering observations so the UI can render all
        relevant burst-centre comparisons against the maintained sync model.
        """
        bearing_deg = _bearing_deg_simple(radar_lat, radar_lon, aircraft_lat, aircraft_lon)
        range_nm = _haversine_nm_simple(radar_lat, radar_lon, aircraft_lat, aircraft_lon)
        prop_delay_us = _compute_propagation_delay_us(range_nm)
        prop_corrected_us = (burst_centroid_us - prop_delay_us) if RADAR_SYNC_PROP_DELAY_ENABLED else burst_centroid_us
        timeline_buf = self._live_burst_timeline_obs.setdefault(
            iid, deque(maxlen=self._BURST_SYNC_TIMELINE_OBS_MAX)
        )
        sync = self._live_sync_states.get(iid)
        period_s = sync.period_s if sync is not None and sync.period_s > 0 else 0.0
        motion_estimate = _estimate_aircraft_bearing_rate(
            icao=icao,
            bearing_deg=bearing_deg,
            burst_centroid_us=burst_centroid_us,
            pos_age_s=pos_age_s,
            history=list(timeline_buf),
        )
        bearing_rate_deg_s = motion_estimate.get("bearing_rate_deg_s")
        motion_comp_dt_us = _compute_motion_comp_dt_us(period_s, bearing_rate_deg_s) if period_s > 0 else None
        motion_block_reason = motion_estimate.get("motion_comp_block_reason")
        motion_applied = bool(
            RADAR_SYNC_MOTION_COMP_PHASE_ENABLED
            and motion_comp_dt_us is not None
            and motion_block_reason is None
        )
        if not RADAR_SYNC_MOTION_COMP_PHASE_ENABLED:
            motion_block_reason = "disabled"
        elif motion_comp_dt_us is None and motion_block_reason is None:
            motion_block_reason = "bearing_rate_unavailable"
        effective_us = prop_corrected_us - motion_comp_dt_us if motion_applied else prop_corrected_us
        obs = AlignedBurstSyncObs(
            burst_centroid_us=burst_centroid_us,
            icao=icao,
            bearing_deg=bearing_deg,
            n_replies=n_replies,
            signal_dbfs=signal_dbfs,
            pos_age_s=pos_age_s,
            range_nm=range_nm,
            ts=time.time(),
            sync_update_eligible=sync_update_eligible,
            raw_arrival_us=burst_centroid_us,
            prop_delay_aircraft_to_receiver_us=prop_delay_us,
            prop_delay_radar_to_aircraft_us=None,
            effective_arrival_us=effective_us,
            bearing_rate_deg_s=bearing_rate_deg_s,
            motion_comp_dt_us=motion_comp_dt_us,
            motion_corrected_beast_us=effective_us,
            motion_comp_applied=motion_applied,
            motion_comp_block_reason=motion_block_reason,
            burst_center_simple_us=burst_center_simple_us,
            burst_center_weighted_us=burst_center_weighted_us,
            burst_center_delta_us=burst_center_delta_us,
            burst_center_method=burst_center_method,
            burst_ts_first_reply_beast_us=burst_ts_first_reply_beast_us,
            burst_ts_strongest_reply_beast_us=burst_ts_strongest_reply_beast_us,
            burst_ts_simple_centroid_beast_us=burst_ts_simple_centroid_beast_us,
            burst_ts_weighted_centroid_beast_us=burst_ts_weighted_centroid_beast_us,
            burst_ts_mid_strong_window_beast_us=burst_ts_mid_strong_window_beast_us,
            burst_ts_last_reply_beast_us=burst_ts_last_reply_beast_us,
            burst_span_us=burst_span_us,
            peak_amplitude=peak_amplitude,
            position_interpolated=position_interpolated,
            position_extrapolated=position_extrapolated,
            position_source_age_s=position_source_age_s,
            truth_position_ts_beast_us=truth_position_ts_beast_us,
        )
        timeline_buf.append(obs)
        self._prune_live_sync_observation_buffer(timeline_buf, now_ts=obs.ts)

    @staticmethod
    def _score_sync_burst_observation(obs: AlignedBurstSyncObs) -> float:
        """Quality weight for one aligned burst observation. Higher = more influence.

        Combines reply count, signal strength, and position age into a single
        weight that reduces the influence of marginal or stale observations
        without blocking them entirely.
        """
        # Reply count: more replies → better burst-centre accuracy
        n_weight = min(obs.n_replies / 4.0, 1.0)

        # Signal weight: stronger signal → better amplitude-weighted centre
        if obs.signal_dbfs is not None:
            # Map [-50 dBFS, -10 dBFS] → [0.2, 1.0]; stronger signal gets more weight
            sig_weight = max(0.2, min(1.0, (obs.signal_dbfs + 50.0) / 40.0))
        else:
            sig_weight = 0.5  # neutral when signal strength is unavailable

        # Position age: fresher ADS-B position → more reliable geometric bearing
        if obs.pos_age_s <= 1.0:
            age_weight = 1.0
        elif obs.pos_age_s <= 5.0:
            age_weight = 0.8
        elif obs.pos_age_s <= 10.0:
            age_weight = 0.5
        else:
            age_weight = 0.2

        return n_weight * sig_weight * age_weight

    @staticmethod
    def _classify_sync_residual(abs_residual_deg: float) -> str:
        """Classify a sync residual magnitude as 'inlier', 'soft', or 'rejected'.

        Used by _update_multi_aircraft_sync_state() to weight observations.
        Sync maintenance uses stricter thresholds than localisation candidate
        acceptance so weak data does not aggressively steer the phase anchor.
        """
        if abs_residual_deg <= 20.0:
            return "inlier"
        if abs_residual_deg <= 50.0:
            return "soft"
        return "rejected"

    @staticmethod
    def _implied_phase_offset_deg(
        entry: dict,
        sync: LiveSyncState,
        epoch_us: float,
    ) -> float:
        """Return the offset implied by one observation at epoch_us.

        The timestamp is the same Beast-relative effective timestamp used by
        predict_sync_observation().  Waveform correction is inverted because the
        predictor computes `bearing = phase + offset - waveform_correction`.
        """
        period_us = sync.period_s * 1_000_000.0
        phase_rel = ((entry["effective_us"] - epoch_us) / period_us * 360.0) if period_us > 0 else 0.0
        waveform_corr = getattr(entry["prediction"], "waveform_correction_deg", 0.0) or 0.0
        obs = entry["obs"]
        return (obs.bearing_deg + waveform_corr - phase_rel) % 360.0

    def _select_phase_anchor_aircraft(
        self,
        iid: int,
        scored: list[dict],
        existing: LiveSyncState,
        epoch_us: float,
        now_ts: float,
    ) -> dict:
        """Rank and select the aircraft used for absolute phase anchoring.

        This is deliberately separate from period fitting.  Candidates are
        scored on their own short-window coherence: enough recent observations,
        low circular spread of implied offsets, fresh positions, fit eligibility,
        burst quality, and residual-quality memory.  The current anchor gets
        hysteresis so the selected branch does not flap between similar aircraft.
        """
        by_icao: dict[str, list[dict]] = defaultdict(list)
        for entry in scored:
            obs = entry["obs"]
            if not getattr(obs, "sync_update_eligible", True):
                continue
            if obs.icao:
                by_icao[obs.icao].append(entry)

        quality_memory = self._live_icao_sync_quality.get(iid) or {}
        candidates: list[dict] = []
        for icao, rows in by_icao.items():
            count = len(rows)
            offsets = [self._implied_phase_offset_deg(row, existing, epoch_us) for row in rows]
            weights = [max(row.get("anchor_weight", 0.0), 0.05) for row in rows]
            offset_mean = _circular_weighted_mean_deg(offsets, weights)
            spread = _circular_mad_deg(offsets, offset_mean)
            fit_count = sum(1 for row in rows if row.get("fit_eligible") and row.get("weight", 0.0) > 0.0)
            fit_fraction = fit_count / count if count else 0.0
            pos_ages = sorted(row["obs"].pos_age_s for row in rows if row["obs"].pos_age_s is not None)
            median_pos_age = _median_float(pos_ages) if pos_ages else None
            last_age_s = now_ts - max(row["obs_ts"] for row in rows)
            signals = [row["obs"].signal_dbfs for row in rows if row["obs"].signal_dbfs is not None]
            mean_signal = sum(signals) / len(signals) if signals else None
            mean_weight = sum(weights) / len(weights) if weights else 0.0
            q_entry = quality_memory.get(icao)
            q_mad = q_entry.residual_mad_deg if q_entry is not None else None
            q_bias = abs(q_entry.residual_median_deg) if q_entry is not None else None

            reject_reasons = []
            warning_reasons = []
            if count < 3:
                reject_reasons.append("insufficient_observations")
            if spread is None or spread > 60.0:
                reject_reasons.append("phase_spread_too_large")
            elif spread > 18.0:
                warning_reasons.append("phase_spread_high")
            if median_pos_age is not None and median_pos_age > 15.0:
                reject_reasons.append("stale_positions")
            elif median_pos_age is not None and median_pos_age > 6.0:
                warning_reasons.append("position_age_high")
            if last_age_s > 30.0:
                reject_reasons.append("stale_anchor_observations")
            elif last_age_s > 12.0:
                warning_reasons.append("anchor_observations_aging")
            quality_warning = _icao_quality_anchor_warning(q_entry)
            if quality_warning is not None:
                warning_reasons.append(quality_warning)

            count_score = min(count / 8.0, 1.0)
            spread_score = 0.0 if spread is None else max(0.0, min(1.0, 1.0 - spread / 30.0))
            age_score = 0.5 if median_pos_age is None else max(0.0, min(1.0, 1.0 - median_pos_age / 15.0))
            signal_score = 0.6 if mean_signal is None else max(0.0, min(1.0, (mean_signal + 50.0) / 35.0))
            quality_score = _icao_quality_memory_score(q_entry)
            score = 100.0 * (
                0.28 * count_score
                + 0.30 * spread_score
                + 0.18 * fit_fraction
                + 0.12 * age_score
                + 0.07 * signal_score
                + 0.05 * quality_score
            )
            if reject_reasons:
                score *= 0.25
            elif warning_reasons:
                score *= 0.75

            candidates.append({
                "icao": icao,
                "score": score,
                "status": "rejected" if reject_reasons else "candidate",
                "reject_reasons": reject_reasons,
                "warning_reasons": warning_reasons,
                "obs_count": count,
                "fit_eligible_count": fit_count,
                "fit_eligible_fraction": fit_fraction,
                "offset_mean_deg": offset_mean,
                "spread_deg": spread,
                "median_pos_age_s": median_pos_age,
                "last_age_s": last_age_s,
                "mean_signal_dbfs": mean_signal,
                "mean_weight": mean_weight,
                "quality_residual_mad_deg": q_mad,
                "quality_residual_bias_deg": q_bias,
                "score_breakdown": {
                    "count": count_score,
                    "spread": spread_score,
                    "fit_fraction": fit_fraction,
                    "position_age": age_score,
                    "signal": signal_score,
                    "quality_memory": quality_score,
                },
            })

        candidates.sort(key=lambda row: row["score"], reverse=True)
        eligible = [row for row in candidates if row["status"] != "rejected"]
        selected = eligible[0] if eligible else None
        replacement_reason = "no_eligible_anchor" if selected is None else None

        current = existing.phase_anchor_icao
        current_candidate = next((row for row in eligible if row["icao"] == current), None) if current else None
        if selected is not None and current_candidate is not None and selected["icao"] != current:
            materially_better = selected["score"] >= current_candidate["score"] + 18.0 and selected["score"] >= current_candidate["score"] * 1.35
            current_poor = (
                (current_candidate.get("spread_deg") is not None and current_candidate["spread_deg"] > 12.0)
                or current_candidate.get("last_age_s", 0.0) > 8.0
                or current_candidate.get("fit_eligible_fraction", 0.0) < 0.6
            )
            if not materially_better and not current_poor:
                selected = current_candidate
                replacement_reason = "kept_current_anchor_hysteresis"
            else:
                replacement_reason = "better_candidate" if materially_better else "current_anchor_degraded"
        elif selected is not None and current and selected["icao"] != current:
            replacement_reason = "current_anchor_not_eligible"
        elif selected is not None and not current:
            replacement_reason = "initial_anchor"

        if selected is not None:
            for row in candidates:
                if row["icao"] == selected["icao"]:
                    row["status"] = "selected"
                    break

        return {
            "selected": selected,
            "candidates": candidates,
            "replacement_reason": replacement_reason,
        }

    def _solve_phase_anchor_from_icao(
        self,
        anchor_icao: str,
        scored: list[dict],
        existing: LiveSyncState,
        epoch_us: float,
    ) -> dict | None:
        """Solve absolute phase offset from one selected aircraft."""
        rows = [
            row for row in scored
            if row["icao"] == anchor_icao
            and row.get("anchor_weight", 0.0) > 0.0
            and getattr(row["obs"], "sync_update_eligible", True)
            and row["obs"].pos_age_s <= 15.0
            and row.get("fit_reject_reason") != "near_wrap_residual"
        ]
        if len(rows) < 3:
            rows = [
                row for row in scored
                if row["icao"] == anchor_icao
                and row.get("anchor_weight", 0.0) > 0.0
                and getattr(row["obs"], "sync_update_eligible", True)
                and row.get("fit_reject_reason") != "near_wrap_residual"
            ]
        if len(rows) < 3:
            return None

        offsets = [self._implied_phase_offset_deg(row, existing, epoch_us) for row in rows]
        weights = [max(row.get("anchor_weight", 0.0), 0.05) for row in rows]
        raw = _circular_weighted_mean_deg(offsets, weights)
        spread = _circular_mad_deg(offsets, raw)
        if raw is None:
            return None

        existing_at_epoch = (
            ((epoch_us - existing.phase_epoch_us) / (existing.period_s * 1_000_000.0) * 360.0)
            + existing.phase_offset_deg
        ) % 360.0 if existing.period_s > 0 else existing.phase_offset_deg
        delta = _circular_delta_deg(raw, existing_at_epoch) or 0.0
        # Anchor updates may correct a wrong absolute branch, but still cap each
        # step so one outlier burst cannot jerk the live sweep.
        alpha = 0.35 if existing.phase_anchor_icao == anchor_icao else 0.65
        if spread is not None and spread <= 4.0 and existing.phase_anchor_icao != anchor_icao:
            alpha = 0.8
        max_step = 120.0 if existing.phase_anchor_icao != anchor_icao and spread is not None and spread <= 5.0 and len(rows) >= 4 else 45.0
        limited_delta = max(-max_step, min(max_step, delta * alpha))
        smoothed = (existing_at_epoch + limited_delta) % 360.0
        return {
            "offset_raw_deg": raw,
            "offset_smoothed_deg": smoothed,
            "spread_deg": spread,
            "obs_count": len(rows),
            "delta_from_existing_deg": delta,
        }

    def _validate_phase_anchor_against_population(
        self,
        anchor_icao: str,
        anchor_offset_deg: float,
        scored: list[dict],
        existing: LiveSyncState,
        epoch_us: float,
    ) -> dict:
        """Use non-anchor aircraft to validate and gently nudge the anchor."""
        by_icao: dict[str, list[dict]] = defaultdict(list)
        for row in scored:
            if row["icao"] == anchor_icao:
                continue
            if row.get("anchor_weight", 0.0) <= 0.0:
                continue
            if not getattr(row["obs"], "sync_update_eligible", True):
                continue
            if row.get("fit_reject_reason") == "near_wrap_residual":
                continue
            by_icao[row["icao"]].append(row)

        contributors = []
        rejected = []
        for icao, rows in by_icao.items():
            if len(rows) < 2:
                rejected.append({"icao": icao, "reason": "insufficient_observations", "error_deg": None})
                continue
            offsets = [self._implied_phase_offset_deg(row, existing, epoch_us) for row in rows]
            weights = [max(row.get("anchor_weight", 0.0), 0.05) for row in rows]
            offset = _circular_weighted_mean_deg(offsets, weights)
            spread = _circular_mad_deg(offsets, offset)
            error = _circular_delta_deg(offset, anchor_offset_deg) if offset is not None else None
            if offset is None or error is None:
                rejected.append({"icao": icao, "reason": "offset_unavailable", "error_deg": None})
            elif spread is not None and spread > 20.0:
                rejected.append({"icao": icao, "reason": "phase_spread_too_large", "error_deg": error})
            elif abs(error) > 45.0:
                rejected.append({"icao": icao, "reason": "branch_disagreement", "error_deg": error})
            elif abs(error) > 25.0:
                rejected.append({"icao": icao, "reason": "validation_gate", "error_deg": error})
            else:
                contributors.append({
                    "icao": icao,
                    "error_deg": error,
                    "offset_deg": offset,
                    "spread_deg": spread,
                    "obs_count": len(rows),
                    "weight": sum(weights),
                })

        errors = [row["error_deg"] for row in contributors]
        median_error = _median_float(sorted(errors)) if errors else None
        nudge = 0.0
        if contributors:
            weighted_error = sum(row["error_deg"] * row["weight"] for row in contributors) / sum(row["weight"] for row in contributors)
            # Validation aircraft may refine the selected branch but cannot pick
            # a different branch; cap the nudge tightly.
            nudge = max(-3.0, min(3.0, weighted_error * 0.15))

        strong_rejects = [row for row in rejected if row.get("error_deg") is not None and abs(row["error_deg"]) > 45.0]
        if contributors:
            status = "confirmed"
        elif strong_rejects:
            status = "population_disagrees"
        else:
            status = "anchor_only"

        return {
            "contributors": contributors,
            "rejected": rejected,
            "contributor_count": len(contributors),
            "reject_count": len(rejected),
            "median_error_deg": median_error,
            "nudge_deg": nudge,
            "status": status,
        }

    def _resolve_phase_anchor_state(
        self,
        iid: int,
        scored: list[dict],
        existing: LiveSyncState,
        epoch_us: float,
        now_ts: float,
        mixed_fallback_offset: float,
    ) -> dict:
        """Resolve absolute-phase anchor selection, solve, and validation.

        This keeps the absolute-phase branch as one coherent unit separate from
        the broader multi-aircraft period-fit state machine.
        """
        anchor_selection = self._select_phase_anchor_aircraft(
            iid=iid,
            scored=scored,
            existing=existing,
            epoch_us=epoch_us,
            now_ts=now_ts,
        )
        selected_anchor = anchor_selection.get("selected")
        anchor_solution = None
        validation = {
            "contributors": [],
            "rejected": [],
            "contributor_count": 0,
            "reject_count": 0,
            "median_error_deg": None,
            "nudge_deg": 0.0,
            "status": "unavailable",
        }
        phase_anchor_status = "fallback_mixed"
        phase_anchor_icao = existing.phase_anchor_icao
        phase_anchor_score = 0.0
        phase_anchor_obs_count = 0
        phase_anchor_raw = None
        phase_anchor_smoothed = None
        phase_anchor_spread = None
        phase_anchor_since_ts = existing.phase_anchor_since_ts
        phase_anchor_replacement_reason = anchor_selection.get("replacement_reason")
        new_offset = mixed_fallback_offset

        if selected_anchor is not None:
            phase_anchor_icao = selected_anchor["icao"]
            anchor_solution = self._solve_phase_anchor_from_icao(
                phase_anchor_icao,
                scored,
                existing,
                epoch_us,
            )
            if anchor_solution is not None:
                phase_anchor_score = selected_anchor["score"]
                phase_anchor_obs_count = anchor_solution["obs_count"]
                phase_anchor_raw = anchor_solution["offset_raw_deg"]
                phase_anchor_smoothed = anchor_solution["offset_smoothed_deg"]
                phase_anchor_spread = anchor_solution["spread_deg"]
                if existing.phase_anchor_icao != phase_anchor_icao or existing.phase_anchor_since_ts is None:
                    phase_anchor_since_ts = now_ts
                validation = self._validate_phase_anchor_against_population(
                    phase_anchor_icao,
                    phase_anchor_raw,
                    scored,
                    existing,
                    epoch_us,
                )
                if (
                    validation["status"] == "population_disagrees"
                    and validation["reject_count"] >= 2
                    and validation["contributor_count"] == 0
                ):
                    phase_anchor_status = "population_veto"
                    phase_anchor_replacement_reason = "population_veto"
                    new_offset = mixed_fallback_offset
                else:
                    new_offset = (phase_anchor_smoothed + validation["nudge_deg"]) % 360.0
                    phase_anchor_status = "selected" if validation["status"] != "anchor_only" else "anchor_only"
            else:
                phase_anchor_status = "fallback_mixed_anchor_solve_failed"
                phase_anchor_replacement_reason = "anchor_solve_failed"

        return {
            "anchor_selection": anchor_selection,
            "selected_anchor": selected_anchor,
            "anchor_solution": anchor_solution,
            "validation": validation,
            "offset_deg": new_offset,
            "phase_anchor_icao": phase_anchor_icao,
            "phase_anchor_score": phase_anchor_score,
            "phase_anchor_obs_count": phase_anchor_obs_count,
            "phase_anchor_offset_raw_deg": phase_anchor_raw,
            "phase_anchor_offset_smoothed_deg": phase_anchor_smoothed,
            "phase_anchor_spread_deg": phase_anchor_spread,
            "phase_anchor_status": phase_anchor_status,
            "phase_anchor_since_ts": phase_anchor_since_ts,
            "phase_anchor_replacement_reason": phase_anchor_replacement_reason,
        }

    def _update_multi_aircraft_sync_state(
        self,
        iid: int,
        period_s: float,
        sync_quality: float | None = None,
    ) -> None:
        """Update per-IID sync state from the multi-aircraft aligned burst buffer.

        This is the primary sync-maintenance path.  Instead of anchoring on a
        single reference aircraft (as _update_live_sync_state_filtered() does),
        this method fits a robust phase correction from all recent dominant-family
        burst observations across multiple aircraft.

        The phase correction is applied with a conservative gain so the model
        stays stable even when some observations are noisy.  At least two distinct
        aircraft must contribute for an update to proceed — single-aircraft sync is
        inherently fragile against position errors and burst-centre noise.

        Sync maintenance is intentionally stricter than localisation candidate
        acceptance: rejected observations do not influence the phase anchor, but
        they do not suppress localisation attempts either.
        """
        existing = self._live_sync_states.get(iid)
        if existing is None:
            # No seed sync state yet — first frame must still initialise via
            # _update_live_sync_state_filtered() before multi-aircraft updates apply.
            return

        # Use the refined live period (existing.period_s) if it has diverged from
        # the caller's coarse estimate; this keeps per-update residuals aligned with
        # the model the localiser actually sees.  The caller-supplied period_s is
        # the aggregate base — stored as period_base_s when absent.
        base_period_s = existing.period_base_s if existing.period_base_s > 0 else period_s
        live_period_s = existing.period_s if existing.period_s > 0 else period_s
        period_us = live_period_s * 1e6
        now_ts = time.time()

        # Rolling window covering the last few rotations — enough for a robust estimate
        # without stale observations pulling the phase away from the current truth.
        _MULTI_SYNC_WINDOW_ROTATIONS = 6
        window_s = max(live_period_s * _MULTI_SYNC_WINDOW_ROTATIONS, 30.0)
        cutoff_ts = now_ts - window_s
        if self.radar_core_event_sink is not None:
            obs_snapshot = self._go_aligned_burst_sync_snapshot(iid, window_s=window_s)
        else:
            obs_buf = self._live_aligned_burst_obs.get(iid)
            if not obs_buf:
                return
            obs_snapshot = list(obs_buf)

        recent_obs = [o for o in obs_snapshot if o.ts >= cutoff_ts]
        if len(recent_obs) < 3:
            # Too few recent observations to fit a robust correction.
            return

        waveform_bins = self._live_waveform_bins.get(iid)
        icao_quality = self._live_icao_sync_quality.setdefault(iid, {})

        # Compute residuals for each observation against the authoritative model.
        # Residual = observed_bearing - predicted_bearing (wrapped to (-180, 180]).
        # Period fitting uses corrected residuals after current propagation and
        # waveform correction, and uses effective Beast time as the x-axis.  This
        # keeps the fit on the same basis as the predictor and avoids fitting
        # scheduler/wall-clock timing drift.
        #
        # Four predict_sync_observation calls are made per observation to produce
        # four distinct residual decompositions (raw, after-propagation,
        # without-motion, and fully-corrected).  Each decomposition is stored in
        # the scored dict and consumed downstream for diagnostics and period fitting.
        # Collapsing these into fewer calls would require deriving one correction
        # from another, which depends on implementation details of
        # predict_sync_observation that are not guaranteed to be additive.  Left
        # as-is to avoid silent semantic drift.
        scored: list[dict] = []
        fit_reject_reasons: dict[str, int] = defaultdict(int)
        for obs in recent_obs:
            uncorrected_prediction = predict_sync_observation(
                existing,
                obs.burst_centroid_us,
                range_nm=obs.range_nm,
                waveform_bins=None,
                apply_propagation=False,
                apply_waveform=False,
                apply_motion=False,
            )
            raw_prediction = predict_sync_observation(
                existing,
                obs.burst_centroid_us,
                range_nm=obs.range_nm,
                waveform_bins=None,
                apply_waveform=False,
                apply_motion=False,
            )
            without_motion_prediction = predict_sync_observation(
                existing,
                obs.burst_centroid_us,
                range_nm=obs.range_nm,
                waveform_bins=waveform_bins,
                apply_motion=False,
            )
            corrected_prediction = predict_sync_observation(
                existing,
                obs.burst_centroid_us,
                range_nm=obs.range_nm,
                waveform_bins=waveform_bins,
                apply_motion=bool(RADAR_SYNC_MOTION_COMP_FIT_ENABLED),
                bearing_rate_deg_s=getattr(obs, "bearing_rate_deg_s", None),
                motion_comp_dt_us=getattr(obs, "motion_comp_dt_us", None),
                motion_comp_block_reason=getattr(obs, "motion_comp_block_reason", None),
            )
            residual_raw = (
                obs.bearing_deg - uncorrected_prediction.predicted_bearing_deg + 540.0
            ) % 360.0 - 180.0
            residual_after_prop = (
                obs.bearing_deg - raw_prediction.predicted_bearing_deg + 540.0
            ) % 360.0 - 180.0
            residual = (
                obs.bearing_deg - corrected_prediction.predicted_bearing_deg + 540.0
            ) % 360.0 - 180.0
            residual_without_motion = (
                obs.bearing_deg - without_motion_prediction.predicted_bearing_deg + 540.0
            ) % 360.0 - 180.0
            abs_r = abs(residual)
            status = self._classify_sync_residual(abs_r)
            base_w = self._score_sync_burst_observation(obs)
            # Per-ICAO quality downweight: noisy aircraft get smaller influence.
            q_entry = icao_quality.get(obs.icao)
            q_reject = _icao_quality_reject_reason(q_entry)
            if q_entry is not None:
                mad = max(q_entry.residual_mad_deg, 0.5)
                q_multiplier = max(0.1, min(1.0, 1.0 / (1.0 + mad / 3.0)))
            else:
                q_multiplier = 1.0
            if status == "rejected":
                effective_w = 0.0
            elif status == "soft":
                effective_w = base_w * 0.2 * q_multiplier
            else:
                effective_w = base_w * q_multiplier

            fit_reject_reason = None
            if not getattr(obs, "sync_update_eligible", True):
                fit_reject_reason = "not_sync_update_eligible"
            elif abs_r >= 150.0:
                fit_reject_reason = "near_wrap_residual"
            elif abs_r > 35.0:
                fit_reject_reason = "residual_gate"
            elif obs.pos_age_s > 8.0:
                fit_reject_reason = "stale_position"
            elif q_reject is not None:
                fit_reject_reason = q_reject
            elif effective_w <= 0:
                fit_reject_reason = "zero_weight"

            fit_eligible = fit_reject_reason is None
            if fit_reject_reason is not None:
                fit_reject_reasons[fit_reject_reason] += 1

            scored.append({
                "residual": residual,
                "residual_raw": residual_raw,
                "residual_after_prop": residual_after_prop,
                "residual_without_motion": residual_without_motion,
                "motion_comp_improvement_deg": abs(residual_without_motion) - abs(residual),
                "weight": effective_w,
                "anchor_weight": base_w * q_multiplier,
                "status": status,
                "icao": obs.icao,
                "effective_us": corrected_prediction.effective_arrival_us,
                "phase_in_rot": corrected_prediction.phase_in_rot_deg,
                "obs_ts": obs.ts,
                "fit_eligible": fit_eligible,
                "fit_reject_reason": fit_reject_reason,
                "prediction": corrected_prediction,
                "obs": obs,
            })

        # Multi-aircraft period fitting remains distinct from absolute phase
        # anchoring.  A single high-quality aircraft may maintain the absolute
        # branch, but period refinement still requires a multi-aircraft,
        # long-baseline fit below.
        contributing_icaos = {
            e["icao"] for e in scored
            if e["weight"] > 0 and e["status"] != "rejected"
        }

        n_inliers = sum(1 for e in scored if e["status"] == "inlier")
        n_rejected = sum(1 for e in scored if e["status"] == "rejected")
        fit_scored = [
            e for e in scored
            if e["fit_eligible"] and e["weight"] > 0 and e["status"] != "rejected"
        ]
        fit_contributing_icaos = {e["icao"] for e in fit_scored}

        # A majority of residual outliers blocks period refinement, but does
        # not by itself block anchor selection.  A wrong absolute branch makes
        # every good anchor observation look like a residual outlier until the
        # branch is corrected.
        majority_rejected = n_rejected >= len(recent_obs) // 2 + 1

        # Weighted linear fit residual ~ a + b*(effective_beast_time_s - t_ref).
        # a → phase correction term; b → angular-rate mismatch (deg/s) that converts
        # to a small period correction.  Both fall out of the same fit so phase and
        # period adjustments never fight each other.
        fit_pool = fit_scored if fit_scored else [
            e for e in scored
            if e.get("anchor_weight", 0.0) > 0.0
            and e.get("fit_reject_reason") != "near_wrap_residual"
        ]
        if not fit_pool:
            existing.holdover = True
            existing.usable = False
            return
        t_ref = min(e["effective_us"] for e in fit_pool) / 1_000_000.0
        xs = [(e["effective_us"] / 1_000_000.0) - t_ref for e in fit_pool]
        ys = [e["residual"] for e in fit_pool]
        ws = [e["weight"] if e in fit_scored else e.get("anchor_weight", 0.0) for e in fit_pool]
        a_fit, b_fit = _fit_weighted_slope(xs, ys, ws)

        # Weighted phase correction (the a term) — equivalent to the previous
        # weighted mean when b=0.
        phase_correction = a_fit

        # Conservative gain: multi-aircraft sync should be stable and predictable.
        # Lower gain than the single-frame alpha so the model does not jump on
        # individual noisy frames.  Gain is ~0.10–0.15 depending on inlier count.
        n_eff = max(n_inliers, 1)
        _MULTI_SYNC_GAIN_BASE = 0.12
        _MULTI_SYNC_GAIN_MAX = 0.20
        gain = min(_MULTI_SYNC_GAIN_BASE + 0.01 * (n_eff - 1), _MULTI_SYNC_GAIN_MAX)
        limited_correction = phase_correction * gain

        # Advance epoch to the most recent inlier/soft observation.
        # Keeping the epoch fresh prevents accumulated error from large
        # (burst_centroid_us - phase_epoch_us) distances.
        anchor_pool = [
            e for e in scored
            if e.get("anchor_weight", 0.0) > 0.0
            and e.get("fit_reject_reason") != "near_wrap_residual"
        ]
        if not anchor_pool:
            return
        new_epoch_us = max(e["effective_us"] for e in anchor_pool)

        # Propagate the existing model to the new epoch.  This is used only as
        # the smoothing baseline and fallback; the preferred absolute phase
        # offset comes from one selected anchor aircraft below.
        existing_at_new = (
            (new_epoch_us - existing.phase_epoch_us) / period_us * 360.0
            + existing.phase_offset_deg
        ) % 360.0
        mixed_fallback_offset = (existing_at_new + limited_correction) % 360.0
        anchor_resolution = self._resolve_phase_anchor_state(
            iid=iid,
            scored=scored,
            existing=existing,
            now_ts=now_ts,
            epoch_us=new_epoch_us,
            mixed_fallback_offset=mixed_fallback_offset,
        )
        anchor_selection = anchor_resolution["anchor_selection"]
        anchor_solution = anchor_resolution["anchor_solution"]
        validation = anchor_resolution["validation"]
        new_offset = anchor_resolution["offset_deg"]
        phase_anchor_icao = anchor_resolution["phase_anchor_icao"]
        phase_anchor_score = anchor_resolution["phase_anchor_score"]
        phase_anchor_obs_count = anchor_resolution["phase_anchor_obs_count"]
        phase_anchor_raw = anchor_resolution["phase_anchor_offset_raw_deg"]
        phase_anchor_smoothed = anchor_resolution["phase_anchor_offset_smoothed_deg"]
        phase_anchor_spread = anchor_resolution["phase_anchor_spread_deg"]
        phase_anchor_status = anchor_resolution["phase_anchor_status"]
        phase_anchor_since_ts = anchor_resolution["phase_anchor_since_ts"]
        phase_anchor_replacement_reason = anchor_resolution["phase_anchor_replacement_reason"]
        phase_correction = _circular_delta_deg(new_offset, existing_at_new) or 0.0

        # Live period refinement from slope.
        # residual = observed - predicted.  If residual rises with effective
        # time, the predicted beam is falling behind: increase angular rate,
        # which decreases period_s.  Negative slope does the opposite.
        #
        # The rotation period is set by hardware and rarely changes.  Once
        # established the model should be stable.  Three design choices enforce
        # this:
        #
        #   1. Slope EMA — the fitted slope b_fit is noisy on a single window.
        #      We maintain a slow EMA of slope (τ ≈ 12 updates) and drive
        #      period corrections from the smoothed value, not the raw fit.
        #      This prevents one noisy window from immediately moving the period.
        #
        #   2. Persistent-slope gate — any period update requires the smoothed
        #      slope to have pointed in the same direction for at least 5 of the
        #      last 8 stored entries AND exceed a dead-band threshold.  Transient
        #      sign flips that cause overshoot-then-correct oscillation are
        #      suppressed because the gate will not re-open immediately after the
        #      period moves.
        #
        #   3. Reduced gain — _PERIOD_GAIN is lowered from 0.25 to 0.12 so each
        #      accepted correction is smaller and the convergence is monotone
        #      rather than oscillatory.
        refined_period_s = live_period_s
        span_s = max(xs, default=0.0) if xs else 0.0
        _PERIOD_REFINE_MIN_INLIERS = 6
        _PERIOD_REFINE_MIN_SPAN_ROT = 2.0
        _PERIOD_GAIN = 0.12
        _PERIOD_PPM_PER_UPDATE_MAX = 60.0
        _PERIOD_PPM_PER_UPDATE_STRONG_MAX = 400.0
        _PERIOD_PPM_FROM_BASE_MAX = 2000.0
        _PERIOD_PPM_FROM_BASE_STRONG_MAX = 15000.0
        # Slow EMA of the fitted slope.  α ≈ 0.08 → time constant ~12 updates.
        # Stored in residual_slope_deg_per_s; the raw b_fit is in slope_history.
        _SLOPE_EMA_ALPHA = 0.08
        _SLOPE_DEAD_BAND = 0.08  # deg/s — smoothed slope must exceed this to drive period change
        smoothed_slope = (
            (1.0 - _SLOPE_EMA_ALPHA) * existing.residual_slope_deg_per_s
            + _SLOPE_EMA_ALPHA * b_fit
        )
        period_update_term = 0.0
        period_update_applied = 0.0
        period_update_direction = "none"
        period_update_ppm_unclamped = 0.0
        period_update_ppm_applied = 0.0
        period_update_block_reason = None
        period_update_clamp_reason = None
        period_update_allowed = False
        # Persistence check over a wider window (last 8 entries, require 5 to agree).
        # Uses the stored smoothed-slope values (residual_slope_deg_per_s field),
        # not raw b_fit, so the gate reflects the same damped signal used for correction.
        previous_slope_rows = list(self._live_slope_history.get(iid) or [])[-8:]
        _PERSIST_MIN_ENTRIES = 5
        _PERSIST_DEAD_BAND = _SLOPE_DEAD_BAND
        slope_signs = [
            1 if float(row.get("residual_slope_deg_per_s") or 0.0) > 0 else -1
            for row in previous_slope_rows
            if abs(float(row.get("residual_slope_deg_per_s") or 0.0)) >= _PERSIST_DEAD_BAND
        ]
        current_slope_sign = 1 if smoothed_slope > 0 else -1 if smoothed_slope < 0 else 0
        persistent_slope = (
            current_slope_sign != 0
            and len(slope_signs) >= _PERSIST_MIN_ENTRIES
            and sum(1 for s in slope_signs if s == current_slope_sign) >= _PERSIST_MIN_ENTRIES
            and abs(smoothed_slope) >= _SLOPE_DEAD_BAND
        )
        strong_fit_for_period = (
            len(fit_scored) >= 12
            and len(fit_contributing_icaos) >= 3
            and span_s >= 4.0 * live_period_s
            and n_rejected <= max(1, len(recent_obs) // 3)
        )
        adaptive_period_clamp = strong_fit_for_period and persistent_slope
        ppm_per_update_limit = (
            _PERIOD_PPM_PER_UPDATE_STRONG_MAX
            if adaptive_period_clamp else _PERIOD_PPM_PER_UPDATE_MAX
        )
        ppm_from_base_limit = (
            _PERIOD_PPM_FROM_BASE_STRONG_MAX
            if adaptive_period_clamp else _PERIOD_PPM_FROM_BASE_MAX
        )
        freeze_period_refine = bool(existing.period_reacquire_active)
        # persistent_slope is now a hard gate on any period update, not just
        # the strong-clamp path.  Without a persistent smoothed slope the period
        # holds its current value even if the raw fit shows a non-zero slope.
        refine_ok = (
            bool(RADAR_SYNC_PERIOD_REFINE_ENABLED)
            and not freeze_period_refine
            and not majority_rejected
            and len(fit_scored) >= _PERIOD_REFINE_MIN_INLIERS
            and len(fit_contributing_icaos) >= 2
            and span_s >= _PERIOD_REFINE_MIN_SPAN_ROT * live_period_s
            and persistent_slope
        )
        if freeze_period_refine:
            period_update_block_reason = "reacquire_active"
        elif not RADAR_SYNC_PERIOD_REFINE_ENABLED:
            period_update_block_reason = "disabled"
        elif majority_rejected:
            period_update_block_reason = "majority_rejected"
        elif len(fit_scored) < _PERIOD_REFINE_MIN_INLIERS:
            period_update_block_reason = "insufficient_fit_observations"
        elif len(fit_contributing_icaos) < 2:
            period_update_block_reason = "insufficient_fit_icaos"
        elif span_s < _PERIOD_REFINE_MIN_SPAN_ROT * live_period_s:
            period_update_block_reason = "insufficient_fit_span"
        elif not persistent_slope:
            period_update_block_reason = "slope_not_persistent"
        if refine_ok and live_period_s > 0:
            period_update_allowed = True
            rate_nominal = 360.0 / live_period_s
            # Use the smoothed slope — not the raw b_fit — for the period correction
            # so transient noise in the fit does not immediately move the period.
            rate_target = rate_nominal + smoothed_slope * _PERIOD_GAIN
            if rate_target > 0:
                candidate = 360.0 / rate_target
                period_update_term = candidate - live_period_s
                period_update_ppm_unclamped = period_update_term / live_period_s * 1e6
                # Per-update ppm clamp.
                delta_ppm = (candidate - live_period_s) / live_period_s * 1e6
                if delta_ppm > ppm_per_update_limit:
                    candidate = live_period_s * (1.0 + ppm_per_update_limit * 1e-6)
                    period_update_clamp_reason = "per_update_clamped"
                elif delta_ppm < -ppm_per_update_limit:
                    candidate = live_period_s * (1.0 - ppm_per_update_limit * 1e-6)
                    period_update_clamp_reason = "per_update_clamped"
                # Absolute ppm from base clamp.
                if base_period_s > 0:
                    abs_ppm = (candidate - base_period_s) / base_period_s * 1e6
                    if abs_ppm > ppm_from_base_limit:
                        candidate = base_period_s * (1.0 + ppm_from_base_limit * 1e-6)
                        period_update_clamp_reason = "base_ppm_clamped"
                    elif abs_ppm < -ppm_from_base_limit:
                        candidate = base_period_s * (1.0 - ppm_from_base_limit * 1e-6)
                        period_update_clamp_reason = "base_ppm_clamped"
                refined_period_s = candidate
                period_update_applied = refined_period_s - live_period_s
                period_update_ppm_applied = period_update_applied / live_period_s * 1e6
                if period_update_applied > 0:
                    period_update_direction = "increase"
                elif period_update_applied < 0:
                    period_update_direction = "decrease"
                else:
                    period_update_direction = "none"
            else:
                period_update_allowed = False
                period_update_block_reason = "non_positive_rate_target"
        period_refine_block_reason = period_update_block_reason
        period_correction_status = _classify_period_correction_status(
            period_update_allowed,
            period_update_block_reason,
            period_update_clamp_reason,
            period_update_term,
            period_update_applied,
            b_fit,
            len(fit_scored),
            len(recent_obs),
            span_s,
        )
        period_correction_ppm = (
            (refined_period_s - base_period_s) / base_period_s * 1e6
            if base_period_s > 0 else 0.0
        )
        period_failure = _assess_period_failure_mode(
            recent_obs_count=len(recent_obs),
            n_rejected=n_rejected,
            recent_icao_count=len({e["icao"] for e in scored}),
            fit_support_count=len(fit_scored),
            fit_contributing_icao_count=len(fit_contributing_icaos),
            period_update_block_reason=period_update_block_reason,
            phase_correction_deg=phase_correction,
            phase_anchor_status=phase_anchor_status,
            phase_validation_status=validation["status"],
            phase_validation_contributors=validation["contributor_count"],
            fit_reject_reasons=dict(fit_reject_reasons),
            scored=scored,
            fit_origin_s=t_ref,
            fit_intercept_deg=a_fit,
            fit_slope_deg_per_s=b_fit,
            fit_span_s=span_s,
            period_s=live_period_s,
        )
        anchor_failure = _assess_anchor_failure_mode(
            phase_anchor_status=phase_anchor_status,
            phase_validation_status=validation["status"],
            phase_validation_contributors=validation["contributor_count"],
            phase_validation_reject_count=validation["reject_count"],
            phase_correction_deg=phase_correction,
        )
        wrong_period_suspect = bool(period_failure["wrong_period_suspect"])
        period_failure_streak = existing.period_failure_streak + 1 if wrong_period_suspect else 0
        clean_reacquire_update = (
            len(recent_obs) > 0
            and (n_rejected / len(recent_obs)) < 0.25
            and len(fit_scored) >= 6
            and len(fit_contributing_icaos) >= 2
            and (period_failure["detrended_median_abs_residual_deg"] or 999.0) <= 8.0
            and validation["status"] != "population_disagrees"
        )
        reacquire_active = bool(existing.period_reacquire_active)
        reacquire_reason = existing.period_reacquire_reason
        reacquire_started_ts = existing.period_reacquire_started_ts
        period_reacquire_trigger = existing.period_reacquire_trigger
        period_refine_mode = "suspect" if wrong_period_suspect else "normal"
        period_authoritative_source = "refined"
        period_failure_primary_class = period_failure["failure_primary_class"]
        clean_reacquire_streak = 0
        if wrong_period_suspect:
            self._live_period_clean_update_streak[iid] = 0

        enter_reacquire = False
        if reacquire_active:
            if clean_reacquire_update:
                clean_reacquire_streak = self._live_period_clean_update_streak.get(iid, 0) + 1
                self._live_period_clean_update_streak[iid] = clean_reacquire_streak
                if clean_reacquire_streak >= 2:
                    reacquire_active = False
                    reacquire_reason = None
                    reacquire_started_ts = None
                    period_reacquire_trigger = None
                    period_refine_mode = "normal"
                    period_authoritative_source = "refined"
                    period_failure_streak = 0
                    self._live_period_clean_update_streak[iid] = 0
                else:
                    period_refine_mode = "reacquire"
                    period_authoritative_source = "base"
            else:
                self._live_period_clean_update_streak[iid] = 0
                clean_reacquire_streak = 0
                period_refine_mode = "reacquire"
                period_authoritative_source = "base"
                if wrong_period_suspect:
                    reacquire_reason = period_failure["failure_reason"]
        else:
            severe_combined_failure = (
                period_failure["failure_primary_class"] == "combined"
                and anchor_failure["severe_anchor_failure"]
                and wrong_period_suspect
            )
            if period_failure_streak >= 3:
                enter_reacquire = True
                period_reacquire_trigger = "persistent_period_family_failure"
            elif period_failure["immediate_reacquire"]:
                enter_reacquire = True
                period_reacquire_trigger = "severe_period_family_failure"
            elif severe_combined_failure:
                enter_reacquire = True
                period_reacquire_trigger = "severe_combined_failure"
        if enter_reacquire:
            reacquire_active = True
            reacquire_reason = period_failure["failure_reason"]
            reacquire_started_ts = now_ts
            period_refine_mode = "reacquire"
            period_authoritative_source = "base"
            self._live_period_clean_update_streak[iid] = 0
        if reacquire_active:
            refined_period_s = base_period_s if base_period_s > 0 else live_period_s
            period_update_allowed = False
            period_update_term = 0.0
            period_update_applied = 0.0
            period_update_direction = "none"
            period_update_ppm_unclamped = 0.0
            period_update_ppm_applied = 0.0
            period_update_block_reason = "reacquire_active"
            period_refine_block_reason = "reacquire_active"
            period_update_clamp_reason = None
            period_correction_status = _classify_period_correction_status(
                False,
                period_update_block_reason,
                None,
                0.0,
                0.0,
                b_fit,
                len(fit_scored),
                len(recent_obs),
                span_s,
            )
            period_correction_ppm = (
                (refined_period_s - base_period_s) / base_period_s * 1e6
                if base_period_s > 0 else 0.0
            )
        elif anchor_failure["anchor_branch_suspect"] and period_failure_primary_class == "clean":
            period_failure_primary_class = "anchor_branch"
            period_refine_mode = "normal"

        # Waveform bin learning: slow EMA of detrended residuals keyed by
        # phase-in-rotation.  Period refinement sees long-term drift first;
        # waveform learning then tracks repeatable intra-rotation structure.
        waveform_learning_enabled = False
        waveform_update_block_reason = None
        if RADAR_SYNC_WAVEFORM_ENABLED:
            n_bins = max(int(RADAR_SYNC_WAVEFORM_BIN_COUNT), 4)
            if waveform_bins is None or len(waveform_bins) != n_bins:
                waveform_bins = [WaveformBin() for _ in range(n_bins)]
                self._live_waveform_bins[iid] = waveform_bins
            _WAVEFORM_ALPHA = 0.02
            waveform_learning_enabled = abs(b_fit) <= 2.0 or len(fit_scored) < _PERIOD_REFINE_MIN_INLIERS
            if not waveform_learning_enabled:
                waveform_update_block_reason = "slope_too_large"
            for e in scored:
                if not waveform_learning_enabled or e["weight"] <= 0 or e["status"] == "rejected":
                    continue
                residual_detrended = e["residual_raw"] - (a_fit + b_fit * ((e["effective_us"] / 1_000_000.0) - t_ref))
                idx = _waveform_bin_index(e["phase_in_rot"], n_bins)
                bin_entry = waveform_bins[idx]
                bin_entry.correction_deg = (
                    (1.0 - _WAVEFORM_ALPHA) * bin_entry.correction_deg
                    + _WAVEFORM_ALPHA * residual_detrended
                )
                bin_entry.weight += e["weight"]
                bin_entry.n += 1
            # Circular 3-wide triangular smoothing (0.25, 0.5, 0.25) over correction.
            if n_bins >= 3:
                smoothed = [0.0] * n_bins
                for i in range(n_bins):
                    a = waveform_bins[(i - 1) % n_bins].correction_deg
                    b = waveform_bins[i].correction_deg
                    c = waveform_bins[(i + 1) % n_bins].correction_deg
                    smoothed[i] = 0.25 * a + 0.5 * b + 0.25 * c
                for i in range(n_bins):
                    waveform_bins[i].correction_deg = smoothed[i]
            # Decide whether the waveform has sufficient coverage to be applied.
            _WAVEFORM_MIN_BINS_FILLED = max(n_bins * 2 // 3, 8)
            _WAVEFORM_MIN_N_PER_BIN = 5
            bins_filled = sum(1 for bin_entry in waveform_bins if bin_entry.n >= _WAVEFORM_MIN_N_PER_BIN)
            waveform_applied_new = bins_filled >= _WAVEFORM_MIN_BINS_FILLED
        else:
            waveform_applied_new = False
            waveform_update_block_reason = "disabled"

        # Waveform residual reduction diagnostic: average |residual_raw| vs
        # |residual_corrected| across inlier/soft observations.
        abs_raw = [abs(e["residual_raw"]) for e in scored if e["status"] in ("inlier", "soft")]
        abs_corr = [abs(e["residual"]) for e in scored if e["status"] in ("inlier", "soft")]
        if abs_raw and abs_corr:
            reduction = (sum(abs_raw) / len(abs_raw)) - (sum(abs_corr) / len(abs_corr))
        else:
            reduction = existing.waveform_residual_reduction_deg
        _WAVEFORM_REDUCTION_ALPHA = 0.2
        waveform_reduction_ema = (
            (1.0 - _WAVEFORM_REDUCTION_ALPHA) * existing.waveform_residual_reduction_deg
            + _WAVEFORM_REDUCTION_ALPHA * reduction
        )

        motion_applied_entries = [
            e for e in scored
            if e["prediction"].motion_comp_applied and e["status"] in ("inlier", "soft")
        ]
        motion_blocked_count = sum(
            1 for e in scored
            if e["prediction"].motion_comp_enabled and not e["prediction"].motion_comp_applied
        )
        motion_mean_dt_us = (
            sum(e["prediction"].motion_comp_dt_us or 0.0 for e in motion_applied_entries) / len(motion_applied_entries)
            if motion_applied_entries else 0.0
        )
        motion_mean_improvement = (
            sum(e["motion_comp_improvement_deg"] for e in motion_applied_entries) / len(motion_applied_entries)
            if motion_applied_entries else 0.0
        )
        high_rate_entries = [
            e for e in motion_applied_entries
            if e["prediction"].bearing_rate_deg_s is not None and abs(e["prediction"].bearing_rate_deg_s) >= 0.2
        ]
        motion_high_rate_mean_improvement = (
            sum(e["motion_comp_improvement_deg"] for e in high_rate_entries) / len(high_rate_entries)
            if high_rate_entries else None
        )

        # Per-ICAO quality memory update: EMA of signed residual (bias) and
        # |residual − bias| (spread).  Used as a downweight multiplier above.
        _update_icao_sync_quality_memory(icao_quality, scored)

        # Derive sync_jitter_deg from the spread of inlier residuals.
        # This ties jitter to actual recent behaviour rather than a fixed constant.
        inlier_abs_residuals = [abs(e["residual"]) for e in scored if e["status"] == "inlier"]
        if len(inlier_abs_residuals) >= 3:
            try:
                new_jitter = min(max(statistics.stdev(inlier_abs_residuals), 1.5), 15.0)
            except statistics.StatisticsError:
                new_jitter = existing.sync_jitter_deg
        elif len(inlier_abs_residuals) >= 1:
            new_jitter = min(max(statistics.mean(inlier_abs_residuals), 1.5), 15.0)
        else:
            # No clean inliers — inflate jitter slightly but do not reset.
            new_jitter = min(existing.sync_jitter_deg * 1.1, 15.0)

        # Residual EMA: use abs(phase_correction) as a proxy for recent residual scatter.
        _RESIDUAL_EMA_ALPHA = 0.2
        new_ema = (
            (1.0 - _RESIDUAL_EMA_ALPHA) * existing.residual_ema_deg
            + _RESIDUAL_EMA_ALPHA * abs(phase_correction)
        )

        q = sync_quality if sync_quality is not None else existing.sync_quality

        # Derive usable from current sync evidence rather than inheriting the previous value.
        # Requires: ≥3 inliers, ≥2 contributing aircraft, jitter below threshold,
        # and acceptable rejection ratio.
        _MIN_INLIERS_FOR_USABLE = 3
        _MAX_JITTER_FOR_USABLE = 15.0
        anchor_ok = (
            anchor_solution is not None
            and phase_anchor_icao is not None
            and phase_anchor_obs_count >= 3
            and (phase_anchor_spread is None or phase_anchor_spread <= 18.0)
            and phase_anchor_status in ("selected", "anchor_only")
        )
        new_usable = (
            (anchor_ok or (n_inliers >= _MIN_INLIERS_FOR_USABLE and len(contributing_icaos) >= 2))
            and new_jitter < _MAX_JITTER_FOR_USABLE
            and (anchor_ok or n_rejected < len(recent_obs) // 2 + 1)
        )

        predictor_consistency = {
            "burst_sync": True,
            "period_fit": True,
            "localiser_live": True,
            "position_verification": True,
        }

        new_state = LiveSyncState(
            iid=iid,
            period_s=refined_period_s,
            phase_epoch_us=new_epoch_us,
            phase_offset_deg=new_offset,
            sync_quality=q,
            sync_jitter_deg=new_jitter,
            last_sync_update_ts=now_ts,
            source="multi_aircraft_burst",
            usable=new_usable,
            residual_ema_deg=new_ema,
            n_sync_frames=existing.n_sync_frames + 1,
            n_rejected_frames=existing.n_rejected_frames + (1 if n_rejected > max(len(recent_obs) // 2, 1) else 0),
            last_residual_deg=phase_correction,
            holdover=False,
            n_burst_obs_inliers=n_inliers,
            n_burst_obs_rejected=n_rejected,
            contributing_icao_count=len(contributing_icaos),
            period_base_s=base_period_s,
            residual_slope_deg_per_s=smoothed_slope,
            period_correction_ppm=period_correction_ppm,
            period_refine_enabled=bool(RADAR_SYNC_PERIOD_REFINE_ENABLED),
            period_update_term=period_update_term,
            period_update_direction=period_update_direction,
            period_update_applied=period_update_applied,
            period_update_gain=_PERIOD_GAIN,
            period_refine_block_reason=period_refine_block_reason,
            period_update_proposed_s=period_update_term,
            period_update_proposed_us=period_update_term * 1_000_000.0,
            period_update_applied_s=period_update_applied,
            period_update_applied_us=period_update_applied * 1_000_000.0,
            period_update_ppm_unclamped=period_update_ppm_unclamped,
            period_update_ppm_applied=period_update_ppm_applied,
            period_update_block_reason=period_update_block_reason,
            period_update_clamp_reason=period_update_clamp_reason,
            period_update_allowed=period_update_allowed,
            period_update_fit_support=len(fit_scored),
            period_update_fit_span_s=span_s,
            period_correction_status=period_correction_status,
            period_update_safety_ppm_per_update=ppm_per_update_limit,
            period_update_safety_ppm_from_base=ppm_from_base_limit,
            fit_time_basis="effective_beast_time_s",
            fit_residual_basis="observed_minus_authoritative_prediction_after_prop_motion_waveform_deg",
            fit_total_observations=len(recent_obs),
            fit_eligible_observations=len(fit_scored),
            fit_rejected_observations=len(recent_obs) - len(fit_scored),
            fit_reject_reasons=dict(fit_reject_reasons),
            fit_contributing_icao_count=len(fit_contributing_icaos),
            fit_span_s=span_s,
            predictor_consistency=predictor_consistency,
            waveform_enabled=bool(RADAR_SYNC_WAVEFORM_ENABLED),
            waveform_applied=waveform_applied_new,
            waveform_bin_count=len(waveform_bins) if waveform_bins else 0,
            waveform_residual_reduction_deg=waveform_reduction_ema,
            waveform_learning_enabled=waveform_learning_enabled,
            waveform_update_block_reason=waveform_update_block_reason,
            waveform_learning_residual_basis="residual_after_waveform_detrended_deg",
            prop_delay_enabled=bool(RADAR_SYNC_PROP_DELAY_ENABLED),
            motion_comp_phase_enabled=bool(RADAR_SYNC_MOTION_COMP_PHASE_ENABLED),
            motion_comp_fit_enabled=bool(RADAR_SYNC_MOTION_COMP_FIT_ENABLED),
            motion_comp_applied_count=len(motion_applied_entries),
            motion_comp_blocked_count=motion_blocked_count,
            motion_comp_mean_dt_us=motion_mean_dt_us,
            motion_comp_mean_residual_improvement_deg=motion_mean_improvement,
            motion_comp_high_rate_mean_residual_improvement_deg=motion_high_rate_mean_improvement,
            phase_anchor_icao=phase_anchor_icao,
            phase_anchor_score=phase_anchor_score,
            phase_anchor_obs_count=phase_anchor_obs_count,
            phase_anchor_offset_raw_deg=phase_anchor_raw,
            phase_anchor_offset_smoothed_deg=phase_anchor_smoothed,
            phase_anchor_spread_deg=phase_anchor_spread,
            phase_anchor_status=phase_anchor_status,
            phase_anchor_since_ts=phase_anchor_since_ts,
            phase_anchor_replacement_reason=phase_anchor_replacement_reason,
            phase_anchor_candidate_count=len(anchor_selection.get("candidates") or []),
            phase_validation_contributors=validation["contributor_count"],
            phase_validation_reject_count=validation["reject_count"],
            phase_validation_median_error_deg=validation["median_error_deg"],
            phase_validation_status=validation["status"],
            phase_anchor_candidates=anchor_selection.get("candidates") or [],
            period_failure_score=period_failure["failure_score"],
            period_failure_streak=period_failure_streak,
            period_reacquire_active=reacquire_active,
            period_reacquire_reason=reacquire_reason,
            period_reacquire_started_ts=reacquire_started_ts,
            period_refine_mode=period_refine_mode,
            period_authoritative_source=period_authoritative_source,
            period_failure_primary_class=period_failure_primary_class,
            period_reacquire_trigger=period_reacquire_trigger,
            period_recovery_clean_update=clean_reacquire_update,
            period_recovery_clean_streak=clean_reacquire_streak,
        )
        self._live_sync_states[iid] = new_state

        update_entry = {
            "ts": now_ts,
            "source": new_state.source,
            "residual_slope_deg_per_s": b_fit,
            "period_s": refined_period_s,
            "period_base_s": base_period_s,
            "period_correction_ppm": period_correction_ppm,
            "period_update_term": period_update_term,
            "period_update_proposed_s": period_update_term,
            "period_update_proposed_us": period_update_term * 1_000_000.0,
            "period_update_applied": period_update_applied,
            "period_update_applied_s": period_update_applied,
            "period_update_applied_us": period_update_applied * 1_000_000.0,
            "period_update_direction": period_update_direction,
            "period_update_gain": _PERIOD_GAIN,
            "period_update_ppm_unclamped": period_update_ppm_unclamped,
            "period_update_ppm_applied": period_update_ppm_applied,
            "period_update_allowed": period_update_allowed,
            "period_update_block_reason": period_update_block_reason,
            "period_update_clamp_reason": period_update_clamp_reason,
            "period_correction_status": period_correction_status,
            "period_s_before": live_period_s,
            "period_s_after": refined_period_s,
            "period_update_safety_ppm_per_update": ppm_per_update_limit,
            "period_update_safety_ppm_from_base": ppm_from_base_limit,
            "period_refine_block_reason": period_refine_block_reason,
            "n_fit_observations": len(fit_scored),
            "n_total_observations": len(recent_obs),
            "n_fit_icaos": len(fit_contributing_icaos),
            "fit_span_s": span_s,
            "fit_reject_reasons": dict(fit_reject_reasons),
            "waveform_learning_enabled": waveform_learning_enabled,
            "waveform_update_block_reason": waveform_update_block_reason,
            "phase_anchor_icao": phase_anchor_icao,
            "phase_anchor_score": phase_anchor_score,
            "phase_anchor_status": phase_anchor_status,
            "phase_anchor_replacement_reason": phase_anchor_replacement_reason,
            "phase_validation_contributors": validation["contributor_count"],
            "phase_validation_reject_count": validation["reject_count"],
            "phase_validation_median_error_deg": validation["median_error_deg"],
            "period_failure_score": period_failure["failure_score"],
            "period_failure_streak": period_failure_streak,
            "period_refine_mode": period_refine_mode,
            "period_authoritative_source": period_authoritative_source,
            "period_reacquire_active": reacquire_active,
            "period_reacquire_reason": reacquire_reason,
            "period_reacquire_started_ts": reacquire_started_ts,
            "period_wrong_family_suspect": wrong_period_suspect,
            "period_failure_reason": period_failure["failure_reason"],
            "period_failure_primary_class": period_failure_primary_class,
            "period_failure_conditions": list(period_failure["primary_conditions"]),
            "period_failure_secondary_conditions": list(period_failure["secondary_conditions"]),
            "raw_median_abs_residual_deg": period_failure["raw_median_abs_residual_deg"],
            "detrended_median_abs_residual_deg": period_failure["detrended_median_abs_residual_deg"],
            "detrended_improvement_ratio": period_failure["detrended_improvement_ratio"],
            "dominant_error_mode": period_failure["dominant_error_mode"],
            "period_anchor_failure_reason": anchor_failure["anchor_failure_reason"],
            "period_reacquire_trigger": period_reacquire_trigger,
            "period_recovery_clean_update": clean_reacquire_update,
            "period_recovery_clean_streak": clean_reacquire_streak,
        }
        self._live_period_update_history.setdefault(iid, deque(maxlen=80)).append(update_entry)
        self._live_slope_history.setdefault(iid, deque(maxlen=80)).append({
            "ts": now_ts,
            # residual_slope_deg_per_s is the EMA-smoothed value — this is what the
            # persistence gate and period correction use.  raw_slope_deg_per_s is the
            # instantaneous fit result, kept for diagnostics only.
            "residual_slope_deg_per_s": smoothed_slope,
            "raw_slope_deg_per_s": b_fit,
            "fit_span_s": span_s,
            "n_fit_observations": len(fit_scored),
        })
        self._live_period_history.setdefault(iid, deque(maxlen=80)).append({
            "ts": now_ts,
            "period_s": refined_period_s,
            "period_base_s": base_period_s,
            "period_correction_ppm": period_correction_ppm,
            "period_correction_status": period_correction_status,
        })

    def _finalize_pending_burst(
        self,
        iid: int,
        icao: str,
    ) -> dict | None:
        pending_bursts = self._live_bursts.get(iid)
        if pending_bursts is None:
            return None

        replies = pending_bursts.pop(icao, [])
        if not replies:
            return None

        refinement = refine_burst_center(replies)
        timestamp_candidates = _compute_burst_timestamp_candidates(replies)
        burst_centroid_us = refinement["beam_center_us"]
        burst_signal = max((s for _, s in replies if s is not None), default=None)

        centroid_hist = self._live_burst_centroids[iid].setdefault(icao, [])
        centroid_hist.append(burst_centroid_us)
        if len(centroid_hist) > 30:
            centroid_hist.pop(0)

        fired = {
            "icao": icao,
            "burst_centroid_us": burst_centroid_us,
            "burst_signal": burst_signal,
            "n_replies": len(replies),
            "burst_center_method": refinement.get("beam_center_method", "centroid"),
            "burst_center_simple_us": refinement.get("beam_center_simple_us"),
            "burst_center_weighted_us": refinement.get("beam_center_weighted_us"),
            "burst_center_delta_us": refinement.get("beam_center_delta_us"),
            **timestamp_candidates,
        }
        if self._diagnostics_enabled:
            fired["replies"] = [
                {"arrival_us": arrival_us, "signal_dbfs": signal_dbfs}
                for arrival_us, signal_dbfs in replies
            ]
        return fired

    def _finalize_expired_pending_bursts(
        self,
        iid: int,
        current_arrival_us: float,
    ) -> list[dict]:
        pending_bursts = self._live_bursts.get(iid)
        last_arrival = self._live_last_arrival.get(iid)
        if pending_bursts is None or last_arrival is None:
            return []

        expired_icaos = [
            icao
            for icao, last_us in list(last_arrival.items())
            if current_arrival_us - last_us > BURST_GAP_US
        ]
        fired_bursts: list[dict] = []
        for expired_icao in expired_icaos:
            fired = self._finalize_pending_burst(iid, expired_icao)
            last_arrival.pop(expired_icao, None)
            if fired is not None:
                fired_bursts.append(fired)

        fired_bursts.sort(key=lambda burst: burst["burst_centroid_us"])
        return fired_bursts

    def _record_dwell_profile(
        self,
        iid: int,
        icao: str,
        beam_center_us: float,
        replies: list[dict] | None,
    ) -> None:
        """Retain completed per-reply dwell data only when diagnostics are enabled."""
        if not self._diagnostics_enabled or not replies:
            return
        self._dwell_profiles.setdefault(
            iid,
            deque(maxlen=self._DWELL_PROFILE_MAX),
        ).append({
            "icao": icao,
            "beam_center_us": beam_center_us,
            "replies": list(replies),
        })

    def _burst_matches_dominant_period_family(
        self,
        iid: int,
        icao: str,
        period_s: float,
    ) -> bool:
        model = self._models.get(iid)
        if model is not None and model.rotation_model is not None:
            rotation_model = model.rotation_model
            if icao in rotation_model.folded:
                return True
            if icao in rotation_model.residual or icao in rotation_model.secondary_folded:
                return False

        centroids = self._live_burst_centroids.get(iid, {}).get(icao, [])
        if len(centroids) < MIN_BURSTS:
            return False

        sorted_centroids = sorted(centroids)
        intervals_s = [
            (sorted_centroids[i + 1] - sorted_centroids[i]) / 1_000_000.0
            for i in range(len(sorted_centroids) - 1)
        ]
        valid = [iv for iv in intervals_s if 0.5 < iv < 30.0]
        if len(valid) < 2:
            return False

        median_period = statistics.median(valid)
        return abs(median_period - period_s) / period_s <= PERIOD_MATCH_TOLERANCE

    def _should_suppress_frame_start(self, iid: int, start_us: float, period_s: float) -> bool:
        last_start = self._live_last_frame_start_us.get(iid)
        if last_start is None:
            return False
        min_spacing_us = period_s * 1_000_000.0 * _MIN_FRAME_START_SEPARATION_FRACTION
        return (start_us - last_start) < min_spacing_us

    def _burst_matches_reference_phase_family(
        self,
        iid: int,
        ref_icao: str,
        ref_arrival_us: float,
        icao: str,
        burst_centroid_us: float,
        period_s: float,
    ) -> bool:
        if icao == ref_icao or period_s <= 0:
            return True

        ref_centroids = self._live_burst_centroids.get(iid, {}).get(ref_icao, [])
        icao_centroids = self._live_burst_centroids.get(iid, {}).get(icao, [])
        if not ref_centroids or not icao_centroids:
            return True

        period_us = period_s * 1_000_000.0
        tolerance_us = max(CO_SWEEP_WINDOW_US, period_us * _PHASE_FAMILY_TOLERANCE_FRACTION)

        historical_offsets_us: list[float] = []
        prior_refs = [centroid for centroid in ref_centroids if centroid < ref_arrival_us]
        prior_obs = [centroid for centroid in icao_centroids if centroid < burst_centroid_us]
        if not prior_refs or not prior_obs:
            return True

        for obs_centroid in prior_obs:
            nearest_ref = min(prior_refs, key=lambda ref_centroid: abs(obs_centroid - ref_centroid))
            offset_us = (obs_centroid - nearest_ref) % period_us
            historical_offsets_us.append(offset_us)

        if len(historical_offsets_us) < _PHASE_FAMILY_HISTORY_MIN:
            return True

        historical_offsets_us.sort()
        expected_offset_us = statistics.median(historical_offsets_us)
        observed_offset_us = (burst_centroid_us - ref_arrival_us) % period_us
        circular_delta_us = min(
            abs(observed_offset_us - expected_offset_us),
            period_us - abs(observed_offset_us - expected_offset_us),
        )
        return circular_delta_us <= tolerance_us

    @staticmethod
    def _eligible_for_pair_generation(model: RadarIID, icao: str) -> bool:
        rotation_model = getattr(model, "rotation_model", None)
        if rotation_model is None:
            return True
        if icao in getattr(rotation_model, "residual", {}):
            return False
        if icao in getattr(rotation_model, "secondary_folded", {}):
            return False
        folded = getattr(rotation_model, "folded", {})
        if folded:
            return icao in folded
        return True

    def _on_df11_frame_builder(self, iid: int, icao: str, arrival_us: float, signal_dbfs: float | None) -> None:
        """Process a single DF11 reply for live frame building.

        Incremental burst detection per aircraft:
          - Replies within BURST_GAP_US are accumulated into a pending burst.
          - A gap > BURST_GAP_US finalises the previous burst → centroid computed.

        When a reference aircraft burst fires → close current frame, start new one.
        When another aircraft burst fires → add to current frame with phase.

        Reference aircraft is selected automatically once enough live burst data
        has accumulated (≥4 bursts per aircraft, ≥2 aircraft with data).
        """
        model = self._models.get(iid)
        # Compute authoritative period; may be None during bootstrap.  Burst
        # accumulation and BurstRecord emission proceed regardless — only frame
        # timing logic is gated on having a valid period.
        period_s: float | None = None
        if model is not None and model.period_s is not None:
            period_s = self._get_authoritative_frame_period_s(iid, model.period_s)
        period_us = period_s * 1_000_000.0 if period_s is not None else None

        # Ensure per-IID state exists
        if iid not in self._live_bursts:
            self._live_bursts[iid] = {}
            self._live_burst_diagnostic_replies[iid] = {}
            self._live_last_arrival[iid] = {}
            self._live_burst_centroids[iid] = {}
            self._live_frames[iid] = None
            self._live_completed_frames[iid] = deque(maxlen=self._LIVE_FRAMES_MAX)
            self._live_frame_counters[iid] = 0
            self._burst_records.setdefault(iid, deque())
        if iid not in self._live_burst_diagnostic_replies:
            self._live_burst_diagnostic_replies[iid] = {}
        if iid not in self._live_aligned_burst_obs:
            self._live_aligned_burst_obs[iid] = deque(maxlen=self._MULTI_SYNC_OBS_MAX)

        pending_bursts = self._live_bursts[iid]
        last_arrival = self._live_last_arrival[iid]

        fired_bursts = self._finalize_expired_pending_bursts(iid, arrival_us)

        # Add this reply to the pending burst
        pending_bursts.setdefault(icao, []).append((arrival_us, signal_dbfs))
        last_arrival[icao] = arrival_us

        if not fired_bursts:
            return  # Still accumulating

        # --- One or more bursts have fired ---
        for fired_burst in fired_bursts:
            fired_icao = fired_burst["icao"]
            burst_centroid_us = fired_burst["burst_centroid_us"]
            burst_signal = fired_burst["burst_signal"]
            n_replies = fired_burst.get("n_replies", 1)

            # Emit compact BurstRecord for rotation analysis and sync fitting.
            # Core fields only; enrichment (position, family flags) can be layered
            # on separately if needed by other consumers.
            self._append_burst_record(
                iid,
                BurstRecord(
                    iid=iid,
                    icao=fired_icao,
                    centroid_us=burst_centroid_us,
                    n_replies=n_replies,
                    signal_dbfs=burst_signal,
                ),
            )
            self._record_dwell_profile(
                iid=iid,
                icao=fired_icao,
                beam_center_us=burst_centroid_us,
                replies=fired_burst.get("replies"),
            )

            # All remaining logic requires a usable period; skip during bootstrap
            # so that burst records and centroid history still accumulate.
            if period_us is None:
                continue

            current_frame = self._live_frames.get(iid)
            if current_frame is not None and burst_centroid_us >= current_frame.ref_arrival_us + period_us:
                self._finalize_live_frame(iid, period_s)
                current_frame = None

            # Freeze the reference for an open frame. Only choose/re-evaluate the
            # reference when no frame is currently in progress.
            if current_frame is not None:
                ref_icao = current_frame.ref_icao
            else:
                override = model.reference_aircraft_override
                prev_ref_icao = model.reference_aircraft.ref_icao if model.reference_aircraft else None
                if override is not None:
                    ref_icao = override
                    if prev_ref_icao != override:
                        from .models import ReferenceAircraftInfo
                        model.reference_aircraft = ReferenceAircraftInfo(
                            ref_icao=override,
                            ref_score=None,
                            ref_since_sweep=0,
                            hysteresis_margin=0.0,
                        )
                else:
                    ref_icao = self._select_reference_from_live_bursts(
                        iid, period_s, now_us=burst_centroid_us
                    )
                    if ref_icao is None:
                        continue  # Not enough data yet

            # Look up position from the ADS-B tracker
            lat = lon = None
            interpolated = False
            position_age_seconds = 0.0
            position_extrapolated = False
            position_source_age_s = None
            pos: dict | None = None
            try:
                wall_ts = self._estimate_wall_time_from_arrival_us(burst_centroid_us, arrival_us)
                if wall_ts is not None:
                    pos = self._adsb_tracker.get_position_at(fired_icao, wall_ts)
                    if pos:
                        lat = pos.get("lat")
                        lon = pos.get("lon")
                        interpolated = pos.get("interpolated", False)
                        position_age_seconds = pos.get("position_age_seconds", 0.0)
                        position_extrapolated = bool(pos.get("extrapolated", False))
                        position_source_age_s = pos.get("source_age_seconds")
            except Exception:
                pass

            from .models import SweepFrameObservation, LiveFrameState

            # Record one burst-centre Stage3LiveDetection per fired burst (Python fallback path).
            # Done before frame qualification checks so all radar-illuminated ICAOs are captured.
            self._record_live_burst_detection(iid, fired_icao, burst_centroid_us, burst_signal, pos)

            # Record burst-centre sync observations for both visualisation coverage
            # and (when dominant) sync maintenance updates.
            matches_dominant_for_sync = self._burst_matches_dominant_period_family(iid, fired_icao, period_s)
            if lat is not None and lon is not None:
                _radar_pos_fb = _get_authoritative_radar_position(model)
                if _radar_pos_fb["lat"] is not None and _radar_pos_fb["lon"] is not None:
                    self._record_burst_sync_timeline_obs(
                        iid=iid,
                        icao=fired_icao,
                        burst_centroid_us=burst_centroid_us,
                        radar_lat=_radar_pos_fb["lat"],
                        radar_lon=_radar_pos_fb["lon"],
                        aircraft_lat=lat,
                        aircraft_lon=lon,
                        n_replies=fired_burst.get("n_replies", 1),
                        signal_dbfs=burst_signal,
                        pos_age_s=position_age_seconds,
                        sync_update_eligible=matches_dominant_for_sync,
                        burst_center_method=fired_burst.get("burst_center_method", "centroid"),
                        burst_center_simple_us=fired_burst.get("burst_center_simple_us"),
                        burst_center_weighted_us=fired_burst.get("burst_center_weighted_us"),
                        burst_center_delta_us=fired_burst.get("burst_center_delta_us"),
                        burst_ts_first_reply_beast_us=fired_burst.get("burst_ts_first_reply_beast_us"),
                        burst_ts_strongest_reply_beast_us=fired_burst.get("burst_ts_strongest_reply_beast_us"),
                        burst_ts_simple_centroid_beast_us=fired_burst.get("burst_ts_simple_centroid_beast_us"),
                        burst_ts_weighted_centroid_beast_us=fired_burst.get("burst_ts_weighted_centroid_beast_us"),
                        burst_ts_mid_strong_window_beast_us=fired_burst.get("burst_ts_mid_strong_window_beast_us"),
                        burst_ts_last_reply_beast_us=fired_burst.get("burst_ts_last_reply_beast_us"),
                        burst_span_us=fired_burst.get("burst_span_us"),
                        peak_amplitude=fired_burst.get("peak_amplitude"),
                        position_interpolated=interpolated,
                        position_extrapolated=position_extrapolated,
                        position_source_age_s=position_source_age_s,
                        truth_position_ts_beast_us=(
                            burst_centroid_us - position_source_age_s * 1_000_000.0
                            if position_source_age_s is not None else None
                        ),
                    )
                    if matches_dominant_for_sync:
                        self._record_aligned_burst_sync_obs(
                            iid=iid,
                            icao=fired_icao,
                            burst_centroid_us=burst_centroid_us,
                            radar_lat=_radar_pos_fb["lat"],
                            radar_lon=_radar_pos_fb["lon"],
                            aircraft_lat=lat,
                            aircraft_lon=lon,
                            n_replies=fired_burst.get("n_replies", 1),
                            signal_dbfs=burst_signal,
                            pos_age_s=position_age_seconds,
                            period_s=period_s,
                        )

            if fired_icao == ref_icao:
                # Reference burst inside an already-open frame is treated as a duplicate
                # hit/sidelobe unless the frame has already expired, which was handled above.
                if current_frame is not None:
                    continue

                if not matches_dominant_for_sync:
                    continue

                if self._should_suppress_frame_start(iid, burst_centroid_us, period_s):
                    continue

                if lat is None or lon is None:
                    self._live_frames[iid] = None
                    continue

                self._live_frames[iid] = LiveFrameState(
                    ref_icao=ref_icao,
                    ref_lat=lat,
                    ref_lon=lon,
                    ref_arrival_us=burst_centroid_us,
                    seen_icaos={ref_icao},
                    ref_pos_age_s=position_age_seconds,
                )
                self._live_last_frame_start_us[iid] = burst_centroid_us
            else:
                current_frame = self._live_frames.get(iid)
                if current_frame is None:
                    continue
                if burst_centroid_us < current_frame.ref_arrival_us:
                    continue
                if burst_centroid_us >= current_frame.ref_arrival_us + period_us:
                    continue
                if lat is None or lon is None:
                    continue
                if not matches_dominant_for_sync:
                    continue
                if not self._burst_matches_reference_phase_family(
                    iid,
                    current_frame.ref_icao,
                    current_frame.ref_arrival_us,
                    fired_icao,
                    burst_centroid_us,
                    period_s,
                ):
                    continue

                if fired_icao in current_frame.seen_icaos:
                    continue

                current_frame.observations.append(SweepFrameObservation(
                    icao=fired_icao,
                    lat=lat,
                    lon=lon,
                    arrival_us=burst_centroid_us,
                    signal_dbfs=burst_signal,
                    interpolated=interpolated,
                    n_replies=n_replies,
                    position_age_seconds=position_age_seconds,
                ))
                current_frame.seen_icaos.add(fired_icao)
                current_frame.n_aircraft_seen += 1

    def _select_reference_from_live_bursts(self, iid: int, period_s: float, now_us: float = 0.0) -> str | None:
        """Select (or re-evaluate) reference aircraft from completed burst centroid history.

        Picks the aircraft whose inter-burst timing best matches the aggregate rotation
        period. Called on every burst so the reference updates when a better candidate
        appears or the current one leaves coverage.

        Recency filter: only aircraft whose most-recent centroid is within 5 periods
        are eligible — this ensures a departed aircraft is dropped promptly.

        Hysteresis: a challenger must score ≥25% better than the current reference
        to trigger a switch, preventing rapid churn when two candidates are close.

        Returns the selected ICAO or None if not enough data yet.
        """
        import statistics
        from .models import ReferenceAircraftInfo

        MIN_BURSTS_FOR_REF = 4
        RECENCY_PERIODS = 5  # candidate must have burst within this many periods
        HYSTERESIS = 0.25    # challenger must be this fraction better to displace current ref

        burst_centroids = self._live_burst_centroids.get(iid, {})
        recency_threshold_us = period_s * RECENCY_PERIODS * 1_000_000.0

        candidates = {}
        for icao, centroids in burst_centroids.items():
            if len(centroids) < MIN_BURSTS_FOR_REF:
                continue
            # Recency check: last centroid must be recent enough
            last_centroid = max(centroids)
            if now_us > 0 and (now_us - last_centroid) > recency_threshold_us:
                continue

            sorted_centroids = sorted(centroids)
            intervals_s = [
                (sorted_centroids[i + 1] - sorted_centroids[i]) / 1_000_000
                for i in range(len(sorted_centroids) - 1)
            ]
            valid = [iv for iv in intervals_s if 0.5 < iv < 30.0]
            if len(valid) < 2:
                continue

            median_period = statistics.median(valid)
            std_dev = statistics.stdev(valid) if len(valid) > 1 else 0.0

            period_dev = abs(median_period - period_s) / period_s
            period_tightness = std_dev / period_s
            count_factor = 1.0 / max(len(centroids), 1)

            candidates[icao] = period_dev + period_tightness + count_factor

        if not candidates:
            return None

        best_icao = min(candidates, key=candidates.get)
        best_score = candidates[best_icao]

        # Hysteresis: only displace the current reference if the challenger is
        # significantly better (avoids churn when two candidates are close).
        model = self._models[iid]
        current = model.reference_aircraft
        if current is not None and current.ref_icao is not None:
            current_icao = current.ref_icao
            current_score = candidates.get(current_icao)
            if current_score is not None:
                # Current ref is still active — require challenger to beat it by HYSTERESIS
                if best_score > current_score * (1.0 - HYSTERESIS):
                    best_icao = current_icao
                    best_score = current_score

        # Build challenger list for diagnostics
        challengers = []
        for icao, score in sorted(candidates.items(), key=lambda x: x[1])[:5]:
            ratio = score / best_score if best_score > 0 else float("inf")
            challengers.append({
                "icao": icao, "score": round(score, 6), "ratio_to_best": round(ratio, 3),
                "status": "current" if icao == best_icao else "better_switch" if ratio < 0.75 else "close_skip",
            })

        info = ReferenceAircraftInfo(
            ref_icao=best_icao,
            ref_score=best_score,
            ref_since_sweep=0,
            hysteresis_margin=HYSTERESIS,
            challengers=challengers,
        )
        model.reference_aircraft = info
        return best_icao

    # ------------------------------------------------------------------
    # Rotation models
    # ------------------------------------------------------------------

    def _get_authoritative_frame_period_s(
        self, iid: int, fallback_period_s: float | None
    ) -> float | None:
        """Return the refined sync period for frame building when available.

        Uses LiveSyncState.period_s when the sync is usable, not in holdover,
        and the period is positive.  Falls back to the coarse rotation-model
        period (fallback_period_s) during bootstrap or degraded sync conditions.
        """
        sync = self._live_sync_states.get(iid)
        if (
            sync is not None
            and sync.usable
            and not sync.holdover
        ):
            if sync.period_reacquire_active or sync.period_authoritative_source == "base":
                if sync.period_base_s and sync.period_base_s > 0:
                    return sync.period_base_s
            if sync.period_s and sync.period_s > 0:
                return sync.period_s
        return fallback_period_s

    def get_authoritative_display_period_s(self, iid: int) -> float | None:
        """Return the period currently in use for display in the IID selector.

        Mirrors _get_authoritative_frame_period_s: prefers LiveSyncState.period_s
        when sync is usable and not in holdover, falls back to the coarse model
        period.  Used by the /api/radar/iids endpoint so the UI shows the same
        period the frame builder is actually using rather than the stale coarse value.
        """
        model = self._models.get(iid)
        fallback = model.period_s if model is not None else None
        return self._get_authoritative_frame_period_s(iid, fallback)

    def get_authoritative_display_period_std_s(self, iid: int) -> float | None:
        """Return the period sigma for display in the IID selector.

        When refined sync is authoritative (usable, not in holdover), converts
        sync_jitter_deg to period-domain sigma: jitter_deg / 360 * period_s.
        Falls back to the coarse rotation-model period_std_s otherwise.
        """
        sync = self._live_sync_states.get(iid)
        if (
            sync is not None
            and sync.usable
            and not sync.holdover
        ):
            effective_period_s = sync.period_s
            if sync.period_reacquire_active or sync.period_authoritative_source == "base":
                effective_period_s = sync.period_base_s
            if effective_period_s and effective_period_s > 0:
                return sync.sync_jitter_deg / 360.0 * effective_period_s if sync.sync_jitter_deg else None
        model = self._models.get(iid)
        return model.period_std_s if model is not None else None

    def get_authoritative_display_rpm(self, iid: int) -> float | None:
        """Return the RPM for display in the IID selector.

        Derived from the authoritative period so it always matches period_s.
        """
        period_s = self.get_authoritative_display_period_s(iid)
        if period_s and period_s > 0:
            return round(60.0 / period_s, 3)
        model = self._models.get(iid)
        return model.rpm if model is not None else None

    def update_rotation_models(
        self,
        max_iids_per_call: int | None = None,
        max_runtime_ms: float | None = None,
    ) -> None:
        """Analyse accumulated IID events and update per-IID rotation models.

        Called periodically (every 30s) from a background task.
        Also prunes events older than IID_EVENT_MAX_AGE_S.
        """
        t0 = time.perf_counter()
        snapshot_s = 0.0
        tracker_refresh_s = 0.0
        history_fetch_s = 0.0
        cache_build_s = 0.0
        sweep_build_s = 0.0
        analyse_swap_s = 0.0
        event_count = 0
        deferred_count = 0
        budget_s = (
            max_runtime_ms / 1000.0
            if max_runtime_ms is not None and max_runtime_ms > 0
            else None
        )
        self._update_active.set()
        try:
            now_us: int
            now_ts = time.time()
            with self._lock:
                t_snapshot = time.perf_counter()
                # Estimate "now" in µs: prefer latest burst centroid, fall back to
                # _iid_latest_arrival_us (avoids scanning the full _iid_events deque).
                _latest_burst_us = max(
                    (br_deque[-1].centroid_us for br_deque in self._burst_records.values() if br_deque),
                    default=None,
                )
                _latest_event_us = (
                    max(self._iid_latest_arrival_us.values())
                    if self._iid_latest_arrival_us else None
                )
                if _latest_burst_us is None and _latest_event_us is None:
                    return
                now_us = int(max(filter(None, [_latest_burst_us, _latest_event_us])))
                cutoff_us = now_us - int(IID_EVENT_MAX_AGE_S * 1_000_000)

                # Prune old events from the left
                while self._iid_events and self._iid_events[0][0] < cutoff_us:
                    self._iid_events.popleft()

                # Prune burst records beyond BURST_RECORD_MAX_AGE_S
                burst_cutoff_us = now_us - int(BURST_RECORD_MAX_AGE_S * 1_000_000)
                for br_deque in self._burst_records.values():
                    while br_deque and br_deque[0].centroid_us < burst_cutoff_us:
                        br_deque.popleft()
                for dwell_deque in self._dwell_profiles.values():
                    while dwell_deque and dwell_deque[0].get("beam_center_us", 0.0) < burst_cutoff_us:
                        dwell_deque.popleft()

                dirty_iids = set(self._dirty_iids)
                self._dirty_iids.clear()
                snapshot_s = time.perf_counter() - t_snapshot

            if not dirty_iids:
                record_rotation_update_timing({
                    "ts_s": time.time(),
                    "total_ms": round((time.perf_counter() - t0) * 1000, 2),
                    "snapshot_ms": round(snapshot_s * 1000, 2),
                    "tracker_refresh_ms": round(tracker_refresh_s * 1000, 2),
                    "history_fetch_ms": 0.0,
                    "cache_build_ms": 0.0,
                    "sweep_build_ms": 0.0,
                    "analyse_swap_ms": 0.0,
                    "iid_count": 0,
                    "event_count": event_count,
                    "deferred_iid_count": 0,
                })
                return

            with self._lock:
                deferred_iids = set()
                due_iids = set()
                for iid in dirty_iids:
                    model = self._models.get(iid)
                    if (
                        model is not None
                        and model.status in ("SINGLE_RADAR", "LIKELY_SINGLE")
                        and model.last_updated
                        and (now_ts - model.last_updated) < STABLE_REANALYZE_INTERVAL_S
                    ):
                        deferred_iids.add(iid)
                    else:
                        due_iids.add(iid)
                if deferred_iids:
                    self._dirty_iids.update(deferred_iids)
                if not due_iids:
                    record_rotation_update_timing({
                        "ts_s": time.time(),
                        "total_ms": round((time.perf_counter() - t0) * 1000, 2),
                        "snapshot_ms": round(snapshot_s * 1000, 2),
                        "tracker_refresh_ms": round(tracker_refresh_s * 1000, 2),
                        "history_fetch_ms": 0.0,
                        "cache_build_ms": 0.0,
                        "sweep_build_ms": 0.0,
                        "analyse_swap_ms": 0.0,
                        "iid_count": 0,
                        "event_count": 0,
                        "deferred_iid_count": len(deferred_iids),
                    })
                    return
                analysis_cutoff_us = now_us - int(ROTATION_ANALYSIS_MAX_AGE_S * 1_000_000)
                # Lightweight observability count — no need to filter the full deque.
                event_count = len(self._iid_events)
                # Build burst record snapshot for rotation analysis
                by_iid_bursts: dict[int, list] = {}
                for iid in due_iids:
                    br_deque = self._burst_records.get(iid)
                    if br_deque:
                        relevant = [r for r in br_deque if r.centroid_us >= analysis_cutoff_us]
                        if relevant:
                            by_iid_bursts[iid] = relevant

                # Bootstrap fallback: IIDs not yet producing burst records (e.g.
                # after purge/reset) still have raw events in _iid_events.  Scan
                # the deque once to extract them, breaking the circular dependency:
                #   burst records → frame builder → model → burst records
                bootstrap_iids = due_iids - by_iid_bursts.keys()
                by_iid_events: dict[int, list] = {}
                if bootstrap_iids and self._iid_events:
                    for ev in self._iid_events:
                        ev_iid = ev[1]
                        if ev_iid in bootstrap_iids and ev[0] >= analysis_cutoff_us:
                            by_iid_events.setdefault(ev_iid, []).append(ev)

            if not by_iid_bursts and not by_iid_events:
                return

            if max_iids_per_call is not None and max_iids_per_call > 0 and len(by_iid_bursts) > max_iids_per_call:
                ranked_iids = sorted(by_iid_bursts, key=lambda iid: len(by_iid_bursts[iid]), reverse=True)
                deferred_iids = set(ranked_iids[max_iids_per_call:])
                by_iid_bursts = {iid: by_iid_bursts[iid] for iid in ranked_iids[:max_iids_per_call]}
                with self._lock:
                    self._dirty_iids.update(deferred_iids)

            if not by_iid_bursts and not by_iid_events:
                record_rotation_update_timing({
                    "ts_s": time.time(),
                    "total_ms": round((time.perf_counter() - t0) * 1000, 2),
                    "snapshot_ms": round(snapshot_s * 1000, 2),
                    "tracker_refresh_ms": round(tracker_refresh_s * 1000, 2),
                    "history_fetch_ms": 0.0,
                    "cache_build_ms": 0.0,
                    "sweep_build_ms": 0.0,
                    "analyse_swap_ms": 0.0,
                    "iid_count": 0,
                    "event_count": event_count,
                    "deferred_iid_count": 0,
                })
                return

            def _iid_priority(iid: int) -> tuple[float, int]:
                model = self._models.get(iid)
                return ((model.last_updated if model is not None else 0.0) or 0.0, iid)

            ordered_iids = sorted(by_iid_bursts.keys() | by_iid_events.keys(), key=_iid_priority)

            # Refresh the live-frame-builder tracker with current aircraft positions.
            # The live path (_on_df11_frame_builder) uses this for real-time burst
            # lookups; get_positions_snapshot() is cheap (one brief lock, 5 scalar
            # fields per aircraft).  At most 30s stale, but velocity projection in
            # get_position_at() handles the delta.
            if self._aircraft_state is not None:
                try:
                    t_tracker = time.perf_counter()
                    for pos in self._aircraft_state.get_positions_snapshot():
                        self._adsb_tracker.update(
                            pos["icao"],
                            pos["lat"],
                            pos["lon"],
                            pos.get("gs"),
                            pos.get("track"),
                            ts=pos.get("last_pos_ts") or time.time(),
                        )
                    tracker_refresh_s = time.perf_counter() - t_tracker
                except Exception:
                    pass

            # Periodically prune per-ICAO state to prevent accumulation with ICAO churn.
            self._prune_live_icao_state()

            analysed_models: dict[int, RotationModel] = {}
            t_sweeps = time.perf_counter()
            history_fetch_s = 0.0
            cache_build_s = 0.0
            unprocessed_iids: set[int] = set()
            min_delta = self._ROTATION_ANALYSIS_MIN_DELTA
            for index, iid in enumerate(ordered_iids):
                analysis_cap = self._rotation_analysis_dynamic_cap(iid, now_us=float(now_us))
                if iid in by_iid_bursts:
                    burst_list = by_iid_bursts[iid]
                    prev_count, _prev_ts = self._rotation_analysis_meta.get(iid, (0, 0.0))
                    existing_model = self._models.get(iid)
                    # Skip reanalysis when the IID has an established model and the
                    # burst delta since the last run is below threshold.
                    if (
                        existing_model is not None
                        and existing_model.rotation_model is not None
                        and prev_count > 0
                        and (len(burst_list) - prev_count) < min_delta
                    ):
                        continue
                    if analysis_cap > 0 and len(burst_list) > analysis_cap:
                        dropped = len(burst_list) - analysis_cap
                        self._rotation_analysis_cap_hits_by_iid[iid] = (
                            self._rotation_analysis_cap_hits_by_iid.get(iid, 0) + dropped
                        )
                        burst_list_for_analysis = burst_list[-analysis_cap:]
                    else:
                        burst_list_for_analysis = burst_list
                    analysed_models[iid] = _analyse_burst_records(burst_list_for_analysis)
                    self._rotation_analysis_meta[iid] = (len(burst_list), time.time())
                else:
                    # Bootstrap path: no burst records yet (e.g. after purge/reset).
                    # Analyse raw _iid_events so the first model can be seeded and
                    # the frame builder can start producing burst records.
                    iid_events = by_iid_events[iid]
                    if analysis_cap > 0 and len(iid_events) > analysis_cap:
                        dropped = len(iid_events) - analysis_cap
                        self._rotation_analysis_cap_hits_by_iid[iid] = (
                            self._rotation_analysis_cap_hits_by_iid.get(iid, 0) + dropped
                        )
                        iid_events = iid_events[-analysis_cap:]
                    analysed_models[iid] = _analyse_iid_events(iid_events)
                    # Do not update _rotation_analysis_meta: event count is volatile
                    # and the delta-skip guard must not suppress bootstrap cycles.
                if budget_s is not None and (time.perf_counter() - t_sweeps) >= budget_s:
                    unprocessed_iids.update(ordered_iids[index + 1:])
                    break
            sweep_build_s = time.perf_counter() - t_sweeps
            deferred_count = len(unprocessed_iids)
            if unprocessed_iids:
                with self._lock:
                    self._dirty_iids.update(unprocessed_iids)

            # Fast pointer/state swap inside the lock only.
            t_analyse = time.perf_counter()
            with self._lock:
                for iid, model in analysed_models.items():
                    if iid not in self._models:
                        self._models[iid] = RadarIID(iid=iid)

                    radar_iid = self._models[iid]
                    radar_iid.rotation_model = model
                    _reinforce_radar_characteristics(radar_iid, model)
                    radar_iid.last_updated = model.last_updated or time.time()
            analyse_swap_s = time.perf_counter() - t_analyse
            record_rotation_update_timing({
                "ts_s": time.time(),
                "total_ms": round((time.perf_counter() - t0) * 1000, 2),
                "snapshot_ms": round(snapshot_s * 1000, 2),
                "tracker_refresh_ms": round(tracker_refresh_s * 1000, 2),
                "history_fetch_ms": round(history_fetch_s * 1000, 2),
                "cache_build_ms": round(cache_build_s * 1000, 2),
                "sweep_build_ms": round(sweep_build_s * 1000, 2),
                "analyse_swap_ms": round(analyse_swap_s * 1000, 2),
                "iid_count": len(analysed_models),
                "event_count": event_count,
                "deferred_iid_count": deferred_count,
            })

            log.debug("RadarState: updated %d IID rotation models", len(analysed_models))
        finally:
            self._update_active.clear()

    def is_update_active(self) -> bool:
        return self._update_active.is_set()

    def _build_sweeps_from_burst_records(
        self, burst_records: list[BurstRecord],
    ) -> list[dict]:
        """Build sweep summary dicts from BurstRecord objects.

        Equivalent to _build_sweep_data but consumes already-computed burst
        centroids rather than raw events. Skips position lookup — callers that
        need positions must resolve them separately.
        Returns sweep dicts compatible with update_calibration_pairs / get_sweep_history.
        """
        if not burst_records:
            return []
        sorted_records = sorted(burst_records, key=lambda r: r.centroid_us)

        # Group into sweeps by CO_SWEEP_WINDOW_US gap between burst centroids
        sweep_groups: list[list[BurstRecord]] = []
        current_group: list[BurstRecord] = [sorted_records[0]]
        sweep_anchor = sorted_records[0].centroid_us

        for rec in sorted_records[1:]:
            if rec.centroid_us - sweep_anchor < CO_SWEEP_WINDOW_US:
                current_group.append(rec)
            else:
                sweep_groups.append(current_group)
                current_group = [rec]
                sweep_anchor = rec.centroid_us
        sweep_groups.append(current_group)

        result = []
        for group in sweep_groups:
            centroid = int(sum(r.centroid_us for r in group) / len(group))
            aircraft_in_sweep = [
                {
                    "icao": r.icao,
                    "arrivals_us": [r.centroid_us],
                    "beam_center_us": r.centroid_us,
                    "signal_dbfs": r.signal_dbfs,
                }
                for r in group
            ]
            result.append({
                "centroid_us": centroid,
                "n_aircraft": len(group),
                "aircraft": aircraft_in_sweep,
            })
        return result

    def _build_sweep_data(
        self, events: list, position_cache: dict[tuple[str, int], dict | None],
    ) -> list[dict]:
        """Build sweep history data outside the lock. Pure computation, no locking.

        Takes raw events and pre-captured positions, returns a list of sweep dicts.
        """
        # Group by ICAO for this IID
        icao_samples: dict[str, list[tuple[int, float | None]]] = defaultdict(list)
        for arrival_us, _iid, icao, signal_dbfs in events:
            if icao:
                icao_samples[icao].append((arrival_us, signal_dbfs))

        # Collect all bursts with their ICAO tag
        all_bursts: list[tuple[int, str, dict]] = []
        for icao, samples in icao_samples.items():
            for burst in detect_bursts_with_signals(samples):
                all_bursts.append((burst.get("beam_center_us", burst["centroid_us"]), icao, burst))

        if not all_bursts:
            return []

        # Group into sweeps by CO_SWEEP_WINDOW_US around centroid clusters
        all_bursts.sort(key=lambda x: x[0])
        sweeps: list[list[tuple[int, str, dict]]] = []
        current_sweep: list[tuple[int, str, dict]] = [all_bursts[0]]
        sweep_centroid = all_bursts[0][0]

        for burst in all_bursts[1:]:
            if burst[0] - sweep_centroid < CO_SWEEP_WINDOW_US:
                current_sweep.append(burst)
            else:
                sweeps.append(current_sweep)
                current_sweep = [burst]
                sweep_centroid = burst[0]
        sweeps.append(current_sweep)

        result = []
        for sweep_bursts in sweeps:
            centroid = sum(b[0] for b in sweep_bursts) // len(sweep_bursts)
            aircraft_in_sweep = []
            for b in sweep_bursts:
                icao = b[1]
                beam_center_us = b[2].get("beam_center_us", b[2]["centroid_us"])

                pos = position_cache.get((icao, beam_center_us))

                entry = {
                    "icao": icao,
                    "arrivals_us": b[2]["arrivals_us"],
                    "replies": b[2]["replies"],
                    "signal_dbfs": b[2].get("signal_dbfs"),
                    "centroid_us": b[2]["centroid_us"],
                    "beam_center_us": beam_center_us,
                    "beam_center_method": b[2].get("beam_center_method", "centroid"),
                }

                if pos is not None and pos.get("lat") is not None and pos.get("lon") is not None:
                    entry["lat"] = pos["lat"]
                    entry["lon"] = pos["lon"]
                    entry["interpolated"] = pos.get("interpolated", False)
                    entry["position_source"] = pos.get("source", "unknown")
                    entry["groundspeed_kts"] = pos.get("groundspeed_kts")
                    entry["track_deg"] = pos.get("track_deg")

                aircraft_in_sweep.append(entry)

            result.append({
                "centroid_us": centroid,
                "n_aircraft": len(sweep_bursts),
                "aircraft": aircraft_in_sweep,
            })

        return result

    def get_rotation_model(self, iid: int) -> RadarIID | None:
        with self._lock:
            return self._models.get(iid)

    def reset_iid(self, iid: int) -> bool:
        """Clear learned in-memory state for one IID so it can be relearned."""
        with self._lock:
            had_any = False
            if iid in self._dirty_iids:
                self._dirty_iids.discard(iid)
                had_any = True
            if iid in self._models:
                del self._models[iid]
                had_any = True
            if iid in self._sweep_history:
                del self._sweep_history[iid]
                had_any = True
            for live_dict in (self._live_bursts, self._live_last_arrival,
                               self._live_burst_diagnostic_replies,
                               self._live_burst_centroids, self._live_frames,
                               self._live_last_frame_start_us,
                               self._live_completed_frames,
                               self._live_frame_counters,
                               self._live_aligned_burst_obs,
                               self._live_burst_timeline_obs,
                               self._live_sync_states,
                               self._last_multi_sync_update_ts,
                               self._live_waveform_bins,
                               self._live_icao_sync_quality,
                               self._live_period_update_history,
                               self._live_slope_history,
                               self._live_period_history,
                               self._live_sync_snapshot_cache,
                               self._live_sync_snapshot_seq,
                               self._live_sweep_frame_summary_cache,
                               self._live_sweep_frame_summary_signature,
                               self._live_sweep_frame_summary_revision,
                               self._live_sweep_frame_summary_last_cache_hit,
                               self._rotation_analysis_meta,
                               self._burst_record_dynamic_cap_by_iid,
                               self._burst_record_cap_hits_by_iid,
                               self._burst_record_last_active_aircraft_by_iid,
                               self._rotation_analysis_dynamic_cap_by_iid,
                               self._rotation_analysis_cap_hits_by_iid,
                               self._rotation_analysis_last_active_aircraft_by_iid,
                               self._burst_records,
                               self._dwell_profiles):
                if iid in live_dict:
                    del live_dict[iid]
                    had_any = True
            with self._fm_mailbox_lock:
                if iid in self._fm_mailbox:
                    del self._fm_mailbox[iid]
                    had_any = True
                if not self._fm_mailbox:
                    self._fm_mailbox_event.clear()
            if iid in self._native_burst_processors:
                del self._native_burst_processors[iid]
                had_any = True
            if iid in self._go_fm_states:
                del self._go_fm_states[iid]
                had_any = True
            if iid in self._go_fm_pipeline_stats:
                del self._go_fm_pipeline_stats[iid]
                had_any = True
            if iid in self._go_frame_positions:
                del self._go_frame_positions[iid]
                self._go_frame_positions_revision[iid] = self._go_frame_positions_revision.get(iid, 0) + 1
                had_any = True
            if iid in self._go_sweep_frames_by_iid:
                del self._go_sweep_frames_by_iid[iid]
                self._go_sweep_frames_revision[iid] = self._go_sweep_frames_revision.get(iid, 0) + 1
                had_any = True
            if iid in self._go_sync_states_by_iid:
                del self._go_sync_states_by_iid[iid]
                had_any = True
            if iid in self._go_multi_sync_states_by_iid:
                del self._go_multi_sync_states_by_iid[iid]
                had_any = True
            if iid in self._go_multi_sync_admission_by_iid:
                del self._go_multi_sync_admission_by_iid[iid]
                had_any = True
            if iid in self._compact_sync_debug_by_iid:
                del self._compact_sync_debug_by_iid[iid]
                had_any = True
            if iid in self._go_iid_state_revision:
                del self._go_iid_state_revision[iid]
                had_any = True
            if iid in self._go_reference_aircraft_by_iid:
                del self._go_reference_aircraft_by_iid[iid]
                had_any = True
            if iid in self._radar_core_frames_injected_by_iid:
                del self._radar_core_frames_injected_by_iid[iid]
                had_any = True
            if iid in self._python_frames_finalized_by_iid:
                self._python_frames_finalized_total = max(
                    0,
                    self._python_frames_finalized_total - self._python_frames_finalized_by_iid[iid],
                )
                del self._python_frames_finalized_by_iid[iid]
                had_any = True
            if self._iid_events:
                filtered_events = deque(
                    (ev for ev in self._iid_events if ev[1] != iid),
                    maxlen=_IID_EVENTS_MAX,
                )
                had_any = had_any or len(filtered_events) != len(self._iid_events)
                self._iid_events = filtered_events
            if self._live_detection_buffer:
                filtered_detections = deque(
                    (det for det in self._live_detection_buffer if det.iid != iid),
                    maxlen=self._LIVE_DETECTION_BUFFER_MAX,
                )
                had_any = had_any or len(filtered_detections) != len(self._live_detection_buffer)
                self._live_detection_buffer = filtered_detections
            if self._go_track_observations:
                filtered_go_detections = deque(
                    (det for det in self._go_track_observations if int(det.get("iid", -1)) != iid),
                    maxlen=self._GO_TRACK_OBSERVATIONS_MAX,
                )
                had_any = had_any or len(filtered_go_detections) != len(self._go_track_observations)
                self._go_track_observations = filtered_go_detections
            if self._go_evidence_events:
                filtered_go_evidence = deque(
                    (det for det in self._go_evidence_events if int(det.get("iid", -1)) != iid),
                    maxlen=self._GO_EVIDENCE_EVENTS_MAX,
                )
                had_any = had_any or len(filtered_go_evidence) != len(self._go_evidence_events)
                self._go_evidence_events = filtered_go_evidence
            if iid in self._iid_latest_arrival_us:
                del self._iid_latest_arrival_us[iid]
                had_any = True
            if self._pending_pairs:
                filtered_pairs = [pair for pair in self._pending_pairs if pair.iid != iid]
                had_any = had_any or len(filtered_pairs) != len(self._pending_pairs)
                self._pending_pairs = filtered_pairs
            if self._seen_pair_keys:
                filtered_keys = {key for key in self._seen_pair_keys if key[0] != iid}
                had_any = had_any or len(filtered_keys) != len(self._seen_pair_keys)
                self._seen_pair_keys = filtered_keys
            return had_any

    def reset_all(self) -> dict:
        """Clear all in-memory radar learning so every IID is relearned from scratch."""
        with self._lock:
            cleared = {
                "models": len(self._models),
                "sweeps": len(self._sweep_history),
                "events": len(self._iid_events),
                "burst_records": sum(len(v) for v in self._burst_records.values()),
                "pending_pairs": len(self._pending_pairs),
                "seen_pair_keys": len(self._seen_pair_keys),
                "sync_states": len(self._live_sync_states),
                "waveform_bins": len(self._live_waveform_bins),
                "icao_sync_quality": len(self._live_icao_sync_quality),
                "multi_sync_throttle": len(self._last_multi_sync_update_ts),
                "period_update_history": len(self._live_period_update_history),
            }
            self._models.clear()
            self._sweep_history.clear()
            self._iid_events.clear()
            self._burst_records.clear()
            self._dirty_iids.clear()
            self._iid_latest_arrival_us.clear()
            self._pending_pairs.clear()
            self._seen_pair_keys.clear()
            self._live_bursts.clear()
            self._live_burst_diagnostic_replies.clear()
            self._live_last_arrival.clear()
            self._live_burst_centroids.clear()
            self._live_frames.clear()
            self._live_last_frame_start_us.clear()
            self._live_completed_frames.clear()
            self._live_frame_counters.clear()
            self._live_aligned_burst_obs.clear()
            self._live_burst_timeline_obs.clear()
            self._live_sync_states.clear()
            self._last_multi_sync_update_ts.clear()
            self._live_waveform_bins.clear()
            self._live_icao_sync_quality.clear()
            self._live_period_update_history.clear()
            self._live_slope_history.clear()
            self._live_period_history.clear()
            self._live_period_clean_update_streak.clear()
            self._live_sync_snapshot_cache.clear()
            self._live_sync_snapshot_seq.clear()
            self._live_sync_snapshot_last_cache_hit.clear()
            self._live_sweep_frame_summary_cache.clear()
            self._live_sweep_frame_summary_signature.clear()
            self._live_sweep_frame_summary_revision.clear()
            self._live_sweep_frame_summary_last_cache_hit.clear()
            self._rotation_analysis_meta.clear()
            self._burst_record_dynamic_cap_by_iid.clear()
            self._burst_record_cap_hits_by_iid.clear()
            self._burst_record_last_active_aircraft_by_iid.clear()
            self._rotation_analysis_dynamic_cap_by_iid.clear()
            self._rotation_analysis_cap_hits_by_iid.clear()
            self._rotation_analysis_last_active_aircraft_by_iid.clear()
            self._live_detection_buffer.clear()
            self._native_burst_processors.clear()
            self._dwell_profiles.clear()
            self._go_fm_states.clear()
            self._go_fm_pipeline_stats.clear()
            self._go_frame_positions.clear()
            self._go_frame_positions_revision.clear()
            self._go_sweep_frames_by_iid.clear()
            self._go_sweep_frames_revision.clear()
            self._go_sync_states_by_iid.clear()
            self._go_multi_sync_states_by_iid.clear()
            self._go_multi_sync_admission_by_iid.clear()
            self._compact_sync_debug_by_iid.clear()
            self._go_iid_state_revision.clear()
            self._go_reference_aircraft_by_iid.clear()
            self._go_track_observations.clear()
            self._go_evidence_events.clear()
            self._radar_core_frames_injected_by_iid.clear()
            self._python_frames_finalized_total = 0
            self._python_frames_finalized_by_iid.clear()
            with self._fm_mailbox_lock:
                self._fm_mailbox.clear()
                self._fm_mailbox_event.clear()
            return cleared

    def get_all_rotation_models(self) -> dict[int, RadarIID]:
        with self._lock:
            return dict(self._models)

    def get_localisation_control(self, iid: int) -> RadarIID | None:
        with self._lock:
            return self._models.get(iid)

    def localisation_enabled(self, iid: int) -> bool:
        with self._lock:
            model = self._models.get(iid)
            if model is None:
                return True
            return model.resolution_mode == "auto"

    def set_manual_position(
        self,
        iid: int,
        lat: float,
        lon: float,
        note: str | None = None,
    ) -> RadarIID:
        with self._lock:
            model = self._models.get(iid)
            if model is None:
                model = RadarIID(iid=iid)
                self._models[iid] = model
            model.manual_lat = lat
            model.manual_lon = lon
            model.manual_note = note
            model.manual_updated_ts = time.time()
            return model

    def lock_manual_position(self, iid: int) -> tuple[bool, RadarIID | None, str | None]:
        with self._lock:
            model = self._models.get(iid)
            if model is None:
                model = RadarIID(iid=iid)
                self._models[iid] = model
            if model.manual_lat is None or model.manual_lon is None:
                return False, model, "manual position not set"
            model.resolution_mode = "locked_position"
            return True, model, None

    def unlock_position(self, iid: int) -> tuple[bool, RadarIID | None]:
        with self._lock:
            model = self._models.get(iid)
            if model is None:
                return False, None
            model.resolution_mode = "auto"
            return True, model

    def mark_unresolvable(self, iid: int, reason: str | None = None) -> RadarIID:
        with self._lock:
            model = self._models.get(iid)
            if model is None:
                model = RadarIID(iid=iid)
                self._models[iid] = model
            model.resolution_mode = "locked_unresolvable"
            model.unresolvable_reason = reason
            model.unresolvable_updated_ts = time.time()
            return model

    def clear_unresolvable(self, iid: int) -> tuple[bool, RadarIID | None]:
        with self._lock:
            model = self._models.get(iid)
            if model is None:
                return False, None
            model.resolution_mode = "auto"
            model.unresolvable_reason = None
            model.unresolvable_updated_ts = None
            return True, model

    def _nearest_pos_from_histories(
        self,
        pos_histories: dict[str, list[tuple]],
        latest_arrival_us: float | None,
        icao: str,
        burst_beast_us: int,
    ) -> dict | None:
        samples = pos_histories.get(icao)
        if not samples:
            return None

        beast_samples = [(s[0], s) for s in samples if s[0] > 0]
        if beast_samples:
            _beast_ts_us, s = min(beast_samples, key=lambda x: abs(x[0] - burst_beast_us))
            dt_s = (burst_beast_us - s[0]) / 1_000_000.0
            lat, lon, gs, trk = s[2], s[3], s[4], s[5]
            if gs is not None and trk is not None and gs > 0 and abs(dt_s) < 10.0:
                import math
                v_ms = gs * 0.514444
                dlat = (v_ms * dt_s * math.cos(math.radians(trk))) / 111_320
                dlon = (v_ms * dt_s * math.sin(math.radians(trk))) / (
                    111_320 * max(math.cos(math.radians(lat)), 1e-6)
                )
                return {
                    "lat": lat + dlat,
                    "lon": lon + dlon,
                    "interpolated": abs(dt_s) > 0.1,
                    "groundspeed_kts": gs,
                    "track_deg": trk,
                    "source": "beast_ts_match",
                }
            return {
                "lat": lat,
                "lon": lon,
                "interpolated": False,
                "groundspeed_kts": gs,
                "track_deg": trk,
                "source": "beast_ts_match",
            }

        wall_ts = self._estimate_wall_time_from_arrival_us(burst_beast_us, latest_arrival_us)
        if wall_ts is None:
            return None
        ts_samples = sorted(samples, key=lambda s: abs(s[1] - wall_ts))
        if not ts_samples:
            return None
        s = ts_samples[0]
        return {
            "lat": s[2],
            "lon": s[3],
            "interpolated": False,
            "groundspeed_kts": s[4],
            "track_deg": s[5],
            "source": "wall_ts_match",
        }

    def _compute_sweep_history_for_iid(self, iid: int) -> list[dict]:
        with self._lock:
            br_deque = self._burst_records.get(iid)
            if not br_deque:
                return []
            records_snapshot = list(br_deque)
            latest_arrival_us = br_deque[-1].centroid_us

        if not records_snapshot:
            return []

        active_icaos = {r.icao for r in records_snapshot if r.icao}
        pos_histories: dict[str, list[tuple]] = {}
        if self._aircraft_state is not None and active_icaos:
            try:
                pos_histories = self._aircraft_state.get_position_histories_bulk(active_icaos, window_s=300.0)
            except Exception:
                pos_histories = {}

        sweeps = self._build_sweeps_from_burst_records(records_snapshot)
        # Enrich with positions from history
        for sweep in sweeps:
            for ac_entry in sweep.get("aircraft", []):
                icao = ac_entry.get("icao")
                beam_center_us = ac_entry.get("beam_center_us")
                if icao and beam_center_us is not None:
                    pos = self._nearest_pos_from_histories(
                        pos_histories, latest_arrival_us, icao, int(beam_center_us)
                    )
                    if pos and pos.get("lat") is not None:
                        ac_entry["lat"] = pos["lat"]
                        ac_entry["lon"] = pos["lon"]
                        ac_entry["interpolated"] = pos.get("interpolated", False)

        with self._lock:
            if iid not in self._sweep_history:
                self._sweep_history[iid] = deque(maxlen=self._SWEEP_HISTORY_MAX)
            self._sweep_history[iid].clear()
            self._sweep_history[iid].extend(sweeps)
        return sweeps

    def get_sweep_history(self, iid: int, n: int = 30) -> list[dict]:
        with self._lock:
            history = self._sweep_history.get(iid)
            items = list(history) if history else []
        if not items:
            items = self._compute_sweep_history_for_iid(iid)
            if not items:
                return []
        with self._lock:
            items = list(self._sweep_history.get(iid, [])) or items
            return items[-n:]

    # ------------------------------------------------------------------
    # IID activity summary (for /api/radar/iids)
    # ------------------------------------------------------------------

    def get_iid_activity(self, window_s: float = 600.0) -> dict[int, dict]:
        """Return per-IID activity within the last window_s seconds of Beast time.

        Primary source: _burst_records (preferred; populated once a model exists).
        Bootstrap fallback: _iid_events (used for IIDs not yet in _burst_records,
        e.g. after purge/reset, so the IID selector shows activity during bootstrap).
        """
        with self._lock:
            latest_burst_us = max(
                (br_deque[-1].centroid_us for br_deque in self._burst_records.values() if br_deque),
                default=None,
            )
            latest_event_us = self._iid_events[-1][0] if self._iid_events else None
            if latest_burst_us is None and latest_event_us is None:
                return {}
            now_us = max(x for x in [latest_burst_us, latest_event_us] if x is not None)
            cutoff_us = now_us - int(window_s * 1_000_000)
            burst_snapshot = {iid: list(deq) for iid, deq in self._burst_records.items()}
            iids_with_bursts = set(burst_snapshot.keys())
            # Only snapshot raw events for IIDs not already covered by burst records.
            raw_events_snapshot = (
                [ev for ev in self._iid_events if ev[1] not in iids_with_bursts]
                if self._iid_events else []
            )

        activity: dict[int, dict] = {}
        for iid, records in burst_snapshot.items():
            for rec in records:
                if rec.centroid_us < cutoff_us:
                    continue
                entry = activity.get(iid)
                if entry is None:
                    activity[iid] = {
                        "count": rec.n_replies,
                        "last_us": rec.centroid_us,
                        "latest_icao": rec.icao,
                    }
                else:
                    entry["count"] += rec.n_replies
                    if rec.centroid_us >= entry["last_us"]:
                        entry["last_us"] = rec.centroid_us
                        if rec.icao:
                            entry["latest_icao"] = rec.icao

        # Bootstrap fallback: derive activity from raw events for IIDs that do not
        # yet have burst records (e.g. immediately after purge/reset).
        for arrival_us, iid, icao, _sig in raw_events_snapshot:
            if arrival_us < cutoff_us:
                continue
            entry = activity.get(iid)
            if entry is None:
                activity[iid] = {"count": 1, "last_us": arrival_us, "latest_icao": icao}
            else:
                entry["count"] += 1
                if arrival_us >= entry["last_us"]:
                    entry["last_us"] = arrival_us
                    if icao:
                        entry["latest_icao"] = icao

        return activity

    def get_latest_arrival_us(self) -> float | None:
        """Return the latest Beast-relative arrival timestamp seen by the radar state."""
        with self._lock:
            latest = max(
                (br_deque[-1].centroid_us for br_deque in self._burst_records.values() if br_deque),
                default=None,
            )
            if latest is not None:
                return latest
            # Fallback to raw events while _burst_records is warming up
            return self._iid_events[-1][0] if self._iid_events else None

    def get_iid_latest_arrival_us(self, iid: int) -> float | None:
        """Return the latest Beast-relative arrival timestamp seen for one IID."""
        with self._lock:
            return self._iid_latest_arrival_us.get(iid)

    def get_iid_timeline(self, iid: int, window_s: float = 30.0) -> dict:
        """Return per-ICAO burst centroid timestamps for a single IID."""
        with self._lock:
            br_deque = self._burst_records.get(iid)
            go_evidence = [entry for entry in self._go_evidence_events if int(entry.get("iid", -1)) == iid]
            if br_deque:
                now_us = br_deque[-1].centroid_us
                cutoff_us = now_us - int(window_s * 1_000_000)
                records_snapshot = list(br_deque)
            else:
                records_snapshot = []
                if not go_evidence:
                    return {}
                now_us = max(float(entry.get("arrival_us") or 0.0) for entry in go_evidence)
                cutoff_us = now_us - int(window_s * 1_000_000)

        icao_arrivals: dict[str, list[float]] = defaultdict(list)
        for rec in records_snapshot:
            if rec.centroid_us >= cutoff_us and rec.icao:
                icao_arrivals[rec.icao].append(rec.centroid_us)
        if not icao_arrivals and go_evidence:
            for entry in go_evidence:
                arrival_us = float(entry.get("arrival_us") or 0.0)
                icao = entry.get("icao")
                if arrival_us >= cutoff_us and icao:
                    icao_arrivals[str(icao)].append(arrival_us)
        return dict(icao_arrivals)

    def _build_df11_residual_observations(
        self,
        sync: "LiveSyncState",
        waveform_bins: list,
        iid_events: list[tuple[float, int, str, "float | None"]],
        latest_arrival_us: "float | None",
    ) -> list[dict]:
        """Compute DF11 residual observations for the burst-sync residual chart.

        This is the authoritative backend path for individual DF11 arrival
        residuals.  It uses the same predict_sync_observation() call as burst
        observations so both chart layers share:
          - Beast-relative timestamp basis
          - current refined period / phase / anchor state
          - propagation correction policy (driven by sync.prop_delay_enabled)
          - waveform correction policy (driven by sync.waveform_enabled)
          - residual wrapping convention: (obs - pred + 540) % 360 - 180

        Motion compensation is intentionally omitted for individual DF11
        arrivals: no per-message bearing-rate estimate is available, and the
        burst-centre averaging that makes motion comp meaningful does not apply
        to single-message events.

        Individual DF11 arrivals that cannot be given a trustworthy residual
        (no ADS-B position available at that timestamp) are omitted rather
        than guessed — prefer omission over false precision.

        Previously, DF11 residual dots were computed in the frontend using a
        local predictBearingFromSyncModel() function.  That was inconsistent
        with the backend burst residuals because it used a stale sync snapshot,
        a different predictor code path, and potentially different waveform/
        propagation correction state.  This method replaces that approach.
        """
        if sync is None or not sync.usable:
            return []
        receiver_lat = self._receiver_lat
        receiver_lon = self._receiver_lon
        if receiver_lat is None or receiver_lon is None:
            return []

        results: list[dict] = []
        for arrival_us, _iid, icao, signal_dbfs in iid_events:
            # Estimate wall-clock time for this arrival using the same method
            # used for burst-centre position lookup.
            wall_ts = self._estimate_wall_time_from_arrival_us(arrival_us, latest_arrival_us)
            if wall_ts is None:
                continue
            pos = self._adsb_tracker.get_position_at(icao, wall_ts)
            if pos is None or pos.get("lat") is None or pos.get("lon") is None:
                # No trustworthy aircraft position — omit this dot entirely.
                continue
            truth_lat: float = pos["lat"]
            truth_lon: float = pos["lon"]
            # Bearing and range from the receiver to the aircraft at arrival time.
            # Same geometry functions used for burst observations.
            bearing_deg = _bearing_deg_simple(receiver_lat, receiver_lon, truth_lat, truth_lon)
            range_nm = _haversine_nm_simple(receiver_lat, receiver_lon, truth_lat, truth_lon)
            # Authoritative residual prediction — identical predictor path to
            # get_burst_sync_timeline() burst entries.
            prediction = predict_sync_observation(
                sync,
                arrival_us,
                range_nm=range_nm,
                waveform_bins=waveform_bins,
                # Motion comp not applied: no bearing-rate estimate for single messages.
                bearing_rate_deg_s=None,
                motion_comp_dt_us=None,
                motion_comp_block_reason="individual_df11_arrival",
            )
            # Residual wrapping: identical to burst observations.
            residual_deg = (
                bearing_deg - prediction.predicted_bearing_deg + 540.0
            ) % 360.0 - 180.0
            # timing_class uses the same threshold as BURST_SYNC_DF11_ON_TIME_THRESHOLD_DEG
            # in RadarPage.jsx so the frontend colour mapping is consistent.
            abs_res = abs(residual_deg)
            if abs_res <= _DF11_RESIDUAL_ON_TIME_THRESHOLD_DEG:
                timing_class = "on_time"
            elif residual_deg > 0:
                timing_class = "early"
            else:
                timing_class = "late"
            results.append({
                "icao": icao,
                "arrival_beast_us": arrival_us,
                "effective_beast_us": prediction.effective_arrival_us,
                "true_bearing_deg": round(bearing_deg, 4),
                "predicted_deg": round(prediction.predicted_bearing_deg, 4),
                "residual_deg": round(residual_deg, 4),
                "timing_class": timing_class,
                "range_nm": round(range_nm, 2),
                "pos_age_s": pos.get("position_age_seconds"),
                "signal_dbfs": signal_dbfs,
                # fit_eligible is always False for individual arrivals — only
                # burst-centre observations drive sync fitting.
                "fit_eligible": False,
                "fit_reject_reason": "individual_df11_arrival",
                "residual_source": "df11",
            })
        return results

    def _go_burst_sync_timeline_snapshot(
        self,
        iid: int,
        window_s: float,
    ) -> list[AlignedBurstSyncObs]:
        """Rebuild timeline observations lazily from Go-owned burst evidence."""
        evidence = self._go_evidence_event_snapshot(iid)
        if not evidence:
            return []

        with self._lock:
            model = self._models.get(iid)
            sync = self._live_sync_states.get(iid)
        radar_pos = _get_authoritative_radar_position(model) if model is not None else {"lat": None, "lon": None}
        radar_lat = radar_pos.get("lat")
        radar_lon = radar_pos.get("lon")
        if radar_lat is None or radar_lon is None:
            return []

        cutoff_ts = time.time() - window_s
        period_s = sync.period_s if sync is not None and sync.period_s > 0 else 0.0
        observations: list[AlignedBurstSyncObs] = []
        for entry in sorted(evidence, key=lambda row: (float(row.get("wall_ts") or 0.0), float(row.get("arrival_us") or 0.0))):
            if str(entry.get("kind") or "burst_fired") != "burst_fired":
                continue
            wall_ts = float(entry.get("wall_ts") or 0.0)
            if wall_ts < cutoff_ts:
                continue
            truth_lat = entry.get("truth_lat")
            truth_lon = entry.get("truth_lon")
            if truth_lat is None or truth_lon is None:
                continue
            bearing_deg = _bearing_deg_simple(radar_lat, radar_lon, float(truth_lat), float(truth_lon))
            range_nm = _haversine_nm_simple(radar_lat, radar_lon, float(truth_lat), float(truth_lon))
            pos_age_s = float(entry.get("position_age_seconds") or 0.0)
            motion_estimate = _estimate_aircraft_bearing_rate(
                icao=str(entry["icao"]),
                bearing_deg=bearing_deg,
                burst_centroid_us=float(entry["arrival_us"]),
                pos_age_s=pos_age_s,
                history=observations,
            )
            bearing_rate_deg_s = motion_estimate.get("bearing_rate_deg_s")
            motion_comp_dt_us = _compute_motion_comp_dt_us(period_s, bearing_rate_deg_s) if period_s > 0 else None
            motion_block_reason = motion_estimate.get("motion_comp_block_reason")
            motion_applied = bool(
                RADAR_SYNC_MOTION_COMP_PHASE_ENABLED
                and motion_comp_dt_us is not None
                and motion_block_reason is None
            )
            if not RADAR_SYNC_MOTION_COMP_PHASE_ENABLED:
                motion_block_reason = "disabled"
            elif motion_comp_dt_us is None and motion_block_reason is None:
                motion_block_reason = "bearing_rate_unavailable"
            prop_delay_us = _compute_propagation_delay_us(range_nm)
            prop_corrected_us = (
                float(entry["arrival_us"]) - prop_delay_us
                if RADAR_SYNC_PROP_DELAY_ENABLED else float(entry["arrival_us"])
            )
            effective_us = prop_corrected_us - motion_comp_dt_us if motion_applied else prop_corrected_us
            observations.append(AlignedBurstSyncObs(
                burst_centroid_us=float(entry["arrival_us"]),
                icao=str(entry["icao"]),
                bearing_deg=bearing_deg,
                n_replies=int(entry.get("n_replies") or 0),
                signal_dbfs=entry.get("signal_dbfs"),
                pos_age_s=pos_age_s,
                range_nm=range_nm,
                ts=wall_ts,
                sync_update_eligible=bool(
                    entry.get("compact_sync_eligible", entry.get("sync_eligible", False))
                ),
                raw_arrival_us=float(entry["arrival_us"]),
                prop_delay_aircraft_to_receiver_us=prop_delay_us,
                prop_delay_radar_to_aircraft_us=None,
                effective_arrival_us=effective_us,
                bearing_rate_deg_s=bearing_rate_deg_s,
                motion_comp_dt_us=motion_comp_dt_us,
                motion_corrected_beast_us=effective_us,
                motion_comp_applied=motion_applied,
                motion_comp_block_reason=motion_block_reason,
                burst_center_simple_us=entry.get("simple_centroid_us"),
                burst_center_weighted_us=entry.get("weighted_centroid_us"),
                burst_center_delta_us=entry.get("centroid_delta_us"),
                burst_center_method=(
                    "amplitude_weighted"
                    if entry.get("weighted_centroid_us") is not None
                    else "centroid"
                ),
                burst_ts_first_reply_beast_us=entry.get("first_reply_us"),
                burst_ts_strongest_reply_beast_us=entry.get("strongest_reply_us"),
                burst_ts_simple_centroid_beast_us=entry.get("simple_centroid_us"),
                burst_ts_weighted_centroid_beast_us=entry.get("weighted_centroid_us"),
                burst_ts_mid_strong_window_beast_us=entry.get("mid_strong_window_us"),
                burst_ts_last_reply_beast_us=entry.get("last_reply_us"),
                burst_span_us=entry.get("span_us"),
                peak_amplitude=entry.get("peak_amplitude"),
                position_interpolated=False,
                position_extrapolated=False,
                position_source_age_s=pos_age_s,
                truth_position_ts_beast_us=(
                    float(entry["arrival_us"]) - pos_age_s * 1_000_000.0
                    if pos_age_s > 0 else float(entry["arrival_us"])
                ),
            ))
        return observations

    def _go_sweep_frame_sync_timeline_snapshot(
        self,
        iid: int,
        window_s: float,
    ) -> list[AlignedBurstSyncObs]:
        """Rebuild compact alignment observations from retained Go sweep frames.

        This is a fallback when raw Go burst evidence is no longer retained but
        the IID is still producing accepted sweep frames. The rows are compact
        alignment evidence only; they do not restore Python's richer refined
        diagnostics.
        """
        with self._lock:
            go_frames = list(self._go_sweep_frames_by_iid.get(iid, ()))
            model = self._models.get(iid)
            latest_arrival_us = self._iid_latest_arrival_us.get(iid)
            sync = self._live_sync_states.get(iid)
        if not go_frames:
            return []

        radar_pos = _get_authoritative_radar_position(model) if model is not None else {"lat": None, "lon": None}
        radar_lat = radar_pos.get("lat")
        radar_lon = radar_pos.get("lon")
        if radar_lat is None or radar_lon is None:
            return []

        cutoff_ts = time.time() - window_s
        observations: list[AlignedBurstSyncObs] = []
        period_s = sync.period_s if sync is not None and sync.period_s > 0 else 0.0
        for frame in go_frames:
            frame_rows = [
                (frame.ref_icao, frame.ref_arrival_us, frame.ref_lat, frame.ref_lon, 1, getattr(frame, "ref_pos_age_s", 0.0)),
            ]
            frame_rows.extend(
                (
                    obs.icao,
                    obs.arrival_us,
                    obs.lat,
                    obs.lon,
                    getattr(obs, "n_replies", 1),
                    getattr(obs, "position_age_seconds", 0.0),
                )
                for obs in frame.observations
            )
            for icao, arrival_us, lat, lon, n_replies, pos_age_s in frame_rows:
                wall_ts = self._estimate_wall_time_from_arrival_us(float(arrival_us), latest_arrival_us)
                if wall_ts is None or wall_ts < cutoff_ts:
                    continue
                bearing_deg = _bearing_deg_simple(radar_lat, radar_lon, float(lat), float(lon))
                range_nm = _haversine_nm_simple(radar_lat, radar_lon, float(lat), float(lon))
                motion_estimate = _estimate_aircraft_bearing_rate(
                    icao=str(icao),
                    bearing_deg=bearing_deg,
                    burst_centroid_us=float(arrival_us),
                    pos_age_s=float(pos_age_s or 0.0),
                    history=observations,
                )
                bearing_rate_deg_s = motion_estimate.get("bearing_rate_deg_s")
                motion_comp_dt_us = _compute_motion_comp_dt_us(period_s, bearing_rate_deg_s) if period_s > 0 else None
                motion_block_reason = motion_estimate.get("motion_comp_block_reason")
                motion_applied = bool(
                    RADAR_SYNC_MOTION_COMP_PHASE_ENABLED
                    and motion_comp_dt_us is not None
                    and motion_block_reason is None
                )
                if not RADAR_SYNC_MOTION_COMP_PHASE_ENABLED:
                    motion_block_reason = "disabled"
                elif motion_comp_dt_us is None and motion_block_reason is None:
                    motion_block_reason = "bearing_rate_unavailable"
                prop_delay_us = _compute_propagation_delay_us(range_nm)
                prop_corrected_us = (
                    float(arrival_us) - prop_delay_us
                    if RADAR_SYNC_PROP_DELAY_ENABLED else float(arrival_us)
                )
                effective_us = prop_corrected_us - motion_comp_dt_us if motion_applied else prop_corrected_us
                observations.append(AlignedBurstSyncObs(
                    burst_centroid_us=float(arrival_us),
                    icao=str(icao),
                    bearing_deg=bearing_deg,
                    n_replies=int(n_replies or 1),
                    signal_dbfs=None,
                    pos_age_s=float(pos_age_s or 0.0),
                    range_nm=range_nm,
                    ts=wall_ts,
                    sync_update_eligible=True,
                    raw_arrival_us=float(arrival_us),
                    prop_delay_aircraft_to_receiver_us=prop_delay_us,
                    prop_delay_radar_to_aircraft_us=None,
                    effective_arrival_us=effective_us,
                    bearing_rate_deg_s=bearing_rate_deg_s,
                    motion_comp_dt_us=motion_comp_dt_us,
                    motion_corrected_beast_us=effective_us,
                    motion_comp_applied=motion_applied,
                    motion_comp_block_reason=motion_block_reason,
                    burst_center_method="go_sweep_frame",
                    position_interpolated=False,
                    position_extrapolated=False,
                    position_source_age_s=float(pos_age_s or 0.0),
                    truth_position_ts_beast_us=(
                        float(arrival_us) - float(pos_age_s or 0.0) * 1_000_000.0
                        if float(pos_age_s or 0.0) > 0 else float(arrival_us)
                    ),
                ))
        observations.sort(key=lambda obs: (obs.ts, obs.burst_centroid_us, obs.icao))
        return observations

    def _go_aligned_burst_sync_snapshot(
        self,
        iid: int,
        window_s: float | None = None,
    ) -> list[AlignedBurstSyncObs]:
        """Rebuild sync-driving aligned observations from Go-owned burst evidence."""
        observations = self._go_burst_sync_timeline_snapshot(
            iid,
            window_s=window_s or 300.0,
        )
        aligned = [
            obs for obs in observations
            if getattr(obs, "sync_update_eligible", False)
        ]
        if len(aligned) > self._MULTI_SYNC_OBS_MAX:
            aligned = aligned[-self._MULTI_SYNC_OBS_MAX:]
        return aligned

    def _build_compact_burst_sync_timeline_entries(
        self,
        sync: LiveSyncState,
        obs_snapshot: list[AlignedBurstSyncObs],
        window_s: float,
    ) -> list[dict]:
        """Build a compact burst timeline for Go-owned sync sources.

        Non-`multi_aircraft_burst` sources do not carry Python's richer anchor,
        waveform, or fit-history state. Avoid rebuilding those diagnostics and
        expose only the compact predictor/residual view that is still valid.
        """
        now_ts = time.time()
        cutoff_ts = now_ts - window_s
        entries: list[dict] = []
        refined_go_source = getattr(sync, "source", None) == "go_multi_aircraft_burst"
        candidate_by_icao = {}
        if refined_go_source:
            candidate_by_icao = {
                row.get("icao"): row
                for row in (getattr(sync, "phase_anchor_candidates", []) or [])
                if row.get("icao")
            }
        for obs in obs_snapshot:
            if obs.ts < cutoff_ts:
                continue
            prediction = predict_sync_observation(
                sync,
                obs.burst_centroid_us,
                range_nm=getattr(obs, "range_nm", None),
                waveform_bins=None,
                bearing_rate_deg_s=getattr(obs, "bearing_rate_deg_s", None),
                motion_comp_dt_us=getattr(obs, "motion_comp_dt_us", None),
                motion_comp_block_reason=getattr(obs, "motion_comp_block_reason", None),
            )
            residual_deg = (
                obs.bearing_deg - prediction.predicted_bearing_deg + 540.0
            ) % 360.0 - 180.0
            classification = self._classify_sync_residual(abs(residual_deg))
            weight = self._score_sync_burst_observation(obs)
            fit_eligible = bool(getattr(obs, "sync_update_eligible", True)) and classification != "rejected" and weight > 0
            implied_phase_offset = None
            anchor_relative_error = None
            phase_anchor_contributor = False
            phase_anchor_reject_reason = None
            if refined_go_source:
                implied_phase_offset = (
                    obs.bearing_deg
                    + float(getattr(prediction, "waveform_correction_deg", 0.0) or 0.0)
                    - float(getattr(prediction, "phase_in_rot_deg", 0.0) or 0.0)
                ) % 360.0
                anchor_relative_error = _circular_delta_deg(
                    implied_phase_offset,
                    getattr(sync, "phase_anchor_offset_smoothed_deg", None),
                )
                phase_anchor_contributor = obs.icao == getattr(sync, "phase_anchor_icao", None)
                candidate = candidate_by_icao.get(obs.icao) or {}
                reject_reasons = candidate.get("reject_reasons") or []
                phase_anchor_reject_reason = ",".join(reject_reasons) if reject_reasons else None
            entries.append({
                "beam_center_us": obs.burst_centroid_us,
                "wall_ts": obs.ts,
                "icao": obs.icao,
                "bearing_deg": obs.bearing_deg,
                "predicted_deg": prediction.predicted_bearing_deg,
                "predicted_raw_deg": prediction.predicted_bearing_raw_deg,
                "predicted_after_prop_deg": prediction.predicted_bearing_deg,
                "pred_without_motion_deg": prediction.predicted_bearing_deg,
                "pred_with_motion_deg": prediction.predicted_bearing_deg,
                "predicted_corrected_deg": prediction.predicted_bearing_deg,
                "residual_deg": residual_deg,
                "residual_raw_deg": residual_deg,
                "residual_after_prop_deg": residual_deg,
                "residual_without_motion_deg": residual_deg,
                "residual_with_motion_deg": residual_deg,
                "residual_after_waveform_deg": residual_deg,
                "residual_corrected_deg": residual_deg,
                "motion_comp_improvement_deg": 0.0,
                "residual_for_period_fit_deg": residual_deg if fit_eligible else None,
                "implied_phase_offset_deg": implied_phase_offset,
                "anchor_relative_phase_error_deg": anchor_relative_error,
                "phase_anchor_contributor": phase_anchor_contributor,
                "phase_anchor_reject_reason": phase_anchor_reject_reason,
                "phase_in_rot_deg": prediction.phase_in_rot_deg,
                "raw_arrival_us": getattr(obs, "raw_arrival_us", obs.burst_centroid_us),
                "prop_corrected_beast_us": prediction.prop_corrected_beast_us,
                "effective_arrival_us": prediction.effective_arrival_us,
                "motion_corrected_beast_us": prediction.motion_corrected_beast_us,
                "prop_delay_us": prediction.propagation_correction_us,
                "bearing_rate_deg_s": prediction.bearing_rate_deg_s,
                "motion_comp_dt_us": prediction.motion_comp_dt_us,
                "motion_comp_enabled": prediction.motion_comp_enabled,
                "motion_comp_applied": prediction.motion_comp_applied,
                "motion_comp_block_reason": prediction.motion_comp_block_reason,
                "waveform_correction_deg": prediction.waveform_correction_deg,
                "waveform_applied": prediction.waveform_applied,
                "prediction_path": prediction.predictor_version,
                "weight": weight,
                "classification": classification,
                "fit_eligible": fit_eligible,
                "fit_reject_reason": None if fit_eligible else ("go_refined_sync" if refined_go_source else "compact_go_sync"),
                "n_replies": obs.n_replies,
                "signal_dbfs": obs.signal_dbfs,
                "pos_age_s": obs.pos_age_s,
                "range_nm": obs.range_nm,
                "sync_update_eligible": bool(getattr(obs, "sync_update_eligible", True)),
                "burst_center_method": getattr(obs, "burst_center_method", "centroid"),
                "burst_center_simple_us": getattr(obs, "burst_center_simple_us", None),
                "burst_center_weighted_us": getattr(obs, "burst_center_weighted_us", None),
                "burst_center_delta_us": getattr(obs, "burst_center_delta_us", None),
            })
        entries.sort(key=lambda e: e["beam_center_us"])
        return entries

    @staticmethod
    def _build_compact_residual_observations_from_entries(entries: list[dict], source: str) -> list[dict]:
        """Fallback residual-dot rows when only compact Go burst/frame evidence exists."""
        results: list[dict] = []
        for entry in entries:
            residual_deg = float(entry.get("residual_deg") or 0.0)
            abs_res = abs(residual_deg)
            if abs_res <= _DF11_RESIDUAL_ON_TIME_THRESHOLD_DEG:
                timing_class = "on_time"
            elif residual_deg > 0:
                timing_class = "early"
            else:
                timing_class = "late"
            results.append({
                "icao": entry.get("icao"),
                "arrival_beast_us": entry.get("raw_arrival_us", entry.get("beam_center_us")),
                "effective_beast_us": entry.get("effective_arrival_us"),
                "true_bearing_deg": round(float(entry.get("bearing_deg") or 0.0), 4),
                "predicted_deg": round(float(entry.get("predicted_deg") or 0.0), 4),
                "residual_deg": round(residual_deg, 4),
                "timing_class": timing_class,
                "range_nm": round(float(entry.get("range_nm") or 0.0), 2),
                "pos_age_s": entry.get("pos_age_s"),
                "signal_dbfs": entry.get("signal_dbfs"),
                "fit_eligible": False,
                "fit_reject_reason": "compact_go_alignment",
                "residual_source": source,
            })
        return results

    def _build_compact_sync_debug_payload(
        self,
        iid: int,
        sync: LiveSyncState,
        burst_timeline: dict,
        limit: int,
    ) -> dict:
        """Return a compact sync-debug payload for Go-owned sync sources."""
        observations = list(burst_timeline.get("observations") or [])
        if limit > 0:
            observations = observations[-limit:]
        motion_summary = burst_timeline.get("motion_comp_summary") or {}
        retention_diagnostics = burst_timeline.get("retention_diagnostics")
        fit_eligible_count = sum(1 for row in observations if row.get("fit_eligible"))
        sync_update_eligible_count = sum(1 for row in observations if row.get("sync_update_eligible"))
        alignment_status = burst_timeline.get("alignment_status")
        refined_source = getattr(sync, "source", None) == "go_multi_aircraft_burst"
        return {
            "iid": iid,
            "available": True,
            "observations": observations,
            "summary": {
                "iid": iid,
                "sync_source": getattr(sync, "source", None),
                "diagnostics_mode": "refined_go_sync" if refined_source else "compact_go_sync",
                "rich_diagnostics_available": False,
                "compact_reason": "rich_python_multi_aircraft_diagnostics_unavailable_for_go_sync_source",
                "wall_clock_used_operationally": False,
                "observation_count": len(observations),
                "fit_eligible_count": fit_eligible_count,
                "sync_update_eligible_count": sync_update_eligible_count,
                "motion_comp_applied_count": int(motion_summary.get("applied_count") or 0),
                "retention_diagnostics": retention_diagnostics,
                "alignment_status": alignment_status,
            },
            "retention_diagnostics": retention_diagnostics,
            "alignment_status": alignment_status,
            "observation_model_diagnostics": {
                "mode": "refined_go_sync" if refined_source else "compact_go_sync",
                "reason": "rich_python_multi_aircraft_diagnostics_unavailable_for_sync_source",
            },
        }

    def _build_sync_mode_diagnostics(
        self,
        iid: int,
        sync: LiveSyncState | None,
        alignment_status: dict | None,
    ) -> dict:
        with self._lock:
            compact_debug = dict(self._compact_sync_debug_by_iid.get(iid) or {})
            go_admission = dict(self._go_multi_sync_admission_by_iid.get(iid) or {})
        sync_source = getattr(sync, "source", None) if sync is not None else None
        refined_active = bool(sync_source in {"multi_aircraft_burst", "go_multi_aircraft_burst"})
        refined_present = bool(refined_active or (go_admission and go_admission.get("counts")))
        refined_usable = bool(sync is not None and refined_active and getattr(sync, "usable", False))
        authority_mode = getattr(sync, "active_authority_mode", None) if sync is not None else None
        if authority_mode == "dominant_recovery":
            active_mode = "dominant_recovery"
            active_label = "Dominant-period recovery"
        elif authority_mode == "compact_authoritative":
            active_mode = "compact_bootstrap"
            active_label = "Compact sync (authoritative)"
        elif refined_active:
            active_mode = "refined_multi_aircraft"
            active_label = "Refined sync (multi-aircraft)"
        else:
            active_mode = "compact_bootstrap"
            active_label = "Compact sync (sweep-frame)"
        no_anchor_reason = None
        if refined_active:
            no_anchor_reason = getattr(sync, "phase_anchor_no_candidate_reason", None)
            if not getattr(sync, "phase_anchor_icao", None) and not no_anchor_reason:
                no_anchor_reason = "no_anchor_selected"
        else:
            no_anchor_reason = (
                ((alignment_status or {}).get("reason"))
                or "awaiting_refined_multi_sync"
            )

        return {
            "active_source": sync_source,
            "active_mode": active_mode,
            "active_label": active_label,
            "dominant_period_s": getattr(sync, "dominant_period_s", None) if sync is not None else None,
            "active_family_prior_s": getattr(sync, "active_family_prior_s", None) if sync is not None else None,
            "active_family_prior_source": getattr(sync, "active_family_prior_source", None) if sync is not None else None,
            "dominant_prior_active": bool(getattr(sync, "dominant_prior_active", False)) if sync is not None else False,
            "recovery_mode_active": bool(
                getattr(sync, "recovery_mode_active", getattr(sync, "period_reacquire_active", False))
            ) if sync is not None else False,
            "recovery_trigger_reasons": (
                list(getattr(sync, "recovery_trigger_reasons", []) or [])
                if sync is not None else []
            ),
            "compact_gating_bypassed": bool(getattr(sync, "compact_gating_bypassed", False)) if sync is not None else False,
            "recovery_relaxed_admitted_observations": int(
                getattr(sync, "recovery_relaxed_admitted_observations", 0) or 0
            ) if sync is not None else 0,
            "active_authority_mode": getattr(sync, "active_authority_mode", None) if sync is not None else None,
            "authority_switch_count": int(getattr(sync, "authority_switch_count", 0) or 0) if sync is not None else 0,
            "last_authority_switch_ts": getattr(sync, "last_authority_switch_ts", None) if sync is not None else None,
            "last_authority_switch_reason": getattr(sync, "last_authority_switch_reason", None) if sync is not None else None,
            "authority_enter_streak": int(getattr(sync, "authority_enter_streak", 0) or 0) if sync is not None else 0,
            "authority_exit_streak": int(getattr(sync, "authority_exit_streak", 0) or 0) if sync is not None else 0,
            "anchor_switch_count": int(getattr(sync, "anchor_switch_count", 0) or 0) if sync is not None else 0,
            "last_anchor_switch_ts": getattr(sync, "last_anchor_switch_ts", None) if sync is not None else None,
            "last_anchor_switch_reason": getattr(sync, "last_anchor_switch_reason", None) if sync is not None else None,
            "anchor_hold_updates": int(getattr(sync, "anchor_hold_updates", 0) or 0) if sync is not None else 0,
            "compact": {
                "active": not refined_active,
                "period_s": getattr(sync, "compact_period_s", None) if sync is not None else None,
                "period_delta_to_dominant_s": (
                    getattr(sync, "compact_period_delta_to_dominant_s", None)
                    if sync is not None else None
                ),
                "period_delta_to_dominant_ppm": (
                    getattr(sync, "compact_period_delta_to_dominant_ppm", None)
                    if sync is not None else None
                ),
                "unreliable": bool(getattr(sync, "compact_sync_unreliable", False)) if sync is not None else False,
                "reference_icao": compact_debug.get("current_reference_icao"),
                "last_reference_icao": compact_debug.get("last_reference_icao"),
                "reference_changed_recently": bool(compact_debug.get("reference_changed_recently")),
                "reference_change_count": int(compact_debug.get("reference_change_count") or 0),
                "last_reference_change_ts": compact_debug.get("last_reference_change_ts"),
                "phase_epoch_us": compact_debug.get("current_phase_epoch_us"),
                "last_phase_epoch_us": compact_debug.get("last_phase_epoch_us"),
                "phase_epoch_changed_recently": bool(compact_debug.get("phase_epoch_changed_recently")),
                "sync_reset_count": int(compact_debug.get("sync_reset_count") or 0),
                "last_sync_reset_ts": compact_debug.get("last_sync_reset_ts"),
                "last_sync_reset_reason": compact_debug.get("last_sync_reset_reason"),
                "holdover": compact_debug.get("holdover"),
                "last_holdover_transition": compact_debug.get("last_holdover_transition"),
                "last_holdover_transition_ts": compact_debug.get("last_holdover_transition_ts"),
            },
            "refined": {
                "present": refined_present,
                "active": refined_active,
                "usable": refined_usable,
                "period_s": getattr(sync, "period_s", None) if refined_active else None,
                "period_delta_to_dominant_s": (
                    getattr(sync, "dominant_period_delta_s", None) if refined_active else None
                ),
                "period_delta_to_dominant_ppm": (
                    getattr(sync, "dominant_period_delta_ppm", None) if refined_active else None
                ),
                "active_authority_mode": getattr(sync, "active_authority_mode", None) if refined_active else None,
                "anchor_icao": getattr(sync, "phase_anchor_icao", None) if refined_active else None,
                "anchor_candidate_count": int(getattr(sync, "phase_anchor_candidate_count", 0) or 0) if refined_active else 0,
                "fit_total_observations": int(getattr(sync, "fit_total_observations", 0) or 0) if refined_active else 0,
                "fit_eligible_observations": int(getattr(sync, "fit_eligible_observations", 0) or 0) if refined_active else 0,
                "fit_rejected_observations": int(getattr(sync, "fit_rejected_observations", 0) or 0) if refined_active else 0,
                "fit_contributing_icao_count": int(getattr(sync, "fit_contributing_icao_count", 0) or 0) if refined_active else 0,
                "fit_reject_reasons": (
                    dict(getattr(sync, "fit_reject_reasons", {}) or {})
                    if refined_active else {}
                ),
                "no_anchor_reason": no_anchor_reason,
                "admission": go_admission or None,
            },
        }

    def get_burst_sync_timeline(self, iid: int, window_s: float = 60.0) -> dict:
        """Return burst-centre sync observations with residuals for verification plotting.

        Each entry represents one burst-centre observation compared against the
        current sync model. Timeline payloads intentionally include both
        sync-update-eligible and non-eligible observations so visual diagnostics
        are not restricted to the narrow sync-driving subset.

        Returns a dict with:
          - "observations": list of dicts (one per burst, sorted oldest-first)
          - "sync_state": current LiveSyncState fields (or None)
          - "window_s": window actually used
        """
        with self._lock:
            sync = self._live_sync_states.get(iid)
            model = self._models.get(iid)
            timeline_obs_buf = self._live_burst_timeline_obs.get(iid)
            aligned_obs_buf = self._live_aligned_burst_obs.get(iid)
            go_evidence = [entry for entry in self._go_evidence_events if int(entry.get("iid", -1)) == iid]
            go_frame_revision = self._go_sweep_frames_revision.get(iid, 0)
            has_go_evidence = bool(go_evidence)
            timeline_obs_snapshot = [] if has_go_evidence else (list(timeline_obs_buf) if timeline_obs_buf else [])
            aligned_obs_snapshot = list(aligned_obs_buf) if aligned_obs_buf else []
            obs_buf = timeline_obs_buf
            if obs_buf is None:
                obs_buf = aligned_obs_buf
            obs_snapshot = [] if has_go_evidence else (list(obs_buf) if obs_buf else [])
            waveform_bins = list(self._live_waveform_bins.get(iid) or [])
            icao_quality = dict(self._live_icao_sync_quality.get(iid) or {})
            update_history = list(self._live_period_update_history.get(iid) or [])
            slope_history = list(self._live_slope_history.get(iid) or [])
            period_history = list(self._live_period_history.get(iid) or [])
            latest_arrival_us_for_iid = self._iid_latest_arrival_us.get(iid)
            go_admission = dict(self._go_multi_sync_admission_by_iid.get(iid) or {})
            _df11_cutoff_us = (latest_arrival_us_for_iid or 0.0) - window_s * 1_000_000.0
            # Take a fast deque snapshot under lock; filtering happens outside so the
            # decoder thread is not blocked while we scan 30k+ elements at Python speed.
            _iid_events_copy = list(self._iid_events)

        if has_go_evidence:
            timeline_obs_snapshot = self._go_burst_sync_timeline_snapshot(iid, window_s=window_s)
            obs_snapshot = timeline_obs_snapshot or aligned_obs_snapshot
        elif _sync_source_is_go_compact(sync) and not obs_snapshot:
            timeline_obs_snapshot = self._go_sweep_frame_sync_timeline_snapshot(iid, window_s=window_s)
            if timeline_obs_snapshot:
                obs_snapshot = timeline_obs_snapshot

        retention_diagnostics = self._build_live_sync_retention_diagnostics(
            iid,
            aligned_snapshot=aligned_obs_snapshot,
            timeline_snapshot=timeline_obs_snapshot,
        )
        radar_pos = _get_authoritative_radar_position(model) if model is not None else {"lat": None, "lon": None, "source": "none"}
        alignment_status = None
        if _sync_source_is_go_compact(sync) or has_go_evidence or go_admission:
            mode = "bootstrap_go_sync" if getattr(sync, "source", None) == "sweep_frame_go" else "refined_go_sync"
            reason = None
            detail = None
            if timeline_obs_snapshot:
                if has_go_evidence:
                    reason = "go_evidence_projected"
                    detail = "Using Go-retained burst evidence for compact alignment rows."
                else:
                    reason = "go_sweep_frames_projected"
                    detail = "Using retained Go sweep frames for compact alignment rows after raw burst evidence aged out."
            elif not has_go_evidence and go_frame_revision <= 0:
                reason = "no_go_evidence_retained"
                detail = "No retained Go burst evidence or sweep-frame history is available for this IID yet."
            elif radar_pos.get("lat") is None or radar_pos.get("lon") is None:
                reason = "radar_position_unavailable"
                detail = "Go frame/evidence history exists, but radar position is unavailable so alignment rows cannot be projected yet."
            elif sync is None:
                reason = "sync_state_unavailable"
                detail = "Go evidence is present, but no live sync state is available yet."
            elif getattr(sync, "source", None) == "sweep_frame_go":
                reason = "awaiting_refined_multi_sync"
                detail = "Bootstrap Go frame sync is active; refined multi-aircraft sync has not been admitted yet."
            else:
                reason = "go_alignment_unavailable"
                detail = "Go sync state is present, but compact alignment rows could not be rebuilt from retained evidence or sweep frames."
            alignment_status = {
                "mode": mode,
                "reason": reason,
                "detail": detail,
                "go_evidence_count": len(go_evidence),
                "projected_observation_count": len(timeline_obs_snapshot),
                "go_sweep_frame_revision": go_frame_revision,
                "sync_source": getattr(sync, "source", None) if sync is not None else None,
                "period_authoritative_source": getattr(sync, "period_authoritative_source", None) if sync is not None else None,
                "radar_position_source": radar_pos.get("source"),
                "multi_sync_admission": go_admission or None,
            }
        sync_mode_diagnostics = self._build_sync_mode_diagnostics(iid, sync, alignment_status)

        # Filter outside the lock — safe since we operate on an immutable snapshot.
        iid_events_for_df11 = [
            _ev for _ev in _iid_events_copy
            if _ev[0] >= _df11_cutoff_us and _ev[1] == iid
        ]

        if not obs_snapshot or sync is None:
            return {
                "observations": [],
                "sync_state": _live_sync_state_to_dict(sync) if sync else None,
                "window_s": window_s,
                "waveform_bins": _serialise_waveform_bins(waveform_bins),
                "per_icao_quality": [],
                "period_update_history": update_history,
                "slope_history": slope_history,
                "period_history": period_history,
                "predictor_consistency": getattr(sync, "predictor_consistency", None) if sync else None,
                "phase_anchor_candidates": getattr(sync, "phase_anchor_candidates", []) if sync else [],
                # No sync state → no authoritative residuals possible.
                "df11_residual_observations": [],
                "chart_overlay_consistent": False,
                "retention_diagnostics": retention_diagnostics,
                "alignment_status": alignment_status,
                "sync_mode_diagnostics": sync_mode_diagnostics,
            }

        if not _sync_source_has_rich_python_diagnostics(sync):
            entries = self._build_compact_burst_sync_timeline_entries(
                sync=sync,
                obs_snapshot=obs_snapshot,
                window_s=window_s,
            )
            motion_applied_entries = [e for e in entries if e.get("motion_comp_applied")]
            motion_improvements = [
                e.get("motion_comp_improvement_deg")
                for e in motion_applied_entries
                if e.get("motion_comp_improvement_deg") is not None
            ]

            if self._aircraft_state is not None:
                try:
                    for _pos in self._aircraft_state.get_positions_snapshot():
                        self._adsb_tracker.update(
                            _pos["icao"],
                            _pos["lat"],
                            _pos["lon"],
                            _pos.get("gs"),
                            _pos.get("track"),
                            ts=_pos.get("last_pos_ts") or time.time(),
                        )
                except Exception:
                    pass

            df11_residual_observations = self._build_df11_residual_observations(
                sync=sync,
                waveform_bins=waveform_bins,
                iid_events=iid_events_for_df11,
                latest_arrival_us=latest_arrival_us_for_iid,
            )
            if not df11_residual_observations and entries:
                residual_source = "go_evidence_compact" if has_go_evidence else "go_sweep_frame_compact"
                df11_residual_observations = self._build_compact_residual_observations_from_entries(
                    entries,
                    source=residual_source,
                )

            return {
                "observations": entries,
                "sync_state": _live_sync_state_to_dict(sync),
                "window_s": window_s,
                "waveform_bins": _serialise_waveform_bins(waveform_bins),
                "per_icao_quality": [],
                "period_update_history": [],
                "slope_history": [],
                "period_history": [],
                "predictor_consistency": getattr(sync, "predictor_consistency", None),
                "phase_anchor_candidates": getattr(sync, "phase_anchor_candidates", []),
                "motion_comp_summary": {
                    "phase_enabled": bool(getattr(sync, "motion_comp_phase_enabled", False)),
                    "fit_enabled": bool(getattr(sync, "motion_comp_fit_enabled", False)),
                    "applied_count": len(motion_applied_entries),
                    "blocked_count": sum(1 for e in entries if e.get("motion_comp_enabled") and not e.get("motion_comp_applied")),
                    "mean_motion_comp_dt_us": (
                        sum(e.get("motion_comp_dt_us") or 0.0 for e in motion_applied_entries) / len(motion_applied_entries)
                        if motion_applied_entries else 0.0
                    ),
                    "mean_residual_improvement_deg": (
                        sum(motion_improvements) / len(motion_improvements)
                        if motion_improvements else None
                    ),
                },
                "df11_residual_observations": df11_residual_observations,
                "chart_overlay_consistent": True,
                "retention_diagnostics": retention_diagnostics,
                "diagnostics_mode": "refined_go_sync" if getattr(sync, "source", None) == "go_multi_aircraft_burst" else "compact_go_sync",
                "alignment_status": alignment_status,
                "sync_mode_diagnostics": sync_mode_diagnostics,
            }

        now_ts = time.time()
        cutoff_ts = now_ts - window_s
        candidate_by_icao = {
            row.get("icao"): row
            for row in getattr(sync, "phase_anchor_candidates", []) or []
            if row.get("icao")
        }

        entries = []
        for obs in obs_snapshot:
            if obs.ts < cutoff_ts:
                continue
            # Authoritative predictor: same path used by fitting and the live localiser.
            uncorrected_prediction = predict_sync_observation(
                sync,
                obs.burst_centroid_us,
                range_nm=getattr(obs, "range_nm", None),
                waveform_bins=None,
                apply_propagation=False,
                apply_waveform=False,
                apply_motion=False,
            )
            raw_prediction = predict_sync_observation(
                sync,
                obs.burst_centroid_us,
                range_nm=getattr(obs, "range_nm", None),
                waveform_bins=None,
                apply_waveform=False,
                apply_motion=False,
            )
            without_motion_prediction = predict_sync_observation(
                sync,
                obs.burst_centroid_us,
                range_nm=getattr(obs, "range_nm", None),
                waveform_bins=waveform_bins,
                apply_motion=False,
            )
            corrected_prediction = predict_sync_observation(
                sync,
                obs.burst_centroid_us,
                range_nm=getattr(obs, "range_nm", None),
                waveform_bins=waveform_bins,
                bearing_rate_deg_s=getattr(obs, "bearing_rate_deg_s", None),
                motion_comp_dt_us=getattr(obs, "motion_comp_dt_us", None),
                motion_comp_block_reason=getattr(obs, "motion_comp_block_reason", None),
            )
            residual_raw = (
                obs.bearing_deg - uncorrected_prediction.predicted_bearing_deg + 540.0
            ) % 360.0 - 180.0
            residual_after_prop = (
                obs.bearing_deg - raw_prediction.predicted_bearing_deg + 540.0
            ) % 360.0 - 180.0
            residual_corr = (
                obs.bearing_deg - corrected_prediction.predicted_bearing_deg + 540.0
            ) % 360.0 - 180.0
            residual_without_motion = (
                obs.bearing_deg - without_motion_prediction.predicted_bearing_deg + 540.0
            ) % 360.0 - 180.0
            implied_phase_offset = (
                obs.bearing_deg
                + corrected_prediction.waveform_correction_deg
                - corrected_prediction.phase_in_rot_deg
            ) % 360.0
            anchor_relative_error = _circular_delta_deg(
                implied_phase_offset,
                getattr(sync, "phase_anchor_offset_smoothed_deg", None),
            )
            candidate = candidate_by_icao.get(obs.icao) or {}
            candidate_reasons = candidate.get("reject_reasons") or []
            abs_r = abs(residual_corr)
            classification = self._classify_sync_residual(abs_r)
            weight = self._score_sync_burst_observation(obs)
            q_entry = icao_quality.get(obs.icao)
            fit_reject_reason = None
            if not getattr(obs, "sync_update_eligible", True):
                fit_reject_reason = "not_sync_update_eligible"
            elif abs_r >= 150.0:
                fit_reject_reason = "near_wrap_residual"
            elif abs_r > 35.0:
                fit_reject_reason = "residual_gate"
            elif obs.pos_age_s > 8.0:
                fit_reject_reason = "stale_position"
            elif _icao_quality_reject_reason(q_entry) is not None:
                fit_reject_reason = _icao_quality_reject_reason(q_entry)
            elif classification == "rejected" or weight <= 0:
                fit_reject_reason = "zero_weight"
            fit_eligible = fit_reject_reason is None
            entries.append({
                "beam_center_us": obs.burst_centroid_us,
                "wall_ts": obs.ts,
                "icao": obs.icao,
                "bearing_deg": obs.bearing_deg,
                "predicted_deg": corrected_prediction.predicted_bearing_deg,
                "predicted_raw_deg": raw_prediction.predicted_bearing_raw_deg,
                "predicted_after_prop_deg": raw_prediction.predicted_bearing_deg,
                "pred_without_motion_deg": without_motion_prediction.predicted_bearing_deg,
                "pred_with_motion_deg": corrected_prediction.predicted_bearing_deg,
                "predicted_corrected_deg": corrected_prediction.predicted_bearing_deg,
                "residual_deg": residual_corr,
                # Separate raw vs corrected residual so the UI can show the
                # effect of the waveform correction directly.
                "residual_raw_deg": residual_raw,
                "residual_after_prop_deg": residual_after_prop,
                "residual_without_motion_deg": residual_without_motion,
                "residual_with_motion_deg": residual_corr,
                "residual_after_waveform_deg": residual_corr,
                "residual_corrected_deg": residual_corr,
                "motion_comp_improvement_deg": abs(residual_without_motion) - abs(residual_corr),
                "residual_for_period_fit_deg": residual_corr if fit_eligible else None,
                "implied_phase_offset_deg": implied_phase_offset,
                "anchor_relative_phase_error_deg": anchor_relative_error,
                "phase_anchor_contributor": obs.icao == getattr(sync, "phase_anchor_icao", None),
                "phase_anchor_reject_reason": ",".join(candidate_reasons) if candidate_reasons else None,
                "phase_in_rot_deg": corrected_prediction.phase_in_rot_deg,
                "raw_arrival_us": getattr(obs, "raw_arrival_us", obs.burst_centroid_us),
                "prop_corrected_beast_us": corrected_prediction.prop_corrected_beast_us,
                "effective_arrival_us": corrected_prediction.effective_arrival_us,
                "motion_corrected_beast_us": corrected_prediction.motion_corrected_beast_us,
                "prop_delay_us": corrected_prediction.propagation_correction_us,
                "bearing_rate_deg_s": corrected_prediction.bearing_rate_deg_s,
                "motion_comp_dt_us": corrected_prediction.motion_comp_dt_us,
                "motion_comp_enabled": corrected_prediction.motion_comp_enabled,
                "motion_comp_applied": corrected_prediction.motion_comp_applied,
                "motion_comp_block_reason": corrected_prediction.motion_comp_block_reason,
                "waveform_correction_deg": corrected_prediction.waveform_correction_deg,
                "waveform_applied": corrected_prediction.waveform_applied,
                "prediction_path": corrected_prediction.predictor_version,
                "weight": weight,
                "classification": classification,
                "fit_eligible": fit_eligible,
                "fit_reject_reason": fit_reject_reason,
                "n_replies": obs.n_replies,
                "signal_dbfs": obs.signal_dbfs,
                "pos_age_s": obs.pos_age_s,
                "range_nm": obs.range_nm,
                "sync_update_eligible": bool(getattr(obs, "sync_update_eligible", True)),
                "burst_center_method": getattr(obs, "burst_center_method", "centroid"),
                "burst_center_simple_us": getattr(obs, "burst_center_simple_us", None),
                "burst_center_weighted_us": getattr(obs, "burst_center_weighted_us", None),
                "burst_center_delta_us": getattr(obs, "burst_center_delta_us", None),
            })

        # Sort chronologically by burst centre timestamp
        entries.sort(key=lambda e: e["beam_center_us"])
        motion_applied_entries = [e for e in entries if e.get("motion_comp_applied")]
        motion_improvements = [
            e.get("motion_comp_improvement_deg")
            for e in motion_applied_entries
            if e.get("motion_comp_improvement_deg") is not None
        ]

        waveform_payload = _serialise_waveform_bins(waveform_bins)

        # Serialise per-ICAO quality memory.
        quality_payload = [
            {
                "icao": icao,
                "residual_median_deg": entry.residual_median_deg,
                "residual_mad_deg": entry.residual_mad_deg,
                "n": entry.n_recent,
                "last_ts": entry.last_ts,
            }
            for icao, entry in icao_quality.items()
        ]
        quality_payload.sort(key=lambda x: -x["n"])

        # Refresh the position tracker with current aircraft positions before
        # computing DF11 residuals.  _adsb_tracker is normally refreshed only
        # inside update_rotation_models() (every ~30s), so without this step
        # get_position_at() would return None for all events and produce no dots.
        if self._aircraft_state is not None:
            try:
                for _pos in self._aircraft_state.get_positions_snapshot():
                    self._adsb_tracker.update(
                        _pos["icao"],
                        _pos["lat"],
                        _pos["lon"],
                        _pos.get("gs"),
                        _pos.get("track"),
                        ts=_pos.get("last_pos_ts") or time.time(),
                    )
            except Exception:
                pass

        # Build backend DF11 residual observations using the same authoritative
        # predictor (predict_sync_observation) and the same sync snapshot used
        # for burst observations above.  Both chart layers are therefore
        # timing-consistent: identical timestamp basis, predictor path,
        # propagation correction, waveform correction, and residual wrapping.
        df11_residual_observations = self._build_df11_residual_observations(
            sync=sync,
            waveform_bins=waveform_bins,
            iid_events=iid_events_for_df11,
            latest_arrival_us=latest_arrival_us_for_iid,
        )

        return {
            "observations": entries,
            "sync_state": _live_sync_state_to_dict(sync),
            "window_s": window_s,
            "waveform_bins": waveform_payload,
            "per_icao_quality": quality_payload,
            "period_update_history": update_history,
            "slope_history": slope_history,
            "period_history": period_history,
            "predictor_consistency": getattr(sync, "predictor_consistency", None),
            "phase_anchor_candidates": getattr(sync, "phase_anchor_candidates", []),
            "motion_comp_summary": {
                "phase_enabled": bool(getattr(sync, "motion_comp_phase_enabled", False)),
                "fit_enabled": bool(getattr(sync, "motion_comp_fit_enabled", False)),
                "applied_count": len(motion_applied_entries),
                "blocked_count": sum(1 for e in entries if e.get("motion_comp_enabled") and not e.get("motion_comp_applied")),
                "mean_motion_comp_dt_us": (
                    sum(e.get("motion_comp_dt_us") or 0.0 for e in motion_applied_entries) / len(motion_applied_entries)
                    if motion_applied_entries else 0.0
                ),
                "mean_residual_improvement_deg": (
                    sum(motion_improvements) / len(motion_improvements)
                    if motion_improvements else None
                ),
            },
            # Both burst_observations and df11_residual_observations were derived from
            # the same sync snapshot and the same predict_sync_observation() call.
            # They are directly comparable on the residual chart.
            "df11_residual_observations": df11_residual_observations,
            "chart_overlay_consistent": True,
            "retention_diagnostics": retention_diagnostics,
            "alignment_status": alignment_status,
            "sync_mode_diagnostics": sync_mode_diagnostics,
        }

    def get_live_sync_snapshot(self, iid: int, window_s: float = 90.0, debug_limit: int = 120) -> dict:
        """Return the shared compact sync snapshot used by the pushed Radar UI feed.

        The snapshot combines the fast-changing sync payloads that previously
        required separate frontend polls.  A signature cache prevents the
        websocket loop, HTTP fallback, and reconnects from rebuilding the
        lightweight live sync state when no relevant input has changed.
        """
        with self._lock:
            sync = self._live_sync_states.get(iid)
            model = self._models.get(iid)
            go_evidence = [entry for entry in self._go_evidence_events if int(entry.get("iid", -1)) == iid]
            go_admission = dict(self._go_multi_sync_admission_by_iid.get(iid) or {})
            compact_debug = dict(self._compact_sync_debug_by_iid.get(iid) or {})
            go_frame_revision = self._go_sweep_frames_revision.get(iid, 0)
            obs_buf = None if go_evidence else (self._live_burst_timeline_obs.get(iid) or self._live_aligned_burst_obs.get(iid))
            obs_len = len(go_evidence) if go_evidence else (len(obs_buf) if obs_buf else 0)
            last_obs_us = None
            last_obs_ts = None
            if go_evidence:
                last_obs = go_evidence[-1]
                last_obs_us = last_obs.get("arrival_us")
                last_obs_ts = last_obs.get("wall_ts")
            elif obs_buf:
                last_obs = obs_buf[-1]
                last_obs_us = getattr(last_obs, "burst_centroid_us", None)
                last_obs_ts = getattr(last_obs, "ts", None)
            signature = (
                float(window_s),
                int(debug_limit),
                self._iid_latest_arrival_us.get(iid),
                getattr(sync, "last_sync_update_ts", None),
                getattr(sync, "period_s", None),
                getattr(sync, "period_correction_status", None),
                getattr(model, "last_updated", None),
                getattr(model, "period_s", None),
                getattr(model, "status", None),
                obs_len,
                last_obs_us,
                last_obs_ts,
                go_admission.get("last_reason"),
                go_admission.get("last_ts"),
                tuple(sorted((go_admission.get("counts") or {}).items())),
                compact_debug.get("current_reference_icao"),
                compact_debug.get("last_reference_icao"),
                compact_debug.get("last_reference_change_ts"),
                compact_debug.get("current_phase_epoch_us"),
                compact_debug.get("last_phase_epoch_change_ts"),
                compact_debug.get("last_sync_reset_ts"),
                compact_debug.get("last_holdover_transition_ts"),
                go_frame_revision,
            )
            cached = self._live_sync_snapshot_cache.get(iid)
            if cached is not None and cached[0] == signature:
                self._live_sync_snapshot_last_cache_hit[iid] = True
                return cached[1]
            sequence = self._live_sync_snapshot_seq.get(iid, 0) + 1
            self._live_sync_snapshot_seq[iid] = sequence
            self._live_sync_snapshot_last_cache_hit[iid] = False

        burst_timeline = self.get_burst_sync_timeline(iid, window_s=window_s)
        snapshot = {
            "type": "radar_sync",
            "iid": iid,
            "sequence": sequence,
            "server_ts": time.time(),
            "window_s": window_s,
            "sync_state": burst_timeline.get("sync_state"),
            "observations": burst_timeline.get("observations", []),
            # Backend-computed DF11 residual observations for the burst-sync chart overlay.
            # Both "observations" (burst) and "df11_residual_observations" are derived from
            # the same authoritative sync snapshot and predict_sync_observation() path.
            # chart_overlay_consistent=True confirms they are directly comparable.
            "df11_residual_observations": burst_timeline.get("df11_residual_observations", []),
            "chart_overlay_consistent": burst_timeline.get("chart_overlay_consistent", False),
            "waveform_bins": burst_timeline.get("waveform_bins", []),
            "phase_anchor_candidates": burst_timeline.get("phase_anchor_candidates", []),
            "motion_comp_summary": burst_timeline.get("motion_comp_summary"),
            "retention_diagnostics": burst_timeline.get("retention_diagnostics"),
            "alignment_status": burst_timeline.get("alignment_status"),
            "sync_mode_diagnostics": burst_timeline.get("sync_mode_diagnostics"),
            "transport": {
                "source": "shared_snapshot",
                "cached": False,
            },
        }
        with self._lock:
            self._live_sync_snapshot_cache[iid] = (signature, snapshot)
        return snapshot

    def get_live_sync_snapshot_last_cache_hit(self, iid: int) -> bool:
        with self._lock:
            return bool(self._live_sync_snapshot_last_cache_hit.get(iid, False))

    def get_sync_debug_payload(self, iid: int, window_s: float = 60.0, limit: int = 80) -> dict:
        """Return per-observation sync consistency diagnostics for one IID.

        Operational sync math in this payload is Beast-relative.  The
        authoritative prediction uses the burst-centre Beast timestamp and lets
        predict_sync_observation() derive `effective_beast_us` after propagation
        correction when enabled.  Wall-clock conversion is computed only as an
        explicit diagnostic comparison and is never fed back into sync state.
        """
        with self._lock:
            sync = self._live_sync_states.get(iid)
            timeline_obs_buf = self._live_burst_timeline_obs.get(iid)
            aligned_obs_buf = self._live_aligned_burst_obs.get(iid)
            has_go_evidence = any(int(entry.get("iid", -1)) == iid for entry in self._go_evidence_events)
            timeline_obs_snapshot = [] if has_go_evidence else (list(timeline_obs_buf) if timeline_obs_buf else [])
            aligned_obs_snapshot = list(aligned_obs_buf) if aligned_obs_buf else []
            obs_buf = timeline_obs_buf
            if obs_buf is None:
                obs_buf = aligned_obs_buf
            obs_snapshot = [] if has_go_evidence else (list(obs_buf) if obs_buf else [])
            waveform_bins = list(self._live_waveform_bins.get(iid) or [])
            icao_quality = dict(self._live_icao_sync_quality.get(iid) or {})
            latest_arrival_beast_us = self._iid_latest_arrival_us.get(iid)

        if has_go_evidence:
            timeline_obs_snapshot = self._go_burst_sync_timeline_snapshot(iid, window_s=window_s)
            obs_snapshot = timeline_obs_snapshot or aligned_obs_snapshot

        retention_diagnostics = self._build_live_sync_retention_diagnostics(
            iid,
            aligned_snapshot=aligned_obs_snapshot,
            timeline_snapshot=timeline_obs_snapshot,
        )

        if sync is None:
            burst_timeline = self.get_burst_sync_timeline(iid, window_s=window_s)
            return {
                "iid": iid,
                "available": False,
                "reason": "sync_state_unavailable",
                "observations": [],
                "summary": {
                    "iid": iid,
                    "wall_clock_used_operationally": False,
                    "retention_diagnostics": retention_diagnostics,
                    "alignment_status": burst_timeline.get("alignment_status"),
                },
                "retention_diagnostics": retention_diagnostics,
                "alignment_status": burst_timeline.get("alignment_status"),
            }

        if not _sync_source_has_rich_python_diagnostics(sync):
            burst_timeline = self.get_burst_sync_timeline(iid, window_s=window_s)
            payload = self._build_compact_sync_debug_payload(
                iid=iid,
                sync=sync,
                burst_timeline=burst_timeline,
                limit=limit,
            )
            return payload

        now_ts = time.time()
        cutoff_ts = now_ts - window_s
        recent_obs = [obs for obs in obs_snapshot if obs.ts >= cutoff_ts]
        recent_obs.sort(key=lambda obs: obs.burst_centroid_us)
        if limit > 0:
            recent_obs = recent_obs[-limit:]
        candidate_by_icao = {
            row.get("icao"): row
            for row in getattr(sync, "phase_anchor_candidates", []) or []
            if row.get("icao")
        }

        def _predict_from_beast_input(input_beast_us: float | None) -> SyncPrediction | None:
            if input_beast_us is None:
                return None
            return predict_sync_observation(
                sync,
                input_beast_us,
                range_nm=None,
                waveform_bins=waveform_bins,
                apply_propagation=False,
            )

        def _mean_abs(values: list[float | None]) -> float | None:
            clean = [abs(v) for v in values if v is not None]
            return (sum(clean) / len(clean)) if clean else None

        def _max_abs(values: list[float | None]) -> float | None:
            clean = [abs(v) for v in values if v is not None]
            return max(clean) if clean else None

        def _median_abs(values: list[float | None]) -> float | None:
            clean = sorted(abs(v) for v in values if v is not None)
            if not clean:
                return None
            mid = len(clean) // 2
            if len(clean) % 2:
                return clean[mid]
            return (clean[mid - 1] + clean[mid]) / 2.0

        observations: list[dict] = []
        localiser_deltas: list[float | None] = []
        position_deltas: list[float | None] = []
        burstsync_deltas: list[float | None] = []
        raw_effective_deltas: list[float | None] = []
        wall_effective_deltas: list[float | None] = []
        roundtrip_errors: list[float | None] = []
        residuals_without_motion: list[float | None] = []
        residuals_with_motion: list[float | None] = []
        fit_residuals_without_motion: list[float | None] = []
        fit_residuals_with_motion: list[float | None] = []
        high_rate_residuals_without_motion: list[float | None] = []
        high_rate_residuals_with_motion: list[float | None] = []
        motion_improvements: list[float | None] = []

        localiser_predictor = None
        try:
            from .aircraft_localiser import predict_localiser_live_path_bearing
            localiser_predictor = predict_localiser_live_path_bearing
        except Exception:
            localiser_predictor = None

        for obs in recent_obs:
            raw_arrival_beast_us = float(getattr(obs, "raw_arrival_us", obs.burst_centroid_us) or obs.burst_centroid_us)
            burst_center_beast_us = float(obs.burst_centroid_us)
            range_nm = getattr(obs, "range_nm", None)
            authoritative = predict_sync_observation(
                sync,
                burst_center_beast_us,
                range_nm=range_nm,
                waveform_bins=waveform_bins,
                bearing_rate_deg_s=getattr(obs, "bearing_rate_deg_s", None),
                motion_comp_dt_us=getattr(obs, "motion_comp_dt_us", None),
                motion_comp_block_reason=getattr(obs, "motion_comp_block_reason", None),
            )
            without_motion_prediction = predict_sync_observation(
                sync,
                burst_center_beast_us,
                range_nm=range_nm,
                waveform_bins=waveform_bins,
                apply_motion=False,
            )
            effective_beast_us = authoritative.effective_arrival_us

            pred_using_raw = _predict_from_beast_input(raw_arrival_beast_us)
            pred_using_burst_center = _predict_from_beast_input(burst_center_beast_us)
            pred_using_effective = _predict_from_beast_input(effective_beast_us)

            wall_clock_beast_us = None
            if latest_arrival_beast_us is not None:
                wall_clock_beast_us = latest_arrival_beast_us - max(0.0, now_ts - obs.ts) * 1_000_000.0
            pred_using_wall = _predict_from_beast_input(wall_clock_beast_us)
            wall_roundtrip_error_us = (
                wall_clock_beast_us - burst_center_beast_us
                if wall_clock_beast_us is not None else None
            )

            if localiser_predictor is not None:
                pred_localiser_live_deg = localiser_predictor(
                    sync,
                    burst_center_beast_us,
                    range_nm=range_nm,
                    waveform_bins=waveform_bins,
                    bearing_rate_deg_s=getattr(obs, "bearing_rate_deg_s", None),
                    motion_comp_dt_us=getattr(obs, "motion_comp_dt_us", None),
                    motion_comp_block_reason=getattr(obs, "motion_comp_block_reason", None),
                )
            else:
                pred_localiser_live_deg = authoritative.predicted_bearing_deg

            # Backend-side mirrors of the frontend position-verification and
            # burst-sync paths.  Keeping these here makes path comparison depend
            # on one backend implementation, not a frontend reimplementation.
            pred_position_verification_deg = predict_sync_observation(
                sync,
                burst_center_beast_us,
                range_nm=range_nm,
                waveform_bins=waveform_bins,
                bearing_rate_deg_s=getattr(obs, "bearing_rate_deg_s", None),
                motion_comp_dt_us=getattr(obs, "motion_comp_dt_us", None),
                motion_comp_block_reason=getattr(obs, "motion_comp_block_reason", None),
            ).predicted_bearing_deg
            pred_burst_sync_deg = authoritative.predicted_bearing_deg

            pred_authoritative_deg = authoritative.predicted_bearing_deg
            resid_authoritative_deg = _circular_delta_deg(obs.bearing_deg, pred_authoritative_deg)
            candidate_timestamps = {
                "first_reply": getattr(obs, "burst_ts_first_reply_beast_us", None),
                "strongest_reply": getattr(obs, "burst_ts_strongest_reply_beast_us", None),
                "simple_centroid": getattr(obs, "burst_ts_simple_centroid_beast_us", None),
                "weighted_centroid": getattr(obs, "burst_ts_weighted_centroid_beast_us", None),
                "mid_strong_window": getattr(obs, "burst_ts_mid_strong_window_beast_us", None),
                "last_reply": getattr(obs, "burst_ts_last_reply_beast_us", None),
            }
            candidate_residuals: dict[str, float | None] = {}
            candidate_predictions: dict[str, float | None] = {}
            candidate_phases: dict[str, float | None] = {}
            for method_name, ts_beast_us in candidate_timestamps.items():
                if ts_beast_us is None:
                    candidate_residuals[method_name] = None
                    candidate_predictions[method_name] = None
                    candidate_phases[method_name] = None
                    continue
                candidate_prediction = predict_sync_observation(
                    sync,
                    float(ts_beast_us),
                    range_nm=range_nm,
                    waveform_bins=waveform_bins,
                    bearing_rate_deg_s=getattr(obs, "bearing_rate_deg_s", None),
                    motion_comp_dt_us=getattr(obs, "motion_comp_dt_us", None),
                    motion_comp_block_reason=getattr(obs, "motion_comp_block_reason", None),
                )
                candidate_predictions[method_name] = candidate_prediction.predicted_bearing_deg
                candidate_phases[method_name] = candidate_prediction.phase_in_rot_deg
                candidate_residuals[method_name] = _circular_delta_deg(
                    obs.bearing_deg,
                    candidate_prediction.predicted_bearing_deg,
                )
            resid_without_motion_deg = _circular_delta_deg(
                obs.bearing_deg, without_motion_prediction.predicted_bearing_deg,
            )
            resid_localiser_deg = _circular_delta_deg(obs.bearing_deg, pred_localiser_live_deg)
            resid_position_deg = _circular_delta_deg(obs.bearing_deg, pred_position_verification_deg)
            resid_burstsync_deg = _circular_delta_deg(obs.bearing_deg, pred_burst_sync_deg)

            delta_localiser = _circular_delta_deg(pred_localiser_live_deg, pred_authoritative_deg)
            delta_position = _circular_delta_deg(pred_position_verification_deg, pred_authoritative_deg)
            delta_burstsync = _circular_delta_deg(pred_burst_sync_deg, pred_authoritative_deg)
            delta_raw_effective = _circular_delta_deg(
                pred_using_raw.predicted_bearing_deg if pred_using_raw else None,
                pred_using_effective.predicted_bearing_deg if pred_using_effective else None,
            )
            delta_wall_effective = _circular_delta_deg(
                pred_using_wall.predicted_bearing_deg if pred_using_wall else None,
                pred_using_effective.predicted_bearing_deg if pred_using_effective else None,
            )

            abs_r = abs(resid_authoritative_deg or 0.0)
            classification = self._classify_sync_residual(abs_r)
            weight = self._score_sync_burst_observation(obs)
            q_entry = icao_quality.get(obs.icao)
            fit_reject_reason = None
            if not getattr(obs, "sync_update_eligible", True):
                fit_reject_reason = "not_sync_update_eligible"
            elif abs_r >= 150.0:
                fit_reject_reason = "near_wrap_residual"
            elif abs_r > 35.0:
                fit_reject_reason = "residual_gate"
            elif obs.pos_age_s > 8.0:
                fit_reject_reason = "stale_position"
            elif _icao_quality_reject_reason(q_entry) is not None:
                fit_reject_reason = _icao_quality_reject_reason(q_entry)
            elif classification == "rejected" or weight <= 0:
                fit_reject_reason = "zero_weight"

            period_us = sync.period_s * 1_000_000.0
            phase_from_period_only_deg = (
                ((effective_beast_us / period_us) * 360.0) % 360.0
                if period_us > 0 else None
            )
            phase_after_epoch_deg = authoritative.phase_in_rot_deg
            implied_phase_offset = (
                obs.bearing_deg
                + authoritative.waveform_correction_deg
                - authoritative.phase_in_rot_deg
            ) % 360.0
            anchor_relative_error = _circular_delta_deg(
                implied_phase_offset,
                getattr(sync, "phase_anchor_offset_smoothed_deg", None),
            )
            candidate = candidate_by_icao.get(obs.icao) or {}
            candidate_reasons = candidate.get("reject_reasons") or []

            observations.append({
                "iid": iid,
                "icao": obs.icao,
                "raw_arrival_beast_us": raw_arrival_beast_us,
                "burst_center_beast_us": burst_center_beast_us,
                "effective_beast_us": effective_beast_us,
                "prop_corrected_beast_us": authoritative.prop_corrected_beast_us,
                "motion_corrected_beast_us": authoritative.motion_corrected_beast_us,
                "prop_delay_us": authoritative.propagation_correction_us,
                "bearing_rate_deg_s": authoritative.bearing_rate_deg_s,
                "motion_comp_dt_us": authoritative.motion_comp_dt_us,
                "motion_comp_enabled": authoritative.motion_comp_enabled,
                "motion_comp_applied": authoritative.motion_comp_applied,
                "motion_comp_block_reason": authoritative.motion_comp_block_reason,
                "wall_ts": obs.ts,
                "range_nm": range_nm,
                "true_bearing_deg": obs.bearing_deg,
                "pred_authoritative_deg": pred_authoritative_deg,
                "pred_without_motion_deg": without_motion_prediction.predicted_bearing_deg,
                "pred_with_motion_deg": pred_authoritative_deg,
                "phase_authoritative_deg": authoritative.phase_in_rot_deg,
                "pred_localiser_live_deg": pred_localiser_live_deg,
                "pred_position_verification_deg": pred_position_verification_deg,
                "pred_burst_sync_deg": pred_burst_sync_deg,
                "pred_using_raw_beast_deg": pred_using_raw.predicted_bearing_deg if pred_using_raw else None,
                "pred_using_burst_center_deg": pred_using_burst_center.predicted_bearing_deg if pred_using_burst_center else None,
                "pred_using_effective_beast_deg": pred_using_effective.predicted_bearing_deg if pred_using_effective else None,
                "pred_using_wall_clock_deg": pred_using_wall.predicted_bearing_deg if pred_using_wall else None,
                "resid_authoritative_deg": resid_authoritative_deg,
                "resid_without_motion_deg": resid_without_motion_deg,
                "resid_with_motion_deg": resid_authoritative_deg,
                "resid_first_reply_deg": candidate_residuals.get("first_reply"),
                "resid_strongest_reply_deg": candidate_residuals.get("strongest_reply"),
                "resid_simple_centroid_deg": candidate_residuals.get("simple_centroid"),
                "resid_weighted_centroid_deg": candidate_residuals.get("weighted_centroid"),
                "resid_mid_strong_window_deg": candidate_residuals.get("mid_strong_window"),
                "resid_last_reply_deg": candidate_residuals.get("last_reply"),
                "resid_improvement_first_reply_deg": (
                    abs(resid_authoritative_deg) - abs(candidate_residuals["first_reply"])
                    if resid_authoritative_deg is not None and candidate_residuals.get("first_reply") is not None else None
                ),
                "resid_improvement_strongest_reply_deg": (
                    abs(resid_authoritative_deg) - abs(candidate_residuals["strongest_reply"])
                    if resid_authoritative_deg is not None and candidate_residuals.get("strongest_reply") is not None else None
                ),
                "resid_improvement_simple_centroid_deg": (
                    abs(resid_authoritative_deg) - abs(candidate_residuals["simple_centroid"])
                    if resid_authoritative_deg is not None and candidate_residuals.get("simple_centroid") is not None else None
                ),
                "resid_improvement_weighted_centroid_deg": (
                    abs(resid_authoritative_deg) - abs(candidate_residuals["weighted_centroid"])
                    if resid_authoritative_deg is not None and candidate_residuals.get("weighted_centroid") is not None else None
                ),
                "resid_improvement_mid_strong_window_deg": (
                    abs(resid_authoritative_deg) - abs(candidate_residuals["mid_strong_window"])
                    if resid_authoritative_deg is not None and candidate_residuals.get("mid_strong_window") is not None else None
                ),
                "resid_improvement_last_reply_deg": (
                    abs(resid_authoritative_deg) - abs(candidate_residuals["last_reply"])
                    if resid_authoritative_deg is not None and candidate_residuals.get("last_reply") is not None else None
                ),
                "pred_first_reply_deg": candidate_predictions.get("first_reply"),
                "pred_strongest_reply_deg": candidate_predictions.get("strongest_reply"),
                "pred_simple_centroid_deg": candidate_predictions.get("simple_centroid"),
                "pred_weighted_centroid_deg": candidate_predictions.get("weighted_centroid"),
                "pred_mid_strong_window_deg": candidate_predictions.get("mid_strong_window"),
                "pred_last_reply_deg": candidate_predictions.get("last_reply"),
                "phase_first_reply_deg": candidate_phases.get("first_reply"),
                "phase_strongest_reply_deg": candidate_phases.get("strongest_reply"),
                "phase_simple_centroid_deg": candidate_phases.get("simple_centroid"),
                "phase_weighted_centroid_deg": candidate_phases.get("weighted_centroid"),
                "phase_mid_strong_window_deg": candidate_phases.get("mid_strong_window"),
                "phase_last_reply_deg": candidate_phases.get("last_reply"),
                "motion_comp_improvement_deg": (
                    abs(resid_without_motion_deg) - abs(resid_authoritative_deg)
                    if resid_without_motion_deg is not None and resid_authoritative_deg is not None else None
                ),
                "resid_localiser_deg": resid_localiser_deg,
                "resid_position_verification_deg": resid_position_deg,
                "resid_burst_sync_deg": resid_burstsync_deg,
                "delta_localiser_vs_authoritative_deg": delta_localiser,
                "delta_position_vs_authoritative_deg": delta_position,
                "delta_burstsync_vs_authoritative_deg": delta_burstsync,
                "delta_raw_vs_effective_deg": delta_raw_effective,
                "delta_wall_vs_effective_deg": delta_wall_effective,
                "fit_eligible": fit_reject_reason is None,
                "fit_reject_reason": fit_reject_reason,
                "sync_update_eligible": bool(getattr(obs, "sync_update_eligible", True)),
                "phase_from_period_only_deg": phase_from_period_only_deg,
                "phase_after_epoch_deg": phase_after_epoch_deg,
                "implied_phase_offset_deg": implied_phase_offset,
                "anchor_relative_phase_error_deg": anchor_relative_error,
                "phase_anchor_contributor": obs.icao == getattr(sync, "phase_anchor_icao", None),
                "phase_anchor_reject_reason": ",".join(candidate_reasons) if candidate_reasons else None,
                "waveform_correction_deg": authoritative.waveform_correction_deg,
                "pred_after_waveform_deg": pred_authoritative_deg,
                "wall_to_beast_roundtrip_error_us": wall_roundtrip_error_us,
                "classification": classification,
                "weight": weight,
                "n_replies": obs.n_replies,
                "burst_reply_count": obs.n_replies,
                "pos_age_s": obs.pos_age_s,
                "position_age_ms": obs.pos_age_s * 1000.0 if obs.pos_age_s is not None else None,
                "position_interpolated": bool(getattr(obs, "position_interpolated", False)),
                "position_extrapolated": bool(getattr(obs, "position_extrapolated", False)),
                "position_source_age_ms": (
                    abs(getattr(obs, "position_source_age_s")) * 1000.0
                    if getattr(obs, "position_source_age_s", None) is not None else None
                ),
                "truth_position_ts_beast_us": getattr(obs, "truth_position_ts_beast_us", None),
                "signal_dbfs": obs.signal_dbfs,
                "signal_strength": obs.signal_dbfs,
                "peak_amplitude": getattr(obs, "peak_amplitude", None),
                "burst_width_us": getattr(obs, "burst_span_us", None),
                "burst_duration_us": getattr(obs, "burst_span_us", None),
                "burst_span_us": getattr(obs, "burst_span_us", None),
                "burst_center_method": getattr(obs, "burst_center_method", "centroid"),
                "burst_center_simple_us": getattr(obs, "burst_center_simple_us", None),
                "burst_center_weighted_us": getattr(obs, "burst_center_weighted_us", None),
                "burst_center_delta_us": getattr(obs, "burst_center_delta_us", None),
                "burst_ts_first_reply_beast_us": candidate_timestamps.get("first_reply"),
                "burst_ts_strongest_reply_beast_us": candidate_timestamps.get("strongest_reply"),
                "burst_ts_simple_centroid_beast_us": candidate_timestamps.get("simple_centroid"),
                "burst_ts_weighted_centroid_beast_us": candidate_timestamps.get("weighted_centroid"),
                "burst_ts_mid_strong_window_beast_us": candidate_timestamps.get("mid_strong_window"),
                "burst_ts_last_reply_beast_us": candidate_timestamps.get("last_reply"),
            })

            localiser_deltas.append(delta_localiser)
            position_deltas.append(delta_position)
            burstsync_deltas.append(delta_burstsync)
            raw_effective_deltas.append(delta_raw_effective)
            wall_effective_deltas.append(delta_wall_effective)
            roundtrip_errors.append(wall_roundtrip_error_us)
            residuals_without_motion.append(resid_without_motion_deg)
            residuals_with_motion.append(resid_authoritative_deg)
            improvement = (
                abs(resid_without_motion_deg) - abs(resid_authoritative_deg)
                if resid_without_motion_deg is not None and resid_authoritative_deg is not None else None
            )
            motion_improvements.append(improvement)
            if fit_reject_reason is None:
                fit_residuals_without_motion.append(resid_without_motion_deg)
                fit_residuals_with_motion.append(resid_authoritative_deg)
            br = getattr(obs, "bearing_rate_deg_s", None)
            if br is not None and abs(br) >= 0.2:
                high_rate_residuals_without_motion.append(resid_without_motion_deg)
                high_rate_residuals_with_motion.append(resid_authoritative_deg)

        # Diagnostic-only detrending: remove the current period-refinement
        # residual slope from the plotted residuals without changing solver
        # state, phase anchors, waveform learning, or period updates.
        fit_origin_candidates = [
            float(row["effective_beast_us"])
            for row in observations
            if row.get("fit_eligible") and row.get("effective_beast_us") is not None
        ]
        all_effective_candidates = [
            float(row["effective_beast_us"])
            for row in observations
            if row.get("effective_beast_us") is not None
        ]
        fit_time_origin_beast_us = (
            min(fit_origin_candidates)
            if fit_origin_candidates else (min(all_effective_candidates) if all_effective_candidates else None)
        )
        fit_slope_deg_per_s = getattr(sync, "residual_slope_deg_per_s", 0.0) or 0.0
        local_fit_rows = [
            row for row in observations
            if row.get("fit_eligible")
            and row.get("effective_beast_us") is not None
            and row.get("resid_authoritative_deg") is not None
            and (row.get("weight") or 0.0) > 0.0
        ]
        if fit_time_origin_beast_us is not None and len(local_fit_rows) >= 2:
            xs = [(float(row["effective_beast_us"]) - float(fit_time_origin_beast_us)) / 1_000_000.0 for row in local_fit_rows]
            ys = [float(row["resid_authoritative_deg"]) for row in local_fit_rows]
            ws = [float(row.get("weight") or 0.0) for row in local_fit_rows]
            _a_debug_fit, fit_slope_deg_per_s = _fit_weighted_slope(xs, ys, ws)
        period_us = sync.period_s * 1_000_000.0 if sync.period_s and sync.period_s > 0 else None
        for row in observations:
            effective_us = row.get("effective_beast_us")
            residual_raw_deg = row.get("resid_authoritative_deg")
            row["residual_raw_deg"] = residual_raw_deg
            row["residual_deg"] = residual_raw_deg
            row["fit_slope_deg_per_s"] = fit_slope_deg_per_s
            row["fit_time_origin_beast_us"] = fit_time_origin_beast_us
            row["phase_deg"] = row.get("phase_authoritative_deg")
            if effective_us is None or fit_time_origin_beast_us is None:
                row["time_offset_s"] = None
                row["detrend_component_deg"] = None
                row["residual_detrended_deg"] = residual_raw_deg
            else:
                time_offset_s = (float(effective_us) - float(fit_time_origin_beast_us)) / 1_000_000.0
                detrend_component_deg = fit_slope_deg_per_s * time_offset_s
                row["time_offset_s"] = time_offset_s
                row["detrend_component_deg"] = detrend_component_deg
                row["residual_detrended_deg"] = _circular_delta_deg(residual_raw_deg, detrend_component_deg)
            if period_us and effective_us is not None:
                cycle_index = int(_math.floor((float(effective_us) - sync.phase_epoch_us) / period_us))
                row["cycle_index"] = cycle_index
                row["cycle_start_beast_us"] = sync.phase_epoch_us + cycle_index * period_us
            else:
                row["cycle_index"] = None
                row["cycle_start_beast_us"] = None

        raw_residual_stats = _residual_stats([row.get("residual_raw_deg") for row in observations])
        detrended_residual_stats = _residual_stats([row.get("residual_detrended_deg") for row in observations])
        effective_span_s = (
            (max(all_effective_candidates) - min(all_effective_candidates)) / 1_000_000.0
            if len(all_effective_candidates) >= 2 else 0.0
        )
        phase_shape_diagnostics = _compute_folded_phase_shape(observations)
        error_mode = _classify_sync_error_mode(
            slope_deg_per_s=fit_slope_deg_per_s,
            time_span_s=effective_span_s,
            raw_mad_deg=raw_residual_stats["robust_spread_mad_deg"],
            detrended_mad_deg=detrended_residual_stats["robust_spread_mad_deg"],
            phase_shape_strength_deg=phase_shape_diagnostics["phase_shape_strength_deg"],
            repeatability=phase_shape_diagnostics["cycle_to_cycle_repeatability"],
        )

        def _method_unavailable_reasons(rows: list[dict]) -> dict[str, str | None]:
            reasons: dict[str, str | None] = {}
            for method_name, ts_field, resid_field in _BURST_TIMESTAMP_METHODS:
                has_ts = any(row.get(ts_field) is not None for row in rows)
                has_resid = any(row.get(resid_field) is not None for row in rows)
                if has_resid:
                    reasons[method_name] = None
                elif has_ts:
                    reasons[method_name] = "candidate timestamp present but residual unavailable"
                elif method_name in {"strongest_reply", "weighted_centroid", "mid_strong_window"}:
                    reasons[method_name] = "per-reply signal data unavailable in this window"
                else:
                    reasons[method_name] = "reply-level timestamps unavailable in this window"
            return reasons

        def _method_summary(rows: list[dict], reasons: dict[str, str | None] | None = None) -> list[dict]:
            out = []
            for method_name, _ts_field, resid_field in _BURST_TIMESTAMP_METHODS:
                paired_rows = [
                    row for row in rows
                    if row.get("resid_authoritative_deg") is not None and row.get(resid_field) is not None
                ]
                stats = _residual_stats([row.get(resid_field) for row in paired_rows])
                paired_operational_stats = _residual_stats([
                    row.get("resid_authoritative_deg") for row in paired_rows
                ])
                unavailable_reason = (reasons or {}).get(method_name)
                out.append({
                    "method": method_name,
                    "residual_field": resid_field,
                    "available": stats["count"] > 0,
                    "unavailable_reason": unavailable_reason if stats["count"] <= 0 else None,
                    "paired_operational_median_abs_residual_deg": paired_operational_stats["median_abs_residual_deg"],
                    **stats,
                })
            return out

        def _bin_rows(rows: list[dict], field: str, bins: list[tuple[str, float | None, float | None]]) -> list[dict]:
            out = []
            for label, lo, hi in bins:
                selected = []
                for row in rows:
                    value = row.get(field)
                    if value is None:
                        continue
                    value_f = float(value)
                    if lo is not None and value_f < lo:
                        continue
                    if hi is not None and value_f >= hi:
                        continue
                    selected.append(row)
                out.append({
                    "bin": label,
                    "field": field,
                    **_residual_stats([row.get("resid_authoritative_deg") for row in selected]),
                    "methods": _method_summary(selected, _method_unavailable_reasons(selected)),
                })
            return out

        def _corr_abs(rows: list[dict], x_field: str, y_field: str = "resid_authoritative_deg") -> float | None:
            pairs = []
            for row in rows:
                x = row.get(x_field)
                y = row.get(y_field)
                if x is None or y is None:
                    continue
                x_f = float(x)
                y_f = abs(float(y))
                if _math.isfinite(x_f) and _math.isfinite(y_f):
                    pairs.append((x_f, y_f))
            if len(pairs) < 3:
                return None
            xs = [p[0] for p in pairs]
            ys = [p[1] for p in pairs]
            mx = sum(xs) / len(xs)
            my = sum(ys) / len(ys)
            den_x = sum((x - mx) ** 2 for x in xs)
            den_y = sum((y - my) ** 2 for y in ys)
            if den_x <= 0 or den_y <= 0:
                return None
            return sum((x - mx) * (y - my) for x, y in pairs) / _math.sqrt(den_x * den_y)

        method_unavailable_reasons = _method_unavailable_reasons(observations)
        method_overall = _method_summary(observations, method_unavailable_reasons)
        fit_rows = [row for row in observations if row.get("fit_eligible")]
        high_quality_rows = [
            row for row in observations
            if row.get("fit_eligible")
            and (row.get("n_replies") or 0) >= 4
            and (row.get("pos_age_s") or 0.0) <= 1.0
            and (row.get("signal_dbfs") is None or row.get("signal_dbfs") >= -25.0)
        ]
        method_fit = _method_summary(fit_rows, _method_unavailable_reasons(fit_rows))
        method_high_quality = _method_summary(high_quality_rows, _method_unavailable_reasons(high_quality_rows))
        current_median_abs = _residual_stats([row.get("resid_authoritative_deg") for row in observations])["median_abs_residual_deg"]
        best_method = min(
            [entry for entry in method_overall if entry["median_abs_residual_deg"] is not None],
            key=lambda entry: entry["median_abs_residual_deg"],
            default=None,
        )
        best_method_name = best_method["method"] if best_method is not None else None
        best_method_median = best_method["median_abs_residual_deg"] if best_method is not None else None
        best_paired_operational_median = (
            best_method.get("paired_operational_median_abs_residual_deg") if best_method is not None else None
        )
        best_improvement = (
            best_paired_operational_median - best_method_median
            if best_paired_operational_median is not None and best_method_median is not None else None
        )

        operational_methods = defaultdict(int)
        for row in observations:
            operational_methods[row.get("burst_center_method") or "unknown"] += 1
        operational_method = max(operational_methods.items(), key=lambda item: item[1])[0] if operational_methods else "unknown"

        signal_bins = _bin_rows(observations, "signal_dbfs", [
            ("strong", -25.0, None),
            ("medium", -40.0, -25.0),
            ("weak", None, -40.0),
        ])
        burst_width_bins = _bin_rows(observations, "burst_span_us", [
            ("short", None, 20_000.0),
            ("medium", 20_000.0, 80_000.0),
            ("long", 80_000.0, None),
        ])
        reply_count_bins = _bin_rows(observations, "n_replies", [
            ("low", None, 3.0),
            ("medium", 3.0, 6.0),
            ("high", 6.0, None),
        ])
        position_age_bins = _bin_rows(observations, "position_age_ms", [
            ("fresh", None, 250.0),
            ("recent", 250.0, 1000.0),
            ("stale", 1000.0, None),
        ])
        bearing_rate_bins = _bin_rows(observations, "bearing_rate_deg_s", [
            ("left_fast", None, -0.2),
            ("near_stationary", -0.2, 0.2),
            ("right_fast", 0.2, None),
        ])
        range_bins = _bin_rows(observations, "range_nm", [
            ("near", None, 25.0),
            ("mid", 25.0, 75.0),
            ("far", 75.0, None),
        ])

        per_icao = []
        by_icao: dict[str, list[dict]] = defaultdict(list)
        for row in observations:
            if row.get("icao"):
                by_icao[row["icao"]].append(row)
        for icao, rows in by_icao.items():
            current_stats = _residual_stats([row.get("resid_authoritative_deg") for row in rows])
            method_stats = _method_summary(rows, _method_unavailable_reasons(rows))
            best = min(
                [entry for entry in method_stats if entry["median_abs_residual_deg"] is not None],
                key=lambda entry: entry["median_abs_residual_deg"],
                default=None,
            )
            current_med = current_stats["median_abs_residual_deg"]
            best_med = best["median_abs_residual_deg"] if best is not None else None
            per_icao.append({
                "icao": icao,
                "count": len(rows),
                "mean_residual_deg": current_stats["mean_residual_deg"],
                "median_residual_deg": current_stats["median_residual_deg"],
                "absolute_residual_spread_deg": current_stats["robust_spread_mad_deg"],
                "median_abs_residual_deg": current_med,
                "mean_range_nm": sum((row.get("range_nm") or 0.0) for row in rows) / len(rows),
                "mean_bearing_rate_deg_s": (
                    sum((row.get("bearing_rate_deg_s") or 0.0) for row in rows) / len(rows)
                ),
                "mean_position_age_ms": (
                    sum((row.get("position_age_ms") or 0.0) for row in rows) / len(rows)
                ),
                "best_burst_timestamp_method": best["method"] if best is not None else None,
                "best_method_median_abs_residual_deg": best_med,
                "best_vs_operational_improvement_deg": (
                    current_med - best_med if current_med is not None and best_med is not None else None
                ),
            })
        per_icao.sort(key=lambda row: (row["count"], row.get("median_abs_residual_deg") or 0.0), reverse=True)

        def _worst_bin_label(bin_rows: list[dict]) -> str | None:
            eligible = [row for row in bin_rows if row.get("count", 0) > 0 and row.get("median_abs_residual_deg") is not None]
            if not eligible:
                return None
            return max(eligible, key=lambda row: row["median_abs_residual_deg"])["bin"]

        pos_age_corr = _corr_abs(observations, "position_age_ms")
        bearing_rate_corr = _corr_abs(observations, "bearing_rate_deg_s")
        range_corr = _corr_abs(observations, "range_nm")
        aircraft_medians = [row["median_residual_deg"] for row in per_icao if row.get("median_residual_deg") is not None]
        aircraft_offset_spread = _residual_stats(aircraft_medians)["robust_spread_mad_deg"]
        likely_contributors = []
        if best_improvement is not None and best_improvement > 3.0:
            likely_contributors.append({
                "type": "timestamp_definition",
                "score": best_improvement,
                "detail": f"{best_method_name} improves median |residual| by {best_improvement:.2f} deg",
            })
        if pos_age_corr is not None and abs(pos_age_corr) >= 0.35:
            likely_contributors.append({
                "type": "truth_position_timing",
                "score": abs(pos_age_corr),
                "detail": f"|residual| correlation with position age is {pos_age_corr:.2f}",
            })
        if bearing_rate_corr is not None and abs(bearing_rate_corr) >= 0.35:
            likely_contributors.append({
                "type": "motion_compensation",
                "score": abs(bearing_rate_corr),
                "detail": f"|residual| correlation with bearing rate is {bearing_rate_corr:.2f}",
            })
        weak_bin = next((row for row in signal_bins if row["bin"] == "weak"), None)
        strong_bin = next((row for row in signal_bins if row["bin"] == "strong"), None)
        if weak_bin and strong_bin and weak_bin.get("median_abs_residual_deg") is not None and strong_bin.get("median_abs_residual_deg") is not None:
            weak_gap = weak_bin["median_abs_residual_deg"] - strong_bin["median_abs_residual_deg"]
            if weak_gap > 3.0:
                likely_contributors.append({
                    "type": "burst_shape_signal",
                    "score": weak_gap,
                    "detail": f"weak bursts are {weak_gap:.2f} deg worse than strong bursts",
                })
        if aircraft_offset_spread is not None and aircraft_offset_spread > 3.0:
            likely_contributors.append({
                "type": "aircraft_specific_bias",
                "score": aircraft_offset_spread,
                "detail": f"per-aircraft median residual MAD is {aircraft_offset_spread:.2f} deg",
            })
        if not likely_contributors and observations:
            likely_contributors.append({
                "type": "deeper_model_mismatch",
                "score": current_median_abs or 0.0,
                "detail": "no timestamp, quality, position-age, motion, or ICAO split dominates",
            })
        likely_contributors.sort(key=lambda item: item["score"], reverse=True)

        observation_model_diagnostics = {
            "operational_burst_timestamp_method": operational_method,
            "operational_residual_field": "resid_authoritative_deg",
            "detrended_residual_field": "residual_detrended_deg",
            "folded_phase_shape": phase_shape_diagnostics,
            "dominant_error_mode": error_mode["dominant_error_mode"],
            "error_mode_diagnostics": error_mode,
            "best_diagnostic_burst_timestamp_method": best_method_name,
            "best_diagnostic_method_median_abs_residual_deg": best_method_median,
            "best_vs_operational_median_abs_improvement_deg": best_improvement,
            "method_summary_overall": method_overall,
            "method_summary_fit_driving": method_fit,
            "method_summary_high_quality": method_high_quality,
            "available_burst_timestamp_methods": [
                entry["method"] for entry in method_overall if entry.get("available")
            ],
            "unavailable_burst_timestamp_methods": [
                entry["method"] for entry in method_overall if not entry.get("available")
            ],
            "method_unavailable_reasons": {
                method: reason
                for method, reason in method_unavailable_reasons.items()
                if reason is not None
            },
            "bins": {
                "signal_strength": signal_bins,
                "burst_width": burst_width_bins,
                "reply_count": reply_count_bins,
                "position_age": position_age_bins,
                "bearing_rate": bearing_rate_bins,
                "range": range_bins,
            },
            "correlations": {
                "abs_residual_vs_position_age": pos_age_corr,
                "abs_residual_vs_bearing_rate": bearing_rate_corr,
                "abs_residual_vs_range": range_corr,
            },
            "per_icao": per_icao,
            "spread_strongest_by": {
                "signal_class": _worst_bin_label(signal_bins),
                "burst_width": _worst_bin_label(burst_width_bins),
                "reply_count": _worst_bin_label(reply_count_bins),
                "position_age": _worst_bin_label(position_age_bins),
                "bearing_rate": _worst_bin_label(bearing_rate_bins),
                "aircraft_identity_mad_deg": aircraft_offset_spread,
            },
            "likely_contributors": likely_contributors,
        }

        tolerance_deg = 0.05
        summary = {
            "iid": iid,
            "available": True,
            "window_s": window_s,
            "observation_count": len(observations),
            "wall_clock_used_operationally": False,
            "operational_time_basis": "effective_beast_us",
            "operational_burst_timestamp_method": operational_method,
            "best_diagnostic_burst_timestamp_method": best_method_name,
            "best_diagnostic_method_median_abs_residual_deg": best_method_median,
            "best_vs_operational_median_abs_improvement_deg": best_improvement,
            "current_period_s": sync.period_s,
            "base_period_s": getattr(sync, "period_base_s", None),
            "dominant_period_s": getattr(sync, "dominant_period_s", None),
            "dominant_prior_period_s": getattr(sync, "dominant_prior_period_s", None),
            "trusted_refined_period_s": getattr(sync, "trusted_refined_period_s", None),
            "bootstrap_period_s": getattr(sync, "bootstrap_period_s", None),
            "active_family_prior_s": getattr(sync, "active_family_prior_s", None),
            "active_family_prior_source": getattr(sync, "active_family_prior_source", None),
            "dominant_prior_active": getattr(sync, "dominant_prior_active", None),
            "period_delta_to_dominant_s": getattr(sync, "dominant_period_delta_s", None),
            "period_delta_to_dominant_ppm": getattr(sync, "dominant_period_delta_ppm", None),
            "compact_period_s": getattr(sync, "compact_period_s", None),
            "compact_period_delta_to_dominant_s": getattr(sync, "compact_period_delta_to_dominant_s", None),
            "compact_period_delta_to_dominant_ppm": getattr(sync, "compact_period_delta_to_dominant_ppm", None),
            "compact_sync_unreliable": getattr(sync, "compact_sync_unreliable", None),
            "recovery_mode_active": getattr(sync, "recovery_mode_active", None),
            "recovery_trigger_reasons": getattr(sync, "recovery_trigger_reasons", None),
            "compact_gating_bypassed": getattr(sync, "compact_gating_bypassed", None),
            "recovery_relaxed_admitted_observations": getattr(sync, "recovery_relaxed_admitted_observations", None),
            "active_authority_mode": getattr(sync, "active_authority_mode", None),
            "authority_switch_count": getattr(sync, "authority_switch_count", None),
            "last_authority_switch_ts": getattr(sync, "last_authority_switch_ts", None),
            "last_authority_switch_reason": getattr(sync, "last_authority_switch_reason", None),
            "authority_enter_streak": getattr(sync, "authority_enter_streak", None),
            "authority_exit_streak": getattr(sync, "authority_exit_streak", None),
            "anchor_switch_count": getattr(sync, "anchor_switch_count", None),
            "last_anchor_switch_ts": getattr(sync, "last_anchor_switch_ts", None),
            "last_anchor_switch_reason": getattr(sync, "last_anchor_switch_reason", None),
            "anchor_hold_updates": getattr(sync, "anchor_hold_updates", None),
            "period_refine_mode": getattr(sync, "period_refine_mode", None),
            "period_authoritative_source": getattr(sync, "period_authoritative_source", None),
            "period_failure_score": getattr(sync, "period_failure_score", None),
            "period_failure_streak": getattr(sync, "period_failure_streak", None),
            "period_reacquire_active": getattr(sync, "period_reacquire_active", None),
            "period_reacquire_reason": getattr(sync, "period_reacquire_reason", None),
            "period_failure_primary_class": getattr(sync, "period_failure_primary_class", None),
            "period_reacquire_trigger": getattr(sync, "period_reacquire_trigger", None),
            "period_recovery_clean_update": getattr(sync, "period_recovery_clean_update", None),
            "period_recovery_clean_streak": getattr(sync, "period_recovery_clean_streak", None),
            "current_slope_deg_per_s": getattr(sync, "residual_slope_deg_per_s", None),
            "fit_slope_deg_per_s": fit_slope_deg_per_s,
            "fit_time_origin_beast_us": fit_time_origin_beast_us,
            "detrending_basis": "diagnostic_only_weighted_fit_residual_vs_effective_beast_time_s",
            "raw_median_abs_residual_deg": raw_residual_stats["median_abs_residual_deg"],
            "detrended_median_abs_residual_deg": detrended_residual_stats["median_abs_residual_deg"],
            "raw_mad_deg": raw_residual_stats["robust_spread_mad_deg"],
            "detrended_mad_deg": detrended_residual_stats["robust_spread_mad_deg"],
            "dominant_error_mode": error_mode["dominant_error_mode"],
            "period_drift_strength": error_mode["period_drift_strength"],
            "phase_shape_strength": error_mode["phase_shape_strength"],
            "phase_shape_strength_deg": error_mode["phase_shape_strength_deg"],
            "cycle_to_cycle_repeatability": error_mode["cycle_to_cycle_repeatability"],
            "phase_shape_repeatability_score": error_mode["cycle_to_cycle_repeatability"],
            "phase_epoch_us": sync.phase_epoch_us,
            "phase_offset_deg": sync.phase_offset_deg,
            "phase_anchor_icao": getattr(sync, "phase_anchor_icao", None),
            "phase_anchor_score": getattr(sync, "phase_anchor_score", None),
            "phase_anchor_status": getattr(sync, "phase_anchor_status", None),
            "phase_anchor_offset_raw_deg": getattr(sync, "phase_anchor_offset_raw_deg", None),
            "phase_anchor_offset_smoothed_deg": getattr(sync, "phase_anchor_offset_smoothed_deg", None),
            "phase_anchor_spread_deg": getattr(sync, "phase_anchor_spread_deg", None),
            "phase_anchor_obs_count": getattr(sync, "phase_anchor_obs_count", None),
            "phase_anchor_since_ts": getattr(sync, "phase_anchor_since_ts", None),
            "phase_anchor_replacement_reason": getattr(sync, "phase_anchor_replacement_reason", None),
            "phase_anchor_candidate_count": getattr(sync, "phase_anchor_candidate_count", None),
            "phase_validation_contributors": getattr(sync, "phase_validation_contributors", None),
            "phase_validation_reject_count": getattr(sync, "phase_validation_reject_count", None),
            "phase_validation_median_error_deg": getattr(sync, "phase_validation_median_error_deg", None),
            "phase_validation_status": getattr(sync, "phase_validation_status", None),
            "phase_anchor_candidates": getattr(sync, "phase_anchor_candidates", []),
            "fit_total_observations": getattr(sync, "fit_total_observations", None),
            "fit_eligible_observations": getattr(sync, "fit_eligible_observations", None),
            "fit_rejected_observations": getattr(sync, "fit_rejected_observations", None),
            "fit_reject_counts": getattr(sync, "fit_reject_reasons", None),
            "period_refine_block_reason": getattr(sync, "period_refine_block_reason", None),
            "period_update_proposed_s": getattr(sync, "period_update_proposed_s", None),
            "period_update_proposed_us": getattr(sync, "period_update_proposed_us", None),
            "period_update_applied_s": getattr(sync, "period_update_applied_s", None),
            "period_update_applied_us": getattr(sync, "period_update_applied_us", None),
            "period_update_gain": getattr(sync, "period_update_gain", None),
            "period_update_ppm_unclamped": getattr(sync, "period_update_ppm_unclamped", None),
            "period_update_ppm_applied": getattr(sync, "period_update_ppm_applied", None),
            "period_update_allowed": getattr(sync, "period_update_allowed", None),
            "period_update_block_reason": getattr(sync, "period_update_block_reason", None),
            "period_update_clamp_reason": getattr(sync, "period_update_clamp_reason", None),
            "period_update_fit_support": getattr(sync, "period_update_fit_support", None),
            "period_update_fit_span_s": getattr(sync, "period_update_fit_span_s", None),
            "period_correction_status": getattr(sync, "period_correction_status", None),
            "period_update_safety_ppm_per_update": getattr(sync, "period_update_safety_ppm_per_update", None),
            "period_update_safety_ppm_from_base": getattr(sync, "period_update_safety_ppm_from_base", None),
            "waveform_enabled": getattr(sync, "waveform_enabled", None),
            "waveform_applied": getattr(sync, "waveform_applied", None),
            "prop_delay_enabled": getattr(sync, "prop_delay_enabled", None),
            "motion_comp_phase_enabled": getattr(sync, "motion_comp_phase_enabled", None),
            "motion_comp_fit_enabled": getattr(sync, "motion_comp_fit_enabled", None),
            "motion_comp_applied_count": sum(1 for obs in observations if obs.get("motion_comp_applied")),
            "motion_comp_blocked_count": sum(1 for obs in observations if obs.get("motion_comp_enabled") and not obs.get("motion_comp_applied")),
            "motion_comp_mean_dt_us": (
                sum(obs.get("motion_comp_dt_us") or 0.0 for obs in observations if obs.get("motion_comp_applied"))
                / max(1, sum(1 for obs in observations if obs.get("motion_comp_applied")))
            ),
            "mean_abs_residual_without_motion_deg": _mean_abs(residuals_without_motion),
            "mean_abs_residual_with_motion_deg": _mean_abs(residuals_with_motion),
            "median_abs_residual_without_motion_deg": _median_abs(residuals_without_motion),
            "median_abs_residual_with_motion_deg": _median_abs(residuals_with_motion),
            "fit_mean_abs_residual_without_motion_deg": _mean_abs(fit_residuals_without_motion),
            "fit_mean_abs_residual_with_motion_deg": _mean_abs(fit_residuals_with_motion),
            "fit_median_abs_residual_without_motion_deg": _median_abs(fit_residuals_without_motion),
            "fit_median_abs_residual_with_motion_deg": _median_abs(fit_residuals_with_motion),
            "high_rate_mean_abs_residual_without_motion_deg": _mean_abs(high_rate_residuals_without_motion),
            "high_rate_mean_abs_residual_with_motion_deg": _mean_abs(high_rate_residuals_with_motion),
            "high_rate_median_abs_residual_without_motion_deg": _median_abs(high_rate_residuals_without_motion),
            "high_rate_median_abs_residual_with_motion_deg": _median_abs(high_rate_residuals_with_motion),
            "mean_motion_comp_improvement_deg": (
                sum(v for v in motion_improvements if v is not None) / len([v for v in motion_improvements if v is not None])
                if any(v is not None for v in motion_improvements) else None
            ),
            "predictor_consistency_tolerance_deg": tolerance_deg,
            "predictors_consistent_localiser": (_max_abs(localiser_deltas) or 0.0) <= tolerance_deg,
            "predictors_consistent_position_verification": (_max_abs(position_deltas) or 0.0) <= tolerance_deg,
            "predictors_consistent_burst_sync": (_max_abs(burstsync_deltas) or 0.0) <= tolerance_deg,
            "mean_delta_localiser_vs_authoritative_deg": _mean_abs(localiser_deltas),
            "max_delta_localiser_vs_authoritative_deg": _max_abs(localiser_deltas),
            "mean_delta_position_vs_authoritative_deg": _mean_abs(position_deltas),
            "max_delta_position_vs_authoritative_deg": _max_abs(position_deltas),
            "mean_delta_burstsync_vs_authoritative_deg": _mean_abs(burstsync_deltas),
            "max_delta_burstsync_vs_authoritative_deg": _max_abs(burstsync_deltas),
            "mean_wall_roundtrip_error_us": _mean_abs(roundtrip_errors),
            "max_wall_roundtrip_error_us": _max_abs(roundtrip_errors),
            "mean_raw_vs_effective_prediction_delta_deg": _mean_abs(raw_effective_deltas),
            "max_raw_vs_effective_prediction_delta_deg": _max_abs(raw_effective_deltas),
            "mean_wall_vs_effective_prediction_delta_deg": _mean_abs(wall_effective_deltas),
            "max_wall_vs_effective_prediction_delta_deg": _max_abs(wall_effective_deltas),
            "predictor_consistency_metrics": {
                "localiser": {
                    "mean_abs_delta_deg": _mean_abs(localiser_deltas),
                    "max_abs_delta_deg": _max_abs(localiser_deltas),
                    "tolerance_deg": tolerance_deg,
                },
                "position_verification": {
                    "mean_abs_delta_deg": _mean_abs(position_deltas),
                    "max_abs_delta_deg": _max_abs(position_deltas),
                    "tolerance_deg": tolerance_deg,
                },
                "burst_sync": {
                    "mean_abs_delta_deg": _mean_abs(burstsync_deltas),
                    "max_abs_delta_deg": _max_abs(burstsync_deltas),
                    "tolerance_deg": tolerance_deg,
                },
            },
            "observation_model_diagnosis": {
                "operational_burst_timestamp_method": operational_method,
                "dominant_error_mode": error_mode["dominant_error_mode"],
                "period_drift_strength": error_mode["period_drift_strength"],
                "phase_shape_strength": error_mode["phase_shape_strength"],
                "cycle_to_cycle_repeatability": error_mode["cycle_to_cycle_repeatability"],
                "best_diagnostic_burst_timestamp_method": best_method_name,
                "best_diagnostic_method_median_abs_residual_deg": best_method_median,
                "best_vs_operational_median_abs_improvement_deg": best_improvement,
                "available_burst_timestamp_methods": observation_model_diagnostics["available_burst_timestamp_methods"],
                "unavailable_burst_timestamp_methods": observation_model_diagnostics["unavailable_burst_timestamp_methods"],
                "method_unavailable_reasons": observation_model_diagnostics["method_unavailable_reasons"],
                "spread_strongest_by": observation_model_diagnostics["spread_strongest_by"],
                "likely_contributors": likely_contributors,
            },
            "retention_diagnostics": retention_diagnostics,
        }

        return {
            "iid": iid,
            "available": True,
            "sync_state": _live_sync_state_to_dict(sync),
            "summary": summary,
            "observations": observations,
            "observation_model_diagnostics": observation_model_diagnostics,
            "retention_diagnostics": retention_diagnostics,
        }

    def get_dwell_profile(self, iid: int, icao: str, sweep_idx: int | None = None) -> list[dict]:
        """Return per-reply RSSI+timestamp within the most recent (or specified) burst."""
        with self._lock:
            history = self._sweep_history.get(iid)
            sweeps = list(history) if history else []
            dwell_entries = list(self._dwell_profiles.get(iid, []))

        if not sweeps:
            sweeps = self._compute_sweep_history_for_iid(iid)
            if not sweeps:
                return []

        if not sweeps:
            return []

        if sweep_idx is not None:
            if sweep_idx < 0 or sweep_idx >= len(sweeps):
                return []
            sweep = sweeps[sweep_idx]
        else:
            # Find most recent sweep containing this ICAO
            sweep = None
            for s in reversed(sweeps):
                if any(a["icao"] == icao for a in s["aircraft"]):
                    sweep = s
                    break
            if sweep is None:
                return []

        # Find the burst for this ICAO in the sweep
        for aircraft_entry in sweep["aircraft"]:
            if aircraft_entry["icao"] == icao:
                replies = aircraft_entry.get("replies")
                if replies:
                    return list(replies)
                beam_center_us = aircraft_entry.get("beam_center_us")
                if beam_center_us is None:
                    return []
                matches = [
                    entry for entry in dwell_entries
                    if entry.get("icao") == icao
                    and abs(float(entry.get("beam_center_us", 0.0)) - float(beam_center_us)) <= 1.0
                ]
                if not matches:
                    return []
                return list(matches[-1].get("replies", []))
        return []

    def get_aircraft_burst_position(self, icao: str, ts_us: int) -> dict | None:
        """Return aircraft position/geometry for one burst timestamp, if available."""
        pos = self._get_aircraft_position(icao, ts_us, self.get_latest_arrival_us())
        if pos is None:
            return None

        import config as _config
        from .localiser import _haversine_m, _bearing_deg

        receiver_lat = getattr(_config, "RECEIVER_LAT", None)
        receiver_lon = getattr(_config, "RECEIVER_LON", None)
        if receiver_lat is not None and receiver_lon is not None:
            pos["range_nm"] = round(_haversine_m(receiver_lat, receiver_lon, pos["lat"], pos["lon"]) / 1852.0, 2)
            pos["bearing_deg"] = round(_bearing_deg(receiver_lat, receiver_lon, pos["lat"], pos["lon"]), 2)
        else:
            pos["range_nm"] = None
            pos["bearing_deg"] = None
        return pos

    def _estimate_wall_time_from_arrival_us(self, arrival_us: float, latest_arrival_us: float | None) -> float | None:
        """Approximate wall time for a Beast-relative arrival timestamp."""
        if latest_arrival_us is None:
            return None
        age_s = max(0.0, (latest_arrival_us - arrival_us) / 1_000_000.0)
        return time.time() - age_s

    def _lookup_track_position(self, icao: str, target_wall_ts: float) -> dict | None:
        """Return a historical/interpolated aircraft position near target_wall_ts."""
        if self._track_store is None:
            return None

        tracks = self._track_store.get_tracks({icao})
        points = tracks.get(icao) or []
        if not points:
            return None

        points = sorted(
            [point for point in points if point.get("lat") is not None and point.get("lon") is not None],
            key=lambda point: point["ts"],
        )
        if not points:
            return None

        before = None
        after = None
        for point in points:
            if point["ts"] <= target_wall_ts:
                before = point
            if point["ts"] >= target_wall_ts:
                after = point
                break

        if before is not None and after is not None and before is not after:
            dt_s = after["ts"] - before["ts"]
            if 0 < dt_s <= POS_AGE_INTERP_S:
                frac = (target_wall_ts - before["ts"]) / dt_s
                lat = before["lat"] + ((after["lat"] - before["lat"]) * frac)
                lon = before["lon"] + ((after["lon"] - before["lon"]) * frac)
                return {
                    "lat": lat,
                    "lon": lon,
                    "alt_reliable": True,
                    "interpolated": True,
                }

        nearest = before or after
        if nearest is None:
            return None
        if abs(nearest["ts"] - target_wall_ts) > POS_AGE_INTERP_S:
            return None
        return {
            "lat": nearest["lat"],
            "lon": nearest["lon"],
            "alt_reliable": True,
            "interpolated": False,
        }

    def _project_position_from_sample(self, sample: dict, dt_s: float) -> dict | None:
        """Project a sample forward/backward by a short interval using its motion vector."""
        if abs(dt_s) > POS_VECTOR_EXTRAP_S:
            return None

        groundspeed_kts = sample.get("groundspeed_kts")
        track_deg = sample.get("track_deg")
        if groundspeed_kts is not None and track_deg is not None:
            speed_ms = float(groundspeed_kts) * 0.514444
            heading_deg = float(track_deg)
        else:
            airspeed_kts = sample.get("airspeed_kts")
            heading_deg = sample.get("heading_deg")
            if airspeed_kts is None or heading_deg is None:
                return None
            speed_ms = float(airspeed_kts) * 0.514444
            heading_deg = float(heading_deg)

        import math

        lat = float(sample["lat"])
        lon = float(sample["lon"])
        dlat = (speed_ms * dt_s * math.cos(math.radians(heading_deg))) / 111_320
        dlon = (speed_ms * dt_s * math.sin(math.radians(heading_deg))) / (
            111_320 * max(math.cos(math.radians(lat)), 1e-6)
        )
        return {
            "lat": lat + dlat,
            "lon": lon + dlon,
            "alt_reliable": True,
            "interpolated": True,
        }

    def _lookup_adsb_position(self, icao: str, target_wall_ts: float) -> dict | None:
        """Return an ADS-B-timestamped aircraft position near target_wall_ts.

        ADS-B positions are transmitted frequently (~every 0.5s), so there should
        always be a sample very close to any DF11 timestamp. We use linear
        interpolation between the two nearest ADS-B samples, with a generous
        window to handle occasional gaps.
        """
        if self._aircraft_state is None:
            return None

        try:
            points = self._aircraft_state.get_aircraft_position_history(icao, window_s=IID_EVENT_MAX_AGE_S)
        except Exception:
            return None
        if not points:
            return None

        points = sorted(
            [point for point in points if point.get("lat") is not None and point.get("lon") is not None],
            key=lambda point: point["ts"],
        )
        if not points:
            return None

        # Find the two ADS-B samples bracketing the target time
        before = None
        after = None
        for point in points:
            if point["ts"] <= target_wall_ts:
                before = point
            if point["ts"] >= target_wall_ts:
                after = point
                break

        # Interpolate between bracketing samples
        if before is not None and after is not None and before is not after:
            dt_s = after["ts"] - before["ts"]
            # ADS-B is transmitted frequently; even 60s gaps are acceptable
            # for linear interpolation (aircraft move ~6km in 60s at 200kts)
            if 0 < dt_s <= 60.0:
                frac = (target_wall_ts - before["ts"]) / dt_s
                lat = before["lat"] + ((after["lat"] - before["lat"]) * frac)
                lon = before["lon"] + ((after["lon"] - before["lon"]) * frac)
                return {
                    "lat": lat,
                    "lon": lon,
                    "alt_reliable": True,
                    "interpolated": True,
                    "source": "adsb_interpolated",
                    "groundspeed_kts": before.get("groundspeed_kts"),
                    "track_deg": before.get("track_deg"),
                }

        # Fallback: use nearest sample if within 2 seconds
        nearest = None
        nearest_dt = None
        nearest_signed_dt = None
        for candidate in (before, after):
            if candidate is None:
                continue
            signed_dt_s = target_wall_ts - candidate["ts"]
            dt_s = abs(signed_dt_s)
            if nearest_dt is None or dt_s < nearest_dt:
                nearest = candidate
                nearest_dt = dt_s
                nearest_signed_dt = signed_dt_s

        if nearest is None or nearest_dt is None or nearest_signed_dt is None:
            return None
        if nearest_dt <= 2.0:
            return {
                "lat": nearest["lat"],
                "lon": nearest["lon"],
                "alt_reliable": True,
                "interpolated": False,
                "source": "adsb_nearest",
                "groundspeed_kts": nearest.get("groundspeed_kts"),
                "track_deg": nearest.get("track_deg"),
            }

        # Try velocity-based projection for larger gaps
        projected = self._project_position_from_sample(nearest, nearest_signed_dt)
        if projected is not None:
            projected["source"] = "adsb_projected"
            return projected

        return None
        return None

    # ------------------------------------------------------------------
    # Co-sweep calibration pairs (Stage 2)
    # ------------------------------------------------------------------

    def _get_aircraft_position(self, icao: str, ts_us: float, latest_arrival_us: float | None = None) -> dict | None:
        """Pull aircraft position at ts_us from aircraft_state, interpolating if needed.

        Returns dict with lat, lon, pos_reliable, alt_reliable or None.
        """
        target_wall_ts = self._estimate_wall_time_from_arrival_us(ts_us, latest_arrival_us)
        if target_wall_ts is not None:
            historical_adsb = self._lookup_adsb_position(icao, target_wall_ts)
            if historical_adsb is not None:
                return historical_adsb
            historical = self._lookup_track_position(icao, target_wall_ts)
            if historical is not None:
                return historical
        if self._aircraft_state is None:
            return None
        try:
            ac = self._aircraft_state.get_aircraft_live(icao)
            if ac is None:
                return None
            if not ac.get("pos_confident"):
                return None
            lat = ac.get("lat")
            lon = ac.get("lon")
            if lat is None or lon is None:
                return None

            # Compute position age in seconds (ts_us is Beast µs relative, ac age is wall time)
            # We use wall-clock age from get_aircraft_live's last_pos_age
            pos_age_s = ac.get("last_pos_age")
            if pos_age_s is None:
                return None

            if pos_age_s > POS_AGE_INTERP_S:
                return None  # too stale

            if pos_age_s > POS_AGE_FRESH_S:
                # Interpolate using heading and airspeed
                heading = ac.get("heading_deg")
                airspeed = ac.get("airspeed_kts")
                if heading is not None and airspeed is not None and airspeed > 0:
                    import math
                    # Convert airspeed to m/s
                    v_ms = airspeed * 0.514444
                    dt_s = pos_age_s
                    # Dead-reckoning: lat/lon adjustment
                    dlat = (v_ms * dt_s * math.cos(math.radians(heading))) / 111_320
                    dlon = (v_ms * dt_s * math.sin(math.radians(heading))) / (
                        111_320 * math.cos(math.radians(lat))
                    )
                    lat += dlat
                    lon += dlon
                    return {
                        "lat": lat, "lon": lon,
                        "alt_reliable": ac.get("altitude") is not None,
                        "interpolated": True,
                    }
                return None  # stale with no velocity — can't interpolate

            return {
                "lat": lat, "lon": lon,
                "alt_reliable": ac.get("altitude") is not None,
                "interpolated": False,
            }
        except Exception:
            return None

    def update_calibration_pairs(self) -> list[CalibrationPair]:
        """Find co-sweep aircraft pairs and compute TDOA for each.

        Returns newly identified CalibrationPair objects (also appended to
        self._pending_pairs).
        """
        import config as _config
        receiver_lat = getattr(_config, "RECEIVER_LAT", None)
        receiver_lon = getattr(_config, "RECEIVER_LON", None)
        if receiver_lat is None or receiver_lon is None:
            return []

        with self._lock:
            models_snapshot = dict(self._models)
            latest_arrival_us = self._iid_events[-1][0] if self._iid_events else None
            if latest_arrival_us is None:
                # Fall back to most recent burst record centroid
                for br_deque in self._burst_records.values():
                    if br_deque:
                        latest_arrival_us = max(latest_arrival_us or 0, br_deque[-1].centroid_us)
            seen_pair_keys = set(self._seen_pair_keys)
            if latest_arrival_us is None:
                return []
            eligible_iids = {
                iid
                for iid, model in models_snapshot.items()
                if not model.multi_radar_flag and model.status in ("SINGLE_RADAR", "LIKELY_SINGLE")
            }
            if not eligible_iids:
                return []
            cutoff_us = latest_arrival_us - int(PAIR_GENERATION_WINDOW_S * 1_000_000)
            sweep_snapshot: dict[int, list[dict]] = {}
            for iid in eligible_iids:
                history = self._sweep_history.get(iid)
                if history:
                    sweep_snapshot[iid] = [
                        sweep for sweep in history
                        if sweep.get("centroid_us") is not None and sweep["centroid_us"] >= cutoff_us
                    ]
            # Snapshot recent burst records per eligible IID for fallback sweep rebuild
            recent_bursts_by_iid: dict[int, list] = {}
            for iid in eligible_iids:
                br_deque = self._burst_records.get(iid)
                if br_deque:
                    relevant = [r for r in br_deque if r.centroid_us >= cutoff_us]
                    if relevant:
                        recent_bursts_by_iid[iid] = relevant

        for iid, burst_list in recent_bursts_by_iid.items():
            rebuilt_sweeps = self._build_sweeps_from_burst_records(burst_list)
            if rebuilt_sweeps and not sweep_snapshot.get(iid):
                sweep_snapshot[iid] = rebuilt_sweeps

        new_pairs: list[CalibrationPair] = []
        position_cache: dict[tuple[str, float], dict | None] = {}

        for iid, model in models_snapshot.items():
            if model.multi_radar_flag:
                continue
            if model.status not in ("SINGLE_RADAR", "LIKELY_SINGLE"):
                continue

            sweeps = sweep_snapshot.get(iid, [])
            for sweep in sweeps:
                aircraft = sweep.get("aircraft", [])
                if len(aircraft) < 2:
                    continue

                centroid_us = sweep["centroid_us"]

                # Get positions for all aircraft in this sweep
                positioned: list[tuple[str, dict, float]] = []
                for ac_entry in aircraft:
                    icao = ac_entry["icao"]
                    if not self._eligible_for_pair_generation(model, icao):
                        continue
                    ac_arrivals = ac_entry.get("arrivals_us", [centroid_us])
                    raw_centroid = (sum(ac_arrivals) / len(ac_arrivals)) if ac_arrivals else centroid_us
                    ac_beam_center = ac_entry.get("beam_center_us", raw_centroid)
                    cache_key = (icao, ac_beam_center)
                    if cache_key not in position_cache:
                        position_cache[cache_key] = self._get_aircraft_position(
                            icao,
                            ac_beam_center,
                            latest_arrival_us=latest_arrival_us,
                        )
                    pos = position_cache[cache_key]
                    if pos is not None:
                        positioned.append((icao, pos, ac_beam_center))

                if len(positioned) < 2:
                    continue

                # Form all pairs within CO_SWEEP_WINDOW_US
                for i in range(len(positioned)):
                    for j in range(i + 1, len(positioned)):
                        icao_a, pos_a, centroid_a = positioned[i]
                        icao_b, pos_b, centroid_b = positioned[j]

                        if abs(centroid_a - centroid_b) > CO_SWEEP_WINDOW_US:
                            continue

                        # TDOA: difference in arrival times in µs
                        tdoa_us = float(centroid_a - centroid_b)
                        if abs(tdoa_us) > MAX_PAIR_TDOA_US:
                            continue

                        if icao_b < icao_a:
                            icao_a, icao_b = icao_b, icao_a
                            pos_a, pos_b = pos_b, pos_a
                            centroid_a, centroid_b = centroid_b, centroid_a
                            tdoa_us = -tdoa_us

                        sweep_wall_ts = self._estimate_wall_time_from_arrival_us(centroid_us, latest_arrival_us) or time.time()
                        pair_key = (
                            iid,
                            round(sweep_wall_ts, 1),
                            icao_a,
                            icao_b,
                            round(pos_a["lat"], 5),
                            round(pos_a["lon"], 5),
                            round(pos_b["lat"], 5),
                            round(pos_b["lon"], 5),
                            round(tdoa_us, 3),
                        )
                        if pair_key in seen_pair_keys:
                            continue
                        seen_pair_keys.add(pair_key)

                        pair = CalibrationPair(
                            iid=iid,
                            ts=sweep_wall_ts,
                            icao_a=icao_a,
                            icao_b=icao_b,
                            lat_a=pos_a["lat"],
                            lon_a=pos_a["lon"],
                            lat_b=pos_b["lat"],
                            lon_b=pos_b["lon"],
                            tdoa_us=tdoa_us,
                            receiver_lat=receiver_lat,
                            receiver_lon=receiver_lon,
                        )
                        new_pairs.append(pair)

        with self._lock:
            self._pending_pairs.extend(new_pairs)
            self._seen_pair_keys = seen_pair_keys

        return new_pairs

    def pop_pending_pairs(self) -> list[CalibrationPair]:
        """Return and clear the pending calibration pairs buffer."""
        with self._lock:
            pairs = list(self._pending_pairs)
            self._pending_pairs.clear()
        return pairs

    def requeue_pending_pairs(self, pairs: list[CalibrationPair]) -> None:
        """Put unsaved calibration pairs back into the pending buffer."""
        if not pairs:
            return
        with self._lock:
            self._pending_pairs = list(pairs) + self._pending_pairs

    # ------------------------------------------------------------------
    # Startup load from DB
    # ------------------------------------------------------------------

    def load_from_db(self, db_models: list[dict]) -> None:
        """Populate models from persisted DB rows on startup."""
        with self._lock:
            for row in db_models:
                iid = row["iid"]
                model = RadarIID(
                    iid=iid,
                    resolution_mode=row.get("resolution_mode", "auto"),
                    status=row.get("status", "UNKNOWN"),
                    period_s=row.get("period_s"),
                    secondary_period_s=row.get("secondary_period_s"),
                    period_std_s=row.get("period_std_s"),
                    rpm=row.get("rpm"),
                    lat=row.get("lat"),
                    lon=row.get("lon"),
                    cep_m=row.get("cep_m"),
                    n_pairs=row.get("n_pairs", 0),
                    last_updated=row.get("last_updated", 0.0),
                    multi_radar_flag=bool(row.get("multi_radar_flag", 0)),
                    primary_support_count=row.get("primary_support_count", 0),
                    secondary_support_count=row.get("secondary_support_count", 0),
                    fm_lat=row.get("fm_lat"),
                    fm_lon=row.get("fm_lon"),
                    fm_cep_m=row.get("fm_cep_m"),
                    fm_source=row.get("fm_source"),
                    ci_lat=row.get("ci_lat"),
                    ci_lon=row.get("ci_lon"),
                    ci_cep_m=row.get("ci_cep_m"),
                    ci_source=row.get("ci_source"),
                    ci_n_pairs=row.get("ci_n_pairs", 0),
                    ci_last_updated=row.get("ci_last_updated"),
                    manual_lat=row.get("manual_lat"),
                    manual_lon=row.get("manual_lon"),
                    manual_note=row.get("manual_note"),
                    manual_updated_ts=row.get("manual_updated_ts"),
                    unresolvable_reason=row.get("unresolvable_reason"),
                    unresolvable_updated_ts=row.get("unresolvable_updated_ts"),
                )
                self._models[iid] = model
        log.info("RadarState: loaded %d IID models from DB", len(db_models))

    def update_localisation(
        self,
        iid: int,
        lat: float,
        lon: float,
        cep_m: float,
        n_pairs: int,
        n_sweeps: int | None = None,
        method: str = "solver",
    ) -> None:
        """Update the stored radar position for an IID after solver converges."""
        with self._lock:
            if iid not in self._models:
                self._models[iid] = RadarIID(iid=iid)
            model = self._models[iid]
            model.lat = lat
            model.lon = lon
            model.cep_m = cep_m
            model.n_pairs = n_pairs
            model.last_updated = time.time()
            model.convergence_history.append({
                "ts": model.last_updated,
                "lat": lat,
                "lon": lon,
                "cep_m": cep_m,
                "n_pairs": n_pairs,
                "n_sweeps": n_sweeps,
                "method": method,
            })
            # Keep last 50 convergence entries
            if len(model.convergence_history) > 50:
                model.convergence_history = model.convergence_history[-50:]
        self._bootstrap_live_sync_from_recent_frame_if_possible(iid)

    def update_forward_model_location(
        self,
        iid: int,
        lat: float,
        lon: float,
        cep_m: float,
        n_observations: int,
        window_s: float,
        source: str = "airport_prior",
        coincident_validation: dict | None = None,
    ) -> None:
        """Update the stored forward-model radar position for an IID."""
        with self._lock:
            if iid not in self._models:
                self._models[iid] = RadarIID(iid=iid)
            model = self._models[iid]
            model.fm_lat = lat
            model.fm_lon = lon
            model.fm_cep_m = cep_m
            model.fm_source = source
            model.fm_n_observations = n_observations
            model.fm_window_s = window_s
            model.fm_coincident_validation = coincident_validation
            model.last_updated = time.time()
            model.fm_convergence_history.append({
                "ts": model.last_updated,
                "lat": lat,
                "lon": lon,
                "cep_m": cep_m,
                "n_observations": n_observations,
                "source": source,
                "coincident_validation": coincident_validation,
            })
            # Keep last 50 convergence entries
            if len(model.fm_convergence_history) > 50:
                model.fm_convergence_history = model.fm_convergence_history[-50:]
        self._bootstrap_live_sync_from_recent_frame_if_possible(iid)

    def record_forward_model_attempt(self, iid: int, result: dict | None, elapsed_ms: float) -> None:
        """Record the latest FM run outcome for operator diagnostics."""
        with self._lock:
            if iid not in self._models:
                self._models[iid] = RadarIID(iid=iid)
            model = self._models[iid]
            payload = result or {"error": "unknown failure", "stage": "unknown"}
            success = "error" not in payload
            detail_keys = (
                "n_frames",
                "n_good",
                "n_good_frames",
                "n_marginal",
                "n_pairs",
                "n_observations",
                "n_unique_aircraft",
                "intersection_direction",
                "intersection_rms_km",
                "cluster_member_count",
                "dominance_ratio",
                "high_quality_frames",
                "azimuth_spread_deg",
                "final_residual_sigma_deg",
                "seed_residual_sigma_deg",
            )
            detail = {
                key: payload.get(key)
                for key in detail_keys
                if payload.get(key) is not None
            }
            model.fm_last_run = {
                "ts": time.time(),
                "success": success,
                "stored": payload.get("stored") if success else False,
                "elapsed_ms": round(elapsed_ms, 2),
                "stage": payload.get("stage") or ("complete" if success else "unknown"),
                "reason": payload.get("error") or (
                    "Not stored because it regressed against the current FM uncertainty"
                    if success and payload.get("stored") is False
                    else None
                ),
                "lat": payload.get("lat"),
                "lon": payload.get("lon"),
                "cep_m": payload.get("cep_m"),
                "source": payload.get("source"),
                "detail": detail,
            }

    # ------------------------------------------------------------------
    # Stage 3: live sync state accessors
    # ------------------------------------------------------------------

    def get_live_sync_state(self, iid: int) -> LiveSyncState | None:
        """Return the current live sync state for one IID, or None."""
        return self._live_sync_states.get(iid)

    def get_all_live_sync_states(self) -> dict[int, LiveSyncState]:
        """Return a snapshot of all current live sync states."""
        return dict(self._live_sync_states)

    _STAGE3_SYNC_SOURCES = frozenset({"multi_aircraft_burst", "go_multi_aircraft_burst"})

    def get_stage3_live_sync_state(self, iid: int) -> LiveSyncState | None:
        """Return the Stage 3-authoritative sync state for one IID.

        Stage 3 aircraft localisation remains logically separate from the
        earlier radar-localisation stages. Only the richer
        `multi_aircraft_burst` / `go_multi_aircraft_burst` sync models are
        eligible Stage 3 input; earlier bootstrap sources remain available
        through the general live-sync getters but are intentionally excluded.
        """
        sync = self._live_sync_states.get(iid)
        if sync is None or sync.source not in self._STAGE3_SYNC_SOURCES:
            return None
        return sync

    def get_all_stage3_live_sync_states(self) -> dict[int, LiveSyncState]:
        """Return a snapshot of Stage 3-authoritative sync states only."""
        return {
            iid: sync
            for iid, sync in self._live_sync_states.items()
            if sync.source in self._STAGE3_SYNC_SOURCES
        }

    def get_go_live_sync_state(self, iid: int) -> dict | None:
        """Return the compact mirrored Go sync state for one IID, or None."""
        state = self._go_sync_states_by_iid.get(iid)
        if state is None:
            return None
        return dict(state)

    def get_all_go_live_sync_states(self) -> dict[int, dict]:
        """Return a snapshot of compact mirrored Go sync state for all IIDs."""
        return {iid: dict(state) for iid, state in self._go_sync_states_by_iid.items()}

    def get_live_waveform_bins(self, iid: int) -> list[WaveformBin]:
        """Return a snapshot of the learned waveform bins for one IID."""
        return list(self._live_waveform_bins.get(iid) or [])

    def get_stage3_live_waveform_bins(self, iid: int) -> list[WaveformBin]:
        """Return waveform bins only when the IID has Stage 3-authoritative sync."""
        sync = self._live_sync_states.get(iid)
        if sync is None or sync.source not in self._STAGE3_SYNC_SOURCES:
            return []
        return list(self._live_waveform_bins.get(iid) or [])

    def update_live_sync_state(self, iid: int, **kwargs) -> None:
        """Merge updated fields into an existing LiveSyncState (e.g. jitter from calibration)."""
        existing = self._live_sync_states.get(iid)
        if existing is None:
            return
        for key, value in kwargs.items():
            if hasattr(existing, key):
                object.__setattr__(existing, key, value)

    # ------------------------------------------------------------------
    # Stage 3: live detection buffer accessors
    # ------------------------------------------------------------------

    def get_recent_live_detections_for_iid(
        self,
        iid: int,
        max_age_s: float = 30.0,
    ) -> list[Stage3LiveDetection]:
        """Return recent live detections for one IID, newest first."""
        cutoff = time.time() - max_age_s
        go_snapshot = self._go_track_observation_snapshot()
        if go_snapshot:
            return [
                self._stage3_detection_from_go_observation(entry)
                for entry in reversed(go_snapshot)
                if int(entry["iid"]) == iid and float(entry["wall_ts"]) >= cutoff
            ]
        with self._lock:
            snapshot = list(self._live_detection_buffer)
        return [d for d in reversed(snapshot) if d.iid == iid and d.wall_ts >= cutoff]

    def get_recent_live_detections_for_icao(
        self,
        icao: str,
        max_age_s: float = 30.0,
    ) -> list[Stage3LiveDetection]:
        """Return recent live detections for one ICAO across all IIDs, newest first."""
        cutoff = time.time() - max_age_s
        go_snapshot = self._go_track_observation_snapshot()
        if go_snapshot:
            return [
                self._stage3_detection_from_go_observation(entry)
                for entry in reversed(go_snapshot)
                if entry.get("icao") == icao and float(entry["wall_ts"]) >= cutoff
            ]
        with self._lock:
            snapshot = list(self._live_detection_buffer)
        return [d for d in reversed(snapshot) if d.icao == icao and d.wall_ts >= cutoff]

    def get_recent_live_detections(
        self,
        iid_subset: set[int] | None = None,
        max_age_s: float = 30.0,
    ) -> list[Stage3LiveDetection]:
        """Return recent live detections, optionally filtered to a set of IIDs."""
        cutoff = time.time() - max_age_s
        go_snapshot = self._go_track_observation_snapshot()
        if go_snapshot:
            return [
                self._stage3_detection_from_go_observation(entry)
                for entry in reversed(go_snapshot)
                if float(entry["wall_ts"]) >= cutoff and (iid_subset is None or int(entry["iid"]) in iid_subset)
            ]
        with self._lock:
            snapshot = list(self._live_detection_buffer)
        return [
            d for d in reversed(snapshot)
            if d.wall_ts >= cutoff and (iid_subset is None or d.iid in iid_subset)
        ]

    def record_live_radar_detection(
        self,
        iid: int,
        icao: str | None,
        arrival_us: float,
        df: int,
        signal_dbfs: float | None,
        truth_lat: float | None = None,
        truth_lon: float | None = None,
        position_age_seconds: float | None = None,
        association_confidence: float = 1.0,
    ) -> None:
        """Explicitly record a Stage 3-usable live detection (called from external code)."""
        det = Stage3LiveDetection(
            iid=iid,
            icao=icao,
            arrival_us=arrival_us,
            wall_ts=time.time(),
            df=df,
            signal_dbfs=signal_dbfs,
            receiver_lat=self._receiver_lat,
            receiver_lon=self._receiver_lon,
            truth_lat=truth_lat,
            truth_lon=truth_lon,
            position_age_seconds=position_age_seconds,
            association_confidence=association_confidence,
        )
        with self._lock:
            self._live_detection_buffer.append(det)

    def update_coincident_location(
        self,
        iid: int,
        lat: float,
        lon: float,
        cep_m: float,
        n_pairs: int,
        source: str = "coincident_intersection",
    ) -> None:
        """Update the stored coincident-illumination position for an IID."""
        with self._lock:
            if iid not in self._models:
                self._models[iid] = RadarIID(iid=iid)
            model = self._models[iid]
            model.ci_lat = lat
            model.ci_lon = lon
            model.ci_cep_m = cep_m
            model.ci_source = source
            model.ci_n_pairs = n_pairs
            model.ci_last_updated = time.time()
        self._bootstrap_live_sync_from_recent_frame_if_possible(iid)

    # ------------------------------------------------------------------
    # SweepFrame building and reference aircraft selection
    # ------------------------------------------------------------------

    def build_sweep_frames(self, iid: int) -> list:
        """Return live accumulated SweepFrames for an IID.

        Frames are built in real time by _on_df11_frame_builder as each DF11
        arrives. This method returns them (including in-progress frame).
        """
        return self.get_sweep_frames(iid)

    def get_sweep_frames(self, iid: int) -> list:
        """Return live accumulated SweepFrames for an IID, including in-progress frame."""
        go_frames = list(self._go_sweep_frames_by_iid.get(iid, ()))
        if go_frames:
            return go_frames
        frames = list(self._live_completed_frames.get(iid, []))
        # Include the in-progress frame if any
        in_progress = self._live_frames.get(iid)
        if in_progress is not None:
            from .models import SweepFrame
            n_obs = len(in_progress.observations)
            n_aircraft = n_obs + 1
            quality = "good" if n_aircraft >= 4 else ("marginal" if n_aircraft >= 3 else "insufficient")
            if n_aircraft >= 3:
                # Use the next counter value (not yet incremented) so this synthetic
                # frame never collides with any completed frame.
                in_progress_index = self._live_frame_counters.get(iid, 0)
                frames.append(SweepFrame(
                    frame_index=in_progress_index,
                    sweep_start_us=in_progress.ref_arrival_us,
                    ref_icao=in_progress.ref_icao,
                    ref_lat=in_progress.ref_lat,
                    ref_lon=in_progress.ref_lon,
                    ref_arrival_us=in_progress.ref_arrival_us,
                    observations=list(in_progress.observations),
                    quality=quality,
                    period_s=None,
                ))
        return frames

    @staticmethod
    def _frame_observation_payload(obs, period_s: float | None, ref_arrival_us: float) -> dict:
        dt_us = obs.arrival_us - ref_arrival_us
        dt_s = dt_us / 1_000_000.0
        if period_s and period_s > 0:
            observed_phase = ((dt_s / period_s) * 360.0) % 360.0
        else:
            observed_phase = None
        return {
            "icao": obs.icao,
            "lat": round(obs.lat, 6),
            "lon": round(obs.lon, 6),
            "arrival_us": obs.arrival_us,
            "observed_phase_deg": round(observed_phase, 2) if observed_phase is not None else None,
            "interpolated": obs.interpolated,
        }

    def _build_live_sweep_frame_summary_payload_locked(self, iid: int, model: RadarIID | None) -> dict:
        go_frames = list(self._go_sweep_frames_by_iid.get(iid, ()))
        completed = go_frames if go_frames else list(self._live_completed_frames.get(iid, []))
        in_progress = None if go_frames else self._live_frames.get(iid)
        latest_arrival_us = self._iid_latest_arrival_us.get(iid)
        if latest_arrival_us is None:
            latest_arrival_us = self._iid_events[-1][0] if self._iid_events else None
        out_frames: list[dict] = []

        for frame in completed:
            out_frames.append({
                "frame_index": frame.frame_index,
                "sweep_start_us": frame.sweep_start_us,
                "ref_icao": frame.ref_icao,
                "ref_lat": round(frame.ref_lat, 6),
                "ref_lon": round(frame.ref_lon, 6),
                "ref_arrival_us": frame.ref_arrival_us,
                "period_s": frame.period_s,
                "quality": frame.quality,
                "n_aircraft": 1 + len(frame.observations),
                "observations": [
                    self._frame_observation_payload(obs, frame.period_s, frame.ref_arrival_us)
                    for obs in frame.observations
                ],
            })

        if in_progress is not None:
            n_aircraft = 1 + len(in_progress.observations)
            if n_aircraft >= 3:
                quality = "good" if n_aircraft >= 4 else "marginal"
                in_progress_index = self._live_frame_counters.get(iid, 0)
                out_frames.append({
                    "frame_index": in_progress_index,
                    "sweep_start_us": in_progress.ref_arrival_us,
                    "ref_icao": in_progress.ref_icao,
                    "ref_lat": round(in_progress.ref_lat, 6),
                    "ref_lon": round(in_progress.ref_lon, 6),
                    "ref_arrival_us": in_progress.ref_arrival_us,
                    "period_s": None,
                    "quality": quality,
                    "n_aircraft": n_aircraft,
                    "observations": [
                        self._frame_observation_payload(obs, None, in_progress.ref_arrival_us)
                        for obs in in_progress.observations
                    ],
                })

        return {
            "iid": iid,
            "n_frames": len(out_frames),
            "frames": out_frames,
            "latest_arrival_us": latest_arrival_us,
            "last_updated": model.last_updated if model is not None else None,
        }

    def get_sweep_frame_summary_payload(self, iid: int) -> dict:
        """Return cached lightweight SweepFrame JSON for live UI transport."""
        with self._lock:
            model = self._models.get(iid)
            go_frames = list(self._go_sweep_frames_by_iid.get(iid, ()))
            if go_frames:
                signature = (
                    "go",
                    self._go_sweep_frames_revision.get(iid, 0),
                    self._iid_latest_arrival_us.get(iid),
                    model.last_updated if model is not None else None,
                )
            else:
                completed = self._live_completed_frames.get(iid)
                in_progress = self._live_frames.get(iid)
                frame_counter = self._live_frame_counters.get(iid, 0)
                completed_len = len(completed) if completed is not None else 0
                first_completed = completed[0].frame_index if completed_len > 0 else None
                last_completed = completed[-1].frame_index if completed_len > 0 else None
                in_progress_signature = None
                if in_progress is not None:
                    in_progress_signature = (
                        in_progress.ref_icao,
                        round(float(in_progress.ref_arrival_us), 3),
                        len(in_progress.observations),
                        tuple(
                            (
                                obs.icao,
                                round(float(obs.arrival_us), 3),
                                round(float(obs.lat), 6),
                                round(float(obs.lon), 6),
                                bool(obs.interpolated),
                            )
                            for obs in in_progress.observations
                        ),
                    )
                signature = (
                    "python",
                    frame_counter,
                    completed_len,
                    first_completed,
                    last_completed,
                    in_progress_signature,
                    self._iid_latest_arrival_us.get(iid),
                    model.last_updated if model is not None else None,
                )
            cached = self._live_sweep_frame_summary_cache.get(iid)
            if cached is not None and self._live_sweep_frame_summary_signature.get(iid) == signature:
                self._live_sweep_frame_summary_last_cache_hit[iid] = True
                return cached

            revision = self._live_sweep_frame_summary_revision.get(iid, 0) + 1
            payload = self._build_live_sweep_frame_summary_payload_locked(iid, model)
            payload["transport"] = {
                "cached": False,
                "source": "live_sweep_frame_summary_cache",
                "revision": revision,
            }
            self._live_sweep_frame_summary_cache[iid] = payload
            self._live_sweep_frame_summary_signature[iid] = signature
            self._live_sweep_frame_summary_revision[iid] = revision
            self._live_sweep_frame_summary_last_cache_hit[iid] = False
            return payload

    def get_sweep_frame_summary_revision(self, iid: int) -> int:
        with self._lock:
            return int(self._live_sweep_frame_summary_revision.get(iid, 0))

    def get_sweep_frame_summary_last_cache_hit(self, iid: int) -> bool:
        with self._lock:
            return bool(self._live_sweep_frame_summary_last_cache_hit.get(iid, False))

    def get_reference_aircraft(self, iid: int) -> dict:
        """Return current reference aircraft selection state."""
        go_ref = self._go_reference_aircraft_by_iid.get(iid)
        if go_ref is not None:
            return dict(go_ref)
        model = self._models.get(iid)
        if model is None:
            return {"status": "NO_MODEL"}

        ref = model.reference_aircraft
        if ref is None or ref.ref_icao is None:
            return {
                "status": "NOT_SELECTED",
                "reason": "No reference aircraft selected yet",
            }

        return {
            "status": "SELECTED",
            "ref_icao": ref.ref_icao,
            "ref_score": ref.ref_score,
            "ref_since_sweep": ref.ref_since_sweep,
            "hysteresis_margin": ref.hysteresis_margin,
            "challengers": ref.challengers,
        }

    def set_reference_aircraft_override(self, iid: int, icao: Optional[str]) -> bool:
        """Set or clear the manual reference aircraft override.

        When set, the next FM run will use this ICAO as the reference instead of
        auto-selecting based on score. The override is cleared after each FM run.
        """
        model = self._models.get(iid)
        if model is None:
            return False
        model.reference_aircraft_override = icao
        return True

    def get_pipeline_health(self, iid: int, sweep_frames: list | None = None) -> dict:
        """Return 5-stage pipeline health for an IID."""
        model = self._models.get(iid)
        if model is None:
            return {
                "stages": {
                    "period": {"status": "not_started", "detail": "No model for this IID"},
                    "reference": {"status": "not_started", "detail": ""},
                    "frames": {"status": "not_started", "detail": ""},
                    "scoring": {"status": "not_started", "detail": ""},
                    "optimisation": {"status": "not_started", "detail": ""},
                }
            }

        # Stage 1: Period
        if model.period_s is not None:
            n_icaos = model.rotation_model.n_qualifying if model.rotation_model else 0
            period_status = "working"
            period_detail = f"{model.period_s:.3f}s from {n_icaos} aircraft"
        else:
            period_status = "accumulating"
            period_detail = "Period not yet established"

        # Stage 2: Reference
        ref = model.reference_aircraft
        if ref and ref.ref_icao:
            ref_status = "working"
            ref_detail = f"{ref.ref_icao} stable since sweep #{ref.ref_since_sweep}"
        elif model.period_s is not None:
            ref_status = "accumulating"
            ref_detail = "Waiting for enough bursts per aircraft"
        else:
            ref_status = "not_started"
            ref_detail = "No period established yet"

        # Stage 3: Frames
        if sweep_frames is None:
            sweep_frames = self.get_sweep_frames(iid)
        n_frames = len(sweep_frames)
        n_good = sum(1 for f in sweep_frames if f.quality == "good")
        if n_frames > 0:
            frames_status = "working"
            frames_detail = f"{n_frames} frames ({n_good} good)"
        elif model.period_s is not None:
            frames_status = "accumulating"
            frames_detail = "Building sweep frames from burst data"
        else:
            frames_status = "not_started"
            frames_detail = "No period established"

        # Stage 4: Scoring
        if model.airport_hypothesis:
            top = model.airport_hypothesis[0] if model.airport_hypothesis else None
            scoring_status = "working"
            scoring_detail = f"Best: {top.get('airport_icao', '?')} score={top.get('score', 0):.1f}" if top else "Scored"
        elif n_frames > 0:
            scoring_status = "accumulating"
            scoring_detail = "Airport hypothesis not yet run"
        else:
            scoring_status = "not_started"
            scoring_detail = "No frames to score"

        # Stage 5: Optimisation
        if model.fm_lat is not None and model.fm_lon is not None:
            opt_status = "working"
            opt_detail = f"({model.fm_lat:.4f}, {model.fm_lon:.4f}) CEP {model.fm_cep_m:.0f}m" if model.fm_cep_m else "Position estimated"
        elif scoring_status == "working":
            opt_status = "accumulating"
            opt_detail = "Optimisation not yet run"
        else:
            opt_status = "not_started"
            opt_detail = ""

        return {
            "stages": {
                "period": {"status": period_status, "detail": period_detail},
                "reference": {"status": ref_status, "detail": ref_detail},
                "frames": {"status": frames_status, "detail": frames_detail},
                "scoring": {"status": scoring_status, "detail": scoring_detail},
                "optimisation": {"status": opt_status, "detail": opt_detail},
            }
        }

    def get_live_frame_counts(self, iid: int) -> dict[str, int]:
        """Return cheap live SweepFrame counts without materialising frame payloads."""
        with self._lock:
            completed = list(self._live_completed_frames.get(iid, []))
            in_progress = self._live_frames.get(iid)

        n_frames = len(completed)
        n_good = sum(1 for frame in completed if getattr(frame, "quality", None) == "good")
        n_marginal = sum(1 for frame in completed if getattr(frame, "quality", None) == "marginal")

        if in_progress is not None:
            n_aircraft = 1 + len(getattr(in_progress, "observations", []))
            if n_aircraft >= 3:
                n_frames += 1
                if n_aircraft >= 4:
                    n_good += 1
                else:
                    n_marginal += 1

        return {
            "n_frames": n_frames,
            "n_good": n_good,
            "n_marginal": n_marginal,
            "n_usable": n_good + n_marginal,
        }

    @staticmethod
    def _summarise_burst_records_by_icao(records: list[BurstRecord]) -> dict:
        counts: dict[str, int] = {}
        for rec in records:
            counts[rec.icao] = counts.get(rec.icao, 0) + 1
        per_icao = sorted(counts.values())
        if not per_icao:
            return {
                "icaos": 0,
                "min": 0,
                "median": 0.0,
                "max": 0,
                "reference_eligible": 0,
            }
        return {
            "icaos": len(per_icao),
            "min": per_icao[0],
            "median": statistics.median(per_icao),
            "max": per_icao[-1],
            "reference_eligible": sum(1 for c in per_icao if c >= MIN_BURSTS),
        }

    @staticmethod
    def _retained_span_s(records: list[BurstRecord]) -> float:
        if len(records) < 2:
            return 0.0
        return max(0.0, (records[-1].centroid_us - records[0].centroid_us) / 1_000_000.0)

    def get_live_pipeline_debug(self, iid: int) -> dict:
        """Compact per-IID live pipeline diagnostics for frame/sync debugging."""
        with self._lock:
            model = self._models.get(iid)
            sync = self._live_sync_states.get(iid)
            open_frame = self._live_frames.get(iid)
            timeline_obs = list(self._live_burst_timeline_obs.get(iid, []))
            aligned_obs = list(self._live_aligned_burst_obs.get(iid, []))
            completed = list(self._live_completed_frames.get(iid, []))
            burst_records = list(self._burst_records.get(iid, []))
            last_arrival = dict(self._live_last_arrival.get(iid, {}))
            go_injected = self._radar_core_frames_injected_by_iid.get(iid, 0)
            python_finalized = self._python_frames_finalized_by_iid.get(iid, 0)
            latest_arrival_us = self._iid_latest_arrival_us.get(iid)
            burst_dynamic_cap = self._burst_record_dynamic_cap_by_iid.get(iid, _BURST_RECORDS_MIN_PER_IID)
            burst_cap_hits = self._burst_record_cap_hits_by_iid.get(iid, 0)
            burst_last_active = self._burst_record_last_active_aircraft_by_iid.get(iid, 0)
            analysis_dynamic_cap = self._rotation_analysis_dynamic_cap_by_iid.get(iid, _ROTATION_ANALYSIS_MIN_EVENTS_PER_IID)
            analysis_cap_hits = self._rotation_analysis_cap_hits_by_iid.get(iid, 0)
            analysis_last_active = self._rotation_analysis_last_active_aircraft_by_iid.get(iid, 0)

        has_period = bool(model is not None and model.period_s is not None)
        reference_icao = None
        if model is not None and model.reference_aircraft is not None:
            reference_icao = model.reference_aircraft.ref_icao
        if reference_icao is None and model is not None and model.reference_aircraft_override is not None:
            reference_icao = model.reference_aircraft_override
        has_reference_icao = bool(reference_icao)

        open_frame_n_aircraft = 0
        open_frame_ref_icao = None
        if open_frame is not None:
            open_frame_ref_icao = open_frame.ref_icao
            open_frame_n_aircraft = 1 + len(getattr(open_frame, "observations", []))

        timeline_count = len(timeline_obs)
        aligned_count = len(aligned_obs)
        completed_count = len(completed)
        sync_state_present = sync is not None
        sync_state_usable = bool(sync is not None and sync.usable)
        snapshot_observations_count = 0
        if sync_state_present:
            snapshot_observations_count = timeline_count if timeline_count > 0 else aligned_count

        if not sync_state_present:
            snapshot_empty_reason = "sync_state_absent"
        elif (timeline_count + aligned_count) <= 0:
            snapshot_empty_reason = "no_timeline_or_aligned_observations"
        else:
            snapshot_empty_reason = None

        now_us = float(latest_arrival_us) if latest_arrival_us is not None else 0.0
        if now_us > 0:
            cutoff_us = now_us - (_ACTIVE_AIRCRAFT_LOOKBACK_S * 1_000_000.0)
            active_aircraft_est = sum(1 for ts in last_arrival.values() if ts >= cutoff_us)
        else:
            active_aircraft_est = len(last_arrival)
        if active_aircraft_est <= 0:
            active_aircraft_est = len({rec.icao for rec in burst_records})
        active_aircraft_est = max(active_aircraft_est, 0)

        burst_summary = self._summarise_burst_records_by_icao(burst_records)
        dominant_family_count = 0
        if model is not None and getattr(model, "rotation_model", None) is not None:
            dominant_family_count = len(getattr(model.rotation_model, "folded", {}) or {})
        reference_selection_sparse = bool(
            has_period
            and not has_reference_icao
            and burst_summary.get("reference_eligible", 0) <= 0
        )
        frame_bootstrap_sparse = bool(
            has_period
            and completed_count <= 0
            and burst_summary.get("median", 0.0) < MIN_BURSTS
        )

        return {
            "iid": iid,
            "rotation_status": model.status if model is not None else None,
            "period_s": model.period_s if model is not None else None,
            "has_period": has_period,
            "reference_icao": reference_icao,
            "has_reference_icao": has_reference_icao,
            "sync_state_present": sync_state_present,
            "sync_state_usable": sync_state_usable,
            "sync_state_source": sync.source if sync is not None else None,
            "sync_quality": sync.sync_quality if sync is not None else None,
            "open_frame": open_frame is not None,
            "open_frame_ref_icao": open_frame_ref_icao,
            "open_frame_n_aircraft": open_frame_n_aircraft,
            "timeline_observation_count": timeline_count,
            "aligned_observation_count": aligned_count,
            "completed_frame_count": completed_count,
            "go_frames_injected_count": go_injected,
            "go_frames_enabled": self._radar_core_frames_enabled,
            "go_frames_injected_total": self._radar_core_frames_injected,
            "go_frame_inject_errors_total": self._radar_core_frame_inject_errors,
            "frame_path_mode": (
                "go_authoritative"
                if self._radar_core_frames_enabled else
                "python_builder_authoritative"
            ),
            "python_builder_active": not self._radar_core_frames_enabled,
            "python_builder_status": (
                "suppressed_by_go_frames"
                if self._radar_core_frames_enabled else
                "active"
            ),
            "python_builder_frames_finalized_legacy_count": python_finalized,
            "python_builder_frames_finalized_legacy_total": self._python_frames_finalized_total,
            "latest_arrival_us": latest_arrival_us,
            "sync_snapshot_observations_count": snapshot_observations_count,
            "sync_snapshot_empty_reason": snapshot_empty_reason,
            "retained_state": {
                "active_aircraft_estimate": active_aircraft_est,
                "burst_records_total": len(burst_records),
                "burst_records_retained_span_s": self._retained_span_s(burst_records),
                "burst_records_dynamic_cap": burst_dynamic_cap,
                "burst_records_cap_hit": len(burst_records) >= burst_dynamic_cap,
                "burst_records_cap_hits_total": burst_cap_hits,
                "burst_records_last_active_aircraft": burst_last_active,
                "burst_records_per_icao": burst_summary,
                "rotation_analysis_dynamic_cap": analysis_dynamic_cap,
                "rotation_analysis_cap_hits_total": analysis_cap_hits,
                "rotation_analysis_last_active_aircraft": analysis_last_active,
                "reference_eligible_aircraft_count": burst_summary.get("reference_eligible", 0),
                "dominant_family_aircraft_count": dominant_family_count,
                "reference_selection_sparse_history": reference_selection_sparse,
                "frame_bootstrap_sparse_history": frame_bootstrap_sparse,
                "aligned_observation_count": aligned_count,
                "timeline_observation_count": timeline_count,
                "aligned_retained_span_s": self._summarise_live_sync_observation_buffer(
                    aligned_obs, self._MULTI_SYNC_OBS_MAX
                )["retained_duration_s"],
                "timeline_retained_span_s": self._summarise_live_sync_observation_buffer(
                    timeline_obs, self._BURST_SYNC_TIMELINE_OBS_MAX
                )["retained_duration_s"],
            },
        }

    def reset_forward_model(self, iid: int) -> bool:
        """Clear forward-model state for one IID (position, hypothesis, convergence).

        Must acquire the lock to race with update_forward_model_location().
        """
        with self._lock:
            model = self._models.get(iid)
            if model is None:
                return False
            model.fm_lat = None
            model.fm_lon = None
            model.fm_cep_m = None
            model.fm_source = None
            model.fm_n_observations = 0
            model.fm_window_s = 0.0
            model.airport_hypothesis = []
            model.fm_convergence_history = []
            model.fm_last_run = None
            model.fm_coincident_validation = None
            self._go_fm_states.pop(iid, None)
            self._go_fm_pipeline_stats.pop(iid, None)
            if iid in self._go_frame_positions:
                del self._go_frame_positions[iid]
                self._go_frame_positions_revision[iid] = self._go_frame_positions_revision.get(iid, 0) + 1
            return True
