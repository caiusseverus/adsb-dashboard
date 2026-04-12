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
from typing import TYPE_CHECKING, Optional, Any

try:
    import decode_cffi as _decode_cffi
except Exception:
    _decode_cffi = None

from .models import RadarIID, RotationModel, CalibrationPair

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
IID_EVENT_MAX_AGE_S = 1800  # 30 minutes — enough for reliable rotation model
ROTATION_ANALYSIS_MAX_AGE_S = 120.0   # 2 min ≈ 24–40 rotations — sufficient for period detection
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
_MIN_FRAME_START_SEPARATION_FRACTION = 0.5
_PHASE_FAMILY_HISTORY_MIN = 2
_PHASE_FAMILY_TOLERANCE_FRACTION = 0.15


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

        # If within 1 second, use position directly
        if abs(age) <= 1.0:
            return {"lat": lat, "lon": lon, "interpolated": False}

        # Try to project using velocity vector
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
                "groundspeed_kts": groundspeed_kts,
                "track_deg": track_deg,
            }

        # No velocity data — use position directly if reasonably fresh
        # Most aircraft don't transmit gs/track, so this is the common case
        # 5 seconds at 200m/s = ~1km position error, acceptable for radar localisation
        if abs(age) <= 5.0:
            return {"lat": lat, "lon": lon, "interpolated": False}

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
        }

    weight_sum = sum(weight for _arrival_us, weight in weighted_samples)
    if weight_sum <= 0:
        return {
            "beam_center_us": raw_centroid_us,
            "beam_center_method": "centroid",
        }

    weighted_center = (
        sum(arrival_us * weight for arrival_us, weight in weighted_samples) / weight_sum
    )
    return {
        "beam_center_us": weighted_center,
        "beam_center_method": "amplitude_weighted",
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
            cluster_med = round(statistics.median(cluster), 4)
            series_candidates.append(
                {
                    "period_s": cluster_med,
                    "n_intervals": len(cluster),
                    "std_s": round(statistics.stdev(cluster), 6) if len(cluster) > 1 else 0.0,
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
        "mean_period_s": round(statistics.mean(filtered), 4),
        "std_s": strongest["std_s"],
        "all_intervals_s": [round(iv, 4) for iv in intervals_s],
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
            non_snapped.append(round(iv, 4))

    total = len(intervals_s)
    n_snapped = sum(mult_counts.values())
    snap_rate = n_snapped / total if total > 0 else 0.0

    total_sweeps = sum(n * c for n, c in mult_counts.items())
    implied_detect = n_snapped / total_sweeps if total_sweeps > 0 else 0.0

    return {
        "snap_rate": round(snap_rate, 3),
        "mult_counts": dict(sorted(mult_counts.items())),
        "non_snapped": non_snapped,
        "implied_detect": round(implied_detect, 3),
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
        return best_sequence, round(coverage, 3)

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
            detection_rate = round(1.0 / nearest_int, 3)
            folded[icao] = {
                "raw_period_s": round(best_match["period_s"], 4),
                "multiplier": nearest_int,
                "folded_period_s": round(best_match["period_s"] / nearest_int, 4),
                "detection_rate": detection_rate,
                "method": "median",
                "snap_info": None,
            }
            folded_count += 1
            support_weight += best_match["n_intervals"] * detection_rate
            if nearest_int == 1:
                direct_count += 1
        else:
            residual[icao] = round(period, 4)

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
                "folded_period_s": round(candidate, 4),
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
            detection_rate = round((len(sequence) - 1) / implied_sweeps, 3) if implied_sweeps > 0 else 0.0
            folded[icao] = {
                "raw_period_s": residual[icao],
                "multiplier": 1,
                "folded_period_s": round(candidate, 4),
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
        "dominant_period_s": round(candidate, 4),
        "folded": folded,
        "residual": still_residual,
        "support_weight": round(support_weight, 6),
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

    candidates = sorted({
        round(series["period_s"], 4)
        for result in icao_results.values()
        for series in (result.get("series_candidates") or [])
        if series.get("period_s") is not None
    } | {
        round(result["median_period_s"], 4)
        for result in icao_results.values()
        if result.get("median_period_s") is not None
    })
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


def _periods_match(period_a: float | None, period_b: float | None, tolerance: float = PERIOD_MATCH_TOLERANCE) -> bool:
    if period_a is None or period_b is None or period_a <= 0 or period_b <= 0:
        return False
    return abs(period_a - period_b) / max(period_a, period_b) <= tolerance


def _blend_period(existing_period: float | None, support_count: int, observed_period: float) -> tuple[float, int]:
    if existing_period is None or existing_period <= 0:
        return round(observed_period, 4), 1
    weight = max(1, min(support_count, SUPPORT_CAP))
    blended = ((existing_period * weight) + observed_period) / (weight + 1)
    return round(blended, 4), min(weight + 1, SUPPORT_CAP)


def _confidence_from_support(support_count: int, target: int) -> float:
    if target <= 0:
        return 0.0
    return round(min(support_count / target, 1.0), 3)


def _reinforce_period_slot(existing_period: float | None, support_count: int, observed_period: float | None) -> tuple[float | None, int]:
    if observed_period is None:
        return existing_period, support_count
    if existing_period is None:
        return round(observed_period, 4), 1
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
                primary_period, primary_support = round(observed_period, 4), support_gain
            continue

        if support_gain > 0:
            primary_support = max(primary_support - 1, 0)
            replacement_threshold = max(MIN_QUALIFYING_ICAOS, primary_support + 1)
            if support_gain >= replacement_threshold:
                primary_period = round(observed_period, 4)
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

    def __init__(self, aircraft_state: Optional["AircraftState"] = None, track_store: Optional[Any] = None) -> None:
        self._aircraft_state = aircraft_state
        self._track_store = track_store

        # Lightweight ADS-B position tracker for real-time position capture
        self._adsb_tracker = AircraftPositionTracker()

        # Long-lived DF11 event deque: (arrival_us, iid, icao, signal_dbfs)
        # Pruned by age in update_rotation_models()
        self._iid_events: deque[tuple[float, int, str, float | None]] = deque()
        self._iid_latest_arrival_us: dict[int, float] = {}

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
        # Per-IID last arrival time per aircraft (for gap detection): {iid: {icao: arrival_us}}
        self._live_last_arrival: dict[int, dict[str, float]] = {}
        # Per-IID per-aircraft completed burst centroid history: {iid: {icao: [centroid_us, ...]}}
        # Used by reference selection — inter-centroid intervals are radar-period-sized.
        self._live_burst_centroids: dict[int, dict[str, list[float]]] = {}
        # Per-IID current frame being built: {iid: LiveFrameState | None}
        self._live_frames: dict[int, "LiveFrameState | None"] = {}
        # Per-IID last accepted frame-start timestamp to suppress near-duplicate openings.
        self._live_last_frame_start_us: dict[int, float] = {}
        # Per-IID completed frames: {iid: deque of SweepFrame} — bounded to most recent frames.
        # 500 frames ≈ 33 minutes at 4s period; keeps memory bounded.
        self._LIVE_FRAMES_MAX = 500
        self._live_completed_frames: dict[int, deque] = {}
        self._native_burst_processors: dict[int, Any] = {}

        # Real-time DF11 flash events for the sweep diagram.
        # Written from the decoder thread; read from the async event loop.
        # Sequence counter lets the frontend poll for new events since last fetch.
        # Bounded deque prevents unbounded growth; 2000 events ≈ a few seconds at high rate.
        self._flash_events: deque[tuple[int, int, str, int]] = deque(maxlen=2000)
        self._flash_seq: int = 0

        import threading
        self._lock = threading.Lock()
        self._update_active = threading.Event()

        # Injected at startup; called outside _lock for each completed good/marginal frame.
        # Signature: (iid: int, frame: SweepFrame, period_s: float) -> None
        self.per_frame_solve_callback = None

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

            # Real-time flash event — read by the polling endpoint for the sweep diagram.
            # Written outside _lock to avoid contention; deque.append is GIL-safe.
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

    def _current_reference_is_recent(
        self,
        iid: int,
        ref_icao: str | None,
        now_us: float,
        period_s: float,
        recency_periods: float = 5.0,
    ) -> bool:
        if ref_icao is None or period_s <= 0:
            return False
        centroids = self._live_burst_centroids.get(iid, {}).get(ref_icao, [])
        if not centroids:
            return False
        return now_us - centroids[-1] <= period_s * recency_periods * 1_000_000.0

    def _process_fired_bursts(self, iid: int, fired_bursts: list[dict]) -> dict:
        metrics = _new_fired_burst_phase_metrics()
        t_setup = time.perf_counter()
        model = self._models.get(iid)
        if model is None or model.period_s is None:
            return metrics
        period_s = model.period_s
        period_us = period_s * 1_000_000.0
        native_processor = self._native_burst_processors.get(iid)

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
        metrics["setup_ms"] += (time.perf_counter() - t_setup) * 1000

        for fired_burst in fired_bursts:
            metrics["fired_burst_count"] += 1
            fired_icao = fired_burst["icao"]
            burst_centroid_us = fired_burst["burst_centroid_us"]
            burst_signal = fired_burst["burst_signal"]
            trigger_arrival_us = fired_burst.get("trigger_arrival_us", burst_centroid_us)

            t_centroid = time.perf_counter()
            centroid_hist = self._live_burst_centroids[iid].setdefault(fired_icao, [])
            centroid_hist.append(burst_centroid_us)
            if len(centroid_hist) > 30:
                centroid_hist.pop(0)
            metrics["centroid_ms"] += (time.perf_counter() - t_centroid) * 1000

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
                        if self._current_reference_is_recent(iid, current_ref_icao, burst_centroid_us, period_s):
                            metrics["reference_select_count"] += 1
                            metrics["reference_reuse_count"] += 1
                            ref_icao = current_ref_icao
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
                        else:
                            metrics["reference_select_count"] += 1
                            metrics["reference_rescore_count"] += 1
                            ref_icao = self._select_reference_from_live_bursts(
                                iid, period_s, now_us=burst_centroid_us
                            )
                        if ref_icao is None:
                            continue
                finally:
                    metrics["reference_select_ms"] += (time.perf_counter() - t_reference_select) * 1000

            lat = lon = None
            interpolated = False
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
            except Exception:
                pass
            finally:
                metrics["position_lookup_ms"] += (time.perf_counter() - t_position_lookup) * 1000

            from .models import SweepFrameObservation, LiveFrameState

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
                ))
                current_frame.seen_icaos.add(fired_icao)
                current_frame.n_aircraft_seen += 1
                metrics["observation_count"] += 1
                metrics["frame_mutation_ms"] += (time.perf_counter() - t_frame_mutation) * 1000
        return metrics

    def on_df11_batch(self, events: list[tuple[int, int, str, float | None]]) -> None:
        """Process a batch of pre-decoded DF11 events from the radar worker."""
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
            if not events:
                return
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
            t_append = time.perf_counter()
            with self._lock:
                for iid, icao_hex, signal_dbfs, arrival_us in prepared:
                    self._iid_events.append((arrival_us, iid, icao_hex, signal_dbfs))
                    self._iid_latest_arrival_us[iid] = arrival_us
                    self._dirty_iids.add(iid)
            append_s = time.perf_counter() - t_append

            t_group = time.perf_counter()
            grouped_events: dict[int, list[tuple[float, str, float | None]]] = defaultdict(list)
            for iid, icao_hex, signal_dbfs, arrival_us in prepared:
                self._flash_seq += 1
                self._flash_events.append((self._flash_seq, iid, icao_hex, int(arrival_us)))
                grouped_events[iid].append((arrival_us, icao_hex, signal_dbfs))
            group_s = time.perf_counter() - t_group

            t_builder = time.perf_counter()
            t_builder_cpu = time.thread_time()
            native_available = _decode_cffi is not None and hasattr(_decode_cffi, "RadarBurstProcessor")
            for iid, iid_events in grouped_events.items():
                model = self._models.get(iid)
                if model is None or model.period_s is None:
                    continue
                active_iid_count += 1
                self._ensure_live_builder_state(iid)
                iid_events.sort(key=lambda event: event[0])
                if native_available:
                    processor = self._native_burst_processors.get(iid)
                    if processor is None:
                        processor = _decode_cffi.RadarBurstProcessor()
                        self._native_burst_processors[iid] = processor
                    t_native_burst = time.perf_counter()
                    fired_bursts = processor.process_batch(iid_events, BURST_GAP_US)
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
            frame = SweepFrame(
                frame_index=len(self._live_completed_frames[iid]),
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
            if self.per_frame_solve_callback is not None and quality in ("good", "marginal"):
                metrics["fm_callback_count"] += 1
                t_fm_callback = time.perf_counter()
                try:
                    self.per_frame_solve_callback(iid, frame, period_s)
                except Exception:
                    pass  # never let FM errors affect the sweep builder
                finally:
                    metrics["fm_callback_ms"] += (time.perf_counter() - t_fm_callback) * 1000

        self._live_frames[iid] = None
        return metrics

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

        total_w = 0.0
        weighted_sum = 0.0
        for ts, sig in replies:
            w = 10.0 ** (sig / 20.0) if sig is not None else 1.0
            weighted_sum += ts * w
            total_w += w
        burst_centroid_us = (weighted_sum / total_w) if total_w > 0 else replies[-1][0]
        burst_signal = max((s for _, s in replies if s is not None), default=None)

        centroid_hist = self._live_burst_centroids[iid].setdefault(icao, [])
        centroid_hist.append(burst_centroid_us)
        if len(centroid_hist) > 30:
            centroid_hist.pop(0)

        return {
            "icao": icao,
            "burst_centroid_us": burst_centroid_us,
            "burst_signal": burst_signal,
        }

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
        if model is None or model.period_s is None:
            return
        period_s = model.period_s
        period_us = period_s * 1_000_000.0

        # Ensure per-IID state exists
        if iid not in self._live_bursts:
            self._live_bursts[iid] = {}
            self._live_last_arrival[iid] = {}
            self._live_burst_centroids[iid] = {}
            self._live_frames[iid] = None
            self._live_completed_frames[iid] = deque(maxlen=self._LIVE_FRAMES_MAX)

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
            try:
                wall_ts = self._estimate_wall_time_from_arrival_us(burst_centroid_us, arrival_us)
                if wall_ts is not None:
                    pos = self._adsb_tracker.get_position_at(fired_icao, wall_ts)
                    if pos:
                        lat = pos.get("lat")
                        lon = pos.get("lon")
                        interpolated = pos.get("interpolated", False)
            except Exception:
                pass

            from .models import SweepFrameObservation, LiveFrameState

            if fired_icao == ref_icao:
                # Reference burst inside an already-open frame is treated as a duplicate
                # hit/sidelobe unless the frame has already expired, which was handled above.
                if current_frame is not None:
                    continue

                if not self._burst_matches_dominant_period_family(iid, fired_icao, period_s):
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
                if not self._burst_matches_dominant_period_family(iid, fired_icao, period_s):
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
                ))
                current_frame.seen_icaos.add(fired_icao)
                current_frame.n_aircraft_seen += 1

    def _select_reference_from_live_bursts(
        self, iid: int, period_s: float, now_us: float = 0.0
    ) -> str | None:
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
                if not self._iid_events:
                    return
                t_snapshot = time.perf_counter()
                # Estimate "now" in µs from the latest arrival
                now_us = self._iid_events[-1][0]
                cutoff_us = now_us - int(IID_EVENT_MAX_AGE_S * 1_000_000)

                # Prune old events from the left
                while self._iid_events and self._iid_events[0][0] < cutoff_us:
                    self._iid_events.popleft()

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
                events_snapshot = [
                    ev for ev in self._iid_events
                    if ev[0] >= analysis_cutoff_us and ev[1] in due_iids
                ]

            if not events_snapshot:
                return
            event_count = len(events_snapshot)

            # Group by IID
            by_iid: dict[int, list] = defaultdict(list)
            for ev in events_snapshot:
                if ev[1] in due_iids:
                    by_iid[ev[1]].append(ev)
            if max_iids_per_call is not None and max_iids_per_call > 0 and len(by_iid) > max_iids_per_call:
                ranked_iids = sorted(by_iid, key=lambda iid: len(by_iid[iid]), reverse=True)
                deferred_iids = set(ranked_iids[max_iids_per_call:])
                by_iid = {iid: by_iid[iid] for iid in ranked_iids[:max_iids_per_call]}
                with self._lock:
                    self._dirty_iids.update(deferred_iids)

            if not by_iid:
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

            ordered_iids = sorted(by_iid, key=_iid_priority)

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

            analysed_models: dict[int, RotationModel] = {}
            t_sweeps = time.perf_counter()
            history_fetch_s = 0.0
            cache_build_s = 0.0
            unprocessed_iids: set[int] = set()
            for index, iid in enumerate(ordered_iids):
                evs = by_iid[iid]
                analysed_models[iid] = _analyse_iid_events(evs)
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
            if iid in self._models:
                del self._models[iid]
                had_any = True
            if iid in self._sweep_history:
                del self._sweep_history[iid]
                had_any = True
            for live_dict in (self._live_bursts, self._live_last_arrival,
                               self._live_burst_centroids, self._live_frames,
                               self._live_last_frame_start_us,
                               self._live_completed_frames):
                if iid in live_dict:
                    del live_dict[iid]
                    had_any = True
            if iid in self._native_burst_processors:
                del self._native_burst_processors[iid]
                had_any = True
            if self._iid_events:
                filtered_events = deque(ev for ev in self._iid_events if ev[1] != iid)
                had_any = had_any or len(filtered_events) != len(self._iid_events)
                self._iid_events = filtered_events
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
                "pending_pairs": len(self._pending_pairs),
                "seen_pair_keys": len(self._seen_pair_keys),
            }
            self._models.clear()
            self._sweep_history.clear()
            self._iid_events.clear()
            self._iid_latest_arrival_us.clear()
            self._pending_pairs.clear()
            self._seen_pair_keys.clear()
            self._live_bursts.clear()
            self._live_last_arrival.clear()
            self._live_burst_centroids.clear()
            self._live_frames.clear()
            self._live_last_frame_start_us.clear()
            self._live_completed_frames.clear()
            self._native_burst_processors.clear()
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
            if not self._iid_events:
                return []
            events_snapshot = [ev for ev in self._iid_events if ev[1] == iid]
            latest_arrival_us = self._iid_events[-1][0]

        if not events_snapshot:
            return []

        active_icaos = {icao for _arrival_us, _iid, icao, _sig in events_snapshot if icao}
        pos_histories: dict[str, list[tuple]] = {}
        if self._aircraft_state is not None and active_icaos:
            try:
                pos_histories = self._aircraft_state.get_position_histories_bulk(active_icaos, window_s=300.0)
            except Exception:
                pos_histories = {}

        position_cache: dict[tuple[str, int], dict | None] = {}
        icao_samples: dict[str, list[tuple[int, float | None]]] = defaultdict(list)
        for arrival_us, _ev_iid, icao, signal_dbfs in events_snapshot:
            if icao:
                icao_samples[icao].append((arrival_us, signal_dbfs))
        for icao, samples in icao_samples.items():
            for burst in detect_bursts_with_signals(samples):
                beam_center_us = burst.get("beam_center_us", burst["centroid_us"])
                key = (icao, beam_center_us)
                if key not in position_cache:
                    position_cache[key] = self._nearest_pos_from_histories(
                        pos_histories, latest_arrival_us, icao, int(beam_center_us)
                    )
        sweeps = self._build_sweep_data(events_snapshot, position_cache)
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
        """Return per-IID activity within the last window_s seconds of Beast time."""
        with self._lock:
            if not self._iid_events:
                return {}
            now_us = self._iid_events[-1][0]
            cutoff_us = now_us - int(window_s * 1_000_000)
            events_snapshot = list(self._iid_events)

        activity: dict[int, dict] = {}
        for arrival_us, iid, icao, _sig in events_snapshot:
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
            if not self._iid_events:
                return None
            return self._iid_events[-1][0]

    def get_iid_latest_arrival_us(self, iid: int) -> float | None:
        """Return the latest Beast-relative arrival timestamp seen for one IID."""
        with self._lock:
            return self._iid_latest_arrival_us.get(iid)

    def get_iid_timeline(self, iid: int, window_s: float = 30.0) -> dict:
        """Return per-ICAO arrival timestamps for a single IID."""
        icao_arrivals: dict[str, list[int]] = defaultdict(list)
        with self._lock:
            if not self._iid_events:
                return {}
            now_us = self._iid_events[-1][0]
            cutoff_us = now_us - int(window_s * 1_000_000)
            for arrival_us, ev_iid, icao, _sig in reversed(self._iid_events):
                if arrival_us < cutoff_us:
                    break
                if ev_iid == iid and icao:
                    icao_arrivals[icao].append(arrival_us)

        return {icao: list(reversed(arrivals)) for icao, arrivals in icao_arrivals.items()}

    def get_dwell_profile(self, iid: int, icao: str, sweep_idx: int | None = None) -> list[dict]:
        """Return per-reply RSSI+timestamp within the most recent (or specified) burst."""
        with self._lock:
            history = self._sweep_history.get(iid)
            sweeps = list(history) if history else []

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
                return list(aircraft_entry.get("replies", []))
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
            recent_events_by_iid: dict[int, list[tuple[float, int, str, float | None]]] = defaultdict(list)
            for event in reversed(self._iid_events):
                arrival_us, iid, icao, signal_dbfs = event
                if arrival_us < cutoff_us:
                    break
                if iid in eligible_iids:
                    recent_events_by_iid[iid].append((arrival_us, iid, icao, signal_dbfs))

        for iid, events in recent_events_by_iid.items():
            if not events:
                continue
            events.reverse()
            rebuilt_sweeps = self._build_sweep_data(events, {})
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
        frames = list(self._live_completed_frames.get(iid, []))
        # Include the in-progress frame if any
        in_progress = self._live_frames.get(iid)
        if in_progress is not None:
            from .models import SweepFrame
            n_obs = len(in_progress.observations)
            n_aircraft = n_obs + 1
            quality = "good" if n_aircraft >= 4 else ("marginal" if n_aircraft >= 3 else "insufficient")
            if n_aircraft >= 3:
                frames.append(SweepFrame(
                    frame_index=len(frames),
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

    def get_reference_aircraft(self, iid: int) -> dict:
        """Return current reference aircraft selection state."""
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
            return True
