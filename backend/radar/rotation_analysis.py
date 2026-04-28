from __future__ import annotations

import statistics
import time
from collections import defaultdict

from .burst_detection import detect_bursts
from .models import BurstRecord, RotationModel


_MIN_BURSTS = 4
_SERIES_CLUSTER_TOLERANCE = 0.12


def analyse_icao(bursts: list[dict]) -> dict | None:
    """Compute rotation period from inter-burst intervals for one ICAO."""
    if len(bursts) < _MIN_BURSTS:
        return None

    centroids = [b["centroid_us"] for b in bursts]
    intervals_us = [centroids[i + 1] - centroids[i] for i in range(len(centroids) - 1)]
    intervals_s = [iv / 1_000_000 for iv in intervals_us]

    valid = [iv for iv in intervals_s if 1.0 < iv < 30.0]
    if len(valid) < 2:
        return None

    def _cluster_repeated_intervals(intervals_s: list[float]) -> list[dict]:
        clusters: list[list[float]] = []
        for interval_s in sorted(intervals_s):
            placed = False
            for cluster in clusters:
                cluster_med = statistics.median(cluster)
                if (
                    cluster_med > 0
                    and abs(interval_s - cluster_med) / cluster_med <= _SERIES_CLUSTER_TOLERANCE
                ):
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
        "avg_replies_per_burst": round(statistics.mean(b["n_replies"] for b in bursts), 1),
    }


def _snap_intervals(intervals_s: list[float], base_period: float, tolerance: float = 0.08) -> dict:
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


def _evaluate_base_candidate(candidate: float, icao_results: dict[str, dict], tolerance: float = 0.05) -> dict:
    """Score one base-period candidate against all ICAO burst series."""
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

            if len(matched) > len(best_sequence) or (
                len(matched) == len(best_sequence) and error_sum < best_error
            ):
                best_sequence = matched
                best_error = error_sum

        coverage = len(best_sequence) / len(centroids_us) if centroids_us else 0.0
        return best_sequence, coverage

    for icao, result in icao_results.items():
        period = result["median_period_s"]
        series_candidates = result.get("series_candidates") or [
            {
                "period_s": period,
                "n_intervals": result.get("n_intervals", 0),
                "std_s": result.get("std_s", 0.0),
            }
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
    """Detect and fold missed-sweep harmonics into the dominant period."""
    if not icao_results:
        return {"dominant_period_s": None, "folded": {}, "residual": {}}

    candidate_values = sorted(
        [
            series["period_s"]
            for result in icao_results.values()
            for series in (result.get("series_candidates") or [])
            if series.get("period_s") is not None
        ]
        + [
            result["median_period_s"]
            for result in icao_results.values()
            if result.get("median_period_s") is not None
        ]
    )
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


def _rotation_model_from_icao_results(icao_results: dict[str, dict]) -> RotationModel:
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
    elif n_residual > 0:
        verdict = "CHECK_MULTI"
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


def _analyse_iid_events(events_for_iid: list[tuple[int, int, str, float | None]]) -> RotationModel:
    """Run full rotation analysis for one IID."""
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

    return _rotation_model_from_icao_results(icao_results)


def _analyse_burst_records(records: list[BurstRecord]) -> RotationModel:
    """Run full rotation analysis for one IID from BurstRecord objects."""
    icao_bursts: dict[str, list[dict]] = defaultdict(list)
    for record in records:
        if record.icao:
            icao_bursts[record.icao].append(
                {"centroid_us": record.centroid_us, "n_replies": record.n_replies}
            )
    for bursts in icao_bursts.values():
        bursts.sort(key=lambda b: b["centroid_us"])

    icao_results: dict[str, dict] = {}
    for icao, bursts in icao_bursts.items():
        result = analyse_icao(bursts)
        if result is not None:
            icao_results[icao] = result

    return _rotation_model_from_icao_results(icao_results)
