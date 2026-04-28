from __future__ import annotations

import math as _math


def _median_float(values: list[float]) -> float | None:
    if not values:
        return None
    clean = sorted(values)
    mid = len(clean) // 2
    if len(clean) % 2:
        return clean[mid]
    return (clean[mid - 1] + clean[mid]) / 2.0


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


def _clamp_float(value: float, lo: float, hi: float) -> float:
    return max(lo, min(hi, value))


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
