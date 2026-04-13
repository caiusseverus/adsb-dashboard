"""Circle scoring and admission for inscribed-angle radar localisation.

Scoring components
------------------
1. phi_weight          - sin(delta_phi) ** 1.5, inscribed-angle geometry quality
2. age_weight          - decay from the worse ADS-B position age in the pair
3. reply_weight        - confidence from the weaker reply count in the pair
4. uncertainty_weight  - penalty for broad timing-derived circle corridors
5. prior_weight        - weak consistency factor from the accumulator position

Selection
---------
Hard gates -> score ordering -> per-frame and per-aircraft pair caps.

All named constants are module-level; no magic numbers inline.
"""

from __future__ import annotations

import math
from dataclasses import dataclass
from typing import Optional


# -- Named constants ---------------------------------------------------------

SIGMA_BAND_FLOOR_METRES: float = 500.0
"""Minimum sigma_band width in metres. Prevents over-confident circles."""

SIGMA_BAND_REFERENCE_METRES: float = 3_000.0
"""Low-kilometre scale where timing uncertainty starts to materially downweight a circle."""

BURST_CENTROID_BASE_UNCERTAINTY_SEC: float = 0.030
"""Base burst centroid timing uncertainty (30 ms), divided by sqrt(weakest replies)."""

POSITION_AGE_TAU_SEC: float = 4.0
"""Time constant for ADS-B position staleness decay (seconds)."""

MIN_REPLIES_PER_BURST: int = 2
"""Minimum reply count for both burst centroids in a pair."""

MAX_POSITION_AGE_SEC: float = 10.0
"""Maximum ADS-B position age for both aircraft in a pair."""

MIN_CIRCLE_SCORE: float = 0.05
"""Minimum final pair circle score to enter the admitted candidate set."""

MAX_PAIRS_PER_AIRCRAFT_PER_FRAME: int = 8
"""Per-frame cap preventing one aircraft from dominating the all-pairs set."""

MAX_CIRCLES_PER_FRAME: int = 48
"""Per-frame cap on admitted circles; large enough for all-pairs geometry, bounded for solve cost."""


# -- Data structures ---------------------------------------------------------

@dataclass
class ScoredCircle:
    """A scored pair-derived circle with full diagnostics."""

    # Stable identity and pair source
    circle_index: int
    icao_a: str
    icao_b: str
    frame_index: int

    # Circle geometry (ENU metres from receiver)
    center_enu_m: tuple[float, float]
    radius_m: float
    pair_baseline_m: float
    delta_phi: float

    # Score components (all in [0, 1])
    phi_weight: float
    residual_score: float
    age_weight: float
    uncertainty_weight: float
    reply_weight: float
    prior_weight: float
    intrinsic_weight: float
    circle_score: float

    # Diagnostics
    sigma_band_metres: float
    residual_metres: Optional[float]
    constraint_angle_deg: float
    selected: bool = False
    exclusion_reason: Optional[str] = None

    # Pair data for gates and diagnostics
    interpolated_a: bool = False
    interpolated_b: bool = False
    n_replies_a: int = 0
    n_replies_b: int = 0
    position_age_a_seconds: float = 0.0
    position_age_b_seconds: float = 0.0


# -- Public API --------------------------------------------------------------

def compute_circle_scores(
    circles: list[dict],
    p_bar_enu_m: Optional[tuple[float, float]],
    omega: float,
) -> list[ScoredCircle]:
    """Score all candidate pair circles.

    ``p_bar_enu_m`` is deliberately only a weak prior. Poor prior consistency
    can reduce a circle by at most 30%; intrinsic geometry, age, replies, and
    uncertainty drive the admission ordering.
    """
    scored: list[ScoredCircle] = []

    for fallback_index, circ in enumerate(circles):
        result = _score_single_circle(
            circle_index=int(circ.get("circle_index", fallback_index)),
            center_enu_m=circ["center_enu_m"],
            radius_m=float(circ["radius_m"]),
            delta_phi=float(circ["delta_phi"]),
            icao_a=str(circ.get("icao_a", "")),
            icao_b=str(circ.get("icao_b", "")),
            pair_baseline_m=float(circ.get("pair_baseline_m", 0.0)),
            interpolated_a=bool(circ.get("interpolated_a", False)),
            interpolated_b=bool(circ.get("interpolated_b", False)),
            n_replies_a=int(circ.get("n_replies_a", circ.get("n_replies", 0))),
            n_replies_b=int(circ.get("n_replies_b", circ.get("n_replies", 0))),
            position_age_a_seconds=float(circ.get("position_age_a_seconds", circ.get("position_age_seconds", 0.0))),
            position_age_b_seconds=float(circ.get("position_age_b_seconds", circ.get("position_age_seconds", 0.0))),
            frame_index=int(circ.get("frame_index", 0)),
            p_bar_enu_m=p_bar_enu_m,
            omega=omega,
        )
        scored.append(result)

    return scored


def select_circles(scored: list[ScoredCircle]) -> list[ScoredCircle]:
    """Return the admitted weighted set for candidate generation.

    The selector keeps all circles that pass hard gates and score threshold,
    ordered by score, while enforcing per-frame participation caps. This avoids
    collapsing all-pairs evidence back to a tiny greedy subset.
    """
    for sc in scored:
        sc.selected = False
        if sc.exclusion_reason is None:
            sc.exclusion_reason = _check_hard_gates(sc)

    gated = [sc for sc in scored if sc.exclusion_reason is None]
    gated.sort(key=lambda s: s.circle_score, reverse=True)

    selected: list[ScoredCircle] = []
    frame_counts: dict[int, int] = {}
    aircraft_frame_counts: dict[tuple[int, str], int] = {}

    for candidate in gated:
        frame_count = frame_counts.get(candidate.frame_index, 0)
        if frame_count >= MAX_CIRCLES_PER_FRAME:
            candidate.exclusion_reason = "frame_cap"
            continue

        key_a = (candidate.frame_index, candidate.icao_a)
        key_b = (candidate.frame_index, candidate.icao_b)
        if (
            aircraft_frame_counts.get(key_a, 0) >= MAX_PAIRS_PER_AIRCRAFT_PER_FRAME
            or aircraft_frame_counts.get(key_b, 0) >= MAX_PAIRS_PER_AIRCRAFT_PER_FRAME
        ):
            candidate.exclusion_reason = "aircraft_cap"
            continue

        candidate.selected = True
        selected.append(candidate)
        frame_counts[candidate.frame_index] = frame_count + 1
        aircraft_frame_counts[key_a] = aircraft_frame_counts.get(key_a, 0) + 1
        aircraft_frame_counts[key_b] = aircraft_frame_counts.get(key_b, 0) + 1

    return selected


def circular_distance(angle1_rad: float, angle2_rad: float) -> float:
    """Minimum angular distance between two angles, in [0, pi]."""
    diff = abs(angle1_rad - angle2_rad) % (2.0 * math.pi)
    return min(diff, 2.0 * math.pi - diff)


# -- Internal helpers --------------------------------------------------------

def _score_single_circle(
    *,
    circle_index: int,
    center_enu_m: tuple[float, float],
    radius_m: float,
    delta_phi: float,
    icao_a: str,
    icao_b: str,
    pair_baseline_m: float,
    interpolated_a: bool,
    interpolated_b: bool,
    n_replies_a: int,
    n_replies_b: int,
    position_age_a_seconds: float,
    position_age_b_seconds: float,
    frame_index: int,
    p_bar_enu_m: Optional[tuple[float, float]],
    omega: float,
) -> ScoredCircle:
    """Compute all score components for one pair circle."""
    degenerate_low = delta_phi < math.radians(2.0)
    degenerate_high = delta_phi > math.radians(178.0)
    invalid_geometry = (
        degenerate_low
        or degenerate_high
        or not math.isfinite(radius_m)
        or radius_m <= 0.0
        or pair_baseline_m <= 0.0
    )

    if invalid_geometry:
        return _make_scored_circle(
            circle_index=circle_index,
            icao_a=icao_a,
            icao_b=icao_b,
            frame_index=frame_index,
            center_enu_m=center_enu_m,
            radius_m=radius_m,
            pair_baseline_m=pair_baseline_m,
            delta_phi=delta_phi,
            phi_weight=0.0,
            residual_score=0.0,
            age_weight=0.0,
            uncertainty_weight=0.0,
            reply_weight=0.0,
            prior_weight=0.0,
            intrinsic_weight=0.0,
            circle_score=0.0,
            sigma_band_metres=SIGMA_BAND_FLOOR_METRES,
            residual_metres=None,
            constraint_angle_deg=0.0,
            interpolated_a=interpolated_a,
            interpolated_b=interpolated_b,
            n_replies_a=n_replies_a,
            n_replies_b=n_replies_b,
            position_age_a_seconds=position_age_a_seconds,
            position_age_b_seconds=position_age_b_seconds,
            exclusion_reason="degenerate_delta_phi",
        )

    sin_phi = max(math.sin(delta_phi), 1e-12)
    phi_weight = sin_phi ** 1.5

    min_replies = max(1, min(n_replies_a, n_replies_b))
    delta_t_uncertainty = BURST_CENTROID_BASE_UNCERTAINTY_SEC / math.sqrt(min_replies)
    delta_phi_uncertainty = delta_t_uncertainty * omega
    sigma_band = _compute_sigma_band_metres(pair_baseline_m, delta_phi_uncertainty, sin_phi)
    uncertainty_weight = 1.0 / (1.0 + (sigma_band / SIGMA_BAND_REFERENCE_METRES) ** 2)

    if p_bar_enu_m is not None:
        dx = p_bar_enu_m[0] - center_enu_m[0]
        dy = p_bar_enu_m[1] - center_enu_m[1]
        dist_to_centre = math.hypot(dx, dy)
        residual_metres = abs(dist_to_centre - radius_m)
        residual_score = math.exp(-0.5 * (residual_metres / sigma_band) ** 2)
        prior_weight = 0.7 + 0.3 * residual_score
        constraint_angle = math.atan2(dy, dx)
    else:
        residual_metres = None
        residual_score = 1.0
        prior_weight = 1.0
        constraint_angle = math.atan2(center_enu_m[1], center_enu_m[0])

    worst_age = max(position_age_a_seconds, position_age_b_seconds)
    age_weight = math.exp(-worst_age / POSITION_AGE_TAU_SEC)
    reply_weight = min(1.0, math.sqrt(min_replies / 4.0))
    intrinsic_weight = phi_weight * age_weight * reply_weight * uncertainty_weight
    circle_score = _clamp01(intrinsic_weight * prior_weight)

    return _make_scored_circle(
        circle_index=circle_index,
        icao_a=icao_a,
        icao_b=icao_b,
        frame_index=frame_index,
        center_enu_m=center_enu_m,
        radius_m=radius_m,
        pair_baseline_m=pair_baseline_m,
        delta_phi=delta_phi,
        phi_weight=_clamp01(phi_weight),
        residual_score=_clamp01(residual_score),
        age_weight=_clamp01(age_weight),
        uncertainty_weight=_clamp01(uncertainty_weight),
        reply_weight=_clamp01(reply_weight),
        prior_weight=_clamp01(prior_weight),
        intrinsic_weight=_clamp01(intrinsic_weight),
        circle_score=circle_score,
        sigma_band_metres=sigma_band,
        residual_metres=residual_metres,
        constraint_angle_deg=math.degrees(constraint_angle),
        interpolated_a=interpolated_a,
        interpolated_b=interpolated_b,
        n_replies_a=n_replies_a,
        n_replies_b=n_replies_b,
        position_age_a_seconds=position_age_a_seconds,
        position_age_b_seconds=position_age_b_seconds,
    )


def _compute_sigma_band_metres(
    pair_baseline_m: float,
    delta_phi_uncertainty: float,
    sin_phi: float,
) -> float:
    """Return an unsigned timing-derived corridor width for the pair circle.

    The old derivative used ``cos(delta_phi)`` directly, which changes sign
    across the valid inscribed-angle range. This approximation keeps the same
    intent, but uses an unsigned sensitivity that grows for weak small-angle
    geometry and with centroid timing uncertainty.
    """
    sigma = pair_baseline_m * abs(delta_phi_uncertainty) / (2.0 * max(sin_phi * sin_phi, 1e-12))
    return max(SIGMA_BAND_FLOOR_METRES, sigma)


def _make_scored_circle(
    *,
    circle_index: int,
    icao_a: str,
    icao_b: str,
    frame_index: int,
    center_enu_m: tuple[float, float],
    radius_m: float,
    pair_baseline_m: float,
    delta_phi: float,
    phi_weight: float,
    residual_score: float,
    age_weight: float,
    uncertainty_weight: float,
    reply_weight: float,
    prior_weight: float,
    intrinsic_weight: float,
    circle_score: float,
    sigma_band_metres: float,
    residual_metres: Optional[float],
    constraint_angle_deg: float,
    interpolated_a: bool,
    interpolated_b: bool,
    n_replies_a: int,
    n_replies_b: int,
    position_age_a_seconds: float,
    position_age_b_seconds: float,
    exclusion_reason: Optional[str] = None,
) -> ScoredCircle:
    return ScoredCircle(
        circle_index=circle_index,
        icao_a=icao_a,
        icao_b=icao_b,
        frame_index=frame_index,
        center_enu_m=center_enu_m,
        radius_m=radius_m,
        pair_baseline_m=pair_baseline_m,
        delta_phi=delta_phi,
        phi_weight=phi_weight,
        residual_score=residual_score,
        age_weight=age_weight,
        uncertainty_weight=uncertainty_weight,
        reply_weight=reply_weight,
        prior_weight=prior_weight,
        intrinsic_weight=intrinsic_weight,
        circle_score=circle_score,
        sigma_band_metres=sigma_band_metres,
        residual_metres=residual_metres,
        constraint_angle_deg=constraint_angle_deg,
        interpolated_a=interpolated_a,
        interpolated_b=interpolated_b,
        n_replies_a=n_replies_a,
        n_replies_b=n_replies_b,
        position_age_a_seconds=position_age_a_seconds,
        position_age_b_seconds=position_age_b_seconds,
        exclusion_reason=exclusion_reason,
    )


def _check_hard_gates(sc: ScoredCircle) -> Optional[str]:
    """Check hard gates for a scored circle. Returns exclusion reason or None."""
    if sc.exclusion_reason == "degenerate_delta_phi":
        return "degenerate_delta_phi"
    if min(sc.n_replies_a, sc.n_replies_b) < MIN_REPLIES_PER_BURST:
        return "insufficient_replies"
    if max(sc.position_age_a_seconds, sc.position_age_b_seconds) > MAX_POSITION_AGE_SEC:
        return "stale_position"
    if sc.circle_score < MIN_CIRCLE_SCORE:
        return "low_score"
    return None


def _clamp01(value: float) -> float:
    if not math.isfinite(value):
        return 0.0
    return max(0.0, min(1.0, value))
