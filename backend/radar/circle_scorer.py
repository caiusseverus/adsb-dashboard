"""Circle scoring and selection pipeline for inscribed-angle radar localisation.

Scoring components
------------------
1. phi_weight      — sin(delta_phi), subtended angle geometry quality
2. sigma_band      — width of constraining corridor at solution point (metres)
3. residual_score  — Gaussian penalty for circle missing P_bar (accumulator)
4. age_weight      — exponential decay for stale ADS-B position fixes

Selection
---------
Hard gates → greedy constraint-direction diversity → ICAO diversity post-filter.

All named constants are module-level; no magic numbers inline.
"""

from __future__ import annotations

import math
from dataclasses import dataclass
from typing import Optional


# ── Named constants ────────────────────────────────────────────────────────

SIGMA_BAND_FLOOR_METRES: float = 500.0
"""Minimum sigma_band width in metres. Prevents over-confident circles."""

BURST_CENTROID_BASE_UNCERTAINTY_SEC: float = 0.030
"""Base burst centroid timing uncertainty (30 ms). Divided by sqrt(n_replies)."""

POSITION_AGE_TAU_SEC: float = 4.0
"""Time constant for ADS-B position staleness decay (seconds)."""

MIN_REPLIES_PER_BURST: int = 2
"""Minimum number of replies in a burst for the observation to qualify."""

MAX_POSITION_AGE_SEC: float = 10.0
"""Maximum age of ADS-B position fix (seconds). Older observations excluded."""

MIN_CIRCLE_SCORE: float = 0.05
"""Minimum circle_score to survive hard gating."""

MAX_CIRCLES: int = 6
"""Maximum number of circles selected for intersection solving."""

MIN_CONSTRAINT_SEPARATION_DEG: float = 25.0
"""Minimum angular separation (degrees) between constraint directions for diversity."""

DIVERSITY_OVERRIDE_THRESHOLD: float = 0.85
"""High-scoring circles bypass diversity requirement above this threshold."""


# ── Data structures ────────────────────────────────────────────────────────

@dataclass
class ScoredCircle:
    """A scored circle with full diagnostics."""
    # Circle geometry (ENU metres from receiver)
    center_enu_m: tuple[float, float]   # (east, north)
    radius_m: float

    # Score components (all in [0, 1])
    phi_weight: float
    residual_score: float
    age_weight: float
    circle_score: float

    # Diagnostics
    sigma_band_metres: float
    residual_metres: Optional[float]    # None when P_bar unavailable
    constraint_angle_deg: float
    selected: bool = False
    exclusion_reason: Optional[str] = None

    # Source data (for ICAO diversity enforcement and hard gates)
    frame_index: int = 0
    icao: str = ""
    n_replies: int = 0
    position_age_seconds: float = 0.0
    delta_phi: float = 0.0


# ── Public API ─────────────────────────────────────────────────────────────

def compute_circle_scores(
    circles: list[dict],
    p_bar_enu_m: Optional[tuple[float, float]],
    omega: float,
) -> list[ScoredCircle]:
    """Score all candidate circles.

    Parameters
    ----------
    circles : list[dict]
        Each dict must contain:
        - center_enu_m: tuple[float, float] — circle centre in ENU metres
        - radius_m: float — circle radius in metres
        - delta_phi: float — subtended angle in radians [0, pi]
        - n_replies: int — reply count for burst centroid timing
        - position_age_seconds: float — seconds since ADS-B position fix
        - frame_index: int — source frame index
        - icao: str — observed aircraft ICAO
    p_bar_enu_m : tuple[float, float] or None
        Accumulator radar position in ENU metres. None on first solve.
    omega : float
        Radar rotation rate in radians/second. omega = 2*pi / period_s.

    Returns
    -------
    list[ScoredCircle]
        All circles scored, including excluded ones (with exclusion_reason set).

    Scoring equations
    -----------------
    phi_weight = sin(delta_phi)
        Subtended angle geometry quality. Replaces baseline_km/100.

    delta_phi_uncertainty = delta_t_uncertainty * omega
    delta_t_uncertainty = BURST_CENTROID_BASE_UNCERTAINTY_SEC / sqrt(n_replies)
    sigma_band = (b * cos(delta_phi) * delta_phi_uncertainty) / (2 * sin(delta_phi)^2)
        Width of constraining corridor at solution point (metres).
        b = baseline = 2 * r * sin(delta_phi)  [from inscribed angle geometry]

    If P_bar available:
        dist_to_centre = norm(P_bar - C)
        residual_metres = |dist_to_centre - r|
        residual_score = exp(-0.5 * (residual_metres / sigma_band)^2)
    Else:
        residual_score = 1.0  # no penalty without prior

    age_weight = exp(-position_age_seconds / POSITION_AGE_TAU_SEC)
        Exponential decay for stale position data.

    circle_score = phi_weight * residual_score * age_weight
    """
    scored: list[ScoredCircle] = []

    for circ in circles:
        center_enu_m: tuple[float, float] = circ["center_enu_m"]
        radius_m: float = circ["radius_m"]
        delta_phi: float = circ["delta_phi"]
        n_replies: int = circ["n_replies"]
        position_age_seconds: float = circ["position_age_seconds"]
        frame_index: int = circ["frame_index"]
        icao: str = circ["icao"]

        result = _score_single_circle(
            center_enu_m=center_enu_m,
            radius_m=radius_m,
            delta_phi=delta_phi,
            n_replies=n_replies,
            position_age_seconds=position_age_seconds,
            p_bar_enu_m=p_bar_enu_m,
            omega=omega,
            frame_index=frame_index,
            icao=icao,
        )
        scored.append(result)

    return scored


def select_circles(
    scored: list[ScoredCircle],
) -> list[ScoredCircle]:
    """Apply hard gates and greedy diversity selection.

    Steps
    -----
    1. Hard gates: delta_phi range, n_replies, position age, min score
    2. Greedy diversity selection by constraint direction
    3. ICAO diversity post-filter (max 4 per ICAO, no dup in frame)
       with replacement from remaining pool.

    Parameters
    ----------
    scored : list[ScoredCircle]
        Output of compute_circle_scores().

    Returns
    -------
    list[ScoredCircle]
        Selected circles with ``selected=True`` set. Non-selected circles
        are not included in the return list.

    Invariants
    ----------
    - len(selected) <= MAX_CIRCLES
    - No two selected circles from the same frame share an ICAO
    - Each ICAO appears at most 4 times across all frames
    """
    # Step 1 — Hard gates
    gated = []
    for sc in scored:
        reason = _check_hard_gates(sc)
        if reason is not None:
            sc.exclusion_reason = reason
            continue
        gated.append(sc)

    # Step 2 — Greedy diversity selection
    gated.sort(key=lambda s: s.circle_score, reverse=True)
    selected: list[ScoredCircle] = []
    remaining = list(gated)  # copy for potential replacement

    for candidate in list(remaining):
        if len(selected) >= MAX_CIRCLES:
            break
        remaining.remove(candidate)
        if not selected:
            selected.append(candidate)
            candidate.selected = True
            continue

        angular_distances = [
            circular_distance(
                math.radians(candidate.constraint_angle_deg),
                math.radians(s.constraint_angle_deg),
            )
            for s in selected
        ]
        min_angular_distance = min(angular_distances)
        min_sep_rad = math.radians(MIN_CONSTRAINT_SEPARATION_DEG)

        diversity_ok = min_angular_distance > min_sep_rad
        score_justifies = candidate.circle_score > DIVERSITY_OVERRIDE_THRESHOLD

        if diversity_ok or score_justifies:
            selected.append(candidate)
            candidate.selected = True

    # Step 3 — ICAO diversity post-filter with replacement
    selected = _apply_icao_diversity_with_replacement(selected, remaining)

    return selected


def circular_distance(angle1_rad: float, angle2_rad: float) -> float:
    """Minimum angular distance between two angles, in [0, pi].

    Handles wrap-around at 2*pi boundary.
    """
    diff = abs(angle1_rad - angle2_rad) % (2.0 * math.pi)
    return min(diff, 2.0 * math.pi - diff)


# ── Internal helpers ───────────────────────────────────────────────────────

def _score_single_circle(
    center_enu_m: tuple[float, float],
    radius_m: float,
    delta_phi: float,
    n_replies: int,
    position_age_seconds: float,
    p_bar_enu_m: Optional[tuple[float, float]],
    omega: float,
    frame_index: int,
    icao: str,
) -> ScoredCircle:
    """Compute all score components for one circle.

    Returns a ScoredCircle with all fields populated.
    If degenerate geometry, returns score 0.0 with exclusion_reason set.
    """
    # Hard gate: degenerate geometry
    # Gate at 2° and 178° — near-degenerate inscribed angles
    degenerate_low = delta_phi < math.radians(2.0)
    degenerate_high = delta_phi > math.radians(178.0)
    if degenerate_low or degenerate_high:
        return ScoredCircle(
            center_enu_m=center_enu_m,
            radius_m=radius_m,
            phi_weight=0.0,
            residual_score=0.0,
            age_weight=0.0,
            circle_score=0.0,
            sigma_band_metres=SIGMA_BAND_FLOOR_METRES,
            residual_metres=None,
            constraint_angle_deg=0.0,
            frame_index=frame_index,
            icao=icao,
            n_replies=n_replies,
            position_age_seconds=position_age_seconds,
            delta_phi=delta_phi,
            exclusion_reason="degenerate_delta_phi",
        )

    # Component 1: Subtended angle weight
    # phi_weight = sin(delta_phi) — replaces baseline_km/100 leverage
    # Replaces binary |sin(Δφ)| < sin(5°) gate — weight falls naturally
    phi_weight = math.sin(delta_phi)

    # Component 2: Uncertainty band width
    # delta_phi_uncertainty = delta_t_uncertainty * omega
    # delta_t_uncertainty = BURST_CENTROID_BASE_UNCERTAINTY_SEC / sqrt(n_replies)
    # sigma_band = (b * cos(delta_phi) * delta_phi_uncertainty) / (2 * sin(delta_phi)^2)
    # where b = baseline = 2 * r * sin(delta_phi)  [inscribed angle geometry]
    delta_t_uncertainty = BURST_CENTROID_BASE_UNCERTAINTY_SEC / math.sqrt(max(n_replies, 1))
    delta_phi_uncertainty = delta_t_uncertainty * omega
    baseline_m = 2.0 * radius_m * math.sin(delta_phi)  # b = 2r sin(Δφ)
    sin_phi_sq = math.sin(delta_phi) ** 2
    if sin_phi_sq < 1e-12:
        sigma_band = SIGMA_BAND_FLOOR_METRES
    else:
        sigma_band = (
            baseline_m * math.cos(delta_phi) * delta_phi_uncertainty
        ) / (2.0 * sin_phi_sq)
    sigma_band = max(sigma_band, SIGMA_BAND_FLOOR_METRES)

    # Component 3: Residual against consensus position
    if p_bar_enu_m is not None:
        # dist_to_centre = ||P_bar - C||
        dx = p_bar_enu_m[0] - center_enu_m[0]
        dy = p_bar_enu_m[1] - center_enu_m[1]
        dist_to_centre = math.hypot(dx, dy)
        # residual_metres = |dist_to_centre - r|
        residual_metres = abs(dist_to_centre - radius_m)
        # residual_score = exp(-0.5 * (residual / sigma_band)^2)
        residual_score = math.exp(-0.5 * (residual_metres / sigma_band) ** 2)
    else:
        residual_metres = None
        residual_score = 1.0  # no penalty without prior

    # Component 4: Data quality — replaces binary interpolation penalty
    # age_weight = exp(-position_age / POSITION_AGE_TAU_SEC)
    # Replaces binary 1.0/0.7 direct/interpolated distinction
    age_weight = math.exp(-position_age_seconds / POSITION_AGE_TAU_SEC)

    # Final circle score — multiplicative combination
    # circle_score = phi_weight * residual_score * age_weight
    circle_score = phi_weight * residual_score * age_weight

    # Clamp to [0, 1] — should be natural but guard against floating-point
    circle_score = max(0.0, min(1.0, circle_score))

    # Constraint direction for diversity selection
    # constraint_dir = (P_bar - C) / ||P_bar - C||
    # constraint_angle = atan2(constraint_dir[1], constraint_dir[0])
    if p_bar_enu_m is not None:
        dx_dir = p_bar_enu_m[0] - center_enu_m[0]
        dy_dir = p_bar_enu_m[1] - center_enu_m[1]
        constraint_angle = math.atan2(dy_dir, dx_dir)
    else:
        # Proxy: direction from receiver (origin) to circle midpoint
        # Circle midpoint in ENU = center + offset along some direction.
        # Use the circle center direction as proxy (receiver → circle centre).
        constraint_angle = math.atan2(center_enu_m[1], center_enu_m[0])

    constraint_angle_deg = math.degrees(constraint_angle)

    return ScoredCircle(
        center_enu_m=center_enu_m,
        radius_m=radius_m,
        phi_weight=phi_weight,
        residual_score=residual_score,
        age_weight=age_weight,
        circle_score=circle_score,
        sigma_band_metres=sigma_band,
        residual_metres=residual_metres,
        constraint_angle_deg=constraint_angle_deg,
        frame_index=frame_index,
        icao=icao,
        n_replies=n_replies,
        position_age_seconds=position_age_seconds,
        delta_phi=delta_phi,
    )


def _check_hard_gates(sc: ScoredCircle) -> Optional[str]:
    """Check hard gates for a scored circle. Returns exclusion reason or None.

    Gates (in order):
    1. degenerate_delta_phi — delta_phi < 2° or > 178°
    2. insufficient_replies — n_replies < MIN_REPLIES_PER_BURST
    3. stale_position — position_age_seconds > MAX_POSITION_AGE_SEC
    4. low_score — circle_score < MIN_CIRCLE_SCORE
    """
    if sc.exclusion_reason == "degenerate_delta_phi":
        return "degenerate_delta_phi"
    if sc.n_replies < MIN_REPLIES_PER_BURST:
        return "insufficient_replies"
    if sc.position_age_seconds > MAX_POSITION_AGE_SEC:
        return "stale_position"
    if sc.circle_score < MIN_CIRCLE_SCORE:
        return "low_score"
    return None


def _apply_icao_diversity_with_replacement(
    selected: list[ScoredCircle],
    remaining: list[ScoredCircle],
) -> list[ScoredCircle]:
    """Apply ICAO diversity constraints with replacement from remaining pool.

    - Max 4 appearances per ICAO across all frames
    - No duplicate ICAO within the same frame

    Violators are removed and replaced from the remaining pool (sorted by
    circle_score) if a suitable candidate exists.
    """
    icao_counts: dict[str, int] = {}
    frame_icaos: dict[int, set[str]] = {}

    # First pass: count ICAOs and track frame usage in current selection
    for sc in selected:
        frame_icaos.setdefault(sc.frame_index, set()).add(sc.icao)
        icao_counts[sc.icao] = icao_counts.get(sc.icao, 0) + 1

    # Find violators
    valid: list[ScoredCircle] = []
    for sc in selected:
        # Check duplicate within frame (shouldn't happen from diversity selection,
        # but guard anyway)
        is_violator = False

        # Check max 4 per ICAO — only the 5th+ instance is a violator
        temp_counts: dict[str, int] = {}
        for v in valid:
            temp_counts[v.icao] = temp_counts.get(v.icao, 0) + 1
        if temp_counts.get(sc.icao, 0) >= 4:
            is_violator = True

        # Check duplicate within frame
        frame_set = set()
        for v in valid:
            if v.frame_index == sc.frame_index:
                if v.icao == sc.icao:
                    is_violator = True
                frame_set.add(v.icao)
        if sc.icao in frame_set:
            is_violator = True

        if not is_violator:
            valid.append(sc)
        else:
            sc.selected = False

    # Try to fill empty slots from remaining pool
    remaining.sort(key=lambda s: s.circle_score, reverse=True)
    for candidate in remaining:
        if len(valid) >= MAX_CIRCLES:
            break
        if candidate.selected:
            continue  # Already selected

        # Check ICAO constraints
        temp_counts: dict[str, int] = {}
        for v in valid:
            temp_counts[v.icao] = temp_counts.get(v.icao, 0) + 1

        if temp_counts.get(candidate.icao, 0) >= 4:
            continue  # Would violate ICAO cap

        frame_set = set()
        for v in valid:
            if v.frame_index == candidate.frame_index:
                frame_set.add(v.icao)
        if candidate.icao in frame_set:
            continue  # Would duplicate ICAO in frame

        valid.append(candidate)
        candidate.selected = True

    return valid
