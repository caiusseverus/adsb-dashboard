"""Population residual monitor for burst sync.

Independently measures whether the wider aircraft population agrees with the
current anchor-derived phase basis.  Read-only: does not mutate sync state,
period authority, phase authority, or handoff state.

Usage
-----
summary = compute_population_residual_summary(entries, sync, iid)
payload = summary.to_api_dict()

``entries`` is the list of recomputed observation dicts already built by
``get_burst_sync_timeline``.  Each dict must contain at minimum:

    residual_deg, icao, pos_age_s, sync_update_eligible, classification,
    wall_ts (optional, used for window span)
"""
from __future__ import annotations

import math
from dataclasses import dataclass, field

from .angular import _circular_delta_deg, _circular_weighted_mean_deg

# ── Configurable thresholds ───────────────────────────────────────────────────

DISAGREEMENT_THRESHOLD_DEG: float = 25.0
MIN_DISAGREEING_ICAOS: int = 3
MIN_NON_ANCHOR_ICAOS: int = 4
MIN_OBS_PER_ICAO: int = 2
POS_FRESHNESS_GATE_S: float = 8.0
MONITOR_WINDOW_S: float = 120.0
MAX_PER_ICAO_IN_PAYLOAD: int = 20

_ELIGIBLE_PHASE_BASES = frozenset({"anchor_relative", "sweep_epoch_only"})

DEFAULT_THRESHOLDS: dict = {
    "disagreement_deg": DISAGREEMENT_THRESHOLD_DEG,
    "min_disagreeing_icaos": MIN_DISAGREEING_ICAOS,
    "min_non_anchor_icaos": MIN_NON_ANCHOR_ICAOS,
    "min_obs_per_icao": MIN_OBS_PER_ICAO,
    "pos_freshness_gate_s": POS_FRESHNESS_GATE_S,
    "monitor_window_s": MONITOR_WINDOW_S,
}


# ── Public data types ─────────────────────────────────────────────────────────


@dataclass
class PopulationResidualObservation:
    """One eligible burst observation contributed to the population monitor."""

    iid: int
    icao: str
    residual_deg: float
    predicted_deg: float | None
    observed_bearing_deg: float | None
    timestamp_s: float
    beast_us: float | None
    pos_age_s: float | None
    n_replies: int | None
    signal_dbfs: float | None
    classification: str | None
    sync_update_eligible: bool
    phase_basis: str | None
    phase_anchor_icao: str | None
    phase_anchor_status: str | None
    dominant_family: bool | None
    reject_reason: str | None


@dataclass
class PopulationResidualSummary:
    """Population-wide residual consistency summary for one IID.

    Status values
    -------------
    unavailable         — no sync state or ineligible phase basis
    insufficient_data   — not enough eligible observations or ICAOs
    anchor_unavailable  — phase basis is anchor_relative but anchor not found
                          in contributing observations, or sweep_epoch_only
    population_agrees   — anchor and population residuals are consistent
    population_disagrees — ≥ MIN_DISAGREEING_ICAOS disagree by > threshold
    population_mixed    — spread or minor disagreement but not enough for
                          population_disagrees
    """

    iid: int
    status: str
    reason: str | None
    window_s: float
    observation_count: int
    eligible_observation_count: int
    contributing_icao_count: int
    disagreeing_icao_count: int
    anchor_icao: str | None
    anchor_residual_mean_deg: float | None
    population_residual_mean_deg: float | None
    anchor_population_delta_deg: float | None
    population_residual_spread_deg: float | None
    worst_icao: str | None
    worst_icao_delta_deg: float | None
    per_icao: list[dict] = field(default_factory=list)
    rejection_counts: dict[str, int] = field(default_factory=dict)
    phase_basis: str | None = None

    def to_api_dict(self) -> dict:
        """Serialise to a JSON-safe dict suitable for API payloads.

        per_icao is trimmed to MAX_PER_ICAO_IN_PAYLOAD entries, sorted with
        the anchor ICAO first and remaining ICAOs by descending |delta|.

        Per-ICAO chip value semantics:
          residual_mean_deg  — circular mean of corrected residual_deg
                               (bearing - predicted_bearing, ±180°).
                               Includes propagation, motion, and waveform
                               corrections.  NOT a raw phase offset.
                               Anchor chips may show non-zero values because
                               residual = true_offset + correction_terms.
          delta_from_anchor_deg — circular delta from the anchor's
                                  residual_mean_deg.  This is what
                                  determines agreement/disagreement.
        """
        anchor_rows = [r for r in self.per_icao if r.get("is_anchor")]
        non_anchor_rows = sorted(
            (r for r in self.per_icao if not r.get("is_anchor")),
            key=lambda r: -(abs(r.get("delta_from_anchor_deg") or 0.0)),
        )
        ordered = anchor_rows + non_anchor_rows
        trimmed = ordered[:MAX_PER_ICAO_IN_PAYLOAD]
        omitted = max(0, len(self.per_icao) - len(trimmed))
        return {
            "status": self.status,
            "reason": self.reason,
            "phase_basis": self.phase_basis,
            "window_s": round(self.window_s, 1),
            "observation_count": self.observation_count,
            "eligible_observation_count": self.eligible_observation_count,
            "contributing_icao_count": self.contributing_icao_count,
            "disagreeing_icao_count": self.disagreeing_icao_count,
            "anchor_icao": self.anchor_icao,
            "anchor_residual_mean_deg": _round2(self.anchor_residual_mean_deg),
            "population_residual_mean_deg": _round2(self.population_residual_mean_deg),
            "anchor_population_delta_deg": _round2(self.anchor_population_delta_deg),
            "population_residual_spread_deg": _round2(self.population_residual_spread_deg),
            "worst_icao": self.worst_icao,
            "worst_icao_delta_deg": _round2(self.worst_icao_delta_deg),
            "per_icao": trimmed,
            "per_icao_total_count": len(self.per_icao),
            "per_icao_omitted_count": omitted,
            "rejection_counts": dict(self.rejection_counts),
            "thresholds": DEFAULT_THRESHOLDS,
            "residual_basis_description": (
                "residual_mean_deg = circular_mean(bearing - predicted_bearing) "
                "with propagation, motion, and waveform corrections applied. "
                "Anchor chips may show non-zero values because residual ≈ "
                "true_offset + correction_terms. Agreement is determined by "
                "delta_from_anchor_deg, not absolute residual_mean_deg."
            ),
        }


# ── Internal helpers ──────────────────────────────────────────────────────────


def _round2(v: float | None) -> float | None:
    return round(v, 2) if v is not None and math.isfinite(v) else None


def _circular_mean_residuals_deg(values: list[float]) -> float | None:
    """Circular mean of angular values, returned in ±180°.

    Uses the standard sin/cos projection via _circular_weighted_mean_deg
    (which returns [0, 360)) then maps back to ±180°.
    """
    raw = _circular_weighted_mean_deg(values)
    if raw is None:
        return None
    return (raw + 180.0) % 360.0 - 180.0


def _circular_spread_deg(values: list[float]) -> float | None:
    """Circular standard deviation using mean resultant length, in degrees.

    Returns None for fewer than 2 finite values.  Result is non-negative.
    """
    clean = [float(v) for v in values if math.isfinite(float(v))]
    if len(clean) < 2:
        return None
    n = len(clean)
    sin_sum = sum(math.sin(math.radians(v)) for v in clean)
    cos_sum = sum(math.cos(math.radians(v)) for v in clean)
    R = math.sqrt((sin_sum / n) ** 2 + (cos_sum / n) ** 2)
    R = min(R, 1.0 - 1e-12)
    return math.degrees(math.sqrt(max(0.0, -2.0 * math.log(R))))


def _derive_phase_basis_and_anchor(sync) -> tuple[str, str | None, str]:
    """Return (phase_basis, phase_anchor_icao, phase_anchor_status) from sync state.

    Prefers the typed phase_basis field when available.
    """
    typed_basis = getattr(sync, "phase_basis", None)
    if typed_basis in {"sweep_epoch_only", "anchor_relative", "geographic"}:
        phase_basis = typed_basis
    else:
        phase_anchor_icao: str | None = getattr(sync, "phase_anchor_icao", None)
        phase_anchor_status: str = str(getattr(sync, "phase_anchor_status", "") or "")
        if phase_anchor_icao and phase_anchor_status in {"selected", "anchor_only"}:
            phase_basis = "anchor_relative"
        else:
            phase_basis = "sweep_epoch_only"
    phase_anchor_icao: str | None = getattr(sync, "phase_anchor_icao", None)
    phase_anchor_status: str = str(getattr(sync, "phase_anchor_status", "") or "")
    return phase_basis, phase_anchor_icao, phase_anchor_status


def _make_empty(
    iid: int,
    status: str,
    reason: str | None,
    anchor_icao: str | None = None,
    observation_count: int = 0,
    eligible_observation_count: int = 0,
    window_s: float = 0.0,
    per_icao: list[dict] | None = None,
    contributing_icao_count: int = 0,
    anchor_residual_mean_deg: float | None = None,
    rejection_counts: dict[str, int] | None = None,
    phase_basis: str | None = None,
) -> PopulationResidualSummary:
    return PopulationResidualSummary(
        iid=iid,
        status=status,
        reason=reason,
        phase_basis=phase_basis,
        window_s=window_s,
        observation_count=observation_count,
        eligible_observation_count=eligible_observation_count,
        contributing_icao_count=contributing_icao_count,
        disagreeing_icao_count=0,
        anchor_icao=anchor_icao,
        anchor_residual_mean_deg=anchor_residual_mean_deg,
        population_residual_mean_deg=None,
        anchor_population_delta_deg=None,
        population_residual_spread_deg=None,
        worst_icao=None,
        worst_icao_delta_deg=None,
        per_icao=per_icao or [],
        rejection_counts=rejection_counts or {},
    )


# ── Public API ────────────────────────────────────────────────────────────────


def compute_population_residual_summary(
    entries: list[dict],
    sync,
    iid: int,
    freshness_gate_s: float = POS_FRESHNESS_GATE_S,
) -> PopulationResidualSummary:
    """Compute population-wide residual consistency summary from observation entries.

    Parameters
    ----------
    entries:
        Recomputed observation dicts from ``get_burst_sync_timeline``.
        Required keys: ``residual_deg``, ``icao``, ``pos_age_s``,
        ``sync_update_eligible``, ``classification``.
        Optional: ``wall_ts`` (used only for window_s span reporting),
        ``predicted_deg``, ``bearing_deg``.
    sync:
        LiveSyncState or None.  Used to derive phase_basis and anchor_icao.
    iid:
        IID being monitored.
    freshness_gate_s:
        Maximum ``pos_age_s`` for an observation to be eligible.
        Defaults to POS_FRESHNESS_GATE_S (8s).
    """
    if sync is None:
        return _make_empty(iid, "unavailable", "no_sync_state", observation_count=len(entries), phase_basis=None)

    phase_basis, phase_anchor_icao, phase_anchor_status = _derive_phase_basis_and_anchor(sync)

    if phase_basis not in _ELIGIBLE_PHASE_BASES:
        return _make_empty(
            iid, "unavailable", "phase_basis_unavailable",
            observation_count=len(entries), phase_basis=phase_basis,
        )

    total_count = len(entries)

    rejection_counts: dict[str, int] = {}

    eligible: list[dict] = []
    hard_rejected_count = 0
    for entry in entries:
        if not entry.get("sync_update_eligible"):
            rejection_counts["not_sync_update_eligible"] = rejection_counts.get("not_sync_update_eligible", 0) + 1
            continue
        residual_raw = entry.get("residual_deg")
        if residual_raw is None:
            rejection_counts["missing_residual"] = rejection_counts.get("missing_residual", 0) + 1
            continue
        try:
            residual = float(residual_raw)
        except (TypeError, ValueError):
            rejection_counts["invalid_residual"] = rejection_counts.get("invalid_residual", 0) + 1
            continue
        if not math.isfinite(residual):
            rejection_counts["invalid_residual"] = rejection_counts.get("invalid_residual", 0) + 1
            continue
        icao = entry.get("icao")
        if not icao:
            rejection_counts["missing_icao"] = rejection_counts.get("missing_icao", 0) + 1
            continue
        pos_age_raw = entry.get("pos_age_s")
        if pos_age_raw is None:
            rejection_counts["missing_pos_age"] = rejection_counts.get("missing_pos_age", 0) + 1
            continue
        try:
            pos_age = float(pos_age_raw)
        except (TypeError, ValueError):
            rejection_counts["missing_pos_age"] = rejection_counts.get("missing_pos_age", 0) + 1
            continue
        if not math.isfinite(pos_age) or pos_age > freshness_gate_s:
            rejection_counts["stale_position"] = rejection_counts.get("stale_position", 0) + 1
            continue
        cls = entry.get("classification")
        if cls == "rejected":
            hard_rejected_count += 1
            rejection_counts["hard_rejected"] = rejection_counts.get("hard_rejected", 0) + 1
            continue
        eligible.append(entry)

    if not eligible:
        if total_count == 0:
            reason = "no_observations"
        elif total_count == hard_rejected_count:
            reason = "all_observations_rejected"
        else:
            reason = "insufficient_observations"
        return _make_empty(
            iid, "insufficient_data", reason,
            anchor_icao=phase_anchor_icao,
            observation_count=total_count,
            rejection_counts=rejection_counts,
            phase_basis=phase_basis,
        )

    # ── Window span ───────────────────────────────────────────────────────
    timestamps = [
        float(e["wall_ts"])
        for e in eligible
        if e.get("wall_ts") is not None and math.isfinite(float(e.get("wall_ts") or "nan"))
    ]
    window_s = (max(timestamps) - min(timestamps)) if len(timestamps) >= 2 else 0.0

    # ── Group residuals by ICAO ───────────────────────────────────────────
    by_icao: dict[str, list[float]] = {}
    for entry in eligible:
        key = str(entry["icao"])
        by_icao.setdefault(key, []).append(float(entry["residual_deg"]))

    # ── Build per-ICAO rows (all ICAOs that appear in eligible obs) ───────
    all_per_icao: list[dict] = []
    contributing_summaries: list[dict] = []
    below_min_obs_count = 0
    for icao in sorted(by_icao, key=lambda k: -len(by_icao[k])):
        residuals = by_icao[icao]
        count = len(residuals)
        is_contributing = count >= MIN_OBS_PER_ICAO
        if not is_contributing:
            below_min_obs_count += 1
        mean = _circular_mean_residuals_deg(residuals) if is_contributing else None
        spread = _circular_spread_deg(residuals) if is_contributing and count >= 2 else None
        row: dict = {
            "icao": icao,
            "count": count,
            "residual_mean_deg": _round2(mean),
            "residual_spread_deg": _round2(spread),
            "is_anchor": icao == phase_anchor_icao,
            "contributing": is_contributing,
            "delta_from_anchor_deg": None,
            "disagrees": False,
        }
        all_per_icao.append(row)
        if is_contributing:
            contributing_summaries.append(row)

    contributing_icao_count = len(contributing_summaries)
    rejection_counts["below_min_obs_per_icao"] = below_min_obs_count

    if contributing_icao_count == 0:
        return _make_empty(
            iid, "insufficient_data", "insufficient_observations",
            anchor_icao=phase_anchor_icao,
            observation_count=total_count,
            eligible_observation_count=len(eligible),
            window_s=window_s,
            per_icao=all_per_icao,
            rejection_counts=rejection_counts,
            phase_basis=phase_basis,
        )

    # ── Anchor availability check ─────────────────────────────────────────
    anchor_summary = next((s for s in contributing_summaries if s["is_anchor"]), None)
    anchor_mean: float | None = anchor_summary["residual_mean_deg"] if anchor_summary else None

    if anchor_summary is None:
        return _make_empty(
            iid, "anchor_unavailable", "no_anchor",
            anchor_icao=phase_anchor_icao,
            observation_count=total_count,
            eligible_observation_count=len(eligible),
            window_s=window_s,
            per_icao=all_per_icao,
            contributing_icao_count=contributing_icao_count,
            rejection_counts=rejection_counts,
            phase_basis=phase_basis,
        )

    # ── Non-anchor count check ────────────────────────────────────────────
    non_anchor_summaries = [s for s in contributing_summaries if not s["is_anchor"]]
    if len(non_anchor_summaries) < MIN_NON_ANCHOR_ICAOS:
        return _make_empty(
            iid, "insufficient_data", "insufficient_non_anchor_icaos",
            anchor_icao=phase_anchor_icao,
            observation_count=total_count,
            eligible_observation_count=len(eligible),
            window_s=window_s,
            per_icao=all_per_icao,
            contributing_icao_count=contributing_icao_count,
            anchor_residual_mean_deg=anchor_mean,
            rejection_counts=rejection_counts,
            phase_basis=phase_basis,
        )

    # ── Population mean (non-anchor contributing ICAOs) ───────────────────
    pop_residuals: list[float] = []
    for s in non_anchor_summaries:
        pop_residuals.extend(by_icao[s["icao"]])
    population_mean = _circular_mean_residuals_deg(pop_residuals)
    population_spread = _circular_spread_deg(pop_residuals)

    # ── Anchor vs population delta ────────────────────────────────────────
    anchor_population_delta: float | None = _circular_delta_deg(anchor_mean, population_mean)

    # ── Per-ICAO deltas from anchor; identify disagreeing ICAOs ──────────
    disagreeing_count = 0
    worst_icao: str | None = None
    worst_delta: float | None = None
    for s in non_anchor_summaries:
        mean = s["residual_mean_deg"]
        delta = _circular_delta_deg(mean, anchor_mean)
        s["delta_from_anchor_deg"] = _round2(delta)
        disagrees = delta is not None and abs(delta) > DISAGREEMENT_THRESHOLD_DEG
        s["disagrees"] = disagrees
        if disagrees:
            disagreeing_count += 1
        if delta is not None and (worst_delta is None or abs(delta) > abs(worst_delta)):
            worst_icao = s["icao"]
            worst_delta = delta

    # ── Status determination ──────────────────────────────────────────────
    if (
        disagreeing_count >= MIN_DISAGREEING_ICAOS
        and anchor_population_delta is not None
        and abs(anchor_population_delta) >= DISAGREEMENT_THRESHOLD_DEG
    ):
        status = "population_disagrees"
        reason: str | None = "population_delta_exceeds_threshold"
    elif (
        (population_spread is not None and population_spread > DISAGREEMENT_THRESHOLD_DEG)
        or (0 < disagreeing_count < MIN_DISAGREEING_ICAOS)
    ):
        status = "population_mixed"
        reason = "mixed_population"
    else:
        status = "population_agrees"
        reason = None

    return PopulationResidualSummary(
        iid=iid,
        status=status,
        reason=reason,
        phase_basis=phase_basis,
        window_s=window_s,
        observation_count=total_count,
        eligible_observation_count=len(eligible),
        contributing_icao_count=contributing_icao_count,
        disagreeing_icao_count=disagreeing_count,
        anchor_icao=phase_anchor_icao,
        anchor_residual_mean_deg=anchor_mean,
        population_residual_mean_deg=_round2(population_mean),
        anchor_population_delta_deg=_round2(anchor_population_delta),
        population_residual_spread_deg=_round2(population_spread),
        worst_icao=worst_icao,
        worst_icao_delta_deg=_round2(worst_delta),
        per_icao=all_per_icao,
        rejection_counts=rejection_counts,
    )
