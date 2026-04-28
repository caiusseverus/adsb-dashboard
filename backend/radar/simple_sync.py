from __future__ import annotations

import math as _math
import statistics
import time
from collections import defaultdict, deque
from typing import TYPE_CHECKING

from .angular import _circular_delta_deg
from .sync_prediction import predict_sync_observation
from .sync_models import LiveSyncState
from .sync_quality import _icao_quality_reject_reason, _update_icao_sync_quality_memory

try:
    from config import (
        RADAR_SYNC_MOTION_COMP_FIT_ENABLED,
        RADAR_SYNC_MOTION_COMP_PHASE_ENABLED,
        RADAR_SYNC_PERIOD_REFINE_ENABLED,
        RADAR_SYNC_PROP_DELAY_ENABLED,
    )
except Exception:  # pragma: no cover - config not importable in some test harnesses
    RADAR_SYNC_PERIOD_REFINE_ENABLED = True
    RADAR_SYNC_PROP_DELAY_ENABLED = True
    RADAR_SYNC_MOTION_COMP_PHASE_ENABLED = True
    RADAR_SYNC_MOTION_COMP_FIT_ENABLED = True

if TYPE_CHECKING:
    from .sweep import RadarState


_SIMPLE_SYNC_FIT_WINDOW_ROTATIONS = 6.0
_SIMPLE_SYNC_FIT_WINDOW_MIN_S = 30.0


def _fit_weighted_slope(xs: list[float], ys: list[float], ws: list[float]) -> tuple[float, float]:
    """Weighted least-squares linear fit y = a + b*x."""
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


def _is_finite_number(value: object) -> bool:
    return isinstance(value, (int, float)) and _math.isfinite(float(value))


def _fit_per_aircraft_slope(scored: list[dict], period_base_s: float) -> dict:
    """Fit residual slope per ICAO using unwrapped timelines; return consensus only when >=2 ICAOs agree."""
    _PER_ICAO_MIN_OBS = 3
    _PER_ICAO_MIN_SPAN_ROT = 2.0
    _AMBIGUOUS_UNWRAP_THRESHOLD_DEG = 160.0
    _SLOPE_MAGNITUDE_RATIO_MAX = 4.0
    _SLOPE_ABS_FLOOR_DEG_S = 0.01

    had_nonfinite = False
    valid: list[dict] = []
    for e in scored:
        icao = e.get("icao", "")
        if not icao:
            continue
        eff = e.get("effective_us", None)
        res = e.get("residual", None)
        w = e.get("weight", None)
        if not (_is_finite_number(eff) and _is_finite_number(res) and _is_finite_number(w)):
            had_nonfinite = True
            continue
        if w <= 0:
            continue
        valid.append({**e, "effective_us": float(eff), "residual": float(res), "weight": float(w)})

    by_icao: dict[str, list[dict]] = {}
    for e in valid:
        by_icao.setdefault(e["icao"], []).append(e)

    _null = dict(
        slope_deg_per_s=None,
        icao_slopes={},
        icao_count=0,
        sign_agreement=False,
        fit_span_s=0.0,
        per_icao_reject_reasons={},
    )
    if len(by_icao) < 2:
        reason = "nonfinite_input" if had_nonfinite else "insufficient_icaos"
        return {**_null, "reject_reason": reason}

    icao_slopes: dict[str, float] = {}
    per_icao_reject: dict[str, str] = {}
    t_all_min = float("inf")
    t_all_max = float("-inf")

    for icao, entries in by_icao.items():
        if len(entries) < _PER_ICAO_MIN_OBS:
            per_icao_reject[icao] = "insufficient_icao_observations"
            continue

        entries_sorted = sorted(entries, key=lambda e: e["effective_us"])
        ts_s = [e["effective_us"] / 1_000_000.0 for e in entries_sorted]
        span = ts_s[-1] - ts_s[0]

        if span < _PER_ICAO_MIN_SPAN_ROT * period_base_s:
            per_icao_reject[icao] = "insufficient_icao_span"
            continue

        residuals_raw = [e["residual"] for e in entries_sorted]
        unwrapped = [residuals_raw[0]]
        prev = residuals_raw[0]
        ambiguous = False
        for raw in residuals_raw[1:]:
            delta = (raw - prev + 540.0) % 360.0 - 180.0
            if abs(delta) > _AMBIGUOUS_UNWRAP_THRESHOLD_DEG:
                ambiguous = True
                break
            unwrapped.append(unwrapped[-1] + delta)
            prev = raw

        if ambiguous:
            per_icao_reject[icao] = "ambiguous_unwrap"
            continue

        assert len(unwrapped) == len(ts_s)
        t_ref = ts_s[0]
        xs = [t - t_ref for t in ts_s]
        ws = [e["weight"] for e in entries_sorted]
        _, slope = _fit_weighted_slope(xs, unwrapped, ws)

        icao_slopes[icao] = slope
        t_all_min = min(t_all_min, ts_s[0])
        t_all_max = max(t_all_max, ts_s[-1])

    if len(icao_slopes) < 2:
        reason = "nonfinite_input" if had_nonfinite else "insufficient_icaos"
        return {
            **_null,
            "icao_slopes": icao_slopes,
            "icao_count": len(icao_slopes),
            "per_icao_reject_reasons": per_icao_reject,
            "reject_reason": reason,
        }

    fit_span_s = t_all_max - t_all_min if t_all_max > t_all_min else 0.0

    signs = [1 if s > 0 else -1 if s < 0 else 0 for s in icao_slopes.values()]
    nonzero = [s for s in signs if s != 0]
    if len(nonzero) < 2 or len(set(nonzero)) != 1:
        return {
            **_null,
            "icao_slopes": icao_slopes,
            "icao_count": len(icao_slopes),
            "fit_span_s": fit_span_s,
            "per_icao_reject_reasons": per_icao_reject,
            "reject_reason": "sign_disagreement",
        }

    magnitudes = [abs(s) for s in icao_slopes.values()]
    sorted_mags = sorted(magnitudes)
    med_mag = sorted_mags[len(sorted_mags) // 2]
    if med_mag < _SLOPE_ABS_FLOOR_DEG_S:
        magnitude_disagrees = any(m > _SLOPE_ABS_FLOOR_DEG_S for m in magnitudes)
    else:
        magnitude_disagrees = any(
            m > _SLOPE_MAGNITUDE_RATIO_MAX * med_mag
            or m < med_mag / _SLOPE_MAGNITUDE_RATIO_MAX
            for m in magnitudes
        )
    if magnitude_disagrees:
        return {
            **_null,
            "icao_slopes": icao_slopes,
            "icao_count": len(icao_slopes),
            "fit_span_s": fit_span_s,
            "per_icao_reject_reasons": per_icao_reject,
            "reject_reason": "slope_magnitude_disagreement",
        }

    sorted_slopes = sorted(icao_slopes.values())
    n = len(sorted_slopes)
    consensus = (
        sorted_slopes[n // 2]
        if n % 2
        else (sorted_slopes[n // 2 - 1] + sorted_slopes[n // 2]) / 2.0
    )

    return {
        "slope_deg_per_s": consensus,
        "icao_slopes": icao_slopes,
        "icao_count": len(icao_slopes),
        "sign_agreement": True,
        "fit_span_s": fit_span_s,
        "reject_reason": None,
        "per_icao_reject_reasons": per_icao_reject,
    }


def fit_window_s_for_period(period_s: float | None) -> float:
    try:
        period = float(period_s or 0.0)
    except (TypeError, ValueError):
        period = 0.0
    return max(period * _SIMPLE_SYNC_FIT_WINDOW_ROTATIONS, _SIMPLE_SYNC_FIT_WINDOW_MIN_S)


def update_simple_live_sync_state(
    state: "RadarState",
    iid: int,
    period_s: float,
    sync_quality: float | None = None,
) -> None:
    existing = state._live_sync_states.get(iid)
    if existing is None:
        return

    base_period_s = existing.period_base_s if existing.period_base_s > 0 else period_s
    if base_period_s <= 0:
        return
    live_period_s = existing.period_s if existing.period_s > 0 else base_period_s
    period_us = live_period_s * 1_000_000.0
    now_ts = time.time()

    window_s = fit_window_s_for_period(base_period_s)
    cutoff_ts = now_ts - window_s
    obs_buf = state._live_aligned_burst_obs.get(iid)
    if not obs_buf:
        return
    obs_snapshot = list(obs_buf)

    recent_obs = [o for o in obs_snapshot if o.ts >= cutoff_ts]
    if len(recent_obs) < 3:
        return

    icao_quality = state._live_icao_sync_quality.setdefault(iid, {})

    scored: list[dict] = []
    fit_reject_reasons: dict[str, int] = defaultdict(int)
    for obs in recent_obs:
        pred = predict_sync_observation(
            existing,
            obs.burst_centroid_us,
            range_nm=obs.range_nm,
            apply_propagation=True,
            apply_motion=bool(RADAR_SYNC_MOTION_COMP_FIT_ENABLED),
            bearing_rate_deg_s=getattr(obs, "bearing_rate_deg_s", None),
            motion_comp_dt_us=getattr(obs, "motion_comp_dt_us", None),
            motion_comp_block_reason=getattr(obs, "motion_comp_block_reason", None),
        )
        residual = (obs.bearing_deg - pred.predicted_bearing_deg + 540.0) % 360.0 - 180.0
        abs_r = abs(residual)
        status = state._classify_sync_residual(abs_r)
        base_w = state._score_sync_burst_observation(obs)
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

        scored.append(
            {
                "residual": residual,
                "weight": effective_w,
                "anchor_weight": base_w * q_multiplier,
                "status": status,
                "icao": obs.icao,
                "effective_us": pred.effective_arrival_us,
                "phase_in_rot": pred.phase_in_rot_deg,
                "obs_ts": obs.ts,
                "fit_eligible": fit_eligible,
                "fit_reject_reason": fit_reject_reason,
                "prediction": pred,
                "obs": obs,
            }
        )

    contributing_icaos = {
        e["icao"] for e in scored if e["weight"] > 0 and e["status"] != "rejected"
    }
    n_inliers = sum(1 for e in scored if e["status"] == "inlier")
    n_rejected = sum(1 for e in scored if e["status"] == "rejected")
    fit_scored = [
        e for e in scored if e["fit_eligible"] and e["weight"] > 0 and e["status"] != "rejected"
    ]
    fit_contributing_icaos = {e["icao"] for e in fit_scored}

    anchor_pool = [
        e
        for e in scored
        if e.get("anchor_weight", 0.0) > 0.0 and e.get("fit_reject_reason") != "near_wrap_residual"
    ]
    if not anchor_pool:
        existing.holdover = True
        existing.usable = False
        return

    new_epoch_us = max(e["effective_us"] for e in anchor_pool)
    existing_at_new = (
        (new_epoch_us - existing.phase_epoch_us) / period_us * 360.0 + existing.phase_offset_deg
    ) % 360.0

    anchor_resolution = state._resolve_phase_anchor_state(
        iid=iid,
        scored=scored,
        existing=existing,
        now_ts=now_ts,
        epoch_us=new_epoch_us,
        mixed_fallback_offset=existing_at_new,
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

    phase_anchor_spread_val = phase_anchor_spread if phase_anchor_spread is not None else 999.0
    v_status = validation["status"]
    v_median_err = validation.get("median_error_deg")
    if v_median_err is None:
        v_median_err = 999.0
    contributor_icaos_list = validation.get("contributor_icaos")
    v_icao_count = len(contributor_icaos_list) if contributor_icaos_list else 0
    if (
        phase_anchor_status == "selected"
        and phase_anchor_spread_val < 8.0
        and v_icao_count >= 2
        and v_status not in {"population_disagrees", "population_veto"}
        and abs(v_median_err) < 10.0
    ):
        phase_status = "trusted"
        phase_status_reason = None
    elif (
        phase_anchor_status in {"selected", "anchor_only"}
        and phase_anchor_spread_val < 25.0
        and phase_anchor_obs_count >= 2
    ):
        phase_status = "provisional"
        phase_status_reason = (
            f"spread_{phase_anchor_spread_val:.1f}_deg"
            if phase_anchor_spread_val >= 8.0
            else f"icaos_{v_icao_count}"
            if v_icao_count < 2
            else v_status
        )
    else:
        phase_status = "untrusted"
        phase_status_reason = (
            phase_anchor_status
            if phase_anchor_status not in {"selected", "anchor_only"}
            else f"spread_{phase_anchor_spread_val:.1f}_deg"
        )

    _PERIOD_PPM_PER_UPDATE_MAX = 60.0
    _PERIOD_PPM_FROM_BASE_MAX = 500.0
    _SLOPE_EMA_ALPHA = 0.08
    _SLOPE_DEAD_BAND = 0.08
    _PERIOD_GAIN = 0.12
    _PERIOD_REFINE_MIN_INLIERS = 6
    _SLOPE_HISTORY_MAX = 80

    per_aircraft_result = _fit_per_aircraft_slope(fit_scored, base_period_s)
    per_aircraft_slope = per_aircraft_result["slope_deg_per_s"]

    smoothed_slope = existing.residual_slope_deg_per_s
    if per_aircraft_slope is not None:
        smoothed_slope = (
            (1.0 - _SLOPE_EMA_ALPHA) * smoothed_slope + _SLOPE_EMA_ALPHA * per_aircraft_slope
        )
    period_update_block_reason = (
        per_aircraft_result.get("reject_reason") if per_aircraft_slope is None else None
    )

    slope_history = state._live_slope_history.setdefault(iid, deque(maxlen=_SLOPE_HISTORY_MAX))
    slope_history.append(
        {
            "ts": now_ts,
            "residual_slope_deg_per_s": smoothed_slope,
            "raw_slope_deg_per_s": per_aircraft_slope,
            "slope_source": "per_aircraft_consensus" if per_aircraft_slope is not None else "none",
            "fit_span_s": per_aircraft_result.get("fit_span_s", 0.0),
            "n_fit_observations": len(fit_scored),
        }
    )
    recent_window = list(slope_history)[-8:]
    tagged_entries = [e for e in recent_window if e.get("slope_source") == "per_aircraft_consensus"]
    persist_signs = [
        1
        if e["residual_slope_deg_per_s"] > _SLOPE_DEAD_BAND
        else -1
        if e["residual_slope_deg_per_s"] < -_SLOPE_DEAD_BAND
        else 0
        for e in tagged_entries
    ]
    nonzero_signs = [s for s in persist_signs if s != 0]
    persistent_slope = (
        len(nonzero_signs) >= 5
        and len(set(nonzero_signs)) == 1
        and abs(smoothed_slope) >= _SLOPE_DEAD_BAND
        and per_aircraft_slope is not None
    )
    if not persistent_slope and period_update_block_reason is None:
        period_update_block_reason = "slope_not_persistent"

    fit_span_s = (
        (
            max(e["effective_us"] for e in fit_scored) - min(e["effective_us"] for e in fit_scored)
        )
        / 1_000_000.0
        if len(fit_scored) >= 2
        else 0.0
    )
    period_update_allowed = (
        bool(RADAR_SYNC_PERIOD_REFINE_ENABLED)
        and persistent_slope
        and n_inliers >= _PERIOD_REFINE_MIN_INLIERS
        and len(fit_contributing_icaos) >= 2
        and fit_span_s >= 2.0 * base_period_s
    )
    if not period_update_allowed and period_update_block_reason is None:
        period_update_block_reason = "period_refine_disabled"

    refined_period_s = live_period_s
    period_update_applied = 0.0
    period_update_ppm_applied = 0.0
    period_update_ppm_from_base = (
        (live_period_s - base_period_s) / base_period_s * 1e6 if base_period_s > 0 else 0.0
    )
    period_update_ppm_step = 0.0
    period_update_clamp_reason = None

    if period_update_allowed:
        rate_nominal = 360.0 / base_period_s
        rate_target = rate_nominal + smoothed_slope * _PERIOD_GAIN
        if rate_target <= 0:
            period_update_allowed = False
            period_update_block_reason = "non_positive_rate_target"
        else:
            target_from_base_s = 360.0 / rate_target
            step_limit_s = abs(live_period_s) * _PERIOD_PPM_PER_UPDATE_MAX * 1e-6
            stepped_period_s = live_period_s + max(
                -step_limit_s,
                min(step_limit_s, target_from_base_s - live_period_s),
            )
            base_limit_s = abs(base_period_s) * _PERIOD_PPM_FROM_BASE_MAX * 1e-6
            refined_period_s = max(
                base_period_s - base_limit_s,
                min(base_period_s + base_limit_s, stepped_period_s),
            )
            period_update_applied = refined_period_s - live_period_s
            period_update_ppm_applied = (
                period_update_applied / live_period_s * 1e6 if live_period_s > 0 else 0.0
            )
            period_update_ppm_from_base = (
                (refined_period_s - base_period_s) / base_period_s * 1e6 if base_period_s > 0 else 0.0
            )
            period_update_ppm_step = (
                (stepped_period_s - live_period_s) / live_period_s * 1e6 if live_period_s > 0 else 0.0
            )
            period_update_clamp_reason = (
                "base_ppm_clamped"
                if abs(period_update_ppm_from_base) >= _PERIOD_PPM_FROM_BASE_MAX * 0.99
                else "per_update_clamped"
                if abs(stepped_period_s - target_from_base_s) > 1e-9
                else None
            )

    period_correction_ppm = (
        (refined_period_s - base_period_s) / base_period_s * 1e6 if base_period_s > 0 else 0.0
    )
    period_correction_status = (
        "holding"
        if not period_update_allowed
        else "clamp_limited"
        if period_update_clamp_reason is not None
        else "converging"
    )

    inlier_abs = [abs(e["residual"]) for e in scored if e["status"] == "inlier"]
    if len(inlier_abs) >= 3:
        try:
            new_jitter = min(max(statistics.stdev(inlier_abs), 1.5), 15.0)
        except statistics.StatisticsError:
            new_jitter = existing.sync_jitter_deg
    elif inlier_abs:
        new_jitter = min(max(statistics.mean(inlier_abs), 1.5), 15.0)
    else:
        new_jitter = min(existing.sync_jitter_deg * 1.1, 15.0)

    _RESIDUAL_EMA_ALPHA = 0.2
    new_ema = (1.0 - _RESIDUAL_EMA_ALPHA) * existing.residual_ema_deg + _RESIDUAL_EMA_ALPHA * abs(
        phase_correction
    )
    q = sync_quality if sync_quality is not None else existing.sync_quality

    anchor_ok = (
        anchor_solution is not None
        and phase_anchor_icao is not None
        and phase_anchor_obs_count >= 3
        and (phase_anchor_spread is None or phase_anchor_spread <= 18.0)
        and phase_anchor_status in ("selected", "anchor_only")
    )
    new_usable = (
        (anchor_ok or (n_inliers >= 3 and len(contributing_icaos) >= 2))
        and new_jitter < 15.0
        and (anchor_ok or n_rejected < len(recent_obs) // 2 + 1)
    )

    _update_icao_sync_quality_memory(icao_quality, scored)

    period_authoritative_source = "refined" if period_update_allowed else "base"

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
        period_update_allowed=period_update_allowed,
        period_update_block_reason=period_update_block_reason,
        period_update_clamp_reason=period_update_clamp_reason,
        period_update_applied=period_update_applied,
        period_update_ppm_applied=period_update_ppm_applied,
        period_update_ppm_from_base=period_update_ppm_from_base,
        period_update_ppm_step=period_update_ppm_step,
        period_update_ppm_limit_from_base=_PERIOD_PPM_FROM_BASE_MAX,
        period_update_ppm_limit_step=_PERIOD_PPM_PER_UPDATE_MAX,
        period_update_safety_ppm_from_base=_PERIOD_PPM_FROM_BASE_MAX,
        period_update_safety_ppm_per_update=_PERIOD_PPM_PER_UPDATE_MAX,
        period_update_fit_support=n_inliers,
        period_update_fit_span_s=fit_span_s,
        period_correction_status=period_correction_status,
        period_authoritative_source=period_authoritative_source,
        phase_status=phase_status,
        phase_status_reason=phase_status_reason,
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
        phase_anchor_candidates=anchor_selection.get("candidates") or [],
        phase_validation_contributors=validation["contributor_count"],
        phase_validation_reject_count=validation["reject_count"],
        phase_validation_median_error_deg=validation.get("median_error_deg"),
        phase_validation_status=validation["status"],
        fit_time_basis="effective_beast_time_s",
        fit_residual_basis="observed_minus_predicted_after_prop_motion_deg",
        fit_total_observations=len(recent_obs),
        fit_eligible_observations=len(fit_scored),
        fit_rejected_observations=n_rejected,
        fit_reject_reasons=dict(fit_reject_reasons),
        fit_contributing_icao_count=len(fit_contributing_icaos),
        fit_span_s=fit_span_s,
        prop_delay_enabled=bool(RADAR_SYNC_PROP_DELAY_ENABLED),
        motion_comp_phase_enabled=bool(RADAR_SYNC_MOTION_COMP_PHASE_ENABLED),
        motion_comp_fit_enabled=bool(RADAR_SYNC_MOTION_COMP_FIT_ENABLED),
    )
    state._live_sync_states[iid] = new_state

    state._live_period_history.setdefault(iid, deque(maxlen=80)).append(
        {
            "ts": now_ts,
            "period_s": refined_period_s,
            "period_base_s": base_period_s,
            "period_correction_ppm": period_correction_ppm,
            "period_correction_status": period_correction_status,
        }
    )
