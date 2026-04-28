from __future__ import annotations

import math as _math
import time
from typing import TYPE_CHECKING, Any

from .geo import _bearing_deg_simple, _haversine_nm_simple
from .motion_comp import _estimate_aircraft_bearing_rate
from .radar_position import get_authoritative_radar_position
from .sync_prediction import _compute_motion_comp_dt_us, _compute_propagation_delay_us, predict_sync_observation
from .sync_models import AlignedBurstSyncObs, LiveSyncState

try:
    from config import RADAR_SYNC_MOTION_COMP_PHASE_ENABLED, RADAR_SYNC_PROP_DELAY_ENABLED
except Exception:  # pragma: no cover
    RADAR_SYNC_MOTION_COMP_PHASE_ENABLED = True
    RADAR_SYNC_PROP_DELAY_ENABLED = True

if TYPE_CHECKING:
    pass


def summarise_live_sync_observation_buffer(obs_snapshot: list["AlignedBurstSyncObs"], max_entries: int) -> dict:
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


def build_live_sync_retention_diagnostics(
    state: Any,
    iid: int,
    aligned_snapshot: list["AlignedBurstSyncObs"],
    timeline_snapshot: list["AlignedBurstSyncObs"],
) -> dict:
    return {
        "iid": iid,
        "retention_target_s": state._LIVE_SYNC_OBS_RETENTION_S,
        "aligned": summarise_live_sync_observation_buffer(aligned_snapshot, state._MULTI_SYNC_OBS_MAX),
        "timeline": summarise_live_sync_observation_buffer(timeline_snapshot, state._BURST_SYNC_TIMELINE_OBS_MAX),
    }


def build_display_retention_diagnostic(
    state: Any,
    sync: "LiveSyncState | None",
    observations: list[dict],
    df11_residual_observations: list[dict],
    window_s: float,
) -> dict:
    now_ts = time.time()
    all_ts = []
    for entry in observations or []:
        ts = entry.get("wall_ts")
        if ts is not None:
            all_ts.append(float(ts))
    for entry in df11_residual_observations or []:
        ts = entry.get("wall_ts") or entry.get("estimated_wall_ts")
        if ts is not None:
            all_ts.append(float(ts))
    plotted_count = len(all_ts)
    oldest_age = (now_ts - min(all_ts)) if all_ts else None
    newest_age = (now_ts - max(all_ts)) if all_ts else None
    fit_window_s = state._fit_window_s_for_period(getattr(sync, "period_s", None) if sync is not None else None)
    return {
        "axis_window_s": float(window_s),
        "fit_window_s": float(fit_window_s),
        "display_window_s": float(window_s),
        "plotted_point_count": plotted_count,
        "oldest_point_age_s": oldest_age,
        "newest_point_age_s": newest_age,
        "backend_payload_point_count": plotted_count,
        "backend_payload_oldest_age_s": oldest_age,
        "frontend_buffer_window_s": float(window_s),
        "points_source": "backend_history" if plotted_count > 0 else "empty",
        "evidence_buffer_size": len(getattr(state, "_go_evidence_events", []) or []),
        "evidence_buffer_max": int(getattr(state, "_GO_EVIDENCE_EVENTS_MAX", 0) or 0),
    }


def build_compact_burst_sync_timeline_entries(
    state: Any,
    sync: "LiveSyncState",
    obs_snapshot: list["AlignedBurstSyncObs"],
    window_s: float,
) -> list[dict]:
    now_ts = time.time()
    cutoff_ts = now_ts - window_s
    entries: list[dict] = []
    for obs in obs_snapshot:
        if obs.ts < cutoff_ts:
            continue
        prediction = predict_sync_observation(
            sync,
            obs.burst_centroid_us,
            range_nm=getattr(obs, "range_nm", None),
            bearing_rate_deg_s=getattr(obs, "bearing_rate_deg_s", None),
            motion_comp_dt_us=getattr(obs, "motion_comp_dt_us", None),
            motion_comp_block_reason=getattr(obs, "motion_comp_block_reason", None),
        )
        residual_deg = (obs.bearing_deg - prediction.predicted_bearing_deg + 540.0) % 360.0 - 180.0
        classification = state._classify_sync_residual(abs(residual_deg))
        weight = state._score_sync_burst_observation(obs)
        fit_eligible = bool(getattr(obs, "sync_update_eligible", True)) and classification != "rejected" and weight > 0
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
            "implied_phase_offset_deg": None,
            "anchor_relative_phase_error_deg": None,
            "phase_anchor_contributor": False,
            "phase_anchor_reject_reason": None,
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
            "prediction_path": prediction.predictor_version,
            "weight": weight,
            "classification": classification,
            "fit_eligible": fit_eligible,
            "fit_reject_reason": None if fit_eligible else "compact_go_sync",
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


def build_compact_residual_observations_from_entries(entries: list[dict], source: str, on_time_threshold_deg: float) -> list[dict]:
    results: list[dict] = []
    for entry in entries:
        residual_deg = float(entry.get("residual_deg") or 0.0)
        abs_res = abs(residual_deg)
        if abs_res <= on_time_threshold_deg:
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


def build_compact_sync_debug_payload(state: Any, iid: int, sync: "LiveSyncState", burst_timeline: dict, limit: int) -> dict:
    observations = list(burst_timeline.get("observations") or [])
    if limit > 0:
        observations = observations[-limit:]
    motion_summary = burst_timeline.get("motion_comp_summary") or {}
    retention_diagnostics = burst_timeline.get("retention_diagnostics")
    fit_eligible_count = sum(1 for row in observations if row.get("fit_eligible"))
    sync_update_eligible_count = sum(1 for row in observations if row.get("sync_update_eligible"))
    alignment_status = burst_timeline.get("alignment_status")
    return {
        "iid": iid,
        "available": True,
        "observations": observations,
        "summary": {
            "iid": iid,
            "sync_source": getattr(sync, "source", None),
            "diagnostics_mode": "compact_go_sync",
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
            "mode": "compact_go_sync",
            "reason": "rich_python_multi_aircraft_diagnostics_unavailable_for_sync_source",
        },
    }


def build_sync_mode_diagnostics(state: Any, iid: int, sync: "LiveSyncState | None", alignment_status: dict | None) -> dict:
    with state._lock:
        compact_debug = dict(state._compact_sync_debug_by_iid.get(iid) or {})
        go_admission = dict(state._go_multi_sync_admission_by_iid.get(iid) or {})
    sync_source = getattr(sync, "source", None) if sync is not None else None
    refined_active = bool(sync_source == "multi_aircraft_burst")
    active_mode = "refined_multi_aircraft" if refined_active else "compact_bootstrap"
    active_label = "Python live sync" if refined_active else "Compact sync"
    if refined_active:
        no_anchor_reason = getattr(sync, "phase_anchor_no_candidate_reason", None)
        if not getattr(sync, "phase_anchor_icao", None) and not no_anchor_reason:
            no_anchor_reason = "no_anchor_selected"
    else:
        no_anchor_reason = ((alignment_status or {}).get("reason")) or "awaiting_refined_multi_sync"
    return {
        "active_source": sync_source,
        "active_mode": active_mode,
        "active_label": active_label,
        "compact": {
            "active": not refined_active,
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
        "python_sync": {
            "present": refined_active,
            "active": refined_active,
            "usable": bool(sync is not None and refined_active and getattr(sync, "usable", False)),
            "period_s": getattr(sync, "period_s", None) if refined_active else None,
            "anchor_icao": getattr(sync, "phase_anchor_icao", None) if refined_active else None,
            "anchor_candidate_count": int(getattr(sync, "phase_anchor_candidate_count", 0) or 0) if refined_active else 0,
            "fit_total_observations": int(getattr(sync, "fit_total_observations", 0) or 0) if refined_active else 0,
            "fit_eligible_observations": int(getattr(sync, "fit_eligible_observations", 0) or 0) if refined_active else 0,
            "fit_rejected_observations": int(getattr(sync, "fit_rejected_observations", 0) or 0) if refined_active else 0,
            "phase_status": getattr(sync, "phase_status", None) if refined_active else None,
            "no_anchor_reason": no_anchor_reason,
        },
        "go_admission": go_admission or None,
    }


def apply_selected_anchor_relative_offsets(circular_delta_deg, rows: list[dict], anchor_icao: str | None) -> float | None:
    if not anchor_icao:
        return None
    latest_anchor: dict | None = None
    latest_ts = float("-inf")
    for row in rows:
        if row.get("icao") != anchor_icao:
            continue
        implied = row.get("implied_phase_offset_deg")
        try:
            implied_value = float(implied)
        except (TypeError, ValueError):
            continue
        if not _math.isfinite(implied_value):
            continue
        ts_candidates = (
            row.get("beam_center_us"),
            row.get("raw_arrival_us"),
            row.get("effective_beast_us"),
            row.get("wall_ts"),
        )
        ts = next((float(value) for value in ts_candidates if value is not None and _math.isfinite(float(value))), 0.0)
        if latest_anchor is None or ts >= latest_ts:
            latest_anchor = row
            latest_ts = ts
    if latest_anchor is None:
        return None
    anchor_offset = float(latest_anchor["implied_phase_offset_deg"])
    for row in rows:
        implied = row.get("implied_phase_offset_deg")
        try:
            implied_value = float(implied)
        except (TypeError, ValueError):
            row["anchor_relative_phase_error_deg"] = None
            continue
        if not _math.isfinite(implied_value):
            row["anchor_relative_phase_error_deg"] = None
            continue
        row["anchor_relative_phase_error_deg"] = circular_delta_deg(implied_value, anchor_offset)
    latest_anchor["anchor_relative_phase_error_deg"] = 0.0
    return anchor_offset


def build_df11_residual_observations(
    state: Any,
    sync: "LiveSyncState",
    iid_events: list[tuple[float, int, str, float | None]],
    latest_arrival_us: float | None,
    on_time_threshold_deg: float,
) -> list[dict]:
    if sync is None or not sync.usable:
        return []
    receiver_lat = state._receiver_lat
    receiver_lon = state._receiver_lon
    if receiver_lat is None or receiver_lon is None:
        return []
    results: list[dict] = []
    for arrival_us, _iid, icao, signal_dbfs in iid_events:
        wall_ts = state._estimate_wall_time_from_arrival_us(arrival_us, latest_arrival_us)
        if wall_ts is None:
            continue
        pos = state._adsb_tracker.get_position_at(icao, wall_ts)
        if pos is None or pos.get("lat") is None or pos.get("lon") is None:
            continue
        truth_lat = pos["lat"]
        truth_lon = pos["lon"]
        bearing_deg = _bearing_deg_simple(receiver_lat, receiver_lon, truth_lat, truth_lon)
        range_nm = _haversine_nm_simple(receiver_lat, receiver_lon, truth_lat, truth_lon)
        prediction = predict_sync_observation(
            sync,
            arrival_us,
            range_nm=range_nm,
            bearing_rate_deg_s=None,
            motion_comp_dt_us=None,
            motion_comp_block_reason="individual_df11_arrival",
        )
        residual_deg = (bearing_deg - prediction.predicted_bearing_deg + 540.0) % 360.0 - 180.0
        abs_res = abs(residual_deg)
        if abs_res <= on_time_threshold_deg:
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
            "fit_eligible": False,
            "fit_reject_reason": "individual_df11_arrival",
            "residual_source": "df11",
        })
    return results


def go_burst_sync_timeline_snapshot(state: Any, iid: int, window_s: float) -> list["AlignedBurstSyncObs"]:
    evidence = state._go_evidence_event_snapshot(iid)
    if not evidence:
        return []
    with state._lock:
        model = state._models.get(iid)
        sync = state._live_sync_states.get(iid)
    radar_pos = get_authoritative_radar_position(model) if model is not None else {"lat": None, "lon": None}
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
        motion_applied = bool(RADAR_SYNC_MOTION_COMP_PHASE_ENABLED and motion_comp_dt_us is not None and motion_block_reason is None)
        if not RADAR_SYNC_MOTION_COMP_PHASE_ENABLED:
            motion_block_reason = "disabled"
        elif motion_comp_dt_us is None and motion_block_reason is None:
            motion_block_reason = "bearing_rate_unavailable"
        prop_delay_us = _compute_propagation_delay_us(range_nm)
        prop_corrected_us = float(entry["arrival_us"]) - prop_delay_us if RADAR_SYNC_PROP_DELAY_ENABLED else float(entry["arrival_us"])
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
            sync_update_eligible=bool(entry.get("go_compact_timing_candidate", entry.get("go_timing_candidate", False))),
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
            burst_center_method="amplitude_weighted" if entry.get("weighted_centroid_us") is not None else "centroid",
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
            truth_position_ts_beast_us=float(entry["arrival_us"]) - pos_age_s * 1_000_000.0 if pos_age_s > 0 else float(entry["arrival_us"]),
        ))
    return observations


def go_sweep_frame_sync_timeline_snapshot(state: Any, iid: int, window_s: float) -> list["AlignedBurstSyncObs"]:
    with state._lock:
        go_frames = list(state._go_sweep_frames_by_iid.get(iid, ()))
        model = state._models.get(iid)
        latest_arrival_us = state._iid_latest_arrival_us.get(iid)
        sync = state._live_sync_states.get(iid)
    if not go_frames:
        return []
    radar_pos = get_authoritative_radar_position(model) if model is not None else {"lat": None, "lon": None}
    radar_lat = radar_pos.get("lat")
    radar_lon = radar_pos.get("lon")
    if radar_lat is None or radar_lon is None:
        return []
    cutoff_ts = time.time() - window_s
    observations: list[AlignedBurstSyncObs] = []
    period_s = sync.period_s if sync is not None and sync.period_s > 0 else 0.0
    for frame in go_frames:
        frame_rows = [(frame.ref_icao, frame.ref_arrival_us, frame.ref_lat, frame.ref_lon, 1, getattr(frame, "ref_pos_age_s", 0.0))]
        frame_rows.extend((obs.icao, obs.arrival_us, obs.lat, obs.lon, getattr(obs, "n_replies", 1), getattr(obs, "position_age_seconds", 0.0)) for obs in frame.observations)
        for icao, arrival_us, lat, lon, n_replies, pos_age_s in frame_rows:
            wall_ts = state._estimate_wall_time_from_arrival_us(float(arrival_us), latest_arrival_us)
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
            motion_applied = bool(RADAR_SYNC_MOTION_COMP_PHASE_ENABLED and motion_comp_dt_us is not None and motion_block_reason is None)
            if not RADAR_SYNC_MOTION_COMP_PHASE_ENABLED:
                motion_block_reason = "disabled"
            elif motion_comp_dt_us is None and motion_block_reason is None:
                motion_block_reason = "bearing_rate_unavailable"
            prop_delay_us = _compute_propagation_delay_us(range_nm)
            prop_corrected_us = float(arrival_us) - prop_delay_us if RADAR_SYNC_PROP_DELAY_ENABLED else float(arrival_us)
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
                truth_position_ts_beast_us=float(arrival_us) - float(pos_age_s or 0.0) * 1_000_000.0 if float(pos_age_s or 0.0) > 0 else float(arrival_us),
            ))
    observations.sort(key=lambda obs: (obs.ts, obs.burst_centroid_us, obs.icao))
    return observations
