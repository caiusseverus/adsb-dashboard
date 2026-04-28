from __future__ import annotations

from collections import deque
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from .models import SweepFrame


def normalise_go_frame_position(entry: dict) -> dict | None:
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


def go_frame_position_signature(entry: dict) -> tuple:
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


def set_go_frame_positions_locked(
    state: Any,
    iid: int,
    entries: list[dict],
) -> bool:
    existing = list(state._go_frame_positions.get(iid, ()))
    existing_sig = tuple(go_frame_position_signature(entry) for entry in existing)
    next_sig = tuple(go_frame_position_signature(entry) for entry in entries)
    if existing_sig == next_sig:
        return False
    state._go_frame_positions[iid] = deque(entries, maxlen=state._GO_FRAME_POSITIONS_MAX)
    state._go_frame_positions_revision[iid] = state._go_frame_positions_revision.get(iid, 0) + 1
    return True


def normalise_go_multi_sync_admission(entry: dict) -> dict | None:
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


def normalise_go_anchor_candidates(entries: list | None) -> list[dict]:
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


def normalise_go_track_observation(entry: dict) -> dict | None:
    if not isinstance(entry, dict):
        return None
    try:
        return {
            "iid": int(entry["iid"]),
            "icao": f"{int(entry['icao']):06X}",
            "arrival_us": float(entry["arrival_us"]),
            "wall_ts": float(entry["wall_ts"]),
            "signal_dbfs": float(entry["signal_dbfs"]) if entry.get("signal_dbfs") is not None else None,
            "truth_lat": float(entry["truth_lat"]) if entry.get("truth_lat") is not None else None,
            "truth_lon": float(entry["truth_lon"]) if entry.get("truth_lon") is not None else None,
            "position_age_seconds": float(entry["position_age_s"]) if entry.get("position_age_s") is not None else None,
            "association_confidence": float(entry.get("association_confidence") or 0.0),
            "dominant_family": bool(entry.get("dominant_family")),
        }
    except Exception:
        return None


@dataclass
class GoSweepFrame:
    iid: int
    frame: Any


def normalise_go_sweep_frame(entry: dict) -> GoSweepFrame | None:
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
        return GoSweepFrame(iid=iid, frame=frame)
    except Exception:
        return None


def go_sweep_frame_signature(frames: list["SweepFrame"]) -> tuple:
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


def normalise_go_evidence_event(entry: dict) -> dict | None:
    if not isinstance(entry, dict):
        return None
    try:
        return {
            "kind": str(entry.get("kind") or "burst_fired"),
            "iid": int(entry["iid"]),
            "icao": f"{int(entry['icao']):06X}",
            "arrival_us": float(entry["arrival_us"]),
            "simple_centroid_us": float(entry["simple_centroid_us"]) if entry.get("simple_centroid_us") is not None else None,
            "weighted_centroid_us": float(entry["weighted_centroid_us"]) if entry.get("weighted_centroid_us") is not None else None,
            "centroid_delta_us": float(entry["centroid_delta_us"]) if entry.get("centroid_delta_us") is not None else None,
            "first_reply_us": float(entry["first_reply_us"]) if entry.get("first_reply_us") is not None else None,
            "strongest_reply_us": float(entry["strongest_reply_us"]) if entry.get("strongest_reply_us") is not None else None,
            "mid_strong_window_us": float(entry["mid_strong_window_us"]) if entry.get("mid_strong_window_us") is not None else None,
            "last_reply_us": float(entry["last_reply_us"]) if entry.get("last_reply_us") is not None else None,
            "span_us": float(entry["span_us"]) if entry.get("span_us") is not None else None,
            "peak_amplitude": float(entry["peak_amplitude"]) if entry.get("peak_amplitude") is not None else None,
            "wall_ts": float(entry["wall_ts"]),
            "n_replies": int(entry.get("n_replies") or 0),
            "signal_dbfs": float(entry["signal_dbfs"]) if entry.get("signal_dbfs") is not None else None,
            "truth_lat": float(entry["truth_lat"]) if entry.get("truth_lat") is not None else None,
            "truth_lon": float(entry["truth_lon"]) if entry.get("truth_lon") is not None else None,
            "position_age_seconds": float(entry["position_age_s"]) if entry.get("position_age_s") is not None else None,
            "association_confidence": float(entry.get("association_confidence") or 0.0),
            "dominant_family": bool(entry.get("dominant_family")),
        }
    except Exception:
        return None


def go_track_observation_snapshot(state: Any) -> list[dict]:
    with state._lock:
        return list(state._go_track_observations)


def go_evidence_event_snapshot(state: Any, iid: int | None = None) -> list[dict]:
    with state._lock:
        if iid is None:
            return list(state._go_evidence_events)
        return [entry for entry in state._go_evidence_events if int(entry.get("iid", -1)) == iid]


def go_multi_sync_admission_snapshot(state: Any, iid: int) -> dict | None:
    with state._lock:
        payload = state._go_multi_sync_admission_by_iid.get(iid)
        return dict(payload) if payload is not None else None


def prune_go_evidence_events_locked(state: Any, now_ts: float, retention_s: float) -> int:
    cutoff_ts = now_ts - retention_s
    pruned = 0
    while state._go_evidence_events and float(state._go_evidence_events[0].get("wall_ts") or 0.0) < cutoff_ts:
        state._go_evidence_events.popleft()
        pruned += 1
    return pruned
