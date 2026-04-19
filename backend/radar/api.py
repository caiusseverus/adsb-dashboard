"""
radar/api.py — FastAPI router for passive radar endpoints.

All endpoints are independent — each stage's data is queryable regardless
of whether later stages have converged.
"""

from __future__ import annotations

import asyncio
import logging
import math
import time
from collections import deque
from types import SimpleNamespace
from typing import Callable, Optional, TYPE_CHECKING

from fastapi import APIRouter, Query
from pydantic import BaseModel, Field
from .sweep import (
    PRIMARY_CONFIDENCE_TARGET,
    SECONDARY_CONFIDENCE_TARGET,
    _confidence_from_support,
    detect_bursts,
)
from .localiser import _bearing_deg, _haversine_m

if TYPE_CHECKING:
    from .sweep import RadarState

log = logging.getLogger(__name__)

router = APIRouter(prefix="/api/radar", tags=["radar"])

# Populated by main.py after instantiation
_state: Optional["RadarState"] = None
_radar_core_stats_provider: Optional[Callable[[], dict]] = None

# Shared ForwardModel instance — airports are loaded once and cached.
# Imported lazily so the module can load without scipy installed.
_fm: Optional[object] = None
api_timings: deque[dict] = deque(maxlen=400)


def register_radar_core_stats_provider(provider: Callable[[], dict]) -> None:
    """Injected by main.py so radar endpoints can include live radar-core stats."""
    global _radar_core_stats_provider
    _radar_core_stats_provider = provider


def _record_api_timing(endpoint: str, started_at: float) -> None:
    api_timings.append({
        "endpoint": endpoint,
        "elapsed_ms": round((time.perf_counter() - started_at) * 1000, 2),
        "ts_s": time.time(),
    })


# TTL cache for expensive diagnostic endpoints keyed by "iid:type".
# Calibration pairs accumulate at most once per 30s maintenance cycle, so
# caching for 30s eliminates redundant computation on rapid IID switches and
# the 15s frontend poll without staleness risk.
_diag_cache: dict[str, tuple[float, dict]] = {}
_DIAG_CACHE_TTL_S = 30.0


def _get_diag_cache(key: str) -> dict | None:
    entry = _diag_cache.get(key)
    if entry is None:
        return None
    expire_ts, result = entry
    if time.monotonic() > expire_ts:
        _diag_cache.pop(key, None)
        return None
    return result


def _set_diag_cache(key: str, result: dict) -> None:
    _diag_cache[key] = (time.monotonic() + _DIAG_CACHE_TTL_S, result)


class ManualPositionPayload(BaseModel):
    lat: float = Field(..., ge=-90.0, le=90.0)
    lon: float = Field(..., ge=-180.0, le=180.0)
    note: Optional[str] = Field(default=None, max_length=500)


class UnresolvablePayload(BaseModel):
    reason: Optional[str] = Field(default=None, max_length=500)


def _get_fm():
    global _fm
    if _fm is None:
        from radar.forward_model import ForwardModel
        _fm = ForwardModel()
    return _fm


def _readiness_from_confidence(confidence: float, period_s: float | None) -> str:
    if period_s is None:
        return "INSUFFICIENT"
    if confidence >= 0.75:
        return "ESTABLISHED"
    if confidence >= 0.35:
        return "PROVISIONAL"
    if confidence > 0.0:
        return "TENTATIVE"
    return "INSUFFICIENT"


def _authoritative_position(model) -> dict:
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
            "note": model.manual_note,
            "updated_ts": model.manual_updated_ts,
        }

    comparison = _build_solution_comparison(model)
    if comparison["selected"]["source"] != "none":
        return comparison["selected"]

    return {"source": "none", "lat": None, "lon": None, "cep_m": None}


def _automatic_solutions(model) -> list[dict]:
    if model is None:
        return []
    out: list[dict] = []
    if model.ci_lat is not None and model.ci_lon is not None:
        out.append({
            "source": "coincident_illumination",
            "lat": model.ci_lat,
            "lon": model.ci_lon,
            "cep_m": model.ci_cep_m,
            "updated_ts": model.ci_last_updated,
            "n_pairs": model.ci_n_pairs,
        })
    if model.fm_lat is not None and model.fm_lon is not None:
        out.append({
            "source": "fm",
            "lat": model.fm_lat,
            "lon": model.fm_lon,
            "cep_m": model.fm_cep_m,
            "updated_ts": model.last_updated,
            "n_observations": model.fm_n_observations,
        })
    if model.lat is not None and model.lon is not None and not model.multi_radar_flag:
        out.append({
            "source": "tdoa",
            "lat": model.lat,
            "lon": model.lon,
            "cep_m": model.cep_m,
            "updated_ts": model.last_updated,
            "n_pairs": model.n_pairs,
        })
    return out


def _build_combined_solution(candidates: list[dict]) -> dict | None:
    if len(candidates) < 2:
        return None
    finite = [candidate for candidate in candidates if candidate.get("cep_m") is not None]
    if len(finite) < 2:
        return None

    best = min(finite, key=lambda candidate: candidate["cep_m"])
    agreeing = [
        candidate for candidate in finite
        if _haversine_m(best["lat"], best["lon"], candidate["lat"], candidate["lon"]) <= 25_000.0
    ]
    if len(agreeing) < 2:
        return None

    total_weight = 0.0
    sum_lat = 0.0
    sum_lon = 0.0
    for candidate in agreeing:
        cep_m = max(candidate["cep_m"], 1.0)
        weight = 1.0 / (cep_m ** 2)
        total_weight += weight
        sum_lat += candidate["lat"] * weight
        sum_lon += candidate["lon"] * weight
    if total_weight <= 0:
        return None

    combined = {
        "source": "combined",
        "lat": sum_lat / total_weight,
        "lon": sum_lon / total_weight,
        "cep_m": min(candidate["cep_m"] for candidate in agreeing) / math.sqrt(len(agreeing)),
        "updated_ts": max(candidate.get("updated_ts") or 0.0 for candidate in agreeing) or None,
        "contributors": [candidate["source"] for candidate in agreeing],
    }
    return combined


def _build_solution_comparison(model) -> dict:
    candidates = _automatic_solutions(model)
    combined = _build_combined_solution(candidates)
    ranked = list(candidates)
    if combined is not None:
        ranked.append(combined)
    ranked.sort(key=lambda candidate: (candidate.get("cep_m") is None, candidate.get("cep_m") or float("inf")))
    selected = ranked[0] if ranked else {"source": "none", "lat": None, "lon": None, "cep_m": None}
    return {
        "methods": ranked,
        "selected": selected,
    }


def _best_effort_automatic_position(model) -> dict:
    comparison = _build_solution_comparison(model)
    selected = comparison["selected"]
    if selected.get("source") == "none":
        return {"source": "none", "lat": None, "lon": None, "cep_m": None}
    return selected


def _load_calibration_pair_rows(iid: int) -> list[dict]:
    from db import stats_db
    return stats_db.load_calibration_pairs(iid)


def _count_calibration_pair_rows(iid: int) -> int:
    from db import stats_db
    return stats_db.count_calibration_pairs(iid)


def _build_method_statuses(iid: int, model) -> list[dict]:
    comparison = _build_solution_comparison(model)
    solved = {entry["source"]: entry for entry in comparison["methods"]}
    pair_count = _count_calibration_pair_rows(iid) if model is not None else 0
    frame_count = 0
    good_frame_count = 0
    if _state is not None:
        counts = _state.get_live_frame_counts(iid)
        frame_count = counts["n_frames"]
        good_frame_count = counts.get("n_usable", counts.get("n_good", 0))

    methods: list[dict] = []

    if model is not None and model.manual_lat is not None and model.manual_lon is not None:
        methods.append({
            "source": "manual",
            "status": "locked" if model.resolution_mode == "locked_position" else "available",
            "lat": model.manual_lat,
            "lon": model.manual_lon,
            "cep_m": None,
            "updated_ts": model.manual_updated_ts,
            "notes": model.manual_note or ("Authoritative manual position" if model.resolution_mode == "locked_position" else "Manual position saved"),
        })
    else:
        methods.append({
            "source": "manual",
            "status": "unset",
            "lat": None,
            "lon": None,
            "cep_m": None,
            "updated_ts": None,
            "notes": "No manual position saved",
        })

    if "fm" in solved:
        fm = solved["fm"]
        methods.append({
            **fm,
            "status": "solved",
            "notes": f"{fm.get('n_observations', 0)} obs",
        })
    elif model is None:
        methods.append({"source": "fm", "status": "unavailable", "lat": None, "lon": None, "cep_m": None, "updated_ts": None, "notes": "IID not available"})
    elif model.resolution_mode != "auto":
        methods.append({"source": "fm", "status": "paused", "lat": None, "lon": None, "cep_m": None, "updated_ts": None, "notes": f"Suppressed by resolution mode {model.resolution_mode}"})
    elif model.period_s is None:
        methods.append({"source": "fm", "status": "collecting", "lat": None, "lon": None, "cep_m": None, "updated_ts": None, "notes": "Waiting for a stable rotation period before building frames"})
    elif good_frame_count == 0:
        methods.append({"source": "fm", "status": "collecting", "lat": None, "lon": None, "cep_m": None, "updated_ts": None, "notes": f"Rotation learned; {frame_count} sweep frames seen, 0 usable so far"})
    else:
        methods.append({"source": "fm", "status": "collecting", "lat": None, "lon": None, "cep_m": None, "updated_ts": None, "notes": f"{good_frame_count} usable sweep frames available; no accepted FM solution yet"})

    if "tdoa" in solved:
        tdoa = solved["tdoa"]
        methods.append({
            **tdoa,
            "status": "solved",
            "notes": f"{tdoa.get('n_pairs', 0)} pairs",
        })
    elif model is None:
        methods.append({"source": "tdoa", "status": "unavailable", "lat": None, "lon": None, "cep_m": None, "updated_ts": None, "notes": "IID not available"})
    elif model.multi_radar_flag:
        methods.append({"source": "tdoa", "status": "blocked", "lat": None, "lon": None, "cep_m": None, "updated_ts": None, "notes": "IID is flagged multi-radar, so single-site TDOA is suppressed"})
    elif pair_count == 0:
        methods.append({"source": "tdoa", "status": "collecting", "lat": None, "lon": None, "cep_m": None, "updated_ts": None, "notes": "No calibration pairs captured yet"})
    else:
        methods.append({"source": "tdoa", "status": "inactive", "lat": None, "lon": None, "cep_m": None, "updated_ts": None, "notes": f"{pair_count} calibration pairs stored, but no active TDOA background solver is running"})

    if "coincident_illumination" in solved:
        ci = solved["coincident_illumination"]
        methods.append({
            **ci,
            "status": "solved",
            "notes": f"{ci.get('n_pairs', 0)} pairs",
        })
    elif model is None:
        methods.append({
            "source": "coincident_illumination",
            "status": "unavailable",
            "lat": None,
            "lon": None,
            "cep_m": None,
            "updated_ts": None,
            "n_pairs": 0,
            "notes": "IID not available",
        })
    elif model.resolution_mode != "auto":
        methods.append({
            "source": "coincident_illumination",
            "status": "paused",
            "lat": None,
            "lon": None,
            "cep_m": None,
            "updated_ts": None,
            "n_pairs": 0,
            "notes": f"Suppressed by resolution mode {model.resolution_mode}",
        })
    elif pair_count == 0:
        methods.append({
            "source": "coincident_illumination",
            "status": "collecting",
            "lat": None,
            "lon": None,
            "cep_m": None,
            "updated_ts": None,
            "n_pairs": 0,
            "notes": "No calibration pairs captured yet",
        })
    else:
        methods.append({
            "source": "coincident_illumination",
            "status": "collecting",
            "lat": None,
            "lon": None,
            "cep_m": None,
            "updated_ts": None,
            "n_pairs": pair_count,
            "notes": f"{pair_count} calibration pairs stored; see coincident diagnostics for detailed family filtering status",
        })

    methods.append({
        "source": "inscribed_angle",
        "status": "evidence_only" if good_frame_count > 0 else "collecting",
        "lat": None,
        "lon": None,
        "cep_m": None,
        "updated_ts": None,
        "notes": (
            f"{good_frame_count} usable sweep frames available for geometry display; not a standalone stored solver"
            if good_frame_count > 0
            else "Waiting for usable sweep frames; explanatory geometry only"
        ),
    })

    if "combined" in solved:
        combined = solved["combined"]
        methods.append({
            **combined,
            "status": "solved",
            "notes": ", ".join(combined.get("contributors", [])),
        })
    else:
        methods.append({
            "source": "combined",
            "status": "waiting",
            "lat": None,
            "lon": None,
            "cep_m": None,
            "updated_ts": None,
            "notes": "Requires at least two automatic solved methods that agree closely enough",
        })

    order = {"manual": 0, "fm": 1, "tdoa": 2, "coincident_illumination": 3, "inscribed_angle": 4, "combined": 5}
    methods.sort(key=lambda method: order.get(method["source"], 99))
    return methods


def _control_payload(model) -> dict:
    auth = _authoritative_position(model)
    comparison = _build_solution_comparison(model)
    best_effort = _best_effort_automatic_position(model) if model is not None else {"source": "none", "lat": None, "lon": None, "cep_m": None}
    manual_reference_error_m = None
    if (
        model is not None
        and model.manual_lat is not None
        and model.manual_lon is not None
        and best_effort.get("source") != "none"
        and best_effort.get("lat") is not None
        and best_effort.get("lon") is not None
    ):
        manual_reference_error_m = _haversine_m(
            model.manual_lat,
            model.manual_lon,
            best_effort["lat"],
            best_effort["lon"],
        )
    return {
        "resolution_mode": model.resolution_mode if model is not None else "auto",
        "manual_lat": model.manual_lat if model is not None else None,
        "manual_lon": model.manual_lon if model is not None else None,
        "manual_note": model.manual_note if model is not None else None,
        "manual_updated_ts": model.manual_updated_ts if model is not None else None,
        "unresolvable_reason": model.unresolvable_reason if model is not None else None,
        "unresolvable_updated_ts": model.unresolvable_updated_ts if model is not None else None,
        "display_source": auth["source"],
        "display_lat": auth["lat"],
        "display_lon": auth["lon"],
        "display_cep_m": auth["cep_m"],
        "best_effort_source": best_effort.get("source"),
        "best_effort_lat": best_effort.get("lat"),
        "best_effort_lon": best_effort.get("lon"),
        "best_effort_cep_m": best_effort.get("cep_m"),
        "manual_reference_error_m": round(manual_reference_error_m, 1) if manual_reference_error_m is not None else None,
        "available_solutions": comparison["methods"],
    }


def _point_feature(lat: float, lon: float, **properties) -> dict:
    return {
        "geometry": {"type": "Point", "coordinates": [round(lon, 6), round(lat, 6)]},
        "properties": properties,
    }


def _line_feature(points: list[tuple[float, float]], **properties) -> dict:
    return {
        "geometry": {
            "type": "LineString",
            "coordinates": [[round(lon, 6), round(lat, 6)] for lat, lon in points],
        },
        "properties": properties,
    }


def _circle_feature(center_lat: float, center_lon: float, radius_km: float, **properties) -> dict:
    return {
        "geometry": {
            "type": "Circle",
            "center": [round(center_lon, 6), round(center_lat, 6)],
            "radius_km": round(radius_km, 3),
        },
        "properties": properties,
    }


def _layer(
    method: str,
    label: str,
    geometry_type: str,
    features: list[dict],
    *,
    confidence: float | None = None,
    source_count: int | None = None,
    active_estimate: dict | None = None,
) -> dict:
    return {
        "method": method,
        "label": label,
        "geometry_type": geometry_type,
        "confidence": confidence,
        "source_count": source_count,
        "active_estimate": active_estimate,
        "features": features,
    }


def _sanitize_floats(obj):
    """Recursively replace non-finite floats (inf, -inf, nan) with None."""
    if isinstance(obj, float):
        return None if not (obj == obj and obj != float("inf") and obj != float("-inf")) else obj
    if isinstance(obj, dict):
        return {k: _sanitize_floats(v) for k, v in obj.items()}
    if isinstance(obj, list):
        return [_sanitize_floats(v) for v in obj]
    if isinstance(obj, tuple):
        return tuple(_sanitize_floats(v) for v in obj)
    return obj


def _extend_line_through_points(
    lat_a: float,
    lon_a: float,
    lat_b: float,
    lon_b: float,
    *,
    extra_km: float = 300.0,
) -> list[tuple[float, float]]:
    mean_lat = (lat_a + lat_b) / 2.0
    km_per_deg_lat = 111.32
    km_per_deg_lon = max(111.32 * math.cos(math.radians(mean_lat)), 1e-6)

    ax = lon_a * km_per_deg_lon
    ay = lat_a * km_per_deg_lat
    bx = lon_b * km_per_deg_lon
    by = lat_b * km_per_deg_lat
    dx = bx - ax
    dy = by - ay
    norm = math.hypot(dx, dy)
    if norm <= 1e-6:
        return [(lat_a, lon_a), (lat_b, lon_b)]
    ux = dx / norm
    uy = dy / norm
    start_x = ax - ux * extra_km
    start_y = ay - uy * extra_km
    end_x = bx + ux * extra_km
    end_y = by + uy * extra_km
    return [
        (start_y / km_per_deg_lat, start_x / km_per_deg_lon),
        (end_y / km_per_deg_lat, end_x / km_per_deg_lon),
    ]


def _ray_from_origin(
    origin_lat: float,
    origin_lon: float,
    target_lat: float,
    target_lon: float,
    *,
    length_km: float = 450.0,
) -> list[tuple[float, float]]:
    mean_lat = (origin_lat + target_lat) / 2.0
    km_per_deg_lat = 111.32
    km_per_deg_lon = max(111.32 * math.cos(math.radians(mean_lat)), 1e-6)
    dx = (target_lon - origin_lon) * km_per_deg_lon
    dy = (target_lat - origin_lat) * km_per_deg_lat
    norm = math.hypot(dx, dy)
    if norm <= 1e-6:
        return [(origin_lat, origin_lon), (target_lat, target_lon)]
    ux = dx / norm
    uy = dy / norm
    end_lat = origin_lat + (uy * length_km / km_per_deg_lat)
    end_lon = origin_lon + (ux * length_km / km_per_deg_lon)
    return [(origin_lat, origin_lon), (end_lat, end_lon)]


async def _persist_model(iid: int) -> None:
    from db import stats_db

    if _state is None:
        return
    model = _state.get_rotation_model(iid)
    if model is None:
        return
    await asyncio.to_thread(stats_db.upsert_radar_iid, model)


def _evidence_meta(iid: int, method: str, model) -> dict:
    auth = _authoritative_position(model)
    return {
        "iid": iid,
        "method": method,
        "resolution_mode": model.resolution_mode if model is not None else "auto",
        "manual_lat": model.manual_lat if model is not None else None,
        "manual_lon": model.manual_lon if model is not None else None,
        "display_position": {
            "source": auth["source"],
            "lat": auth["lat"],
            "lon": auth["lon"],
            "cep_m": auth["cep_m"],
        },
    }


def _build_tdoa_evidence(iid: int, model) -> dict:
    # TDOA solver disabled — return empty evidence so the map layer slot still
    # exists but renders nothing.
    return {
        **_evidence_meta(iid, "tdoa", model),
        "available": False,
        "reason": "TDOA solver is disabled",
        "layers": [],
    }


def _build_coincident_illumination_evidence(iid: int, model) -> dict:
    if _state is None or model is None or model.fm_lat is None or model.fm_lon is None or model.period_s is None:
        return {
            **_evidence_meta(iid, "coincident_illumination", model),
            "available": False,
            "reason": "forward-model solution required",
            "layers": [],
        }

    from .forward_model import (
        _CI_VALIDATION_MAX_AIRCRAFT_PER_FRAME,
        _CI_VALIDATION_MAX_FRAMES,
        _CI_VALIDATION_OBSERVED_DT_TOL_MS,
        _CI_VALIDATION_PREDICTED_BEARING_TOL_DEG,
        _angular_separation_deg,
        validate_coincident_alignment,
    )

    frames = [
        f for f in _state.get_sweep_frames(iid)
        if getattr(f, "quality", None) in ("good", "marginal")
    ][-_CI_VALIDATION_MAX_FRAMES:]
    if not frames:
        return {
            **_evidence_meta(iid, "coincident_illumination", model),
            "available": False,
            "reason": "no recent sweep frames",
            "layers": [],
        }

    estimate = {
        "source": "fm",
        "lat": model.fm_lat,
        "lon": model.fm_lon,
        "cep_m": model.fm_cep_m,
    }
    validation = validate_coincident_alignment(
        model.fm_lat,
        model.fm_lon,
        frames,
        model.period_s,
    )

    aircraft_features: list[dict] = []
    ray_features: list[dict] = []
    aircraft_seen: set[tuple[str, float, float]] = set()
    pair_count = 0
    for frame in frames:
        aircraft = [{
            "icao": frame.ref_icao,
            "lat": frame.ref_lat,
            "lon": frame.ref_lon,
            "arrival_us": frame.ref_arrival_us,
            "role": "reference",
        }]
        aircraft.extend({
            "icao": obs.icao,
            "lat": obs.lat,
            "lon": obs.lon,
            "arrival_us": obs.arrival_us,
            "role": "observation",
        } for obs in frame.observations)
        aircraft = aircraft[:_CI_VALIDATION_MAX_AIRCRAFT_PER_FRAME]
        bearings = [
            _bearing_deg(model.fm_lat, model.fm_lon, ac["lat"], ac["lon"])
            for ac in aircraft
        ]

        for i in range(len(aircraft)):
            for j in range(i + 1, len(aircraft)):
                bearing_delta = _angular_separation_deg(bearings[i], bearings[j])
                if bearing_delta > _CI_VALIDATION_PREDICTED_BEARING_TOL_DEG:
                    continue
                pair_count += 1
                a = aircraft[i]
                b = aircraft[j]
                for ac in (a, b):
                    key = (ac["icao"], ac["lat"], ac["lon"])
                    if key not in aircraft_seen:
                        aircraft_seen.add(key)
                        aircraft_features.append(_point_feature(
                            ac["lat"],
                            ac["lon"],
                            icao=ac["icao"],
                            role=ac["role"],
                        ))
                dist_a = _haversine_m(model.fm_lat, model.fm_lon, a["lat"], a["lon"])
                dist_b = _haversine_m(model.fm_lat, model.fm_lon, b["lat"], b["lon"])
                target = a if dist_a >= dist_b else b
                ray_length_km = max(40.0, min(220.0, max(dist_a, dist_b) / 1000.0 + 20.0))
                observed_dt_ms = abs(a["arrival_us"] - b["arrival_us"]) / 1000.0
                ray_features.append(_line_feature(
                    _ray_from_origin(
                        model.fm_lat,
                        model.fm_lon,
                        target["lat"],
                        target["lon"],
                        length_km=ray_length_km,
                    ),
                    frame_index=getattr(frame, "frame_index", None),
                    icao_a=a["icao"],
                    icao_b=b["icao"],
                    bearing_delta_deg=round(bearing_delta, 2),
                    observed_dt_ms=round(observed_dt_ms, 2),
                    validation=(
                        "support"
                        if observed_dt_ms <= _CI_VALIDATION_OBSERVED_DT_TOL_MS
                        else "missed"
                    ),
                    line_type="predicted_coincident_ray",
                ))

    layers = [
        _layer(
            "coincident_illumination",
            "Coincident Aircraft",
            "point",
            aircraft_features,
            source_count=len(aircraft_features),
            active_estimate=estimate,
        ),
        _layer(
            "coincident_illumination",
            "Predicted Coincident Rays",
            "line",
            ray_features,
            source_count=len(ray_features),
            active_estimate=estimate,
        ),
    ]
    layers.append(
        _layer(
            "coincident_illumination",
            "FM Estimate",
            "point",
            [_point_feature(estimate["lat"], estimate["lon"], source="fm", cep_m=estimate["cep_m"])],
            source_count=1,
            active_estimate=estimate,
        )
    )
    return {
        **_evidence_meta(iid, "coincident_illumination", model),
        "available": pair_count > 0,
        "reason": None if pair_count > 0 else "no predicted coincident pairs",
        "source_count": pair_count,
        "validation": validation,
        "layers": layers,
    }


def _build_coincident_diagnostics(iid: int, model) -> dict:
    cache_key = f"{iid}:coincident"
    cached = _get_diag_cache(cache_key)
    if cached is not None:
        return cached

    from db import stats_db
    from .localiser import RadarLocaliser
    from .models import CalibrationPair

    # 500 recent pairs give sufficient family-stability coverage while keeping
    # the O(L²) geometry step bounded (localiser caps geometry pairs at 60).
    rows = stats_db.load_calibration_pairs(iid, limit=500)
    pairs = [
        CalibrationPair(
            iid=row["iid"],
            ts=row["ts"],
            icao_a=row["icao_a"],
            icao_b=row["icao_b"],
            lat_a=row["lat_a"],
            lon_a=row["lon_a"],
            lat_b=row["lat_b"],
            lon_b=row["lon_b"],
            tdoa_us=row["tdoa_us"],
            receiver_lat=row["receiver_lat"],
            receiver_lon=row["receiver_lon"],
        )
        for row in rows
    ]

    diagnostics = RadarLocaliser().coincident_diagnostics(pairs)

    # FM is the primary solver.  CI must be seeded from an FM position to avoid
    # converging on coherent but incorrect intersection clusters.
    has_fm = model is not None and model.fm_lat is not None and model.fm_lon is not None
    if not has_fm and diagnostics.get("blocker") is None:
        # All other gates passed, but we still block because there is no FM seed.
        diagnostics["blocker"] = "Waiting for FM solution — run FM first to seed this solver."
        diagnostics["status"] = "waiting_for_fm"

    diagnostics.update({
        "available": True,
        "iid": iid,
        "resolution_mode": model.resolution_mode if model is not None else "auto",
        "stored_solution": (
            {
                "source": "coincident_illumination",
                "lat": model.ci_lat,
                "lon": model.ci_lon,
                "cep_m": model.ci_cep_m,
                "n_pairs": model.ci_n_pairs,
                "updated_ts": model.ci_last_updated,
            }
            if model is not None and model.ci_lat is not None and model.ci_lon is not None
            else None
        ),
        "display_position": _authoritative_position(model),
    })
    _set_diag_cache(cache_key, diagnostics)
    return diagnostics


def _build_tdoa_diagnostics(iid: int, model) -> dict:
    # TDOA solver disabled: O(N²) pair filtering and unbounded scipy sweep-solve
    # loop caused multi-minute CPU spikes that blocked message decoding.
    # Returns a stub so the endpoint remains callable without triggering computation.
    return {
        "iid": iid,
        "available": False,
        "status": "disabled",
        "blocker": "TDOA solver is disabled pending a bounded-cost reimplementation.",
        "resolution_mode": model.resolution_mode if model is not None else "auto",
        "stored_solution": None,
        "last_run": model.tdoa_last_run if model is not None else None,
        "display_position": _authoritative_position(model),
    }


def _build_forward_model_evidence(iid: int, model) -> dict:
    if _state is None:
        return {
            **_evidence_meta(iid, "forward_model", model),
            "available": False,
            "reason": "radar module not initialised",
            "layers": [],
        }

    frames = [f for f in _state.get_sweep_frames(iid) if getattr(f, "quality", None) in ("good", "marginal")][-8:]
    if not frames:
        return {
            **_evidence_meta(iid, "forward_model", model),
            "available": False,
            "reason": "no sweep frames",
            "layers": [],
        }

    aircraft_features: list[dict] = []
    ref_lines: list[dict] = []
    estimate = None
    if model is not None and model.fm_lat is not None and model.fm_lon is not None:
        estimate = {"lat": model.fm_lat, "lon": model.fm_lon, "cep_m": model.fm_cep_m, "source": "fm"}

    for frame in frames:
        aircraft_features.append(
            _point_feature(
                frame.ref_lat,
                frame.ref_lon,
                icao=frame.ref_icao,
                role="reference",
                frame_index=frame.frame_index,
            )
        )
        if estimate is not None:
            ref_lines.append(
                _line_feature(
                    [(estimate["lat"], estimate["lon"]), (frame.ref_lat, frame.ref_lon)],
                    frame_index=frame.frame_index,
                    icao=frame.ref_icao,
                    role="reference_bearing",
                )
            )
        for obs in frame.observations:
            dt_s = (obs.arrival_us - frame.ref_arrival_us) / 1_000_000.0
            observed_phase_deg = ((dt_s / frame.period_s) * 360.0) % 360.0 if frame.period_s else None
            aircraft_features.append(
                _point_feature(
                    obs.lat,
                    obs.lon,
                    icao=obs.icao,
                    role="observation",
                    frame_index=frame.frame_index,
                    observed_phase_deg=round(observed_phase_deg, 2) if observed_phase_deg is not None else None,
                    interpolated=bool(getattr(obs, "interpolated", False)),
                )
            )
            if estimate is not None:
                ref_lines.append(
                    _line_feature(
                        [(estimate["lat"], estimate["lon"]), (obs.lat, obs.lon)],
                        frame_index=frame.frame_index,
                        icao=obs.icao,
                        role="observation_bearing",
                    )
                )

    fm = _get_fm()
    frame_est_features = [
        _point_feature(
            fp.lat, fp.lon,
            role="frame_position_estimate",
            frame_index=fp.frame_index,
            sweep_start_us=fp.sweep_start_us,
            n_contributing_arcs=fp.n_contributing_arcs,
            cep_km=round(fp.cep_km, 2),
            azimuth_spread_deg=round(fp.azimuth_spread_deg, 1),
        )
        for fp in fm.get_frame_positions(iid)
    ]

    layers = [
        _layer("forward_model", "Sweep Aircraft", "point", aircraft_features, source_count=len(aircraft_features), active_estimate=estimate),
        _layer("forward_model", "Predicted Bearings", "line", ref_lines, source_count=len(ref_lines), active_estimate=estimate),
    ]
    if frame_est_features:
        layers.append(
            _layer(
                "forward_model",
                "Per-Frame Estimates",
                "point",
                frame_est_features,
                source_count=len(frame_est_features),
                active_estimate=None,
            )
        )
    if estimate is not None:
        layers.append(
            _layer(
                "forward_model",
                "Forward-Model Estimate",
                "point",
                [_point_feature(estimate["lat"], estimate["lon"], source="fm", cep_m=estimate["cep_m"], fm_source=model.fm_source if model is not None else None)],
                source_count=1,
                active_estimate=estimate,
            )
        )
    return {
        **_evidence_meta(iid, "forward_model", model),
        "available": True,
        "source_count": len(frames),
        "layers": layers,
    }


def _build_inscribed_angle_evidence(iid: int, model) -> dict:
    if _state is None or model is None or model.period_s is None:
        return {
            **_evidence_meta(iid, "inscribed_angle", model),
            "available": False,
            "reason": "insufficient sweep-frame context",
            "layers": [],
        }

    from .forward_model import (
        _OPTIM_MAX_FRAMES,
        _preprocess_scoring_frames,
        _select_intersection_observations_with_diagnostics,
    )

    frames = [f for f in _state.get_sweep_frames(iid) if getattr(f, "quality", None) in ("good", "marginal")]
    if not frames:
        return {
            **_evidence_meta(iid, "inscribed_angle", model),
            "available": False,
            "reason": "no sweep frames",
            "layers": [],
        }

    scored_frames = _preprocess_scoring_frames(frames[-_OPTIM_MAX_FRAMES:], model.period_s, sweep_direction=1)
    selected, diagnostics = _select_intersection_observations_with_diagnostics(scored_frames, min_sin_phi=0.15)
    if not selected:
        return {
            **_evidence_meta(iid, "inscribed_angle", model),
            "available": False,
            "reason": "no qualifying observations",
            "selection_diagnostics": diagnostics,
            "layers": [],
        }

    mean_lat = sum(obs.ref_lat + obs.obs_lat for obs in selected) / (2 * len(selected))
    km_per_deg_lat = 111.32
    km_per_deg_lon = max(111.32 * math.cos(math.radians(mean_lat)), 1e-6)

    def to_xy(lat: float, lon: float) -> tuple[float, float]:
        return lon * km_per_deg_lon, lat * km_per_deg_lat

    def from_xy(x: float, y: float) -> tuple[float, float]:
        return y / km_per_deg_lat, x / km_per_deg_lon

    circle_features: list[dict] = []
    point_features: list[dict] = []
    for idx, obs in enumerate(selected[:30]):
        ax, ay = to_xy(obs.ref_lat, obs.ref_lon)
        bx, by = to_xy(obs.obs_lat, obs.obs_lon)
        phi_rad = math.radians(obs.observed_phase_deg)
        sin_phi = math.sin(phi_rad)
        d = math.hypot(bx - ax, by - ay)
        if d < 1e-6 or abs(sin_phi) < 1e-6:
            continue
        radius_km = d / (2.0 * abs(sin_phi))
        px, py = -((by - ay) / d), (bx - ax) / d
        h = -(d / 2.0) * (math.cos(phi_rad) / sin_phi)
        cx = (ax + bx) / 2.0 + h * px
        cy = (ay + by) / 2.0 + h * py
        center_lat, center_lon = from_xy(cx, cy)
        circle_features.append(
            _circle_feature(
                center_lat,
                center_lon,
                radius_km,
                pair_index=idx,
                ref_icao=obs.icao,
                observed_phase_deg=round(obs.observed_phase_deg, 2),
                quality_weight=round(obs.quality_weight, 4),
            )
        )
        point_features.append(_point_feature(obs.ref_lat, obs.ref_lon, icao=obs.icao, role="reference_like"))
        point_features.append(_point_feature(obs.obs_lat, obs.obs_lon, icao=obs.icao, role="observation"))

    estimate = None
    if model.fm_lat is not None and model.fm_lon is not None:
        estimate = {"lat": model.fm_lat, "lon": model.fm_lon, "cep_m": model.fm_cep_m, "source": "fm"}

    layers = [
        _layer("inscribed_angle", "Sweep Aircraft", "point", point_features, source_count=len(point_features), active_estimate=estimate),
        _layer("inscribed_angle", "Inscribed-Angle Circles", "circle", circle_features, source_count=len(circle_features), active_estimate=estimate),
    ]
    if estimate is not None:
        layers.append(
            _layer(
                "inscribed_angle",
                "Intersection Seed / FM Estimate",
                "point",
                [_point_feature(estimate["lat"], estimate["lon"], source="fm", cep_m=estimate["cep_m"])],
                source_count=1,
                active_estimate=estimate,
            )
        )
    return {
        **_evidence_meta(iid, "inscribed_angle", model),
        "available": True,
        "source_count": len(circle_features),
        "selection_diagnostics": diagnostics,
        "layers": layers,
    }


def build_iid_timeline_payload(state: "RadarState" | None, iid: int, window_s: float = 30.0) -> dict:
    """Build the Stage 1 timeline payload for one IID."""
    if state is None:
        return {"iid": iid, "icaos": []}

    icao_arrivals = state.get_iid_timeline(iid, window_s)
    model = state.get_rotation_model(iid)
    rotation_model = model.rotation_model if model is not None else None
    folded = getattr(rotation_model, "folded", {}) if rotation_model is not None else {}
    residual = getattr(rotation_model, "residual", {}) if rotation_model is not None else {}
    primary_period_s = model.period_s if model else None

    icaos_out = []
    for icao, arrivals in icao_arrivals.items():
        folded_entry = folded.get(icao)
        family_series = _build_family_series(
            arrivals,
            primary_period_s=primary_period_s,
            primary_entry=folded_entry,
        )
        if family_series:
            families = [series["family"] for series in family_series]
            if len(families) == 1:
                classification = families[0]
            elif any(family.startswith("primary") for family in families):
                classification = "primary"
            elif icao in residual or any(family == "residual" for family in families):
                classification = "residual"
            else:
                classification = "unclassified"
            primary_series = next((series for series in family_series if series["family"].startswith("primary")), None)
            icaos_out.append({
                "icao": icao,
                "arrivals_us": arrivals,
                "classification": classification,
                "multiplier": primary_series["multiplier"] if primary_series else None,
                "method": primary_series["method"] if primary_series else None,
                "family_series": family_series,
            })
            continue

        icaos_out.append({
            "icao": icao,
            "arrivals_us": arrivals,
            "classification": "unclassified",
            "multiplier": None,
            "method": None,
            "family_series": [],
        })

    return {
        "iid": iid,
        "window_s": window_s,
        "dominant_period_s": primary_period_s,
        "secondary_period_s": None,
        "icaos": icaos_out,
    }


def build_iid_rotation_payload(state: "RadarState" | None, iid: int) -> dict:
    """Build the reinforced rotation-model payload for one IID."""
    if state is None:
        return {"iid": iid, "status": "INSUFFICIENT_DATA"}

    model = state.get_rotation_model(iid)
    if model is None or model.rotation_model is None:
        return {"iid": iid, "status": "INSUFFICIENT_DATA"}

    rm = model.rotation_model
    primary_confidence = _confidence_from_support(model.primary_support_count, PRIMARY_CONFIDENCE_TARGET)
    return {
        "iid": iid,
        "status": model.status,
        "primary_readiness": _readiness_from_confidence(primary_confidence, model.period_s),
        "period_s": model.period_s,
        "secondary_period_s": None,
        "period_std_s": model.period_std_s,
        "rpm": model.rpm,
        "dominant_period_s": model.period_s,
        "n_icaos_qualifying": rm.n_qualifying,
        "n_harmonic": rm.n_harmonic,
        "n_residual": rm.n_residual,
        "current_status": rm.status,
        "current_period_s": rm.dominant_period_s,
        "current_secondary_period_s": None,
        "primary_support_count": model.primary_support_count,
        "secondary_support_count": 0,
        "primary_confidence": primary_confidence,
        "secondary_confidence": 0.0,
        "last_updated": rm.last_updated,
    }


def build_iid_sync_snapshot_payload(
    state: "RadarState" | None,
    iid: int,
    window_s: float = 90.0,
    debug_limit: int = 120,
) -> dict:
    """Build the shared selected-IID sync snapshot for HTTP and websocket clients."""
    if state is None:
        return {
            "type": "radar_sync",
            "iid": iid,
            "sequence": 0,
            "server_ts": time.time(),
            "window_s": window_s,
            "rotation": {"iid": iid, "status": None},
            "sync_state": None,
            "observations": [],
            "sync_debug": {
                "iid": iid,
                "available": False,
                "reason": "radar module not initialised",
                "observations": [],
                "summary": {"iid": iid, "wall_clock_used_operationally": False},
            },
        }
    snapshot = state.get_live_sync_snapshot(iid, window_s=window_s, debug_limit=debug_limit)
    payload = dict(snapshot)
    payload["rotation"] = build_iid_rotation_payload(state, iid)
    payload["rotation"].update(_control_payload(state.get_rotation_model(iid)))
    return payload


def _pick_family_sequence(
    centroids_us: list[int],
    period_s: float | None,
    tolerance: float = 0.12,
) -> list[int]:
    """Return the strongest forward sequence that fits integer multiples of period_s."""
    if period_s is None or period_s <= 0 or len(centroids_us) < 2:
        return []

    best: list[int] = []
    best_error = float("inf")

    for anchor_idx, anchor_us in enumerate(centroids_us):
        matched = [anchor_us]
        error_sum = 0.0
        for centroid_us in centroids_us[anchor_idx + 1:]:
            delta_s = (centroid_us - anchor_us) / 1_000_000.0
            nearest = round(delta_s / period_s)
            if nearest < 1:
                continue
            frac_error = abs(delta_s - (nearest * period_s)) / period_s
            if frac_error <= tolerance:
                matched.append(centroid_us)
                error_sum += frac_error

        if len(matched) > len(best) or (len(matched) == len(best) and error_sum < best_error):
            best = matched
            best_error = error_sum

    return best if len(best) >= 2 else []


def _remove_matched_centroids(centroids_us: list[int], matched_us: list[int]) -> list[int]:
    remaining = list(centroids_us)
    for centroid_us in matched_us:
        try:
            remaining.remove(centroid_us)
        except ValueError:
            continue
    return remaining


def _build_family_series(
    arrivals_us: list[int],
    primary_period_s: float | None,
    primary_entry: dict | None,
) -> list[dict]:
    """Split one ICAO's bursts into primary/residual series for the Stage 1 visual."""
    bursts = detect_bursts(arrivals_us)
    centroids_us = [burst["centroid_us"] for burst in bursts]
    if not centroids_us:
        return []

    remaining = centroids_us
    series: list[dict] = []

    primary_sequence = _pick_family_sequence(remaining, primary_period_s)
    if primary_sequence:
        primary_multiplier = int((primary_entry or {}).get("multiplier", 1))
        series.append({
            "family": "primary_harmonic" if primary_multiplier > 1 else "primary",
            "arrivals_us": primary_sequence,
            "multiplier": primary_multiplier,
            "method": (primary_entry or {}).get("method"),
        })
        remaining = _remove_matched_centroids(remaining, primary_sequence)

    if remaining:
        series.append({
            "family": "residual",
            "arrivals_us": remaining,
            "multiplier": None,
            "method": None,
        })

    return series


# ---------------------------------------------------------------------------
# Stage 1: IID activity and rotation
# ---------------------------------------------------------------------------

@router.get("/iids")
async def get_iids(window_s: float = Query(default=600.0, ge=10, le=7200)):
    """All IIDs seen in the last window_s seconds with activity counts."""
    if _state is None:
        return {"iids": [], "window_s": window_s}

    activity = _state.get_iid_activity(window_s)
    models = _state.get_all_rotation_models()
    now_wall = time.time()
    latest_arrival_us = _state.get_latest_arrival_us()

    iids_out = []
    for iid, entry in activity.items():
        model = models.get(iid)
        last_us = entry.get("last_us", 0)
        if latest_arrival_us is not None and last_us <= latest_arrival_us:
            last_seen = now_wall - ((latest_arrival_us - last_us) / 1_000_000.0)
        else:
            last_seen = now_wall
        iids_out.append({
            "iid": iid,
            "count": entry["count"],
            "last_seen": round(last_seen, 1),
            "latest_icao": entry.get("latest_icao", ""),
            "period_s": model.period_s if model else None,
            "status": model.status if model else None,
            # Forward model fields
            "fm_lat": model.fm_lat if model else None,
            "fm_lon": model.fm_lon if model else None,
            "fm_cep_m": model.fm_cep_m if model else None,
            "fm_source": model.fm_source if model else None,
            **_control_payload(model),
        })

    iids_out.sort(key=lambda x: x["iid"])

    import config as _config
    return {
        "iids": iids_out,
        "window_s": window_s,
        "receiver_lat": getattr(_config, "RECEIVER_LAT", None),
        "receiver_lon": getattr(_config, "RECEIVER_LON", None),
    }


@router.get("/iids/{iid}/timeline")
async def get_iid_timeline(iid: int, window_s: float = Query(default=30.0, ge=5, le=300)):
    """Per-ICAO arrival timestamps for a single IID."""
    t0 = time.perf_counter()
    try:
        return build_iid_timeline_payload(_state, iid, window_s)
    finally:
        _record_api_timing("iid_timeline", t0)


@router.get("/iids/{iid}/burst_sync_timeline")
@router.get("/iids/{iid}/burst-sync-timeline")
async def get_burst_sync_timeline(iid: int, window_s: float = Query(default=60.0, ge=10, le=300)):
    """Burst-centre sync observations with residuals for verification plotting.

    Returns one entry per burst-centre observation (not per DF11 message) within
    the requested window, annotated with predicted bearing, residual, and
    classification against the current sync model. Payloads include observations
    that did not steer sync updates so visual diagnostics are not artificially sparse.
    """
    import config as _config
    if not _config.RADAR_DIAGNOSTICS:
        return {"observations": [], "sync_state": None, "window_s": window_s, "diagnostics_disabled": True}
    t0 = time.perf_counter()
    try:
        if _state is None:
            return {"observations": [], "sync_state": None, "window_s": window_s}
        return _state.get_burst_sync_timeline(iid, window_s)
    finally:
        _record_api_timing("burst_sync_timeline", t0)


@router.get("/iids/{iid}/sync-debug")
async def get_iid_sync_debug(
    iid: int,
    window_s: float = Query(default=60.0, ge=10, le=300),
    limit: int = Query(default=80, ge=1, le=300),
):
    """Targeted sync consistency diagnostics for one IID.

    The payload compares authoritative, localiser-live, position-verification,
    and burst-sync predictions for the exact same burst-centre observations.
    Wall-clock-derived prediction is included only as a diagnostic comparison;
    operational sync math remains Beast-relative.
    """
    import config as _config
    if not _config.RADAR_DIAGNOSTICS:
        return {
            "iid": iid,
            "available": False,
            "reason": "RADAR_DIAGNOSTICS not enabled",
            "observations": [],
            "summary": {"iid": iid, "wall_clock_used_operationally": False},
            "diagnostics_disabled": True,
        }
    t0 = time.perf_counter()
    try:
        if _state is None:
            return {
                "iid": iid,
                "available": False,
                "reason": "radar module not initialised",
                "observations": [],
                "summary": {"iid": iid, "wall_clock_used_operationally": False},
            }
        return _state.get_sync_debug_payload(iid, window_s=window_s, limit=limit)
    finally:
        _record_api_timing("sync_debug", t0)


@router.get("/iids/{iid}/sync-snapshot")
async def get_iid_sync_snapshot(
    iid: int,
    window_s: float = Query(default=90.0, ge=10, le=300),
    debug_limit: int = Query(default=120, ge=1, le=300),
):
    """Shared fast-changing Radar sync snapshot used by the pushed UI feed."""
    t0 = time.perf_counter()
    try:
        return build_iid_sync_snapshot_payload(_state, iid, window_s=window_s, debug_limit=debug_limit)
    finally:
        _record_api_timing("sync_snapshot", t0)


@router.get("/iids/{iid}/rotation")
async def get_iid_rotation(iid: int):
    """Current rotation model for a single IID."""
    t0 = time.perf_counter()
    try:
        payload = build_iid_rotation_payload(_state, iid)
        if _state is not None:
            payload.update(_control_payload(_state.get_rotation_model(iid)))
        return payload
    finally:
        _record_api_timing("iid_rotation", t0)


@router.get("/iids/{iid}/control")
async def get_iid_control(iid: int):
    """Current operator control state for one IID."""
    t0 = time.perf_counter()
    try:
        if _state is None:
            return {"iid": iid, "available": False, "reason": "radar module not initialised"}

        model = _state.get_localisation_control(iid)
        return {
            "iid": iid,
            "available": model is not None,
            **_control_payload(model),
        }
    finally:
        _record_api_timing("iid_control", t0)


@router.get("/iids/{iid}/solution-comparison")
async def get_iid_solution_comparison(iid: int):
    """Compare available localisation methods and expose the selected final result.

    Diagnostics-only: the active position is available from /api/radar/live/latest.
    """
    import config as _config
    if not _config.RADAR_DIAGNOSTICS:
        return {"iid": iid, "available": False, "reason": "RADAR_DIAGNOSTICS not enabled",
                "methods": [], "diagnostics_disabled": True}
    t0 = time.perf_counter()
    try:
        if _state is None:
            return {"iid": iid, "available": False, "reason": "radar module not initialised", "methods": []}

        model = _state.get_rotation_model(iid)
        if model is None:
            return {"iid": iid, "available": False, "reason": "IID not seen", "methods": []}

        comparison = _build_solution_comparison(model)
        methods = _build_method_statuses(iid, model)
        return {
            "iid": iid,
            "available": True,
            "resolution_mode": model.resolution_mode,
            "selected": comparison["selected"],
            "methods": methods,
        }
    finally:
        _record_api_timing("iid_solution_comparison", t0)


@router.get("/iids/{iid}/coincident-diagnostics")
async def get_iid_coincident_diagnostics(iid: int):
    """Expose coincident-solver intermediate evidence and current blocker state."""
    t0 = time.perf_counter()
    try:
        if _state is None:
            return {"iid": iid, "available": False, "reason": "radar module not initialised"}

        model = _state.get_rotation_model(iid)
        payload = await asyncio.to_thread(_build_coincident_diagnostics, iid, model)
        return payload
    finally:
        _record_api_timing("iid_coincident_diagnostics", t0)


@router.get("/iids/{iid}/tdoa-diagnostics")
async def get_iid_tdoa_diagnostics(iid: int):
    """Expose TDOA readiness, sweep viability, and last manual-run outcome."""
    t0 = time.perf_counter()
    try:
        if _state is None:
            return {"iid": iid, "available": False, "reason": "radar module not initialised"}

        model = _state.get_rotation_model(iid)
        payload = await asyncio.to_thread(_build_tdoa_diagnostics, iid, model)
        return payload
    finally:
        _record_api_timing("iid_tdoa_diagnostics", t0)


@router.post("/iids/{iid}/manual-position")
async def set_iid_manual_position(iid: int, payload: ManualPositionPayload):
    """Set or update the operator-supplied manual position for one IID."""
    if _state is None:
        return {"iid": iid, "updated": False, "reason": "radar module not initialised"}

    _state.set_manual_position(iid, payload.lat, payload.lon, payload.note)
    try:
        await _persist_model(iid)
    except Exception:
        log.exception("manual-position: failed to persist IID %d", iid)
        return {"iid": iid, "updated": True, "persisted": False, **_control_payload(_state.get_rotation_model(iid))}

    return {"iid": iid, "updated": True, "persisted": True, **_control_payload(_state.get_rotation_model(iid))}


@router.post("/iids/{iid}/lock-position")
async def lock_iid_position(iid: int):
    """Lock the operator-supplied manual position as authoritative."""
    if _state is None:
        return {"iid": iid, "locked": False, "reason": "radar module not initialised"}

    ok, model, reason = _state.lock_manual_position(iid)
    if not ok:
        return {"iid": iid, "locked": False, "reason": reason, **_control_payload(model)}

    try:
        await _persist_model(iid)
    except Exception:
        log.exception("lock-position: failed to persist IID %d", iid)
        return {"iid": iid, "locked": True, "persisted": False, **_control_payload(model)}

    return {"iid": iid, "locked": True, "persisted": True, **_control_payload(model)}


@router.post("/iids/{iid}/unlock-position")
async def unlock_iid_position(iid: int):
    """Return an IID from manual lock back to auto mode."""
    if _state is None:
        return {"iid": iid, "unlocked": False, "reason": "radar module not initialised"}

    ok, model = _state.unlock_position(iid)
    if not ok:
        return {"iid": iid, "unlocked": False, "reason": "IID not seen"}

    try:
        await _persist_model(iid)
    except Exception:
        log.exception("unlock-position: failed to persist IID %d", iid)
        return {"iid": iid, "unlocked": True, "persisted": False, **_control_payload(model)}

    return {"iid": iid, "unlocked": True, "persisted": True, **_control_payload(model)}


@router.post("/iids/{iid}/mark-unresolvable")
async def mark_iid_unresolvable(iid: int, payload: UnresolvablePayload):
    """Mark an IID as operator-locked unresolvable."""
    if _state is None:
        return {"iid": iid, "updated": False, "reason": "radar module not initialised"}

    model = _state.mark_unresolvable(iid, payload.reason)
    try:
        await _persist_model(iid)
    except Exception:
        log.exception("mark-unresolvable: failed to persist IID %d", iid)
        return {"iid": iid, "updated": True, "persisted": False, **_control_payload(model)}

    return {"iid": iid, "updated": True, "persisted": True, **_control_payload(model)}


@router.post("/iids/{iid}/clear-unresolvable")
async def clear_iid_unresolvable(iid: int):
    """Clear operator unresolvable lock and return to auto mode."""
    if _state is None:
        return {"iid": iid, "updated": False, "reason": "radar module not initialised"}

    ok, model = _state.clear_unresolvable(iid)
    if not ok:
        return {"iid": iid, "updated": False, "reason": "IID not seen"}

    try:
        await _persist_model(iid)
    except Exception:
        log.exception("clear-unresolvable: failed to persist IID %d", iid)
        return {"iid": iid, "updated": True, "persisted": False, **_control_payload(model)}

    return {"iid": iid, "updated": True, "persisted": True, **_control_payload(model)}


@router.post("/iids/{iid}/reset")
async def reset_iid_rotation(iid: int):
    """Clear in-memory learned state for one IID so characteristics are relearned."""
    if _state is None:
        return {"iid": iid, "reset": False, "reason": "radar module not initialised"}
    did_reset = _state.reset_iid(iid)
    return {"iid": iid, "reset": did_reset}


@router.post("/reset")
async def reset_all_radar_learning():
    """Clear all learned passive-radar state: memory, calibration history, and frame positions."""
    if _state is None:
        return {"reset": False, "reason": "radar module not initialised"}

    from db import stats_db

    memory = _state.reset_all()
    persisted = await asyncio.to_thread(stats_db.clear_radar_learning)

    # Also clear ForwardModel's in-memory frame position cache so it doesn't
    # re-serve deleted rows on the next FM run.
    try:
        fm = _get_fm()
        with fm._frame_positions_lock:
            fm._frame_positions.clear()
            fm._frame_positions_loaded.clear()
    except Exception:
        pass

    return {
        "reset": True,
        "memory": memory,
        "persisted": persisted,
    }


@router.get("/rotation")
async def get_all_rotation():
    """Summary table of rotation models across all active IIDs.

    Extended with period stability, FM fields, pipeline stage statuses,
    and a manual Run button without extra calls.
    """
    t0 = time.perf_counter()
    try:
        if _state is None:
            return []

        models = _state.get_all_rotation_models()
        out = []
        for iid, model in sorted(models.items()):
            rm = model.rotation_model
            sweep_frames = _state.get_sweep_frames(iid)

            health = _state.get_pipeline_health(iid, sweep_frames=sweep_frames)
            stages = health.get("stages", {})

            out.append({
            "iid": iid,
            "status": model.status,
            "primary_readiness": _readiness_from_confidence(
                _confidence_from_support(model.primary_support_count, PRIMARY_CONFIDENCE_TARGET),
                model.period_s,
            ),
            "period_s": model.period_s,
            "period_std_s": model.period_std_s,
            "secondary_period_s": None,
            "rpm": model.rpm,
            "n_icaos_qualifying": rm.n_qualifying if rm else 0,
            "n_harmonic": rm.n_harmonic if rm else 0,
            "primary_support_count": model.primary_support_count,
            "secondary_support_count": 0,
            "primary_confidence": _confidence_from_support(model.primary_support_count, PRIMARY_CONFIDENCE_TARGET),
            "secondary_confidence": 0.0,
            "last_updated": model.last_updated,
            # FM fields
            "fm_cep_m": model.fm_cep_m,
            "fm_source": model.fm_source,
            # Reference aircraft
            "ref_icao": model.reference_aircraft.ref_icao if model.reference_aircraft else None,
            # Sweep frame counts
            "n_frames_good": sum(
                1 for f in sweep_frames
                if getattr(f, "quality", None) == "good"
            ),
            # Pipeline stage compact status
            "stage_reference": stages.get("reference", {}).get("status", "not_started"),
            "stage_frames": stages.get("frames", {}).get("status", "not_started"),
            "stage_scoring": stages.get("scoring", {}).get("status", "not_started"),
            "stage_optimisation": stages.get("optimisation", {}).get("status", "not_started"),
                **_control_payload(model),
            })
        return out
    finally:
        _record_api_timing("rotation_summary", t0)


# ---------------------------------------------------------------------------
# Stage 2: Sweep waterfall and dwell profiles
# ---------------------------------------------------------------------------

@router.get("/iids/{iid}/sweeps")
async def get_iid_sweeps(iid: int, n: int = Query(default=30, ge=1, le=200)):
    """Burst centroids per sweep for waterfall visualisation."""
    import config as _config
    if not _config.RADAR_DIAGNOSTICS:
        return {"iid": iid, "sweeps": [], "diagnostics_disabled": True}
    if _state is None:
        return {"iid": iid, "sweeps": []}

    sweeps = _state.get_sweep_history(iid, n)
    model = _state.get_rotation_model(iid)
    display = _authoritative_position(model)
    has_location = (
        display["source"] != "none"
        and display["lat"] is not None
        and display["lon"] is not None
    )

    sweeps_out = []
    for idx, sweep in enumerate(sweeps):
        azimuths = []
        for ac in sweep.get("aircraft", []):
            az_entry: dict = {
                "icao": ac["icao"],
                "range_nm": None,
                "azimuth_deg": None,
                "interpolated": False,
            }

            position = None
            if ac.get("lat") is not None and ac.get("lon") is not None:
                position = {
                    "lat": ac["lat"],
                    "lon": ac["lon"],
                    "interpolated": bool(ac.get("interpolated")),
                }
            else:
                position = _state.get_aircraft_burst_position(ac["icao"], ac.get("centroid_us", sweep["centroid_us"]))
            if position is not None:
                if position.get("range_nm") is not None:
                    az_entry["range_nm"] = position.get("range_nm")
                elif position.get("lat") is not None and position.get("lon") is not None:
                    import config as _config
                    receiver_lat = getattr(_config, "RECEIVER_LAT", None)
                    receiver_lon = getattr(_config, "RECEIVER_LON", None)
                    if receiver_lat is not None and receiver_lon is not None:
                        from .localiser import _haversine_m
                        az_entry["range_nm"] = round(
                            _haversine_m(receiver_lat, receiver_lon, position["lat"], position["lon"]) / 1852.0,
                            2,
                        )
                az_entry["interpolated"] = bool(position.get("interpolated"))
                if has_location:
                    az_entry["azimuth_deg"] = round(
                        _bearing_deg(display["lat"], display["lon"], position["lat"], position["lon"]),
                        2,
                    )

            azimuths.append(az_entry)

        sweeps_out.append({
            "sweep_idx": idx,
            "centroid_us": sweep["centroid_us"],
            "n_aircraft": sweep["n_aircraft"],
            "azimuths": azimuths,
        })

    return {"iid": iid, "sweeps": sweeps_out}


@router.get("/iids/{iid}/dwell")
async def get_iid_dwell(
    iid: int,
    icao: str = Query(...),
    sweep_idx: Optional[int] = Query(default=None),
):
    """Per-reply RSSI profile for a selected aircraft/sweep."""
    if _state is None:
        return {"replies": []}

    replies = _state.get_dwell_profile(iid, icao.upper(), sweep_idx)
    return {
        "iid": iid,
        "icao": icao.upper(),
        "sweep_idx": sweep_idx,
        "replies": replies,
    }


# ---------------------------------------------------------------------------
# Stage 2: Localisation
# ---------------------------------------------------------------------------

@router.get("/iids/{iid}/location")
async def get_iid_location(iid: int):
    """Current position estimate for a single IID."""
    if _state is None:
        return {"iid": iid, "status": "NOT_LOCALISED", "reason": "radar module not initialised"}

    model = _state.get_rotation_model(iid)
    if model is None:
        return {"iid": iid, "status": "NOT_LOCALISED", "reason": "IID not seen"}

    auth = _authoritative_position(model)
    if model.resolution_mode == "locked_unresolvable":
        return {
            "iid": iid,
            "status": "NOT_LOCALISED",
            "reason": "IID marked unresolvable",
            **_control_payload(model),
        }

    if auth["source"] == "manual":
        return {
            "iid": iid,
            "lat": auth["lat"],
            "lon": auth["lon"],
            "cep_m": None,
            "n_pairs": model.n_pairs,
            "status": model.status,
            "source": "manual",
            "last_updated": model.manual_updated_ts,
            "convergence_history": model.convergence_history[-20:],
            **_control_payload(model),
        }

    if auth["source"] == "fm":
        return {
            "iid": iid,
            "lat": auth["lat"],
            "lon": auth["lon"],
            "cep_m": auth["cep_m"],
            "n_pairs": model.n_pairs,
            "status": model.status,
            "source": "fm",
            "last_updated": model.last_updated,
            "convergence_history": model.fm_convergence_history[-20:],
            **_control_payload(model),
        }

    if auth["source"] == "coincident_illumination":
        return {
            "iid": iid,
            "lat": auth["lat"],
            "lon": auth["lon"],
            "cep_m": auth["cep_m"],
            "n_pairs": model.ci_n_pairs,
            "status": model.status,
            "source": "coincident_illumination",
            "last_updated": model.ci_last_updated,
            "convergence_history": [],
            **_control_payload(model),
        }

    if auth["source"] == "combined":
        return {
            "iid": iid,
            "lat": auth["lat"],
            "lon": auth["lon"],
            "cep_m": auth["cep_m"],
            "n_pairs": model.n_pairs,
            "status": model.status,
            "source": "combined",
            "last_updated": auth.get("updated_ts"),
            "contributors": auth.get("contributors", []),
            "convergence_history": [],
            **_control_payload(model),
        }

    if model.multi_radar_flag:
        return {"iid": iid, "status": "NOT_LOCALISED", "reason": "multi-radar IID", **_control_payload(model)}

    if model.lat is None:
        return {
            "iid": iid,
            "status": "NOT_LOCALISED",
            "reason": "insufficient calibration pairs",
            **_control_payload(model),
        }

    return {
        "iid": iid,
        "lat": model.lat,
        "lon": model.lon,
        "cep_m": model.cep_m,
        "n_pairs": model.n_pairs,
        "status": model.status,
        "source": "tdoa",
        "last_updated": model.last_updated,
        "convergence_history": model.convergence_history[-20:],
        **_control_payload(model),
    }


@router.get("/iids/{iid}/hyperbolas")
async def get_iid_hyperbolas(iid: int):
    """TDOA hyperbola curves for map overlay (Stage 2)."""
    from db import stats_db
    from .localiser import RadarLocaliser

    pairs_rows = stats_db.load_calibration_pairs(iid)
    if not pairs_rows:
        return {"iid": iid, "hyperbolas": []}

    from .models import CalibrationPair
    pairs = [
        CalibrationPair(
            iid=row["iid"],
            ts=row["ts"],
            icao_a=row["icao_a"],
            icao_b=row["icao_b"],
            lat_a=row["lat_a"],
            lon_a=row["lon_a"],
            lat_b=row["lat_b"],
            lon_b=row["lon_b"],
            tdoa_us=row["tdoa_us"],
            receiver_lat=row["receiver_lat"],
            receiver_lon=row["receiver_lon"],
        )
        for row in pairs_rows[:20]  # limit to 20 hyperbolas
    ]

    localiser = RadarLocaliser()
    hyperbolas = []
    for i, pair in enumerate(pairs):
        try:
            pts = localiser.compute_hyperbola_points(pair, n_points=50)
            hyperbolas.append({
                "pair_id": i,
                "icao_a": pair.icao_a,
                "icao_b": pair.icao_b,
                "tdoa_us": pair.tdoa_us,
                "hyperbola_points": pts,
            })
        except Exception as e:
            log.debug("Hyperbola compute failed for pair %d: %s", i, e)

    return {"iid": iid, "hyperbolas": hyperbolas}


def _build_evidence_for_method(iid: int, method: str):
    model = _state.get_rotation_model(iid) if _state is not None else None
    if method == "tdoa":
        return _build_tdoa_evidence(iid, model)
    if method == "coincident_illumination":
        return _build_coincident_illumination_evidence(iid, model)
    if method == "forward_model":
        return _build_forward_model_evidence(iid, model)
    if method == "inscribed_angle":
        return _build_inscribed_angle_evidence(iid, model)
    return {
        **_evidence_meta(iid, method, model),
        "available": False,
        "reason": "unknown method",
        "layers": [],
    }


@router.get("/iids/{iid}/evidence")
async def get_iid_evidence(iid: int):
    """Return all localisation evidence layers for one IID."""
    t0 = time.perf_counter()
    try:
        if _state is None:
            return {"iid": iid, "available": False, "reason": "radar module not initialised", "methods": []}

        model = _state.get_rotation_model(iid)
        methods = await asyncio.to_thread(
            lambda: [
                _build_tdoa_evidence(iid, model),
                _build_coincident_illumination_evidence(iid, model),
                _build_forward_model_evidence(iid, model),
                _build_inscribed_angle_evidence(iid, model),
            ]
        )
        return {
            "iid": iid,
            "available": True,
            **_control_payload(model),
            "methods": methods,
        }
    finally:
        _record_api_timing("iid_evidence", t0)


@router.get("/iids/{iid}/evidence/{method}")
async def get_iid_evidence_method(iid: int, method: str):
    """Return one method-specific localisation evidence payload."""
    t0 = time.perf_counter()
    safe_method = method if method in {"tdoa", "coincident_illumination", "forward_model", "inscribed_angle"} else "unknown"
    try:
        if _state is None:
            return {"iid": iid, "method": method, "available": False, "reason": "radar module not initialised", "layers": []}
        return await asyncio.to_thread(_build_evidence_for_method, iid, method)
    finally:
        _record_api_timing(f"iid_evidence_{safe_method}", t0)


@router.get("/map")
async def get_radar_map():
    """GeoJSON FeatureCollection of all localised radars."""
    if _state is None:
        return {"type": "FeatureCollection", "features": []}

    models = _state.get_all_rotation_models()
    features = []
    for iid, model in sorted(models.items()):
        auth = _authoritative_position(model)
        if auth["source"] == "none":
            continue

        props = {
            "iid": iid,
            "period_s": model.period_s,
            "rpm": model.rpm,
            "cep_m": auth["cep_m"],
            "status": model.status,
            "n_pairs": model.n_pairs,
            "last_updated": auth.get("updated_ts", model.last_updated),
            "source": auth["source"],
            "resolution_mode": model.resolution_mode,
            "display_source": auth["source"],
        }
        if auth["source"] == "fm":
            props["fm_source"] = model.fm_source
            props["n_observations"] = model.fm_n_observations
        if auth["source"] == "manual":
            props["manual_note"] = model.manual_note

        features.append({
            "type": "Feature",
            "geometry": {"type": "Point", "coordinates": [auth["lon"], auth["lat"]]},
            "properties": props,
        })

    return {"type": "FeatureCollection", "features": features}


# ---------------------------------------------------------------------------
# Forward Model endpoints
# ---------------------------------------------------------------------------

@router.get("/iids/{iid}/fm-status")
async def get_iid_fm_status(iid: int):
    """Diagnostics for the forward model pipeline on one IID.

    Shows rotation model state, observation counts, last FM pass result,
    and airport hypothesis state — all in one call for debugging.
    """
    if _state is None:
        return {"iid": iid, "available": False, "reason": "radar module not initialised"}

    model = _state.get_rotation_model(iid)
    if model is None:
        return {"iid": iid, "available": False, "reason": "IID not seen"}

    # Rotation model info
    status = {
        "iid": iid,
        "available": True,
        "rotation_model": {
            "period_s": model.period_s,
            "rpm": model.rpm,
            "status": model.status,
            "multi_radar_flag": model.multi_radar_flag,
        },
        "forward_model": {
            "has_position": model.fm_lat is not None,
            "fm_lat": model.fm_lat,
            "fm_lon": model.fm_lon,
            "fm_cep_m": model.fm_cep_m,
            "fm_source": model.fm_source,
            "n_observations": model.fm_n_observations,
            "window_s": model.fm_window_s,
            "n_convergence_entries": len(model.fm_convergence_history),
            "n_airport_candidates": len(model.airport_hypothesis),
            "coincident_validation": getattr(model, "fm_coincident_validation", None),
            "last_run": getattr(model, "fm_last_run", None),
        },
    }

    # If hypothesis data exists, include the top candidate
    if model.airport_hypothesis:
        top = model.airport_hypothesis[0]
        status["forward_model"]["top_candidate"] = {
            "airport_icao": top.get("airport_icao"),
            "score": top.get("score"),
            "residual_sigma_ms": top.get("residual_sigma_ms"),
            "n_aircraft": top.get("n_aircraft"),
        }
        status["forward_model"]["directional_signal"] = top.get("directional_signal")

    return status


def _build_frame_accumulation_summary(iid: int, model) -> dict:
    """Summarise the per-frame position buffer for one IID."""
    try:
        fm = _get_fm()
        estimates = fm.get_frame_positions(iid)
        centroid = fm.compute_weighted_centroid(iid) if estimates else None

        # Error vs manual reference
        error_m = None
        if centroid and model.manual_lat is not None and model.manual_lon is not None:
            error_m = _haversine_m(
                centroid["lat"], centroid["lon"],
                model.manual_lat, model.manual_lon,
            )

        return {
            "n_estimates": len(estimates),
            "centroid": centroid,
            "error_vs_manual_m": round(error_m) if error_m is not None else None,
        }
    except Exception:
        return {"n_estimates": 0, "centroid": None, "error_vs_manual_m": None}


_FM_DIAG_CACHE_TTL_S = 3.0
_fm_diag_cache: dict[int, tuple[float, dict]] = {}


@router.get("/iids/{iid}/fm-diagnostics")
async def get_iid_fm_diagnostics(iid: int):
    """Show the data funnel: how many burst centroids become usable SweepFrames.

    This answers: 'why is run_full_pipeline returning None?'

    Result is cached per-IID for a few seconds because the underlying work
    (build_sweep_frames + get_sweep_history) is expensive and the frontend
    polls this endpoint repeatedly.
    """
    t0 = time.perf_counter()
    cached = _fm_diag_cache.get(iid)
    if cached is not None and (t0 - cached[0]) < _FM_DIAG_CACHE_TTL_S:
        _record_api_timing("iid_fm_diagnostics", t0)
        return cached[1]
    try:
        if _state is None:
            return {"iid": iid, "available": False, "reason": "radar module not initialised"}

        model = _state.get_rotation_model(iid)
        if model is None:
            return {"iid": iid, "available": False, "reason": "IID not seen"}

        # Build sweep frames to diagnose the data funnel
        sweep_frames = _state.build_sweep_frames(iid)
        n_good = sum(1 for f in sweep_frames if f.quality == "good")
        n_marginal = sum(1 for f in sweep_frames if f.quality == "marginal")
        n_observations = sum(1 + len(f.observations) for f in sweep_frames)
        n_unique = len(set(
            [f.ref_icao for f in sweep_frames] +
            [obs.icao for f in sweep_frames for obs in f.observations]
        )) if sweep_frames else 0

        # Sweep history info
        sweeps = _state.get_sweep_history(iid, n=_state._SWEEP_HISTORY_MAX)
        n_sweeps = len(sweeps)
        sweep_aircraft_counts = [s.get("n_aircraft", 0) for s in sweeps]
        max_aircraft = max(sweep_aircraft_counts) if sweep_aircraft_counts else 0

        payload = {
            "iid": iid,
            "available": True,
            "rotation_model": {
                "period_s": model.period_s,
                "rpm": model.rpm,
                "status": model.status,
                "multi_radar_flag": model.multi_radar_flag,
            },
            "data_funnel": {
                "sweeps_in_history": n_sweeps,
                "max_aircraft_per_sweep": max_aircraft,
                "sweep_frames_built": len(sweep_frames),
                "good_frames": n_good,
                "marginal_frames": n_marginal,
                "total_observations": n_observations,
                "unique_aircraft": n_unique,
                "bottleneck": (
                    "rotation model not established" if model.period_s is None else
                    "no sweeps in history" if n_sweeps == 0 else
                    f"only {max_aircraft} aircraft per sweep — need 3+" if max_aircraft < 3 else
                    f"0 valid frames built from {n_sweeps} sweeps" if len(sweep_frames) == 0 else
                    f"only {n_marginal} marginal frames — need good frames with 4+ aircraft" if n_good == 0 else
                    None
                ),
            },
            "forward_model": {
                "has_position": model.fm_lat is not None,
                "fm_lat": model.fm_lat,
                "fm_lon": model.fm_lon,
                "fm_cep_m": model.fm_cep_m,
                "fm_source": model.fm_source,
                "n_observations": model.fm_n_observations,
                "n_convergence_entries": len(model.fm_convergence_history),
                "n_airport_candidates": len(model.airport_hypothesis),
                "coincident_validation": getattr(model, "fm_coincident_validation", None),
                "last_run": getattr(model, "fm_last_run", None),
                "top_candidate": (
                    {
                        "airport_icao": model.airport_hypothesis[0].get("airport_icao"),
                        "airport_name": model.airport_hypothesis[0].get("airport_name") or model.airport_hypothesis[0].get("name"),
                        "score": model.airport_hypothesis[0].get("score"),
                        "residual_sigma_deg": model.airport_hypothesis[0].get("residual_sigma_deg"),
                        "residual_sigma_ms": model.airport_hypothesis[0].get("residual_sigma_ms"),
                        "n_aircraft": model.airport_hypothesis[0].get("n_aircraft"),
                        "directional_signal": model.airport_hypothesis[0].get("directional_signal"),
                    }
                    if model.airport_hypothesis
                    else None
                ),
            },
            "frame_accumulation": _build_frame_accumulation_summary(iid, model),
        }
        _fm_diag_cache[iid] = (time.perf_counter(), payload)
        return payload
    finally:
        _record_api_timing("iid_fm_diagnostics", t0)


@router.post("/iids/{iid}/fm-run")
async def run_iid_fm(iid: int):
    """Manually trigger a forward model pass for one IID.

    Use this for debugging: runs the full airport hypothesis + 2D optimisation
    pipeline immediately instead of waiting for the 10-minute background cycle.
    Any manual reference aircraft override is consumed (cleared) after this run.
    """
    if _state is None:
        return {"iid": iid, "success": False, "reason": "radar module not initialised"}

    model = _state.get_rotation_model(iid)
    if model is None:
        return {"iid": iid, "success": False, "reason": "IID not seen"}

    fm = _get_fm()
    try:
        import asyncio
        t_run = time.perf_counter()
        result = await asyncio.to_thread(fm.run_full_pipeline, iid, _state)
        elapsed_ms = (time.perf_counter() - t_run) * 1000
        _state.record_forward_model_attempt(iid, result, elapsed_ms)
        # Consume the manual reference override regardless of success/failure
        _state.set_reference_aircraft_override(iid, None)
        if result is None or "error" in result:
            return {
                "iid": iid,
                "success": False,
                "reason": result.get("error", "unknown failure") if result else "unknown failure",
                "stage": result.get("stage") if result else None,
                "detail": {k: v for k, v in (result or {}).items() if k not in ("error", "stage")},
                "rotation_period_s": model.period_s,
                "rotation_status": model.status,
                "multi_radar_flag": model.multi_radar_flag,
            }
        return {
            "iid": iid,
            "success": True,
            "result": result,
        }
    except Exception as e:
        # Consume the manual reference override even on exception
        _state.set_reference_aircraft_override(iid, None)
        try:
            _state.record_forward_model_attempt(
                iid,
                {"error": str(e), "stage": "exception"},
                0.0,
            )
        except Exception:
            pass
        log.exception("FM manual run failed for IID %d", iid)
        return {"iid": iid, "success": False, "error": str(e)}


@router.post("/iids/{iid}/coincident-run")
async def run_iid_coincident(iid: int):
    """Manually trigger a coincident-illumination solve for one IID.

    Requires an FM solution to be present as a seed.  Without it the beam-line
    intersections can cluster around a coherent but incorrect position.
    """
    if _state is None:
        return {"iid": iid, "success": False, "reason": "radar module not initialised"}

    model = _state.get_rotation_model(iid)
    if model is None:
        return {"iid": iid, "success": False, "reason": "IID not seen"}

    if model.fm_lat is None or model.fm_lon is None:
        return {
            "iid": iid,
            "success": False,
            "reason": "No FM solution — run FM first to seed the coincident solver.",
        }

    from .localiser import RadarLocaliser
    from .models import CalibrationPair

    def _load_and_solve():
        from db import stats_db
        rows = stats_db.load_calibration_pairs(iid)
        pairs = [
            CalibrationPair(
                iid=row["iid"],
                ts=row["ts"],
                icao_a=row["icao_a"],
                icao_b=row["icao_b"],
                lat_a=row["lat_a"],
                lon_a=row["lon_a"],
                lat_b=row["lat_b"],
                lon_b=row["lon_b"],
                tdoa_us=row["tdoa_us"],
                receiver_lat=row["receiver_lat"],
                receiver_lon=row["receiver_lon"],
            )
            for row in rows
        ]
        return RadarLocaliser().solve_coincident(pairs, model.fm_lat, model.fm_lon)

    try:
        import asyncio
        lat, lon, cep_m, n_pairs = await asyncio.to_thread(_load_and_solve)
        _state.update_coincident_location(iid=iid, lat=lat, lon=lon, cep_m=cep_m, n_pairs=n_pairs)
        try:
            updated = _state.get_rotation_model(iid)
            if updated is not None:
                from db import stats_db
                await asyncio.to_thread(stats_db.upsert_radar_iid, updated)
        except Exception:
            log.exception("CI manual run: failed to persist IID %d", iid)
        return {
            "iid": iid,
            "success": True,
            "result": {"lat": lat, "lon": lon, "cep_m": cep_m, "n_pairs": n_pairs},
        }
    except ValueError as exc:
        return {"iid": iid, "success": False, "reason": str(exc)}
    except Exception:
        log.exception("CI manual run failed for IID %d", iid)
        return {"iid": iid, "success": False, "reason": "unexpected solver error"}


@router.post("/iids/{iid}/tdoa-run")
async def run_iid_tdoa(iid: int):
    """TDOA solver is disabled — returns immediately without running any computation."""
    return {"iid": iid, "success": False, "reason": "TDOA solver is disabled"}


@router.get("/iids/{iid}/airport-hypothesis")
async def get_iid_airport_hypothesis(iid: int):
    """Ranked airport candidates for a given IID based on forward model scoring."""
    if _state is None:
        return {"iid": iid, "status": "NOT_AVAILABLE", "reason": "radar module not initialised"}

    model = _state.get_rotation_model(iid)
    if model is None:
        return {"iid": iid, "status": "NOT_AVAILABLE", "reason": "IID not seen"}

    # Return cached hypothesis results from the model (runtime-only)
    if model.airport_hypothesis:
        return {
            "iid": iid,
            "period_s": model.period_s,
            "status": "AVAILABLE",
            "candidates": model.airport_hypothesis,
        }

    # Provide diagnostic context so users can debug why scoring hasn't happened
    return {
        "iid": iid,
        "period_s": model.period_s,
        "status": "NOT_SCORED_YET",
        "reason": "forward model pass not yet completed for this IID",
        "diagnostics": {
            "multi_radar_flag": model.multi_radar_flag,
            "rotation_status": model.status,
            "hint": "FM pass runs every 10 minutes. Check /api/radar/iids/{iid}/fm-status for details.",
        },
        "candidates": [],
    }


@router.get("/iids/{iid}/fm-location")
async def get_iid_fm_location(iid: int):
    """Current forward-model position estimate for a single IID."""
    if _state is None:
        return {"iid": iid, "status": "NOT_LOCALISED", "reason": "radar module not initialised"}

    model = _state.get_rotation_model(iid)
    if model is None:
        return {"iid": iid, "status": "NOT_LOCALISED", "reason": "IID not seen"}

    if model.fm_lat is None:
        return {
            "iid": iid,
            "status": "NOT_LOCALISED",
            "reason": "forward model has not produced a position estimate",
        }

    return {
        "iid": iid,
        "lat": model.fm_lat,
        "lon": model.fm_lon,
        "cep_m": model.fm_cep_m,
        "source": model.fm_source,
        "n_observations": model.fm_n_observations,
        "window_s": model.fm_window_s,
        "status": "LOCALISED",
        "last_updated": model.last_updated,
        # Truncated to last 20 — use GET /iids/{iid}/fm-convergence for full history.
        "convergence_history": model.fm_convergence_history[-20:],
    }


@router.get("/iids/{iid}/fm-convergence")
async def get_iid_fm_convergence(iid: int):
    """Position convergence history for the forward model.

    Always-on path returns the last 20 entries (most recent fixes).
    Full history requires RADAR_DIAGNOSTICS=1.
    """
    import config as _config
    if _state is None:
        return {"iid": iid, "history": []}

    model = _state.get_rotation_model(iid)
    if model is None:
        return {"iid": iid, "history": []}

    history = model.fm_convergence_history
    if not _config.RADAR_DIAGNOSTICS:
        history = history[-20:] if len(history) > 20 else history

    return {"iid": iid, "history": history}


@router.post("/iids/{iid}/fm-reset")
async def reset_iid_fm(iid: int):
    """Clear forward-model state for one IID (position, hypothesis, convergence).

    The reset is performed inside the RadarState lock to avoid racing with
    the background loop's update_forward_model_location() writes.
    """
    if _state is None:
        return {"iid": iid, "reset": False, "reason": "radar module not initialised"}

    did_reset = _state.reset_forward_model(iid)
    if not did_reset:
        return {"iid": iid, "reset": False, "reason": "IID not seen"}

    _get_fm().clear_frame_positions(iid)

    # Flush immediately so the reset survives a server restart.
    # Without this, the old FM fields would be restored from DB on next boot.
    try:
        from db import stats_db
        model = _state.get_rotation_model(iid)
        if model is not None:
            await asyncio.to_thread(stats_db.upsert_radar_iid, model)
    except Exception:
        log.exception("FM reset: failed to flush IID %d to DB", iid)
        # Reset was done in memory — return success anyway

    return {"iid": iid, "reset": True}


@router.get("/iids/{iid}/frame-positions/filter-analysis")
async def analyze_frame_position_filter(iid: int):
    """Run the four-stage frame filter and classify each frame as inlier or outlier.

    Returns the sweep_start_us values of frames rejected by the filter so the
    caller can highlight or delete them.
    """
    import config as _cfg
    fm = _get_fm()
    estimates = fm.get_frame_positions(iid)
    if not estimates:
        return {
            "iid": iid, "n_total": 0, "n_stage0_survivors": 0,
            "n_inliers": 0, "n_outliers": 0,
            "outlier_sweep_start_us": [], "rejection_counts": {},
        }
    receiver_lat = getattr(_cfg, "RECEIVER_LAT", None)
    receiver_lon = getattr(_cfg, "RECEIVER_LON", None)
    if receiver_lat is None or receiver_lon is None:
        from fastapi import HTTPException
        raise HTTPException(status_code=400, detail="RECEIVER_LAT/LON not configured")
    from .frame_filter import filter_frame_estimates
    result = filter_frame_estimates(estimates, receiver_lat, receiver_lon)
    all_sus = {e.sweep_start_us for e in estimates}
    outlier_sus = sorted(all_sus - result.inlier_sweep_start_us)
    return {
        "iid": iid,
        "n_total": result.n_total,
        "n_stage0_survivors": result.n_stage0_survivors,
        "n_inliers": result.n_inliers,
        "n_outliers": len(outlier_sus),
        "outlier_sweep_start_us": outlier_sus,
        "rejection_counts": result.rejection_counts,
    }


class _BulkDeleteBody(BaseModel):
    sweep_start_us: list[float]


@router.post("/iids/{iid}/frame-positions/bulk-delete")
async def bulk_delete_frame_positions(iid: int, body: _BulkDeleteBody):
    """Delete multiple per-frame position estimates and rerun FM pipeline."""
    fm = _get_fm()
    deleted = fm.remove_frame_positions_bulk(iid, body.sweep_start_us)
    if deleted > 0 and _state is not None:
        result = await asyncio.to_thread(fm.run_full_pipeline, iid, _state)
        if result is not None:
            _state.record_forward_model_attempt(iid, result, 0)
    return {"iid": iid, "deleted": deleted}


@router.delete("/iids/{iid}/frame-positions/{frame_index}")
async def delete_frame_position(iid: int, frame_index: int):
    """Remove a single per-frame position estimate and immediately recompute the centroid."""
    import asyncio
    fm = _get_fm()
    removed = fm.remove_frame_position(iid, frame_index)
    if removed and _state is not None:
        result = await asyncio.to_thread(fm.run_full_pipeline, iid, _state)
        if result is not None:
            _state.record_forward_model_attempt(iid, result, 0)
    return {"iid": iid, "frame_index": frame_index, "removed": removed}


@router.get("/iids/{iid}/sweep-frames")
async def get_iid_sweep_frames(iid: int):
    """Return SweepFrames for an IID with per-aircraft phase information."""
    t0 = time.perf_counter()
    try:
        if _state is None:
            return {"iid": iid, "frames": []}

        model = _state.get_rotation_model(iid)
        if model is None:
            return {"iid": iid, "frames": []}

        frames = _state.get_sweep_frames(iid)
        if not frames:
            return {"iid": iid, "frames": []}

        # Get period for phase computation — use the frame's own period_s
        out_frames = []
        for frame in frames:
            frame_period = frame.period_s
            out_obs = []
            for obs in frame.observations:
                # Compute observed phase relative to reference aircraft (0-360°)
                dt_us = obs.arrival_us - frame.ref_arrival_us
                dt_s = dt_us / 1_000_000.0
                if frame_period and frame_period > 0:
                    observed_phase = ((dt_s / frame_period) * 360.0) % 360.0
                else:
                    observed_phase = None

                out_obs.append({
                    "icao": obs.icao,
                    "lat": round(obs.lat, 6),
                    "lon": round(obs.lon, 6),
                    "arrival_us": obs.arrival_us,
                    "observed_phase_deg": round(observed_phase, 2) if observed_phase is not None else None,
                    "interpolated": obs.interpolated,
                })
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
                "observations": out_obs,
            })

        return {
            "iid": iid,
            "n_frames": len(out_frames),
            "frames": out_frames,
            "latest_arrival_us": _state.get_latest_arrival_us(),
            "last_updated": model.last_updated if model else None,
        }
    finally:
        _record_api_timing("iid_sweep_frames", t0)


@router.get("/iids/{iid}/sweep-frames/{frame_index}/fm-geometry")
async def get_iid_sweep_frame_fm_geometry(iid: int, frame_index: int, direction: int = 1):
    """Return per-frame FM intersection geometry for visual debugging."""
    t0 = time.perf_counter()
    try:
        if _state is None:
            return {"iid": iid, "frame_index": frame_index, "available": False, "reason": "radar module not initialised"}

        model = _state.get_rotation_model(iid)
        if model is None or model.period_s is None:
            return {"iid": iid, "frame_index": frame_index, "available": False, "reason": "rotation model with period required"}

        frame = next((f for f in _state.get_sweep_frames(iid) if f.frame_index == frame_index), None)
        if frame is None:
            return {"iid": iid, "frame_index": frame_index, "available": False, "reason": "frame not found"}
        if not frame.observations:
            return {"iid": iid, "frame_index": frame_index, "available": False, "reason": "frame has no observations"}

        sweep_direction = 1 if direction >= 0 else -1
        import config as _config
        from .forward_model import _PER_FRAME_MIN_CONTRIBUTING_ARCS, _PER_FRAME_MAX_CEP_KM
        recv_lat = getattr(_config, "RECEIVER_LAT", None)
        recv_lon = getattr(_config, "RECEIVER_LON", None)

        points = [
            _point_feature(
                frame.ref_lat,
                frame.ref_lon,
                icao=frame.ref_icao,
                role="frame_aircraft",
                source_role="sweep_reference",
                arrival_us=frame.ref_arrival_us,
            )
        ]
        aircraft_by_icao = {
            frame.ref_icao: {
                "lat": frame.ref_lat,
                "lon": frame.ref_lon,
                "arrival_us": frame.ref_arrival_us,
                "interpolated": False,
            }
        }
        for obs in frame.observations:
            aircraft_by_icao[obs.icao] = {
                "lat": obs.lat,
                "lon": obs.lon,
                "arrival_us": obs.arrival_us,
                "interpolated": obs.interpolated,
            }
            points.append(_point_feature(
                obs.lat,
                obs.lon,
                icao=obs.icao,
                role="frame_aircraft",
                arrival_us=obs.arrival_us,
                interpolated=obs.interpolated,
            ))

        baselines: list[dict] = []
        circles: list[dict] = []
        admitted_pair_circles: list[dict] = []
        inlier_pair_circles: list[dict] = []
        pair_circle_summary: dict = {}
        candidate_clusters: list[dict] = []
        diagnostics: dict = {}
        frame_lat = None
        frame_lon = None
        frame_cep_km = None
        frame_n_arcs = None
        frame_solve_reason = None
        solve_result = None
        would_accumulate = None
        accumulation_rejection_reason = None
        accumulation_gate_metrics: Optional[dict] = None

        def from_receiver_xy(x_km: float, y_km: float) -> tuple[float, float]:
            if recv_lat is None or recv_lon is None:
                return frame.ref_lat, frame.ref_lon
            r_km = 6371.0
            cos_orig = max(math.cos(math.radians(recv_lat)), 1e-12)
            lat = recv_lat + math.degrees(y_km / r_km)
            lon = recv_lon + math.degrees(x_km / (r_km * cos_orig))
            return lat, lon

        MIN_CEP_KM = 0.05  # solver artifact threshold, matches frame_filter.MIN_CEP_KM
        if recv_lat is not None and recv_lon is not None:
            from .forward_model import ForwardModel
            solve_result = ForwardModel._solve_by_intersection_attempt(
                [frame],
                model.period_s,
                recv_lat,
                recv_lon,
                direction=sweep_direction,
                max_per_frame=50,
                max_per_icao=50,
            )
            if not solve_result.get("success"):
                frame_solve_reason = solve_result.get("reason", "unknown")
                diagnostics = solve_result.get("detail", {}).get("selection_diagnostics", {})
                # Extract pair/cluster diagnostics from the rejected payload so the UI
                # can render admitted pairs, scored pairs, and candidate clusters even
                # when the automatic solver rejects the frame.
                admitted_pair_circles = solve_result.get("admitted_pair_circles", [])
                pair_circle_summary = solve_result.get("pair_circle_summary", {})
                candidate_clusters = solve_result.get("candidate_clusters", [])
                # Even on solver rejection, check accumulation admission against the
                # best candidate cluster if available.
                from .forward_model import ForwardModel
                _clusters = solve_result.get("candidate_clusters", [])
                if _clusters:
                    best_cluster = _clusters[0]
                    second_cluster = _clusters[1] if len(_clusters) > 1 else {}
                    _synthetic_result = {
                        "centroid_uncertainty_km": best_cluster.get("cluster_compactness_km"),
                        "n_inlier_pair_circles": best_cluster.get("cluster_raw_inlier_count"),
                        "n_contributing_arcs": best_cluster.get("member_count"),
                        "best_cluster_support_score": best_cluster.get("cluster_support_score"),
                        "support_dominance_ratio": best_cluster.get("support_dominance_ratio"),
                        "pairwise_weighted_rms_deg": best_cluster.get("pairwise_weighted_rms_deg"),
                        "cluster_member_count": best_cluster.get("member_count", 0),
                        "second_cluster_member_count": second_cluster.get("member_count", 0),
                        "member_dominance_ratio": best_cluster.get("member_dominance_ratio"),
                        "weight_dominance_ratio": (
                            best_cluster.get("cluster_total_weight", 0.0) / second_cluster.get("cluster_total_weight", 1.0)
                            if second_cluster.get("cluster_total_weight", 0.0) > 0.0 else None
                        ),
                    }
                    _rej, _tier = ForwardModel._classify_frame_for_accumulation(_synthetic_result)
                    accumulation_rejection = _rej
                    would_accumulate = _rej is None
                    accumulation_rejection_reason = _rej
                    accumulation_gate_metrics = {**_synthetic_result, "admission_tier": _tier}
            else:
                result = solve_result["result"]
                admitted_pair_circles = result.get("admitted_pair_circles", [])
                inlier_pair_circles = result.get("inlier_pair_circles", [])
                pair_circle_summary = result.get("pair_circle_summary", {})
                candidate_clusters = result.get("candidate_clusters", [])
                diagnostics = result.get("selection_diagnostics", {})
                raw_cep = solve_result["result"].get("centroid_uncertainty_km")
                raw_arcs = solve_result["result"].get("n_contributing_arcs", 0)
                # Check accumulation admission regardless of per-frame display gates.
                # This tells us whether the frame would enter the centroid buffer.
                # The centroid accumulator uses _classify_frame_for_accumulation(), which
                # enforces a tiered admission policy: high-confidence frames must pass all
                # quality gates; geometry-dominant frames bypass noisy pairwise RMS when
                # member/weight dominance is strong.
                from .forward_model import ForwardModel
                _rej, _tier = ForwardModel._classify_frame_for_accumulation(result)
                accumulation_rejection = _rej
                would_accumulate = _rej is None
                accumulation_rejection_reason = _rej
                accumulation_gate_metrics = {
                    "centroid_uncertainty_km": result.get("centroid_uncertainty_km"),
                    "n_inlier_pair_circles": result.get("n_inlier_pair_circles", result.get("n_contributing_arcs", 0)),
                    "best_cluster_support_score": result.get("best_cluster_support_score"),
                    "support_dominance_ratio": result.get("support_dominance_ratio"),
                    "pairwise_weighted_rms_deg": result.get("pairwise_weighted_rms_deg"),
                    "cluster_member_count": result.get("cluster_member_count", 0),
                    "second_cluster_member_count": result.get("second_cluster_member_count", 0),
                    "member_dominance_ratio": result.get("member_dominance_ratio"),
                    "weight_dominance_ratio": result.get("weight_dominance_ratio"),
                    "admission_tier": _tier,
                }

                # Per-frame display gate: only the diagnostic thresholds required to
                # show a location and CEP on the UI.  This is intentionally more
                # permissive than the accumulation gate so operators can still inspect
                # the geometry of marginal frames.  cep_km < 0.05 km is a solver
                # artifact (perfectly coincident intersection points from degenerate
                # geometry).
                if raw_cep is not None and raw_cep >= MIN_CEP_KM and raw_arcs >= _PER_FRAME_MIN_CONTRIBUTING_ARCS and raw_cep < _PER_FRAME_MAX_CEP_KM:
                    frame_lat = solve_result["result"].get("lat")
                    frame_lon = solve_result["result"].get("lon")
                    frame_cep_km = raw_cep
                    frame_n_arcs = raw_arcs
                    if frame_lat is not None and frame_lon is not None:
                        points.append(_point_feature(
                            frame_lat,
                            frame_lon,
                            role="frame_estimate",
                            source="frame_fm",
                            cep_km=round(frame_cep_km, 2),
                            n_arcs=frame_n_arcs,
                        ))
                        circles.append(_circle_feature(
                            frame_lat,
                            frame_lon,
                            frame_cep_km,
                            circle_type="frame_cep",
                            source="frame_fm",
                            selected=True,
                            cep_km=round(frame_cep_km, 2),
                            n_arcs=frame_n_arcs,
                        ))
                else:
                    frame_solve_reason = (
                        "solver_artifact" if (raw_cep is not None and raw_cep < MIN_CEP_KM)
                        else f"cep={raw_cep}km arcs={raw_arcs}" if raw_cep is not None
                        else "no_result"
                    )
        else:
            frame_solve_reason = "receiver coordinates not configured"

        for pair in admitted_pair_circles:
            a = aircraft_by_icao.get(pair.get("icao_a"))
            b = aircraft_by_icao.get(pair.get("icao_b"))
            if a is not None and b is not None:
                baselines.append(_line_feature(
                    [(a["lat"], a["lon"]), (b["lat"], b["lon"])],
                    icao_a=pair.get("icao_a"),
                    icao_b=pair.get("icao_b"),
                    pair_label=f"{pair.get('icao_a')} <-> {pair.get('icao_b')}",
                    circle_index=pair.get("circle_index"),
                    inlier=pair.get("inlier", False),
                    selected=True,
                    line_type="admitted_pair_baseline",
                    normalized_residual=pair.get("normalized_residual"),
                ))
            if pair.get("cx_km") is not None and pair.get("cy_km") is not None and pair.get("R_km") is not None:
                center_lat, center_lon = from_receiver_xy(pair["cx_km"], pair["cy_km"])
                circles.append(_circle_feature(
                    center_lat,
                    center_lon,
                    pair["R_km"],
                    circle_type="admitted_pair_circle",
                    circle_index=pair.get("circle_index"),
                    icao_a=pair.get("icao_a"),
                    icao_b=pair.get("icao_b"),
                    pair_label=f"{pair.get('icao_a')} <-> {pair.get('icao_b')}",
                    selected=True,
                    inlier=pair.get("inlier", False),
                    circle_score=pair.get("circle_score"),
                    normalized_residual=pair.get("normalized_residual"),
                ))

        layers = [
            _layer("frame_fm_geometry", "Frame Aircraft", "point", points, source_count=len(points)),
            _layer("frame_fm_geometry", "Admitted Pair Baselines", "line", baselines, source_count=len(baselines)),
            _layer("frame_fm_geometry", "Admitted Pair Circles", "circle", circles, source_count=len(circles)),
        ]

        return _sanitize_floats({
            "iid": iid,
            "frame_index": frame.frame_index,
            "available": True,
            "direction": "CW" if sweep_direction == 1 else "CCW",
            "period_s": model.period_s,
            "quality": frame.quality,
            "ref_icao": frame.ref_icao,
            "n_aircraft": 1 + len(frame.observations),
            "selection_diagnostics": diagnostics,
            "observations": admitted_pair_circles,
            "admitted_pair_circles": admitted_pair_circles,
            "inlier_pair_circles": inlier_pair_circles,
            "pair_circle_summary": pair_circle_summary,
            "layers": layers,
            "frame_lat": round(frame_lat, 6) if frame_lat is not None else None,
            "frame_lon": round(frame_lon, 6) if frame_lon is not None else None,
            "frame_cep_km": round(frame_cep_km, 2) if frame_cep_km is not None else None,
            "frame_n_arcs": frame_n_arcs,
            "frame_n_inlier_pair_circles": frame_n_arcs,
            "frame_solve_reason": frame_solve_reason,
            "would_accumulate": would_accumulate,
            "accumulation_rejection_reason": accumulation_rejection_reason,
            "accumulation_gate_metrics": accumulation_gate_metrics,
            "candidate_clusters": candidate_clusters,
            "scored_pair_circles": solve_result.get("scored_pair_circles", []) if not solve_result.get("success") else result.get("scored_pair_circles", []),
        })
    finally:
        _record_api_timing("iid_sweep_frame_fm_geometry", t0)


@router.post("/iids/{iid}/sweep-frames/{frame_index}/fm-geometry/manual-preview")
async def post_iid_sweep_frame_fm_geometry_manual_preview(
    iid: int,
    frame_index: int,
    body: dict,
):
    """Run a manual preview solve using only a user-selected subset of admitted pairs.

    This endpoint never writes to the accumulation buffer or the stored FM position.
    It returns the same diagnostic shape as the normal fm-geometry endpoint so the
    UI can overlay the preview result on the map.
    """
    t0 = time.perf_counter()
    try:
        if _state is None:
            return {"iid": iid, "frame_index": frame_index, "available": False, "reason": "radar module not initialised"}

        model = _state.get_rotation_model(iid)
        if model is None or model.period_s is None:
            return {"iid": iid, "frame_index": frame_index, "available": False, "reason": "rotation model with period required"}

        frame = next((f for f in _state.get_sweep_frames(iid) if f.frame_index == frame_index), None)
        if frame is None:
            return {"iid": iid, "frame_index": frame_index, "available": False, "reason": "frame not found"}
        if not frame.observations:
            return {"iid": iid, "frame_index": frame_index, "available": False, "reason": "frame has no observations"}

        direction = int(body.get("direction", 1))
        selected_circle_indices = body.get("selected_circle_indices", [])
        if not selected_circle_indices:
            return {"iid": iid, "frame_index": frame_index, "available": False, "reason": "no selected_circle_indices provided"}

        sweep_direction = 1 if direction >= 0 else -1
        import config as _config
        from .forward_model import ForwardModel
        recv_lat = getattr(_config, "RECEIVER_LAT", None)
        recv_lon = getattr(_config, "RECEIVER_LON", None)
        if recv_lat is None or recv_lon is None:
            return {"iid": iid, "frame_index": frame_index, "available": False, "reason": "receiver coordinates not configured"}

        solve_result = ForwardModel._solve_by_intersection_attempt(
            [frame],
            model.period_s,
            recv_lat,
            recv_lon,
            direction=sweep_direction,
            max_per_frame=50,
            max_per_icao=50,
            manual_selected_circle_indices=set(int(idx) for idx in selected_circle_indices),
            manually_forced_preview=True,
        )

        # Reuse the same extraction logic as the GET endpoint
        admitted_pair_circles = []
        inlier_pair_circles = []
        pair_circle_summary = {}
        candidate_clusters = []
        diagnostics = {}
        frame_lat = None
        frame_lon = None
        frame_cep_km = None
        frame_n_arcs = None
        frame_solve_reason = None
        would_accumulate = None
        accumulation_rejection_reason = None
        accumulation_gate_metrics: Optional[dict] = None

        if not solve_result.get("success"):
            frame_solve_reason = solve_result.get("reason", "unknown")
            diagnostics = solve_result.get("detail", {}).get("selection_diagnostics", {})
            admitted_pair_circles = solve_result.get("admitted_pair_circles", [])
            pair_circle_summary = solve_result.get("pair_circle_summary", {})
            candidate_clusters = solve_result.get("candidate_clusters", [])
            # Even on solver rejection, check accumulation admission against the
            # best candidate cluster if available.  This is only a prediction — the
            # manual preview never writes to the accumulation buffer.
            from .forward_model import ForwardModel
            _clusters = solve_result.get("candidate_clusters", [])
            if _clusters:
                best_cluster = _clusters[0]
                second_cluster = _clusters[1] if len(_clusters) > 1 else {}
                _synthetic_result = {
                    "centroid_uncertainty_km": best_cluster.get("cluster_compactness_km"),
                    "n_inlier_pair_circles": best_cluster.get("cluster_raw_inlier_count"),
                    "n_contributing_arcs": best_cluster.get("member_count"),
                    "best_cluster_support_score": best_cluster.get("cluster_support_score"),
                    "support_dominance_ratio": best_cluster.get("support_dominance_ratio"),
                    "pairwise_weighted_rms_deg": best_cluster.get("pairwise_weighted_rms_deg"),
                    "cluster_member_count": best_cluster.get("member_count", 0),
                    "second_cluster_member_count": second_cluster.get("member_count", 0),
                    "member_dominance_ratio": best_cluster.get("member_dominance_ratio"),
                    "weight_dominance_ratio": (
                        best_cluster.get("cluster_total_weight", 0.0) / second_cluster.get("cluster_total_weight", 1.0)
                        if second_cluster.get("cluster_total_weight", 0.0) > 0.0 else None
                    ),
                }
                _rej, _tier = ForwardModel._classify_frame_for_accumulation(_synthetic_result)
                would_accumulate = _rej is None
                accumulation_rejection_reason = _rej
                accumulation_gate_metrics = {**_synthetic_result, "admission_tier": _tier}
        else:
            result = solve_result["result"]
            admitted_pair_circles = result.get("admitted_pair_circles", [])
            inlier_pair_circles = result.get("inlier_pair_circles", [])
            pair_circle_summary = result.get("pair_circle_summary", {})
            diagnostics = result.get("selection_diagnostics", {})
            candidate_clusters = result.get("candidate_clusters", [])
            raw_cep = result.get("centroid_uncertainty_km")
            raw_arcs = result.get("n_contributing_arcs", 0)
            frame_lat = result.get("lat")
            frame_lon = result.get("lon")
            if raw_cep is not None:
                frame_cep_km = raw_cep
                frame_n_arcs = raw_arcs
            # Accumulation admission check (same as GET endpoint).
            _rej, _tier = ForwardModel._classify_frame_for_accumulation(result)
            would_accumulate = _rej is None
            accumulation_rejection_reason = _rej
            accumulation_gate_metrics = {
                "centroid_uncertainty_km": result.get("centroid_uncertainty_km"),
                "n_inlier_pair_circles": result.get("n_inlier_pair_circles", result.get("n_contributing_arcs", 0)),
                "best_cluster_support_score": result.get("best_cluster_support_score"),
                "support_dominance_ratio": result.get("support_dominance_ratio"),
                "pairwise_weighted_rms_deg": result.get("pairwise_weighted_rms_deg"),
                "cluster_member_count": result.get("cluster_member_count", 0),
                "second_cluster_member_count": result.get("second_cluster_member_count", 0),
                "member_dominance_ratio": result.get("member_dominance_ratio"),
                "weight_dominance_ratio": result.get("weight_dominance_ratio"),
                "admission_tier": _tier,
            }

        # Build the same layers structure as the GET endpoint so the UI can
        # render preview geometry on the map.
        points = [
            _point_feature(
                frame.ref_lat,
                frame.ref_lon,
                icao=frame.ref_icao,
                role="frame_aircraft",
                source_role="sweep_reference",
                arrival_us=frame.ref_arrival_us,
            )
        ]
        aircraft_by_icao = {
            frame.ref_icao: {
                "lat": frame.ref_lat,
                "lon": frame.ref_lon,
                "arrival_us": frame.ref_arrival_us,
                "interpolated": False,
            }
        }
        for obs in frame.observations:
            aircraft_by_icao[obs.icao] = {
                "lat": obs.lat,
                "lon": obs.lon,
                "arrival_us": obs.arrival_us,
                "interpolated": obs.interpolated,
            }
            points.append(_point_feature(
                obs.lat,
                obs.lon,
                icao=obs.icao,
                role="frame_aircraft",
                arrival_us=obs.arrival_us,
                interpolated=obs.interpolated,
            ))

        baselines: list[dict] = []
        circles: list[dict] = []

        def from_receiver_xy(x_km: float, y_km: float) -> tuple[float, float]:
            r_km = 6371.0
            cos_orig = max(math.cos(math.radians(recv_lat)), 1e-12)
            lat = recv_lat + math.degrees(y_km / r_km)
            lon = recv_lon + math.degrees(x_km / (r_km * cos_orig))
            return lat, lon

        MIN_CEP_KM = 0.05
        if frame_lat is not None and frame_lon is not None and frame_cep_km is not None:
            if frame_cep_km >= MIN_CEP_KM:
                points.append(_point_feature(
                    frame_lat,
                    frame_lon,
                    role="frame_estimate",
                    source="frame_fm",
                    cep_km=round(frame_cep_km, 2),
                    n_arcs=frame_n_arcs,
                ))
                circles.append(_circle_feature(
                    frame_lat,
                    frame_lon,
                    frame_cep_km,
                    circle_type="frame_cep",
                    source="frame_fm",
                    selected=True,
                    cep_km=round(frame_cep_km, 2),
                    n_arcs=frame_n_arcs,
                ))

        for pair in admitted_pair_circles:
            a = aircraft_by_icao.get(pair.get("icao_a"))
            b = aircraft_by_icao.get(pair.get("icao_b"))
            if a is not None and b is not None:
                baselines.append(_line_feature(
                    [(a["lat"], a["lon"]), (b["lat"], b["lon"])],
                    icao_a=pair.get("icao_a"),
                    icao_b=pair.get("icao_b"),
                    pair_label=f"{pair.get('icao_a')} <-> {pair.get('icao_b')}",
                    circle_index=pair.get("circle_index"),
                    inlier=pair.get("inlier", False),
                    selected=True,
                    line_type="admitted_pair_baseline",
                    normalized_residual=pair.get("normalized_residual"),
                ))
            if pair.get("cx_km") is not None and pair.get("cy_km") is not None and pair.get("R_km") is not None:
                center_lat, center_lon = from_receiver_xy(pair["cx_km"], pair["cy_km"])
                circles.append(_circle_feature(
                    center_lat,
                    center_lon,
                    pair["R_km"],
                    circle_type="admitted_pair_circle",
                    circle_index=pair.get("circle_index"),
                    icao_a=pair.get("icao_a"),
                    icao_b=pair.get("icao_b"),
                    pair_label=f"{pair.get('icao_a')} <-> {pair.get('icao_b')}",
                    selected=True,
                    inlier=pair.get("inlier", False),
                    circle_score=pair.get("circle_score"),
                    normalized_residual=pair.get("normalized_residual"),
                ))

        layers = [
            _layer("frame_fm_geometry", "Frame Aircraft", "point", points, source_count=len(points)),
            _layer("frame_fm_geometry", "Admitted Pair Baselines", "line", baselines, source_count=len(baselines)),
            _layer("frame_fm_geometry", "Admitted Pair Circles", "circle", circles, source_count=len(circles)),
        ]

        return _sanitize_floats({
            "iid": iid,
            "frame_index": frame.frame_index,
            "available": True,
            "direction": "CW" if sweep_direction == 1 else "CCW",
            "period_s": model.period_s,
            "quality": frame.quality,
            "ref_icao": frame.ref_icao,
            "n_aircraft": 1 + len(frame.observations),
            "selection_diagnostics": diagnostics,
            "observations": admitted_pair_circles,
            "admitted_pair_circles": admitted_pair_circles,
            "inlier_pair_circles": inlier_pair_circles,
            "pair_circle_summary": pair_circle_summary,
            "candidate_clusters": candidate_clusters,
            "layers": layers,
            "frame_lat": round(frame_lat, 6) if frame_lat is not None else None,
            "frame_lon": round(frame_lon, 6) if frame_lon is not None else None,
            "frame_cep_km": round(frame_cep_km, 2) if frame_cep_km is not None else None,
            "frame_n_arcs": frame_n_arcs,
            "frame_n_inlier_pair_circles": frame_n_arcs,
            "frame_solve_reason": frame_solve_reason,
            "would_accumulate": would_accumulate,
            "accumulation_rejection_reason": accumulation_rejection_reason,
            "scored_pair_circles": solve_result.get("scored_pair_circles", []),
            "manually_forced_preview": True,
            "automatic_acceptance_status": solve_result.get("automatic_acceptance_status", "accepted"),
        })
    finally:
        _record_api_timing("iid_sweep_frame_fm_geometry_manual_preview", t0)


@router.get("/iids/{iid}/reference-aircraft")
async def get_iid_reference_aircraft(iid: int):
    """Return current reference aircraft selection state."""
    if _state is None:
        return {"iid": iid, "status": "NOT_INITIALISED"}

    info = _state.get_reference_aircraft(iid)
    return {"iid": iid, **info}


@router.get("/iids/{iid}/pipeline-health")
async def get_iid_pipeline_health(iid: int):
    """Return 5-stage pipeline health for an IID."""
    t0 = time.perf_counter()
    try:
        if _state is None:
            return {
                "iid": iid,
                "stages": {
                    "period": {"status": "not_started", "detail": "Radar module not initialised"},
                    "reference": {"status": "not_started", "detail": ""},
                    "frames": {"status": "not_started", "detail": ""},
                    "scoring": {"status": "not_started", "detail": ""},
                    "optimisation": {"status": "not_started", "detail": ""},
                },
            }

        counts = _state.get_live_frame_counts(iid)
        lightweight_frames = [SimpleNamespace(quality="good") for _ in range(counts["n_good"])]
        if counts["n_frames"] > counts["n_good"]:
            lightweight_frames.extend(
                SimpleNamespace(quality="marginal")
                for _ in range(counts["n_frames"] - counts["n_good"])
            )
        return {"iid": iid, **_state.get_pipeline_health(iid, sweep_frames=lightweight_frames)}
    finally:
        _record_api_timing("iid_pipeline_health", t0)


@router.get("/iids/{iid}/pipeline-debug")
async def get_iid_pipeline_debug(iid: int):
    """Detailed Stage 3/4 live radar diagnostics for one IID."""
    t0 = time.perf_counter()
    try:
        if _state is None:
            return {"iid": iid, "available": False, "reason": "radar module not initialised"}

        python_debug = _state.get_live_pipeline_debug(iid)

        radar_core_stats = _radar_core_stats_provider() if _radar_core_stats_provider is not None else {}
        latest_health = radar_core_stats.get("latest_health") or {}
        latest_snapshot = radar_core_stats.get("latest_snapshot") or {}
        go_iids = latest_snapshot.get("iids") or {}
        go_iid = go_iids.get(str(iid))
        if go_iid is None:
            go_iid = go_iids.get(iid)

        go_frame = (go_iid or {}).get("frame_accumulator") or {}
        go_gate_counts = go_frame.get("gate_counts") or {}
        go_frames_emitted_total = latest_snapshot.get("frames_emitted", latest_health.get("fe"))
        python_frames_received = radar_core_stats.get("frames_received")
        go_iid_completed = go_frame.get("completed_frames", 0)
        frame_ready_seen = bool((python_frames_received or 0) > 0 or go_iid_completed > 0)

        dominant_blocking_gate = None
        if not frame_ready_seen and go_gate_counts:
            non_emit = {
                key: int(value)
                for key, value in go_gate_counts.items()
                if key != "frame_emitted"
            }
            if non_emit:
                dominant_blocking_gate = max(non_emit.items(), key=lambda item: item[1])[0]

        return {
            "iid": iid,
            "available": True,
            "python": python_debug,
            "go": {
                "client_connected": bool(radar_core_stats.get("connected", False)),
                "frames_received_by_python": python_frames_received,
                "frames_emitted_total": go_frames_emitted_total,
                "frame_ready_seen": frame_ready_seen,
                "latest_health_age_s": radar_core_stats.get("latest_health_age_s"),
                "latest_snapshot_age_s": radar_core_stats.get("latest_snapshot_age_s"),
                "iid_snapshot_available": go_iid is not None,
                "iid_state": {
                    "status": (go_iid or {}).get("status"),
                    "has_period": bool((go_iid or {}).get("has_period", False)),
                    "period_s": (go_iid or {}).get("period_s"),
                    "has_reference_icao": bool((go_iid or {}).get("has_reference_icao", False)),
                    "reference_icao": (go_iid or {}).get("reference_icao"),
                    "sync_state_present": bool((go_iid or {}).get("sync_state_present", False)),
                    "sync_state_usable": bool((go_iid or {}).get("sync_state_usable", False)),
                    "sync_quality": (go_iid or {}).get("sync_quality"),
                },
                "frame_accumulator": go_frame,
                "dominant_blocking_gate": dominant_blocking_gate,
            },
            "inferred_blocker": (
                "go_iid_snapshot_unavailable" if go_iid is None else
                "go_missing_period" if not (go_iid or {}).get("has_period") else
                "go_missing_reference_icao" if not (go_iid or {}).get("has_reference_icao") else
                "go_no_frame_ready" if not frame_ready_seen else
                "python_sync_state_absent" if not python_debug.get("sync_state_present") else
                "python_sync_snapshot_empty" if (
                    python_debug.get("sync_state_present")
                    and python_debug.get("sync_snapshot_observations_count", 0) <= 0
                ) else
                None
            ),
        }
    finally:
        _record_api_timing("iid_pipeline_debug", t0)


@router.get("/iids/pipeline-health")
async def get_all_pipeline_health():
    """Aggregated 5-stage pipeline health for all IIDs.

    Returns a dict keyed by IID string, each with the same stages structure
    as the per-IID endpoint. Used by the frontend IID selector to show
    pipeline stage status pills without N separate requests.
    """
    if _state is None:
        return {}

    models = _state.get_all_rotation_models()
    result = {}
    for iid in models:
        health = _state.get_pipeline_health(iid)
        result[str(iid)] = health.get("stages", {})
    return result


@router.post("/iids/{iid}/set-reference-aircraft")
async def set_reference_aircraft(iid: int, icao: dict):
    """Manually set the reference aircraft for an IID's next FM run.

    Body: {"icao": "ABC123"}  — set override
    Body: {"icao": null}       — clear override, revert to auto-selection

    The override is temporary: the next FM run will use it, but subsequent
    runs will re-evaluate unless re-set.
    """
    if _state is None:
        return {"iid": iid, "success": False, "reason": "Radar module not initialised"}

    target_icao = icao.get("icao")  # may be None to clear
    _state.set_reference_aircraft_override(iid, target_icao)
    return {
        "iid": iid,
        "success": True,
        "reference_icao": target_icao,
    }


@router.get("/iids/{iid}/df11-flash")
def get_df11_flash(iid: int, since: int = Query(0)):
    """Return DF11 flash events for this IID since the given sequence number.

    Poll at ~100ms for real-time sweep diagram dot flashing.
    Returns {events: [{seq, icao, ts_us}], seq: <latest_seq>}.
    """
    import config as _config
    if not _config.RADAR_DIAGNOSTICS:
        return {"events": [], "seq": since, "diagnostics_disabled": True}
    if _state is None:
        return {"events": [], "seq": 0}
    events = [
        {"seq": seq, "icao": icao, "ts_us": ts_us}
        for seq, ev_iid, icao, ts_us in list(_state._flash_events)
        if ev_iid == iid and seq > since
    ]
    return {"events": events, "seq": events[-1]["seq"] if events else since}


# ---------------------------------------------------------------------------
# Lean live-state stream (Stage 7)
# ---------------------------------------------------------------------------

def build_radar_live_state_payload(state: "RadarState" | None) -> dict:
    """Build the lean multi-IID live-state payload for /ws/radar/live.

    One entry per active IID: current period, sync health, localiser result.
    No observation arrays — frontend accumulates rolling history from this stream.
    """
    server_ts = time.time()
    if state is None:
        return {"type": "radar_live", "server_ts": server_ts, "iids": []}

    models = state.get_all_rotation_models()
    iids_out = []
    with state._lock:
        for iid, model in sorted(models.items()):
            sync = state._live_sync_states.get(iid)
            entry: dict = {
                "iid": iid,
                "status": model.status,
                "period_s": model.period_s,
                "period_std_s": model.period_std_s,
                "rpm": model.rpm,
                "last_updated": model.last_updated,
                "ref_icao": model.reference_aircraft.ref_icao if model.reference_aircraft else None,
                # Localiser best-estimate (first non-None source wins: manual > CI > FM > TDOA)
                "localiser": _authoritative_position(model),
                # Operator-set labels (slow-changing, needed for IID table)
                "manual_note": model.manual_note,
                "unresolvable_reason": model.unresolvable_reason,
                # Sync health — None when no sync state yet
                "sync": None,
            }
            if sync is not None:
                entry["sync"] = {
                    "period_s": sync.period_s,
                    "period_correction_ppm": sync.period_correction_ppm,
                    "period_correction_status": sync.period_correction_status,
                    "sync_jitter_deg": sync.sync_jitter_deg,
                    "residual_ema_deg": sync.residual_ema_deg,
                    "n_sync_frames": sync.n_sync_frames,
                    "n_rejected_frames": sync.n_rejected_frames,
                    "holdover": sync.holdover,
                    "phase_anchor_icao": sync.phase_anchor_icao,
                    "fit_support_count": sync.n_burst_obs_inliers,
                    "fit_reject_count": sync.n_burst_obs_rejected,
                    "usable": sync.usable,
                }
            iids_out.append(entry)

    return {"type": "radar_live", "server_ts": server_ts, "iids": iids_out}


def _radar_live_signature(state: "RadarState" | None) -> tuple:
    """Lightweight change-detection signature across all active IIDs."""
    if state is None:
        return ()
    sig_parts = []
    with state._lock:
        for iid, model in state._models.items():
            sync = state._live_sync_states.get(iid)
            sig_parts.append((
                iid,
                model.period_s,
                model.last_updated,
                model.lat, model.lon,
                model.fm_lat, model.fm_lon,
                getattr(sync, "last_sync_update_ts", None),
                getattr(sync, "period_s", None),
                getattr(sync, "n_sync_frames", None),
                getattr(sync, "holdover", None),
            ))
    return tuple(sig_parts)


@router.get("/live/latest")
async def get_radar_live_latest():
    """Current lean live-state snapshot for all active IIDs (initial page load)."""
    return build_radar_live_state_payload(_state)


@router.get("/adsb-tracker")
async def get_adsb_tracker():
    """Diagnostic: show ADS-B position tracker state."""
    if _state is None:
        return {"error": "radar module not initialised"}
    tracker = _state._adsb_tracker
    with tracker._lock:
        count = len(tracker._positions)
        icaos = {}
        for icao, entry in tracker._positions.items():
            icaos[icao] = {
                "lat": entry["lat"],
                "lon": entry["lon"],
                "gs": entry.get("groundspeed_kts"),
                "track": entry.get("track_deg"),
                "age_s": round(time.time() - entry["ts"], 1),
            }
    return {"count": count, "icaos": dict(sorted(icaos.items())[:50])}
