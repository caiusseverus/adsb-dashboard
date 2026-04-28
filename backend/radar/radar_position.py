from __future__ import annotations

from typing import Any


def _none_position() -> dict[str, Any]:
    return {"source": "none", "lat": None, "lon": None, "cep_m": None}


def get_authoritative_radar_position(model: Any) -> dict[str, Any]:
    """Return the best-available radar position for a radar model.

    Priority: manual > CI > FM > TDOA (by lowest CEP).
    """
    if model is None:
        return _none_position()

    if getattr(model, "resolution_mode", None) == "locked_unresolvable":
        return _none_position()

    if (
        getattr(model, "resolution_mode", None) == "locked_position"
        and getattr(model, "manual_lat", None) is not None
        and getattr(model, "manual_lon", None) is not None
    ):
        return {
            "source": "manual",
            "lat": model.manual_lat,
            "lon": model.manual_lon,
            "cep_m": None,
        }

    candidates: list[dict[str, Any]] = []
    if getattr(model, "ci_lat", None) is not None and getattr(model, "ci_lon", None) is not None:
        candidates.append({
            "source": "ci",
            "lat": model.ci_lat,
            "lon": model.ci_lon,
            "cep_m": getattr(model, "ci_cep_m", None),
        })
    if getattr(model, "fm_lat", None) is not None and getattr(model, "fm_lon", None) is not None:
        candidates.append({
            "source": "fm",
            "lat": model.fm_lat,
            "lon": model.fm_lon,
            "cep_m": getattr(model, "fm_cep_m", None),
        })
    if (
        getattr(model, "lat", None) is not None
        and getattr(model, "lon", None) is not None
        and not bool(getattr(model, "multi_radar_flag", False))
    ):
        candidates.append({
            "source": "tdoa",
            "lat": model.lat,
            "lon": model.lon,
            "cep_m": getattr(model, "cep_m", None),
        })

    if not candidates:
        return _none_position()

    candidates.sort(key=lambda c: (c.get("cep_m") is None, c.get("cep_m") or float("inf")))
    return candidates[0]
