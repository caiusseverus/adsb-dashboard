from __future__ import annotations

import math as _math


def _bearing_deg_simple(lat1: float, lon1: float, lat2: float, lon2: float) -> float:
    """Forward azimuth from point 1 to point 2 in [0, 360)."""
    phi1 = _math.radians(lat1)
    phi2 = _math.radians(lat2)
    dlam = _math.radians(lon2 - lon1)
    x = _math.cos(phi1) * _math.sin(phi2) - _math.sin(phi1) * _math.cos(phi2) * _math.cos(dlam)
    y = _math.sin(dlam) * _math.cos(phi2)
    return (_math.degrees(_math.atan2(y, x)) + 360) % 360


def _haversine_nm_simple(lat1: float, lon1: float, lat2: float, lon2: float) -> float:
    """Great-circle distance between two lat/lon points in nautical miles."""
    r_m = 6_371_000.0
    phi1 = _math.radians(lat1)
    phi2 = _math.radians(lat2)
    dphi = _math.radians(lat2 - lat1)
    dlam = _math.radians(lon2 - lon1)
    a = _math.sin(dphi / 2.0) ** 2 + _math.cos(phi1) * _math.cos(phi2) * _math.sin(dlam / 2.0) ** 2
    c = 2.0 * _math.asin(_math.sqrt(max(0.0, min(1.0, a))))
    return (r_m * c) / 1852.0
