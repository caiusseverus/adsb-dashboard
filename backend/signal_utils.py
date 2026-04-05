"""Shared signal conversion helpers.

The dashboard's canonical display contract uses readsb-style dBFS semantics:
`0 dBFS` is strongest/full scale and weaker signals become more negative.
"""

from __future__ import annotations

import math
from collections.abc import Iterable


_READSB_AIRCRAFT_SIGNAL_WINDOW = 8
_READSB_SIGNAL_EPSILON = 1e-5


def clamp_raw_signal(raw: float | int | None) -> int | None:
    if raw is None:
        return None
    return max(0, min(255, int(round(float(raw)))))


def raw_signal_to_power(raw: float | int | None) -> float | None:
    """Convert Beast amplitude byte to readsb-style normalized power."""
    clamped = clamp_raw_signal(raw)
    if clamped is None:
        return None
    amplitude = clamped / 255.0
    return amplitude * amplitude


def power_to_dbfs(power: float | None, epsilon: float = 0.0) -> float | None:
    if power is None:
        return None
    level = max(0.0, float(power)) + max(0.0, float(epsilon))
    if level <= 0.0:
        return None
    return round(10.0 * math.log10(level), 1)


def raw_signal_to_dbfs(raw: float | int | None) -> float | None:
    """Convert a Beast-style raw RSSI byte into display-grade dBFS."""
    return power_to_dbfs(raw_signal_to_power(raw))


def average_raw_signals_to_dbfs(raw_values: Iterable[float | int | None]) -> float | None:
    """Match readsb's aircraft RSSI display smoothing over the last 8 samples."""
    powers = [power for raw in raw_values if (power := raw_signal_to_power(raw)) is not None]
    if not powers:
        return None
    avg_power = (sum(powers) + _READSB_SIGNAL_EPSILON) / _READSB_AIRCRAFT_SIGNAL_WINDOW
    return power_to_dbfs(avg_power)


def clamp_dbfs(dbfs: float | int | None) -> float | None:
    if dbfs is None:
        return None
    return max(-127.5, min(0.0, round(float(dbfs), 1)))


def dbfs_to_raw_signal(dbfs: float | int | None) -> int | None:
    """Convert display-grade dBFS back to the Beast/readsb raw-byte equivalent."""
    clamped = clamp_dbfs(dbfs)
    if clamped is None:
        return None
    amplitude = 10.0 ** (clamped / 20.0)
    return clamp_raw_signal(amplitude * 255.0)
