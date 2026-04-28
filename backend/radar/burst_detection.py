from __future__ import annotations


BURST_GAP_US = 200_000


def _signal_weight(signal_dbfs: float | None) -> float | None:
    """Convert canonical dBFS into a positive relative weight."""
    if signal_dbfs is None:
        return None
    return 10 ** (signal_dbfs / 20.0)


def refine_burst_center(reply_samples: list[tuple[float, float | None]]) -> dict:
    """Estimate a better beam-centre timestamp from per-reply timing and amplitude."""
    arrivals_us = [arrival_us for arrival_us, _signal_dbfs in reply_samples]
    raw_centroid_us = sum(arrivals_us) / len(arrivals_us)

    weighted_samples: list[tuple[float, float]] = []
    for arrival_us, signal_dbfs in reply_samples:
        weight = _signal_weight(signal_dbfs)
        if weight is not None and weight > 0:
            weighted_samples.append((arrival_us, weight))

    if len(weighted_samples) < 2:
        return {
            "beam_center_us": raw_centroid_us,
            "beam_center_method": "centroid",
            "beam_center_simple_us": raw_centroid_us,
            "beam_center_weighted_us": None,
            "beam_center_delta_us": 0.0,
        }

    weight_sum = sum(weight for _arrival_us, weight in weighted_samples)
    if weight_sum <= 0:
        return {
            "beam_center_us": raw_centroid_us,
            "beam_center_method": "centroid",
            "beam_center_simple_us": raw_centroid_us,
            "beam_center_weighted_us": None,
            "beam_center_delta_us": 0.0,
        }

    weighted_center = (
        sum(arrival_us * weight for arrival_us, weight in weighted_samples) / weight_sum
    )
    return {
        "beam_center_us": weighted_center,
        "beam_center_method": "amplitude_weighted",
        "beam_center_simple_us": raw_centroid_us,
        "beam_center_weighted_us": weighted_center,
        "beam_center_delta_us": weighted_center - raw_centroid_us,
    }


def _compute_burst_timestamp_candidates(reply_samples: list[tuple[float, float | None]]) -> dict:
    """Return diagnostic timestamp definitions for one burst in Beast microseconds."""
    if not reply_samples:
        return {
            "burst_ts_first_reply_beast_us": None,
            "burst_ts_strongest_reply_beast_us": None,
            "burst_ts_simple_centroid_beast_us": None,
            "burst_ts_weighted_centroid_beast_us": None,
            "burst_ts_mid_strong_window_beast_us": None,
            "burst_ts_last_reply_beast_us": None,
            "burst_span_us": None,
            "peak_amplitude": None,
        }

    samples = sorted(reply_samples, key=lambda item: item[0])
    arrivals_us = [float(arrival_us) for arrival_us, _signal_dbfs in samples]
    simple_centroid = sum(arrivals_us) / len(arrivals_us)
    weighted_samples: list[tuple[float, float]] = []
    strongest_ts = None
    strongest_signal = None
    for arrival_us, signal_dbfs in samples:
        if signal_dbfs is None:
            continue
        if strongest_signal is None or signal_dbfs > strongest_signal:
            strongest_signal = signal_dbfs
            strongest_ts = float(arrival_us)
        weight = _signal_weight(signal_dbfs)
        if weight is not None and weight > 0:
            weighted_samples.append((float(arrival_us), weight))

    weighted_centroid = None
    if len(weighted_samples) >= 2:
        weight_sum = sum(weight for _arrival_us, weight in weighted_samples)
        if weight_sum > 0:
            weighted_centroid = (
                sum(arrival_us * weight for arrival_us, weight in weighted_samples) / weight_sum
            )

    mid_strong_window = None
    if strongest_signal is not None:
        strong_arrivals = [
            float(arrival_us)
            for arrival_us, signal_dbfs in samples
            if signal_dbfs is not None and signal_dbfs >= strongest_signal - 6.0
        ]
        if strong_arrivals:
            mid_strong_window = (min(strong_arrivals) + max(strong_arrivals)) / 2.0

    return {
        "burst_ts_first_reply_beast_us": arrivals_us[0],
        "burst_ts_strongest_reply_beast_us": strongest_ts,
        "burst_ts_simple_centroid_beast_us": simple_centroid,
        "burst_ts_weighted_centroid_beast_us": weighted_centroid,
        "burst_ts_mid_strong_window_beast_us": mid_strong_window,
        "burst_ts_last_reply_beast_us": arrivals_us[-1],
        "burst_span_us": arrivals_us[-1] - arrivals_us[0] if len(arrivals_us) > 1 else 0.0,
        "peak_amplitude": strongest_signal,
    }


def detect_bursts(arrival_us_list: list[float]) -> list[dict]:
    """Group sorted arrival times into bursts separated by BURST_GAP_US."""
    if not arrival_us_list:
        return []

    sorted_ts = (
        arrival_us_list
        if all(
            arrival_us_list[i] <= arrival_us_list[i + 1]
            for i in range(len(arrival_us_list) - 1)
        )
        else sorted(arrival_us_list)
    )
    groups: list[list[float]] = []
    current = [sorted_ts[0]]

    for ts in sorted_ts[1:]:
        if ts - current[-1] > BURST_GAP_US:
            groups.append(current)
            current = [ts]
        else:
            current.append(ts)
    groups.append(current)

    return [
        {
            "centroid_us": sum(b) // len(b),
            "n_replies": len(b),
            "span_us": b[-1] - b[0] if len(b) > 1 else 0,
            "arrivals_us": b,
        }
        for b in groups
    ]


def detect_bursts_with_signals(reply_samples: list[tuple[float, float | None]]) -> list[dict]:
    """Group sorted `(arrival_us, signal_dbfs)` samples into bursts."""
    if not reply_samples:
        return []

    sorted_samples = sorted(reply_samples, key=lambda item: item[0])
    groups: list[list[tuple[float, float | None]]] = []
    current = [sorted_samples[0]]

    for sample in sorted_samples[1:]:
        if sample[0] - current[-1][0] > BURST_GAP_US:
            groups.append(current)
            current = [sample]
        else:
            current.append(sample)
    groups.append(current)

    bursts = []
    for group in groups:
        arrivals_us = [arrival_us for arrival_us, _signal_dbfs in group]
        signals = [signal_dbfs for _arrival_us, signal_dbfs in group if signal_dbfs is not None]
        replies = [
            {"arrival_us": arrival_us, "signal_dbfs": signal_dbfs}
            for arrival_us, signal_dbfs in group
        ]
        refinement = refine_burst_center(group)
        timestamp_candidates = _compute_burst_timestamp_candidates(group)
        bursts.append(
            {
                "centroid_us": sum(arrivals_us) / len(arrivals_us),
                "n_replies": len(arrivals_us),
                "span_us": arrivals_us[-1] - arrivals_us[0] if len(arrivals_us) > 1 else 0,
                "arrivals_us": arrivals_us,
                "replies": replies,
                "signal_dbfs": round(sum(signals) / len(signals), 2) if signals else None,
                "beam_center_us": refinement["beam_center_us"],
                "beam_center_method": refinement["beam_center_method"],
                "beam_center_simple_us": refinement.get("beam_center_simple_us"),
                "beam_center_weighted_us": refinement.get("beam_center_weighted_us"),
                "beam_center_delta_us": refinement.get("beam_center_delta_us"),
                **timestamp_candidates,
            }
        )

    return bursts
