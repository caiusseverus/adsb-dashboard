"""
fm_live_validate.py — Repeated live forward-model validation for selected IIDs.

Calls the running backend's FM endpoints, triggers manual `fm-run` passes, and
prints a compact summary of acceptance/rejection plus the Stage 5 quality-gate
metrics. Use this to evaluate representative live IIDs across different traffic
periods without manually hitting each endpoint.

Examples:
    cd backend
    uv run python tools/fm_live_validate.py --iid 7 --iid 21
    uv run python tools/fm_live_validate.py --iid 7 --repeat 5 --delay 30
    uv run python tools/fm_live_validate.py --iid 7 --expected 7=51.4706,-0.4619
    uv run python tools/fm_live_validate.py --iid 7 --json-out /tmp/fm-live.json
"""

from __future__ import annotations

import argparse
import json
import math
import socket
import sys
import time
import urllib.error
import urllib.request


DEFAULT_HOST = "http://localhost:8000"
DEFAULT_TIMEOUT_S = 60.0


def _haversine_m(lat1: float, lon1: float, lat2: float, lon2: float) -> float:
    r = 6_371_000.0
    phi1, phi2 = math.radians(lat1), math.radians(lat2)
    dphi = math.radians(lat2 - lat1)
    dlam = math.radians(lon2 - lon1)
    a = math.sin(dphi / 2) ** 2 + math.cos(phi1) * math.cos(phi2) * math.sin(dlam / 2) ** 2
    return 2 * r * math.asin(math.sqrt(a))


def _request_json(
    url: str,
    method: str = "GET",
    body: dict | None = None,
    timeout_s: float = DEFAULT_TIMEOUT_S,
) -> dict:
    data = None
    headers = {}
    if body is not None:
        data = json.dumps(body).encode("utf-8")
        headers["Content-Type"] = "application/json"
    request = urllib.request.Request(url, method=method, data=data, headers=headers)
    with urllib.request.urlopen(request, timeout=timeout_s) as response:
        return json.loads(response.read())


def _get(path: str, host: str, timeout_s: float = DEFAULT_TIMEOUT_S) -> dict:
    return _request_json(host.rstrip("/") + path, timeout_s=timeout_s)


def _post(
    path: str,
    host: str,
    body: dict | None = None,
    timeout_s: float = DEFAULT_TIMEOUT_S,
) -> dict:
    return _request_json(host.rstrip("/") + path, method="POST", body=body, timeout_s=timeout_s)


def _parse_expected(values: list[str]) -> dict[int, tuple[float, float]]:
    expected: dict[int, tuple[float, float]] = {}
    for raw in values:
        try:
            iid_raw, coords_raw = raw.split("=", 1)
            lat_raw, lon_raw = coords_raw.split(",", 1)
            expected[int(iid_raw)] = (float(lat_raw), float(lon_raw))
        except ValueError as exc:
            raise SystemExit(f"Invalid --expected value '{raw}'. Use IID=LAT,LON.") from exc
    return expected


def _safe_get(mapping: dict, path: list[str], default=None):
    current = mapping
    for key in path:
        if not isinstance(current, dict) or key not in current:
            return default
        current = current[key]
    return current


def _failure_summary(
    iid: int,
    reason: str,
    stage: str,
    *,
    status_before: dict | None = None,
    diagnostics: dict | None = None,
    pipeline_health: dict | None = None,
    expected: tuple[float, float] | None = None,
) -> dict:
    summary = {
        "iid": iid,
        "rotation_period_s": _safe_get(status_before or {}, ["rotation_model", "period_s"]),
        "rotation_status": _safe_get(status_before or {}, ["rotation_model", "status"]),
        "multi_radar_flag": _safe_get(status_before or {}, ["rotation_model", "multi_radar_flag"]),
        "pipeline_health": (pipeline_health or {}).get("stages", {}),
        "data_funnel": (diagnostics or {}).get("data_funnel", {}),
        "raw_result": None,
        "success": False,
        "stage": stage,
        "reason": reason,
        "detail": {},
        "fm_status_after": {},
        "lat": None,
        "lon": None,
        "cep_m": None,
        "intersection_rms_km": None,
        "intersection_direction": None,
        "n_pairs": None,
        "n_frames": None,
        "n_good_frames": None,
        "n_selected_observations": None,
        "interpolated_fraction": None,
        "azimuth_spread_deg": None,
        "high_quality_frames": None,
        "cluster_member_count": None,
        "dominance_ratio": None,
        "receiver_distance_m": None,
        "raw_candidate_count": None,
        "plausible_candidate_count": None,
        "degenerate_baseline_pairs": None,
        "total_scored_observations": None,
        "eligible_observations": None,
        "dropped_low_quality": None,
        "dropped_weak_angle": None,
        "dropped_same_azimuth_bin": None,
        "dropped_per_icao_cap": None,
        "dropped_duplicate_icao_in_frame": None,
        "dropped_per_frame_cap": None,
        "stored": False,
        "refinement_enabled": False,
        "expected_lat": expected[0] if expected is not None else None,
        "expected_lon": expected[1] if expected is not None else None,
        "distance_to_expected_m": None,
    }
    return summary


def _collect_one_run(
    host: str,
    iid: int,
    expected: tuple[float, float] | None,
    timeout_s: float,
) -> dict:
    try:
        status_before = _get(f"/api/radar/iids/{iid}/fm-status", host, timeout_s=timeout_s)
        diagnostics = _get(f"/api/radar/iids/{iid}/fm-diagnostics", host, timeout_s=timeout_s)
        pipeline_health = _get(f"/api/radar/iids/{iid}/pipeline-health", host, timeout_s=timeout_s)
    except (urllib.error.URLError, TimeoutError, socket.timeout) as exc:
        return _failure_summary(
            iid,
            reason=f"preflight request failed: {exc}",
            stage="request_error",
            expected=expected,
        )

    try:
        run_result = _post(f"/api/radar/iids/{iid}/fm-run", host, timeout_s=timeout_s)
    except (urllib.error.URLError, TimeoutError, socket.timeout) as exc:
        return _failure_summary(
            iid,
            reason=f"fm-run request failed: {exc}",
            stage="request_timeout" if isinstance(exc, (TimeoutError, socket.timeout)) else "request_error",
            status_before=status_before,
            diagnostics=diagnostics,
            pipeline_health=pipeline_health,
            expected=expected,
        )

    try:
        status_after = _get(f"/api/radar/iids/{iid}/fm-status", host, timeout_s=timeout_s)
    except (urllib.error.URLError, TimeoutError, socket.timeout):
        status_after = {}

    summary = {
        "iid": iid,
        "rotation_period_s": _safe_get(status_before, ["rotation_model", "period_s"]),
        "rotation_status": _safe_get(status_before, ["rotation_model", "status"]),
        "multi_radar_flag": _safe_get(status_before, ["rotation_model", "multi_radar_flag"]),
        "pipeline_health": pipeline_health.get("stages", {}),
        "data_funnel": diagnostics.get("data_funnel", {}),
        "raw_result": run_result,
        "success": bool(run_result.get("success")),
        "stage": run_result.get("stage"),
        "reason": run_result.get("reason"),
        "detail": run_result.get("detail", {}),
        "fm_status_after": _safe_get(status_after, ["forward_model"], {}),
    }

    if summary["success"]:
        result = run_result.get("result", {})
        summary.update({
            "lat": result.get("lat"),
            "lon": result.get("lon"),
            "cep_m": result.get("cep_m"),
            "intersection_rms_km": result.get("intersection_rms_km"),
            "intersection_direction": result.get("intersection_direction"),
            "n_pairs": result.get("n_pairs"),
            "n_frames": result.get("n_frames"),
            "n_good_frames": result.get("n_good_frames"),
            "n_selected_observations": result.get("n_selected_observations"),
            "interpolated_fraction": result.get("interpolated_fraction"),
            "azimuth_spread_deg": result.get("azimuth_spread_deg"),
            "high_quality_frames": result.get("high_quality_frames"),
            "cluster_member_count": result.get("cluster_member_count"),
            "dominance_ratio": result.get("dominance_ratio"),
            "receiver_distance_m": result.get("receiver_distance_m"),
            "raw_candidate_count": result.get("raw_candidate_count"),
            "plausible_candidate_count": result.get("plausible_candidate_count"),
            "degenerate_baseline_pairs": result.get("degenerate_baseline_pairs"),
            "total_scored_observations": _safe_get(result, ["selection_diagnostics", "total_scored_observations"]),
            "eligible_observations": _safe_get(result, ["selection_diagnostics", "eligible_observations"]),
            "dropped_low_quality": _safe_get(result, ["selection_diagnostics", "dropped_low_quality"]),
            "dropped_weak_angle": _safe_get(result, ["selection_diagnostics", "dropped_weak_angle"]),
            "dropped_same_azimuth_bin": _safe_get(result, ["selection_diagnostics", "dropped_same_azimuth_bin"]),
            "dropped_per_icao_cap": _safe_get(result, ["selection_diagnostics", "dropped_per_icao_cap"]),
            "dropped_duplicate_icao_in_frame": _safe_get(result, ["selection_diagnostics", "dropped_duplicate_icao_in_frame"]),
            "dropped_per_frame_cap": _safe_get(result, ["selection_diagnostics", "dropped_per_frame_cap"]),
            "stored": result.get("stored"),
            "refinement_enabled": result.get("refinement_enabled"),
            "refinement_applied": result.get("refinement_applied"),
            "seed_fit_score": result.get("seed_fit_score"),
            "seed_residual_sigma_deg": result.get("seed_residual_sigma_deg"),
            "seed_mean_residual_deg": result.get("seed_mean_residual_deg"),
            "seed_replay_summary": result.get("seed_replay_summary"),
            "seed_example_frame": result.get("seed_example_frame"),
            "final_fit_score": result.get("final_fit_score"),
            "final_residual_sigma_deg": result.get("final_residual_sigma_deg"),
            "final_mean_residual_deg": result.get("final_mean_residual_deg"),
            "final_replay_summary": result.get("final_replay_summary"),
            "final_example_frame": result.get("final_example_frame"),
        })
    else:
        detail = summary["detail"]
        summary.update({
            "lat": detail.get("lat"),
            "lon": detail.get("lon"),
            "cep_m": detail.get("cep_m"),
            "intersection_rms_km": detail.get("intersection_rms_km"),
            "intersection_direction": detail.get("intersection_direction"),
            "n_pairs": detail.get("n_pairs"),
            "n_frames": detail.get("n_frames"),
            "n_good_frames": detail.get("n_good_frames"),
            "n_selected_observations": detail.get("n_selected_observations"),
            "interpolated_fraction": detail.get("interpolated_fraction"),
            "azimuth_spread_deg": detail.get("azimuth_spread_deg"),
            "high_quality_frames": detail.get("high_quality_frames"),
            "cluster_member_count": detail.get("cluster_member_count"),
            "dominance_ratio": detail.get("dominance_ratio"),
            "receiver_distance_m": detail.get("receiver_distance_m"),
            "raw_candidate_count": detail.get("raw_candidate_count"),
            "plausible_candidate_count": detail.get("plausible_candidate_count"),
            "degenerate_baseline_pairs": detail.get("degenerate_baseline_pairs"),
            "total_scored_observations": detail.get("total_scored_observations"),
            "eligible_observations": detail.get("eligible_observations"),
            "dropped_low_quality": detail.get("dropped_low_quality"),
            "dropped_weak_angle": detail.get("dropped_weak_angle"),
            "dropped_same_azimuth_bin": detail.get("dropped_same_azimuth_bin"),
            "dropped_per_icao_cap": detail.get("dropped_per_icao_cap"),
            "dropped_duplicate_icao_in_frame": detail.get("dropped_duplicate_icao_in_frame"),
            "dropped_per_frame_cap": detail.get("dropped_per_frame_cap"),
            "stored": False,
            "refinement_enabled": False,
            "refinement_applied": detail.get("refinement_applied"),
            "seed_fit_score": detail.get("seed_fit_score"),
            "seed_residual_sigma_deg": detail.get("seed_residual_sigma_deg"),
            "seed_mean_residual_deg": detail.get("seed_mean_residual_deg"),
            "seed_replay_summary": detail.get("seed_replay_summary"),
            "seed_example_frame": detail.get("seed_example_frame"),
            "final_fit_score": detail.get("final_fit_score"),
            "final_residual_sigma_deg": detail.get("final_residual_sigma_deg"),
            "final_mean_residual_deg": detail.get("final_mean_residual_deg"),
            "final_replay_summary": detail.get("final_replay_summary"),
            "final_example_frame": detail.get("final_example_frame"),
        })

    if expected is not None and summary.get("lat") is not None and summary.get("lon") is not None:
        summary["expected_lat"] = expected[0]
        summary["expected_lon"] = expected[1]
        summary["distance_to_expected_m"] = _haversine_m(
            expected[0], expected[1], summary["lat"], summary["lon"]
        )
    else:
        summary["expected_lat"] = None
        summary["expected_lon"] = None
        summary["distance_to_expected_m"] = None

    return summary


def _fmt(value) -> str:
    if value is None:
        return "-"
    if isinstance(value, float):
        if math.isinf(value):
            return "inf"
        if abs(value) >= 100:
            return f"{value:.1f}"
        return f"{value:.3f}"
    return str(value)


def _print_run_summary(run_idx: int, summary: dict) -> None:
    header = f"IID {summary['iid']} run {run_idx}"
    if summary["success"]:
        print(
            f"{header}: OK"
            f" stored={summary['stored']}"
            f" lat={_fmt(summary.get('lat'))}"
            f" lon={_fmt(summary.get('lon'))}"
            f" cep_m={_fmt(summary.get('cep_m'))}"
            f" rms_km={_fmt(summary.get('intersection_rms_km'))}"
            f" dist_m={_fmt(summary.get('distance_to_expected_m'))}"
            f" recv_m={_fmt(summary.get('receiver_distance_m'))}"
        )
    else:
        print(
            f"{header}: FAIL"
            f" stage={_fmt(summary.get('stage'))}"
            f" reason={summary.get('reason', 'unknown')}"
            f" rms_km={_fmt(summary.get('intersection_rms_km'))}"
            f" dist_m={_fmt(summary.get('distance_to_expected_m'))}"
            f" recv_m={_fmt(summary.get('receiver_distance_m'))}"
        )

    print(
        "  "
        f"pairs={_fmt(summary.get('n_pairs'))} "
        f"frames={_fmt(summary.get('n_frames'))}/{_fmt(summary.get('n_good_frames'))}good "
        f"selected={_fmt(summary.get('n_selected_observations'))} "
        f"eligible={_fmt(summary.get('eligible_observations'))} "
        f"az_spread={_fmt(summary.get('azimuth_spread_deg'))} "
        f"interp_frac={_fmt(summary.get('interpolated_fraction'))} "
        f"hq_frames={_fmt(summary.get('high_quality_frames'))} "
        f"cluster_members={_fmt(summary.get('cluster_member_count'))} "
        f"dominance={_fmt(summary.get('dominance_ratio'))}"
    )
    print(
        "  "
        f"scored={_fmt(summary.get('total_scored_observations'))} "
        f"drop_low_q={_fmt(summary.get('dropped_low_quality'))} "
        f"drop_weak={_fmt(summary.get('dropped_weak_angle'))} "
        f"drop_bin={_fmt(summary.get('dropped_same_azimuth_bin'))} "
        f"drop_icao={_fmt(summary.get('dropped_per_icao_cap'))} "
        f"drop_dup={_fmt(summary.get('dropped_duplicate_icao_in_frame'))} "
        f"drop_frame={_fmt(summary.get('dropped_per_frame_cap'))}"
    )
    print(
        "  "
        f"raw_candidates={_fmt(summary.get('raw_candidate_count'))} "
        f"plausible={_fmt(summary.get('plausible_candidate_count'))} "
        f"degenerate_pairs={_fmt(summary.get('degenerate_baseline_pairs'))}"
    )
    print(
        "  "
        f"seed_sigma_deg={_fmt(summary.get('seed_residual_sigma_deg'))} "
        f"final_sigma_deg={_fmt(summary.get('final_residual_sigma_deg'))} "
        f"refined={_fmt(summary.get('refinement_applied'))}"
    )
    replay = summary.get("final_replay_summary") or summary.get("seed_replay_summary") or {}
    if replay:
        print(
            "  "
            f"replay_residuals={_fmt(replay.get('n_residuals'))} "
            f"p90_abs_deg={_fmt(replay.get('p90_abs_residual_deg'))} "
            f"worst_frame={_fmt(replay.get('worst_frame_index'))} "
            f"worst_ref={_fmt(replay.get('worst_frame_ref_icao'))} "
            f"worst_rms_deg={_fmt(replay.get('worst_frame_weighted_rms_deg'))}"
        )
    example = summary.get("final_example_frame") or summary.get("seed_example_frame") or {}
    if example:
        print(
            "  "
            f"example_frame={_fmt(example.get('frame_index'))} "
            f"ref={_fmt(example.get('ref_icao'))} "
            f"obs={_fmt(example.get('n_observations'))} "
            f"selected={_fmt(example.get('n_selected_observations'))} "
            f"frame_sigma_deg={_fmt(example.get('residual_sigma_deg'))}"
        )


def _print_aggregate(results: list[dict]) -> None:
    print("\nAggregate:")
    by_iid: dict[int, list[dict]] = {}
    for result in results:
        by_iid.setdefault(result["iid"], []).append(result)

    for iid in sorted(by_iid):
        rows = by_iid[iid]
        successes = sum(1 for row in rows if row["success"])
        stored = sum(1 for row in rows if row.get("stored"))
        distances = [row["distance_to_expected_m"] for row in rows if row["distance_to_expected_m"] is not None]
        ceps = [row["cep_m"] for row in rows if row["cep_m"] is not None]
        stages: dict[str, int] = {}
        for row in rows:
            key = row.get("stage") or ("success" if row["success"] else "unknown")
            stages[key] = stages.get(key, 0) + 1

        print(
            f"  IID {iid}: runs={len(rows)} success={successes} stored={stored}"
            f" mean_cep_m={_fmt(sum(ceps) / len(ceps) if ceps else None)}"
            f" mean_dist_m={_fmt(sum(distances) / len(distances) if distances else None)}"
            f" stages={stages}"
        )


def main() -> None:
    parser = argparse.ArgumentParser(description="Repeated live FM validation for selected IIDs")
    parser.add_argument("--iid", type=int, action="append", required=True, help="IID to validate; may be repeated")
    parser.add_argument("--repeat", type=int, default=1, help="Number of FM runs per IID")
    parser.add_argument("--delay", type=float, default=0.0, help="Seconds to wait between repeated runs")
    parser.add_argument("--host", default=DEFAULT_HOST, help="Backend host, default http://localhost:8000")
    parser.add_argument(
        "--timeout",
        type=float,
        default=DEFAULT_TIMEOUT_S,
        help=f"Per-request timeout in seconds, default {DEFAULT_TIMEOUT_S:g}",
    )
    parser.add_argument(
        "--expected",
        action="append",
        default=[],
        help="Expected site as IID=LAT,LON; may be repeated",
    )
    parser.add_argument("--json-out", default=None, help="Optional path to write raw JSON results")
    args = parser.parse_args()

    expected_sites = _parse_expected(args.expected)
    results: list[dict] = []

    try:
        _get("/api/radar/iids", args.host, timeout_s=args.timeout)
    except (urllib.error.URLError, TimeoutError, socket.timeout) as exc:
        raise SystemExit(f"Failed to reach backend at {args.host}: {exc}") from exc

    for run_idx in range(1, args.repeat + 1):
        for iid in args.iid:
            summary = _collect_one_run(args.host, iid, expected_sites.get(iid), timeout_s=args.timeout)
            results.append(summary)
            _print_run_summary(run_idx, summary)
        if run_idx < args.repeat and args.delay > 0:
            time.sleep(args.delay)

    _print_aggregate(results)

    if args.json_out:
        with open(args.json_out, "w", encoding="utf-8") as handle:
            json.dump(results, handle, indent=2, sort_keys=True)
        print(f"\nWrote JSON results to {args.json_out}")


if __name__ == "__main__":
    main()
