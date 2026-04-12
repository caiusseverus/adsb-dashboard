"""
fm_diagnose.py — Forward model single-frame position diagnostic.

Fetches live sweep frames and rotation data from the running backend, then
runs the forward model optimiser on a single frame (or the best available frame)
and prints a detailed residual breakdown.

Usage:
    cd backend
    uv run python tools/fm_diagnose.py --iid 21
    uv run python tools/fm_diagnose.py --iid 21 --frame 5
    uv run python tools/fm_diagnose.py --iid 21 --all-frames  # score every frame
    uv run python tools/fm_diagnose.py --iid 21 --true-lat 51.5 --true-lon -0.45
"""

from __future__ import annotations

import argparse
import json
import math
import sys
import urllib.request
from typing import Optional


# ── Config ────────────────────────────────────────────────────────────────────

DEFAULT_HOST = "http://localhost:8000"


# ── Geometry helpers ──────────────────────────────────────────────────────────

def _bearing_deg(lat1: float, lon1: float, lat2: float, lon2: float) -> float:
    phi1, phi2 = math.radians(lat1), math.radians(lat2)
    dlam = math.radians(lon2 - lon1)
    x = math.cos(phi1) * math.sin(phi2) - math.sin(phi1) * math.cos(phi2) * math.cos(dlam)
    y = math.sin(dlam) * math.cos(phi2)
    return (math.degrees(math.atan2(y, x)) + 360) % 360


def _haversine_m(lat1: float, lon1: float, lat2: float, lon2: float) -> float:
    R = 6_371_000.0
    phi1, phi2 = math.radians(lat1), math.radians(lat2)
    dphi = math.radians(lat2 - lat1)
    dlam = math.radians(lon2 - lon1)
    a = math.sin(dphi / 2) ** 2 + math.cos(phi1) * math.cos(phi2) * math.sin(dlam / 2) ** 2
    return 2 * R * math.asin(math.sqrt(a))


# ── Scoring (mirrors forward_model.py exactly) ────────────────────────────────

def score_position(r_lat: float, r_lon: float, frame: dict, period_s: float,
                   direction: int = 1) -> tuple[float, list[dict]]:
    """Score a candidate position against one frame. Returns (score, per-obs details)."""
    ref_lat = frame["ref_lat"]
    ref_lon = frame["ref_lon"]
    ref_us = frame["ref_arrival_us"]

    details = []
    total_score = 0.0

    for obs in frame["observations"]:
        lat, lon = obs["lat"], obs["lon"]
        if lat is None or lon is None:
            continue

        dt_us = obs["arrival_us"] - ref_us
        dt_s = dt_us / 1_000_000.0
        observed_phase = ((dt_s / period_s) * 360.0) % 360.0

        bearing_obs = _bearing_deg(r_lat, r_lon, lat, lon)
        bearing_ref = _bearing_deg(r_lat, r_lon, ref_lat, ref_lon)
        predicted_phase = ((bearing_obs - bearing_ref) * direction) % 360.0

        residual = (observed_phase - predicted_phase + 540.0) % 360.0 - 180.0

        details.append({
            "icao": obs["icao"],
            "lat": lat,
            "lon": lon,
            "bearing_from_radar_deg": bearing_obs,
            "bearing_ref_deg": bearing_ref,
            "dt_s": round(dt_s, 4),
            "observed_phase_deg": round(observed_phase, 2),
            "predicted_phase_deg": round(predicted_phase, 2),
            "residual_deg": round(residual, 2),
            "interpolated": obs.get("interpolated", False),
        })
        total_score += residual ** 2

    return total_score, details


def optimise_position(frame: dict, period_s: float, initial_lat: float,
                      initial_lon: float) -> tuple[Optional[tuple], int, float]:
    """Run Nelder-Mead optimisation for both sweep directions. Returns (best_result, direction, score)."""
    try:
        from scipy.optimize import minimize
    except ImportError:
        print("ERROR: scipy not installed. Run: uv add scipy")
        sys.exit(1)

    best_result = None
    best_direction = 1
    best_score = float("inf")

    for direction in (1, -1):
        def objective(x, d=direction):
            s, _ = score_position(x[0], x[1], frame, period_s, d)
            return s

        result = minimize(
            fun=objective,
            x0=[initial_lat, initial_lon],
            method="Nelder-Mead",
            options={"maxiter": 2000, "xatol": 1e-5, "fatol": 1e-8, "adaptive": True},
        )
        if result.fun < best_score:
            best_score = result.fun
            best_result = result
            best_direction = direction

    return best_result, best_direction, best_score


# ── HTTP helpers ──────────────────────────────────────────────────────────────

def _get(path: str, host: str) -> dict:
    url = host.rstrip("/") + path
    try:
        with urllib.request.urlopen(url, timeout=10) as r:
            return json.loads(r.read())
    except Exception as e:
        print(f"ERROR fetching {url}: {e}")
        sys.exit(1)


# ── Main ──────────────────────────────────────────────────────────────────────

def print_frame_summary(frame: dict, period_s: float, prefix: str = "") -> None:
    print(f"{prefix}Frame {frame['frame_index']}: quality={frame['quality']} "
          f"n_aircraft={frame['n_aircraft']} "
          f"ref={frame['ref_icao']} ({frame['ref_lat']:.4f}, {frame['ref_lon']:.4f})")
    for obs in frame["observations"]:
        interp = " [interp]" if obs.get("interpolated") else ""
        phase = f"{obs['observed_phase_deg']:.1f}°" if obs.get("observed_phase_deg") is not None else "?"
        print(f"{prefix}  {obs['icao']} ({obs['lat']:.4f},{obs['lon']:.4f}) "
              f"phase={phase}{interp}")


def analyse_frame(frame: dict, period_s: float, receiver_lat: float, receiver_lon: float,
                  true_lat: Optional[float] = None, true_lon: Optional[float] = None) -> None:
    """Optimise position from one frame and print full diagnostics."""

    print(f"\n{'='*70}")
    print_frame_summary(frame, period_s)
    print()

    # Count usable observations (have positions)
    usable = [o for o in frame["observations"] if o["lat"] is not None and o["lon"] is not None]
    n_total = len(frame["observations"])
    n_usable = len(usable)
    if n_usable < 2:
        print(f"  SKIP: only {n_usable}/{n_total} observations have positions")
        return

    print(f"  Usable observations: {n_usable}/{n_total}")
    if n_usable < n_total:
        missing = [o["icao"] for o in frame["observations"] if o["lat"] is None]
        print(f"  Missing positions: {', '.join(missing)}")

    # Score at receiver location (rough sanity check)
    rec_score_cw, _ = score_position(receiver_lat, receiver_lon, frame, period_s, 1)
    rec_score_ccw, _ = score_position(receiver_lat, receiver_lon, frame, period_s, -1)
    print(f"\n  Score at receiver ({receiver_lat:.4f}, {receiver_lon:.4f}): "
          f"CW={rec_score_cw:.1f}  CCW={rec_score_ccw:.1f}")

    if true_lat is not None:
        true_score_cw, true_details_cw = score_position(true_lat, true_lon, frame, period_s, 1)
        true_score_ccw, true_details_ccw = score_position(true_lat, true_lon, frame, period_s, -1)
        best_true = min(true_score_cw, true_score_ccw)
        best_true_details = true_details_cw if true_score_cw <= true_score_ccw else true_details_ccw
        best_true_dir = "CW" if true_score_cw <= true_score_ccw else "CCW"
        print(f"  Score at true pos  ({true_lat:.4f}, {true_lon:.4f}): "
              f"CW={true_score_cw:.1f}  CCW={true_score_ccw:.1f}  "
              f"[best={best_true:.1f} {best_true_dir}]")
        print(f"\n  Residuals at true position ({best_true_dir}):")
        for d in best_true_details:
            interp = " [interp]" if d["interpolated"] else ""
            print(f"    {d['icao']}: bearing_ref={d['bearing_ref_deg']:.1f}° "
                  f"bearing_obs={d['bearing_from_radar_deg']:.1f}° "
                  f"obs_phase={d['observed_phase_deg']:.1f}° "
                  f"pred_phase={d['predicted_phase_deg']:.1f}° "
                  f"residual={d['residual_deg']:+.1f}°{interp}")

    # Optimise
    print(f"\n  Optimising from receiver position...")
    best_result, best_direction, best_score = optimise_position(
        frame, period_s, receiver_lat, receiver_lon
    )

    if best_result is None:
        print("  FAILED: optimisation did not converge")
        return

    opt_lat, opt_lon = float(best_result.x[0]), float(best_result.x[1])
    dir_label = "clockwise" if best_direction == 1 else "counterclockwise"
    dist_from_receiver = _haversine_m(receiver_lat, receiver_lon, opt_lat, opt_lon)

    print(f"  Optimised position: ({opt_lat:.5f}, {opt_lon:.5f})")
    print(f"  Direction: {dir_label}")
    print(f"  Final score: {best_score:.2f}")
    print(f"  Distance from receiver: {dist_from_receiver/1000:.1f} km")

    if true_lat is not None:
        dist_from_true = _haversine_m(true_lat, true_lon, opt_lat, opt_lon)
        print(f"  Distance from true pos: {dist_from_true/1000:.1f} km")

    # Residuals at optimised position
    _, opt_details = score_position(opt_lat, opt_lon, frame, period_s, best_direction)
    print(f"\n  Residuals at optimised position ({dir_label}):")
    for d in opt_details:
        interp = " [interp]" if d["interpolated"] else ""
        print(f"    {d['icao']}: bearing_ref={d['bearing_ref_deg']:.1f}° "
              f"bearing_obs={d['bearing_from_radar_deg']:.1f}° "
              f"obs_phase={d['observed_phase_deg']:.1f}° "
              f"pred_phase={d['predicted_phase_deg']:.1f}° "
              f"residual={d['residual_deg']:+.1f}°{interp}")

    rms = math.sqrt(best_score / len(opt_details)) if opt_details else 0
    print(f"\n  RMS residual: {rms:.2f}°")

    # Check: if residuals are all roughly equal magnitude but varied sign,
    # it's a systematic offset — likely a reference aircraft position problem.
    if opt_details:
        residuals = [d["residual_deg"] for d in opt_details]
        mean_r = sum(residuals) / len(residuals)
        all_same_sign = all(r > 0 for r in residuals) or all(r < 0 for r in residuals)
        if abs(mean_r) > 10:
            print(f"\n  WARNING: mean residual = {mean_r:+.1f}° — systematic offset.")
            print(f"  This may indicate a wrong reference aircraft position or period error.")
        if all_same_sign:
            print(f"\n  WARNING: all residuals have the same sign — strong systematic bias.")
        interp_count = sum(1 for d in opt_details if d["interpolated"])
        if interp_count > 0:
            print(f"\n  NOTE: {interp_count}/{len(opt_details)} observations use interpolated positions.")


def main():
    parser = argparse.ArgumentParser(description="Forward model single-frame diagnostic")
    parser.add_argument("--iid", type=int, required=True, help="IID to analyse")
    parser.add_argument("--frame", type=int, default=None, help="Specific frame index to use")
    parser.add_argument("--all-frames", action="store_true", help="Analyse all quality frames")
    parser.add_argument("--last-n", type=int, default=5,
                        help="Analyse the last N frames (default: 5)")
    parser.add_argument("--true-lat", type=float, default=None, help="Known true radar latitude")
    parser.add_argument("--true-lon", type=float, default=None, help="Known true radar longitude")
    parser.add_argument("--host", default=DEFAULT_HOST)
    args = parser.parse_args()

    # Fetch rotation model for period
    rotation = _get(f"/api/radar/iids/{args.iid}/rotation", args.host)
    period_s = rotation.get("period_s")
    if period_s is None:
        print(f"ERROR: IID {args.iid} has no period estimate yet")
        print(f"Rotation status: {rotation.get('status')}")
        sys.exit(1)

    print(f"IID {args.iid}: period={period_s:.4f}s  status={rotation.get('status')}")

    # Fetch receiver position
    iids_data = _get("/api/radar/iids", args.host)
    receiver_lat = iids_data.get("receiver_lat")
    receiver_lon = iids_data.get("receiver_lon")

    if receiver_lat is None:
        print("ERROR: receiver position not available (check RECEIVER_LAT/LON config)")
        sys.exit(1)

    print(f"Receiver: ({receiver_lat:.4f}, {receiver_lon:.4f})")
    if args.true_lat:
        d = _haversine_m(receiver_lat, receiver_lon, args.true_lat, args.true_lon)
        print(f"True radar: ({args.true_lat:.4f}, {args.true_lon:.4f}) — {d/1000:.1f} km from receiver")

    # Fetch sweep frames
    frames_data = _get(f"/api/radar/iids/{args.iid}/sweep-frames", args.host)
    all_frames = frames_data.get("frames", [])
    quality_frames = [f for f in all_frames if f["quality"] in ("good", "marginal")]

    print(f"\nFrames: {len(all_frames)} total, {len(quality_frames)} quality")

    if not quality_frames:
        print("No quality frames available yet.")
        sys.exit(0)

    if args.frame is not None:
        # Specific frame by index
        matches = [f for f in all_frames if f["frame_index"] == args.frame]
        if not matches:
            print(f"Frame {args.frame} not found. Available: {[f['frame_index'] for f in all_frames]}")
            sys.exit(1)
        frames_to_analyse = matches
    elif args.all_frames:
        frames_to_analyse = quality_frames
    else:
        frames_to_analyse = quality_frames[-args.last_n:]
        print(f"Analysing last {len(frames_to_analyse)} quality frames "
              f"(use --all-frames or --frame N for more)")

    for frame in frames_to_analyse:
        analyse_frame(frame, period_s, receiver_lat, receiver_lon,
                      args.true_lat, args.true_lon)

    print(f"\n{'='*70}")
    print("Done.")


if __name__ == "__main__":
    main()
