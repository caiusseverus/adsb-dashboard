#!/usr/bin/env python3
"""
T5 — Signal conversion consistency check.

Compares the `signal` field (Beast RSSI scale 0–255) across two running
backend instances — typically one in beast mode and one in readsb mode.
Also validates that the dBFS → 0–255 conversion formula produces values
in a sensible range and that no aircraft are clamped to the extremes.

readsb gives signal as dBFS (e.g. -8.5) — always negative, more negative
= weaker.  Conversion applied in update_from_json():
    beast_equiv = clamp(int(-rssi_dbfs * 2), 0, 255)

Expected: values within ±30 units of the Beast reading for the same aircraft.
The systematic offset is expected because Beast gives per-message peak RSSI
while readsb gives a recent rolling average.

Usage:
    # Run against two local instances (beast=8000, readsb=8001):
    python tools/signal_check.py

    # Specify URLs explicitly:
    python tools/signal_check.py --beast http://localhost:8000 \\
                                  --readsb http://localhost:8001

    # Single-instance check (readsb mode only — validates range/clamping):
    python tools/signal_check.py --single http://localhost:8000

Exit code: 0 = pass, 1 = failures found.
"""

import sys
import time
import json
import math
import argparse
import statistics
from urllib.request import urlopen
from urllib.error import URLError

_TOLERANCE     = 30    # max acceptable |beast - readsb_equiv| per aircraft
_CLAMP_WARN    = 5     # flag if this many aircraft are clamped at 0 or 255
_MIN_COMMON    = 3     # need at least this many common aircraft for comparison


def fetch(url: str, timeout: float = 5.0) -> dict | None:
    try:
        with urlopen(url, timeout=timeout) as r:
            return json.loads(r.read())
    except (URLError, json.JSONDecodeError) as exc:
        print(f"  [warn] {url}: {exc}", file=sys.stderr)
        return None


def by_icao(snap: dict) -> dict[str, dict]:
    return {ac["icao"]: ac for ac in snap.get("aircraft", [])}


# ── Single-instance checks ────────────────────────────────────────────────────

def check_single(snap: dict, label: str) -> tuple[bool, list[str]]:
    """
    Validate signal values in one snapshot:
    - All values in 0–255
    - No systematic clamping at extremes
    - Distribution looks reasonable (not all-zero, not all-255)
    """
    issues: list[str] = []
    aircraft = snap.get("aircraft", [])

    signals = [ac["signal"] for ac in aircraft if ac.get("signal") is not None]
    if not signals:
        issues.append(f"{label}: no aircraft with signal values")
        return False, issues

    out_of_range = [s for s in signals if s < 0 or s > 255]
    if out_of_range:
        issues.append(
            f"{label}: {len(out_of_range)} signal values outside 0–255: "
            + str(out_of_range[:5])
        )

    clamped_0   = signals.count(0)
    clamped_255 = signals.count(255)
    if clamped_0 > _CLAMP_WARN:
        issues.append(
            f"{label}: {clamped_0}/{len(signals)} aircraft have signal=0 "
            "(strongest possible — possible clamping)"
        )
    if clamped_255 > _CLAMP_WARN:
        issues.append(
            f"{label}: {clamped_255}/{len(signals)} aircraft have signal=255 "
            "(weakest possible — possible clamping or no-signal placeholder)"
        )

    mean_sig = statistics.mean(signals)
    # A typical receiver sees signals in the 30–180 range (0=best, 255=worst).
    # Outside 5–230 suggests something is wrong with the conversion.
    if mean_sig < 5:
        issues.append(
            f"{label}: mean signal={mean_sig:.1f} — suspiciously strong "
            "(are all readings being clamped to 0?)"
        )
    if mean_sig > 230:
        issues.append(
            f"{label}: mean signal={mean_sig:.1f} — suspiciously weak "
            "(possible conversion error or no-signal placeholder)"
        )

    print(f"  {label}: n={len(signals)}  "
          f"min={min(signals)}  max={max(signals)}  "
          f"mean={mean_sig:.1f}  median={statistics.median(signals):.0f}  "
          f"clamped_0={clamped_0}  clamped_255={clamped_255}")

    return len(issues) == 0, issues


# ── Two-instance comparison ───────────────────────────────────────────────────

def compare_signal(snap_b: dict, snap_r: dict,
                   min_age: float = 10.0) -> tuple[bool, list[str], dict]:
    """
    Compare signal values for aircraft present in both snapshots.
    Returns (pass, issues, stats_dict).
    """
    issues: list[str] = []
    by_b = by_icao(snap_b)
    by_r = by_icao(snap_r)
    common = set(by_b) & set(by_r)

    # Filter by age — freshly-seen aircraft may not have stable signal readings
    stable = [
        icao for icao in common
        if (by_b[icao].get("age", 0) >= min_age
            and by_r[icao].get("age", 0) >= min_age)
    ]

    if len(stable) < _MIN_COMMON:
        issues.append(
            f"Only {len(stable)} aircraft stable in both instances "
            f"(need {_MIN_COMMON}) — wait for more traffic or lower --min-age"
        )
        return False, issues, {}

    deltas: list[float] = []
    offenders: list[tuple[str, int, int, int]] = []   # (icao, beast, readsb, delta)

    print(f"\n  {'ICAO':>8}  {'Beast':>6}  {'readsb':>6}  {'delta':>6}  {'age_b':>6}  status")
    print(f"  {'─'*8}  {'─'*6}  {'─'*6}  {'─'*6}  {'─'*6}  ──────")

    for icao in sorted(stable):
        b_sig = by_b[icao].get("signal")
        r_sig = by_r[icao].get("signal")
        age_b = by_b[icao].get("age", 0)

        if b_sig is None or r_sig is None:
            continue

        delta = abs(b_sig - r_sig)
        deltas.append(delta)

        ok = delta <= _TOLERANCE
        status = "OK" if ok else f"MISMATCH (Δ{delta})"
        if not ok:
            offenders.append((icao, b_sig, r_sig, delta))

        print(f"  {icao:>8}  {b_sig:>6}  {r_sig:>6}  {delta:>+6.0f}  "
              f"{age_b:>6.0f}  {status}")

    print()

    stats: dict = {}
    if deltas:
        stats = {
            "n":          len(deltas),
            "mean_delta": round(statistics.mean(deltas), 1),
            "max_delta":  max(deltas),
            "p90_delta":  sorted(deltas)[int(len(deltas) * 0.9)],
        }
        print(f"  Delta stats: n={stats['n']}  "
              f"mean={stats['mean_delta']}  "
              f"max={stats['max_delta']}  "
              f"p90={stats['p90_delta']}")

        # Systematic offset is expected (per-message peak vs rolling avg).
        # Fail only if the *mean* exceeds tolerance, not individual outliers
        # (one parked aircraft with steady signal can appear very different).
        mean_over = stats["mean_delta"] > _TOLERANCE
        if mean_over:
            issues.append(
                f"mean signal delta {stats['mean_delta']} exceeds tolerance {_TOLERANCE} "
                "— systematic conversion error likely"
            )

        if offenders:
            pct = len(offenders) / len(deltas) * 100
            note = (
                f"{len(offenders)}/{len(deltas)} aircraft ({pct:.0f}%) "
                f"exceed ±{_TOLERANCE} unit tolerance"
            )
            if pct > 30:
                issues.append(note)
            else:
                print(f"  NOTE: {note} (≤30% is acceptable — per-message vs average)")

    return len(issues) == 0, issues, stats


# ── Conversion formula verification ──────────────────────────────────────────

def verify_formula() -> tuple[bool, list[str]]:
    """Spot-check the dBFS → 0-255 conversion formula against known values."""
    issues: list[str] = []
    cases = [
        # (rssi_dbfs, expected_beast, description)
        (-3.0,   6,   "very strong (-3 dBFS)"),
        (-10.0,  20,  "strong (-10 dBFS)"),
        (-20.0,  40,  "good (-20 dBFS)"),
        (-40.0,  80,  "moderate (-40 dBFS)"),
        (-80.0,  160, "weak (-80 dBFS)"),
        (-120.0, 240, "very weak (-120 dBFS)"),
        (-0.5,   1,   "near-clamp strong"),
        (-200.0, 255, "clamp at weak end"),
    ]

    print("  Formula verification: clamp(int(-rssi_dbfs * 2), 0, 255)")
    print(f"  {'dBFS':>8}  {'expected':>8}  {'got':>5}  status")
    all_ok = True
    for rssi_dbfs, expected, desc in cases:
        got = max(0, min(255, int(-rssi_dbfs * 2)))
        ok  = got == expected
        if not ok:
            issues.append(f"formula: rssi={rssi_dbfs} → {got}, expected {expected} ({desc})")
            all_ok = False
        print(f"  {rssi_dbfs:>8.1f}  {expected:>8}  {got:>5}  {'OK' if ok else 'FAIL'}")

    return all_ok, issues


# ── Main ─────────────────────────────────────────────────────────────────────

def main():
    ap = argparse.ArgumentParser(
        description="T5 signal conversion consistency check"
    )
    ap.add_argument("--beast",  default="http://localhost:8000",
                    help="Beast-mode instance URL (default: port 8000)")
    ap.add_argument("--readsb", default="http://localhost:8001",
                    help="readsb-mode instance URL (default: port 8001)")
    ap.add_argument("--single", metavar="URL",
                    help="Single-instance mode: only check range/clamping on one URL")
    ap.add_argument("--min-age", type=float, default=10.0,
                    help="Skip aircraft younger than N seconds (default: 10)")
    ap.add_argument("--rounds", type=int, default=3,
                    help="Poll rounds for two-instance comparison (default: 3)")
    ap.add_argument("--interval", type=float, default=10.0,
                    help="Seconds between rounds (default: 10)")
    args = ap.parse_args()

    all_issues: list[str] = []

    # Always verify the conversion formula (no live system needed)
    print("── Conversion formula check ────────────────────────────────────────")
    ok, issues = verify_formula()
    all_issues.extend(issues)
    print(f"  Result: {'PASS' if ok else 'FAIL'}")

    if args.single:
        # Single-instance mode
        print(f"\n── Single-instance signal range check ({args.single}) ──────────────")
        snap = fetch(args.single + "/api/stats")
        if snap is None:
            all_issues.append(f"Cannot reach {args.single}")
        else:
            ok, issues = check_single(snap, "instance")
            all_issues.extend(issues)
            print(f"  Result: {'PASS' if ok else 'FAIL'}")
    else:
        # Two-instance comparison
        print(f"\n── Single-instance range checks ────────────────────────────────────")
        snap_b = fetch(args.beast  + "/api/stats")
        snap_r = fetch(args.readsb + "/api/stats")

        if snap_b is None:
            all_issues.append(f"Cannot reach beast instance: {args.beast}")
        else:
            ok, issues = check_single(snap_b, "beast ")
            all_issues.extend(issues)

        if snap_r is None:
            all_issues.append(f"Cannot reach readsb instance: {args.readsb}")
        else:
            ok, issues = check_single(snap_r, "readsb")
            all_issues.extend(issues)

        if snap_b and snap_r:
            print(f"\n── Two-instance signal comparison ({args.rounds} rounds) ───────────────")
            print(f"  beast  → {args.beast}")
            print(f"  readsb → {args.readsb}")
            print(f"  Tolerance: ±{_TOLERANCE} units  |  min-age: {args.min_age}s")

            all_deltas: list[float] = []
            for i in range(1, args.rounds + 1):
                print(f"\n  [round {i}/{args.rounds}]")
                snap_b = fetch(args.beast  + "/api/stats")
                snap_r = fetch(args.readsb + "/api/stats")
                if snap_b and snap_r:
                    ok, issues, stats = compare_signal(snap_b, snap_r, args.min_age)
                    all_issues.extend(issues)
                    if "mean_delta" in stats:
                        all_deltas.append(stats["mean_delta"])
                if i < args.rounds:
                    time.sleep(args.interval)

            if all_deltas:
                overall_mean = statistics.mean(all_deltas)
                print(f"\n  Overall mean delta across rounds: {overall_mean:.1f} units")
                if overall_mean <= _TOLERANCE:
                    print(f"  Within ±{_TOLERANCE} tolerance — conversion consistent.")
                else:
                    all_issues.append(
                        f"Overall mean delta {overall_mean:.1f} exceeds tolerance {_TOLERANCE}"
                    )

    # Summary
    print()
    print("=" * 70)
    print("SUMMARY")
    print("=" * 70)
    if all_issues:
        print(f"FAIL — {len(all_issues)} issue(s):")
        for iss in all_issues:
            print(f"  {iss}")
        return 1
    print("PASS — signal conversion is consistent and within tolerance.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
