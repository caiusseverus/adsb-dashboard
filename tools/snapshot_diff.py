#!/usr/bin/env python3
"""
T2 — readsb vs Beast field mapping comparison.

Polls /api/stats from two running backend instances simultaneously,
matches aircraft by ICAO, and reports per-field divergences.

Typical setup:
    Primary instance   (beast mode)  : http://localhost:8000
    Secondary instance (readsb mode) : http://localhost:8001

Usage:
    python tools/snapshot_diff.py [--a URL] [--b URL] [--interval S] [--rounds N]

    # Run 12 polls (60 s) comparing localhost:8000 vs localhost:8001:
    python tools/snapshot_diff.py --rounds 12

    # Continuous until Ctrl-C:
    python tools/snapshot_diff.py

Exit code:
    0  all observed fields within tolerance
    1  unexpected divergences found
"""

import sys
import time
import argparse
import json
from collections import defaultdict
from urllib.request import urlopen
from urllib.error import URLError

# ── Field comparison config ───────────────────────────────────────────────────
#
# EXACT_FIELDS: must match precisely (None vs value is a mismatch)
# APPROX_FIELDS: numeric tolerance (abs delta)
# IGNORE_FIELDS: expected to differ between modes — skip silently
# READSB_ONLY_FIELDS: only present in readsb/hybrid; check they are non-null
#                     when a position is available
#
EXACT_FIELDS = (
    "icao", "callsign", "squawk", "military", "mlat",
)

APPROX_FIELDS = {
    "altitude":          25,     # ft — different filter chains, filter lag
    "signal":            30,     # 0-255 units — per-message vs rolling avg
    "lat":               0.002,  # degrees — CPR decode timing difference
    "lon":               0.002,
    "heading_deg":       2.0,    # degrees
    "vertical_rate_fpm": 128,    # fpm — BDS 6.0 quantisation
    "airspeed_kts":      5,      # kt — IAS vs TAS disambiguation possible
    "mach":              0.02,
    "selected_alt":      100,    # ft — MCP rounding
}

# Fields not compared (expected to differ structurally between modes)
IGNORE_FIELDS = {
    "age", "msg_count", "last_pos_age", "last_alt_age",
    "pos_global", "pos_reliable_odd", "pos_reliable_even", "pos_confident",
    "mlat_source", "mlat_msg_count", "mlat_quality", "mlat_sources",
    "acas_ra_active", "acas_ra_desc", "acas_ra_corrective",
    "acas_threat_icao", "acas_sensitivity",
    "range_nm", "bearing_deg",          # same source data, minor float diff
    "type_desc", "type_full_name",       # enrichment timing may differ
    "sighting_count",                    # DB-sourced, may differ
    # readsb-only (not in beast mode — checked separately)
    "alt_geom", "gs", "track", "track_rate", "roll", "true_heading",
    "geom_rate", "emergency", "nav_qnh", "nav_altitude_fms", "nav_heading",
    "nav_modes", "nic", "rc", "nac_p", "nac_v", "sil", "gva", "sda",
    "adsb_version", "wind_dir", "wind_speed", "oat", "tat",
}

# readsb-only fields that *should* be non-null when readsb has a position
READSB_ONLY_FIELDS = (
    "alt_geom", "gs", "track",           # usually present for ADS-B aircraft
    "nic", "nac_p", "nac_v", "sil",      # ADS-B quality — version 1/2 only
)


# ── HTTP fetch ────────────────────────────────────────────────────────────────

def fetch_snapshot(url: str, timeout: float = 5.0) -> dict | None:
    try:
        with urlopen(url, timeout=timeout) as resp:
            return json.loads(resp.read())
    except (URLError, json.JSONDecodeError) as exc:
        print(f"  [warn] fetch {url}: {exc}", file=sys.stderr)
        return None


# ── Per-aircraft comparison ───────────────────────────────────────────────────

def compare_aircraft(a_ac: dict, b_ac: dict) -> list[str]:
    """Return list of mismatch strings for one aircraft (empty = clean)."""
    issues = []

    for field in EXACT_FIELDS:
        av = a_ac.get(field)
        bv = b_ac.get(field)
        if av is None and bv is None:
            continue
        if av != bv:
            issues.append(f"  {field}: A={av!r}  B={bv!r}")

    for field, tol in APPROX_FIELDS.items():
        av = a_ac.get(field)
        bv = b_ac.get(field)
        if av is None and bv is None:
            continue
        if av is None or bv is None:
            # One side missing — flag only if the present side is non-trivial
            present = av if av is not None else bv
            if present is not None:
                issues.append(f"  {field}: A={av!r}  B={bv!r}  (one side None)")
        elif abs(av - bv) > tol:
            issues.append(f"  {field}: A={av}  B={bv}  (Δ{abs(av-bv):.1f} > {tol})")

    return issues


def check_readsb_only(b_ac: dict) -> list[str]:
    """Check that expected readsb-only fields are populated when pos_confident."""
    issues = []
    if not b_ac.get("pos_confident"):
        return issues   # no position — some fields legitimately absent
    for field in READSB_ONLY_FIELDS:
        if b_ac.get(field) is None:
            issues.append(f"  {field}: expected non-null in readsb mode (pos_confident=True)")
    return issues


# ── Main loop ─────────────────────────────────────────────────────────────────

def main():
    ap = argparse.ArgumentParser(
        description="Compare per-aircraft snapshots from two backend instances"
    )
    ap.add_argument("--a", default="http://localhost:8000",
                    help="URL of instance A (default: beast mode, port 8000)")
    ap.add_argument("--b", default="http://localhost:8001",
                    help="URL of instance B (default: readsb mode, port 8001)")
    ap.add_argument("--interval", type=float, default=5.0,
                    help="Seconds between polls (default: 5)")
    ap.add_argument("--rounds", type=int, default=0,
                    help="Stop after N rounds (0 = run until Ctrl-C)")
    ap.add_argument("--verbose", "-v", action="store_true",
                    help="Print OK aircraft as well as mismatches")
    ap.add_argument("--min-age", type=float, default=10.0,
                    help="Skip aircraft seen for less than N seconds (default: 10)")
    args = ap.parse_args()

    url_a = args.a.rstrip("/") + "/api/stats"
    url_b = args.b.rstrip("/") + "/api/stats"

    print(f"A (beast): {url_a}")
    print(f"B (readsb): {url_b}")
    print(f"Polling every {args.interval}s  |  min-age filter: {args.min_age}s")
    print("Press Ctrl-C to stop.\n")

    cumulative: dict = defaultdict(int)
    field_mismatch_counts: dict = defaultdict(int)
    round_num = 0

    try:
        while True:
            round_num += 1
            t0 = time.time()

            snap_a = fetch_snapshot(url_a)
            snap_b = fetch_snapshot(url_b)

            if snap_a is None or snap_b is None:
                print(f"[round {round_num:4d}] fetch failed — skipping")
            else:
                # Index by ICAO
                by_icao_a = {ac["icao"]: ac for ac in snap_a.get("aircraft", [])}
                by_icao_b = {ac["icao"]: ac for ac in snap_b.get("aircraft", [])}

                common = set(by_icao_a) & set(by_icao_b)
                only_a = set(by_icao_a) - set(by_icao_b)
                only_b = set(by_icao_b) - set(by_icao_a)

                cumulative["rounds"]     += 1
                cumulative["common"]     += len(common)
                cumulative["only_a"]     += len(only_a)
                cumulative["only_b"]     += len(only_b)

                round_issues = 0
                for icao in sorted(common):
                    a_ac = by_icao_a[icao]
                    b_ac = by_icao_b[icao]

                    # Skip freshly-seen aircraft — enrichment and filter warm-up
                    age_a = a_ac.get("age", 0)
                    age_b = b_ac.get("age", 0)
                    if min(age_a, age_b) < args.min_age:
                        continue

                    issues = compare_aircraft(a_ac, b_ac)
                    readsb_issues = check_readsb_only(b_ac)

                    if issues or readsb_issues:
                        round_issues += 1
                        cumulative["aircraft_with_issues"] += 1
                        for line in issues:
                            field = line.strip().split(":")[0]
                            field_mismatch_counts[field] += 1
                        print(f"  [{icao}] {a_ac.get('callsign','?'):8s}  "
                              f"alt={a_ac.get('altitude','?')}  age={age_a:.0f}s")
                        for line in issues + readsb_issues:
                            print(line)
                    elif args.verbose:
                        print(f"  [{icao}] OK  {a_ac.get('callsign','?'):8s}")

                print(
                    f"[round {round_num:4d}]  "
                    f"common={len(common)}  only_A={len(only_a)}  only_B={len(only_b)}  "
                    f"issues={round_issues}"
                )

            if args.rounds and round_num >= args.rounds:
                break

            elapsed = time.time() - t0
            sleep = max(0.0, args.interval - elapsed)
            time.sleep(sleep)

    except KeyboardInterrupt:
        print("\nStopped by user.")

    # ── Final summary ─────────────────────────────────────────────────────────
    print()
    print("=" * 70)
    print("SUMMARY")
    print("=" * 70)
    r = cumulative["rounds"]
    if r == 0:
        print("No rounds completed.")
        return 1

    print(f"  Rounds completed        : {r}")
    print(f"  Aircraft compared (tot) : {cumulative['common']}")
    print(f"  Aircraft with issues    : {cumulative['aircraft_with_issues']}")
    print(f"  Avg only-in-A per round : {cumulative['only_a']/r:.1f}")
    print(f"  Avg only-in-B per round : {cumulative['only_b']/r:.1f}")

    if field_mismatch_counts:
        print()
        print("  Field mismatch counts (descending):")
        for field, cnt in sorted(field_mismatch_counts.items(), key=lambda x: -x[1]):
            print(f"    {field:<25s} {cnt}")

    print()
    if cumulative["aircraft_with_issues"] == 0:
        print("PASS — no unexpected field divergences observed.")
        return 0
    else:
        print(f"FAIL — {cumulative['aircraft_with_issues']} aircraft had unexpected divergences.")
        return 1


if __name__ == "__main__":
    sys.exit(main())
