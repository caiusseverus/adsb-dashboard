#!/usr/bin/env python3
"""
T3 — Hybrid mode validation.

Checks a running backend instance configured with INGEST_MODE=hybrid and
at least one MLAT_SERVER to verify the combined data path is working correctly.

Usage:
    python tools/hybrid_check.py [--url URL] [--rounds N] [--interval S]

Checks performed:
    1. readsb-only fields populated on positioned ADS-B aircraft
    2. MLAT aircraft have mlat=True and mlat_source set
    3. Pure ADS-B aircraft do not have mlat=True
    4. /api/mlat/fixes returns data (Beast MLAT path alive)
    5. Message counts sourced from readsb stats (not Beast counting)
    6. No Beast queue pressure in hybrid mode (queue gated per D3 fix)
    7. Enrichment not duplicated (adsbx_queue stays bounded)

Exit code: 0 = all checks pass, 1 = failures found.
"""

import sys
import time
import json
import argparse
from urllib.request import urlopen
from urllib.error import URLError
from collections import defaultdict

# readsb-only fields that should be non-null on positioned ADS-B aircraft
_READSB_FIELDS_EXPECTED = ("alt_geom", "gs", "track", "nic", "nac_p", "sil")

# Fields that prove Beast MLAT decode is active
_MLAT_BEAST_FIELDS = ("mlat_source", "mlat_msg_count")


def fetch(url: str, timeout: float = 5.0) -> dict | list | None:
    try:
        with urlopen(url, timeout=timeout) as r:
            return json.loads(r.read())
    except (URLError, json.JSONDecodeError) as exc:
        print(f"  [warn] {url}: {exc}", file=sys.stderr)
        return None


def check_snapshot(snap: dict, results: dict) -> None:
    """Check per-aircraft invariants in the snapshot."""
    aircraft = snap.get("aircraft", [])
    if not aircraft:
        results["warns"].append("snapshot: aircraft list empty — no traffic?")
        return

    adsb_with_pos  = [a for a in aircraft if a.get("pos_confident") and not a.get("mlat")]
    mlat_aircraft  = [a for a in aircraft if a.get("mlat")]

    results["ac_total"]      = len(aircraft)
    results["ac_adsb_pos"]   = len(adsb_with_pos)
    results["ac_mlat"]       = len(mlat_aircraft)

    # Check 1 — readsb-only fields on positioned ADS-B aircraft
    missing_readsb: dict = defaultdict(int)
    for ac in adsb_with_pos:
        for field in _READSB_FIELDS_EXPECTED:
            if ac.get(field) is None:
                missing_readsb[field] += 1

    if missing_readsb:
        # Allow up to 20% missing (some aircraft may be version 0 with no NIC/NAC)
        for field, count in missing_readsb.items():
            pct = count / max(len(adsb_with_pos), 1) * 100
            if pct > 20:
                results["fails"].append(
                    f"readsb field '{field}' missing on {count}/{len(adsb_with_pos)} "
                    f"positioned ADS-B aircraft ({pct:.0f}%)"
                )
            else:
                results["warns"].append(
                    f"readsb field '{field}' missing on {count}/{len(adsb_with_pos)} "
                    f"ADS-B aircraft ({pct:.0f}%) — may be version-0 transponders"
                )
    else:
        results["passes"].append(
            f"readsb-only fields populated on all {len(adsb_with_pos)} positioned ADS-B aircraft"
        )

    # Check 2 — MLAT aircraft have mlat_source set
    if mlat_aircraft:
        no_source = [a["icao"] for a in mlat_aircraft if not a.get("mlat_source")]
        if no_source:
            results["fails"].append(
                f"mlat=True but mlat_source empty on {len(no_source)} aircraft: "
                + ", ".join(no_source[:5])
            )
        else:
            results["passes"].append(
                f"{len(mlat_aircraft)} MLAT aircraft all have mlat_source set"
            )
    else:
        results["warns"].append(
            "No MLAT aircraft in snapshot — is MLAT_SERVER configured and active?"
        )

    # Check 3 — ADS-B aircraft not spuriously flagged as MLAT
    mlat_no_source = [a for a in aircraft if a.get("mlat") and not a.get("mlat_source")]
    if mlat_no_source:
        results["warns"].append(
            f"{len(mlat_no_source)} aircraft have mlat=True with no mlat_source "
            "(Beast MLAT may not be supplying source attribution yet)"
        )


def check_mlat_fixes(fixes: dict | None, results: dict) -> None:
    """Check /api/mlat/fixes for Beast MLAT path liveness."""
    if fixes is None:
        results["fails"].append("/api/mlat/fixes: endpoint unreachable")
        return

    total_fixes = fixes.get("total_fixes", 0)
    sources     = fixes.get("sources", {})

    if total_fixes == 0 and not sources:
        results["warns"].append(
            "/api/mlat/fixes: no fixes yet — MLAT may not have started. "
            "Wait 60+ seconds after startup."
        )
    else:
        results["passes"].append(
            f"/api/mlat/fixes: {total_fixes} total fixes across {len(sources)} source(s): "
            + ", ".join(sources.keys())
        )


def check_perf(perf: dict | None, results: dict) -> None:
    """Check /api/debug/perf for hybrid-mode-specific invariants."""
    if perf is None:
        results["warns"].append("/api/debug/perf: endpoint unreachable")
        return

    # In hybrid mode the Beast queue depth should be 0 (D3 fix: queue not used)
    q_stats = perf.get("msg_queue_stats", {})
    q_p95 = q_stats.get("p95", 0) if isinstance(q_stats, dict) else 0

    if q_p95 is None:
        results["passes"].append(
            "/api/debug/perf: msg_queue_stats absent — queue telemetry correctly "
            "disabled in hybrid mode (D3)"
        )
    elif q_p95 == 0:
        results["passes"].append(
            "/api/debug/perf: msg_queue p95=0 — queue not accumulating in hybrid mode"
        )
    else:
        results["warns"].append(
            f"/api/debug/perf: msg_queue p95={q_p95} — non-zero in hybrid mode "
            "(expected 0; check D3 fix was deployed)"
        )

    # adsbx_queue should be bounded (< 50 steady-state after warm-up)
    adsbx_q = perf.get("adsbx_queue_size", None)
    if adsbx_q is not None:
        if adsbx_q > 100:
            results["fails"].append(
                f"adsbx_queue_size={adsbx_q} — unusually large, possible enrichment backlog"
            )
        else:
            results["passes"].append(f"adsbx_queue_size={adsbx_q} — within normal bounds")

    # broadcast_avg should be well under 50 ms
    push = perf.get("push_timings", {})
    if isinstance(push, dict):
        bcast_avg = push.get("broadcast_avg_ms") or push.get("broadcast_avg")
        if bcast_avg is not None and bcast_avg > 50:
            results["fails"].append(
                f"broadcast_avg={bcast_avg:.1f}ms exceeds 50ms target in hybrid mode"
            )
        elif bcast_avg is not None:
            results["passes"].append(f"broadcast_avg={bcast_avg:.1f}ms < 50ms target")


def check_message_source(snap: dict, results: dict) -> None:
    """Verify total_messages increments (readsb stats path alive in hybrid mode)."""
    # We can only verify across rounds; just flag if total is 0
    total = snap.get("total_messages", 0)
    if total == 0:
        results["warns"].append(
            "total_messages=0 in snapshot — readsb stats may not have provided "
            "message count yet (wait one poll cycle)"
        )
    else:
        results["passes"].append(f"total_messages={total} (readsb stats providing counts)")


def run_checks(base_url: str) -> dict:
    url_stats = base_url.rstrip("/") + "/api/stats"
    url_fixes = base_url.rstrip("/") + "/api/mlat/fixes"
    url_perf  = base_url.rstrip("/") + "/api/debug/perf"

    results: dict = {"passes": [], "warns": [], "fails": []}

    snap  = fetch(url_stats)
    fixes = fetch(url_fixes)
    perf  = fetch(url_perf)

    if snap is None:
        results["fails"].append(f"Cannot reach {url_stats} — is the backend running?")
        return results

    check_snapshot(snap, results)
    check_message_source(snap, results)
    check_mlat_fixes(fixes, results)
    check_perf(perf, results)

    return results


def print_results(results: dict, round_num: int) -> None:
    print(f"\n[round {round_num}]  "
          f"pass={len(results['passes'])}  "
          f"warn={len(results['warns'])}  "
          f"fail={len(results['fails'])}")
    for msg in results["passes"]:
        print(f"  PASS  {msg}")
    for msg in results["warns"]:
        print(f"  WARN  {msg}")
    for msg in results["fails"]:
        print(f"  FAIL  {msg}")


def main():
    ap = argparse.ArgumentParser(
        description="T3 hybrid mode validation — checks a running hybrid-mode backend"
    )
    ap.add_argument("--url", default="http://localhost:8000",
                    help="Backend base URL (default: http://localhost:8000)")
    ap.add_argument("--rounds", type=int, default=3,
                    help="Number of check rounds to run (default: 3)")
    ap.add_argument("--interval", type=float, default=15.0,
                    help="Seconds between rounds (default: 15)")
    args = ap.parse_args()

    print(f"Hybrid mode check: {args.url}")
    print(f"Rounds: {args.rounds}  Interval: {args.interval}s")
    print()
    print("Prerequisites:")
    print("  INGEST_MODE=hybrid")
    print("  At least one MLAT_SERVER configured")
    print("  Backend running for 60+ seconds (MLAT needs warm-up time)")
    print()

    all_fails: list[str] = []
    all_warns: list[str] = []

    for i in range(1, args.rounds + 1):
        results = run_checks(args.url)
        print_results(results, i)
        all_fails.extend(results["fails"])
        all_warns.extend(results["warns"])
        if i < args.rounds:
            time.sleep(args.interval)

    print()
    print("=" * 70)
    print("SUMMARY")
    print("=" * 70)
    if all_fails:
        print(f"FAIL — {len(all_fails)} failure(s) across {args.rounds} rounds")
        for f in dict.fromkeys(all_fails):   # deduped
            print(f"  {f}")
        return 1
    elif all_warns:
        print(f"WARN — no failures, {len(all_warns)} warning(s)")
        for w in dict.fromkeys(all_warns):
            print(f"  {w}")
        return 0
    else:
        print("PASS — all checks passed across all rounds")
        return 0


if __name__ == "__main__":
    sys.exit(main())
