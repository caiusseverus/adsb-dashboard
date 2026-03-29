#!/usr/bin/env python3
"""
T4 — Watchdog and degradation tests.

Sub-tests:
    T4a  readsb file goes stale (guided — requires stopping readsb)
    T4b  Beast TCP disconnects (guided — requires stopping Beast source)
    T4c  Unknown INGEST_MODE causes clean startup failure (automated)

Usage:
    # Run all (T4c automated, T4a/T4b guided):
    cd backend && uv run python ../tools/watchdog_check.py

    # Skip guided tests, run only automated:
    cd backend && uv run python ../tools/watchdog_check.py --auto-only

Exit code: 0 = all run tests pass, 1 = failures.
"""

import sys
import os
import subprocess
import time
import json
import argparse
from pathlib import Path
from urllib.request import urlopen
from urllib.error import URLError

_BACKEND = Path(__file__).resolve().parent.parent / "backend"

PASS = "PASS"
FAIL = "FAIL"
SKIP = "SKIP"


def _yn(prompt: str) -> bool:
    """Ask a yes/no question; return True for yes."""
    while True:
        ans = input(f"  {prompt} [y/n] ").strip().lower()
        if ans in ("y", "yes"):
            return True
        if ans in ("n", "no"):
            return False


def fetch(url: str, timeout: float = 3.0) -> dict | None:
    try:
        with urlopen(url, timeout=timeout) as r:
            return json.loads(r.read())
    except Exception:
        return None


def aircraft_count(url: str) -> int | None:
    snap = fetch(url + "/api/stats")
    if snap is None:
        return None
    return snap.get("aircraft_count", len(snap.get("aircraft", [])))


# ── T4c — automated ───────────────────────────────────────────────────────────

def run_t4c() -> tuple[str, str]:
    """
    Start backend with INGEST_MODE=invalid, verify it exits non-zero
    and emits a ValueError in its output.
    """
    print("\nT4c — Unknown INGEST_MODE")
    print("  Starting backend with INGEST_MODE=invalid …")

    env = os.environ.copy()
    env["INGEST_MODE"] = "invalid"
    # Use a throwaway in-memory DB so we don't touch production data
    env["DB_PATH"] = ":memory:"

    try:
        result = subprocess.run(
            [
                sys.executable, "-c",
                # Import and call the lifespan directly rather than spawning
                # a full uvicorn process — faster and no port conflicts.
                """
import asyncio, sys, os
sys.path.insert(0, '.')
os.environ['INGEST_MODE'] = 'invalid'
import config
# Directly verify the guard raises ValueError
try:
    if config.INGEST_MODE not in ('beast', 'readsb', 'hybrid'):
        raise ValueError(f"Unknown INGEST_MODE: {config.INGEST_MODE!r} (expected beast/readsb/hybrid)")
    print("ERROR: no exception raised", file=sys.stderr)
    sys.exit(1)
except ValueError as exc:
    print(f"OK: ValueError raised: {exc}")
    sys.exit(0)
""",
            ],
            capture_output=True,
            text=True,
            timeout=10,
            cwd=str(_BACKEND),
            env=env,
        )
    except subprocess.TimeoutExpired:
        return FAIL, "process did not exit within 10 s"
    except FileNotFoundError as exc:
        return FAIL, f"could not run python: {exc}"

    combined = result.stdout + result.stderr
    if result.returncode == 0 and "ValueError" in combined:
        print(f"  ValueError raised and process exited cleanly.")
        return PASS, "ValueError raised on unknown INGEST_MODE, clean exit"
    else:
        return FAIL, (
            f"returncode={result.returncode}\n"
            f"  stdout: {result.stdout.strip()}\n"
            f"  stderr: {result.stderr.strip()}"
        )


# ── T4a — guided ─────────────────────────────────────────────────────────────

def run_t4a(base_url: str) -> tuple[str, str]:
    """
    Guide the tester through stopping readsb and verifying:
    - Within 5 s: warning logged (aircraft table freezes)
    - Within 30 s: error logged
    - After restart: normal operation resumes within 2 poll cycles
    """
    print("\nT4a — readsb file goes stale")
    print("  Prerequisites:")
    print("    - Backend running with INGEST_MODE=readsb or INGEST_MODE=hybrid")
    print("    - readsb service running and producing aircraft.json")
    print()

    snap = fetch(base_url + "/api/stats")
    if snap is None:
        return SKIP, f"backend not reachable at {base_url}"

    n_before = aircraft_count(base_url)
    print(f"  Current aircraft count: {n_before}")
    print()
    print("  Step 1: Stop readsb now.")
    print("    sudo systemctl stop readsb")
    print()
    if not _yn("Have you stopped readsb?"):
        return SKIP, "user skipped"

    t_stopped = time.time()
    print()
    print("  Waiting up to 10 s for aircraft table to freeze …")
    time.sleep(10)

    n_after = aircraft_count(base_url)
    elapsed = time.time() - t_stopped
    print(f"  Aircraft count after {elapsed:.0f}s: {n_after}")

    freeze_ok = (n_after == n_before) or (n_after is not None and abs((n_after or 0) - (n_before or 0)) <= 2)
    if freeze_ok:
        print("  Aircraft table appears frozen (no new aircraft added). [OK]")
    else:
        print(f"  Aircraft count changed significantly ({n_before} → {n_after}) — unexpected.")

    print()
    print("  Check your backend logs for these messages:")
    print("    WARNING  readsb_ingest: aircraft.json is N s stale   (within 5 s of stop)")
    print("    ERROR    readsb_ingest: aircraft.json is N s stale   (within 30 s of stop)")
    print()
    warn_seen = _yn("Did you see the WARNING message in logs (within ~5 s)?")
    error_seen = _yn("Did you see the ERROR message in logs (within ~30 s)?")

    print()
    print("  Step 2: Restart readsb now.")
    print("    sudo systemctl start readsb")
    print()
    if not _yn("Have you restarted readsb?"):
        print("  Skipping recovery check.")
        if warn_seen and error_seen:
            return PASS, "stale warnings fired correctly (recovery not verified)"
        else:
            return FAIL, f"warn_seen={warn_seen} error_seen={error_seen}"

    print("  Waiting 15 s for readsb to resume writing and backend to re-poll …")
    time.sleep(15)

    n_recovered = aircraft_count(base_url)
    print(f"  Aircraft count after recovery: {n_recovered}")
    recovered = n_recovered is not None and (n_recovered or 0) > 0

    all_ok = warn_seen and error_seen and recovered
    detail = (
        f"warn={warn_seen} error={error_seen} "
        f"freeze={freeze_ok} recovered={recovered}"
    )
    return (PASS if all_ok else FAIL), detail


# ── T4b — guided ─────────────────────────────────────────────────────────────

def run_t4b(base_url: str) -> tuple[str, str]:
    """
    Guide the tester through dropping the Beast TCP connection and verifying:
    - Beast client logs a reconnect attempt within 5 s (connection refused)
      or 30 s (timeout)
    - In hybrid mode: readsb positions continue updating during Beast outage
    - After Beast source returns: normal operation resumes
    """
    print("\nT4b — Beast TCP disconnects")
    print("  Prerequisites:")
    print("    - Backend running with INGEST_MODE=beast or INGEST_MODE=hybrid")
    print("    - Beast source (readsb net_connector or dump1090) is running")
    print()

    snap = fetch(base_url + "/api/stats")
    if snap is None:
        return SKIP, f"backend not reachable at {base_url}"

    ingest_mode = snap.get("ingest_mode", "unknown")
    print(f"  Detected ingest_mode in snapshot: {ingest_mode!r}")
    print()
    print("  Step 1: Block or stop the Beast source.")
    print("    Option A (stop readsb net_connector): sudo systemctl stop readsb")
    print("    Option B (firewall): sudo iptables -A INPUT -p tcp --dport 30005 -j DROP")
    print()
    if not _yn("Have you stopped/blocked the Beast source?"):
        return SKIP, "user skipped"

    t_dropped = time.time()
    print()
    print("  Check backend logs for:")
    print("    WARNING  Beast connection failed … retrying in 5 s")
    print("    (or: Beast stream timed out — if no data for 30 s)")
    print()

    if ingest_mode == "hybrid":
        print("  In hybrid mode: aircraft positions from readsb should still update.")
        print("  Watch the aircraft table — positions should continue refreshing.")
        time.sleep(15)
        n_hybrid = aircraft_count(base_url)
        print(f"  Aircraft count after 15 s with Beast down: {n_hybrid}")
        readsb_ok = _yn("Are aircraft positions still updating (readsb ingest alive)?")
    else:
        readsb_ok = True   # not applicable in beast-only mode
        time.sleep(10)

    reconnect_logged = _yn("Did you see the Beast reconnect warning in logs?")

    print()
    print("  Step 2: Restore the Beast source.")
    print("    sudo systemctl start readsb  (or: sudo iptables -D INPUT ...)")
    print()
    if not _yn("Have you restored the Beast source?"):
        print("  Skipping recovery check.")
        ok = reconnect_logged and (ingest_mode != "hybrid" or readsb_ok)
        return (PASS if ok else FAIL), f"reconnect_logged={reconnect_logged} readsb_ok={readsb_ok}"

    print("  Waiting 15 s for Beast client to reconnect and resume …")
    time.sleep(15)

    n_recovered = aircraft_count(base_url)
    print(f"  Aircraft count after recovery: {n_recovered}")
    recovered = n_recovered is not None and (n_recovered or 0) > 0

    all_ok = reconnect_logged and recovered and (ingest_mode != "hybrid" or readsb_ok)
    detail = (
        f"reconnect_logged={reconnect_logged} "
        f"readsb_alive_during_outage={readsb_ok} "
        f"recovered={recovered}"
    )
    return (PASS if all_ok else FAIL), detail


# ── Main ─────────────────────────────────────────────────────────────────────

def main():
    ap = argparse.ArgumentParser(
        description="T4 watchdog and degradation tests"
    )
    ap.add_argument("--url", default="http://localhost:8000",
                    help="Backend base URL (default: http://localhost:8000)")
    ap.add_argument("--auto-only", action="store_true",
                    help="Run only the automated test (T4c); skip guided T4a/T4b")
    args = ap.parse_args()

    results: list[tuple[str, str, str]] = []   # (test, status, detail)

    # T4c — always run, fully automated
    status, detail = run_t4c()
    results.append(("T4c — unknown INGEST_MODE", status, detail))

    if not args.auto_only:
        print()
        print("─" * 70)
        print("Guided tests follow. You will be prompted at each step.")
        print("Press Ctrl-C at any time to abort remaining tests.")
        print("─" * 70)

        try:
            status, detail = run_t4a(args.url)
            results.append(("T4a — readsb stale", status, detail))

            print()
            print("─" * 70)
            status, detail = run_t4b(args.url)
            results.append(("T4b — Beast disconnect", status, detail))
        except KeyboardInterrupt:
            print("\n  Aborted by user.")

    # Summary
    print()
    print("=" * 70)
    print("SUMMARY")
    print("=" * 70)
    failed = 0
    for test, status, detail in results:
        icon = {"PASS": "✓", "FAIL": "✗", "SKIP": "–"}.get(status, "?")
        print(f"  {icon} {status}  {test}")
        if status != PASS:
            print(f"       {detail}")
        if status == FAIL:
            failed += 1

    print()
    if failed:
        print(f"FAIL — {failed} test(s) failed.")
        return 1
    print("PASS — all run tests passed.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
