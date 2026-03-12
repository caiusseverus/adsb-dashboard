#!/usr/bin/env python3
"""
run_benchmark.py — standalone pipeline benchmark for adsb-dashboard.

Run directly from the backend/ directory (no server required):

    cd backend
    uv run python run_benchmark.py            # full run, ~5000 iterations
    uv run python run_benchmark.py --quick    # 500 iterations, faster
    uv run python run_benchmark.py --save results_baseline.json
    uv run python run_benchmark.py --save results_after_cython.json
    uv run python run_benchmark.py --compare results_baseline.json results_after_cython.json

The decoder is NOT running when this script executes (there is no live server),
so there is no GIL contention and no need for the pause mechanism.
"""

import argparse
import json
import os
import sys
import time

HERE = os.path.dirname(os.path.abspath(__file__))
if HERE not in sys.path:
    sys.path.insert(0, HERE)

# ─── Colour helpers ───────────────────────────────────────────────────────────

RESET  = "\033[0m";  BOLD  = "\033[1m";  DIM   = "\033[2m"
GREEN  = "\033[32m"; YELLOW= "\033[33m"; RED   = "\033[31m"
CYAN   = "\033[36m"; WHITE = "\033[97m"

def _c(text, *codes): return "".join(codes) + str(text) + RESET
def _bar(value, max_val, width=30, colour=CYAN):
    filled = max(0, min(width, int(round((value / max(max_val, 1)) * width))))
    return _c("█" * filled, colour) + _c("░" * (width - filled), DIM)

def _fmt_rate(r):
    if r >= 3500: return _c(f"{r:,} msg/s", GREEN, BOLD)
    if r >= 2000: return _c(f"{r:,} msg/s", YELLOW)
    return _c(f"{r:,} msg/s", RED)

def _fmt_us(us, target):
    if us <= target:        return _c(f"{us:.1f} µs", GREEN)
    if us <= target * 2:    return _c(f"{us:.1f} µs", YELLOW)
    return _c(f"{us:.1f} µs", RED)

# ─── Output ───────────────────────────────────────────────────────────────────

def _print_stage(name, desc, data, target):
    p50, p95, p99 = data["p50_us"], data["p95_us"], data["p99_us"]
    mean, maxus   = data["mean_us"], data["max_us"]
    rate, n       = data["max_sustained_rate"], data["samples"]
    max_bar = max(p99 * 1.1, target * 1.5)

    print(f"\n  {_c(name, BOLD, WHITE)}")
    print(f"  {_c(desc, DIM)}")
    print(f"  p50  {_bar(p50, max_bar, 28, CYAN)}   {_fmt_us(p50, target)}")
    print(f"  p95  {_bar(p95, max_bar, 28, YELLOW)}   {_fmt_us(p95, target)}  ← target ≤{target}µs")
    print(f"  p99  {_bar(p99, max_bar, 28, RED)}   {_fmt_us(p99, target)}")
    print(f"  mean {_c(f'{mean:.1f} µs', DIM)}   max {_c(f'{maxus:.1f} µs', DIM)}   {n:,} samples")
    print(f"  sustained rate: {_fmt_rate(rate)}")


def _print_results(r):
    print()
    print(_c("═" * 64, DIM))
    print(_c("  ADS-B Dashboard — Pipeline Benchmark", BOLD, WHITE))
    print(_c("═" * 64, DIM))

    py_ver      = r['python_version']
    pymodes_ver = r.get('pymodes_version', '?')
    orjson_ver  = r.get('orjson_version', '')
    corpus_size = r['corpus_size']
    warm_count  = r.get('warm_aircraft_count', '?')
    n_msgs      = r['n_msgs']
    cython      = r.get('pymodes_cython', False)
    orj         = r.get('orjson', False)
    paused      = r.get('decoder_paused_during_run', False)

    cython_str  = _c('OK loaded', GREEN) if cython else _c('X pure Python -- pip install pyModeS[cython]', RED)
    orjson_str  = _c('OK ' + orjson_ver, GREEN) if orj else _c('X not installed -- pip install orjson', YELLOW)
    paused_str  = _c('yes - clean measurement', GREEN) if paused else _c('no - may include GIL noise', YELLOW)

    print(f"\n  {_c('Environment', BOLD)}")
    print(f"  Python        {_c(py_ver, CYAN)}")
    print(f"  pyModeS       {_c(pymodes_ver, CYAN)}  Cython: {cython_str}")
    print(f"  orjson        {orjson_str}")
    print(f"  Corpus        {corpus_size} frame types, {warm_count} warm aircraft")
    print(f"  Iterations    {n_msgs} per stage")
    print(f"  Decoder paused  {paused_str}")

    stages = [
        ("stage_beast_parse",   "Beast parse",        "TCP bytes → message dict",                                  40),
        ("stage_decode_warm",   "Decode — warm",      "process_message() known aircraft, no I/O",                  400),
        ("stage_new_aircraft",  "Decode — cold",      "process_message() first frame per ICAO, full enrichment",   400),
        ("stage_get_snapshot",  "get_snapshot()",     "Build broadcast dict under _lock",                          500),
        ("stage_json_serial",   "JSON serialise",     "orjson/json snapshot → bytes",                              200),
        ("stage_full_pipeline", "Full pipeline ★",    "Beast parse + decode end-to-end",                           400),
    ]
    print(f"\n  {_c('Stage breakdown', BOLD)}")
    for key, name, desc, target in stages:
        if key in r:
            _print_stage(name, desc, r[key], target)

    verdict = r.get("verdict", "?")
    detail  = r.get("verdict_detail", "")
    colours = {"PASS": GREEN, "MARGINAL": YELLOW, "FAIL": RED}
    col     = colours.get(verdict, WHITE)
    print()
    print(_c("─" * 64, DIM))
    print(f"  {_c(verdict, col, BOLD)}  {detail}")
    print(_c("─" * 64, DIM))
    print(f"  Total benchmark time: {r['total_bench_time_s']}s")
    print()


# ─── Compare mode ─────────────────────────────────────────────────────────────

def _compare(path_a, path_b):
    with open(path_a) as f: a = json.load(f)
    with open(path_b) as f: b = json.load(f)

    stages = [
        ("stage_beast_parse",   "Beast parse"),
        ("stage_decode_warm",   "Decode warm"),
        ("stage_new_aircraft",  "New aircraft"),
        ("stage_get_snapshot",  "get_snapshot"),
        ("stage_json_serial",   "JSON serial"),
        ("stage_full_pipeline", "Full pipeline ★"),
    ]
    la, lb = os.path.basename(path_a), os.path.basename(path_b)
    col = 22

    print()
    print(_c("═" * 72, DIM))
    print(_c("  Benchmark Comparison", BOLD, WHITE))
    print(_c("═" * 72, DIM))
    print(f"  {'Stage':<{col}}  {'Metric':<8}  {_c(la, CYAN):<38}  {_c(lb, CYAN)}")
    print(_c("  " + "─" * 68, DIM))

    for key, name in stages:
        da, db_ = a.get(key), b.get(key)
        if not da or not db_: continue
        for metric in ("p50_us", "p95_us", "p99_us", "max_sustained_rate"):
            va, vb = da[metric], db_[metric]
            is_rate = metric == "max_sustained_rate"
            improved = (vb > va) if is_rate else (vb < va)
            degraded = (vb < va) if is_rate else (vb > va)
            delta = _c(f"▲ {abs(vb-va):.1f}", GREEN) if improved else (_c(f"▼ {abs(vb-va):.1f}", RED) if degraded else _c("—", DIM))
            unit  = "msg/s" if is_rate else "µs"
            label = name if metric == "p50_us" else ""
            mname = metric.replace("_us","").replace("max_sustained_rate","rate")
            print(f"  {label:<{col}}  {mname:<8}  {va:>8.1f} {unit}  →  {vb:>8.1f} {unit}   {delta}")
        print()

    va_v, vb_v = a.get("verdict","?"), b.get("verdict","?")
    vc = lambda v: GREEN if v=="PASS" else YELLOW if v=="MARGINAL" else RED
    print(f"  Verdict:  {_c(va_v, vc(va_v), BOLD)}  →  {_c(vb_v, vc(vb_v), BOLD)}")
    print(_c("═" * 72, DIM))
    print()


# ─── Entry point ──────────────────────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser(description="ADS-B Dashboard pipeline benchmark")
    parser.add_argument("--quick",   action="store_true", help="500 iterations (faster, less stable)")
    parser.add_argument("--save",    metavar="FILE",       help="Save results to JSON")
    parser.add_argument("--compare", metavar=("A","B"),   nargs=2, help="Compare two saved JSON files")
    args = parser.parse_args()

    if args.compare:
        _compare(args.compare[0], args.compare[1])
        return

    n = 500 if args.quick else 5000
    if args.quick:
        print(_c("\n  Quick mode — 500 iterations\n", YELLOW))

    print(_c(f"\n  Running benchmark ({n:,} iterations per stage)…", DIM))

    import benchmark as _bm
    # Standalone: no live decoder running, so paused=False is correct
    results = _bm.run_benchmark(n_msgs=n, paused=False)

    _print_results(results)

    if args.save:
        with open(args.save, "w") as f:
            json.dump(results, f, indent=2)
        print(_c(f"  Results saved to {args.save}\n", GREEN))


if __name__ == "__main__":
    main()
