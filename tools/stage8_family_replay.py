#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import math
from collections import Counter, defaultdict
from pathlib import Path


def _is_finite_number(value: object) -> bool:
    return isinstance(value, (int, float)) and math.isfinite(float(value))


def _circular_delta_deg(a: float, b: float) -> float:
    return (a - b + 540.0) % 360.0 - 180.0


def _circular_mean_deg(values: list[float]) -> float | None:
    if not values:
        return None
    xs = [math.cos(math.radians(v)) for v in values]
    ys = [math.sin(math.radians(v)) for v in values]
    if not xs or not ys:
        return None
    angle = math.degrees(math.atan2(sum(ys), sum(xs))) % 360.0
    return float(angle)


def _load_rows(path: Path) -> list[dict]:
    rows: list[dict] = []
    with path.open("r", encoding="utf-8") as fp:
        for line in fp:
            line = line.strip()
            if not line:
                continue
            row = json.loads(line)
            if isinstance(row, dict) and row.get("type") == "stage8_residual_family_row":
                rows.append(row)
    return rows


def _summarise_iid(rows: list[dict]) -> dict:
    included = [r for r in rows if bool(r.get("fit_eligible")) and _is_finite_number(r.get("residual_deg"))]
    excluded = [r for r in rows if not bool(r.get("fit_eligible"))]
    chart_only = [r for r in rows if bool(r.get("chart_only_diagnostic"))]
    family_groups: dict[str, list[dict]] = defaultdict(list)
    for row in included:
        family_groups[str(row.get("family_id") or "unclassified")].append(row)

    family_summary = {}
    for family_id, family_rows in family_groups.items():
        residuals = [float(r["residual_deg"]) for r in family_rows if _is_finite_number(r.get("residual_deg"))]
        circular_mean = _circular_mean_deg(residuals)
        spread = None
        if circular_mean is not None and residuals:
            spread = sum(abs(_circular_delta_deg(v, circular_mean)) for v in residuals) / len(residuals)
        icao_counts = Counter(str(r.get("icao") or "") for r in family_rows if r.get("icao"))
        family_summary[family_id] = {
            "row_count": len(family_rows),
            "icao_count": len(icao_counts),
            "top_icaos": icao_counts.most_common(10),
            "circular_mean_deg": circular_mean,
            "circular_spread_deg": spread,
        }

    dominant_count = len(family_groups.get("primary", []))
    secondary_count = len(family_groups.get("secondary", []))
    dominant_mean = family_summary.get("primary", {}).get("circular_mean_deg")
    secondary_mean = family_summary.get("secondary", {}).get("circular_mean_deg")
    separation = None
    if _is_finite_number(dominant_mean) and _is_finite_number(secondary_mean):
        separation = abs(_circular_delta_deg(float(secondary_mean), float(dominant_mean)))

    latest_state = None
    latest_reason = None
    latest_rows = sorted(rows, key=lambda r: float(r.get("wall_ts") or 0.0))
    if latest_rows:
        latest_state = latest_rows[-1].get("contamination_state")
        latest_reason = latest_rows[-1].get("contamination_reason")

    return {
        "contamination_state": latest_state,
        "contamination_reason": latest_reason,
        "family_count": len(family_groups),
        "dominant_family_count": dominant_count,
        "secondary_family_count": secondary_count,
        "dominant_secondary_separation_deg": separation,
        "rows_total": len(rows),
        "rows_included_in_classification": len(included),
        "rows_excluded_from_classification": len(excluded),
        "rows_chart_only_diagnostics": len(chart_only),
        "families": family_summary,
        "chart_bands_correspond_to_classified_families": bool(
            secondary_count > 0
            and any(str(r.get("display_residual_class") or "").startswith(("burst_", "df11_")) for r in included)
        ),
    }


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Replay Stage 8 residual-family captures.")
    parser.add_argument("--input", required=True)
    parser.add_argument("--output", required=True)
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    rows = _load_rows(Path(args.input))
    by_iid: dict[int, list[dict]] = defaultdict(list)
    for row in rows:
        iid = int(row.get("iid") or -1)
        if iid >= 0:
            by_iid[iid].append(row)
    summary = {
        "input": args.input,
        "total_rows": len(rows),
        "iids": {str(iid): _summarise_iid(iid_rows) for iid, iid_rows in sorted(by_iid.items())},
    }
    out_path = Path(args.output)
    out_path.parent.mkdir(parents=True, exist_ok=True)
    out_path.write_text(json.dumps(summary, indent=2, sort_keys=True), encoding="utf-8")
    print(str(out_path))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
