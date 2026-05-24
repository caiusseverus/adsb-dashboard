#!/usr/bin/env python3
"""Summarise live DF11 refined-vs-base shadow verification compact rows."""
from __future__ import annotations

import argparse
import json
import math
from collections import Counter, defaultdict
from pathlib import Path
from statistics import median
from typing import Any


def _num(value: Any) -> float | None:
    try:
        f = float(value)
    except (TypeError, ValueError):
        return None
    return f if math.isfinite(f) else None


def _bool(value: Any) -> bool | None:
    if isinstance(value, bool):
        return value
    if isinstance(value, (int, float)):
        return bool(value)
    if isinstance(value, str):
        v = value.strip().lower()
        if v in {"true", "1", "yes", "on"}:
            return True
        if v in {"false", "0", "no", "off"}:
            return False
    return None


def _q(values: list[float], quantile: float) -> float | None:
    clean = sorted(v for v in values if math.isfinite(v))
    if not clean:
        return None
    if len(clean) == 1:
        return clean[0]
    pos = (len(clean) - 1) * quantile
    lo = math.floor(pos)
    hi = math.ceil(pos)
    if lo == hi:
        return clean[int(lo)]
    return clean[int(lo)] + (clean[int(hi)] - clean[int(lo)]) * (pos - lo)


def _load_rows(path: Path) -> list[dict[str, Any]]:
    rows = []
    with path.open("r", encoding="utf-8") as fh:
        for line in fh:
            if not line.strip():
                continue
            row = json.loads(line)
            if row.get("iid") is not None and _num(row.get("timestamp")) is not None:
                rows.append(row)
    return rows


def _intervals(rows: list[dict[str, Any]]) -> list[tuple[dict[str, Any], float]]:
    rows = sorted(rows, key=lambda r: float(r["timestamp"]))
    if not rows:
        return []
    gaps = [float(b["timestamp"]) - float(a["timestamp"]) for a, b in zip(rows, rows[1:]) if float(b["timestamp"]) >= float(a["timestamp"])]
    default_dt = median(gaps) if gaps else 0.0
    max_dt = max(default_dt * 3.0, default_dt, 1.0) if default_dt > 0 else 0.0
    out = []
    for idx, row in enumerate(rows):
        if idx + 1 < len(rows):
            dt = max(0.0, float(rows[idx + 1]["timestamp"]) - float(row["timestamp"]))
            if max_dt > 0:
                dt = min(dt, max_dt)
        else:
            dt = default_dt
        out.append((row, dt))
    return out


def _is_active_refined(row: dict[str, Any]) -> bool:
    return _bool(row.get("go_operational_active")) is True and str(row.get("period_authority") or "") == "go_refined"


def _verification_available(row: dict[str, Any]) -> bool:
    return str(row.get("df11_verification_reason") or "") == "ok"


def _state(row: dict[str, Any]) -> str:
    state = str(row.get("df11_verification_state") or "")
    if state:
        return state
    if not _verification_available(row):
        return "insufficient_data"
    better = _bool(row.get("df11_refined_better_than_base"))
    if better is True:
        return "refined_better"
    if better is False:
        return "refined_worse"
    return "insufficient_data"


def _sum_dwell(pairs: list[tuple[dict[str, Any], float]], predicate) -> float:
    return sum(dt for row, dt in pairs if predicate(row))


def _transition_count(rows: list[dict[str, Any]]) -> int:
    prev = None
    count = 0
    for row in sorted(rows, key=lambda r: float(r["timestamp"])):
        state = _state(row)
        if state not in {"refined_better", "refined_worse"}:
            continue
        if prev is not None and state != prev:
            count += 1
        prev = state
    return count


def _summarise_iid(rows: list[dict[str, Any]]) -> dict[str, Any]:
    pairs = _intervals(rows)
    total = sum(dt for _, dt in pairs)
    state_dwell = Counter()
    for row, dt in pairs:
        state_dwell[_state(row)] += dt
    active_dwell = _sum_dwell(pairs, _is_active_refined)
    active_bad = _sum_dwell(
        pairs,
        lambda r: _is_active_refined(r) and (_bool(r.get("df11_refined_better_than_base")) is False or _state(r) == "refined_worse"),
    )
    active_not_ok = _sum_dwell(
        pairs,
        lambda r: _is_active_refined(r) and str(r.get("df11_verification_reason") or "") != "ok",
    )
    active_good = _sum_dwell(
        pairs,
        lambda r: _is_active_refined(r) and _state(r) == "refined_better" and str(r.get("df11_verification_confidence") or "") in {"medium", "high"},
    )
    active_suppressed_by_shadow = _sum_dwell(
        pairs,
        lambda r: _is_active_refined(r) and not (_state(r) == "refined_better" and str(r.get("df11_verification_confidence") or "") in {"medium", "high"}),
    )
    deltas = [_num(r.get("df11_refined_alignment_delta")) for r in rows]
    deltas = [d for d in deltas if d is not None]
    base_on = [_num(r.get("df11_base_on_time_count")) for r in rows]
    refined_on = [_num(r.get("df11_refined_on_time_count")) for r in rows]
    base_spread = [_num(r.get("df11_base_residual_spread_deg")) for r in rows]
    refined_spread = [_num(r.get("df11_refined_residual_spread_deg")) for r in rows]
    return {
        "row_count": len(rows),
        "capture_span_s": max(float(r["timestamp"]) for r in rows) - min(float(r["timestamp"]) for r in rows) if rows else 0.0,
        "estimated_dwell_s": total,
        "verification_available_dwell_s": _sum_dwell(pairs, _verification_available),
        "verification_available_ratio": (_sum_dwell(pairs, _verification_available) / total) if total else None,
        "refined_better_ratio": (state_dwell["refined_better"] / total) if total else None,
        "refined_worse_ratio": (state_dwell["refined_worse"] / total) if total else None,
        "insufficient_data_ratio": ((state_dwell["insufficient_data"] + state_dwell["unavailable"]) / total) if total else None,
        "active_refined_dwell_s": active_dwell,
        "active_refined_bad_dwell_s": active_bad,
        "active_refined_not_ok_dwell_s": active_not_ok,
        "active_refined_good_retained_by_shadow_s": active_good,
        "active_refined_suppressed_by_shadow_s": active_suppressed_by_shadow,
        "base_on_time_median": _q([v for v in base_on if v is not None], 0.5),
        "refined_on_time_median": _q([v for v in refined_on if v is not None], 0.5),
        "base_spread_median_deg": _q([v for v in base_spread if v is not None], 0.5),
        "refined_spread_median_deg": _q([v for v in refined_spread if v is not None], 0.5),
        "alignment_delta_p50": _q(deltas, 0.5),
        "alignment_delta_p95": _q(deltas, 0.95),
        "better_worse_transition_count": _transition_count(rows),
        "state_dwell_s": dict(state_dwell),
    }


def _case_rows(rows: list[dict[str, Any]], predicate) -> list[dict[str, Any]]:
    return [row for row in rows if predicate(row)]


def _case_summary(rows: list[dict[str, Any]]) -> dict[str, Any]:
    if not rows:
        return {"row_count": 0, "iids": []}
    by_iid = defaultdict(list)
    for row in rows:
        by_iid[int(row["iid"])].append(row)
    cases = []
    for iid, iid_rows in sorted(by_iid.items()):
        pairs = _intervals(iid_rows)
        future_hold_or_reject = any(
            _bool(r.get("holdover")) is True or (_num(r.get("consecutive_hard_residual_rejects")) or 0.0) > 0
            for r in iid_rows
        )
        cases.append({
            "iid": iid,
            "row_count": len(iid_rows),
            "dwell_s": sum(dt for _, dt in pairs),
            "readiness_reasons": Counter(str(r.get("readiness_reason") or r.get("handoff_reason") or "") for r in iid_rows).most_common(5),
            "slope_states": Counter(str(r.get("slope_not_converged_reason") or r.get("slope_converged") or "") for r in iid_rows).most_common(5),
            "holdover_rows": sum(1 for r in iid_rows if _bool(r.get("holdover")) is True),
            "retained_delta_move_30s_p95": _q([v for r in iid_rows if (v := _num(r.get("retained_go_delta_abs_movement_s_30s"))) is not None], 0.95),
            "applied_delta_move_30s_p95": _q([v for r in iid_rows if (v := _num(r.get("applied_delta_abs_movement_s_30s"))) is not None], 0.95),
            "alignment_delta_p50": _q([v for r in iid_rows if (v := _num(r.get("df11_refined_alignment_delta"))) is not None], 0.5),
            "alignment_delta_p95": _q([v for r in iid_rows if (v := _num(r.get("df11_refined_alignment_delta"))) is not None], 0.95),
            "refined_spread_p50": _q([v for r in iid_rows if (v := _num(r.get("df11_refined_residual_spread_deg"))) is not None], 0.5),
            "base_spread_p50": _q([v for r in iid_rows if (v := _num(r.get("df11_base_residual_spread_deg"))) is not None], 0.5),
            "base_would_have_been_better_rows": sum(1 for r in iid_rows if _num(r.get("df11_refined_alignment_delta")) is not None and float(r["df11_refined_alignment_delta"]) > 0),
            "subsequent_holdover_or_hard_reject_seen_in_case_rows": future_hold_or_reject,
        })
    return {"row_count": len(rows), "iids": cases}


def build_report(rows: list[dict[str, Any]]) -> dict[str, Any]:
    by_iid = defaultdict(list)
    for row in rows:
        by_iid[int(row["iid"])].append(row)
    per_iid = {str(iid): _summarise_iid(iid_rows) for iid, iid_rows in sorted(by_iid.items())}
    false_active = _case_rows(
        rows,
        lambda r: _is_active_refined(r) and (_bool(r.get("df11_refined_better_than_base")) is False or _state(r) == "refined_worse"),
    )
    safe_refined = _case_rows(
        rows,
        lambda r: _is_active_refined(r) and _state(r) == "refined_better" and str(r.get("df11_verification_confidence") or "") in {"medium", "high"},
    )
    active_rows = _case_rows(rows, _is_active_refined)
    suppressed = _case_rows(
        rows,
        lambda r: _is_active_refined(r) and not (_state(r) == "refined_better" and str(r.get("df11_verification_confidence") or "") in {"medium", "high"}),
    )
    return {
        "row_count": len(rows),
        "iid_count": len(by_iid),
        "per_iid": per_iid,
        "false_active_cases": _case_summary(false_active),
        "safe_refined_cases": _case_summary(safe_refined),
        "shadow_enforcement_simulation": {
            "active_refined_rows": len(active_rows),
            "active_refined_rows_suppressed": len(suppressed),
            "active_refined_rows_retained": len(safe_refined),
            "bad_active_rows_avoided": len(false_active),
            "good_active_rows_suppressed_due_insufficient_data": sum(
                1 for r in suppressed
                if _bool(r.get("df11_refined_better_than_base")) is True and _state(r) in {"insufficient_data", "unavailable"}
            ),
        },
    }


def main() -> int:
    parser = argparse.ArgumentParser(description="Summarise live DF11 verification compact rows.")
    parser.add_argument("rows_jsonl", type=Path)
    parser.add_argument("--output", type=Path)
    args = parser.parse_args()
    report = build_report(_load_rows(args.rows_jsonl))
    text = json.dumps(report, indent=2, sort_keys=True)
    if args.output:
        args.output.parent.mkdir(parents=True, exist_ok=True)
        args.output.write_text(text + "\n", encoding="utf-8")
    print(text)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
