#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import pathlib
import time
import urllib.request
from collections import Counter, defaultdict
from datetime import datetime, timezone
from typing import Any


def _http_json(url: str, timeout_s: float = 8.0) -> dict[str, Any]:
    with urllib.request.urlopen(url, timeout=timeout_s) as resp:
        return json.loads(resp.read().decode("utf-8"))


def _pick(*vals: Any) -> Any:
    for v in vals:
        if v is not None:
            return v
    return None


def extract_compact_row(payload: dict[str, Any], iid: int, ts: float) -> dict[str, Any]:
    ss = payload.get("sync_state") or {}

    go_unusable = _pick(
        ss.get("go_sync_unusable_reason"),
        ss.get("go_diagnostic_go_sync_unusable_reason"),
    )

    tq_count = _pick(
        ss.get("transition_quarantine_count"),
        ss.get("go_diagnostic_transition_quarantine_count"),
        payload.get("transition_quarantine_count"),
        payload.get("go_diagnostic_transition_quarantine_count"),
    )
    tq_fit_excl = _pick(
        ss.get("transition_quarantine_fit_excluded_count"),
        ss.get("go_diagnostic_transition_quarantine_fit_excluded_count"),
        payload.get("transition_quarantine_fit_excluded_count"),
        payload.get("go_diagnostic_transition_quarantine_fit_excluded_count"),
    )
    tq_hard_supp = _pick(
        ss.get("transition_quarantine_hard_reject_suppressed_count"),
        ss.get("go_diagnostic_transition_quarantine_hard_reject_suppressed_count"),
        payload.get("transition_quarantine_hard_reject_suppressed_count"),
        payload.get("go_diagnostic_transition_quarantine_hard_reject_suppressed_count"),
    )

    stale_go_evidence_raw = _pick(
        ss.get("stale_go_evidence"),
        ss.get("anchor_retained_without_current_evidence"),
        payload.get("stale_go_evidence"),
    )
    handoff_reason = _pick(ss.get("handoff_reason"), payload.get("handoff_reason"))
    go_operational_blocking_gate = _pick(ss.get("go_operational_blocking_gate"), payload.get("go_operational_blocking_gate"))
    if stale_go_evidence_raw is None:
        stale_go_evidence = (
            handoff_reason == "stale_go_evidence"
            or go_operational_blocking_gate == "go_readiness.go_evidence_fresh"
        )
    else:
        stale_go_evidence = (
            bool(stale_go_evidence_raw)
            or handoff_reason == "stale_go_evidence"
            or go_operational_blocking_gate == "go_readiness.go_evidence_fresh"
        )

    return {
        "iid": iid,
        "timestamp": ts,
        "sync_state_present": bool(payload.get("sync_state") is not None),
        "go_operational_enabled": _pick(ss.get("go_operational_enabled"), payload.get("go_operational_enabled")),
        "go_operational_active": _pick(ss.get("go_operational_active"), payload.get("go_operational_active")),
        "go_operational_blocking_gate": go_operational_blocking_gate,
        "blocking_gate": _pick(ss.get("blocking_gate"), payload.get("blocking_gate")),
        "handoff_state": _pick(ss.get("handoff_state"), payload.get("handoff_state")),
        "handoff_reason": handoff_reason,
        "go_sync_unusable_reason": go_unusable,
        "go_diagnostic_go_sync_unusable_reason": _pick(ss.get("go_diagnostic_go_sync_unusable_reason"), payload.get("go_diagnostic_go_sync_unusable_reason")),
        "holdover": _pick(ss.get("holdover"), payload.get("holdover")),
        "holdover_reason": _pick(ss.get("holdover_reason"), ss.get("go_diagnostic_holdover_reason"), payload.get("holdover_reason")),
        "strict_gate_primary_fail_reason": _pick(ss.get("strict_gate_primary_fail_reason"), ss.get("strict_gate_fail_reason"), payload.get("strict_gate_primary_fail_reason"), payload.get("strict_gate_fail_reason")),
        "quality_gate_fail_reason": _pick(ss.get("quality_gate_fail_reason"), payload.get("quality_gate_fail_reason")),
        "sync_quality": _pick(ss.get("sync_quality"), ss.get("go_diagnostic_sync_quality"), payload.get("sync_quality")),
        "status_at_quality_eval": _pick(ss.get("status_at_quality_eval"), ss.get("go_diagnostic_status_at_quality_eval"), payload.get("status_at_quality_eval")),
        "quality_effective_value": _pick(ss.get("go_diagnostic_go_sync_usable_quality_value"), payload.get("quality_effective_value")),
        "transition_quarantine_count": tq_count,
        "transition_quarantine_fit_excluded_count": tq_fit_excl,
        "transition_quarantine_hard_reject_suppressed_count": tq_hard_supp,
        "stale_go_evidence": stale_go_evidence,
        "phase_status_display": _pick(ss.get("phase_status_display"), payload.get("phase_status_display")),
        "phase_evidence_fresh": _pick(ss.get("phase_evidence_fresh"), payload.get("phase_evidence_fresh")),
        "phase_evidence_age_s": _pick(ss.get("phase_evidence_age_s"), payload.get("phase_evidence_age_s")),
    }


def side_by_side(raw_payload: dict[str, Any], compact: dict[str, Any]) -> dict[str, Any]:
    ss = raw_payload.get("sync_state") or {}
    return {
        "raw_sync_state.go_operational_active": ss.get("go_operational_active"),
        "compact.go_operational_active": compact.get("go_operational_active"),
        "raw_sync_state.handoff_state": ss.get("handoff_state"),
        "compact.handoff_state": compact.get("handoff_state"),
        "raw_sync_state.handoff_reason": ss.get("handoff_reason"),
        "compact.handoff_reason": compact.get("handoff_reason"),
        "raw_sync_state.go_operational_blocking_gate": ss.get("go_operational_blocking_gate"),
        "compact.go_operational_blocking_gate": compact.get("go_operational_blocking_gate"),
        "raw_sync_state.blocking_gate": ss.get("blocking_gate"),
        "compact.blocking_gate": compact.get("blocking_gate"),
        "raw_sync_state.go_sync_unusable_reason": ss.get("go_sync_unusable_reason"),
        "compact.go_sync_unusable_reason": compact.get("go_sync_unusable_reason"),
        "raw_sync_state.holdover_reason": ss.get("holdover_reason"),
        "compact.holdover_reason": compact.get("holdover_reason"),
        "raw_sync_state.strict_gate_primary_fail_reason": ss.get("strict_gate_primary_fail_reason"),
        "compact.strict_gate_primary_fail_reason": compact.get("strict_gate_primary_fail_reason"),
        "raw_sync_state.transition_quarantine_count": ss.get("transition_quarantine_count"),
        "compact.transition_quarantine_count": compact.get("transition_quarantine_count"),
        "raw_sync_state.transition_quarantine_fit_excluded_count": ss.get("transition_quarantine_fit_excluded_count"),
        "compact.transition_quarantine_fit_excluded_count": compact.get("transition_quarantine_fit_excluded_count"),
        "raw_sync_state.transition_quarantine_hard_reject_suppressed_count": ss.get("transition_quarantine_hard_reject_suppressed_count"),
        "compact.transition_quarantine_hard_reject_suppressed_count": compact.get("transition_quarantine_hard_reject_suppressed_count"),
    }


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--base-url", default="http://127.0.0.1:8000")
    ap.add_argument("--duration-s", type=int, default=600)
    ap.add_argument("--interval-s", type=float, default=5.0)
    ap.add_argument("--top-n", type=int, default=25)
    ap.add_argument("--out-dir", default="tasks/radar_sync_baseline")
    ap.add_argument("--label", default="")
    args = ap.parse_args()

    out_dir = pathlib.Path(args.out_dir)
    out_dir.mkdir(parents=True, exist_ok=True)
    stamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    lbl = f"_{args.label}" if args.label else ""
    rows_path = out_dir / f"blocker_baseline_compact{lbl}_{stamp}_rows.jsonl"
    summary_path = out_dir / f"blocker_baseline_compact{lbl}_{stamp}_summary.json"
    sbs_path = out_dir / f"blocker_baseline_compact{lbl}_{stamp}_raw_vs_compact.json"

    iids_payload = _http_json(f"{args.base_url.rstrip('/')}/api/radar/iids")
    top_rows = sorted(iids_payload.get("iids", []), key=lambda r: r.get("count", 0), reverse=True)[: args.top_n]
    iids = [int(r["iid"]) for r in top_rows if isinstance(r.get("iid"), int)]

    start = time.time()
    deadline = start + args.duration_s
    rows = []
    fails = []
    side = []

    while time.time() < deadline:
        tick = time.time()
        for iid in iids:
            url = f"{args.base_url.rstrip('/')}/api/radar/iids/{iid}/sync-snapshot?window_s=90&compact=1"
            try:
                payload = _http_json(url)
                compact = extract_compact_row(payload, iid=iid, ts=tick)
                rows.append(compact)
                if len(side) < 5:
                    side.append({"iid": iid, "timestamp": tick, **side_by_side(payload, compact)})
            except Exception as exc:
                fails.append({"iid": iid, "timestamp": tick, "error": str(exc)})
        sleep_s = args.interval_s - (time.time() - tick)
        if sleep_s > 0:
            time.sleep(sleep_s)

    with rows_path.open("w", encoding="utf-8") as f:
        for r in rows:
            f.write(json.dumps(r, separators=(",", ":")) + "\n")
    with sbs_path.open("w", encoding="utf-8") as f:
        json.dump(side, f, indent=2)

    def count(field: str) -> dict[str, int]:
        c = Counter(str(r.get(field)) for r in rows)
        return dict(c.most_common())

    by_iid = defaultdict(list)
    for r in rows:
        by_iid[r["iid"]].append(r)

    summary = {
        "capture": {
            "rows": len(rows),
            "failures": len(fails),
            "duration_s": args.duration_s,
            "interval_s": args.interval_s,
            "selected_iids": iids,
            "start_ts": start,
            "end_ts": time.time(),
            "side_by_side_file": str(sbs_path),
        },
        "go_operational_active_dwell_by_iid": {str(iid): sum(1 for r in rr if r.get("go_operational_active") is True) for iid, rr in by_iid.items()},
        "handoff_state_dwell_subset": {k: count("handoff_state").get(k, 0) for k in ["GO_REFINED_READY", "GO_REFINING", "HOLDOVER", "BOOTSTRAPPING_PY"]},
        "go_operational_blocking_gate_counts": count("go_operational_blocking_gate"),
        "handoff_reason_counts": count("handoff_reason"),
        "go_sync_unusable_reason_counts": count("go_sync_unusable_reason"),
        "holdover_reason_counts": count("holdover_reason"),
        "strict_gate_primary_fail_reason_counts": count("strict_gate_primary_fail_reason"),
        "transition_quarantine_counters": {
            "max_count": max((r.get("transition_quarantine_count") for r in rows if r.get("transition_quarantine_count") is not None), default=None),
            "max_fit_excluded": max((r.get("transition_quarantine_fit_excluded_count") for r in rows if r.get("transition_quarantine_fit_excluded_count") is not None), default=None),
            "max_hard_reject_suppressed": max((r.get("transition_quarantine_hard_reject_suppressed_count") for r in rows if r.get("transition_quarantine_hard_reject_suppressed_count") is not None), default=None),
        },
        "stale_go_evidence_counts": count("stale_go_evidence"),
        "stale_anchor_trust_violations": sum(1 for r in rows if r.get("phase_status_display") == "anchor_trusted" and (r.get("phase_evidence_fresh") is False or (isinstance(r.get("phase_evidence_age_s"), (int, float)) and r.get("phase_evidence_age_s") > 120))),
        "go_state_unclassified_or_missing_blocker_rows": sum(1 for r in rows if r.get("sync_state_present") and not r.get("go_operational_active") and r.get("go_operational_blocking_gate") is None),
        "raw_vs_compact_examples": side,
    }

    with summary_path.open("w", encoding="utf-8") as f:
        json.dump(summary, f, indent=2)

    print(json.dumps({"rows_file": str(rows_path), "summary_file": str(summary_path), "side_by_side_file": str(sbs_path), "rows": len(rows), "fails": len(fails)}))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
