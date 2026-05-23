#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import pathlib
import subprocess
import sys
import time
import urllib.error
import urllib.request
from datetime import datetime, timezone


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Capture radar sync baseline payloads over time.")
    parser.add_argument("--base-url", default="http://localhost:8000")
    parser.add_argument("--iid", type=int)
    parser.add_argument("--all-active", action="store_true", help="Capture all active IIDs from /api/radar/iids.")
    parser.add_argument("--duration-s", type=float, default=900.0)
    parser.add_argument("--interval-s", type=float, default=1.0)
    parser.add_argument("--out-dir", default="tasks/radar_sync_baseline")
    parser.add_argument("--label", default="")
    parser.add_argument("--residual-family-capture", action="store_true")
    parser.add_argument("--selected-iids", default="", help="Comma-separated IID allowlist for residual-family capture.")
    parser.add_argument("--per-iid-cap", type=int, default=5000)
    parser.add_argument("--run-cap", type=int, default=50000)
    args = parser.parse_args(argv)
    if args.iid is None and not args.all_active:
        parser.error("--iid is required unless --all-active is used")
    if args.duration_s <= 0:
        parser.error("--duration-s must be > 0")
    if args.interval_s <= 0:
        parser.error("--interval-s must be > 0")
    if args.per_iid_cap <= 0:
        parser.error("--per-iid-cap must be > 0")
    if args.run_cap <= 0:
        parser.error("--run-cap must be > 0")
    return args


def _git_sha() -> str | None:
    try:
        result = subprocess.run(
            ["git", "rev-parse", "--short", "HEAD"],
            check=True,
            capture_output=True,
            text=True,
        )
        return result.stdout.strip() or None
    except Exception:
        return None


def _http_json(url: str, timeout_s: float = 5.0) -> tuple[int | None, object | None, str | None]:
    try:
        with urllib.request.urlopen(url, timeout=timeout_s) as resp:
            status = int(getattr(resp, "status", 200))
            body = resp.read()
        try:
            payload = json.loads(body.decode("utf-8"))
        except Exception as exc:
            return status, None, f"json_decode_error: {exc}"
        return status, payload, None
    except urllib.error.HTTPError as exc:
        return int(exc.code), None, str(exc)
    except Exception as exc:
        return None, None, str(exc)


def _active_iids(base_url: str) -> list[int]:
    status, payload, error = _http_json(f"{base_url.rstrip('/')}/api/radar/iids")
    if status != 200 or not isinstance(payload, dict):
        raise RuntimeError(f"failed to fetch active IIDs: status={status} error={error}")
    rows = payload.get("iids")
    if not isinstance(rows, list):
        return []
    out: list[int] = []
    for row in rows:
        if isinstance(row, dict) and isinstance(row.get("iid"), int):
            out.append(int(row["iid"]))
    return sorted(set(out))


def _parse_selected_iids(text: str) -> set[int]:
    out: set[int] = set()
    for raw in (text or "").split(","):
        raw = raw.strip()
        if not raw:
            continue
        out.add(int(raw))
    return out


def _extract_residual_rows(payload: object) -> list[dict]:
    if not isinstance(payload, dict):
        return []
    out: list[dict] = []
    groups = (
        ("recorded_burst", payload.get("recorded_observations") or []),
        ("recorded_df11", payload.get("recorded_df11_residual_observations") or []),
        ("recomputed_burst", payload.get("recomputed_observations") or []),
        ("recomputed_df11", payload.get("recomputed_df11_residual_observations") or []),
    )
    for origin, rows in groups:
        if not isinstance(rows, list):
            continue
        for row in rows:
            if isinstance(row, dict):
                item = dict(row)
                item["row_origin"] = origin
                out.append(item)
    return out


def run_capture(args: argparse.Namespace) -> tuple[int, int, pathlib.Path]:
    base_url = args.base_url.rstrip("/")
    iids = [args.iid] if args.iid is not None else _active_iids(base_url)
    if not iids:
        raise RuntimeError("no IID targets found")

    out_dir = pathlib.Path(args.out_dir)
    out_dir.mkdir(parents=True, exist_ok=True)
    stamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    label = f"_{args.label}" if args.label else ""
    iid_label = "all" if args.all_active else f"iid{args.iid}"
    out_path = out_dir / f"sync_capture_{iid_label}{label}_{stamp}.ndjson"

    endpoints = ("sync-snapshot", "burst_sync_timeline")
    if args.residual_family_capture:
        endpoints = ("sync-snapshot", "chart-history?window_s=300&mode=all&max_burst_points=3000&max_df11_points=3000")
    git_sha = _git_sha()
    started_mono = time.monotonic()
    deadline = started_mono + args.duration_s
    ok = 0
    fail = 0
    family_rows_written = 0
    family_rows_by_iid: dict[int, int] = {}
    selected_iids = _parse_selected_iids(args.selected_iids) if args.residual_family_capture else set()

    with out_path.open("w", encoding="utf-8") as fp:
        while True:
            now_mono = time.monotonic()
            if now_mono > deadline:
                break
            capture_iso = datetime.now(timezone.utc).isoformat()
            elapsed = now_mono - started_mono
            for iid in iids:
                for endpoint in endpoints:
                    url = f"{base_url}/api/radar/iids/{iid}/{endpoint}"
                    status, payload, error = _http_json(url)
                    record = {
                        "ts_iso": capture_iso,
                        "capture_monotonic_s": elapsed,
                        "git_sha": git_sha,
                        "iid": iid,
                        "endpoint": endpoint,
                        "url": url,
                        "http_status": status,
                        "ok": bool(status == 200 and error is None),
                    }
                    if error is not None:
                        record["error"] = error
                        fail += 1
                    else:
                        record["payload"] = payload
                        ok += 1
                    fp.write(json.dumps(record, separators=(",", ":")) + "\n")

                    if (
                        args.residual_family_capture
                        and error is None
                        and endpoint.startswith("chart-history")
                    ):
                        if selected_iids and iid not in selected_iids:
                            continue
                        rows = _extract_residual_rows(payload)
                        for row in rows:
                            iid_count = family_rows_by_iid.get(iid, 0)
                            if iid_count >= args.per_iid_cap or family_rows_written >= args.run_cap:
                                continue
                            out_row = {
                                "type": "stage8_residual_family_row",
                                "capture_ts_iso": capture_iso,
                                "capture_monotonic_s": elapsed,
                                "git_sha": git_sha,
                                "iid": iid,
                                "endpoint": endpoint,
                                "row_origin": row.get("row_origin"),
                                "event_id": row.get("event_id"),
                                "wall_ts": row.get("wall_ts"),
                                "icao": row.get("icao"),
                                "residual_deg": row.get("residual_deg"),
                                "corrected_residual_deg": row.get("corrected_residual_deg"),
                                "residual_class": row.get("residual_class") or row.get("classification"),
                                "display_residual_class": row.get("display_residual_class"),
                                "sync_update_eligible": row.get("sync_update_eligible"),
                                "fit_eligible": row.get("fit_eligible"),
                                "dominant_family": row.get("dominant_family"),
                                "family_id": row.get("family_id"),
                                "family_role": row.get("family_role"),
                                "family_assignment_reason": row.get("family_assignment_reason"),
                                "contamination_state": row.get("contamination_state"),
                                "contamination_reason": row.get("contamination_reason"),
                                "contamination_gate_reason": row.get("contamination_gate_reason"),
                                "phase_basis": row.get("phase_basis"),
                                "period_authority": row.get("period_authority"),
                                "sync_authority": row.get("sync_authority"),
                                "source_path": row.get("source_path"),
                                "reject_reason": row.get("reject_reason"),
                                "exclusion_reason": row.get("exclusion_reason"),
                                "position_age_s": row.get("position_age_s") or row.get("pos_age_s"),
                                "range_nm": row.get("range_nm"),
                                "classifier_input": row.get("classifier_input"),
                                "chart_only_diagnostic": row.get("chart_only_diagnostic"),
                            }
                            fp.write(json.dumps(out_row, separators=(",", ":")) + "\n")
                            family_rows_by_iid[iid] = iid_count + 1
                            family_rows_written += 1
            fp.flush()
            sleep_s = args.interval_s - (time.monotonic() - now_mono)
            if sleep_s > 0:
                time.sleep(sleep_s)

    return ok, fail, out_path


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv)
    try:
        ok, fail, out_path = run_capture(args)
    except Exception as exc:
        print(f"capture failed: {exc}", file=sys.stderr)
        return 1

    print(f"successful samples: {ok}")
    print(f"failed samples: {fail}")
    print(f"output: {out_path}")
    if args.residual_family_capture:
        print("residual-family capture mode: enabled")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
