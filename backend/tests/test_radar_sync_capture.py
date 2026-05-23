from __future__ import annotations

import importlib.util
import json
from pathlib import Path


def _load_capture_module():
    root = Path(__file__).resolve().parents[2]
    mod_path = root / "tools" / "radar_sync_capture.py"
    spec = importlib.util.spec_from_file_location("radar_sync_capture", mod_path)
    assert spec is not None and spec.loader is not None
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod


def test_capture_argparse_requires_iid_or_all_active():
    mod = _load_capture_module()
    try:
        mod.parse_args([])
        assert False, "expected SystemExit"
    except SystemExit as exc:
        assert exc.code == 2

    args = mod.parse_args(["--iid", "7"])
    assert args.iid == 7
    assert args.base_url == "http://localhost:8000"
    assert args.duration_s == 900.0

    args_all = mod.parse_args(["--all-active"])
    assert args_all.all_active is True


def test_capture_argparse_rejects_invalid_duration_and_interval():
    mod = _load_capture_module()

    try:
        mod.parse_args(["--iid", "7", "--duration-s", "0"])
        assert False, "expected SystemExit"
    except SystemExit as exc:
        assert exc.code == 2

    try:
        mod.parse_args(["--iid", "7", "--interval-s", "0"])
        assert False, "expected SystemExit"
    except SystemExit as exc:
        assert exc.code == 2


def test_capture_records_http_failures_without_aborting(tmp_path, monkeypatch):
    mod = _load_capture_module()

    calls: list[str] = []

    def fake_http_json(url: str, timeout_s: float = 5.0):
        calls.append(url)
        if url.endswith("/sync-snapshot"):
            return 500, None, "boom"
        return 200, {"ok": True}, None

    monkeypatch.setattr(mod, "_http_json", fake_http_json)
    monkeypatch.setattr(mod, "_git_sha", lambda: "deadbee")

    args = mod.parse_args([
        "--iid", "9",
        "--duration-s", "0.05",
        "--interval-s", "0.05",
        "--out-dir", str(tmp_path),
        "--label", "test",
    ])

    ok, fail, out_path = mod.run_capture(args)

    assert ok >= 1
    assert fail >= 1
    assert out_path.exists()

    rows = [json.loads(line) for line in out_path.read_text(encoding="utf-8").splitlines() if line.strip()]
    assert any((row.get("ok") is False and row.get("error") == "boom") for row in rows)
    assert any((row.get("ok") is True and row.get("payload", {}).get("ok") is True) for row in rows)


def test_residual_family_capture_includes_required_fields_and_caps(tmp_path, monkeypatch):
    mod = _load_capture_module()

    def fake_http_json(url: str, timeout_s: float = 5.0):
        if "chart-history" in url:
            payload = {
                "recorded_observations": [
                    {
                        "event_id": "33:burst:ABCDEF:1",
                        "wall_ts": 100.0,
                        "icao": "ABCDEF",
                        "residual_deg": 2.0,
                        "corrected_residual_deg": 2.0,
                        "residual_class": "inlier",
                        "display_residual_class": "burst_inlier",
                        "sync_update_eligible": True,
                        "fit_eligible": True,
                        "dominant_family": True,
                        "family_id": "primary",
                        "family_role": "dominant",
                        "family_assignment_reason": "test",
                        "contamination_state": "single_family",
                        "contamination_reason": "test_reason",
                        "contamination_gate_reason": "test_gate",
                        "phase_basis": "anchor_relative",
                        "period_authority": "python",
                        "sync_authority": "python",
                        "source_path": "recorded_event",
                        "reject_reason": None,
                        "exclusion_reason": None,
                        "position_age_s": 0.4,
                        "range_nm": 12.0,
                        "classifier_input": True,
                        "chart_only_diagnostic": False,
                    },
                    {
                        "event_id": "33:burst:ABCDEF:2",
                        "wall_ts": 101.0,
                        "icao": "ABCDEF",
                        "residual_deg": 55.0,
                        "display_residual_class": "burst_residual",
                        "sync_update_eligible": False,
                        "fit_eligible": False,
                        "family_id": "unclassified",
                        "family_role": "unclassified",
                        "family_assignment_reason": "not_fit",
                        "contamination_state": "single_family",
                        "contamination_reason": "test_reason",
                        "contamination_gate_reason": "test_gate",
                        "source_path": "recorded_event",
                        "reject_reason": "residual_gate",
                        "exclusion_reason": "residual_gate",
                        "chart_only_diagnostic": True,
                    },
                ],
            }
            return 200, payload, None
        return 200, {"ok": True}, None

    monkeypatch.setattr(mod, "_http_json", fake_http_json)
    monkeypatch.setattr(mod, "_git_sha", lambda: "deadbee")

    args = mod.parse_args([
        "--iid", "33",
        "--duration-s", "0.05",
        "--interval-s", "0.05",
        "--out-dir", str(tmp_path),
        "--label", "stage8",
        "--residual-family-capture",
        "--per-iid-cap", "1",
        "--run-cap", "1",
    ])
    _, _, out_path = mod.run_capture(args)
    rows = [json.loads(line) for line in out_path.read_text(encoding="utf-8").splitlines() if line.strip()]
    family_rows = [r for r in rows if r.get("type") == "stage8_residual_family_row"]
    assert len(family_rows) == 1
    row = family_rows[0]
    for key in (
        "iid", "icao", "event_id", "residual_deg", "display_residual_class",
        "sync_update_eligible", "fit_eligible", "family_id", "family_role",
        "family_assignment_reason", "contamination_state", "phase_basis",
        "period_authority", "sync_authority", "source_path",
    ):
        assert key in row
