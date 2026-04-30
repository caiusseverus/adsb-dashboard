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
