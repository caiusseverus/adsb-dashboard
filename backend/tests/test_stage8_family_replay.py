from __future__ import annotations

import importlib.util
import json
from pathlib import Path


def _load_module():
    root = Path(__file__).resolve().parents[2]
    mod_path = root / "tools" / "stage8_family_replay.py"
    spec = importlib.util.spec_from_file_location("stage8_family_replay", mod_path)
    assert spec is not None and spec.loader is not None
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod


def test_replay_summarises_family_groups_and_excludes_chart_only(tmp_path):
    mod = _load_module()
    rows = [
        {
            "type": "stage8_residual_family_row",
            "iid": 33,
            "icao": "AAAAAA",
            "residual_deg": 2.0,
            "fit_eligible": True,
            "family_id": "primary",
            "display_residual_class": "burst_inlier",
            "contamination_state": "contaminated",
            "contamination_reason": "secondary_family_detected",
        },
        {
            "type": "stage8_residual_family_row",
            "iid": 33,
            "icao": "BBBBBB",
            "residual_deg": 58.0,
            "fit_eligible": True,
            "family_id": "secondary",
            "display_residual_class": "burst_residual",
            "contamination_state": "contaminated",
            "contamination_reason": "secondary_family_detected",
        },
        {
            "type": "stage8_residual_family_row",
            "iid": 33,
            "icao": "CCCCCC",
            "residual_deg": 100.0,
            "fit_eligible": False,
            "chart_only_diagnostic": True,
            "family_id": "unclassified",
            "display_residual_class": "df11_late",
        },
    ]
    path = tmp_path / "rows.jsonl"
    path.write_text("".join(json.dumps(r) + "\n" for r in rows), encoding="utf-8")
    loaded = mod._load_rows(path)
    assert len(loaded) == 3
    summary = mod._summarise_iid(loaded)
    assert summary["rows_included_in_classification"] == 2
    assert summary["rows_chart_only_diagnostics"] == 1
    assert summary["dominant_family_count"] == 1
    assert summary["secondary_family_count"] == 1
    assert summary["chart_bands_correspond_to_classified_families"] is True
