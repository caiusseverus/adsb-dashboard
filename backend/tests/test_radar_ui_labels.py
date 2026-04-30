from pathlib import Path


def test_radar_page_uses_anchor_relative_phase_wording():
    radar_page = Path(__file__).resolve().parents[2] / "frontend" / "src" / "pages" / "RadarPage.jsx"
    text = radar_page.read_text(encoding="utf-8")
    assert "Anchor-relative phase" in text
    assert "Geographic radar beam direction is not yet known." in text
    assert "Phase trust status:" not in text


def test_radar_page_operational_period_uses_canonical_fields():
    radar_page = Path(__file__).resolve().parents[2] / "frontend" / "src" / "pages" / "RadarPage.jsx"
    text = radar_page.read_text(encoding="utf-8")
    assert "syncState.base_period_s" in text
    assert "syncState.period_delta_s" in text
    assert "syncState.effective_period_s" in text
    assert "const base = Number(syncState.period_base_s)" not in text
    assert "go_runtime_operational" not in text


def test_radar_page_shows_authority_labels_and_separates_diagnostic_delta():
    radar_page = Path(__file__).resolve().parents[2] / "frontend" / "src" / "pages" / "RadarPage.jsx"
    text = radar_page.read_text(encoding="utf-8")
    assert "Refinement status" in text
    assert "Sync authority" in text
    assert "Period authority" in text
    assert "Go Δ (diagnostic)" in text
