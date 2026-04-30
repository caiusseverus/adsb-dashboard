from pathlib import Path


def test_radar_page_uses_anchor_relative_phase_wording():
    radar_page = Path(__file__).resolve().parents[2] / "frontend" / "src" / "pages" / "RadarPage.jsx"
    text = radar_page.read_text(encoding="utf-8")
    assert "Anchor-relative phase" in text
    assert "Geographic radar beam direction is not yet known." in text
    assert "Phase trust status:" not in text
