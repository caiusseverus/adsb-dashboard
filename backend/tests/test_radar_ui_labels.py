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
    assert "Phase authority" in text
    assert "Handoff state" in text
    assert "Handoff reason" in text
    assert "Blocking gates:" in text
    assert "Go Δ (diagnostic)" in text


def test_radar_page_labels_recorded_mode_as_immutable_without_recomputed_fallback_wording():
    radar_page = Path(__file__).resolve().parents[2] / "frontend" / "src" / "pages" / "RadarPage.jsx"
    text = radar_page.read_text(encoding="utf-8")
    assert "Recorded immutable event history" in text
    assert "Recorded warming up (showing recomputed fallback)" not in text
    assert "Recorded residual data is unavailable or still insufficient for this IID." in text


def test_radar_page_labels_recomputed_mode_as_projection_and_uses_explicit_bases():
    radar_page = Path(__file__).resolve().parents[2] / "frontend" / "src" / "pages" / "RadarPage.jsx"
    text = radar_page.read_text(encoding="utf-8")
    assert "Recomputed current projection" in text
    assert "recorded event basis" in text
    assert "RESIDUAL_BASIS_COMPACT_BOOTSTRAP" in text
    assert "RESIDUAL_BASIS_RUNTIME_EFFECTIVE" in text
    assert "RESIDUAL_BASIS_GO_RUNTIME_DIAGNOSTIC" in text


def test_radar_page_uses_recorded_event_buffer_and_explicit_empty_reasons():
    radar_page = Path(__file__).resolve().parents[2] / "frontend" / "src" / "pages" / "RadarPage.jsx"
    text = radar_page.read_text(encoding="utf-8")
    assert "setRecordedEventBuffer" in text
    assert "event?.event_id" in text
    assert "Residual-vs-bearing empty:" in text
    assert "Residual-vs-range empty:" in text
    assert "frontend_recorded_buffer_size" in text


def test_radar_page_all_icaos_chip_uses_distinct_recorded_burst_icaos():
    radar_page = Path(__file__).resolve().parents[2] / "frontend" / "src" / "pages" / "RadarPage.jsx"
    text = radar_page.read_text(encoding="utf-8")
    assert "All ICAOs ({sortedIcaos.length})" in text
    assert "recorded_event_count_in_window" in text
