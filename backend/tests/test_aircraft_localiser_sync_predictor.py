import os
import sys
from types import SimpleNamespace

import pytest

sys.path.insert(0, os.path.dirname(os.path.dirname(__file__)))

from radar.aircraft_localiser import AircraftLocaliser, _haversine_m
from radar.aircraft_models import RadarBearingCalibration, Stage3LiveDetection
from radar.sweep import LiveSyncState, WaveformBin, predict_sync_observation


def test_live_bearing_uses_authoritative_sync_predictor():
    localiser = AircraftLocaliser(
        radar_state=SimpleNamespace(),
        aircraft_state=None,
        receiver_lat=51.0,
        receiver_lon=-1.0,
    )
    sync = LiveSyncState(
        iid=7,
        period_s=10.0,
        phase_epoch_us=0.0,
        phase_offset_deg=5.0,
        sync_quality=1.0,
        sync_jitter_deg=2.0,
        last_sync_update_ts=1000.0,
        source="multi_aircraft_burst",
        usable=True,
        waveform_enabled=True,
        waveform_applied=True,
        prop_delay_enabled=True,
    )
    bins = [WaveformBin(correction_deg=8.0, weight=10.0, n=10) for _ in range(24)]
    detection = Stage3LiveDetection(
        iid=7,
        icao="ABC123",
        arrival_us=5_000_000.0,
        wall_ts=1000.0,
        df=11,
        signal_dbfs=-12.0,
        receiver_lat=51.0,
        receiver_lon=-1.0,
        truth_lat=51.0,
        truth_lon=-0.9,
        position_age_seconds=0.5,
        association_confidence=1.0,
    )
    calibration = RadarBearingCalibration(
        iid=7,
        bearing_offset_deg=2.0,
        effective_delay_us=0.0,
        bearing_sigma_deg=1.0,
        n_samples=50,
        quality="stable",
        last_calibrated_ts=1000.0,
    )

    obs = localiser._bearing_from_live_detection(
        detection, sync, 51.0, -1.0, calibration, bins,
    )
    # Recompute with the same range basis used by _bearing_from_live_detection.
    range_nm = _haversine_m(51.0, -1.0, detection.truth_lat, detection.truth_lon) / 1852.0
    prediction = predict_sync_observation(
        sync,
        detection.arrival_us,
        range_nm=range_nm,
        waveform_bins=bins,
    )

    assert obs is not None
    assert obs.phase_deg == pytest.approx(prediction.phase_in_rot_deg, abs=0.02)
    assert obs.bearing_obs_deg == pytest.approx((prediction.predicted_bearing_deg + 2.0) % 360.0, abs=0.02)


# ---------------------------------------------------------------------------
# Absolute phase trust gate — localiser must reject relative-only Go sync
# ---------------------------------------------------------------------------

def _make_go_sync(*, absolute_phase_trusted: bool, usable: bool = True) -> LiveSyncState:
    """Build a minimal Go-sourced sync state for gating tests."""
    return LiveSyncState(
        iid=7,
        period_s=4.0,
        phase_epoch_us=0.0,
        phase_offset_deg=45.0,
        sync_quality=0.8,
        sync_jitter_deg=3.0,
        last_sync_update_ts=1000.0,
        source="go_multi_aircraft_burst",
        usable=usable,
        absolute_phase_trusted=absolute_phase_trusted,
        # Populate heuristic fields that would normally gate Python-sourced sync.
        phase_anchor_icao="AAAAAA",
        phase_anchor_status="selected",
        phase_anchor_spread_deg=5.0,
        phase_validation_status="validated",
        phase_validation_contributors=3,
        phase_validation_median_error_deg=2.0,
    )


def test_go_sync_absolute_phase_trusted_true_passes_gate():
    """Go sync with absolute_phase_trusted=True must pass the localiser gate."""
    sync = _make_go_sync(absolute_phase_trusted=True)
    assert AircraftLocaliser._sync_state_has_trusted_absolute_phase(sync) is True


def test_go_sync_absolute_phase_trusted_false_rejected():
    """Go sync with absolute_phase_trusted=False must be rejected by the gate
    even when all heuristic fields look valid. The Go decision is authoritative."""
    sync = _make_go_sync(absolute_phase_trusted=False)
    assert AircraftLocaliser._sync_state_has_trusted_absolute_phase(sync) is False


def test_go_sync_without_apt_field_defaults_to_false():
    """When Go doesn't emit the absolute_phase_trusted field (old binary), the
    default is False — the conservative safe choice for localisation."""
    sync = _make_go_sync(absolute_phase_trusted=False)
    # absolute_phase_trusted=False means the gate rejects it even when heuristics pass.
    assert AircraftLocaliser._sync_state_has_trusted_absolute_phase(sync) is False


def test_python_sync_not_rejected_as_go():
    """Python-sourced sync must still pass heuristic checks (not short-circuited)."""
    sync = LiveSyncState(
        iid=7, period_s=4.0, phase_epoch_us=0.0, phase_offset_deg=45.0,
        sync_quality=0.8, sync_jitter_deg=3.0, last_sync_update_ts=1000.0,
        source="multi_aircraft_burst", usable=True,
        phase_anchor_icao="AAAAAA",
        phase_anchor_status="selected",
        phase_anchor_spread_deg=5.0,
        phase_validation_status="validated",
        phase_validation_contributors=3,
        phase_validation_median_error_deg=2.0,
    )
    assert AircraftLocaliser._sync_state_has_trusted_absolute_phase(sync) is True


def test_unknown_source_rejected():
    """An unrecognised source must always be rejected."""
    sync = LiveSyncState(
        iid=7, period_s=4.0, phase_epoch_us=0.0, phase_offset_deg=45.0,
        sync_quality=0.8, sync_jitter_deg=3.0, last_sync_update_ts=1000.0,
        source="unknown_source", usable=True,
    )
    assert AircraftLocaliser._sync_state_has_trusted_absolute_phase(sync) is False
