import os
import sys
from types import SimpleNamespace

import pytest

sys.path.insert(0, os.path.dirname(os.path.dirname(__file__)))

from radar.aircraft_localiser import AircraftLocaliser, _haversine_m
from radar.aircraft_models import RadarBearingCalibration, Stage3LiveDetection
from radar.sweep import LiveSyncState, predict_sync_observation


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
        prop_delay_enabled=True,
    )
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
        detection, sync, 51.0, -1.0, calibration,
    )
    # Recompute with the same range basis used by _bearing_from_live_detection.
    range_nm = _haversine_m(51.0, -1.0, detection.truth_lat, detection.truth_lon) / 1852.0
    prediction = predict_sync_observation(
        sync,
        detection.arrival_us,
        range_nm=range_nm,
    )

    assert obs is not None
    assert obs.phase_deg == pytest.approx(prediction.phase_in_rot_deg, abs=0.02)
    assert obs.bearing_obs_deg == pytest.approx((prediction.predicted_bearing_deg + 2.0) % 360.0, abs=0.02)


# ---------------------------------------------------------------------------
# Absolute phase trust gate — only Python simple sync with phase_status="trusted"
# ---------------------------------------------------------------------------

def test_trusted_phase_status_passes_gate():
    """phase_status='trusted' on multi_aircraft_burst source must pass the gate."""
    sync = LiveSyncState(
        iid=7, period_s=4.0, phase_epoch_us=0.0, phase_offset_deg=45.0,
        sync_quality=0.8, sync_jitter_deg=3.0, last_sync_update_ts=1000.0,
        source="multi_aircraft_burst", usable=True,
        phase_status="trusted",
    )
    assert AircraftLocaliser._sync_state_has_trusted_absolute_phase(sync) is True


def test_non_trusted_phase_status_rejected():
    """phase_status other than 'trusted' must be rejected regardless of other fields."""
    for status in ("provisional", "untrusted", None):
        sync = LiveSyncState(
            iid=7, period_s=4.0, phase_epoch_us=0.0, phase_offset_deg=45.0,
            sync_quality=0.8, sync_jitter_deg=3.0, last_sync_update_ts=1000.0,
            source="multi_aircraft_burst", usable=True,
            phase_status=status,
        )
        assert AircraftLocaliser._sync_state_has_trusted_absolute_phase(sync) is False, \
            f"expected False for phase_status={status!r}"


def test_go_source_always_rejected():
    """Go-sourced sync must be rejected regardless of phase_status value."""
    sync = LiveSyncState(
        iid=7, period_s=4.0, phase_epoch_us=0.0, phase_offset_deg=45.0,
        sync_quality=0.8, sync_jitter_deg=3.0, last_sync_update_ts=1000.0,
        source="go_multi_aircraft_burst", usable=True,
        phase_status="trusted",
    )
    assert AircraftLocaliser._sync_state_has_trusted_absolute_phase(sync) is False


def test_unknown_source_rejected():
    """An unrecognised source must always be rejected."""
    sync = LiveSyncState(
        iid=7, period_s=4.0, phase_epoch_us=0.0, phase_offset_deg=45.0,
        sync_quality=0.8, sync_jitter_deg=3.0, last_sync_update_ts=1000.0,
        source="unknown_source", usable=True,
    )
    assert AircraftLocaliser._sync_state_has_trusted_absolute_phase(sync) is False
