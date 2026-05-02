"""Tests for Stage 6 phase semantics corrections."""

import pytest
from radar.sweep import _normalise_phase_fields


class TestNormalisePhaseFields:
    def test_sweep_epoch_only_serialization(self):
        payload = {
            "phase_basis": "sweep_epoch_only",
            "phase_is_absolute": False,
            "phase_authority": "py_bootstrap",
            "phase_offset_deg": 236.0,
            "consistency_warnings": [],
        }
        result = _normalise_phase_fields(payload)
        assert result["phase_offset_basis"] == "sweep_epoch"
        assert result["phase_is_absolute"] is False
        assert result["phase_offset_geographic_deg"] is None
        assert "phase_is_absolute_normalised_to_false_not_geographic" not in result["consistency_warnings"]

    def test_anchor_relative_serialization(self):
        payload = {
            "phase_basis": "anchor_relative",
            "phase_is_absolute": False,
            "phase_authority": "py_anchor_relative",
            "phase_offset_deg": 123.4,
            "consistency_warnings": [],
        }
        result = _normalise_phase_fields(payload)
        assert result["phase_offset_basis"] == "anchor_relative"
        assert result["phase_is_absolute"] is False
        assert result["phase_offset_geographic_deg"] is None

    def test_geographic_serialization_with_absolute(self):
        payload = {
            "phase_basis": "geographic",
            "phase_is_absolute": True,
            "phase_authority": "go_runtime",
            "phase_offset_deg": 50.0,
            "phase_offset_geographic_deg": 50.0,
            "consistency_warnings": [],
        }
        result = _normalise_phase_fields(payload)
        assert result["phase_offset_basis"] == "geographic"
        assert result["phase_is_absolute"] is True
        assert result["phase_offset_geographic_deg"] == 50.0

    def test_geographic_without_absolute_is_normalised(self):
        payload = {
            "phase_basis": "geographic",
            "phase_is_absolute": False,
            "phase_authority": "go_runtime",
            "phase_offset_deg": 50.0,
            "phase_offset_geographic_deg": None,
            "consistency_warnings": [],
        }
        result = _normalise_phase_fields(payload)
        assert result["phase_offset_basis"] == "geographic"
        assert result["phase_is_absolute"] is False

    def test_contradictory_absolute_not_geographic_is_normalised(self):
        payload = {
            "phase_basis": "sweep_epoch_only",
            "phase_is_absolute": True,
            "phase_authority": "py_bootstrap",
            "phase_offset_deg": 100.0,
            "consistency_warnings": [],
        }
        result = _normalise_phase_fields(payload)
        assert result["phase_is_absolute"] is False
        assert "phase_is_absolute_normalised_to_false_not_geographic" in result["consistency_warnings"]

    def test_phase_offset_geographic_null_when_not_geographic(self):
        payload = {
            "phase_basis": "anchor_relative",
            "phase_is_absolute": False,
            "phase_authority": "py_anchor_relative",
            "phase_offset_geographic_deg": 45.0,
            "consistency_warnings": [],
        }
        result = _normalise_phase_fields(payload)
        assert result["phase_offset_geographic_deg"] is None

    def test_unavailable_basis_forces_not_absolute(self):
        payload = {
            "phase_basis": "unavailable",
            "phase_is_absolute": True,
            "phase_authority": "unavailable",
            "phase_offset_geographic_deg": 30.0,
            "consistency_warnings": [],
        }
        result = _normalise_phase_fields(payload)
        assert result["phase_is_absolute"] is False
        assert result["phase_offset_geographic_deg"] is None

    def test_unknown_basis_defaults_to_unavailable(self):
        payload = {
            "phase_basis": "something_unknown",
            "phase_is_absolute": False,
            "phase_authority": "unknown",
            "consistency_warnings": [],
        }
        result = _normalise_phase_fields(payload)
        assert result["phase_offset_basis"] == "unavailable"
        assert result["phase_is_absolute"] is False

    def test_phase_offset_basis_mismatch_normalised(self):
        payload = {
            "phase_basis": "anchor_relative",
            "phase_is_absolute": False,
            "phase_authority": "py_anchor_relative",
            "phase_offset_basis": "sweep_epoch",
            "consistency_warnings": [],
        }
        result = _normalise_phase_fields(payload)
        assert result["phase_offset_basis"] == "anchor_relative"
        assert "phase_offset_basis_mismatch" in str(result["consistency_warnings"])

    def test_phase_offset_basis_invalid_value_normalised(self):
        payload = {
            "phase_basis": "anchor_relative",
            "phase_is_absolute": False,
            "phase_authority": "py_anchor_relative",
            "phase_offset_basis": "garbage",
            "consistency_warnings": [],
        }
        result = _normalise_phase_fields(payload)
        assert result["phase_offset_basis"] == "anchor_relative"
        assert "phase_offset_basis_invalid" in str(result["consistency_warnings"])

    def test_phase_offset_basis_preserved_when_correct(self):
        payload = {
            "phase_basis": "geographic",
            "phase_is_absolute": True,
            "phase_authority": "go_runtime",
            "phase_offset_deg": 50.0,
            "phase_offset_geographic_deg": 50.0,
            "phase_offset_basis": "geographic",
            "consistency_warnings": [],
        }
        result = _normalise_phase_fields(payload)
        assert result["phase_offset_basis"] == "geographic"
        assert len([w for w in result["consistency_warnings"]
                    if "phase_offset_basis" in str(w)]) == 0

    def test_phase_authority_basis_mismatch_warns_when_absolute_but_not_geographic(self):
        """When phase_basis=geographic but authority is non-geographic, warn."""
        payload = {
            "phase_basis": "geographic",
            "phase_is_absolute": True,
            "phase_authority": "py_bootstrap",
            "phase_offset_deg": 100.0,
            "phase_offset_geographic_deg": 100.0,
            "consistency_warnings": [],
        }
        result = _normalise_phase_fields(payload)
        assert "phase_authority_phase_basis_mismatch" in str(result["consistency_warnings"])

    def test_sweep_epoch_only_no_geographic_offset_in_serialized(self):
        payload = {
            "phase_basis": "sweep_epoch_only",
            "phase_is_absolute": False,
            "phase_offset_geographic_deg": 99.9,
            "consistency_warnings": [],
        }
        result = _normalise_phase_fields(payload)
        assert result["phase_offset_geographic_deg"] is None
        assert result["phase_is_absolute"] is False


class TestDerivePhaseBasis:
    def test_sweep_epoch_only_when_no_anchor(self):
        from radar.sync_models import LiveSyncState
        sync = LiveSyncState(
            iid=1,
            period_s=5.0,
            phase_epoch_us=0.0,
            phase_offset_deg=0.0,
            sync_quality=0.8,
            sync_jitter_deg=3.0,
            last_sync_update_ts=0.0,
            source="multi_aircraft_burst",
            usable=True,
            phase_anchor_icao=None,
            phase_anchor_status="unavailable",
        )
        from radar.aircraft_localiser import AircraftLocaliser
        assert AircraftLocaliser._derive_phase_basis(sync) == "sweep_epoch_only"

    def test_anchor_relative_when_anchor_selected(self):
        from radar.sync_models import LiveSyncState
        sync = LiveSyncState(
            iid=1,
            period_s=5.0,
            phase_epoch_us=0.0,
            phase_offset_deg=0.0,
            sync_quality=0.8,
            sync_jitter_deg=3.0,
            last_sync_update_ts=0.0,
            source="multi_aircraft_burst",
            usable=True,
            phase_anchor_icao="ABCDEF",
            phase_anchor_status="selected",
        )
        from radar.aircraft_localiser import AircraftLocaliser
        assert AircraftLocaliser._derive_phase_basis(sync) == "anchor_relative"

    def test_anchor_relative_when_anchor_only(self):
        from radar.sync_models import LiveSyncState
        sync = LiveSyncState(
            iid=1,
            period_s=5.0,
            phase_epoch_us=0.0,
            phase_offset_deg=0.0,
            sync_quality=0.8,
            sync_jitter_deg=3.0,
            last_sync_update_ts=0.0,
            source="multi_aircraft_burst",
            usable=True,
            phase_anchor_icao="ABCDEF",
            phase_anchor_status="anchor_only",
        )
        from radar.aircraft_localiser import AircraftLocaliser
        assert AircraftLocaliser._derive_phase_basis(sync) == "anchor_relative"


class TestGeographicPhaseGate:
    def test_sweep_epoch_only_rejected(self):
        from radar.sync_models import LiveSyncState
        from radar.aircraft_localiser import AircraftLocaliser
        sync = LiveSyncState(
            iid=1,
            period_s=5.0,
            phase_epoch_us=0.0,
            phase_offset_deg=0.0,
            sync_quality=0.8,
            sync_jitter_deg=3.0,
            last_sync_update_ts=0.0,
            source="multi_aircraft_burst",
            usable=True,
            phase_anchor_icao=None,
            phase_anchor_status="unavailable",
        )
        ok, reason = AircraftLocaliser._sync_state_has_geographic_phase(sync)
        assert ok is False
        assert reason == "phase_basis_sweep_epoch_only"

    def test_anchor_relative_rejected(self):
        from radar.sync_models import LiveSyncState
        from radar.aircraft_localiser import AircraftLocaliser
        sync = LiveSyncState(
            iid=1,
            period_s=5.0,
            phase_epoch_us=0.0,
            phase_offset_deg=0.0,
            sync_quality=0.8,
            sync_jitter_deg=3.0,
            last_sync_update_ts=0.0,
            source="multi_aircraft_burst",
            usable=True,
            phase_anchor_icao="ABCDEF",
            phase_anchor_status="selected",
        )
        ok, reason = AircraftLocaliser._sync_state_has_geographic_phase(sync)
        assert ok is False
        assert reason == "phase_basis_anchor_relative"

    def test_geographic_without_absolute_false_rejected(self):
        from radar.sync_models import LiveSyncState
        from radar.aircraft_localiser import AircraftLocaliser
        from radar.aircraft_localiser import REASON_PHASE_ABSOLUTE_FALSE
        sync = LiveSyncState(
            iid=1,
            period_s=5.0,
            phase_epoch_us=0.0,
            phase_offset_deg=0.0,
            sync_quality=0.8,
            sync_jitter_deg=3.0,
            last_sync_update_ts=0.0,
            source="multi_aircraft_burst",
            usable=True,
        )
        sync_dict = {
            "phase_basis": "geographic",
            "phase_is_absolute": False,
            "phase_offset_geographic_deg": None,
        }
        ok, reason = AircraftLocaliser._sync_state_has_geographic_phase(sync, sync_state_dict=sync_dict)
        assert ok is False
        assert reason == REASON_PHASE_ABSOLUTE_FALSE

    def test_geographic_with_absolute_but_null_offset_rejected(self):
        from radar.sync_models import LiveSyncState
        from radar.aircraft_localiser import AircraftLocaliser
        from radar.aircraft_localiser import REASON_PHASE_OFFSET_GEOGRAPHIC_UNAVAILABLE
        sync = LiveSyncState(
            iid=1,
            period_s=5.0,
            phase_epoch_us=0.0,
            phase_offset_deg=0.0,
            sync_quality=0.8,
            sync_jitter_deg=3.0,
            last_sync_update_ts=0.0,
            source="multi_aircraft_burst",
            usable=True,
        )
        sync_dict = {
            "phase_basis": "geographic",
            "phase_is_absolute": True,
            "phase_offset_geographic_deg": None,
        }
        ok, reason = AircraftLocaliser._sync_state_has_geographic_phase(sync, sync_state_dict=sync_dict)
        assert ok is False
        assert reason == REASON_PHASE_OFFSET_GEOGRAPHIC_UNAVAILABLE

    def test_geographic_with_absolute_and_finite_offset_passes(self):
        from radar.sync_models import LiveSyncState
        from radar.aircraft_localiser import AircraftLocaliser
        sync = LiveSyncState(
            iid=1,
            period_s=5.0,
            phase_epoch_us=0.0,
            phase_offset_deg=0.0,
            sync_quality=0.8,
            sync_jitter_deg=3.0,
            last_sync_update_ts=0.0,
            source="multi_aircraft_burst",
            usable=True,
        )
        sync_dict = {
            "phase_basis": "geographic",
            "phase_is_absolute": True,
            "phase_offset_geographic_deg": 45.0,
        }
        ok, reason = AircraftLocaliser._sync_state_has_geographic_phase(sync, sync_state_dict=sync_dict)
        assert ok is True
        assert reason is None

    def test_none_sync_rejected(self):
        from radar.aircraft_localiser import AircraftLocaliser
        ok, reason = AircraftLocaliser._sync_state_has_geographic_phase(None)
        assert ok is False
        assert reason == "phase_not_geographic"

    def test_unavailable_basis_rejected(self):
        from radar.aircraft_localiser import AircraftLocaliser
        from radar.aircraft_localiser import REASON_PHASE_NOT_GEOGRAPHIC
        sync_dict = {
            "phase_is_absolute": True,
            "phase_offset_geographic_deg": 50.0,
        }
        ok, reason = AircraftLocaliser._sync_state_has_geographic_phase(
            None, sync_state_dict=sync_dict
        )
        assert ok is False
        assert reason == REASON_PHASE_NOT_GEOGRAPHIC


class TestRecordedEventPhaseFields:
    def test_recorded_burst_event_has_event_phase_fields(self):
        from radar.sweep import _normalise_phase_fields
        sync_snapshot = _normalise_phase_fields({
            "phase_basis": "anchor_relative",
            "phase_is_absolute": False,
            "phase_authority": "py_anchor_relative",
            "phase_offset_deg": 123.4,
            "phase_offset_basis": "anchor_relative",
            "phase_offset_geographic_deg": None,
            "phase_anchor_icao": "ABCDEF",
            "phase_anchor_status": "selected",
            "phase_status": "trusted",
            "base_period_s": 5.0,
            "period_delta_s": 0.0,
            "effective_period_s": 5.0,
            "period_authority": "py_base",
            "sync_authority": "py_bootstrap",
            "phase_epoch_us": 1.0,
            "effective_period_source": "period_base_s",
            "period_delta_source": "none",
            "handoff_state": "BASE_PERIOD_READY",
            "handoff_reason": "bootstrap",
            "consistency_warnings": [],
        })
        event = {
            "event_id": "1:burst:ABCDEF:1000.000",
            "event_kind": "burst",
            "wall_ts": 1000.0,
            "arrival_beast_us": 1000.0,
            "beam_center_us": 1000.0,
            "iid": 1,
            "icao": "ABCDEF",
            "centroid_timestamp_us": 1000.0,
            "residual_deg": 5.0,
            "residual_basis": "runtime_effective",
            "display_residual_class": "burst_nominal",
            "classification": None,
            "timing_class": None,
            "bearing_deg": 50.0,
            "predicted_deg": 45.0,
            "range_nm": 10.0,
            "corrected_residual_deg": 5.0,
            "pos_age_s": 1.0,
            "aircraft_position_age_s": 1.0,
            "n_replies": 4,
            "signal_dbfs": -30.0,
            "sync_update_eligible": True,
            "fit_eligible": True,
            "dominant_family": True,
            "refinement_status": "stable",
            "reject_reason": None,
            "base_period_s": sync_snapshot.get("base_period_s"),
            "period_delta_s": sync_snapshot.get("period_delta_s"),
            "effective_period_s": sync_snapshot.get("effective_period_s"),
            "period_authority": sync_snapshot.get("period_authority"),
            "sync_authority": sync_snapshot.get("sync_authority"),
            "phase_authority": sync_snapshot.get("phase_authority"),
            "event_base_period_s": sync_snapshot.get("base_period_s"),
            "event_period_delta_s": sync_snapshot.get("period_delta_s"),
            "event_effective_period_s": sync_snapshot.get("effective_period_s"),
            "event_period_authority": sync_snapshot.get("period_authority"),
            "event_sync_authority": sync_snapshot.get("sync_authority"),
            "event_phase_authority": sync_snapshot.get("phase_authority"),
            "phase_basis": sync_snapshot.get("phase_basis"),
            "event_phase_basis": sync_snapshot.get("phase_basis"),
            "phase_is_absolute": bool(sync_snapshot.get("phase_is_absolute", False)),
            "event_phase_is_absolute": bool(sync_snapshot.get("phase_is_absolute", False)),
            "phase_offset_deg": float(sync_snapshot.get("phase_offset_deg") or 0.0),
            "event_phase_offset_deg": float(sync_snapshot.get("phase_offset_deg") or 0.0),
            "phase_offset_basis": sync_snapshot.get("phase_offset_basis"),
            "event_phase_offset_basis": sync_snapshot.get("phase_offset_basis"),
            "phase_offset_geographic_deg": sync_snapshot.get("phase_offset_geographic_deg"),
            "event_phase_offset_geographic_deg": sync_snapshot.get("phase_offset_geographic_deg"),
            "phase_anchor_icao": sync_snapshot.get("phase_anchor_icao"),
            "event_phase_anchor_icao": sync_snapshot.get("phase_anchor_icao"),
            "phase_anchor_status": sync_snapshot.get("phase_anchor_status"),
            "event_phase_anchor_status": sync_snapshot.get("phase_anchor_status"),
            "phase_epoch_us": float(sync_snapshot.get("phase_epoch_us") or 0.0),
            "effective_period_source": sync_snapshot.get("effective_period_source"),
            "period_delta_source": sync_snapshot.get("period_delta_source"),
            "source_path": "recorded_python_bootstrap",
            "recording_path_kind": "recorded_burst_alignment",
            "handoff_state": sync_snapshot.get("handoff_state"),
            "handoff_reason": sync_snapshot.get("handoff_reason"),
            "event_handoff_state": sync_snapshot.get("handoff_state"),
            "event_handoff_reason": sync_snapshot.get("handoff_reason"),
            "sync_revision": 0,
        }
        assert event["event_phase_basis"] == "anchor_relative"
        assert event["event_phase_is_absolute"] is False
        assert event["event_phase_offset_basis"] == "anchor_relative"
        assert event["event_phase_offset_geographic_deg"] is None
        assert event["event_phase_authority"] == "py_anchor_relative"
        assert event["event_phase_anchor_icao"] == "ABCDEF"
        assert event["event_phase_anchor_status"] == "selected"

    def test_recorded_event_shows_event_phase_different_from_current_phase(self):
        current_snapshot = {
            "phase_basis": "anchor_relative",
            "event_phase_basis": "sweep_epoch_only",
            "phase_is_absolute": False,
            "event_phase_is_absolute": False,
        }
        assert current_snapshot["phase_basis"] != current_snapshot["event_phase_basis"]


class TestRejectionReasonsImported:
    def test_rejection_reason_constants_available(self):
        from radar.aircraft_localiser import (
            REASON_PHASE_NOT_GEOGRAPHIC,
            REASON_PHASE_BASIS_SWEEP_EPOCH_ONLY,
            REASON_PHASE_BASIS_ANCHOR_RELATIVE,
            REASON_PHASE_ABSOLUTE_FALSE,
            REASON_PHASE_OFFSET_GEOGRAPHIC_UNAVAILABLE,
        )
        assert REASON_PHASE_NOT_GEOGRAPHIC == "phase_not_geographic"
        assert REASON_PHASE_BASIS_SWEEP_EPOCH_ONLY == "phase_basis_sweep_epoch_only"
        assert REASON_PHASE_BASIS_ANCHOR_RELATIVE == "phase_basis_anchor_relative"
        assert REASON_PHASE_ABSOLUTE_FALSE == "phase_absolute_false"
        assert REASON_PHASE_OFFSET_GEOGRAPHIC_UNAVAILABLE == "phase_offset_geographic_unavailable"

    def test_all_rejection_reasons_are_strings(self):
        from radar.aircraft_localiser import (
            REASON_PHASE_NOT_GEOGRAPHIC,
            REASON_PHASE_BASIS_SWEEP_EPOCH_ONLY,
            REASON_PHASE_BASIS_ANCHOR_RELATIVE,
            REASON_PHASE_ABSOLUTE_FALSE,
            REASON_PHASE_OFFSET_GEOGRAPHIC_UNAVAILABLE,
        )
        for reason in [
            REASON_PHASE_NOT_GEOGRAPHIC,
            REASON_PHASE_BASIS_SWEEP_EPOCH_ONLY,
            REASON_PHASE_BASIS_ANCHOR_RELATIVE,
            REASON_PHASE_ABSOLUTE_FALSE,
            REASON_PHASE_OFFSET_GEOGRAPHIC_UNAVAILABLE,
        ]:
            assert isinstance(reason, str)
            assert len(reason) > 0