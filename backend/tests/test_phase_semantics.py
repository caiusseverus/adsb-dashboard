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