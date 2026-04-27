"""Stage 3 aircraft localiser: target-aircraft live-ray behaviour.

These tests cover the operational guarantees of the refactored localiser:

    * Exactly one authoritative live bearing ray per contributing radar.
    * Strict freshness gating (wall-clock and radar-period multiples).
    * Display ray set == solver input set.
    * Seed generation only accepts forward-ray intersections.
    * Stale observations surface as rejected rays with a reason, not as
      silent fallbacks into the accepted set.
    * Track fix history is exposed in the evidence payload.
"""

from __future__ import annotations

import os
import sys
from types import SimpleNamespace

import pytest

sys.path.insert(0, os.path.dirname(os.path.dirname(__file__)))

from radar.aircraft_localiser import (
    AircraftLocaliser,
    REASON_ABSOLUTE_PHASE_UNTRUSTED,
    REASON_BEARING_TRUTH_MISMATCH,
    REASON_NO_FORWARD_INTERSECTIONS,
    REASON_NO_ELIGIBLE_RADARS,
    REASON_NO_SYNC,
    REASON_STALE_OBSERVATION,
    REASON_SYNC_QUALITY_LOW,
    _ray_intersection_enu,
    _select_best_live_ray_per_radar,
    generate_seeds,
)
from radar.aircraft_models import (
    AircraftFix,
    AircraftTrackState,
    RadarBearingCalibration,
    RadarBearingObservation,
    Stage3LiveDetection,
    Stage3LiveRay,
)
from radar.sweep import LiveSyncState


# ---------------------------------------------------------------------------
# Fakes
# ---------------------------------------------------------------------------

def _radar_iid(*, period_s: float = 10.0, lat: float, lon: float):
    """Minimal RadarIID-shaped object for the authoritative-position helper."""
    return SimpleNamespace(
        period_s=period_s,
        resolution_mode="locked_position",
        manual_lat=lat,
        manual_lon=lon,
        ci_lat=None, ci_lon=None, ci_cep_m=None,
        fm_lat=None, fm_lon=None, fm_cep_m=None,
        lat=None, lon=None, cep_m=None,
        multi_radar_flag=False,
        status="SINGLE_RADAR",
    )


def _sync(
    iid: int,
    *,
    usable: bool = True,
    period_s: float = 10.0,
    phase_status: str = "trusted",
    phase_anchor_status: str = "selected",
    phase_anchor_icao: str | None = "TEST01",
    phase_validation_status: str = "ok",
    phase_anchor_spread_deg: float | None = None,
    source: str = "multi_aircraft_burst",
) -> LiveSyncState:
    return LiveSyncState(
        iid=iid,
        period_s=period_s,
        phase_epoch_us=0.0,
        phase_offset_deg=0.0,
        sync_quality=1.0,
        sync_jitter_deg=2.0,
        last_sync_update_ts=0.0,
        source=source,
        usable=usable,
        phase_status=phase_status,
        phase_anchor_status=phase_anchor_status,
        phase_anchor_icao=phase_anchor_icao,
        phase_validation_status=phase_validation_status,
        phase_anchor_spread_deg=phase_anchor_spread_deg,
    )


def _cal(iid: int) -> RadarBearingCalibration:
    return RadarBearingCalibration(
        iid=iid,
        bearing_offset_deg=0.0,
        effective_delay_us=0.0,
        bearing_sigma_deg=1.5,
        n_samples=60,
        quality="stable",
        last_calibrated_ts=0.0,
    )


class FakeRadarState:
    def __init__(self, models: dict, syncs: dict, detections_by_icao: dict):
        self._models = models
        self._syncs = syncs
        self._detections = detections_by_icao

    def get_all_rotation_models(self):
        return dict(self._models)

    def get_all_live_sync_states(self):
        return dict(self._syncs)

    def get_live_sync_state(self, iid):
        return self._syncs.get(iid)

    def get_all_stage3_live_sync_states(self):
        return {
            iid: sync
            for iid, sync in self._syncs.items()
            if getattr(sync, "source", None) == "multi_aircraft_burst"
        }

    def get_stage3_live_sync_state(self, iid):
        sync = self._syncs.get(iid)
        if sync is None or getattr(sync, "source", None) != "multi_aircraft_burst":
            return None
        return sync

    def get_live_waveform_bins(self, iid):
        return []

    def get_stage3_live_waveform_bins(self, iid):
        sync = self._syncs.get(iid)
        if sync is None or getattr(sync, "source", None) != "multi_aircraft_burst":
            return []
        return []

    def get_recent_live_detections_for_icao(self, icao, max_age_s: float = 30.0):
        # Newest-first, matching the real implementation.
        dets = list(self._detections.get(icao, []))
        return sorted(dets, key=lambda d: d.wall_ts, reverse=True)


def _make_localiser(radar_state):
    loc = AircraftLocaliser(
        radar_state=radar_state,
        aircraft_state=None,
        receiver_lat=51.0,
        receiver_lon=-1.0,
    )
    for iid in radar_state.get_all_rotation_models():
        loc._calibrations[iid] = _cal(iid)
    return loc


# ---------------------------------------------------------------------------
# Forward-ray geometry
# ---------------------------------------------------------------------------

def test_ray_intersection_rejects_backward_intersection():
    # Both rays point east (90°); the only meeting point is at infinity.
    # With a small bearing difference, the intersection lies far behind
    # the second radar — must be rejected.
    pt = _ray_intersection_enu(0.0, 0.0, 45.0, 1000.0, 0.0, 135.0)
    # Ray 1 from (0,0) points NE; ray 2 from (1000,0) points SE.
    # They diverge — intersection is behind one origin, so None.
    assert pt is None


def test_ray_intersection_accepts_forward_intersection():
    # Ray 1 from (0,0) points east (90°); ray 2 from (0,1000) points SE (135°).
    # They converge forward.
    pt = _ray_intersection_enu(0.0, 0.0, 90.0, 0.0, 1000.0, 135.0)
    assert pt is not None
    x, y = pt
    assert x > 0.0


def test_generate_seeds_filters_backward_cases():
    # Two radars, both looking away from each other — no forward intersection.
    obs_a = RadarBearingObservation(
        iid=1, icao="A", arrival_us=0, phase_deg=0, bearing_obs_deg=270.0,
        bearing_sigma_deg=1.0, radar_lat=51.0, radar_lon=-1.0,
        receiver_lat=51.0, receiver_lon=-1.0, association_confidence=1.0,
    )
    obs_b = RadarBearingObservation(
        iid=2, icao="A", arrival_us=0, phase_deg=0, bearing_obs_deg=90.0,
        bearing_sigma_deg=1.0, radar_lat=51.0, radar_lon=-0.9,
        receiver_lat=51.0, receiver_lon=-1.0, association_confidence=1.0,
    )
    seeds = generate_seeds([obs_a, obs_b])
    assert seeds == []


# ---------------------------------------------------------------------------
# Authoritative selection
# ---------------------------------------------------------------------------

def _det(
    iid: int, icao: str, wall_ts: float, *,
    arrival_us: float | None = None,
    truth_lat: float | None = None,
    truth_lon: float | None = None,
    position_age_seconds: float = 0.2,
) -> Stage3LiveDetection:
    return Stage3LiveDetection(
        iid=iid,
        icao=icao,
        arrival_us=arrival_us if arrival_us is not None else wall_ts * 1e6,
        wall_ts=wall_ts,
        df=11,
        signal_dbfs=-20.0,
        receiver_lat=51.0,
        receiver_lon=-1.0,
        truth_lat=truth_lat,
        truth_lon=truth_lon,
        position_age_seconds=position_age_seconds,
        association_confidence=1.0,
    )


def test_one_observation_per_radar_newest_wins():
    now = __import__("time").time()
    # Two radars each with three detections, differing wall_ts.
    models = {
        1: _radar_iid(lat=51.05, lon=-1.05),
        2: _radar_iid(lat=51.10, lon=-0.95),
    }
    syncs = {1: _sync(1), 2: _sync(2)}
    det_by_icao = {
        "ABC": [
            _det(1, "ABC", now - 4.0),
            _det(1, "ABC", now - 2.0),
            _det(1, "ABC", now - 0.5, arrival_us=999_999_999.0),
            _det(2, "ABC", now - 3.0),
            _det(2, "ABC", now - 1.0, arrival_us=888_888_888.0),
        ],
    }
    radar_state = FakeRadarState(models, syncs, det_by_icao)
    loc = _make_localiser(radar_state)

    sel = loc.select_authoritative_observations("ABC", None, now)
    accepted = sel["accepted"]
    assert len(accepted) == 2
    by_iid = {o.iid: o for o in accepted}
    assert by_iid[1].arrival_us == 999_999_999.0
    assert by_iid[2].arrival_us == 888_888_888.0
    assert sel["per_radar_reasons"] == {}


def test_stage3_selection_ignores_go_frame_sync_states():
    now = __import__("time").time()
    models = {1: _radar_iid(lat=51.05, lon=-1.05)}
    syncs = {1: _sync(1, source="sweep_frame_go")}
    det_by_icao = {
        "ABC": [
            _det(1, "ABC", now - 0.5, arrival_us=999_999.0),
        ],
    }
    radar_state = FakeRadarState(models, syncs, det_by_icao)
    loc = _make_localiser(radar_state)

    sel = loc.select_authoritative_observations("ABC", None, now)

    assert sel["accepted"] == []
    assert sel["per_radar_reasons"][1] == REASON_ABSOLUTE_PHASE_UNTRUSTED
    assert radar_state.get_stage3_live_waveform_bins(1) == []


def test_stale_observation_produces_rejected_ray_not_accepted():
    now = __import__("time").time()
    # Only-stale-for-this-radar detections: 20 s old, well beyond the 5 s gate.
    models = {
        1: _radar_iid(lat=51.05, lon=-1.05),
        2: _radar_iid(lat=51.10, lon=-0.95),
    }
    syncs = {1: _sync(1, period_s=3.0), 2: _sync(2, period_s=3.0)}
    det_by_icao = {
        "ABC": [
            _det(1, "ABC", now - 20.0),  # stale
            _det(2, "ABC", now - 0.2),   # fresh
        ],
    }
    loc = _make_localiser(FakeRadarState(models, syncs, det_by_icao))

    sel = loc.select_authoritative_observations("ABC", None, now)
    assert {o.iid for o in sel["accepted"]} == {2}
    assert sel["per_radar_reasons"].get(1) == REASON_STALE_OBSERVATION
    assert len(sel["rejected_rays"]) == 1
    assert sel["rejected_rays"][0].rejection_reason == REASON_STALE_OBSERVATION


def test_display_set_matches_solver_input_set():
    now = __import__("time").time()
    models = {
        1: _radar_iid(lat=51.05, lon=-1.05),
        2: _radar_iid(lat=51.10, lon=-0.95),
    }
    syncs = {1: _sync(1), 2: _sync(2)}
    det_by_icao = {
        "ABC": [
            _det(1, "ABC", now - 0.6),
            _det(1, "ABC", now - 0.2),   # newest for IID 1
            _det(2, "ABC", now - 0.3),
        ],
    }
    loc = _make_localiser(FakeRadarState(models, syncs, det_by_icao))
    loc._solve_for_icao("ABC")

    current_rays = loc._current_rays_by_icao.get("ABC", [])
    # Display layer: exactly one ray per radar.
    assert len({r.iid for r in current_rays}) == len(current_rays)
    # And arrival_us for IID 1 must be the newest (wall_ts=now-0.2 → arrival_us match).
    by_iid = {r.iid: r for r in current_rays}
    assert by_iid[1].arrival_us == pytest.approx((now - 0.2) * 1e6)


def test_no_mutation_of_shared_sync_jitter():
    now = __import__("time").time()
    models = {
        1: _radar_iid(lat=51.05, lon=-1.05),
        2: _radar_iid(lat=51.10, lon=-0.95),
    }
    syncs = {1: _sync(1), 2: _sync(2)}
    original_jitters = {iid: s.sync_jitter_deg for iid, s in syncs.items()}
    det_by_icao = {
        "ABC": [
            _det(1, "ABC", now - 0.2),
            _det(2, "ABC", now - 0.2),
        ],
    }
    loc = _make_localiser(FakeRadarState(models, syncs, det_by_icao))
    loc._solve_for_icao("ABC")
    for iid, s in syncs.items():
        assert s.sync_jitter_deg == original_jitters[iid]


# ---------------------------------------------------------------------------
# Evidence / rejection population
# ---------------------------------------------------------------------------

def test_rejected_ray_buffer_populated_when_only_stale():
    now = __import__("time").time()
    models = {
        1: _radar_iid(lat=51.05, lon=-1.05),
        2: _radar_iid(lat=51.10, lon=-0.95),
    }
    syncs = {1: _sync(1), 2: _sync(2)}
    det_by_icao = {
        "ABC": [
            _det(1, "ABC", now - 30.0),
            _det(2, "ABC", now - 30.0),
        ],
    }
    loc = _make_localiser(FakeRadarState(models, syncs, det_by_icao))
    fix = loc._solve_for_icao("ABC")

    assert fix is None
    assert len(loc._current_rays_by_icao.get("ABC", [])) == 0
    rejected = loc._current_rejected_rays_by_icao.get("ABC", [])
    assert len(rejected) == 2
    assert all(r.rejection_reason == REASON_STALE_OBSERVATION for r in rejected)
    reasons = loc._last_rejection_reasons_by_icao["ABC"]
    assert reasons["global"] is not None


def test_fix_history_exposed_in_evidence():
    now = __import__("time").time()
    models = {
        1: _radar_iid(lat=51.05, lon=-1.05),
        2: _radar_iid(lat=51.10, lon=-0.95),
    }
    syncs = {1: _sync(1), 2: _sync(2)}
    loc = _make_localiser(FakeRadarState(models, syncs, {"ABC": []}))

    # Seed a synthetic track with multiple fixes.
    hist = [
        AircraftFix(
            track_id="ABC", lat=51.0 + i * 0.01, lon=-1.0 + i * 0.01,
            alt_ft=None, cep_m=500.0, geometry_score=0.8,
            n_radars=2, n_observations=2, solver_status="ok",
            solver_detail={}, ts=now - (3 - i),
        )
        for i in range(3)
    ]
    track = AircraftTrackState(
        track_id="ABC", lat=hist[-1].lat, lon=hist[-1].lon,
        vx_mps=0.0, vy_mps=0.0, alt_ft=None,
        position_covariance=[1.0, 0.0, 0.0, 1.0],
        last_update_ts=now, source="stage3", history=hist,
    )
    loc._tracks["ABC"] = track

    ev = loc.build_evidence("ABC")
    assert ev["available"] is True
    history_layer = next(l for l in ev["layers"] if l["method"] == "fix_history")
    # LineString + up to 10 points.
    assert history_layer["source_count"] >= 3


def test_seed_failure_reason_recorded():
    now = __import__("time").time()
    # Two radars at the same location — any bearing pair yields zero base
    # offset and near-parallel / no forward intersection for wide azimuths.
    models = {
        1: _radar_iid(lat=51.0, lon=-1.0),
        2: _radar_iid(lat=51.0, lon=-1.0),
    }
    syncs = {1: _sync(1), 2: _sync(2)}
    det_by_icao = {
        "ABC": [
            _det(1, "ABC", now - 0.2, truth_lat=51.05, truth_lon=-0.95),
            # Make IID 2 bearing point the opposite way so rays cannot meet forward.
            _det(2, "ABC", now - 0.2, truth_lat=50.95, truth_lon=-1.05),
        ],
    }
    loc = _make_localiser(FakeRadarState(models, syncs, det_by_icao))
    fix = loc._solve_for_icao("ABC")
    # Coincident radars — either no forward intersection or insufficient radars
    # worth of geometry; in either case the failure reason must be recorded.
    reasons = loc._last_rejection_reasons_by_icao["ABC"]
    assert fix is None
    assert reasons["global"] is not None


def test_collapse_display_selector_unique_per_iid():
    rays = [
        Stage3LiveRay(
            track_id="ABC", icao="ABC", iid=1, ts=0.0,
            radar_lat=51.0, radar_lon=-1.0,
            bearing_deg=90.0, bearing_sigma_deg=1.0, accepted=True,
            arrival_us=1.0, association_confidence=1.0,
        ),
        Stage3LiveRay(
            track_id="ABC", icao="ABC", iid=1, ts=0.0,
            radar_lat=51.0, radar_lon=-1.0,
            bearing_deg=91.0, bearing_sigma_deg=1.0, accepted=True,
            arrival_us=2.0, association_confidence=1.0,
        ),
        Stage3LiveRay(
            track_id="ABC", icao="ABC", iid=2, ts=0.0,
            radar_lat=51.0, radar_lon=-0.9,
            bearing_deg=180.0, bearing_sigma_deg=1.0, accepted=True,
            arrival_us=1.5, association_confidence=1.0,
        ),
    ]
    out = _select_best_live_ray_per_radar(rays)
    assert len(out) == 2
    by_iid = {r.iid: r for r in out}
    assert by_iid[1].arrival_us == 2.0


# ---------------------------------------------------------------------------
# Fail-closed: absolute phase untrusted
# ---------------------------------------------------------------------------

def test_untrusted_absolute_phase_rejected():
    now = __import__("time").time()
    models = {1: _radar_iid(lat=51.0, lon=-1.0)}
    # phase_status not "trusted" → gate rejects
    syncs = {1: _sync(1, phase_status="untrusted")}
    det_by_icao = {"XYZ": [_det(1, "XYZ", now - 0.2)]}
    loc = _make_localiser(FakeRadarState(models, syncs, det_by_icao))

    sel = loc.select_authoritative_observations("XYZ", None, now)
    assert sel["accepted"] == []
    assert sel["per_radar_reasons"].get(1) == REASON_ABSOLUTE_PHASE_UNTRUSTED


def test_provisional_phase_rejected():
    now = __import__("time").time()
    models = {1: _radar_iid(lat=51.0, lon=-1.0)}
    syncs = {1: _sync(1, phase_status="provisional")}
    det_by_icao = {"XYZ": [_det(1, "XYZ", now - 0.2)]}
    loc = _make_localiser(FakeRadarState(models, syncs, det_by_icao))

    sel = loc.select_authoritative_observations("XYZ", None, now)
    assert sel["accepted"] == []
    assert sel["per_radar_reasons"].get(1) == REASON_ABSOLUTE_PHASE_UNTRUSTED


def test_absent_phase_status_rejected():
    now = __import__("time").time()
    models = {1: _radar_iid(lat=51.0, lon=-1.0)}
    # No phase_status field → defaults to "untrusted"
    syncs = {1: _sync(1, phase_status=None)}
    det_by_icao = {"XYZ": [_det(1, "XYZ", now - 0.2)]}
    loc = _make_localiser(FakeRadarState(models, syncs, det_by_icao))

    sel = loc.select_authoritative_observations("XYZ", None, now)
    assert sel["accepted"] == []
    assert sel["per_radar_reasons"].get(1) == REASON_ABSOLUTE_PHASE_UNTRUSTED


def test_untrusted_phase_wrong_source():
    now = __import__("time").time()
    models = {1: _radar_iid(lat=51.0, lon=-1.0)}
    syncs = {1: _sync(1, source="sweep_frame")}
    det_by_icao = {"XYZ": [_det(1, "XYZ", now - 0.2)]}
    loc = _make_localiser(FakeRadarState(models, syncs, det_by_icao))

    sel = loc.select_authoritative_observations("XYZ", None, now)
    assert sel["accepted"] == []
    assert sel["per_radar_reasons"].get(1) == REASON_ABSOLUTE_PHASE_UNTRUSTED


# ---------------------------------------------------------------------------
# Fail-closed: bearing-truth mismatch
# ---------------------------------------------------------------------------

def test_bearing_truth_mismatch_rejected():
    now = __import__("time").time()
    # Radar at (51.0, -1.0). Truth at (51.1, -1.0) → bearing ~0° (north).
    # With phase_epoch_us=0, phase_offset_deg=0, period_s=10.0:
    #   arrival_us = 2.5e6 → phase = 90° → predicted bearing = 90° (east).
    # Error = |wrap(90° - 0°)| = 90° > 45° → mismatch.
    models = {1: _radar_iid(lat=51.0, lon=-1.0)}
    syncs = {1: _sync(1, period_s=10.0)}
    det = Stage3LiveDetection(
        iid=1,
        icao="XYZ",
        arrival_us=2_500_000.0,   # quarter-period → 90° phase
        wall_ts=now - 0.2,
        df=11,
        signal_dbfs=-20.0,
        receiver_lat=51.0,
        receiver_lon=-1.0,
        truth_lat=51.1,           # due north of radar
        truth_lon=-1.0,
        position_age_seconds=0.2,
        association_confidence=1.0,
    )
    det_by_icao = {"XYZ": [det]}
    loc = _make_localiser(FakeRadarState(models, syncs, det_by_icao))

    sel = loc.select_authoritative_observations("XYZ", None, now)
    assert sel["accepted"] == []
    assert sel["per_radar_reasons"].get(1) == REASON_BEARING_TRUTH_MISMATCH
    assert len(sel["rejected_rays"]) == 1
    assert sel["rejected_rays"][0].rejection_reason == REASON_BEARING_TRUTH_MISMATCH
    assert sel["rejected_rays"][0].iid == 1


def test_bearing_truth_mismatch_not_applied_to_stale_truth():
    now = __import__("time").time()
    # Same 90° mismatch setup as above but position_age_seconds > 3.0 → gate skipped.
    models = {1: _radar_iid(lat=51.0, lon=-1.0)}
    syncs = {1: _sync(1, period_s=10.0)}
    det = Stage3LiveDetection(
        iid=1,
        icao="XYZ",
        arrival_us=2_500_000.0,
        wall_ts=now - 0.2,
        df=11,
        signal_dbfs=-20.0,
        receiver_lat=51.0,
        receiver_lon=-1.0,
        truth_lat=51.1,
        truth_lon=-1.0,
        position_age_seconds=5.0,  # stale truth → gate bypassed
        association_confidence=1.0,
    )
    det_by_icao = {"XYZ": [det]}
    loc = _make_localiser(FakeRadarState(models, syncs, det_by_icao))

    sel = loc.select_authoritative_observations("XYZ", None, now)
    # Should be accepted because position_age is too old for the gate to fire.
    assert len(sel["accepted"]) == 1
    assert sel["per_radar_reasons"] == {}


# ---------------------------------------------------------------------------
# Global rejection reason derivation
# ---------------------------------------------------------------------------

def test_global_reason_prefers_real_failure_not_no_eligible_radars():
    # All stale → reason should be stale, not no_eligible_radars
    reasons_stale = {1: REASON_STALE_OBSERVATION, 2: REASON_STALE_OBSERVATION}
    assert AircraftLocaliser._derive_global_rejection_reason(reasons_stale) == REASON_STALE_OBSERVATION

    # Mixed with REASON_ABSOLUTE_PHASE_UNTRUSTED → that wins over stale
    reasons_mixed = {
        1: REASON_STALE_OBSERVATION,
        2: REASON_ABSOLUTE_PHASE_UNTRUSTED,
    }
    assert AircraftLocaliser._derive_global_rejection_reason(reasons_mixed) == REASON_ABSOLUTE_PHASE_UNTRUSTED

    # With REASON_BEARING_TRUTH_MISMATCH → that wins over everything
    reasons_mismatch = {
        1: REASON_STALE_OBSERVATION,
        2: REASON_ABSOLUTE_PHASE_UNTRUSTED,
        3: REASON_BEARING_TRUTH_MISMATCH,
    }
    assert AircraftLocaliser._derive_global_rejection_reason(reasons_mismatch) == REASON_BEARING_TRUTH_MISMATCH

    # Empty → REASON_NO_ELIGIBLE_RADARS
    assert AircraftLocaliser._derive_global_rejection_reason({}) == REASON_NO_ELIGIBLE_RADARS

    # Single sync-quality-low reason
    assert AircraftLocaliser._derive_global_rejection_reason(
        {1: REASON_SYNC_QUALITY_LOW}
    ) == REASON_SYNC_QUALITY_LOW


def test_solve_for_icao_global_reason_uses_derive():
    now = __import__("time").time()
    # Both radars stale → global reason should be stale_observation, not no_eligible_radars
    models = {
        1: _radar_iid(lat=51.05, lon=-1.05),
        2: _radar_iid(lat=51.10, lon=-0.95),
    }
    syncs = {1: _sync(1, period_s=3.0), 2: _sync(2, period_s=3.0)}
    det_by_icao = {
        "ABC": [
            _det(1, "ABC", now - 20.0),
            _det(2, "ABC", now - 20.0),
        ],
    }
    loc = _make_localiser(FakeRadarState(models, syncs, det_by_icao))
    fix = loc._solve_for_icao("ABC")
    assert fix is None
    reasons = loc._last_rejection_reasons_by_icao["ABC"]
    assert reasons["global"] == REASON_STALE_OBSERVATION
