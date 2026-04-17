import asyncio
import os
import sys
import time
from collections import deque
from types import SimpleNamespace

import pytest

sys.path.insert(0, os.path.dirname(os.path.dirname(__file__)))

from radar import api as radar_api
from radar.models import RadarIID, SweepFrame, SweepFrameObservation
from radar.sweep import AlignedBurstSyncObs, LiveSyncState, RadarState


def test_get_iids_reports_per_iid_last_seen_from_event_times():
    state = RadarState()
    state._iid_events = deque([
        (1_000_000, 10, "AAAAAA", None),
        (1_500_000, 11, "BBBBBB", None),
        (3_000_000, 10, "CCCCCC", None),
    ])
    state._models = {
        10: RadarIID(iid=10, status="SINGLE_RADAR", period_s=4.0),
        11: RadarIID(iid=11, status="CHECK_MULTI", period_s=5.0),
    }

    prior_state = radar_api._state
    radar_api._state = state
    start = time.time()
    try:
        payload = asyncio.run(radar_api.get_iids(window_s=600.0))
    finally:
        radar_api._state = prior_state

    assert payload["window_s"] == 600.0
    rows = {row["iid"]: row for row in payload["iids"]}

    assert rows[10]["latest_icao"] == "CCCCCC"
    assert rows[11]["latest_icao"] == "BBBBBB"
    assert rows[10]["last_seen"] > rows[11]["last_seen"]

    # IID 11's latest event is 1.5 s older than IID 10's latest event.
    observed_delta = rows[10]["last_seen"] - rows[11]["last_seen"]
    assert observed_delta == 1.5

    # Sanity check that the values are anchored to the current wall clock.
    assert rows[10]["last_seen"] <= round(time.time(), 1)
    assert rows[10]["last_seen"] >= round(start - 0.5, 1)


def test_get_iid_timeline_includes_primary_harmonic_and_residual_classification():
    state = RadarState()
    state._iid_events = deque([
        (1_000_000, 21, "AAAAAA", None),
        (5_000_000, 21, "AAAAAA", None),
        (1_500_000, 21, "BBBBBB", None),
        (9_500_000, 21, "BBBBBB", None),
        (2_000_000, 21, "CCCCCC", None),
        (4_000_000, 21, "CCCCCC", None),
    ])
    state._models = {
        21: RadarIID(
            iid=21,
            status="CHECK_MULTI",
            period_s=4.0,
            rotation_model=SimpleNamespace(
                folded={
                    "AAAAAA": {"multiplier": 1, "method": "median"},
                    "BBBBBB": {"multiplier": 2, "method": "median"},
                },
                residual={"CCCCCC": 2.0},
            ),
        ),
    }

    prior_state = radar_api._state
    radar_api._state = state
    try:
        payload = asyncio.run(radar_api.get_iid_timeline(21, window_s=600.0))
    finally:
        radar_api._state = prior_state

    rows = {row["icao"]: row for row in payload["icaos"]}
    assert rows["AAAAAA"]["classification"] == "primary"
    assert rows["AAAAAA"]["multiplier"] == 1
    assert rows["AAAAAA"]["method"] == "median"
    assert [series["family"] for series in rows["AAAAAA"]["family_series"]] == ["primary"]
    assert rows["BBBBBB"]["classification"] == "primary_harmonic"
    assert rows["BBBBBB"]["multiplier"] == 2
    assert [series["family"] for series in rows["BBBBBB"]["family_series"]] == ["primary_harmonic"]
    assert rows["CCCCCC"]["classification"] == "residual"
    assert rows["CCCCCC"]["multiplier"] is None
    assert [series["family"] for series in rows["CCCCCC"]["family_series"]] == ["residual"]


def test_get_iid_sync_debug_endpoint_exposes_summary_and_observation(monkeypatch):
    state = RadarState()
    now_ts = 1_000.0
    state._iid_latest_arrival_us[23] = 4_100_000.0
    state._live_sync_states[23] = LiveSyncState(
        iid=23,
        period_s=4.0,
        phase_epoch_us=0.0,
        phase_offset_deg=0.0,
        sync_quality=1.0,
        sync_jitter_deg=3.0,
        last_sync_update_ts=now_ts,
        source="multi_aircraft_burst",
        usable=True,
    )
    state._live_burst_timeline_obs[23] = deque([
        AlignedBurstSyncObs(
            burst_centroid_us=4_100_000.0,
            icao="AAAAAA",
            bearing_deg=9.0,
            n_replies=4,
            signal_dbfs=-18.0,
            pos_age_s=0.4,
            range_nm=12.0,
            ts=now_ts,
            sync_update_eligible=True,
            raw_arrival_us=4_100_000.0,
        )
    ], maxlen=state._BURST_SYNC_TIMELINE_OBS_MAX)

    prior_state = radar_api._state
    radar_api._state = state
    try:
        monkeypatch.setattr("radar.sweep.time.time", lambda: now_ts)
        payload = asyncio.run(radar_api.get_iid_sync_debug(23, window_s=60.0, limit=20))
    finally:
        radar_api._state = prior_state

    assert payload["available"] is True
    assert payload["summary"]["wall_clock_used_operationally"] is False
    assert payload["summary"]["predictors_consistent_burst_sync"] is True
    assert "motion_comp_applied_count" in payload["summary"]
    assert "period_update_proposed_us" in payload["summary"]
    assert "period_update_block_reason" in payload["summary"]
    assert "period_update_clamp_reason" in payload["summary"]
    assert "period_correction_status" in payload["summary"]
    assert payload["observations"][0]["iid"] == 23
    assert "pred_position_verification_deg" in payload["observations"][0]
    assert "pred_using_wall_clock_deg" in payload["observations"][0]
    assert "motion_comp_improvement_deg" in payload["observations"][0]
    assert "residual_detrended_deg" in payload["observations"][0]
    assert "phase_deg" in payload["observations"][0]
    assert "cycle_index" in payload["observations"][0]
    assert "observation_model_diagnostics" in payload
    assert payload["observation_model_diagnostics"]["folded_phase_shape"]["phase_bins"]
    assert payload["summary"]["dominant_error_mode"] in {
        "period_drift",
        "repeatable_phase_shape",
        "unstable_cycle_shape",
        "mixed",
    }
    assert payload["summary"]["operational_burst_timestamp_method"] is not None


def test_get_iid_sync_snapshot_endpoint_combines_fast_sync_payloads(monkeypatch):
    state = RadarState()
    now_ts = 1_000.0
    state._models[23] = RadarIID(iid=23, status="SINGLE_RADAR", period_s=4.0)
    state._iid_latest_arrival_us[23] = 4_100_000.0
    state._live_sync_states[23] = LiveSyncState(
        iid=23,
        period_s=4.0,
        phase_epoch_us=0.0,
        phase_offset_deg=0.0,
        sync_quality=1.0,
        sync_jitter_deg=3.0,
        last_sync_update_ts=now_ts,
        source="multi_aircraft_burst",
        usable=True,
    )
    state._live_burst_timeline_obs[23] = deque([
        AlignedBurstSyncObs(
            burst_centroid_us=4_100_000.0,
            icao="AAAAAA",
            bearing_deg=9.0,
            n_replies=4,
            signal_dbfs=-18.0,
            pos_age_s=0.4,
            range_nm=12.0,
            ts=now_ts,
            sync_update_eligible=True,
            raw_arrival_us=4_100_000.0,
        )
    ], maxlen=state._BURST_SYNC_TIMELINE_OBS_MAX)

    prior_state = radar_api._state
    radar_api._state = state
    try:
        monkeypatch.setattr("radar.sweep.time.time", lambda: now_ts)
        payload = asyncio.run(radar_api.get_iid_sync_snapshot(23, window_s=60.0, debug_limit=20))
    finally:
        radar_api._state = prior_state

    assert payload["type"] == "radar_sync"
    assert payload["sequence"] >= 1
    assert payload["rotation"]["iid"] == 23
    assert "observations" in payload
    assert "sync_state" in payload
    assert "sync_debug" in payload
    assert payload["sync_debug"]["summary"]["operational_time_basis"] == "effective_beast_us"


def test_get_iid_timeline_marks_non_primary_family_points_as_residual():
    state = RadarState()
    state._iid_events = deque([
        (1_000_000, 21, "AAAAAA", None),
        (5_000_000, 21, "AAAAAA", None),
        (2_000_000, 21, "BBBBBB", None),
        (7_000_000, 21, "BBBBBB", None),
    ])
    state._models = {
        21: RadarIID(
            iid=21,
            status="CHECK_MULTI",
            period_s=4.0,
            rotation_model=SimpleNamespace(
                folded={"AAAAAA": {"multiplier": 1, "method": "median"}},
                secondary_folded={},
                residual={"BBBBBB": 5.0},
            ),
        ),
    }

    prior_state = radar_api._state
    radar_api._state = state
    try:
        payload = asyncio.run(radar_api.get_iid_timeline(21, window_s=600.0))
    finally:
        radar_api._state = prior_state

    rows = {row["icao"]: row for row in payload["icaos"]}
    assert payload["secondary_period_s"] is None
    assert rows["AAAAAA"]["classification"] == "primary"
    assert [series["family"] for series in rows["AAAAAA"]["family_series"]] == ["primary"]
    assert rows["BBBBBB"]["classification"] == "residual"
    assert [series["family"] for series in rows["BBBBBB"]["family_series"]] == ["residual"]


def test_get_iid_timeline_keeps_primary_points_primary_when_row_has_secondary_series():
    state = RadarState()
    state._iid_events = deque([
        (1_000_000, 21, "AAAAAA", None),
        (5_000_000, 21, "AAAAAA", None),
        (9_000_000, 21, "AAAAAA", None),
        (2_000_000, 21, "AAAAAA", None),
        (7_000_000, 21, "AAAAAA", None),
    ])
    state._models = {
        21: RadarIID(
            iid=21,
            status="CHECK_MULTI",
            period_s=4.0,
            rotation_model=SimpleNamespace(
                folded={"AAAAAA": {"multiplier": 1, "method": "median"}},
                secondary_folded={},
                residual={"AAAAAA": 5.0},
            ),
        ),
    }

    prior_state = radar_api._state
    radar_api._state = state
    try:
        payload = asyncio.run(radar_api.get_iid_timeline(21, window_s=600.0))
    finally:
        radar_api._state = prior_state

    row = payload["icaos"][0]
    assert row["classification"] == "primary"
    families = [series["family"] for series in row["family_series"]]
    assert families == ["primary", "residual"]
    assert row["family_series"][0]["arrivals_us"] == [1_000_000, 5_000_000, 9_000_000]
    assert row["family_series"][1]["arrivals_us"] == [2_000_000, 7_000_000]


def test_get_iid_rotation_reports_reinforced_confidence():
    state = RadarState()
    state._models = {
        6: RadarIID(
            iid=6,
            status="LIKELY_SINGLE",
            period_s=4.0,
            period_std_s=0.02,
            rpm=15.0,
            primary_support_count=6,
            secondary_support_count=0,
            rotation_model=SimpleNamespace(
                status="CHECK_MULTI",
                dominant_period_s=4.02,
                secondary_period_s=None,
                period_std_s=0.03,
                rpm=14.9,
                n_qualifying=5,
                n_harmonic=1,
                n_residual=1,
                last_updated=123.0,
            ),
        ),
    }

    prior_state = radar_api._state
    radar_api._state = state
    try:
        payload = asyncio.run(radar_api.get_iid_rotation(6))
    finally:
        radar_api._state = prior_state

    assert payload["status"] == "LIKELY_SINGLE"
    assert payload["primary_readiness"] == "PROVISIONAL"
    assert payload["period_s"] == 4.0
    assert payload["secondary_period_s"] is None
    assert payload["current_status"] == "CHECK_MULTI"
    assert payload["current_period_s"] == 4.02
    assert payload["primary_confidence"] == 0.5
    assert payload["secondary_confidence"] == 0.0


def test_get_all_rotation_includes_primary_readiness():
    state = RadarState()
    state._models = {
        9: RadarIID(
            iid=9,
            status="SINGLE_RADAR",
            period_s=4.0,
            period_std_s=0.02,
            rpm=15.0,
            primary_support_count=12,
        ),
        10: RadarIID(
            iid=10,
            status="LIKELY_SINGLE",
            period_s=4.0,
            period_std_s=0.03,
            rpm=15.0,
            primary_support_count=2,
        ),
    }

    prior_state = radar_api._state
    radar_api._state = state
    try:
        payload = asyncio.run(radar_api.get_all_rotation())
    finally:
        radar_api._state = prior_state

    rows = {row["iid"]: row for row in payload}
    assert rows[9]["primary_readiness"] == "ESTABLISHED"
    assert rows[10]["primary_readiness"] == "TENTATIVE"


def test_reset_iid_rotation_endpoint_clears_iid_state():
    state = RadarState()
    state._iid_events = deque([(1_000_000, 6, "AAAAAA", None)])
    state._models = {
        6: RadarIID(iid=6, status="SINGLE_RADAR", period_s=4.0, primary_support_count=8),
    }

    prior_state = radar_api._state
    radar_api._state = state
    try:
        payload = asyncio.run(radar_api.reset_iid_rotation(6))
    finally:
        radar_api._state = prior_state

    assert payload == {"iid": 6, "reset": True}
    assert state.get_rotation_model(6) is None


def test_get_iid_pipeline_health_reports_live_sweep_frames():
    state = RadarState()
    state._models = {
        7: RadarIID(iid=7, status="LIKELY_SINGLE", period_s=4.0),
    }
    state._live_completed_frames[7] = deque([
        SweepFrame(
            frame_index=0,
            sweep_start_us=0.0,
            ref_icao="AAAAAA",
            ref_lat=51.0,
            ref_lon=-1.0,
            ref_arrival_us=0.0,
            observations=[
                SweepFrameObservation("BBBBBB", 51.1, -1.1, 100_000.0),
                SweepFrameObservation("CCCCCC", 51.2, -1.2, 200_000.0),
                SweepFrameObservation("DDDDDD", 51.3, -1.3, 300_000.0),
            ],
            quality="good",
            period_s=4.0,
        )
    ], maxlen=state._LIVE_FRAMES_MAX)

    prior_state = radar_api._state
    radar_api._state = state
    try:
        payload = asyncio.run(radar_api.get_iid_pipeline_health(7))
    finally:
        radar_api._state = prior_state

    stages = payload["stages"]
    assert stages["frames"]["status"] == "working"
    assert stages["frames"]["detail"] == "1 frames (1 good)"
    assert stages["scoring"]["status"] == "accumulating"
    assert stages["scoring"]["detail"] == "Airport hypothesis not yet run"


def test_get_iid_sweeps_uses_precomputed_sweep_positions_without_relookup(monkeypatch):
    state = RadarState()
    state._models = {
        7: RadarIID(iid=7, status="SINGLE_RADAR", period_s=4.0, lat=51.5, lon=-1.5),
    }
    state._sweep_history = {
        7: deque([
            {
                "centroid_us": 5_010_000,
                "n_aircraft": 1,
                "aircraft": [
                    {
                        "icao": "AAAAAA",
                        "centroid_us": 5_000_000,
                        "lat": 51.2,
                        "lon": -1.2,
                        "interpolated": True,
                    }
                ],
            }
        ])
    }

    def fail_relookup(*args, **kwargs):
        raise AssertionError("burst position re-lookup should not happen when sweep data already has lat/lon")

    monkeypatch.setattr(state, "get_aircraft_burst_position", fail_relookup)

    prior_state = radar_api._state
    radar_api._state = state
    try:
        payload = asyncio.run(radar_api.get_iid_sweeps(7, n=30))
    finally:
        radar_api._state = prior_state

    assert payload["iid"] == 7
    assert len(payload["sweeps"]) == 1
    az = payload["sweeps"][0]["azimuths"][0]
    assert az["icao"] == "AAAAAA"
    assert az["range_nm"] is not None
    assert az["azimuth_deg"] is not None
    assert az["interpolated"] is True


def test_get_iid_sweep_frames_includes_period_and_latest_arrival_anchor():
    state = RadarState()
    state._iid_events = deque([
        (2_500_000, 7, "AAAAAA", None),
    ])
    state._models = {
        7: RadarIID(iid=7, status="SINGLE_RADAR", period_s=4.0),
    }
    state._live_completed_frames[7] = deque([
        SweepFrame(
            frame_index=0,
            sweep_start_us=1_000_000.0,
            ref_icao="AAAAAA",
            ref_lat=51.0,
            ref_lon=-1.0,
            ref_arrival_us=1_000_000.0,
            observations=[
                SweepFrameObservation("BBBBBB", 51.1, -1.1, 1_200_000.0),
            ],
            quality="marginal",
            period_s=4.0,
        )
    ], maxlen=state._LIVE_FRAMES_MAX)

    prior_state = radar_api._state
    radar_api._state = state
    try:
        payload = asyncio.run(radar_api.get_iid_sweep_frames(7))
    finally:
        radar_api._state = prior_state

    assert payload["latest_arrival_us"] == 2_500_000
    assert payload["frames"][0]["period_s"] == 4.0
    assert payload["frames"][0]["ref_arrival_us"] == 1_000_000.0
    assert payload["frames"][0]["observations"][0]["arrival_us"] == 1_200_000.0


def test_reset_all_radar_learning_clears_memory_and_db(monkeypatch):
    state = RadarState()
    state._iid_events = deque([(1_000_000, 6, "AAAAAA", None)])
    state._models = {
        6: RadarIID(iid=6, status="SINGLE_RADAR", period_s=4.0, primary_support_count=8),
    }

    class FakeStatsDB:
        def clear_radar_learning(self):
            return {"radar_iids_deleted": 3, "calibration_deleted": 9, "frame_positions_deleted": 12}

    prior_state = radar_api._state
    prior_db_module = sys.modules.get("db")
    radar_api._state = state
    sys.modules["db"] = SimpleNamespace(stats_db=FakeStatsDB())
    try:
        payload = asyncio.run(radar_api.reset_all_radar_learning())
    finally:
        radar_api._state = prior_state
        if prior_db_module is not None:
            sys.modules["db"] = prior_db_module
        else:
            del sys.modules["db"]

    assert payload["reset"] is True
    assert payload["memory"]["models"] == 1
    assert payload["memory"]["events"] == 1
    assert payload["persisted"] == {"radar_iids_deleted": 3, "calibration_deleted": 9, "frame_positions_deleted": 12}
    assert state.get_rotation_model(6) is None
    assert list(state._iid_events) == []


def test_get_iid_sweeps_includes_range_and_azimuth_when_position_is_available():
    state = RadarState()
    state._sweep_history = {
        7: deque([
            {
                "centroid_us": 1_000_000,
                "n_aircraft": 1,
                "aircraft": [
                    {
                        "icao": "AAAAAA",
                        "centroid_us": 1_020_000,
                        "arrivals_us": [1_000_000, 1_040_000],
                        "replies": [
                            {"arrival_us": 1_000_000, "signal_dbfs": -12.0},
                            {"arrival_us": 1_040_000, "signal_dbfs": -11.5},
                        ],
                        "signal_dbfs": -11.75,
                    }
                ],
            }
        ])
    }
    state._models = {
        7: RadarIID(iid=7, status="SINGLE_RADAR", lat=51.0, lon=-1.0),
    }

    def fake_position(_icao: str, _ts_us: int):
        return {
            "lat": 51.5,
            "lon": -0.5,
            "range_nm": 42.2,
            "bearing_deg": 33.0,
            "interpolated": True,
        }

    state.get_aircraft_burst_position = fake_position  # type: ignore[method-assign]

    prior_state = radar_api._state
    radar_api._state = state
    try:
        payload = asyncio.run(radar_api.get_iid_sweeps(7, n=30))
    finally:
        radar_api._state = prior_state

    entry = payload["sweeps"][0]["azimuths"][0]
    assert entry["icao"] == "AAAAAA"
    assert entry["range_nm"] == 42.2
    assert entry["azimuth_deg"] is not None
    assert entry["interpolated"] is True


def test_get_iid_sweeps_suppresses_azimuth_for_non_localisable_iid():
    state = RadarState()
    state._sweep_history = {
        62: deque([
            {
                "centroid_us": 1_000_000,
                "n_aircraft": 1,
                "aircraft": [
                    {
                        "icao": "AAAAAA",
                        "centroid_us": 1_020_000,
                        "arrivals_us": [1_000_000],
                        "replies": [{"arrival_us": 1_000_000, "signal_dbfs": -10.0}],
                        "signal_dbfs": -10.0,
                    }
                ],
            }
        ])
    }
    state._models = {
        62: RadarIID(
            iid=62,
            status="CHECK_MULTI",
            multi_radar_flag=True,
            lat=51.0,
            lon=-1.0,
        ),
    }

    def fake_position(_icao: str, _ts_us: int):
        return {
            "lat": 51.5,
            "lon": -0.5,
            "range_nm": 42.2,
            "bearing_deg": 33.0,
            "interpolated": False,
        }

    state.get_aircraft_burst_position = fake_position  # type: ignore[method-assign]

    prior_state = radar_api._state
    radar_api._state = state
    try:
        payload = asyncio.run(radar_api.get_iid_sweeps(62, n=30))
    finally:
        radar_api._state = prior_state

    entry = payload["sweeps"][0]["azimuths"][0]
    assert entry["range_nm"] == 42.2
    assert entry["azimuth_deg"] is None


def test_get_iid_dwell_returns_true_per_reply_signals():
    state = RadarState()
    state._sweep_history = {
        8: deque([
            {
                "centroid_us": 2_000_000,
                "n_aircraft": 1,
                "aircraft": [
                    {
                        "icao": "BBBBBB",
                        "arrivals_us": [2_000_000, 2_030_000, 2_060_000],
                        "replies": [
                            {"arrival_us": 2_000_000, "signal_dbfs": -18.1},
                            {"arrival_us": 2_030_000, "signal_dbfs": -16.4},
                            {"arrival_us": 2_060_000, "signal_dbfs": -17.0},
                        ],
                        "signal_dbfs": -17.17,
                    }
                ],
            }
        ])
    }

    prior_state = radar_api._state
    radar_api._state = state
    try:
        payload = asyncio.run(radar_api.get_iid_dwell(8, icao="BBBBBB", sweep_idx=0))
    finally:
        radar_api._state = prior_state

    assert payload["replies"] == [
        {"arrival_us": 2_000_000, "signal_dbfs": -18.1},
        {"arrival_us": 2_030_000, "signal_dbfs": -16.4},
        {"arrival_us": 2_060_000, "signal_dbfs": -17.0},
    ]


def test_manual_position_controls_persist_and_lock(monkeypatch):
    state = RadarState()
    state._models = {
        44: RadarIID(iid=44, status="SINGLE_RADAR", period_s=4.0),
    }

    persisted = []

    class FakeStatsDB:
        def upsert_radar_iid(self, model):
            persisted.append((model.iid, model.resolution_mode, model.manual_lat, model.manual_lon))

    prior_state = radar_api._state
    prior_db_module = sys.modules.get("db")
    radar_api._state = state
    sys.modules["db"] = SimpleNamespace(stats_db=FakeStatsDB())
    try:
        manual = asyncio.run(
            radar_api.set_iid_manual_position(
                44,
                radar_api.ManualPositionPayload(lat=51.5007, lon=-0.1246, note="Tower compound"),
            )
        )
        locked = asyncio.run(radar_api.lock_iid_position(44))
        control = asyncio.run(radar_api.get_iid_control(44))
    finally:
        radar_api._state = prior_state
        if prior_db_module is not None:
            sys.modules["db"] = prior_db_module
        else:
            del sys.modules["db"]

    assert manual["updated"] is True
    assert manual["display_source"] == "none"
    assert locked["locked"] is True
    assert locked["resolution_mode"] == "locked_position"
    assert locked["display_source"] == "manual"
    assert control["manual_note"] == "Tower compound"
    assert control["display_lat"] == 51.5007
    assert control["display_lon"] == -0.1246
    assert control["best_effort_source"] == "none"
    assert control["manual_reference_error_m"] is None
    assert persisted[-1] == (44, "locked_position", 51.5007, -0.1246)


def test_control_payload_reports_manual_reference_error_against_best_effort():
    state = RadarState()
    state._models = {
        44: RadarIID(
            iid=44,
            status="SINGLE_RADAR",
            resolution_mode="locked_position",
            manual_lat=51.5007,
            manual_lon=-0.1246,
            fm_lat=51.5107,
            fm_lon=-0.1246,
            fm_cep_m=1200.0,
        ),
    }

    prior_state = radar_api._state
    radar_api._state = state
    try:
        control = asyncio.run(radar_api.get_iid_control(44))
    finally:
        radar_api._state = prior_state

    assert control["best_effort_source"] == "fm"
    assert control["manual_reference_error_m"] is not None
    assert control["manual_reference_error_m"] > 1000.0


def test_mark_unresolvable_suppresses_authoritative_position_and_can_clear(monkeypatch):
    state = RadarState()
    state._models = {
        45: RadarIID(iid=45, status="SINGLE_RADAR", lat=51.0, lon=-1.0, cep_m=1200.0),
    }

    class FakeStatsDB:
        def upsert_radar_iid(self, model):
            return None

    prior_state = radar_api._state
    prior_db_module = sys.modules.get("db")
    radar_api._state = state
    sys.modules["db"] = SimpleNamespace(stats_db=FakeStatsDB())
    try:
        marked = asyncio.run(
            radar_api.mark_iid_unresolvable(
                45,
                radar_api.UnresolvablePayload(reason="Insufficient coherent pulse family"),
            )
        )
        location = asyncio.run(radar_api.get_iid_location(45))
        cleared = asyncio.run(radar_api.clear_iid_unresolvable(45))
    finally:
        radar_api._state = prior_state
        if prior_db_module is not None:
            sys.modules["db"] = prior_db_module
        else:
            del sys.modules["db"]

    assert marked["updated"] is True
    assert marked["resolution_mode"] == "locked_unresolvable"
    assert marked["display_source"] == "none"
    assert location["status"] == "NOT_LOCALISED"
    assert location["reason"] == "IID marked unresolvable"
    assert cleared["updated"] is True
    assert cleared["resolution_mode"] == "auto"


def test_manual_tdoa_run_returns_disabled():
    # TDOA solver is disabled; the endpoint must return immediately without computation.
    prior_state = radar_api._state
    radar_api._state = RadarState()
    try:
        payload = asyncio.run(radar_api.run_iid_tdoa(46))
    finally:
        radar_api._state = prior_state

    assert payload["success"] is False
    assert payload["reason"] == "TDOA solver is disabled"


def test_get_iid_tdoa_diagnostics_returns_disabled_stub():
    # TDOA solver is disabled; endpoint must return a fast stub without loading pairs.
    state = RadarState()
    state._models = {
        46: RadarIID(
            iid=46,
            resolution_mode="auto",
            tdoa_last_run={"ts": 1300.0, "success": False},
        )
    }

    prior_state = radar_api._state
    radar_api._state = state
    try:
        payload = asyncio.run(radar_api.get_iid_tdoa_diagnostics(46))
    finally:
        radar_api._state = prior_state

    assert payload["available"] is False
    assert payload["status"] == "disabled"
    assert payload["iid"] == 46
    assert "last_run" in payload


def test_get_radar_map_uses_authoritative_display_position():
    state = RadarState()
    state._models = {
        7: RadarIID(
            iid=7,
            resolution_mode="locked_position",
            manual_lat=51.501,
            manual_lon=-0.141,
            manual_note="Visual confirmation",
            lat=51.0,
            lon=-1.0,
            fm_lat=52.0,
            fm_lon=-2.0,
        ),
        8: RadarIID(
            iid=8,
            resolution_mode="auto",
            fm_lat=53.0,
            fm_lon=-3.0,
            fm_cep_m=900.0,
            fm_source="airport_prior",
            fm_n_observations=17,
        ),
        9: RadarIID(
            iid=9,
            resolution_mode="locked_unresolvable",
            lat=54.0,
            lon=-4.0,
            fm_lat=55.0,
            fm_lon=-5.0,
        ),
    }

    prior_state = radar_api._state
    radar_api._state = state
    try:
        payload = asyncio.run(radar_api.get_radar_map())
    finally:
        radar_api._state = prior_state

    features = {feature["properties"]["iid"]: feature for feature in payload["features"]}
    assert set(features) == {7, 8}
    assert features[7]["geometry"]["coordinates"] == [-0.141, 51.501]
    assert features[7]["properties"]["source"] == "manual"
    assert features[7]["properties"]["manual_note"] == "Visual confirmation"
    assert features[8]["geometry"]["coordinates"] == [-3.0, 53.0]
    assert features[8]["properties"]["source"] == "fm"


def test_get_iid_location_prefers_manual_when_locked():
    state = RadarState()
    state._models = {
        12: RadarIID(
            iid=12,
            resolution_mode="locked_position",
            manual_lat=51.61,
            manual_lon=-0.22,
            manual_updated_ts=1234.0,
            lat=51.0,
            lon=-1.0,
            fm_lat=52.0,
            fm_lon=-2.0,
            n_pairs=4,
            status="SINGLE_RADAR",
        ),
    }

    prior_state = radar_api._state
    radar_api._state = state
    try:
        payload = asyncio.run(radar_api.get_iid_location(12))
    finally:
        radar_api._state = prior_state

    assert payload["source"] == "manual"
    assert payload["lat"] == 51.61
    assert payload["lon"] == -0.22
    assert payload["display_source"] == "manual"


def test_solution_comparison_prefers_best_automatic_method():
    state = RadarState()
    state._models = {
        30: RadarIID(
            iid=30,
            resolution_mode="auto",
            lat=51.0,
            lon=-1.0,
            cep_m=5000.0,
            fm_lat=51.1,
            fm_lon=-1.1,
            fm_cep_m=1200.0,
            ci_lat=51.08,
            ci_lon=-1.08,
            ci_cep_m=1800.0,
        )
    }

    class FakeStatsDB:
        def load_calibration_pairs(self, iid, limit=2000):
            assert iid == 30
            return []

        def count_calibration_pairs(self, iid):
            assert iid == 30
            return 0

    prior_state = radar_api._state
    prior_db_module = sys.modules.get("db")
    radar_api._state = state
    sys.modules["db"] = SimpleNamespace(stats_db=FakeStatsDB())
    try:
        payload = asyncio.run(radar_api.get_iid_solution_comparison(30))
    finally:
        radar_api._state = prior_state
        if prior_db_module is not None:
            sys.modules["db"] = prior_db_module
        else:
            del sys.modules["db"]

    assert payload["selected"]["source"] == "combined"
    methods = {method["source"]: method for method in payload["methods"]}
    assert set(methods) >= {"manual", "fm", "tdoa", "coincident_illumination", "inscribed_angle", "combined"}
    assert methods["fm"]["status"] == "solved"
    assert methods["tdoa"]["status"] == "solved"
    assert methods["coincident_illumination"]["status"] == "solved"
    assert methods["combined"]["status"] == "solved"


def test_solution_comparison_reports_tdoa_inactive_when_pairs_exist_but_no_solution():
    state = RadarState()
    state._models = {
        30: RadarIID(
            iid=30,
            resolution_mode="auto",
            period_s=4.0,
        )
    }

    class FakeStatsDB:
        def load_calibration_pairs(self, iid, limit=2000):
            assert iid == 30
            return [{
                "iid": 30,
                "ts": 1000.0,
                "icao_a": "AAAAAA",
                "icao_b": "BBBBBB",
                "lat_a": 51.0,
                "lon_a": -1.0,
                "lat_b": 51.2,
                "lon_b": -1.2,
                "tdoa_us": 45.0,
                "receiver_lat": 51.1,
                "receiver_lon": -1.1,
            }]

        def count_calibration_pairs(self, iid):
            assert iid == 30
            return 1

    prior_state = radar_api._state
    prior_db_module = sys.modules.get("db")
    radar_api._state = state
    sys.modules["db"] = SimpleNamespace(stats_db=FakeStatsDB())
    try:
        payload = asyncio.run(radar_api.get_iid_solution_comparison(30))
    finally:
        radar_api._state = prior_state
        if prior_db_module is not None:
            sys.modules["db"] = prior_db_module
        else:
            del sys.modules["db"]

    methods = {method["source"]: method for method in payload["methods"]}
    assert methods["tdoa"]["status"] == "inactive"
    assert "no active TDOA background solver" in methods["tdoa"]["notes"]


def test_get_iid_location_can_return_coincident_solution():
    state = RadarState()
    state._models = {
        31: RadarIID(
            iid=31,
            resolution_mode="auto",
            ci_lat=51.25,
            ci_lon=-1.25,
            ci_cep_m=1500.0,
            ci_n_pairs=9,
            ci_last_updated=1234.0,
        )
    }

    prior_state = radar_api._state
    radar_api._state = state
    try:
        payload = asyncio.run(radar_api.get_iid_location(31))
    finally:
        radar_api._state = prior_state

    assert payload["source"] == "coincident_illumination"
    assert payload["lat"] == 51.25
    assert payload["lon"] == -1.25


def test_get_iid_location_returns_combined_solution_when_methods_agree():
    state = RadarState()
    state._models = {
        32: RadarIID(
            iid=32,
            resolution_mode="auto",
            lat=51.000,
            lon=-1.000,
            cep_m=4000.0,
            fm_lat=51.010,
            fm_lon=-1.010,
            fm_cep_m=1200.0,
            ci_lat=51.015,
            ci_lon=-1.005,
            ci_cep_m=1600.0,
        )
    }

    prior_state = radar_api._state
    radar_api._state = state
    try:
        payload = asyncio.run(radar_api.get_iid_location(32))
    finally:
        radar_api._state = prior_state

    assert payload["source"] == "combined"
    assert set(payload["contributors"]) >= {"fm", "coincident_illumination", "tdoa"}


def test_get_iid_tdoa_evidence_returns_disabled():
    # TDOA solver is disabled; evidence endpoint must return available=False with no layers.
    state = RadarState()
    state._models = {7: RadarIID(iid=7, status="SINGLE_RADAR")}

    prior_state = radar_api._state
    radar_api._state = state
    try:
        payload = asyncio.run(radar_api.get_iid_evidence_method(7, "tdoa"))
    finally:
        radar_api._state = prior_state

    assert payload["available"] is False
    assert payload["method"] == "tdoa"
    assert payload["layers"] == []


def test_get_iid_coincident_illumination_evidence_returns_predicted_fm_rays():
    state = RadarState()
    state._models = {
        8: RadarIID(
            iid=8,
            resolution_mode="auto",
            period_s=10.0,
            fm_lat=51.0,
            fm_lon=-1.0,
            fm_cep_m=900.0,
        )
    }
    state._live_completed_frames[8] = deque([
        SweepFrame(
            frame_index=0,
            sweep_start_us=1_000_000.0,
            ref_icao="AAAAAA",
            ref_lat=51.10,
            ref_lon=-1.0,
            ref_arrival_us=1_000_000.0,
            observations=[
                SweepFrameObservation("BBBBBB", 51.20, -1.0, 1_010_000.0),
                SweepFrameObservation("CCCCCC", 51.30, -1.0, 1_020_000.0),
                SweepFrameObservation("DDDDDD", 51.40, -1.0, 1_030_000.0),
            ],
            quality="good",
            period_s=10.0,
        ),
    ])

    prior_state = radar_api._state
    radar_api._state = state
    try:
        payload = asyncio.run(radar_api.get_iid_evidence_method(8, "coincident_illumination"))
    finally:
        radar_api._state = prior_state

    assert payload["available"] is True
    assert payload["validation"]["available"] is True
    ray_layer = next(layer for layer in payload["layers"] if layer["label"] == "Predicted Coincident Rays")
    assert ray_layer["features"]
    assert ray_layer["features"][0]["properties"]["line_type"] == "predicted_coincident_ray"
    assert ray_layer["features"][0]["properties"]["validation"] == "support"


def test_get_iid_coincident_illumination_evidence_requires_fm_solution():
    state = RadarState()
    state._models = {8: RadarIID(iid=8, resolution_mode="auto")}

    prior_state = radar_api._state
    radar_api._state = state
    try:
        payload = asyncio.run(radar_api.get_iid_evidence_method(8, "coincident_illumination"))
    finally:
        radar_api._state = prior_state

    assert payload["available"] is False
    assert payload["reason"] == "forward-model solution required"
    assert payload["layers"] == []


def test_get_iid_coincident_diagnostics_reports_solver_progress():
    pair_rows = []
    families = [
        ("AAAAAA", "AAA001", (51.05, -1.20), (51.45, -0.60)),
        ("BBBBBB", "BBB001", (51.00, -0.80), (51.50, -0.90)),
        ("CCCCCC", "CCC001", (51.15, -1.25), (51.35, -0.45)),
        ("DDDDDD", "DDD001", (50.95, -1.05), (51.55, -0.65)),
    ]
    for family_idx, (icao_a, icao_b, a, b) in enumerate(families):
        for repeat_idx, offset in enumerate((0.0, 40.0, -35.0)):
            pair_rows.append({
                "iid": 8,
                "ts": 1_000.0 + family_idx * 30.0 + repeat_idx * 5.0,
                "icao_a": icao_a,
                "icao_b": icao_b,
                "lat_a": a[0],
                "lon_a": a[1],
                "lat_b": b[0],
                "lon_b": b[1],
                "tdoa_us": 120.0 + offset,
                "receiver_lat": 51.0,
                "receiver_lon": -1.0,
            })

    class FakeStatsDB:
        def load_calibration_pairs(self, iid, limit=2000):
            assert iid == 8
            return pair_rows

    state = RadarState()
    state._models = {
        8: RadarIID(
            iid=8,
                resolution_mode="auto",
                fm_lat=51.24,
                fm_lon=-0.86,
                fm_cep_m=900.0,
                ci_lat=51.25,
                ci_lon=-0.85,
                ci_cep_m=1400.0,
            ci_n_pairs=12,
            ci_last_updated=1_234.0,
        )
    }

    prior_state = radar_api._state
    prior_db_module = sys.modules.get("db")
    radar_api._state = state
    sys.modules["db"] = SimpleNamespace(stats_db=FakeStatsDB())
    # Clear any stale cache entry that could mask a real computation failure.
    radar_api._diag_cache.pop("8:coincident", None)
    try:
        payload = asyncio.run(radar_api.get_iid_coincident_diagnostics(8))
    finally:
        radar_api._state = prior_state
        if prior_db_module is not None:
            sys.modules["db"] = prior_db_module
        else:
            del sys.modules["db"]

    assert payload["available"] is True
    assert payload["status"] == "ready"
    assert payload["stable_families"] == 4
    assert payload["coincident_pairs"] == 12
    assert payload["usable_lines"] >= 4
    assert payload["intersections"] >= 3
    assert payload["stored_solution"]["source"] == "coincident_illumination"
    assert payload["top_families"][0]["stable"] is True


def test_get_iid_fm_diagnostics_includes_last_run_status():
    state = RadarState()
    state._models = {
        8: RadarIID(
            iid=8,
            resolution_mode="auto",
            status="SINGLE_RADAR",
            period_s=10.0,
            fm_lat=51.1,
            fm_lon=-1.2,
            fm_cep_m=900.0,
            fm_n_observations=12,
            fm_last_run={
                "ts": 1234.0,
                "success": False,
                "stored": False,
                "elapsed_ms": 321.0,
                "stage": "quality_gates",
                "reason": "intersection quality gate failed: dominance_ratio",
                "detail": {"n_frames": 4, "n_pairs": 6},
            },
        )
    }

    prior_state = radar_api._state
    radar_api._state = state
    try:
        payload = asyncio.run(radar_api.get_iid_fm_diagnostics(8))
    finally:
        radar_api._state = prior_state

    assert payload["available"] is True
    last_run = payload["forward_model"]["last_run"]
    assert last_run["stage"] == "quality_gates"
    assert last_run["reason"] == "intersection quality gate failed: dominance_ratio"
    assert last_run["detail"] == {"n_frames": 4, "n_pairs": 6}


def test_get_iid_inscribed_angle_and_forward_model_evidence_returns_geometry_layers():
    state = RadarState()
    state._models = {
        9: RadarIID(iid=9, period_s=4.0, fm_lat=51.5, fm_lon=-1.5, fm_cep_m=800.0, fm_source="intersection_refined"),
    }
    state._live_completed_frames[9] = deque([
        SweepFrame(
            frame_index=0,
            sweep_start_us=0.0,
            ref_icao="AAAAAA",
            ref_lat=51.0,
            ref_lon=-1.0,
            ref_arrival_us=0.0,
            observations=[
                SweepFrameObservation("BBBBBB", 51.4, -1.2, 1_000_000.0),
                SweepFrameObservation("CCCCCC", 51.6, -1.8, 1_500_000.0),
            ],
            quality="good",
            period_s=4.0,
        )
    ], maxlen=state._LIVE_FRAMES_MAX)

    prior_state = radar_api._state
    radar_api._state = state
    try:
        inscribed = asyncio.run(radar_api.get_iid_evidence_method(9, "inscribed_angle"))
        forward_model = asyncio.run(radar_api.get_iid_evidence_method(9, "forward_model"))
        combined = asyncio.run(radar_api.get_iid_evidence(9))
    finally:
        radar_api._state = prior_state

    assert inscribed["available"] is True
    circle_layer = next(layer for layer in inscribed["layers"] if layer["label"] == "Inscribed-Angle Circles")
    assert circle_layer["features"]
    assert circle_layer["features"][0]["geometry"]["type"] == "Circle"

    assert forward_model["available"] is True
    bearing_layer = next(layer for layer in forward_model["layers"] if layer["label"] == "Predicted Bearings")
    assert bearing_layer["features"]
    assert combined["available"] is True
    assert {method["method"] for method in combined["methods"]} == {
        "tdoa",
        "coincident_illumination",
        "forward_model",
        "inscribed_angle",
    }


def test_get_iid_sweep_frame_fm_geometry_returns_pair_circles(monkeypatch):
    import config
    from radar.localiser import _bearing_deg

    true_lat, true_lon = 51.0, -1.0
    period_s = 10.0
    ref_icao, ref_lat, ref_lon = "AAAAAA", 51.4, -1.4
    aircraft = [
        ("BBBBBB", 50.6, -0.8),
        ("CCCCCC", 50.7, -1.3),
        ("DDDDDD", 51.5, -0.4),
        ("EEEEEE", 51.2, -1.5),
    ]
    ref_bearing = _bearing_deg(true_lat, true_lon, ref_lat, ref_lon)
    observations = []
    for icao, lat, lon in aircraft:
        phase_deg = (_bearing_deg(true_lat, true_lon, lat, lon) - ref_bearing) % 360.0
        observations.append(SweepFrameObservation(
            icao,
            lat,
            lon,
            int((phase_deg / 360.0) * period_s * 1_000_000),
            n_replies=3,
        ))

    monkeypatch.setattr(config, "RECEIVER_LAT", true_lat)
    monkeypatch.setattr(config, "RECEIVER_LON", true_lon)

    state = RadarState()
    state._models = {9: RadarIID(iid=9, period_s=period_s, status="SINGLE_RADAR")}
    state._live_completed_frames[9] = deque([
        SweepFrame(
            frame_index=0,
            sweep_start_us=0.0,
            ref_icao=ref_icao,
            ref_lat=ref_lat,
            ref_lon=ref_lon,
            ref_arrival_us=0.0,
            observations=observations,
            quality="good",
            period_s=period_s,
        )
    ], maxlen=state._LIVE_FRAMES_MAX)

    prior_state = radar_api._state
    radar_api._state = state
    try:
        payload = asyncio.run(radar_api.get_iid_sweep_frame_fm_geometry(9, 0))
    finally:
        radar_api._state = prior_state

    assert payload["available"] is True
    assert payload["ref_icao"] == "AAAAAA"
    assert payload["pair_circle_summary"]["total_raw_pair_circles"] == 10
    assert payload["pair_circle_summary"]["total_admitted_pair_circles"] >= 1
    circle_layer = next(layer for layer in payload["layers"] if layer["label"] == "Admitted Pair Circles")
    assert circle_layer["features"]
    assert circle_layer["features"][0]["geometry"]["type"] == "Circle"
    assert {"icao_a", "icao_b", "circle_score", "normalized_residual", "inlier"} <= set(payload["admitted_pair_circles"][0])
    assert payload["pair_circle_summary"]["admitted_pairs_not_containing_reference"] > 0


def test_get_iid_sweep_frame_fm_geometry_returns_frame_estimate_overlay(monkeypatch):
    import config
    import radar.forward_model as forward_model
    from radar.localiser import _bearing_deg

    true_lat, true_lon = 51.0, -1.0
    period_s = 10.0
    ref_icao, ref_lat, ref_lon = "AAAAAA", 51.4, -1.4
    aircraft = [
        ("BBBBBB", 50.6, -0.8),
        ("CCCCCC", 50.7, -1.3),
        ("DDDDDD", 51.5, -0.4),
        ("EEEEEE", 51.2, -1.5),
    ]
    ref_bearing = _bearing_deg(true_lat, true_lon, ref_lat, ref_lon)
    observations = []
    for icao, lat, lon in aircraft:
        phase_deg = (_bearing_deg(true_lat, true_lon, lat, lon) - ref_bearing) % 360.0
        observations.append(SweepFrameObservation(
            icao,
            lat,
            lon,
            int((phase_deg / 360.0) * period_s * 1_000_000),
            n_replies=3,
        ))

    state = RadarState()
    state._models = {9: RadarIID(iid=9, period_s=period_s, status="SINGLE_RADAR")}
    state._live_completed_frames[9] = deque([
        SweepFrame(
            frame_index=0,
            sweep_start_us=0.0,
            ref_icao=ref_icao,
            ref_lat=ref_lat,
            ref_lon=ref_lon,
            ref_arrival_us=0.0,
            observations=observations,
            quality="good",
            period_s=period_s,
        )
    ], maxlen=state._LIVE_FRAMES_MAX)

    monkeypatch.setattr(config, "RECEIVER_LAT", true_lat)
    monkeypatch.setattr(config, "RECEIVER_LON", true_lon)
    monkeypatch.setattr(forward_model, "_PER_FRAME_MIN_CONTRIBUTING_ARCS", 1)

    prior_state = radar_api._state
    radar_api._state = state
    try:
        payload = asyncio.run(radar_api.get_iid_sweep_frame_fm_geometry(9, 0))
    finally:
        radar_api._state = prior_state

    assert payload["pair_circle_summary"]["total_raw_pair_circles"] == 10
    assert payload["pair_circle_summary"]["total_admitted_pair_circles"] == len(payload["admitted_pair_circles"])
    assert payload["pair_circle_summary"]["total_inlier_pair_circles"] == len(payload["inlier_pair_circles"])
    assert payload["pair_circle_summary"]["admitted_pairs_not_containing_reference"] > 0
    assert payload["inlier_pair_circles"]
    assert all(pair["inlier"] for pair in payload["inlier_pair_circles"])
    circle_layer = next(layer for layer in payload["layers"] if layer["label"] == "Admitted Pair Circles")
    assert any(
        feature["properties"].get("inlier")
        for feature in circle_layer["features"]
        if feature["properties"].get("circle_type") == "admitted_pair_circle"
    )
