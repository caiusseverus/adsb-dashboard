import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(__file__)))

from collections import deque
from types import SimpleNamespace

import config
import pytest
from radar.sweep import _analyse_iid_events
from radar.models import BurstRecord, LiveFrameState, RadarIID, ReferenceAircraftInfo, RotationModel
from radar.sweep import (
    AlignedBurstSyncObs,
    IcaoSyncQuality,
    LiveSyncState,
    RadarState,
    WaveformBin,
    predict_sync_observation,
    _reinforce_radar_characteristics,
    detect_bursts,
    detect_bursts_with_signals,
)


def _make_events(iid: int, icao: str, arrivals_s: list[float]) -> list[tuple[int, int, str, None]]:
    return [
        (int(arrival_s * 1_000_000), iid, icao, None)
        for arrival_s in arrivals_s
    ]


def _make_aligned_sync_obs(
    state: RadarState,
    *,
    period_s: float,
    residuals_deg: list[float],
    start_s: float = 0.0,
    step_s: float = 4.0,
    icaos: tuple[str, ...] = ("AAAAAA", "BBBBBB"),
    ts_start: float = 995.0,
) -> deque:
    obs = deque(maxlen=state._MULTI_SYNC_OBS_MAX)
    for idx, residual_deg in enumerate(residuals_deg):
        t_s = start_s + idx * step_s
        predicted = ((t_s / period_s) * 360.0) % 360.0
        obs.append(AlignedBurstSyncObs(
            burst_centroid_us=t_s * 1_000_000.0,
            icao=icaos[idx % len(icaos)],
            bearing_deg=(predicted + residual_deg) % 360.0,
            n_replies=4,
            signal_dbfs=-12.0,
            pos_age_s=0.2,
            range_nm=0.0,
            ts=ts_start + idx * 0.5,
            sync_update_eligible=True,
        ))
    return obs


def test_analyse_iid_events_prefers_dominant_family_over_half_period_clutter():
    events = []
    events.extend(_make_events(6, "AAAAAA", [0.0, 4.0, 8.0, 12.0, 16.0]))
    events.extend(_make_events(6, "BBBBBB", [0.3, 4.3, 8.3, 12.3, 16.3]))
    # Interleaved residual series that would support a false ~2 s candidate.
    events.extend(_make_events(6, "CCCCCC", [0.0, 2.0, 4.0, 6.0, 8.0, 10.0]))

    model = _analyse_iid_events(events)

    assert model.dominant_period_s == 4.0
    assert model.folded["AAAAAA"]["multiplier"] == 1
    assert model.folded["BBBBBB"]["multiplier"] == 1
    assert model.residual["CCCCCC"] == 2.0


def test_analyse_iid_events_leaves_non_primary_family_as_residual():
    events = []
    events.extend(_make_events(21, "AAAAAA", [0.0, 4.0, 8.0, 12.0, 16.0]))
    events.extend(_make_events(21, "BBBBBB", [0.3, 4.3, 8.3, 12.3, 16.3]))
    events.extend(_make_events(21, "FFFFFF", [0.7, 4.7, 8.7, 12.7, 16.7]))
    events.extend(_make_events(21, "CCCCCC", [0.0, 5.0, 10.0, 15.0, 20.0]))
    events.extend(_make_events(21, "DDDDDD", [0.4, 5.4, 10.4, 15.4, 20.4]))
    events.extend(_make_events(21, "EEEEEE", [0.8, 5.8, 10.8, 15.8, 20.8]))

    model = _analyse_iid_events(events)

    assert model.dominant_period_s == 4.0
    assert model.secondary_period_s is None
    assert model.folded["AAAAAA"]["multiplier"] == 1
    assert model.folded["FFFFFF"]["multiplier"] == 1
    assert model.secondary_folded == {}
    assert model.residual == {"CCCCCC": 5.0, "DDDDDD": 5.0, "EEEEEE": 5.0}


def test_analyse_iid_events_rescues_noisy_row_with_clear_primary_centroid_sequence():
    events = []
    events.extend(_make_events(31, "AAAAAA", [0.0, 4.0, 8.0, 12.0, 16.0]))
    events.extend(_make_events(31, "BBBBBB", [0.4, 4.4, 8.4, 12.4, 16.4]))
    # This ICAO has extra clutter bursts, but an obvious repeated 4 s cadence remains visible.
    events.extend(_make_events(31, "CCCCCC", [0.3, 2.1, 4.3, 6.1, 8.3, 12.3, 16.3]))

    model = _analyse_iid_events(events)

    assert model.dominant_period_s == 4.0
    assert model.folded["CCCCCC"]["multiplier"] == 1
    assert model.folded["CCCCCC"]["method"] in {"median", "centroid"}
    assert "CCCCCC" not in model.residual


def test_reinforce_radar_characteristics_builds_long_term_primary_and_secondary_periods():
    iid_model = RadarIID(iid=1)

    _reinforce_radar_characteristics(
        iid_model,
        RotationModel(
            dominant_period_s=3.97,
            primary_direct_count=3,
            period_std_s=0.03,
            status="LIKELY_SINGLE",
        ),
    )
    _reinforce_radar_characteristics(
        iid_model,
        RotationModel(
            dominant_period_s=3.99,
            primary_direct_count=2,
            period_std_s=0.02,
            status="LIKELY_SINGLE",
        ),
    )

    assert iid_model.period_s == 3.975
    assert iid_model.secondary_period_s is None
    assert iid_model.primary_support_count == 5
    assert iid_model.secondary_support_count == 0
    assert iid_model.status == "LIKELY_SINGLE"
    assert iid_model.multi_radar_flag is False


def test_reinforce_radar_characteristics_ignores_tentative_secondary_family():
    iid_model = RadarIID(iid=12)

    _reinforce_radar_characteristics(
        iid_model,
        RotationModel(
            dominant_period_s=4.0,
            primary_direct_count=12,
            period_std_s=0.02,
            status="LIKELY_SINGLE",
        ),
    )

    assert iid_model.status == "SINGLE_RADAR"
    assert iid_model.multi_radar_flag is False
    assert iid_model.secondary_period_s is None


def test_reinforce_radar_characteristics_can_replace_bad_initial_estimate():
    iid_model = RadarIID(iid=6, period_s=2.0, primary_support_count=6)

    stronger_model = RotationModel(
        dominant_period_s=4.0,
        primary_direct_count=5,
        period_std_s=0.02,
        status="LIKELY_SINGLE",
    )

    _reinforce_radar_characteristics(iid_model, stronger_model)
    assert iid_model.period_s == 2.0
    assert iid_model.primary_support_count == 5

    _reinforce_radar_characteristics(iid_model, stronger_model)
    assert iid_model.period_s == 4.0
    assert iid_model.primary_support_count == 5


def test_reinforce_radar_characteristics_does_not_grow_support_without_direct_cohort_evidence():
    iid_model = RadarIID(iid=8, period_s=4.0, primary_support_count=3)

    _reinforce_radar_characteristics(
        iid_model,
        RotationModel(
            dominant_period_s=4.0,
            primary_direct_count=0,
            period_std_s=0.15,
            status="CHECK_MULTI",
        ),
    )

    assert iid_model.period_s == 4.0
    assert iid_model.primary_support_count == 3


def test_reset_iid_clears_in_memory_learning_state():
    state = RadarState()
    state._iid_events = deque([
        (1_000_000, 9, "AAAAAA", None),
        (2_000_000, 9, "BBBBBB", None),
        (3_000_000, 10, "CCCCCC", None),
    ])
    state._models = {
        9: RadarIID(iid=9, period_s=4.0, primary_support_count=8),
        10: RadarIID(iid=10, period_s=5.0, primary_support_count=4),
    }
    state._sweep_history = {9: deque([{"centroid_us": 1_000_000}]), 10: deque([{"centroid_us": 2_000_000}])}
    state._burst_records = {
        9: deque([BurstRecord(iid=9, icao="AAAAAA", centroid_us=1_000_000, n_replies=2)]),
        10: deque([BurstRecord(iid=10, icao="CCCCCC", centroid_us=3_000_000, n_replies=1)]),
    }
    state._dwell_profiles = {
        9: deque([{"icao": "AAAAAA", "beam_center_us": 1_000_000, "replies": []}]),
        10: deque([{"icao": "CCCCCC", "beam_center_us": 3_000_000, "replies": []}]),
    }
    state._last_multi_sync_update_ts = {9: 100.0, 10: 200.0}
    state._live_waveform_bins = {
        9: [WaveformBin(correction_deg=1.0, weight=2.0, n=3)],
        10: [WaveformBin(correction_deg=0.5, weight=1.0, n=2)],
    }
    state._live_icao_sync_quality = {
        9: {"AAAAAA": IcaoSyncQuality(residual_mad_deg=2.0)},
        10: {"BBBBBB": IcaoSyncQuality(residual_mad_deg=1.0)},
    }

    did_reset = state.reset_iid(9)

    assert did_reset is True
    assert 9 not in state._models
    assert 9 not in state._sweep_history
    assert 9 not in state._burst_records
    assert 9 not in state._dwell_profiles
    assert 9 not in state._last_multi_sync_update_ts
    assert 9 not in state._live_waveform_bins
    assert 9 not in state._live_icao_sync_quality
    assert all(event[1] != 9 for event in state._iid_events)
    assert 10 in state._models
    assert 10 in state._burst_records
    assert 10 in state._dwell_profiles
    assert 10 in state._last_multi_sync_update_ts
    assert 10 in state._live_waveform_bins
    assert 10 in state._live_icao_sync_quality


def test_reset_all_clears_sync_refinement_state():
    state = RadarState()
    state._models = {7: RadarIID(iid=7, period_s=4.0, primary_support_count=6)}
    state._iid_events = deque([(1_000_000, 7, "AAAAAA", None)])
    state._burst_records = {
        7: deque([BurstRecord(iid=7, icao="AAAAAA", centroid_us=1_000_000, n_replies=2)]),
    }
    state._dwell_profiles = {
        7: deque([{"icao": "AAAAAA", "beam_center_us": 1_000_000, "replies": []}]),
    }
    state._last_multi_sync_update_ts = {7: 123.0}
    state._live_waveform_bins = {7: [WaveformBin(correction_deg=1.5, weight=4.0, n=8)]}
    state._live_icao_sync_quality = {7: {"AAAAAA": IcaoSyncQuality(residual_mad_deg=3.0)}}
    state._live_sync_states = {
        7: LiveSyncState(
            iid=7,
            period_s=4.0,
            phase_epoch_us=0.0,
            phase_offset_deg=0.0,
            sync_quality=1.0,
            sync_jitter_deg=2.0,
            last_sync_update_ts=1000.0,
            source="multi_aircraft_burst",
            usable=True,
        )
    }

    cleared = state.reset_all()

    assert cleared["sync_states"] == 1
    assert cleared["burst_records"] == 1
    assert cleared["waveform_bins"] == 1
    assert cleared["icao_sync_quality"] == 1
    assert cleared["multi_sync_throttle"] == 1
    assert state._live_sync_states == {}
    assert state._burst_records == {}
    assert state._dwell_profiles == {}
    assert state._live_waveform_bins == {}
    assert state._live_icao_sync_quality == {}
    assert state._last_multi_sync_update_ts == {}


def test_detect_bursts_with_signals_refines_beam_center_toward_stronger_replies():
    bursts = detect_bursts_with_signals([
        (1_000_000, -24.0),
        (1_004_000, -6.0),
        (1_008_000, -18.0),
    ])

    assert len(bursts) == 1
    burst = bursts[0]
    assert burst["centroid_us"] == 1_004_000
    assert burst["beam_center_method"] == "amplitude_weighted"
    assert burst["beam_center_simple_us"] == pytest.approx(1_004_000)
    assert burst["beam_center_weighted_us"] == pytest.approx(burst["beam_center_us"])
    assert burst["beam_center_delta_us"] == pytest.approx(burst["beam_center_us"] - burst["beam_center_simple_us"])
    assert burst["beam_center_us"] > 1_004_000
    assert burst["beam_center_us"] < 1_005_000
    assert burst["burst_ts_first_reply_beast_us"] == pytest.approx(1_000_000)
    assert burst["burst_ts_strongest_reply_beast_us"] == pytest.approx(1_004_000)
    assert burst["burst_ts_simple_centroid_beast_us"] == pytest.approx(1_004_000)
    assert burst["burst_ts_weighted_centroid_beast_us"] == pytest.approx(burst["beam_center_us"])
    assert burst["burst_ts_mid_strong_window_beast_us"] == pytest.approx(1_004_000)
    assert burst["burst_ts_last_reply_beast_us"] == pytest.approx(1_008_000)
    assert burst["burst_span_us"] == pytest.approx(8_000)
    assert burst["peak_amplitude"] == pytest.approx(-6.0)


def test_detect_bursts_preserves_order_insensitive_grouping():
    sorted_bursts = detect_bursts([1_000_000, 1_010_000, 1_500_000, 1_510_000])
    unsorted_bursts = detect_bursts([1_500_000, 1_000_000, 1_510_000, 1_010_000])

    assert unsorted_bursts == sorted_bursts


def test_detect_bursts_with_signals_preserves_fractional_microsecond_center():
    bursts = detect_bursts_with_signals([
        (1_000_000.0, -20.0),
        (1_000_001.0, -20.0),
    ])

    assert len(bursts) == 1
    assert bursts[0]["centroid_us"] == pytest.approx(1_000_000.5)
    assert bursts[0]["beam_center_us"] == pytest.approx(1_000_000.5)


def test_on_frame_preserves_beast_sub_microsecond_resolution(monkeypatch):
    import types

    fake_decode = types.SimpleNamespace(
        decode_message=lambda raw, signal=0: {"df": 11, "addr": 0xABC123, "iid": 7}
    )
    monkeypatch.setitem(sys.modules, "decode_cffi", fake_decode)

    state = RadarState()
    state.on_frame({
        "raw": b"\x00",
        "timestamp": 13,
        "signal": 20,
    })
    state.on_frame({
        "raw": b"\x00",
        "timestamp": 26,
        "signal": 20,
    })

    assert len(state._iid_events) == 2
    arrival_us, iid, icao, signal_dbfs = state._iid_events[1]
    assert arrival_us == pytest.approx(13 / 12)
    assert iid == 7
    assert icao == "ABC123"
    assert signal_dbfs == pytest.approx(-10.0)


def test_on_df11_event_marks_iid_dirty_for_rotation_updates():
    state = RadarState()

    state.on_df11_event(
        timestamp=12_000,
        iid=7,
        icao_hex="ABC123",
        signal_dbfs=-10.0,
    )

    assert state._iid_events[-1][1] == 7
    assert 7 in state._dirty_iids


def test_on_df11_batch_marks_iids_dirty_for_rotation_updates():
    state = RadarState()

    state.on_df11_batch([
        (12_000, 7, "ABC123", -10.0),
        (12_120, 8, "DEF456", -12.0),
    ])

    assert {event[1] for event in state._iid_events} == {7, 8}
    assert state._dirty_iids == {7, 8}


def test_get_iid_latest_arrival_us_tracks_and_clears_per_iid():
    state = RadarState()

    state.on_df11_batch([
        (12_000, 7, "ABC123", -10.0),
        (24_000, 7, "ABC123", -9.0),
        (36_000, 8, "DEF456", -12.0),
    ])

    assert state.get_iid_latest_arrival_us(7) == pytest.approx(1_000.0)
    assert state.get_iid_latest_arrival_us(8) == pytest.approx(2_000.0)

    state.reset_iid(7)

    assert state.get_iid_latest_arrival_us(7) is None
    assert state.get_iid_latest_arrival_us(8) == pytest.approx(2_000.0)


def test_get_iid_timeline_returns_recent_selected_iid_in_ascending_order():
    state = RadarState()
    # Entries must be in centroid_us order; last entry defines "now" for window cutoff
    state._burst_records[7] = deque([
        BurstRecord(iid=7, icao="OLD777", centroid_us=1_000_000, n_replies=1),
        BurstRecord(iid=7, icao="ABC123", centroid_us=9_000_000, n_replies=1),
        BurstRecord(iid=7, icao="ABC123", centroid_us=10_000_000, n_replies=1),
        BurstRecord(iid=7, icao="DEF456", centroid_us=11_000_000, n_replies=1),
    ])
    state._burst_records[8] = deque([
        BurstRecord(iid=8, icao="OTHER8", centroid_us=8_000_000, n_replies=1),
    ])

    timeline = state.get_iid_timeline(7, window_s=3.0)

    assert timeline == {
        "ABC123": [9_000_000, 10_000_000],
        "DEF456": [11_000_000],
    }


def test_get_burst_sync_timeline_includes_non_sync_driving_observations():
    state = RadarState()
    now_ts = 1_000.0
    state._live_sync_states[7] = LiveSyncState(
        iid=7,
        period_s=4.0,
        phase_epoch_us=0.0,
        phase_offset_deg=0.0,
        sync_quality=1.0,
        sync_jitter_deg=3.0,
        last_sync_update_ts=now_ts,
        source="multi_aircraft_burst",
        usable=True,
    )
    state._live_burst_timeline_obs[7] = deque([
        AlignedBurstSyncObs(
            burst_centroid_us=4_100_000.0,
            icao="AAAAAA",
            bearing_deg=7.0,
            n_replies=3,
            signal_dbfs=-18.0,
            pos_age_s=0.4,
            range_nm=12.0,
            ts=now_ts - 5.0,
            sync_update_eligible=False,
        ),
        AlignedBurstSyncObs(
            burst_centroid_us=8_200_000.0,
            icao="BBBBBB",
            bearing_deg=20.0,
            n_replies=4,
            signal_dbfs=-15.0,
            pos_age_s=0.3,
            range_nm=18.0,
            ts=now_ts - 4.0,
            sync_update_eligible=True,
        ),
    ], maxlen=state._BURST_SYNC_TIMELINE_OBS_MAX)

    with pytest.MonkeyPatch.context() as monkeypatch:
        monkeypatch.setattr("radar.sweep.time.time", lambda: now_ts)
        timeline = state.get_burst_sync_timeline(7, window_s=60.0)

    observations = timeline["observations"]
    assert len(observations) == 2
    assert observations[0]["icao"] == "AAAAAA"
    assert observations[0]["sync_update_eligible"] is False
    assert observations[1]["icao"] == "BBBBBB"
    assert observations[1]["sync_update_eligible"] is True
    assert "fit_eligible" in observations[1]
    assert "predicted_corrected_deg" in observations[1]
    assert timeline["predictor_consistency"] is not None
    retention = timeline["retention_diagnostics"]
    assert retention["iid"] == 7
    assert retention["retention_target_s"] >= 300.0
    assert retention["aligned"]["count"] == 0
    assert retention["timeline"]["count"] == 2
    assert retention["timeline"]["oldest_burst_centroid_us"] == pytest.approx(4_100_000.0)
    assert retention["timeline"]["newest_burst_centroid_us"] == pytest.approx(8_200_000.0)
    assert retention["timeline"]["retained_duration_s"] == pytest.approx(4.1)


def test_live_sync_observation_buffers_prune_by_age_with_high_count_caps(monkeypatch):
    state = RadarState()
    assert state._MULTI_SYNC_OBS_MAX > 200
    assert state._BURST_SYNC_TIMELINE_OBS_MAX > 200
    state._LIVE_SYNC_OBS_RETENTION_S = 90.0

    now = {"ts": 1_000.0}
    monkeypatch.setattr("radar.sweep.time.time", lambda: now["ts"])

    samples = [
        (900.0, 4_000_000.0),
        (930.0, 7_000_000.0),
        (1_000.0, 11_000_000.0),
    ]
    for wall_ts, burst_us in samples:
        now["ts"] = wall_ts
        state._record_aligned_burst_sync_obs(
            iid=11,
            icao="AAAAAA",
            burst_centroid_us=burst_us,
            radar_lat=51.0,
            radar_lon=-0.1,
            aircraft_lat=51.2,
            aircraft_lon=0.0,
            n_replies=4,
            signal_dbfs=-14.0,
            pos_age_s=0.4,
            period_s=0.0,
        )
        state._record_burst_sync_timeline_obs(
            iid=11,
            icao="AAAAAA",
            burst_centroid_us=burst_us,
            radar_lat=51.0,
            radar_lon=-0.1,
            aircraft_lat=51.2,
            aircraft_lon=0.0,
            n_replies=4,
            signal_dbfs=-14.0,
            pos_age_s=0.4,
            sync_update_eligible=True,
        )

    aligned = list(state._live_aligned_burst_obs[11])
    timeline = list(state._live_burst_timeline_obs[11])
    assert len(aligned) == 2
    assert len(timeline) == 2
    assert aligned[0].burst_centroid_us == pytest.approx(7_000_000.0)
    assert timeline[0].burst_centroid_us == pytest.approx(7_000_000.0)
    assert aligned[-1].burst_centroid_us == pytest.approx(11_000_000.0)
    assert timeline[-1].burst_centroid_us == pytest.approx(11_000_000.0)


def test_get_sync_debug_payload_compares_predictor_paths_on_same_observation(monkeypatch):
    state = RadarState()
    now_ts = 1_000.0
    state._iid_latest_arrival_us[7] = 8_200_000.0
    state._live_sync_states[7] = LiveSyncState(
        iid=7,
        period_s=4.0,
        phase_epoch_us=0.0,
        phase_offset_deg=0.0,
        sync_quality=1.0,
        sync_jitter_deg=3.0,
        last_sync_update_ts=now_ts,
        source="multi_aircraft_burst",
        usable=True,
        prop_delay_enabled=True,
    )
    state._live_burst_timeline_obs[7] = deque([
        AlignedBurstSyncObs(
            burst_centroid_us=8_200_000.0,
            icao="BBBBBB",
            bearing_deg=20.0,
            n_replies=4,
            signal_dbfs=-15.0,
            pos_age_s=0.3,
            range_nm=18.0,
            ts=now_ts,
            sync_update_eligible=True,
            raw_arrival_us=8_200_000.0,
            burst_center_method="amplitude_weighted",
            burst_ts_first_reply_beast_us=8_190_000.0,
            burst_ts_strongest_reply_beast_us=8_200_000.0,
            burst_ts_simple_centroid_beast_us=8_198_000.0,
            burst_ts_weighted_centroid_beast_us=8_200_000.0,
            burst_ts_mid_strong_window_beast_us=8_200_000.0,
            burst_ts_last_reply_beast_us=8_206_000.0,
            burst_span_us=16_000.0,
            peak_amplitude=-12.0,
            position_interpolated=True,
            position_extrapolated=True,
            position_source_age_s=0.3,
            truth_position_ts_beast_us=7_900_000.0,
        ),
    ], maxlen=state._BURST_SYNC_TIMELINE_OBS_MAX)

    monkeypatch.setattr("radar.sweep.time.time", lambda: now_ts)
    payload = state.get_sync_debug_payload(7, window_s=60.0)

    assert payload["summary"]["wall_clock_used_operationally"] is False
    assert payload["summary"]["operational_time_basis"] == "effective_beast_us"
    assert payload["summary"]["predictors_consistent_localiser"] is True
    assert payload["summary"]["predictors_consistent_position_verification"] is True
    assert payload["summary"]["predictors_consistent_burst_sync"] is True

    obs = payload["observations"][0]
    assert obs["raw_arrival_beast_us"] == pytest.approx(8_200_000.0)
    assert obs["burst_center_beast_us"] == pytest.approx(8_200_000.0)
    assert obs["effective_beast_us"] < obs["burst_center_beast_us"]
    assert obs["prop_delay_us"] > 0.0
    assert obs["pred_authoritative_deg"] == pytest.approx(obs["pred_using_effective_beast_deg"])
    assert obs["pred_using_wall_clock_deg"] is not None
    assert obs["wall_to_beast_roundtrip_error_us"] == pytest.approx(0.0)
    assert obs["delta_localiser_vs_authoritative_deg"] == pytest.approx(0.0)
    assert obs["delta_position_vs_authoritative_deg"] == pytest.approx(0.0)
    assert obs["delta_burstsync_vs_authoritative_deg"] == pytest.approx(0.0)
    assert obs["fit_eligible"] is True
    assert "bearing_rate_deg_s" in obs
    assert "motion_comp_dt_us" in obs
    assert "motion_comp_applied" in obs
    assert "resid_without_motion_deg" in obs
    assert "resid_with_motion_deg" in obs
    assert "motion_comp_improvement_deg" in obs
    assert obs["residual_raw_deg"] == pytest.approx(obs["resid_authoritative_deg"])
    assert obs["residual_detrended_deg"] is not None
    assert obs["detrend_component_deg"] is not None
    assert obs["fit_slope_deg_per_s"] is not None
    assert obs["fit_time_origin_beast_us"] is not None
    assert obs["phase_deg"] == pytest.approx(obs["phase_authoritative_deg"])
    assert obs["cycle_index"] is not None
    assert obs["cycle_start_beast_us"] is not None
    assert obs["burst_ts_first_reply_beast_us"] == pytest.approx(8_190_000.0)
    assert obs["resid_first_reply_deg"] is not None
    assert obs["resid_weighted_centroid_deg"] == pytest.approx(obs["resid_authoritative_deg"])
    assert obs["burst_span_us"] == pytest.approx(16_000.0)
    assert obs["peak_amplitude"] == pytest.approx(-12.0)
    assert obs["position_interpolated"] is True
    assert obs["position_extrapolated"] is True
    assert obs["position_age_ms"] == pytest.approx(300.0)
    assert obs["position_source_age_ms"] == pytest.approx(300.0)
    assert obs["truth_position_ts_beast_us"] == pytest.approx(7_900_000.0)
    diag = payload["observation_model_diagnostics"]
    assert diag["operational_burst_timestamp_method"] == "amplitude_weighted"
    assert diag["best_diagnostic_burst_timestamp_method"] is not None
    assert diag["method_summary_overall"]
    assert diag["method_summary_fit_driving"]
    assert diag["folded_phase_shape"]["phase_bins"]
    assert diag["dominant_error_mode"] in {
        "period_drift",
        "repeatable_phase_shape",
        "unstable_cycle_shape",
        "mixed",
    }
    assert diag["bins"]["position_age"]
    assert diag["per_icao"][0]["icao"] == "BBBBBB"
    assert payload["summary"]["observation_model_diagnosis"]["likely_contributors"]
    assert payload["summary"]["fit_time_origin_beast_us"] is not None
    assert payload["summary"]["raw_median_abs_residual_deg"] is not None
    assert payload["summary"]["detrended_median_abs_residual_deg"] is not None
    assert payload["summary"]["dominant_error_mode"] == diag["dominant_error_mode"]
    assert payload["retention_diagnostics"]["timeline"]["count"] == 1
    assert payload["summary"]["retention_diagnostics"]["timeline"]["count"] == 1


def test_native_burst_path_populates_observation_model_timestamp_candidates(monkeypatch):
    import radar.sweep as sweep_module

    class FakeNativeBurstProcessor:
        def __init__(self):
            self.pending = {}

        def process_batch(self, events, burst_gap_us):
            fired = []
            for arrival_us, icao, signal_dbfs in events:
                replies = self.pending.get(icao, [])
                if replies and arrival_us - replies[-1][0] > burst_gap_us:
                    fired.append({
                        "icao": icao,
                        "burst_centroid_us": 1_003_000.0,
                        "burst_signal": max(s for _arrival, s in replies if s is not None),
                        "trigger_arrival_us": arrival_us,
                        "n_replies": len(replies),
                    })
                    replies = []
                replies.append((arrival_us, signal_dbfs))
                self.pending[icao] = replies
            return fired

        def matches_dominant_period(self, *args, **kwargs):
            return True

        def select_reference(self, **kwargs):
            return "AAAAAA"

    monkeypatch.setattr(
        sweep_module,
        "_decode_cffi",
        SimpleNamespace(RadarBurstProcessor=FakeNativeBurstProcessor),
    )
    monkeypatch.setattr(sweep_module.time, "time", lambda: 1_000.0)

    state = RadarState()
    state._models[31] = RadarIID(
        iid=31,
        period_s=4.0,
        manual_lat=51.0,
        manual_lon=-1.0,
        resolution_mode="locked_position",
    )
    state._live_sync_states[31] = LiveSyncState(
        iid=31,
        period_s=4.0,
        phase_epoch_us=0.0,
        phase_offset_deg=0.0,
        sync_quality=1.0,
        sync_jitter_deg=3.0,
        last_sync_update_ts=1_000.0,
        source="multi_aircraft_burst",
        usable=True,
    )
    state._adsb_tracker.update("AAAAAA", 51.1, -1.1, ts=1_000.0)

    state.on_df11_batch([
        (int(1_000_000.0 * 12), 31, "AAAAAA", -24.0),
        (int(1_004_000.0 * 12), 31, "AAAAAA", -6.0),
        (int(1_008_000.0 * 12), 31, "AAAAAA", -18.0),
    ])
    state.on_df11_batch([
        (int(1_500_000.0 * 12), 31, "AAAAAA", -12.0),
    ])

    payload = state.get_sync_debug_payload(31, window_s=60.0)

    assert payload["observations"]
    obs = payload["observations"][0]
    assert obs["burst_ts_first_reply_beast_us"] == pytest.approx(0.0)
    assert obs["burst_ts_strongest_reply_beast_us"] == pytest.approx(4_000.0)
    assert obs["burst_ts_simple_centroid_beast_us"] == pytest.approx(4_000.0)
    assert obs["burst_ts_last_reply_beast_us"] == pytest.approx(8_000.0)
    assert obs["resid_first_reply_deg"] is not None
    assert obs["phase_first_reply_deg"] is not None
    assert obs["resid_improvement_first_reply_deg"] is not None
    diag = payload["observation_model_diagnostics"]
    assert "first_reply" in diag["available_burst_timestamp_methods"]
    assert "simple_centroid" in diag["available_burst_timestamp_methods"]
    assert diag["method_summary_overall"][0]["count"] > 0
    assert diag["best_diagnostic_burst_timestamp_method"] is not None


def test_authoritative_sync_predictor_applies_prop_and_waveform():
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
    bins = [WaveformBin(correction_deg=10.0, weight=10.0, n=10) for _ in range(24)]

    prediction = predict_sync_observation(sync, 5_000_000.0, range_nm=10.0, waveform_bins=bins)
    no_prop = predict_sync_observation(
        sync,
        5_000_000.0,
        range_nm=10.0,
        waveform_bins=bins,
        apply_propagation=False,
        apply_waveform=False,
    )

    assert prediction.effective_arrival_us < 5_000_000.0
    assert prediction.propagation_correction_us > 0.0
    assert prediction.waveform_correction_deg == pytest.approx(10.0)
    assert prediction.predicted_bearing_deg == pytest.approx(
        (prediction.predicted_bearing_raw_deg - 10.0) % 360.0
    )
    assert no_prop.predicted_bearing_deg == pytest.approx(185.0)


def test_authoritative_sync_predictor_applies_motion_compensation():
    sync = LiveSyncState(
        iid=7,
        period_s=4.0,
        phase_epoch_us=0.0,
        phase_offset_deg=0.0,
        sync_quality=1.0,
        sync_jitter_deg=2.0,
        last_sync_update_ts=1000.0,
        source="multi_aircraft_burst",
        usable=True,
        motion_comp_phase_enabled=True,
    )

    without_motion = predict_sync_observation(
        sync,
        4_000_000.0,
        apply_propagation=False,
        apply_waveform=False,
        apply_motion=False,
    )
    with_motion = predict_sync_observation(
        sync,
        4_000_000.0,
        apply_propagation=False,
        apply_waveform=False,
        bearing_rate_deg_s=1.0,
    )

    assert with_motion.motion_comp_applied is True
    assert with_motion.motion_comp_dt_us == pytest.approx((1.0 / 360.0) * 16.0 * 1_000_000.0)
    assert with_motion.effective_arrival_us < without_motion.effective_arrival_us
    assert ((with_motion.predicted_bearing_deg - without_motion.predicted_bearing_deg + 540.0) % 360.0 - 180.0) == pytest.approx(-4.0)


def test_period_refinement_uses_effective_time_slope_and_correct_sign(monkeypatch):
    import radar.sweep as sweep_module

    monkeypatch.setattr(sweep_module.time, "time", lambda: 1_000.0)

    state = RadarState()
    # Seed the sync state with a non-zero smoothed slope so the EMA starts warm.
    # The persistence gate requires 5/8 history entries to agree in sign and
    # exceed the dead-band, so we also pre-populate slope history below.
    state._live_sync_states[7] = LiveSyncState(
        iid=7,
        period_s=10.0,
        phase_epoch_us=0.0,
        phase_offset_deg=0.0,
        sync_quality=1.0,
        sync_jitter_deg=2.0,
        last_sync_update_ts=999.0,
        source="multi_aircraft_burst",
        usable=True,
        period_base_s=10.0,
        prop_delay_enabled=True,
        period_refine_enabled=True,
        residual_slope_deg_per_s=0.5,   # pre-warm the slope EMA
    )
    # Pre-populate slope history with 6 consistent positive entries so the
    # persistence gate (≥5/8 agree, smoothed ≥ dead-band) will open.
    from collections import deque as _deque
    state._live_slope_history[7] = _deque(maxlen=80)
    for _ in range(6):
        state._live_slope_history[7].append({
            "ts": 998.0,
            "residual_slope_deg_per_s": 0.5,
            "raw_slope_deg_per_s": 0.5,
            "fit_span_s": 28.0,
            "n_fit_observations": 8,
        })

    obs = deque(maxlen=state._MULTI_SYNC_OBS_MAX)
    for idx, t_s in enumerate([0, 4, 8, 12, 16, 20, 24, 28]):
        icao = "AAAAAA" if idx % 2 == 0 else "BBBBBB"
        predicted = ((t_s / 10.0) * 360.0) % 360.0
        residual = 0.5 * t_s
        obs.append(AlignedBurstSyncObs(
            burst_centroid_us=t_s * 1_000_000.0,
            icao=icao,
            bearing_deg=(predicted + residual) % 360.0,
            n_replies=4,
            signal_dbfs=-12.0,
            pos_age_s=0.2,
            range_nm=0.0,
            ts=995.0 + idx * 0.5,
            sync_update_eligible=True,
        ))
    state._live_aligned_burst_obs[7] = obs

    state._update_multi_aircraft_sync_state(7, period_s=10.0)

    sync = state.get_live_sync_state(7)
    # residual_slope_deg_per_s is now the EMA-smoothed value.  Starting from 0.5
    # and blending b_fit ≈ 0.5 with α=0.08, the result stays close to 0.5.
    assert sync.residual_slope_deg_per_s == pytest.approx(0.5, rel=0.1)
    assert sync.period_s < 10.0
    assert sync.period_update_direction == "decrease"
    assert sync.period_update_applied < 0.0
    assert sync.period_update_proposed_s < 0.0
    assert sync.period_update_applied_s == pytest.approx(sync.period_update_applied)
    assert sync.period_update_allowed is True
    assert sync.period_update_block_reason is None
    assert sync.period_update_fit_support == 8
    assert sync.period_update_fit_span_s == pytest.approx(sync.fit_span_s)
    assert sync.period_correction_status in {"converging", "clamp_limited"}
    assert sync.period_update_gain == pytest.approx(0.12)
    assert sync.fit_eligible_observations == 8
    assert sync.fit_time_basis == "effective_beast_time_s"
    assert sync.period_refine_mode == "normal"
    assert sync.period_authoritative_source == "refined"
    assert sync.period_reacquire_active is False
    timeline = state.get_burst_sync_timeline(7, window_s=60.0)
    assert timeline["period_update_history"]
    update = timeline["period_update_history"][-1]
    assert update["period_s_before"] == pytest.approx(10.0)
    assert update["period_s_after"] == pytest.approx(sync.period_s)
    assert "period_update_clamp_reason" in update


def test_period_failure_immediately_reacquires_on_high_reject_fraction_and_low_fit_support(monkeypatch):
    import radar.sweep as sweep_module

    monkeypatch.setattr(sweep_module.time, "time", lambda: 1_000.0)

    state = RadarState()
    state._live_sync_states[7] = LiveSyncState(
        iid=7,
        period_s=10.3,
        phase_epoch_us=0.0,
        phase_offset_deg=0.0,
        sync_quality=1.0,
        sync_jitter_deg=2.0,
        last_sync_update_ts=999.0,
        source="multi_aircraft_burst",
        usable=True,
        period_base_s=10.0,
    )
    state._live_aligned_burst_obs[7] = _make_aligned_sync_obs(
        state,
        period_s=10.0,
        residuals_deg=[90.0, 90.0, 90.0, 90.0, 90.0],
        icaos=("AAAAAA",),
    )

    state._update_multi_aircraft_sync_state(7, period_s=10.0)

    sync = state.get_live_sync_state(7)
    assert sync.period_reacquire_active is True
    assert sync.period_refine_mode == "reacquire"
    assert sync.period_authoritative_source == "base"
    assert sync.period_reacquire_reason == "high_rejected_fraction,low_fit_support,single_icao_fit"
    assert sync.period_s == pytest.approx(10.0)
    assert sync.period_failure_score >= 5.0


def test_period_failure_reacquires_after_repeated_suspect_updates(monkeypatch):
    import radar.sweep as sweep_module

    monkeypatch.setattr(sweep_module.time, "time", lambda: 1_000.0)

    state = RadarState()
    state._live_sync_states[7] = LiveSyncState(
        iid=7,
        period_s=10.2,
        phase_epoch_us=0.0,
        phase_offset_deg=0.0,
        sync_quality=1.0,
        sync_jitter_deg=2.0,
        last_sync_update_ts=999.0,
        source="multi_aircraft_burst",
        usable=True,
        period_base_s=10.0,
        residual_slope_deg_per_s=0.0,
    )

    streaks = []
    modes = []
    reacquire_flags = []
    for attempt in range(3):
        state._live_aligned_burst_obs[7] = _make_aligned_sync_obs(
            state,
            period_s=10.0,
            residuals_deg=[25.0] * 8,
            start_s=attempt * 40.0,
        )
        state.update_live_sync_state(7, phase_epoch_us=0.0, phase_offset_deg=0.0)
        state._update_multi_aircraft_sync_state(7, period_s=10.0)
        sync = state.get_live_sync_state(7)
        streaks.append(sync.period_failure_streak)
        modes.append(sync.period_refine_mode)
        reacquire_flags.append(sync.period_reacquire_active)

    sync = state.get_live_sync_state(7)
    assert streaks[0] == 1
    assert modes[0] == "suspect"
    assert reacquire_flags[0] is False
    assert sync.period_reacquire_active is True
    assert sync.period_refine_mode == "reacquire"
    assert sync.period_authoritative_source == "base"
    assert sync.period_s == pytest.approx(10.0)


def test_authoritative_frame_period_uses_base_while_reacquiring():
    state = RadarState()
    state._live_sync_states[7] = LiveSyncState(
        iid=7,
        period_s=10.4,
        phase_epoch_us=0.0,
        phase_offset_deg=0.0,
        sync_quality=1.0,
        sync_jitter_deg=2.0,
        last_sync_update_ts=999.0,
        source="multi_aircraft_burst",
        usable=True,
        period_base_s=10.0,
        period_reacquire_active=True,
        period_refine_mode="reacquire",
        period_authoritative_source="base",
    )

    assert state._get_authoritative_frame_period_s(7, 9.8) == pytest.approx(10.0)
    assert state.get_authoritative_display_period_s(7) == pytest.approx(10.0)
    assert state.get_authoritative_display_period_std_s(7) == pytest.approx((2.0 / 360.0) * 10.0)


def test_period_reacquire_exits_only_after_two_clean_updates(monkeypatch):
    import radar.sweep as sweep_module

    now = {"ts": 1_000.0}
    monkeypatch.setattr(sweep_module.time, "time", lambda: now["ts"])

    state = RadarState()
    state._live_sync_states[7] = LiveSyncState(
        iid=7,
        period_s=10.0,
        phase_epoch_us=0.0,
        phase_offset_deg=0.0,
        sync_quality=1.0,
        sync_jitter_deg=2.0,
        last_sync_update_ts=999.0,
        source="multi_aircraft_burst",
        usable=True,
        period_base_s=10.0,
        period_failure_streak=3,
        period_reacquire_active=True,
        period_reacquire_reason="high_rejected_fraction",
        period_reacquire_started_ts=990.0,
        period_refine_mode="reacquire",
        period_authoritative_source="base",
    )
    clean_obs = _make_aligned_sync_obs(
        state,
        period_s=10.0,
        residuals_deg=[3.0, 2.0, 4.0, 3.0, 2.0, 4.0, 3.0, 2.0],
    )
    state._live_aligned_burst_obs[7] = clean_obs

    state._update_multi_aircraft_sync_state(7, period_s=10.0)
    first = state.get_live_sync_state(7)
    assert first.period_reacquire_active is True
    assert first.period_authoritative_source == "base"
    assert first.period_reacquire_reason == "high_rejected_fraction"

    now["ts"] = 1_001.0
    state._update_multi_aircraft_sync_state(7, period_s=10.0)
    second = state.get_live_sync_state(7)
    assert second.period_reacquire_active is False
    assert second.period_refine_mode == "normal"
    assert second.period_authoritative_source == "refined"
    assert second.period_reacquire_reason is None
    assert second.period_failure_streak == 0


def test_mild_noise_does_not_flap_into_reacquire(monkeypatch):
    import radar.sweep as sweep_module

    monkeypatch.setattr(sweep_module.time, "time", lambda: 1_000.0)

    state = RadarState()
    state._live_sync_states[7] = LiveSyncState(
        iid=7,
        period_s=10.0,
        phase_epoch_us=0.0,
        phase_offset_deg=0.0,
        sync_quality=1.0,
        sync_jitter_deg=2.0,
        last_sync_update_ts=999.0,
        source="multi_aircraft_burst",
        usable=True,
        period_base_s=10.0,
    )
    state._live_aligned_burst_obs[7] = _make_aligned_sync_obs(
        state,
        period_s=10.0,
        residuals_deg=[2.0, -3.0, 4.0, -2.0, 3.0, -4.0, 2.0, -1.0],
    )

    state._update_multi_aircraft_sync_state(7, period_s=10.0)

    sync = state.get_live_sync_state(7)
    assert sync.period_update_block_reason == "slope_not_persistent"
    assert sync.period_refine_mode == "normal"
    assert sync.period_reacquire_active is False
    assert sync.period_failure_streak == 0


def test_live_sync_snapshot_reuses_cached_payload_until_sync_inputs_change(monkeypatch):
    import config as _cfg
    import radar.sweep as sweep_module

    monkeypatch.setattr(_cfg, "RADAR_DIAGNOSTICS", True)
    monkeypatch.setattr("radar.sweep.RADAR_DIAGNOSTICS", True)
    monkeypatch.setattr(sweep_module.time, "time", lambda: 1_000.0)

    state = RadarState()
    state._iid_latest_arrival_us[7] = 4_000_000.0
    state._live_sync_states[7] = LiveSyncState(
        iid=7,
        period_s=4.0,
        phase_epoch_us=0.0,
        phase_offset_deg=0.0,
        sync_quality=1.0,
        sync_jitter_deg=2.0,
        last_sync_update_ts=999.0,
        source="multi_aircraft_burst",
        usable=True,
        period_base_s=4.0,
    )
    state._live_burst_timeline_obs[7] = deque([
        AlignedBurstSyncObs(
            burst_centroid_us=4_000_000.0,
            icao="AAAAAA",
            bearing_deg=0.0,
            n_replies=4,
            signal_dbfs=-12.0,
            pos_age_s=0.2,
            range_nm=0.0,
            ts=999.0,
            sync_update_eligible=True,
        )
    ], maxlen=state._BURST_SYNC_TIMELINE_OBS_MAX)

    first = state.get_live_sync_snapshot(7, window_s=90.0, debug_limit=20)
    second = state.get_live_sync_snapshot(7, window_s=90.0, debug_limit=20)

    assert first is second
    assert first["sequence"] == second["sequence"]
    assert first["type"] == "radar_sync"
    assert "waveform_bins" in first
    assert "phase_anchor_candidates" in first
    assert first["retention_diagnostics"]["timeline"]["count"] == 1
    assert "period_refine_mode" in first["sync_state"]
    assert "period_authoritative_source" in first["sync_state"]
    assert "period_failure_score" in first["sync_state"]
    assert "period_reacquire_active" in first["sync_state"]


def test_phase_anchor_selected_aircraft_recovers_wrong_absolute_branch(monkeypatch):
    import radar.sweep as sweep_module

    monkeypatch.setattr(sweep_module.time, "time", lambda: 1_000.0)

    state = RadarState()
    state._live_sync_states[7] = LiveSyncState(
        iid=7,
        period_s=10.0,
        phase_epoch_us=0.0,
        phase_offset_deg=0.0,
        sync_quality=1.0,
        sync_jitter_deg=2.0,
        last_sync_update_ts=999.0,
        source="multi_aircraft_burst",
        usable=True,
        period_base_s=10.0,
    )
    obs = deque(maxlen=state._MULTI_SYNC_OBS_MAX)
    # One aircraft has a tight implied absolute offset near 120 degrees.  From
    # the old mixed-branch state these look like large residual outliers, so the
    # anchor path must not depend on the old residual gate.
    for idx, t_s in enumerate([0.0, 10.0, 20.0, 30.0]):
        obs.append(AlignedBurstSyncObs(
            burst_centroid_us=t_s * 1_000_000.0,
            icao="AAAAAA",
            bearing_deg=120.0,
            n_replies=5,
            signal_dbfs=-12.0,
            pos_age_s=0.2,
            range_nm=0.0,
            ts=997.0 + idx,
            sync_update_eligible=True,
        ))
    state._live_aligned_burst_obs[7] = obs

    state._update_multi_aircraft_sync_state(7, period_s=10.0)

    sync = state.get_live_sync_state(7)
    assert sync.phase_anchor_icao == "AAAAAA"
    assert sync.phase_anchor_status == "anchor_only"
    assert sync.phase_anchor_obs_count == 4
    assert sync.phase_anchor_offset_raw_deg == pytest.approx(120.0)
    assert sync.phase_offset_deg == pytest.approx(96.0)
    assert sync.phase_validation_status == "anchor_only"
    assert sync.period_refine_block_reason == "majority_rejected"
    timeline = state.get_burst_sync_timeline(7, window_s=60.0)
    assert timeline["phase_anchor_candidates"][0]["icao"] == "AAAAAA"
    assert timeline["observations"][0]["implied_phase_offset_deg"] == pytest.approx(120.0)
    assert timeline["observations"][0]["phase_anchor_contributor"] is True


def test_phase_anchor_quality_memory_is_warning_not_hard_reject(monkeypatch):
    import radar.sweep as sweep_module

    monkeypatch.setattr(sweep_module.time, "time", lambda: 1_000.0)

    state = RadarState()
    state._live_sync_states[7] = LiveSyncState(
        iid=7,
        period_s=10.0,
        phase_epoch_us=0.0,
        phase_offset_deg=0.0,
        sync_quality=1.0,
        sync_jitter_deg=2.0,
        last_sync_update_ts=999.0,
        source="multi_aircraft_burst",
        usable=True,
        period_base_s=10.0,
    )
    state._live_icao_sync_quality[7] = {
        "AAAAAA": IcaoSyncQuality(
            residual_median_deg=95.0,
            residual_mad_deg=35.0,
            n_recent=12,
            last_ts=999.0,
        ),
    }
    obs = deque(maxlen=state._MULTI_SYNC_OBS_MAX)
    for idx, t_s in enumerate([0.0, 10.0, 20.0, 30.0]):
        obs.append(AlignedBurstSyncObs(
            burst_centroid_us=t_s * 1_000_000.0,
            icao="AAAAAA",
            bearing_deg=110.0,
            n_replies=5,
            signal_dbfs=-12.0,
            pos_age_s=0.2,
            range_nm=0.0,
            ts=997.0 + idx,
            sync_update_eligible=True,
        ))
    state._live_aligned_burst_obs[7] = obs

    state._update_multi_aircraft_sync_state(7, period_s=10.0)

    sync = state.get_live_sync_state(7)
    assert sync.phase_anchor_icao == "AAAAAA"
    assert sync.phase_anchor_offset_raw_deg == pytest.approx(110.0)
    candidate = sync.phase_anchor_candidates[0]
    assert candidate["status"] == "selected"
    assert candidate["reject_reasons"] == []
    assert "poor_icao_quality_memory" in candidate["warning_reasons"]


def test_phase_anchor_uses_population_only_as_small_validation_nudge(monkeypatch):
    import radar.sweep as sweep_module

    monkeypatch.setattr(sweep_module.time, "time", lambda: 1_000.0)

    state = RadarState()
    state._live_sync_states[7] = LiveSyncState(
        iid=7,
        period_s=10.0,
        phase_epoch_us=0.0,
        phase_offset_deg=0.0,
        sync_quality=1.0,
        sync_jitter_deg=2.0,
        last_sync_update_ts=999.0,
        source="multi_aircraft_burst",
        usable=True,
        period_base_s=10.0,
    )
    obs = deque(maxlen=state._MULTI_SYNC_OBS_MAX)
    for idx, t_s in enumerate([0.0, 10.0, 20.0, 30.0, 40.0]):
        obs.append(AlignedBurstSyncObs(
            burst_centroid_us=t_s * 1_000_000.0,
            icao="ANCHOR",
            bearing_deg=30.0,
            n_replies=5,
            signal_dbfs=-10.0,
            pos_age_s=0.2,
            range_nm=0.0,
            ts=995.0 + idx,
            sync_update_eligible=True,
        ))
    for idx, t_s in enumerate([5.0, 15.0, 25.0]):
        # Validator agrees within +8 degrees.  It may nudge the branch by
        # roughly 1.2 degrees, not replace the anchor with its own offset.
        obs.append(AlignedBurstSyncObs(
            burst_centroid_us=t_s * 1_000_000.0,
            icao="BBBBBB",
            bearing_deg=(30.0 + ((t_s / 10.0) * 360.0) + 8.0) % 360.0,
            n_replies=4,
            signal_dbfs=-14.0,
            pos_age_s=0.2,
            range_nm=0.0,
            ts=997.0 + idx,
            sync_update_eligible=True,
        ))
    state._live_aligned_burst_obs[7] = obs

    state._update_multi_aircraft_sync_state(7, period_s=10.0)

    sync = state.get_live_sync_state(7)
    assert sync.phase_anchor_icao == "ANCHOR"
    assert sync.phase_anchor_offset_raw_deg == pytest.approx(30.0)
    assert sync.phase_validation_status == "confirmed"
    assert sync.phase_validation_contributors == 1
    assert sync.phase_validation_median_error_deg == pytest.approx(8.0)
    assert sync.phase_offset_deg == pytest.approx(25.2)


def test_period_fit_rejects_large_residuals_without_hiding_timeline(monkeypatch):
    import radar.sweep as sweep_module

    monkeypatch.setattr(sweep_module.time, "time", lambda: 1_000.0)

    state = RadarState()
    state._live_sync_states[7] = LiveSyncState(
        iid=7,
        period_s=10.0,
        phase_epoch_us=0.0,
        phase_offset_deg=0.0,
        sync_quality=1.0,
        sync_jitter_deg=2.0,
        last_sync_update_ts=999.0,
        source="multi_aircraft_burst",
        usable=True,
        period_base_s=10.0,
    )
    state._live_burst_timeline_obs[7] = deque([
        AlignedBurstSyncObs(
            burst_centroid_us=1_000_000.0,
            icao="AAAAAA",
            bearing_deg=150.0,
            n_replies=4,
            signal_dbfs=-12.0,
            pos_age_s=0.2,
            range_nm=5.0,
            ts=999.0,
            sync_update_eligible=True,
        )
    ], maxlen=state._BURST_SYNC_TIMELINE_OBS_MAX)

    timeline = state.get_burst_sync_timeline(7, window_s=60.0)

    assert len(timeline["observations"]) == 1
    assert timeline["observations"][0]["fit_eligible"] is False
    assert timeline["observations"][0]["fit_reject_reason"] == "residual_gate"


def test_update_rotation_models_defers_recently_stable_iids(monkeypatch):
    import radar.sweep as sweep_module

    monkeypatch.setattr(sweep_module.time, "time", lambda: 1_000.0)

    state = RadarState()
    state._iid_events = deque([
        (1_000_000, 7, "AAAAAA", None),
        (5_000_000, 7, "AAAAAA", None),
    ])
    state._dirty_iids = {7}
    state._models = {
        7: RadarIID(iid=7, status="SINGLE_RADAR", period_s=4.0, last_updated=950.0),
    }

    monkeypatch.setattr(sweep_module, "_analyse_iid_events", lambda events: (_ for _ in ()).throw(AssertionError("unexpected reanalysis")))

    state.update_rotation_models()

    assert 7 in state._dirty_iids


def test_update_rotation_models_requeues_iids_left_outside_runtime_budget(monkeypatch):
    import radar.sweep as sweep_module

    state = RadarState()
    state._burst_records[7] = deque([
        BurstRecord(iid=7, icao="AAAAAA", centroid_us=1_000_000, n_replies=1),
        BurstRecord(iid=7, icao="AAAAAA", centroid_us=3_000_000, n_replies=1),
    ])
    state._burst_records[8] = deque([
        BurstRecord(iid=8, icao="BBBBBB", centroid_us=2_000_000, n_replies=1),
        BurstRecord(iid=8, icao="BBBBBB", centroid_us=4_000_000, n_replies=1),
    ])
    state._dirty_iids = {7, 8}
    state._models = {
        7: RadarIID(iid=7, last_updated=100.0),
        8: RadarIID(iid=8, last_updated=200.0),
    }

    analysed_iids = []

    def fake_analyse(records):
        analysed_iids.append(records[0].iid)
        return RotationModel(dominant_period_s=4.0, primary_direct_count=4, status="LIKELY_SINGLE")

    perf_values = iter([
        10.0,   # update start
        10.001, # snapshot start
        10.002, # snapshot done
        10.003, # sweep start
        11.000, # after first IID: budget exceeded
        11.001, # sweep done
        11.002, # analyse-swap start
        11.003, # analyse-swap done
        11.004, # record total
    ])

    monkeypatch.setattr(sweep_module, "_analyse_burst_records", fake_analyse)
    monkeypatch.setattr(sweep_module.time, "perf_counter", lambda: next(perf_values))

    state.update_rotation_models(max_runtime_ms=100.0)

    assert analysed_iids == [7]
    assert 8 in state._dirty_iids


def test_live_frame_builder_uses_fixed_reference_window_and_period_family_admission(monkeypatch):
    monkeypatch.setattr("radar.sweep.time.time", lambda: 1000.0)

    state = RadarState()
    model = RadarIID(
        iid=7,
        period_s=4.0,
        reference_aircraft_override="AAAAAA",
        rotation_model=RotationModel(
            folded={"AAAAAA": {"multiplier": 1}, "BBBBBB": {"multiplier": 1}, "DDDDDD": {"multiplier": 1}},
            residual={"CCCCCC": 4.0},
        ),
    )
    state._models[7] = model

    for icao, lat, lon in (
        ("AAAAAA", 51.0, -1.0),
        ("BBBBBB", 51.1, -1.1),
        ("CCCCCC", 51.2, -1.2),
        ("DDDDDD", 51.3, -1.3),
        ("EEEEEE", 51.4, -1.4),
    ):
        state._adsb_tracker.update(icao, lat, lon, ts=1000.0)

    state._on_df11_frame_builder(7, "AAAAAA", 0.0, -10.0)
    state._on_df11_frame_builder(7, "AAAAAA", 300_000.0, -10.0)
    assert state._live_frames[7] is not None
    assert state._live_frames[7].ref_icao == "AAAAAA"
    assert state._live_frames[7].ref_arrival_us == pytest.approx(0.0)

    state._on_df11_frame_builder(7, "BBBBBB", 100_000.0, -20.0)
    state._on_df11_frame_builder(7, "BBBBBB", 400_000.0, -30.0)
    assert len(state._live_frames[7].observations) == 1
    assert state._live_frames[7].observations[0].icao == "BBBBBB"
    assert state._live_frames[7].observations[0].arrival_us == pytest.approx(100_000.0)

    state._on_df11_frame_builder(7, "BBBBBB", 700_000.0, -5.0)
    assert len(state._live_frames[7].observations) == 1
    assert state._live_frames[7].observations[0].arrival_us == pytest.approx(100_000.0)

    state._on_df11_frame_builder(7, "CCCCCC", 200_000.0, -15.0)
    state._on_df11_frame_builder(7, "CCCCCC", 500_000.0, -15.0)
    assert [obs.icao for obs in state._live_frames[7].observations] == ["BBBBBB"]

    state._on_df11_frame_builder(7, "DDDDDD", 300_000.0, -18.0)
    state._on_df11_frame_builder(7, "DDDDDD", 600_000.0, -18.0)
    assert [obs.icao for obs in state._live_frames[7].observations] == ["BBBBBB", "DDDDDD"]

    state._on_df11_frame_builder(7, "EEEEEE", 4_100_000.0, -15.0)
    state._on_df11_frame_builder(7, "EEEEEE", 4_400_000.0, -15.0)

    frames = state.build_sweep_frames(7)
    assert len(frames) == 1
    frame = frames[0]
    assert frame.ref_icao == "AAAAAA"
    assert frame.ref_arrival_us == pytest.approx(0.0)
    assert frame.sweep_start_us == pytest.approx(0.0)
    assert [obs.icao for obs in frame.observations] == ["BBBBBB", "DDDDDD"]


def test_on_df11_batch_builds_live_frame_via_native_burst_path(monkeypatch):
    monkeypatch.setattr("radar.sweep.time.time", lambda: 1000.0)

    state = RadarState()
    model = RadarIID(
        iid=13,
        period_s=4.0,
        reference_aircraft_override="AAAAAA",
        rotation_model=RotationModel(
            folded={"AAAAAA": {"multiplier": 1}, "BBBBBB": {"multiplier": 1}},
        ),
    )
    state._models[13] = model
    state._adsb_tracker.update("AAAAAA", 51.0, -1.0, ts=1000.0)
    state._adsb_tracker.update("BBBBBB", 51.1, -1.1, ts=1000.0)

    state.on_df11_batch([
        (0, 13, "AAAAAA", -10.0),
        (3_600_000, 13, "AAAAAA", -10.0),
        (1_200_000, 13, "BBBBBB", -20.0),
        (4_800_000, 13, "BBBBBB", -18.0),
    ])

    assert state._live_frames[13] is not None
    assert state._live_frames[13].ref_icao == "AAAAAA"
    assert [obs.icao for obs in state._live_frames[13].observations] == ["BBBBBB"]


def test_on_df11_batch_native_path_handles_many_burst_expiries(monkeypatch):
    monkeypatch.setattr("radar.sweep.time.time", lambda: 1000.0)

    state = RadarState()
    folded = {"AAAAAA": {"multiplier": 1}}
    for index in range(70):
        folded[f"{index + 1:06X}"] = {"multiplier": 1}
    state._models[17] = RadarIID(
        iid=17,
        period_s=4.0,
        reference_aircraft_override="AAAAAA",
        rotation_model=RotationModel(folded=folded),
    )

    state._adsb_tracker.update("AAAAAA", 51.0, -1.0, ts=1000.0)
    events = [
        (0, 17, "AAAAAA", -10.0),
        (300_000, 17, "AAAAAA", -10.0),
    ]
    for index in range(70):
        icao = f"{index + 1:06X}"
        state._adsb_tracker.update(icao, 51.0 + index * 0.01, -1.0 - index * 0.01, ts=1000.0)
        arrival_us = 100_000.0 + index * 1_000.0
        events.append((int(arrival_us * 12), 17, icao, -20.0))
        events.append((int((arrival_us + 20_000.0) * 12), 17, icao, -18.0))

    events.append((int(4_500_000.0 * 12), 17, "FFFFFF", -15.0))

    state.on_df11_batch(events)

    assert state._live_frames[17] is not None
    assert len(state._live_frames[17].observations) == 70


def test_native_reference_selection_matches_python_scoring():
    import importlib

    decode_cffi = importlib.import_module("decode_cffi")
    decode_cffi = importlib.reload(decode_cffi)

    state = RadarState()
    model = RadarIID(iid=19, period_s=10.0)
    state._models[19] = model
    state._live_burst_centroids[19] = {
        "AAAAAA": [0.0, 10_000_000.0, 20_000_000.0, 30_000_000.0, 40_000_000.0, 50_000_000.0],
        "BBBBBB": [0.0, 10_300_000.0, 20_000_000.0, 30_300_000.0, 40_000_000.0, 50_300_000.0],
        "CCCCCC": [0.0, 9_500_000.0, 19_000_000.0, 30_000_000.0, 39_500_000.0, 49_000_000.0],
    }

    python_ref = state._select_reference_from_live_bursts(iid=19, period_s=10.0)

    processor = decode_cffi.RadarBurstProcessor()
    for icao, centroids in state._live_burst_centroids[19].items():
        events: list[tuple[float, str, float | None]] = []
        for centroid_us in centroids:
            events.append((centroid_us, icao, -20.0))
            events.append((centroid_us + 20_000.0, icao, -18.0))
        events.append((centroids[-1] + 10_000_000.0, "FFFFFF", -15.0))
        processor.process_batch(events, 30_000.0)

    native_ref = processor.select_reference(
        period_s=10.0,
        now_us=50_300_000.0,
        min_bursts_for_ref=4,
        recency_periods=5.0,
        hysteresis=0.25,
        current_ref_icao=None,
    )

    assert python_ref == "AAAAAA"
    assert native_ref == python_ref


def test_fired_burst_processing_rescores_once_per_fired_burst_batch():
    class CountingNativeProcessor:
        def __init__(self):
            self.select_calls = 0

        def select_reference(self, **kwargs):
            self.select_calls += 1
            return "CCCCCC"

        def matches_dominant_period(self, *args, **kwargs):
            return True

        def matches_phase_family(self, *args, **kwargs):
            return True

    state = RadarState()
    processor = CountingNativeProcessor()
    state._models[23] = RadarIID(
        iid=23,
        period_s=4.0,
        reference_aircraft=ReferenceAircraftInfo(ref_icao="AAAAAA"),
    )
    state._native_burst_processors[23] = processor
    state._live_bursts[23] = {}
    state._live_last_arrival[23] = {}
    state._live_frames[23] = None
    state._live_completed_frames[23] = deque(maxlen=state._LIVE_FRAMES_MAX)
    state._live_burst_centroids[23] = {
        "AAAAAA": [0.0, 4_000_000.0, 8_000_000.0],
    }

    metrics = state._process_fired_bursts(23, [
        {"icao": "BBBBBB", "burst_centroid_us": 8_100_000.0, "burst_signal": None},
        {"icao": "CCCCCC", "burst_centroid_us": 8_200_000.0, "burst_signal": None},
    ])

    assert processor.select_calls == 1
    assert metrics["reference_select_count"] == 2
    assert metrics["reference_reuse_count"] == 1
    assert metrics["reference_rescore_count"] == 1


def test_fired_burst_processing_emits_burst_records_and_diagnostic_dwell():
    class NativeProcessor:
        def select_reference(self, **kwargs):
            return "AAAAAA"

        def matches_dominant_period(self, *args, **kwargs):
            return True

        def matches_phase_family(self, *args, **kwargs):
            return True

    state = RadarState()
    state._diagnostics_enabled = True
    state._models[25] = RadarIID(
        iid=25,
        period_s=4.0,
        reference_aircraft_override="AAAAAA",
        rotation_model=RotationModel(folded={"AAAAAA": {"multiplier": 1}}),
    )
    state._native_burst_processors[25] = NativeProcessor()

    state._process_fired_bursts(25, [{
        "icao": "AAAAAA",
        "burst_centroid_us": 12_345_678.0,
        "burst_signal": -9.5,
        "n_replies": 2,
        "replies": [
            {"arrival_us": 12_345_000.0, "signal_dbfs": -15.0},
            {"arrival_us": 12_346_000.0, "signal_dbfs": -9.5},
        ],
    }])

    records = list(state._burst_records[25])
    assert records == [
        BurstRecord(
            iid=25,
            icao="AAAAAA",
            centroid_us=12_345_678.0,
            n_replies=2,
            signal_dbfs=-9.5,
        )
    ]
    assert state._dwell_profiles[25][0]["replies"] == [
        {"arrival_us": 12_345_000.0, "signal_dbfs": -15.0},
        {"arrival_us": 12_346_000.0, "signal_dbfs": -9.5},
    ]


def test_dwell_profile_uses_diagnostic_replies_after_burst_record_rebuild():
    state = RadarState()
    state._burst_records[25] = deque([
        BurstRecord(
            iid=25,
            icao="AAAAAA",
            centroid_us=12_345_678.0,
            n_replies=2,
            signal_dbfs=-9.5,
        ),
    ])
    state._dwell_profiles[25] = deque([
        {
            "icao": "AAAAAA",
            "beam_center_us": 12_345_678.0,
            "replies": [
                {"arrival_us": 12_345_000.0, "signal_dbfs": -15.0},
                {"arrival_us": 12_346_000.0, "signal_dbfs": -9.5},
            ],
        },
    ])

    replies = state.get_dwell_profile(25, "AAAAAA")

    assert replies == [
        {"arrival_us": 12_345_000.0, "signal_dbfs": -15.0},
        {"arrival_us": 12_346_000.0, "signal_dbfs": -9.5},
    ]


def test_memory_stats_include_burst_records():
    state = RadarState()
    state._burst_records[25] = deque([
        BurstRecord(iid=25, icao="AAAAAA", centroid_us=1_000_000.0, n_replies=1),
        BurstRecord(iid=25, icao="BBBBBB", centroid_us=2_000_000.0, n_replies=2),
    ])
    state._burst_records[26] = deque([
        BurstRecord(iid=26, icao="CCCCCC", centroid_us=3_000_000.0, n_replies=1),
    ])

    stats = state.get_memory_stats()

    assert stats["burst_records_total"] == 3
    assert stats["burst_records_iids"] == 2
    assert stats["burst_records_max_per_iid"] == 2
    assert stats["burst_records_avg_per_iid"] == 1.5


def test_density_aware_burst_record_retention_scales_with_active_aircraft():
    state = RadarState()
    iid = 77
    state._models[iid] = RadarIID(iid=iid, status="SINGLE_RADAR", period_s=4.0)
    state._live_last_arrival[iid] = {f"{n:06X}": 10_000_000.0 for n in range(100)}
    state._live_bursts[iid] = {}

    base_us = 10_000_000.0
    for n in range(5_000):
        icao = f"{(n % 100):06X}"
        state._append_burst_record(
            iid,
            BurstRecord(
                iid=iid,
                icao=icao,
                centroid_us=base_us + (n * 1_000.0),
                n_replies=2,
                signal_dbfs=-12.0,
            ),
        )

    debug = state.get_live_pipeline_debug(iid)
    retained = debug["retained_state"]

    assert retained["burst_records_dynamic_cap"] == 2240
    assert retained["burst_records_total"] == 2240
    assert retained["burst_records_cap_hit"] is True
    assert retained["burst_records_cap_hits_total"] > 0
    assert retained["burst_records_per_icao"]["icaos"] == 100
    assert retained["burst_records_per_icao"]["median"] >= 4.0
    assert retained["reference_eligible_aircraft_count"] >= 80


def test_fired_burst_processing_can_start_frame_after_reference_rescore(monkeypatch):
    monkeypatch.setattr("radar.sweep.time.time", lambda: 1000.0)

    class CountingNativeProcessor:
        def __init__(self):
            self.select_calls = 0

        def select_reference(self, **kwargs):
            self.select_calls += 1
            return "CCCCCC"

        def matches_dominant_period(self, *args, **kwargs):
            return True

        def matches_phase_family(self, *args, **kwargs):
            return True

    state = RadarState()
    processor = CountingNativeProcessor()
    state._models[24] = RadarIID(
        iid=24,
        period_s=4.0,
        reference_aircraft=ReferenceAircraftInfo(ref_icao="AAAAAA"),
    )
    state._native_burst_processors[24] = processor
    state._live_bursts[24] = {}
    state._live_last_arrival[24] = {}
    state._live_frames[24] = None
    state._live_completed_frames[24] = deque(maxlen=state._LIVE_FRAMES_MAX)
    state._live_burst_centroids[24] = {
        "AAAAAA": [0.0],
    }
    state._adsb_tracker.update("CCCCCC", 51.0, -1.0, ts=1000.0)

    metrics = state._process_fired_bursts(24, [
        {"icao": "CCCCCC", "burst_centroid_us": 40_100_000.0, "burst_signal": None},
        {"icao": "DDDDDD", "burst_centroid_us": 40_200_000.0, "burst_signal": None},
    ])

    assert processor.select_calls == 1
    assert metrics["reference_select_count"] == 1
    assert metrics["reference_reuse_count"] == 0
    assert metrics["reference_rescore_count"] == 1
    assert state._live_frames[24] is not None
    assert state._live_frames[24].ref_icao == "CCCCCC"


def test_live_frame_builder_ignores_duplicate_reference_bursts_within_open_window(monkeypatch):
    monkeypatch.setattr("radar.sweep.time.time", lambda: 1000.0)

    state = RadarState()
    model = RadarIID(
        iid=9,
        period_s=4.0,
        reference_aircraft_override="AAAAAA",
        rotation_model=RotationModel(
            folded={"AAAAAA": {"multiplier": 1}},
        ),
    )
    state._models[9] = model
    state._adsb_tracker.update("AAAAAA", 51.0, -1.0, ts=1000.0)

    state._on_df11_frame_builder(9, "AAAAAA", 0.0, -10.0)
    state._on_df11_frame_builder(9, "AAAAAA", 300_000.0, -10.0)
    assert state._live_frames[9] is not None
    assert state._live_frames[9].ref_arrival_us == pytest.approx(0.0)

    state._on_df11_frame_builder(9, "AAAAAA", 600_000.0, -10.0)

    assert state._live_frames[9] is not None
    assert state._live_frames[9].ref_arrival_us == pytest.approx(0.0)
    assert len(state._live_completed_frames[9]) == 0


def test_live_frame_builder_finalizes_stale_non_reference_burst_on_other_icao_activity(monkeypatch):
    monkeypatch.setattr("radar.sweep.time.time", lambda: 1000.0)

    state = RadarState()
    model = RadarIID(
        iid=11,
        period_s=4.0,
        reference_aircraft_override="AAAAAA",
        rotation_model=RotationModel(
            folded={"AAAAAA": {"multiplier": 1}, "BBBBBB": {"multiplier": 1}},
        ),
    )
    state._models[11] = model

    for icao, lat, lon in (
        ("AAAAAA", 51.0, -1.0),
        ("BBBBBB", 51.1, -1.1),
        ("CCCCCC", 51.2, -1.2),
    ):
        state._adsb_tracker.update(icao, lat, lon, ts=1000.0)

    state._on_df11_frame_builder(11, "AAAAAA", 0.0, -10.0)
    state._on_df11_frame_builder(11, "AAAAAA", 300_000.0, -10.0)
    assert state._live_frames[11] is not None
    assert state._live_frames[11].ref_arrival_us == pytest.approx(0.0)

    state._on_df11_frame_builder(11, "BBBBBB", 100_000.0, -20.0)
    state._on_df11_frame_builder(11, "BBBBBB", 120_000.0, -18.0)
    assert state._live_frames[11].observations == []

    state._on_df11_frame_builder(11, "CCCCCC", 500_000.0, -15.0)

    assert len(state._live_frames[11].observations) == 1
    observation = state._live_frames[11].observations[0]
    assert observation.icao == "BBBBBB"
    assert observation.arrival_us == pytest.approx((100_000.0 + 120_000.0) / 2.0, abs=5_000.0)


def test_live_frame_builder_rejects_burst_inconsistent_with_historical_phase_family(monkeypatch):
    monkeypatch.setattr("radar.sweep.time.time", lambda: 1000.0)

    state = RadarState()
    model = RadarIID(
        iid=12,
        period_s=4.0,
        reference_aircraft_override="AAAAAA",
        rotation_model=RotationModel(
            folded={"AAAAAA": {"multiplier": 1}, "BBBBBB": {"multiplier": 1}, "CCCCCC": {"multiplier": 1}},
        ),
    )
    state._models[12] = model
    state._live_bursts[12] = {}
    state._live_last_arrival[12] = {}
    state._live_completed_frames[12] = deque(maxlen=state._LIVE_FRAMES_MAX)
    state._live_burst_centroids[12] = {
        "AAAAAA": [-8_000_000.0, -4_000_000.0, 0.0],
        "BBBBBB": [-7_900_000.0, -3_900_000.0],
    }
    state._live_frames[12] = LiveFrameState(
        ref_icao="AAAAAA",
        ref_lat=51.0,
        ref_lon=-1.0,
        ref_arrival_us=0.0,
        seen_icaos={"AAAAAA"},
    )

    for icao, lat, lon in (
        ("BBBBBB", 51.1, -1.1),
        ("CCCCCC", 51.2, -1.2),
    ):
        state._adsb_tracker.update(icao, lat, lon, ts=1000.0)

    state._on_df11_frame_builder(12, "BBBBBB", 2_000_000.0, -20.0)
    state._on_df11_frame_builder(12, "BBBBBB", 2_020_000.0, -18.0)
    state._on_df11_frame_builder(12, "CCCCCC", 2_500_000.0, -15.0)

    assert state._live_frames[12] is not None
    assert state._live_frames[12].observations == []


def test_update_calibration_pairs_uses_historical_track_position_at_sweep_time(monkeypatch):
    import radar.sweep as sweep_module

    monkeypatch.setattr(config, "RECEIVER_LAT", 51.0)
    monkeypatch.setattr(config, "RECEIVER_LON", -1.0)
    monkeypatch.setattr(sweep_module.time, "time", lambda: 1_000.0)

    track_store = SimpleNamespace(
        get_tracks=lambda icaos: {
            "AAAAAA": [
                {"ts": 995.0, "lat": 51.1, "lon": -1.1},
                {"ts": 1000.0, "lat": 51.2, "lon": -1.2},
            ],
            "BBBBBB": [
                {"ts": 995.0, "lat": 52.1, "lon": -2.1},
                {"ts": 1000.0, "lat": 52.2, "lon": -2.2},
            ],
        }
    )
    aircraft_state = SimpleNamespace(
        get_aircraft_live=lambda icao: {
            "pos_confident": True,
            "lat": 10.0,
            "lon": 10.0,
            "last_pos_age": 0.0,
            "altitude": 30000,
        }
    )

    state = RadarState(aircraft_state=aircraft_state, track_store=track_store)
    state._iid_events = deque([
        (5_000_000, 7, "AAAAAA", None),
        (5_002_000, 7, "BBBBBB", None),
    ])
    state._models = {
        7: RadarIID(iid=7, status="SINGLE_RADAR", period_s=4.0),
    }
    state._sweep_history = {
        7: deque([
            {
                "centroid_us": 5_010_000,
                "n_aircraft": 2,
                "aircraft": [
                        {"icao": "AAAAAA", "arrivals_us": [5_000_000]},
                        {"icao": "BBBBBB", "arrivals_us": [5_002_000]},
                ],
            }
        ])
    }

    pairs = state.update_calibration_pairs()

    assert len(pairs) == 1
    pair = pairs[0]
    assert pair.lat_a == pytest.approx(51.2, abs=0.001)
    assert pair.lon_a == pytest.approx(-1.2, abs=0.001)
    assert pair.lat_b == pytest.approx(52.2, abs=0.001)
    assert pair.lon_b == pytest.approx(-2.2, abs=0.001)


def test_update_calibration_pairs_is_idempotent_across_repeated_passes(monkeypatch):
    import radar.sweep as sweep_module

    monkeypatch.setattr(config, "RECEIVER_LAT", 51.0)
    monkeypatch.setattr(config, "RECEIVER_LON", -1.0)
    monkeypatch.setattr(sweep_module.time, "time", lambda: 1_000.0)

    track_store = SimpleNamespace(
        get_tracks=lambda icaos: {
            "AAAAAA": [{"ts": 1000.0, "lat": 51.2, "lon": -1.2}],
            "BBBBBB": [{"ts": 1000.0, "lat": 52.2, "lon": -2.2}],
        }
    )

    state = RadarState(aircraft_state=None, track_store=track_store)
    state._iid_events = deque([
        (5_000_000, 7, "AAAAAA", None),
        (5_002_000, 7, "BBBBBB", None),
    ])
    state._models = {
        7: RadarIID(iid=7, status="SINGLE_RADAR", period_s=4.0),
    }
    state._sweep_history = {
        7: deque([
            {
                "centroid_us": 5_010_000,
                "n_aircraft": 2,
                "aircraft": [
                        {"icao": "AAAAAA", "arrivals_us": [5_000_000]},
                        {"icao": "BBBBBB", "arrivals_us": [5_002_000]},
                ],
            }
        ])
    }

    first = state.update_calibration_pairs()
    second = state.update_calibration_pairs()

    assert len(first) == 1
    assert second == []


def test_update_calibration_pairs_builds_recent_sweeps_from_live_events_when_cache_is_empty(monkeypatch):
    import radar.sweep as sweep_module

    monkeypatch.setattr(config, "RECEIVER_LAT", 51.0)
    monkeypatch.setattr(config, "RECEIVER_LON", -1.0)
    monkeypatch.setattr(sweep_module.time, "time", lambda: 1_000.0)

    state = RadarState(aircraft_state=None, track_store=None)
    state._burst_records[7] = deque([
        BurstRecord(iid=7, icao="AAAAAA", centroid_us=5_000_000, n_replies=1),
        BurstRecord(iid=7, icao="BBBBBB", centroid_us=5_002_000, n_replies=1),
    ])
    state._models = {
        7: RadarIID(iid=7, status="SINGLE_RADAR", period_s=4.0),
    }
    state._sweep_history = {}

    def fake_get_aircraft_position(icao, ts_us, latest_arrival_us=None):
        return {
            "lat": 51.0 if icao == "AAAAAA" else 52.0,
            "lon": -1.0 if icao == "AAAAAA" else -2.0,
            "interpolated": False,
        }

    state._compute_sweep_history_for_iid = lambda iid: (_ for _ in ()).throw(AssertionError("unexpected full sweep rebuild"))
    state._get_aircraft_position = fake_get_aircraft_position

    pairs = state.update_calibration_pairs()

    assert len(pairs) == 1
    assert pairs[0].icao_a == "AAAAAA"
    assert pairs[0].icao_b == "BBBBBB"


def test_update_calibration_pairs_rejects_large_tdoa_within_broad_sweep_cluster(monkeypatch):
    import radar.sweep as sweep_module

    monkeypatch.setattr(config, "RECEIVER_LAT", 51.0)
    monkeypatch.setattr(config, "RECEIVER_LON", -1.0)
    monkeypatch.setattr(sweep_module.time, "time", lambda: 1_000.0)

    aircraft_state = SimpleNamespace(
        get_aircraft_position_history=lambda icao, window_s=1800.0: {
            "AAAAAA": [{"ts": 1000.0, "lat": 51.2, "lon": -1.2, "groundspeed_kts": 220.0, "track_deg": 90.0, "heading_deg": 90.0, "airspeed_kts": 210}],
            "BBBBBB": [{"ts": 1000.0, "lat": 52.2, "lon": -2.2, "groundspeed_kts": 220.0, "track_deg": 90.0, "heading_deg": 90.0, "airspeed_kts": 210}],
        }.get(icao, []),
        get_aircraft_live=lambda icao: None,
    )

    state = RadarState(aircraft_state=aircraft_state, track_store=None)
    state._iid_events = deque([
        (5_000_000, 7, "AAAAAA", None),
        (5_020_000, 7, "BBBBBB", None),
    ])
    state._models = {7: RadarIID(iid=7, status="SINGLE_RADAR", period_s=4.0)}
    state._sweep_history = {
        7: deque([
            {
                "centroid_us": 5_010_000,
                "n_aircraft": 2,
                "aircraft": [
                        {"icao": "AAAAAA", "arrivals_us": [5_000_000]},
                        {"icao": "BBBBBB", "arrivals_us": [5_020_000]},
                ],
            }
        ])
    }

    pairs = state.update_calibration_pairs()

    assert pairs == []


def test_update_calibration_pairs_uses_adjacent_adsb_samples_for_interpolation(monkeypatch):
    import radar.sweep as sweep_module

    monkeypatch.setattr(config, "RECEIVER_LAT", 51.0)
    monkeypatch.setattr(config, "RECEIVER_LON", -1.0)
    monkeypatch.setattr(sweep_module.time, "time", lambda: 1_000.0)

    aircraft_state = SimpleNamespace(
        get_aircraft_position_history=lambda icao, window_s=1800.0: {
            "AAAAAA": [
                {"ts": 994.0, "lat": 51.0, "lon": -1.0, "groundspeed_kts": 220.0, "track_deg": 90.0, "heading_deg": 90.0, "airspeed_kts": 210},
                {"ts": 1000.0, "lat": 51.6, "lon": -1.6, "groundspeed_kts": 220.0, "track_deg": 90.0, "heading_deg": 90.0, "airspeed_kts": 210},
            ],
            "BBBBBB": [
                {"ts": 994.0, "lat": 52.0, "lon": -2.0, "groundspeed_kts": 220.0, "track_deg": 90.0, "heading_deg": 90.0, "airspeed_kts": 210},
                {"ts": 1000.0, "lat": 52.6, "lon": -2.6, "groundspeed_kts": 220.0, "track_deg": 90.0, "heading_deg": 90.0, "airspeed_kts": 210},
            ],
        }.get(icao, []),
        get_aircraft_live=lambda icao: None,
    )

    state = RadarState(aircraft_state=aircraft_state, track_store=None)
    state._iid_events = deque([
        (5_000_000, 7, "AAAAAA", None),
        (5_002_000, 7, "BBBBBB", None),
    ])
    state._models = {7: RadarIID(iid=7, status="SINGLE_RADAR", period_s=4.0)}
    state._sweep_history = {
        7: deque([
            {
                "centroid_us": 5_010_000,
                "n_aircraft": 2,
                "aircraft": [
                        {"icao": "AAAAAA", "arrivals_us": [5_000_000]},
                        {"icao": "BBBBBB", "arrivals_us": [5_002_000]},
                ],
            }
        ])
    }

    pairs = state.update_calibration_pairs()

    assert len(pairs) == 1
    assert pairs[0].lat_a == pytest.approx(51.598, abs=0.01)
    assert pairs[0].lon_a == pytest.approx(-1.598, abs=0.01)
    assert pairs[0].lat_b == pytest.approx(52.598, abs=0.01)
    assert pairs[0].lon_b == pytest.approx(-2.598, abs=0.01)


def test_update_calibration_pairs_uses_refined_beam_center_when_available(monkeypatch):
    import radar.sweep as sweep_module

    monkeypatch.setattr(config, "RECEIVER_LAT", 51.0)
    monkeypatch.setattr(config, "RECEIVER_LON", -1.0)
    monkeypatch.setattr(sweep_module.time, "time", lambda: 1_000.0)

    aircraft_state = SimpleNamespace(
        get_aircraft_position_history=lambda icao, window_s=1800.0: {
            "AAAAAA": [{"ts": 1000.0, "lat": 51.2, "lon": -1.2, "groundspeed_kts": 220.0, "track_deg": 90.0, "heading_deg": 90.0, "airspeed_kts": 210}],
            "BBBBBB": [{"ts": 1000.0, "lat": 52.2, "lon": -2.2, "groundspeed_kts": 220.0, "track_deg": 90.0, "heading_deg": 90.0, "airspeed_kts": 210}],
        }.get(icao, []),
        get_aircraft_live=lambda icao: None,
    )

    state = RadarState(aircraft_state=aircraft_state, track_store=None)
    state._iid_events = deque([
        (5_000_000, 7, "AAAAAA", None),
        (5_002_000, 7, "BBBBBB", None),
    ])
    state._models = {7: RadarIID(iid=7, status="SINGLE_RADAR", period_s=4.0)}
    state._sweep_history = {
        7: deque([
            {
                "centroid_us": 5_010_000,
                "n_aircraft": 2,
                "aircraft": [
                    {"icao": "AAAAAA", "arrivals_us": [5_000_000], "beam_center_us": 5_000_400},
                    {"icao": "BBBBBB", "arrivals_us": [5_002_000], "beam_center_us": 5_001_100},
                ],
            }
        ])
    }

    pairs = state.update_calibration_pairs()

    assert len(pairs) == 1
    assert pairs[0].tdoa_us == pytest.approx(-700.0)


def test_update_calibration_pairs_looks_up_position_at_each_aircraft_beam_center(monkeypatch):
    import radar.sweep as sweep_module

    monkeypatch.setattr(config, "RECEIVER_LAT", 51.0)
    monkeypatch.setattr(config, "RECEIVER_LON", -1.0)
    monkeypatch.setattr(sweep_module.time, "time", lambda: 1_000.0)

    state = RadarState(aircraft_state=None, track_store=None)
    state._models = {7: RadarIID(iid=7, status="SINGLE_RADAR", period_s=4.0)}
    state._iid_events = deque([
        (5_020_500, 7, "BBBBBB", None),
    ])
    state._sweep_history = {
        7: deque([
            {
                "centroid_us": 5_010_000,
                "n_aircraft": 2,
                "aircraft": [
                    {"icao": "AAAAAA", "arrivals_us": [5_000_000], "beam_center_us": 5_000_111},
                    {"icao": "BBBBBB", "arrivals_us": [5_004_000], "beam_center_us": 5_004_222},
                ],
            }
        ])
    }

    looked_up = []

    def fake_get_aircraft_position(icao, ts_us, latest_arrival_us=None):
        looked_up.append((icao, ts_us))
        return {
            "lat": 51.0 if icao == "AAAAAA" else 52.0,
            "lon": -1.0 if icao == "AAAAAA" else -2.0,
            "interpolated": False,
        }

    state._get_aircraft_position = fake_get_aircraft_position

    pairs = state.update_calibration_pairs()

    assert len(pairs) == 1
    assert looked_up == [
        ("AAAAAA", 5_000_111),
        ("BBBBBB", 5_004_222),
    ]


def test_update_calibration_pairs_caches_position_lookup_per_aircraft_and_beam_center(monkeypatch):
    import radar.sweep as sweep_module

    monkeypatch.setattr(config, "RECEIVER_LAT", 51.0)
    monkeypatch.setattr(config, "RECEIVER_LON", -1.0)
    monkeypatch.setattr(sweep_module.time, "time", lambda: 1_000.0)

    state = RadarState(aircraft_state=None, track_store=None)
    state._models = {7: RadarIID(iid=7, status="SINGLE_RADAR", period_s=4.0)}
    state._iid_events = deque([
        (5_004_500, 7, "BBBBBB", None),
    ])
    repeated_sweep = {
        "centroid_us": 5_002_000,
        "n_aircraft": 2,
        "aircraft": [
            {"icao": "AAAAAA", "arrivals_us": [5_000_000], "beam_center_us": 5_000_111},
            {"icao": "BBBBBB", "arrivals_us": [5_004_000], "beam_center_us": 5_004_222},
        ],
    }
    state._sweep_history = {
        7: deque([repeated_sweep, repeated_sweep.copy()])
    }

    lookup_calls = []

    def fake_get_aircraft_position(icao, ts_us, latest_arrival_us=None):
        lookup_calls.append((icao, ts_us))
        return {
            "lat": 51.0 if icao == "AAAAAA" else 52.0,
            "lon": -1.0 if icao == "AAAAAA" else -2.0,
            "interpolated": False,
        }

    state._get_aircraft_position = fake_get_aircraft_position

    state.update_calibration_pairs()

    assert lookup_calls == [
        ("AAAAAA", 5_000_111),
        ("BBBBBB", 5_004_222),
    ]


def test_update_calibration_pairs_skips_residual_aircraft_outside_dominant_family(monkeypatch):
    import radar.sweep as sweep_module

    monkeypatch.setattr(config, "RECEIVER_LAT", 51.0)
    monkeypatch.setattr(config, "RECEIVER_LON", -1.0)
    monkeypatch.setattr(sweep_module.time, "time", lambda: 1_000.0)

    state = RadarState(aircraft_state=None, track_store=None)
    state._models = {
        7: RadarIID(
            iid=7,
            status="SINGLE_RADAR",
            period_s=4.0,
            rotation_model=RotationModel(
                folded={"AAAAAA": {"multiplier": 1}, "BBBBBB": {"multiplier": 1}},
                residual={"CCCCCC": 4.0},
            ),
        ),
    }
    state._iid_events = deque([
        (5_004_500, 7, "BBBBBB", None),
    ])
    state._sweep_history = {
        7: deque([
            {
                "centroid_us": 5_002_000,
                "n_aircraft": 3,
                "aircraft": [
                    {"icao": "AAAAAA", "arrivals_us": [5_000_000], "beam_center_us": 5_000_111},
                    {"icao": "BBBBBB", "arrivals_us": [5_004_000], "beam_center_us": 5_004_222},
                    {"icao": "CCCCCC", "arrivals_us": [5_004_100], "beam_center_us": 5_004_333},
                ],
            }
        ])
    }

    looked_up = []

    def fake_get_aircraft_position(icao, ts_us, latest_arrival_us=None):
        looked_up.append((icao, ts_us))
        return {
            "lat": 51.0 if icao == "AAAAAA" else 52.0 if icao == "BBBBBB" else 53.0,
            "lon": -1.0 if icao == "AAAAAA" else -2.0 if icao == "BBBBBB" else -3.0,
            "interpolated": False,
        }

    state._get_aircraft_position = fake_get_aircraft_position

    pairs = state.update_calibration_pairs()

    assert len(pairs) == 1
    assert {(pair.icao_a, pair.icao_b) for pair in pairs} == {("AAAAAA", "BBBBBB")}
    assert looked_up == [
        ("AAAAAA", 5_000_111),
        ("BBBBBB", 5_004_222),
    ]


def test_update_calibration_pairs_short_extrapolation_uses_motion_vector(monkeypatch):
    import radar.sweep as sweep_module

    monkeypatch.setattr(config, "RECEIVER_LAT", 51.0)
    monkeypatch.setattr(config, "RECEIVER_LON", -1.0)
    monkeypatch.setattr(sweep_module.time, "time", lambda: 1_000.0)

    aircraft_state = SimpleNamespace(
        get_aircraft_position_history=lambda icao, window_s=1800.0: {
            "AAAAAA": [
                {"ts": 997.5, "lat": 51.0, "lon": -1.0, "groundspeed_kts": 360.0, "track_deg": 90.0, "heading_deg": 90.0, "airspeed_kts": 330},
            ],
            "BBBBBB": [
                {"ts": 997.5, "lat": 52.0, "lon": -2.0, "groundspeed_kts": 360.0, "track_deg": 90.0, "heading_deg": 90.0, "airspeed_kts": 330},
            ],
            }.get(icao, []),
            get_aircraft_live=lambda icao: None,
        )

    state = RadarState(aircraft_state=aircraft_state, track_store=None)
    state._iid_events = deque([
        (5_000_000, 7, "AAAAAA", None),
        (5_002_000, 7, "BBBBBB", None),
    ])
    state._models = {7: RadarIID(iid=7, status="SINGLE_RADAR", period_s=4.0)}
    state._sweep_history = {
        7: deque([
            {
                "centroid_us": 5_010_000,
                "n_aircraft": 2,
                "aircraft": [
                    {"icao": "AAAAAA", "arrivals_us": [5_000_000]},
                    {"icao": "BBBBBB", "arrivals_us": [5_002_000]},
                ],
            }
        ])
    }

    pairs = state.update_calibration_pairs()

    assert len(pairs) == 1
    assert pairs[0].lon_a > -1.0
    assert pairs[0].lon_b > -2.0


def test_lookup_adsb_position_projects_backward_from_future_sample():
    aircraft_state = SimpleNamespace(
        get_aircraft_position_history=lambda icao, window_s=1800.0: [
            {
                "ts": 1004.0,
                "lat": 51.0,
                "lon": -1.0,
                "groundspeed_kts": 360.0,
                "track_deg": 90.0,
            }
        ],
        get_aircraft_live=lambda icao: None,
    )

    state = RadarState(aircraft_state=aircraft_state, track_store=None)

    pos = state._lookup_adsb_position("AAAAAA", 1001.0)

    assert pos is not None
    assert pos["source"] == "adsb_projected"
    assert pos["interpolated"] is True
    assert pos["lon"] < -1.0


def test_lookup_adsb_position_projects_forward_from_past_sample():
    aircraft_state = SimpleNamespace(
        get_aircraft_position_history=lambda icao, window_s=1800.0: [
            {
                "ts": 1001.0,
                "lat": 51.0,
                "lon": -1.0,
                "groundspeed_kts": 360.0,
                "track_deg": 90.0,
            }
        ],
        get_aircraft_live=lambda icao: None,
    )

    state = RadarState(aircraft_state=aircraft_state, track_store=None)

    pos = state._lookup_adsb_position("AAAAAA", 1004.0)

    assert pos is not None
    assert pos["source"] == "adsb_projected"
    assert pos["interpolated"] is True
    assert pos["lon"] > -1.0


def test_lookup_adsb_position_does_not_project_beyond_vector_window():
    aircraft_state = SimpleNamespace(
        get_aircraft_position_history=lambda icao, window_s=1800.0: [
            {
                "ts": 1001.0,
                "lat": 51.0,
                "lon": -1.0,
                "groundspeed_kts": 360.0,
                "track_deg": 90.0,
            }
        ],
        get_aircraft_live=lambda icao: None,
    )

    state = RadarState(aircraft_state=aircraft_state, track_store=None)

    pos = state._lookup_adsb_position("AAAAAA", 1004.5)

    assert pos is None


# ---------------------------------------------------------------------------
# FM mailbox semantics — frame finalisation must hand work off to the worker
# rather than invoking the legacy callback inline.
# ---------------------------------------------------------------------------

def _seed_live_frame(state: RadarState, iid: int, *, n_observations: int) -> None:
    """Helper: install a LiveFrameState with enough observations to finalise."""
    from radar.models import SweepFrameObservation
    state._ensure_live_builder_state(iid)
    state._live_frames[iid] = LiveFrameState(
        ref_icao="AAAAAA",
        ref_lat=51.0,
        ref_lon=-1.0,
        ref_arrival_us=0.0,
        seen_icaos={"AAAAAA"},
    )
    for i in range(n_observations):
        state._live_frames[iid].observations.append(
            SweepFrameObservation(
                icao=f"OBS{i:03d}",
                lat=51.0 + i * 0.01,
                lon=-1.0 + i * 0.01,
                arrival_us=1000.0 * (i + 1),
            )
        )


def test_finalize_live_frame_enqueues_fm_mailbox_instead_of_inline_callback():
    state = RadarState()
    _seed_live_frame(state, iid=41, n_observations=3)  # 4 aircraft → "good"

    callback_invocations: list = []
    state.per_frame_solve_callback = lambda *args, **kw: callback_invocations.append(args)

    metrics = state._finalize_live_frame(41, period_s=4.0)

    # Legacy callback must NOT be invoked inline any more.
    assert callback_invocations == []
    # Mailbox must hold the frame for the worker.
    pending = state.claim_pending_fm_frames()
    assert len(pending) == 1
    enq_iid, enq_frame, enq_period = pending[0]
    assert enq_iid == 41
    assert enq_frame.quality == "good"
    assert enq_period == 4.0
    # Metrics still report a single handoff.
    assert metrics["fm_callback_count"] == 1


def test_fm_mailbox_latest_frame_wins_for_same_iid():
    state = RadarState()

    _seed_live_frame(state, iid=42, n_observations=3)
    state._finalize_live_frame(42, period_s=4.0)

    _seed_live_frame(state, iid=42, n_observations=4)  # 5 aircraft
    state._finalize_live_frame(42, period_s=4.5)

    pending = state.claim_pending_fm_frames()
    assert len(pending) == 1  # same IID coalesced
    _iid, frame, period_s = pending[0]
    assert period_s == 4.5
    assert len(frame.observations) == 4
    # Mailbox cleared on claim.
    assert state.claim_pending_fm_frames() == []


def test_fm_mailbox_preserves_distinct_iids():
    state = RadarState()
    _seed_live_frame(state, iid=50, n_observations=3)
    state._finalize_live_frame(50, period_s=4.0)
    _seed_live_frame(state, iid=51, n_observations=3)
    state._finalize_live_frame(51, period_s=5.0)

    pending = {iid: (frame, period_s) for iid, frame, period_s in state.claim_pending_fm_frames()}
    assert set(pending.keys()) == {50, 51}
    assert pending[50][1] == 4.0
    assert pending[51][1] == 5.0


def test_radar_core_frame_injection_populates_fm_mailbox_and_completed_buffer():
    state = RadarState()
    state.enable_radar_core_frames(True)

    state.inject_frame_from_go({
        "t": 11,
        "i": 61,
        "fi": 12,
        "p": 4.25,
        "rc": 0xAAAAAA,
        "rla": 51.0,
        "rlo": -1.0,
        "ra": 123_456.0,
        "obs": [
            {"c": 0xBBBBBB, "la": 51.1, "lo": -1.1, "a": 124_000.0, "n": 3, "pa": 0.5},
        ],
        "q": "marginal",
    })

    pending = state.claim_pending_fm_frames()
    assert len(pending) == 1
    iid, frame, period_s = pending[0]
    assert iid == 61
    assert period_s == 4.25
    assert frame.frame_index == 12
    assert frame.ref_icao == "AAAAAA"
    assert frame.ref_lat == 51.0
    assert frame.ref_lon == -1.0
    assert frame.quality == "marginal"
    assert frame.observations[0].icao == "BBBBBB"
    assert frame.observations[0].position_age_seconds == 0.5
    assert state.get_live_frame_counts(61)["n_frames"] == 1
    stats = state.get_memory_stats()
    assert stats["radar_core_frames_enabled"] is True
    assert stats["radar_core_frames_injected"] == 1
    assert stats["radar_core_frame_inject_errors"] == 0


def test_radar_core_frames_enabled_suppresses_python_fm_mailbox_injection():
    state = RadarState()
    state.enable_radar_core_frames(True)
    _seed_live_frame(state, iid=62, n_observations=3)

    metrics = state._finalize_live_frame(62, period_s=4.0)

    assert metrics["fm_callback_count"] == 0
    assert state.claim_pending_fm_frames() == []


def test_radar_core_frame_injection_bootstraps_live_sync_state_when_missing():
    state = RadarState()
    state.enable_radar_core_frames(True)
    state._models[63] = RadarIID(
        iid=63,
        status="SINGLE_RADAR",
        period_s=4.0,
        fm_lat=51.0,
        fm_lon=-1.0,
    )

    state.inject_frame_from_go({
        "t": 11,
        "i": 63,
        "fi": 4,
        "p": 4.0,
        "rc": 0xAAAAAA,
        "rla": 51.2,
        "rlo": -1.2,
        "ra": 500_000.0,
        "rpa": 0.5,  # explicit fresh age required for 3-aircraft frame to seed sync
        "obs": [
            {"c": 0xBBBBBB, "la": 51.3, "lo": -1.3, "a": 500_500.0, "n": 2, "pa": 0.3},
            {"c": 0xCCCCCC, "la": 51.4, "lo": -1.4, "a": 501_000.0, "n": 2, "pa": 0.4},
        ],
        "q": "marginal",
    })

    sync = state.get_live_sync_state(63)
    assert sync is not None
    assert sync.usable is True
    assert sync.source == "sweep_frame"
    debug = state.get_live_pipeline_debug(63)
    assert debug["sync_state_present"] is True
    assert debug["go_frames_injected_count"] == 1
    assert debug["completed_frame_count"] == 1


# --- Fix 2: ref_pos_age_s propagation from Go frames ---

def _make_base_radar_state_for_age_tests():
    """Return a RadarState seeded with a SINGLE_RADAR model for IID 77 at a known position."""
    state = RadarState()
    from radar.models import RadarIID, RotationModel
    period = 4.0
    model = RadarIID(iid=77)
    model.status = "SINGLE_RADAR"
    model.period_s = period
    rot = RotationModel()
    rot.status = "SINGLE_RADAR"
    rot.dominant_period_s = period
    model.rotation_model = rot
    model.lat = 51.0
    model.lon = -1.0
    model.cep_m = 1000.0
    state._models[77] = model
    return state


def test_inject_frame_from_go_3aircraft_fresh_age_seeds_sync():
    """A 3-aircraft Go frame with rpa <= 2.0 must be eligible to seed sync."""
    state = _make_base_radar_state_for_age_tests()

    state.inject_frame_from_go({
        "i": 77,
        "p": 4.0,
        "rc": 0xAAAAAA,
        "rla": 51.2,
        "rlo": -1.2,
        "ra": 1_000_000.0,
        "rpa": 1.5,  # fresh — within 2.0s threshold
        "obs": [
            {"c": 0xBBBBBB, "la": 51.3, "lo": -1.3, "a": 1_500_000.0, "n": 2, "pa": 0.5},
            {"c": 0xCCCCCC, "la": 51.4, "lo": -1.4, "a": 2_000_000.0, "n": 2, "pa": 0.6},
        ],
        "q": "marginal",
    })

    sync = state.get_live_sync_state(77)
    assert sync is not None, "3-aircraft frame with fresh rpa should seed sync"


def test_inject_frame_from_go_3aircraft_missing_age_does_not_seed_sync():
    """A 3-aircraft Go frame without rpa (unknown age) must not seed sync via the
    freshness shortcut — only 4+ aircraft frames are eligible when age is unknown."""
    state = _make_base_radar_state_for_age_tests()

    state.inject_frame_from_go({
        "i": 77,
        "p": 4.0,
        "rc": 0xAAAAAA,
        "rla": 51.2,
        "rlo": -1.2,
        "ra": 1_000_000.0,
        # "rpa" deliberately absent — unknown age
        "obs": [
            {"c": 0xBBBBBB, "la": 51.3, "lo": -1.3, "a": 1_500_000.0, "n": 2, "pa": 0.5},
            {"c": 0xCCCCCC, "la": 51.4, "lo": -1.4, "a": 2_000_000.0, "n": 2, "pa": 0.6},
        ],
        "q": "marginal",
    })

    sync = state.get_live_sync_state(77)
    assert sync is None, (
        "3-aircraft frame with unknown rpa must NOT seed sync (freshness shortcut "
        "should not fire when age is unknown)"
    )


def test_inject_frame_from_go_4aircraft_no_rpa_seeds_sync():
    """A 4-aircraft Go frame with no rpa must still seed sync (4-aircraft path
    does not require ref position freshness)."""
    state = _make_base_radar_state_for_age_tests()

    state.inject_frame_from_go({
        "i": 77,
        "p": 4.0,
        "rc": 0xAAAAAA,
        "rla": 51.2,
        "rlo": -1.2,
        "ra": 1_000_000.0,
        # no "rpa"
        "obs": [
            {"c": 0xBBBBBB, "la": 51.3, "lo": -1.3, "a": 1_500_000.0, "n": 2, "pa": 0.5},
            {"c": 0xCCCCCC, "la": 51.4, "lo": -1.4, "a": 2_000_000.0, "n": 2, "pa": 0.6},
            {"c": 0xDDDDDD, "la": 51.5, "lo": -1.5, "a": 2_500_000.0, "n": 2, "pa": 0.7},
        ],
        "q": "good",
    })

    sync = state.get_live_sync_state(77)
    assert sync is not None, "4-aircraft frame without rpa must still seed sync"
