import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(__file__)))

from collections import deque
from types import SimpleNamespace

import config
import pytest
from radar.sweep import _analyse_iid_events
from radar.models import LiveFrameState, RadarIID, ReferenceAircraftInfo, RotationModel
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
    assert 9 not in state._last_multi_sync_update_ts
    assert 9 not in state._live_waveform_bins
    assert 9 not in state._live_icao_sync_quality
    assert all(event[1] != 9 for event in state._iid_events)
    assert 10 in state._models
    assert 10 in state._last_multi_sync_update_ts
    assert 10 in state._live_waveform_bins
    assert 10 in state._live_icao_sync_quality


def test_reset_all_clears_sync_refinement_state():
    state = RadarState()
    state._models = {7: RadarIID(iid=7, period_s=4.0, primary_support_count=6)}
    state._iid_events = deque([(1_000_000, 7, "AAAAAA", None)])
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
    assert cleared["waveform_bins"] == 1
    assert cleared["icao_sync_quality"] == 1
    assert cleared["multi_sync_throttle"] == 1
    assert state._live_sync_states == {}
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
    state._iid_events = deque([
        (1_000_000, 7, "OLD777", None),
        (8_000_000, 8, "OTHER8", None),
        (9_000_000, 7, "ABC123", None),
        (10_000_000, 7, "ABC123", None),
        (11_000_000, 7, "DEF456", None),
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


def test_period_refinement_uses_effective_time_slope_and_correct_sign(monkeypatch):
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
        prop_delay_enabled=True,
        period_refine_enabled=True,
    )

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
    assert sync.residual_slope_deg_per_s == pytest.approx(0.5, rel=0.05)
    assert sync.period_s < 10.0
    assert sync.period_update_direction == "decrease"
    assert sync.period_update_applied < 0.0
    assert sync.period_update_gain == pytest.approx(0.25)
    assert sync.fit_eligible_observations == 8
    assert sync.fit_time_basis == "effective_beast_time_s"
    assert state.get_burst_sync_timeline(7, window_s=60.0)["period_update_history"]


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
    state._iid_events = deque([
        (1_000_000, 7, "AAAAAA", None),
        (2_000_000, 8, "BBBBBB", None),
        (3_000_000, 7, "AAAAAA", None),
        (4_000_000, 8, "BBBBBB", None),
    ])
    state._dirty_iids = {7, 8}
    state._models = {
        7: RadarIID(iid=7, last_updated=100.0),
        8: RadarIID(iid=8, last_updated=200.0),
    }

    analysed_iids = []

    def fake_analyse(events):
        analysed_iids.append(events[0][1])
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

    monkeypatch.setattr(sweep_module, "_analyse_iid_events", fake_analyse)
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
    state._iid_events = deque([
        (5_000_000, 7, "AAAAAA", None),
        (5_002_000, 7, "BBBBBB", None),
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
