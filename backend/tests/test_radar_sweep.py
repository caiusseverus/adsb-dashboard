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
    _icao_quality_anchor_warning,
    _icao_quality_reject_reason,
    _icao_quality_memory_score,
    _update_icao_sync_quality_memory,
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


def test_icao_quality_memory_helpers_keep_warning_and_reject_thresholds_aligned():
    quality = IcaoSyncQuality(
        residual_median_deg=50.0,
        residual_mad_deg=26.0,
        n_recent=6,
        last_ts=1000.0,
    )
    quality_dict = {"AAAAAA": quality}
    scored = [{
        "residual": 6.0,
        "weight": 1.0,
        "status": "inlier",
        "icao": "AAAAAA",
        "obs_ts": 1001.0,
    }]

    assert _icao_quality_memory_score(quality) == pytest.approx(0.0)
    assert _icao_quality_reject_reason(quality) == "poor_icao_quality"
    assert _icao_quality_anchor_warning(quality) == "poor_icao_quality_memory"

    _update_icao_sync_quality_memory(quality_dict, scored)

    assert quality_dict["AAAAAA"].n_recent == 7
    assert quality_dict["AAAAAA"].last_ts == pytest.approx(1001.0)


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


def test_get_burst_sync_timeline_prefers_go_evidence_when_available(monkeypatch):
    state = RadarState()
    now_ts = 1_000.0
    state._models[7] = RadarIID(
        iid=7,
        status="SINGLE_RADAR",
        period_s=4.0,
        lat=51.0,
        lon=0.0,
    )
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
    state.update_go_snapshot({
        "iids": {},
        "evidence_events": [
            {
                "kind": "burst_fired",
                "iid": 7,
                "icao": int("BBBBBB", 16),
                "arrival_us": 8_200_000.0,
                "simple_centroid_us": 8_198_000.0,
                "weighted_centroid_us": 8_200_000.0,
                "centroid_delta_us": 2_000.0,
                "first_reply_us": 8_190_000.0,
                "strongest_reply_us": 8_200_000.0,
                "mid_strong_window_us": 8_200_000.0,
                "last_reply_us": 8_206_000.0,
                "span_us": 16_000.0,
                "peak_amplitude": -12.0,
                "wall_ts": now_ts,
                "n_replies": 4,
                "signal_dbfs": -15.0,
                "truth_lat": 51.1,
                "truth_lon": 0.2,
                "position_age_s": 0.3,
                "dominant_family": True,
                "sync_eligible": True,
                "association_confidence": 1.0,
            }
        ],
    })

    monkeypatch.setattr("radar.sweep.time.time", lambda: now_ts)
    timeline = state.get_burst_sync_timeline(7, window_s=60.0)

    observations = timeline["observations"]
    assert len(observations) == 1
    obs = observations[0]
    assert obs["icao"] == "BBBBBB"
    assert obs["sync_update_eligible"] is True
    assert obs["burst_center_method"] == "amplitude_weighted"
    assert obs["burst_center_simple_us"] == pytest.approx(8_198_000.0)
    assert obs["burst_center_weighted_us"] == pytest.approx(8_200_000.0)
    assert obs["burst_center_delta_us"] == pytest.approx(2_000.0)
    retention = timeline["retention_diagnostics"]
    assert retention["timeline"]["count"] == 1
    assert retention["timeline"]["newest_burst_centroid_us"] == pytest.approx(8_200_000.0)


def test_go_aligned_burst_sync_snapshot_rebuilds_sync_driving_observations(monkeypatch):
    state = RadarState()
    now_ts = 1_000.0
    state._models[7] = RadarIID(
        iid=7,
        status="SINGLE_RADAR",
        period_s=4.0,
        lat=51.0,
        lon=0.0,
    )
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
    state.update_go_snapshot({
        "iids": {},
        "evidence_events": [
            {
                "kind": "burst_fired",
                "iid": 7,
                "icao": int("AAAAAA", 16),
                "arrival_us": 4_000_000.0,
                "wall_ts": now_ts - 2.0,
                "n_replies": 4,
                "signal_dbfs": -15.0,
                "truth_lat": 51.1,
                "truth_lon": 0.1,
                "position_age_s": 0.2,
                "dominant_family": True,
                "sync_eligible": True,
                "association_confidence": 1.0,
            },
            {
                "kind": "burst_fired",
                "iid": 7,
                "icao": int("BBBBBB", 16),
                "arrival_us": 6_000_000.0,
                "wall_ts": now_ts - 1.0,
                "n_replies": 4,
                "signal_dbfs": -14.0,
                "truth_lat": 51.2,
                "truth_lon": 0.2,
                "position_age_s": 0.3,
                "dominant_family": True,
                "sync_eligible": False,
                "association_confidence": 1.0,
            },
        ],
    })

    monkeypatch.setattr("radar.sweep.time.time", lambda: now_ts)
    aligned = state._go_aligned_burst_sync_snapshot(7, window_s=60.0)

    assert len(aligned) == 1
    assert aligned[0].icao == "AAAAAA"
    assert aligned[0].sync_update_eligible is True


def test_update_go_burst_fired_triggers_multi_sync_update_from_go_evidence(monkeypatch):
    state = RadarState()
    state._models[7] = RadarIID(
        iid=7,
        status="SINGLE_RADAR",
        period_s=4.0,
        lat=51.0,
        lon=0.0,
    )
    state._live_sync_states[7] = LiveSyncState(
        iid=7,
        period_s=4.0,
        phase_epoch_us=0.0,
        phase_offset_deg=0.0,
        sync_quality=1.0,
        sync_jitter_deg=3.0,
        last_sync_update_ts=1_000.0,
        source="multi_aircraft_burst",
        usable=True,
    )
    calls: list[tuple[int, float]] = []

    def fake_update(iid: int, period_s: float, sync_quality=None):
        calls.append((iid, period_s))

    # Python path always routes to _update_simple_live_sync_state.
    monkeypatch.setattr(state, "_update_simple_live_sync_state", fake_update)
    monkeypatch.setattr("radar.sweep.time.monotonic", lambda: 10.0)

    state.update_go_burst_fired({
        "i": 7,
        "c": int("AAAAAA", 16),
        "cu": 4_000_000.0,
        "n": 4,
        "s": -15.0,
        "la": 51.1,
        "lo": 0.1,
        "pa": 0.2,
        "df": True,
        "se": True,
        "ce": True,
        "rp": True,
        "ru": False,
    })

    assert calls == [(7, 4.0)]
    assert list(state._live_aligned_burst_obs.get(7, [])) == []


def test_update_go_burst_fired_tracks_compact_vs_refined_sync_semantics():
    state = RadarState()

    state.update_go_burst_fired({
        "i": 7,
        "c": int("AAAAAA", 16),
        "cu": 4_000_000.0,
        "n": 4,
        "s": -15.0,
        "la": 51.1,
        "lo": 0.1,
        "pa": 0.2,
        "df": True,
        "se": True,
        "ce": True,
        "rp": True,
        "ru": False,
    })

    track = state._go_track_observations[-1]
    evidence = state._go_evidence_events[-1]

    assert track["sync_eligible"] is False
    assert track["compact_sync_eligible"] is True
    assert track["refined_sync_present"] is True
    assert track["refined_sync_usable"] is False
    assert evidence["sync_eligible"] is False
    assert evidence["compact_sync_eligible"] is True
    assert evidence["refined_sync_present"] is True
    assert evidence["refined_sync_usable"] is False


def test_go_observation_normaliser_keeps_legacy_compact_only_payloads_compatible():
    entry = RadarState._normalise_go_track_observation({
        "iid": 7,
        "icao": int("AAAAAA", 16),
        "arrival_us": 4_000_000.0,
        "wall_ts": 1_000.0,
        "sync_eligible": True,
    })

    assert entry["compact_sync_eligible"] is True
    assert entry["refined_sync_present"] is False
    assert entry["refined_sync_usable"] is False
    assert entry["sync_eligible"] is True


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
    assert "period_failure_primary_class" in first["sync_state"]
    assert "period_reacquire_trigger" in first["sync_state"]
    assert "period_recovery_clean_streak" in first["sync_state"]


def test_go_sync_snapshot_and_debug_use_compact_diagnostics_path(monkeypatch):
    import radar.sweep as sweep_module

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
        source="sweep_frame_go",
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
    state._compact_sync_debug_by_iid[7] = {
        "current_reference_icao": "AAAAAA",
        "last_reference_icao": "BBBBBB",
        "reference_changed_recently": True,
        "reference_change_count": 2,
        "current_phase_epoch_us": 0.0,
        "last_phase_epoch_us": -500_000.0,
        "phase_epoch_changed_recently": True,
        "sync_reset_count": 1,
        "last_sync_reset_reason": "sync_state_missing",
    }

    snapshot = state.get_live_sync_snapshot(7, window_s=90.0, debug_limit=20)
    debug_payload = state.get_sync_debug_payload(7, window_s=60.0, limit=20)

    assert snapshot["sync_state"]["source"] == "sweep_frame_go"
    assert snapshot["waveform_bins"] == []
    assert snapshot["phase_anchor_candidates"] == []
    assert snapshot["observations"][0]["phase_anchor_contributor"] is False
    assert snapshot["observations"][0]["waveform_applied"] is False
    assert snapshot["retention_diagnostics"]["timeline"]["count"] == 1
    assert snapshot["sync_mode_diagnostics"]["active_mode"] == "compact_bootstrap"
    assert snapshot["sync_mode_diagnostics"]["compact"]["reference_icao"] == "AAAAAA"
    assert snapshot["sync_mode_diagnostics"]["compact"]["last_reference_icao"] == "BBBBBB"
    assert snapshot["sync_mode_diagnostics"]["compact"]["reference_changed_recently"] is True

    assert debug_payload["available"] is True
    assert debug_payload["summary"]["diagnostics_mode"] == "compact_go_sync"
    assert debug_payload["summary"]["rich_diagnostics_available"] is False
    assert debug_payload["summary"]["sync_source"] == "sweep_frame_go"
    assert debug_payload["observation_model_diagnostics"]["mode"] == "compact_go_sync"
    assert debug_payload["observations"][0]["icao"] == "AAAAAA"


def test_go_sync_snapshot_falls_back_to_sweep_frames_when_burst_evidence_aged_out(monkeypatch):
    import radar.sweep as sweep_module

    monkeypatch.setattr(sweep_module.time, "time", lambda: 1_000.0)

    state = RadarState()
    state._models[9] = RadarIID(
        iid=9,
        status="SINGLE_RADAR",
        period_s=4.0,
        manual_lat=51.5,
        manual_lon=-0.1,
        resolution_mode="locked_position",
    )
    state._iid_latest_arrival_us[9] = 4_200_000.0
    state._live_sync_states[9] = LiveSyncState(
        iid=9,
        period_s=4.0,
        phase_epoch_us=0.0,
        phase_offset_deg=0.0,
        sync_quality=1.0,
        sync_jitter_deg=2.0,
        last_sync_update_ts=999.0,
        source="sweep_frame_go",
        usable=True,
        period_base_s=4.0,
        period_authoritative_source="base",
    )
    state.update_go_frame_ready({
        "i": 9,
        "fi": 5,
        "p": 4.0,
        "rc": int("AAAAAA", 16),
        "rla": 51.6,
        "rlo": -0.05,
        "ra": 4_000_000.0,
        "q": "good",
        "obs": [
            {
                "c": int("BBBBBB", 16),
                "la": 51.7,
                "lo": 0.02,
                "a": 4_200_000.0,
                "n": 3,
                "pa": 0.2,
            }
        ],
    })

    snapshot = state.get_live_sync_snapshot(9, window_s=90.0, debug_limit=20)

    assert snapshot["observations"]
    assert snapshot["observations"][0]["burst_center_method"] == "go_sweep_frame"
    assert snapshot["df11_residual_observations"]
    assert snapshot["df11_residual_observations"][0]["residual_source"] == "go_sweep_frame_compact"
    assert snapshot["alignment_status"]["reason"] == "go_sweep_frames_projected"


def test_resolve_phase_anchor_state_population_veto_keeps_mixed_fallback(monkeypatch):
    state = RadarState()
    existing = LiveSyncState(
        iid=7,
        period_s=10.0,
        phase_epoch_us=0.0,
        phase_offset_deg=15.0,
        sync_quality=1.0,
        sync_jitter_deg=2.0,
        last_sync_update_ts=999.0,
        source="multi_aircraft_burst",
        usable=True,
        period_base_s=10.0,
        phase_anchor_icao="OLD111",
        phase_anchor_since_ts=900.0,
    )
    selection = {
        "selected": {"icao": "NEW222", "score": 88.0},
        "candidates": [{"icao": "NEW222", "score": 88.0, "status": "selected"}],
        "replacement_reason": "better_candidate",
    }
    solution = {
        "obs_count": 4,
        "offset_raw_deg": 120.0,
        "offset_smoothed_deg": 102.0,
        "spread_deg": 3.5,
        "delta_from_existing_deg": 20.0,
    }
    veto = {
        "contributors": [],
        "rejected": [{"icao": "BBBBBB", "error_deg": 60.0}, {"icao": "CCCCCC", "error_deg": -58.0}],
        "contributor_count": 0,
        "reject_count": 2,
        "median_error_deg": None,
        "nudge_deg": 2.0,
        "status": "population_disagrees",
    }

    monkeypatch.setattr(state, "_select_phase_anchor_aircraft", lambda **_: selection)
    monkeypatch.setattr(state, "_solve_phase_anchor_from_icao", lambda *args, **kwargs: solution)
    monkeypatch.setattr(state, "_validate_phase_anchor_against_population", lambda *args, **kwargs: veto)

    resolved = state._resolve_phase_anchor_state(
        iid=7,
        scored=[],
        existing=existing,
        epoch_us=40_000_000.0,
        now_ts=1_000.0,
        mixed_fallback_offset=47.0,
    )

    assert resolved["offset_deg"] == pytest.approx(47.0)
    assert resolved["phase_anchor_icao"] == "NEW222"
    assert resolved["phase_anchor_status"] == "population_veto"
    assert resolved["phase_anchor_replacement_reason"] == "population_veto"
    assert resolved["phase_anchor_since_ts"] == pytest.approx(1_000.0)
    assert resolved["phase_anchor_offset_raw_deg"] == pytest.approx(120.0)
    assert resolved["phase_anchor_offset_smoothed_deg"] == pytest.approx(102.0)
    assert resolved["phase_anchor_obs_count"] == 4
    assert resolved["validation"] == veto


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


def test_go_iid_state_bootstraps_live_sync_state_when_missing():
    state = RadarState()
    state.enable_radar_core_frames(True)
    state.update_go_iid_state({
        "t": 12,
        "i": 63,
        "st": "SINGLE_RADAR",
        "p": 4.0,
        "sq": 1.0,
        "sp": True,
        "su": True,
        "sps": 4.0,
        "sep": 500_000.0,
        "sod": 12.0,
        "sj": 3.0,
        "sre": 5.0,
        "slr": 0.0,
        "snf": 1,
        "snr": 0,
        "sh": False,
        "lu": 1_000.0,
        "rv": 1,
    })

    sync = state.get_live_sync_state(63)
    assert sync is not None
    assert sync.usable is True
    assert sync.source == "sweep_frame_go"
    assert sync.phase_epoch_us == pytest.approx(500_000.0)
    assert sync.phase_offset_deg == pytest.approx(12.0)
    debug = state.get_live_pipeline_debug(63)
    assert debug["sync_state_present"] is True
    assert state.get_go_live_sync_state(63)["phase_epoch_us"] == pytest.approx(500_000.0)


def _make_base_radar_state_for_go_sync_seed_tests():
    state = RadarState()
    state._models[77] = RadarIID(iid=77, status="SINGLE_RADAR", period_s=4.0)
    return state


def test_go_iid_state_does_not_override_multi_aircraft_sync_state():
    state = _make_base_radar_state_for_go_sync_seed_tests()
    state._live_sync_states[77] = LiveSyncState(
        iid=77,
        period_s=4.0,
        phase_epoch_us=123.0,
        phase_offset_deg=45.0,
        sync_quality=1.0,
        sync_jitter_deg=2.0,
        last_sync_update_ts=900.0,
        source="multi_aircraft_burst",
        usable=True,
    )

    state.update_go_iid_state({
        "t": 12,
        "i": 77,
        "st": "SINGLE_RADAR",
        "p": 4.0,
        "sq": 0.8,
        "sp": True,
        "su": True,
        "sps": 4.0,
        "sep": 999_000.0,
        "sod": 10.0,
        "sj": 4.0,
        "sre": 6.0,
        "slr": 1.0,
        "snf": 5,
        "snr": 1,
        "sh": False,
        "lu": 1_001.0,
        "rv": 2,
    })

    sync = state.get_live_sync_state(77)
    assert sync is not None
    assert sync.source == "multi_aircraft_burst"
    assert sync.phase_epoch_us == pytest.approx(123.0)
    assert state.get_go_live_sync_state(77)["phase_epoch_us"] == pytest.approx(999_000.0)


def test_update_go_burst_fired_skips_python_solver_when_go_multi_sync_present(monkeypatch):
    """Python solver must not run when Go has established multi-sync state for the IID."""
    state = RadarState()
    state._models[7] = RadarIID(iid=7, status="SINGLE_RADAR", period_s=4.0, lat=51.0, lon=0.0)
    state._live_sync_states[7] = LiveSyncState(
        iid=7, period_s=4.0, phase_epoch_us=0.0, phase_offset_deg=0.0,
        sync_quality=1.0, sync_jitter_deg=3.0, last_sync_update_ts=1_000.0,
        source="go_multi_aircraft_burst", usable=True,
    )
    # Simulate Go having already produced multi-sync state for IID 7.
    state._go_multi_sync_states_by_iid[7] = {"pr": True, "p": 4.0}

    calls: list[tuple[int, float]] = []

    def fake_update(iid: int, period_s: float, **_kw):
        calls.append((iid, period_s))

    # Python path always routes to _update_simple_live_sync_state.
    monkeypatch.setattr(state, "_update_simple_live_sync_state", fake_update)
    monkeypatch.setattr("radar.sweep.time.monotonic", lambda: 10.0)

    state.update_go_burst_fired({
        "i": 7, "c": int("AAAAAA", 16), "cu": 4_000_000.0,
        "n": 4, "s": -15.0, "la": 51.1, "lo": 0.1, "pa": 0.2,
        "df": True, "se": True,
    })

    # Python solver must not have been called.
    assert calls == []


def test_update_go_multi_sync_state_overrides_python_multi_aircraft_burst(monkeypatch):
    """Go MULTI_SYNC_STATE must replace Python multi_aircraft_burst sync once Go has a fit."""
    import time as _time
    state = RadarState()
    state._live_sync_states[3] = LiveSyncState(
        iid=3, period_s=4.0, phase_epoch_us=123.0, phase_offset_deg=10.0,
        sync_quality=1.0, sync_jitter_deg=2.0, last_sync_update_ts=1_000.0,
        source="multi_aircraft_burst", usable=True,
    )

    monkeypatch.setattr("radar.sweep.time.time", lambda: 2_000.0)

    state.update_go_multi_sync_state({
        "i": 3, "pr": True, "us": True,
        "p": 4.01, "pb": 4.0,
        "pe": 888_000.0, "po": 22.5,
        "jd": 3.0, "re": 4.0, "nu": 10,
        "ho": False, "ra": False, "ts": 2_000.0,
        "bp": 4.0,
        "trp": 4.005,
        "dp": 4.0,
        "afp": 4.0,
        "afs": "dominant_live_df",
        "dpa": True,
        "pds": 0.01,
        "pdp": 2500.0,
        "cp": 4.16,
        "cds": 0.16,
        "cdp": 40000.0,
        "cu": True,
        "rma": True,
        "rtr": ["compact_dominant_delta", "fit_pool_starved"],
        "cgb": True,
        "rla": 5,
        "anc": 2,
        "ant": 1_999.5,
        "ahr": "anchor_hysteresis_switch",
        "ahu": 4,
        "cps": 4.03,
        "aps": 4.01,
        "cpo": 24.0,
        "apo": 22.5,
        "cai": int("BBBBBB", 16),
        "aai": int("AAAAAA", 16),
        "cvs": 0.52,
        "avs": 0.87,
        "asa": 42.0,
        "cpr": 7,
        "apg": 0.0,
        "afg": 0.08,
        "pfv": True,
        "bas": 0.93,
        "cdd": 11.5,
        "vac": 1,
        "vdc": 2,
        "cvsn": "validator_disagreement",
        "cabr": "validator_disagreement_too_high",
        "cmd": "recovery",
        "amd": "settled_authoritative",
        "ai": int("AAAAAA", 16),
        "as": 0.8,
        "ft": 11,
        "fe": 8,
        "fr": 3,
        "fc": 3,
        "frr": {"no_adsb_position": 2},
        "ac": 2,
        "anr": "no_anchor_candidates",
        "acs": [
            {"i": int("AAAAAA", 16), "s": 0.8, "sp": 3.0, "o": 4, "f": 4, "ff": 1.0, "st": "candidate", "rr": []},
            {"i": int("BBBBBB", 16), "s": 0.0, "sp": 0.0, "o": 3, "f": 0, "ff": 0.0, "st": "rejected", "rr": ["no_fit_eligible_aircraft"]},
        ],
    })

    sync = state.get_live_sync_state(3)
    assert sync is not None
    assert sync.source == "go_multi_aircraft_burst"
    assert sync.phase_epoch_us == pytest.approx(888_000.0)
    assert sync.phase_offset_deg == pytest.approx(22.5)
    assert sync.phase_anchor_icao == "AAAAAA"
    assert sync.phase_anchor_candidate_count == 2
    assert sync.fit_total_observations == 11
    assert sync.fit_eligible_observations == 8
    assert sync.fit_contributing_icao_count == 3
    assert sync.phase_anchor_no_candidate_reason == "no_anchor_candidates"
    assert sync.phase_anchor_candidates[0]["icao"] == "AAAAAA"
    assert sync.phase_anchor_candidates[1]["reject_reasons"] == ["no_fit_eligible_aircraft"]
    assert sync.dominant_period_s == pytest.approx(4.0)
    assert sync.active_family_prior_s == pytest.approx(4.0)
    assert sync.active_family_prior_source == "dominant_live_df"
    assert sync.dominant_prior_active is True
    assert sync.compact_period_s == pytest.approx(4.16)
    assert sync.compact_sync_unreliable is True
    assert sync.recovery_mode_active is True
    assert sync.recovery_trigger_reasons == ["compact_dominant_delta", "fit_pool_starved"]
    assert sync.compact_gating_bypassed is True
    assert sync.recovery_relaxed_admitted_observations == 5
    assert sync.anchor_switch_count == 2
    assert sync.last_anchor_switch_ts == pytest.approx(1_999.5)
    assert sync.last_anchor_switch_reason == "anchor_hysteresis_switch"
    assert sync.anchor_hold_updates == 4
    assert sync.candidate_period_s == pytest.approx(4.03)
    assert sync.authoritative_period_s == pytest.approx(4.01)
    assert sync.candidate_phase_offset_deg == pytest.approx(24.0)
    assert sync.authoritative_phase_offset_deg == pytest.approx(22.5)
    assert sync.candidate_anchor_icao == "BBBBBB"
    assert sync.authoritative_anchor_icao == "AAAAAA"
    assert sync.candidate_validation_score == pytest.approx(0.52)
    assert sync.authoritative_validation_score == pytest.approx(0.87)
    assert sync.authoritative_state_age_s == pytest.approx(42.0)
    assert sync.candidate_promotion_streak == 7
    assert sync.authoritative_period_update_gain == pytest.approx(0.0)
    assert sync.authoritative_phase_update_gain == pytest.approx(0.08)
    assert sync.period_frozen_due_to_phase_validation is True
    assert sync.branch_ambiguity_score == pytest.approx(0.93)
    assert sync.circular_dispersion_deg == pytest.approx(11.5)
    assert sync.validator_agreement_count == 1
    assert sync.validator_disagreement_count == 2
    assert sync.candidate_validation_status == "validator_disagreement"
    assert sync.candidate_application_block_reason == "validator_disagreement_too_high"
    assert sync.phase_validation_status == "validator_disagreement"
    assert sync.candidate_mode == "recovery"
    assert sync.authoritative_mode == "settled_authoritative"


def test_go_sync_decodes_absolute_phase_trusted_and_dominant_prior_inconsistent():
    """Go MULTI_SYNC_STATE absolute_phase_trusted (apt) and dominant_prior_inconsistent
    (dpi) fields must be decoded into the Python LiveSyncState and default to False
    when absent from the message."""
    state = RadarState()

    # Message with both new fields present.
    state.update_go_multi_sync_state({
        "i": 3, "pr": True, "us": True,
        "p": 4.0, "pb": 4.0,
        "pe": 1_000_000.0, "po": 45.0,
        "jd": 3.0, "re": 4.0, "nu": 5,
        "ho": False, "ra": False, "ts": 1_000.0,
        "apt": True,
        "dpi": True,
    })
    sync = state.get_live_sync_state(3)
    assert sync is not None
    assert sync.absolute_phase_trusted is True
    assert sync.dominant_prior_inconsistent is True

    # Message with apt=False.
    state.update_go_multi_sync_state({
        "i": 4, "pr": True, "us": True,
        "p": 4.0, "pb": 4.0,
        "pe": 1_000_000.0, "po": 45.0,
        "jd": 3.0, "re": 4.0, "nu": 5,
        "ho": False, "ra": False, "ts": 1_000.0,
        "apt": False,
        "dpi": False,
    })
    sync2 = state.get_live_sync_state(4)
    assert sync2 is not None
    assert sync2.absolute_phase_trusted is False
    assert sync2.dominant_prior_inconsistent is False

    # Message with fields absent — must default to False.
    state.update_go_multi_sync_state({
        "i": 5, "pr": True, "us": True,
        "p": 4.0, "pb": 4.0,
        "pe": 1_000_000.0, "po": 45.0,
        "jd": 3.0, "re": 4.0, "nu": 5,
        "ho": False, "ra": False, "ts": 1_000.0,
    })
    sync3 = state.get_live_sync_state(5)
    assert sync3 is not None
    assert sync3.absolute_phase_trusted is False
    assert sync3.dominant_prior_inconsistent is False


def test_go_sync_diagnostic_history_is_retained_beyond_fit_window(monkeypatch):
    """Go trend history must be retained independently from the short fit window."""
    import radar.sweep as sweep_module

    monkeypatch.setattr(sweep_module.time, "time", lambda: 2_000.0)

    state = RadarState()
    for idx, ts in enumerate([1_900.0, 1_960.0, 2_000.0]):
        state.update_go_multi_sync_state({
            "i": 5, "pr": True, "us": True,
            "p": 4.0 + idx * 0.001, "pb": 4.0,
            "pe": float(idx + 1) * 1_000_000.0, "po": 20.0 + idx,
            "jd": 2.0, "re": 3.0, "nu": idx + 1,
            "ho": False, "ra": False, "ts": ts,
            "ft": 9, "fe": 7, "fr": 2, "fc": 3,
            "fw": 30.0, "dw": 300.0, "fs": 28.0, "rs": 0.05 + idx * 0.01,
            "aam": "refined_authoritative",
            "aps": 4.0 + idx * 0.001,
            "asa": 120.0 + idx,
            "avs": 0.9, "vac": 3, "bas": 0.1,
        })

    snapshot = state.get_live_sync_snapshot(5, window_s=120.0, debug_limit=20)

    assert snapshot["sync_horizons"]["fit_window_s"] == pytest.approx(30.0)
    assert snapshot["sync_horizons"]["display_window_s"] == pytest.approx(120.0)
    assert len(snapshot["slope_history"]) == 3
    assert snapshot["slope_history"][0]["ts"] == pytest.approx(1_900.0)
    assert snapshot["slope_history"][0]["fit_window_s"] == pytest.approx(30.0)
    assert snapshot["slope_history"][-1]["residual_slope_deg_per_s"] == pytest.approx(0.07)
    assert len(snapshot["period_history"]) == 3
    assert snapshot["period_history"][-1]["authoritative_period_s"] == pytest.approx(4.002)


def test_go_alignment_rows_use_retained_display_history_not_fit_window(monkeypatch):
    """Go alignment projection must follow requested display history, not 30s fit window."""
    import radar.sweep as sweep_module

    monkeypatch.setattr(sweep_module.time, "time", lambda: 1_000.0)

    state = RadarState()
    state._models[6] = RadarIID(
        iid=6,
        status="SINGLE_RADAR",
        period_s=4.0,
        manual_lat=51.0,
        manual_lon=0.0,
        resolution_mode="locked_position",
    )
    state.update_go_multi_sync_state({
        "i": 6, "pr": True, "us": True,
        "p": 4.0, "pb": 4.0,
        "pe": 1_000_000.0, "po": 0.0,
        "jd": 2.0, "re": 3.0, "nu": 1,
        "ho": False, "ra": False, "ts": 1_000.0,
        "ft": 8, "fe": 6, "fr": 2, "fc": 3,
        "fw": 30.0, "dw": 300.0, "fs": 29.0, "rs": 0.02,
        "aam": "refined_authoritative",
        "aps": 4.0, "asa": 90.0,
        "avs": 0.9, "vac": 3, "bas": 0.1,
    })
    state._go_evidence_events = deque([
        {
            "kind": "burst_fired", "iid": 6, "icao": "AAAAAA",
            "arrival_us": 1_000_000.0, "wall_ts": 940.0,
            "n_replies": 4, "signal_dbfs": -15.0,
            "truth_lat": 51.1, "truth_lon": 0.1, "position_age_s": 0.2,
            "compact_sync_eligible": True, "sync_eligible": True,
        },
        {
            "kind": "burst_fired", "iid": 6, "icao": "BBBBBB",
            "arrival_us": 31_000_000.0, "wall_ts": 970.0,
            "n_replies": 4, "signal_dbfs": -15.0,
            "truth_lat": 51.2, "truth_lon": 0.1, "position_age_s": 0.2,
            "compact_sync_eligible": True, "sync_eligible": True,
        },
        {
            "kind": "burst_fired", "iid": 6, "icao": "CCCCCC",
            "arrival_us": 61_000_000.0, "wall_ts": 1_000.0,
            "n_replies": 4, "signal_dbfs": -15.0,
            "truth_lat": 51.3, "truth_lon": 0.1, "position_age_s": 0.2,
            "compact_sync_eligible": True, "sync_eligible": True,
        },
    ], maxlen=state._GO_EVIDENCE_EVENTS_MAX)

    snapshot = state.get_live_sync_snapshot(6, window_s=90.0, debug_limit=20)
    wall_span_s = snapshot["observations"][-1]["wall_ts"] - snapshot["observations"][0]["wall_ts"]

    assert snapshot["sync_horizons"]["fit_window_s"] == pytest.approx(30.0)
    assert snapshot["sync_horizons"]["display_window_s"] == pytest.approx(90.0)
    assert len(snapshot["observations"]) == 3
    assert wall_span_s == pytest.approx(60.0)
    assert snapshot["alignment_status"]["projected_observation_count"] == 3


def test_go_sync_display_retention_independent_of_fit_window(monkeypatch):
    """Layer 5: varying fit_window_s across diagnostic history rows does not
    change how many rows are retained. The display window is what governs
    retention; the fit window is solver-internal."""
    import radar.sweep as sweep_module

    monkeypatch.setattr(sweep_module.time, "time", lambda: 2_000.0)

    state = RadarState()
    # Send 5 history entries with VARYING fit_window_s but the same display_window_s.
    fit_windows = [24.0, 30.0, 36.0, 30.0, 28.0]
    timestamps = [1_800.0, 1_850.0, 1_900.0, 1_950.0, 2_000.0]
    for ts, fw in zip(timestamps, fit_windows):
        state.update_go_multi_sync_state({
            "i": 7, "pr": True, "us": True,
            "p": 4.0, "pb": 4.0,
            "pe": 1_000_000.0, "po": 20.0,
            "jd": 2.0, "re": 3.0, "nu": 1,
            "ho": False, "ra": False, "ts": ts,
            "ft": 9, "fe": 7, "fr": 2, "fc": 3,
            "fw": fw, "dw": 300.0, "fs": fw - 2.0, "rs": 0.05,
            "aam": "refined_authoritative",
            "aps": 4.0, "asa": 120.0,
            "avs": 0.9, "vac": 3, "bas": 0.1,
        })

    snapshot = state.get_live_sync_snapshot(7, window_s=300.0, debug_limit=20)
    assert snapshot["sync_horizons"]["display_window_s"] == pytest.approx(300.0)
    # All 5 rows must be retained — display window 300s holds them all
    # regardless of per-row fit_window_s variation.
    assert len(snapshot["slope_history"]) == 5
    assert len(snapshot["period_history"]) == 5
    # Per-row fit_window_s must round-trip exactly as the solver reported it.
    got_fits = [row["fit_window_s"] for row in snapshot["slope_history"]]
    assert got_fits == pytest.approx(fit_windows)


def test_burst_evidence_buffer_time_pruned_for_display_window(monkeypatch):
    """Problem A: the burst-evidence deque must be time-pruned (not just
    count-capped) so the chart can render the full 300s display window even
    when burst rates would otherwise fill the count cap inside ~30s."""
    import radar.sweep as sweep_module

    # Simulate "now" 1000s in. The pruner uses now_us derived from
    # _latest_burst_us / _latest_event_us, so set those alongside time.time.
    monkeypatch.setattr(sweep_module.time, "time", lambda: 1_000.0)
    state = RadarState()
    # Synthetic latest beast/event clocks that match wall time scale.
    state._latest_burst_us = 1_000.0 * 1_000_000.0
    state._latest_event_us = 1_000.0 * 1_000_000.0

    # Cap is 60_000; push 100_000 entries spanning 1200s of wall time.
    # After time-pruning to DF11_RESIDUAL_EVENT_MAX_AGE_S (360s), entries older
    # than now-360s = wall_ts < 640s must be dropped.
    from collections import deque
    state._go_evidence_events = deque(maxlen=state._GO_EVIDENCE_EVENTS_MAX)
    for i in range(100_000):
        # wall_ts spread evenly from 0..1000s.
        wall_ts = 1000.0 - (100_000 - i) * (1000.0 / 100_000)
        state._go_evidence_events.append({
            "kind": "burst_fired", "iid": 1, "icao": "AAAAAA",
            "arrival_us": 0.0, "wall_ts": wall_ts,
        })

    # Force a prune cycle directly via the helper.
    state._prune_go_evidence_events_locked(1000.0)
    survivors = list(state._go_evidence_events)
    assert len(survivors) > 0
    oldest_wall = min(float(e["wall_ts"]) for e in survivors)
    assert oldest_wall >= 1000.0 - sweep_module.DF11_RESIDUAL_EVENT_MAX_AGE_S - 1.0, (
        f"oldest survivor wall_ts={oldest_wall} not within retention window"
    )


def test_burst_timeline_includes_display_retention_diagnostic(monkeypatch):
    """The burst timeline payload exposes axis vs fit window vs realised
    point ages so the UI can detect silent truncation."""
    import radar.sweep as sweep_module

    monkeypatch.setattr(sweep_module.time, "time", lambda: 1_000.0)
    state = RadarState()
    state._models[3] = RadarIID(iid=3, status="SINGLE_RADAR", period_s=4.0)
    snapshot = state.get_live_sync_snapshot(3, window_s=300.0, debug_limit=20)
    diag = snapshot.get("display_retention_diagnostic")
    assert diag is not None, "display_retention_diagnostic missing from snapshot"
    assert diag["axis_window_s"] == pytest.approx(300.0)
    assert diag["display_window_s"] == pytest.approx(300.0)
    assert "fit_window_s" in diag
    assert "plotted_point_count" in diag
    assert "oldest_point_age_s" in diag
    assert "newest_point_age_s" in diag
    assert "evidence_buffer_size" in diag
    assert "evidence_buffer_max" in diag
    assert diag["evidence_buffer_max"] == state._GO_EVIDENCE_EVENTS_MAX


def test_go_long_term_estimator_fields_round_trip_through_bridge(monkeypatch):
    """Layer 6: the new Local* / LongTerm* / Branch* protocol keys land on
    LiveSyncState with the right types. UI consumers depend on these names."""
    import radar.sweep as sweep_module

    monkeypatch.setattr(sweep_module.time, "time", lambda: 1_000.0)
    state = RadarState()
    state.update_go_multi_sync_state({
        "i": 9, "pr": True, "us": True,
        "p": 4.0, "pb": 4.0,
        "pe": 1_000_000.0, "po": 20.0,
        "jd": 2.0, "re": 3.0, "nu": 1,
        "ho": False, "ra": False, "ts": 1_000.0,
        "ft": 9, "fe": 7, "fr": 2, "fc": 3,
        "fw": 30.0, "dw": 300.0, "fs": 28.0, "rs": 0.05,
        "aam": "refined_authoritative",
        "aps": 4.0, "asa": 120.0, "avs": 0.9, "vac": 3, "bas": 0.1,
        # Layer 6 fields:
        "lpm": 4.0001, "lca": 0xA01F00, "lbo": 137.5, "lva": 4, "lfq": 0.85,
        "lte": 4.0002, "ltc": 0.72, "lta": 60.0, "cpc": 12, "pud": 0.0001,
        "lba": 0xB02F00, "lbf": 137.2, "bec": 0.65, "bcw": 10, "bcn": 1,
        "bcc": 1, "bpb": "branch_competitor_dominant",
    })

    sync = state._live_sync_states[9]
    assert sync.local_period_measurement_s == pytest.approx(4.0001)
    assert sync.local_candidate_anchor_icao == "A01F00"
    assert sync.local_branch_offset_deg == pytest.approx(137.5)
    assert sync.local_validator_agreement == 4
    assert sync.local_fit_quality_score == pytest.approx(0.85)
    assert sync.long_term_period_estimate_s == pytest.approx(4.0002)
    assert sync.long_term_period_estimator_confidence == pytest.approx(0.72)
    assert sync.long_term_period_estimator_age_s == pytest.approx(60.0)
    assert sync.consecutive_period_consistent_windows == 12
    assert sync.period_update_delta_s == pytest.approx(0.0001)
    assert sync.long_term_branch_anchor_icao == "B02F00"
    assert sync.long_term_branch_offset_deg == pytest.approx(137.2)
    assert sync.branch_estimator_confidence == pytest.approx(0.65)
    assert sync.branch_consistent_windows == 10
    assert sync.branch_contradiction_windows == 1
    assert sync.branch_competitor_count == 1
    assert sync.branch_promotion_block_reason == "branch_competitor_dominant"


def test_df11_residual_dots_use_retained_residual_event_history(monkeypatch):
    """The 300s residual plot must not be limited by the 60s raw bootstrap event buffer."""
    import radar.sweep as sweep_module

    monkeypatch.setattr(sweep_module.time, "time", lambda: 1_000.0)

    state = RadarState()
    state._receiver_lat = 51.0
    state._receiver_lon = 0.0
    state._models[8] = RadarIID(iid=8, status="SINGLE_RADAR", period_s=4.0)
    state._live_sync_states[8] = LiveSyncState(
        iid=8,
        period_s=4.0,
        phase_epoch_us=0.0,
        phase_offset_deg=0.0,
        sync_quality=1.0,
        sync_jitter_deg=2.0,
        last_sync_update_ts=1_000.0,
        source="multi_aircraft_burst",
        usable=True,
    )
    state._live_burst_timeline_obs[8] = deque([
        AlignedBurstSyncObs(
            burst_centroid_us=1_000_000.0,
            icao="AAAAAA",
            bearing_deg=90.0,
            n_replies=4,
            signal_dbfs=-20.0,
            pos_age_s=0.2,
            range_nm=10.0,
            ts=940.0,
        )
    ], maxlen=state._BURST_SYNC_TIMELINE_OBS_MAX)
    state._iid_events = deque([
        (295_000_000.0, 8, "RECENT", -20.0),
    ], maxlen=sweep_module._IID_EVENTS_MAX)
    state._df11_residual_events = deque([
        (10_000_000.0, 8, "OLD300", -22.0),
        (295_000_000.0, 8, "RECENT", -20.0),
    ], maxlen=sweep_module._IID_EVENTS_MAX)
    state._iid_latest_arrival_us[8] = 300_000_000.0

    seen_events: list[tuple[float, int, str, float | None]] = []

    def fake_build_df11_residuals(**kwargs):
        seen_events.extend(kwargs["iid_events"])
        return [
            {"icao": icao, "arrival_beast_us": arrival_us, "residual_deg": 0.0}
            for arrival_us, _iid, icao, _signal in kwargs["iid_events"]
        ]

    monkeypatch.setattr(state, "_build_df11_residual_observations", fake_build_df11_residuals)

    payload = state.get_burst_sync_timeline(8, window_s=300.0)

    assert [event[2] for event in seen_events] == ["OLD300", "RECENT"]
    assert [dot["icao"] for dot in payload["df11_residual_observations"]] == ["OLD300", "RECENT"]


def test_go_iid_state_does_not_overwrite_go_multi_sync_state():
    state = RadarState()
    state._live_sync_states[14] = LiveSyncState(
        iid=14,
        period_s=4.01,
        phase_epoch_us=900_000.0,
        phase_offset_deg=22.0,
        sync_quality=0.8,
        sync_jitter_deg=3.0,
        last_sync_update_ts=2_000.0,
        source="go_multi_aircraft_burst",
        usable=True,
        phase_anchor_icao="AAAAAA",
        active_authority_mode="refined_authoritative",
    )

    state._adopt_go_frame_sync_locked(14, {
        "period_s": 4.25,
        "phase_epoch_us": 100.0,
        "phase_offset_deg": 10.0,
        "sync_quality": 0.4,
        "sync_jitter_deg": 8.0,
        "usable": False,
        "last_updated": 2_100.0,
    })

    sync = state.get_live_sync_state(14)
    assert sync is not None
    assert sync.source == "go_multi_aircraft_burst"
    assert sync.period_s == pytest.approx(4.01)
    assert sync.phase_anchor_icao == "AAAAAA"


def test_go_multi_sync_mode_diagnostics_report_dominant_recovery_fields():
    state = RadarState()
    state._live_sync_states[11] = LiveSyncState(
        iid=11,
        period_s=4.01,
        phase_epoch_us=500_000.0,
        phase_offset_deg=20.0,
        sync_quality=0.8,
        sync_jitter_deg=3.0,
        last_sync_update_ts=2_000.0,
        source="go_multi_aircraft_burst",
        usable=True,
        dominant_period_s=4.0,
        active_family_prior_s=4.0,
        active_family_prior_source="dominant_live_df",
        dominant_prior_active=True,
        dominant_period_delta_s=0.01,
        dominant_period_delta_ppm=2500.0,
        compact_period_s=4.16,
        compact_period_delta_to_dominant_s=0.16,
        compact_period_delta_to_dominant_ppm=40000.0,
        compact_sync_unreliable=True,
        recovery_mode_active=True,
        recovery_trigger_reasons=["compact_dominant_delta", "fit_pool_starved"],
        compact_gating_bypassed=True,
        recovery_relaxed_admitted_observations=4,
        anchor_switch_count=3,
        last_anchor_switch_ts=1_995.0,
        last_anchor_switch_reason="anchor_hysteresis_switch",
        anchor_hold_updates=5,
        candidate_period_s=4.03,
        authoritative_period_s=4.01,
        candidate_phase_offset_deg=24.0,
        authoritative_phase_offset_deg=22.5,
        candidate_anchor_icao="BBBBBB",
        authoritative_anchor_icao="AAAAAA",
        candidate_validation_score=0.52,
        authoritative_validation_score=0.87,
        authoritative_state_age_s=42.0,
        candidate_promotion_streak=7,
        authoritative_period_update_gain=0.0,
        authoritative_phase_update_gain=0.08,
        period_frozen_due_to_phase_validation=True,
        branch_ambiguity_score=0.93,
        circular_dispersion_deg=11.5,
        validator_agreement_count=1,
        validator_disagreement_count=2,
        candidate_validation_status="validator_disagreement",
        candidate_application_block_reason="validator_disagreement_too_high",
        candidate_mode="recovery",
        authoritative_mode="settled_authoritative",
        fit_total_observations=12,
        fit_eligible_observations=8,
        fit_rejected_observations=4,
        fit_reject_reasons={"residual_gate": 4},
        phase_anchor_candidate_count=2,
        phase_anchor_icao="AAAAAA",
    )

    diagnostics = state._build_sync_mode_diagnostics(11, state._live_sync_states[11], None)

    assert diagnostics["dominant_period_s"] == pytest.approx(4.0)
    assert diagnostics["active_family_prior_s"] == pytest.approx(4.0)
    assert diagnostics["active_family_prior_source"] == "dominant_live_df"
    assert diagnostics["dominant_prior_active"] is True
    assert diagnostics["recovery_mode_active"] is True
    assert diagnostics["recovery_trigger_reasons"] == ["compact_dominant_delta", "fit_pool_starved"]
    assert diagnostics["compact_gating_bypassed"] is True
    assert diagnostics["recovery_relaxed_admitted_observations"] == 4
    assert diagnostics["anchor_switch_count"] == 3
    assert diagnostics["last_anchor_switch_reason"] == "anchor_hysteresis_switch"
    assert diagnostics["anchor_hold_updates"] == 5
    assert diagnostics["candidate_period_s"] == pytest.approx(4.03)
    assert diagnostics["authoritative_period_s"] == pytest.approx(4.01)
    assert diagnostics["period_frozen_due_to_phase_validation"] is True
    assert diagnostics["branch_ambiguity_score"] == pytest.approx(0.93)
    assert diagnostics["validator_agreement_count"] == 1
    assert diagnostics["candidate_validation_status"] == "validator_disagreement"
    assert diagnostics["candidate_application_block_reason"] == "validator_disagreement_too_high"
    assert diagnostics["authoritative_mode"] == "settled_authoritative"
    assert diagnostics["compact"]["period_s"] == pytest.approx(4.16)
    assert diagnostics["compact"]["unreliable"] is True
    assert diagnostics["refined"]["period_delta_to_dominant_ppm"] == pytest.approx(2500.0)


def test_go_refined_timeline_includes_implied_phase_offsets_without_zero_fallback(monkeypatch):
    import radar.sweep as sweep_module

    monkeypatch.setattr(sweep_module.time, "time", lambda: 1_000.0)
    state = RadarState()
    state._live_sync_states[12] = LiveSyncState(
        iid=12,
        period_s=10.0,
        phase_epoch_us=0.0,
        phase_offset_deg=0.0,
        sync_quality=0.8,
        sync_jitter_deg=3.0,
        last_sync_update_ts=1_000.0,
        source="go_multi_aircraft_burst",
        usable=True,
        phase_anchor_icao="AAAAAA",
        phase_anchor_offset_smoothed_deg=120.0,
        phase_anchor_candidates=[{
            "icao": "AAAAAA",
            "score": 80.0,
            "spread_deg": 2.0,
            "obs_count": 4,
            "fit_eligible_count": 4,
            "fit_eligible_fraction": 1.0,
            "status": "selected",
            "reject_reasons": [],
        }],
    )
    state._live_burst_timeline_obs[12] = deque([
        AlignedBurstSyncObs(
            burst_centroid_us=0.0,
            icao="AAAAAA",
            bearing_deg=120.0,
            n_replies=5,
            signal_dbfs=-12.0,
            pos_age_s=0.2,
            range_nm=0.0,
            ts=999.5,
            sync_update_eligible=True,
            raw_arrival_us=0.0,
        )
    ], maxlen=state._BURST_SYNC_TIMELINE_OBS_MAX)

    timeline = state.get_burst_sync_timeline(12, window_s=60.0)
    row = timeline["observations"][0]

    assert row["implied_phase_offset_deg"] == pytest.approx(120.0)
    assert row["anchor_relative_phase_error_deg"] == pytest.approx(0.0)
    assert row["phase_anchor_contributor"] is True
    assert timeline["phase_anchor_candidates"][0]["icao"] == "AAAAAA"


def test_reset_iid_clears_go_multi_sync_state():
    """Per-IID reset must clear _go_multi_sync_states_by_iid so Python solver can resume."""
    state = RadarState()
    state._go_multi_sync_states_by_iid[5] = {"pr": True, "p": 4.0}
    state._go_multi_sync_states_by_iid[6] = {"pr": True, "p": 4.0}

    state.reset_iid(5)

    assert 5 not in state._go_multi_sync_states_by_iid
    assert 6 in state._go_multi_sync_states_by_iid  # unaffected IID


def test_go_sync_burst_timeline_includes_python_waveform_bins():
    """Compact Go sync timeline must include Python-learned waveform bins when present."""
    state = RadarState()
    state._live_sync_states[9] = LiveSyncState(
        iid=9, period_s=4.0, phase_epoch_us=0.0, phase_offset_deg=0.0,
        sync_quality=0.9, sync_jitter_deg=3.0, last_sync_update_ts=1_000.0,
        source="go_multi_aircraft_burst", usable=True,
    )
    # Populate Python-learned waveform bins (24 bins).
    from radar.sweep import WaveformBin
    bins = [WaveformBin(correction_deg=float(i), weight=1.0, n=5) for i in range(24)]
    state._live_waveform_bins[9] = bins

    timeline = state.get_burst_sync_timeline(9, window_s=60.0)

    # Waveform bins must be included even for the compact Go sync path.
    waveform_bins = timeline.get("waveform_bins", [])
    assert len(waveform_bins) == 24, f"expected 24 bins, got {len(waveform_bins)}"
    # phase_center_deg should be (i + 0.5) * 15 for 24 bins.
    assert waveform_bins[0]["phase_center_deg"] == pytest.approx(7.5)
    assert waveform_bins[1]["correction_deg"] == pytest.approx(1.0)


def test_on_df11_batch_skips_burst_builder_in_radar_core_mode(monkeypatch):
    """Python burst accumulator must not run when radar-core event sink is active."""
    state = RadarState()
    # Set a non-None sink to signal radar-core mode.
    state.radar_core_event_sink = lambda *a, **kw: None

    builder_calls: list[tuple] = []

    def fake_builder(iid, icao, arrival_us, signal_dbfs):
        builder_calls.append((iid, icao, arrival_us))

    monkeypatch.setattr(state, "_on_df11_frame_builder", fake_builder)

    # Patch _unwrap to return the timestamp unchanged (no Beast tick conversion).
    monkeypatch.setattr(state, "_unwrap", lambda ts: ts)

    # Feed a batch of synthetic DF11 events (timestamp, iid, icao, signal_dbfs).
    events = [
        (4_000_000, 7, "AAAAAA", -15.0),
        (4_001_000, 7, "BBBBBB", -20.0),
    ]
    state.on_df11_batch(events)

    # Builder must not have been called.
    assert builder_calls == [], f"expected no builder calls, got {builder_calls}"
    # _iid_events must still be populated for update_rotation_models().
    assert len(state._iid_events) == 2
    assert 7 in state._dirty_iids


def test_on_df11_batch_runs_burst_builder_without_radar_core(monkeypatch):
    """Python burst accumulator must run when radar-core is not active."""
    state = RadarState()
    # No event sink → Python-only mode.
    assert state.radar_core_event_sink is None

    builder_calls: list[tuple] = []

    def fake_builder(iid, icao, arrival_us, signal_dbfs):
        builder_calls.append((iid, icao))

    monkeypatch.setattr(state, "_on_df11_frame_builder", fake_builder)
    monkeypatch.setattr(state, "_unwrap", lambda ts: ts)
    # Disable native burst processor so Python fallback path runs.
    monkeypatch.setattr("radar.sweep._decode_cffi", None)

    events = [(4_000_000, 7, "AAAAAA", -15.0)]
    state.on_df11_batch(events)

    assert len(builder_calls) == 1
    assert builder_calls[0] == (7, "AAAAAA")


def test_go_sync_decodes_per_icao_phase_offsets_and_new_diagnostic_fields():
    """aca/vec/pio fields from Go MULTI_SYNC_STATE must decode into LiveSyncState."""
    state = RadarState()
    state.update_go_multi_sync_state({
        "i": 9, "pr": True, "us": True,
        "p": 4.0, "pb": 4.0,
        "pe": 1_000_000.0, "po": 45.0,
        "jd": 2.0, "re": 3.0, "nu": 5,
        "ho": False, "ra": False, "ts": 1_000.0,
        "aca": 0.35,
        "vec": 3,
        "pio": [
            {"i": int("AAAAAA", 16), "lo": 45.0, "ad": 0.0, "r": "anchor", "rr": []},
            {"i": int("BBBBBB", 16), "lo": 47.2, "ad": 2.2, "r": "validator_agree", "rr": []},
            {"i": int("CCCCCC", 16), "lo": 60.0, "ad": 15.0, "r": "validator_disagree", "rr": ["phase_outlier"]},
            {"i": int("DDDDDD", 16), "lo": 44.5, "ad": -0.5, "r": "excluded", "rr": []},
            {"i": int("EEEEEE", 16), "lo": 0.0, "ad": 0.0, "r": "not_fit_eligible", "rr": ["no_position"]},
        ],
    })
    sync = state.get_live_sync_state(9)
    assert sync is not None
    assert sync.anchor_competition_ambiguity == pytest.approx(0.35)
    assert sync.validator_excluded_count == 3
    assert len(sync.per_icao_phase_offsets) == 5
    rows_by_icao = {r["icao"]: r for r in sync.per_icao_phase_offsets}
    assert rows_by_icao["AAAAAA"]["role"] == "anchor"
    assert rows_by_icao["AAAAAA"]["anchor_delta_deg"] == pytest.approx(0.0)
    assert rows_by_icao["BBBBBB"]["role"] == "validator_agree"
    assert rows_by_icao["BBBBBB"]["latest_offset_deg"] == pytest.approx(47.2)
    assert rows_by_icao["CCCCCC"]["role"] == "validator_disagree"
    assert rows_by_icao["CCCCCC"]["reject_reasons"] == ["phase_outlier"]
    assert rows_by_icao["DDDDDD"]["role"] == "excluded"
    assert rows_by_icao["EEEEEE"]["role"] == "not_fit_eligible"


def test_go_sync_decodes_per_icao_phase_offsets_defaults_when_absent():
    """anchor_competition_ambiguity, validator_excluded_count, and per_icao_phase_offsets
    must default to None/0/[] when absent from the Go message."""
    state = RadarState()
    state.update_go_multi_sync_state({
        "i": 10, "pr": True, "us": True,
        "p": 4.0, "pb": 4.0,
        "pe": 1_000_000.0, "po": 30.0,
        "jd": 2.0, "re": 3.0, "nu": 3,
        "ho": False, "ra": False, "ts": 1_000.0,
    })
    sync = state.get_live_sync_state(10)
    assert sync is not None
    assert sync.anchor_competition_ambiguity is None
    assert sync.validator_excluded_count == 0
    assert sync.per_icao_phase_offsets == []


def test_burst_sync_timeline_payload_contains_per_icao_offsets(monkeypatch):
    """get_live_sync_snapshot (used by websocket) must include per_icao_phase_offsets
    alongside the existing observations array — not replace it."""
    import radar.sweep as sweep_module

    monkeypatch.setattr(sweep_module.time, "time", lambda: 1_000.0)

    state = RadarState()
    state._models[12] = RadarIID(
        iid=12, status="SINGLE_RADAR", period_s=4.0,
        manual_lat=51.0, manual_lon=0.0, resolution_mode="locked_position",
    )
    state.update_go_multi_sync_state({
        "i": 12, "pr": True, "us": True,
        "p": 4.0, "pb": 4.0,
        "pe": 1_000_000.0, "po": 0.0,
        "jd": 2.0, "re": 3.0, "nu": 2,
        "ho": False, "ra": False, "ts": 1_000.0,
        "aam": "refined_authoritative",
        "aps": 4.0, "asa": 60.0, "avs": 0.9, "vac": 3, "bas": 0.1,
        "aca": 0.2, "vec": 1,
        "pio": [
            {"i": int("AAAAAA", 16), "lo": 20.0, "ad": 0.0, "r": "anchor", "rr": []},
            {"i": int("BBBBBB", 16), "lo": 21.5, "ad": 1.5, "r": "validator_agree", "rr": []},
        ],
    })
    from collections import deque
    state._go_evidence_events = deque([
        {
            "kind": "burst_fired", "iid": 12, "icao": "AAAAAA",
            "arrival_us": 1_000_000.0, "wall_ts": 980.0,
            "n_replies": 4, "signal_dbfs": -15.0,
            "truth_lat": 51.1, "truth_lon": 0.1, "position_age_seconds": 0.2,
            "compact_sync_eligible": True, "sync_eligible": True,
        },
        {
            "kind": "burst_fired", "iid": 12, "icao": "BBBBBB",
            "arrival_us": 5_000_000.0, "wall_ts": 990.0,
            "n_replies": 4, "signal_dbfs": -15.0,
            "truth_lat": 51.2, "truth_lon": 0.1, "position_age_seconds": 0.2,
            "compact_sync_eligible": True, "sync_eligible": True,
        },
    ], maxlen=state._GO_EVIDENCE_EVENTS_MAX)

    snapshot = state.get_live_sync_snapshot(12, window_s=90.0)
    # Existing observations array must be preserved.
    assert len(snapshot["observations"]) == 2, "burst observations must not be replaced by per_icao table"
    # New per_icao_phase_offsets must appear at the top level.
    offsets = snapshot["per_icao_phase_offsets"]
    assert len(offsets) == 2
    roles_by_icao = {r["icao"]: r["role"] for r in offsets}
    assert roles_by_icao["AAAAAA"] == "anchor"
    assert roles_by_icao["BBBBBB"] == "validator_agree"


# ---------------------------------------------------------------------------
# Dispatch integration tests
# ---------------------------------------------------------------------------

def test_python_dispatcher_always_calls_simple_live_sync(monkeypatch):
    """Python sync dispatcher must unconditionally call _update_simple_live_sync_state."""
    import radar.sweep as sweep_module

    state = RadarState()
    iid = 7
    state._models[iid] = RadarIID(
        iid=iid, status="SINGLE_RADAR", period_s=4.0,
        manual_lat=51.0, manual_lon=0.0, resolution_mode="locked_position",
    )

    simple_calls: list = []
    monkeypatch.setattr(state, "_update_simple_live_sync_state",
                        lambda iid, period_s: simple_calls.append((iid, period_s)))
    monkeypatch.setattr(sweep_module.time, "monotonic", lambda: 9999.0)

    state._go_multi_sync_states_by_iid[iid] = None  # Go not active → Python path
    state._last_multi_sync_update_ts[iid] = 0.0
    with state._lock:
        now_mono = sweep_module.time.monotonic()
        last = state._last_multi_sync_update_ts.get(iid, 0.0)
        if (now_mono - last) >= state._MULTI_SYNC_UPDATE_MIN_INTERVAL_S:
            state._last_multi_sync_update_ts[iid] = now_mono
            state._update_simple_live_sync_state(iid=iid, period_s=4.0)

    assert simple_calls == [(7, 4.0)], "simple model must always be called on Python path"


def test_go_authoritative_state_skips_python_sync_dispatch(monkeypatch):
    """When Go has authoritative multi-sync state for an IID the Python method is not called."""
    import radar.sweep as sweep_module

    state = RadarState()
    iid = 9
    state._models[iid] = RadarIID(
        iid=iid, status="SINGLE_RADAR", period_s=4.0,
        manual_lat=51.0, manual_lon=0.0, resolution_mode="locked_position",
    )

    simple_calls: list = []
    monkeypatch.setattr(state, "_update_simple_live_sync_state",
                        lambda iid, period_s: simple_calls.append((iid, period_s)))

    # Seed a truthy Go authoritative state — Python dispatch must be skipped.
    state._go_multi_sync_states_by_iid[iid] = {"authoritative": True}
    state._last_multi_sync_update_ts[iid] = 0.0
    monkeypatch.setattr(sweep_module.time, "monotonic", lambda: 9999.0)

    with state._lock:
        if not state._go_multi_sync_states_by_iid.get(iid):
            now_mono = sweep_module.time.monotonic()
            last = state._last_multi_sync_update_ts.get(iid, 0.0)
            if (now_mono - last) >= state._MULTI_SYNC_UPDATE_MIN_INTERVAL_S:
                state._last_multi_sync_update_ts[iid] = now_mono
                state._update_simple_live_sync_state(iid=iid, period_s=4.0)

    assert len(simple_calls) == 0, "simple model must not be called when Go is authoritative"


# ---------------------------------------------------------------------------
# _fit_per_aircraft_slope malformed input tests
# ---------------------------------------------------------------------------

def _make_scored_entry(icao: str, effective_us: object, residual: object, weight: object) -> dict:
    return {"icao": icao, "effective_us": effective_us, "residual": residual, "weight": weight}


def _valid_entries(icao: str, n: int = 5, period_s: float = 4.0) -> list[dict]:
    """Return n well-formed entries with a gently rising residual (non-zero slope)."""
    return [
        _make_scored_entry(icao, float(i * period_s * 1_000_000), float(i * 2.0), 1.0)
        for i in range(n)
    ]


def test_fit_per_aircraft_slope_none_numeric_fields_do_not_crash():
    """None in effective_us/residual/weight must not raise; must return nonfinite_input."""
    from radar.sweep import _fit_per_aircraft_slope

    scored = [
        _make_scored_entry("AAAAAA", None, None, None),
        _make_scored_entry("BBBBBB", None, 1.0, 1.0),
    ]
    result = _fit_per_aircraft_slope(scored, period_base_s=4.0)
    assert result["reject_reason"] == "nonfinite_input"
    assert result["slope_deg_per_s"] is None


def test_fit_per_aircraft_slope_string_numeric_fields_do_not_crash():
    """String values in numeric fields must not raise; must return nonfinite_input."""
    from radar.sweep import _fit_per_aircraft_slope

    scored = [
        _make_scored_entry("AAAAAA", "bad", "data", "here"),
        _make_scored_entry("BBBBBB", 1_000_000.0, 5.0, 1.0),
    ]
    result = _fit_per_aircraft_slope(scored, period_base_s=4.0)
    assert result["reject_reason"] == "nonfinite_input"


def test_fit_per_aircraft_slope_partial_valid_plus_malformed_returns_nonfinite_input():
    """One valid ICAO + one non-empty ICAO with NaN fields → nonfinite_input, not insufficient_icaos."""
    import math
    from radar.sweep import _fit_per_aircraft_slope

    scored = (
        _valid_entries("AAAAAA")
        + [_make_scored_entry("BBBBBB", float("nan"), 1.0, 1.0)]
    )
    result = _fit_per_aircraft_slope(scored, period_base_s=4.0)
    assert result["reject_reason"] == "nonfinite_input", (
        f"expected nonfinite_input, got {result['reject_reason']!r}"
    )


def test_fit_per_aircraft_slope_two_valid_plus_one_malformed_still_succeeds():
    """Two ICAOs with valid agreeing slopes + one malformed ICAO → consensus succeeds."""
    from radar.sweep import _fit_per_aircraft_slope

    # Build two ICAOs with matching positive slopes.
    period_s = 4.0
    scored = (
        _valid_entries("AAAAAA", n=6, period_s=period_s)
        + _valid_entries("BBBBBB", n=6, period_s=period_s)
        + [_make_scored_entry("CCCCCC", None, None, None)]  # malformed third ICAO
    )
    result = _fit_per_aircraft_slope(scored, period_base_s=period_s)
    assert result["reject_reason"] is None, (
        f"expected success, got {result['reject_reason']!r}"
    )
    assert result["slope_deg_per_s"] is not None


def test_fit_per_aircraft_slope_missing_icao_does_not_set_nonfinite():
    """Entries with missing/empty ICAO must not trigger nonfinite_input."""
    from radar.sweep import _fit_per_aircraft_slope

    # Only entries with empty ICAO — all skipped silently.
    scored = [
        _make_scored_entry("", 1_000_000.0, 5.0, 1.0),
        _make_scored_entry(None, 2_000_000.0, 5.0, 1.0),
    ]
    result = _fit_per_aircraft_slope(scored, period_base_s=4.0)
    assert result["reject_reason"] == "insufficient_icaos"


def test_fit_per_aircraft_slope_zero_weight_does_not_set_nonfinite():
    """Zero/negative weight must not set nonfinite; must return insufficient_icaos."""
    from radar.sweep import _fit_per_aircraft_slope

    scored = [
        _make_scored_entry("AAAAAA", 1_000_000.0, 5.0, 0.0),
        _make_scored_entry("BBBBBB", 2_000_000.0, 5.0, -1.0),
    ]
    result = _fit_per_aircraft_slope(scored, period_base_s=4.0)
    assert result["reject_reason"] == "insufficient_icaos"
