import socket
import threading
import time

import msgpack

from radar_core import protocol as P
from radar_core.client import RadarCoreClient
from aircraft_state import Aircraft, AircraftState


def test_radar_core_client_uses_one_bidirectional_socket(tmp_path):
    sock_path = str(tmp_path / "radar-core.sock")
    listener = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
    listener.bind(sock_path)
    listener.listen(4)
    listener.settimeout(0.1)

    stop = threading.Event()
    got_callback = threading.Event()
    accepted: list[socket.socket] = []
    received: list[dict] = []

    def serve() -> None:
        while not stop.is_set():
            try:
                conn, _ = listener.accept()
            except TimeoutError:
                continue
            except OSError:
                if stop.is_set():
                    return
                raise
            accepted.append(conn)
            if len(accepted) > 1:
                continue
            rfile = conn.makefile("rb", buffering=0)
            wfile = conn.makefile("wb", buffering=0)
            payload = P.read_frame(rfile)
            received.append(P.dispatch(payload))
            P.write_frame(wfile, P.encode({
                "t": P.MSG_BURST_FIRED,
                "i": 7,
                "c": 0xABCDEF,
                "cu": 123.0,
                "n": 2,
                "s": None,
            }))
            got_callback.wait(1.0)

    server_thread = threading.Thread(target=serve, daemon=True)
    server_thread.start()

    callbacks: list[dict] = []
    client = RadarCoreClient(
        sock_path,
        on_burst_fired=lambda msg: (callbacks.append(msg), got_callback.set()),
        reconnect_delay_s=0.05,
    )
    client.start()
    try:
        client.send_radar_event(123.0, 7, 0xABCDEF, None)
        assert got_callback.wait(2.0)
        time.sleep(0.2)
        assert len(accepted) == 1
        assert received[0]["t"] == P.MSG_RADAR_EVENT
        assert callbacks[0]["t"] == P.MSG_BURST_FIRED
        assert client.stats()["bursts_received"] == 1
    finally:
        client.stop()
        stop.set()
        for conn in accepted:
            conn.close()
        listener.close()
        server_thread.join(timeout=1.0)


def test_radar_core_client_encodes_position_update_with_nullable_altitude():
    payload = RadarCoreClient._encode_item((
        "position_update",
        0x123456,
        51.5,
        -0.12,
        None,
        1_700_000_000.5,
    ))
    msg = P.dispatch(payload)
    assert msg == {
        "t": P.MSG_POSITION_UPDATE,
        "c": 0x123456,
        "la": 51.5,
        "lo": -0.12,
        "al": None,
        "ts": 1_700_000_000.5,
    }


def test_radar_core_client_frame_ready_callback_and_stats():
    frames: list[dict] = []
    client = RadarCoreClient("/tmp/unused-radar-core.sock", on_frame_ready=frames.append)

    msg = {
        "t": P.MSG_FRAME_READY,
        "i": 2,
        "fi": 9,
        "p": 4.0,
        "rc": 0xAAAAAA,
        "rla": 51.0,
        "rlo": -1.0,
        "ra": 123.0,
        "obs": [],
        "q": "marginal",
    }
    client._handle_outbound(msg)

    assert frames == [msg]
    assert client.stats()["frames_received"] == 1


def test_radar_core_client_iid_state_callback_and_stats():
    states: list[dict] = []
    client = RadarCoreClient("/tmp/unused-radar-core.sock", on_iid_state=states.append)

    msg = {
        "t": P.MSG_IID_STATE,
        "i": 2,
        "st": "SINGLE_RADAR",
        "p": 4.0,
        "sq": 0.9,
        "sp": True,
        "su": True,
        "sps": 4.0,
        "sep": 12345.0,
        "sod": 15.0,
        "sj": 2.5,
        "sre": 4.0,
        "slr": 1.5,
        "snf": 6,
        "snr": 1,
        "sh": False,
        "nb": 10,
        "lu": 1000.0,
        "rv": 3,
    }
    client._handle_outbound(msg)

    assert states == [msg]
    stats = client.stats()
    assert stats["iid_states_received"] == 1
    assert stats["callback_errors"] == 0


def test_radar_core_client_records_snapshot_response_in_stats():
    client = RadarCoreClient("/tmp/unused-radar-core.sock")
    client._handle_outbound({
        "t": P.MSG_SNAPSHOT_RESP,
        "r": 9,
        "pl": {"frames_emitted": 3, "iids": {"7": {"has_period": True}}},
    })
    stats = client.stats()
    assert stats["latest_snapshot"]["frames_emitted"] == 3
    assert stats["latest_snapshot"]["iids"]["7"]["has_period"] is True


def test_radar_core_client_exposes_snapshot_observation_helpers():
    client = RadarCoreClient("/tmp/unused-radar-core.sock")
    client._handle_outbound({
        "t": P.MSG_SNAPSHOT_RESP,
        "r": 2,
        "pl": {
            "track_observations": [
                {"iid": 7, "icao": 0xABCDEF, "arrival_us": 12.0},
                {"iid": 8, "icao": 0x123456, "arrival_us": 13.0},
            ],
            "evidence_events": [
                {"iid": 7, "icao": 0xABCDEF, "arrival_us": 12.0, "kind": "burst_fired"},
            ],
        },
    })
    assert len(client.latest_track_observations()) == 2
    assert client.latest_track_observations(iid=7) == [{"iid": 7, "icao": 0xABCDEF, "arrival_us": 12.0}]
    assert client.latest_evidence_events(iid=7)[0]["kind"] == "burst_fired"


def test_radar_core_client_drops_bad_decode_frame_and_recovers_on_reconnect(tmp_path):
    sock_path = str(tmp_path / "radar-core.sock")
    listener = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
    listener.bind(sock_path)
    listener.listen(4)
    listener.settimeout(0.1)

    got_burst = threading.Event()

    def serve() -> None:
        conn1, _ = listener.accept()
        wfile1 = conn1.makefile("wb", buffering=0)
        bad_payload = (
            msgpack.packb({"t": P.MSG_HEALTH, "up": 1.0}, use_bin_type=True)
            + msgpack.packb({"t": P.MSG_HEALTH, "up": 2.0}, use_bin_type=True)
        )
        P.write_frame(wfile1, bad_payload)
        conn1.close()

        conn2, _ = listener.accept()
        wfile2 = conn2.makefile("wb", buffering=0)
        P.write_frame(wfile2, P.encode({
            "t": P.MSG_BURST_FIRED,
            "i": 3,
            "c": 0xABCDEF,
            "cu": 44.0,
            "n": 2,
            "s": None,
        }))
        got_burst.wait(1.0)
        conn2.close()

    server_thread = threading.Thread(target=serve, daemon=True)
    server_thread.start()

    callbacks: list[dict] = []
    client = RadarCoreClient(
        sock_path,
        on_burst_fired=lambda msg: (callbacks.append(msg), got_burst.set()),
        reconnect_delay_s=0.05,
    )
    client.start()
    try:
        assert got_burst.wait(2.0)
        stats = client.stats()
        assert stats["bursts_received"] == 1
        assert stats["connect_attempts"] >= 2
        assert callbacks[0]["t"] == P.MSG_BURST_FIRED
    finally:
        client.stop()
        listener.close()
        server_thread.join(timeout=1.0)


def test_positions_snapshot_exposes_live_radar_core_forwarding_fields():
    state = AircraftState()
    now = time.time()
    ac = Aircraft("ABCDEF")
    ac.lat = 51.5
    ac.lon = -0.12
    ac.altitude = 12000
    ac.alt_reliable = 2
    ac.pos_global = True
    ac.pos_reliable_odd = 2.0
    ac.pos_reliable_even = 2.0
    ac.last_pos_ts = now
    ac.last_alt_ts = now

    with state._lock:
        state._aircraft[ac.icao] = ac

    positions = state.get_positions_snapshot()

    assert len(positions) == 1
    pos = positions[0]
    assert pos["icao"] == "ABCDEF"
    assert pos["lat"] == 51.5
    assert pos["lon"] == -0.12
    assert pos["altitude"] == 12000
    assert pos["last_pos_ts"] == ac.last_pos_ts
    assert pos["last_pos_age"] <= 0.1
    assert pos["pos_confident"] is True


def test_radar_state_prefers_go_track_observations_for_live_detections():
    from radar.sweep import RadarState

    radar_state = RadarState(receiver_lat=51.5, receiver_lon=-0.12)
    radar_state.record_live_radar_detection(
        iid=1,
        icao="AAAAAA",
        arrival_us=10.0,
        df=11,
        signal_dbfs=-20.0,
        truth_lat=51.0,
        truth_lon=-1.0,
        position_age_seconds=0.5,
        association_confidence=1.0,
    )
    radar_state.update_go_burst_fired({
        "i": 2,
        "c": 0xABCDEF,
        "cu": 22.0,
        "n": 3,
        "s": -18.0,
        "la": 52.0,
        "lo": -0.5,
        "pa": 0.4,
        "df": True,
        "se": True,
    })

    detections = radar_state.get_recent_live_detections()
    assert len(detections) == 1
    assert detections[0].iid == 2
    assert detections[0].icao == "ABCDEF"
    assert detections[0].truth_lat == 52.0


def test_radar_state_prefers_go_frame_ready_for_sweep_frame_reads():
    from radar.sweep import RadarState
    from radar.models import LiveFrameState, SweepFrameObservation

    state = RadarState()
    state._live_completed_frames[7] = []
    state._live_frame_counters[7] = 1
    state._live_frames[7] = LiveFrameState(
        ref_icao="AAAAAA",
        ref_lat=51.0,
        ref_lon=-1.0,
        ref_arrival_us=1000.0,
        observations=[
            SweepFrameObservation(
                icao="BBBBBB",
                lat=51.1,
                lon=-1.1,
                arrival_us=1100.0,
            )
        ],
        seen_icaos={"AAAAAA", "BBBBBB"},
    )

    state.update_go_frame_ready({
        "i": 7,
        "fi": 3,
        "p": 4.0,
        "rc": 0xCCCCCC,
        "rla": 52.0,
        "rlo": -0.5,
        "ra": 2000.0,
        "obs": [
            {"c": 0xDDDDDD, "la": 52.1, "lo": -0.6, "a": 2200.0, "n": 3, "pa": 0.4},
        ],
        "q": "marginal",
    })

    frames = state.get_sweep_frames(7)
    assert len(frames) == 1
    assert frames[0].frame_index == 3
    assert frames[0].ref_icao == "CCCCCC"
    assert frames[0].observations[0].icao == "DDDDDD"


def test_radar_state_go_snapshot_drives_reference_and_frame_summary_reads():
    from radar.sweep import RadarState

    state = RadarState()
    state.update_go_snapshot({
        "iids": {
            "12": {
                "iid": 12,
                "has_reference_icao": True,
                "reference_icao": 0xABCDEF,
            }
        },
        "sweep_frames": [
            {
                "iid": 12,
                "frame_index": 9,
                "period_s": 4.0,
                "ref_icao": 0xABCDEF,
                "ref_lat": 51.5,
                "ref_lon": -0.2,
                "ref_arrival_us": 12345.0,
                "quality": "good",
                "observations": [
                    {
                        "icao": 0x010203,
                        "lat": 51.6,
                        "lon": -0.25,
                        "arrival_us": 12400.0,
                        "n_replies": 2,
                        "position_age_s": 0.3,
                    }
                ],
            }
        ],
    })

    reference = state.get_reference_aircraft(12)
    assert reference["status"] == "SELECTED"
    assert reference["ref_icao"] == "ABCDEF"
    assert reference["source"] == "go_snapshot"

    summary = state.get_sweep_frame_summary_payload(12)
    assert summary["n_frames"] == 1
    assert summary["frames"][0]["frame_index"] == 9
    assert summary["frames"][0]["ref_icao"] == "ABCDEF"
    assert summary["frames"][0]["observations"][0]["icao"] == "010203"
