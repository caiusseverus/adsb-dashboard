import socket
import threading
import time

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
