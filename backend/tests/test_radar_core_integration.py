"""
Integration test: start radar-core binary, send RADAR_EVENT messages from Python,
verify BURST_FIRED messages come back with correct centroids.

Requires the radar-core binary at /tmp/radar-core (built by the Go module).
Skipped automatically if the binary is not present.
"""

import math
import os
import socket
import subprocess
import tempfile
import time
import io

import pytest

from radar_core import protocol as P

BINARY = "/tmp/radar-core"
BURST_GAP_US = 200_000.0


@pytest.fixture(scope="module")
def radar_core_proc():
    """Start radar-core with a temp socket path, yield the path, then stop."""
    if not os.path.exists(BINARY):
        pytest.skip("radar-core binary not found at /tmp/radar-core")

    with tempfile.TemporaryDirectory() as tmpdir:
        sock_path = os.path.join(tmpdir, "radar-core.sock")
        proc = subprocess.Popen(
            [BINARY, "--socket", sock_path],
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
        )
        deadline = time.monotonic() + 5.0
        while not os.path.exists(sock_path):
            if time.monotonic() > deadline:
                proc.terminate()
                raise RuntimeError("radar-core socket did not appear")
            time.sleep(0.05)

        yield sock_path

        proc.terminate()
        try:
            proc.wait(timeout=3.0)
        except subprocess.TimeoutExpired:
            proc.kill()


class RC:
    """Helper: wraps a connected Unix socket with send/recv helpers."""

    def __init__(self, sock_path: str):
        self._sock = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
        self._sock.connect(sock_path)
        self._wfile = self._sock.makefile("wb", buffering=0)
        self._rfile = self._sock.makefile("rb", buffering=0)

    def send(self, payload: bytes):
        P.write_frame(self._wfile, payload)

    def send_event(self, arrival_us: float, iid: int, icao: int, signal_dbfs=None):
        self.send(P.radar_event(arrival_us, iid, icao, signal_dbfs))

    def recv(self, timeout: float = 2.0) -> dict:
        self._sock.settimeout(timeout)
        payload = P.read_frame(self._rfile)
        return P.dispatch(payload)

    def close(self):
        self._sock.close()


class TestBurstCentroidAgreement:
    """BURST_FIRED centroids must match the C algorithm to ±2µs."""

    def test_single_icao_no_signal(self, radar_core_proc):
        rc = RC(radar_core_proc)
        # Replies at 0µs and 100µs — centroid = mean = 50µs
        rc.send_event(0.0,       3, 0xAA)
        rc.send_event(100.0,     3, 0xAA)
        rc.send_event(300_000.0, 3, 0xBB)  # gap trigger

        msg = rc.recv()
        assert msg["t"] == P.MSG_BURST_FIRED, f"expected BURST_FIRED got {msg}"
        assert msg["c"] == 0xAA
        assert abs(msg["cu"] - 50.0) < 2.0, f"centroid {msg['cu']} not within 2µs of 50.0"
        assert msg["n"] == 2
        assert msg["s"] is None
        rc.close()

    def test_signal_weighted_centroid(self, radar_core_proc):
        rc = RC(radar_core_proc)
        sig1, sig2 = -30.0, -20.0
        w1 = math.pow(10, sig1 / 20)
        w2 = math.pow(10, sig2 / 20)
        t1, t2 = 0.0, 1000.0
        want = (t1 * w1 + t2 * w2) / (w1 + w2)

        rc.send_event(t1,        5, 0xCC, sig1)
        rc.send_event(t2,        5, 0xCC, sig2)
        rc.send_event(300_000.0, 5, 0xDD)  # trigger

        msg = rc.recv()
        assert msg["t"] == P.MSG_BURST_FIRED
        assert msg["c"] == 0xCC
        assert abs(msg["cu"] - want) < 2.0, f"centroid {msg['cu']} ≠ {want}"
        assert msg["s"] is not None
        assert abs(msg["s"] - (-20.0)) < 0.01  # strongest signal
        rc.close()

    def test_multi_icao_fires_sorted(self, radar_core_proc):
        rc = RC(radar_core_proc)
        # 0x11 → centroid ~50µs; 0x22 → centroid ~1050µs
        rc.send_event(0.0,       7, 0x11)
        rc.send_event(100.0,     7, 0x11)
        rc.send_event(1000.0,    7, 0x22)
        rc.send_event(1100.0,    7, 0x22)
        rc.send_event(500_000.0, 7, 0x33)  # gap trigger

        msgs = [rc.recv(), rc.recv()]
        for m in msgs:
            assert m["t"] == P.MSG_BURST_FIRED

        centroids = [m["cu"] for m in msgs]
        assert centroids == sorted(centroids), f"not sorted: {centroids}"
        assert abs(centroids[0] - 50.0)   < 2.0
        assert abs(centroids[1] - 1050.0) < 2.0
        rc.close()

    def test_snapshot_req(self, radar_core_proc):
        rc = RC(radar_core_proc)
        rc.send(P.snapshot_req(req_id=42, scope="all"))

        msg = rc.recv()
        assert msg["t"] == P.MSG_SNAPSHOT_RESP
        assert msg["r"] == 42
        assert "uptime_s" in msg["pl"]
        rc.close()

    def test_config_update_burst_gap(self, radar_core_proc):
        rc = RC(radar_core_proc)
        # Tighten burst gap to 50µs
        rc.send(P.config_update("BURST_GAP_US", 50.0))
        time.sleep(0.05)

        # Gap between 0µs and 100µs is 100µs > 50µs → fires pending burst on second event
        rc.send_event(0.0,   9, 0xEE)
        rc.send_event(100.0, 9, 0xEE)

        msg = rc.recv()
        assert msg["t"] == P.MSG_BURST_FIRED
        assert msg["c"] == 0xEE
        assert msg["n"] == 1  # only the first reply was in the burst
        rc.close()

    def test_reset_iid(self, radar_core_proc):
        rc = RC(radar_core_proc)
        rc.send(P.config_update("BURST_GAP_US", 200_000.0))  # restore default
        time.sleep(0.05)

        rc.send_event(0.0,  11, 0xAB)
        rc.send_event(50.0, 11, 0xAB)
        rc.send(P.reset_iid(11))
        time.sleep(0.05)

        # Trigger: only 0xCD may fire, not 0xAB (was reset)
        rc.send_event(300_000.0, 11, 0xCD)

        rc._sock.settimeout(0.5)
        try:
            msg = P.dispatch(P.read_frame(rc._rfile))
            if msg["t"] == P.MSG_BURST_FIRED:
                assert msg["c"] != 0xAB, "0xAB burst should have been discarded by reset"
        except TimeoutError:
            pass  # no burst — reset worked
        rc.close()
