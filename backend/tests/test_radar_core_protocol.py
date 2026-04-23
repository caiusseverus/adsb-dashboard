"""
Round-trip tests for the radar_core protocol layer (Python side).

These tests verify that:
  1. Every message type encodes and decodes without error.
  2. All field values survive the encode→decode cycle exactly.
  3. Optional (None) fields round-trip as None.
  4. Framing (write_frame / read_frame) correctly length-prefixes payloads.
  5. Oversized frames are rejected.

The tests are deliberately independent of the network layer — they work
against in-memory buffers so they can run without a socket or radar-core process.
"""

import io
import struct

import pytest

from radar_core import protocol as P


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _frame_rt(payload: bytes) -> bytes:
    buf = io.BytesIO()
    P.write_frame(buf, payload)
    buf.seek(0)
    return P.read_frame(buf)


def _encode_decode(msg_bytes: bytes) -> dict:
    return P.dispatch(msg_bytes)


# ---------------------------------------------------------------------------
# Framing
# ---------------------------------------------------------------------------

class TestFraming:
    def test_empty_payload(self):
        assert _frame_rt(b"") == b""

    def test_single_byte(self):
        assert _frame_rt(b"\x42") == b"\x42"

    def test_1kb_payload(self):
        data = bytes(range(256)) * 4
        assert _frame_rt(data) == data

    def test_65537_bytes(self):
        data = b"\xAB" * 65537
        assert _frame_rt(data) == data

    def test_too_large_write(self):
        with pytest.raises(P.FrameTooLargeError):
            P.write_frame(io.BytesIO(), b"\x00" * (P._MAX_FRAME_BYTES + 1))

    def test_too_large_read(self):
        # Craft a header claiming 2 MiB
        buf = io.BytesIO(struct.pack(">I", 2 * 1024 * 1024))
        with pytest.raises(P.FrameTooLargeError):
            P.read_frame(buf)

    def test_truncated_read(self):
        buf = io.BytesIO(struct.pack(">I", 100))  # claims 100 bytes, has none
        with pytest.raises(EOFError):
            P.read_frame(buf)


# ---------------------------------------------------------------------------
# Inbound message constructors
# ---------------------------------------------------------------------------

class TestRadarEvent:
    def test_with_signal(self):
        d = _encode_decode(P.radar_event(123456789.5, 7, 0xABCDEF, -45.5))
        assert d["t"] == P.MSG_RADAR_EVENT
        assert d["a"] == pytest.approx(123456789.5)
        assert d["i"] == 7
        assert d["c"] == 0xABCDEF
        assert d["s"] == pytest.approx(-45.5, rel=1e-5)

    def test_nil_signal(self):
        d = _encode_decode(P.radar_event(1.0, 0, 0x123456))
        assert d["s"] is None

    def test_icao_zero(self):
        d = _encode_decode(P.radar_event(0.0, 0, 0))
        assert d["c"] == 0

    def test_large_arrival_us(self):
        # Beast timestamps unwrap to ~72-hour monotonic values in µs
        large_ts = 259_200_000_000.0  # 72 h in µs
        d = _encode_decode(P.radar_event(large_ts, 1, 0xFFFFFF))
        assert d["a"] == pytest.approx(large_ts)


class TestPositionUpdate:
    def test_full(self):
        d = _encode_decode(P.position_update(0x400F3C, 51.4775, -0.4614, 35000, 1714000000.123))
        assert d["t"] == P.MSG_POSITION_UPDATE
        assert d["la"] == pytest.approx(51.4775)
        assert d["lo"] == pytest.approx(-0.4614)
        assert d["al"] == 35000
        assert d["ts"] == pytest.approx(1714000000.123)

    def test_nil_altitude(self):
        d = _encode_decode(P.position_update(0x111111, 0.0, 0.0, None, 0.0))
        assert d["al"] is None


class TestConfigUpdate:
    def test_float_value(self):
        d = _encode_decode(P.config_update("BURST_RECORD_MAX_AGE_S", 120.0))
        assert d["k"] == "BURST_RECORD_MAX_AGE_S"
        assert d["v"] == pytest.approx(120.0)

    def test_int_value(self):
        d = _encode_decode(P.config_update("MIN_QUALIFYING_ICAOS", 4))
        assert d["v"] == 4

    def test_bool_value(self):
        d = _encode_decode(P.config_update("RADAR_DIAGNOSTICS", True))
        assert d["v"] is True


class TestSnapshotReq:
    def test_all_scope(self):
        d = _encode_decode(P.snapshot_req(1, "all"))
        assert d["t"] == P.MSG_SNAPSHOT_REQ
        assert d["r"] == 1
        assert d["sc"] == "all"

    def test_iid_scope(self):
        d = _encode_decode(P.snapshot_req(99, "iid:3"))
        assert d["sc"] == "iid:3"


class TestResetIID:
    def test_reset(self):
        d = _encode_decode(P.reset_iid(4))
        assert d["t"] == P.MSG_RESET_IID
        assert d["i"] == 4


# ---------------------------------------------------------------------------
# Outbound message decode (radar-core → Python)
# These are encoded by hand to simulate what Go would send.
# ---------------------------------------------------------------------------

def _make_outbound(fields: dict) -> dict:
    """Encode a dict as msgpack and decode via dispatch()."""
    import msgpack
    return P.dispatch(msgpack.packb(fields, use_bin_type=True))


class TestBurstFiredDecode:
    def test_full(self):
        d = _make_outbound({
            "t": P.MSG_BURST_FIRED,
            "i": 2, "c": 0x3C4B4A,
            "cu": 9876543210.75, "n": 5,
            "s": -33.0,
            "la": 51.5, "lo": -0.1,
            "br": 275.3, "rn": 42.1, "pa": 1.2,
            "df": True, "se": True, "ce": True, "rp": True, "ru": False,
        })
        assert d["t"] == P.MSG_BURST_FIRED
        assert d["cu"] == pytest.approx(9876543210.75)
        assert d["df"] is True
        assert d["se"] is True
        assert d["ce"] is True
        assert d["rp"] is True
        assert d["ru"] is False

    def test_no_position(self):
        d = _make_outbound({
            "t": P.MSG_BURST_FIRED,
            "i": 1, "c": 0x111111,
            "cu": 1000.0, "n": 2,
            "s": None, "la": None, "lo": None,
            "br": None, "rn": None, "pa": None,
            "df": False, "se": False,
        })
        assert d["la"] is None


class TestFrameReadyDecode:
    def test_full(self):
        d = _make_outbound({
            "t": P.MSG_FRAME_READY,
            "i": 4, "fi": 100, "p": 4.008,
            "rc": 0xAA1122, "rla": 52.1, "rlo": 0.3,
            "ra": 9999999.0, "q": "good",
            "obs": [
                {"c": 0xBB2233, "la": 51.9, "lo": 0.1, "a": 10000020.5, "n": 3, "pa": 0.5},
                {"c": 0xCC3344, "la": 52.3, "lo": 0.5, "a": 10000120.0, "n": 4, "pa": 1.1},
            ],
        })
        assert d["t"] == P.MSG_FRAME_READY
        assert d["q"] == "good"
        assert len(d["obs"]) == 2
        assert d["obs"][0]["c"] == 0xBB2233


class TestIIDStateDecode:
    def test_full(self):
        d = _make_outbound({
            "t": P.MSG_IID_STATE,
            "i": 3, "p": 4.008, "rpm": 14.97,
            "st": "SINGLE_RADAR", "rc": 0xDEAD01,
            "sq": 0.95, "nb": 212,
            "lu": 1714000100.0, "rv": 7,
        })
        assert d["st"] == "SINGLE_RADAR"
        assert d["rv"] == 7
        assert d["p"] == pytest.approx(4.008)

    def test_nil_fields(self):
        d = _make_outbound({
            "t": P.MSG_IID_STATE,
            "i": 5, "st": "INSUFFICIENT_DATA",
            "p": None, "rpm": None, "rc": None,
            "sq": 0.0, "nb": 0, "lu": 0.0, "rv": 0,
        })
        assert d["p"] is None
        assert d["rc"] is None


class TestHealthDecode:
    def test_full(self):
        d = _make_outbound({
            "t": P.MSG_HEALTH,
            "up": 3600.5,
            "ei": 1_234_567, "bf": 89_000,
            "fe": 4_200, "qd": 12,
            "dc": 0, "ai": 3,
        })
        assert d["t"] == P.MSG_HEALTH
        assert d["ei"] == 1_234_567
        assert d["ai"] == 3


# ---------------------------------------------------------------------------
# Unknown message type
# ---------------------------------------------------------------------------

class TestDispatchUnknown:
    def test_unknown_type(self):
        import msgpack
        payload = msgpack.packb({"t": 99, "x": "garbage"}, use_bin_type=True)
        with pytest.raises(ValueError, match="unknown message type 99"):
            P.dispatch(payload)
