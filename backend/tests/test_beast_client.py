"""
Unit tests for BeastClient._unescape and _parse_frames.

No network connection required — all tests inject raw bytes directly.
"""

import sys
import os
sys.path.insert(0, os.path.dirname(os.path.dirname(__file__)))

import pytest
from beast_client import BeastClient


def make_client(messages: list) -> BeastClient:
    """Create a BeastClient whose on_message appends to `messages`."""
    return BeastClient("localhost", 30005, lambda msg: messages.append(msg))


# ---------------------------------------------------------------------------
# _unescape
# ---------------------------------------------------------------------------

class TestUnescape:
    def setup_method(self):
        self.client = make_client([])

    def test_normal_bytes(self):
        # 7 plain bytes — no escaping needed
        buf = bytearray(b"\x01\x02\x03\x04\x05\x06\x07extra")
        data, end = self.client._unescape(buf, 0, 7)
        assert data == b"\x01\x02\x03\x04\x05\x06\x07"
        assert end == 7

    def test_escaped_0x1a(self):
        # 0x1a 0x1a in the stream represents a single 0x1a byte
        buf = bytearray(b"\x1a\x1a\x02\x03")
        data, end = self.client._unescape(buf, 0, 3)
        assert data == b"\x1a\x02\x03"
        assert end == 4  # consumed 4 raw bytes to produce 3 unescaped

    def test_partial_read_returns_none(self):
        # Buffer ends before enough unescaped bytes are available
        buf = bytearray(b"\x01\x02")
        data, end = self.client._unescape(buf, 0, 5)
        assert data is None
        assert end is None

    def test_partial_escape_pair_returns_none(self):
        # Buffer ends mid-escape sequence (0x1a at end, no second byte yet)
        buf = bytearray(b"\x01\x1a")
        data, end = self.client._unescape(buf, 0, 2)
        assert data is None
        assert end is None

    def test_framing_error_unescaped_sync(self):
        # 0x1a followed by a non-0x1a byte signals a new frame start — framing error
        buf = bytearray(b"\x01\x1a\x33\x99")
        data, end = self.client._unescape(buf, 0, 3)
        assert data is False

    def test_start_offset(self):
        # start parameter is respected
        buf = bytearray(b"\x00\x00\xAA\xBB\xCC")
        data, end = self.client._unescape(buf, 2, 3)
        assert data == b"\xAA\xBB\xCC"
        assert end == 5


# ---------------------------------------------------------------------------
# _parse_frames / _dispatch integration
# ---------------------------------------------------------------------------

def _build_frame(msg_type: int, timestamp: bytes, signal: int, payload: bytes) -> bytearray:
    """Build a raw Beast frame with proper 0x1a escaping in the body."""
    body = timestamp + bytes([signal]) + payload
    escaped = bytearray()
    for b in body:
        escaped.append(b)
        if b == 0x1A:
            escaped.append(0x1A)  # escape
    return bytearray([0x1A, msg_type]) + escaped


class TestParseFrames:
    def setup_method(self):
        self.messages = []
        self.client = make_client(self.messages)

    def _feed(self, data: bytes):
        self.client._buf.extend(data)
        self.client._parse_frames()

    def test_single_short_frame(self):
        # Mode-S short: type 0x32, 7-byte payload
        ts = b"\x00\x01\x02\x03\x04\x05"
        payload = bytes(7)
        frame = _build_frame(0x32, ts, 0xAB, payload)
        self._feed(frame)
        assert len(self.messages) == 1
        assert self.messages[0]["type"] == 0x32
        assert self.messages[0]["signal"] == 0xAB
        assert self.messages[0]["timestamp"] == 0x000102030405

    def test_single_long_frame(self):
        # Mode-S long: type 0x33, 14-byte payload
        ts = b"\x00\x00\x00\x00\x00\x01"
        payload = bytes(14)
        frame = _build_frame(0x33, ts, 0x10, payload)
        self._feed(frame)
        assert len(self.messages) == 1
        assert self.messages[0]["type"] == 0x33

    def test_two_consecutive_frames(self):
        ts = b"\x00\x00\x00\x00\x00\x01"
        f1 = _build_frame(0x32, ts, 0x01, bytes(7))
        f2 = _build_frame(0x33, ts, 0x02, bytes(14))
        self._feed(f1 + f2)
        assert len(self.messages) == 2

    def test_partial_frame_waits_for_more(self):
        ts = b"\x00\x00\x00\x00\x00\x01"
        frame = _build_frame(0x32, ts, 0x01, bytes(7))
        # Feed only the first half
        self._feed(frame[:len(frame) // 2])
        assert len(self.messages) == 0
        # Feed the rest
        self._feed(frame[len(frame) // 2:])
        assert len(self.messages) == 1

    def test_sync_recovery_skips_garbage(self):
        garbage = b"\xDE\xAD\xBE\xEF"
        ts = b"\x00\x00\x00\x00\x00\x01"
        frame = _build_frame(0x32, ts, 0x55, bytes(7))
        self._feed(garbage + frame)
        assert len(self.messages) == 1

    def test_unknown_type_byte_skipped(self):
        # 0x1A followed by an unknown type byte (not 0x31/0x32/0x33) — should skip
        bad = bytearray([0x1A, 0xFF])
        ts = b"\x00\x00\x00\x00\x00\x01"
        good = _build_frame(0x32, ts, 0x01, bytes(7))
        self._feed(bad + good)
        assert len(self.messages) == 1

    def test_escaped_0x1a_in_payload_decoded_correctly(self):
        # Force a 0x1A byte in the payload — it must be escaped in the raw stream
        ts = b"\x00\x00\x00\x00\x00\x01"
        payload = bytes([0x1A] + [0x00] * 6)  # first payload byte is 0x1A
        frame = _build_frame(0x32, ts, 0x01, payload)
        self._feed(frame)
        assert len(self.messages) == 1
        # The decoded raw hex should start with "1A"
        assert self.messages[0]["raw"].startswith("1A")

    def test_mode_ac_frame(self):
        # Mode-AC: type 0x31, 2-byte payload
        ts = b"\x00\x00\x00\x00\x00\x01"
        frame = _build_frame(0x31, ts, 0x20, bytes(2))
        self._feed(frame)
        assert len(self.messages) == 1
        assert self.messages[0]["type"] == 0x31
