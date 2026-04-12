import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(__file__)))

import decode_cffi


def test_decode_batch_matches_single_message_decode():
    messages = [
        (bytes.fromhex("8D400F0620226469C5A3B1"), 0x3A, 0x000000000001),
        (bytes.fromhex("5DAB1213B2B6C2"), 0x15, 0x00000000000C),
        (b"\x00" * 7, 0x01, 0x0000000000FF),
    ]

    expected = [
        decode_cffi.decode_message(raw, signal=signal, timestamp=timestamp)
        for raw, signal, timestamp in messages
    ]

    batcher = decode_cffi.DecodeBatcher(max_messages=1)
    assert batcher.decode_batch(messages) == expected


def test_decode_batch_fallback_matches_single_message_decode():
    messages = [
        (bytes.fromhex("8D400F0620226469C5A3B1"), 0x3A, 0x000000000001),
        (b"\x00" * 14, 0x01, 0x0000000000FF),
    ]

    batcher = decode_cffi.DecodeBatcher()
    batcher._has_native_batch = False

    assert batcher.decode_batch(messages) == [
        decode_cffi.decode_message(raw, signal=signal, timestamp=timestamp)
        for raw, signal, timestamp in messages
    ]
