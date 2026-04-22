"""
radar_core/protocol.py — Python side of the radar-core IPC protocol.

Wire format: 4-byte big-endian uint32 length prefix, followed by msgpack payload.
All message types mirror the Go protocol/messages.go definitions exactly.
Field names use short keys (matching the codec tags) to minimise wire size.
"""

from __future__ import annotations

import struct
from dataclasses import dataclass, field
from typing import Optional

import msgpack

# ---------------------------------------------------------------------------
# Message type constants
# ---------------------------------------------------------------------------

# Inbound (Python → radar-core)
MSG_RADAR_EVENT      = 1
MSG_POSITION_UPDATE  = 2
MSG_CONFIG_UPDATE    = 3
MSG_SNAPSHOT_REQ     = 4
MSG_RESET_IID        = 5
# 6 reserved — SHUTDOWN not used; radar-core is stopped via SIGTERM

# Outbound (radar-core → Python)
MSG_BURST_FIRED   = 10
MSG_FRAME_READY   = 11
MSG_IID_STATE     = 12
MSG_SNAPSHOT_RESP = 13
MSG_HEALTH        = 14
MSG_FM_FRAME_RESULT  = 15
MSG_FM_STATE         = 16
MSG_MULTI_SYNC_STATE = 17  # per-IID multi-aircraft sync refinement state

# ---------------------------------------------------------------------------
# Framing
# ---------------------------------------------------------------------------

_MAX_FRAME_BYTES = 1 << 20  # 1 MiB


class FrameTooLargeError(Exception):
    pass


def write_frame(sock_or_file, payload: bytes) -> None:
    """Write one length-prefixed frame."""
    if len(payload) > _MAX_FRAME_BYTES:
        raise FrameTooLargeError(f"payload {len(payload)} exceeds max {_MAX_FRAME_BYTES}")
    sock_or_file.write(struct.pack(">I", len(payload)))
    sock_or_file.write(payload)


def read_frame(sock_or_file) -> bytes:
    """Read one length-prefixed frame.  Blocks until a full frame is available."""
    header = _read_exactly(sock_or_file, 4)
    (n,) = struct.unpack(">I", header)
    if n > _MAX_FRAME_BYTES:
        raise FrameTooLargeError(f"frame length {n} exceeds max {_MAX_FRAME_BYTES}")
    return _read_exactly(sock_or_file, n)


def _read_exactly(f, n: int) -> bytes:
    buf = bytearray()
    while len(buf) < n:
        chunk = f.read(n - len(buf))
        if not chunk:
            raise EOFError("connection closed mid-frame")
        buf.extend(chunk)
    return bytes(buf)


# ---------------------------------------------------------------------------
# Codec
# ---------------------------------------------------------------------------

def encode(msg: dict) -> bytes:
    """Serialise a message dict to msgpack bytes."""
    return msgpack.packb(msg, use_bin_type=True)


def decode(payload: bytes) -> dict:
    """Deserialise a msgpack payload to a plain dict."""
    return msgpack.unpackb(payload, raw=False)


# ---------------------------------------------------------------------------
# Message constructors — inbound (Python → radar-core)
# ---------------------------------------------------------------------------

def radar_event(
    arrival_us: float,
    iid: int,
    icao: int,
    signal_dbfs: Optional[float] = None,
) -> bytes:
    return encode({
        "t": MSG_RADAR_EVENT,
        "a": arrival_us,
        "i": iid,
        "c": icao,
        "s": signal_dbfs,
    })


def position_update(
    icao: int,
    lat: float,
    lon: float,
    alt_ft: Optional[int],
    ts: float,
) -> bytes:
    return encode({
        "t": MSG_POSITION_UPDATE,
        "c": icao,
        "la": lat,
        "lo": lon,
        "al": alt_ft,
        "ts": ts,
    })


def config_update(key: str, value) -> bytes:
    return encode({"t": MSG_CONFIG_UPDATE, "k": key, "v": value})


def snapshot_req(req_id: int, scope: str) -> bytes:
    return encode({"t": MSG_SNAPSHOT_REQ, "r": req_id, "sc": scope})


def reset_iid(iid: int) -> bytes:
    return encode({"t": MSG_RESET_IID, "i": iid})


# ---------------------------------------------------------------------------
# Message decoders — outbound (radar-core → Python)
# ---------------------------------------------------------------------------

def decode_burst_fired(d: dict) -> dict:
    """Return the dict as-is; field access uses short keys."""
    return d


def decode_frame_ready(d: dict) -> dict:
    return d


def decode_iid_state(d: dict) -> dict:
    return d


def decode_health(d: dict) -> dict:
    return d


def decode_fm_frame_result(d: dict) -> dict:
    return d


def decode_fm_state(d: dict) -> dict:
    return d


def dispatch(payload: bytes) -> dict:
    """Decode a raw payload and return as a dict.  Raises KeyError on unknown type."""
    d = decode(payload)
    msg_type = d["t"]
    if msg_type not in (
        MSG_BURST_FIRED, MSG_FRAME_READY, MSG_IID_STATE,
        MSG_SNAPSHOT_RESP, MSG_HEALTH, MSG_FM_FRAME_RESULT, MSG_FM_STATE,
        MSG_MULTI_SYNC_STATE,
        MSG_RADAR_EVENT, MSG_POSITION_UPDATE, MSG_CONFIG_UPDATE,
        MSG_SNAPSHOT_REQ, MSG_RESET_IID,
    ):
        raise ValueError(f"unknown message type {msg_type}")
    return d
