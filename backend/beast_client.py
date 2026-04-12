"""
Beast TCP client.

Beast binary format:
  0x1a          – frame sync (never escaped)
  <type>        – 0x31 (Mode-AC, 2-byte msg)
                  0x32 (Mode-S short, 7-byte msg)
                  0x33 (Mode-S long, 14-byte msg)
  <6 bytes>     – 12 MHz timestamp (big-endian)
  <1 byte>      – signal level (RSSI)
  <N bytes>     – message payload

Any 0x1a byte *inside* the data (timestamp/signal/message) is escaped as
0x1a 0x1a.  The leading sync 0x1a is never escaped.
"""

import asyncio
import logging
import time
from collections import deque
from typing import Callable, Optional, Tuple, Union

try:
    import decode_cffi as _decode_cffi
except Exception:  # pragma: no cover - native parser is optional at runtime
    _decode_cffi = None

log = logging.getLogger(__name__)

_MSG_LEN = {0x31: 2, 0x32: 7, 0x33: 14}

# Cumulative count of non-0x1A bytes discarded while re-synchronising the frame
# stream (readsb: stats_current.remote_malformed_beast).  Exposed via /api/debug/perf.
# All BeastClient instances (main + MLAT) aggregate into this single counter.
malformed_bytes: int = 0
chunk_timings: deque[dict[str, float | int | str]] = deque(maxlen=400)


def _record_chunk_timing(sample: dict[str, float | int | str]) -> None:
    chunk_timings.append(sample)

class BeastClient:
    def __init__(self, host: str, port: int, on_message: Callable[[dict], None]):
        self._host = host
        self._port = port
        self._on_message = on_message
        self._buf = bytearray()
        self._running = False
        self._native_parser = None
        if _decode_cffi is not None:
            try:
                if _decode_cffi.has_beast_parser():
                    self._native_parser = _decode_cffi.BeastParser()
            except Exception:
                log.debug("BeastClient: native parser unavailable", exc_info=True)

    async def run(self) -> None:
        self._running = True
        while self._running:
            try:
                await self._connect_and_read()
            except asyncio.TimeoutError:
                log.warning("Beast stream timed out (no data for 30s) – retrying")
            except (ConnectionRefusedError, OSError) as exc:
                log.warning("Beast connection failed (%s:%s): %s – retrying in 5 s", 
                            self._host, self._port, exc)
                await asyncio.sleep(5)
            except Exception as exc:
                log.error("Unexpected error in Beast client: %s – retrying in 5 s", exc)
                await asyncio.sleep(5)

    async def _connect_and_read(self) -> None:
        log.info("Connecting to Beast stream at %s:%s", self._host, self._port)
        reader, writer = await asyncio.open_connection(self._host, self._port)
        log.info("Beast stream connected")
        self._buf.clear()
        try:
            while self._running:
                # wait_for raises TimeoutError, which is caught in run()
                chunk = await asyncio.wait_for(reader.read(4096), timeout=30)
                if not chunk:
                    raise ConnectionError("Remote closed the connection")
                t_parse_start = time.perf_counter()
                t_parse_cpu_start = time.thread_time()
                if self._native_parser is not None:
                    frames_dispatched = self._parse_frames_native(chunk)
                    parser_mode = "native"
                else:
                    self._buf.extend(chunk)
                    if len(self._buf) > 65536:
                        # Buffer overflow — likely connected to a non-Beast endpoint or
                        # a pathological stream. Reconnect rather than exhaust memory.
                        raise ConnectionError("Beast buffer exceeded 64 KB — reconnecting")
                    frames_dispatched = self._parse_frames()
                    parser_mode = "python"
                parse_cpu_s = time.thread_time() - t_parse_cpu_start
                parse_wall_s = time.perf_counter() - t_parse_start
                _record_chunk_timing({
                    "ts_s": time.time(),
                    "chunk_bytes": len(chunk),
                    "frames": frames_dispatched,
                    "parse_wall_ms": round(parse_wall_s * 1000, 2),
                    "parse_cpu_ms": round(parse_cpu_s * 1000, 2),
                    "parse_offcpu_ms": round(max(0.0, parse_wall_s - parse_cpu_s) * 1000, 2),
                    "parser_mode": parser_mode,
                })
        finally:
            writer.close()
            try:
                await writer.wait_closed()
            except Exception:
                pass

    def _parse_frames_native(self, chunk: bytes) -> int:
        global malformed_bytes
        frames, malformed = self._native_parser.parse_chunk(chunk)
        malformed_bytes += malformed
        dispatched = 0
        for frame in frames:
            if frame["type"] == 0x31:
                continue
            self._dispatch_frame(frame)
            dispatched += 1
        return dispatched

    def _parse_frames(self) -> int:
        global malformed_bytes
        buf = self._buf
        dispatched = 0
        while len(buf) >= 2:
            # Synchronize on 0x1a
            if buf[0] != 0x1A:
                idx = buf.find(0x1A)
                if idx == -1:
                    malformed_bytes += len(buf)
                    buf.clear()
                    return dispatched
                malformed_bytes += idx
                del buf[:idx]
                continue

            msg_type = buf[1]
            if msg_type not in _MSG_LEN:
                # If we see 0x1a 0x1a, it's an escaped byte in a lost frame; skip 1 byte
                malformed_bytes += 1
                del buf[:1]
                continue

            needed = 6 + 1 + _MSG_LEN[msg_type]
            data, end = self._unescape(buf, 2, needed)

            if data is None:
                # Incomplete data: wait for more bytes
                break
            
            if data is False:
                # Framing error: found unescaped 0x1a. Discard current sync byte.
                malformed_bytes += 1
                del buf[:1]
                continue

            # Success
            del buf[:end]
            # Mode-AC frames (0x31) carry no ICAO address and are discarded by
            # process_message anyway — skip dispatch to avoid the dict allocation
            # and queue overhead (~10-20% of frames at a typical receiver).
            if msg_type == 0x31:
                continue
            self._dispatch(msg_type, data)
            dispatched += 1
        return dispatched

    def _unescape(self, buf: bytearray, start: int, needed: int) -> Tuple[Union[bytes, bool, None], Optional[int]]:
        """
        Returns:
            (bytes, end_pos) on success
            (None, None) if more data is required
            (False, error_pos) if a framing error (unescaped 0x1a) is encountered
        """
        result = bytearray()
        pos = start
        while len(result) < needed:
            if pos >= len(buf):
                return None, None
            
            b = buf[pos]
            if b == 0x1A:
                if pos + 1 >= len(buf):
                    return None, None
                
                if buf[pos + 1] == 0x1A:
                    result.append(0x1A)
                    pos += 2
                else:
                    # Unescaped sync byte within the frame body
                    return False, pos
            else:
                result.append(b)
                pos += 1
                
        return bytes(result), pos

    def _dispatch(self, msg_type: int, data: bytes) -> None:
        timestamp = int.from_bytes(data[:6], "big")
        signal = data[6]
        self._dispatch_frame({
            "raw": data[7:],
            "timestamp": timestamp,
            "signal": signal,
            "type": msg_type,
        })

    def _dispatch_frame(self, frame: dict) -> None:
        try:
            self._on_message(frame)
        except Exception as exc:
            log.error("Error in Beast message handler: %s", exc)
