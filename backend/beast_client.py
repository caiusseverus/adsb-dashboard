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
from typing import Callable, Optional, Tuple, Union

log = logging.getLogger(__name__)

_MSG_LEN = {0x31: 2, 0x32: 7, 0x33: 14}

class BeastClient:
    def __init__(self, host: str, port: int, on_message: Callable[[dict], None]):
        self._host = host
        self._port = port
        self._on_message = on_message
        self._buf = bytearray()
        self._running = False

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
                self._buf.extend(chunk)
                if len(self._buf) > 65536:
                    # Buffer overflow — likely connected to a non-Beast endpoint or
                    # a pathological stream. Reconnect rather than exhaust memory.
                    raise ConnectionError("Beast buffer exceeded 64 KB — reconnecting")
                self._parse_frames()
        finally:
            writer.close()
            try:
                await writer.wait_closed()
            except Exception:
                pass

    def _parse_frames(self) -> None:
        buf = self._buf
        while len(buf) >= 2:
            # Synchronize on 0x1a
            if buf[0] != 0x1A:
                idx = buf.find(0x1A)
                if idx == -1:
                    buf.clear()
                    return
                del buf[:idx]
                continue

            msg_type = buf[1]
            if msg_type not in _MSG_LEN:
                # If we see 0x1a 0x1a, it's an escaped byte in a lost frame; skip 1 byte
                del buf[:1]
                continue

            needed = 6 + 1 + _MSG_LEN[msg_type]
            data, end = self._unescape(buf, 2, needed)

            if data is None:
                # Incomplete data: wait for more bytes
                break
            
            if data is False:
                # Framing error: found unescaped 0x1a. Discard current sync byte.
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
        msg_hex = data[7:].hex().upper()
        try:
            self._on_message({
                "raw": msg_hex,
                "timestamp": timestamp,
                "signal": signal,
                "type": msg_type,
            })
        except Exception as exc:
            log.error("Error in Beast message handler: %s", exc)
