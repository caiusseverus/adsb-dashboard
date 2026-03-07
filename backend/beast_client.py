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
from typing import Callable

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
                chunk = await asyncio.wait_for(reader.read(4096), timeout=30)
                if not chunk:
                    raise ConnectionError("Remote closed the connection")
                self._buf.extend(chunk)
                self._parse_frames()
        finally:
            writer.close()
            try:
                await writer.wait_closed()
            except Exception:
                pass

    # ------------------------------------------------------------------
    # Frame parsing
    # ------------------------------------------------------------------

    def _parse_frames(self) -> None:
        buf = self._buf
        while len(buf) >= 2:
            # Synchronise on 0x1a
            if buf[0] != 0x1A:
                idx = buf.find(0x1A)
                if idx == -1:
                    buf.clear()
                    return
                del buf[:idx]
                continue

            msg_type = buf[1]
            if msg_type not in _MSG_LEN:
                # 0x1a 0x1a at the start means we're mid-frame; drop the first byte
                del buf[:1]
                continue

            needed = 6 + 1 + _MSG_LEN[msg_type]  # timestamp + signal + message
            data, end = self._unescape(buf, 2, needed)
            if data is None:
                break  # wait for more data

            del buf[:end]
            self._dispatch(msg_type, data)

    def _unescape(self, buf: bytearray, start: int, needed: int):
        """
        Read `needed` unescaped bytes from `buf` beginning at `start`.

        Returns (bytes, end_pos) on success, or (None, None) if more data
        is required.  An unescaped 0x1a (i.e. the start of the next frame
        appearing before we have enough bytes) is treated as a framing error
        and also returns (None, None) so the outer loop can resync.
        """
        result = bytearray()
        pos = start
        while len(result) < needed:
            if pos >= len(buf):
                return None, None
            b = buf[pos]
            if b == 0x1A:
                if pos + 1 >= len(buf):
                    return None, None  # need to see the next byte first
                if buf[pos + 1] == 0x1A:
                    result.append(0x1A)
                    pos += 2
                else:
                    # Unescaped sync – this frame is corrupt; resync
                    return None, None
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
