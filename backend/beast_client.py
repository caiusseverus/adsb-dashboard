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
import socket
from typing import Callable, Optional, Tuple, Union

log = logging.getLogger(__name__)

# Beast binary format length mapping (message payload only)
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
        try:
            while self._running:
                try:
                    await self._connect_and_read()
                except (ConnectionRefusedError, OSError, ConnectionError) as exc:
                    log.warning("Beast connection error (%s:%s): %s – retrying in 5 s", 
                                self._host, self._port, exc)
                    await asyncio.sleep(5)
                except Exception as exc:
                    log.error("Unexpected error in Beast client: %s – retrying in 5 s", exc)
                    await asyncio.sleep(5)
        except asyncio.CancelledError:
            # Re-raise to allow the event loop to finalize the task cleanup
            log.info("Beast client task cancelled; exiting")
            raise
        finally:
            self._running = False

    async def _connect_and_read(self) -> None:
        log.info("Connecting to Beast stream at %s:%s", self._host, self._port)
        reader, writer = await asyncio.open_connection(self._host, self._port)

        # Configure Linux TCP Keep-Alives to detect silent connection drops
        sock = writer.get_extra_info('socket')
        if sock:
            sock.setsockopt(socket.SOL_SOCKET, socket.SO_KEEPALIVE, 1)
            # Send first probe after 60s of idleness
            sock.setsockopt(socket.IPPROTO_TCP, socket.TCP_KEEPIDLE, 60)
            # Interval between probes: 10s
            sock.setsockopt(socket.IPPROTO_TCP, socket.TCP_KEEPINTVL, 10)
            # Close connection after 6 failed probes (~120s total timeout)
            sock.setsockopt(socket.IPPROTO_TCP, socket.TCP_KEEPCNT, 6)

        log.info("Beast stream connected (TCP Keep-Alives enabled)")
        self._buf.clear()
        try:
            while self._running:
                # read() blocks until data arrives or TCP stack signals connection loss
                chunk = await reader.read(4096)
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
                # Corrupt header or mid-stream sync; discard sync byte and resync
                del buf[:1]
                continue

            # Header is 2 bytes (0x1a + type).
            # Needed payload is 6 (timestamp) + 1 (RSSI) + N (message payload)
            needed_payload = 6 + 1 + _MSG_LEN[msg_type]
            data, end_pos = self._unescape(buf, 2, needed_payload)

            if data is None:
                # Incomplete frame data; wait for next TCP chunk
                break

            if data is False:
                # Framing error (unescaped 0x1a in payload); discard header and resync
                del buf[:1]
                continue

            # Valid frame extracted
            del buf[:end_pos]
            self._dispatch(msg_type, data)

    def _unescape(self, buf: bytearray, start: int, needed: int) -> Tuple[Union[bytes, bool, None], Optional[int]]:
        """
        Extracts 'needed' unescaped bytes from 'buf' starting at 'start'.
        Returns:
            (bytes, end_pos) on success.
            (None, None) if more data is required.
            (False, error_pos) if an unescaped 0x1a framing error is detected.
        """
        result = bytearray()
        pos = start
        while len(result) < needed:
            if pos >= len(buf):
                return None, None

            b = buf[pos]
            if b == 0x1A:
                # Ensure lookahead byte is available
                if pos + 1 >= len(buf):
                    return None, None

                if buf[pos + 1] == 0x1A:
                    # Valid escaped 0x1a
                    result.append(0x1A)
                    pos += 2
                else:
                    # Unescaped 0x1a inside data is a protocol violation
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
