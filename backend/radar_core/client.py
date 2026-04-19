"""
radar_core/client.py — Shadow-mode client for radar-core (Stage 1).

Connects to the radar-core Unix socket and runs two tasks:
  - Sink: accepts (arrival_us, iid, icao_int, signal_dbfs) tuples from the
    existing Python radar path and sends them as RADAR_EVENT messages.
  - Receiver: reads BURST_FIRED (and other outbound) messages from radar-core
    and logs them for comparison. In shadow mode the output does NOT feed back
    into RadarState, the FM worker, or any live state.

Usage (from main.py or tests):

    from radar_core.client import RadarCoreClient
    client = RadarCoreClient("/tmp/radar-core.sock")
    client.start()
    # ... in the radar event path:
    client.send_radar_event(arrival_us, iid, icao_int, signal_dbfs)
    # ... on shutdown:
    client.stop()
"""

from __future__ import annotations

import logging
import queue
import socket
import threading
import time
from typing import Optional, Callable

from radar_core import protocol as P

log = logging.getLogger(__name__)

# Maximum events queued before dropping (prevents the shadow tap from
# affecting the Python radar path under backpressure).
_SEND_QUEUE_MAX = 2000
# Maximum BURST_FIRED messages to buffer for comparison logging.
_RECV_LOG_MAX = 10_000


class RadarCoreClient:
    """Shadow-mode IPC client for radar-core.

    Thread-safe. All I/O happens in two daemon threads (sender, receiver).
    The Python radar path calls send_radar_event() without blocking — events
    are queued and sent asynchronously.

    on_burst_fired: optional callback(dict) called for each received BURST_FIRED.
    """

    def __init__(
        self,
        socket_path: str,
        on_burst_fired: Optional[Callable[[dict], None]] = None,
        connect_timeout_s: float = 5.0,
        reconnect_delay_s: float = 2.0,
    ):
        self._socket_path = socket_path
        self._on_burst_fired = on_burst_fired
        self._connect_timeout_s = connect_timeout_s
        self._reconnect_delay_s = reconnect_delay_s

        self._send_queue: queue.Queue = queue.Queue(maxsize=_SEND_QUEUE_MAX)
        self._stop_event = threading.Event()
        self._connected = threading.Event()

        self._sender_thread: Optional[threading.Thread] = None
        self._receiver_thread: Optional[threading.Thread] = None

        # Stats (written from sender/receiver threads, read from any thread).
        self._events_sent = 0
        self._events_dropped = 0
        self._bursts_received = 0
        self._connect_attempts = 0
        self._stats_lock = threading.Lock()

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def start(self) -> None:
        """Start sender and receiver daemon threads."""
        self._sender_thread = threading.Thread(
            target=self._sender_loop, daemon=True, name="rc-sender"
        )
        self._receiver_thread = threading.Thread(
            target=self._receiver_loop, daemon=True, name="rc-receiver"
        )
        self._sender_thread.start()
        self._receiver_thread.start()
        log.info("RadarCoreClient: started (socket=%s)", self._socket_path)

    def stop(self) -> None:
        """Signal threads to stop and wait up to 2s for them to exit."""
        self._stop_event.set()
        if self._sender_thread:
            self._sender_thread.join(timeout=2.0)
        if self._receiver_thread:
            self._receiver_thread.join(timeout=2.0)

    def send_radar_event(
        self,
        arrival_us: float,
        iid: int,
        icao: int,
        signal_dbfs: Optional[float],
    ) -> None:
        """Queue one RADAR_EVENT for sending. Non-blocking; drops if queue is full."""
        try:
            self._send_queue.put_nowait(("radar_event", arrival_us, iid, icao, signal_dbfs))
        except queue.Full:
            with self._stats_lock:
                self._events_dropped += 1

    def send_position_update(
        self,
        icao: int,
        lat: float,
        lon: float,
        alt_ft: Optional[int],
        ts: float,
    ) -> None:
        """Queue one POSITION_UPDATE for sending. Non-blocking; drops if queue is full."""
        try:
            self._send_queue.put_nowait(("position_update", icao, lat, lon, alt_ft, ts))
        except queue.Full:
            with self._stats_lock:
                self._events_dropped += 1

    def is_connected(self) -> bool:
        return self._connected.is_set()

    def stats(self) -> dict:
        with self._stats_lock:
            return {
                "events_sent": self._events_sent,
                "events_dropped": self._events_dropped,
                "bursts_received": self._bursts_received,
                "connect_attempts": self._connect_attempts,
                "connected": self._connected.is_set(),
            }

    # ------------------------------------------------------------------
    # Internal threads
    # ------------------------------------------------------------------

    def _connect(self) -> Optional[socket.socket]:
        """Try to connect to the Unix socket. Returns the socket or None."""
        with self._stats_lock:
            self._connect_attempts += 1
        try:
            sock = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
            sock.settimeout(self._connect_timeout_s)
            sock.connect(self._socket_path)
            sock.settimeout(None)  # blocking I/O after connect
            log.info("RadarCoreClient: connected to %s", self._socket_path)
            return sock
        except OSError as e:
            log.debug("RadarCoreClient: connect failed: %s", e)
            return None

    def _sender_loop(self) -> None:
        """Drain the send queue and write framed msgpack to the socket."""
        sock: Optional[socket.socket] = None
        sockfile = None

        while not self._stop_event.is_set():
            if sock is None:
                sock = self._connect()
                if sock is None:
                    time.sleep(self._reconnect_delay_s)
                    continue
                sockfile = sock.makefile("wb", buffering=0)
                self._connected.set()

            try:
                item = self._send_queue.get(timeout=0.5)
            except queue.Empty:
                continue

            try:
                payload = self._encode_item(item)
                if payload:
                    P.write_frame(sockfile, payload)
                    with self._stats_lock:
                        self._events_sent += 1
            except OSError as e:
                log.warning("RadarCoreClient: send error: %s", e)
                sock.close()
                sock = None
                sockfile = None
                self._connected.clear()

        if sock:
            sock.close()

    def _receiver_loop(self) -> None:
        """Read BURST_FIRED and other outbound messages from radar-core."""
        sock: Optional[socket.socket] = None

        while not self._stop_event.is_set():
            # Wait until the sender has a connection.
            if not self._connected.wait(timeout=1.0):
                continue
            if sock is None:
                try:
                    sock = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
                    sock.settimeout(self._connect_timeout_s)
                    sock.connect(self._socket_path)
                    sock.settimeout(None)
                    log.debug("RadarCoreClient: receiver connected")
                except OSError as e:
                    log.debug("RadarCoreClient: receiver connect failed: %s", e)
                    sock = None
                    time.sleep(self._reconnect_delay_s)
                    continue

            try:
                sockfile = sock.makefile("rb", buffering=0)
                while not self._stop_event.is_set():
                    payload = P.read_frame(sockfile)
                    d = P.dispatch(payload)
                    self._handle_outbound(d)
            except (OSError, EOFError) as e:
                log.debug("RadarCoreClient: receiver disconnected: %s", e)
                sock.close()
                sock = None

        if sock:
            sock.close()

    def _handle_outbound(self, d: dict) -> None:
        msg_type = d.get("t")
        if msg_type == P.MSG_BURST_FIRED:
            with self._stats_lock:
                self._bursts_received += 1
            if self._on_burst_fired:
                self._on_burst_fired(d)
        elif msg_type == P.MSG_HEALTH:
            log.debug(
                "RadarCoreClient: health uptime=%.0fs events_in=%d bursts_fired=%d",
                d.get("up", 0), d.get("ei", 0), d.get("bf", 0),
            )
        elif msg_type == P.MSG_IID_STATE:
            # Shadow-mode comparison logging for Stage 2 parity verification.
            log.debug(
                "RadarCoreClient: IID_STATE iid=%d status=%s period_s=%s rpm=%s "
                "n_burst_records=%d revision=%d",
                d.get("i", "?"), d.get("st", "?"),
                f"{d['p']:.4f}" if d.get("p") is not None else "None",
                f"{d['rpm']:.2f}" if d.get("rpm") is not None else "None",
                d.get("nb", 0), d.get("rv", 0),
            )
        # Other types silently ignored in shadow mode.

    @staticmethod
    def _encode_item(item: tuple) -> Optional[bytes]:
        kind = item[0]
        if kind == "radar_event":
            _, arrival_us, iid, icao, signal_dbfs = item
            return P.radar_event(arrival_us, iid, icao, signal_dbfs)
        elif kind == "position_update":
            _, icao, lat, lon, alt_ft, ts = item
            return P.position_update(icao, lat, lon, alt_ft, ts)
        return None
