"""
radar_core/client.py — IPC client for radar-core.

Connects to the radar-core Unix socket and runs two tasks over one bidirectional
connection:
  - Sink: accepts (arrival_us, iid, icao_int, signal_dbfs) tuples from the
    existing Python radar path and sends them as RADAR_EVENT messages.
  - Receiver: reads BURST_FIRED, FRAME_READY, HEALTH, and SNAPSHOT_RESP.

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
_SNAPSHOT_INTERVAL_S = 5.0


class RadarCoreClient:
    """IPC client for radar-core.

    Thread-safe. All I/O happens in two daemon threads (sender, receiver) that
    share one bidirectional Unix-socket connection.
    The Python radar path calls send_radar_event() without blocking — events
    are queued and sent asynchronously.

    on_burst_fired: optional callback(dict) called for each received BURST_FIRED.
    on_frame_ready: optional callback(dict) called for each received FRAME_READY.
      When RADAR_CORE_FRAMES_ENABLED is True this callback injects the frame
      into RadarState's FM mailbox.
    """

    def __init__(
        self,
        socket_path: str,
        on_burst_fired: Optional[Callable[[dict], None]] = None,
        on_frame_ready: Optional[Callable[[dict], None]] = None,
        connect_timeout_s: float = 5.0,
        reconnect_delay_s: float = 2.0,
    ):
        self._socket_path = socket_path
        self._on_burst_fired = on_burst_fired
        self._on_frame_ready = on_frame_ready
        self._connect_timeout_s = connect_timeout_s
        self._reconnect_delay_s = reconnect_delay_s

        self._send_queue: queue.Queue = queue.Queue(maxsize=_SEND_QUEUE_MAX)
        self._stop_event = threading.Event()
        self._connected = threading.Event()

        self._sender_thread: Optional[threading.Thread] = None
        self._receiver_thread: Optional[threading.Thread] = None
        self._conn_lock = threading.Lock()
        self._send_lock = threading.Lock()
        self._sock: Optional[socket.socket] = None
        self._rfile = None
        self._wfile = None
        self._conn_generation = 0

        # Stats (written from sender/receiver threads, read from any thread).
        self._events_sent = 0
        self._position_updates_sent = 0
        self._events_dropped = 0
        self._bursts_received = 0
        self._frames_received = 0
        self._callback_errors = 0
        self._connect_attempts = 0
        self._latest_health: Optional[dict] = None
        self._latest_health_ts: Optional[float] = None
        self._snapshot_req_id = 0
        self._snapshots_requested = 0
        self._latest_snapshot: Optional[dict] = None
        self._latest_snapshot_ts: Optional[float] = None
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
        log.debug("RadarCoreClient: started (socket=%s)", self._socket_path)

    def stop(self) -> None:
        """Signal threads to stop and wait up to 2s for them to exit."""
        self._stop_event.set()
        self._close_connection()
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

    def wait_until_connected(self, timeout_s: float) -> bool:
        return self._connected.wait(timeout=timeout_s)

    def stats(self) -> dict:
        with self._stats_lock:
            return {
                "events_sent": self._events_sent,
                "position_updates_sent": self._position_updates_sent,
                "events_dropped": self._events_dropped,
                "bursts_received": self._bursts_received,
                "frames_received": self._frames_received,
                "callback_errors": self._callback_errors,
                "connect_attempts": self._connect_attempts,
                "connected": self._connected.is_set(),
                "connection_generation": self._conn_generation,
                "latest_health": self._latest_health,
                "latest_health_age_s": (
                    round(time.time() - self._latest_health_ts, 1)
                    if self._latest_health_ts is not None else None
                ),
                "snapshots_requested": self._snapshots_requested,
                "latest_snapshot": self._latest_snapshot,
                "latest_snapshot_age_s": (
                    round(time.time() - self._latest_snapshot_ts, 1)
                    if self._latest_snapshot_ts is not None else None
                ),
                "send_queue_depth": self._send_queue.qsize(),
            }

    # ------------------------------------------------------------------
    # Internal threads
    # ------------------------------------------------------------------

    def _ensure_connection(self) -> Optional[tuple[int, object, object]]:
        """Return the shared bidirectional connection, connecting if needed."""
        with self._conn_lock:
            if self._sock is not None and self._rfile is not None and self._wfile is not None:
                return self._conn_generation, self._rfile, self._wfile
            if self._stop_event.is_set():
                return None

            connected = self._connect_locked()
            if not connected:
                return None
            return self._conn_generation, self._rfile, self._wfile

    def _connect_locked(self) -> bool:
        """Try to connect to the Unix socket. self._conn_lock must be held."""
        with self._stats_lock:
            self._connect_attempts += 1
        sock = None
        try:
            sock = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
            sock.settimeout(self._connect_timeout_s)
            sock.connect(self._socket_path)
            sock.settimeout(None)  # blocking I/O after connect
            self._sock = sock
            self._rfile = sock.makefile("rb", buffering=0)
            self._wfile = sock.makefile("wb", buffering=0)
            self._conn_generation += 1
            self._connected.set()
            log.debug("RadarCoreClient: connected to %s", self._socket_path)
            return True
        except OSError as e:
            log.debug("RadarCoreClient: connect failed: %s", e)
            if sock is not None:
                try:
                    sock.close()
                except OSError:
                    pass
            return False

    def _drop_connection(self, generation: Optional[int] = None) -> None:
        """Tear down the shared connection if it is still the observed generation."""
        with self._conn_lock:
            if generation is not None and generation != self._conn_generation:
                return
            self._close_connection_locked()

    def _close_connection(self) -> None:
        with self._conn_lock:
            self._close_connection_locked()

    def _close_connection_locked(self) -> None:
        rfile, wfile, sock = self._rfile, self._wfile, self._sock
        self._rfile = None
        self._wfile = None
        self._sock = None
        self._connected.clear()

        for f in (rfile, wfile):
            if f is not None:
                try:
                    f.close()
                except OSError:
                    pass
        if sock is not None:
            try:
                sock.shutdown(socket.SHUT_RDWR)
            except OSError:
                pass
            try:
                sock.close()
            except OSError:
                pass

    def _sender_loop(self) -> None:
        """Drain the send queue and write framed msgpack to the socket."""
        next_snapshot_ts = time.monotonic() + _SNAPSHOT_INTERVAL_S
        while not self._stop_event.is_set():
            conn = self._ensure_connection()
            if conn is None:
                time.sleep(self._reconnect_delay_s)
                continue
            generation, _rfile, wfile = conn

            now_mono = time.monotonic()
            if now_mono >= next_snapshot_ts:
                try:
                    with self._stats_lock:
                        self._snapshot_req_id += 1
                        req_id = self._snapshot_req_id
                    with self._send_lock:
                        P.write_frame(wfile, P.snapshot_req(req_id=req_id, scope="all"))
                    with self._stats_lock:
                        self._snapshots_requested += 1
                    next_snapshot_ts = now_mono + _SNAPSHOT_INTERVAL_S
                except OSError as e:
                    log.debug("RadarCoreClient: snapshot request send error: %s", e)
                    self._drop_connection(generation)
                    next_snapshot_ts = time.monotonic() + _SNAPSHOT_INTERVAL_S
                    continue

            try:
                item = self._send_queue.get(timeout=0.5)
            except queue.Empty:
                continue

            try:
                payload = self._encode_item(item)
                if payload:
                    with self._send_lock:
                        P.write_frame(wfile, payload)
                    with self._stats_lock:
                        if item[0] == "position_update":
                            self._position_updates_sent += 1
                        else:
                            self._events_sent += 1
            except OSError as e:
                log.debug("RadarCoreClient: send error: %s", e)
                self._drop_connection(generation)
                next_snapshot_ts = time.monotonic() + _SNAPSHOT_INTERVAL_S

    def _receiver_loop(self) -> None:
        """Read BURST_FIRED and other outbound messages from radar-core."""
        while not self._stop_event.is_set():
            conn = self._ensure_connection()
            if conn is None:
                time.sleep(self._reconnect_delay_s)
                continue
            generation, rfile, _wfile = conn

            try:
                payload = P.read_frame(rfile)
                d = P.dispatch(payload)
                self._handle_outbound(d)
            except (OSError, EOFError) as e:
                log.debug("RadarCoreClient: receiver disconnected: %s", e)
                self._drop_connection(generation)

    def _handle_outbound(self, d: dict) -> None:
        msg_type = d.get("t")
        if msg_type == P.MSG_BURST_FIRED:
            with self._stats_lock:
                self._bursts_received += 1
            if self._on_burst_fired:
                try:
                    self._on_burst_fired(d)
                except Exception:
                    with self._stats_lock:
                        self._callback_errors += 1
                    log.debug("RadarCoreClient: BURST_FIRED callback failed", exc_info=True)
        elif msg_type == P.MSG_HEALTH:
            with self._stats_lock:
                self._latest_health = dict(d)
                self._latest_health_ts = time.time()
            log.debug(
                "RadarCoreClient: health uptime=%.0fs events_in=%d bursts_fired=%d",
                d.get("up", 0), d.get("ei", 0), d.get("bf", 0),
            )
        elif msg_type == P.MSG_IID_STATE:
            # Shadow-mode comparison logging for Stage 2/3 parity verification.
            log.debug(
                "RadarCoreClient: IID_STATE iid=%d status=%s period_s=%s rpm=%s "
                "sync_quality=%.2f n_burst_records=%d revision=%d",
                d.get("i", "?"), d.get("st", "?"),
                f"{d['p']:.4f}" if d.get("p") is not None else "None",
                f"{d['rpm']:.2f}" if d.get("rpm") is not None else "None",
                d.get("sq", 0.0), d.get("nb", 0), d.get("rv", 0),
            )
        elif msg_type == P.MSG_FRAME_READY:
            with self._stats_lock:
                self._frames_received += 1
            obs = d.get("obs") or []
            log.debug(
                "RadarCoreClient: FRAME_READY iid=%d frame=%d period_s=%.4f "
                "ref=0x%06X n_obs=%d quality=%s",
                d.get("i", "?"), d.get("fi", 0), d.get("p", 0.0),
                d.get("rc", 0), len(obs), d.get("q", "?"),
            )
            if self._on_frame_ready:
                try:
                    self._on_frame_ready(d)
                except Exception:
                    with self._stats_lock:
                        self._callback_errors += 1
                    log.debug("RadarCoreClient: FRAME_READY callback failed", exc_info=True)
        elif msg_type == P.MSG_SNAPSHOT_RESP:
            with self._stats_lock:
                self._latest_snapshot = dict(d.get("pl") or {})
                self._latest_snapshot_ts = time.time()
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
