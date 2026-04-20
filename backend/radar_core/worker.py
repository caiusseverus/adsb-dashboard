from __future__ import annotations

import logging
import os
import socket
import subprocess
import threading
import time
from collections import deque
from pathlib import Path

log = logging.getLogger(__name__)


def _socket_is_listening(socket_path: str, timeout_s: float = 0.25) -> bool:
    client = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
    try:
        client.settimeout(timeout_s)
        client.connect(socket_path)
        return True
    except OSError:
        return False
    finally:
        try:
            client.close()
        except OSError:
            pass


class RadarCoreWorker:
    """Manage the radar-core subprocess lifecycle for backend-owned mode."""

    def __init__(
        self,
        binary_path: str,
        socket_path: str,
        *,
        managed: bool,
        startup_timeout_s: float = 15.0,
        debug_logging: bool = False,
        env_debug_flag: bool = False,
    ) -> None:
        self._binary_path = binary_path
        self._socket_path = socket_path
        self._managed = bool(managed)
        self._startup_timeout_s = float(startup_timeout_s)
        self._debug_logging = bool(debug_logging)
        self._env_debug_flag = bool(env_debug_flag)

        self._process: subprocess.Popen | None = None
        self._started_by_backend = False
        self._state = "idle"
        self._last_error: str | None = None
        self._lock = threading.Lock()

        self._stdout_tail: deque[str] = deque(maxlen=40)
        self._stderr_tail: deque[str] = deque(maxlen=80)
        self._stdout_thread: threading.Thread | None = None
        self._stderr_thread: threading.Thread | None = None
        # Cached socket-ready probe; updated by a background thread so stats()
        # never blocks the asyncio event loop with a connect() call.
        self._socket_ready_cache: bool = False
        self._socket_ready_thread: threading.Thread | None = None
        self._socket_ready_stop = threading.Event()

    def _start_socket_ready_prober(self) -> None:
        """Start a daemon thread that probes socket_ready every 2s into a cache."""
        def _probe_loop() -> None:
            while not self._socket_ready_stop.is_set():
                self._socket_ready_cache = _socket_is_listening(self._socket_path)
                self._socket_ready_stop.wait(timeout=2.0)
        t = threading.Thread(target=_probe_loop, daemon=True, name="rc-socket-probe")
        t.start()
        self._socket_ready_thread = t

    def start(self) -> bool:
        """Start worker in managed mode, or wait for external readiness."""
        with self._lock:
            self._last_error = None
            self._state = "starting"
            self._started_by_backend = False

        if _socket_is_listening(self._socket_path):
            with self._lock:
                self._state = "ready_external_socket"
            self._socket_ready_cache = True
            self._start_socket_ready_prober()
            return True

        if not self._managed:
            ok = self._wait_for_external_socket()
            self._socket_ready_cache = ok
            self._start_socket_ready_prober()
            return ok

        ok = self._start_managed_worker()
        self._socket_ready_cache = ok
        self._start_socket_ready_prober()
        return ok

    def stop(self) -> None:
        """Stop only if backend started the subprocess."""
        self._socket_ready_stop.set()

        with self._lock:
            process = self._process
            started_by_backend = self._started_by_backend

        if process is None or not started_by_backend:
            with self._lock:
                if self._state in {"running", "ready_external_socket"}:
                    self._state = "stopped_external"
            return

        self._terminate_process(process)
        with self._lock:
            self._state = "stopped"
            self._started_by_backend = False
            self._process = None

    def stats(self) -> dict:
        with self._lock:
            process = self._process
            state = self._state
            started_by_backend = self._started_by_backend
            last_error = self._last_error
            stdout_tail = list(self._stdout_tail)
            stderr_tail = list(self._stderr_tail)

        pid = process.pid if process is not None else None
        exit_code = process.poll() if process is not None else None
        socket_exists = os.path.exists(self._socket_path)
        socket_ready = self._socket_ready_cache  # updated by background prober; never blocks

        return {
            "managed": self._managed,
            "backend_managed_autostart": self._managed,
            "started_by_backend": started_by_backend,
            "state": state,
            "binary_path": self._binary_path,
            "socket_path": self._socket_path,
            "socket_exists": socket_exists,
            "socket_ready": socket_ready,
            "pid": pid,
            "exit_code": exit_code,
            "startup_timeout_s": self._startup_timeout_s,
            "last_error": last_error,
            "last_stdout": stdout_tail[-1] if stdout_tail else None,
            "last_stderr": stderr_tail[-1] if stderr_tail else None,
        }

    def _wait_for_external_socket(self) -> bool:
        deadline = time.monotonic() + self._startup_timeout_s
        while time.monotonic() < deadline:
            if _socket_is_listening(self._socket_path):
                with self._lock:
                    self._state = "running_external"
                return True
            time.sleep(0.1)
        with self._lock:
            self._state = "error"
            self._last_error = (
                f"external radar-core socket did not become ready at "
                f"{self._socket_path!r} within {self._startup_timeout_s:.1f}s"
            )
        return False

    def _start_managed_worker(self) -> bool:
        if not os.path.isfile(self._binary_path):
            with self._lock:
                self._state = "error"
                self._last_error = f"radar-core binary not found at {self._binary_path!r}"
            return False
        if not os.access(self._binary_path, os.X_OK):
            with self._lock:
                self._state = "error"
                self._last_error = f"radar-core binary is not executable: {self._binary_path!r}"
            return False

        socket_parent = Path(self._socket_path).parent
        try:
            socket_parent.mkdir(parents=True, exist_ok=True)
        except OSError as exc:
            with self._lock:
                self._state = "error"
                self._last_error = f"failed to create socket directory {str(socket_parent)!r}: {exc}"
            return False

        if os.path.exists(self._socket_path) and not _socket_is_listening(self._socket_path):
            try:
                os.remove(self._socket_path)
            except OSError as exc:
                with self._lock:
                    self._state = "error"
                    self._last_error = f"failed to remove stale socket {self._socket_path!r}: {exc}"
                return False

        env = os.environ.copy()
        env["RADAR_CORE_DEBUG"] = "1" if self._env_debug_flag else "0"

        try:
            process = subprocess.Popen(
                [self._binary_path, "--socket", self._socket_path],
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                text=True,
                bufsize=1,
                env=env,
            )
        except OSError as exc:
            with self._lock:
                self._state = "error"
                self._last_error = f"failed to launch radar-core: {exc}"
            return False

        with self._lock:
            self._process = process
            self._started_by_backend = True
            self._state = "launching"

        self._start_stream_reader_threads(process)
        return self._wait_for_managed_ready(process)

    def _wait_for_managed_ready(self, process: subprocess.Popen) -> bool:
        deadline = time.monotonic() + self._startup_timeout_s
        while time.monotonic() < deadline:
            if _socket_is_listening(self._socket_path):
                with self._lock:
                    self._state = "running"
                return True
            exit_code = process.poll()
            if exit_code is not None:
                stderr_hint = self.stats().get("last_stderr")
                with self._lock:
                    self._state = "error"
                    base = f"radar-core exited before readiness (exit={exit_code})"
                    self._last_error = f"{base}; stderr={stderr_hint}" if stderr_hint else base
                return False
            time.sleep(0.1)

        with self._lock:
            self._state = "error"
            base = (
                f"radar-core socket did not become ready at {self._socket_path!r} "
                f"within {self._startup_timeout_s:.1f}s"
            )
            stderr_hint = self._stderr_tail[-1] if self._stderr_tail else None
            self._last_error = f"{base}; stderr={stderr_hint}" if stderr_hint else base
        self._terminate_process(process)
        return False

    def _terminate_process(self, process: subprocess.Popen) -> None:
        if process.poll() is not None:
            return
        try:
            process.terminate()
            process.wait(timeout=5.0)
            return
        except subprocess.TimeoutExpired:
            pass
        except OSError:
            return

        try:
            process.kill()
            process.wait(timeout=2.0)
        except (subprocess.TimeoutExpired, OSError):
            pass

    def _start_stream_reader_threads(self, process: subprocess.Popen) -> None:
        def _reader(stream_name: str, stream, ring: deque[str]) -> None:
            if stream is None:
                return
            try:
                for line in iter(stream.readline, ""):
                    text = line.strip()
                    if not text:
                        continue
                    with self._lock:
                        ring.append(text)
                    if self._debug_logging:
                        log.debug("radar-core %s: %s", stream_name, text)
            except Exception:
                if self._debug_logging:
                    log.debug("radar-core %s reader failed", stream_name, exc_info=True)
            finally:
                try:
                    stream.close()
                except Exception:
                    pass

        self._stdout_thread = threading.Thread(
            target=_reader,
            args=("stdout", process.stdout, self._stdout_tail),
            daemon=True,
            name="radar-core-stdout",
        )
        self._stderr_thread = threading.Thread(
            target=_reader,
            args=("stderr", process.stderr, self._stderr_tail),
            daemon=True,
            name="radar-core-stderr",
        )
        self._stdout_thread.start()
        self._stderr_thread.start()
