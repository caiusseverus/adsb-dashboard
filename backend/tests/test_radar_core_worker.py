import os
import socket
import subprocess
import sys
import textwrap

from radar_core.worker import RadarCoreWorker


def _write_fake_radar_core(path: str) -> None:
    script = textwrap.dedent(
        """\
        #!/usr/bin/env python3
        import argparse
        import os
        import signal
        import socket
        import time

        parser = argparse.ArgumentParser()
        parser.add_argument("--socket", required=True)
        args = parser.parse_args()

        os.makedirs(os.path.dirname(args.socket), exist_ok=True)
        try:
            os.remove(args.socket)
        except FileNotFoundError:
            pass

        srv = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
        srv.bind(args.socket)
        srv.listen(1)
        srv.settimeout(0.2)

        running = True
        def _stop(*_):
            nonlocal_running[0] = False

        nonlocal_running = [True]
        signal.signal(signal.SIGTERM, _stop)
        signal.signal(signal.SIGINT, _stop)

        while nonlocal_running[0]:
            try:
                conn, _ = srv.accept()
                conn.close()
            except TimeoutError:
                pass
            except OSError:
                break
            time.sleep(0.01)

        srv.close()
        """
    )
    with open(path, "w", encoding="utf-8") as f:
        f.write(script)
    os.chmod(path, 0o755)


def test_radar_core_worker_managed_start_and_stop(tmp_path):
    fake_bin = tmp_path / "fake-radar-core"
    sock_path = tmp_path / "run" / "radar-core.sock"
    _write_fake_radar_core(str(fake_bin))

    worker = RadarCoreWorker(
        binary_path=str(fake_bin),
        socket_path=str(sock_path),
        managed=True,
        startup_timeout_s=3.0,
    )

    assert worker.start() is True
    running = worker.stats()
    assert running["state"] == "running"
    assert running["started_by_backend"] is True
    assert running["socket_ready"] is True

    worker.stop()
    stopped = worker.stats()
    assert stopped["started_by_backend"] is False
    assert stopped["state"] == "stopped"


def test_radar_core_worker_external_mode_uses_existing_socket(tmp_path):
    sock_path = str(tmp_path / "existing.sock")
    server = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
    server.bind(sock_path)
    server.listen(1)
    try:
        worker = RadarCoreWorker(
            binary_path="/nonexistent/radar-core",
            socket_path=sock_path,
            managed=False,
            startup_timeout_s=0.5,
        )
        assert worker.start() is True
        stats = worker.stats()
        assert stats["started_by_backend"] is False
        assert stats["state"] in {"ready_external_socket", "running_external"}
    finally:
        server.close()


def test_radar_core_worker_reports_missing_binary(tmp_path):
    worker = RadarCoreWorker(
        binary_path=str(tmp_path / "missing-radar-core"),
        socket_path=str(tmp_path / "radar-core.sock"),
        managed=True,
        startup_timeout_s=0.5,
    )
    assert worker.start() is False
    stats = worker.stats()
    assert stats["state"] == "error"
    assert "not found" in (stats["last_error"] or "")


def test_radar_core_worker_removes_stale_socket_file(tmp_path):
    fake_bin = tmp_path / "fake-radar-core"
    sock_path = tmp_path / "run" / "radar-core.sock"
    _write_fake_radar_core(str(fake_bin))
    sock_path.parent.mkdir(parents=True, exist_ok=True)
    sock_path.write_text("stale", encoding="utf-8")

    worker = RadarCoreWorker(
        binary_path=str(fake_bin),
        socket_path=str(sock_path),
        managed=True,
        startup_timeout_s=3.0,
    )

    assert worker.start() is True
    stats = worker.stats()
    assert stats["running"] is True
    assert stats["socket_exists"] is True
    worker.stop()


def test_radar_core_worker_start_is_idempotent_while_running(tmp_path):
    fake_bin = tmp_path / "fake-radar-core"
    sock_path = tmp_path / "run" / "radar-core.sock"
    _write_fake_radar_core(str(fake_bin))

    worker = RadarCoreWorker(
        binary_path=str(fake_bin),
        socket_path=str(sock_path),
        managed=True,
        startup_timeout_s=3.0,
    )

    assert worker.start() is True
    first = worker.stats()
    assert first["pid"] is not None

    assert worker.start() is True
    second = worker.stats()
    assert second["pid"] == first["pid"]
    assert second["running"] is True

    worker.stop()


def test_radar_core_worker_marks_exited_process_not_running(tmp_path):
    sock_path = tmp_path / "run" / "radar-core.sock"
    worker = RadarCoreWorker(
        binary_path=sys.executable,
        socket_path=str(sock_path),
        managed=True,
        startup_timeout_s=0.5,
    )

    proc = subprocess.Popen([sys.executable, "-c", "pass"])
    proc.wait(timeout=2.0)
    worker._process = proc
    worker._started_by_backend = True
    worker._state = "running"

    stats = worker.stats()
    assert stats["running"] is False
    assert stats["exit_code"] is None
    assert stats["pid"] is None
    assert stats["state"] == "exited"
    assert "exit=0" in (stats["last_error"] or "")
    assert worker.stats()["started_by_backend"] is False
