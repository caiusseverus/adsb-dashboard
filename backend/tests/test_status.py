import asyncio

import status


def test_status_exposes_stage8_and_go_refiner_flags(monkeypatch):
    monkeypatch.setattr(status._config, "RADAR_SYNC_CONTAMINATION_DETECTION_ENABLED", True)
    monkeypatch.setattr(status._config, "RADAR_SYNC_GO_REFINER_OPERATIONAL", False)
    monkeypatch.setattr(status._config, "RADAR_CORE_ENABLED", True)
    monkeypatch.setattr(status._config, "RADAR_CORE_MANAGED", True)

    status.register_runtime_stats(
        lambda: {
            "ws_clients": 0,
            "radar_core": {
                "radar_core_pid": 1234,
                "radar_core_running": False,
                "radar_core_defunct": False,
                "radar_core_exit_code": 2,
                "socket_path": "/tmp/radar-core.sock",
                "radar_core_connected": False,
                "radar_core_last_error": "exited",
            },
        }
    )

    payload = asyncio.run(status.get_status())
    cfg = payload["config"]

    assert cfg["runtime_git_sha"] == status.RUNTIME_GIT_SHA
    assert cfg["RADAR_SYNC_CONTAMINATION_DETECTION_ENABLED"] is True
    assert cfg["RADAR_SYNC_GO_REFINER_OPERATIONAL"] is False
    assert cfg["RADAR_CORE_ENABLED"] is True
    assert cfg["RADAR_CORE_MANAGED"] is True


def test_status_reports_radar_core_runtime_health(monkeypatch):
    status.register_runtime_stats(
        lambda: {
            "ws_clients": 0,
            "radar_core": {
                "radar_core_pid": 7788,
                "radar_core_running": False,
                "radar_core_defunct": True,
                "radar_core_exit_code": 9,
                "socket_path": "/tmp/test.sock",
                "radar_core_connected": False,
                "radar_core_last_error": "radar-core exited (exit=9)",
            },
        }
    )

    payload = asyncio.run(status.get_status())
    runtime = payload["runtime"]

    assert runtime["radar_core_pid"] == 7788
    assert runtime["radar_core_running"] is False
    assert runtime["radar_core_defunct"] is True
    assert runtime["radar_core_exit_code"] == 9
    assert runtime["radar_core_socket_path"] == "/tmp/test.sock"
    assert runtime["radar_core_connected"] is False
    assert "exited" in runtime["radar_core_last_error"]


def test_status_payload_backward_compatible_keys_present():
    status.register_runtime_stats(lambda: {"ws_clients": 0, "radar_core": {}})
    payload = asyncio.run(status.get_status())

    assert "config" in payload
    assert "radar_core" in payload
    assert "in_memory_state" in payload
    assert "runtime_git_sha" in payload["config"]
    assert "radar_core_enabled" in payload["config"]
