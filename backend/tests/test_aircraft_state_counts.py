import os
import sys

import config

sys.path.insert(0, os.path.dirname(os.path.dirname(__file__)))

import aircraft_state as state_module
from aircraft_state import AircraftState


def test_hybrid_mode_uses_beast_messages_for_live_totals(monkeypatch):
    monkeypatch.setattr(config, "INGEST_MODE", "hybrid")

    state = AircraftState()
    monkeypatch.setattr(state, "_decode", lambda *args, **kwargs: None)

    state.process_message({"raw": b"\x8d", "signal": 0, "timestamp": 123})
    snapshot = state.get_snapshot()
    assert snapshot["total_messages"] == 1

    state.update_from_json([], now=1_000.0, total_messages=10_000)
    snapshot = state.get_snapshot()
    assert snapshot["total_messages"] == 1

    state.process_message({"raw": b"\x8d", "signal": 0, "timestamp": 124})
    snapshot = state.get_snapshot()
    assert snapshot["total_messages"] == 2


def test_readsb_mode_still_uses_aircraft_json_message_deltas(monkeypatch):
    monkeypatch.setattr(config, "INGEST_MODE", "readsb")

    state = AircraftState()

    state.update_from_json([], now=1_000.0, total_messages=10_000)
    snapshot = state.get_snapshot()
    assert snapshot["total_messages"] == 0

    state.update_from_json([], now=1_001.0, total_messages=10_280)
    snapshot = state.get_snapshot()
    assert snapshot["total_messages"] == 280


def test_process_messages_batch_records_phase_timing(monkeypatch):
    monkeypatch.setattr(config, "INGEST_MODE", "beast")

    state_module.decoder_phase_timings.clear()
    state = AircraftState()
    monkeypatch.setattr(state, "_predecode_native_message", lambda raw, signal: {"df": 17, "addr": 0xABC123, "correctedbits": 0})
    monkeypatch.setattr(state, "_decode", lambda *args, **kwargs: None)

    state.process_messages_batch([
        ({"raw": b"\x8d" + b"\x00" * 13, "signal": 11, "timestamp": 123}, None),
        ({"raw": b"\x8d" + b"\x00" * 13, "signal": 12, "timestamp": 124}, None),
    ])

    sample = state_module.decoder_phase_timings[-1]
    assert sample["batch_size"] == 2
    assert sample["predecode_wall_ms"] >= 0
    assert sample["predecode_cpu_ms"] >= 0
    assert sample["lock_wait_ms"] >= 0
    assert sample["apply_wall_ms"] >= 0
    assert sample["apply_cpu_ms"] >= 0
    assert sample["total_wall_ms"] >= sample["apply_wall_ms"]
