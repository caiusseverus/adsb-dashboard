import os
import sys

import config

sys.path.insert(0, os.path.dirname(os.path.dirname(__file__)))

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
