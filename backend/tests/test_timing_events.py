import os
import sys
import asyncio

sys.path.insert(0, os.path.dirname(os.path.dirname(__file__)))

from aircraft_state import AircraftState
from timing import router, timing_events


def test_timing_events_store_inline_icao_without_aircraft_lookup():
    state = AircraftState()

    timing_ref = state._timing_arrival_ref(12_000_000, None)
    state._record_timing_event(timing_ref, 17, 14, 37, "ABC123")

    state._aircraft["ABC123"] = object()
    state._aircraft.clear()

    events = state.get_timing_events(0)

    assert len(events) == 1
    seq, arrival_us, df, msg_len, signal_raw, source_class, icao = events[0]
    assert seq == 1
    assert arrival_us == 0
    assert df == 17
    assert msg_len == 14
    assert signal_raw == 37
    assert source_class == int(state._classify_timing_source(17))
    assert icao == "ABC123"


def test_timing_events_endpoint_serializes_widened_shape():
    class FakeState:
        def get_timing_events(self, since_seq):
            assert since_seq == 0
            return [(7, 12345, 17, 14, 21, 6, "ABC123")]

        def get_timing_now_us(self):
            return 13000

    router._state = FakeState()

    payload = asyncio.run(timing_events(0))

    assert payload == {
        "now_us": 13000,
        "events": [[7, 12345, 17, 14, 21, 6, "ABC123"]],
    }
