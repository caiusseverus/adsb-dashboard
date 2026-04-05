import os
import sys
import asyncio
import math

sys.path.insert(0, os.path.dirname(os.path.dirname(__file__)))

from aircraft_state import Aircraft, AircraftState, _published_position
from timing import router, timing_events


def test_timing_events_store_inline_icao_without_aircraft_lookup():
    state = AircraftState()

    timing_ref = state._timing_arrival_ref(12_000_000, None)
    state._record_timing_event(timing_ref, 17, 14, 37, "ABC123", 123.4)

    state._aircraft["ABC123"] = object()
    state._aircraft.clear()

    events = state.get_timing_events(0)

    assert len(events) == 1
    seq, arrival_us, df, msg_len, signal_dbfs, source_class, icao, bearing_deg = events[0]
    assert seq == 1
    assert arrival_us == 0
    assert df == 17
    assert msg_len == 14
    assert math.isclose(signal_dbfs, -16.8)
    assert source_class == int(state._classify_timing_source(17))
    assert icao == "ABC123"
    assert bearing_deg == 123.4


def test_timing_events_store_none_when_bearing_is_unavailable():
    state = AircraftState()

    timing_ref = state._timing_arrival_ref(12_000_000, None)
    state._record_timing_event(timing_ref, 11, 7, 12, "ABC123", None)

    events = state.get_timing_events(0)

    assert events == [(1, 0, 11, 7, -26.5, int(state._classify_timing_source(11)), "ABC123", None)]


def test_published_position_gates_bearing_before_timing_event_record():
    state = AircraftState()
    ac = Aircraft(icao="ABC123", lat=51.5, lon=-0.1, range_nm=12.3, bearing_deg=182.0)
    timing_ref = state._timing_arrival_ref(12_000_000, None)

    state._record_timing_event(timing_ref, 17, 14, 44, "ABC123", _published_position(ac)[3])
    assert state.get_timing_events(0)[0][-1] is None

    ac.pos_reliable_odd = 2.0
    ac.pos_reliable_even = 2.0
    ac.pos_global = True
    timing_ref = state._timing_arrival_ref(12_000_120, None)
    state._record_timing_event(timing_ref, 17, 14, 45, "ABC123", _published_position(ac)[3])

    assert state.get_timing_events(1)[0][-1] == 182.0


def test_timing_events_endpoint_serializes_widened_shape():
    class FakeState:
        def get_timing_events(self, since_seq):
            assert since_seq == 0
            return [(7, 12345, 17, 14, -10.5, 6, "ABC123", 278.6)]

        def get_timing_now_us(self):
            return 13000

    router._state = FakeState()

    payload = asyncio.run(timing_events(0))

    assert payload == {
        "now_us": 13000,
        "events": [[7, 12345, 17, 14, -10.5, 6, "ABC123", 278.6]],
    }
