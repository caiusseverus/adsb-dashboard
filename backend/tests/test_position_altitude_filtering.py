"""
Tests for CPR position gate (Fix 1) and altitude filter tightening (Fix 2).
"""
import sys
import os
sys.path.insert(0, os.path.dirname(os.path.dirname(__file__)))

import pytest
from aircraft_state import Aircraft, MsgSource, _pos_reliable, _accept_adsb_position, _accept_altitude


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def make_ac(**kwargs) -> Aircraft:
    """Create a minimal Aircraft for testing. All unset fields use dataclass defaults."""
    return Aircraft(icao="ABC123", **kwargs)


# ---------------------------------------------------------------------------
# Fix 1: _pos_reliable() requires pos_global=True
# ---------------------------------------------------------------------------

class TestPosReliableRequiresGlobal:

    def test_returns_false_when_pos_global_false_even_with_high_scores(self):
        """Local-decode-only aircraft must not be published even if reliability is high."""
        ac = make_ac(
            lat=51.5, lon=-0.1,
            pos_reliable_odd=4.0,
            pos_reliable_even=4.0,
            pos_global=False,   # no global decode yet
        )
        assert _pos_reliable(ac) is False

    def test_returns_true_when_pos_global_true_and_scores_meet_threshold(self):
        """Normal case: global decode established, both scores >= 2.0."""
        ac = make_ac(
            lat=51.5, lon=-0.1,
            pos_reliable_odd=2.0,
            pos_reliable_even=2.0,
            pos_global=True,
        )
        assert _pos_reliable(ac) is True

    def test_returns_false_when_lat_is_none(self):
        """No position at all → False."""
        ac = make_ac(pos_global=True, pos_reliable_odd=4.0, pos_reliable_even=4.0)
        assert _pos_reliable(ac) is False

    def test_mlat_bypasses_pos_global_check(self):
        """MLAT positions are validated by network geometry, not CPR — always trusted."""
        ac = make_ac(lat=51.5, lon=-0.1, mlat=True, pos_global=False)
        assert _pos_reliable(ac) is True

    def test_returns_false_when_scores_below_threshold(self):
        """pos_global=True but scores not yet at publish threshold."""
        ac = make_ac(
            lat=51.5, lon=-0.1,
            pos_reliable_odd=1.9,
            pos_reliable_even=2.0,
            pos_global=True,
        )
        assert _pos_reliable(ac) is False


# ---------------------------------------------------------------------------
# Fix 1: _accept_adsb_position() speed check gated on pos_global
# ---------------------------------------------------------------------------

class TestAcceptAdsbPositionSpeedGate:

    def test_speed_check_skipped_when_pos_global_false(self):
        """
        When pos_global=False, the speed check must be skipped so that the first
        correct global decode is not rejected for being 'too far' from a wrong-zone
        local-decode position stored in ac.lat/ac.lon.

        Simulate: ac has a wrong-zone local position at (51.5, -0.1).
        A correct global decode arrives at (51.5, 3.5) — ~130nm away, which would
        fail the speed gate if checked. With pos_global=False the speed check
        must be bypassed and the new position written.
        """
        ac = make_ac(
            lat=51.5, lon=-0.1,      # wrong-zone local position
            pos_reliable_odd=1.0,
            pos_reliable_even=0.0,
            pos_global=False,
            last_pos_ts=1000.0,
        )
        # Correct global decode 130nm away, 1 second later
        _accept_adsb_position(ac, lat=51.5, lon=3.5, pos_from_global=True,
                               cpr_odd=False, now=1001.0)
        # Position must be accepted and written
        assert ac.lon == pytest.approx(3.5, abs=0.001)
        assert ac.pos_global is True

    def test_speed_check_active_when_pos_global_true(self):
        """
        After global decode is established, the speed check must reject
        physically impossible position jumps.
        """
        ac = make_ac(
            lat=51.5, lon=-0.1,
            pos_reliable_odd=3.0,
            pos_reliable_even=3.0,
            pos_global=True,
            last_pos_ts=1000.0,
        )
        original_lon = ac.lon
        # Position 5000nm away in 1 second — physically impossible
        _accept_adsb_position(ac, lat=51.5, lon=70.0, pos_from_global=True,
                               cpr_odd=False, now=1001.0)
        # Must be rejected: lon unchanged
        assert ac.lon == pytest.approx(original_lon, abs=0.001)


# ---------------------------------------------------------------------------
# Fix 1: _accept_adsb_position() fast-track gated on pos_global
# ---------------------------------------------------------------------------

class TestAcceptAdsbPositionFastTrack:

    def test_fast_track_not_applied_when_pos_global_false(self):
        """
        Fast-track would promote pos_reliable to 2.0 if within 27nm of last known.
        With pos_global=False, the 'last known' is a wrong-zone position — fast-track
        must not fire.
        """
        ac = make_ac(
            lat=51.5, lon=-0.1,       # wrong-zone position
            pos_reliable_odd=0.0,
            pos_reliable_even=0.0,
            pos_global=False,
            last_pos_ts=1000.0,
        )
        # New position within 27nm of the wrong-zone position (so fast-track would fire)
        _accept_adsb_position(ac, lat=51.5, lon=-0.05, pos_from_global=False,
                               cpr_odd=True, now=1001.0)
        # Fast-track must NOT have fired: score should be 0.0 + 1.0 = 1.0, not 2.0
        assert ac.pos_reliable_odd == pytest.approx(1.0, abs=0.01)

    def test_fast_track_fires_when_pos_global_true(self):
        """
        After global decode, fast-track should promote score to 2.0 when within 27nm.
        """
        ac = make_ac(
            lat=51.5, lon=-0.1,
            pos_reliable_odd=0.0,
            pos_reliable_even=0.0,
            pos_global=True,
            last_pos_ts=900.0,   # 100s ago so 1.87nm implies ~67kt — passes speed gate
        )
        _accept_adsb_position(ac, lat=51.5, lon=-0.05, pos_from_global=True,
                               cpr_odd=True, now=1000.0)
        # Fast-track must have promoted odd score to at least 2.0
        assert ac.pos_reliable_odd >= 2.0
